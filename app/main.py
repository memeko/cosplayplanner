from __future__ import annotations

import csv
import calendar
import html
import io
import os
import re
import sqlite3
import uuid
from collections import defaultdict
from datetime import date, datetime, timedelta
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Callable
from urllib.parse import parse_qs, urlparse

import requests
from fastapi import Depends, FastAPI, Request
from fastapi.responses import HTMLResponse, PlainTextResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from passlib.context import CryptContext
from sqlalchemy import and_, func, inspect, or_, select, text
from sqlalchemy.orm import Session
from starlette.middleware.gzip import GZipMiddleware
from starlette.middleware.sessions import SessionMiddleware
from starlette.middleware.trustedhost import TrustedHostMiddleware

from .cosplay2_parser import guess_name_from_url, normalize_url, parse_events_from_homepage
from .database import Base, engine, get_db
from .models import (
    CardComment,
    CommunityArticle,
    CommunityArticleComment,
    CommunityArticleFavorite,
    CommunityMaster,
    CommunityMasterComment,
    CommunityQuestion,
    CommunityQuestionComment,
    CosplanCard,
    Festival,
    FestivalAnnouncement,
    FestivalNotification,
    InProgressCard,
    ProjectSearchPost,
    PersonalCalendarEvent,
    RehearsalCard,
    RehearsalEntry,
    User,
)
from .services import (
    as_list,
    esc_ics,
    get_options,
    merge_unique,
    parse_date,
    parse_float,
    remember_options,
    split_csv,
    to_bool,
)


def load_project_name() -> str:
    default_name = "Cosplay Planner"
    project_name_path = os.getenv("PROJECT_NAME_FILE", "PROJECT_NAME.txt")
    try:
        with open(project_name_path, "r", encoding="utf-8") as handle:
            loaded = handle.read().strip()
            return loaded or default_name
    except OSError:
        return default_name


def load_secret_key() -> str:
    secret = os.getenv("SECRET_KEY", "").strip()
    if not secret:
        raise RuntimeError(
            "SECRET_KEY is required. Set a strong random SECRET_KEY in environment variables."
        )
    if secret == "change-this-secret-key":
        raise RuntimeError("Insecure default SECRET_KEY is not allowed.")
    return secret


PROJECT_NAME = load_project_name()
app = FastAPI(title=PROJECT_NAME)

secret_key = load_secret_key()
session_https_only = to_bool(os.getenv("SESSION_HTTPS_ONLY", "0"))
trusted_hosts = [item.strip() for item in os.getenv("TRUSTED_HOSTS", "").split(",") if item.strip()]

if trusted_hosts:
    app.add_middleware(TrustedHostMiddleware, allowed_hosts=trusted_hosts)

app.add_middleware(GZipMiddleware, minimum_size=1024)
app.add_middleware(
    SessionMiddleware,
    secret_key=secret_key,
    max_age=60 * 60 * 24 * 30,
    same_site="lax",
    https_only=session_https_only,
    session_cookie="cosplay_session",
)

app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="app/templates")

password_context = CryptContext(schemes=["pbkdf2_sha256"], deprecated="auto")
RU_MONTH_NAMES = [
    "",
    "Январь",
    "Февраль",
    "Март",
    "Апрель",
    "Май",
    "Июнь",
    "Июль",
    "Август",
    "Сентябрь",
    "Октябрь",
    "Ноябрь",
    "Декабрь",
]

DEFAULT_NOMINATIONS = [
    "одиночное дефиле",
    "групповое дефиле",
    "сценка",
    "фотокосплей",
    "караоке",
]

NEAREST_BIG_CITY_BY_CITY: dict[str, set[str]] = {
    "тула": {"москва"},
    "калуга": {"москва"},
    "рязань": {"москва"},
    "владимир": {"москва"},
    "тверь": {"москва"},
    "ярославль": {"москва"},
    "смоленск": {"москва"},
    "москва": {"санктпетербург"},
    "санктпетербург": {"москва"},
}

CANONICAL_CITY_LABELS: dict[str, str] = {
    "москва": "Москва",
    "санктпетербург": "Санкт-Петербург",
}

REHEARSAL_SOURCE_PARTICIPANT = "participant"
REHEARSAL_SOURCE_LEADER = "leader"

REHEARSAL_STATUS_PROPOSED = "proposed"
REHEARSAL_STATUS_APPROVED = "approved"
REHEARSAL_STATUS_ACCEPTED = "accepted"
REHEARSAL_STATUS_DECLINED = "declined"

PROJECT_BOARD_STATUS_ACTIVE = "active"
PROJECT_BOARD_STATUS_FOUND = "found"
PROJECT_BOARD_STATUS_INACTIVE = "inactive"

QUESTION_STATUS_OPEN = "open"
QUESTION_STATUS_RESOLVED = "resolved"

MASTER_TYPE_OPTIONS = [
    "фотограф",
    "швея",
    "крафтер",
    "вигмейкер",
    "художник",
    "видеограф",
    "другое",
]

ARTICLE_MAX_TAGS = 15
ARTICLE_MAX_BODY_LENGTH = 15000

ANNOUNCEMENT_STATUS_PENDING = "pending"
ANNOUNCEMENT_STATUS_APPROVED = "approved"
ANNOUNCEMENT_STATUS_REJECTED = "rejected"

SPECIAL_HIGHLIGHT_USERNAME = "brfox_cosplay"
SPECIAL_HIGHLIGHT_EMAIL = "angenzel@gmail.com"

CHARACTER_BIRTHDAYS_SHEET_CSV_URL = (
    "https://docs.google.com/spreadsheets/d/1lUJd6q8k1jt2zIrs66Ebf1lZxvckBioVmtY0FI7rfFw/export?format=csv"
)
GENSHIN_BIRTHDAYS_API_URL = (
    "https://genshin-impact.fandom.com/ru/api.php"
    "?action=parse&page=%D0%94%D0%B5%D0%BD%D1%8C_%D1%80%D0%BE%D0%B6%D0%B4%D0%B5%D0%BD%D0%B8%D1%8F&format=json"
)
ANISEARCH_BIRTHDAYS_MONTH_URL = "https://www.anisearch.com/character/birthdays?month={month}"

HTTP_TIMEOUT_SECONDS = 8
NETWORK_CACHE_TTL_SECONDS = 60 * 60 * 6
NETWORK_CACHE: dict[str, tuple[datetime, Any]] = {}

RU_MONTH_WORDS_TO_NUM = {
    "января": 1,
    "февраля": 2,
    "марта": 3,
    "апреля": 4,
    "мая": 5,
    "июня": 6,
    "июля": 7,
    "августа": 8,
    "сентября": 9,
    "октября": 10,
    "ноября": 11,
    "декабря": 12,
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}

RUSSIA_FIXED_HOLIDAYS = [
    (1, 1, "Новогодние каникулы"),
    (1, 2, "Новогодние каникулы"),
    (1, 3, "Новогодние каникулы"),
    (1, 4, "Новогодние каникулы"),
    (1, 5, "Новогодние каникулы"),
    (1, 6, "Новогодние каникулы"),
    (1, 7, "Рождество Христово"),
    (1, 8, "Новогодние каникулы"),
    (2, 23, "День защитника Отечества"),
    (3, 8, "Международный женский день"),
    (5, 1, "Праздник Весны и Труда"),
    (5, 9, "День Победы"),
    (6, 12, "День России"),
    (11, 4, "День народного единства"),
]

SEASONAL_INFO_EVENTS = [
    {
        "country": "Япония",
        "name": "О-сёгацу (японский Новый год)",
        "kind": "fixed",
        "month": 1,
        "day": 1,
    },
    {
        "country": "Япония",
        "name": "Сэцубун",
        "kind": "fixed",
        "month": 2,
        "day": 3,
    },
    {
        "country": "Япония",
        "name": "Хина-мацури",
        "kind": "fixed",
        "month": 3,
        "day": 3,
    },
    {
        "country": "Япония",
        "name": "Ханами (сезон цветения сакуры)",
        "kind": "range",
        "start_month": 3,
        "start_day": 20,
        "end_month": 4,
        "end_day": 20,
    },
    {
        "country": "Япония",
        "name": "Golden Week",
        "kind": "range",
        "start_month": 4,
        "start_day": 29,
        "end_month": 5,
        "end_day": 5,
    },
    {
        "country": "Япония",
        "name": "Танабата",
        "kind": "fixed",
        "month": 7,
        "day": 7,
    },
    {
        "country": "Япония",
        "name": "Обон",
        "kind": "range",
        "start_month": 8,
        "start_day": 13,
        "end_month": 8,
        "end_day": 16,
    },
    {
        "country": "Япония",
        "name": "Момидзигари (сезон клёнов)",
        "kind": "range",
        "start_month": 10,
        "start_day": 15,
        "end_month": 11,
        "end_day": 30,
    },
    {
        "country": "Китай",
        "name": "Праздник Весны (китайский Новый год, ориентировочно)",
        "kind": "range",
        "start_month": 1,
        "start_day": 20,
        "end_month": 2,
        "end_day": 20,
    },
    {
        "country": "Китай",
        "name": "Праздник фонарей (ориентировочно)",
        "kind": "range",
        "start_month": 2,
        "start_day": 10,
        "end_month": 2,
        "end_day": 20,
    },
    {
        "country": "Китай",
        "name": "Цинмин",
        "kind": "fixed",
        "month": 4,
        "day": 4,
    },
    {
        "country": "Китай",
        "name": "Праздник драконьих лодок (ориентировочно)",
        "kind": "range",
        "start_month": 5,
        "start_day": 25,
        "end_month": 6,
        "end_day": 25,
    },
    {
        "country": "Китай",
        "name": "Праздник середины осени (ориентировочно)",
        "kind": "range",
        "start_month": 9,
        "start_day": 10,
        "end_month": 10,
        "end_day": 10,
    },
    {
        "country": "Китай",
        "name": "Национальный день Китая",
        "kind": "range",
        "start_month": 10,
        "start_day": 1,
        "end_month": 10,
        "end_day": 7,
    },
]


def apply_schema_migrations() -> None:
    # Lightweight SQLite migration path for local/self-host deployments.
    if not str(engine.url).startswith("sqlite"):
        return

    required_columns: dict[str, list[tuple[str, str]]] = {
        "users": [
            ("home_city", "VARCHAR(255)"),
            ("cosplay_nick", "VARCHAR(100)"),
            ("birth_date", "DATE"),
        ],
        "cosplan_cards": [
            ("costume_bought", "BOOLEAN NOT NULL DEFAULT 0"),
            ("costume_link", "TEXT"),
            ("costume_buy_price", "FLOAT"),
            ("costume_fabric_price", "FLOAT"),
            ("costume_hardware_price", "FLOAT"),
            ("costume_notes", "TEXT"),
            ("shoes_buy_price", "FLOAT"),
            ("lenses_price", "FLOAT"),
            ("lenses_currency", "VARCHAR(16)"),
            ("wig_buy_price", "FLOAT"),
            ("coproplayer_nicks_json", "JSON NOT NULL DEFAULT '[]'"),
            ("is_shared_copy", "BOOLEAN NOT NULL DEFAULT 0"),
            ("source_card_id", "INTEGER"),
            ("shared_from_user_id", "INTEGER"),
            ("wig_no_buy_from", "VARCHAR(255)"),
            ("wig_restyle", "BOOLEAN NOT NULL DEFAULT 0"),
            ("craft_type", "VARCHAR(32)"),
            ("craft_master", "VARCHAR(255)"),
            ("craft_price", "FLOAT"),
            ("craft_material_price", "FLOAT"),
            ("craft_deadline", "DATE"),
            ("craft_currency", "VARCHAR(16)"),
            ("related_cards_json", "JSON NOT NULL DEFAULT '[]'"),
            ("references_json", "JSON NOT NULL DEFAULT '[]'"),
            ("pose_references_json", "JSON NOT NULL DEFAULT '[]'"),
            ("unknown_prices_json", "JSON NOT NULL DEFAULT '[]'"),
            ("costume_parts_json", "JSON NOT NULL DEFAULT '[]'"),
            ("craft_parts_json", "JSON NOT NULL DEFAULT '[]'"),
            ("photoset_photographer_price", "FLOAT"),
            ("photoset_studio_price", "FLOAT"),
            ("photoset_props_price", "FLOAT"),
            ("photoset_extra_price", "FLOAT"),
            ("photoset_comment", "TEXT"),
            ("photoset_props_checklist_json", "JSON NOT NULL DEFAULT '[]'"),
            ("performance_track", "VARCHAR(255)"),
            ("performance_video_bg_url", "TEXT"),
            ("performance_script", "TEXT"),
            ("performance_light_script", "TEXT"),
            ("performance_duration", "VARCHAR(8)"),
            ("performance_rehearsal_point", "VARCHAR(255)"),
            ("performance_rehearsal_price", "FLOAT"),
            ("performance_rehearsal_currency", "VARCHAR(16)"),
            ("performance_rehearsal_count", "INTEGER"),
        ],
        "festival_notifications": [
            ("id", "INTEGER PRIMARY KEY"),
            ("user_id", "INTEGER NOT NULL"),
            ("from_user_id", "INTEGER"),
            ("source_card_id", "INTEGER"),
            ("message", "TEXT NOT NULL"),
            ("is_read", "BOOLEAN NOT NULL DEFAULT 0"),
            ("created_at", "DATETIME DEFAULT CURRENT_TIMESTAMP"),
        ],
        "project_search_posts": [
            ("status", "VARCHAR(32) NOT NULL DEFAULT 'active'"),
        ],
        "in_progress_cards": [
            ("is_frozen", "BOOLEAN NOT NULL DEFAULT 0"),
            ("task_rows_json", "JSON NOT NULL DEFAULT '[]'"),
        ],
        "festivals": [
            ("event_end_date", "DATE"),
            ("is_global_announcement", "BOOLEAN NOT NULL DEFAULT 0"),
            ("source_announcement_id", "INTEGER"),
        ],
    }

    with engine.begin() as conn:
        inspector = inspect(conn)
        existing_tables = set(inspector.get_table_names())

        # Ensure table exists (create via SQLAlchemy metadata when possible).
        if "festival_notifications" not in existing_tables:
            FestivalNotification.__table__.create(bind=conn, checkfirst=True)
        if "card_comments" not in existing_tables:
            CardComment.__table__.create(bind=conn, checkfirst=True)
        if "project_search_posts" not in existing_tables:
            ProjectSearchPost.__table__.create(bind=conn, checkfirst=True)
        if "community_questions" not in existing_tables:
            CommunityQuestion.__table__.create(bind=conn, checkfirst=True)
        if "community_question_comments" not in existing_tables:
            CommunityQuestionComment.__table__.create(bind=conn, checkfirst=True)
        if "community_masters" not in existing_tables:
            CommunityMaster.__table__.create(bind=conn, checkfirst=True)
        if "community_master_comments" not in existing_tables:
            CommunityMasterComment.__table__.create(bind=conn, checkfirst=True)
        if "community_articles" not in existing_tables:
            CommunityArticle.__table__.create(bind=conn, checkfirst=True)
        if "community_article_comments" not in existing_tables:
            CommunityArticleComment.__table__.create(bind=conn, checkfirst=True)
        if "community_article_favorites" not in existing_tables:
            CommunityArticleFavorite.__table__.create(bind=conn, checkfirst=True)
        if "festival_announcements" not in existing_tables:
            FestivalAnnouncement.__table__.create(bind=conn, checkfirst=True)
        if "rehearsal_cards" not in existing_tables:
            RehearsalCard.__table__.create(bind=conn, checkfirst=True)
        if "rehearsal_entries" not in existing_tables:
            RehearsalEntry.__table__.create(bind=conn, checkfirst=True)
        if "personal_calendar_events" not in existing_tables:
            PersonalCalendarEvent.__table__.create(bind=conn, checkfirst=True)

        for table_name, columns in required_columns.items():
            if table_name not in existing_tables and table_name != "festival_notifications":
                continue

            current_cols = {col["name"] for col in inspect(conn).get_columns(table_name)}
            for column_name, definition in columns:
                if column_name in current_cols:
                    continue
                if table_name == "festival_notifications" and column_name == "id":
                    # Table create handles PK.
                    continue
                conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {definition}"))


@app.on_event("startup")
def startup() -> None:
    Base.metadata.create_all(bind=engine)
    apply_schema_migrations()
    try:
        auto_backup_if_needed()
    except OSError:
        # Backup creation must not block app startup.
        pass
    except sqlite3.Error:
        pass
    except RuntimeError:
        pass


@app.middleware("http")
async def ensure_daily_backup(request: Request, call_next):
    try:
        auto_backup_if_needed()
    except OSError:
        pass
    except sqlite3.Error:
        pass
    except RuntimeError:
        pass
    return await call_next(request)


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat() + "Z"}


@app.get("/readyz")
def readyz(db: Session = Depends(get_db)) -> dict[str, str]:
    db.execute(text("SELECT 1"))
    return {"status": "ready"}


def estimate_card_total_and_currency(card: CosplanCard) -> tuple[float, str]:
    total = 0.0
    currencies: set[str] = set()
    has_unknown = False

    def add(value: float | None, currency: str | None) -> None:
        nonlocal total
        if value is None:
            return
        total += float(value)
        cleaned = (currency or "").strip().upper()
        if cleaned:
            currencies.add(cleaned)

    add(card.costume_prepayment, card.costume_currency)
    add(card.costume_postpayment, card.costume_currency)
    add(card.costume_buy_price, card.costume_currency)
    add(card.costume_fabric_price, card.costume_currency)
    add(card.costume_hardware_price, card.costume_currency)
    add(card.shoes_buy_price, card.shoes_currency)
    add(card.shoes_price, card.shoes_currency)
    add(card.lenses_price, card.lenses_currency)
    add(card.wig_price, card.wig_currency)
    add(card.wig_buy_price, card.wig_currency)
    add(card.craft_price, card.craft_currency)
    add(card.craft_material_price, card.craft_currency)
    photoset_breakdown = [
        card.photoset_photographer_price,
        card.photoset_studio_price,
        card.photoset_props_price,
        card.photoset_extra_price,
    ]
    if any(value is not None for value in photoset_breakdown):
        for value in photoset_breakdown:
            add(value, card.photoset_currency)
    else:
        add(card.photoset_price, card.photoset_currency)
    add(performance_rehearsal_total(card), card.performance_rehearsal_currency)

    for item in as_list(card.costume_parts_json):
        if not isinstance(item, dict):
            continue
        if to_bool(item.get("unknown")):
            has_unknown = True
            continue
        add(parse_float(str(item.get("price", ""))), str(item.get("currency", "")) or card.costume_currency)
    for item in as_list(card.craft_parts_json):
        if not isinstance(item, dict):
            continue
        if to_bool(item.get("unknown")):
            has_unknown = True
            continue
        add(parse_float(str(item.get("price", ""))), str(item.get("currency", "")) or card.craft_currency)

    if as_list(card.unknown_prices_json):
        has_unknown = True

    if has_unknown:
        return total, "УТОЧНЯЕТСЯ"
    if not currencies:
        return total, ""
    if len(currencies) == 1:
        return total, next(iter(currencies))
    return total, "MIXED"


def estimate_card_total(card: CosplanCard) -> float:
    total, _ = estimate_card_total_and_currency(card)
    return total


def normalize_username(value: str | None) -> str:
    if not value:
        return ""
    cleaned = value.strip()
    if cleaned.startswith("@"):
        cleaned = cleaned[1:]
    return cleaned


def nick_is_special(value: str | None) -> bool:
    return normalize_username(value).casefold() == SPECIAL_HIGHLIGHT_USERNAME


def user_is_special(user: User | None) -> bool:
    if not user:
        return False
    if nick_is_special(user.username) or nick_is_special(user.cosplay_nick):
        return True
    return (user.email or "").strip().casefold() == SPECIAL_HIGHLIGHT_EMAIL


def is_moderator_user(user: User | None) -> bool:
    return user_is_special(user)


def usernames_match(left: str | None, right: str | None) -> bool:
    left_value = normalize_username(left).casefold()
    right_value = normalize_username(right).casefold()
    return bool(left_value) and left_value == right_value


def user_aliases(user: User) -> list[str]:
    return merge_unique([normalize_username(user.username), normalize_username(user.cosplay_nick)])


def preferred_user_alias(user: User) -> str:
    return normalize_username(user.cosplay_nick) or normalize_username(user.username)


def build_user_alias_lookup(db: Session) -> tuple[dict[str, str], dict[str, User], list[str]]:
    users = db.execute(select(User).order_by(User.username)).scalars().all()
    alias_to_username: dict[str, str] = {}
    users_by_username: dict[str, User] = {}
    alias_options: list[str] = []

    for user in users:
        username_key = normalize_username(user.username)
        if not username_key:
            continue
        users_by_username[username_key.casefold()] = user

        for alias in user_aliases(user):
            alias_key = normalize_username(alias)
            if not alias_key:
                continue
            alias_to_username.setdefault(alias_key.casefold(), username_key)
            alias_options.append(alias_key)

    return alias_to_username, users_by_username, merge_unique(alias_options)


def resolve_alias_to_username(raw_alias: str | None, alias_to_username: dict[str, str]) -> str:
    cleaned = normalize_username(raw_alias)
    if not cleaned:
        return ""
    return alias_to_username.get(cleaned.casefold(), cleaned)


def resolve_aliases_to_usernames(raw_aliases: list[str], alias_to_username: dict[str, str]) -> list[str]:
    resolved = [resolve_alias_to_username(alias, alias_to_username) for alias in raw_aliases]
    return merge_unique(resolved)


def user_matches_alias(user: User, alias: str | None) -> bool:
    cleaned = normalize_username(alias).casefold()
    if not cleaned:
        return False
    return cleaned in {normalize_username(user.username).casefold(), normalize_username(user.cosplay_nick).casefold()}


def format_coproplayer_names(
    values: list[str],
    alias_to_username: dict[str, str],
    users_by_username: dict[str, User],
) -> list[str]:
    result: list[str] = []
    for value in values:
        normalized = normalize_username(value)
        if not normalized:
            continue
        canonical_username = alias_to_username.get(normalized.casefold(), normalized)
        target_user = users_by_username.get(canonical_username.casefold())
        if target_user:
            result.append(f"@{preferred_user_alias(target_user)}")
        else:
            result.append(f"@{normalized}")
    return merge_unique(result)


def normalize_city(value: str | None) -> str:
    if not value:
        return ""
    cleaned = value.strip().casefold().replace("ё", "е")
    cleaned = re.sub(r"\b(г\.?|город)\b", " ", cleaned)
    cleaned = re.sub(r"[^a-zа-я0-9]+", " ", cleaned)
    cleaned = " ".join(cleaned.split())
    compact = cleaned.replace(" ", "")

    aliases = {
        "спб": "санктпетербург",
        "санктпетербург": "санктпетербург",
        "санктпетербур": "санктпетербург",
        "sanktpeterburg": "санктпетербург",
        "saintpetersburg": "санктпетербург",
        "stpetersburg": "санктпетербург",
        "питер": "санктпетербург",
        "мск": "москва",
        "москва": "москва",
        "moskva": "москва",
        "moscow": "москва",
    }
    return aliases.get(compact, compact)


def city_matches(base_city: str | None, candidate_city: str | None) -> bool:
    left = normalize_city(base_city)
    right = normalize_city(candidate_city)
    if not left or not right:
        return False
    if left == right:
        return True
    if left in right or right in left:
        return True
    min_len = min(len(left), len(right))
    if min_len >= 5 and SequenceMatcher(None, left, right).ratio() >= 0.84:
        return True
    return False


def split_city_values(raw_value: str | None) -> list[str]:
    if not raw_value:
        return []
    parts = re.split(r"[,\n;|/]+", raw_value)
    return merge_unique([part.strip() for part in parts if part and part.strip()])


def city_matches_any(base_cities: list[str], candidate_city: str | None) -> bool:
    return any(city_matches(base, candidate_city) for base in base_cities if base)


def nearest_big_city_keys_for_home_cities(home_cities: list[str]) -> set[str]:
    normalized_home = {normalize_city(city) for city in home_cities if normalize_city(city)}
    nearest_keys: set[str] = set()
    for city_key in normalized_home:
        nearest_candidates = NEAREST_BIG_CITY_BY_CITY.get(city_key, set())
        for candidate in nearest_candidates:
            if candidate and candidate not in normalized_home:
                nearest_keys.add(candidate)
    return nearest_keys


def nearest_big_city_labels(nearest_city_keys: set[str]) -> list[str]:
    labels: list[str] = []
    for key in sorted(nearest_city_keys):
        labels.append(CANONICAL_CITY_LABELS.get(key, key))
    return labels


def can_comment_on_card(card: CosplanCard, user: User) -> bool:
    if card.plan_type != "project":
        return False
    if card.user_id == user.id:
        return True
    return user_matches_alias(user, card.project_leader)


def card_coproplayer_aliases(card: CosplanCard) -> list[str]:
    return merge_unique(as_list(card.coproplayer_nicks_json), as_list(card.coproplayers_json))


def card_task_assignee_options(
    card: CosplanCard,
    alias_to_username: dict[str, str],
    users_by_username: dict[str, User],
) -> list[dict[str, Any]]:
    options: list[dict[str, Any]] = []
    seen_usernames: set[str] = set()
    raw_aliases = merge_unique([card.project_leader], card_coproplayer_aliases(card))
    for alias in raw_aliases:
        normalized = normalize_username(alias)
        if not normalized:
            continue
        canonical_username = alias_to_username.get(normalized.casefold(), normalized)
        username_key = canonical_username.casefold()
        if username_key in seen_usernames:
            continue
        seen_usernames.add(username_key)
        matched_user = users_by_username.get(username_key)
        display_value = f"@{preferred_user_alias(matched_user)}" if matched_user else f"@{canonical_username}"
        options.append(
            {
                "value": canonical_username,
                "label": display_value,
                "user_id": matched_user.id if matched_user else None,
            }
        )
    return options


def format_in_progress_tasks(
    raw_items: list[Any],
    alias_to_username: dict[str, str],
    users_by_username: dict[str, User],
) -> list[dict[str, Any]]:
    tasks: list[dict[str, Any]] = []
    for item in raw_items:
        if not isinstance(item, dict):
            continue
        assignee_raw = normalize_username(item.get("assignee") or item.get("responsible"))
        canonical_username = alias_to_username.get(assignee_raw.casefold(), assignee_raw) if assignee_raw else ""
        matched_user = users_by_username.get(canonical_username.casefold()) if canonical_username else None
        assignee_label = (
            f"@{preferred_user_alias(matched_user)}"
            if matched_user
            else (f"@{canonical_username}" if canonical_username else "—")
        )
        tasks.append(
            {
                "assignee": canonical_username,
                "assignee_label": assignee_label,
                "task": str(item.get("task") or item.get("text") or "").strip(),
                "done": bool(item.get("done")),
            }
        )
    return tasks


def user_is_card_coproplayer(user: User, card: CosplanCard) -> bool:
    return any(user_matches_alias(user, alias) for alias in card_coproplayer_aliases(card))


def can_edit_card(user: User, card: CosplanCard) -> bool:
    if card.is_shared_copy:
        return False
    if card.user_id == user.id:
        return True
    if user_matches_alias(user, card.project_leader):
        return True
    return user_is_card_coproplayer(user, card)


def resolve_source_card(db: Session, card: CosplanCard | None) -> CosplanCard | None:
    if not card:
        return None
    if card.is_shared_copy and card.source_card_id:
        source = db.get(CosplanCard, card.source_card_id)
        if source:
            return source
    return card


def get_editable_card(db: Session, card_id: int, user: User) -> CosplanCard | None:
    requested = db.get(CosplanCard, card_id)
    source = resolve_source_card(db, requested)
    if not source:
        return None
    if can_edit_card(user, source):
        return source
    return None


def safe_redirect_target(target: str | None, fallback: str) -> str:
    if not target:
        return fallback
    cleaned = target.strip()
    if cleaned.startswith("/"):
        return cleaned
    return fallback


def parse_id_list(values: list[Any]) -> list[int]:
    parsed: list[int] = []
    for value in values:
        try:
            parsed_value = int(str(value).strip())
        except (TypeError, ValueError):
            continue
        if parsed_value <= 0 or parsed_value in parsed:
            continue
        parsed.append(parsed_value)
    return parsed


def parse_related_card_links(raw_values: list[Any], *, legacy_user_id: int | None = None) -> list[dict[str, int]]:
    links: list[dict[str, int]] = []
    seen: set[tuple[int, int]] = set()

    def parse_int(value: Any) -> int | None:
        try:
            parsed = int(str(value).strip())
        except (TypeError, ValueError):
            return None
        return parsed if parsed > 0 else None

    for raw in raw_values:
        card_id: int | None = None
        user_id: int | None = None

        if isinstance(raw, dict):
            card_id = parse_int(raw.get("card_id") or raw.get("id"))
            user_id = parse_int(raw.get("user_id"))
        else:
            card_id = parse_int(raw)
            user_id = legacy_user_id if legacy_user_id and legacy_user_id > 0 else None

        if not card_id:
            continue
        if not user_id and legacy_user_id and legacy_user_id > 0:
            user_id = legacy_user_id
        if not user_id:
            continue

        key = (card_id, user_id)
        if key in seen:
            continue
        seen.add(key)
        links.append({"card_id": card_id, "user_id": user_id})

    return links


def related_card_ids_for_user(
    raw_values: list[Any],
    *,
    target_user_id: int,
    legacy_user_id: int | None = None,
) -> list[int]:
    if target_user_id <= 0:
        return []
    related_links = parse_related_card_links(raw_values, legacy_user_id=legacy_user_id)
    return [item["card_id"] for item in related_links if item["user_id"] == target_user_id]


def get_accessible_card(
    db: Session,
    card_id: int,
    user: User,
    *,
    allow_project_leader: bool = False,
    allow_coproplayer: bool = False,
) -> CosplanCard | None:
    card = db.get(CosplanCard, card_id)
    if not card:
        return None
    if card.user_id == user.id:
        return card
    if allow_project_leader and not card.is_shared_copy and user_matches_alias(user, card.project_leader):
        return card
    if allow_coproplayer and not card.is_shared_copy and user_is_card_coproplayer(user, card):
        return card
    return None


def month_label_ru(value: date) -> str:
    return f"{RU_MONTH_NAMES[value.month]} {value.year}"


def cache_get_or_load(key: str, loader: Callable[[], Any], ttl_seconds: int = NETWORK_CACHE_TTL_SECONDS) -> Any:
    now = datetime.utcnow()
    cached = NETWORK_CACHE.get(key)
    if cached:
        cached_at, payload = cached
        if (now - cached_at).total_seconds() < ttl_seconds:
            return payload
    payload = loader()
    NETWORK_CACHE[key] = (now, payload)
    return payload


def safe_date_with_leap_support(year: int, month: int, day: int) -> date | None:
    try:
        return date(year, month, day)
    except ValueError:
        # 29 февраля в невисокосный год отображаем 28 февраля.
        if month == 2 and day == 29:
            return date(year, 2, 28)
        return None


def upcoming_user_birthdays_this_week(users: list[User], today: date) -> list[dict[str, Any]]:
    week_end = today + timedelta(days=6)
    result: list[dict[str, Any]] = []

    for item in users:
        if not item.birth_date:
            continue
        candidate = safe_date_with_leap_support(today.year, item.birth_date.month, item.birth_date.day)
        if not candidate:
            continue
        if candidate < today:
            candidate = safe_date_with_leap_support(today.year + 1, item.birth_date.month, item.birth_date.day)
        if not candidate or candidate < today or candidate > week_end:
            continue
        display_nick = normalize_username(item.cosplay_nick)
        if not display_nick:
            continue
        result.append(
            {
                "date": candidate,
                "display_nick": display_nick,
                "user": item,
            }
        )

    result.sort(key=lambda row: (row["date"], row["display_nick"].casefold()))
    return result


def parse_day_month_from_text(raw_text: str) -> tuple[int, int] | tuple[None, None]:
    value = (raw_text or "").strip().lower()
    if not value:
        return None, None

    numeric_match = re.search(r"\b(\d{1,2})\s*[./-]\s*(\d{1,2})\b", value)
    if numeric_match:
        day = int(numeric_match.group(1))
        month = int(numeric_match.group(2))
        if 1 <= month <= 12 and 1 <= day <= 31:
            return day, month

    word_match = re.search(
        r"\b(\d{1,2})\s+([а-яa-z]+)",
        value,
        flags=re.IGNORECASE,
    )
    if word_match:
        day = int(word_match.group(1))
        month_word = word_match.group(2).casefold()
        month = RU_MONTH_WORDS_TO_NUM.get(month_word)
        if month and 1 <= day <= 31:
            return day, month

    return None, None


def fetch_character_birthdays_from_sheet(month: int) -> list[dict[str, Any]]:
    def _load() -> list[dict[str, Any]]:
        try:
            response = requests.get(
                CHARACTER_BIRTHDAYS_SHEET_CSV_URL,
                timeout=HTTP_TIMEOUT_SECONDS,
                headers={"User-Agent": "Mozilla/5.0"},
            )
            response.raise_for_status()
        except requests.RequestException:
            return []

        text_value = response.content.decode("utf-8", errors="replace")
        rows = list(csv.reader(io.StringIO(text_value)))
        if not rows:
            return []

        header = [str(item or "").strip().casefold() for item in rows[0]]
        name_index = next((idx for idx, value in enumerate(header) if "имя" in value), 0)
        birthday_index = next((idx for idx, value in enumerate(header) if "рож" in value or "birth" in value), -1)
        if birthday_index < 0:
            return []

        payload: list[dict[str, Any]] = []
        seen: set[tuple[str, int]] = set()
        for row in rows[1:]:
            if birthday_index >= len(row):
                continue
            name = str(row[name_index]).strip() if name_index < len(row) else ""
            birthday_text = str(row[birthday_index]).strip()
            if not name or not birthday_text:
                continue
            day, parsed_month = parse_day_month_from_text(birthday_text)
            if parsed_month != month or not day:
                continue
            key = (name.casefold(), day)
            if key in seen:
                continue
            seen.add(key)
            payload.append(
                {
                    "day": day,
                    "name": name,
                    "source": "Google таблица (персонажи)",
                }
            )
        return payload

    return cache_get_or_load(f"character_birthdays_sheet:{month}", _load)


def fetch_character_birthdays_from_genshin(month: int) -> list[dict[str, Any]]:
    def _load() -> list[dict[str, Any]]:
        try:
            response = requests.get(
                GENSHIN_BIRTHDAYS_API_URL,
                timeout=HTTP_TIMEOUT_SECONDS,
                headers={"User-Agent": "Mozilla/5.0"},
            )
            response.raise_for_status()
            parsed = response.json()
            html_text = (
                parsed.get("parse", {})
                .get("text", {})
                .get("*", "")
            )
        except (requests.RequestException, ValueError):
            return []
        if not html_text:
            return []

        rows = re.findall(r"<tr[^>]*>(.*?)</tr>", html_text, flags=re.IGNORECASE | re.DOTALL)
        payload: list[dict[str, Any]] = []
        seen: set[tuple[str, int]] = set()
        for row_html in rows:
            date_match = re.search(r"\b(\d{2})-(\d{2})\b", row_html)
            if not date_match:
                continue
            month_num = int(date_match.group(1))
            day_num = int(date_match.group(2))
            if month_num != month or day_num <= 0:
                continue

            name_match = re.search(r'<img[^>]+alt="([^"]+)"', row_html)
            if name_match:
                name = html.unescape(name_match.group(1)).strip()
            else:
                plain_row = html.unescape(re.sub(r"<[^>]+>", " ", row_html))
                plain_row = re.sub(r"\s+", " ", plain_row).strip()
                name = plain_row.split(date_match.group(0), 1)[0].strip()
            if not name:
                continue

            key = (name.casefold(), day_num)
            if key in seen:
                continue
            seen.add(key)
            payload.append(
                {
                    "day": day_num,
                    "name": name,
                    "source": "Genshin Impact Wiki",
                }
            )
        return payload

    return cache_get_or_load(f"character_birthdays_genshin:{month}", _load)


def fetch_character_birthdays_from_anisearch(month: int) -> list[dict[str, Any]]:
    def _load() -> list[dict[str, Any]]:
        try:
            response = requests.get(
                ANISEARCH_BIRTHDAYS_MONTH_URL.format(month=month),
                timeout=HTTP_TIMEOUT_SECONDS,
                headers={"User-Agent": "Mozilla/5.0"},
            )
            response.raise_for_status()
            html_text = response.text
        except requests.RequestException:
            return []

        sections = re.findall(
            r'<section id="day-(\d{1,2})"[^>]*>(.*?)</section>',
            html_text,
            flags=re.IGNORECASE | re.DOTALL,
        )
        payload: list[dict[str, Any]] = []
        seen: set[tuple[str, int]] = set()
        for day_raw, section_html in sections:
            try:
                day_num = int(day_raw)
            except ValueError:
                continue
            names = re.findall(r'<img[^>]+alt="([^"]+)"', section_html, flags=re.IGNORECASE)
            for raw_name in names:
                name = html.unescape(raw_name).strip()
                if not name:
                    continue
                key = (name.casefold(), day_num)
                if key in seen:
                    continue
                seen.add(key)
                payload.append(
                    {
                        "day": day_num,
                        "name": name,
                        "source": "aniSearch",
                    }
                )
        return payload

    return cache_get_or_load(f"character_birthdays_anisearch:{month}", _load)


def character_birthdays_this_month(month: int) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    for producer in [
        fetch_character_birthdays_from_genshin,
        fetch_character_birthdays_from_sheet,
        fetch_character_birthdays_from_anisearch,
    ]:
        try:
            merged.extend(producer(month))
        except Exception:
            continue

    # Keep the list practical for page rendering.
    merged.sort(key=lambda item: (int(item.get("day") or 99), str(item.get("name", "")).casefold()))
    return merged[:220]


def clean_character_birthday_name(raw_name: str) -> str:
    value = (raw_name or "").strip()
    if not value:
        return ""
    # Убираем служебные подписи из alt/описаний вроде "Иконка X".
    value = re.sub(r"^\s*(иконка|icon)\s+", "", value, flags=re.IGNORECASE)
    value = re.sub(r"\s*[-:]\s*(иконка|icon)\s*$", "", value, flags=re.IGNORECASE)
    value = value.replace("Иконка", "").replace("icon", "").replace("Icon", "")
    return re.sub(r"\s+", " ", value).strip(" -")


def character_birthdays_today(today: date) -> list[dict[str, Any]]:
    monthly_rows = character_birthdays_this_month(today.month)
    items: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for row in monthly_rows:
        day_num = int(row.get("day") or 0)
        if day_num != today.day:
            continue
        name = clean_character_birthday_name(str(row.get("name", "")))
        source = str(row.get("source", "")).strip()
        if not name:
            continue
        key = (name.casefold(), source.casefold())
        if key in seen:
            continue
        seen.add(key)
        items.append(
            {
                "day": day_num,
                "name": name,
                "source": source or "источник не указан",
            }
        )
    items.sort(key=lambda item: str(item.get("name", "")).casefold())
    return items


def event_matches_day(day_value: date, event: dict[str, Any]) -> bool:
    kind = event.get("kind")
    if kind == "fixed":
        return day_value.month == int(event.get("month", 0)) and day_value.day == int(event.get("day", 0))
    if kind == "range":
        start = safe_date_with_leap_support(
            day_value.year,
            int(event.get("start_month", 1)),
            int(event.get("start_day", 1)),
        )
        end = safe_date_with_leap_support(
            day_value.year,
            int(event.get("end_month", 12)),
            int(event.get("end_day", 31)),
        )
        if not start or not end:
            return False
        return start <= day_value <= end
    return False


def weekly_infopovods(today: date) -> list[dict[str, Any]]:
    week_end = today + timedelta(days=6)
    items: list[dict[str, Any]] = []

    for offset in range(7):
        day_value = today + timedelta(days=offset)
        for month_num, day_num, title in RUSSIA_FIXED_HOLIDAYS:
            if day_value.month == month_num and day_value.day == day_num:
                items.append(
                    {
                        "date": day_value,
                        "date_label": day_value.strftime("%d-%m-%Y"),
                        "country": "Россия",
                        "title": title,
                        "note": "Могут не работать студии!",
                    }
                )

    for event in SEASONAL_INFO_EVENTS:
        if event.get("kind") == "fixed":
            event_day = safe_date_with_leap_support(
                today.year,
                int(event.get("month", 0)),
                int(event.get("day", 0)),
            )
            if event_day and today <= event_day <= week_end:
                items.append(
                    {
                        "date": event_day,
                        "date_label": event_day.strftime("%d-%m-%Y"),
                        "country": str(event.get("country", "")),
                        "title": str(event.get("name", "")),
                        "note": "",
                    }
                )
            continue

        if event.get("kind") == "range":
            start = safe_date_with_leap_support(
                today.year,
                int(event.get("start_month", 1)),
                int(event.get("start_day", 1)),
            )
            end = safe_date_with_leap_support(
                today.year,
                int(event.get("end_month", 12)),
                int(event.get("end_day", 31)),
            )
            if not start or not end:
                continue
            overlap_start = max(start, today)
            overlap_end = min(end, week_end)
            if overlap_start > overlap_end:
                continue
            date_label = (
                overlap_start.strftime("%d-%m-%Y")
                if overlap_start == overlap_end
                else f"{overlap_start.strftime('%d-%m-%Y')} — {overlap_end.strftime('%d-%m-%Y')}"
            )
            items.append(
                {
                    "date": overlap_start,
                    "date_label": date_label,
                    "country": str(event.get("country", "")),
                    "title": str(event.get("name", "")),
                    "note": "",
                }
            )

    # Preserve order by date and remove duplicates.
    seen: set[tuple[str, str, str]] = set()
    deduped: list[dict[str, Any]] = []
    for item in sorted(items, key=lambda row: (row["date"], row["country"], row["title"])):
        key = (str(item.get("date_label", "")), item["country"], item["title"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def month_calendar_grid(
    year: int,
    month: int,
    entries: list[dict[str, Any]],
) -> list[list[dict[str, Any]]]:
    calendar_builder = calendar.Calendar(firstweekday=0)
    matrix = calendar_builder.monthdayscalendar(year, month)
    day_types: dict[int, set[str]] = defaultdict(set)
    for entry in entries:
        entry_date = entry.get("date")
        if not isinstance(entry_date, date) or entry_date.year != year or entry_date.month != month:
            continue
        day_types[entry_date.day].add(str(entry.get("type_key") or ""))

    weeks: list[list[dict[str, Any]]] = []
    for week in matrix:
        week_cells: list[dict[str, Any]] = []
        for day_value in week:
            if day_value <= 0:
                week_cells.append(
                    {
                        "day": 0,
                        "type_keys": [],
                        "single_type": "",
                        "is_multi": False,
                    }
                )
                continue
            types = sorted([value for value in day_types.get(day_value, set()) if value])
            week_cells.append(
                {
                    "day": day_value,
                    "type_keys": types,
                    "single_type": (types[0] if len(types) == 1 else ""),
                    "is_multi": len(types) > 1,
                }
            )
        weeks.append(week_cells)
    return weeks


def festival_range_end(festival: Festival) -> date | None:
    if not festival.event_date:
        return festival.event_end_date
    if not festival.event_end_date or festival.event_end_date < festival.event_date:
        return festival.event_date
    return festival.event_end_date


def festival_is_active(festival: Festival, today: date) -> bool:
    if not festival.event_date:
        return True
    end_date = festival_range_end(festival)
    return bool(end_date and end_date >= today)


def parse_time_hhmm(raw: str) -> str | None:
    value = raw.strip()
    if not value:
        return None
    if len(value) != 5 or value[2] != ":":
        return None
    hh_raw, mm_raw = value.split(":", 1)
    if not (hh_raw.isdigit() and mm_raw.isdigit()):
        return None
    hh = int(hh_raw)
    mm = int(mm_raw)
    if not (0 <= hh <= 23 and 0 <= mm <= 59):
        return None
    return f"{hh:02d}:{mm:02d}"


def normalize_duration_mmss(raw: str | None) -> str | None:
    value = (raw or "").strip()
    if not value:
        return None
    match = re.fullmatch(r"(\d{1,3}):([0-5]\d)", value)
    if not match:
        return None
    minutes = int(match.group(1))
    seconds = int(match.group(2))
    return f"{minutes:02d}:{seconds:02d}"


def parse_positive_int(raw: str | None) -> int | None:
    value = (raw or "").strip()
    if not value:
        return None
    if not re.fullmatch(r"\d+", value):
        return None
    parsed = int(value)
    return parsed if parsed > 0 else None


def looks_like_url(value: str | None) -> bool:
    raw = (value or "").strip().lower()
    return raw.startswith("http://") or raw.startswith("https://")


def is_mp3_url(value: str | None) -> bool:
    if not looks_like_url(value):
        return False
    try:
        parsed = urlparse((value or "").strip())
    except ValueError:
        return False
    return (parsed.path or "").lower().endswith(".mp3")


def performance_rehearsal_total(card: CosplanCard) -> float | None:
    if card.performance_rehearsal_price is None:
        return None
    count = card.performance_rehearsal_count or 0
    if count <= 0:
        return None
    return float(card.performance_rehearsal_price) * float(count)


def rehearsal_status_label(status: str) -> str:
    mapping = {
        REHEARSAL_STATUS_PROPOSED: "Предложено",
        REHEARSAL_STATUS_APPROVED: "Одобрено",
        REHEARSAL_STATUS_ACCEPTED: "Принято",
        REHEARSAL_STATUS_DECLINED: "Отклонено",
    }
    return mapping.get(status, status)


def can_manage_project_card(user: User, card: CosplanCard) -> bool:
    if card.plan_type != "project":
        return False
    return user_matches_alias(user, card.project_leader)


def get_or_create_rehearsal_card(db: Session, *, user_id: int, cosplan_card: CosplanCard) -> RehearsalCard:
    rehearsal_card = db.execute(
        select(RehearsalCard).where(
            RehearsalCard.user_id == user_id,
            RehearsalCard.cosplan_card_id == cosplan_card.id,
        )
    ).scalar_one_or_none()
    if rehearsal_card:
        if rehearsal_card.deadline_date != cosplan_card.project_deadline:
            rehearsal_card.deadline_date = cosplan_card.project_deadline
        return rehearsal_card

    rehearsal_card = RehearsalCard(
        user_id=user_id,
        cosplan_card_id=cosplan_card.id,
        deadline_date=cosplan_card.project_deadline,
    )
    db.add(rehearsal_card)
    db.flush()
    return rehearsal_card


def sqlite_database_path() -> Path | None:
    if not str(engine.url).startswith("sqlite"):
        return None
    db_name = engine.url.database
    if not db_name or db_name == ":memory:":
        return None
    return Path(db_name).expanduser().resolve()


def backup_storage_path() -> Path:
    custom_path = os.getenv("BACKUP_DIR", "").strip()
    if custom_path:
        return Path(custom_path).expanduser().resolve()
    data_dir = Path("/data")
    if data_dir.exists() and os.access(data_dir, os.W_OK):
        return (data_dir / "backups").resolve()
    return Path("./backups").resolve()


def create_sqlite_backup_file(prefix: str = "cosplay-backup") -> Path:
    db_path = sqlite_database_path()
    if not db_path or not db_path.exists():
        raise RuntimeError("SQLite база данных не найдена.")

    backup_dir = backup_storage_path()
    backup_dir.mkdir(parents=True, exist_ok=True)
    suffix = datetime.now().strftime("%Y%m%d-%H%M%S")
    target_path = backup_dir / f"{prefix}-{suffix}.sqlite3"

    with sqlite3.connect(str(db_path)) as source_conn:
        with sqlite3.connect(str(target_path)) as target_conn:
            source_conn.backup(target_conn)
    return target_path


def auto_backup_if_needed() -> None:
    db_path = sqlite_database_path()
    if not db_path or not db_path.exists():
        return

    backup_dir = backup_storage_path()
    backup_dir.mkdir(parents=True, exist_ok=True)
    today_prefix = f"cosplay-backup-{date.today().strftime('%Y%m%d')}"
    exists_today = any(item.name.startswith(today_prefix) for item in backup_dir.glob("cosplay-backup-*.sqlite3"))
    if exists_today:
        return

    create_sqlite_backup_file(prefix=today_prefix)


def _short_names(names: list[str], limit: int = 3) -> str:
    unique = merge_unique(names)
    if not unique:
        return ""
    if len(unique) <= limit:
        return ", ".join(f"«{name}»" for name in unique)
    shown = ", ".join(f"«{name}»" for name in unique[:limit])
    return f"{shown} и ещё {len(unique) - limit}"


def iter_date_range(start_date: date | None, end_date: date | None) -> list[date]:
    if not start_date:
        return []
    resolved_end = end_date or start_date
    if resolved_end < start_date:
        resolved_end = start_date
    days: list[date] = []
    cursor = start_date
    while cursor <= resolved_end:
        days.append(cursor)
        cursor += timedelta(days=1)
    return days


def enqueue_notification_if_missing(
    db: Session,
    *,
    user_id: int,
    from_user_id: int | None,
    source_card_id: int | None,
    message: str,
) -> bool:
    conditions = [FestivalNotification.user_id == user_id, FestivalNotification.message == message]
    if from_user_id is None:
        conditions.append(FestivalNotification.from_user_id.is_(None))
    else:
        conditions.append(FestivalNotification.from_user_id == from_user_id)
    if source_card_id is None:
        conditions.append(FestivalNotification.source_card_id.is_(None))
    else:
        conditions.append(FestivalNotification.source_card_id == source_card_id)

    existing_notes = db.execute(select(FestivalNotification).where(and_(*conditions))).scalars().all()
    if existing_notes:
        existing_notes[0].is_read = False
        for duplicate_note in existing_notes[1:]:
            db.delete(duplicate_note)
        return False

    db.add(
        FestivalNotification(
            user_id=user_id,
            from_user_id=from_user_id,
            source_card_id=source_card_id,
            message=message,
            is_read=False,
        )
    )
    return True


def is_shared_card_notification_message(message: str | None) -> bool:
    value = (message or "").strip().casefold()
    if not value:
        return False
    legacy_markers = [
        "добавил(а) вас как сокосплеера в карточку",
        "обновил(а) карточку",
        "карточка добавлена по вашему нику другим пользователем",
        "карточка по вашему нику обновлена другим пользователем",
    ]
    return any(marker in value for marker in legacy_markers)


def remove_shared_card_notifications(
    db: Session,
    *,
    source_card_id: int,
    user_id: int | None = None,
) -> None:
    stmt = select(FestivalNotification).where(FestivalNotification.source_card_id == source_card_id)
    if user_id is not None:
        stmt = stmt.where(FestivalNotification.user_id == user_id)
    notes = db.execute(stmt).scalars().all()
    for note in notes:
        if is_shared_card_notification_message(note.message):
            db.delete(note)


def user_busy_items_on_date(
    db: Session,
    *,
    user_id: int,
    target_date: date,
    exclude_card_id: int | None = None,
    exclude_source_card_id: int | None = None,
    exclude_festival_id: int | None = None,
    ignore_festival_name: str | None = None,
    exclude_rehearsal_entry_id: int | None = None,
) -> list[str]:
    busy: list[str] = []

    festivals = db.execute(
        select(Festival).where(
            Festival.user_id == user_id,
            Festival.is_going.is_(True),
            Festival.event_date.is_not(None),
        )
    ).scalars().all()
    ignored_name = (ignore_festival_name or "").strip().casefold()
    for festival in festivals:
        if exclude_festival_id and festival.id == exclude_festival_id:
            continue
        if ignored_name and (festival.name or "").strip().casefold() == ignored_name:
            continue
        for festival_date in iter_date_range(festival.event_date, festival.event_end_date):
            if festival_date == target_date:
                busy.append(f"фестиваль «{festival.name or 'Без названия'}»")
                break

    cards = db.execute(
        select(CosplanCard).where(
            CosplanCard.user_id == user_id,
            CosplanCard.photoset_date == target_date,
        )
    ).scalars().all()
    for card in cards:
        if exclude_card_id and card.id == exclude_card_id:
            continue
        if exclude_source_card_id and card.is_shared_copy and card.source_card_id == exclude_source_card_id:
            continue
        busy.append(f"фотосет «{card.character_name or 'Без названия'}»")

    rehearsal_entries = db.execute(
        select(RehearsalEntry).where(
            RehearsalEntry.user_id == user_id,
            RehearsalEntry.entry_date == target_date,
            RehearsalEntry.status.in_(
                [
                    REHEARSAL_STATUS_PROPOSED,
                    REHEARSAL_STATUS_APPROVED,
                    REHEARSAL_STATUS_ACCEPTED,
                ]
            ),
        )
    ).scalars().all()
    for entry in rehearsal_entries:
        if exclude_rehearsal_entry_id and entry.id == exclude_rehearsal_entry_id:
            continue
        status_label = rehearsal_status_label(entry.status).lower()
        card_name = entry.cosplan_card.character_name if entry.cosplan_card else "карточка"
        busy.append(f"репетиция ({status_label}) по «{card_name}»")

    return merge_unique(busy)


def parse_reference_values(raw_value: str) -> list[str]:
    if not raw_value:
        return []
    items: list[str] = []
    chunks = re.split(r"[\n,;]+", raw_value)
    for chunk in chunks:
        value = chunk.strip()
        if not value:
            continue
        if value.startswith("iframe:"):
            iframe_src = value.removeprefix("iframe:").strip()
            if iframe_src.lower().startswith(("http://", "https://")):
                items.append(f"iframe:{iframe_src}")
            continue
        if value.lower().startswith("<iframe"):
            match = re.search(r'src=["\']([^"\']+)["\']', value, flags=re.IGNORECASE)
            if match:
                src = match.group(1).strip()
                if src.lower().startswith(("http://", "https://")):
                    items.append(f"iframe:{src}")
            continue
        if value.lower().startswith(("http://", "https://")):
            items.append(value)
    return merge_unique(items)


def pinterest_embed_src(url: str) -> str | None:
    value = (url or "").strip()
    if value.startswith("iframe:"):
        iframe_src = value.removeprefix("iframe:").strip()
        return iframe_src if iframe_src else None

    match = re.search(r"pinterest\.[^/]+/pin/(\d+)", value, flags=re.IGNORECASE)
    if match:
        pin_id = match.group(1)
        return f"https://assets.pinterest.com/ext/embed.html?id={pin_id}"
    return None


def parse_parts_from_form(form: Any, prefix: str, default_currency: str | None) -> list[dict[str, Any]]:
    row_ids = [str(value).strip() for value in form.getlist(f"{prefix}_part_row_id")]
    links = [str(value).strip() for value in form.getlist(f"{prefix}_part_link")]
    prices = [str(value).strip() for value in form.getlist(f"{prefix}_part_price")]
    comments = [str(value).strip() for value in form.getlist(f"{prefix}_part_comment")]
    unknown_ids = {str(value).strip() for value in form.getlist(f"{prefix}_part_unknown") if str(value).strip()}

    size = max(len(row_ids), len(links), len(prices), len(comments))
    if size == 0:
        return []

    rows: list[dict[str, Any]] = []
    for index in range(size):
        row_id = row_ids[index] if index < len(row_ids) and row_ids[index] else str(index)
        link = links[index] if index < len(links) else ""
        comment = comments[index] if index < len(comments) else ""
        raw_price = prices[index] if index < len(prices) else ""
        unknown = row_id in unknown_ids
        parsed_price = None if unknown else parse_float(raw_price)
        if not (link or comment or raw_price or unknown):
            continue
        rows.append(
            {
                "url": link,
                "price": parsed_price,
                "comment": comment,
                "currency": (default_currency or "").strip().upper(),
                "unknown": unknown,
            }
        )
    return rows


def format_parts_for_form(parts: list[Any]) -> list[dict[str, str]]:
    formatted: list[dict[str, str]] = []
    for idx, raw_item in enumerate(parts):
        if not isinstance(raw_item, dict):
            continue
        price = raw_item.get("price")
        formatted.append(
            {
                "row_id": str(raw_item.get("row_id") or f"row-{idx}"),
                "url": str(raw_item.get("url", "") or ""),
                "price": "" if price is None else f"{price:g}",
                "comment": str(raw_item.get("comment", "") or ""),
                "unknown": "__YES__" if to_bool(raw_item.get("unknown")) else "__NO__",
            }
        )
    return formatted


def parse_checklist_rows_from_form(form: Any, prefix: str) -> list[dict[str, Any]]:
    row_ids = [str(value).strip() for value in form.getlist(f"{prefix}_row_id")]
    texts = [str(value).strip() for value in form.getlist(f"{prefix}_text")]
    done_ids = {str(value).strip() for value in form.getlist(f"{prefix}_done") if str(value).strip()}
    size = max(len(row_ids), len(texts))
    if size == 0:
        return []

    items: list[dict[str, Any]] = []
    for index in range(size):
        row_id = row_ids[index] if index < len(row_ids) and row_ids[index] else str(index)
        text_value = texts[index].strip() if index < len(texts) else ""
        if not text_value:
            continue
        items.append({"text": text_value, "done": row_id in done_ids})
    return items


def format_checklist_for_form(items: list[Any]) -> list[dict[str, str]]:
    formatted: list[dict[str, str]] = []
    for index, raw_item in enumerate(items):
        if isinstance(raw_item, dict):
            text_value = str(raw_item.get("text", "")).strip()
            done_value = "__YES__" if to_bool(raw_item.get("done")) else "__NO__"
        else:
            text_value = str(raw_item).strip()
            done_value = "__NO__"
        if not text_value:
            continue
        formatted.append({"row_id": f"item-{index}", "text": text_value, "done": done_value})
    return formatted


def parse_master_price_rows_from_form(form: Any) -> list[dict[str, Any]]:
    row_ids = [str(value).strip() for value in form.getlist("price_row_id")]
    services = [str(value).strip() for value in form.getlist("price_service")]
    costs = [str(value).strip() for value in form.getlist("price_cost")]
    comments = [str(value).strip() for value in form.getlist("price_comment")]
    size = max(len(row_ids), len(services), len(costs), len(comments))
    if size == 0:
        return []

    rows: list[dict[str, Any]] = []
    for index in range(size):
        row_id = row_ids[index] if index < len(row_ids) and row_ids[index] else str(index)
        service = services[index] if index < len(services) else ""
        raw_cost = costs[index] if index < len(costs) else ""
        comment = comments[index] if index < len(comments) else ""
        parsed_cost = parse_float(raw_cost)
        if not (service or raw_cost or comment):
            continue
        rows.append(
            {
                "row_id": row_id,
                "service": service,
                "cost": parsed_cost,
                "comment": comment,
            }
        )
    return rows


def format_master_price_rows_for_form(rows: list[Any]) -> list[dict[str, str]]:
    formatted: list[dict[str, str]] = []
    for index, raw_row in enumerate(rows):
        if not isinstance(raw_row, dict):
            continue
        cost_value = raw_row.get("cost")
        formatted.append(
            {
                "row_id": str(raw_row.get("row_id") or f"price-{index}"),
                "service": str(raw_row.get("service", "") or ""),
                "cost": "" if cost_value is None else f"{cost_value:g}",
                "comment": str(raw_row.get("comment", "") or ""),
            }
        )
    return formatted


def project_board_status_label(status: str) -> str:
    mapping = {
        PROJECT_BOARD_STATUS_ACTIVE: "Активно",
        PROJECT_BOARD_STATUS_FOUND: "Найдено!",
        PROJECT_BOARD_STATUS_INACTIVE: "Не актуально",
    }
    return mapping.get(status, status)


def question_status_label(status: str) -> str:
    mapping = {
        QUESTION_STATUS_OPEN: "Открыт",
        QUESTION_STATUS_RESOLVED: "Вопрос решен",
    }
    return mapping.get(status, status)


def announcement_status_label(status: str) -> str:
    mapping = {
        ANNOUNCEMENT_STATUS_PENDING: "На модерации",
        ANNOUNCEMENT_STATUS_APPROVED: "Одобрено",
        ANNOUNCEMENT_STATUS_REJECTED: "Отклонено",
    }
    return mapping.get(status, status)


def parse_article_tags(raw_value: str) -> list[str]:
    tags = merge_unique(split_csv(raw_value))
    normalized: list[str] = []
    for tag in tags:
        cleaned = re.sub(r"\s+", " ", tag.strip())
        if not cleaned:
            continue
        if len(cleaned) > 40:
            cleaned = cleaned[:40].rstrip()
        normalized.append(cleaned)
        if len(normalized) >= ARTICLE_MAX_TAGS:
            break
    return normalized


def extract_youtube_embed_url(raw_url: str) -> str | None:
    value = (raw_url or "").strip()
    if not value.lower().startswith(("http://", "https://")):
        return None

    try:
        parsed = urlparse(value)
    except ValueError:
        return None

    host = (parsed.netloc or "").lower()
    video_id = ""
    if "youtu.be" in host:
        video_id = parsed.path.lstrip("/")
    elif "youtube.com" in host:
        path = parsed.path or ""
        if path == "/watch":
            video_id = (parse_qs(parsed.query).get("v") or [""])[0]
        elif path.startswith("/shorts/"):
            video_id = path.split("/shorts/", 1)[1].split("/", 1)[0]
        elif path.startswith("/embed/"):
            video_id = path.split("/embed/", 1)[1].split("/", 1)[0]

    video_id = (video_id or "").strip()
    if not re.fullmatch(r"[A-Za-z0-9_-]{6,20}", video_id):
        return None
    return f"https://www.youtube.com/embed/{video_id}"


def conflict_subject_from_message(message: str | None) -> str:
    text_value = (message or "").strip()
    if "конфликт" not in text_value.casefold():
        return ""
    match = re.search(r"«([^»]+)»", text_value)
    return (match.group(1).strip() if match else "")


def parse_pigeon_message(message: str | None) -> tuple[str, str] | None:
    text_value = (message or "").strip()
    if not text_value:
        return None
    match = re.match(r"^Курлык!\s*\(@([^)]+)\)\s*(.*)$", text_value, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return None
    sender_alias = normalize_username(match.group(1))
    body = (match.group(2) or "").strip()
    if not sender_alias:
        return None
    return sender_alias, body


def is_pigeon_message(message: str | None) -> bool:
    return parse_pigeon_message(message) is not None


def _render_article_inline(text: str) -> str:
    rendered = html.escape(text)

    def color_repl(match: re.Match[str]) -> str:
        color = match.group(1).strip()
        if not re.fullmatch(r"(#[0-9a-fA-F]{3,8}|[a-zA-Z]{3,20})", color):
            return match.group(0)
        content = match.group(2)
        return f'<span style="color:{color}">{content}</span>'

    rendered = re.sub(r"\[color=([^\]]+)\](.+?)\[/color\]", color_repl, rendered, flags=re.IGNORECASE)
    rendered = re.sub(r"\[([^\]]+)\]\((https?://[^\s)]+)\)", r'<a href="\2" target="_blank" rel="noreferrer">\1</a>', rendered)
    rendered = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", rendered)
    rendered = re.sub(r"\*(.+?)\*", r"<em>\1</em>", rendered)
    rendered = re.sub(r"`([^`]+)`", r"<code>\1</code>", rendered)
    return rendered


def render_article_markdown(raw_text: str) -> str:
    text_value = (raw_text or "").replace("\r\n", "\n").strip()
    if not text_value:
        return ""

    lines = text_value.split("\n")
    parts: list[str] = []
    in_list = False

    def close_list() -> None:
        nonlocal in_list
        if in_list:
            parts.append("</ul>")
            in_list = False

    for line in lines:
        stripped = line.strip()
        if not stripped:
            close_list()
            continue

        youtube_embed = extract_youtube_embed_url(stripped)
        if youtube_embed:
            close_list()
            parts.append(
                f'<div class="article-video-wrap"><iframe src="{youtube_embed}" '
                'title="YouTube video" loading="lazy" allowfullscreen></iframe></div>'
            )
            continue

        heading_match = re.match(r"^(#{1,3})\s+(.+)$", stripped)
        if heading_match:
            close_list()
            level = len(heading_match.group(1))
            parts.append(f"<h{level}>{_render_article_inline(heading_match.group(2))}</h{level}>")
            continue

        if stripped.startswith("- "):
            if not in_list:
                parts.append("<ul>")
                in_list = True
            parts.append(f"<li>{_render_article_inline(stripped[2:])}</li>")
            continue

        close_list()
        parts.append(f"<p>{_render_article_inline(stripped)}</p>")

    close_list()
    return "\n".join(parts)


def notify_coproplayer_conflicts_for_card(db: Session, *, card: CosplanCard, owner: User) -> int:
    coproplayers = as_list(card.coproplayer_nicks_json)
    if not coproplayers:
        return 0

    target_users = db.execute(select(User).where(User.username.in_(coproplayers))).scalars().all()
    users_by_username = {normalize_username(item.username).casefold(): item for item in target_users}

    contexts: list[tuple[date, str, str | None]] = []
    if card.photoset_date:
        contexts.append((card.photoset_date, f"фотосет «{card.character_name}»", None))

    planned_names = {name.casefold(): name for name in as_list(card.planned_festivals_json)}
    if planned_names:
        owner_festivals = db.execute(
            select(Festival).where(
                Festival.user_id == owner.id,
                Festival.event_date.is_not(None),
            )
        ).scalars().all()
        for festival in owner_festivals:
            name_key = (festival.name or "").strip().casefold()
            if name_key not in planned_names:
                continue
            for event_day in iter_date_range(festival.event_date, festival.event_end_date):
                contexts.append((event_day, f"выступление на фестивале «{festival.name}»", festival.name or ""))

    notified = 0
    owner_alias = preferred_user_alias(owner)
    for username in coproplayers:
        target_user = users_by_username.get(username.casefold())
        if not target_user or target_user.id == owner.id:
            continue
        target_alias = preferred_user_alias(target_user)

        for target_date, context_name, ignore_festival_name in contexts:
            conflicts = user_busy_items_on_date(
                db,
                user_id=target_user.id,
                target_date=target_date,
                exclude_source_card_id=card.id,
                ignore_festival_name=ignore_festival_name,
            )
            if not conflicts:
                continue
            conflict_text = "; ".join(conflicts)
            readable_date = target_date.strftime("%d-%m-%Y")
            message_for_owner = (
                f"У сокосплеера @{target_alias} конфликт на {readable_date} для «{card.character_name}»: "
                f"{context_name}. Занято: {conflict_text}."
            )
            message_for_target = (
                f"@{owner_alias} указал(а) вас в карточке «{card.character_name}» ({context_name}, {readable_date}), "
                f"но у вас конфликт: {conflict_text}."
            )
            if enqueue_notification_if_missing(
                db,
                user_id=owner.id,
                from_user_id=target_user.id,
                source_card_id=card.id,
                message=message_for_owner,
            ):
                notified += 1
            if enqueue_notification_if_missing(
                db,
                user_id=target_user.id,
                from_user_id=owner.id,
                source_card_id=card.id,
                message=message_for_target,
            ):
                notified += 1
    return notified


def notify_coproplayer_conflicts_for_festival(
    db: Session,
    *,
    festival: Festival,
    owner: User,
) -> int:
    if not festival.is_going:
        return 0
    festival_dates = iter_date_range(festival.event_date, festival.event_end_date)
    if not festival_dates:
        return 0

    coproplayers = as_list(festival.going_coproplayers_json)
    if not coproplayers:
        return 0

    target_users = db.execute(select(User).where(User.username.in_(coproplayers))).scalars().all()
    users_by_username = {normalize_username(item.username).casefold(): item for item in target_users}

    owner_alias = preferred_user_alias(owner)
    notified = 0
    for username in coproplayers:
        target_user = users_by_username.get(username.casefold())
        if not target_user or target_user.id == owner.id:
            continue
        target_alias = preferred_user_alias(target_user)
        for target_date in festival_dates:
            conflicts = user_busy_items_on_date(
                db,
                user_id=target_user.id,
                target_date=target_date,
                ignore_festival_name=festival.name or "",
                exclude_festival_id=festival.id,
            )
            if not conflicts:
                continue
            conflict_text = "; ".join(conflicts)
            readable_date = target_date.strftime("%d-%m-%Y")
            message_for_owner = (
                f"У сокосплеера @{target_alias} конфликт на {readable_date} для фестиваля «{festival.name}»: "
                f"{conflict_text}."
            )
            message_for_target = (
                f"@{owner_alias} отметил(а) вас сокосплеером на фестивале «{festival.name}» ({readable_date}), "
                f"но у вас конфликт: {conflict_text}."
            )
            if enqueue_notification_if_missing(
                db,
                user_id=owner.id,
                from_user_id=target_user.id,
                source_card_id=None,
                message=message_for_owner,
            ):
                notified += 1
            if enqueue_notification_if_missing(
                db,
                user_id=target_user.id,
                from_user_id=owner.id,
                source_card_id=None,
                message=message_for_target,
            ):
                notified += 1
    return notified


def build_card_date_conflicts(
    visible_cards: list[CosplanCard],
    all_cards: list[CosplanCard],
    festivals: list[Festival],
) -> dict[int, list[str]]:
    festival_dates_by_name: dict[str, list[tuple[str, date]]] = defaultdict(list)
    going_by_date: dict[date, list[str]] = defaultdict(list)

    for festival in festivals:
        if not festival.name or not festival.event_date:
            continue
        key = festival.name.strip().casefold()
        festival_dates_by_name[key].append((festival.name.strip(), festival.event_date))
        if festival.is_going:
            going_by_date[festival.event_date].append(festival.name.strip())

    indicated_by_date: dict[date, list[str]] = defaultdict(list)
    for card in all_cards:
        if not card.is_shared_copy:
            continue
        for planned_name in as_list(card.planned_festivals_json):
            key = planned_name.casefold()
            for resolved_name, resolved_date in festival_dates_by_name.get(key, []):
                indicated_by_date[resolved_date].append(resolved_name)

    for key in list(going_by_date.keys()):
        going_by_date[key] = merge_unique(going_by_date[key])
    for key in list(indicated_by_date.keys()):
        indicated_by_date[key] = merge_unique(indicated_by_date[key])

    result: dict[int, list[str]] = {}
    for card in visible_cards:
        warnings: list[str] = []
        own_festivals: list[tuple[str, date]] = []
        seen_own: set[tuple[str, date]] = set()

        for planned_name in as_list(card.planned_festivals_json):
            key = planned_name.casefold()
            for resolved_name, resolved_date in festival_dates_by_name.get(key, []):
                marker = (resolved_name.casefold(), resolved_date)
                if marker in seen_own:
                    continue
                seen_own.add(marker)
                own_festivals.append((resolved_name, resolved_date))

        own_by_date: dict[date, list[str]] = defaultdict(list)
        for festival_name, festival_date in own_festivals:
            own_by_date[festival_date].append(festival_name)

        for festival_date, same_day_names in own_by_date.items():
            if len(same_day_names) > 1:
                warnings.append(
                    f"Совпадают по дате фестивали в карточке: {_short_names(same_day_names)} "
                    f"({festival_date.isoformat()})."
                )

        for festival_name, festival_date in own_festivals:
            same_day_going = [
                name for name in going_by_date.get(festival_date, []) if name.casefold() != festival_name.casefold()
            ]
            if same_day_going:
                warnings.append(
                    f"Фестиваль «{festival_name}» ({festival_date.isoformat()}) совпадает с фестивалем, "
                    f"куда вы идёте: {_short_names(same_day_going)}."
                )

            same_day_indicated = [
                name for name in indicated_by_date.get(festival_date, []) if name.casefold() != festival_name.casefold()
            ]
            if same_day_indicated:
                warnings.append(
                    f"Фестиваль «{festival_name}» ({festival_date.isoformat()}) совпадает с фестивалем, "
                    f"где вас указали: {_short_names(same_day_indicated)}."
                )

        if card.photoset_date:
            same_day_own = own_by_date.get(card.photoset_date, [])
            if same_day_own:
                warnings.append(
                    f"Фотосет ({card.photoset_date.isoformat()}) совпадает с фестивалем карточки: "
                    f"{_short_names(same_day_own)}."
                )

            same_day_going = going_by_date.get(card.photoset_date, [])
            if same_day_going:
                warnings.append(
                    f"Фотосет ({card.photoset_date.isoformat()}) совпадает с фестивалем, куда вы идёте: "
                    f"{_short_names(same_day_going)}."
                )

            same_day_indicated = indicated_by_date.get(card.photoset_date, [])
            if same_day_indicated:
                warnings.append(
                    f"Фотосет ({card.photoset_date.isoformat()}) совпадает с фестивалем, где вас указали: "
                    f"{_short_names(same_day_indicated)}."
                )

        # Keep insertion order but remove duplicates.
        deduped = list(dict.fromkeys(warnings))
        if deduped:
            result[card.id] = deduped

    return result


def card_fields_for_sync() -> list[str]:
    return [
        "character_name",
        "fandom",
        "is_au",
        "au_text",
        "costume_type",
        "sewing_type",
        "sewing_fabric",
        "sewing_hardware",
        "sewing_pattern",
        "costume_executor",
        "costume_deadline",
        "costume_prepayment",
        "costume_postpayment",
        "costume_fabric_price",
        "costume_hardware_price",
        "costume_bought",
        "costume_link",
        "costume_buy_price",
        "costume_currency",
        "costume_notes",
        "shoes_type",
        "shoes_bought",
        "shoes_link",
        "shoes_buy_price",
        "shoes_executor",
        "shoes_deadline",
        "shoes_price",
        "shoes_currency",
        "lenses_enabled",
        "lenses_comment",
        "lenses_color",
        "lenses_price",
        "lenses_currency",
        "wig_type",
        "wigmaker_name",
        "wig_price",
        "wig_buy_price",
        "wig_currency",
        "wig_deadline",
        "wig_link",
        "wig_no_buy_from",
        "wig_restyle",
        "craft_type",
        "craft_master",
        "craft_price",
        "craft_material_price",
        "craft_deadline",
        "craft_currency",
        "plan_type",
        "project_leader",
        "cosbands_json",
        "project_deadline",
        "related_cards_json",
        "planned_festivals_json",
        "submission_date",
        "nominations_json",
        "city",
        "photographers_json",
        "studios_json",
        "photoset_date",
        "photoset_price",
        "photoset_photographer_price",
        "photoset_studio_price",
        "photoset_props_price",
        "photoset_extra_price",
        "photoset_currency",
        "photoset_comment",
        "photoset_props_checklist_json",
        "performance_track",
        "performance_video_bg_url",
        "performance_script",
        "performance_light_script",
        "performance_duration",
        "performance_rehearsal_point",
        "performance_rehearsal_price",
        "performance_rehearsal_currency",
        "performance_rehearsal_count",
        "references_json",
        "pose_references_json",
        "unknown_prices_json",
        "costume_parts_json",
        "craft_parts_json",
        "coproplayers_json",
        "coproplayer_nicks_json",
        "notes",
    ]


def clone_card_data(source: CosplanCard, target: CosplanCard) -> None:
    for field in card_fields_for_sync():
        setattr(target, field, getattr(source, field))


def delete_card_with_runtime_dependents(db: Session, card: CosplanCard) -> None:
    # Explicitly remove "В работе" entries before deleting a card.
    # Without this, SQLAlchemy may try to nullify FK and hit NOT NULL on legacy rows.
    in_progress_rows = db.execute(
        select(InProgressCard).where(InProgressCard.cosplan_card_id == card.id)
    ).scalars().all()
    for row in in_progress_rows:
        db.delete(row)
    db.delete(card)


def sync_shared_cards_for_nicks(source_card: CosplanCard, actor: User, db: Session) -> None:
    if source_card.is_shared_copy:
        return

    source_owner = db.get(User, source_card.user_id)
    source_owner_username = normalize_username(source_owner.username).casefold() if source_owner else ""

    alias_to_username, _, _ = build_user_alias_lookup(db)
    raw_nicks = as_list(source_card.coproplayer_nicks_json)
    resolved_nicks = resolve_aliases_to_usernames(raw_nicks, alias_to_username)
    target_nicks = [nick for nick in resolved_nicks if nick and nick.casefold() != source_owner_username]

    if not target_nicks:
        existing_copies = db.execute(
            select(CosplanCard).where(
                CosplanCard.source_card_id == source_card.id,
                CosplanCard.is_shared_copy.is_(True),
            )
        ).scalars().all()
        for card in existing_copies:
            delete_card_with_runtime_dependents(db, card)
        remove_shared_card_notifications(db, source_card_id=source_card.id)
        return

    matched_users = db.execute(select(User).where(User.username.in_(target_nicks))).scalars().all()
    users_by_nick = {normalize_username(user.username).casefold(): user for user in matched_users}
    target_ids = {user.id for user in users_by_nick.values()}

    existing_copies = db.execute(
        select(CosplanCard).where(
            CosplanCard.source_card_id == source_card.id,
            CosplanCard.is_shared_copy.is_(True),
        )
    ).scalars().all()
    copies_by_user_id = {card.user_id: card for card in existing_copies}

    # Remove obsolete shared copies.
    for user_id, stale_copy in list(copies_by_user_id.items()):
        if user_id in target_ids:
            continue
        delete_card_with_runtime_dependents(db, stale_copy)
        remove_shared_card_notifications(db, source_card_id=source_card.id, user_id=user_id)

    # Upsert shared copies and notify recipients.
    for nick in target_nicks:
        target_user = users_by_nick.get(nick.casefold())
        if not target_user:
            continue

        shared_copy = copies_by_user_id.get(target_user.id)
        if not shared_copy:
            shared_copy = CosplanCard(
                user_id=target_user.id,
                is_shared_copy=True,
                source_card_id=source_card.id,
                shared_from_user_id=actor.id,
                character_name=source_card.character_name,
            )
            db.add(shared_copy)
        clone_card_data(source_card, shared_copy)
        shared_copy.is_shared_copy = True
        shared_copy.source_card_id = source_card.id
        shared_copy.shared_from_user_id = actor.id

        if target_user.id == actor.id:
            continue

        existing_notifications = db.execute(
            select(FestivalNotification).where(
                FestivalNotification.user_id == target_user.id,
                FestivalNotification.source_card_id == source_card.id,
            )
        ).scalars().all()
        shared_notifications = [
            item for item in existing_notifications if is_shared_card_notification_message(item.message)
        ]
        existing_notification = shared_notifications[0] if shared_notifications else None
        for duplicate_note in shared_notifications[1:]:
            db.delete(duplicate_note)

        if existing_notification is None:
            db.add(
                FestivalNotification(
                    user_id=target_user.id,
                    from_user_id=actor.id,
                    source_card_id=source_card.id,
                    message=(
                        "Карточка добавлена по вашему нику другим пользователем. "
                        f"Проект: «{source_card.character_name}» (инициатор: @{preferred_user_alias(actor)})."
                    ),
                    is_read=False,
                )
            )
        else:
            existing_notification.from_user_id = actor.id
            existing_notification.message = (
                "Карточка по вашему нику обновлена другим пользователем. "
                f"Проект: «{source_card.character_name}» (инициатор: @{preferred_user_alias(actor)})."
            )
            existing_notification.is_read = False


def add_flash(request: Request, text: str, kind: str = "info") -> None:
    request.session["flash"] = {"text": text, "kind": kind}


def pop_flash(request: Request) -> dict[str, str] | None:
    flash = request.session.get("flash")
    if flash:
        request.session.pop("flash", None)
    return flash


def current_user(request: Request, db: Session) -> User | None:
    user_id = request.session.get("user_id")
    if not user_id:
        return None
    return db.get(User, int(user_id))


def template_response(
    request: Request,
    name: str,
    user: User | None = None,
    active_tab: str | None = None,
    **context: Any,
) -> HTMLResponse:
    payload = {
        "request": request,
        "user": user,
        "active_tab": active_tab,
        "flash": pop_flash(request),
        "today": date.today(),
        "project_name": PROJECT_NAME,
        "nick_is_special": nick_is_special,
        "user_is_special": user_is_special,
        "notification_conflict_subject": conflict_subject_from_message,
    }
    payload.update(context)
    return templates.TemplateResponse(name, payload)


def redirect(url: str) -> RedirectResponse:
    return RedirectResponse(url=url, status_code=303)


def get_card_form_values(card: CosplanCard | None = None, *, actor_user_id: int | None = None) -> dict[str, Any]:
    if not card:
        return {
            "character_name": "",
            "fandom": "",
            "is_au": False,
            "au_text": "",
            "costume_type": "sew",
            "sewing_type": "self",
            "sewing_fabric": False,
            "sewing_hardware": False,
            "sewing_pattern": False,
            "costume_executor": "",
            "costume_deadline": "",
            "costume_prepayment": "",
            "costume_postpayment": "",
            "costume_fabric_price": "",
            "costume_hardware_price": "",
            "costume_bought": False,
            "costume_link": "",
            "costume_buy_price": "",
            "costume_currency": "RUB",
            "costume_notes": "",
            "shoes_type": "buy",
            "shoes_bought": False,
            "shoes_link": "",
            "shoes_buy_price": "",
            "shoes_executor": "",
            "shoes_deadline": "",
            "shoes_price": "",
            "shoes_currency": "RUB",
            "lenses_enabled": False,
            "lenses_comment": "",
            "lenses_color": "",
            "lenses_price": "",
            "lenses_currency": "RUB",
            "wig_type": "wigmaker",
            "wigmaker_name": "",
            "wig_price": "",
            "wig_buy_price": "",
            "wig_currency": "RUB",
            "wig_deadline": "",
            "wig_link": "",
            "wig_no_buy_from": "",
            "wig_restyle": False,
            "craft_type": "self",
            "craft_master": "",
            "craft_price": "",
            "craft_material_price": "",
            "craft_deadline": "",
            "craft_currency": "RUB",
            "plan_type": "personal",
            "project_leader": "",
            "cosbands_json": [],
            "project_deadline": "",
            "related_card_ids": [],
            "planned_festivals_json": [],
            "submission_date": "",
            "nominations_json": [],
            "city": "",
            "photographers_json": [],
            "studios_json": [],
            "photoset_date": "",
            "photoset_price": "",
            "photoset_photographer_price": "",
            "photoset_studio_price": "",
            "photoset_props_price": "",
            "photoset_extra_price": "",
            "photoset_currency": "RUB",
            "photoset_comment": "",
            "photoset_props_checklist_json": [],
            "photoset_props_checklist_input": "",
            "performance_track": "",
            "performance_video_bg_url": "",
            "performance_script": "",
            "performance_light_script": "",
            "performance_duration": "",
            "performance_rehearsal_point": "",
            "performance_rehearsal_price": "",
            "performance_rehearsal_currency": "RUB",
            "performance_rehearsal_count": "",
            "references_json": [],
            "references_input": "",
            "pose_references_json": [],
            "pose_references_input": "",
            "unknown_prices_json": [],
            "costume_parts_json": [],
            "craft_parts_json": [],
            "coproplayers_json": [],
            "coproplayer_nicks_json": [],
            "coproplayers_input": "",
            "coproplayer_alias_rows": [""],
            "estimated_total": 0.0,
            "estimated_total_currency": "",
            "notes": "",
        }

    estimated_total, estimated_currency = estimate_card_total_and_currency(card)
    coproplayer_alias_rows = as_list(card.coproplayers_json) or as_list(card.coproplayer_nicks_json)
    if not coproplayer_alias_rows:
        coproplayer_alias_rows = [""]
    related_links_user_id = actor_user_id if actor_user_id and actor_user_id > 0 else card.user_id
    related_ids_for_editor = related_card_ids_for_user(
        as_list(card.related_cards_json),
        target_user_id=related_links_user_id,
        legacy_user_id=card.user_id,
    )
    return {
        "character_name": card.character_name or "",
        "fandom": card.fandom or "",
        "is_au": bool(card.is_au),
        "au_text": card.au_text or "",
        "costume_type": card.costume_type or "sew",
        "sewing_type": card.sewing_type or "self",
        "sewing_fabric": bool(card.sewing_fabric),
        "sewing_hardware": bool(card.sewing_hardware),
        "sewing_pattern": bool(card.sewing_pattern),
        "costume_executor": card.costume_executor or "",
        "costume_deadline": card.costume_deadline.isoformat() if card.costume_deadline else "",
        "costume_prepayment": "" if card.costume_prepayment is None else f"{card.costume_prepayment:g}",
        "costume_postpayment": "" if card.costume_postpayment is None else f"{card.costume_postpayment:g}",
        "costume_fabric_price": "" if card.costume_fabric_price is None else f"{card.costume_fabric_price:g}",
        "costume_hardware_price": "" if card.costume_hardware_price is None else f"{card.costume_hardware_price:g}",
        "costume_bought": bool(card.costume_bought),
        "costume_link": card.costume_link or "",
        "costume_buy_price": "" if card.costume_buy_price is None else f"{card.costume_buy_price:g}",
        "costume_currency": card.costume_currency or "RUB",
        "costume_notes": card.costume_notes or "",
        "shoes_type": card.shoes_type or "buy",
        "shoes_bought": bool(card.shoes_bought),
        "shoes_link": card.shoes_link or "",
        "shoes_buy_price": "" if card.shoes_buy_price is None else f"{card.shoes_buy_price:g}",
        "shoes_executor": card.shoes_executor or "",
        "shoes_deadline": card.shoes_deadline.isoformat() if card.shoes_deadline else "",
        "shoes_price": "" if card.shoes_price is None else f"{card.shoes_price:g}",
        "shoes_currency": card.shoes_currency or "RUB",
        "lenses_enabled": bool(card.lenses_enabled),
        "lenses_comment": card.lenses_comment or "",
        "lenses_color": card.lenses_color or "",
        "lenses_price": "" if card.lenses_price is None else f"{card.lenses_price:g}",
        "lenses_currency": card.lenses_currency or "RUB",
        "wig_type": card.wig_type or "wigmaker",
        "wigmaker_name": card.wigmaker_name or "",
        "wig_price": "" if card.wig_price is None else f"{card.wig_price:g}",
        "wig_buy_price": "" if card.wig_buy_price is None else f"{card.wig_buy_price:g}",
        "wig_currency": card.wig_currency or "RUB",
        "wig_deadline": card.wig_deadline.isoformat() if card.wig_deadline else "",
        "wig_link": card.wig_link or "",
        "wig_no_buy_from": card.wig_no_buy_from or "",
        "wig_restyle": bool(card.wig_restyle),
        "craft_type": card.craft_type or "self",
        "craft_master": card.craft_master or "",
        "craft_price": "" if card.craft_price is None else f"{card.craft_price:g}",
        "craft_material_price": "" if card.craft_material_price is None else f"{card.craft_material_price:g}",
        "craft_deadline": card.craft_deadline.isoformat() if card.craft_deadline else "",
        "craft_currency": card.craft_currency or "RUB",
        "plan_type": card.plan_type or "personal",
        "project_leader": card.project_leader or "",
        "cosbands_json": as_list(card.cosbands_json),
        "project_deadline": card.project_deadline.isoformat() if card.project_deadline else "",
        "related_card_ids": related_ids_for_editor,
        "planned_festivals_json": as_list(card.planned_festivals_json),
        "submission_date": card.submission_date.isoformat() if card.submission_date else "",
        "nominations_json": as_list(card.nominations_json),
        "city": card.city or "",
        "photographers_json": as_list(card.photographers_json),
        "studios_json": as_list(card.studios_json),
        "photoset_date": card.photoset_date.isoformat() if card.photoset_date else "",
        "photoset_price": "" if card.photoset_price is None else f"{card.photoset_price:g}",
        "photoset_photographer_price": (
            "" if card.photoset_photographer_price is None else f"{card.photoset_photographer_price:g}"
        ),
        "photoset_studio_price": "" if card.photoset_studio_price is None else f"{card.photoset_studio_price:g}",
        "photoset_props_price": "" if card.photoset_props_price is None else f"{card.photoset_props_price:g}",
        "photoset_extra_price": (
            ""
            if card.photoset_extra_price is None and card.photoset_price is None
            else f"{(card.photoset_extra_price if card.photoset_extra_price is not None else card.photoset_price):g}"
        ),
        "photoset_currency": card.photoset_currency or "RUB",
        "photoset_comment": card.photoset_comment or "",
        "photoset_props_checklist_json": format_checklist_for_form(as_list(card.photoset_props_checklist_json)),
        "performance_track": card.performance_track or "",
        "performance_video_bg_url": card.performance_video_bg_url or "",
        "performance_script": card.performance_script or "",
        "performance_light_script": card.performance_light_script or "",
        "performance_duration": card.performance_duration or "",
        "performance_rehearsal_point": card.performance_rehearsal_point or "",
        "performance_rehearsal_price": (
            "" if card.performance_rehearsal_price is None else f"{card.performance_rehearsal_price:g}"
        ),
        "performance_rehearsal_currency": card.performance_rehearsal_currency or "RUB",
        "performance_rehearsal_count": (
            "" if card.performance_rehearsal_count is None else str(card.performance_rehearsal_count)
        ),
        "references_json": as_list(card.references_json),
        "references_input": "\n".join(as_list(card.references_json)),
        "pose_references_json": as_list(card.pose_references_json),
        "pose_references_input": "\n".join(as_list(card.pose_references_json)),
        "unknown_prices_json": as_list(card.unknown_prices_json),
        "costume_parts_json": format_parts_for_form(as_list(card.costume_parts_json)),
        "craft_parts_json": format_parts_for_form(as_list(card.craft_parts_json)),
        "coproplayers_json": as_list(card.coproplayers_json),
        "coproplayer_nicks_json": as_list(card.coproplayer_nicks_json),
        "coproplayers_input": ", ".join(as_list(card.coproplayers_json) or as_list(card.coproplayer_nicks_json)),
        "coproplayer_alias_rows": coproplayer_alias_rows,
        "estimated_total": estimated_total,
        "estimated_total_currency": estimated_currency,
        "notes": card.notes or "",
    }


def card_options(
    db: Session,
    user: User,
    current_card_id: int | None = None,
    related_cards_user_id: int | None = None,
) -> dict[str, Any]:
    festival_rows = db.execute(
        select(Festival.name, Festival.city).where(Festival.user_id == user.id).order_by(Festival.city, Festival.name)
    ).all()
    festival_items = [{"name": row[0], "city": row[1] or ""} for row in festival_rows if row[0]]
    festival_names = [row["name"] for row in festival_items]

    _, _, alias_options = build_user_alias_lookup(db)
    project_leader_options = merge_unique(
        alias_options,
        get_options(db, user.id, "project_leader"),
    )
    coproplayer_alias_options = merge_unique(
        alias_options,
        get_options(db, user.id, "coproplayer"),
        get_options(db, user.id, "coproplayer_nick"),
    )

    all_festival_options = merge_unique(festival_names, get_options(db, user.id, "festival"))
    festival_custom_options = [value for value in all_festival_options if value not in set(festival_names)]
    related_user = db.get(User, related_cards_user_id) if related_cards_user_id else user
    if not related_user:
        related_user = user
    author_alias = preferred_user_alias(related_user)
    own_cards = db.execute(
        select(CosplanCard)
        .where(
            CosplanCard.user_id == related_user.id,
            CosplanCard.is_shared_copy.is_(False),
        )
        .order_by(CosplanCard.updated_at.desc(), CosplanCard.id.desc())
    ).scalars().all()
    related_card_options = [
        {
            "id": int(card.id),
            "label": f"{card.character_name}, @{author_alias}",
        }
        for card in own_cards
        if card.character_name and (not current_card_id or card.id != current_card_id)
    ]

    return {
        "fandom_options": get_options(db, user.id, "fandom"),
        "cosband_options": get_options(db, user.id, "cosband"),
        "festival_options": all_festival_options,
        "festival_items": festival_items,
        "festival_custom_options": festival_custom_options,
        "nomination_options": merge_unique(DEFAULT_NOMINATIONS, get_options(db, user.id, "nomination")),
        "photographer_options": get_options(db, user.id, "photographer"),
        "studio_options": get_options(db, user.id, "studio"),
        "coproplayer_alias_options": coproplayer_alias_options,
        "project_leader_options": project_leader_options,
        "related_card_options": related_card_options,
        "currency_options": merge_unique(
            ["RUB", "USD", "EUR"],
            get_options(db, user.id, "currency"),
        ),
    }


def get_festival_form_values(festival: Festival | None = None) -> dict[str, Any]:
    if not festival:
        return {
            "name": "",
            "url": "",
            "city": "",
            "event_date": "",
            "event_end_date": "",
            "submission_deadline": "",
            "nomination_1": "",
            "nomination_2": "",
            "nomination_3": "",
            "is_going": False,
            "going_coproplayers_json": [],
            "going_coproplayers_input": "",
        }

    return {
        "name": festival.name or "",
        "url": festival.url or "",
        "city": festival.city or "",
        "event_date": festival.event_date.isoformat() if festival.event_date else "",
        "event_end_date": festival.event_end_date.isoformat() if festival.event_end_date else "",
        "submission_deadline": festival.submission_deadline.isoformat() if festival.submission_deadline else "",
        "nomination_1": festival.nomination_1 or "",
        "nomination_2": festival.nomination_2 or "",
        "nomination_3": festival.nomination_3 or "",
        "is_going": bool(festival.is_going),
        "going_coproplayers_json": as_list(festival.going_coproplayers_json),
        "going_coproplayers_input": ", ".join(as_list(festival.going_coproplayers_json)),
    }


def get_festival_announcement_form_values(announcement: FestivalAnnouncement | None = None) -> dict[str, Any]:
    if not announcement:
        return {
            "name": "",
            "url": "",
            "city": "",
            "event_date": "",
            "event_end_date": "",
            "submission_deadline": "",
            "nomination_1": "",
            "nomination_2": "",
            "nomination_3": "",
        }
    return {
        "name": announcement.name or "",
        "url": announcement.url or "",
        "city": announcement.city or "",
        "event_date": announcement.event_date.isoformat() if announcement.event_date else "",
        "event_end_date": announcement.event_end_date.isoformat() if announcement.event_end_date else "",
        "submission_deadline": announcement.submission_deadline.isoformat() if announcement.submission_deadline else "",
        "nomination_1": announcement.nomination_1 or "",
        "nomination_2": announcement.nomination_2 or "",
        "nomination_3": announcement.nomination_3 or "",
    }


def save_festival_announcement_from_form(form: Any, announcement: FestivalAnnouncement) -> tuple[bool, str]:
    name = str(form.get("name", "")).strip()
    if not name:
        return False, "Название фестиваля обязательно."

    event_date = parse_date(str(form.get("event_date", "")))
    event_end_date = parse_date(str(form.get("event_end_date", "")))
    if not event_date and event_end_date:
        event_date = event_end_date
    if event_date and event_end_date and event_end_date < event_date:
        event_end_date = event_date

    announcement.name = name
    announcement.url = str(form.get("url", "")).strip() or None
    announcement.city = str(form.get("city", "")).strip() or None
    announcement.event_date = event_date
    announcement.event_end_date = event_end_date
    announcement.submission_deadline = parse_date(str(form.get("submission_deadline", "")))
    announcement.nomination_1 = str(form.get("nomination_1", "")).strip() or None
    announcement.nomination_2 = str(form.get("nomination_2", "")).strip() or None
    announcement.nomination_3 = str(form.get("nomination_3", "")).strip() or None
    return True, ""


def propagate_approved_announcement(
    db: Session,
    announcement: FestivalAnnouncement,
    target_user_ids: list[int] | None = None,
) -> int:
    if announcement.status != ANNOUNCEMENT_STATUS_APPROVED:
        return 0

    user_ids = target_user_ids or [int(item) for item in db.execute(select(User.id)).scalars().all()]
    created = 0
    for user_id in user_ids:
        exists = db.execute(
            select(Festival.id).where(
                Festival.user_id == int(user_id),
                Festival.source_announcement_id == announcement.id,
            )
        ).scalar_one_or_none()
        if exists:
            continue

        db.add(
            Festival(
                user_id=int(user_id),
                name=announcement.name,
                url=announcement.url,
                city=announcement.city,
                event_date=announcement.event_date,
                event_end_date=announcement.event_end_date,
                submission_deadline=announcement.submission_deadline,
                nomination_1=announcement.nomination_1,
                nomination_2=announcement.nomination_2,
                nomination_3=announcement.nomination_3,
                is_going=False,
                going_coproplayers_json=[],
                is_global_announcement=True,
                source_announcement_id=announcement.id,
            )
        )
        created += 1
    return created


def get_project_search_post_form_values(post: ProjectSearchPost | None = None, user: User | None = None) -> dict[str, Any]:
    default_nick = preferred_user_alias(user) if user else ""
    if not post:
        return {
            "fandom": "",
            "event_date": "",
            "event_type": "photoset",
            "status": PROJECT_BOARD_STATUS_ACTIVE,
            "comment": "",
            "contact_nick": default_nick,
            "contact_link": "",
        }

    return {
        "fandom": post.fandom or "",
        "event_date": post.event_date.isoformat() if post.event_date else "",
        "event_type": post.event_type or "photoset",
        "status": post.status or PROJECT_BOARD_STATUS_ACTIVE,
        "comment": post.comment or "",
        "contact_nick": post.contact_nick or default_nick,
        "contact_link": post.contact_link or "",
    }


@app.get("/", response_class=HTMLResponse)
def index(request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if user:
        today = date.today()
        notifications = db.execute(
            select(FestivalNotification)
            .where(FestivalNotification.user_id == user.id)
            .order_by(FestivalNotification.created_at.desc())
            .limit(50)
        ).scalars().all()
        pigeon_notifications: list[dict[str, Any]] = []
        regular_notifications: list[FestivalNotification] = []
        for note in notifications:
            pigeon_payload = parse_pigeon_message(note.message)
            if pigeon_payload and not note.is_read:
                sender_alias, body = pigeon_payload
                pigeon_notifications.append(
                    {
                        "id": note.id,
                        "sender_alias": sender_alias,
                        "body": body,
                        "created_at": note.created_at,
                    }
                )
            else:
                regular_notifications.append(note)

        _, _, alias_options = build_user_alias_lookup(db)
        own_aliases = {item.casefold() for item in user_aliases(user)}
        pigeon_alias_options = sorted(
            [alias for alias in alias_options if alias and alias.casefold() not in own_aliases],
            key=lambda value: value.casefold(),
        )
        users_with_birthdays = db.execute(
            select(User).where(User.birth_date.is_not(None))
        ).scalars().all()
        birthdays_this_week = upcoming_user_birthdays_this_week(users_with_birthdays, today)
        info_events_week = weekly_infopovods(today)
        character_birthdays_today_rows = character_birthdays_today(today)
        unread_notifications = sum(1 for note in notifications if not note.is_read)
        return template_response(
            request,
            "news.html",
            user=user,
            active_tab=None,
            notifications=regular_notifications,
            pigeon_notifications=pigeon_notifications,
            pigeon_alias_options=pigeon_alias_options,
            birthdays_this_week=birthdays_this_week,
            info_events_week=info_events_week,
            character_birthdays_today=character_birthdays_today_rows,
            unread_notifications=unread_notifications,
        )
    return template_response(request, "landing.html", user=None, active_tab=None)


@app.get("/privacy-policy", response_class=HTMLResponse)
def privacy_policy_page(request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    return template_response(request, "privacy_policy.html", user=user, active_tab=None)


@app.get("/register", response_class=HTMLResponse)
def register_page(request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if user:
        return redirect("/cosplan")
    return template_response(request, "register.html", user=None)


@app.post("/register")
async def register_submit(request: Request, db: Session = Depends(get_db)):
    form = await request.form()
    username = str(form.get("username", "")).strip()
    email = str(form.get("email", "")).strip().lower()
    password = str(form.get("password", ""))
    password2 = str(form.get("password_confirm", ""))

    if not username or not email or not password:
        add_flash(request, "Заполните все обязательные поля.", "error")
        return redirect("/register")

    if password != password2:
        add_flash(request, "Пароли не совпадают.", "error")
        return redirect("/register")

    exists_stmt = select(User).where(
        or_(
            User.username == username,
            User.email == email,
            User.cosplay_nick == username,
        )
    )
    if db.execute(exists_stmt).scalar_one_or_none():
        add_flash(request, "Пользователь с таким логином или email уже существует.", "error")
        return redirect("/register")

    user = User(username=username, email=email, password_hash=password_context.hash(password))
    db.add(user)
    db.commit()
    db.refresh(user)

    approved_announcements = db.execute(
        select(FestivalAnnouncement).where(FestivalAnnouncement.status == ANNOUNCEMENT_STATUS_APPROVED)
    ).scalars().all()
    for announcement in approved_announcements:
        propagate_approved_announcement(db, announcement, target_user_ids=[user.id])
    db.commit()

    request.session["user_id"] = user.id
    add_flash(request, "welcome", "welcome")
    return redirect("/cosplan")


@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if user:
        return redirect("/cosplan")
    return template_response(request, "login.html", user=None)


@app.post("/login")
async def login_submit(request: Request, db: Session = Depends(get_db)):
    form = await request.form()
    login_value = str(form.get("login", "")).strip()
    password = str(form.get("password", ""))

    user_stmt = select(User).where(or_(User.username == login_value, User.email == login_value.lower()))
    user = db.execute(user_stmt).scalar_one_or_none()

    if not user or not password_context.verify(password, user.password_hash):
        add_flash(request, "Неверный логин или пароль.", "error")
        return redirect("/login")

    request.session["user_id"] = user.id
    add_flash(request, "Вход выполнен.", "success")
    return redirect("/cosplan")


@app.post("/logout")
def logout(request: Request):
    request.session.pop("user_id", None)
    add_flash(request, "Вы вышли из аккаунта.", "info")
    return redirect("/login")


@app.get("/api/users/search")
def users_search_api(request: Request, q: str = "", limit: int = 8, db: Session = Depends(get_db)) -> dict[str, Any]:
    user = current_user(request, db)
    if not user:
        return {"items": []}

    query = q.strip().casefold()
    if not query:
        return {"items": []}

    max_limit = max(1, min(limit, 20))
    users = db.execute(select(User).order_by(User.username).limit(500)).scalars().all()

    def score(item: User) -> tuple[int, str]:
        usernames = [normalize_username(item.username).casefold(), normalize_username(item.cosplay_nick).casefold()]
        best = 0
        for alias in usernames:
            if not alias:
                continue
            if alias.startswith(query):
                best = max(best, 3)
            elif query in alias:
                best = max(best, 2)
            elif len(query) >= 3 and SequenceMatcher(None, query, alias).ratio() >= 0.75:
                best = max(best, 1)
        return best, normalize_username(item.username).casefold()

    candidates = [item for item in users if score(item)[0] > 0]
    candidates.sort(key=lambda item: (-score(item)[0], score(item)[1]))

    values: list[str] = []
    for item in candidates:
        for alias in user_aliases(item):
            alias_clean = normalize_username(alias)
            if not alias_clean:
                continue
            if alias_clean in values:
                continue
            values.append(alias_clean)
            if len(values) >= max_limit:
                break
        if len(values) >= max_limit:
            break

    return {"items": values}


@app.get("/profile", response_class=HTMLResponse)
def profile_page(request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    is_stats_admin = (
        (user.email or "").strip().casefold() == "angenzel@gmail.com"
        or normalize_username(user.username).casefold() == "brfox_cosplay"
        or normalize_username(user.cosplay_nick).casefold() == "brfox_cosplay"
    )
    admin_stats: dict[str, Any] | None = None
    if is_stats_admin:
        total_users = int(db.execute(select(func.count(User.id))).scalar() or 0)
        total_cosplan_cards = int(
            db.execute(select(func.count(CosplanCard.id)).where(CosplanCard.is_shared_copy.is_(False))).scalar() or 0
        )

        raw_cities = db.execute(select(User.home_city)).scalars().all()
        city_counts: dict[str, int] = defaultdict(int)
        city_labels: dict[str, str] = {}
        for raw_city in raw_cities:
            cleaned = (raw_city or "").strip()
            key = cleaned.casefold() if cleaned else "__empty__"
            city_counts[key] += 1
            if key not in city_labels:
                city_labels[key] = cleaned or "Не указан"

        city_stats = [
            {"city": city_labels[key], "count": count}
            for key, count in city_counts.items()
        ]
        city_stats.sort(key=lambda item: (-item["count"], item["city"]))

        admin_stats = {
            "total_users": total_users,
            "total_cosplan_cards": total_cosplan_cards,
            "city_stats": city_stats,
            "unique_city_count": sum(1 for item in city_stats if item["city"] != "Не указан"),
        }

    return template_response(
        request,
        "profile.html",
        user=user,
        active_tab="profile",
        admin_stats=admin_stats,
    )


@app.post("/profile")
async def profile_update(request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    form = await request.form()
    username = str(form.get("username", "")).strip()
    cosplay_nick = normalize_username(str(form.get("cosplay_nick", "")).strip())
    email = str(form.get("email", "")).strip().lower()
    home_city = str(form.get("home_city", "")).strip()
    birth_date = parse_date(str(form.get("birth_date", "")).strip())
    new_password = str(form.get("new_password", "")).strip()
    new_password_confirm = str(form.get("new_password_confirm", "")).strip()

    if username and username != user.username:
        exists = db.execute(
            select(User).where(
                User.id != user.id,
                or_(User.username == username, User.cosplay_nick == username),
            )
        ).scalar_one_or_none()
        if exists:
            add_flash(request, "Такой ник уже используется как username или ник косплеера.", "error")
            return redirect("/profile")
        user.username = username

    if cosplay_nick and cosplay_nick != normalize_username(user.cosplay_nick):
        exists = db.execute(
            select(User).where(
                User.id != user.id,
                or_(User.cosplay_nick == cosplay_nick, User.username == cosplay_nick),
            )
        ).scalar_one_or_none()
        if exists:
            add_flash(request, "Такой ник косплеера уже используется как username или ник косплеера.", "error")
            return redirect("/profile")
        user.cosplay_nick = cosplay_nick
    elif not cosplay_nick:
        user.cosplay_nick = None

    if email and email != user.email:
        exists = db.execute(select(User).where(User.email == email, User.id != user.id)).scalar_one_or_none()
        if exists:
            add_flash(request, "Такой email уже используется.", "error")
            return redirect("/profile")
        user.email = email

    user.home_city = home_city or None
    user.birth_date = birth_date

    if new_password:
        if new_password != new_password_confirm:
            add_flash(request, "Новые пароли не совпадают.", "error")
            return redirect("/profile")
        user.password_hash = password_context.hash(new_password)

    db.commit()
    add_flash(request, "Профиль обновлён.", "success")
    return redirect("/profile")


@app.get("/cosplan", response_class=HTMLResponse)
def cosplan_list(request: Request, q: str = "", db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    all_cards = db.execute(
        select(CosplanCard).where(CosplanCard.user_id == user.id).order_by(CosplanCard.updated_at.desc())
    ).scalars().all()

    cards = list(all_cards)
    if q.strip():
        alias_to_username, users_by_username, _ = build_user_alias_lookup(db)
        needle = q.strip().casefold()

        def matches(card: CosplanCard) -> bool:
            searchable: list[str] = [
                card.character_name or "",
                card.fandom or "",
                card.city or "",
                card.project_leader or "",
                card.notes or "",
                card.costume_notes or "",
                card.photoset_comment or "",
            ]
            searchable.extend(as_list(card.references_json))
            searchable.extend(as_list(card.pose_references_json))
            for item in as_list(card.costume_parts_json):
                if isinstance(item, dict):
                    searchable.extend([str(item.get("url", "")), str(item.get("comment", ""))])
            for item in as_list(card.craft_parts_json):
                if isinstance(item, dict):
                    searchable.extend([str(item.get("url", "")), str(item.get("comment", ""))])
            for item in as_list(card.photoset_props_checklist_json):
                if isinstance(item, dict):
                    searchable.append(str(item.get("text", "")))
            coproplayers = as_list(card.coproplayers_json) or as_list(card.coproplayer_nicks_json)
            searchable.extend(coproplayers)
            searchable.extend(
                [value.lstrip("@") for value in format_coproplayer_names(coproplayers, alias_to_username, users_by_username)]
            )
            return any(needle in value.casefold() for value in searchable if value)

        cards = [card for card in all_cards if matches(card)]

    festivals = db.execute(
        select(Festival).where(Festival.user_id == user.id, Festival.event_date.is_not(None))
    ).scalars().all()
    card_totals: dict[int, float] = {}
    card_total_currencies: dict[int, str] = {}
    for card in cards:
        total, currency = estimate_card_total_and_currency(card)
        card_totals[card.id] = total
        card_total_currencies[card.id] = currency
    card_date_conflicts = build_card_date_conflicts(cards, all_cards, festivals)
    in_progress_ids = set(
        db.execute(select(InProgressCard.cosplan_card_id).where(InProgressCard.user_id == user.id)).scalars().all()
    )
    rehearsal_stats_by_card: dict[int, dict[str, Any]] = {}
    card_ids = [card.id for card in cards]
    if card_ids:
        rehearsal_entries = db.execute(
            select(RehearsalEntry)
            .where(RehearsalEntry.cosplan_card_id.in_(card_ids))
            .order_by(RehearsalEntry.entry_date, RehearsalEntry.entry_time, RehearsalEntry.id)
        ).scalars().all()
        active_statuses = {REHEARSAL_STATUS_PROPOSED, REHEARSAL_STATUS_APPROVED, REHEARSAL_STATUS_ACCEPTED}
        today = date.today()
        for entry in rehearsal_entries:
            stat = rehearsal_stats_by_card.setdefault(
                entry.cosplan_card_id,
                {
                    "total": 0,
                    "active": 0,
                    "upcoming_date": None,
                },
            )
            stat["total"] += 1
            if entry.status in active_statuses:
                stat["active"] += 1
            if entry.entry_date and entry.entry_date >= today:
                upcoming_date = stat.get("upcoming_date")
                if upcoming_date is None or entry.entry_date < upcoming_date:
                    stat["upcoming_date"] = entry.entry_date

    editable_card_links: dict[int, int] = {}
    for visible_card in cards:
        source_card = resolve_source_card(db, visible_card)
        if source_card and can_edit_card(user, source_card):
            editable_card_links[visible_card.id] = source_card.id

    return template_response(
        request,
        "cosplan_list.html",
        user=user,
        active_tab="cosplan",
        cards=cards,
        card_totals=card_totals,
        card_total_currencies=card_total_currencies,
        card_date_conflicts=card_date_conflicts,
        q=q,
        in_progress_ids=in_progress_ids,
        rehearsal_stats_by_card=rehearsal_stats_by_card,
        editable_card_links=editable_card_links,
    )


@app.get("/cosplan/export.csv")
def cosplan_export_csv(request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    cards = db.execute(
        select(CosplanCard).where(CosplanCard.user_id == user.id).order_by(CosplanCard.updated_at.desc())
    ).scalars().all()

    output = io.StringIO()
    writer = csv.writer(output, delimiter=";")
    writer.writerow(
        [
            "id",
            "shared",
            "source_card_id",
            "character_name",
            "fandom",
            "plan_type",
            "city",
            "planned_festivals",
            "related_cards",
            "nominations",
            "coproplayers",
            "coproplayer_nicks",
            "references",
            "costume_type",
            "shoes_type",
            "wig_type",
            "performance_track",
            "performance_video_bg_url",
            "performance_duration",
            "performance_rehearsal_point",
            "performance_rehearsal_price",
            "performance_rehearsal_currency",
            "performance_rehearsal_count",
            "performance_rehearsal_total",
            "estimated_total",
            "currency_hint",
            "updated_at",
        ]
    )

    for card in cards:
        total, currency = estimate_card_total_and_currency(card)
        writer.writerow(
            [
                card.id,
                "yes" if card.is_shared_copy else "no",
                card.source_card_id or "",
                card.character_name or "",
                card.fandom or "",
                card.plan_type or "",
                card.city or "",
                ", ".join(as_list(card.planned_festivals_json)),
                ", ".join(
                    str(item["card_id"])
                    for item in parse_related_card_links(as_list(card.related_cards_json), legacy_user_id=card.user_id)
                ),
                ", ".join(as_list(card.nominations_json)),
                ", ".join(as_list(card.coproplayers_json)),
                ", ".join(as_list(card.coproplayer_nicks_json)),
                ", ".join(as_list(card.references_json)),
                card.costume_type or "",
                card.shoes_type or "",
                card.wig_type or "",
                card.performance_track or "",
                card.performance_video_bg_url or "",
                card.performance_duration or "",
                card.performance_rehearsal_point or "",
                "" if card.performance_rehearsal_price is None else f"{card.performance_rehearsal_price:g}",
                card.performance_rehearsal_currency or "",
                card.performance_rehearsal_count or "",
                "" if performance_rehearsal_total(card) is None else f"{performance_rehearsal_total(card):g}",
                f"{total:.2f}",
                currency,
                card.updated_at.isoformat() if card.updated_at else "",
            ]
        )

    output.seek(0)
    filename = f"cosplan-{user.username}-{date.today().isoformat()}.csv"
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/cosplan/new", response_class=HTMLResponse)
def cosplan_new(request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    return template_response(
        request,
        "cosplan_form.html",
        user=user,
        active_tab="cosplan",
        editing=False,
        card_id=None,
        form=get_card_form_values(),
        **card_options(db, user, current_card_id=None, related_cards_user_id=user.id),
    )


@app.get("/cosplan/{card_id}", response_class=HTMLResponse)
def cosplan_detail(card_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    card = get_accessible_card(db, card_id, user, allow_project_leader=True, allow_coproplayer=True)
    if not card:
        add_flash(request, "Карточка не найдена.", "error")
        return redirect("/cosplan")
    editable_card = get_editable_card(db, card.id, user)

    card_owner = db.get(User, card.user_id)
    alias_to_username, users_by_username, _ = build_user_alias_lookup(db)

    project_leader_display = ""
    if card.project_leader:
        leader_value = normalize_username(card.project_leader)
        canonical_leader = alias_to_username.get(leader_value.casefold(), leader_value)
        leader_user = users_by_username.get(canonical_leader.casefold())
        project_leader_display = f"@{preferred_user_alias(leader_user)}" if leader_user else f"@{leader_value}"

    raw_coproplayers = merge_unique(as_list(card.coproplayer_nicks_json), as_list(card.coproplayers_json))
    coproplayers_display = format_coproplayer_names(raw_coproplayers, alias_to_username, users_by_username)

    owner_id = card_owner.id if card_owner else user.id
    all_cards = db.execute(
        select(CosplanCard).where(CosplanCard.user_id == owner_id).order_by(CosplanCard.updated_at.desc())
    ).scalars().all()
    festivals = db.execute(
        select(Festival).where(Festival.user_id == owner_id, Festival.event_date.is_not(None))
    ).scalars().all()
    card_date_conflicts = build_card_date_conflicts([card], all_cards, festivals)

    comments = db.execute(
        select(CardComment).where(CardComment.card_id == card.id).order_by(CardComment.created_at, CardComment.id)
    ).scalars().all()
    author_ids = {comment.author_id for comment in comments}
    authors_by_id: dict[int, User] = {}
    if author_ids:
        authors = db.execute(select(User).where(User.id.in_(author_ids))).scalars().all()
        authors_by_id = {author.id: author for author in authors}

    top_level_comments: list[CardComment] = []
    replies_by_parent: dict[int, list[CardComment]] = defaultdict(list)
    for comment in comments:
        if comment.parent_id:
            replies_by_parent[comment.parent_id].append(comment)
        else:
            top_level_comments.append(comment)

    rehearsal_entries = db.execute(
        select(RehearsalEntry)
        .where(RehearsalEntry.cosplan_card_id == card.id)
        .order_by(RehearsalEntry.entry_date, RehearsalEntry.entry_time, RehearsalEntry.id)
    ).scalars().all()
    rehearsal_user_ids = {
        user_id
        for user_id in [entry.user_id for entry in rehearsal_entries]
        + [entry.proposed_by_user_id for entry in rehearsal_entries]
        if user_id
    }
    rehearsal_users_by_id: dict[int, User] = {}
    if rehearsal_user_ids:
        rehearsal_users = db.execute(select(User).where(User.id.in_(rehearsal_user_ids))).scalars().all()
        rehearsal_users_by_id = {item.id: item for item in rehearsal_users}

    rehearsal_rows: list[dict[str, Any]] = []
    for entry in rehearsal_entries:
        participant_user = rehearsal_users_by_id.get(entry.user_id)
        proposer_user = rehearsal_users_by_id.get(entry.proposed_by_user_id) if entry.proposed_by_user_id else None
        rehearsal_rows.append(
            {
                "id": entry.id,
                "entry_date": entry.entry_date,
                "entry_time": entry.entry_time,
                "status": entry.status,
                "source_type": entry.source_type,
                "participant_alias": (
                    f"@{preferred_user_alias(participant_user)}"
                    if participant_user
                    else f"@user-{entry.user_id}"
                ),
                "proposer_alias": f"@{preferred_user_alias(proposer_user)}" if proposer_user else "",
            }
        )

    card_total, card_total_currency = estimate_card_total_and_currency(card)
    performance_total = performance_rehearsal_total(card)
    related_cards: list[dict[str, Any]] = []
    related_links = parse_related_card_links(as_list(card.related_cards_json), legacy_user_id=card.user_id)
    if related_links:
        related_ids = [item["card_id"] for item in related_links]
        linked_cards_rows = db.execute(
            select(CosplanCard).where(
                CosplanCard.id.in_(related_ids),
                CosplanCard.is_shared_copy.is_(False),
            )
        ).scalars().all()
        linked_cards_by_id = {item.id: item for item in linked_cards_rows}
        related_owner_ids = {item.user_id for item in linked_cards_rows}
        related_owner_ids.update(item["user_id"] for item in related_links)
        related_owners: dict[int, User] = {}
        if related_owner_ids:
            related_owner_rows = db.execute(select(User).where(User.id.in_(related_owner_ids))).scalars().all()
            related_owners = {item.id: item for item in related_owner_rows}
        for related_link in related_links:
            related_id = related_link["card_id"]
            linked = linked_cards_by_id.get(related_id)
            if not linked or linked.id == card.id:
                continue
            link_author = related_owners.get(related_link["user_id"]) or related_owners.get(linked.user_id)
            author_label = f"@{preferred_user_alias(link_author)}" if link_author else ""
            card_label = linked.character_name or f"Карточка #{linked.id}"
            related_cards.append(
                {
                    "id": linked.id,
                    "label": f"{card_label}, {author_label}".strip(", "),
                }
            )
    return template_response(
        request,
        "cosplan_detail.html",
        user=user,
        active_tab="cosplan",
        card=card,
        card_owner=card_owner,
        card_owner_display=(f"@{preferred_user_alias(card_owner)}" if card_owner else ""),
        project_leader_display=project_leader_display,
        coproplayers_display=coproplayers_display,
        cosbands=as_list(card.cosbands_json),
        planned_festivals=as_list(card.planned_festivals_json),
        nominations=as_list(card.nominations_json),
        photographers=as_list(card.photographers_json),
        studios=as_list(card.studios_json),
        unknown_price_fields=as_list(card.unknown_prices_json),
        related_cards=related_cards,
        card_total=card_total,
        card_total_currency=card_total_currency,
        reference_urls=as_list(card.references_json),
        pose_reference_urls=as_list(card.pose_references_json),
        costume_parts=as_list(card.costume_parts_json),
        craft_parts=as_list(card.craft_parts_json),
        photoset_props_checklist=format_checklist_for_form(as_list(card.photoset_props_checklist_json)),
        pinterest_embed_src=pinterest_embed_src,
        looks_like_url=looks_like_url,
        is_mp3_url=is_mp3_url,
        performance_total=performance_total,
        card_date_conflicts=card_date_conflicts,
        can_comment=can_comment_on_card(card, user),
        can_edit_card=bool(editable_card),
        edit_card_id=editable_card.id if editable_card else None,
        top_level_comments=top_level_comments,
        replies_by_parent=replies_by_parent,
        comment_authors=authors_by_id,
        rehearsal_rows=rehearsal_rows,
        rehearsal_status_labels={
            REHEARSAL_STATUS_PROPOSED: rehearsal_status_label(REHEARSAL_STATUS_PROPOSED),
            REHEARSAL_STATUS_APPROVED: rehearsal_status_label(REHEARSAL_STATUS_APPROVED),
            REHEARSAL_STATUS_ACCEPTED: rehearsal_status_label(REHEARSAL_STATUS_ACCEPTED),
            REHEARSAL_STATUS_DECLINED: rehearsal_status_label(REHEARSAL_STATUS_DECLINED),
        },
    )


@app.post("/cosplan/{card_id}/comments")
async def cosplan_add_comment(card_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    card = get_accessible_card(db, card_id, user, allow_project_leader=True)
    if not card:
        add_flash(request, "Карточка не найдена.", "error")
        return redirect("/cosplan")

    if not can_comment_on_card(card, user):
        add_flash(request, "Комментарии доступны только участнику и руководителю проекта.", "error")
        return redirect(f"/cosplan/{card_id}")

    form = await request.form()
    body = str(form.get("comment_body", "")).strip()
    parent_id_raw = str(form.get("parent_id", "")).strip()
    redirect_to = safe_redirect_target(str(form.get("redirect_to", "")), f"/cosplan/{card_id}")
    if not body:
        add_flash(request, "Введите текст комментария.", "error")
        return redirect(redirect_to)

    parent_id: int | None = None
    if parent_id_raw:
        try:
            parsed_parent_id = int(parent_id_raw)
        except ValueError:
            parsed_parent_id = 0
        if parsed_parent_id:
            parent_comment = db.execute(
                select(CardComment).where(CardComment.id == parsed_parent_id, CardComment.card_id == card.id)
            ).scalar_one_or_none()
            if parent_comment:
                parent_id = parent_comment.id

    db.add(
        CardComment(
            card_id=card.id,
            author_id=user.id,
            parent_id=parent_id,
            body=body,
        )
    )
    db.commit()
    add_flash(request, "Комментарий добавлен.", "success")
    return redirect(redirect_to)


@app.get("/cosplan/{card_id}/edit", response_class=HTMLResponse)
def cosplan_edit(card_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    card = get_editable_card(db, card_id, user)
    if not card:
        add_flash(request, "Карточка недоступна для редактирования.", "error")
        return redirect("/cosplan")
    owner_for_options = db.get(User, card.user_id) or user

    return template_response(
        request,
        "cosplan_form.html",
        user=user,
        active_tab="cosplan",
        editing=True,
        card_id=card.id,
        form=get_card_form_values(card, actor_user_id=user.id),
        **card_options(
            db,
            owner_for_options,
            current_card_id=card.id,
            related_cards_user_id=user.id,
        ),
    )


def save_card_from_form(form: Any, card: CosplanCard, user: User, db: Session) -> None:
    alias_to_username, _, _ = build_user_alias_lookup(db)
    raw_unknown_prices = {str(value).strip() for value in form.getlist("unknown_prices") if str(value).strip()}
    allowed_unknown_price_fields = {
        "costume_prepayment",
        "costume_postpayment",
        "costume_buy_price",
        "costume_fabric_price",
        "costume_hardware_price",
        "shoes_buy_price",
        "shoes_price",
        "lenses_price",
        "wig_price",
        "wig_buy_price",
        "craft_price",
        "craft_material_price",
        "photoset_price",
        "photoset_photographer_price",
        "photoset_studio_price",
        "photoset_props_price",
        "photoset_extra_price",
        "performance_rehearsal_price",
    }
    unknown_prices = raw_unknown_prices.intersection(allowed_unknown_price_fields)

    def parse_price_field(field_name: str) -> float | None:
        if field_name in unknown_prices:
            return None
        return parse_float(str(form.get(field_name, "")))

    card.character_name = str(form.get("character_name", "")).strip()
    card.fandom = str(form.get("fandom", "")).strip() or None
    card.is_au = to_bool(form.get("is_au"))
    card.au_text = str(form.get("au_text", "")).strip() or None

    card.costume_type = str(form.get("costume_type", "")).strip() or None
    if card.costume_type == "buy":
        card.sewing_type = None
        card.sewing_fabric = False
        card.sewing_hardware = False
        card.sewing_pattern = False
        card.costume_executor = None
        card.costume_deadline = None
        card.costume_prepayment = None
        card.costume_postpayment = None
        card.costume_fabric_price = None
        card.costume_hardware_price = None
        card.costume_bought = to_bool(form.get("costume_bought"))
        card.costume_link = str(form.get("costume_link", "")).strip() or None
        card.costume_buy_price = parse_price_field("costume_buy_price")
    else:
        card.sewing_type = str(form.get("sewing_type", "")).strip() or None
        card.sewing_fabric = to_bool(form.get("sewing_fabric"))
        card.sewing_hardware = to_bool(form.get("sewing_hardware"))
        card.sewing_pattern = to_bool(form.get("sewing_pattern"))
        card.costume_deadline = parse_date(str(form.get("costume_deadline", "")))
        if card.sewing_type == "self":
            card.costume_executor = None
            card.costume_prepayment = None
            card.costume_postpayment = None
            card.costume_fabric_price = parse_price_field("costume_fabric_price")
            card.costume_hardware_price = parse_price_field("costume_hardware_price")
        else:
            card.costume_executor = str(form.get("costume_executor", "")).strip() or None
            card.costume_prepayment = parse_price_field("costume_prepayment")
            card.costume_postpayment = parse_price_field("costume_postpayment")
            card.costume_fabric_price = None
            card.costume_hardware_price = None
        card.costume_bought = False
        card.costume_link = None
        card.costume_buy_price = None
    card.costume_currency = str(form.get("costume_currency", "")).strip() or None
    card.costume_notes = str(form.get("costume_notes", "")).strip() or None

    card.shoes_type = str(form.get("shoes_type", "")).strip() or None
    if card.shoes_type == "buy":
        card.shoes_bought = to_bool(form.get("shoes_bought"))
        card.shoes_link = str(form.get("shoes_link", "")).strip() or None
        card.shoes_buy_price = parse_price_field("shoes_buy_price")
        card.shoes_executor = None
        card.shoes_deadline = None
        card.shoes_price = None
    else:
        card.shoes_bought = False
        card.shoes_link = None
        card.shoes_buy_price = None
        card.shoes_executor = str(form.get("shoes_executor", "")).strip() or None
        card.shoes_deadline = parse_date(str(form.get("shoes_deadline", "")))
        card.shoes_price = parse_price_field("shoes_price")
    card.shoes_currency = str(form.get("shoes_currency", "")).strip() or None

    card.lenses_enabled = to_bool(form.get("lenses_enabled"))
    if card.lenses_enabled:
        card.lenses_comment = str(form.get("lenses_comment", "")).strip() or None
        card.lenses_color = str(form.get("lenses_color", "")).strip() or None
        card.lenses_price = parse_price_field("lenses_price")
        card.lenses_currency = str(form.get("lenses_currency", "")).strip() or None
    else:
        card.lenses_comment = None
        card.lenses_color = None
        card.lenses_price = None
        card.lenses_currency = None

    card.wig_type = str(form.get("wig_type", "")).strip() or None
    if card.wig_type == "buy":
        card.wigmaker_name = None
        card.wig_price = None
        card.wig_deadline = None
        card.wig_no_buy_from = None
        card.wig_restyle = False
        card.wig_buy_price = parse_price_field("wig_buy_price")
        card.wig_link = str(form.get("wig_link", "")).strip() or None
    elif card.wig_type == "no_buy":
        card.wigmaker_name = None
        card.wig_price = None
        card.wig_deadline = None
        card.wig_buy_price = None
        card.wig_link = None
        card.wig_no_buy_from = str(form.get("wig_no_buy_from", "")).strip() or None
        card.wig_restyle = to_bool(form.get("wig_restyle"))
    else:  # wigmaker
        card.wigmaker_name = str(form.get("wigmaker_name", "")).strip() or None
        card.wig_price = parse_price_field("wig_price")
        card.wig_deadline = parse_date(str(form.get("wig_deadline", "")))
        card.wig_buy_price = None
        card.wig_link = None
        card.wig_no_buy_from = None
        card.wig_restyle = False
    card.wig_currency = str(form.get("wig_currency", "")).strip() or None

    card.craft_type = str(form.get("craft_type", "")).strip() or "self"
    if card.craft_type == "order":
        card.craft_master = str(form.get("craft_master", "")).strip() or None
        card.craft_price = parse_price_field("craft_price")
        card.craft_deadline = parse_date(str(form.get("craft_deadline", "")))
        card.craft_material_price = None
    else:
        card.craft_master = None
        card.craft_price = None
        card.craft_deadline = None
        card.craft_material_price = parse_price_field("craft_material_price")
    card.craft_currency = str(form.get("craft_currency", "")).strip() or None

    card.plan_type = str(form.get("plan_type", "")).strip() or None
    if card.plan_type == "project":
        project_leader_raw = str(form.get("project_leader", "")).strip()
        card.project_leader = resolve_alias_to_username(project_leader_raw, alias_to_username) or None
    else:
        card.project_leader = None
    card.project_deadline = parse_date(str(form.get("project_deadline", "")))
    selected_related_ids = parse_id_list(list(form.getlist("related_card_ids")))
    existing_related_links = parse_related_card_links(as_list(card.related_cards_json), legacy_user_id=card.user_id)
    if card.plan_type == "project":
        valid_related_ids: list[int] = []
        if selected_related_ids:
            valid_ids = set(
                db.execute(
                    select(CosplanCard.id).where(
                        CosplanCard.id.in_(selected_related_ids),
                        CosplanCard.user_id == user.id,
                        CosplanCard.is_shared_copy.is_(False),
                    )
                ).scalars().all()
            )
            valid_related_ids = [card_id for card_id in selected_related_ids if card_id in valid_ids and card_id != card.id]
        preserved_links = [item for item in existing_related_links if item["user_id"] != user.id]
        editor_links = [{"card_id": card_id, "user_id": user.id} for card_id in valid_related_ids]
        card.related_cards_json = preserved_links + editor_links
    else:
        card.related_cards_json = []

    cosbands = merge_unique(form.getlist("cosbands"), split_csv(str(form.get("cosbands_new", ""))))
    festivals = merge_unique(
        form.getlist("planned_festivals"),
        split_csv(str(form.get("planned_festivals_new", ""))),
    )
    nominations = merge_unique(form.getlist("nominations"), split_csv(str(form.get("nominations_new", ""))))
    photographers = merge_unique(
        form.getlist("photographers"),
        split_csv(str(form.get("photographers_new", ""))),
    )
    studios = merge_unique(form.getlist("studios"), split_csv(str(form.get("studios_new", ""))))
    coproplayer_alias_rows = [str(value).strip() for value in form.getlist("coproplayer_alias") if str(value).strip()]
    coproplayer_aliases = merge_unique(
        coproplayer_alias_rows,
        split_csv(str(form.get("coproplayers_input", ""))),
        form.getlist("coproplayers"),  # backward compatibility with older forms
        split_csv(str(form.get("coproplayers_new", ""))),  # backward compatibility
        split_csv(str(form.get("coproplayer_nicks_input", ""))),  # backward compatibility
    )
    coproplayer_nicks = resolve_aliases_to_usernames(coproplayer_aliases, alias_to_username)

    card.cosbands_json = cosbands
    card.planned_festivals_json = festivals
    card.submission_date = parse_date(str(form.get("submission_date", "")))
    card.nominations_json = nominations
    card.city = str(form.get("city", "")).strip() or None

    card.photographers_json = photographers
    card.studios_json = studios
    card.photoset_date = parse_date(str(form.get("photoset_date", "")))
    legacy_photoset_price = parse_price_field("photoset_price")
    card.photoset_photographer_price = parse_price_field("photoset_photographer_price")
    card.photoset_studio_price = parse_price_field("photoset_studio_price")
    card.photoset_props_price = parse_price_field("photoset_props_price")
    card.photoset_extra_price = parse_price_field("photoset_extra_price")
    if any(
        value is not None
        for value in [
            card.photoset_photographer_price,
            card.photoset_studio_price,
            card.photoset_props_price,
            card.photoset_extra_price,
        ]
    ):
        card.photoset_price = float(
            (card.photoset_photographer_price or 0.0)
            + (card.photoset_studio_price or 0.0)
            + (card.photoset_props_price or 0.0)
            + (card.photoset_extra_price or 0.0)
        )
    else:
        card.photoset_price = legacy_photoset_price
    card.photoset_currency = str(form.get("photoset_currency", "")).strip() or None
    card.photoset_comment = str(form.get("photoset_comment", "")).strip() or None
    card.photoset_props_checklist_json = parse_checklist_rows_from_form(form, "photoset_prop")

    card.performance_track = str(form.get("performance_track", "")).strip() or None
    card.performance_video_bg_url = str(form.get("performance_video_bg_url", "")).strip() or None
    card.performance_script = str(form.get("performance_script", "")).strip() or None
    card.performance_light_script = str(form.get("performance_light_script", "")).strip() or None
    card.performance_duration = normalize_duration_mmss(str(form.get("performance_duration", "")))
    card.performance_rehearsal_point = str(form.get("performance_rehearsal_point", "")).strip() or None
    card.performance_rehearsal_price = parse_price_field("performance_rehearsal_price")
    card.performance_rehearsal_currency = str(form.get("performance_rehearsal_currency", "")).strip() or None
    card.performance_rehearsal_count = parse_positive_int(str(form.get("performance_rehearsal_count", "")))

    card.references_json = parse_reference_values(str(form.get("references_input", "")))
    card.pose_references_json = parse_reference_values(str(form.get("pose_references_input", "")))
    card.costume_parts_json = parse_parts_from_form(form, "costume", card.costume_currency)
    card.craft_parts_json = parse_parts_from_form(form, "craft", card.craft_currency)
    active_unknown_fields = {"photoset_price"}
    if card.costume_type == "buy":
        active_unknown_fields.add("costume_buy_price")
    elif card.sewing_type == "self":
        active_unknown_fields.update({"costume_fabric_price", "costume_hardware_price"})
    else:
        active_unknown_fields.update({"costume_prepayment", "costume_postpayment"})

    if card.shoes_type == "buy":
        active_unknown_fields.add("shoes_buy_price")
    else:
        active_unknown_fields.add("shoes_price")

    if card.lenses_enabled:
        active_unknown_fields.add("lenses_price")

    if card.wig_type == "buy":
        active_unknown_fields.add("wig_buy_price")
    elif card.wig_type == "wigmaker":
        active_unknown_fields.add("wig_price")

    if card.craft_type == "order":
        active_unknown_fields.add("craft_price")
    else:
        active_unknown_fields.add("craft_material_price")

    active_unknown_fields.update(
        {
            "photoset_photographer_price",
            "photoset_studio_price",
            "photoset_props_price",
            "photoset_extra_price",
            "performance_rehearsal_price",
        }
    )

    card.unknown_prices_json = sorted(unknown_prices.intersection(active_unknown_fields))

    card.coproplayers_json = coproplayer_aliases
    card.coproplayer_nicks_json = coproplayer_nicks
    card.notes = str(form.get("notes", "")).strip() or None

    remember_options(db, user.id, "fandom", [card.fandom] if card.fandom else [])
    remember_options(db, user.id, "cosband", cosbands)
    remember_options(db, user.id, "festival", festivals)
    remember_options(db, user.id, "nomination", nominations)
    remember_options(db, user.id, "photographer", photographers)
    remember_options(db, user.id, "studio", studios)
    remember_options(db, user.id, "coproplayer", merge_unique(coproplayer_aliases, coproplayer_nicks))
    remember_options(db, user.id, "coproplayer_nick", coproplayer_nicks)
    remember_options(db, user.id, "project_leader", [card.project_leader or ""])
    remember_options(
        db,
        user.id,
        "currency",
        [
            card.costume_currency or "",
            card.shoes_currency or "",
            card.lenses_currency or "",
            card.wig_currency or "",
            card.craft_currency or "",
            card.photoset_currency or "",
            card.performance_rehearsal_currency or "",
        ],
    )


@app.post("/cosplan/new")
async def cosplan_create(request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    form = await request.form()
    character_name = str(form.get("character_name", "")).strip()
    if not character_name:
        add_flash(request, "Имя персонажа обязательно.", "error")
        return redirect("/cosplan/new")

    card = CosplanCard(user_id=user.id, character_name=character_name)
    save_card_from_form(form, card, user, db)

    db.add(card)
    db.flush()
    conflict_notifications = notify_coproplayer_conflicts_for_card(db, card=card, owner=user)
    sync_shared_cards_for_nicks(card, user, db)
    db.commit()

    if conflict_notifications:
        add_flash(
            request,
            f"Карточка косплана создана. Найдены конфликты у сокосплееров: {conflict_notifications}.",
            "success",
        )
    else:
        add_flash(request, "Карточка косплана создана.", "success")
    return redirect("/cosplan")


@app.post("/cosplan/{card_id}/edit")
async def cosplan_update(card_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    card = get_editable_card(db, card_id, user)
    if not card:
        add_flash(request, "Карточка недоступна для редактирования.", "error")
        return redirect("/cosplan")

    form = await request.form()
    character_name = str(form.get("character_name", "")).strip()
    if not character_name:
        add_flash(request, "Имя персонажа обязательно.", "error")
        return redirect(f"/cosplan/{card.id}/edit")

    save_card_from_form(form, card, user, db)
    conflict_notifications = notify_coproplayer_conflicts_for_card(db, card=card, owner=user)
    sync_shared_cards_for_nicks(card, user, db)
    linked_rehearsal_cards = db.execute(
        select(RehearsalCard).where(RehearsalCard.cosplan_card_id == card.id)
    ).scalars().all()
    for rehearsal_card in linked_rehearsal_cards:
        rehearsal_card.deadline_date = card.project_deadline
    db.commit()

    if conflict_notifications:
        add_flash(
            request,
            f"Карточка косплана обновлена. Найдены конфликты у сокосплееров: {conflict_notifications}.",
            "success",
        )
    else:
        add_flash(request, "Карточка косплана обновлена.", "success")
    return redirect("/cosplan")


@app.post("/cosplan/{card_id}/delete")
def cosplan_delete(card_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    card = db.execute(select(CosplanCard).where(CosplanCard.id == card_id, CosplanCard.user_id == user.id)).scalar_one_or_none()
    if not card:
        add_flash(request, "Карточка не найдена.", "error")
        return redirect("/cosplan")

    if not card.is_shared_copy:
        shared_copies = db.execute(
            select(CosplanCard).where(
                CosplanCard.source_card_id == card.id,
                CosplanCard.is_shared_copy.is_(True),
            )
        ).scalars().all()
        for shared_copy in shared_copies:
            delete_card_with_runtime_dependents(db, shared_copy)

        notifications = db.execute(
            select(FestivalNotification).where(FestivalNotification.source_card_id == card.id)
        ).scalars().all()
        for notification in notifications:
            db.delete(notification)

    progress = db.execute(
        select(InProgressCard).where(InProgressCard.user_id == user.id, InProgressCard.cosplan_card_id == card.id)
    ).scalar_one_or_none()
    if progress:
        db.delete(progress)

    rehearsal_entries = db.execute(
        select(RehearsalEntry).where(RehearsalEntry.cosplan_card_id == card.id)
    ).scalars().all()
    for rehearsal_entry in rehearsal_entries:
        db.delete(rehearsal_entry)
    rehearsal_cards = db.execute(
        select(RehearsalCard).where(RehearsalCard.cosplan_card_id == card.id)
    ).scalars().all()
    for rehearsal_card in rehearsal_cards:
        db.delete(rehearsal_card)

    delete_card_with_runtime_dependents(db, card)
    db.commit()

    add_flash(request, "Карточка удалена.", "info")
    return redirect("/cosplan")


@app.post("/in-progress/add/{card_id}")
def in_progress_add(card_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    card = db.execute(select(CosplanCard).where(CosplanCard.id == card_id, CosplanCard.user_id == user.id)).scalar_one_or_none()
    if not card:
        add_flash(request, "Карточка косплана не найдена.", "error")
        return redirect("/cosplan")

    existing = db.execute(
        select(InProgressCard).where(InProgressCard.user_id == user.id, InProgressCard.cosplan_card_id == card.id)
    ).scalar_one_or_none()
    if existing:
        add_flash(request, "Карточка уже в разделе «В работе».", "info")
        return redirect("/in-progress")

    progress = InProgressCard(user_id=user.id, cosplan_card_id=card.id, checklist_json=[], task_rows_json=[])
    db.add(progress)
    db.commit()

    add_flash(request, "Карточка добавлена в раздел «В работе».", "success")
    return redirect("/in-progress")


@app.get("/in-progress", response_class=HTMLResponse)
def in_progress_list(request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    progress_items = db.execute(
        select(InProgressCard)
        .where(InProgressCard.user_id == user.id)
        .order_by(InProgressCard.is_frozen.asc(), InProgressCard.updated_at.desc())
    ).scalars().all()
    today = date.today()
    urgent_deadline = today + timedelta(days=14)
    urgent_progress_ids = {
        row.id
        for row in progress_items
        if row.cosplan_card
        and row.cosplan_card.project_deadline
        and today <= row.cosplan_card.project_deadline <= urgent_deadline
        and not row.is_frozen
    }
    progress_card_ids = [row.cosplan_card_id for row in progress_items if row.cosplan_card_id]
    leader_rehearsals_by_card: dict[int, list[RehearsalEntry]] = defaultdict(list)
    task_assignees_by_progress: dict[int, list[dict[str, Any]]] = {}
    task_rows_by_progress: dict[int, list[dict[str, Any]]] = {}
    alias_to_username, users_by_username, _ = build_user_alias_lookup(db)

    for row in progress_items:
        card = row.cosplan_card
        if not card or not card.is_shared_copy:
            continue
        task_assignees_by_progress[row.id] = card_task_assignee_options(card, alias_to_username, users_by_username)
        task_rows_by_progress[row.id] = format_in_progress_tasks(
            as_list(row.task_rows_json),
            alias_to_username,
            users_by_username,
        )

    if progress_card_ids:
        leader_entries = db.execute(
            select(RehearsalEntry)
            .where(
                RehearsalEntry.user_id == user.id,
                RehearsalEntry.source_type == REHEARSAL_SOURCE_LEADER,
                RehearsalEntry.cosplan_card_id.in_(progress_card_ids),
            )
            .order_by(RehearsalEntry.entry_date, RehearsalEntry.entry_time, RehearsalEntry.id)
        ).scalars().all()
        for entry in leader_entries:
            leader_rehearsals_by_card[entry.cosplan_card_id].append(entry)

    return template_response(
        request,
        "in_progress.html",
        user=user,
        active_tab="in-progress",
        progress_items=progress_items,
        urgent_progress_ids=urgent_progress_ids,
        leader_rehearsals_by_card=leader_rehearsals_by_card,
        task_assignees_by_progress=task_assignees_by_progress,
        task_rows_by_progress=task_rows_by_progress,
        rehearsal_status_labels={
            REHEARSAL_STATUS_PROPOSED: rehearsal_status_label(REHEARSAL_STATUS_PROPOSED),
            REHEARSAL_STATUS_ACCEPTED: rehearsal_status_label(REHEARSAL_STATUS_ACCEPTED),
            REHEARSAL_STATUS_DECLINED: rehearsal_status_label(REHEARSAL_STATUS_DECLINED),
        },
    )


@app.post("/in-progress/{progress_id}/checklist/add")
async def in_progress_checklist_add(progress_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    progress = db.execute(
        select(InProgressCard).where(InProgressCard.id == progress_id, InProgressCard.user_id == user.id)
    ).scalar_one_or_none()
    if not progress:
        add_flash(request, "Карточка «В работе» не найдена.", "error")
        return redirect("/in-progress")

    form = await request.form()
    item_text = str(form.get("item_text", "")).strip()
    if item_text:
        items = list(progress.checklist_json or [])
        items.append({"text": item_text, "done": False})
        progress.checklist_json = items
        db.commit()
        add_flash(request, "Пункт добавлен.", "success")

    return redirect("/in-progress")


@app.post("/in-progress/{progress_id}/checklist/toggle/{item_index}")
def in_progress_checklist_toggle(progress_id: int, item_index: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    progress = db.execute(
        select(InProgressCard).where(InProgressCard.id == progress_id, InProgressCard.user_id == user.id)
    ).scalar_one_or_none()
    if not progress:
        add_flash(request, "Карточка «В работе» не найдена.", "error")
        return redirect("/in-progress")

    items = list(progress.checklist_json or [])
    if 0 <= item_index < len(items):
        item = dict(items[item_index])
        item["done"] = not bool(item.get("done"))
        items[item_index] = item
        progress.checklist_json = items
        db.commit()

    return redirect("/in-progress")


@app.post("/in-progress/{progress_id}/checklist/delete/{item_index}")
def in_progress_checklist_delete(progress_id: int, item_index: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    progress = db.execute(
        select(InProgressCard).where(InProgressCard.id == progress_id, InProgressCard.user_id == user.id)
    ).scalar_one_or_none()
    if not progress:
        add_flash(request, "Карточка «В работе» не найдена.", "error")
        return redirect("/in-progress")

    items = list(progress.checklist_json or [])
    if 0 <= item_index < len(items):
        items.pop(item_index)
        progress.checklist_json = items
        db.commit()

    return redirect("/in-progress")


@app.post("/in-progress/{progress_id}/tasks/add")
async def in_progress_task_add(progress_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    progress = db.execute(
        select(InProgressCard).where(InProgressCard.id == progress_id, InProgressCard.user_id == user.id)
    ).scalar_one_or_none()
    if not progress:
        add_flash(request, "Карточка «В работе» не найдена.", "error")
        return redirect("/in-progress")

    card = progress.cosplan_card
    if not card or not card.is_shared_copy:
        add_flash(request, "Блок «Задания» доступен только для карточек «Общий доступ».", "error")
        return redirect("/in-progress")

    form = await request.form()
    task_text = str(form.get("task_text", "")).strip()
    assignee_raw = str(form.get("task_assignee", "")).strip()
    if not task_text:
        add_flash(request, "Введите текст задания.", "error")
        return redirect("/in-progress")
    if not assignee_raw:
        add_flash(request, "Выберите ответственного.", "error")
        return redirect("/in-progress")

    alias_to_username, users_by_username, _ = build_user_alias_lookup(db)
    canonical_assignee = resolve_alias_to_username(assignee_raw, alias_to_username)
    allowed_assignees = {
        (item.get("value") or "").casefold()
        for item in card_task_assignee_options(card, alias_to_username, users_by_username)
    }
    if not canonical_assignee or canonical_assignee.casefold() not in allowed_assignees:
        add_flash(request, "Выберите ответственного из списка участников карточки.", "error")
        return redirect("/in-progress")

    existing_rows = format_in_progress_tasks(
        as_list(progress.task_rows_json),
        alias_to_username,
        users_by_username,
    )
    existing_rows.append(
        {
            "assignee": canonical_assignee,
            "task": task_text,
            "done": False,
        }
    )
    progress.task_rows_json = [
        {
            "assignee": row.get("assignee"),
            "task": row.get("task"),
            "done": bool(row.get("done")),
        }
        for row in existing_rows
        if row.get("task")
    ]

    assignee_user = users_by_username.get(canonical_assignee.casefold())
    if assignee_user and assignee_user.id != user.id:
        enqueue_notification_if_missing(
            db,
            user_id=assignee_user.id,
            from_user_id=user.id,
            source_card_id=card.id,
            message=f"Вам назначено задание по «{card.character_name}»: {task_text}",
        )

    db.commit()
    add_flash(request, "Задание добавлено.", "success")
    return redirect("/in-progress")


@app.post("/in-progress/{progress_id}/tasks/toggle/{task_index}")
def in_progress_task_toggle(progress_id: int, task_index: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    progress = db.execute(
        select(InProgressCard).where(InProgressCard.id == progress_id, InProgressCard.user_id == user.id)
    ).scalar_one_or_none()
    if not progress:
        add_flash(request, "Карточка «В работе» не найдена.", "error")
        return redirect("/in-progress")

    card = progress.cosplan_card
    if not card or not card.is_shared_copy:
        add_flash(request, "Блок «Задания» доступен только для карточек «Общий доступ».", "error")
        return redirect("/in-progress")

    alias_to_username, users_by_username, _ = build_user_alias_lookup(db)
    rows = format_in_progress_tasks(as_list(progress.task_rows_json), alias_to_username, users_by_username)
    if 0 <= task_index < len(rows):
        rows[task_index]["done"] = not bool(rows[task_index].get("done"))
        progress.task_rows_json = [
            {
                "assignee": row.get("assignee"),
                "task": row.get("task"),
                "done": bool(row.get("done")),
            }
            for row in rows
            if row.get("task")
        ]
        db.commit()

    return redirect("/in-progress")


@app.post("/in-progress/{progress_id}/tasks/delete/{task_index}")
def in_progress_task_delete(progress_id: int, task_index: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    progress = db.execute(
        select(InProgressCard).where(InProgressCard.id == progress_id, InProgressCard.user_id == user.id)
    ).scalar_one_or_none()
    if not progress:
        add_flash(request, "Карточка «В работе» не найдена.", "error")
        return redirect("/in-progress")

    card = progress.cosplan_card
    if not card or not card.is_shared_copy:
        add_flash(request, "Блок «Задания» доступен только для карточек «Общий доступ».", "error")
        return redirect("/in-progress")

    alias_to_username, users_by_username, _ = build_user_alias_lookup(db)
    rows = format_in_progress_tasks(as_list(progress.task_rows_json), alias_to_username, users_by_username)
    if 0 <= task_index < len(rows):
        rows.pop(task_index)
        progress.task_rows_json = [
            {
                "assignee": row.get("assignee"),
                "task": row.get("task"),
                "done": bool(row.get("done")),
            }
            for row in rows
            if row.get("task")
        ]
        db.commit()

    return redirect("/in-progress")


@app.post("/in-progress/{progress_id}/remove")
def in_progress_remove(progress_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    progress = db.execute(
        select(InProgressCard).where(InProgressCard.id == progress_id, InProgressCard.user_id == user.id)
    ).scalar_one_or_none()
    if not progress:
        add_flash(request, "Карточка «В работе» не найдена.", "error")
        return redirect("/in-progress")

    db.delete(progress)
    db.commit()

    add_flash(request, "Карточка удалена из раздела «В работе».", "info")
    return redirect("/in-progress")


@app.post("/in-progress/{progress_id}/freeze-toggle")
def in_progress_toggle_freeze(progress_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    progress = db.execute(
        select(InProgressCard).where(InProgressCard.id == progress_id, InProgressCard.user_id == user.id)
    ).scalar_one_or_none()
    if not progress:
        add_flash(request, "Карточка «В работе» не найдена.", "error")
        return redirect("/in-progress")

    progress.is_frozen = not bool(progress.is_frozen)
    db.commit()
    if progress.is_frozen:
        add_flash(request, "Проект заморожен и перемещён в конец списка.", "info")
    else:
        add_flash(request, "Проект разморожен.", "success")
    return redirect("/in-progress")


@app.get("/rehearsals", response_class=HTMLResponse)
def rehearsals_list(request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    available_cards = db.execute(
        select(CosplanCard)
        .where(CosplanCard.user_id == user.id, CosplanCard.is_shared_copy.is_(False))
        .order_by(CosplanCard.character_name)
    ).scalars().all()
    rehearsal_cards = db.execute(
        select(RehearsalCard)
        .where(RehearsalCard.user_id == user.id)
        .order_by(RehearsalCard.updated_at.desc(), RehearsalCard.id.desc())
    ).scalars().all()
    deadlines_synced = False
    for rehearsal_card in rehearsal_cards:
        card = rehearsal_card.cosplan_card
        if not card:
            continue
        if rehearsal_card.deadline_date != card.project_deadline:
            rehearsal_card.deadline_date = card.project_deadline
            deadlines_synced = True
    if deadlines_synced:
        db.commit()

    entries = db.execute(
        select(RehearsalEntry)
        .where(RehearsalEntry.user_id == user.id)
        .order_by(RehearsalEntry.entry_date, RehearsalEntry.entry_time, RehearsalEntry.id)
    ).scalars().all()
    entries_by_card: dict[int, list[RehearsalEntry]] = defaultdict(list)
    participant_entries_count: dict[int, int] = defaultdict(int)
    for entry in entries:
        entries_by_card[entry.rehearsal_card_id].append(entry)
        if entry.source_type == REHEARSAL_SOURCE_PARTICIPANT:
            participant_entries_count[entry.rehearsal_card_id] += 1

    proposer_ids = {entry.proposed_by_user_id for entry in entries if entry.proposed_by_user_id}
    proposers_by_id: dict[int, User] = {}
    if proposer_ids:
        proposers = db.execute(select(User).where(User.id.in_(proposer_ids))).scalars().all()
        proposers_by_id = {item.id: item for item in proposers}

    return template_response(
        request,
        "rehearsals.html",
        user=user,
        active_tab="rehearsals",
        available_cards=available_cards,
        rehearsal_cards=rehearsal_cards,
        entries_by_card=entries_by_card,
        participant_entries_count=participant_entries_count,
        proposers_by_id=proposers_by_id,
        rehearsal_status_labels={
            REHEARSAL_STATUS_PROPOSED: rehearsal_status_label(REHEARSAL_STATUS_PROPOSED),
            REHEARSAL_STATUS_APPROVED: rehearsal_status_label(REHEARSAL_STATUS_APPROVED),
            REHEARSAL_STATUS_ACCEPTED: rehearsal_status_label(REHEARSAL_STATUS_ACCEPTED),
            REHEARSAL_STATUS_DECLINED: rehearsal_status_label(REHEARSAL_STATUS_DECLINED),
        },
    )


@app.post("/rehearsals/new")
async def rehearsals_create(request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    form = await request.form()
    raw_id = str(form.get("cosplan_card_id", "")).strip()
    if not raw_id.isdigit():
        add_flash(request, "Выберите карточку косплана.", "error")
        return redirect("/rehearsals")

    card = db.execute(
        select(CosplanCard).where(
            CosplanCard.id == int(raw_id),
            CosplanCard.user_id == user.id,
            CosplanCard.is_shared_copy.is_(False),
        )
    ).scalar_one_or_none()
    if not card:
        add_flash(request, "Карточка косплана не найдена.", "error")
        return redirect("/rehearsals")

    existing_rehearsal_card = db.execute(
        select(RehearsalCard).where(
            RehearsalCard.user_id == user.id,
            RehearsalCard.cosplan_card_id == card.id,
        )
    ).scalar_one_or_none()
    rehearsal_card = get_or_create_rehearsal_card(db, user_id=user.id, cosplan_card=card)
    db.commit()
    add_flash(
        request,
        "Карточка репетиций обновлена." if existing_rehearsal_card else "Карточка репетиций создана.",
        "success",
    )
    return redirect("/rehearsals")


@app.post("/rehearsals/{rehearsal_card_id}/add-date")
async def rehearsals_add_date(rehearsal_card_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    rehearsal_card = db.execute(
        select(RehearsalCard).where(
            RehearsalCard.id == rehearsal_card_id,
            RehearsalCard.user_id == user.id,
        )
    ).scalar_one_or_none()
    if not rehearsal_card:
        add_flash(request, "Карточка репетиций не найдена.", "error")
        return redirect("/rehearsals")

    form = await request.form()
    entry_date = parse_date(str(form.get("entry_date", "")))
    if not entry_date:
        add_flash(request, "Укажите дату репетиции.", "error")
        return redirect("/rehearsals")

    participant_entries_count = db.execute(
        select(RehearsalEntry)
        .where(
            RehearsalEntry.rehearsal_card_id == rehearsal_card.id,
            RehearsalEntry.source_type == REHEARSAL_SOURCE_PARTICIPANT,
        )
        .order_by(RehearsalEntry.id)
    ).scalars().all()
    if len(participant_entries_count) >= 10:
        add_flash(request, "В одной карточке можно указать не более 10 дат репетиций.", "error")
        return redirect("/rehearsals")

    card = rehearsal_card.cosplan_card
    if card and can_manage_project_card(user, card):
        # Если владелец карточки также руководитель, заявка сразу одобряется.
        status = REHEARSAL_STATUS_APPROVED
    elif card and card.plan_type == "project" and (card.project_leader or "").strip():
        status = REHEARSAL_STATUS_PROPOSED
    else:
        status = REHEARSAL_STATUS_APPROVED

    db.add(
        RehearsalEntry(
            rehearsal_card_id=rehearsal_card.id,
            user_id=user.id,
            cosplan_card_id=rehearsal_card.cosplan_card_id,
            proposed_by_user_id=user.id,
            source_type=REHEARSAL_SOURCE_PARTICIPANT,
            status=status,
            entry_date=entry_date,
            entry_time=None,
        )
    )
    db.commit()
    if status == REHEARSAL_STATUS_PROPOSED:
        add_flash(request, "Дата отправлена руководителю со статусом «Предложено».", "success")
    else:
        add_flash(request, "Дата репетиции добавлена в календарь.", "success")
    return redirect("/rehearsals")


@app.post("/rehearsals/entries/{entry_id}/respond")
async def rehearsals_respond(entry_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    form = await request.form()
    decision = str(form.get("decision", "")).strip().lower()
    next_url = safe_redirect_target(str(form.get("next", "")).strip(), "/rehearsals")
    if decision not in {"accept", "decline"}:
        add_flash(request, "Некорректный статус ответа.", "error")
        return redirect(next_url)

    entry = db.execute(
        select(RehearsalEntry).where(
            RehearsalEntry.id == entry_id,
            RehearsalEntry.user_id == user.id,
            RehearsalEntry.source_type == REHEARSAL_SOURCE_LEADER,
        )
    ).scalar_one_or_none()
    if not entry:
        add_flash(request, "Запись репетиции не найдена.", "error")
        return redirect(next_url)

    entry.status = REHEARSAL_STATUS_ACCEPTED if decision == "accept" else REHEARSAL_STATUS_DECLINED
    db.commit()
    add_flash(
        request,
        "Репетиция принята и добавлена в календарь."
        if entry.status == REHEARSAL_STATUS_ACCEPTED
        else "Репетиция отклонена.",
        "success" if entry.status == REHEARSAL_STATUS_ACCEPTED else "info",
    )
    return redirect(next_url)


@app.post("/rehearsals/entries/{entry_id}/delete")
async def rehearsals_entry_delete(entry_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    form = await request.form()
    next_url = safe_redirect_target(str(form.get("next", "")).strip(), "/rehearsals")

    entry = db.get(RehearsalEntry, entry_id)
    if not entry:
        add_flash(request, "Запись репетиции не найдена.", "error")
        return redirect(next_url)

    card = db.get(CosplanCard, entry.cosplan_card_id)
    can_delete = (
        entry.user_id == user.id
        or (entry.proposed_by_user_id == user.id)
        or (card is not None and can_manage_project_card(user, card))
    )
    if not can_delete:
        add_flash(request, "Недостаточно прав для удаления репетиции.", "error")
        return redirect(next_url)

    db.delete(entry)
    db.commit()
    add_flash(request, "Репетиция удалена.", "info")
    return redirect(next_url)


@app.get("/my-projects", response_class=HTMLResponse)
def my_projects_list(request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    project_cards = db.execute(
        select(CosplanCard).where(
            CosplanCard.plan_type == "project",
            CosplanCard.is_shared_copy.is_(False),
            CosplanCard.project_leader.is_not(None),
        )
    ).scalars().all()
    cards = [card for card in project_cards if user_matches_alias(user, card.project_leader)]
    cards.sort(key=lambda item: item.updated_at or item.created_at or datetime.min, reverse=True)

    owner_ids = {card.user_id for card in cards}
    owners_by_id: dict[int, User] = {}
    if owner_ids:
        owners = db.execute(select(User).where(User.id.in_(owner_ids))).scalars().all()
        owners_by_id = {owner.id: owner for owner in owners}

    card_totals: dict[int, float] = {}
    card_total_currencies: dict[int, str] = {}
    for card in cards:
        total, currency = estimate_card_total_and_currency(card)
        card_totals[card.id] = total
        card_total_currencies[card.id] = currency

    pending_rehearsals_by_card: dict[int, list[RehearsalEntry]] = defaultdict(list)
    leader_rehearsal_history_by_card: dict[int, list[RehearsalEntry]] = defaultdict(list)
    card_ids = [card.id for card in cards]
    if card_ids:
        pending_entries = db.execute(
            select(RehearsalEntry)
            .where(
                RehearsalEntry.cosplan_card_id.in_(card_ids),
                RehearsalEntry.source_type == REHEARSAL_SOURCE_PARTICIPANT,
                RehearsalEntry.status == REHEARSAL_STATUS_PROPOSED,
            )
            .order_by(RehearsalEntry.entry_date, RehearsalEntry.entry_time, RehearsalEntry.id)
        ).scalars().all()
        for entry in pending_entries:
            pending_rehearsals_by_card[entry.cosplan_card_id].append(entry)

        history_entries = db.execute(
            select(RehearsalEntry)
            .where(
                RehearsalEntry.cosplan_card_id.in_(card_ids),
                RehearsalEntry.source_type == REHEARSAL_SOURCE_LEADER,
                RehearsalEntry.status.in_([REHEARSAL_STATUS_ACCEPTED, REHEARSAL_STATUS_DECLINED]),
            )
            .order_by(RehearsalEntry.updated_at.desc(), RehearsalEntry.id.desc())
        ).scalars().all()
        for entry in history_entries:
            leader_rehearsal_history_by_card[entry.cosplan_card_id].append(entry)

    return template_response(
        request,
        "my_projects.html",
        user=user,
        active_tab="my-projects",
        cards=cards,
        owners_by_id=owners_by_id,
        card_totals=card_totals,
        card_total_currencies=card_total_currencies,
        pending_rehearsals_by_card=pending_rehearsals_by_card,
        leader_rehearsal_history_by_card=leader_rehearsal_history_by_card,
        rehearsal_status_labels={
            REHEARSAL_STATUS_PROPOSED: rehearsal_status_label(REHEARSAL_STATUS_PROPOSED),
            REHEARSAL_STATUS_APPROVED: rehearsal_status_label(REHEARSAL_STATUS_APPROVED),
            REHEARSAL_STATUS_ACCEPTED: rehearsal_status_label(REHEARSAL_STATUS_ACCEPTED),
            REHEARSAL_STATUS_DECLINED: rehearsal_status_label(REHEARSAL_STATUS_DECLINED),
        },
    )


@app.post("/my-projects/{card_id}/comment")
async def my_projects_comment(card_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    card = get_accessible_card(db, card_id, user, allow_project_leader=True)
    if not card:
        add_flash(request, "Карточка проекта не найдена.", "error")
        return redirect("/my-projects")

    if not can_comment_on_card(card, user) or not user_matches_alias(user, card.project_leader):
        add_flash(request, "Нет прав на комментарий для этой карточки.", "error")
        return redirect("/my-projects")

    form = await request.form()
    comment = str(form.get("leader_comment", "")).strip()
    if not comment:
        add_flash(request, "Введите комментарий.", "error")
        return redirect("/my-projects")

    db.add(CardComment(card_id=card.id, author_id=user.id, body=comment))
    db.commit()
    add_flash(request, "Комментарий добавлен.", "success")
    return redirect("/my-projects")


@app.post("/my-projects/rehearsals/propose")
async def my_projects_propose_rehearsal(request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    form = await request.form()
    selected_ids: list[int] = []
    for raw in form.getlist("cosplan_card_ids"):
        value = str(raw).strip()
        if value.isdigit():
            selected_ids.append(int(value))
    selected_ids = list(dict.fromkeys(selected_ids))

    if not selected_ids:
        add_flash(request, "Выберите хотя бы одну карточку проекта.", "error")
        return redirect("/my-projects")

    rehearsal_date = parse_date(str(form.get("entry_date", "")))
    if not rehearsal_date:
        add_flash(request, "Укажите дату репетиции.", "error")
        return redirect("/my-projects")

    rehearsal_time = parse_time_hhmm(str(form.get("entry_time", "")))
    raw_time = str(form.get("entry_time", "")).strip()
    if raw_time and rehearsal_time is None:
        add_flash(request, "Время укажите в формате ЧЧ:ММ.", "error")
        return redirect("/my-projects")

    cards = db.execute(select(CosplanCard).where(CosplanCard.id.in_(selected_ids))).scalars().all()
    target_cards = [card for card in cards if can_manage_project_card(user, card)]
    if not target_cards:
        add_flash(request, "Нет доступных карточек для предложения репетиции.", "error")
        return redirect("/my-projects")

    created = 0
    for card in target_cards:
        rehearsal_card = get_or_create_rehearsal_card(db, user_id=card.user_id, cosplan_card=card)
        participant_user = db.get(User, card.user_id)
        if participant_user:
            readable_date = rehearsal_date.strftime("%d-%m-%Y")
            readable_time = f" {rehearsal_time}" if rehearsal_time else ""
            leader_alias = preferred_user_alias(user)
            enqueue_notification_if_missing(
                db,
                user_id=participant_user.id,
                from_user_id=user.id,
                source_card_id=card.id,
                message=(
                    f"Руководитель @{leader_alias} предложил репетицию по «{card.character_name}» "
                    f"на {readable_date}{readable_time}."
                ),
            )
            busy_items = user_busy_items_on_date(
                db,
                user_id=participant_user.id,
                target_date=rehearsal_date,
            )
            if busy_items:
                participant_alias = preferred_user_alias(participant_user)
                conflicts_text = "; ".join(busy_items)
                enqueue_notification_if_missing(
                    db,
                    user_id=user.id,
                    from_user_id=participant_user.id,
                    source_card_id=card.id,
                    message=(
                        f"У участника @{participant_alias} конфликт на {readable_date} для предложенной репетиции "
                        f"по «{card.character_name}»: {conflicts_text}."
                    ),
                )
                enqueue_notification_if_missing(
                    db,
                    user_id=participant_user.id,
                    from_user_id=user.id,
                    source_card_id=card.id,
                    message=(
                        f"@{leader_alias} предложил(а) репетицию по «{card.character_name}» на {readable_date}, "
                        f"но у вас конфликт: {conflicts_text}."
                    ),
                )
        existing_progress = db.execute(
            select(InProgressCard).where(
                InProgressCard.user_id == card.user_id,
                InProgressCard.cosplan_card_id == card.id,
            )
        ).scalar_one_or_none()
        if not existing_progress:
            db.add(
                InProgressCard(
                    user_id=card.user_id,
                    cosplan_card_id=card.id,
                    checklist_json=[],
                    task_rows_json=[],
                )
            )
        db.add(
            RehearsalEntry(
                rehearsal_card_id=rehearsal_card.id,
                user_id=card.user_id,
                cosplan_card_id=card.id,
                proposed_by_user_id=user.id,
                source_type=REHEARSAL_SOURCE_LEADER,
                status=REHEARSAL_STATUS_PROPOSED,
                entry_date=rehearsal_date,
                entry_time=rehearsal_time,
            )
        )
        created += 1

    db.commit()
    add_flash(request, f"Предложение репетиции отправлено для карточек: {created}.", "success")
    return redirect("/my-projects")


@app.post("/my-projects/rehearsals/{entry_id}/decision")
async def my_projects_rehearsal_decision(entry_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    form = await request.form()
    decision = str(form.get("decision", "")).strip().lower()
    if decision not in {"approve", "reject"}:
        add_flash(request, "Некорректное действие.", "error")
        return redirect("/my-projects")

    entry = db.get(RehearsalEntry, entry_id)
    if not entry or entry.source_type != REHEARSAL_SOURCE_PARTICIPANT:
        add_flash(request, "Запись репетиции не найдена.", "error")
        return redirect("/my-projects")

    card = db.get(CosplanCard, entry.cosplan_card_id)
    if not card or not can_manage_project_card(user, card):
        add_flash(request, "Недостаточно прав для изменения статуса.", "error")
        return redirect("/my-projects")

    if decision == "approve":
        entry.status = REHEARSAL_STATUS_APPROVED
        db.commit()
        add_flash(request, "Репетиция одобрена и добавлена участнику в календарь.", "success")
    else:
        db.delete(entry)
        db.commit()
        add_flash(request, "Репетиция отклонена и удалена из предложений.", "info")
    return redirect("/my-projects")


@app.get("/my-calendar", response_class=HTMLResponse)
def my_calendar(request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    today = date.today()
    festivals = db.execute(
        select(Festival).where(
            Festival.user_id == user.id,
            Festival.is_going.is_(True),
            Festival.event_date.is_not(None),
        )
    ).scalars().all()
    festivals = [festival for festival in festivals if festival_is_active(festival, today)]
    cards = db.execute(
        select(CosplanCard).where(
            CosplanCard.user_id == user.id,
            CosplanCard.photoset_date.is_not(None),
            CosplanCard.photoset_date >= today,
        )
    ).scalars().all()
    rehearsal_entries = db.execute(
        select(RehearsalEntry)
        .where(
            RehearsalEntry.user_id == user.id,
            RehearsalEntry.entry_date.is_not(None),
            RehearsalEntry.entry_date >= today,
            or_(
                and_(
                    RehearsalEntry.source_type == REHEARSAL_SOURCE_PARTICIPANT,
                    RehearsalEntry.status == REHEARSAL_STATUS_APPROVED,
                ),
                and_(
                    RehearsalEntry.source_type == REHEARSAL_SOURCE_LEADER,
                    RehearsalEntry.status == REHEARSAL_STATUS_ACCEPTED,
                ),
            ),
        )
        .order_by(RehearsalEntry.entry_date, RehearsalEntry.entry_time, RehearsalEntry.id)
    ).scalars().all()
    personal_events = db.execute(
        select(PersonalCalendarEvent)
        .where(
            PersonalCalendarEvent.user_id == user.id,
            PersonalCalendarEvent.event_date.is_not(None),
            PersonalCalendarEvent.event_date >= today,
        )
        .order_by(PersonalCalendarEvent.event_date, PersonalCalendarEvent.event_time, PersonalCalendarEvent.id)
    ).scalars().all()
    alias_to_username, users_by_username, _ = build_user_alias_lookup(db)

    entries: list[dict[str, Any]] = []
    for festival in festivals:
        coproplayers_display = format_coproplayer_names(
            as_list(festival.going_coproplayers_json),
            alias_to_username,
            users_by_username,
        )
        for festival_day in iter_date_range(festival.event_date, festival.event_end_date):
            entries.append(
                {
                    "date": festival_day,
                    "time": "",
                    "kind": "Фестиваль (Я иду)",
                    "type_key": "festival",
                    "title": festival.name or "Без названия",
                    "city": festival.city or "—",
                    "coproplayers": ", ".join(coproplayers_display),
                    "details": "",
                    "personal_event_id": None,
                }
            )
    for card in cards:
        card_coproplayers = as_list(card.coproplayers_json) or as_list(card.coproplayer_nicks_json)
        coproplayers_display = format_coproplayer_names(
            card_coproplayers,
            alias_to_username,
            users_by_username,
        )
        entries.append(
            {
                "date": card.photoset_date,
                "time": "",
                "kind": "Фотосет",
                "type_key": "photoset",
                "title": card.character_name or "Без карточки",
                "city": card.city or "—",
                "coproplayers": ", ".join(coproplayers_display),
                "details": "",
                "personal_event_id": None,
            }
        )
    for entry in rehearsal_entries:
        card = entry.cosplan_card
        if not card:
            continue
        entries.append(
            {
                "date": entry.entry_date,
                "time": entry.entry_time or "",
                "kind": "Репетиция",
                "type_key": "rehearsal",
                "title": card.character_name or "Без карточки",
                "city": card.city or "—",
                "coproplayers": "",
                "details": "",
                "personal_event_id": None,
            }
        )
    for event in personal_events:
        entries.append(
            {
                "date": event.event_date,
                "time": event.event_time or "",
                "kind": "Личное событие",
                "type_key": "personal",
                "title": event.title or "Без названия",
                "city": "—",
                "coproplayers": "",
                "details": event.details or "",
                "personal_event_id": event.id,
            }
        )

    entries.sort(key=lambda item: (item["date"], item.get("time", ""), item["kind"], item["title"]))
    date_counts: dict[date, int] = defaultdict(int)
    for entry in entries:
        entry_date = entry.get("date")
        if isinstance(entry_date, date):
            date_counts[entry_date] += 1
    for entry in entries:
        entry_date = entry.get("date")
        entry["same_date_count"] = date_counts.get(entry_date, 0) if isinstance(entry_date, date) else 0
        entry["same_date_highlight"] = bool(entry.get("same_date_count", 0) > 1)

    grouped: list[dict[str, Any]] = []
    by_month: dict[tuple[int, int], list[dict[str, Any]]] = defaultdict(list)
    for entry in entries:
        event_date = entry["date"]
        if not isinstance(event_date, date):
            continue
        by_month[(event_date.year, event_date.month)].append(entry)

    for year_month in sorted(by_month.keys()):
        year, month = year_month
        month_date = date(year, month, 1)
        grouped.append(
            {
                "title": month_label_ru(month_date),
                "rows": by_month[year_month],
                "grid_weeks": month_calendar_grid(year, month, by_month[year_month]),
            }
        )

    return template_response(
        request,
        "my_calendar.html",
        user=user,
        active_tab="my-calendar",
        month_groups=grouped,
        month_weekday_labels=["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"],
    )


@app.post("/my-calendar/events/new")
async def my_calendar_event_create(request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    form = await request.form()
    event_date = parse_date(str(form.get("event_date", "")).strip())
    event_time = parse_time_hhmm(str(form.get("event_time", "")))
    event_title = str(form.get("event_title", "")).strip()
    event_details = str(form.get("event_details", "")).strip()

    if not event_date:
        add_flash(request, "Укажите дату события.", "error")
        return redirect("/my-calendar")
    if not event_title:
        add_flash(request, "Укажите название события.", "error")
        return redirect("/my-calendar")

    db.add(
        PersonalCalendarEvent(
            user_id=user.id,
            event_date=event_date,
            event_time=event_time,
            title=event_title,
            details=event_details or None,
        )
    )
    db.commit()
    add_flash(request, "Событие добавлено в календарь.", "success")
    return redirect("/my-calendar")


@app.post("/my-calendar/events/{event_id}/delete")
def my_calendar_event_delete(event_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    event = db.execute(
        select(PersonalCalendarEvent).where(
            PersonalCalendarEvent.id == event_id,
            PersonalCalendarEvent.user_id == user.id,
        )
    ).scalar_one_or_none()
    if not event:
        add_flash(request, "Событие не найдено.", "error")
        return redirect("/my-calendar")

    db.delete(event)
    db.commit()
    add_flash(request, "Событие удалено из календаря.", "info")
    return redirect("/my-calendar")


def project_board_fandom_options(db: Session, user: User) -> list[str]:
    global_fandoms = db.execute(
        select(ProjectSearchPost.fandom).where(ProjectSearchPost.fandom.is_not(None)).order_by(ProjectSearchPost.fandom)
    ).scalars().all()
    return merge_unique(global_fandoms, get_options(db, user.id, "fandom"))


def save_project_search_post_from_form(form: Any, post: ProjectSearchPost) -> tuple[bool, str]:
    fandom = str(form.get("fandom", "")).strip()
    event_date = parse_date(str(form.get("event_date", "")))
    event_type = str(form.get("event_type", "")).strip()
    status = str(form.get("status", PROJECT_BOARD_STATUS_ACTIVE)).strip()
    comment = str(form.get("comment", "")).strip() or None
    contact_nick = normalize_username(str(form.get("contact_nick", "")).strip())
    contact_link = str(form.get("contact_link", "")).strip() or None

    if not fandom:
        return False, "Укажите фандом."
    if not event_date:
        return False, "Укажите дату."
    if event_type not in {"photoset", "festival"}:
        return False, "Выберите тип: фотосет или фестиваль."
    if not contact_nick:
        return False, "Укажите ник человека."
    if status not in {
        PROJECT_BOARD_STATUS_ACTIVE,
        PROJECT_BOARD_STATUS_FOUND,
        PROJECT_BOARD_STATUS_INACTIVE,
    }:
        status = PROJECT_BOARD_STATUS_ACTIVE

    post.fandom = fandom
    post.event_date = event_date
    post.event_type = event_type
    post.status = status
    post.comment = comment
    post.contact_nick = contact_nick
    post.contact_link = contact_link
    return True, ""


@app.get("/project-board", response_class=HTMLResponse)
def project_board_list(request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    q = request.query_params.get("q", "").strip()
    only_mine = to_bool(request.query_params.get("mine", ""))

    all_posts = db.execute(
        select(ProjectSearchPost).order_by(
            ProjectSearchPost.event_date.is_(None),
            ProjectSearchPost.event_date,
            ProjectSearchPost.created_at.desc(),
        )
    ).scalars().all()
    posts = list(all_posts)
    if only_mine:
        posts = [post for post in posts if post.user_id == user.id]
    if q:
        needle = q.casefold()
        posts = [
            post
            for post in posts
            if needle in (post.fandom or "").casefold()
            or needle in (post.comment or "").casefold()
            or needle in (post.contact_nick or "").casefold()
            or needle in (post.contact_link or "").casefold()
            or needle in ("фотосет" if post.event_type == "photoset" else "фестиваль")
        ]

    owner_ids = {post.user_id for post in posts}
    owners_by_id: dict[int, User] = {}
    if owner_ids:
        owners = db.execute(select(User).where(User.id.in_(owner_ids))).scalars().all()
        owners_by_id = {owner.id: owner for owner in owners}

    return template_response(
        request,
        "project_board_list.html",
        user=user,
        active_tab="community",
        community_tab="project-board",
        posts=posts,
        owners_by_id=owners_by_id,
        q=q,
        only_mine=only_mine,
        board_status_labels={
            PROJECT_BOARD_STATUS_ACTIVE: project_board_status_label(PROJECT_BOARD_STATUS_ACTIVE),
            PROJECT_BOARD_STATUS_FOUND: project_board_status_label(PROJECT_BOARD_STATUS_FOUND),
            PROJECT_BOARD_STATUS_INACTIVE: project_board_status_label(PROJECT_BOARD_STATUS_INACTIVE),
        },
    )


@app.get("/project-board/new", response_class=HTMLResponse)
def project_board_new(request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    return template_response(
        request,
        "project_board_form.html",
        user=user,
        active_tab="community",
        community_tab="project-board",
        editing=False,
        post_id=None,
        form=get_project_search_post_form_values(user=user),
        fandom_options=project_board_fandom_options(db, user),
    )


@app.post("/project-board/new")
async def project_board_create(request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    form = await request.form()
    post = ProjectSearchPost(user_id=user.id, fandom="")
    ok, error_text = save_project_search_post_from_form(form, post)
    if not ok:
        add_flash(request, error_text, "error")
        return redirect("/project-board/new")

    db.add(post)
    remember_options(db, user.id, "fandom", [post.fandom])
    db.commit()
    add_flash(request, "Объявление добавлено.", "success")
    return redirect("/project-board")


@app.get("/project-board/{post_id}/edit", response_class=HTMLResponse)
def project_board_edit(post_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    post = db.get(ProjectSearchPost, post_id)
    if not post:
        add_flash(request, "Объявление не найдено.", "error")
        return redirect("/project-board")
    if post.user_id != user.id:
        add_flash(request, "Редактировать можно только своё объявление.", "error")
        return redirect("/project-board")

    return template_response(
        request,
        "project_board_form.html",
        user=user,
        active_tab="community",
        community_tab="project-board",
        editing=True,
        post_id=post.id,
        form=get_project_search_post_form_values(post, user),
        fandom_options=project_board_fandom_options(db, user),
    )


@app.post("/project-board/{post_id}/edit")
async def project_board_update(post_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    post = db.get(ProjectSearchPost, post_id)
    if not post:
        add_flash(request, "Объявление не найдено.", "error")
        return redirect("/project-board")
    if post.user_id != user.id:
        add_flash(request, "Редактировать можно только своё объявление.", "error")
        return redirect("/project-board")

    form = await request.form()
    ok, error_text = save_project_search_post_from_form(form, post)
    if not ok:
        add_flash(request, error_text, "error")
        return redirect(f"/project-board/{post_id}/edit")

    remember_options(db, user.id, "fandom", [post.fandom])
    db.commit()
    add_flash(request, "Объявление обновлено.", "success")
    return redirect("/project-board")


@app.post("/project-board/{post_id}/status")
async def project_board_update_status(post_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    post = db.get(ProjectSearchPost, post_id)
    if not post:
        add_flash(request, "Объявление не найдено.", "error")
        return redirect("/project-board")
    if post.user_id != user.id:
        add_flash(request, "Изменять статус может только автор карточки.", "error")
        return redirect("/project-board")

    form = await request.form()
    status = str(form.get("status", "")).strip()
    if status not in {
        PROJECT_BOARD_STATUS_ACTIVE,
        PROJECT_BOARD_STATUS_FOUND,
        PROJECT_BOARD_STATUS_INACTIVE,
    }:
        add_flash(request, "Некорректный статус.", "error")
        return redirect("/project-board")

    post.status = status
    db.commit()
    add_flash(request, "Статус объявления обновлен.", "success")
    return redirect("/project-board")


@app.get("/community")
def community_index(request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")
    return redirect("/project-board")


def get_question_form_values(question: CommunityQuestion | None = None) -> dict[str, Any]:
    if not question:
        return {
            "title": "",
            "body": "",
            "status": QUESTION_STATUS_OPEN,
        }
    return {
        "title": question.title or "",
        "body": question.body or "",
        "status": question.status or QUESTION_STATUS_OPEN,
    }


def save_question_from_form(form: Any, question: CommunityQuestion) -> tuple[bool, str]:
    title = str(form.get("title", "")).strip()
    body = str(form.get("body", "")).strip()
    status = str(form.get("status", QUESTION_STATUS_OPEN)).strip()

    if not title:
        return False, "Укажите заголовок вопроса."
    if not body:
        return False, "Введите текст вопроса."
    if len(body) > 6000:
        return False, "Текст вопроса слишком длинный (до 6000 символов)."
    if status not in {QUESTION_STATUS_OPEN, QUESTION_STATUS_RESOLVED}:
        status = QUESTION_STATUS_OPEN

    question.title = title
    question.body = body
    question.status = status
    return True, ""


@app.get("/community/questions", response_class=HTMLResponse)
def community_questions_list(request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    q = request.query_params.get("q", "").strip()
    questions = db.execute(
        select(CommunityQuestion).order_by(CommunityQuestion.updated_at.desc(), CommunityQuestion.id.desc())
    ).scalars().all()
    if q:
        needle = q.casefold()
        questions = [
            item
            for item in questions
            if needle in (item.title or "").casefold() or needle in (item.body or "").casefold()
        ]

    owner_ids = {item.user_id for item in questions}
    owners_by_id: dict[int, User] = {}
    if owner_ids:
        owners = db.execute(select(User).where(User.id.in_(owner_ids))).scalars().all()
        owners_by_id = {item.id: item for item in owners}

    comments_counts_raw = db.execute(
        select(CommunityQuestionComment.question_id, func.count(CommunityQuestionComment.id))
        .group_by(CommunityQuestionComment.question_id)
    ).all()
    comment_counts = {int(row[0]): int(row[1]) for row in comments_counts_raw}

    return template_response(
        request,
        "community_questions_list.html",
        user=user,
        active_tab="community",
        community_tab="questions",
        questions=questions,
        owners_by_id=owners_by_id,
        comment_counts=comment_counts,
        q=q,
        question_status_labels={
            QUESTION_STATUS_OPEN: question_status_label(QUESTION_STATUS_OPEN),
            QUESTION_STATUS_RESOLVED: question_status_label(QUESTION_STATUS_RESOLVED),
        },
    )


@app.get("/community/questions/new", response_class=HTMLResponse)
def community_questions_new(request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")
    return template_response(
        request,
        "community_question_form.html",
        user=user,
        active_tab="community",
        community_tab="questions",
        editing=False,
        question_id=None,
        form=get_question_form_values(),
        question_status_labels={
            QUESTION_STATUS_OPEN: question_status_label(QUESTION_STATUS_OPEN),
            QUESTION_STATUS_RESOLVED: question_status_label(QUESTION_STATUS_RESOLVED),
        },
    )


@app.post("/community/questions/new")
async def community_questions_create(request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")
    form = await request.form()
    question = CommunityQuestion(user_id=user.id, title="", body="")
    ok, error_text = save_question_from_form(form, question)
    if not ok:
        add_flash(request, error_text, "error")
        return redirect("/community/questions/new")

    db.add(question)
    db.commit()
    add_flash(request, "Вопрос опубликован.", "success")
    return redirect("/community/questions")


@app.get("/community/questions/{question_id}", response_class=HTMLResponse)
def community_questions_detail(question_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    question = db.get(CommunityQuestion, question_id)
    if not question:
        add_flash(request, "Вопрос не найден.", "error")
        return redirect("/community/questions")

    comments = db.execute(
        select(CommunityQuestionComment)
        .where(CommunityQuestionComment.question_id == question.id)
        .order_by(CommunityQuestionComment.created_at, CommunityQuestionComment.id)
    ).scalars().all()
    author_ids = {question.user_id, *(item.user_id for item in comments)}
    authors_by_id: dict[int, User] = {}
    if author_ids:
        authors = db.execute(select(User).where(User.id.in_(author_ids))).scalars().all()
        authors_by_id = {item.id: item for item in authors}

    return template_response(
        request,
        "community_question_detail.html",
        user=user,
        active_tab="community",
        community_tab="questions",
        question=question,
        comments=comments,
        authors_by_id=authors_by_id,
        question_status_labels={
            QUESTION_STATUS_OPEN: question_status_label(QUESTION_STATUS_OPEN),
            QUESTION_STATUS_RESOLVED: question_status_label(QUESTION_STATUS_RESOLVED),
        },
    )


@app.get("/community/questions/{question_id}/edit", response_class=HTMLResponse)
def community_questions_edit(question_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    question = db.get(CommunityQuestion, question_id)
    if not question:
        add_flash(request, "Вопрос не найден.", "error")
        return redirect("/community/questions")
    if question.user_id != user.id:
        add_flash(request, "Редактировать можно только свой вопрос.", "error")
        return redirect(f"/community/questions/{question_id}")

    return template_response(
        request,
        "community_question_form.html",
        user=user,
        active_tab="community",
        community_tab="questions",
        editing=True,
        question_id=question.id,
        form=get_question_form_values(question),
        question_status_labels={
            QUESTION_STATUS_OPEN: question_status_label(QUESTION_STATUS_OPEN),
            QUESTION_STATUS_RESOLVED: question_status_label(QUESTION_STATUS_RESOLVED),
        },
    )


@app.post("/community/questions/{question_id}/edit")
async def community_questions_update(question_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    question = db.get(CommunityQuestion, question_id)
    if not question:
        add_flash(request, "Вопрос не найден.", "error")
        return redirect("/community/questions")
    if question.user_id != user.id:
        add_flash(request, "Редактировать можно только свой вопрос.", "error")
        return redirect(f"/community/questions/{question_id}")

    form = await request.form()
    ok, error_text = save_question_from_form(form, question)
    if not ok:
        add_flash(request, error_text, "error")
        return redirect(f"/community/questions/{question_id}/edit")

    db.commit()
    add_flash(request, "Вопрос обновлен.", "success")
    return redirect(f"/community/questions/{question_id}")


@app.post("/community/questions/{question_id}/status")
async def community_questions_update_status(question_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    question = db.get(CommunityQuestion, question_id)
    if not question:
        add_flash(request, "Вопрос не найден.", "error")
        return redirect("/community/questions")
    if question.user_id != user.id:
        add_flash(request, "Изменять статус может только автор вопроса.", "error")
        return redirect(f"/community/questions/{question_id}")

    form = await request.form()
    status = str(form.get("status", "")).strip()
    if status not in {QUESTION_STATUS_OPEN, QUESTION_STATUS_RESOLVED}:
        add_flash(request, "Некорректный статус.", "error")
        return redirect(f"/community/questions/{question_id}")

    question.status = status
    db.commit()
    add_flash(request, "Статус вопроса обновлен.", "success")
    return redirect(f"/community/questions/{question_id}")


@app.post("/community/questions/{question_id}/comments")
async def community_questions_add_comment(question_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    question = db.get(CommunityQuestion, question_id)
    if not question:
        add_flash(request, "Вопрос не найден.", "error")
        return redirect("/community/questions")

    form = await request.form()
    body = str(form.get("body", "")).strip()
    if not body:
        add_flash(request, "Введите текст комментария.", "error")
        return redirect(f"/community/questions/{question_id}")

    db.add(CommunityQuestionComment(question_id=question.id, user_id=user.id, body=body))
    db.commit()
    add_flash(request, "Комментарий добавлен.", "success")
    return redirect(f"/community/questions/{question_id}")


def get_master_form_values(master: CommunityMaster | None = None) -> dict[str, Any]:
    if not master:
        return {
            "nick": "",
            "master_type": MASTER_TYPE_OPTIONS[0],
            "details": "",
            "gallery_input": "",
            "price_rows": [],
        }

    return {
        "nick": master.nick or "",
        "master_type": master.master_type or MASTER_TYPE_OPTIONS[0],
        "details": master.details or "",
        "gallery_input": "\n".join(as_list(master.gallery_json)),
        "price_rows": format_master_price_rows_for_form(as_list(master.price_list_json)),
    }


def save_master_from_form(form: Any, master: CommunityMaster) -> tuple[bool, str]:
    nick = normalize_username(str(form.get("nick", "")).strip())
    master_type = str(form.get("master_type", "")).strip().lower()
    details = str(form.get("details", "")).strip()
    gallery_input = str(form.get("gallery_input", ""))
    price_rows = parse_master_price_rows_from_form(form)

    if not nick:
        return False, "Укажите ник мастера."
    if master_type not in MASTER_TYPE_OPTIONS:
        return False, "Выберите корректный тип мастера."
    if not details:
        return False, "Заполните поле «Подробнее»."
    if len(details) > 2000:
        return False, "Поле «Подробнее» должно быть не длиннее 2000 символов."

    master.nick = nick
    master.master_type = master_type
    master.details = details
    master.gallery_json = parse_reference_values(gallery_input)
    master.price_list_json = price_rows
    return True, ""


@app.get("/community/masters", response_class=HTMLResponse)
def community_masters_list(request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    q = request.query_params.get("q", "").strip()
    master_type = request.query_params.get("type", "").strip().lower()
    masters = db.execute(
        select(CommunityMaster).order_by(CommunityMaster.updated_at.desc(), CommunityMaster.id.desc())
    ).scalars().all()

    if master_type and master_type in MASTER_TYPE_OPTIONS:
        masters = [item for item in masters if (item.master_type or "").strip().lower() == master_type]
    if q:
        needle = q.casefold()
        masters = [item for item in masters if needle in (item.nick or "").casefold()]

    owner_ids = {item.user_id for item in masters}
    owners_by_id: dict[int, User] = {}
    if owner_ids:
        owners = db.execute(select(User).where(User.id.in_(owner_ids))).scalars().all()
        owners_by_id = {item.id: item for item in owners}

    comments_counts_raw = db.execute(
        select(CommunityMasterComment.master_id, func.count(CommunityMasterComment.id))
        .group_by(CommunityMasterComment.master_id)
    ).all()
    comment_counts = {int(row[0]): int(row[1]) for row in comments_counts_raw}

    return template_response(
        request,
        "community_masters_list.html",
        user=user,
        active_tab="community",
        community_tab="masters",
        masters=masters,
        owners_by_id=owners_by_id,
        comment_counts=comment_counts,
        q=q,
        selected_type=master_type,
        master_type_options=MASTER_TYPE_OPTIONS,
    )


@app.get("/community/masters/new", response_class=HTMLResponse)
def community_masters_new(request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")
    return template_response(
        request,
        "community_master_form.html",
        user=user,
        active_tab="community",
        community_tab="masters",
        editing=False,
        master_id=None,
        form=get_master_form_values(),
        master_type_options=MASTER_TYPE_OPTIONS,
    )


@app.post("/community/masters/new")
async def community_masters_create(request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")
    form = await request.form()
    master = CommunityMaster(user_id=user.id, nick="", master_type=MASTER_TYPE_OPTIONS[0], details="")
    ok, error_text = save_master_from_form(form, master)
    if not ok:
        add_flash(request, error_text, "error")
        return redirect("/community/masters/new")

    db.add(master)
    db.commit()
    add_flash(request, "Карточка мастера опубликована.", "success")
    return redirect("/community/masters")


@app.get("/community/masters/{master_id}", response_class=HTMLResponse)
def community_masters_detail(master_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    master = db.get(CommunityMaster, master_id)
    if not master:
        add_flash(request, "Карточка мастера не найдена.", "error")
        return redirect("/community/masters")

    comments = db.execute(
        select(CommunityMasterComment)
        .where(CommunityMasterComment.master_id == master.id)
        .order_by(CommunityMasterComment.created_at, CommunityMasterComment.id)
    ).scalars().all()
    author_ids = {master.user_id, *(item.user_id for item in comments)}
    authors_by_id: dict[int, User] = {}
    if author_ids:
        authors = db.execute(select(User).where(User.id.in_(author_ids))).scalars().all()
        authors_by_id = {item.id: item for item in authors}

    return template_response(
        request,
        "community_master_detail.html",
        user=user,
        active_tab="community",
        community_tab="masters",
        master=master,
        comments=comments,
        authors_by_id=authors_by_id,
        price_rows=format_master_price_rows_for_form(as_list(master.price_list_json)),
    )


@app.get("/community/masters/{master_id}/edit", response_class=HTMLResponse)
def community_masters_edit(master_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    master = db.get(CommunityMaster, master_id)
    if not master:
        add_flash(request, "Карточка мастера не найдена.", "error")
        return redirect("/community/masters")
    if master.user_id != user.id:
        add_flash(request, "Редактировать можно только свою карточку мастера.", "error")
        return redirect(f"/community/masters/{master_id}")

    return template_response(
        request,
        "community_master_form.html",
        user=user,
        active_tab="community",
        community_tab="masters",
        editing=True,
        master_id=master.id,
        form=get_master_form_values(master),
        master_type_options=MASTER_TYPE_OPTIONS,
    )


@app.post("/community/masters/{master_id}/edit")
async def community_masters_update(master_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    master = db.get(CommunityMaster, master_id)
    if not master:
        add_flash(request, "Карточка мастера не найдена.", "error")
        return redirect("/community/masters")
    if master.user_id != user.id:
        add_flash(request, "Редактировать можно только свою карточку мастера.", "error")
        return redirect(f"/community/masters/{master_id}")

    form = await request.form()
    ok, error_text = save_master_from_form(form, master)
    if not ok:
        add_flash(request, error_text, "error")
        return redirect(f"/community/masters/{master_id}/edit")

    db.commit()
    add_flash(request, "Карточка мастера обновлена.", "success")
    return redirect(f"/community/masters/{master_id}")


@app.post("/community/masters/{master_id}/comments")
async def community_masters_add_comment(master_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    master = db.get(CommunityMaster, master_id)
    if not master:
        add_flash(request, "Карточка мастера не найдена.", "error")
        return redirect("/community/masters")

    form = await request.form()
    body = str(form.get("body", "")).strip()
    if not body:
        add_flash(request, "Введите текст комментария.", "error")
        return redirect(f"/community/masters/{master_id}")

    db.add(CommunityMasterComment(master_id=master.id, user_id=user.id, body=body))
    db.commit()
    add_flash(request, "Комментарий добавлен.", "success")
    return redirect(f"/community/masters/{master_id}")


@app.post("/community/masters/{master_id}/delete")
def community_masters_delete(master_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    master = db.get(CommunityMaster, master_id)
    if not master:
        add_flash(request, "Карточка мастера не найдена.", "error")
        return redirect("/community/masters")
    if master.user_id != user.id:
        add_flash(request, "Удалять можно только свою карточку мастера.", "error")
        return redirect(f"/community/masters/{master_id}")

    db.delete(master)
    db.commit()
    add_flash(request, "Карточка мастера удалена.", "info")
    return redirect("/community/masters")


def get_article_form_values(article: CommunityArticle | None = None, user: User | None = None) -> dict[str, Any]:
    if not article:
        return {
            "topic": "",
            "author_name": preferred_user_alias(user) if user else "",
            "body_markdown": "",
            "tags_input": "",
            "tags_json": [],
        }
    tags = as_list(article.tags_json)
    return {
        "topic": article.topic or "",
        "author_name": article.author_name or "",
        "body_markdown": article.body_markdown or "",
        "tags_input": ", ".join(tags),
        "tags_json": tags,
    }


def save_article_from_form(form: Any, article: CommunityArticle) -> tuple[bool, str]:
    topic = str(form.get("topic", "")).strip()
    author_name = str(form.get("author_name", "")).strip()
    body_markdown = str(form.get("body_markdown", "")).strip()
    tags_input = str(form.get("tags_input", "")).strip()
    raw_tags = merge_unique(split_csv(tags_input))
    if len(raw_tags) > ARTICLE_MAX_TAGS:
        return False, f"Можно указать не более {ARTICLE_MAX_TAGS} тегов."
    tags = parse_article_tags(tags_input)

    if not topic:
        return False, "Укажите тему статьи."
    if len(topic) > 255:
        return False, "Тема статьи слишком длинная (до 255 символов)."
    if not author_name:
        return False, "Укажите автора статьи."
    if len(author_name) > 120:
        return False, "Поле «Автор» слишком длинное (до 120 символов)."
    if not body_markdown:
        return False, "Заполните текст статьи."
    if len(body_markdown) > ARTICLE_MAX_BODY_LENGTH:
        return False, f"Статья должна быть не длиннее {ARTICLE_MAX_BODY_LENGTH} символов."

    article.topic = topic
    article.author_name = author_name
    article.body_markdown = body_markdown
    article.tags_json = tags
    return True, ""


def build_authority_map(db: Session) -> dict[int, bool]:
    rows = db.execute(
        select(
            CommunityArticle.user_id,
            func.count(CommunityArticleFavorite.id),
        )
        .select_from(CommunityArticle)
        .join(CommunityArticleFavorite, CommunityArticleFavorite.article_id == CommunityArticle.id, isouter=True)
        .group_by(CommunityArticle.user_id)
    ).all()
    return {int(user_id): int(count or 0) > 50 for user_id, count in rows}


@app.get("/community/articles", response_class=HTMLResponse)
def community_articles_list(request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    q = request.query_params.get("q", "").strip()
    only_favorites = to_bool(request.query_params.get("favorites", ""))

    articles = db.execute(
        select(CommunityArticle).order_by(CommunityArticle.updated_at.desc(), CommunityArticle.id.desc())
    ).scalars().all()

    if q:
        needle = q.casefold()
        articles = [
            item
            for item in articles
            if needle in (item.topic or "").casefold()
            or any(needle in tag.casefold() for tag in as_list(item.tags_json))
        ]

    favorite_article_ids = set(
        db.execute(
            select(CommunityArticleFavorite.article_id).where(CommunityArticleFavorite.user_id == user.id)
        ).scalars().all()
    )
    if only_favorites:
        articles = [item for item in articles if item.id in favorite_article_ids]

    article_ids = [item.id for item in articles]
    owner_ids = {item.user_id for item in articles}
    owners_by_id: dict[int, User] = {}
    if owner_ids:
        owners = db.execute(select(User).where(User.id.in_(owner_ids))).scalars().all()
        owners_by_id = {item.id: item for item in owners}

    comment_counts: dict[int, int] = {}
    favorite_counts: dict[int, int] = {}
    if article_ids:
        comment_counts = {
            int(article_id): int(count or 0)
            for article_id, count in db.execute(
                select(CommunityArticleComment.article_id, func.count(CommunityArticleComment.id))
                .where(CommunityArticleComment.article_id.in_(article_ids))
                .group_by(CommunityArticleComment.article_id)
            ).all()
        }
        favorite_counts = {
            int(article_id): int(count or 0)
            for article_id, count in db.execute(
                select(CommunityArticleFavorite.article_id, func.count(CommunityArticleFavorite.id))
                .where(CommunityArticleFavorite.article_id.in_(article_ids))
                .group_by(CommunityArticleFavorite.article_id)
            ).all()
        }

    return template_response(
        request,
        "community_articles_list.html",
        user=user,
        active_tab="community",
        community_tab="articles",
        articles=articles,
        owners_by_id=owners_by_id,
        q=q,
        only_favorites=only_favorites,
        favorite_article_ids=favorite_article_ids,
        favorite_counts=favorite_counts,
        comment_counts=comment_counts,
        author_is_authority=build_authority_map(db),
    )


@app.get("/community/articles/new", response_class=HTMLResponse)
def community_articles_new(request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")
    return template_response(
        request,
        "community_article_form.html",
        user=user,
        active_tab="community",
        community_tab="articles",
        editing=False,
        article_id=None,
        form=get_article_form_values(user=user),
    )


@app.post("/community/articles/new")
async def community_articles_create(request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    form = await request.form()
    article = CommunityArticle(user_id=user.id, topic="", author_name="", body_markdown="")
    ok, error_text = save_article_from_form(form, article)
    if not ok:
        add_flash(request, error_text, "error")
        return redirect("/community/articles/new")

    db.add(article)
    db.commit()
    add_flash(request, "Статья опубликована.", "success")
    return redirect(f"/community/articles/{article.id}")


@app.get("/community/articles/{article_id}", response_class=HTMLResponse)
def community_articles_detail(article_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    article = db.get(CommunityArticle, article_id)
    if not article:
        add_flash(request, "Статья не найдена.", "error")
        return redirect("/community/articles")

    comments = db.execute(
        select(CommunityArticleComment)
        .where(CommunityArticleComment.article_id == article.id)
        .order_by(CommunityArticleComment.created_at, CommunityArticleComment.id)
    ).scalars().all()
    author_ids = {article.user_id, *(item.user_id for item in comments)}
    authors_by_id: dict[int, User] = {}
    if author_ids:
        authors = db.execute(select(User).where(User.id.in_(author_ids))).scalars().all()
        authors_by_id = {item.id: item for item in authors}

    favorite_count = int(
        db.execute(
            select(func.count(CommunityArticleFavorite.id)).where(CommunityArticleFavorite.article_id == article.id)
        ).scalar()
        or 0
    )
    is_favorite = bool(
        db.execute(
            select(CommunityArticleFavorite.id).where(
                CommunityArticleFavorite.article_id == article.id,
                CommunityArticleFavorite.user_id == user.id,
            )
        ).scalar_one_or_none()
    )

    return template_response(
        request,
        "community_article_detail.html",
        user=user,
        active_tab="community",
        community_tab="articles",
        article=article,
        comments=comments,
        authors_by_id=authors_by_id,
        article_html=render_article_markdown(article.body_markdown or ""),
        favorite_count=favorite_count,
        is_favorite=is_favorite,
        author_is_authority=build_authority_map(db),
    )


@app.get("/community/articles/{article_id}/edit", response_class=HTMLResponse)
def community_articles_edit(article_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    article = db.get(CommunityArticle, article_id)
    if not article:
        add_flash(request, "Статья не найдена.", "error")
        return redirect("/community/articles")
    if article.user_id != user.id:
        add_flash(request, "Редактировать можно только свою статью.", "error")
        return redirect(f"/community/articles/{article_id}")

    return template_response(
        request,
        "community_article_form.html",
        user=user,
        active_tab="community",
        community_tab="articles",
        editing=True,
        article_id=article.id,
        form=get_article_form_values(article=article),
    )


@app.post("/community/articles/{article_id}/edit")
async def community_articles_update(article_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    article = db.get(CommunityArticle, article_id)
    if not article:
        add_flash(request, "Статья не найдена.", "error")
        return redirect("/community/articles")
    if article.user_id != user.id:
        add_flash(request, "Редактировать можно только свою статью.", "error")
        return redirect(f"/community/articles/{article_id}")

    form = await request.form()
    ok, error_text = save_article_from_form(form, article)
    if not ok:
        add_flash(request, error_text, "error")
        return redirect(f"/community/articles/{article_id}/edit")

    db.commit()
    add_flash(request, "Статья обновлена.", "success")
    return redirect(f"/community/articles/{article_id}")


@app.post("/community/articles/{article_id}/delete")
def community_articles_delete(article_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    article = db.get(CommunityArticle, article_id)
    if not article:
        add_flash(request, "Статья не найдена.", "error")
        return redirect("/community/articles")
    if article.user_id != user.id:
        add_flash(request, "Удалять можно только свою статью.", "error")
        return redirect(f"/community/articles/{article_id}")

    db.delete(article)
    db.commit()
    add_flash(request, "Статья удалена.", "info")
    return redirect("/community/articles")


@app.post("/community/articles/{article_id}/favorite")
async def community_articles_toggle_favorite(article_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    article = db.get(CommunityArticle, article_id)
    if not article:
        add_flash(request, "Статья не найдена.", "error")
        return redirect("/community/articles")

    existing = db.execute(
        select(CommunityArticleFavorite).where(
            CommunityArticleFavorite.article_id == article.id,
            CommunityArticleFavorite.user_id == user.id,
        )
    ).scalar_one_or_none()
    if existing:
        db.delete(existing)
    else:
        db.add(CommunityArticleFavorite(article_id=article.id, user_id=user.id))
    db.commit()

    form = await request.form()
    next_url = safe_redirect_target(str(form.get("next", "")).strip(), f"/community/articles/{article.id}")
    return redirect(next_url)


@app.post("/community/articles/{article_id}/comments")
async def community_articles_add_comment(article_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    article = db.get(CommunityArticle, article_id)
    if not article:
        add_flash(request, "Статья не найдена.", "error")
        return redirect("/community/articles")

    form = await request.form()
    body = str(form.get("body", "")).strip()
    if not body:
        add_flash(request, "Введите текст комментария.", "error")
        return redirect(f"/community/articles/{article_id}")

    db.add(CommunityArticleComment(article_id=article.id, user_id=user.id, body=body))
    db.commit()
    add_flash(request, "Комментарий добавлен.", "success")
    return redirect(f"/community/articles/{article_id}")


@app.get("/festivals", response_class=HTMLResponse)
def festivals_list(request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    city_filter = request.query_params.get("city", "").strip()
    city_filter_values = split_city_values(city_filter)
    nomination_filter = request.query_params.get("nomination", "").strip()
    coproplayer_filter = request.query_params.get("coproplayer", "").strip()
    only_going = to_bool(request.query_params.get("only_going", ""))

    festivals = db.execute(
        select(Festival).where(Festival.user_id == user.id).order_by(Festival.event_date.is_(None), Festival.event_date, Festival.name)
    ).scalars().all()

    today = date.today()
    active_festivals = [
        festival
        for festival in festivals
        if festival_is_active(festival, today)
    ]

    alias_to_username, users_by_username, alias_options = build_user_alias_lookup(db)
    festival_coproplayers_display: dict[int, list[str]] = {}

    filtered: list[Festival] = []
    for festival in active_festivals:
        raw_coproplayers = as_list(festival.going_coproplayers_json)
        display_coproplayers = format_coproplayer_names(raw_coproplayers, alias_to_username, users_by_username)
        festival_coproplayers_display[festival.id] = display_coproplayers

        if only_going and not festival.is_going:
            continue

        if city_filter_values and not city_matches_any(city_filter_values, festival.city):
            continue

        nominations = [festival.nomination_1 or "", festival.nomination_2 or "", festival.nomination_3 or ""]
        if nomination_filter and not any(nomination_filter.casefold() in value.casefold() for value in nominations if value):
            continue

        coproplayer_search_targets = merge_unique(
            raw_coproplayers,
            [value.lstrip("@") for value in display_coproplayers],
        )
        if coproplayer_filter and not any(
            coproplayer_filter.casefold() in value.casefold() for value in coproplayer_search_targets
        ):
            continue

        filtered.append(festival)

    home_city_value = user.home_city or ""
    home_city_values = split_city_values(home_city_value)
    nearest_city_keys = nearest_big_city_keys_for_home_cities(home_city_values)
    nearest_city_labels = nearest_big_city_labels(nearest_city_keys)

    home_city_festival_ids: set[int] = set()
    if home_city_values:
        home_city_festival_ids = {
            festival.id
            for festival in filtered
            if city_matches_any(home_city_values, festival.city)
        }
    nearest_city_festival_ids: set[int] = set()
    if nearest_city_keys:
        nearest_city_festival_ids = {
            festival.id
            for festival in filtered
            if festival.id not in home_city_festival_ids
            and any(city_matches(city_key, festival.city) for city_key in nearest_city_keys)
        }

    own_cards = db.execute(
        select(CosplanCard).where(CosplanCard.user_id == user.id, CosplanCard.is_shared_copy.is_(False))
    ).scalars().all()
    shared_cards = db.execute(
        select(CosplanCard).where(CosplanCard.user_id == user.id, CosplanCard.is_shared_copy.is_(True))
    ).scalars().all()

    planned_festival_names = {
        name.casefold()
        for card in own_cards
        for name in as_list(card.planned_festivals_json)
        if name
    }
    shared_planned_festival_names = {
        name.casefold()
        for card in shared_cards
        for name in as_list(card.planned_festivals_json)
        if name
    }

    month_limit = date.today() + timedelta(days=30)
    summary_rows: list[dict[str, Any]] = []
    for festival in active_festivals:
        is_home_city = city_matches_any(home_city_values, festival.city)
        is_nearest_city = (not is_home_city) and any(
            city_matches(city_key, festival.city) for city_key in nearest_city_keys
        )
        festival_end_date = festival_range_end(festival)
        if (
            festival.event_date
            and festival_end_date
            and festival_end_date >= today
            and festival.event_date <= month_limit
        ):
            summary_rows.append(
                {
                    "kind": "Событие",
                    "festival": festival,
                    "date": festival.event_date if festival.event_date >= today else today,
                    "is_home_city": is_home_city,
                    "is_nearest_city": is_nearest_city,
                }
            )
        if festival.submission_deadline and today <= festival.submission_deadline <= month_limit:
            summary_rows.append(
                {
                    "kind": "Дедлайн подачи",
                    "festival": festival,
                    "date": festival.submission_deadline,
                    "is_home_city": is_home_city,
                    "is_nearest_city": is_nearest_city,
                }
            )

    summary_rows.sort(key=lambda item: item["date"])

    city_options = merge_unique([festival.city for festival in active_festivals if festival.city])
    nomination_options = merge_unique(
        DEFAULT_NOMINATIONS,
        [festival.nomination_1 or "" for festival in active_festivals],
        [festival.nomination_2 or "" for festival in active_festivals],
        [festival.nomination_3 or "" for festival in active_festivals],
        get_options(db, user.id, "nomination"),
    )
    coproplayer_options = merge_unique(
        [value for festival in active_festivals for value in as_list(festival.going_coproplayers_json)],
        alias_options,
        get_options(db, user.id, "coproplayer"),
    )

    show_summary = not any([city_filter, nomination_filter, coproplayer_filter, only_going])

    moderator_announcements: list[FestivalAnnouncement] = []
    if is_moderator_user(user):
        moderator_announcements = db.execute(
            select(FestivalAnnouncement)
            .where(FestivalAnnouncement.status == ANNOUNCEMENT_STATUS_PENDING)
            .order_by(FestivalAnnouncement.created_at.desc(), FestivalAnnouncement.id.desc())
        ).scalars().all()

    own_announcements = db.execute(
        select(FestivalAnnouncement)
        .where(FestivalAnnouncement.requester_user_id == user.id)
        .order_by(FestivalAnnouncement.created_at.desc(), FestivalAnnouncement.id.desc())
        .limit(25)
    ).scalars().all()

    announcement_user_ids = {
        int(item.requester_user_id)
        for item in moderator_announcements + own_announcements
        if item.requester_user_id
    }
    announcement_requesters_by_id: dict[int, User] = {}
    if announcement_user_ids:
        requesters = db.execute(select(User).where(User.id.in_(announcement_user_ids))).scalars().all()
        announcement_requesters_by_id = {item.id: item for item in requesters}

    return template_response(
        request,
        "festivals_list.html",
        user=user,
        active_tab="festivals",
        festivals=filtered,
        planned_festival_names=planned_festival_names,
        shared_planned_festival_names=shared_planned_festival_names,
        city_filter=city_filter,
        nomination_filter=nomination_filter,
        coproplayer_filter=coproplayer_filter,
        only_going=only_going,
        city_options=city_options,
        nomination_options=nomination_options,
        coproplayer_options=coproplayer_options,
        festival_coproplayers_display=festival_coproplayers_display,
        show_summary=show_summary,
        summary_rows=summary_rows,
        user_home_city=user.home_city or "",
        user_home_cities=home_city_values,
        home_city_festival_ids=home_city_festival_ids,
        nearest_city_festival_ids=nearest_city_festival_ids,
        nearest_city_labels=nearest_city_labels,
        current_query=request.url.query or "",
        moderator_announcements=moderator_announcements,
        own_announcements=own_announcements,
        announcement_requesters_by_id=announcement_requesters_by_id,
        announcement_status_labels={
            ANNOUNCEMENT_STATUS_PENDING: announcement_status_label(ANNOUNCEMENT_STATUS_PENDING),
            ANNOUNCEMENT_STATUS_APPROVED: announcement_status_label(ANNOUNCEMENT_STATUS_APPROVED),
            ANNOUNCEMENT_STATUS_REJECTED: announcement_status_label(ANNOUNCEMENT_STATUS_REJECTED),
        },
    )


@app.post("/festivals/notifications/mark-read")
async def festivals_notifications_mark_read(request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")
    form = await request.form()
    next_url = safe_redirect_target(str(form.get("next", "")).strip(), "/festivals")
    db.execute(
        text("UPDATE festival_notifications SET is_read = 1 WHERE user_id = :user_id"),
        {"user_id": user.id},
    )
    db.commit()
    add_flash(request, "Уведомления отмечены как прочитанные.", "success")
    return redirect(next_url)


@app.post("/notifications/pigeon")
async def notifications_send_pigeon(request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    form = await request.form()
    recipient_alias_raw = str(form.get("recipient_alias", "")).strip()
    message_body = str(form.get("message", "")).strip()
    next_url = safe_redirect_target(str(form.get("next", "")).strip(), "/")

    if not recipient_alias_raw:
        add_flash(request, "Укажите ник получателя.", "error")
        return redirect(next_url)
    if not message_body:
        add_flash(request, "Введите текст сообщения.", "error")
        return redirect(next_url)
    if len(message_body) > 1500:
        add_flash(request, "Сообщение слишком длинное (максимум 1500 символов).", "error")
        return redirect(next_url)

    alias_to_username, users_by_username, _ = build_user_alias_lookup(db)
    canonical_username = resolve_alias_to_username(recipient_alias_raw, alias_to_username)
    recipient = users_by_username.get(canonical_username.casefold())
    if not recipient:
        add_flash(request, "Пользователь с таким ником не найден.", "error")
        return redirect(next_url)
    if recipient.id == user.id:
        add_flash(request, "Нельзя отправить голубя самому себе.", "error")
        return redirect(next_url)

    sender_alias = preferred_user_alias(user)
    payload = f"Курлык! (@{sender_alias}) {message_body}"
    enqueue_notification_if_missing(
        db,
        user_id=recipient.id,
        from_user_id=user.id,
        source_card_id=None,
        message=payload,
    )
    db.commit()

    add_flash(request, f"Птица отправлена пользователю @{preferred_user_alias(recipient)}.", "success")
    return redirect(next_url)


@app.post("/festivals/notifications/clear")
async def festivals_notifications_clear(request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")
    form = await request.form()
    next_url = safe_redirect_target(str(form.get("next", "")).strip(), "/festivals")
    db.execute(
        text("DELETE FROM festival_notifications WHERE user_id = :user_id"),
        {"user_id": user.id},
    )
    db.commit()
    add_flash(request, "Список оповещений очищен.", "success")
    return redirect(next_url)


@app.post("/festivals/notifications/{notification_id}/ignore")
async def festivals_notification_ignore(notification_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    notification = db.execute(
        select(FestivalNotification).where(
            FestivalNotification.id == notification_id,
            FestivalNotification.user_id == user.id,
        )
    ).scalar_one_or_none()
    if not notification:
        add_flash(request, "Оповещение не найдено.", "error")
        return redirect("/festivals")

    db.delete(notification)
    db.commit()

    form = await request.form()
    next_url = safe_redirect_target(str(form.get("next", "")).strip(), "/festivals")
    add_flash(request, "Оповещение скрыто: конфликт больше не учитывается.", "info")
    return redirect(next_url)


@app.get("/festivals/announcements/new", response_class=HTMLResponse)
def festivals_announcements_new(request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")
    return template_response(
        request,
        "festival_announcement_form.html",
        user=user,
        active_tab="festivals",
        editing=False,
        announcement_id=None,
        form=get_festival_announcement_form_values(),
    )


@app.post("/festivals/announcements/new")
async def festivals_announcements_create(request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    form = await request.form()
    announcement = FestivalAnnouncement(
        requester_user_id=user.id,
        name="",
        status=ANNOUNCEMENT_STATUS_PENDING,
    )
    ok, error_text = save_festival_announcement_from_form(form, announcement)
    if not ok:
        add_flash(request, error_text, "error")
        return redirect("/festivals/announcements/new")

    db.add(announcement)
    db.commit()
    add_flash(request, "Заявка на добавление мероприятия отправлена на модерацию.", "success")
    return redirect("/festivals")


@app.post("/festivals/announcements/{announcement_id}/approve")
def festivals_announcements_approve(announcement_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")
    if not is_moderator_user(user):
        add_flash(request, "Одобрять анонсы может только модератор.", "error")
        return redirect("/festivals")

    announcement = db.get(FestivalAnnouncement, announcement_id)
    if not announcement:
        add_flash(request, "Заявка на анонс не найдена.", "error")
        return redirect("/festivals")
    if announcement.status != ANNOUNCEMENT_STATUS_PENDING:
        add_flash(request, "Эта заявка уже обработана.", "error")
        return redirect("/festivals")

    announcement.status = ANNOUNCEMENT_STATUS_APPROVED
    announcement.reviewed_by_user_id = user.id
    announcement.reviewed_at = datetime.utcnow()
    propagated = propagate_approved_announcement(db, announcement)

    if announcement.requester_user_id != user.id:
        enqueue_notification_if_missing(
            db,
            user_id=announcement.requester_user_id,
            from_user_id=user.id,
            source_card_id=None,
            message=f"Заявка на добавление мероприятия одобрена: «{announcement.name}».",
        )

    db.commit()
    add_flash(request, f"Анонс одобрен и опубликован пользователям: {propagated}.", "success")
    return redirect("/festivals")


@app.post("/festivals/announcements/{announcement_id}/reject")
def festivals_announcements_reject(announcement_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")
    if not is_moderator_user(user):
        add_flash(request, "Отклонять анонсы может только модератор.", "error")
        return redirect("/festivals")

    announcement = db.get(FestivalAnnouncement, announcement_id)
    if not announcement:
        add_flash(request, "Заявка на анонс не найдена.", "error")
        return redirect("/festivals")
    if announcement.status != ANNOUNCEMENT_STATUS_PENDING:
        add_flash(request, "Эта заявка уже обработана.", "error")
        return redirect("/festivals")

    announcement.status = ANNOUNCEMENT_STATUS_REJECTED
    announcement.reviewed_by_user_id = user.id
    announcement.reviewed_at = datetime.utcnow()

    enqueue_notification_if_missing(
        db,
        user_id=announcement.requester_user_id,
        from_user_id=user.id,
        source_card_id=None,
        message="Заявка на добавление мероприятия отклонена",
    )

    db.commit()
    add_flash(request, "Заявка отклонена.", "info")
    return redirect("/festivals")


@app.get("/festivals/new", response_class=HTMLResponse)
def festivals_new(request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    _, _, alias_options = build_user_alias_lookup(db)
    return template_response(
        request,
        "festival_form.html",
        user=user,
        active_tab="festivals",
        editing=False,
        festival_id=None,
        form=get_festival_form_values(),
        coproplayer_alias_options=merge_unique(alias_options, get_options(db, user.id, "coproplayer")),
    )


@app.get("/festivals/{festival_id}/edit", response_class=HTMLResponse)
def festivals_edit(festival_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    festival = db.execute(
        select(Festival).where(Festival.id == festival_id, Festival.user_id == user.id)
    ).scalar_one_or_none()
    if not festival:
        add_flash(request, "Фестиваль не найден.", "error")
        return redirect("/festivals")
    if festival.is_global_announcement:
        add_flash(request, "Карточка анонса доступна только для просмотра и отметки «Я иду».", "error")
        return redirect("/festivals")

    _, _, alias_options = build_user_alias_lookup(db)
    return template_response(
        request,
        "festival_form.html",
        user=user,
        active_tab="festivals",
        editing=True,
        festival_id=festival.id,
        form=get_festival_form_values(festival),
        coproplayer_alias_options=merge_unique(alias_options, get_options(db, user.id, "coproplayer")),
    )


def save_festival_from_form(form: Any, festival: Festival, user: User, db: Session) -> None:
    alias_to_username, _, _ = build_user_alias_lookup(db)

    event_date = parse_date(str(form.get("event_date", "")))
    event_end_date = parse_date(str(form.get("event_end_date", "")))
    if not event_date and event_end_date:
        event_date = event_end_date
    if event_date and event_end_date and event_end_date < event_date:
        event_end_date = event_date

    festival.name = str(form.get("name", "")).strip()
    festival.url = str(form.get("url", "")).strip() or None
    festival.city = str(form.get("city", "")).strip() or None
    festival.event_date = event_date
    festival.event_end_date = event_end_date
    festival.submission_deadline = parse_date(str(form.get("submission_deadline", "")))
    festival.nomination_1 = str(form.get("nomination_1", "")).strip() or None
    festival.nomination_2 = str(form.get("nomination_2", "")).strip() or None
    festival.nomination_3 = str(form.get("nomination_3", "")).strip() or None
    festival.is_going = to_bool(form.get("is_going"))

    raw_coproplayer_aliases = merge_unique(
        split_csv(str(form.get("going_coproplayers_input", ""))),
        form.getlist("going_coproplayers"),  # backward compatibility with previous form
        split_csv(str(form.get("going_coproplayers_new", ""))),  # backward compatibility
    )
    festival.going_coproplayers_json = resolve_aliases_to_usernames(raw_coproplayer_aliases, alias_to_username)

    remember_options(db, user.id, "coproplayer", merge_unique(raw_coproplayer_aliases, festival.going_coproplayers_json))
    remember_options(
        db,
        user.id,
        "nomination",
        merge_unique(DEFAULT_NOMINATIONS, [festival.nomination_1 or "", festival.nomination_2 or "", festival.nomination_3 or ""]),
    )
    remember_options(db, user.id, "festival", [festival.name])


@app.post("/festivals/new")
async def festivals_create(request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    form = await request.form()
    name = str(form.get("name", "")).strip()
    if not name:
        add_flash(request, "Название фестиваля обязательно.", "error")
        return redirect("/festivals/new")

    festival = Festival(user_id=user.id, name=name)
    save_festival_from_form(form, festival, user, db)

    db.add(festival)
    db.flush()
    notify_count = notify_coproplayer_conflicts_for_festival(db, festival=festival, owner=user)
    db.commit()

    if notify_count:
        add_flash(request, f"Фестиваль создан. Конфликтов по сокосплеерам: {notify_count}.", "success")
    else:
        add_flash(request, "Фестиваль создан.", "success")
    return redirect("/festivals")


@app.post("/festivals/{festival_id}/edit")
async def festivals_update(festival_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    festival = db.execute(
        select(Festival).where(Festival.id == festival_id, Festival.user_id == user.id)
    ).scalar_one_or_none()
    if not festival:
        add_flash(request, "Фестиваль не найден.", "error")
        return redirect("/festivals")
    if festival.is_global_announcement:
        add_flash(request, "Карточку анонса нельзя редактировать.", "error")
        return redirect("/festivals")

    form = await request.form()
    name = str(form.get("name", "")).strip()
    if not name:
        add_flash(request, "Название фестиваля обязательно.", "error")
        return redirect(f"/festivals/{festival_id}/edit")

    save_festival_from_form(form, festival, user, db)
    notify_count = notify_coproplayer_conflicts_for_festival(db, festival=festival, owner=user)
    db.commit()

    if notify_count:
        add_flash(request, f"Фестиваль обновлён. Конфликтов по сокосплеерам: {notify_count}.", "success")
    else:
        add_flash(request, "Фестиваль обновлён.", "success")
    return redirect("/festivals")


@app.post("/festivals/{festival_id}/toggle-going")
async def festivals_toggle_going(festival_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    festival = db.execute(
        select(Festival).where(Festival.id == festival_id, Festival.user_id == user.id)
    ).scalar_one_or_none()
    if not festival:
        add_flash(request, "Фестиваль не найден.", "error")
        return redirect("/festivals")

    form = await request.form()
    festival.is_going = to_bool(form.get("is_going"))
    if not festival.is_going:
        festival.going_coproplayers_json = []

    notify_count = notify_coproplayer_conflicts_for_festival(db, festival=festival, owner=user)
    db.commit()

    next_url = safe_redirect_target(str(form.get("next", "")).strip(), "/festivals")
    if notify_count:
        add_flash(request, f"Отметка «Я иду» обновлена. Конфликтов по сокосплеерам: {notify_count}.", "success")
    else:
        add_flash(request, "Отметка «Я иду» обновлена.", "success")
    return redirect(next_url)


@app.post("/festivals/{festival_id}/delete")
def festivals_delete(festival_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    festival = db.execute(
        select(Festival).where(Festival.id == festival_id, Festival.user_id == user.id)
    ).scalar_one_or_none()
    if not festival:
        add_flash(request, "Фестиваль не найден.", "error")
        return redirect("/festivals")
    if festival.is_global_announcement:
        add_flash(request, "Карточку анонса нельзя удалять.", "error")
        return redirect("/festivals")

    db.delete(festival)
    db.commit()

    add_flash(request, "Фестиваль удалён.", "info")
    return redirect("/festivals")


@app.post("/festivals/import-cosplay2")
def festivals_import_cosplay2(request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    try:
        response = requests.get("https://cosplay2.ru/", timeout=20)
        response.raise_for_status()
    except requests.RequestException:
        add_flash(request, "Не удалось получить данные с cosplay2.ru", "error")
        return redirect("/festivals")

    parsed_events = parse_events_from_homepage(response.text)

    if not parsed_events:
        add_flash(request, "На cosplay2.ru не удалось найти структурированные данные фестивалей.", "error")
        return redirect("/festivals")

    existing_rows = db.execute(select(Festival).where(Festival.user_id == user.id)).scalars().all()
    existing_by_url: dict[str, Festival] = {}
    for row in existing_rows:
        normalized = normalize_url(row.url)
        if normalized:
            existing_by_url[normalized] = row

    imported = 0
    updated = 0
    imported_names: list[str] = []

    for event in parsed_events:
        normalized_url = normalize_url(event.url)
        if not normalized_url:
            continue

        existing = existing_by_url.get(normalized_url)
        if existing:
            changed = False
            guessed_existing_name = guess_name_from_url(existing.url)

            if event.name and (
                not existing.name
                or (guessed_existing_name and existing.name.casefold() == guessed_existing_name.casefold())
            ):
                existing.name = event.name
                changed = True

            if event.city and not existing.city:
                existing.city = event.city
                changed = True

            if event.event_date and not existing.event_date:
                existing.event_date = event.event_date
                changed = True

            if event.submission_deadline and not existing.submission_deadline:
                existing.submission_deadline = event.submission_deadline
                changed = True

            if changed:
                updated += 1
            continue

        festival = Festival(
            user_id=user.id,
            name=event.name,
            url=normalized_url,
            city=event.city,
            event_date=event.event_date,
            submission_deadline=event.submission_deadline,
        )
        db.add(festival)
        existing_by_url[normalized_url] = festival
        imported += 1
        imported_names.append(event.name)

    if imported or updated:
        remember_options(db, user.id, "festival", imported_names)
        db.commit()
        add_flash(
            request,
            f"Импорт с cosplay2.ru завершён: новых {imported}, обновлено {updated}.",
            "success",
        )
    else:
        add_flash(request, "Новых или обновляемых фестивалей не найдено.", "info")

    return redirect("/festivals")


@app.get("/festivals/export.ics")
def festivals_export_ics(request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    only_going = to_bool(request.query_params.get("only_going", "1"))
    stmt = select(Festival).where(Festival.user_id == user.id)
    festivals = db.execute(stmt.order_by(Festival.event_date.is_(None), Festival.event_date, Festival.name)).scalars().all()

    lines = [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//Cosplay Planner//RU",
        "CALSCALE:GREGORIAN",
        "METHOD:PUBLISH",
    ]

    dtstamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

    for festival in festivals:
        if only_going and not festival.is_going:
            continue

        if festival.event_date:
            event_end_date = festival_range_end(festival)
            lines.extend(
                [
                    "BEGIN:VEVENT",
                    f"UID:{uuid.uuid4()}@cosplay-planner.local",
                    f"DTSTAMP:{dtstamp}",
                    f"SUMMARY:{esc_ics(festival.name)}",
                    f"DTSTART;VALUE=DATE:{festival.event_date.strftime('%Y%m%d')}",
                    *(
                        [f"DTEND;VALUE=DATE:{(event_end_date + timedelta(days=1)).strftime('%Y%m%d')}"]
                        if event_end_date and event_end_date > festival.event_date
                        else []
                    ),
                    f"LOCATION:{esc_ics(festival.city)}",
                    f"URL:{esc_ics(festival.url)}",
                    f"DESCRIPTION:{esc_ics('Фестиваль. Номинации: ' + ', '.join([n for n in [festival.nomination_1, festival.nomination_2, festival.nomination_3] if n]))}",
                    "END:VEVENT",
                ]
            )

        if festival.submission_deadline:
            lines.extend(
                [
                    "BEGIN:VEVENT",
                    f"UID:{uuid.uuid4()}@cosplay-planner.local",
                    f"DTSTAMP:{dtstamp}",
                    f"SUMMARY:{esc_ics('Дедлайн подачи: ' + festival.name)}",
                    f"DTSTART;VALUE=DATE:{festival.submission_deadline.strftime('%Y%m%d')}",
                    f"LOCATION:{esc_ics(festival.city)}",
                    f"URL:{esc_ics(festival.url)}",
                    "DESCRIPTION:Дедлайн подачи заявки",
                    "END:VEVENT",
                ]
            )

    lines.append("END:VCALENDAR")
    body = "\r\n".join(lines) + "\r\n"

    filename = "cosplay-festivals.ics"
    return PlainTextResponse(
        body,
        media_type="text/calendar; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
