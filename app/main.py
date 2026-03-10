from __future__ import annotations

import csv
import io
import os
import sqlite3
import uuid
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import requests
from fastapi import Depends, FastAPI, Request
from fastapi.responses import HTMLResponse, PlainTextResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from passlib.context import CryptContext
from sqlalchemy import inspect, or_, select, text
from sqlalchemy.orm import Session
from starlette.middleware.gzip import GZipMiddleware
from starlette.middleware.sessions import SessionMiddleware
from starlette.middleware.trustedhost import TrustedHostMiddleware

from .cosplay2_parser import guess_name_from_url, normalize_url, parse_events_from_homepage
from .database import Base, engine, get_db
from .models import CardComment, CosplanCard, Festival, FestivalNotification, InProgressCard, ProjectSearchPost, User
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


PROJECT_NAME = load_project_name()
app = FastAPI(title=PROJECT_NAME)

secret_key = os.getenv("SECRET_KEY", "change-this-secret-key")
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


def apply_schema_migrations() -> None:
    # Lightweight SQLite migration path for local/self-host deployments.
    if not str(engine.url).startswith("sqlite"):
        return

    required_columns: dict[str, list[tuple[str, str]]] = {
        "users": [
            ("home_city", "VARCHAR(255)"),
            ("cosplay_nick", "VARCHAR(100)"),
        ],
        "cosplan_cards": [
            ("costume_bought", "BOOLEAN NOT NULL DEFAULT 0"),
            ("costume_link", "TEXT"),
            ("costume_buy_price", "FLOAT"),
            ("costume_fabric_price", "FLOAT"),
            ("costume_hardware_price", "FLOAT"),
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
        "in_progress_cards": [
            ("is_frozen", "BOOLEAN NOT NULL DEFAULT 0"),
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
    add(card.photoset_price, card.photoset_currency)

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
    cleaned = value.strip().casefold()
    cleaned = cleaned.replace("г.", "").replace("город", "").strip()
    cleaned = " ".join(cleaned.split())
    return cleaned


def city_matches(base_city: str | None, candidate_city: str | None) -> bool:
    left = normalize_city(base_city)
    right = normalize_city(candidate_city)
    if not left or not right:
        return False
    if left == right:
        return True
    return left in right or right in left


def can_comment_on_card(card: CosplanCard, user: User) -> bool:
    if card.plan_type != "project":
        return False
    if card.user_id == user.id:
        return True
    return user_matches_alias(user, card.project_leader)


def safe_redirect_target(target: str | None, fallback: str) -> str:
    if not target:
        return fallback
    cleaned = target.strip()
    if cleaned.startswith("/"):
        return cleaned
    return fallback


def get_accessible_card(
    db: Session,
    card_id: int,
    user: User,
    *,
    allow_project_leader: bool = False,
) -> CosplanCard | None:
    card = db.get(CosplanCard, card_id)
    if not card:
        return None
    if card.user_id == user.id:
        return card
    if allow_project_leader and not card.is_shared_copy and user_matches_alias(user, card.project_leader):
        return card
    return None


def month_label_ru(value: date) -> str:
    return f"{RU_MONTH_NAMES[value.month]} {value.year}"


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
        "planned_festivals_json",
        "submission_date",
        "nominations_json",
        "city",
        "photographers_json",
        "studios_json",
        "photoset_date",
        "photoset_price",
        "photoset_currency",
        "coproplayers_json",
        "coproplayer_nicks_json",
        "notes",
    ]


def clone_card_data(source: CosplanCard, target: CosplanCard) -> None:
    for field in card_fields_for_sync():
        setattr(target, field, getattr(source, field))


def sync_shared_cards_for_nicks(source_card: CosplanCard, owner: User, db: Session) -> None:
    if source_card.is_shared_copy:
        return

    alias_to_username, _, _ = build_user_alias_lookup(db)
    raw_nicks = as_list(source_card.coproplayer_nicks_json)
    resolved_nicks = resolve_aliases_to_usernames(raw_nicks, alias_to_username)
    owner_username = normalize_username(owner.username).casefold()
    target_nicks = [nick for nick in resolved_nicks if nick and nick.casefold() != owner_username]

    if not target_nicks:
        existing_copies = db.execute(
            select(CosplanCard).where(
                CosplanCard.source_card_id == source_card.id,
                CosplanCard.is_shared_copy.is_(True),
            )
        ).scalars().all()
        for card in existing_copies:
            db.delete(card)
        db.execute(
            text("DELETE FROM festival_notifications WHERE source_card_id = :source_card_id"),
            {"source_card_id": source_card.id},
        )
        return

    matched_users = db.execute(select(User).where(User.username.in_(target_nicks))).scalars().all()
    users_by_nick = {normalize_username(user.username).casefold(): user for user in matched_users if user.id != owner.id}
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
        db.delete(stale_copy)
        db.execute(
            text(
                "DELETE FROM festival_notifications "
                "WHERE source_card_id = :source_card_id AND user_id = :user_id"
            ),
            {"source_card_id": source_card.id, "user_id": user_id},
        )

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
                shared_from_user_id=owner.id,
                character_name=source_card.character_name,
            )
            db.add(shared_copy)
        clone_card_data(source_card, shared_copy)
        shared_copy.is_shared_copy = True
        shared_copy.source_card_id = source_card.id
        shared_copy.shared_from_user_id = owner.id

        existing_notification = db.execute(
            select(FestivalNotification).where(
                FestivalNotification.user_id == target_user.id,
                FestivalNotification.source_card_id == source_card.id,
            )
        ).scalar_one_or_none()
        if not existing_notification:
            db.add(
                FestivalNotification(
                    user_id=target_user.id,
                    from_user_id=owner.id,
                    source_card_id=source_card.id,
                    message=f"{owner.username} добавил(а) вас как сокосплеера в карточку '{source_card.character_name}'.",
                    is_read=False,
                )
            )
        else:
            existing_notification.from_user_id = owner.id
            existing_notification.message = (
                f"{owner.username} обновил(а) карточку '{source_card.character_name}' с вашим ником."
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
    }
    payload.update(context)
    return templates.TemplateResponse(name, payload)


def redirect(url: str) -> RedirectResponse:
    return RedirectResponse(url=url, status_code=303)


def get_card_form_values(card: CosplanCard | None = None) -> dict[str, Any]:
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
            "planned_festivals_json": [],
            "submission_date": "",
            "nominations_json": [],
            "city": "",
            "photographers_json": [],
            "studios_json": [],
            "photoset_date": "",
            "photoset_price": "",
            "photoset_currency": "RUB",
            "coproplayers_json": [],
            "coproplayer_nicks_json": [],
            "coproplayers_input": "",
            "estimated_total": 0.0,
            "estimated_total_currency": "",
            "notes": "",
        }

    estimated_total, estimated_currency = estimate_card_total_and_currency(card)
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
        "planned_festivals_json": as_list(card.planned_festivals_json),
        "submission_date": card.submission_date.isoformat() if card.submission_date else "",
        "nominations_json": as_list(card.nominations_json),
        "city": card.city or "",
        "photographers_json": as_list(card.photographers_json),
        "studios_json": as_list(card.studios_json),
        "photoset_date": card.photoset_date.isoformat() if card.photoset_date else "",
        "photoset_price": "" if card.photoset_price is None else f"{card.photoset_price:g}",
        "photoset_currency": card.photoset_currency or "RUB",
        "coproplayers_json": as_list(card.coproplayers_json),
        "coproplayer_nicks_json": as_list(card.coproplayer_nicks_json),
        "coproplayers_input": ", ".join(as_list(card.coproplayers_json) or as_list(card.coproplayer_nicks_json)),
        "estimated_total": estimated_total,
        "estimated_total_currency": estimated_currency,
        "notes": card.notes or "",
    }


def card_options(db: Session, user: User) -> dict[str, Any]:
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
        "submission_deadline": festival.submission_deadline.isoformat() if festival.submission_deadline else "",
        "nomination_1": festival.nomination_1 or "",
        "nomination_2": festival.nomination_2 or "",
        "nomination_3": festival.nomination_3 or "",
        "is_going": bool(festival.is_going),
        "going_coproplayers_json": as_list(festival.going_coproplayers_json),
        "going_coproplayers_input": ", ".join(as_list(festival.going_coproplayers_json)),
    }


def get_project_search_post_form_values(post: ProjectSearchPost | None = None, user: User | None = None) -> dict[str, Any]:
    default_nick = preferred_user_alias(user) if user else ""
    if not post:
        return {
            "fandom": "",
            "event_date": "",
            "event_type": "photoset",
            "comment": "",
            "contact_nick": default_nick,
            "contact_link": "",
        }

    return {
        "fandom": post.fandom or "",
        "event_date": post.event_date.isoformat() if post.event_date else "",
        "event_type": post.event_type or "photoset",
        "comment": post.comment or "",
        "contact_nick": post.contact_nick or default_nick,
        "contact_link": post.contact_link or "",
    }


@app.get("/", response_class=HTMLResponse)
def index(request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if user:
        return redirect("/cosplan")
    return redirect("/login")


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

    request.session["user_id"] = user.id
    add_flash(request, "Аккаунт создан.", "success")
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


@app.get("/profile", response_class=HTMLResponse)
def profile_page(request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")
    return template_response(
        request,
        "profile.html",
        user=user,
        active_tab="profile",
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
            ]
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
            "nominations",
            "coproplayers",
            "coproplayer_nicks",
            "costume_type",
            "shoes_type",
            "wig_type",
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
                ", ".join(as_list(card.nominations_json)),
                ", ".join(as_list(card.coproplayers_json)),
                ", ".join(as_list(card.coproplayer_nicks_json)),
                card.costume_type or "",
                card.shoes_type or "",
                card.wig_type or "",
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
        **card_options(db, user),
    )


@app.get("/cosplan/{card_id}", response_class=HTMLResponse)
def cosplan_detail(card_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    card = get_accessible_card(db, card_id, user, allow_project_leader=True)
    if not card:
        add_flash(request, "Карточка не найдена.", "error")
        return redirect("/cosplan")

    card_owner = db.get(User, card.user_id)
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

    card_total, card_total_currency = estimate_card_total_and_currency(card)
    return template_response(
        request,
        "cosplan_detail.html",
        user=user,
        active_tab="cosplan",
        card=card,
        card_owner=card_owner,
        card_total=card_total,
        card_total_currency=card_total_currency,
        card_date_conflicts=card_date_conflicts,
        can_comment=can_comment_on_card(card, user),
        top_level_comments=top_level_comments,
        replies_by_parent=replies_by_parent,
        comment_authors=authors_by_id,
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

    card = db.execute(select(CosplanCard).where(CosplanCard.id == card_id, CosplanCard.user_id == user.id)).scalar_one_or_none()
    if not card:
        add_flash(request, "Карточка не найдена.", "error")
        return redirect("/cosplan")
    if card.is_shared_copy:
        add_flash(request, "Карточка добавлена другим пользователем и доступна только для просмотра.", "info")
        return redirect("/cosplan")

    return template_response(
        request,
        "cosplan_form.html",
        user=user,
        active_tab="cosplan",
        editing=True,
        card_id=card.id,
        form=get_card_form_values(card),
        **card_options(db, user),
    )


def save_card_from_form(form: Any, card: CosplanCard, user: User, db: Session) -> None:
    alias_to_username, _, _ = build_user_alias_lookup(db)

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
        card.costume_buy_price = parse_float(str(form.get("costume_buy_price", "")))
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
            card.costume_fabric_price = parse_float(str(form.get("costume_fabric_price", "")))
            card.costume_hardware_price = parse_float(str(form.get("costume_hardware_price", "")))
        else:
            card.costume_executor = str(form.get("costume_executor", "")).strip() or None
            card.costume_prepayment = parse_float(str(form.get("costume_prepayment", "")))
            card.costume_postpayment = parse_float(str(form.get("costume_postpayment", "")))
            card.costume_fabric_price = None
            card.costume_hardware_price = None
        card.costume_bought = False
        card.costume_link = None
        card.costume_buy_price = None
    card.costume_currency = str(form.get("costume_currency", "")).strip() or None

    card.shoes_type = str(form.get("shoes_type", "")).strip() or None
    if card.shoes_type == "buy":
        card.shoes_bought = to_bool(form.get("shoes_bought"))
        card.shoes_link = str(form.get("shoes_link", "")).strip() or None
        card.shoes_buy_price = parse_float(str(form.get("shoes_buy_price", "")))
        card.shoes_executor = None
        card.shoes_deadline = None
        card.shoes_price = None
    else:
        card.shoes_bought = False
        card.shoes_link = None
        card.shoes_buy_price = None
        card.shoes_executor = str(form.get("shoes_executor", "")).strip() or None
        card.shoes_deadline = parse_date(str(form.get("shoes_deadline", "")))
        card.shoes_price = parse_float(str(form.get("shoes_price", "")))
    card.shoes_currency = str(form.get("shoes_currency", "")).strip() or None

    card.lenses_enabled = to_bool(form.get("lenses_enabled"))
    if card.lenses_enabled:
        card.lenses_comment = str(form.get("lenses_comment", "")).strip() or None
        card.lenses_color = str(form.get("lenses_color", "")).strip() or None
        card.lenses_price = parse_float(str(form.get("lenses_price", "")))
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
        card.wig_buy_price = parse_float(str(form.get("wig_buy_price", "")))
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
        card.wig_price = parse_float(str(form.get("wig_price", "")))
        card.wig_deadline = parse_date(str(form.get("wig_deadline", "")))
        card.wig_buy_price = None
        card.wig_link = None
        card.wig_no_buy_from = None
        card.wig_restyle = False
    card.wig_currency = str(form.get("wig_currency", "")).strip() or None

    card.craft_type = str(form.get("craft_type", "")).strip() or "self"
    if card.craft_type == "order":
        card.craft_master = str(form.get("craft_master", "")).strip() or None
        card.craft_price = parse_float(str(form.get("craft_price", "")))
        card.craft_deadline = parse_date(str(form.get("craft_deadline", "")))
        card.craft_material_price = None
    else:
        card.craft_master = None
        card.craft_price = None
        card.craft_deadline = None
        card.craft_material_price = parse_float(str(form.get("craft_material_price", "")))
    card.craft_currency = str(form.get("craft_currency", "")).strip() or None

    card.plan_type = str(form.get("plan_type", "")).strip() or None
    if card.plan_type == "project":
        project_leader_raw = str(form.get("project_leader", "")).strip()
        card.project_leader = resolve_alias_to_username(project_leader_raw, alias_to_username) or None
    else:
        card.project_leader = None
    card.project_deadline = parse_date(str(form.get("project_deadline", "")))

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
    coproplayer_aliases = merge_unique(
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
    card.photoset_price = parse_float(str(form.get("photoset_price", "")))
    card.photoset_currency = str(form.get("photoset_currency", "")).strip() or None

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
    sync_shared_cards_for_nicks(card, user, db)
    db.commit()

    add_flash(request, "Карточка косплана создана.", "success")
    return redirect("/cosplan")


@app.post("/cosplan/{card_id}/edit")
async def cosplan_update(card_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    card = db.execute(select(CosplanCard).where(CosplanCard.id == card_id, CosplanCard.user_id == user.id)).scalar_one_or_none()
    if not card:
        add_flash(request, "Карточка не найдена.", "error")
        return redirect("/cosplan")
    if card.is_shared_copy:
        add_flash(request, "Карточка, добавленная другим пользователем, редактируется у автора.", "error")
        return redirect("/cosplan")

    form = await request.form()
    character_name = str(form.get("character_name", "")).strip()
    if not character_name:
        add_flash(request, "Имя персонажа обязательно.", "error")
        return redirect(f"/cosplan/{card_id}/edit")

    save_card_from_form(form, card, user, db)
    sync_shared_cards_for_nicks(card, user, db)
    db.commit()

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
            db.delete(shared_copy)

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

    db.delete(card)
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
        add_flash(request, "Карточка уже в In Progress.", "info")
        return redirect("/in-progress")

    progress = InProgressCard(user_id=user.id, cosplan_card_id=card.id, checklist_json=[])
    db.add(progress)
    db.commit()

    add_flash(request, "Карточка добавлена в In Progress.", "success")
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

    return template_response(
        request,
        "in_progress.html",
        user=user,
        active_tab="in-progress",
        progress_items=progress_items,
        urgent_progress_ids=urgent_progress_ids,
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
        add_flash(request, "Карточка In Progress не найдена.", "error")
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
        add_flash(request, "Карточка In Progress не найдена.", "error")
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
        add_flash(request, "Карточка In Progress не найдена.", "error")
        return redirect("/in-progress")

    items = list(progress.checklist_json or [])
    if 0 <= item_index < len(items):
        items.pop(item_index)
        progress.checklist_json = items
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
        add_flash(request, "Карточка In Progress не найдена.", "error")
        return redirect("/in-progress")

    db.delete(progress)
    db.commit()

    add_flash(request, "Карточка удалена из In Progress.", "info")
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
        add_flash(request, "Карточка In Progress не найдена.", "error")
        return redirect("/in-progress")

    progress.is_frozen = not bool(progress.is_frozen)
    db.commit()
    if progress.is_frozen:
        add_flash(request, "Проект заморожен и перемещён в конец списка.", "info")
    else:
        add_flash(request, "Проект разморожен.", "success")
    return redirect("/in-progress")


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
    return template_response(
        request,
        "my_projects.html",
        user=user,
        active_tab="my-projects",
        cards=cards,
        owners_by_id=owners_by_id,
        card_totals=card_totals,
        card_total_currencies=card_total_currencies,
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
            Festival.event_date >= today,
        )
    ).scalars().all()
    cards = db.execute(
        select(CosplanCard).where(
            CosplanCard.user_id == user.id,
            CosplanCard.photoset_date.is_not(None),
            CosplanCard.photoset_date >= today,
        )
    ).scalars().all()
    alias_to_username, users_by_username, _ = build_user_alias_lookup(db)

    entries: list[dict[str, Any]] = []
    for festival in festivals:
        coproplayers_display = format_coproplayer_names(
            as_list(festival.going_coproplayers_json),
            alias_to_username,
            users_by_username,
        )
        entries.append(
            {
                "date": festival.event_date,
                "kind": "Фестиваль (Я иду)",
                "title": festival.name or "Без названия",
                "city": festival.city or "—",
                "coproplayers": ", ".join(coproplayers_display),
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
                "kind": "Фотосет",
                "title": card.character_name or "Без карточки",
                "city": card.city or "—",
                "coproplayers": ", ".join(coproplayers_display),
            }
        )

    entries.sort(key=lambda item: (item["date"], item["kind"], item["title"]))
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
            }
        )

    return template_response(
        request,
        "my_calendar.html",
        user=user,
        active_tab="my-calendar",
        month_groups=grouped,
    )


def project_board_fandom_options(db: Session, user: User) -> list[str]:
    global_fandoms = db.execute(
        select(ProjectSearchPost.fandom).where(ProjectSearchPost.fandom.is_not(None)).order_by(ProjectSearchPost.fandom)
    ).scalars().all()
    return merge_unique(global_fandoms, get_options(db, user.id, "fandom"))


def save_project_search_post_from_form(form: Any, post: ProjectSearchPost) -> tuple[bool, str]:
    fandom = str(form.get("fandom", "")).strip()
    event_date = parse_date(str(form.get("event_date", "")))
    event_type = str(form.get("event_type", "")).strip()
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

    post.fandom = fandom
    post.event_date = event_date
    post.event_type = event_type
    post.comment = comment
    post.contact_nick = contact_nick
    post.contact_link = contact_link
    return True, ""


@app.get("/project-board", response_class=HTMLResponse)
def project_board_list(request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    posts = db.execute(
        select(ProjectSearchPost).order_by(
            ProjectSearchPost.event_date.is_(None),
            ProjectSearchPost.event_date,
            ProjectSearchPost.created_at.desc(),
        )
    ).scalars().all()

    owner_ids = {post.user_id for post in posts}
    owners_by_id: dict[int, User] = {}
    if owner_ids:
        owners = db.execute(select(User).where(User.id.in_(owner_ids))).scalars().all()
        owners_by_id = {owner.id: owner for owner in owners}

    return template_response(
        request,
        "project_board_list.html",
        user=user,
        active_tab="project-board",
        posts=posts,
        owners_by_id=owners_by_id,
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
        active_tab="project-board",
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
        active_tab="project-board",
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


@app.get("/festivals", response_class=HTMLResponse)
def festivals_list(request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    city_filter = request.query_params.get("city", "").strip()
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
        if not festival.event_date or festival.event_date >= today
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

        if city_filter and city_filter.casefold() not in (festival.city or "").casefold():
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
    home_city_festival_ids: set[int] = set()
    if home_city_value:
        home_city_festival_ids = {
            festival.id
            for festival in filtered
            if city_matches(home_city_value, festival.city)
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
        is_home_city = city_matches(home_city_value, festival.city)
        if festival.event_date and today <= festival.event_date <= month_limit:
            summary_rows.append(
                {
                    "kind": "Событие",
                    "festival": festival,
                    "date": festival.event_date,
                    "is_home_city": is_home_city,
                }
            )
        if festival.submission_deadline and today <= festival.submission_deadline <= month_limit:
            summary_rows.append(
                {
                    "kind": "Дедлайн подачи",
                    "festival": festival,
                    "date": festival.submission_deadline,
                    "is_home_city": is_home_city,
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
    notifications = db.execute(
        select(FestivalNotification)
        .where(FestivalNotification.user_id == user.id)
        .order_by(FestivalNotification.created_at.desc())
        .limit(30)
    ).scalars().all()
    unread_notifications = sum(1 for note in notifications if not note.is_read)

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
        notifications=notifications,
        unread_notifications=unread_notifications,
        user_home_city=user.home_city or "",
        home_city_festival_ids=home_city_festival_ids,
    )


@app.post("/festivals/notifications/mark-read")
def festivals_notifications_mark_read(request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")
    db.execute(
        text("UPDATE festival_notifications SET is_read = 1 WHERE user_id = :user_id"),
        {"user_id": user.id},
    )
    db.commit()
    add_flash(request, "Уведомления отмечены как прочитанные.", "success")
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

    festival.name = str(form.get("name", "")).strip()
    festival.url = str(form.get("url", "")).strip() or None
    festival.city = str(form.get("city", "")).strip() or None
    festival.event_date = parse_date(str(form.get("event_date", "")))
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
    db.commit()

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

    form = await request.form()
    name = str(form.get("name", "")).strip()
    if not name:
        add_flash(request, "Название фестиваля обязательно.", "error")
        return redirect(f"/festivals/{festival_id}/edit")

    save_festival_from_form(form, festival, user, db)
    db.commit()

    add_flash(request, "Фестиваль обновлён.", "success")
    return redirect("/festivals")


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
            lines.extend(
                [
                    "BEGIN:VEVENT",
                    f"UID:{uuid.uuid4()}@cosplay-planner.local",
                    f"DTSTAMP:{dtstamp}",
                    f"SUMMARY:{esc_ics(festival.name)}",
                    f"DTSTART;VALUE=DATE:{festival.event_date.strftime('%Y%m%d')}",
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
