from __future__ import annotations

import csv
import calendar
import colorsys
import hashlib
import html
import io
import json
import mimetypes
import os
import re
import secrets
import sqlite3
import smtplib
import threading
import time
import uuid
from collections import defaultdict
from datetime import date, datetime, timedelta
from difflib import SequenceMatcher
from email.message import EmailMessage
from pathlib import Path
from typing import Any, Callable
from urllib.parse import parse_qs, quote, unquote, urlparse
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import requests
from fastapi import Depends, FastAPI, File, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, PlainTextResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from markupsafe import Markup
from passlib.context import CryptContext
from PIL import Image, ImageOps, UnidentifiedImageError
from sqlalchemy import and_, func, inspect, or_, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from starlette.middleware.gzip import GZipMiddleware
from starlette.middleware.sessions import SessionMiddleware
from starlette.middleware.trustedhost import TrustedHostMiddleware

from .cosplay2_parser import guess_name_from_url, normalize_url, parse_events_from_homepage
from .database import Base, SessionLocal, engine, get_db
from .models import (
    CardComment,
    CommunityArticle,
    CommunityArticleComment,
    CommunityArticleFavorite,
    CommunityCosplayer,
    CommunityCosplayerComment,
    CommunityMaster,
    CommunityMasterComment,
    CommunityMasterOrder,
    CommunityMasterRating,
    CommunityQuestion,
    CommunityQuestionComment,
    CommunityStudio,
    CommunityStudioComment,
    ContentPlanPost,
    CosplanCard,
    Festival,
    FestivalAnnouncement,
    FestivalNotification,
    HomeNews,
    InProgressCard,
    ProjectSearchPost,
    ProjectSearchComment,
    PersonalCalendarEvent,
    PasswordResetToken,
    RehearsalCard,
    RehearsalEntry,
    User,
    UserOption,
    WorkShiftDay,
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
SITE_URL = os.getenv("BASE_SITE_URL", "https://cosplay-planner.ru").rstrip("/")
SEO_DESCRIPTION = (
    "Cosplay Planner — онлайн-органайзер для косплееров: косплей органайзер, "
    "планирование косплей проектов, косплей фестивали, бюджетный косплей, "
    "косплей фото и командная работа в одном месте."
)
SEO_KEYWORDS = (
    "косплей, косплей органайзер, косплей это, планирование, планер, "
    "командная работа, организация командной работы, проект, косплей проект, "
    "косплей фестиваль, бюджетный косплей, косплей аниме, косплей фото"
)
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

CALENDAR_VIEW_MY = "my"
CALENDAR_VIEW_BUDGET = "budget"
CALENDAR_VIEW_CONTENT = "content"
CALENDAR_VIEW_OPTIONS = {CALENDAR_VIEW_MY, CALENDAR_VIEW_BUDGET, CALENDAR_VIEW_CONTENT}

CONTENT_SOCIAL_OPTIONS = ["ТГ", "IT", "VK", "tw", "rednote", "boosty", "другое"]
CONTENT_STATUS_OPTIONS = ["plan", "draft", "published"]
CONTENT_STATUS_LABELS = {
    "plan": "План",
    "draft": "Черновик",
    "published": "Опубликовано",
}
CONTENT_TELEGRAM_TOKEN_GROUP = "content_telegram_bot_token"
CONTENT_TELEGRAM_CHAT_GROUP = "content_telegram_chat"
CONTENT_TELEGRAM_PACK_GROUP = "content_telegram_premium_pack_id"
CONTENT_TELEGRAM_CHANNEL_GROUP = "content_telegram_channel"
CONTENT_TELEGRAM_PREMIUM_EMOJI_GROUP = "content_telegram_premium_emoji"
CONTENT_RUBRIC_TAG_GROUP = "content_rubric_tag"
CONTENT_TELEGRAM_IMAGE_MAX_SIDE = 2000
CONTENT_TELEGRAM_IMAGE_RETENTION_HOURS = max(
    1,
    min(168, int(os.getenv("CONTENT_TELEGRAM_IMAGE_RETENTION_HOURS", "24"))),
)
CONTENT_TELEGRAM_LOOP_SLEEP_SECONDS = max(
    15,
    min(300, int(os.getenv("CONTENT_TELEGRAM_LOOP_SLEEP", "60"))),
)
try:
    SITE_TIMEZONE = ZoneInfo(os.getenv("SITE_TIMEZONE", "Europe/Moscow"))
except ZoneInfoNotFoundError:
    SITE_TIMEZONE = ZoneInfo("UTC")

COSPLAYER_COLLAB_OPTIONS = {
    "open": "Открыт(а)",
    "pause": "Сейчас не рассматриваю",
    "closed": "Нет",
}

COSPLAYER_SKILL_OPTIONS = [
    "Спецгость фестивалей",
    "Лектор",
    "Ведущий",
    "Крафтер",
    "Швея",
    "Художник",
    "Фотограф",
    "Фотоартист",
    "Многократный призер/победитель",
]

CONTENT_RUBRIC_PALETTE = [
    "#8ecae6",
    "#ffb703",
    "#fb8500",
    "#90be6d",
    "#577590",
    "#ff7f51",
    "#b8c0ff",
    "#f28482",
    "#84a59d",
    "#6d597a",
    "#43aa8b",
    "#f9c74f",
]

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_BOT_ENABLED = bool(TELEGRAM_BOT_TOKEN) and to_bool(os.getenv("TELEGRAM_BOT_ENABLED", "1"))
TELEGRAM_POLL_TIMEOUT_SECONDS = max(5, min(60, int(os.getenv("TELEGRAM_POLL_TIMEOUT", "20"))))
TELEGRAM_LOOP_SLEEP_SECONDS = max(1, min(30, int(os.getenv("TELEGRAM_LOOP_SLEEP", "2"))))
TELEGRAM_DISPATCH_LIMIT = max(10, min(200, int(os.getenv("TELEGRAM_DISPATCH_LIMIT", "80"))))
VK_BOT_TOKEN = os.getenv("VK_BOT_TOKEN", "").strip()
VK_BOT_ENABLED = bool(VK_BOT_TOKEN) and to_bool(os.getenv("VK_BOT_ENABLED", "1"))
VK_BOT_GROUP_ID = str(os.getenv("VK_BOT_GROUP_ID", "") or "").strip()
VK_BOT_CONFIRMATION_TOKEN = str(os.getenv("VK_BOT_CONFIRMATION_TOKEN", "") or "").strip()
VK_BOT_SECRET = str(os.getenv("VK_BOT_SECRET", "") or "").strip()
VK_BOT_COMMUNITY_DOMAIN = (os.getenv("VK_BOT_COMMUNITY_DOMAIN", "cosplayplanner") or "cosplayplanner").strip()
VK_BOT_LOOP_SLEEP_SECONDS = max(1, min(30, int(os.getenv("VK_BOT_LOOP_SLEEP", "2") or "2")))
VK_BOT_DISPATCH_LIMIT = max(10, min(200, int(os.getenv("VK_BOT_DISPATCH_LIMIT", "80") or "80")))
APP_BASE_URL = (os.getenv("APP_BASE_URL", "http://127.0.0.1:8000") or "http://127.0.0.1:8000").strip().rstrip("/")
SMTP_HOST = os.getenv("SMTP_HOST", "").strip()
SMTP_PORT = int(os.getenv("SMTP_PORT", "587") or "587")
SMTP_USER = os.getenv("SMTP_USER", "").strip()
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "").strip()
SMTP_FROM_EMAIL = (os.getenv("SMTP_FROM_EMAIL", "") or SMTP_USER).strip()
SMTP_USE_TLS = to_bool(os.getenv("SMTP_USE_TLS", "1"))
SMTP_USE_SSL = to_bool(os.getenv("SMTP_USE_SSL", "0"))
PASSWORD_RESET_TOKEN_MINUTES = max(5, min(24 * 60, int(os.getenv("PASSWORD_RESET_TOKEN_MINUTES", "30") or "30")))

VKID_ENABLED = to_bool(os.getenv("VKID_ENABLED", "1"))
VKID_APP_ID = int(os.getenv("VKID_APP_ID", "54500249") or "54500249")
VKID_REDIRECT_URL = (
    os.getenv("VKID_REDIRECT_URL", "https://cosplay-planner.ru/") or "https://cosplay-planner.ru/"
).strip()
VKID_SCOPE = (os.getenv("VKID_SCOPE", "") or "").strip()
VK_API_VERSION = (os.getenv("VK_API_VERSION", "5.199") or "5.199").strip()
VK_API_TOKEN = (os.getenv("VK_API_TOKEN", "") or os.getenv("VK_IMPORT_TOKEN", "") or "").strip()
VK_IMPORT_ENABLED = bool(VK_API_TOKEN) and to_bool(os.getenv("VK_IMPORT_ENABLED", "1"))
VK_IMPORT_WALL_DOMAIN = (os.getenv("VK_IMPORT_WALL_DOMAIN", "cosplay_teamm") or "cosplay_teamm").strip()
VK_IMPORT_WALL_COUNT = max(10, min(100, int(os.getenv("VK_IMPORT_WALL_COUNT", "50") or "50")))
RAF_OWNER_ID = int(os.getenv("RAF_OWNER_ID", "-22664912") or "-22664912")
RAF_PAGE_TITLES = [
    "Календарь_2026_январь-май",
    "Календарь_2026_июнь-август",
    "Календарь_2026_сентябрь-декабрь",
]
RAF_PAGE_URLS = [
    "https://vk.com/pages?hash=bf2fe57dc20023910b&oid=-22664912&p=Календарь_2026_январь-май",
    "https://vk.com/pages?hash=bf2fe57dc20023910b&oid=-22664912&p=Календарь_2026_июнь-август",
    "https://vk.com/pages?hash=bf2fe57dc20023910b&oid=-22664912&p=Календарь_2026_сентябрь-декабрь",
]
MASTER_IMPORT_INTERVAL_HOURS = max(1, min(24, int(os.getenv("MASTER_IMPORT_INTERVAL_HOURS", "12") or "12")))
RAF_IMPORT_INTERVAL_HOURS = max(1, min(72, int(os.getenv("RAF_IMPORT_INTERVAL_HOURS", "24") or "24")))
MASTER_IMPORT_SOURCE_LABEL = "cosplay_team"
COSPLAY2_IMPORT_SOURCE_LABEL = "cos2"
RAF_IMPORT_SOURCE_LABEL = "raf"
VKID_PUBLIC_INFO_URL = "https://id.vk.ru/oauth2/public_info"
IMPORT_SOURCE_LABELS = {
    MASTER_IMPORT_SOURCE_LABEL: "Cosplay Team",
    COSPLAY2_IMPORT_SOURCE_LABEL: "взято с Cos2",
    RAF_IMPORT_SOURCE_LABEL: "взято с РАФ",
}
FESTIVAL_NAME_DUPLICATE_STOP_WORDS = {
    "fest",
    "festival",
    "con",
    "anime",
    "cosplay",
    "косплей",
    "фест",
    "фестиваль",
    "аниме",
    "феста",
    "феста",
    "эвент",
    "event",
}

telegram_auth_state_lock = threading.Lock()
telegram_auth_state: dict[str, dict[str, str]] = {}
telegram_worker_lock = threading.Lock()
telegram_worker_thread: threading.Thread | None = None
content_telegram_worker_lock = threading.Lock()
content_telegram_worker_thread: threading.Thread | None = None
vk_bot_auth_state_lock = threading.Lock()
vk_bot_auth_state: dict[str, dict[str, str]] = {}
vk_bot_worker_lock = threading.Lock()
vk_bot_worker_thread: threading.Thread | None = None

MASTER_TYPE_OPTIONS = [
    "фотограф",
    "швея",
    "крафтер",
    "вигмейкер",
    "художник",
    "видеограф",
    "другое",
]

STUDIO_TAG_OPTIONS = [
    "Китай",
    "Япония",
    "современная",
    "лофт",
    "Средневековье",
    "романтика",
    "повседневная",
    "закусочная",
    "кибер",
    "циклорама",
    "природа",
    "уличная",
    "частная",
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
MAX_UPLOAD_INPUT_BYTES = 20 * 1024 * 1024
MAX_GALLERY_IMAGE_BYTES = 30 * 1024
MAX_GALLERY_IMAGE_WIDTH = 512

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
            ("telegram_chat_id", "VARCHAR(64)"),
            ("telegram_linked_at", "DATETIME"),
            ("telegram_secret_code_hash", "VARCHAR(255)"),
            ("telegram_secret_code_updated_at", "DATETIME"),
            ("vk_bot_user_id", "VARCHAR(64)"),
            ("vk_bot_peer_id", "VARCHAR(64)"),
            ("vk_bot_linked_at", "DATETIME"),
            ("vk_user_id", "VARCHAR(64)"),
            ("vk_screen_name", "VARCHAR(255)"),
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
            ("is_priority", "BOOLEAN NOT NULL DEFAULT 0"),
            ("is_completed", "BOOLEAN NOT NULL DEFAULT 0"),
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
            ("reply_to_notification_id", "INTEGER"),
            ("message", "TEXT NOT NULL"),
            ("is_read", "BOOLEAN NOT NULL DEFAULT 0"),
            ("telegram_sent_at", "DATETIME"),
            ("vk_sent_at", "DATETIME"),
            ("created_at", "DATETIME DEFAULT CURRENT_TIMESTAMP"),
        ],
        "project_search_posts": [
            ("status", "VARCHAR(32) NOT NULL DEFAULT 'active'"),
            ("city", "VARCHAR(255)"),
        ],
        "in_progress_cards": [
            ("is_frozen", "BOOLEAN NOT NULL DEFAULT 0"),
            ("task_rows_json", "JSON NOT NULL DEFAULT '[]'"),
        ],
        "festivals": [
            ("event_end_date", "DATE"),
            ("is_global_announcement", "BOOLEAN NOT NULL DEFAULT 0"),
            ("source_announcement_id", "INTEGER"),
            ("import_source", "VARCHAR(64)"),
            ("import_external_id", "VARCHAR(128)"),
        ],
        "work_shift_days": [
            ("is_half_day", "BOOLEAN NOT NULL DEFAULT 0"),
        ],
        "personal_calendar_events": [
            ("event_city", "VARCHAR(255)"),
        ],
        "community_master_comments": [
            ("is_client", "BOOLEAN NOT NULL DEFAULT 0"),
            ("images_json", "JSON NOT NULL DEFAULT '[]'"),
        ],
        "community_masters": [
            ("import_source", "VARCHAR(64)"),
            ("import_external_id", "VARCHAR(128)"),
            ("import_url", "TEXT"),
        ],
        "content_plan_posts": [
            ("description", "TEXT"),
            ("publish_time", "VARCHAR(8)"),
            ("socials_json", "JSON NOT NULL DEFAULT '[]'"),
            ("rubric", "VARCHAR(120) NOT NULL DEFAULT 'Общее'"),
            ("rubric_tag", "VARCHAR(120)"),
            ("status", "VARCHAR(32) NOT NULL DEFAULT 'plan'"),
            ("telegram_body_html", "TEXT"),
            ("telegram_photos_json", "JSON NOT NULL DEFAULT '[]'"),
            ("telegram_channels_json", "JSON NOT NULL DEFAULT '[]'"),
            ("telegram_cleanup_photos_json", "JSON NOT NULL DEFAULT '[]'"),
            ("telegram_message_id", "VARCHAR(64)"),
            ("telegram_message_ids_json", "JSON NOT NULL DEFAULT '[]'"),
            ("telegram_published_at", "DATETIME"),
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
        if "project_search_comments" not in existing_tables:
            ProjectSearchComment.__table__.create(bind=conn, checkfirst=True)
        if "community_questions" not in existing_tables:
            CommunityQuestion.__table__.create(bind=conn, checkfirst=True)
        if "community_question_comments" not in existing_tables:
            CommunityQuestionComment.__table__.create(bind=conn, checkfirst=True)
        if "community_masters" not in existing_tables:
            CommunityMaster.__table__.create(bind=conn, checkfirst=True)
        if "community_master_comments" not in existing_tables:
            CommunityMasterComment.__table__.create(bind=conn, checkfirst=True)
        if "community_master_ratings" not in existing_tables:
            CommunityMasterRating.__table__.create(bind=conn, checkfirst=True)
        if "community_studios" not in existing_tables:
            CommunityStudio.__table__.create(bind=conn, checkfirst=True)
        if "community_studio_comments" not in existing_tables:
            CommunityStudioComment.__table__.create(bind=conn, checkfirst=True)
        if "community_cosplayers" not in existing_tables:
            CommunityCosplayer.__table__.create(bind=conn, checkfirst=True)
        if "community_cosplayer_comments" not in existing_tables:
            CommunityCosplayerComment.__table__.create(bind=conn, checkfirst=True)
        if "community_articles" not in existing_tables:
            CommunityArticle.__table__.create(bind=conn, checkfirst=True)
        if "community_article_comments" not in existing_tables:
            CommunityArticleComment.__table__.create(bind=conn, checkfirst=True)
        if "community_article_favorites" not in existing_tables:
            CommunityArticleFavorite.__table__.create(bind=conn, checkfirst=True)
        if "festival_announcements" not in existing_tables:
            FestivalAnnouncement.__table__.create(bind=conn, checkfirst=True)
        if "home_news" not in existing_tables:
            HomeNews.__table__.create(bind=conn, checkfirst=True)
        if "rehearsal_cards" not in existing_tables:
            RehearsalCard.__table__.create(bind=conn, checkfirst=True)
        if "rehearsal_entries" not in existing_tables:
            RehearsalEntry.__table__.create(bind=conn, checkfirst=True)
        if "personal_calendar_events" not in existing_tables:
            PersonalCalendarEvent.__table__.create(bind=conn, checkfirst=True)
        if "work_shift_days" not in existing_tables:
            WorkShiftDay.__table__.create(bind=conn, checkfirst=True)
        if "content_plan_posts" not in existing_tables:
            ContentPlanPost.__table__.create(bind=conn, checkfirst=True)
        if "password_reset_tokens" not in existing_tables:
            PasswordResetToken.__table__.create(bind=conn, checkfirst=True)

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

        conn.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS ix_users_vk_user_id "
                "ON users (vk_user_id)"
            )
        )
        conn.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS ix_users_vk_bot_user_id "
                "ON users (vk_bot_user_id)"
            )
        )
        conn.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS ix_users_vk_bot_peer_id "
                "ON users (vk_bot_peer_id)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_users_vk_screen_name "
                "ON users (vk_screen_name)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_festival_notifications_telegram_sent_at "
                "ON festival_notifications (telegram_sent_at)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_festival_notifications_vk_sent_at "
                "ON festival_notifications (vk_sent_at)"
            )
        )


@app.on_event("startup")
def startup() -> None:
    Base.metadata.create_all(bind=engine)
    apply_schema_migrations()
    start_telegram_worker()
    start_content_telegram_worker()
    start_vk_bot_worker()
    try:
        auto_backup_if_needed()
    except OSError:
        # Backup creation must not block app startup.
        pass
    except sqlite3.Error:
        pass
    except RuntimeError:
        pass
    try:
        auto_import_external_sources_if_needed()
    except OSError:
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
    try:
        auto_import_external_sources_if_needed()
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


@app.get("/media/{filename}", include_in_schema=False)
def media_file(filename: str):
    safe_name = safe_media_filename(filename)
    file_path = media_storage_path() / safe_name
    if not file_path.exists() or not file_path.is_file():
        raise HTTPException(status_code=404, detail="Файл не найден.")
    media_type = mimetypes.guess_type(str(file_path))[0] or "application/octet-stream"
    return FileResponse(str(file_path), media_type=media_type)


@app.post("/media/upload-image")
async def media_upload_image(
    request: Request,
    image: UploadFile = File(...),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    user = current_user(request, db)
    if not user:
        raise HTTPException(status_code=401, detail="Требуется авторизация.")

    if not image.filename:
        raise HTTPException(status_code=400, detail="Файл не передан.")
    content_type = (image.content_type or "").lower()
    if not content_type.startswith("image/"):
        raise HTTPException(status_code=400, detail="Нужен файл изображения.")

    raw_bytes = await image.read(MAX_UPLOAD_INPUT_BYTES + 1)
    if len(raw_bytes) > MAX_UPLOAD_INPUT_BYTES:
        raise HTTPException(status_code=400, detail="Изображение слишком большое (до 20 МБ).")

    try:
        webp_bytes, width, height = compress_image_to_webp(
            raw_bytes,
            max_output_bytes=MAX_GALLERY_IMAGE_BYTES,
            max_width=MAX_GALLERY_IMAGE_WIDTH,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    file_name = f"{datetime.utcnow().strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:14]}.webp"
    destination = media_storage_path() / file_name
    destination.write_bytes(webp_bytes)

    public_path = f"/media/{file_name}"
    base_url = str(request.base_url).rstrip("/")
    return {
        "ok": True,
        "url": f"{base_url}{public_path}",
        "path": public_path,
        "size_bytes": len(webp_bytes),
        "width": width,
        "height": height,
        "format": "webp",
    }


@app.post("/media/upload-content-image")
async def media_upload_content_image(
    request: Request,
    image: UploadFile = File(...),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    user = current_user(request, db)
    if not user:
        raise HTTPException(status_code=401, detail="Требуется авторизация.")

    if not image.filename:
        raise HTTPException(status_code=400, detail="Файл не передан.")
    content_type = (image.content_type or "").lower()
    if not content_type.startswith("image/"):
        raise HTTPException(status_code=400, detail="Нужен файл изображения.")

    raw_bytes = await image.read(MAX_UPLOAD_INPUT_BYTES + 1)
    if len(raw_bytes) > MAX_UPLOAD_INPUT_BYTES:
        raise HTTPException(status_code=400, detail="Изображение слишком большое (до 20 МБ).")

    try:
        prepared_bytes, width, height, file_ext, file_format = prepare_content_image_upload(raw_bytes)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    file_name = f"{datetime.utcnow().strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:14]}{file_ext}"
    destination = media_storage_path() / file_name
    destination.write_bytes(prepared_bytes)

    public_path = f"/media/{file_name}"
    base_url = str(request.base_url).rstrip("/")
    return {
        "ok": True,
        "url": f"{base_url}{public_path}",
        "path": public_path,
        "size_bytes": len(prepared_bytes),
        "width": width,
        "height": height,
        "format": file_format,
    }


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


def can_manage_master(user: User | None, master: CommunityMaster | None) -> bool:
    if not user or not master:
        return False
    if master.user_id == user.id:
        return True
    return user_is_special(user) and bool(master.import_source)


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
        task_text = str(item.get("task") or item.get("text") or "").strip()
        if not task_text:
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
                "task": task_text,
                "done": bool(item.get("done")),
            }
        )
    return tasks


def task_scope_card_ids(db: Session, card: CosplanCard | None) -> list[int]:
    source_card = resolve_source_card(db, card)
    if not source_card:
        return []

    ids: list[int] = [source_card.id]
    shared_ids = db.execute(
        select(CosplanCard.id).where(
            CosplanCard.source_card_id == source_card.id,
            CosplanCard.is_shared_copy.is_(True),
        )
    ).scalars().all()
    for item_id in shared_ids:
        if item_id not in ids:
            ids.append(item_id)
    return ids


def task_scope_progress_rows(db: Session, card: CosplanCard | None) -> list[InProgressCard]:
    scope_ids = task_scope_card_ids(db, card)
    if not scope_ids:
        return []
    return db.execute(
        select(InProgressCard)
        .where(InProgressCard.cosplan_card_id.in_(scope_ids))
        .order_by(InProgressCard.updated_at.desc(), InProgressCard.id.desc())
    ).scalars().all()


def task_rows_to_storage(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "assignee": str(row.get("assignee") or "").strip(),
            "task": str(row.get("task") or "").strip(),
            "done": bool(row.get("done")),
        }
        for row in rows
        if str(row.get("task") or "").strip()
    ]


def load_scoped_task_rows(
    db: Session,
    card: CosplanCard | None,
    alias_to_username: dict[str, str],
    users_by_username: dict[str, User],
) -> list[dict[str, Any]]:
    for progress_row in task_scope_progress_rows(db, card):
        formatted = format_in_progress_tasks(
            as_list(progress_row.task_rows_json),
            alias_to_username,
            users_by_username,
        )
        if formatted:
            return formatted
    return []


def store_scoped_task_rows(db: Session, card: CosplanCard | None, rows: list[dict[str, Any]]) -> None:
    progress_rows = task_scope_progress_rows(db, card)
    if not progress_rows:
        return
    payload = task_rows_to_storage(rows)
    for progress_row in progress_rows:
        progress_row.task_rows_json = payload


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
                    "anime_title": "Genshin Impact",
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
        seen: set[tuple[str, int, str]] = set()
        for day_raw, section_html in sections:
            try:
                day_num = int(day_raw)
            except ValueError:
                continue

            cards = re.findall(r"<li>(.*?)</li>", section_html, flags=re.IGNORECASE | re.DOTALL)
            for card_html in cards:
                name_match = re.search(r'<img[^>]+alt="([^"]+)"', card_html, flags=re.IGNORECASE)
                if not name_match:
                    continue
                name = html.unescape(name_match.group(1)).strip()
                if not name:
                    continue
                character_url_match = re.search(r'<a[^>]+href="([^"]*character/[^"]+)"', card_html, flags=re.IGNORECASE)
                character_url = html.unescape(character_url_match.group(1)).strip() if character_url_match else ""
                anime_match = re.search(r'<span class="company">([^<]+)</span>', card_html, flags=re.IGNORECASE)
                anime_title = html.unescape(anime_match.group(1)).strip() if anime_match else ""

                key = (name.casefold(), day_num, character_url.casefold() or anime_title.casefold())
                if key in seen:
                    continue
                seen.add(key)
                payload.append(
                    {
                        "day": day_num,
                        "name": name,
                        "source": "aniSearch",
                        "character_url": character_url,
                        "anime_title": anime_title,
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
    value = re.sub(r"^\s*(персонаж|character)\s+", "", value, flags=re.IGNORECASE)
    value = re.sub(r"\s*[-:]\s*(иконка|icon)\s*$", "", value, flags=re.IGNORECASE)
    value = value.replace("Иконка", "").replace("icon", "").replace("Icon", "")
    return re.sub(r"\s+", " ", value).strip(" -")


def normalize_anisearch_character_url(raw_url: str | None) -> str:
    value = (raw_url or "").strip()
    if not value:
        return ""
    if value.startswith("http://") or value.startswith("https://"):
        return value
    return f"https://www.anisearch.com/{value.lstrip('/')}"


def fetch_anisearch_anime_title(character_url: str | None, fallback_title: str | None = None) -> str:
    fallback = re.sub(r"\s+", " ", (fallback_title or "").strip())
    normalized_url = normalize_anisearch_character_url(character_url)
    if not normalized_url:
        return fallback

    def _load() -> str:
        try:
            response = requests.get(
                normalized_url,
                timeout=HTTP_TIMEOUT_SECONDS,
                headers={"User-Agent": "Mozilla/5.0"},
            )
            response.raise_for_status()
            html_text = response.text
        except requests.RequestException:
            return fallback

        anime_block_match = re.search(
            r'<div class="anime">\s*<span class="header">\s*Anime:\s*</span>(.*?)</div>',
            html_text,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if not anime_block_match:
            return fallback

        anime_block = anime_block_match.group(1)
        anime_title_match = re.search(r"<a[^>]*>(.*?)</a>", anime_block, flags=re.IGNORECASE | re.DOTALL)
        if anime_title_match:
            anime_title_raw = anime_title_match.group(1)
        else:
            anime_title_raw = anime_block

        anime_title = html.unescape(re.sub(r"<[^>]+>", " ", anime_title_raw))
        anime_title = re.sub(r"\s+", " ", anime_title).strip(" -")
        return anime_title or fallback

    return cache_get_or_load(f"anisearch_character_anime:{normalized_url}", _load)


def character_display_name(name: str, anime_title: str | None = None) -> str:
    base_name = clean_character_birthday_name(name)
    title = re.sub(r"\s+", " ", (anime_title or "").strip())
    if not base_name:
        return ""
    if not title:
        return base_name
    if re.search(rf"\(\s*{re.escape(title)}\s*\)$", base_name, flags=re.IGNORECASE):
        return base_name
    return f"{base_name} ({title})"


def character_birthdays_today(today: date) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    seen: set[str] = set()
    for producer in [
        fetch_character_birthdays_from_genshin,
        fetch_character_birthdays_from_sheet,
        fetch_character_birthdays_from_anisearch,
    ]:
        try:
            rows = producer(today.month)
        except Exception:
            continue
        for row in rows:
            day_num = int(row.get("day") or 0)
            if day_num != today.day:
                continue
            source = str(row.get("source", "")).strip()
            anime_title = str(row.get("anime_title", "")).strip()
            if source.casefold() == "genshin impact wiki" and not anime_title:
                anime_title = "Genshin Impact"
            if source.casefold() == "anisearch":
                anime_title = fetch_anisearch_anime_title(
                    str(row.get("character_url", "")).strip(),
                    fallback_title=anime_title,
                )
            display_name = character_display_name(str(row.get("name", "")), anime_title)
            if not display_name:
                continue
            key = display_name.casefold()
            if key in seen:
                continue
            seen.add(key)
            items.append(
                {
                    "day": day_num,
                    "name": display_name,
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
    shift_days: set[int] | None = None,
    shift_half_days: set[int] | None = None,
) -> list[list[dict[str, Any]]]:
    calendar_builder = calendar.Calendar(firstweekday=0)
    matrix = calendar_builder.monthdayscalendar(year, month)
    day_types: dict[int, set[str]] = defaultdict(set)
    day_labels: dict[int, list[str]] = defaultdict(list)
    normalized_shift_days = {int(day) for day in (shift_days or set()) if isinstance(day, int) and day > 0}
    normalized_shift_half_days = {int(day) for day in (shift_half_days or set()) if isinstance(day, int) and day > 0}
    for entry in entries:
        entry_date = entry.get("date")
        if not isinstance(entry_date, date) or entry_date.year != year or entry_date.month != month:
            continue
        day_types[entry_date.day].add(str(entry.get("type_key") or ""))
        title = str(entry.get("title") or "").strip()
        kind = str(entry.get("kind") or "").strip()
        label = title or kind
        if label and label not in day_labels[entry_date.day]:
            day_labels[entry_date.day].append(label)

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
            has_shift = day_value in normalized_shift_days
            has_shift_half_day = day_value in normalized_shift_half_days
            has_shift_full_day = has_shift and not has_shift_half_day
            week_cells.append(
                {
                    "day": day_value,
                    "type_keys": types,
                    "single_type": (types[0] if len(types) == 1 else ""),
                    "is_multi": len(types) > 1,
                    "labels": day_labels.get(day_value, []),
                    "has_work_shift": has_shift,
                    "has_full_day_shift": has_shift_full_day,
                    "is_work_shift_only": has_shift_full_day and not types,
                    "has_half_day_shift": has_shift_half_day,
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


def normalize_calendar_view(raw: str | None) -> str:
    value = (raw or "").strip().lower()
    if value in CALENDAR_VIEW_OPTIONS:
        return value
    return CALENDAR_VIEW_MY


def calendar_redirect_for_view(raw_view: str | None = None) -> RedirectResponse:
    view = normalize_calendar_view(raw_view)
    if view == CALENDAR_VIEW_MY:
        return redirect("/my-calendar")
    return redirect(f"/my-calendar?view={view}")


def shift_months_safe(base_date: date, month_delta: int) -> date:
    month_index = (base_date.month - 1) + month_delta
    year = base_date.year + (month_index // 12)
    month = (month_index % 12) + 1
    day = min(base_date.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


def card_anchor_date_for_budget(card: CosplanCard, festival_start_by_name: dict[str, date]) -> date | None:
    if card.photoset_date:
        return card.photoset_date

    planned_dates: list[date] = []
    for festival_name in as_list(card.planned_festivals_json):
        festival_date = festival_start_by_name.get(str(festival_name).strip().casefold())
        if festival_date:
            planned_dates.append(festival_date)
    if planned_dates:
        return min(planned_dates)
    if card.project_deadline:
        return card.project_deadline
    return None


def build_budget_month_groups(user: User, db: Session) -> list[dict[str, Any]]:
    cards = db.execute(
        select(CosplanCard).where(
            CosplanCard.user_id == user.id,
            CosplanCard.is_shared_copy.is_(False),
        )
    ).scalars().all()
    festivals = db.execute(
        select(Festival).where(
            Festival.user_id == user.id,
            Festival.event_date.is_not(None),
        )
    ).scalars().all()

    festival_start_by_name: dict[str, date] = {}
    for festival in festivals:
        festival_name_key = (festival.name or "").strip().casefold()
        if not festival_name_key or not festival.event_date:
            continue
        existing = festival_start_by_name.get(festival_name_key)
        if existing is None or festival.event_date < existing:
            festival_start_by_name[festival_name_key] = festival.event_date

    month_card_totals: dict[tuple[int, int], dict[int, float]] = defaultdict(lambda: defaultdict(float))
    card_titles: dict[int, str] = {}

    def add_budget_cost(card: CosplanCard, target_date: date | None, amount: float | None) -> None:
        if not target_date or amount is None:
            return
        normalized = float(amount)
        if normalized <= 0:
            return
        month_card_totals[(target_date.year, target_date.month)][card.id] += normalized
        card_titles[card.id] = card.character_name or f"Карточка #{card.id}"

    for card in cards:
        # Дедлайновые траты.
        if card.costume_deadline:
            add_budget_cost(card, card.costume_deadline, card.costume_prepayment)
            add_budget_cost(card, card.costume_deadline, card.costume_postpayment)
        if card.craft_deadline:
            add_budget_cost(card, card.craft_deadline, card.craft_price)
            add_budget_cost(card, card.craft_deadline, card.craft_material_price)
        if card.wig_deadline and (card.wig_type or "").strip().lower() == "wigmaker":
            add_budget_cost(card, card.wig_deadline, card.wig_price)
        if card.photoset_date:
            has_split_photoset = (
                card.photoset_photographer_price is not None
                or card.photoset_studio_price is not None
            )
            if has_split_photoset:
                add_budget_cost(card, card.photoset_date, card.photoset_photographer_price)
                add_budget_cost(card, card.photoset_date, card.photoset_studio_price)
            else:
                # Legacy fallback for cards created before split price fields.
                add_budget_cost(card, card.photoset_date, card.photoset_price)

        # Покупки заранее: за 2 месяца до даты фотосета/фестиваля.
        anchor_date = card_anchor_date_for_budget(card, festival_start_by_name)
        if anchor_date:
            purchase_date = shift_months_safe(anchor_date, -2)
            add_budget_cost(card, purchase_date, card.costume_buy_price)
            add_budget_cost(card, purchase_date, card.wig_buy_price)
            if not card.craft_deadline:
                if (card.craft_type or "").strip().lower() == "order":
                    add_budget_cost(card, purchase_date, card.craft_price)
                else:
                    add_budget_cost(card, purchase_date, card.craft_material_price)

    groups: list[dict[str, Any]] = []
    for year_month in sorted(month_card_totals.keys()):
        year, month = year_month
        card_map = month_card_totals[year_month]
        rows = [
            {
                "card_id": card_id,
                "card_title": card_titles.get(card_id, f"Карточка #{card_id}"),
                "amount": amount,
            }
            for card_id, amount in card_map.items()
            if amount > 0
        ]
        rows.sort(key=lambda item: (item["card_title"].casefold(), item["card_id"]))
        month_total = sum(item["amount"] for item in rows)
        groups.append(
            {
                "title": month_label_ru(date(year, month, 1)),
                "rows": rows,
                "month_total": month_total,
                "is_over_limit": month_total > 100000,
            }
        )
    return groups


def normalize_content_status(raw_status: str | None) -> str:
    value = (raw_status or "").strip().lower()
    if value in CONTENT_STATUS_OPTIONS:
        return value
    return "plan"


def get_content_plan_form_values(
    post: ContentPlanPost | None = None,
    rubric_tags: dict[str, str] | None = None,
    telegram_channels: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    tag_map = rubric_tags or {}
    available_telegram_channels = telegram_channels or []
    default_channel_ids = (
        [available_telegram_channels[0]["chat_id"]]
        if len(available_telegram_channels) == 1 and available_telegram_channels[0].get("chat_id")
        else []
    )
    if not post:
        return {
            "title": "",
            "description": "",
            "publish_date": "",
            "publish_time": "",
            "socials_json": [],
            "socials_other": "",
            "telegram_channel_ids": default_channel_ids,
            "rubric_existing": "",
            "rubric_new": "",
            "rubric_tag_value": "",
            "status": "plan",
            "telegram_body_html": "",
            "telegram_photos_input": "",
        }

    socials = as_list(post.socials_json)
    socials_other_values = [value for value in socials if value not in CONTENT_SOCIAL_OPTIONS]
    return {
        "title": post.title or "",
        "description": post.description or "",
        "publish_date": post.publish_date.isoformat() if post.publish_date else "",
        "publish_time": post.publish_time or "",
        "socials_json": [value for value in socials if value in CONTENT_SOCIAL_OPTIONS],
        "socials_other": ", ".join(socials_other_values),
        "telegram_channel_ids": as_list(post.telegram_channels_json) or default_channel_ids,
        "rubric_existing": post.rubric or "",
        "rubric_new": "",
        "rubric_tag_value": normalize_content_rubric_tag(post.rubric_tag) or tag_map.get(post.rubric or "", ""),
        "status": normalize_content_status(post.status),
        "telegram_body_html": post.telegram_body_html or "",
        "telegram_photos_input": "\n".join(as_list(post.telegram_photos_json)),
    }


def save_content_plan_post_from_form(form: Any, post: ContentPlanPost, user: User, db: Session) -> tuple[bool, str]:
    title = str(form.get("title", "")).strip()
    description = str(form.get("description", "")).strip()
    publish_date = parse_date(str(form.get("publish_date", "")).strip())
    publish_time = parse_time_hhmm(str(form.get("publish_time", "")).strip())
    status = normalize_content_status(str(form.get("status", "")).strip())
    rubric_existing = str(form.get("rubric_existing", "")).strip()
    rubric_new = str(form.get("rubric_new", "")).strip()
    rubric_tag_value = str(form.get("rubric_tag_value", "")).strip()
    rubric = rubric_new or rubric_existing
    socials = merge_unique(form.getlist("socials"), split_csv(str(form.get("socials_other", "")).strip()))
    telegram_body_html = str(form.get("telegram_body_html", "")).strip()
    telegram_photos = parse_reference_values(str(form.get("telegram_photos_input", "")))[:10]
    available_telegram_channels = get_content_telegram_channels(db, user.id)
    selected_telegram_channels = resolve_content_telegram_channels(
        form.getlist("telegram_channel_ids"),
        available_telegram_channels,
    )
    selected_telegram_channel_ids = [channel["chat_id"] for channel in selected_telegram_channels]
    rubric_tags = get_content_rubric_tags(db, user.id)

    if not title:
        return False, "Укажите название публикации."
    if not publish_date:
        return False, "Укажите дату публикации."
    if not rubric:
        return False, "Укажите рубрику (выберите или создайте новую)."
    if len(title) > 255:
        return False, "Название публикации должно быть не длиннее 255 символов."
    if len(rubric) > 120:
        return False, "Название рубрики должно быть не длиннее 120 символов."
    if len(description) > 4000:
        return False, "Краткое описание должно быть не длиннее 4000 символов."
    if len(telegram_body_html) > 12000:
        return False, "Текст для Telegram должен быть не длиннее 12000 символов."
    if any(item.casefold() == "тг" for item in socials) and not publish_time:
        return False, "Для автопубликации в Telegram укажите время публикации."
    if any(item.casefold() == "тг" for item in socials):
        if not available_telegram_channels:
            return False, "Сначала добавьте хотя бы один Telegram-канал в настройках."
        if not selected_telegram_channel_ids:
            if len(available_telegram_channels) == 1:
                selected_telegram_channel_ids = [available_telegram_channels[0]["chat_id"]]
            else:
                return False, "Выберите хотя бы один Telegram-канал для публикации."

    normalized_tag = normalize_content_rubric_tag(rubric_tag_value)
    if rubric_new and not normalized_tag:
        return False, "Для новой рубрики укажите тег рубрики."
    if rubric_tag_value and not normalized_tag:
        return False, "Тег рубрики должен содержать буквы, цифры или знак подчеркивания."
    resolved_rubric_tag = normalized_tag or normalize_content_rubric_tag(rubric_tags.get(rubric, ""))

    post.title = title
    post.description = description or None
    post.publish_date = publish_date
    post.publish_time = publish_time
    post.socials_json = socials
    post.rubric = rubric
    post.rubric_tag = resolved_rubric_tag or None
    post.status = status
    post.telegram_body_html = telegram_body_html or None
    post.telegram_photos_json = telegram_photos
    post.telegram_channels_json = selected_telegram_channel_ids

    if resolved_rubric_tag and (rubric_new or rubric_existing):
        rubric_tags[rubric] = resolved_rubric_tag
        save_content_rubric_tags(db, user.id, rubric_tags)

    remember_options(db, user.id, "content_rubric", [rubric])
    return True, ""


def get_content_telegram_settings(user: User, db: Session) -> dict[str, Any]:
    premium_entries = get_content_premium_emoji_entries(db, user.id)
    channels = get_content_telegram_channels(db, user.id)
    return {
        "bot_token": get_user_option_value(db, user.id, CONTENT_TELEGRAM_TOKEN_GROUP),
        "chat_id": channels[0]["chat_id"] if channels else get_user_option_value(db, user.id, CONTENT_TELEGRAM_CHAT_GROUP),
        "channels_text": format_content_telegram_channel_lines(channels),
        "channels": channels,
        "premium_emojis_text": format_content_premium_emoji_lines(premium_entries),
        "premium_emojis": premium_entries,
    }


def mask_secret_value(value: str | None) -> str:
    raw = (value or "").strip()
    if len(raw) <= 8:
        return "•" * len(raw) if raw else ""
    return f"{raw[:4]}{'•' * max(4, len(raw) - 8)}{raw[-4:]}"


def normalize_telegram_target(value: str | None) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    if raw.startswith("@"):
        return raw
    if raw.startswith("-100") and raw[1:].isdigit():
        return raw
    if raw.lstrip("-").isdigit():
        return raw
    if raw.lower().startswith(("https://t.me/", "http://t.me/", "t.me/")):
        parsed = build_external_url(raw)
        try:
            path = urlparse(parsed).path.strip("/")
        except ValueError:
            path = ""
        if path:
            return f"@{path.split('/', 1)[0]}"
    if re.fullmatch(r"[A-Za-z0-9_]{3,}", raw):
        return f"@{raw}"
    return raw


def telegram_custom_api_url(token: str, method: str) -> str:
    return f"https://api.telegram.org/bot{token}/{method}"


def telegram_custom_request(
    token: str,
    method: str,
    *,
    json_payload: dict[str, Any] | None = None,
    data_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    response = requests.post(
        telegram_custom_api_url(token, method),
        json=json_payload,
        data=data_payload,
        timeout=20,
    )
    try:
        payload = response.json() if response.content else {}
    except ValueError as exc:
        raise RuntimeError("Telegram вернул некорректный ответ.") from exc
    if not response.ok or not payload.get("ok"):
        description = str(payload.get("description") or f"HTTP {response.status_code}")
        raise RuntimeError(f"Telegram API: {description}")
    return payload


def strip_telegram_html(text_value: str) -> str:
    return re.sub(r"<[^>]+>", "", text_value or "")


def append_rubric_tag_to_message(message_html: str, rubric_tag: str | None) -> str:
    normalized_tag = normalize_content_rubric_tag(rubric_tag)
    if not normalized_tag:
        return message_html
    plain_text = strip_telegram_html(message_html)
    if normalized_tag.casefold() in plain_text.casefold():
        return message_html
    suffix = html.escape(normalized_tag)
    if not message_html.strip():
        return suffix
    return f"{message_html.rstrip()}\n\n{suffix}"


def content_post_targets_telegram(post: ContentPlanPost) -> bool:
    return any(str(item).strip().casefold() == "тг" for item in as_list(post.socials_json))


def content_post_publish_datetime(post: ContentPlanPost) -> datetime | None:
    if not post.publish_date:
        return None
    publish_time = parse_time_hhmm(post.publish_time or "")
    if not publish_time:
        return None
    hour_text, minute_text = publish_time.split(":", 1)
    return datetime(
        post.publish_date.year,
        post.publish_date.month,
        post.publish_date.day,
        int(hour_text),
        int(minute_text),
        tzinfo=SITE_TIMEZONE,
    )


def normalize_local_media_reference(value: str | None) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""

    path_value = ""
    if raw.startswith("/media/"):
        path_value = raw
    elif raw.lower().startswith(("http://", "https://")):
        try:
            parsed = urlparse(raw)
            site_parsed = urlparse(SITE_URL)
        except ValueError:
            return ""
        if parsed.netloc and site_parsed.netloc and parsed.netloc.casefold() != site_parsed.netloc.casefold():
            return ""
        path_value = parsed.path or ""
    if not path_value.startswith("/media/"):
        return ""

    filename = unquote(path_value.removeprefix("/media/")).strip()
    if not filename or "/" in filename:
        return ""
    safe_name = safe_media_filename(filename)
    return f"/media/{safe_name}"


def local_media_reference_to_path(reference: str | None) -> Path | None:
    normalized_ref = normalize_local_media_reference(reference)
    if not normalized_ref:
        return None
    return media_storage_path() / safe_media_filename(normalized_ref.removeprefix("/media/"))


def mark_content_post_telegram_published(
    post: ContentPlanPost,
    *,
    message_id: str | None = None,
    channel_message_ids: list[dict[str, str]] | None = None,
    rubric_tag: str | None = None,
) -> None:
    normalized_tag = normalize_content_rubric_tag(rubric_tag) or normalize_content_rubric_tag(post.rubric_tag)
    if normalized_tag:
        post.rubric_tag = normalized_tag
    message_id_rows = [
        {
            "chat_id": str(item.get("chat_id") or "").strip(),
            "title": str(item.get("title") or "").strip(),
            "message_id": str(item.get("message_id") or "").strip(),
        }
        for item in as_list(channel_message_ids)
        if isinstance(item, dict) and str(item.get("chat_id") or "").strip() and str(item.get("message_id") or "").strip()
    ]
    last_message_id = (message_id or "").strip()
    if not last_message_id and message_id_rows:
        last_message_id = str(message_id_rows[-1].get("message_id") or "").strip()
    post.telegram_message_id = last_message_id or None
    post.telegram_message_ids_json = message_id_rows
    post.telegram_published_at = datetime.utcnow()
    post.telegram_cleanup_photos_json = as_list(post.telegram_photos_json)
    if normalize_content_status(post.status) != "published":
        post.status = "published"


def publish_content_post_to_telegram(
    *,
    token: str,
    chat_id: str,
    post: ContentPlanPost,
    rubric_tag: str | None = None,
) -> str:
    html_body = (post.telegram_body_html or "").strip()
    photo_urls = [build_external_url(item) for item in as_list(post.telegram_photos_json) if build_external_url(item)]
    fallback_text = html.escape(post.title or "Пост")
    message_html = append_rubric_tag_to_message(html_body or fallback_text, rubric_tag)
    if len(strip_telegram_html(message_html)) > 4096:
        raise RuntimeError("Сообщение для Telegram слишком длинное (максимум 4096 символов без учета HTML-тегов).")

    last_message_id = ""
    if photo_urls:
        first_photo = photo_urls[0]
        remaining = photo_urls[1:]
        caption_text = message_html if len(strip_telegram_html(message_html)) <= 1024 else ""
        photo_payload = telegram_custom_request(
            token,
            "sendPhoto",
            json_payload={
                "chat_id": chat_id,
                "photo": first_photo,
                "caption": caption_text,
                "parse_mode": "HTML" if caption_text else None,
                "show_caption_above_media": True if caption_text else None,
            },
        )
        result = photo_payload.get("result") or {}
        last_message_id = str(result.get("message_id") or "")
        for url in remaining:
            extra_payload = telegram_custom_request(
                token,
                "sendPhoto",
                json_payload={"chat_id": chat_id, "photo": url},
            )
            extra_result = extra_payload.get("result") or {}
            last_message_id = str(extra_result.get("message_id") or last_message_id)
        if not caption_text:
            text_payload = telegram_custom_request(
                token,
                "sendMessage",
                json_payload={
                    "chat_id": chat_id,
                    "text": message_html,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": False,
                },
            )
            text_result = text_payload.get("result") or {}
            last_message_id = str(text_result.get("message_id") or last_message_id)
        return last_message_id

    text_payload = telegram_custom_request(
        token,
        "sendMessage",
        json_payload={
            "chat_id": chat_id,
            "text": message_html,
            "parse_mode": "HTML",
            "disable_web_page_preview": False,
        },
    )
    text_result = text_payload.get("result") or {}
    return str(text_result.get("message_id") or "")


def publish_content_post_to_telegram_channels(
    *,
    token: str,
    channels: list[dict[str, str]],
    post: ContentPlanPost,
    rubric_tag: str | None = None,
) -> tuple[list[dict[str, str]], list[str]]:
    successful: list[dict[str, str]] = []
    errors: list[str] = []
    for channel in channels:
        chat_id = str(channel.get("chat_id") or "").strip()
        title = str(channel.get("title") or "").strip() or chat_id
        if not chat_id:
            continue
        try:
            message_id = publish_content_post_to_telegram(
                token=token,
                chat_id=chat_id,
                post=post,
                rubric_tag=rubric_tag,
            )
        except RuntimeError as exc:
            errors.append(f"{title}: {exc}")
            continue
        successful.append(
            {
                "chat_id": chat_id,
                "title": title,
                "message_id": message_id,
            }
        )
    return successful, errors


def hsl_to_hex(hue: float, saturation: float, lightness: float) -> str:
    r, g, b = colorsys.hls_to_rgb(hue / 360.0, lightness, saturation)
    return f"#{int(r * 255):02x}{int(g * 255):02x}{int(b * 255):02x}"


def rubric_color_map(rubrics: list[str]) -> dict[str, str]:
    color_map: dict[str, str] = {}
    for index, rubric in enumerate(rubrics):
        if index < len(CONTENT_RUBRIC_PALETTE):
            color_map[rubric] = CONTENT_RUBRIC_PALETTE[index]
            continue
        hue = abs(hash(rubric.casefold())) % 360
        color_map[rubric] = hsl_to_hex(hue, 0.58, 0.66)
    return color_map


def content_calendar_grid(
    year: int,
    month: int,
    content_rows: list[dict[str, Any]],
) -> list[list[dict[str, Any]]]:
    calendar_builder = calendar.Calendar(firstweekday=0)
    matrix = calendar_builder.monthdayscalendar(year, month)

    day_colors: dict[int, list[str]] = defaultdict(list)
    day_items: dict[int, list[dict[str, str]]] = defaultdict(list)
    for row in content_rows:
        publish_date = row.get("date")
        if not isinstance(publish_date, date):
            continue
        if publish_date.year != year or publish_date.month != month:
            continue
        color = str(row.get("rubric_color") or "").strip()
        if color and color not in day_colors[publish_date.day]:
            day_colors[publish_date.day].append(color)
        title = str(row.get("title") or "").strip() or "Без названия"
        time_text = str(row.get("time") or "").strip()
        socials_text = str(row.get("socials_text") or "").strip()
        socials_label = socials_text if socials_text and socials_text != "—" else "другое"
        schedule_label = f"{time_text} • {socials_label}" if time_text else socials_label
        day_items[publish_date.day].append(
            {
                "title": title,
                "socials": socials_label,
                "time": time_text,
                "schedule": schedule_label,
            }
        )

    weeks: list[list[dict[str, Any]]] = []
    for week in matrix:
        week_cells: list[dict[str, Any]] = []
        for day_value in week:
            if day_value <= 0:
                week_cells.append({"day": 0, "bg_style": "", "content_items": []})
                continue
            colors = day_colors.get(day_value, [])
            bg_style = ""
            if len(colors) == 1:
                color = colors[0]
                bg_style = f"background: linear-gradient(0deg, {color}44, {color}44), #f8fafc;"
            elif len(colors) > 1:
                gradient_parts: list[str] = []
                color_count = len(colors)
                for idx, color in enumerate(colors):
                    start = (idx * 100.0) / color_count
                    end = ((idx + 1) * 100.0) / color_count
                    gradient_parts.append(f"{color}66 {start:.2f}%")
                    gradient_parts.append(f"{color}66 {end:.2f}%")
                bg_style = "background: linear-gradient(90deg, " + ", ".join(gradient_parts) + ");"
            week_cells.append({"day": day_value, "bg_style": bg_style, "content_items": day_items.get(day_value, [])})
        weeks.append(week_cells)
    return weeks


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


def looks_like_telegram_username(value: str | None) -> bool:
    raw = (value or "").strip()
    return bool(re.fullmatch(r"@[A-Za-z0-9_]{3,}", raw))


def normalize_url_with_scheme(value: str | None) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    if raw.startswith("/"):
        return f"{SITE_URL}{raw}"
    if looks_like_url(raw):
        return raw
    if raw.lower().startswith(("www.", "t.me/", "telegram.me/", "vk.com/", "m.vk.com/")):
        return f"https://{raw}"
    return raw


def classify_external_url(value: str | None) -> str:
    normalized = normalize_url_with_scheme(value)
    if not normalized:
        return ""
    if looks_like_telegram_username(normalized):
        return "telegram"
    try:
        parsed = urlparse(normalized)
    except ValueError:
        return ""
    host = (parsed.netloc or "").lower()
    if "t.me" in host or "telegram.me" in host:
        return "telegram"
    if host.endswith("vk.com"):
        return "vk"
    if host:
        return "site"
    return ""


def build_external_url(value: str | None) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    if looks_like_telegram_username(raw):
        return f"https://t.me/{raw[1:]}"
    return normalize_url_with_scheme(raw)


def button_label_for_external_url(value: str | None) -> str:
    kind = classify_external_url(value)
    if kind == "telegram":
        return "Телеграм"
    if kind == "vk":
        return "VK"
    if kind == "site":
        return "Сайт"
    return ""


def _trim_link_trailing_punctuation(value: str) -> str:
    trimmed = value.rstrip()
    while trimmed and trimmed[-1] in ".,;!?)]}>»":
        trimmed = trimmed[:-1]
    return trimmed


def extract_urls_from_text(value: str | None) -> list[str]:
    text_value = (value or "").strip()
    if not text_value:
        return []
    pattern = re.compile(r"((?:https?://|www\.|t\.me/|telegram\.me/|vk\.com/|m\.vk\.com/|/media/)[^\s<]+)", re.IGNORECASE)
    found: list[str] = []
    for match in pattern.finditer(text_value):
        candidate = _trim_link_trailing_punctuation(match.group(1))
        built = build_external_url(candidate)
        if built:
            found.append(built)
    if looks_like_telegram_username(text_value):
        found.append(build_external_url(text_value))
    return merge_unique(found)


def external_contact_buttons(*values: Any, include_site: bool = True) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for raw_value in values:
        if raw_value is None:
            continue
        if isinstance(raw_value, (list, tuple, set)):
            for nested in raw_value:
                for row in external_contact_buttons(nested, include_site=include_site):
                    key = (row["label"], row["url"])
                    if key not in seen:
                        seen.add(key)
                        rows.append(row)
            continue
        text_value = str(raw_value).strip()
        if not text_value:
            continue
        candidates = [build_external_url(text_value)] if (
            looks_like_telegram_username(text_value) or classify_external_url(text_value)
        ) else []
        candidates.extend(extract_urls_from_text(text_value))
        for candidate in merge_unique(candidates):
            kind = classify_external_url(candidate)
            if not kind:
                continue
            if kind == "site" and not include_site:
                continue
            label = button_label_for_external_url(candidate)
            key = (label, candidate)
            if label and key not in seen:
                seen.add(key)
                rows.append({"label": label, "url": candidate})
    return rows


def static_pixel_emoji_root() -> Path:
    return Path("app/static/pixel-emoji").resolve()


def slugify_pixel_emoji_code(relative_path: str) -> str:
    stem = Path(relative_path).with_suffix("").as_posix()
    slug = re.sub(r"[^a-z0-9]+", "-", stem.casefold()).strip("-")
    return slug or "emoji"


def build_pixel_emoji_catalog() -> list[dict[str, str]]:
    root = static_pixel_emoji_root()
    if not root.exists():
        return []
    rows: list[dict[str, str]] = []
    for path in sorted(root.rglob("*.png")):
        relative = path.relative_to(root)
        relative_posix = relative.as_posix()
        rows.append(
            {
                "code": slugify_pixel_emoji_code(relative_posix),
                "label": relative.stem,
                "url": f"/static/pixel-emoji/{relative_posix}",
            }
        )
    return rows


PIXEL_EMOJI_CATALOG = build_pixel_emoji_catalog()
PIXEL_EMOJI_BY_CODE = {item["code"]: item for item in PIXEL_EMOJI_CATALOG}
TEXT_RENDER_TOKEN_RE = re.compile(
    r"(\[\[emoji:(?P<emoji>[a-z0-9-]+)\]\]|(?P<url>(?:https?://|www\.|t\.me/|telegram\.me/|vk\.com/|m\.vk\.com/|/media/)[^\s<]+))",
    re.IGNORECASE,
)


def render_text_content(value: str | None) -> Markup:
    text_value = (value or "")
    if not text_value:
        return Markup("")

    parts: list[str] = []
    last_end = 0
    for match in TEXT_RENDER_TOKEN_RE.finditer(text_value):
        if match.start() > last_end:
            parts.append(html.escape(text_value[last_end:match.start()]).replace("\n", "<br>"))
        emoji_code = match.group("emoji")
        raw_url = match.group("url")
        if emoji_code:
            emoji = PIXEL_EMOJI_BY_CODE.get(emoji_code.casefold())
            if emoji:
                parts.append(
                    '<img class="inline-pixel-emoji" src="{src}" alt="{alt}" title="{alt}" loading="lazy" />'.format(
                        src=html.escape(emoji["url"], quote=True),
                        alt=html.escape(emoji["label"], quote=True),
                    )
                )
            else:
                parts.append(html.escape(match.group(0)))
        elif raw_url:
            trimmed = _trim_link_trailing_punctuation(raw_url)
            href = build_external_url(trimmed)
            if href:
                parts.append(
                    '<a href="{href}" target="_blank" rel="noreferrer">{label}</a>'.format(
                        href=html.escape(href, quote=True),
                        label=html.escape(trimmed),
                    )
                )
            else:
                parts.append(html.escape(raw_url))
        last_end = match.end()

    if last_end < len(text_value):
        parts.append(html.escape(text_value[last_end:]).replace("\n", "<br>"))
    return Markup("".join(parts))


def build_text_preview(value: str | None, limit: int = 200) -> str:
    if limit <= 0:
        return ""

    compact_value = "\n".join(line.strip() for line in str(value or "").splitlines() if line.strip()).strip()
    if not compact_value:
        return ""
    if len(compact_value) <= limit:
        return compact_value

    truncated = compact_value[:limit].rstrip()
    word_safe = re.sub(r"\s+\S*$", "", truncated).rstrip()
    if word_safe and len(word_safe) >= max(20, limit // 3):
        truncated = word_safe
    return truncated + "…"


def replace_pixel_emoji_tokens_for_bots(value: str | None) -> str:
    text_value = (value or "")
    if not text_value:
        return ""

    def repl(match: re.Match[str]) -> str:
        emoji_code = (match.group(1) or "").casefold()
        emoji = PIXEL_EMOJI_BY_CODE.get(emoji_code)
        if emoji:
            return f":{emoji['label']}:"
        return match.group(0)

    return re.sub(r"\[\[emoji:([a-z0-9-]+)\]\]", repl, text_value, flags=re.IGNORECASE)


templates.env.filters["render_text"] = render_text_content
templates.env.filters["preview_text"] = build_text_preview
templates.env.filters["urlencode"] = lambda value: quote(str(value or ""))


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


def media_storage_path() -> Path:
    custom_path = os.getenv("MEDIA_DIR", "").strip()
    if custom_path:
        path = Path(custom_path).expanduser().resolve()
        path.mkdir(parents=True, exist_ok=True)
        return path

    data_dir = Path("/data")
    if data_dir.exists() and os.access(data_dir, os.W_OK):
        path = (data_dir / "media").resolve()
        path.mkdir(parents=True, exist_ok=True)
        return path

    path = Path("./app/static/media").resolve()
    path.mkdir(parents=True, exist_ok=True)
    return path


def safe_media_filename(value: str) -> str:
    name = (value or "").strip()
    if not re.fullmatch(r"[A-Za-z0-9._-]+", name):
        raise HTTPException(status_code=404, detail="Файл не найден.")
    return name


def _resize_to_width(image: Image.Image, width: int) -> Image.Image:
    if image.width <= width:
        return image.copy()
    ratio = width / float(image.width)
    new_height = max(1, int(round(image.height * ratio)))
    return image.resize((width, new_height), Image.Resampling.LANCZOS)


def _resize_to_max_side(image: Image.Image, max_side: int) -> Image.Image:
    longest_side = max(image.width, image.height)
    if longest_side <= max_side:
        return image.copy()
    ratio = max_side / float(longest_side)
    new_width = max(1, int(round(image.width * ratio)))
    new_height = max(1, int(round(image.height * ratio)))
    return image.resize((new_width, new_height), Image.Resampling.LANCZOS)


def prepare_content_image_upload(raw_bytes: bytes) -> tuple[bytes, int, int, str, str]:
    if not raw_bytes:
        raise ValueError("Файл пуст.")

    try:
        with Image.open(io.BytesIO(raw_bytes)) as source:
            source_format = (source.format or "").upper()
            prepared = ImageOps.exif_transpose(source)
            width, height = prepared.size
            has_alpha = "A" in prepared.getbands()
            if max(width, height) <= CONTENT_TELEGRAM_IMAGE_MAX_SIDE and source_format in {"JPEG", "PNG", "WEBP"}:
                file_ext = ".jpg" if source_format == "JPEG" else f".{source_format.lower()}"
                return raw_bytes, width, height, file_ext, source_format.lower()

            resized = _resize_to_max_side(prepared, CONTENT_TELEGRAM_IMAGE_MAX_SIDE)
            save_target = resized
            if source_format == "PNG":
                if save_target.mode not in {"RGBA", "LA"}:
                    save_target = save_target.convert("RGBA")
                output_format = "PNG"
                file_ext = ".png"
                save_kwargs: dict[str, Any] = {"format": output_format}
            elif source_format == "WEBP":
                if has_alpha and save_target.mode != "RGBA":
                    save_target = save_target.convert("RGBA")
                elif save_target.mode in {"P", "L", "CMYK"}:
                    save_target = save_target.convert("RGB")
                elif save_target.mode not in {"RGB", "RGBA"}:
                    save_target = save_target.convert("RGB")
                output_format = "WEBP"
                file_ext = ".webp"
                save_kwargs = {"format": output_format, "quality": 95, "method": 6}
            elif has_alpha:
                if save_target.mode not in {"RGBA", "LA"}:
                    save_target = save_target.convert("RGBA")
                output_format = "PNG"
                file_ext = ".png"
                save_kwargs = {"format": output_format}
            else:
                if save_target.mode in {"P", "L", "CMYK", "RGBA", "LA"}:
                    save_target = save_target.convert("RGB")
                elif save_target.mode != "RGB":
                    save_target = save_target.convert("RGB")
                output_format = "JPEG"
                file_ext = ".jpg"
                save_kwargs = {"format": output_format, "quality": 95, "optimize": True}
    except UnidentifiedImageError as exc:
        raise ValueError("Неподдерживаемый формат изображения.") from exc
    except OSError as exc:
        raise ValueError("Не удалось прочитать изображение.") from exc

    buffer = io.BytesIO()
    save_target.save(buffer, **save_kwargs)
    return buffer.getvalue(), save_target.width, save_target.height, file_ext, output_format.lower()


def compress_image_to_webp(
    raw_bytes: bytes,
    *,
    max_output_bytes: int = MAX_GALLERY_IMAGE_BYTES,
    max_width: int = MAX_GALLERY_IMAGE_WIDTH,
) -> tuple[bytes, int, int]:
    if not raw_bytes:
        raise ValueError("Файл пуст.")

    try:
        with Image.open(io.BytesIO(raw_bytes)) as source:
            prepared = ImageOps.exif_transpose(source)
            if prepared.mode in {"P", "L", "CMYK"}:
                prepared = prepared.convert("RGB")
            elif prepared.mode not in {"RGB", "RGBA"}:
                prepared = prepared.convert("RGB")
    except UnidentifiedImageError as exc:
        raise ValueError("Неподдерживаемый формат изображения.") from exc
    except OSError as exc:
        raise ValueError("Не удалось прочитать изображение.") from exc

    start_width = min(prepared.width, max_width)
    widths: list[int] = []
    cursor = start_width
    while cursor >= 32:
        widths.append(cursor)
        if cursor <= 64:
            break
        cursor = max(32, int(cursor * 0.85))
    widths = list(dict.fromkeys(widths))

    qualities = [82, 74, 66, 58, 50, 42, 34, 28, 22, 16, 10, 6]
    best_blob: bytes | None = None
    best_dims = (prepared.width, prepared.height)

    for width in widths:
        resized = _resize_to_width(prepared, width)
        for quality in qualities:
            buffer = io.BytesIO()
            resized.save(
                buffer,
                format="WEBP",
                quality=quality,
                method=6,
            )
            blob = buffer.getvalue()
            if best_blob is None or len(blob) < len(best_blob):
                best_blob = blob
                best_dims = (resized.width, resized.height)
            if len(blob) <= max_output_bytes:
                return blob, resized.width, resized.height

    if best_blob is not None and len(best_blob) <= max_output_bytes:
        return best_blob, best_dims[0], best_dims[1]
    raise ValueError("Не удалось сжать изображение до 30 КБ. Попробуйте другое изображение.")


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
    reply_to_notification_id: int | None = None,
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
    if reply_to_notification_id is None:
        conditions.append(FestivalNotification.reply_to_notification_id.is_(None))
    else:
        conditions.append(FestivalNotification.reply_to_notification_id == reply_to_notification_id)

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
            reply_to_notification_id=reply_to_notification_id,
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


def is_external_bot_eligible_notification(message: str | None) -> bool:
    text_value = (message or "").strip()
    if not text_value:
        return False
    lower = text_value.casefold()
    if is_pigeon_message(text_value):
        return True
    if is_shared_card_notification_message(text_value):
        return True
    if "вам назначено задание" in lower:
        return True
    if "новый комментарий в вашей карточке мастера" in lower:
        return True
    if "новый комментарий в вашей карточке студии" in lower:
        return True
    if "новый комментарий в вашем объявлении поиска" in lower:
        return True
    return False


def is_telegram_eligible_notification(message: str | None) -> bool:
    return is_external_bot_eligible_notification(message)


def telegram_api_url(method: str) -> str:
    return f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}"


def telegram_send_message(chat_id: str, message: str) -> tuple[bool, int | None]:
    if not TELEGRAM_BOT_ENABLED or not TELEGRAM_BOT_TOKEN:
        return False, None
    payload = {
        "chat_id": chat_id,
        "text": message,
        "disable_web_page_preview": True,
    }
    try:
        response = requests.post(
            telegram_api_url("sendMessage"),
            json=payload,
            timeout=20,
        )
        response_payload = response.json() if response.content else {}
    except (requests.RequestException, ValueError):
        return False, None
    if not response.ok or not response_payload.get("ok"):
        return False, int(response_payload.get("error_code") or response.status_code or 0)
    return True, None


def telegram_delete_message(chat_id: str, message_id: int) -> bool:
    if not TELEGRAM_BOT_ENABLED or not TELEGRAM_BOT_TOKEN:
        return False
    payload = {
        "chat_id": chat_id,
        "message_id": int(message_id),
    }
    try:
        response = requests.post(
            telegram_api_url("deleteMessage"),
            json=payload,
            timeout=15,
        )
        response_payload = response.json() if response.content else {}
    except (requests.RequestException, ValueError):
        return False
    return bool(response.ok and response_payload.get("ok"))


def telegram_get_updates(offset: int) -> list[dict[str, Any]]:
    if not TELEGRAM_BOT_ENABLED or not TELEGRAM_BOT_TOKEN:
        return []
    params = {
        "timeout": TELEGRAM_POLL_TIMEOUT_SECONDS,
        "offset": offset,
    }
    try:
        response = requests.get(
            telegram_api_url("getUpdates"),
            params=params,
            timeout=TELEGRAM_POLL_TIMEOUT_SECONDS + 10,
        )
        payload = response.json() if response.content else {}
    except (requests.RequestException, ValueError):
        return []
    if not response.ok or not payload.get("ok"):
        return []
    results = payload.get("result")
    return results if isinstance(results, list) else []


def reset_telegram_auth(chat_id: str) -> None:
    with telegram_auth_state_lock:
        telegram_auth_state.pop(chat_id, None)


def set_telegram_auth_step(chat_id: str, *, step: str, username: str = "") -> None:
    with telegram_auth_state_lock:
        telegram_auth_state[chat_id] = {
            "step": step,
            "username": username,
        }


def get_telegram_auth_step(chat_id: str) -> dict[str, str]:
    with telegram_auth_state_lock:
        return dict(telegram_auth_state.get(chat_id, {}))


def start_telegram_auth(chat_id: str, *, with_greeting: bool = False) -> None:
    set_telegram_auth_step(chat_id, step="username", username="")
    if with_greeting:
        telegram_send_message(
            chat_id,
            (
                "Вас приветствует помощник по оповещениям от портала Cosplay Planner. "
                "Пожалуйста, пройдите авторизацию. После нее вам будут доступны оповещения "
                "о входящих сообщениях на сайте, заданиях в коллективных проектах, информация "
                "о добавлении в коспланы и комментариях на карточке мастера/студии. "
                "Приятного использования!"
            ),
        )
    telegram_send_message(
        chat_id,
        "Введите ваш ник на сайте (можно @username или cosplay_nick).",
    )


def resolve_user_for_telegram_login(db: Session, raw_username: str) -> User | None:
    normalized = normalize_username(raw_username).casefold()
    if not normalized:
        return None
    user = db.execute(select(User).where(func.lower(User.username) == normalized)).scalar_one_or_none()
    if user:
        return user
    return db.execute(select(User).where(func.lower(User.cosplay_nick) == normalized)).scalar_one_or_none()


def verify_user_telegram_secret_code(user: User, raw_code: str) -> bool:
    secret_hash = (user.telegram_secret_code_hash or "").strip()
    if not secret_hash:
        return False
    try:
        return bool(password_context.verify(raw_code, secret_hash))
    except Exception:
        return False


def handle_telegram_auth_message(chat_id: str, text_value: str) -> bool:
    state = get_telegram_auth_step(chat_id)
    step = state.get("step", "")
    if step == "username":
        entered_username = normalize_username(text_value)
        if not entered_username:
            telegram_send_message(chat_id, "Ник не распознан. Введите ник ещё раз.")
            return True
        set_telegram_auth_step(chat_id, step="secret_code", username=entered_username)
        telegram_send_message(chat_id, "Теперь отправьте секретный код для бота из профиля на сайте.")
        return True

    if step == "secret_code":
        entered_secret_code = text_value or ""
        username = state.get("username", "")
        if not username:
            start_telegram_auth(chat_id)
            return True
        with SessionLocal() as db:
            user = resolve_user_for_telegram_login(db, username)
            if not user:
                telegram_send_message(chat_id, "Неверный ник или секретный код. Попробуйте снова.")
                return True
            if not (user.telegram_secret_code_hash or "").strip():
                telegram_send_message(
                    chat_id,
                    "Для этого аккаунта не задан секретный код. Задайте его в профиле на сайте и попробуйте снова.",
                )
                return True
            if not verify_user_telegram_secret_code(user, entered_secret_code):
                telegram_send_message(chat_id, "Неверный ник или секретный код. Попробуйте снова.")
                return True

            linked_users = db.execute(
                select(User).where(
                    User.telegram_chat_id == chat_id,
                    User.id != user.id,
                )
            ).scalars().all()
            for linked_user in linked_users:
                linked_user.telegram_chat_id = None
                linked_user.telegram_linked_at = None

            user.telegram_chat_id = chat_id
            user.telegram_linked_at = datetime.utcnow()
            db.commit()

        reset_telegram_auth(chat_id)
        telegram_send_message(chat_id, "Авторизация успешно пройдена!")
        return True

    return False


def handle_telegram_reply_command(chat_id: str, text_value: str) -> bool:
    command_match = re.match(r"^/reply(?:@\w+)?\s+(.+)$", text_value.strip(), flags=re.IGNORECASE | re.DOTALL)
    if not command_match:
        return False
    reply_body = command_match.group(1).strip()
    if not reply_body:
        telegram_send_message(chat_id, "После /reply укажите текст ответа.")
        return True
    with SessionLocal() as db:
        sender = db.execute(select(User).where(User.telegram_chat_id == chat_id)).scalar_one_or_none()
        if not sender:
            telegram_send_message(chat_id, "Сначала пройдите авторизацию через /start или /login.")
            return True
        latest_note = latest_pigeon_notification_for_reply(db, sender.id)
        if not latest_note or latest_note.from_user_id is None:
            telegram_send_message(chat_id, "Не найдено входящих голубей, на которые можно ответить.")
            return True
        recipient = db.get(User, latest_note.from_user_id)
        if not recipient:
            telegram_send_message(chat_id, "Получатель ответа не найден.")
            return True
        send_pigeon_notification(
            db,
            sender=sender,
            recipient=recipient,
            message_body=reply_body,
            reply_to_notification_id=latest_note.id,
        )
        db.commit()
    telegram_send_message(chat_id, f"Ответ отправлен пользователю @{preferred_user_alias(recipient)}.")
    return True


def handle_telegram_update(update: dict[str, Any]) -> None:
    message = update.get("message")
    if not isinstance(message, dict):
        return
    chat = message.get("chat") or {}
    chat_id_raw = chat.get("id")
    if chat_id_raw is None:
        return
    chat_id = str(chat_id_raw)
    message_id = int(message.get("message_id") or 0)

    text_value = str(message.get("text") or "").strip()
    if not text_value:
        telegram_send_message(chat_id, "Поддерживаются только текстовые сообщения.")
        return

    lowered = text_value.casefold()
    if lowered == "/start":
        start_telegram_auth(chat_id, with_greeting=True)
        return
    if lowered == "/login":
        start_telegram_auth(chat_id, with_greeting=False)
        return
    if lowered == "/logout":
        with SessionLocal() as db:
            linked_users = db.execute(select(User).where(User.telegram_chat_id == chat_id)).scalars().all()
            for linked_user in linked_users:
                linked_user.telegram_chat_id = None
                linked_user.telegram_linked_at = None
            if linked_users:
                db.commit()
        reset_telegram_auth(chat_id)
        telegram_send_message(chat_id, "Telegram-привязка удалена.")
        return
    if lowered.startswith("/reply"):
        if handle_telegram_reply_command(chat_id, text_value):
            return

    # If user sends secret-code step text, try to remove this message from chat history.
    state = get_telegram_auth_step(chat_id)
    if state.get("step") == "secret_code" and message_id > 0:
        telegram_delete_message(chat_id, message_id)

    if handle_telegram_auth_message(chat_id, text_value):
        return

    telegram_send_message(
        chat_id,
        "Чтобы подключить уведомления, отправьте /start или /login.",
    )


def format_external_bot_notification_message(message: str | None) -> str:
    text_value = replace_pixel_emoji_tokens_for_bots(message)
    text_value = (text_value or "").strip()
    if not text_value:
        return ""
    pigeon_payload = parse_pigeon_message(text_value)
    if pigeon_payload:
        sender_alias, body = pigeon_payload
        body_text = body.strip() or "Без текста"
        return (
            f"Вам пришло сообщение от пользователя @{sender_alias} с текстом:\n\n{body_text}\n\n"
            "Чтобы ответить, отправьте: /reply ваш текст"
        )
    return text_value


def format_telegram_notification_message(message: str | None) -> str:
    return format_external_bot_notification_message(message)


def dispatch_telegram_notifications() -> None:
    if not TELEGRAM_BOT_ENABLED:
        return
    with SessionLocal() as db:
        users = db.execute(select(User).where(User.telegram_chat_id.is_not(None))).scalars().all()
        if not users:
            return
        now_utc = datetime.utcnow()

        for user in users:
            chat_id = (user.telegram_chat_id or "").strip()
            if not chat_id:
                continue

            stmt = (
                select(FestivalNotification)
                .where(
                    FestivalNotification.user_id == user.id,
                    FestivalNotification.telegram_sent_at.is_(None),
                )
                .order_by(FestivalNotification.created_at.asc(), FestivalNotification.id.asc())
                .limit(TELEGRAM_DISPATCH_LIMIT)
            )
            if user.telegram_linked_at:
                stmt = stmt.where(FestivalNotification.created_at >= user.telegram_linked_at)

            notifications = db.execute(stmt).scalars().all()
            if not notifications:
                continue

            must_commit = False
            for note in notifications:
                if not is_telegram_eligible_notification(note.message):
                    note.telegram_sent_at = now_utc
                    must_commit = True
                    continue

                telegram_text = format_telegram_notification_message(note.message)
                if not telegram_text:
                    note.telegram_sent_at = now_utc
                    must_commit = True
                    continue

                ok, error_code = telegram_send_message(chat_id, telegram_text)
                if ok:
                    note.telegram_sent_at = now_utc
                    must_commit = True
                    continue

                if error_code in {401, 403}:
                    user.telegram_chat_id = None
                    user.telegram_linked_at = None
                    must_commit = True
                    break

            if must_commit:
                db.commit()


def telegram_bot_loop() -> None:
    if not TELEGRAM_BOT_ENABLED:
        return
    offset = 0
    while True:
        try:
            updates = telegram_get_updates(offset)
            for update in updates:
                update_id = int(update.get("update_id") or 0)
                if update_id >= offset:
                    offset = update_id + 1
                handle_telegram_update(update)
            dispatch_telegram_notifications()
        except Exception:
            # Telegram loop must not crash app process.
            pass
        time.sleep(TELEGRAM_LOOP_SLEEP_SECONDS)


def start_telegram_worker() -> None:
    global telegram_worker_thread
    if not TELEGRAM_BOT_ENABLED:
        return
    with telegram_worker_lock:
        if telegram_worker_thread and telegram_worker_thread.is_alive():
            return
        telegram_worker_thread = threading.Thread(
            target=telegram_bot_loop,
            name="telegram-bot-worker",
            daemon=True,
        )
        telegram_worker_thread.start()


def cleanup_expired_content_telegram_media(db: Session) -> None:
    cutoff = datetime.utcnow() - timedelta(hours=CONTENT_TELEGRAM_IMAGE_RETENTION_HOURS)
    posts = db.execute(select(ContentPlanPost)).scalars().all()
    if not posts:
        return

    protected_refs: set[str] = set()
    stale_posts: list[ContentPlanPost] = []
    for post in posts:
        current_refs = {
            normalized
            for item in as_list(post.telegram_photos_json)
            if (normalized := normalize_local_media_reference(item))
        }
        cleanup_refs = {
            normalized
            for item in as_list(post.telegram_cleanup_photos_json)
            if (normalized := normalize_local_media_reference(item))
        }
        if not cleanup_refs:
            if current_refs:
                protected_refs.update(current_refs)
            continue
        if post.telegram_published_at and post.telegram_published_at <= cutoff:
            stale_posts.append(post)
            protected_refs.update(current_refs - cleanup_refs)
            continue
        protected_refs.update(current_refs)
        protected_refs.update(cleanup_refs)

    if not stale_posts:
        return

    has_changes = False
    for post in stale_posts:
        snapshot_items = as_list(post.telegram_cleanup_photos_json)
        current_items = as_list(post.telegram_photos_json)
        remaining_snapshot: list[str] = []
        deleted_refs: set[str] = set()
        post_changed = False

        for item in snapshot_items:
            local_ref = normalize_local_media_reference(item)
            if not local_ref:
                post_changed = True
                continue
            if local_ref in protected_refs:
                remaining_snapshot.append(item)
                continue
            file_path = local_media_reference_to_path(local_ref)
            if file_path and file_path.exists():
                try:
                    file_path.unlink()
                except OSError:
                    remaining_snapshot.append(item)
                    continue
            deleted_refs.add(local_ref)
            post_changed = True

        if deleted_refs:
            kept_current_items = [
                item for item in current_items if normalize_local_media_reference(item) not in deleted_refs
            ]
            if kept_current_items != current_items:
                post.telegram_photos_json = kept_current_items
                post_changed = True

        if remaining_snapshot != snapshot_items:
            post.telegram_cleanup_photos_json = remaining_snapshot
            post_changed = True

        if post_changed:
            has_changes = True

    if has_changes:
        db.commit()


def dispatch_scheduled_content_posts() -> None:
    now_local = datetime.now(SITE_TIMEZONE)
    with SessionLocal() as db:
        pending_posts = db.execute(
            select(ContentPlanPost).where(ContentPlanPost.telegram_published_at.is_(None))
        ).scalars().all()
        users_cache: dict[int, User | None] = {}
        settings_cache: dict[int, dict[str, Any]] = {}
        rubric_tag_cache: dict[int, dict[str, str]] = {}
        has_changes = False

        for post in pending_posts:
            if not content_post_targets_telegram(post):
                continue
            publish_at = content_post_publish_datetime(post)
            if publish_at is None or publish_at > now_local:
                continue

            if post.user_id not in users_cache:
                users_cache[post.user_id] = db.get(User, post.user_id)
            user = users_cache.get(post.user_id)
            if not user:
                continue

            if user.id not in settings_cache:
                settings_cache[user.id] = get_content_telegram_settings(user, db)
            telegram_settings = settings_cache[user.id]
            bot_token = str(telegram_settings.get("bot_token") or "").strip()
            available_channels = list(telegram_settings.get("channels") or [])
            if not bot_token or not available_channels:
                continue

            selected_channels = resolve_content_telegram_channels(as_list(post.telegram_channels_json), available_channels)
            if not selected_channels and len(available_channels) == 1:
                selected_channels = [available_channels[0]]
            if not selected_channels:
                continue

            if user.id not in rubric_tag_cache:
                rubric_tag_cache[user.id] = get_content_rubric_tags(db, user.id)
            rubric_tag = normalize_content_rubric_tag(post.rubric_tag) or rubric_tag_cache[user.id].get(post.rubric or "", "")

            sent_messages, _ = publish_content_post_to_telegram_channels(
                token=bot_token,
                channels=selected_channels,
                post=post,
                rubric_tag=rubric_tag,
            )
            if not sent_messages:
                continue

            mark_content_post_telegram_published(
                post,
                channel_message_ids=sent_messages,
                rubric_tag=rubric_tag,
            )
            has_changes = True

        if has_changes:
            db.commit()
        cleanup_expired_content_telegram_media(db)


def content_telegram_loop() -> None:
    while True:
        try:
            dispatch_scheduled_content_posts()
        except Exception:
            pass
        time.sleep(CONTENT_TELEGRAM_LOOP_SLEEP_SECONDS)


def start_content_telegram_worker() -> None:
    global content_telegram_worker_thread
    with content_telegram_worker_lock:
        if content_telegram_worker_thread and content_telegram_worker_thread.is_alive():
            return
        content_telegram_worker_thread = threading.Thread(
            target=content_telegram_loop,
            name="content-telegram-worker",
            daemon=True,
        )
        content_telegram_worker_thread.start()


def vk_bot_api_call(method: str, params: dict[str, Any]) -> dict[str, Any]:
    if not VK_BOT_ENABLED or not VK_BOT_TOKEN:
        return {"error": {"error_code": 0, "error_msg": "VK bot is disabled"}}
    payload = dict(params)
    payload["access_token"] = VK_BOT_TOKEN
    payload["v"] = VK_API_VERSION
    try:
        response = requests.post(
            f"https://api.vk.com/method/{method}",
            data=payload,
            timeout=20,
        )
        return response.json() if response.content else {}
    except (requests.RequestException, ValueError):
        return {"error": {"error_code": 0, "error_msg": "VK API request failed"}}


def vk_bot_send_message(peer_id: str | int, message: str) -> tuple[bool, int | None]:
    if not VK_BOT_ENABLED or not VK_BOT_TOKEN:
        return False, None
    try:
        peer_value = int(str(peer_id).strip())
    except (TypeError, ValueError):
        return False, None

    payload = vk_bot_api_call(
        "messages.send",
        {
            "peer_id": peer_value,
            "message": message,
            "random_id": secrets.randbelow(2_147_483_647),
        },
    )
    error_payload = payload.get("error")
    if isinstance(error_payload, dict):
        return False, int(error_payload.get("error_code") or 0)
    return "response" in payload, None


def reset_vk_bot_auth(sender_id: str) -> None:
    with vk_bot_auth_state_lock:
        vk_bot_auth_state.pop(sender_id, None)


def set_vk_bot_auth_step(sender_id: str, *, step: str, username: str = "") -> None:
    with vk_bot_auth_state_lock:
        vk_bot_auth_state[sender_id] = {
            "step": step,
            "username": username,
        }


def get_vk_bot_auth_step(sender_id: str) -> dict[str, str]:
    with vk_bot_auth_state_lock:
        return dict(vk_bot_auth_state.get(sender_id, {}))


def start_vk_bot_auth(sender_id: str, peer_id: str, *, with_greeting: bool = False) -> None:
    set_vk_bot_auth_step(sender_id, step="username", username="")
    if with_greeting:
        vk_bot_send_message(
            peer_id,
            (
                "Вас приветствует помощник по оповещениям от портала Cosplay Planner. "
                "Пожалуйста, пройдите авторизацию. После нее вам будут доступны оповещения "
                "о входящих сообщениях на сайте, заданиях в коллективных проектах, информация "
                "о добавлении в коспланы и комментариях на карточке мастера/студии. "
                "Приятного использования!"
            ),
        )
    vk_bot_send_message(
        peer_id,
        "Введите ваш ник на сайте (можно @username или cosplay_nick).",
    )


def handle_vk_bot_auth_message(sender_id: str, peer_id: str, text_value: str) -> bool:
    state = get_vk_bot_auth_step(sender_id)
    step = state.get("step", "")
    if step == "username":
        entered_username = normalize_username(text_value)
        if not entered_username:
            vk_bot_send_message(peer_id, "Ник не распознан. Введите ник ещё раз.")
            return True
        set_vk_bot_auth_step(sender_id, step="secret_code", username=entered_username)
        vk_bot_send_message(peer_id, "Теперь отправьте секретный код для бота из профиля на сайте.")
        return True

    if step == "secret_code":
        entered_secret_code = text_value or ""
        username = state.get("username", "")
        if not username:
            start_vk_bot_auth(sender_id, peer_id)
            return True

        with SessionLocal() as db:
            user = resolve_user_for_telegram_login(db, username)
            if not user or not (user.telegram_secret_code_hash or "").strip():
                vk_bot_send_message(
                    peer_id,
                    "Неверный ник или секретный код. Попробуйте снова.",
                )
                return True
            if not verify_user_telegram_secret_code(user, entered_secret_code):
                vk_bot_send_message(
                    peer_id,
                    "Неверный ник или секретный код. Попробуйте снова.",
                )
                return True

            linked_users = db.execute(
                select(User).where(
                    or_(
                        User.vk_bot_user_id == sender_id,
                        User.vk_bot_peer_id == peer_id,
                    ),
                    User.id != user.id,
                )
            ).scalars().all()
            for linked_user in linked_users:
                linked_user.vk_bot_user_id = None
                linked_user.vk_bot_peer_id = None
                linked_user.vk_bot_linked_at = None

            user.vk_bot_user_id = sender_id
            user.vk_bot_peer_id = peer_id
            user.vk_bot_linked_at = datetime.utcnow()
            db.commit()

        reset_vk_bot_auth(sender_id)
        vk_bot_send_message(peer_id, "Авторизация успешно пройдена!")
        return True

    return False


def handle_vk_bot_reply_command(sender_id: str, peer_id: str, text_value: str) -> bool:
    command_match = re.match(r"^(?:/reply|reply|ответ)\s+(.+)$", text_value.strip(), flags=re.IGNORECASE | re.DOTALL)
    if not command_match:
        return False
    reply_body = command_match.group(1).strip()
    if not reply_body:
        vk_bot_send_message(peer_id, "После reply укажите текст ответа.")
        return True
    with SessionLocal() as db:
        sender = db.execute(
            select(User).where(
                or_(
                    User.vk_bot_user_id == sender_id,
                    User.vk_bot_peer_id == peer_id,
                )
            )
        ).scalar_one_or_none()
        if not sender:
            vk_bot_send_message(peer_id, "Сначала пройдите авторизацию через /start или /login.")
            return True
        latest_note = latest_pigeon_notification_for_reply(db, sender.id)
        if not latest_note or latest_note.from_user_id is None:
            vk_bot_send_message(peer_id, "Не найдено входящих голубей, на которые можно ответить.")
            return True
        recipient = db.get(User, latest_note.from_user_id)
        if not recipient:
            vk_bot_send_message(peer_id, "Получатель ответа не найден.")
            return True
        send_pigeon_notification(
            db,
            sender=sender,
            recipient=recipient,
            message_body=reply_body,
            reply_to_notification_id=latest_note.id,
        )
        db.commit()
    vk_bot_send_message(peer_id, f"Ответ отправлен пользователю @{preferred_user_alias(recipient)}.")
    return True


def handle_vk_bot_message(message: dict[str, Any]) -> None:
    sender_raw = message.get("from_id")
    peer_raw = message.get("peer_id")
    if sender_raw is None or peer_raw is None:
        return

    sender_id = str(sender_raw)
    peer_id = str(peer_raw)
    text_value = str(message.get("text") or "").strip()

    if peer_id != sender_id:
        if text_value:
            vk_bot_send_message(peer_id, "Для подключения уведомлений используйте личные сообщения сообщества.")
        return

    if not text_value:
        vk_bot_send_message(peer_id, "Поддерживаются только текстовые сообщения.")
        return

    lowered = text_value.casefold()
    if lowered in {"/start", "start", "начать"}:
        start_vk_bot_auth(sender_id, peer_id, with_greeting=True)
        return
    if lowered in {"/login", "login", "войти"}:
        start_vk_bot_auth(sender_id, peer_id, with_greeting=False)
        return
    if lowered in {"/logout", "logout", "выйти"}:
        with SessionLocal() as db:
            linked_users = db.execute(
                select(User).where(
                    or_(
                        User.vk_bot_user_id == sender_id,
                        User.vk_bot_peer_id == peer_id,
                    )
                )
            ).scalars().all()
            for linked_user in linked_users:
                linked_user.vk_bot_user_id = None
                linked_user.vk_bot_peer_id = None
                linked_user.vk_bot_linked_at = None
            if linked_users:
                db.commit()
        reset_vk_bot_auth(sender_id)
        vk_bot_send_message(peer_id, "VK-привязка удалена.")
        return
    if lowered.startswith("/reply") or lowered.startswith("reply ") or lowered.startswith("ответ "):
        if handle_vk_bot_reply_command(sender_id, peer_id, text_value):
            return

    state = get_vk_bot_auth_step(sender_id)
    if state or not text_value.startswith("/"):
        if handle_vk_bot_auth_message(sender_id, peer_id, text_value):
            return

    with SessionLocal() as db:
        linked_user = db.execute(
            select(User).where(
                or_(
                    User.vk_bot_user_id == sender_id,
                    User.vk_bot_peer_id == peer_id,
                )
            )
        ).scalar_one_or_none()
    if linked_user:
        vk_bot_send_message(peer_id, "Оповещения подключены. Если понадобится перепривязка, отправьте /login.")
        return

    start_vk_bot_auth(sender_id, peer_id, with_greeting=True)


def handle_vk_bot_message_allow(event_object: dict[str, Any]) -> None:
    user_id = str(event_object.get("user_id") or "").strip()
    if not user_id:
        return
    with SessionLocal() as db:
        linked_user = db.execute(select(User).where(User.vk_bot_user_id == user_id)).scalar_one_or_none()
        if linked_user and linked_user.vk_bot_linked_at is None:
            linked_user.vk_bot_linked_at = datetime.utcnow()
            db.commit()


def handle_vk_bot_message_deny(event_object: dict[str, Any]) -> None:
    user_id = str(event_object.get("user_id") or "").strip()
    if not user_id:
        return
    with SessionLocal() as db:
        linked_users = db.execute(select(User).where(User.vk_bot_user_id == user_id)).scalars().all()
        for linked_user in linked_users:
            linked_user.vk_bot_user_id = None
            linked_user.vk_bot_peer_id = None
            linked_user.vk_bot_linked_at = None
        if linked_users:
            db.commit()


def handle_vk_bot_event(payload: dict[str, Any]) -> None:
    event_type = str(payload.get("type") or "").strip()
    event_object = payload.get("object")
    if not isinstance(event_object, dict):
        return

    if event_type == "message_new":
        message = event_object.get("message")
        if isinstance(message, dict):
            handle_vk_bot_message(message)
        return
    if event_type == "message_allow":
        handle_vk_bot_message_allow(event_object)
        return
    if event_type == "message_deny":
        handle_vk_bot_message_deny(event_object)


def dispatch_vk_bot_notifications() -> None:
    if not VK_BOT_ENABLED:
        return
    with SessionLocal() as db:
        users = db.execute(select(User).where(User.vk_bot_peer_id.is_not(None))).scalars().all()
        if not users:
            return
        now_utc = datetime.utcnow()

        for user in users:
            peer_id = (user.vk_bot_peer_id or "").strip()
            if not peer_id:
                continue

            stmt = (
                select(FestivalNotification)
                .where(
                    FestivalNotification.user_id == user.id,
                    FestivalNotification.vk_sent_at.is_(None),
                )
                .order_by(FestivalNotification.created_at.asc(), FestivalNotification.id.asc())
                .limit(VK_BOT_DISPATCH_LIMIT)
            )
            if user.vk_bot_linked_at:
                stmt = stmt.where(FestivalNotification.created_at >= user.vk_bot_linked_at)

            notifications = db.execute(stmt).scalars().all()
            if not notifications:
                continue

            must_commit = False
            for note in notifications:
                if not is_external_bot_eligible_notification(note.message):
                    note.vk_sent_at = now_utc
                    must_commit = True
                    continue

                bot_text = format_external_bot_notification_message(note.message)
                if not bot_text:
                    note.vk_sent_at = now_utc
                    must_commit = True
                    continue

                ok, error_code = vk_bot_send_message(peer_id, bot_text)
                if ok:
                    note.vk_sent_at = now_utc
                    must_commit = True
                    continue

                if error_code in {7, 901, 917}:
                    user.vk_bot_user_id = None
                    user.vk_bot_peer_id = None
                    user.vk_bot_linked_at = None
                    must_commit = True
                    break

            if must_commit:
                db.commit()


def vk_bot_loop() -> None:
    if not VK_BOT_ENABLED:
        return
    while True:
        try:
            dispatch_vk_bot_notifications()
        except Exception:
            pass
        time.sleep(VK_BOT_LOOP_SLEEP_SECONDS)


def start_vk_bot_worker() -> None:
    global vk_bot_worker_thread
    if not VK_BOT_ENABLED:
        return
    with vk_bot_worker_lock:
        if vk_bot_worker_thread and vk_bot_worker_thread.is_alive():
            return
        vk_bot_worker_thread = threading.Thread(
            target=vk_bot_loop,
            name="vk-bot-worker",
            daemon=True,
        )
        vk_bot_worker_thread.start()


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
    normalized_value = re.sub(
        r",\s*(?=(?:https?://|/media/|iframe:|<iframe))",
        "\n",
        raw_value,
        flags=re.IGNORECASE,
    )
    chunks = re.split(r"[\n;]+", normalized_value)
    for chunk in chunks:
        value = chunk.strip()
        if not value:
            continue
        if value.startswith("iframe:"):
            iframe_src = value.removeprefix("iframe:").strip().rstrip(",;")
            if iframe_src.lower().startswith(("http://", "https://")):
                items.append(f"iframe:{iframe_src}")
            continue
        if value.lower().startswith("<iframe"):
            match = re.search(r'src=["\']([^"\']+)["\']', value, flags=re.IGNORECASE)
            if match:
                src = match.group(1).strip().rstrip(",;")
                if src.lower().startswith(("http://", "https://")):
                    items.append(f"iframe:{src}")
            continue
        value = value.rstrip(",;")
        if value.lower().startswith(("http://", "https://")) or value.startswith("/media/"):
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


def save_master_order_from_form(form: Any, order: CommunityMasterOrder) -> tuple[bool, str]:
    subject = str(form.get("subject", "")).strip()
    contact_tg = str(form.get("contact_tg", "")).strip()
    character_fandom = str(form.get("character_fandom", "")).strip()
    details = str(form.get("details", "")).strip()
    deadline = parse_date(str(form.get("deadline", "")).strip())
    references = parse_reference_values(str(form.get("references_input", "")))[:3]

    if not subject:
        return False, "Укажите тему заказа."
    if len(subject) > 255:
        return False, "Тема заказа должна быть не длиннее 255 символов."
    if len(contact_tg) > 255:
        return False, "Поле TG для связи должно быть не длиннее 255 символов."
    if len(character_fandom) > 255:
        return False, "Поле персонажа и фандома должно быть не длиннее 255 символов."
    if len(details) > 4000:
        return False, "Подробности заказа должны быть не длиннее 4000 символов."

    order.subject = subject
    order.contact_tg = contact_tg or None
    order.character_fandom = character_fandom or None
    order.details = details or None
    order.deadline = deadline
    order.references_json = references
    return True, ""


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


def latest_pigeon_notification_for_reply(db: Session, user_id: int) -> FestivalNotification | None:
    notifications = db.execute(
        select(FestivalNotification)
        .where(
            FestivalNotification.user_id == user_id,
            FestivalNotification.from_user_id.is_not(None),
        )
        .order_by(FestivalNotification.created_at.desc(), FestivalNotification.id.desc())
        .limit(50)
    ).scalars().all()
    for note in notifications:
        if is_pigeon_message(note.message):
            return note
    return None


def send_pigeon_notification(
    db: Session,
    *,
    sender: User,
    recipient: User,
    message_body: str,
    reply_to_notification_id: int | None = None,
) -> bool:
    sender_alias = preferred_user_alias(sender)
    payload = f"Курлык! (@{sender_alias}) {message_body.strip()}"
    return enqueue_notification_if_missing(
        db,
        user_id=recipient.id,
        from_user_id=sender.id,
        source_card_id=None,
        reply_to_notification_id=reply_to_notification_id,
        message=payload,
    )


def get_latest_unread_pigeon(db: Session, user_id: int) -> dict[str, Any] | None:
    notifications = db.execute(
        select(FestivalNotification)
        .where(
            FestivalNotification.user_id == user_id,
            FestivalNotification.is_read.is_(False),
        )
        .order_by(FestivalNotification.created_at.desc(), FestivalNotification.id.desc())
        .limit(50)
    ).scalars().all()
    for note in notifications:
        parsed = parse_pigeon_message(note.message)
        if not parsed:
            continue
        sender_alias, body = parsed
        return {
            "id": note.id,
            "sender_alias": sender_alias,
            "body": body or "Без текста",
            "body_html": str(render_text_content(body or "Без текста")),
            "created_at": (note.created_at.isoformat() if note.created_at else ""),
        }
    return None


def get_user_pigeon_notification(db: Session, user_id: int, notification_id: int) -> FestivalNotification | None:
    notification = db.execute(
        select(FestivalNotification).where(
            FestivalNotification.id == notification_id,
            FestivalNotification.user_id == user_id,
        )
    ).scalar_one_or_none()
    if not notification or not is_pigeon_message(notification.message):
        return None
    return notification


def _render_article_inline(text: str) -> str:
    rendered = html.escape(text)

    def color_repl(match: re.Match[str]) -> str:
        color = match.group(1).strip()
        if not re.fullmatch(r"(#[0-9a-fA-F]{3,8}|[a-zA-Z]{3,20})", color):
            return match.group(0)
        content = match.group(2)
        return f'<span style="color:{color}">{content}</span>'

    def image_repl(match: re.Match[str]) -> str:
        alt_text = (match.group(1) or "").strip()
        image_url = (match.group(2) or "").strip()
        if not image_url:
            return match.group(0)
        return (
            '<span class="article-inline-image">'
            f'<img src="{image_url}" alt="{alt_text}" loading="lazy" />'
            "</span>"
        )

    rendered = re.sub(r"\[color=([^\]]+)\](.+?)\[/color\]", color_repl, rendered, flags=re.IGNORECASE)
    rendered = re.sub(r"!\[([^\]]*)\]\(((?:https?://|/)[^\s)]+)\)", image_repl, rendered)
    rendered = re.sub(
        r"(?<!!)\[([^\]]+)\]\((https?://[^\s)]+)\)",
        r'<a href="\2" target="_blank" rel="noreferrer">\1</a>',
        rendered,
    )
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
        "is_priority",
        "is_completed",
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


def get_user_option_value(db: Session, user_id: int, group: str) -> str:
    row = db.execute(
        select(UserOption).where(
            UserOption.user_id == user_id,
            UserOption.group == group,
        )
    ).scalar_one_or_none()
    return str(row.value or "").strip() if row else ""


def set_user_option_value(db: Session, user_id: int, group: str, value: str | None) -> None:
    normalized = str(value or "").strip()
    row = db.execute(
        select(UserOption).where(
            UserOption.user_id == user_id,
            UserOption.group == group,
        )
    ).scalar_one_or_none()
    if normalized:
        if row:
            row.value = normalized
        else:
            db.add(UserOption(user_id=user_id, group=group, value=normalized))
    elif row:
        db.delete(row)


def get_user_option_values(db: Session, user_id: int, group: str) -> list[str]:
    rows = db.execute(
        select(UserOption)
        .where(
            UserOption.user_id == user_id,
            UserOption.group == group,
        )
        .order_by(UserOption.id.asc())
    ).scalars().all()
    return [str(row.value or "").strip() for row in rows if str(row.value or "").strip()]


def replace_user_option_values(db: Session, user_id: int, group: str, values: list[str]) -> None:
    existing_rows = db.execute(
        select(UserOption).where(
            UserOption.user_id == user_id,
            UserOption.group == group,
        )
    ).scalars().all()
    for row in existing_rows:
        db.delete(row)
    if existing_rows:
        db.flush()

    unique_values: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = str(value or "").strip()
        if not normalized:
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        unique_values.append(normalized)

    for value in unique_values:
        db.add(UserOption(user_id=user_id, group=group, value=value))


def encode_content_telegram_channel_value(title: str, chat_id: str) -> str:
    return json.dumps({"title": title, "chat_id": chat_id}, ensure_ascii=False, separators=(",", ":"))


def decode_content_telegram_channel_value(value: str) -> dict[str, str] | None:
    raw = (value or "").strip()
    if not raw:
        return None
    try:
        payload = json.loads(raw)
    except ValueError:
        return None
    if not isinstance(payload, dict):
        return None
    chat_id = normalize_telegram_target(payload.get("chat_id"))
    title = str(payload.get("title") or "").strip() or chat_id
    if not chat_id:
        return None
    return {"title": title[:120], "chat_id": chat_id}


def parse_content_telegram_channel_lines(raw_text: str) -> tuple[list[dict[str, str]], str]:
    entries: list[dict[str, str]] = []
    seen_chat_ids: set[str] = set()
    for index, raw_line in enumerate((raw_text or "").splitlines(), start=1):
        line = raw_line.strip()
        if not line:
            continue
        title = ""
        chat_raw = line
        match = re.fullmatch(r"(.+?)\s+[—-]\s+(.+)", line)
        if match:
            title = match.group(1).strip()
            chat_raw = match.group(2).strip()
        chat_id = normalize_telegram_target(chat_raw)
        if not chat_id:
            return [], f"Строка {index}: укажите канал в формате «Название — @channel» или просто «@channel»."
        if chat_id in seen_chat_ids:
            continue
        seen_chat_ids.add(chat_id)
        entries.append({"title": (title or chat_id)[:120], "chat_id": chat_id})
    return entries, ""


def format_content_telegram_channel_lines(entries: list[dict[str, str]]) -> str:
    return "\n".join(
        (
            f"{entry['title']} — {entry['chat_id']}"
            if entry.get("title") and entry.get("title") != entry.get("chat_id")
            else entry.get("chat_id", "")
        )
        for entry in entries
        if entry.get("chat_id")
    )


def get_content_telegram_channels(db: Session, user_id: int) -> list[dict[str, str]]:
    result: list[dict[str, str]] = []
    seen_chat_ids: set[str] = set()
    for value in get_user_option_values(db, user_id, CONTENT_TELEGRAM_CHANNEL_GROUP):
        decoded = decode_content_telegram_channel_value(value)
        if not decoded:
            continue
        chat_id = decoded["chat_id"]
        if chat_id in seen_chat_ids:
            continue
        seen_chat_ids.add(chat_id)
        result.append(decoded)

    legacy_chat_id = normalize_telegram_target(get_user_option_value(db, user_id, CONTENT_TELEGRAM_CHAT_GROUP))
    if legacy_chat_id and legacy_chat_id not in seen_chat_ids:
        result.append({"title": legacy_chat_id, "chat_id": legacy_chat_id})
    return result


def resolve_content_telegram_channels(
    selected_chat_ids: list[str] | tuple[str, ...],
    available_channels: list[dict[str, str]],
) -> list[dict[str, str]]:
    available_by_chat_id = {
        str(channel.get("chat_id") or "").strip(): channel
        for channel in available_channels
        if str(channel.get("chat_id") or "").strip()
    }
    resolved: list[dict[str, str]] = []
    seen_chat_ids: set[str] = set()
    for raw_chat_id in selected_chat_ids:
        chat_id = normalize_telegram_target(raw_chat_id)
        if not chat_id or chat_id in seen_chat_ids:
            continue
        channel = available_by_chat_id.get(chat_id)
        if not channel:
            continue
        seen_chat_ids.add(chat_id)
        resolved.append(channel)
    return resolved


def encode_content_premium_emoji_value(emoji: str, emoji_id: str) -> str:
    return json.dumps({"emoji": emoji, "emoji_id": emoji_id}, ensure_ascii=False, separators=(",", ":"))


def decode_content_premium_emoji_value(value: str) -> dict[str, str] | None:
    raw = (value or "").strip()
    if not raw:
        return None
    try:
        payload = json.loads(raw)
    except ValueError:
        return None
    if not isinstance(payload, dict):
        return None
    emoji = str(payload.get("emoji") or "").strip()
    emoji_id = str(payload.get("emoji_id") or "").strip()
    if not emoji or not emoji_id.isdigit():
        return None
    return {"emoji": emoji, "emoji_id": emoji_id}


def parse_content_premium_emoji_lines(raw_text: str) -> tuple[list[dict[str, str]], str]:
    entries: list[dict[str, str]] = []
    for index, raw_line in enumerate((raw_text or "").splitlines(), start=1):
        line = raw_line.strip()
        if not line:
            continue
        match = re.fullmatch(r"(.+?)\s*[—-]\s*(\d+)", line)
        if not match:
            return [], f"Строка {index}: используйте формат «💃 — 5327958075158568158»."
        emoji = match.group(1).strip()
        emoji_id = match.group(2).strip()
        if not emoji:
            return [], f"Строка {index}: укажите эмодзи перед ID."
        entries.append({"emoji": emoji, "emoji_id": emoji_id})
    return entries, ""


def format_content_premium_emoji_lines(entries: list[dict[str, str]]) -> str:
    return "\n".join(
        f"{entry['emoji']} — {entry['emoji_id']}"
        for entry in entries
        if entry.get("emoji") and entry.get("emoji_id")
    )


def get_content_premium_emoji_entries(db: Session, user_id: int) -> list[dict[str, str]]:
    result: list[dict[str, str]] = []
    for value in get_user_option_values(db, user_id, CONTENT_TELEGRAM_PREMIUM_EMOJI_GROUP):
        decoded = decode_content_premium_emoji_value(value)
        if decoded:
            result.append(decoded)
    return result


def normalize_content_rubric_tag(value: str | None) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    normalized = raw.replace("-", "_")
    normalized = re.sub(r"\s+", "_", normalized)
    normalized = normalized.lstrip("#")
    normalized = re.sub(r"[^\w_]", "", normalized, flags=re.UNICODE)
    return f"#{normalized}" if normalized else ""


def encode_content_rubric_tag_value(rubric: str, tag: str) -> str:
    return json.dumps({"rubric": rubric, "tag": tag}, ensure_ascii=False, separators=(",", ":"))


def decode_content_rubric_tag_value(value: str) -> tuple[str, str] | None:
    raw = (value or "").strip()
    if not raw:
        return None
    try:
        payload = json.loads(raw)
    except ValueError:
        return None
    if not isinstance(payload, dict):
        return None
    rubric = str(payload.get("rubric") or "").strip()
    tag = normalize_content_rubric_tag(payload.get("tag"))
    if not rubric or not tag:
        return None
    return rubric, tag


def get_content_rubric_tags(db: Session, user_id: int) -> dict[str, str]:
    tags: dict[str, str] = {}
    for value in get_user_option_values(db, user_id, CONTENT_RUBRIC_TAG_GROUP):
        decoded = decode_content_rubric_tag_value(value)
        if decoded:
            rubric, tag = decoded
            tags[rubric] = tag
    return tags


def save_content_rubric_tags(db: Session, user_id: int, tag_map: dict[str, str]) -> None:
    values = [
        encode_content_rubric_tag_value(rubric, tag)
        for rubric, tag in tag_map.items()
        if rubric and tag
    ]
    replace_user_option_values(db, user_id, CONTENT_RUBRIC_TAG_GROUP, values)


def smtp_is_configured() -> bool:
    return bool(SMTP_HOST and SMTP_FROM_EMAIL)


def send_plain_email(*, to_email: str, subject: str, body: str) -> bool:
    if not smtp_is_configured():
        return False
    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = SMTP_FROM_EMAIL
    message["To"] = to_email
    message.set_content(body)
    try:
        if SMTP_USE_SSL:
            with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=20) as server:
                if SMTP_USER:
                    server.login(SMTP_USER, SMTP_PASSWORD)
                server.send_message(message)
        else:
            with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=20) as server:
                server.ehlo()
                if SMTP_USE_TLS:
                    server.starttls()
                    server.ehlo()
                if SMTP_USER:
                    server.login(SMTP_USER, SMTP_PASSWORD)
                server.send_message(message)
    except Exception:
        return False
    return True


def hash_password_reset_token(raw_token: str) -> str:
    return hashlib.sha256((raw_token or "").encode("utf-8")).hexdigest()


def build_password_reset_link(raw_token: str) -> str:
    safe_token = quote(raw_token or "")
    return f"{APP_BASE_URL}/reset-password?token={safe_token}"


def create_password_reset_token(db: Session, user_id: int) -> str:
    now_utc = datetime.utcnow()
    active_tokens = db.execute(
        select(PasswordResetToken).where(
            PasswordResetToken.user_id == user_id,
            PasswordResetToken.used_at.is_(None),
            PasswordResetToken.expires_at > now_utc,
        )
    ).scalars().all()
    for token_row in active_tokens:
        token_row.used_at = now_utc

    raw_token = secrets.token_urlsafe(32)
    db.add(
        PasswordResetToken(
            user_id=user_id,
            token_hash=hash_password_reset_token(raw_token),
            expires_at=now_utc + timedelta(minutes=PASSWORD_RESET_TOKEN_MINUTES),
            used_at=None,
        )
    )
    return raw_token


def find_active_password_reset_token(db: Session, raw_token: str | None) -> PasswordResetToken | None:
    token_value = (raw_token or "").strip()
    if not token_value:
        return None
    now_utc = datetime.utcnow()
    token_hash = hash_password_reset_token(token_value)
    return db.execute(
        select(PasswordResetToken).where(
            PasswordResetToken.token_hash == token_hash,
            PasswordResetToken.used_at.is_(None),
            PasswordResetToken.expires_at > now_utc,
        )
    ).scalar_one_or_none()


def _deep_find_string(payload: Any, wanted_keys: set[str]) -> str:
    stack: list[Any] = [payload]
    visited: set[int] = set()

    while stack:
        item = stack.pop(0)
        item_id = id(item)
        if item_id in visited:
            continue
        visited.add(item_id)

        if isinstance(item, dict):
            for key, value in item.items():
                key_norm = str(key).strip().casefold()
                if key_norm in wanted_keys and isinstance(value, str) and value.strip():
                    return value.strip()
                if isinstance(value, (dict, list, tuple)):
                    stack.append(value)
        elif isinstance(item, (list, tuple)):
            for value in item:
                if isinstance(value, (dict, list, tuple)):
                    stack.append(value)

    return ""


def extract_vk_access_token(payload: dict[str, Any]) -> str:
    return _deep_find_string(payload, {"access_token", "accesstoken"})


def extract_vk_id_token(payload: dict[str, Any]) -> str:
    return _deep_find_string(payload, {"id_token", "idtoken"})


def extract_vk_email(payload: dict[str, Any]) -> str:
    email_value = _deep_find_string(payload, {"email", "user_email", "mail"}).lower()
    if "@" not in email_value:
        return ""
    return email_value


def fetch_vk_public_profile(id_token: str) -> dict[str, Any]:
    try:
        response = requests.post(
            VKID_PUBLIC_INFO_URL,
            params={"client_id": VKID_APP_ID},
            data={"id_token": id_token},
            timeout=HTTP_TIMEOUT_SECONDS,
        )
    except requests.RequestException as exc:
        raise RuntimeError("Не удалось связаться с VK ID. Попробуйте позже.") from exc

    if response.status_code != 200:
        raise RuntimeError("VK ID временно недоступен.")

    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError("VK ID вернул некорректный ответ.") from exc

    error_payload = payload.get("error") if isinstance(payload, dict) else None
    if isinstance(error_payload, dict):
        message = str(error_payload.get("error_msg") or "Ошибка авторизации VK").strip()
        raise ValueError(message)

    profile = payload.get("user") if isinstance(payload, dict) else None
    if not isinstance(profile, dict):
        raise RuntimeError("Не удалось получить профиль VK.")

    user_id = str(profile.get("user_id") or "").strip()
    if not user_id:
        raise RuntimeError("В ответе VK отсутствует id пользователя.")

    email = str(profile.get("email") or "").strip().lower()
    if email and "@" not in email:
        email = ""

    return {
        "id": user_id,
        "email": email,
        "first_name": str(profile.get("first_name") or "").strip(),
        "last_name": str(profile.get("last_name") or "").strip(),
        "avatar": str(profile.get("avatar") or "").strip(),
        "phone": str(profile.get("phone") or "").strip(),
    }


def get_verified_vk_profile(payload: dict[str, Any]) -> dict[str, Any]:
    id_token = extract_vk_id_token(payload)
    if not id_token:
        raise ValueError("VK не вернул id_token.")
    return fetch_vk_public_profile(id_token)


def sanitize_vk_username(value: str | None) -> str:
    cleaned = normalize_username(value)
    if not cleaned:
        return ""
    cleaned = re.sub(r"[^\w.-]+", "_", cleaned, flags=re.UNICODE)
    cleaned = re.sub(r"_+", "_", cleaned).strip("_.-")
    return cleaned[:100]


def build_unique_username(db: Session, preferred: str, fallback_seed: str) -> str:
    base = sanitize_vk_username(preferred) or sanitize_vk_username(f"vk_{fallback_seed}")
    if not base:
        base = f"vk_{secrets.token_hex(4)}"

    candidate = base
    counter = 2
    while db.execute(select(User.id).where(User.username == candidate)).first() is not None:
        suffix = f"_{counter}"
        candidate = f"{base[: max(1, 100 - len(suffix))]}{suffix}"
        counter += 1
    return candidate


def build_unique_email(db: Session, preferred_email: str, vk_user_id: str) -> str:
    normalized = (preferred_email or "").strip().lower()
    if normalized and db.execute(select(User.id).where(User.email == normalized)).first() is None:
        return normalized

    base_local = f"vkid_{vk_user_id}"
    candidate = f"{base_local}@vkid.local"
    counter = 2
    while db.execute(select(User.id).where(User.email == candidate)).first() is not None:
        candidate = f"{base_local}_{counter}@vkid.local"
        counter += 1
    return candidate


def upsert_user_by_vk(
    db: Session,
    vk_profile: dict[str, Any],
    payload: dict[str, Any],
) -> User:
    vk_user_id = str(vk_profile.get("id") or "").strip()
    if not vk_user_id:
        raise ValueError("VK не вернул идентификатор пользователя.")

    first_name = str(vk_profile.get("first_name") or "").strip()
    last_name = str(vk_profile.get("last_name") or "").strip()
    display_name = (f"{first_name}_{last_name}").strip("_")

    user_by_vk = db.execute(select(User).where(User.vk_user_id == vk_user_id)).scalar_one_or_none()

    email_candidate = str(vk_profile.get("email") or "").strip().lower() or extract_vk_email(payload)
    user_by_email = None
    if email_candidate:
        user_by_email = db.execute(select(User).where(User.email == email_candidate)).scalar_one_or_none()

    if user_by_vk and user_by_email and user_by_vk.id != user_by_email.id:
        raise ValueError("Этот VK-аккаунт уже связан с другим профилем Cosplay Planner.")

    created_new = False
    user = user_by_vk or user_by_email
    if user is None:
        username = build_unique_username(
            db,
            preferred=display_name or f"vk_{vk_user_id}",
            fallback_seed=vk_user_id,
        )
        email_value = build_unique_email(db, email_candidate, vk_user_id)
        user = User(
            username=username,
            email=email_value,
            password_hash=password_context.hash(secrets.token_urlsafe(24)),
            vk_user_id=vk_user_id,
        )
        db.add(user)
        db.flush()
        created_new = True
    else:
        if user.vk_user_id and user.vk_user_id != vk_user_id:
            raise ValueError("Этот VK-аккаунт уже привязан к другому пользователю.")
        user.vk_user_id = vk_user_id

    if created_new:
        approved_announcements = db.execute(
            select(FestivalAnnouncement).where(FestivalAnnouncement.status == ANNOUNCEMENT_STATUS_APPROVED)
        ).scalars().all()
        for announcement in approved_announcements:
            propagate_approved_announcement(db, announcement, target_user_ids=[user.id])

    try:
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise ValueError("Не удалось завершить авторизацию VK. Попробуйте ещё раз.") from exc

    db.refresh(user)
    return user


def link_existing_user_with_vk(db: Session, user: User, vk_profile: dict[str, Any]) -> User:
    vk_user_id = str(vk_profile.get("id") or "").strip()
    if not vk_user_id:
        raise ValueError("VK не вернул идентификатор пользователя.")

    linked_to_another = db.execute(
        select(User).where(
            User.vk_user_id == vk_user_id,
            User.id != user.id,
        )
    ).scalar_one_or_none()
    if linked_to_another:
        raise ValueError("Этот VK-аккаунт уже связан с другим профилем Cosplay Planner.")

    user.vk_user_id = vk_user_id

    try:
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise ValueError("Не удалось связать VK с текущим профилем. Попробуйте ещё раз.") from exc

    db.refresh(user)
    return user


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
        "external_contact_buttons": external_contact_buttons,
        "build_external_url": build_external_url,
        "button_label_for_external_url": button_label_for_external_url,
        "pixel_emoji_catalog": PIXEL_EMOJI_CATALOG,
        "vkid_enabled": VKID_ENABLED,
        "vkid_app_id": VKID_APP_ID,
        "vkid_redirect_url": VKID_REDIRECT_URL,
        "vkid_scope": VKID_SCOPE,
        "vk_bot_enabled": VK_BOT_ENABLED,
        "vk_bot_link_url": f"https://vk.me/{VK_BOT_COMMUNITY_DOMAIN}" if VK_BOT_COMMUNITY_DOMAIN else "",
        "site_url": SITE_URL,
        "default_seo_description": SEO_DESCRIPTION,
        "default_seo_keywords": SEO_KEYWORDS,
    }
    payload.update(context)
    return templates.TemplateResponse(name, payload)


@app.get("/robots.txt", include_in_schema=False)
def robots_txt() -> FileResponse:
    return FileResponse("app/static/robots.txt", media_type="text/plain; charset=utf-8")


@app.post("/vk/bot/callback", include_in_schema=False)
async def vk_bot_callback(request: Request) -> PlainTextResponse:
    try:
        payload = await request.json()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid VK callback payload.") from exc

    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Invalid VK callback payload.")

    event_type = str(payload.get("type") or "").strip()
    group_id = str(payload.get("group_id") or "").strip()

    if VK_BOT_GROUP_ID and group_id and group_id != VK_BOT_GROUP_ID:
        raise HTTPException(status_code=403, detail="Unexpected VK group id.")

    if event_type == "confirmation":
        if not VK_BOT_CONFIRMATION_TOKEN:
            raise HTTPException(status_code=503, detail="VK bot confirmation token is not configured.")
        return PlainTextResponse(VK_BOT_CONFIRMATION_TOKEN)

    if not VK_BOT_ENABLED:
        return PlainTextResponse("ok")

    if VK_BOT_SECRET:
        incoming_secret = str(payload.get("secret") or "").strip()
        if incoming_secret != VK_BOT_SECRET:
            raise HTTPException(status_code=403, detail="Invalid VK callback secret.")

    handle_vk_bot_event(payload)
    return PlainTextResponse("ok")


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
            "city": "",
            "event_date": "",
            "event_type": "photoset",
            "status": PROJECT_BOARD_STATUS_ACTIVE,
            "comment": "",
            "contact_nick": default_nick,
            "contact_link": "",
        }

    return {
        "fandom": post.fandom or "",
        "city": post.city or "",
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
        news_items = db.execute(
            select(HomeNews).order_by(HomeNews.created_at.desc(), HomeNews.id.desc()).limit(40)
        ).scalars().all()
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
            if pigeon_payload:
                sender_alias, body = pigeon_payload
                pigeon_notifications.append(
                    {
                        "id": note.id,
                        "from_user_id": note.from_user_id,
                        "reply_to_notification_id": note.reply_to_notification_id,
                        "sender_alias": sender_alias,
                        "body": body,
                        "body_html": render_text_content(body),
                        "created_at": note.created_at,
                        "is_read": bool(note.is_read),
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
            news_items=news_items,
            can_manage_news=is_moderator_user(user),
        )
    return template_response(request, "landing.html", user=None, active_tab=None)


@app.post("/home-news/new")
async def home_news_create(request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")
    if not is_moderator_user(user):
        add_flash(request, "Недостаточно прав для публикации новости.", "error")
        return redirect("/")

    form = await request.form()
    body = str(form.get("body", "")).strip()
    if not body:
        add_flash(request, "Введите текст новости.", "error")
        return redirect("/")
    if len(body) > 8000:
        add_flash(request, "Текст новости слишком длинный (до 8000 символов).", "error")
        return redirect("/")

    db.add(HomeNews(author_id=user.id, body=body))
    db.commit()
    add_flash(request, "Новость опубликована.", "success")
    return redirect("/")


@app.post("/home-news/{news_id}/edit")
async def home_news_update(news_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")
    if not is_moderator_user(user):
        add_flash(request, "Недостаточно прав для редактирования новости.", "error")
        return redirect("/")

    news_item = db.get(HomeNews, news_id)
    if not news_item:
        add_flash(request, "Новость не найдена.", "error")
        return redirect("/")

    form = await request.form()
    body = str(form.get("body", "")).strip()
    if not body:
        add_flash(request, "Текст новости не может быть пустым.", "error")
        return redirect("/")
    if len(body) > 8000:
        add_flash(request, "Текст новости слишком длинный (до 8000 символов).", "error")
        return redirect("/")

    news_item.body = body
    db.commit()
    add_flash(request, "Новость обновлена.", "success")
    return redirect("/")


@app.post("/home-news/{news_id}/delete")
def home_news_delete(news_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")
    if not is_moderator_user(user):
        add_flash(request, "Недостаточно прав для удаления новости.", "error")
        return redirect("/")

    news_item = db.get(HomeNews, news_id)
    if not news_item:
        add_flash(request, "Новость не найдена.", "error")
        return redirect("/")

    db.delete(news_item)
    db.commit()
    add_flash(request, "Новость удалена.", "info")
    return redirect("/")


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
    return template_response(request, "login_vkid.html", user=None)




@app.post("/auth/vk/complete")
async def auth_vk_complete(request: Request, db: Session = Depends(get_db)) -> dict[str, Any]:
    if not VKID_ENABLED:
        raise HTTPException(status_code=404, detail="VK ID отключен.")

    try:
        payload = await request.json()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Некорректный формат запроса.") from exc

    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Некорректные данные авторизации VK.")

    try:
        vk_profile = get_verified_vk_profile(payload)
        user = upsert_user_by_vk(db, vk_profile, payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    request.session["user_id"] = user.id
    return {"ok": True, "redirect": "/cosplan"}


@app.post("/profile/vk/link")
async def profile_vk_link(request: Request, db: Session = Depends(get_db)) -> dict[str, Any]:
    user = current_user(request, db)
    if not user:
        raise HTTPException(status_code=401, detail="Требуется авторизация.")
    if not VKID_ENABLED:
        raise HTTPException(status_code=404, detail="VK ID отключен.")

    try:
        payload = await request.json()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Некорректный формат запроса.") from exc
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Некорректные данные авторизации VK.")

    try:
        vk_profile = get_verified_vk_profile(payload)
        link_existing_user_with_vk(db, user, vk_profile)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    linked_label = f"id{vk_profile.get('id')}"
    return {
        "ok": True,
        "message": f"Профиль успешно связан с VK ({linked_label}).",
        "redirect": "/profile",
    }


@app.post("/profile/vk/unlink")
def profile_vk_unlink(request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    if not user.vk_user_id:
        add_flash(request, "Профиль VK пока не привязан.", "info")
        return redirect("/profile")

    user.vk_user_id = None
    user.vk_screen_name = None
    db.commit()
    add_flash(request, "Привязка VK удалена.", "info")
    return redirect("/profile")


@app.get("/forgot-password", response_class=HTMLResponse)
def forgot_password_page(request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if user:
        return redirect("/cosplan")
    return template_response(request, "forgot_password.html", user=None)


@app.post("/forgot-password")
async def forgot_password_submit(request: Request, db: Session = Depends(get_db)):
    form = await request.form()
    email = str(form.get("email", "")).strip().lower()
    if not email:
        add_flash(request, "Введите email.", "error")
        return redirect("/forgot-password")

    user = db.execute(select(User).where(User.email == email)).scalar_one_or_none()
    if user:
        if smtp_is_configured():
            raw_token = create_password_reset_token(db, user.id)
            reset_link = build_password_reset_link(raw_token)
            email_body = (
                "Здравствуйте!\n\n"
                "Мы получили запрос на восстановление пароля для вашего аккаунта Cosplay Planner.\n"
                f"Перейдите по ссылке, чтобы задать новый пароль:\n{reset_link}\n\n"
                f"Ссылка действует {PASSWORD_RESET_TOKEN_MINUTES} минут.\n"
                "Если вы не запрашивали восстановление, просто проигнорируйте это письмо."
            )
            sent_ok = send_plain_email(
                to_email=user.email,
                subject="Восстановление пароля Cosplay Planner",
                body=email_body,
            )
            if sent_ok:
                db.commit()
            else:
                db.rollback()
                print("[password-reset] Email send failed.")
        else:
            print("[password-reset] SMTP is not configured; reset email not sent.")

    add_flash(
        request,
        "Если аккаунт с таким email существует, инструкция по восстановлению отправлена.",
        "info",
    )
    return redirect("/login")


@app.get("/reset-password", response_class=HTMLResponse)
def reset_password_page(request: Request, token: str = "", db: Session = Depends(get_db)):
    token_value = (token or "").strip()
    token_valid = bool(find_active_password_reset_token(db, token_value)) if token_value else False
    return template_response(
        request,
        "reset_password.html",
        user=None,
        token=token_value,
        token_valid=token_valid,
    )


@app.post("/reset-password")
async def reset_password_submit(request: Request, db: Session = Depends(get_db)):
    form = await request.form()
    token = str(form.get("token", "")).strip()
    new_password = str(form.get("new_password", "")).strip()
    new_password_confirm = str(form.get("new_password_confirm", "")).strip()

    if not token:
        add_flash(request, "Некорректная ссылка восстановления.", "error")
        return redirect("/forgot-password")
    if not new_password:
        add_flash(request, "Введите новый пароль.", "error")
        return redirect(f"/reset-password?token={quote(token)}")
    if len(new_password) < 6:
        add_flash(request, "Новый пароль должен быть не короче 6 символов.", "error")
        return redirect(f"/reset-password?token={quote(token)}")
    if new_password != new_password_confirm:
        add_flash(request, "Новые пароли не совпадают.", "error")
        return redirect(f"/reset-password?token={quote(token)}")

    token_row = find_active_password_reset_token(db, token)
    if not token_row:
        add_flash(request, "Ссылка недействительна или срок её действия истёк.", "error")
        return redirect("/forgot-password")

    target_user = db.get(User, token_row.user_id)
    if not target_user:
        token_row.used_at = datetime.utcnow()
        db.commit()
        add_flash(request, "Ссылка недействительна или срок её действия истёк.", "error")
        return redirect("/forgot-password")

    now_utc = datetime.utcnow()
    target_user.password_hash = password_context.hash(new_password)
    token_row.used_at = now_utc

    other_tokens = db.execute(
        select(PasswordResetToken).where(
            PasswordResetToken.user_id == target_user.id,
            PasswordResetToken.id != token_row.id,
            PasswordResetToken.used_at.is_(None),
        )
    ).scalars().all()
    for item in other_tokens:
        item.used_at = now_utc

    db.commit()
    add_flash(request, "Пароль обновлён. Войдите с новым паролем.", "success")
    return redirect("/login")


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
    telegram_secret_code = str(form.get("telegram_secret_code", "")).strip()
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

    if telegram_secret_code:
        if len(telegram_secret_code) < 6:
            add_flash(request, "Секретный код для ботов должен быть не короче 6 символов.", "error")
            return redirect("/profile")
        user.telegram_secret_code_hash = password_context.hash(telegram_secret_code)
        user.telegram_secret_code_updated_at = datetime.utcnow()
        # Re-auth in bots after code rotation.
        user.telegram_chat_id = None
        user.telegram_linked_at = None
        user.vk_bot_user_id = None
        user.vk_bot_peer_id = None
        user.vk_bot_linked_at = None

    if new_password:
        if new_password != new_password_confirm:
            add_flash(request, "Новые пароли не совпадают.", "error")
            return redirect("/profile")
        user.password_hash = password_context.hash(new_password)

    db.commit()
    add_flash(request, "Профиль обновлён.", "success")
    return redirect("/profile")


@app.post("/profile/vk-bot/unlink")
def profile_vk_bot_unlink(request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    user.vk_bot_user_id = None
    user.vk_bot_peer_id = None
    user.vk_bot_linked_at = None
    db.commit()
    add_flash(request, "VK-бот отвязан от профиля.", "success")
    return redirect("/profile")


@app.get("/cosplan", response_class=HTMLResponse)
def cosplan_list(
    request: Request,
    q: str = "",
    view: str = "cards",
    tab: str = "current",
    plan_filter: str = "all",
    db: Session = Depends(get_db),
):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    all_cards = db.execute(
        select(CosplanCard)
        .where(CosplanCard.user_id == user.id)
        .order_by(CosplanCard.is_priority.desc(), CosplanCard.updated_at.desc())
    ).scalars().all()

    current_tab = tab if tab in {"current", "completed"} else "current"
    current_filter = plan_filter if plan_filter in {"all", "project", "personal", "frozen"} else "all"

    in_progress_rows = db.execute(
        select(InProgressCard).where(InProgressCard.user_id == user.id)
    ).scalars().all()
    in_progress_ids = {row.cosplan_card_id for row in in_progress_rows if row.cosplan_card_id}
    frozen_card_ids = {
        row.cosplan_card_id for row in in_progress_rows if row.cosplan_card_id and bool(row.is_frozen)
    }

    current_cards_pool = [card for card in all_cards if not card.is_completed]
    completed_cards_pool = [card for card in all_cards if card.is_completed]

    if current_tab == "completed":
        cards = list(completed_cards_pool)
    else:
        cards = list(current_cards_pool)
        if current_filter == "project":
            cards = [card for card in cards if (card.plan_type or "") == "project"]
        elif current_filter == "personal":
            cards = [card for card in cards if (card.plan_type or "") != "project"]
        elif current_filter == "frozen":
            cards = [card for card in cards if card.id in frozen_card_ids]

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

        cards = [card for card in cards if matches(card)]

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

    current_view = view if view in {"cards", "list"} else "cards"

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
        current_view=current_view,
        cards_total=len(cards),
        current_tab=current_tab,
        current_filter=current_filter,
        current_count=len(current_cards_pool),
        completed_count=len(completed_cards_pool),
        in_progress_ids=in_progress_ids,
        frozen_card_ids=frozen_card_ids,
        rehearsal_stats_by_card=rehearsal_stats_by_card,
        editable_card_links=editable_card_links,
        current_query=request.url.query or "",
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


@app.post("/cosplan/{card_id}/priority-toggle")
async def cosplan_priority_toggle(card_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    card = get_editable_card(db, card_id, user)
    if not card:
        add_flash(request, "Карточка недоступна для редактирования.", "error")
        return redirect("/cosplan")

    form = await request.form()
    next_url = safe_redirect_target(str(form.get("next", "")).strip(), "/cosplan")

    card.is_priority = not bool(card.is_priority)
    sync_shared_cards_for_nicks(card, user, db)
    db.commit()

    add_flash(
        request,
        "Карточка отмечена как приоритетная." if card.is_priority else "Приоритет для карточки снят.",
        "success",
    )
    return redirect(next_url)


@app.post("/cosplan/{card_id}/completed-toggle")
async def cosplan_completed_toggle(card_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    card = get_editable_card(db, card_id, user)
    if not card:
        add_flash(request, "Карточка недоступна для редактирования.", "error")
        return redirect("/cosplan")

    form = await request.form()
    next_url = safe_redirect_target(str(form.get("next", "")).strip(), "/cosplan")

    card.is_completed = not bool(card.is_completed)
    sync_shared_cards_for_nicks(card, user, db)
    db.commit()

    add_flash(
        request,
        "Карточка перенесена в завершенные." if card.is_completed else "Карточка возвращена в текущие планы.",
        "success",
    )
    return redirect(next_url)


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
        source_card = resolve_source_card(db, card)
        if not source_card or source_card.plan_type != "project":
            continue
        task_assignees_by_progress[row.id] = card_task_assignee_options(
            source_card,
            alias_to_username,
            users_by_username,
        )
        task_rows_by_progress[row.id] = load_scoped_task_rows(
            db,
            source_card,
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
    source_card = resolve_source_card(db, card)
    if not source_card or source_card.plan_type != "project":
        add_flash(request, "Блок «Задания» доступен только для проектных карточек.", "error")
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
        for item in card_task_assignee_options(source_card, alias_to_username, users_by_username)
    }
    if not canonical_assignee or canonical_assignee.casefold() not in allowed_assignees:
        add_flash(request, "Выберите ответственного из списка участников карточки.", "error")
        return redirect("/in-progress")

    existing_rows = load_scoped_task_rows(
        db,
        source_card,
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
    store_scoped_task_rows(db, source_card, existing_rows)

    assignee_user = users_by_username.get(canonical_assignee.casefold())
    if assignee_user and assignee_user.id != user.id:
        enqueue_notification_if_missing(
            db,
            user_id=assignee_user.id,
            from_user_id=user.id,
            source_card_id=source_card.id,
            message=f"Вам назначено задание по «{source_card.character_name}»: {task_text}",
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
    source_card = resolve_source_card(db, card)
    if not source_card or source_card.plan_type != "project":
        add_flash(request, "Блок «Задания» доступен только для проектных карточек.", "error")
        return redirect("/in-progress")

    alias_to_username, users_by_username, _ = build_user_alias_lookup(db)
    rows = load_scoped_task_rows(
        db,
        source_card,
        alias_to_username,
        users_by_username,
    )
    if 0 <= task_index < len(rows):
        rows[task_index]["done"] = not bool(rows[task_index].get("done"))
        store_scoped_task_rows(db, source_card, rows)
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
    source_card = resolve_source_card(db, card)
    if not source_card or source_card.plan_type != "project":
        add_flash(request, "Блок «Задания» доступен только для проектных карточек.", "error")
        return redirect("/in-progress")

    alias_to_username, users_by_username, _ = build_user_alias_lookup(db)
    rows = load_scoped_task_rows(
        db,
        source_card,
        alias_to_username,
        users_by_username,
    )
    if 0 <= task_index < len(rows):
        rows.pop(task_index)
        store_scoped_task_rows(db, source_card, rows)
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

    active_view = normalize_calendar_view(request.query_params.get("view"))
    edit_post_id_raw = str(request.query_params.get("edit_post_id", "")).strip()
    try:
        edit_post_id = int(edit_post_id_raw) if edit_post_id_raw else None
    except ValueError:
        edit_post_id = None

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
            CosplanCard.is_shared_copy.is_(False),
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
    work_shifts = db.execute(
        select(WorkShiftDay)
        .where(
            WorkShiftDay.user_id == user.id,
            WorkShiftDay.shift_date.is_not(None),
            WorkShiftDay.shift_date >= today,
        )
        .order_by(WorkShiftDay.shift_date, WorkShiftDay.id)
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
                "city": event.event_city or "—",
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

    shift_days_by_month: dict[tuple[int, int], set[int]] = defaultdict(set)
    shift_half_days_by_month: dict[tuple[int, int], set[int]] = defaultdict(set)
    for shift_item in work_shifts:
        shift_date = shift_item.shift_date
        if not isinstance(shift_date, date):
            continue
        shift_days_by_month[(shift_date.year, shift_date.month)].add(shift_date.day)
        if bool(shift_item.is_half_day):
            shift_half_days_by_month[(shift_date.year, shift_date.month)].add(shift_date.day)

    month_keys = set(by_month.keys()) | set(shift_days_by_month.keys())

    for year_month in sorted(month_keys):
        year, month = year_month
        month_date = date(year, month, 1)
        month_rows = by_month.get(year_month, [])
        grouped.append(
            {
                "title": month_label_ru(month_date),
                "rows": month_rows,
                "grid_weeks": month_calendar_grid(
                    year,
                    month,
                    month_rows,
                    shift_days=shift_days_by_month.get(year_month, set()),
                    shift_half_days=shift_half_days_by_month.get(year_month, set()),
                ),
            }
        )

    budget_month_groups = build_budget_month_groups(user, db)

    content_posts = db.execute(
        select(ContentPlanPost)
        .where(ContentPlanPost.user_id == user.id)
        .order_by(ContentPlanPost.publish_date, ContentPlanPost.publish_time, ContentPlanPost.id)
    ).scalars().all()
    telegram_settings = get_content_telegram_settings(user, db)
    telegram_channels = list(telegram_settings.get("channels") or [])
    telegram_channels_by_id = {
        str(channel.get("chat_id") or "").strip(): channel
        for channel in telegram_channels
        if str(channel.get("chat_id") or "").strip()
    }
    rubric_tags = get_content_rubric_tags(db, user.id)
    rubric_options = merge_unique(
        get_options(db, user.id, "content_rubric"),
        [post.rubric for post in content_posts if post.rubric],
    )
    rubric_colors = rubric_color_map(rubric_options)
    content_rows: list[dict[str, Any]] = []
    for post in content_posts:
        row_rubric = post.rubric or "Общее"
        row_color = rubric_colors.get(row_rubric, CONTENT_RUBRIC_PALETTE[0])
        row_telegram_channels = [
            telegram_channels_by_id[channel_id]
            for channel_id in as_list(post.telegram_channels_json)
            if channel_id in telegram_channels_by_id
        ]
        content_rows.append(
            {
                "post_id": post.id,
                "date": post.publish_date,
                "time": post.publish_time or "",
                "title": post.title or "Без названия",
                "description": post.description or "",
                "socials_text": ", ".join(as_list(post.socials_json)) or "—",
                "rubric": row_rubric,
                "rubric_color": row_color,
                "status": normalize_content_status(post.status),
                "status_label": CONTENT_STATUS_LABELS.get(normalize_content_status(post.status), "План"),
                "telegram_channels_text": ", ".join(
                    str(channel.get("title") or channel.get("chat_id") or "").strip()
                    for channel in row_telegram_channels
                    if str(channel.get("title") or channel.get("chat_id") or "").strip()
                ) or "—",
                "telegram_published_at": post.telegram_published_at,
            }
        )

    content_by_month: dict[tuple[int, int], list[dict[str, Any]]] = defaultdict(list)
    for row in content_rows:
        publish_date = row.get("date")
        if not isinstance(publish_date, date):
            continue
        content_by_month[(publish_date.year, publish_date.month)].append(row)

    content_month_groups: list[dict[str, Any]] = []
    for year_month in sorted(content_by_month.keys()):
        year, month = year_month
        month_date = date(year, month, 1)
        month_rows = sorted(
            content_by_month[year_month],
            key=lambda item: (item.get("date"), item.get("time") or "", item.get("title") or ""),
        )
        content_month_groups.append(
            {
                "title": month_label_ru(month_date),
                "rows": month_rows,
                "grid_weeks": content_calendar_grid(year, month, month_rows),
            }
        )

    editing_content_post = None
    if edit_post_id:
        editing_content_post = db.execute(
            select(ContentPlanPost).where(
                ContentPlanPost.id == edit_post_id,
                ContentPlanPost.user_id == user.id,
            )
        ).scalar_one_or_none()

    return template_response(
        request,
        "my_calendar.html",
        user=user,
        active_tab="calendars",
        active_calendar_view=active_view,
        month_groups=grouped,
        work_shift_count=len(work_shifts),
        budget_month_groups=budget_month_groups,
        content_month_groups=content_month_groups,
        content_social_options=CONTENT_SOCIAL_OPTIONS,
        content_status_options=CONTENT_STATUS_OPTIONS,
        content_status_labels=CONTENT_STATUS_LABELS,
        content_rubric_options=rubric_options,
        content_rubric_colors=rubric_colors,
        content_rubric_tags=rubric_tags,
        content_form=get_content_plan_form_values(editing_content_post, rubric_tags, telegram_channels),
        editing_content_post=editing_content_post,
        telegram_settings=telegram_settings,
        telegram_settings_masked={
            "bot_token": mask_secret_value(telegram_settings.get("bot_token")),
            "channels_text": telegram_settings.get("channels_text", ""),
        },
        telegram_content_connected=bool(telegram_settings.get("bot_token") and telegram_channels),
        month_weekday_labels=["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"],
    )


@app.post("/my-calendar/events/new")
async def my_calendar_event_create(request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    form = await request.form()
    next_view = normalize_calendar_view(str(form.get("next_view", CALENDAR_VIEW_MY)))
    event_date = parse_date(str(form.get("event_date", "")).strip())
    event_time = parse_time_hhmm(str(form.get("event_time", "")))
    event_title = str(form.get("event_title", "")).strip()
    event_city = str(form.get("event_city", "")).strip()
    event_details = str(form.get("event_details", "")).strip()

    if not event_date:
        add_flash(request, "Укажите дату события.", "error")
        return calendar_redirect_for_view(next_view)
    if not event_title:
        add_flash(request, "Укажите название события.", "error")
        return calendar_redirect_for_view(next_view)

    db.add(
        PersonalCalendarEvent(
            user_id=user.id,
            event_date=event_date,
            event_time=event_time,
            title=event_title,
            event_city=event_city or None,
            details=event_details or None,
        )
    )
    db.commit()
    add_flash(request, "Событие добавлено в календарь.", "success")
    return calendar_redirect_for_view(next_view)


@app.post("/my-calendar/work-shifts/add")
async def my_calendar_work_shift_add(request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    form = await request.form()
    next_view = normalize_calendar_view(str(form.get("next_view", CALENDAR_VIEW_MY)))
    shift_mode = str(form.get("shift_mode", "block")).strip().lower()
    shift_repeat_kind = str(form.get("shift_repeat_kind", "interval")).strip().lower()
    start_date = parse_date(str(form.get("shift_start_date", "")).strip())
    end_date = parse_date(str(form.get("shift_end_date", "")).strip())
    repeat_every_days_raw = str(form.get("shift_repeat_every_days", "7")).strip()
    repeat_weekdays_raw = form.getlist("shift_repeat_weekdays")
    custom_work_days_raw = str(form.get("shift_custom_work_days", "2")).strip()
    custom_rest_days_raw = str(form.get("shift_custom_rest_days", "2")).strip()
    shift_is_half_day = to_bool(form.get("shift_is_half_day"))

    if shift_mode not in {"block", "repeat"}:
        add_flash(request, "Неверный режим добавления смен.", "error")
        return calendar_redirect_for_view(next_view)
    if not start_date or not end_date:
        add_flash(request, "Укажите дату начала и дату конца смен.", "error")
        return calendar_redirect_for_view(next_view)
    if end_date < start_date:
        start_date, end_date = end_date, start_date

    day_span = (end_date - start_date).days
    if day_span > 3650:
        add_flash(request, "Слишком большой диапазон дат. Укажите не более 10 лет.", "error")
        return calendar_redirect_for_view(next_view)

    repeat_every_days = 1
    repeat_weekdays: set[int] = set()
    custom_work_days = 2
    custom_rest_days = 2
    if shift_mode == "repeat":
        if shift_repeat_kind not in {"interval", "weekdays", "two_by_two", "three_by_three", "five_by_two", "custom"}:
            add_flash(request, "Неверный тип повтора смен.", "error")
            return calendar_redirect_for_view(next_view)
        if shift_repeat_kind == "interval":
            try:
                repeat_every_days = int(repeat_every_days_raw)
            except (TypeError, ValueError):
                repeat_every_days = 0
            if repeat_every_days <= 0:
                add_flash(request, "Для повтора укажите шаг в днях больше нуля.", "error")
                return calendar_redirect_for_view(next_view)
            if repeat_every_days > 60:
                add_flash(request, "Шаг повтора слишком большой. Укажите до 60 дней.", "error")
                return calendar_redirect_for_view(next_view)
        elif shift_repeat_kind == "weekdays":
            for value in repeat_weekdays_raw:
                try:
                    weekday = int(str(value).strip())
                except (TypeError, ValueError):
                    continue
                if 0 <= weekday <= 6:
                    repeat_weekdays.add(weekday)
            if not repeat_weekdays:
                add_flash(request, "Для повтора по дням недели выберите хотя бы один день.", "error")
                return calendar_redirect_for_view(next_view)
        elif shift_repeat_kind == "custom":
            try:
                custom_work_days = int(custom_work_days_raw)
                custom_rest_days = int(custom_rest_days_raw)
            except (TypeError, ValueError):
                custom_work_days = 0
                custom_rest_days = 0
            if custom_work_days <= 0 or custom_rest_days <= 0:
                add_flash(request, "Для пользовательского графика укажите рабочие и выходные дни больше нуля.", "error")
                return calendar_redirect_for_view(next_view)
            if custom_work_days > 31 or custom_rest_days > 31:
                add_flash(request, "Для пользовательского графика укажите значения до 31 дня.", "error")
                return calendar_redirect_for_view(next_view)

    target_dates: set[date] = set()

    def append_cycle_dates(work_days: int, rest_days: int) -> None:
        cycle_len = work_days + rest_days
        current_date = start_date
        day_offset = 0
        while current_date <= end_date:
            if day_offset % cycle_len < work_days:
                target_dates.add(current_date)
            current_date += timedelta(days=1)
            day_offset += 1

    if shift_mode == "block":
        current_date = start_date
        while current_date <= end_date:
            target_dates.add(current_date)
            current_date += timedelta(days=1)
    elif shift_repeat_kind == "interval":
        current_date = start_date
        while current_date <= end_date:
            target_dates.add(current_date)
            current_date += timedelta(days=repeat_every_days)
    elif shift_repeat_kind == "weekdays":
        current_date = start_date
        while current_date <= end_date:
            if current_date.weekday() in repeat_weekdays:
                target_dates.add(current_date)
            current_date += timedelta(days=1)
    elif shift_repeat_kind == "two_by_two":
        append_cycle_dates(2, 2)
    elif shift_repeat_kind == "three_by_three":
        append_cycle_dates(3, 3)
    elif shift_repeat_kind == "five_by_two":
        append_cycle_dates(5, 2)
    else:  # custom
        append_cycle_dates(custom_work_days, custom_rest_days)

    if not target_dates:
        add_flash(request, "Не удалось сформировать даты смен.", "error")
        return calendar_redirect_for_view(next_view)

    existing_rows = db.execute(
        select(WorkShiftDay).where(
            WorkShiftDay.user_id == user.id,
            WorkShiftDay.shift_date >= start_date,
            WorkShiftDay.shift_date <= end_date,
        )
    ).scalars().all()
    existing_by_date: dict[date, WorkShiftDay] = {
        item.shift_date: item for item in existing_rows if item.shift_date and item.shift_date in target_dates
    }
    added_count = 0
    updated_count = 0
    for shift_date in sorted(target_dates):
        existing_row = existing_by_date.get(shift_date)
        if existing_row:
            if bool(existing_row.is_half_day) != bool(shift_is_half_day):
                existing_row.is_half_day = bool(shift_is_half_day)
                updated_count += 1
            continue
        db.add(
            WorkShiftDay(
                user_id=user.id,
                shift_date=shift_date,
                is_half_day=bool(shift_is_half_day),
            )
        )
        added_count += 1

    db.commit()
    if added_count <= 0 and updated_count <= 0:
        add_flash(request, "Все выбранные рабочие смены уже были отмечены.", "info")
    else:
        if shift_mode == "block":
            mode_label = "блоком"
        elif shift_repeat_kind == "interval":
            mode_label = f"повтором каждые {repeat_every_days} дн."
        elif shift_repeat_kind == "weekdays":
            mode_label = "повтором по дням недели"
        elif shift_repeat_kind == "two_by_two":
            mode_label = "повтором 2/2"
        elif shift_repeat_kind == "three_by_three":
            mode_label = "повтором 3/3"
        elif shift_repeat_kind == "five_by_two":
            mode_label = "повтором 5/2"
        else:
            mode_label = f"повтором {custom_work_days}/{custom_rest_days}"
        half_day_label = " (половина дня)" if shift_is_half_day else ""
        details: list[str] = []
        if added_count > 0:
            details.append(f"добавлено: {added_count}")
        if updated_count > 0:
            details.append(f"обновлено: {updated_count}")
        add_flash(request, f"Смены сохранены ({mode_label}{half_day_label}) — {', '.join(details)}.", "success")
    return calendar_redirect_for_view(next_view)


@app.post("/my-calendar/work-shifts/clear")
def my_calendar_work_shift_clear(request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")
    next_view = normalize_calendar_view(str(request.query_params.get("view", CALENDAR_VIEW_MY)))

    removed_count = db.execute(
        select(func.count(WorkShiftDay.id)).where(WorkShiftDay.user_id == user.id)
    ).scalar_one()
    db.execute(
        text("DELETE FROM work_shift_days WHERE user_id = :user_id"),
        {"user_id": user.id},
    )
    db.commit()
    if removed_count:
        add_flash(request, f"Удалено рабочих смен: {int(removed_count)}.", "info")
    else:
        add_flash(request, "Рабочих смен для удаления нет.", "info")
    return calendar_redirect_for_view(next_view)


@app.post("/my-calendar/events/{event_id}/delete")
def my_calendar_event_delete(event_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")
    next_view = normalize_calendar_view(str(request.query_params.get("view", CALENDAR_VIEW_MY)))

    event = db.execute(
        select(PersonalCalendarEvent).where(
            PersonalCalendarEvent.id == event_id,
            PersonalCalendarEvent.user_id == user.id,
        )
    ).scalar_one_or_none()
    if not event:
        add_flash(request, "Событие не найдено.", "error")
        return calendar_redirect_for_view(next_view)

    db.delete(event)
    db.commit()
    add_flash(request, "Событие удалено из календаря.", "info")
    return calendar_redirect_for_view(next_view)


@app.post("/my-calendar/content/new")
async def my_calendar_content_create(request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    form = await request.form()
    next_view = normalize_calendar_view(str(form.get("next_view", CALENDAR_VIEW_CONTENT)))
    post = ContentPlanPost(user_id=user.id, title="", publish_date=date.today(), rubric="Общее")
    ok, error_text = save_content_plan_post_from_form(form, post, user, db)
    if not ok:
        add_flash(request, error_text, "error")
        return calendar_redirect_for_view(next_view)

    db.add(post)
    db.commit()
    add_flash(request, "Пост добавлен в контент-план.", "success")
    return calendar_redirect_for_view(next_view)


@app.post("/my-calendar/content/{post_id}/edit")
async def my_calendar_content_update(post_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    post = db.execute(
        select(ContentPlanPost).where(
            ContentPlanPost.id == post_id,
            ContentPlanPost.user_id == user.id,
        )
    ).scalar_one_or_none()
    if not post:
        add_flash(request, "Пост контент-плана не найден.", "error")
        return calendar_redirect_for_view(CALENDAR_VIEW_CONTENT)

    form = await request.form()
    next_view = normalize_calendar_view(str(form.get("next_view", CALENDAR_VIEW_CONTENT)))
    ok, error_text = save_content_plan_post_from_form(form, post, user, db)
    if not ok:
        add_flash(request, error_text, "error")
        return calendar_redirect_for_view(next_view)

    db.commit()
    add_flash(request, "Пост контент-плана обновлен.", "success")
    return calendar_redirect_for_view(next_view)


@app.post("/my-calendar/content/{post_id}/delete")
def my_calendar_content_delete(post_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    next_view = normalize_calendar_view(str(request.query_params.get("view", CALENDAR_VIEW_CONTENT)))
    post = db.execute(
        select(ContentPlanPost).where(
            ContentPlanPost.id == post_id,
            ContentPlanPost.user_id == user.id,
        )
    ).scalar_one_or_none()
    if not post:
        add_flash(request, "Пост контент-плана не найден.", "error")
        return calendar_redirect_for_view(next_view)

    db.delete(post)
    db.commit()
    add_flash(request, "Пост удален из контент-плана.", "info")
    return calendar_redirect_for_view(next_view)


@app.post("/my-calendar/content/telegram/connect")
async def my_calendar_content_telegram_connect(request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    form = await request.form()
    bot_token = str(form.get("bot_token", "")).strip()
    telegram_channels_text = str(form.get("telegram_channels_text", "")).strip()
    premium_emojis_text = str(form.get("premium_emojis_text", "")).strip()

    if not bot_token or ":" not in bot_token:
        add_flash(request, "Укажите корректный токен Telegram-бота.", "error")
        return calendar_redirect_for_view(CALENDAR_VIEW_CONTENT)

    channel_entries, channel_error = parse_content_telegram_channel_lines(telegram_channels_text)
    if channel_error:
        add_flash(request, channel_error, "error")
        return calendar_redirect_for_view(CALENDAR_VIEW_CONTENT)
    if not channel_entries:
        add_flash(request, "Добавьте хотя бы один Telegram-канал для публикации.", "error")
        return calendar_redirect_for_view(CALENDAR_VIEW_CONTENT)

    premium_entries, premium_error = parse_content_premium_emoji_lines(premium_emojis_text)
    if premium_error:
        add_flash(request, premium_error, "error")
        return calendar_redirect_for_view(CALENDAR_VIEW_CONTENT)

    set_user_option_value(db, user.id, CONTENT_TELEGRAM_TOKEN_GROUP, bot_token)
    set_user_option_value(db, user.id, CONTENT_TELEGRAM_CHAT_GROUP, channel_entries[0]["chat_id"])
    set_user_option_value(db, user.id, CONTENT_TELEGRAM_PACK_GROUP, "")
    replace_user_option_values(
        db,
        user.id,
        CONTENT_TELEGRAM_CHANNEL_GROUP,
        [
            encode_content_telegram_channel_value(entry["title"], entry["chat_id"])
            for entry in channel_entries
        ],
    )
    replace_user_option_values(
        db,
        user.id,
        CONTENT_TELEGRAM_PREMIUM_EMOJI_GROUP,
        [encode_content_premium_emoji_value(entry["emoji"], entry["emoji_id"]) for entry in premium_entries],
    )
    db.commit()
    add_flash(request, "Настройки Telegram-канала сохранены.", "success")
    return calendar_redirect_for_view(CALENDAR_VIEW_CONTENT)


@app.post("/my-calendar/content/telegram/disconnect")
def my_calendar_content_telegram_disconnect(request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")
    set_user_option_value(db, user.id, CONTENT_TELEGRAM_TOKEN_GROUP, "")
    set_user_option_value(db, user.id, CONTENT_TELEGRAM_CHAT_GROUP, "")
    set_user_option_value(db, user.id, CONTENT_TELEGRAM_PACK_GROUP, "")
    replace_user_option_values(db, user.id, CONTENT_TELEGRAM_CHANNEL_GROUP, [])
    db.commit()
    add_flash(request, "Настройки Telegram-каналов удалены. Библиотека premium-эмодзи сохранена.", "info")
    return calendar_redirect_for_view(CALENDAR_VIEW_CONTENT)


@app.post("/my-calendar/content/{post_id}/telegram-publish")
def my_calendar_content_publish_telegram(post_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    post = db.execute(
        select(ContentPlanPost).where(
            ContentPlanPost.id == post_id,
            ContentPlanPost.user_id == user.id,
        )
    ).scalar_one_or_none()
    if not post:
        add_flash(request, "Пост контент-плана не найден.", "error")
        return calendar_redirect_for_view(CALENDAR_VIEW_CONTENT)

    telegram_settings = get_content_telegram_settings(user, db)
    bot_token = telegram_settings.get("bot_token", "")
    available_channels = list(telegram_settings.get("channels") or [])
    if not bot_token or not available_channels:
        add_flash(request, "Сначала подключите Telegram-каналы в настройках контент-плана.", "error")
        return calendar_redirect_for_view(CALENDAR_VIEW_CONTENT)

    selected_channels = resolve_content_telegram_channels(as_list(post.telegram_channels_json), available_channels)
    if not selected_channels and len(available_channels) == 1:
        selected_channels = [available_channels[0]]
    if not selected_channels:
        add_flash(request, "Выберите хотя бы один Telegram-канал в карточке поста.", "error")
        return calendar_redirect_for_view(CALENDAR_VIEW_CONTENT)

    try:
        rubric_tag = normalize_content_rubric_tag(post.rubric_tag) or get_content_rubric_tags(db, user.id).get(post.rubric or "", "")
        sent_messages, send_errors = publish_content_post_to_telegram_channels(
            token=bot_token,
            channels=selected_channels,
            post=post,
            rubric_tag=rubric_tag,
        )
    except Exception as exc:
        add_flash(request, str(exc), "error")
        return calendar_redirect_for_view(CALENDAR_VIEW_CONTENT)

    if not sent_messages:
        add_flash(request, send_errors[0] if send_errors else "Не удалось опубликовать пост в Telegram.", "error")
        return calendar_redirect_for_view(CALENDAR_VIEW_CONTENT)

    mark_content_post_telegram_published(
        post,
        channel_message_ids=sent_messages,
        rubric_tag=rubric_tag,
    )
    db.commit()
    success_text = f"Пост опубликован в Telegram-каналы: {len(sent_messages)}."
    if send_errors:
        success_text = f"{success_text} Не удалось отправить в: {'; '.join(send_errors)}"
    add_flash(request, success_text, "success")
    return calendar_redirect_for_view(CALENDAR_VIEW_CONTENT)


def ics_calendar_header() -> list[str]:
    return [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//Cosplay Planner//RU",
        "CALSCALE:GREGORIAN",
        "METHOD:PUBLISH",
    ]


def append_ics_event(
    lines: list[str],
    *,
    dtstamp: str,
    uid_prefix: str,
    summary: str,
    event_date: date,
    time_hhmm: str | None = None,
    duration_minutes: int = 60,
    range_end_date: date | None = None,
    location: str | None = None,
    url: str | None = None,
    description: str | None = None,
) -> None:
    lines.extend(
        [
            "BEGIN:VEVENT",
            f"UID:{uid_prefix}-{uuid.uuid4().hex[:12]}@cosplay-planner.local",
            f"DTSTAMP:{dtstamp}",
            f"SUMMARY:{esc_ics(summary)}",
        ]
    )
    normalized_time = parse_time_hhmm(time_hhmm or "")
    if normalized_time:
        hh_raw, mm_raw = normalized_time.split(":", 1)
        start_dt = datetime.combine(event_date, datetime.min.time()).replace(hour=int(hh_raw), minute=int(mm_raw))
        end_dt = start_dt + timedelta(minutes=max(duration_minutes, 15))
        lines.append(f"DTSTART:{start_dt.strftime('%Y%m%dT%H%M%S')}")
        lines.append(f"DTEND:{end_dt.strftime('%Y%m%dT%H%M%S')}")
    else:
        lines.append(f"DTSTART;VALUE=DATE:{event_date.strftime('%Y%m%d')}")
        effective_end = range_end_date if range_end_date and range_end_date >= event_date else event_date
        lines.append(f"DTEND;VALUE=DATE:{(effective_end + timedelta(days=1)).strftime('%Y%m%d')}")

    if location:
        lines.append(f"LOCATION:{esc_ics(location)}")
    if url:
        lines.append(f"URL:{esc_ics(url)}")
    if description:
        lines.append(f"DESCRIPTION:{esc_ics(description)}")
    lines.append("END:VEVENT")


@app.get("/my-calendar/export.ics")
def my_calendar_export_ics(request: Request, db: Session = Depends(get_db)):
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
            CosplanCard.is_shared_copy.is_(False),
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

    lines = ics_calendar_header()
    dtstamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

    for festival in festivals:
        if not festival.event_date:
            continue
        coproplayers_display = format_coproplayer_names(
            as_list(festival.going_coproplayers_json),
            alias_to_username,
            users_by_username,
        )
        description_parts = ["Фестиваль"]
        nomination_values = [item for item in [festival.nomination_1, festival.nomination_2, festival.nomination_3] if item]
        if nomination_values:
            description_parts.append("Номинации: " + ", ".join(nomination_values))
        if coproplayers_display:
            description_parts.append("Сокосплееры: " + ", ".join(coproplayers_display))
        append_ics_event(
            lines,
            dtstamp=dtstamp,
            uid_prefix=f"festival-{festival.id}",
            summary=f"Фестиваль: {festival.name or 'Без названия'}",
            event_date=festival.event_date,
            range_end_date=festival_range_end(festival),
            location=festival.city or "",
            url=festival.url or "",
            description="\n".join(description_parts),
        )

    for card in cards:
        if not card.photoset_date:
            continue
        card_coproplayers = as_list(card.coproplayers_json) or as_list(card.coproplayer_nicks_json)
        coproplayers_display = format_coproplayer_names(
            card_coproplayers,
            alias_to_username,
            users_by_username,
        )
        description_parts = ["Фотосет"]
        if coproplayers_display:
            description_parts.append("Сокосплееры: " + ", ".join(coproplayers_display))
        append_ics_event(
            lines,
            dtstamp=dtstamp,
            uid_prefix=f"photoset-{card.id}",
            summary=f"Фотосет: {card.character_name or 'Без названия'}",
            event_date=card.photoset_date,
            location=card.city or "",
            description="\n".join(description_parts),
        )

    for entry in rehearsal_entries:
        if not entry.entry_date or not entry.cosplan_card:
            continue
        append_ics_event(
            lines,
            dtstamp=dtstamp,
            uid_prefix=f"rehearsal-{entry.id}",
            summary=f"Репетиция: {entry.cosplan_card.character_name or 'Без названия'}",
            event_date=entry.entry_date,
            time_hhmm=entry.entry_time or "",
            duration_minutes=120,
            location=entry.cosplan_card.city or "",
            description="Репетиция по карточке косплана.",
        )

    for event in personal_events:
        append_ics_event(
            lines,
            dtstamp=dtstamp,
            uid_prefix=f"personal-{event.id}",
            summary=f"Личное: {event.title or 'Событие'}",
            event_date=event.event_date,
            time_hhmm=event.event_time or "",
            duration_minutes=60,
            location=event.event_city or "",
            description=event.details or "",
        )

    lines.append("END:VCALENDAR")
    body = "\r\n".join(lines) + "\r\n"
    return PlainTextResponse(
        body,
        media_type="text/calendar; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="cosplay-my-calendar.ics"'},
    )


@app.get("/my-calendar/content/export.ics")
def my_calendar_content_export_ics(request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    today = date.today()
    posts = db.execute(
        select(ContentPlanPost)
        .where(
            ContentPlanPost.user_id == user.id,
            ContentPlanPost.publish_date.is_not(None),
            ContentPlanPost.publish_date >= today,
        )
        .order_by(ContentPlanPost.publish_date, ContentPlanPost.publish_time, ContentPlanPost.id)
    ).scalars().all()

    lines = ics_calendar_header()
    dtstamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

    for post in posts:
        socials_text = ", ".join(as_list(post.socials_json)) or "—"
        description_parts = [
            f"Рубрика: {post.rubric or 'Общее'}",
            f"Площадки: {socials_text}",
            f"Статус: {CONTENT_STATUS_LABELS.get(normalize_content_status(post.status), 'План')}",
        ]
        if post.description:
            description_parts.append(post.description)
        append_ics_event(
            lines,
            dtstamp=dtstamp,
            uid_prefix=f"content-{post.id}",
            summary=f"Контент: {post.title or 'Пост'}",
            event_date=post.publish_date,
            time_hhmm=post.publish_time or "",
            duration_minutes=30,
            description="\n".join(description_parts),
        )

    lines.append("END:VCALENDAR")
    body = "\r\n".join(lines) + "\r\n"
    return PlainTextResponse(
        body,
        media_type="text/calendar; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="cosplay-content-plan.ics"'},
    )


def project_board_fandom_options(db: Session, user: User) -> list[str]:
    global_fandoms = db.execute(
        select(ProjectSearchPost.fandom).where(ProjectSearchPost.fandom.is_not(None)).order_by(ProjectSearchPost.fandom)
    ).scalars().all()
    return merge_unique(global_fandoms, get_options(db, user.id, "fandom"))


def project_board_city_options(db: Session, user: User) -> list[str]:
    global_cities = db.execute(
        select(ProjectSearchPost.city).where(ProjectSearchPost.city.is_not(None)).order_by(ProjectSearchPost.city)
    ).scalars().all()
    return merge_unique(global_cities, get_options(db, user.id, "project_board_city"), get_options(db, user.id, "city"))


def save_project_search_post_from_form(form: Any, post: ProjectSearchPost) -> tuple[bool, str]:
    fandom = str(form.get("fandom", "")).strip()
    city = str(form.get("city", "")).strip()
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
    post.city = city or None
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
    selected_city = request.query_params.get("city", "").strip()
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
    if selected_city:
        posts = [post for post in posts if city_matches(selected_city, post.city)]
    if q:
        needle = q.casefold()
        posts = [
            post
            for post in posts
            if needle in (post.fandom or "").casefold()
            or needle in (post.city or "").casefold()
            or needle in (post.comment or "").casefold()
            or needle in (post.contact_nick or "").casefold()
            or needle in (post.contact_link or "").casefold()
            or needle in ("фотосет" if post.event_type == "photoset" else "фестиваль")
        ]
    posts = sorted(
        posts,
        key=lambda post: 1 if (post.status or PROJECT_BOARD_STATUS_ACTIVE) == PROJECT_BOARD_STATUS_INACTIVE else 0,
    )

    city_options = project_board_city_options(db, user)

    owner_ids = {post.user_id for post in posts}
    post_ids = [post.id for post in posts]
    comments_by_post: dict[int, list[ProjectSearchComment]] = defaultdict(list)

    if post_ids:
        comments = db.execute(
            select(ProjectSearchComment)
            .where(ProjectSearchComment.post_id.in_(post_ids))
            .order_by(ProjectSearchComment.created_at.desc(), ProjectSearchComment.id.desc())
        ).scalars().all()
        for item in comments:
            if len(comments_by_post[item.post_id]) >= 5:
                continue
            comments_by_post[item.post_id].append(item)
            owner_ids.add(item.user_id)

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
        comments_by_post=comments_by_post,
        q=q,
        selected_city=selected_city,
        city_options=city_options,
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
        city_options=project_board_city_options(db, user),
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
    remember_options(db, user.id, "project_board_city", [post.city or ""])
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
        city_options=project_board_city_options(db, user),
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
    remember_options(db, user.id, "project_board_city", [post.city or ""])
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


@app.post("/project-board/{post_id}/comments")
async def project_board_add_comment(post_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    post = db.get(ProjectSearchPost, post_id)
    if not post:
        add_flash(request, "Объявление не найдено.", "error")
        return redirect("/project-board")

    form = await request.form()
    body = str(form.get("comment_body", "")).strip()
    if not body:
        add_flash(request, "Введите комментарий.", "error")
        return redirect("/project-board")

    db.add(
        ProjectSearchComment(
            post_id=post.id,
            user_id=user.id,
            body=body,
        )
    )
    if post.user_id != user.id:
        preview = body if len(body) <= 120 else body[:117].rstrip() + "..."
        db.add(
            FestivalNotification(
                user_id=post.user_id,
                from_user_id=user.id,
                source_card_id=None,
                message=(
                    "Новый комментарий в вашем объявлении поиска "
                    f"«{post.fandom}» от @{preferred_user_alias(user)}: {preview}"
                ),
                is_read=False,
            )
        )
    db.commit()
    add_flash(request, "Комментарий добавлен.", "success")
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


def master_rating_maps(
    db: Session,
    master_ids: list[int],
    current_user_id: int | None = None,
) -> tuple[dict[int, float], dict[int, int], dict[int, int]]:
    if not master_ids:
        return {}, {}, {}

    aggregate_rows = db.execute(
        select(
            CommunityMasterRating.master_id,
            func.avg(CommunityMasterRating.stars),
            func.count(CommunityMasterRating.id),
        )
        .where(CommunityMasterRating.master_id.in_(master_ids))
        .group_by(CommunityMasterRating.master_id)
    ).all()
    avg_by_master: dict[int, float] = {}
    count_by_master: dict[int, int] = {}
    for row in aggregate_rows:
        master_id = int(row[0])
        avg_raw = row[1]
        count_raw = row[2]
        avg_by_master[master_id] = float(avg_raw or 0.0)
        count_by_master[master_id] = int(count_raw or 0)

    user_by_master: dict[int, int] = {}
    if current_user_id:
        user_rows = db.execute(
            select(CommunityMasterRating.master_id, CommunityMasterRating.stars).where(
                CommunityMasterRating.master_id.in_(master_ids),
                CommunityMasterRating.user_id == current_user_id,
            )
        ).all()
        for row in user_rows:
            user_by_master[int(row[0])] = int(row[1] or 0)

    return avg_by_master, count_by_master, user_by_master


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
    master_ids = [item.id for item in masters]
    rating_avg_by_master, rating_count_by_master, user_rating_by_master = master_rating_maps(
        db,
        master_ids,
        current_user_id=user.id,
    )

    return template_response(
        request,
        "community_masters_list.html",
        user=user,
        active_tab="community",
        community_tab="masters",
        masters=masters,
        owners_by_id=owners_by_id,
        comment_counts=comment_counts,
        rating_avg_by_master=rating_avg_by_master,
        rating_count_by_master=rating_count_by_master,
        user_rating_by_master=user_rating_by_master,
        can_import_masters=user_is_special(user) and VK_IMPORT_ENABLED,
        import_source_labels=IMPORT_SOURCE_LABELS,
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
    orders: list[CommunityMasterOrder] = []
    if can_manage_master(user, master):
        orders = db.execute(
            select(CommunityMasterOrder)
            .where(CommunityMasterOrder.master_id == master.id)
            .order_by(CommunityMasterOrder.created_at.desc(), CommunityMasterOrder.id.desc())
        ).scalars().all()

    author_ids = {master.user_id, *(item.user_id for item in comments), *(item.user_id for item in orders)}
    authors_by_id: dict[int, User] = {}
    if author_ids:
        authors = db.execute(select(User).where(User.id.in_(author_ids))).scalars().all()
        authors_by_id = {item.id: item for item in authors}
    rating_avg_by_master, rating_count_by_master, user_rating_by_master = master_rating_maps(
        db,
        [master.id],
        current_user_id=user.id,
    )

    return template_response(
        request,
        "community_master_detail.html",
        user=user,
        active_tab="community",
        community_tab="masters",
        master=master,
        comments=comments,
        orders=orders,
        authors_by_id=authors_by_id,
        price_rows=format_master_price_rows_for_form(as_list(master.price_list_json)),
        rating_avg=rating_avg_by_master.get(master.id, 0.0),
        rating_count=rating_count_by_master.get(master.id, 0),
        user_rating=user_rating_by_master.get(master.id, 0),
        import_source_labels=IMPORT_SOURCE_LABELS,
        can_manage_master=can_manage_master(user, master),
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
    if not can_manage_master(user, master):
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
    if not can_manage_master(user, master):
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
    images_input = str(form.get("images_input", ""))
    images = parse_reference_values(images_input)
    is_client = to_bool(form.get("is_client"))

    if not body and not images:
        add_flash(request, "Добавьте текст комментария или хотя бы одну картинку.", "error")
        return redirect(f"/community/masters/{master_id}")

    db.add(
        CommunityMasterComment(
            master_id=master.id,
            user_id=user.id,
            body=body or "",
            is_client=is_client,
            images_json=images,
        )
    )
    if master.user_id != user.id:
        preview = (body or "Добавлено изображение").strip()
        if len(preview) > 120:
            preview = preview[:117].rstrip() + "..."
        db.add(
            FestivalNotification(
                user_id=master.user_id,
                from_user_id=user.id,
                source_card_id=None,
                message=(
                    "Новый комментарий в вашей карточке мастера "
                    f"@{normalize_username(master.nick)}: {preview}"
                ),
                is_read=False,
            )
        )
    db.commit()
    add_flash(request, "Комментарий добавлен.", "success")
    return redirect(f"/community/masters/{master_id}")


@app.post("/community/masters/{master_id}/orders")
async def community_masters_create_order(master_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    master = db.get(CommunityMaster, master_id)
    if not master:
        add_flash(request, "Карточка мастера не найдена.", "error")
        return redirect("/community/masters")
    if master.user_id == user.id:
        add_flash(request, "Нельзя отправить заказ самому себе.", "error")
        return redirect(f"/community/masters/{master_id}")

    form = await request.form()
    order = CommunityMasterOrder(master_id=master.id, user_id=user.id, subject="")
    ok, error_text = save_master_order_from_form(form, order)
    if not ok:
        add_flash(request, error_text, "error")
        return redirect(f"/community/masters/{master_id}#master-order-form")

    db.add(order)
    enqueue_notification_if_missing(
        db,
        user_id=master.user_id,
        from_user_id=user.id,
        source_card_id=None,
        message=f"У ВАС НОВЫЙ ЗАКАЗ: {order.subject}",
    )
    db.commit()
    add_flash(request, "Заявка отправлена мастеру.", "success")
    return redirect(f"/community/masters/{master_id}")


@app.post("/community/masters/{master_id}/orders/{order_id}/delete")
def community_masters_delete_order(master_id: int, order_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    master = db.get(CommunityMaster, master_id)
    if not master:
        add_flash(request, "Карточка мастера не найдена.", "error")
        return redirect("/community/masters")
    if not can_manage_master(user, master):
        add_flash(request, "Удалять заявки может только владелец карточки мастера.", "error")
        return redirect(f"/community/masters/{master_id}")

    order = db.execute(
        select(CommunityMasterOrder).where(
            CommunityMasterOrder.id == order_id,
            CommunityMasterOrder.master_id == master.id,
        )
    ).scalar_one_or_none()
    if not order:
        add_flash(request, "Заявка не найдена.", "error")
        return redirect(f"/community/masters/{master_id}")

    db.delete(order)
    db.commit()
    add_flash(request, "Заявка удалена.", "info")
    return redirect(f"/community/masters/{master_id}")


@app.post("/community/masters/{master_id}/rate")
async def community_masters_rate(master_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    master = db.get(CommunityMaster, master_id)
    if not master:
        add_flash(request, "Карточка мастера не найдена.", "error")
        return redirect("/community/masters")

    form = await request.form()
    stars_raw = str(form.get("stars", "")).strip()
    try:
        stars = int(stars_raw)
    except ValueError:
        stars = 0
    if stars < 1 or stars > 5:
        add_flash(request, "Оценка должна быть от 1 до 5.", "error")
        return redirect(f"/community/masters/{master_id}")

    existing = db.execute(
        select(CommunityMasterRating).where(
            CommunityMasterRating.master_id == master.id,
            CommunityMasterRating.user_id == user.id,
        )
    ).scalar_one_or_none()
    if existing:
        existing.stars = stars
        add_flash(request, "Оценка обновлена.", "success")
    else:
        db.add(
            CommunityMasterRating(
                master_id=master.id,
                user_id=user.id,
                stars=stars,
            )
        )
        add_flash(request, "Спасибо за оценку!", "success")
    db.commit()
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
    if not can_manage_master(user, master):
        add_flash(request, "Удалять можно только свою карточку мастера.", "error")
        return redirect(f"/community/masters/{master_id}")

    db.delete(master)
    db.commit()
    add_flash(request, "Карточка мастера удалена.", "info")
    return redirect("/community/masters")


def normalize_cosplayer_skills(raw_skills: list[str]) -> list[str]:
    normalized_map = {value.casefold(): value for value in COSPLAYER_SKILL_OPTIONS}
    result: list[str] = []
    for raw_value in raw_skills:
        key = str(raw_value or "").strip().casefold()
        if not key:
            continue
        skill = normalized_map.get(key)
        if skill and skill not in result:
            result.append(skill)
    return result


def get_cosplayer_form_values(cosplayer: CommunityCosplayer | None = None) -> dict[str, Any]:
    if not cosplayer:
        return {
            "nick": "",
            "tg_channel": "",
            "city": "",
            "favorite_directions": "",
            "promo_input": "",
            "about_markdown": "",
            "collab_status": "open",
            "extra_skills_json": [],
        }
    return {
        "nick": cosplayer.nick or "",
        "tg_channel": cosplayer.tg_channel or "",
        "city": cosplayer.city or "",
        "favorite_directions": cosplayer.favorite_directions or "",
        "promo_input": "\n".join(as_list(cosplayer.promo_photos_json)),
        "about_markdown": cosplayer.about_markdown or "",
        "collab_status": cosplayer.collab_status or "open",
        "extra_skills_json": normalize_cosplayer_skills(as_list(cosplayer.extra_skills_json)),
    }


def save_cosplayer_from_form(form: Any, cosplayer: CommunityCosplayer) -> tuple[bool, str]:
    nick = normalize_username(str(form.get("nick", "")).strip())
    tg_channel = str(form.get("tg_channel", "")).strip()
    city = str(form.get("city", "")).strip()
    favorite_directions = str(form.get("favorite_directions", "")).strip()
    promo_input = str(form.get("promo_input", ""))
    about_markdown = str(form.get("about_markdown", "")).strip()
    collab_status = str(form.get("collab_status", "open")).strip().lower()
    extra_skills = normalize_cosplayer_skills([str(item) for item in form.getlist("extra_skills")])

    if not nick:
        return False, "Укажите ник косплеера."
    if collab_status not in COSPLAYER_COLLAB_OPTIONS:
        return False, "Выберите корректную готовность к коллаборациям."
    if len(tg_channel) > 255:
        return False, "Поле ТГК слишком длинное (до 255 символов)."
    if len(city) > 255:
        return False, "Поле «Город» слишком длинное (до 255 символов)."
    if len(favorite_directions) > 5000:
        return False, "Поле «Любимые направления» должно быть до 5000 символов."
    if len(about_markdown) > 5000:
        return False, "Поле «О себе» должно быть до 5000 символов."

    promo_photos = parse_reference_values(promo_input)
    if len(promo_photos) > 5:
        return False, "Можно добавить не более 5 промо-фото."

    cosplayer.nick = nick
    cosplayer.tg_channel = tg_channel or None
    cosplayer.city = city or None
    cosplayer.favorite_directions = favorite_directions or None
    cosplayer.promo_photos_json = promo_photos
    cosplayer.about_markdown = about_markdown or None
    cosplayer.collab_status = collab_status
    cosplayer.extra_skills_json = extra_skills
    return True, ""


@app.get("/community/cosplayers", response_class=HTMLResponse)
def community_cosplayers_list(request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    q = request.query_params.get("q", "").strip()
    city_filter = request.query_params.get("city", "").strip()
    selected_skills = normalize_cosplayer_skills([str(item) for item in request.query_params.getlist("skill")])

    cosplayers = db.execute(
        select(CommunityCosplayer).order_by(CommunityCosplayer.updated_at.desc(), CommunityCosplayer.id.desc())
    ).scalars().all()

    if q:
        needle = q.casefold()
        cosplayers = [
            item
            for item in cosplayers
            if needle in (item.nick or "").casefold()
            or needle in (item.city or "").casefold()
            or needle in (item.favorite_directions or "").casefold()
        ]
    if city_filter:
        city_values = split_city_values(city_filter)
        cosplayers = [item for item in cosplayers if city_matches_any(city_values, item.city)]
    if selected_skills:
        selected_keys = {value.casefold() for value in selected_skills}
        filtered: list[CommunityCosplayer] = []
        for item in cosplayers:
            item_keys = {str(skill).strip().casefold() for skill in as_list(item.extra_skills_json) if str(skill).strip()}
            if selected_keys & item_keys:
                filtered.append(item)
        cosplayers = filtered

    owner_ids = {item.user_id for item in cosplayers}
    owners_by_id: dict[int, User] = {}
    if owner_ids:
        owners = db.execute(select(User).where(User.id.in_(owner_ids))).scalars().all()
        owners_by_id = {item.id: item for item in owners}

    comment_rows = db.execute(
        select(CommunityCosplayerComment.cosplayer_id, func.count(CommunityCosplayerComment.id))
        .group_by(CommunityCosplayerComment.cosplayer_id)
    ).all()
    comment_counts = {int(row[0]): int(row[1]) for row in comment_rows}

    city_options = sorted(
        merge_unique([item.city for item in cosplayers if item.city]),
        key=lambda value: value.casefold(),
    )

    return template_response(
        request,
        "community_cosplayers_list.html",
        user=user,
        active_tab="community",
        community_tab="cosplayers",
        cosplayers=cosplayers,
        owners_by_id=owners_by_id,
        comment_counts=comment_counts,
        q=q,
        city_filter=city_filter,
        selected_skills=selected_skills,
        city_options=city_options,
        collab_labels=COSPLAYER_COLLAB_OPTIONS,
        skills_options=COSPLAYER_SKILL_OPTIONS,
    )


@app.get("/community/cosplayers/new", response_class=HTMLResponse)
def community_cosplayers_new(request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")
    return template_response(
        request,
        "community_cosplayer_form.html",
        user=user,
        active_tab="community",
        community_tab="cosplayers",
        editing=False,
        cosplayer_id=None,
        form=get_cosplayer_form_values(),
        collab_options=COSPLAYER_COLLAB_OPTIONS,
        skills_options=COSPLAYER_SKILL_OPTIONS,
    )


@app.post("/community/cosplayers/new")
async def community_cosplayers_create(request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    form = await request.form()
    cosplayer = CommunityCosplayer(user_id=user.id, nick="", collab_status="open")
    ok, error_text = save_cosplayer_from_form(form, cosplayer)
    if not ok:
        add_flash(request, error_text, "error")
        return redirect("/community/cosplayers/new")

    db.add(cosplayer)
    db.commit()
    add_flash(request, "Карточка косплеера опубликована.", "success")
    return redirect(f"/community/cosplayers/{cosplayer.id}")


@app.get("/community/cosplayers/{cosplayer_id}", response_class=HTMLResponse)
def community_cosplayers_detail(cosplayer_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    cosplayer = db.get(CommunityCosplayer, cosplayer_id)
    if not cosplayer:
        add_flash(request, "Карточка косплеера не найдена.", "error")
        return redirect("/community/cosplayers")

    comments = db.execute(
        select(CommunityCosplayerComment)
        .where(CommunityCosplayerComment.cosplayer_id == cosplayer.id)
        .order_by(CommunityCosplayerComment.created_at, CommunityCosplayerComment.id)
    ).scalars().all()

    author_ids = {cosplayer.user_id, *(item.user_id for item in comments)}
    authors_by_id: dict[int, User] = {}
    if author_ids:
        authors = db.execute(select(User).where(User.id.in_(author_ids))).scalars().all()
        authors_by_id = {item.id: item for item in authors}

    return template_response(
        request,
        "community_cosplayer_detail.html",
        user=user,
        active_tab="community",
        community_tab="cosplayers",
        cosplayer=cosplayer,
        comments=comments,
        authors_by_id=authors_by_id,
        collab_labels=COSPLAYER_COLLAB_OPTIONS,
        about_html=render_article_markdown(cosplayer.about_markdown or ""),
        can_manage=(cosplayer.user_id == user.id or is_moderator_user(user)),
    )


@app.get("/community/cosplayers/{cosplayer_id}/edit", response_class=HTMLResponse)
def community_cosplayers_edit(cosplayer_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    cosplayer = db.get(CommunityCosplayer, cosplayer_id)
    if not cosplayer:
        add_flash(request, "Карточка косплеера не найдена.", "error")
        return redirect("/community/cosplayers")
    if cosplayer.user_id != user.id and not is_moderator_user(user):
        add_flash(request, "Редактировать можно только свою карточку косплеера.", "error")
        return redirect(f"/community/cosplayers/{cosplayer_id}")

    return template_response(
        request,
        "community_cosplayer_form.html",
        user=user,
        active_tab="community",
        community_tab="cosplayers",
        editing=True,
        cosplayer_id=cosplayer.id,
        form=get_cosplayer_form_values(cosplayer),
        collab_options=COSPLAYER_COLLAB_OPTIONS,
        skills_options=COSPLAYER_SKILL_OPTIONS,
    )


@app.post("/community/cosplayers/{cosplayer_id}/edit")
async def community_cosplayers_update(cosplayer_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    cosplayer = db.get(CommunityCosplayer, cosplayer_id)
    if not cosplayer:
        add_flash(request, "Карточка косплеера не найдена.", "error")
        return redirect("/community/cosplayers")
    if cosplayer.user_id != user.id and not is_moderator_user(user):
        add_flash(request, "Редактировать можно только свою карточку косплеера.", "error")
        return redirect(f"/community/cosplayers/{cosplayer_id}")

    form = await request.form()
    ok, error_text = save_cosplayer_from_form(form, cosplayer)
    if not ok:
        add_flash(request, error_text, "error")
        return redirect(f"/community/cosplayers/{cosplayer_id}/edit")

    db.commit()
    add_flash(request, "Карточка косплеера обновлена.", "success")
    return redirect(f"/community/cosplayers/{cosplayer_id}")


@app.post("/community/cosplayers/{cosplayer_id}/delete")
def community_cosplayers_delete(cosplayer_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    cosplayer = db.get(CommunityCosplayer, cosplayer_id)
    if not cosplayer:
        add_flash(request, "Карточка косплеера не найдена.", "error")
        return redirect("/community/cosplayers")
    if cosplayer.user_id != user.id and not is_moderator_user(user):
        add_flash(request, "Удалять можно только свою карточку косплеера.", "error")
        return redirect(f"/community/cosplayers/{cosplayer_id}")

    db.delete(cosplayer)
    db.commit()
    add_flash(request, "Карточка косплеера удалена.", "info")
    return redirect("/community/cosplayers")


@app.post("/community/cosplayers/{cosplayer_id}/comments")
async def community_cosplayers_add_comment(cosplayer_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    cosplayer = db.get(CommunityCosplayer, cosplayer_id)
    if not cosplayer:
        add_flash(request, "Карточка косплеера не найдена.", "error")
        return redirect("/community/cosplayers")

    form = await request.form()
    body = str(form.get("body", "")).strip()
    if not body:
        add_flash(request, "Введите текст комментария.", "error")
        return redirect(f"/community/cosplayers/{cosplayer_id}")

    db.add(
        CommunityCosplayerComment(
            cosplayer_id=cosplayer.id,
            user_id=user.id,
            body=body,
        )
    )
    if cosplayer.user_id != user.id:
        preview = body if len(body) <= 120 else body[:117].rstrip() + "..."
        db.add(
            FestivalNotification(
                user_id=cosplayer.user_id,
                from_user_id=user.id,
                source_card_id=None,
                message=(
                    f"Новый комментарий в вашей карточке косплеера @{normalize_username(cosplayer.nick)}: {preview}"
                ),
                is_read=False,
            )
        )
    db.commit()
    add_flash(request, "Комментарий добавлен.", "success")
    return redirect(f"/community/cosplayers/{cosplayer_id}")


def normalize_studio_tags(raw_tags: list[str]) -> list[str]:
    normalized_map = {value.casefold(): value for value in STUDIO_TAG_OPTIONS}
    result: list[str] = []
    for raw_value in raw_tags:
        key = str(raw_value or "").strip().casefold()
        if not key:
            continue
        tag_value = normalized_map.get(key)
        if tag_value and tag_value not in result:
            result.append(tag_value)
    return result


def get_studio_form_values(studio: CommunityStudio | None = None) -> dict[str, Any]:
    if not studio:
        return {
            "name": "",
            "city": "",
            "address": "",
            "gallery_input": "",
            "contact": "",
            "price_rows": [],
            "tags_json": [],
        }
    return {
        "name": studio.name or "",
        "city": studio.city or "",
        "address": studio.address or "",
        "gallery_input": "\n".join(as_list(studio.gallery_json)),
        "contact": studio.contact or "",
        "price_rows": format_master_price_rows_for_form(as_list(studio.price_list_json)),
        "tags_json": normalize_studio_tags(as_list(studio.tags_json)),
    }


def save_studio_from_form(form: Any, studio: CommunityStudio) -> tuple[bool, str]:
    name = str(form.get("name", "")).strip()
    city = str(form.get("city", "")).strip()
    address = str(form.get("address", "")).strip()
    gallery_input = str(form.get("gallery_input", ""))
    contact = str(form.get("contact", "")).strip()
    price_rows = parse_master_price_rows_from_form(form)
    tags = normalize_studio_tags([str(item) for item in form.getlist("tags")])

    if not name:
        return False, "Укажите название студии."
    if len(name) > 255:
        return False, "Название студии слишком длинное (до 255 символов)."
    if not city:
        return False, "Укажите город."
    if len(city) > 255:
        return False, "Название города слишком длинное (до 255 символов)."
    if len(address) > 255:
        return False, "Адрес слишком длинный (до 255 символов)."
    if len(contact) > 255:
        return False, "Контакт слишком длинный (до 255 символов)."

    studio.name = name
    studio.city = city
    studio.address = address or None
    studio.gallery_json = parse_reference_values(gallery_input)
    studio.contact = contact or None
    studio.price_list_json = price_rows
    studio.tags_json = tags
    return True, ""


@app.get("/community/studios", response_class=HTMLResponse)
def community_studios_list(request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    q = request.query_params.get("q", "").strip()
    city_filter = request.query_params.get("city", "").strip()
    selected_tags = normalize_studio_tags([str(item) for item in request.query_params.getlist("tag")])

    studios = db.execute(
        select(CommunityStudio).order_by(CommunityStudio.updated_at.desc(), CommunityStudio.id.desc())
    ).scalars().all()

    if q:
        needle = q.casefold()
        studios = [
            item
            for item in studios
            if needle in (item.name or "").casefold()
            or needle in (item.city or "").casefold()
            or needle in (item.address or "").casefold()
            or needle in (item.contact or "").casefold()
        ]
    if city_filter:
        city_filter_values = split_city_values(city_filter)
        studios = [item for item in studios if city_matches_any(city_filter_values, item.city)]
    if selected_tags:
        selected_keys = {value.casefold() for value in selected_tags}
        filtered: list[CommunityStudio] = []
        for item in studios:
            item_keys = {str(tag).strip().casefold() for tag in as_list(item.tags_json) if str(tag).strip()}
            if selected_keys & item_keys:
                filtered.append(item)
        studios = filtered

    owner_ids = {item.user_id for item in studios}
    owners_by_id: dict[int, User] = {}
    if owner_ids:
        owners = db.execute(select(User).where(User.id.in_(owner_ids))).scalars().all()
        owners_by_id = {item.id: item for item in owners}

    city_options = sorted(
        merge_unique([item.city for item in studios if item.city]),
        key=lambda value: value.casefold(),
    )

    return template_response(
        request,
        "community_studios_list.html",
        user=user,
        active_tab="community",
        community_tab="studios",
        studios=studios,
        owners_by_id=owners_by_id,
        q=q,
        city_filter=city_filter,
        selected_tags=selected_tags,
        studio_tag_options=STUDIO_TAG_OPTIONS,
        city_options=city_options,
    )


@app.get("/community/studios/new", response_class=HTMLResponse)
def community_studios_new(request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")
    return template_response(
        request,
        "community_studio_form.html",
        user=user,
        active_tab="community",
        community_tab="studios",
        editing=False,
        studio_id=None,
        form=get_studio_form_values(),
        studio_tag_options=STUDIO_TAG_OPTIONS,
    )


@app.post("/community/studios/new")
async def community_studios_create(request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    form = await request.form()
    studio = CommunityStudio(user_id=user.id, name="", city="")
    ok, error_text = save_studio_from_form(form, studio)
    if not ok:
        add_flash(request, error_text, "error")
        return redirect("/community/studios/new")

    db.add(studio)
    db.commit()
    add_flash(request, "Карточка студии опубликована.", "success")
    return redirect(f"/community/studios/{studio.id}")


@app.get("/community/studios/{studio_id}", response_class=HTMLResponse)
def community_studios_detail(studio_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    studio = db.get(CommunityStudio, studio_id)
    if not studio:
        add_flash(request, "Карточка студии не найдена.", "error")
        return redirect("/community/studios")

    owner = db.get(User, studio.user_id)
    comments = db.execute(
        select(CommunityStudioComment)
        .where(CommunityStudioComment.studio_id == studio.id)
        .order_by(CommunityStudioComment.created_at, CommunityStudioComment.id)
    ).scalars().all()
    author_ids = {studio.user_id, *(item.user_id for item in comments)}
    authors_by_id: dict[int, User] = {}
    if author_ids:
        author_rows = db.execute(select(User).where(User.id.in_(author_ids))).scalars().all()
        authors_by_id = {item.id: item for item in author_rows}
    return template_response(
        request,
        "community_studio_detail.html",
        user=user,
        active_tab="community",
        community_tab="studios",
        studio=studio,
        owner=owner,
        price_rows=format_master_price_rows_for_form(as_list(studio.price_list_json)),
        studio_tags=normalize_studio_tags(as_list(studio.tags_json)),
        comments=comments,
        authors_by_id=authors_by_id,
    )


@app.get("/community/studios/{studio_id}/edit", response_class=HTMLResponse)
def community_studios_edit(studio_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    studio = db.get(CommunityStudio, studio_id)
    if not studio:
        add_flash(request, "Карточка студии не найдена.", "error")
        return redirect("/community/studios")
    if studio.user_id != user.id:
        add_flash(request, "Редактировать можно только свою карточку студии.", "error")
        return redirect(f"/community/studios/{studio_id}")

    return template_response(
        request,
        "community_studio_form.html",
        user=user,
        active_tab="community",
        community_tab="studios",
        editing=True,
        studio_id=studio.id,
        form=get_studio_form_values(studio),
        studio_tag_options=STUDIO_TAG_OPTIONS,
    )


@app.post("/community/studios/{studio_id}/edit")
async def community_studios_update(studio_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    studio = db.get(CommunityStudio, studio_id)
    if not studio:
        add_flash(request, "Карточка студии не найдена.", "error")
        return redirect("/community/studios")
    if studio.user_id != user.id:
        add_flash(request, "Редактировать можно только свою карточку студии.", "error")
        return redirect(f"/community/studios/{studio_id}")

    form = await request.form()
    ok, error_text = save_studio_from_form(form, studio)
    if not ok:
        add_flash(request, error_text, "error")
        return redirect(f"/community/studios/{studio_id}/edit")

    db.commit()
    add_flash(request, "Карточка студии обновлена.", "success")
    return redirect(f"/community/studios/{studio_id}")


@app.post("/community/studios/{studio_id}/delete")
def community_studios_delete(studio_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    studio = db.get(CommunityStudio, studio_id)
    if not studio:
        add_flash(request, "Карточка студии не найдена.", "error")
        return redirect("/community/studios")
    if studio.user_id != user.id:
        add_flash(request, "Удалять можно только свою карточку студии.", "error")
        return redirect(f"/community/studios/{studio_id}")

    db.delete(studio)
    db.commit()
    add_flash(request, "Карточка студии удалена.", "info")
    return redirect("/community/studios")


@app.post("/community/studios/{studio_id}/comments")
async def community_studios_add_comment(studio_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    studio = db.get(CommunityStudio, studio_id)
    if not studio:
        add_flash(request, "Карточка студии не найдена.", "error")
        return redirect("/community/studios")

    form = await request.form()
    body = str(form.get("body", "")).strip()
    if not body:
        add_flash(request, "Введите текст комментария.", "error")
        return redirect(f"/community/studios/{studio_id}")

    db.add(
        CommunityStudioComment(
            studio_id=studio.id,
            user_id=user.id,
            body=body,
        )
    )
    if studio.user_id != user.id:
        preview = body if len(body) <= 120 else body[:117].rstrip() + "..."
        db.add(
            FestivalNotification(
                user_id=studio.user_id,
                from_user_id=user.id,
                source_card_id=None,
                message=f"Новый комментарий в вашей карточке студии «{studio.name}»: {preview}",
                is_read=False,
            )
        )
    db.commit()
    add_flash(request, "Комментарий добавлен.", "success")
    return redirect(f"/community/studios/{studio_id}")


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


def app_state_storage_path() -> Path:
    custom_path = (os.getenv("APP_STATE_DIR", "") or "").strip()
    if custom_path:
        path = Path(custom_path).expanduser().resolve()
        path.mkdir(parents=True, exist_ok=True)
        return path

    data_dir = Path("/data")
    if data_dir.exists() and os.access(data_dir, os.W_OK):
        path = (data_dir / "app-state").resolve()
        path.mkdir(parents=True, exist_ok=True)
        return path

    path = Path("./runtime-state").resolve()
    path.mkdir(parents=True, exist_ok=True)
    return path


def external_import_state_path() -> Path:
    return app_state_storage_path() / "external-import-state.json"


def load_external_import_state() -> dict[str, Any]:
    state_path = external_import_state_path()
    if not state_path.exists():
        return {}
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return {}
    if not isinstance(payload, dict):
        return {}
    return payload


def save_external_import_state(state: dict[str, Any]) -> None:
    state_path = external_import_state_path()
    state_path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def parse_state_datetime(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    normalized = raw.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is not None:
        return parsed.astimezone(tz=None).replace(tzinfo=None)
    return parsed


def should_run_import_by_interval(state: dict[str, Any], key: str, interval_hours: int, now: datetime) -> bool:
    last_run = parse_state_datetime(state.get(key))
    if not last_run:
        return True
    return (now - last_run) >= timedelta(hours=max(1, interval_hours))


def vk_api_call(method: str, params: dict[str, Any]) -> dict[str, Any]:
    if not VK_IMPORT_ENABLED:
        raise RuntimeError("Импорт VK недоступен: не задан VK_API_TOKEN.")

    query = dict(params)
    query["access_token"] = VK_API_TOKEN
    query["v"] = VK_API_VERSION

    try:
        response = requests.get(
            f"https://api.vk.com/method/{method}",
            params=query,
            timeout=max(10, HTTP_TIMEOUT_SECONDS * 2),
        )
    except requests.RequestException as exc:
        raise RuntimeError(f"Не удалось связаться с VK API ({method}).") from exc

    if response.status_code != 200:
        raise RuntimeError(f"VK API ({method}) временно недоступен (HTTP {response.status_code}).")

    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError(f"VK API ({method}) вернул некорректный ответ.") from exc

    if not isinstance(payload, dict):
        raise RuntimeError(f"VK API ({method}) вернул неожиданный формат данных.")

    error_payload = payload.get("error")
    if isinstance(error_payload, dict):
        message = str(error_payload.get("error_msg") or "Неизвестная ошибка VK API").strip()
        raise RuntimeError(f"VK API ({method}): {message}")

    response_payload = payload.get("response")
    if not isinstance(response_payload, dict):
        raise RuntimeError(f"VK API ({method}) не вернул ожидаемого поля response.")
    return response_payload


def get_import_owner_user(db: Session) -> User | None:
    users = db.execute(select(User).order_by(User.id)).scalars().all()
    if not users:
        return None
    for item in users:
        if is_moderator_user(item):
            return item
    return users[0]


def normalize_event_name_key(value: str | None) -> str:
    raw = (value or "").strip().casefold()
    if not raw:
        return ""
    return re.sub(r"[^0-9a-zа-яё]+", "", raw)


def festival_name_keywords(value: str | None) -> set[str]:
    raw = (value or "").strip().casefold().replace("ё", "е")
    if not raw:
        return set()
    tokens = re.findall(r"[0-9a-zа-яё]+", raw)
    keywords = {
        token
        for token in tokens
        if len(token) >= 3 and token not in FESTIVAL_NAME_DUPLICATE_STOP_WORDS
    }
    if keywords:
        return keywords
    normalized = normalize_event_name_key(raw)
    return {normalized} if normalized else set()


def festivals_look_like_duplicates(
    left_name: str | None,
    left_city: str | None,
    left_date: date | None,
    right_name: str | None,
    right_city: str | None,
    right_date: date | None,
) -> bool:
    if not left_date or not right_date or left_date != right_date:
        return False
    if not city_matches(left_city, right_city):
        return False

    left_key = normalize_event_name_key(left_name)
    right_key = normalize_event_name_key(right_name)
    if left_key and right_key and left_key == right_key:
        return True

    left_keywords = festival_name_keywords(left_name)
    right_keywords = festival_name_keywords(right_name)
    if not left_keywords or not right_keywords:
        return False
    return bool(left_keywords & right_keywords)


def detect_master_type_from_text(text_value: str | None) -> str:
    text = (text_value or "").casefold()
    keyword_map = [
        ("фотограф", "фотограф"),
        ("фотосесс", "фотограф"),
        ("шве", "швея"),
        ("пошив", "швея"),
        ("крафт", "крафтер"),
        ("виг", "вигмейкер"),
        ("парик", "вигмейкер"),
        ("худож", "художник"),
        ("арт", "художник"),
        ("видеограф", "видеограф"),
        ("видео", "видеограф"),
    ]
    for keyword, master_type in keyword_map:
        if keyword in text:
            return master_type
    return "другое"


def attachment_photo_urls(attachments: list[Any]) -> list[str]:
    urls: list[str] = []
    for item in attachments:
        if not isinstance(item, dict):
            continue
        photo = item.get("photo")
        if not isinstance(photo, dict):
            continue
        sizes = photo.get("sizes")
        if not isinstance(sizes, list):
            continue
        best_url = ""
        best_area = -1
        for size in sizes:
            if not isinstance(size, dict):
                continue
            url = str(size.get("url") or "").strip()
            if not url:
                continue
            width = int(size.get("width") or 0)
            height = int(size.get("height") or 0)
            area = width * height
            if area > best_area:
                best_area = area
                best_url = url
        if best_url:
            urls.append(best_url)
    return merge_unique(urls)


def import_cosplay_team_masters(db: Session, *, since_date: date, fetch_count: int | None = None) -> dict[str, Any]:
    count_value = max(10, min(100, int(fetch_count or VK_IMPORT_WALL_COUNT or 50)))
    payload = vk_api_call(
        "wall.get",
        {
            "domain": VK_IMPORT_WALL_DOMAIN,
            "count": count_value,
            "extended": 1,
            "filter": "owner",
        },
    )
    items = payload.get("items")
    if not isinstance(items, list):
        return {"imported": 0, "skipped": 0, "total": 0}

    import_owner = get_import_owner_user(db)
    if not import_owner:
        return {"imported": 0, "skipped": 0, "total": len(items), "error": "Нет пользователей в системе."}

    profiles_raw = payload.get("profiles")
    groups_raw = payload.get("groups")
    profiles_by_id = {
        int(item.get("id")): item
        for item in (profiles_raw if isinstance(profiles_raw, list) else [])
        if isinstance(item, dict) and item.get("id")
    }
    groups_by_owner_id = {
        -int(item.get("id")): item
        for item in (groups_raw if isinstance(groups_raw, list) else [])
        if isinstance(item, dict) and item.get("id")
    }

    existing_external_ids = {
        str(value).strip()
        for value in db.execute(
            select(CommunityMaster.import_external_id).where(CommunityMaster.import_external_id.is_not(None))
        ).scalars().all()
        if str(value).strip()
    }
    existing_import_urls = {
        str(value).strip()
        for value in db.execute(
            select(CommunityMaster.import_url).where(CommunityMaster.import_url.is_not(None))
        ).scalars().all()
        if str(value).strip()
    }

    imported = 0
    skipped = 0
    for post in items:
        if not isinstance(post, dict):
            continue
        if int(post.get("is_deleted") or 0) == 1:
            continue
        if int(post.get("marked_as_ads") or 0) == 1:
            continue

        post_id = post.get("id")
        owner_id = post.get("owner_id")
        post_ts = int(post.get("date") or 0)
        if not post_id or not owner_id or post_ts <= 0:
            continue

        post_date = datetime.utcfromtimestamp(post_ts).date()
        if post_date < since_date:
            continue

        external_id = f"wall{int(owner_id)}_{int(post_id)}"
        post_url = f"https://vk.com/{external_id}"
        if external_id in existing_external_ids or post_url in existing_import_urls:
            skipped += 1
            continue

        text_value = str(post.get("text") or "").strip()
        signer_id = int(post.get("signer_id") or 0) if post.get("signer_id") else 0
        from_id = int(post.get("from_id") or owner_id)
        author_id = signer_id if signer_id else from_id

        nick_value = ""
        if author_id > 0:
            profile = profiles_by_id.get(author_id, {})
            if isinstance(profile, dict):
                nick_value = sanitize_vk_username(
                    profile.get("screen_name")
                    or f"{profile.get('first_name') or ''}_{profile.get('last_name') or ''}"
                )
        else:
            group = groups_by_owner_id.get(author_id, {})
            if isinstance(group, dict):
                nick_value = sanitize_vk_username(group.get("screen_name") or group.get("name"))

        if not nick_value:
            nick_value = f"vk_{abs(author_id)}"

        details_lines = [
            "Импортировано из Cosplay Team.",
            f"Пост: {post_url}",
        ]
        if text_value:
            details_lines.append("")
            details_lines.append(text_value)
        details = "\n".join(details_lines).strip()
        if len(details) > 6000:
            details = details[:5997].rstrip() + "..."

        gallery_urls = attachment_photo_urls(post.get("attachments") if isinstance(post.get("attachments"), list) else [])
        db.add(
            CommunityMaster(
                user_id=import_owner.id,
                nick=nick_value,
                master_type=detect_master_type_from_text(text_value),
                details=details,
                gallery_json=gallery_urls,
                price_list_json=[],
                import_source=MASTER_IMPORT_SOURCE_LABEL,
                import_external_id=external_id,
                import_url=post_url,
            )
        )
        existing_external_ids.add(external_id)
        existing_import_urls.add(post_url)
        imported += 1

    return {
        "imported": imported,
        "skipped": skipped,
        "total": len(items),
    }


def html_to_plain_lines(value: str) -> list[str]:
    html_value = value or ""
    html_value = re.sub(r"<br\s*/?>", "\n", html_value, flags=re.IGNORECASE)
    html_value = re.sub(r"</(p|div|li|tr|h[1-6]|table|ul|ol)>", "\n", html_value, flags=re.IGNORECASE)
    text_value = re.sub(r"<[^>]+>", " ", html_value)
    text_value = html.unescape(text_value)
    lines: list[str] = []
    for raw_line in text_value.splitlines():
        cleaned = re.sub(r"\s+", " ", raw_line).strip(" \t\r\n•*;")
        if not cleaned:
            continue
        lines.append(cleaned)
    return lines


def parse_year_for_raf_page(page_title: str) -> int:
    match = re.search(r"(20\d{2})", page_title or "")
    if not match:
        return date.today().year
    return int(match.group(1))


def _build_date_or_none(year: int, month: int, day: int) -> date | None:
    try:
        return date(year, month, day)
    except ValueError:
        return None


def parse_date_range_from_line(line_value: str, default_year: int) -> tuple[date | None, date | None, tuple[int, int] | None]:
    numeric_match = re.search(
        r"(?P<d1>\d{1,2})[./](?P<m1>\d{1,2})(?:[./](?P<y1>\d{2,4}))?"
        r"\s*(?:[-–—]\s*(?P<d2>\d{1,2})[./](?P<m2>\d{1,2})(?:[./](?P<y2>\d{2,4}))?)?",
        line_value,
    )
    if numeric_match:
        y1_raw = numeric_match.group("y1") or ""
        y2_raw = numeric_match.group("y2") or ""
        y1 = int(y1_raw) if y1_raw else default_year
        y2 = int(y2_raw) if y2_raw else y1
        if y1 < 100:
            y1 += 2000
        if y2 < 100:
            y2 += 2000
        d1 = _build_date_or_none(y1, int(numeric_match.group("m1")), int(numeric_match.group("d1")))
        if not d1:
            return None, None, None
        d2 = d1
        if numeric_match.group("d2") and numeric_match.group("m2"):
            d2_candidate = _build_date_or_none(y2, int(numeric_match.group("m2")), int(numeric_match.group("d2")))
            if d2_candidate:
                d2 = d2_candidate
        if d2 < d1:
            d2 = d1
        return d1, d2, numeric_match.span()

    word_match = re.search(
        r"(?P<d1>\d{1,2})(?:\s*[-–—]\s*(?P<d2>\d{1,2}))?\s+(?P<mw>[а-яё]+)(?:\s+(?P<year>20\d{2}))?",
        line_value.casefold(),
    )
    if not word_match:
        return None, None, None

    month_word = word_match.group("mw")
    month_value = RU_MONTH_WORDS_TO_NUM.get(month_word)
    if not month_value:
        return None, None, None
    year_value = int(word_match.group("year")) if word_match.group("year") else default_year
    d1 = _build_date_or_none(year_value, month_value, int(word_match.group("d1")))
    if not d1:
        return None, None, None
    d2 = d1
    if word_match.group("d2"):
        d2_candidate = _build_date_or_none(year_value, month_value, int(word_match.group("d2")))
        if d2_candidate and d2_candidate >= d1:
            d2 = d2_candidate
    return d1, d2, word_match.span()


def parse_raf_city_and_name(line_value: str) -> tuple[str, str]:
    value = line_value.strip()
    value = re.sub(r"https?://\S+", "", value).strip(" -—–,.;")
    parts = [item.strip(" -—–,.;") for item in re.split(r"\s+[—–-]\s+", value) if item.strip(" -—–,.;")]
    if not parts:
        return "", ""
    if len(parts) >= 3:
        city_value = re.sub(r"^(г\.?|город)\s+", "", parts[1], flags=re.IGNORECASE).strip()
        name_value = parts[2]
    elif len(parts) == 2:
        first_part = re.sub(r"^(г\.?|город)\s+", "", parts[0], flags=re.IGNORECASE).strip()
        if 1 <= len(first_part.split()) <= 4 and not re.search(r"(фест|fest|аниме|косп|con)", first_part.casefold()):
            city_value = first_part
            name_value = parts[1]
        else:
            city_value = ""
            name_value = parts[1]
    else:
        city_value = ""
        name_value = parts[0]
    return city_value, name_value


def parse_raf_events_from_page_html(page_html: str, page_title: str) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    seen_external_ids: set[str] = set()

    list_items = re.findall(
        r"<li[^>]*>.*?<span class=[\"']l[\"']>(.*?)</span>.*?</li>",
        page_html or "",
        flags=re.IGNORECASE | re.DOTALL,
    )
    for entry_html in list_items:
        entry_value = (entry_html or "").strip()
        if not entry_value:
            continue

        url_match = re.search(r'href=["\']([^"\']+)["\']', entry_value, flags=re.IGNORECASE)
        event_url = html.unescape(url_match.group(1).strip()) if url_match else ""
        if event_url.startswith("/"):
            event_url = f"https://vk.com{event_url}"

        text_value = re.sub(r"<br\s*/?>", "\n", entry_value, flags=re.IGNORECASE)
        text_value = re.sub(r"<[^>]+>", " ", text_value)
        text_value = html.unescape(text_value)
        text_value = re.sub(r"\s+", " ", text_value).strip(" \t\r\n•*;,")
        if not text_value:
            continue

        event_start, event_end, date_span = parse_date_range_from_line(text_value, parse_year_for_raf_page(page_title))
        if not event_start or not event_end or not date_span:
            continue

        remainder = text_value[date_span[1] :].lstrip(" ,—–-")
        parts = [part.strip(" -—–,.;") for part in remainder.split(",") if part.strip(" -—–,.;")]
        name_value = parts[0] if parts else ""
        city_value = (
            re.sub(r"^(г\.?|город)\s+", "", parts[1], flags=re.IGNORECASE).strip()
            if len(parts) > 1
            else ""
        )

        if not name_value:
            city_fallback, name_fallback = parse_raf_city_and_name(remainder)
            city_value = city_value or city_fallback
            name_value = name_fallback

        normalized_name = normalize_event_name_key(name_value)
        if not normalized_name:
            continue

        external_id = f"{page_title}:{event_start.isoformat()}:{normalized_name}"
        if external_id in seen_external_ids:
            continue
        seen_external_ids.add(external_id)
        events.append(
            {
                "name": name_value,
                "city": city_value,
                "url": event_url,
                "event_date": event_start,
                "event_end_date": event_end,
                "external_id": external_id,
            }
        )
    return events


def fetch_raf_page_html(page_url: str) -> tuple[str, str]:
    resolved_title = parse_qs(urlparse(page_url).query).get("p", [""])[0]
    resolved_title = unquote(resolved_title).strip() or "Календарь"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "ru-RU,ru;q=0.9",
    }
    try:
        response = requests.get(
            page_url,
            headers=headers,
            timeout=max(20, HTTP_TIMEOUT_SECONDS * 3),
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        raise RuntimeError(f"Не удалось получить страницу РАФ: {resolved_title}.") from exc

    html_value = response.text or ""
    if not html_value.strip():
        raise RuntimeError(f"Страница РАФ пуста: {resolved_title}.")
    return resolved_title, html_value


def fetch_raf_events_from_vk() -> list[dict[str, Any]]:
    all_events: list[dict[str, Any]] = []
    page_errors: list[str] = []
    for page_url in RAF_PAGE_URLS:
        try:
            resolved_title, html_value = fetch_raf_page_html(page_url)
        except RuntimeError as exc:
            page_errors.append(str(exc))
            continue
        all_events.extend(parse_raf_events_from_page_html(html_value, resolved_title))

    if not all_events and page_errors:
        raise RuntimeError(page_errors[0])

    deduped: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for item in all_events:
        external_id = str(item.get("external_id") or "").strip()
        if not external_id or external_id in seen_ids:
            continue
        seen_ids.add(external_id)
        deduped.append(item)
    return deduped


def import_raf_events_for_user(db: Session, user: User, events: list[dict[str, Any]]) -> int:
    today = date.today()
    existing_festivals = db.execute(
        select(Festival).where(Festival.user_id == user.id)
    ).scalars().all()
    existing_name_keys = {
        normalize_event_name_key(item.name)
        for item in existing_festivals
        if normalize_event_name_key(item.name)
    }
    existing_external_ids = {
        str(item.import_external_id).strip()
        for item in existing_festivals
        if str(item.import_external_id or "").strip()
    }

    imported_names: list[str] = []
    imported_count = 0
    for event in events:
        event_name = str(event.get("name") or "").strip()
        event_date = event.get("event_date")
        if not event_name or not isinstance(event_date, date):
            continue
        if event_date < today:
            continue

        name_key = normalize_event_name_key(event_name)
        external_id = str(event.get("external_id") or "").strip()
        if name_key and name_key in existing_name_keys:
            continue
        if external_id and external_id in existing_external_ids:
            continue
        if any(
            festivals_look_like_duplicates(
                item.name,
                item.city,
                item.event_date,
                event_name,
                str(event.get("city") or "").strip(),
                event_date,
            )
            for item in existing_festivals
        ):
            continue

        festival = Festival(
            user_id=user.id,
            name=event_name,
            url=str(event.get("url") or "").strip() or None,
            city=str(event.get("city") or "").strip() or None,
            event_date=event_date,
            event_end_date=event.get("event_end_date") if isinstance(event.get("event_end_date"), date) else None,
            import_source=RAF_IMPORT_SOURCE_LABEL,
            import_external_id=external_id or None,
        )
        db.add(festival)
        existing_festivals.append(festival)
        if name_key:
            existing_name_keys.add(name_key)
        if external_id:
            existing_external_ids.add(external_id)
        imported_count += 1
        imported_names.append(event_name)

    if imported_names:
        remember_options(db, user.id, "festival", imported_names)
    return imported_count


def auto_import_external_sources_if_needed() -> None:
    if not VK_IMPORT_ENABLED:
        return

    now = datetime.utcnow()
    state = load_external_import_state()
    changed = False
    auto_masters_since = date.today() - timedelta(days=1)

    with SessionLocal() as db:
        if should_run_import_by_interval(state, "masters_last_run_at", MASTER_IMPORT_INTERVAL_HOURS, now):
            state["masters_last_run_at"] = now.isoformat()
            changed = True
            try:
                import_cosplay_team_masters(db, since_date=auto_masters_since, fetch_count=VK_IMPORT_WALL_COUNT)
                db.commit()
            except Exception:
                db.rollback()

        if should_run_import_by_interval(state, "raf_last_run_at", RAF_IMPORT_INTERVAL_HOURS, now):
            state["raf_last_run_at"] = now.isoformat()
            changed = True
            try:
                raf_events = fetch_raf_events_from_vk()
                users = db.execute(select(User).order_by(User.id)).scalars().all()
                for item in users:
                    import_raf_events_for_user(db, item, raf_events)
                db.commit()
            except Exception:
                db.rollback()

    if changed:
        save_external_import_state(state)


@app.post("/community/masters/import-cosplay-team")
def community_masters_import_cosplay_team(request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")
    if not user_is_special(user):
        add_flash(request, "Импорт доступен только @brfox_cosplay.", "error")
        return redirect("/community/masters")
    if not VK_IMPORT_ENABLED:
        add_flash(request, "Импорт недоступен: задайте VK_API_TOKEN в переменных окружения.", "error")
        return redirect("/community/masters")

    state = load_external_import_state()
    state["masters_last_run_at"] = datetime.utcnow().isoformat()
    save_external_import_state(state)
    manual_since = date.today() - timedelta(days=30)

    try:
        result = import_cosplay_team_masters(db, since_date=manual_since, fetch_count=max(VK_IMPORT_WALL_COUNT, 100))
        db.commit()
    except RuntimeError as exc:
        db.rollback()
        add_flash(request, str(exc), "error")
        return redirect("/community/masters")

    if result.get("error"):
        add_flash(request, str(result.get("error")), "error")
    else:
        add_flash(
            request,
            f"Импорт из Cosplay Team за последние 30 дней: добавлено {result.get('imported', 0)}, уже было {result.get('skipped', 0)}.",
            "success",
        )
    return redirect("/community/masters")


@app.post("/festivals/import-raf")
def festivals_import_raf(request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")
    if not user_is_special(user):
        add_flash(request, "Импорт РАФ доступен только @brfox_cosplay.", "error")
        return redirect("/festivals")
    if not VK_IMPORT_ENABLED:
        add_flash(request, "Импорт РАФ недоступен: задайте VK_API_TOKEN в переменных окружения.", "error")
        return redirect("/festivals")

    state = load_external_import_state()
    state["raf_last_run_at"] = datetime.utcnow().isoformat()
    save_external_import_state(state)

    try:
        events = fetch_raf_events_from_vk()
        imported_count = import_raf_events_for_user(db, user, events)
        db.commit()
    except RuntimeError as exc:
        db.rollback()
        add_flash(request, str(exc), "error")
        return redirect("/festivals")

    add_flash(
        request,
        f"Импорт с РАФ завершён: найдено {len(events)}, добавлено {imported_count}.",
        "success" if imported_count else "info",
    )
    return redirect("/festivals")


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
        can_import_cosplay2=user_is_special(user),
        can_import_raf=user_is_special(user) and VK_IMPORT_ENABLED,
        import_source_labels=IMPORT_SOURCE_LABELS,
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


@app.get("/notifications/pigeon/pending")
def notifications_pigeon_pending(request: Request, db: Session = Depends(get_db)) -> dict[str, Any]:
    user = current_user(request, db)
    if not user:
        return {"ok": False, "notification": None}
    return {"ok": True, "notification": get_latest_unread_pigeon(db, user.id)}


@app.post("/notifications/pigeon/{notification_id}/seen")
def notifications_pigeon_seen(notification_id: int, request: Request, db: Session = Depends(get_db)) -> dict[str, Any]:
    user = current_user(request, db)
    if not user:
        return {"ok": False}

    notification = get_user_pigeon_notification(db, user.id, notification_id)
    if not notification:
        return {"ok": False}

    notification.is_read = True
    db.commit()
    return {"ok": True}


@app.post("/notifications/pigeon/{notification_id}/delete")
async def notifications_pigeon_delete(notification_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    is_ajax = request.headers.get("X-Requested-With") == "XMLHttpRequest"
    if not user:
        return {"ok": False} if is_ajax else redirect("/login")

    notification = get_user_pigeon_notification(db, user.id, notification_id)
    if not notification:
        return {"ok": False} if is_ajax else redirect("/")

    db.delete(notification)
    db.commit()

    if is_ajax:
        return {"ok": True}

    form = await request.form()
    next_url = safe_redirect_target(str(form.get("next", "")).strip(), "/")
    add_flash(request, "Голубь удален.", "success")
    return redirect(next_url)


@app.post("/notifications/pigeon")
async def notifications_send_pigeon(request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    form = await request.form()
    recipient_alias_raw = str(form.get("recipient_alias", "")).strip()
    message_body = str(form.get("message", "")).strip()
    reply_to_raw = str(form.get("reply_to_notification_id", "")).strip()
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

    reply_to_notification_id = None
    if reply_to_raw.isdigit():
        reply_note = db.execute(
            select(FestivalNotification).where(
                FestivalNotification.id == int(reply_to_raw),
                FestivalNotification.user_id == user.id,
            )
        ).scalar_one_or_none()
        if reply_note and is_pigeon_message(reply_note.message):
            reply_to_notification_id = reply_note.id
    send_pigeon_notification(
        db,
        sender=user,
        recipient=recipient,
        message_body=message_body,
        reply_to_notification_id=reply_to_notification_id,
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


@app.post("/festivals/announcements/{announcement_id}/delete")
def festivals_announcements_delete(announcement_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")

    announcement = db.get(FestivalAnnouncement, announcement_id)
    if not announcement:
        add_flash(request, "Заявка на анонс не найдена.", "error")
        return redirect("/festivals")
    if announcement.status != ANNOUNCEMENT_STATUS_REJECTED:
        add_flash(request, "Удалять можно только отклонённые анонсы.", "error")
        return redirect("/festivals")
    if announcement.requester_user_id != user.id and not is_moderator_user(user):
        add_flash(request, "Недостаточно прав для удаления этой заявки.", "error")
        return redirect("/festivals")

    db.delete(announcement)
    db.commit()
    add_flash(request, "Информация об отклонённом анонсе удалена.", "info")
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


@app.post("/festivals/{festival_id}/delete-all")
async def festivals_delete_all(festival_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")
    if not user_is_special(user):
        add_flash(request, "Удаление фестиваля у всех доступно только @brfox_cosplay.", "error")
        return redirect("/festivals")

    festival = db.get(Festival, festival_id)
    if not festival:
        add_flash(request, "Фестиваль не найден.", "error")
        return redirect("/festivals")

    form = await request.form()
    next_url = safe_redirect_target(str(form.get("next", "")).strip(), "/festivals")

    all_festivals = db.execute(select(Festival)).scalars().all()
    target_items: list[Festival] = []
    for item in all_festivals:
        if festival.source_announcement_id and item.source_announcement_id == festival.source_announcement_id:
            target_items.append(item)
            continue
        if (
            festival.import_source
            and festival.import_external_id
            and item.import_source == festival.import_source
            and item.import_external_id == festival.import_external_id
        ):
            target_items.append(item)
            continue
        if festivals_look_like_duplicates(
            item.name,
            item.city,
            item.event_date,
            festival.name,
            festival.city,
            festival.event_date,
        ):
            target_items.append(item)

    deleted_count = 0
    seen_ids: set[int] = set()
    for item in target_items:
        if item.id in seen_ids:
            continue
        seen_ids.add(item.id)
        db.delete(item)
        deleted_count += 1

    if festival.source_announcement_id:
        source_announcement = db.get(FestivalAnnouncement, festival.source_announcement_id)
        if source_announcement:
            db.delete(source_announcement)

    db.commit()
    add_flash(request, f"Фестиваль удалён у всех пользователей: {deleted_count}.", "info")
    return redirect(next_url)


@app.post("/festivals/import-cosplay2")
def festivals_import_cosplay2(request: Request, db: Session = Depends(get_db)):
    user = current_user(request, db)
    if not user:
        return redirect("/login")
    if not user_is_special(user):
        add_flash(request, "Импорт с cosplay2.ru доступен только @brfox_cosplay.", "error")
        return redirect("/festivals")

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

            if existing.import_source != COSPLAY2_IMPORT_SOURCE_LABEL:
                existing.import_source = COSPLAY2_IMPORT_SOURCE_LABEL
                changed = True
            if existing.import_external_id != normalized_url:
                existing.import_external_id = normalized_url
                changed = True

            if changed:
                updated += 1
            continue

        if any(
            festivals_look_like_duplicates(
                row.name,
                row.city,
                row.event_date,
                event.name,
                event.city,
                event.event_date,
            )
            for row in existing_rows
        ):
            continue

        festival = Festival(
            user_id=user.id,
            name=event.name,
            url=normalized_url,
            city=event.city,
            event_date=event.event_date,
            submission_deadline=event.submission_deadline,
            import_source=COSPLAY2_IMPORT_SOURCE_LABEL,
            import_external_id=normalized_url,
        )
        db.add(festival)
        existing_by_url[normalized_url] = festival
        existing_rows.append(festival)
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
