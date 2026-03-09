from __future__ import annotations

from datetime import date, datetime
from typing import Iterable

from sqlalchemy import select
from sqlalchemy.orm import Session

from .models import UserOption


def parse_date(value: str | None) -> date | None:
    if not value:
        return None
    value = value.strip()
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def parse_float(value: str | None) -> float | None:
    if value is None:
        return None
    cleaned = value.strip().replace(" ", "")
    if not cleaned:
        return None
    cleaned = cleaned.replace(",", ".")
    try:
        return float(cleaned)
    except ValueError:
        return None


def split_csv(raw: str | None) -> list[str]:
    if not raw:
        return []

    normalized = raw.replace("\n", ",").replace(";", ",")
    parts = [item.strip() for item in normalized.split(",")]
    return [item for item in parts if item]


def merge_unique(*groups: Iterable[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()

    for group in groups:
        for item in group:
            cleaned = item.strip()
            if not cleaned:
                continue
            key = cleaned.casefold()
            if key in seen:
                continue
            seen.add(key)
            result.append(cleaned)

    return result


def as_list(value: object) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    return []


def get_options(db: Session, user_id: int, group: str) -> list[str]:
    stmt = (
        select(UserOption.value)
        .where(UserOption.user_id == user_id, UserOption.group == group)
        .order_by(UserOption.value)
    )
    values = db.execute(stmt).scalars().all()
    return [value for value in values if value]


def remember_options(db: Session, user_id: int, group: str, values: Iterable[str]) -> None:
    cleaned_values = merge_unique(values)
    if not cleaned_values:
        return

    existing = set(get_options(db, user_id, group))
    for value in cleaned_values:
        if value in existing:
            continue
        db.add(UserOption(user_id=user_id, group=group, value=value))


def iso_today() -> date:
    return datetime.utcnow().date()


def to_bool(value: object) -> bool:
    return str(value).lower() in {"1", "true", "on", "yes"}


def esc_ics(text: str | None) -> str:
    if not text:
        return ""
    return text.replace("\\", "\\\\").replace(";", "\\;").replace(",", "\\,").replace("\n", "\\n")
