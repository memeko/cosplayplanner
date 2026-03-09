from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any
from urllib.parse import urlparse


@dataclass
class Cosplay2Event:
    name: str
    url: str
    city: str | None
    event_date: date | None
    submission_deadline: date | None
    description: str | None


def normalize_url(url: str | None) -> str | None:
    if not url:
        return None

    parsed = urlparse(url.strip())
    if not parsed.scheme or not parsed.netloc:
        return None

    normalized = f"{parsed.scheme.lower()}://{parsed.netloc.lower()}{parsed.path or ''}"
    return normalized.rstrip("/")


def guess_name_from_url(url: str | None) -> str | None:
    normalized = normalize_url(url)
    if not normalized:
        return None

    host = urlparse(normalized).netloc
    slug = host.split(".", 1)[0]
    if not slug:
        return None

    name = re.sub(r"\s+", " ", slug.replace("-", " ").replace("_", " ")).strip()
    return name or None


def parse_event_date(value: str | None) -> date | None:
    if not value:
        return None

    candidate = value.strip()
    if not candidate:
        return None

    formats = [
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%z",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(candidate, fmt).date()
        except ValueError:
            continue

    try:
        return datetime.fromisoformat(candidate.replace("Z", "+00:00")).date()
    except ValueError:
        return None


def extract_deadline(text: str | None) -> date | None:
    if not text:
        return None

    lowered = text.lower()

    patterns = [
        r"(?:дедлайн|подач[аи]|заявок[^\d]{0,30})(\d{1,2}[./]\d{1,2}[./]\d{2,4})",
        r"(?:дедлайн|подач[аи]|заявок[^\d]{0,30})(\d{4}-\d{2}-\d{2})",
    ]

    for pattern in patterns:
        match = re.search(pattern, lowered)
        if not match:
            continue

        raw = match.group(1)
        if "/" in raw or "." in raw:
            chunks = re.split(r"[./]", raw)
            if len(chunks) == 3:
                day, month, year = chunks
                if len(year) == 2:
                    year = f"20{year}"
                try:
                    return date(int(year), int(month), int(day))
                except ValueError:
                    continue
        else:
            try:
                return date.fromisoformat(raw)
            except ValueError:
                continue

    return None


def cleanup_loose_json(payload: str) -> str:
    cleaned = payload
    for _ in range(8):
        updated = re.sub(r",\s*([}\]])", r"\1", cleaned)
        if updated == cleaned:
            break
        cleaned = updated
    return cleaned


def parse_ld_json_blocks(html: str) -> list[Any]:
    blocks = re.findall(r"<script type=\"application/ld\+json\">\s*(.*?)\s*</script>", html, re.S)
    parsed_blocks: list[Any] = []

    for block in blocks:
        cleaned = cleanup_loose_json(block)
        try:
            parsed_blocks.append(json.loads(cleaned))
        except json.JSONDecodeError:
            continue

    return parsed_blocks


def parse_events_from_homepage(html: str) -> list[Cosplay2Event]:
    parsed_blocks = parse_ld_json_blocks(html)
    events: list[Cosplay2Event] = []
    seen_urls: set[str] = set()

    for block in parsed_blocks:
        items = []
        if isinstance(block, dict):
            if block.get("@type") == "Event":
                items = [block]
            else:
                raw_items = block.get("itemListElement")
                if isinstance(raw_items, list):
                    items = raw_items
        elif isinstance(block, list):
            items = block

        for item in items:
            if not isinstance(item, dict):
                continue
            if item.get("@type") != "Event":
                continue

            raw_url = normalize_url(str(item.get("url", "")).strip())
            if not raw_url or raw_url in seen_urls:
                continue

            name = str(item.get("name", "")).strip()
            if not name:
                guessed = guess_name_from_url(raw_url)
                if not guessed:
                    continue
                name = guessed

            location = item.get("location") if isinstance(item.get("location"), dict) else {}
            city: str | None = None
            if isinstance(location, dict):
                city = str(location.get("name", "")).strip() or None
                if not city:
                    address = location.get("address") if isinstance(location.get("address"), dict) else {}
                    if isinstance(address, dict):
                        city = str(address.get("addressLocality", "")).strip() or None

            description = str(item.get("description", "")).strip() or None
            event_date = parse_event_date(str(item.get("startDate", "")).strip())
            submission_deadline = extract_deadline(description)

            events.append(
                Cosplay2Event(
                    name=name,
                    url=raw_url,
                    city=city,
                    event_date=event_date,
                    submission_deadline=submission_deadline,
                    description=description,
                )
            )
            seen_urls.add(raw_url)

    return events
