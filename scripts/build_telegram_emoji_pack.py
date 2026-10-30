#!/usr/bin/env python3
"""Build the static Cosplay Planner custom-emoji pack for Telegram."""

from __future__ import annotations

import csv
import math
import random
import shutil
from pathlib import Path

from PIL import Image, ImageChops, ImageDraw, ImageFilter, ImageFont, ImageOps


ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "telegram-emoji-pack"
PNG = OUT / "png"
WEBP = OUT / "webp"
PREVIEW = OUT / "previews"
SIZE = 400
FINAL = 100
FONT_PATH = Path("/Users/anso/Library/Fonts/Montserrat-Medium.ttf")

LATIN_UPPER = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
LATIN_LOWER = "abcdefghijklmnopqrstuvwxyz"
CYR_UPPER = "АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ"
CYR_LOWER = "абвгдеёжзийклмнопрстуфхцчшщъыьэюя"
DIGITS = "0123456789"


def ensure_dirs() -> None:
    for p in (PNG, WEBP, PREVIEW):
        p.mkdir(parents=True, exist_ok=True)


def save_asset(name: str, image: Image.Image) -> None:
    image = image.convert("RGBA").resize((FINAL, FINAL), Image.Resampling.LANCZOS)
    image.save(PNG / f"{name}.png", optimize=True)
    image.save(WEBP / f"{name}.webp", format="WEBP", lossless=True, method=6)


def gradient(size: int, top=(255, 255, 255), middle=(190, 204, 255), bottom=(120, 93, 237)) -> Image.Image:
    im = Image.new("RGBA", (size, size))
    px = im.load()
    for y in range(size):
        t = y / max(1, size - 1)
        if t < 0.52:
            q = t / 0.52
            a, b = top, middle
        else:
            q = (t - 0.52) / 0.48
            a, b = middle, bottom
        c = tuple(round(a[i] * (1 - q) + b[i] * q) for i in range(3))
        for x in range(size):
            px[x, y] = (*c, 255)
    return im


def glyph_image(ch: str) -> Image.Image:
    font = ImageFont.truetype(str(FONT_PATH), 274)
    mask = Image.new("L", (SIZE, SIZE))
    d = ImageDraw.Draw(mask)
    box = d.textbbox((0, 0), ch, font=font, stroke_width=0)
    w, h = box[2] - box[0], box[3] - box[1]
    x = (SIZE - w) // 2 - box[0]
    y = (SIZE - h) // 2 - box[1] - 5
    d.text((x, y), ch, font=font, fill=255)

    out = Image.new("RGBA", (SIZE, SIZE))
    glow = mask.filter(ImageFilter.GaussianBlur(22))
    glow2 = mask.filter(ImageFilter.GaussianBlur(9))
    out.alpha_composite(Image.merge("RGBA", (glow, glow, glow, glow.point(lambda p: p * 115 // 255))))

    edge = mask.filter(ImageFilter.MaxFilter(21))
    ring = ImageChops.subtract(edge, mask.filter(ImageFilter.MaxFilter(5)))
    violet = Image.new("RGBA", (SIZE, SIZE), (104, 82, 238, 225))
    violet.putalpha(ring.point(lambda p: p * 225 // 255))
    out.alpha_composite(violet)

    inner_edge = mask.filter(ImageFilter.MaxFilter(7))
    inner_ring = ImageChops.subtract(inner_edge, mask)
    pearl = Image.new("RGBA", (SIZE, SIZE), (236, 240, 255, 255))
    pearl.putalpha(inner_ring)
    out.alpha_composite(pearl)

    fill = gradient(SIZE)
    fill.putalpha(mask)
    out.alpha_composite(fill)

    shine = Image.new("RGBA", (SIZE, SIZE))
    sd = ImageDraw.Draw(shine)
    sd.ellipse((95, 58, 225, 112), fill=(255, 255, 255, 85))
    shine = shine.filter(ImageFilter.GaussianBlur(20))
    shine.putalpha(ImageChops.multiply(shine.getchannel("A"), mask))
    out.alpha_composite(shine)
    return out


def crop_alpha(path: Path, padding: int = 18) -> Image.Image:
    im = Image.open(path).convert("RGBA")
    bbox = im.getchannel("A").getbbox()
    if bbox:
        im = im.crop(bbox)
    canvas = Image.new("RGBA", (SIZE, SIZE))
    fit = ImageOps.contain(im, (SIZE - padding * 2, SIZE - padding * 2), Image.Resampling.LANCZOS)
    canvas.alpha_composite(fit, ((SIZE - fit.width) // 2, (SIZE - fit.height) // 2))
    return canvas


def four_point_star(color: tuple[int, int, int], seed: int) -> Image.Image:
    rng = random.Random(seed)
    out = Image.new("RGBA", (SIZE, SIZE))
    glow = Image.new("RGBA", (SIZE, SIZE))
    gd = ImageDraw.Draw(glow)
    gd.ellipse((70, 70, 330, 330), fill=(*color, 115))
    glow = glow.filter(ImageFilter.GaussianBlur(48))
    out.alpha_composite(glow)
    d = ImageDraw.Draw(out)
    pts = [(200, 25), (226, 165), (375, 200), (226, 235), (200, 375), (174, 235), (25, 200), (174, 165)]
    d.polygon(pts, fill=(*color, 235), outline=(255, 255, 255, 245), width=7)
    inner = [(200, 75), (214, 180), (325, 200), (214, 220), (200, 325), (186, 220), (75, 200), (186, 180)]
    d.polygon(inner, fill=(247, 248, 255, 230))
    d.ellipse((184, 184, 216, 216), fill=(255, 255, 255, 255))
    for _ in range(5):
        x, y = rng.randint(65, 335), rng.randint(65, 335)
        r = rng.randint(3, 7)
        d.ellipse((x-r, y-r, x+r, y+r), fill=(255, 255, 255, rng.randint(150, 235)))
    return out


def mist(seed: int, violet: bool = False) -> Image.Image:
    rng = random.Random(seed)
    out = Image.new("RGBA", (SIZE, SIZE))
    base = (177, 160, 255) if violet else (208, 235, 250)
    for i in range(11):
        layer = Image.new("RGBA", (SIZE, SIZE))
        d = ImageDraw.Draw(layer)
        x = rng.randint(20, 325)
        y = rng.randint(95, 285)
        w = rng.randint(95, 220)
        h = rng.randint(28, 68)
        d.ellipse((x-w//2, y-h//2, x+w//2, y+h//2), fill=(*base, rng.randint(45, 105)))
        layer = layer.filter(ImageFilter.GaussianBlur(rng.randint(20, 42)))
        out.alpha_composite(layer)
    ribbons = Image.new("RGBA", (SIZE, SIZE))
    rd = ImageDraw.Draw(ribbons)
    for n in range(3):
        inset = 42 + n * 24
        y_shift = rng.randint(-32, 32)
        rd.arc((inset, 82 + y_shift, SIZE - inset, 305 + y_shift), 205, 338,
               fill=(*base, 95 - n * 18), width=7 - n)
    ribbons = ribbons.filter(ImageFilter.GaussianBlur(5))
    out.alpha_composite(ribbons)
    d = ImageDraw.Draw(out)
    for _ in range(8):
        x, y = rng.randint(45, 355), rng.randint(90, 310)
        r = rng.randint(2, 5)
        d.ellipse((x-r, y-r, x+r, y+r), fill=(255, 255, 255, rng.randint(100, 210)))
    return out


def build() -> None:
    ensure_dirs()
    rows = []
    groups = [
        ("latin_upper", LATIN_UPPER), ("latin_lower", LATIN_LOWER),
        ("cyrillic_upper", CYR_UPPER), ("cyrillic_lower", CYR_LOWER),
        ("digits", DIGITS),
    ]
    for group, chars in groups:
        for ch in chars:
            name = f"{group}_{ord(ch):04x}"
            save_asset(name, glyph_image(ch))
            rows.append((name, group, ch, ch))

    motif_sources = [
        ("star_crystal", ROOT / "app/static/star.png", "⭐"),
        ("star_violet", ROOT / "brfox-social-kit/star-violet.png", "💜"),
        ("star_pearl", ROOT / "brfox-social-kit/star-pearl-white.png", "🤍"),
        ("star_gold", ROOT / "brfox-social-kit/star-champagne-gold.png", "🌟"),
        ("comet_tail", ROOT / "app/static/star2.png", "☄️"),
        ("cospi_dreamer", ROOT / "app/static/foxy.png", "🦊"),
        ("cospi_wizard", ROOT / "social-media-kit/sticker-foxy-wizard.png", "🧙"),
    ]
    for name, source, alt in motif_sources:
        save_asset(name, crop_alpha(source, 12 if name.startswith("cospi") else 22))
        rows.append((name, "motif", "", alt))

    for i, color in enumerate(((183,160,255), (205,225,255), (255,225,158)), 1):
        name = f"sparkle_{i:02d}"
        save_asset(name, four_point_star(color, i))
        rows.append((name, "sparkle", "", "✨"))
    for i in range(1, 5):
        name = f"mist_{i:02d}"
        save_asset(name, mist(i, violet=i % 2 == 0))
        rows.append((name, "mist", "", "🌫️"))

    with (OUT / "manifest.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(("file_stem", "group", "character", "telegram_alt_emoji"))
        writer.writerows(rows)

    make_previews(rows)


def make_previews(rows: list[tuple[str, str, str, str]]) -> None:
    groups = []
    for group in ("motif", "sparkle", "mist", "latin_upper", "latin_lower", "cyrillic_upper", "cyrillic_lower", "digits"):
        items = [r for r in rows if r[1] == group]
        if items:
            groups.append((group, items))
    for group, items in groups:
        cols = 10
        rows_n = math.ceil(len(items) / cols)
        sheet = Image.new("RGB", (cols * 120, rows_n * 135), (28, 27, 34))
        d = ImageDraw.Draw(sheet)
        label_font = ImageFont.truetype(str(FONT_PATH), 16)
        for idx, (name, _, ch, _) in enumerate(items):
            im = Image.open(PNG / f"{name}.png").convert("RGBA").resize((100,100), Image.Resampling.NEAREST)
            x = (idx % cols) * 120 + 10
            y = (idx // cols) * 135 + 5
            sheet.paste(im, (x, y), im)
            d.text((x + 50, y + 108), ch or name.split("_")[-1], font=label_font, fill=(235,232,245), anchor="ma")
        sheet.save(PREVIEW / f"{group}.png", optimize=True)


if __name__ == "__main__":
    build()
