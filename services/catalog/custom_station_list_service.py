"""
Custom Station List PDF
========================
One call -> a branded station list PDF for any customer:
    company name + the logo the user pasted in the frontend (base64 / data URL)
    + which states (and highlighted major cities) to list.

States and major cities default to SS Transport's main network (DEFAULT_STATES /
DEFAULT_MAJOR_CITIES). Cities come live from the `cities` table (cached for 10
minutes, so repeat PDFs take well under a second). Built-in fonts only, so it
renders the same on any server.
"""
import base64
import io
import os
import re
import time
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.lib.utils import ImageReader
from reportlab.pdfgen import canvas as rl_canvas
from PIL import Image as PILImage

from services.supabase_client import get_supabase

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SS_LOGO_PATH = os.path.join(BASE_DIR, "assets", "ss-movesecure-logo-transparent.png")

DEFAULT_STATES = ["Uttar Pradesh", "Delhi", "Bihar", "Jharkhand", "Odisha", "Assam"]
# matched against cities.city_name / city_code; label is what prints
DEFAULT_MAJOR_CITIES = [
    {"label": "DELHI", "match": ["DELHI"]},
    {"label": "KANPUR", "match": ["KANPUR"]},
    {"label": "KOLKATA", "match": ["KOLKATA"]},
    {"label": "PATNA CITY", "match": ["PATNA"]},
    {"label": "ASANSOL", "match": ["ASANSOL"]},
    {"label": "NEPAL", "match": ["NEPAL"]},
    {"label": "IMPHAL", "match": ["IMPHAL MANIPUR", "IMPHAL"]},
    {"label": "LUCKNOW", "match": ["LUCKNOW"]},
    {"label": "GORAKHPUR", "match": ["GORAKHPUR"]},
    {"label": "BASTI", "match": ["BASTI"]},
    {"label": "BANARAS", "match": ["BANARAS"]},
    {"label": "PRAYAGRAJ", "match": ["PRAYAGRAJ"]},
]
STATE_ALIASES = {"UP": "Uttar Pradesh", "U.P": "Uttar Pradesh", "U.P.": "Uttar Pradesh", "ORISSA": "Odisha",
                 "NEW DELHI": "Delhi", "BENGAL": "West Bengal", "WB": "West Bengal", "J&K": "Jammu & Kashmir"}

COMPANY_NAME = "SS MOVESECURE LOGISTICS PVT LTD"
COMPANY_PHONE = "+91-7668291228  |  +91-7902122230"
COMPANY_WEBSITE = "www.ssmovesecure.com"
STATIONS_URL = "https://www.ssmovesecure.com/#stations"

# Theme = the SS Movesecure truck logo: blue cab + orange body.
# (names kept from the old navy/gold theme so the drawing code is unchanged)
NAVY = colors.HexColor("#161A8A")        # logo blue — header, banners, chips
NAVY_LIGHT = colors.HexColor("#3A3FB0")
GOLD = colors.HexColor("#F26522")        # logo orange — accent bars, footer, bullets
GOLD_LIGHT = colors.HexColor("#FFB27A")  # light orange — text on blue
GOLD_PALE = colors.HexColor("#FFF1E8")   # pale orange — major cities band
DARK = colors.HexColor("#1A1A1A")
GREY = colors.HexColor("#5A5A5A")
WHITE = colors.white

MAX_LOGO_BYTES = 5 * 1024 * 1024
_CACHE = {"at": 0.0, "rows": None}
CACHE_SECONDS = 600


# ── data ─────────────────────────────────────────────────────────────────────
def _all_cities() -> list[dict]:
    if _CACHE["rows"] is not None and time.time() - _CACHE["at"] < CACHE_SECONDS:
        return _CACHE["rows"]
    sb, rows, off = get_supabase(), [], 0
    while True:
        r = sb.table("cities").select("city_name, city_code, state_name").range(off, off + 999).execute().data or []
        rows += r
        if len(r) < 1000:
            break
        off += 1000
    _CACHE.update(at=time.time(), rows=rows)
    return rows


def _norm_state(s: str) -> str:
    s = (s or "").strip()
    return STATE_ALIASES.get(s.upper(), s)


def available_states() -> dict:
    """States present in the cities table, with city counts — for the frontend picker."""
    counts: dict[str, int] = {}
    for r in _all_cities():
        if r.get("state_name"):
            counts[r["state_name"]] = counts.get(r["state_name"], 0) + 1
    return {
        "status": "success",
        "data": {
            "default_states": DEFAULT_STATES,
            "default_major_cities": [m["label"] for m in DEFAULT_MAJOR_CITIES],
            "states": [{"state_name": k, "city_count": v} for k, v in sorted(counts.items())],
        },
    }


def _cities_by_state(states: list[str]) -> dict[str, list[str]]:
    rows = _all_cities()
    out = {}
    for st in states:
        names = sorted({(r["city_name"] or "").strip().upper() for r in rows
                        if (r.get("state_name") or "").strip().lower() == st.lower() and r.get("city_name")})
        out[st] = names
    return out


def _major_cities(spec) -> list[str]:
    """Printed labels for the MAJOR CITIES band (strings or {label} dicts), de-duplicated."""
    out = []
    for m in spec:
        if isinstance(m, str):
            m = {"label": m.strip().upper(), "match": [m.strip().upper()]}
        label = m["label"].strip().upper()
        if label and label not in out:
            out.append(label)
    return out


# ── logo ─────────────────────────────────────────────────────────────────────
def _decode_logo(logo: str | bytes | None) -> PILImage.Image | None:
    """Accepts a data URL ('data:image/png;base64,....'), plain base64, or raw bytes."""
    if not logo:
        return None
    if isinstance(logo, str):
        m = re.match(r"^data:image/[a-zA-Z0-9.+-]+;base64,(.*)$", logo.strip(), re.S)
        raw = base64.b64decode(m.group(1) if m else logo.strip(), validate=False)
    else:
        raw = logo
    if len(raw) > MAX_LOGO_BYTES:
        raise ValueError("logo is larger than 5 MB")
    try:
        img = PILImage.open(io.BytesIO(raw))
        img.load()
    except Exception:
        raise ValueError("logo is not a readable image (send a PNG / JPG / WEBP)")
    img = img.convert("RGBA")
    img.thumbnail((900, 900))                       # plenty for print, keeps the PDF small
    return img


def _reader(img: PILImage.Image, alpha: float | None = None) -> ImageReader:
    if alpha is not None:
        img = img.copy()
        img.putalpha(img.getchannel("A").point(lambda v: int(v * alpha)))
    buf = io.BytesIO()
    img.save(buf, format="PNG", optimize=True)
    buf.seek(0)
    return ImageReader(buf)


def _draw_fit(c, img: ImageReader, x: float, y: float, max_w: float, max_h: float, align="left") -> float:
    iw, ih = img.getSize()
    s = min(max_w / iw, max_h / ih)
    w, h = iw * s, ih * s
    if align == "center":
        x = x - w / 2
    elif align == "right":
        x = x - w
    c.drawImage(img, x, y + (max_h - h) / 2, width=w, height=h, mask="auto")
    return w


def _fit(c, text, font, size, max_w, min_size=7):
    while size > min_size and c.stringWidth(text, font, size) > max_w:
        size -= 0.5
    return size


# ── PDF ──────────────────────────────────────────────────────────────────────
PAGE_W, PAGE_H = A4
M = 10 * mm
HEADER_H = 38 * mm
FOOTER_H = 16 * mm
ROW_H = 5.6 * mm
BANNER_H = 9 * mm
COLS = 3


def generate_custom_station_list_pdf(data: dict) -> bytes:
    """Build the PDF. Text size: data["text_scale"] = "auto" (default) grows the station
    text to fill the empty space without adding a page; or a number 0.8-2.0 (1 = old size)."""
    ts = data.get("text_scale", "auto")
    if ts not in (None, "", "auto"):
        try:
            ts = float(ts)
        except (TypeError, ValueError):
            raise ValueError("text_scale must be 'auto' or a number between 0.8 and 2.0")
        if not 0.8 <= ts <= 2.0:
            raise ValueError("text_scale must be 'auto' or a number between 0.8 and 2.0")
        return _render(data, ts)[0]
    best, pages = _render(data, 1.0)
    lo, hi = 1.0, 1.9
    for _ in range(6):                     # binary search: largest scale with no extra page
        mid = (lo + hi) / 2
        pdf, n = _render(data, mid)
        if n <= pages:
            lo, best = mid, pdf
        else:
            hi = mid
    return best


def _render(data: dict, scale: float) -> tuple[bytes, int]:
    """
    data = {
      company_name: "SHINE PRODUCTS"            (required),
      logo: "data:image/png;base64,..."        (the pasted image; optional),
      states?: ["Uttar Pradesh", "Bihar", ...]  (default DEFAULT_STATES; 'UP' / 'Orissa' accepted),
      major_cities?: ["DELHI", "KANPUR", ...]   (default DEFAULT_MAJOR_CITIES; [] hides the band),
      major_cities_position?: "bottom" | "top"  (default "bottom" — after the state lists),
      title?: "STATION LIST FOR SHINE PRODUCTS",
      show_watermark?: true
    }
    """
    company = (data.get("company_name") or "").strip().upper()
    if not company:
        raise ValueError("company_name is required")
    position = str(data.get("major_cities_position") or "bottom").strip().lower()
    if position not in ("bottom", "top"):
        raise ValueError("major_cities_position must be 'bottom' or 'top'")
    majors_top = position == "top"
    states = [_norm_state(s) for s in (data.get("states") or DEFAULT_STATES) if str(s).strip()]
    majors = _major_cities(data["major_cities"] if data.get("major_cities") is not None else DEFAULT_MAJOR_CITIES)
    title = (data.get("title") or f"STATION LIST FOR {company}").strip().upper()
    logo_img = _decode_logo(data.get("logo"))
    logo = _reader(logo_img) if logo_img else None
    ss_logo = ImageReader(SS_LOGO_PATH) if os.path.exists(SS_LOGO_PATH) else None
    watermark = _reader(logo_img, alpha=0.07) if (logo_img and data.get("show_watermark", True)) else None
    cities = _cities_by_state(states)
    total = sum(len(v) for v in cities.values())

    row_h = ROW_H * scale
    banner_h = BANNER_H * min(scale, 1.4)
    name_fs = min(9 * scale + 1.5, 15)          # station names: bigger than the old 9pt even at scale 1
    banner_fs = 11 * min(scale, 1.4)
    buf = io.BytesIO()
    c = rl_canvas.Canvas(buf, pagesize=A4)
    c.setTitle(title)
    c.setAuthor(COMPANY_NAME)
    state = {"page": 0, "y": 0.0}

    def header():
        top = PAGE_H - M
        c.setFillColor(NAVY)
        c.rect(0, PAGE_H - HEADER_H, PAGE_W, HEADER_H, fill=1, stroke=0)
        c.setFillColor(GOLD)
        c.rect(0, PAGE_H - HEADER_H - 2.2 * mm, PAGE_W, 2.2 * mm, fill=1, stroke=0)
        box = 26 * mm
        text_x = M
        if logo:                                     # customer logo on a white tile
            c.setFillColor(WHITE)
            c.roundRect(M, PAGE_H - HEADER_H + 6 * mm, box, box, 3 * mm, fill=1, stroke=0)
            _draw_fit(c, logo, M + 2 * mm, PAGE_H - HEADER_H + 8 * mm, box - 4 * mm, box - 4 * mm)
            text_x = M + box + 5 * mm
        right_w = 0
        if ss_logo:
            # white tile so the logo's blue cab stays visible on the blue header
            c.setFillColor(WHITE)
            c.roundRect(PAGE_W - M - 30 * mm, PAGE_H - HEADER_H + 13.5 * mm, 30 * mm, 17 * mm, 2.5 * mm, fill=1, stroke=0)
            right_w = _draw_fit(c, ss_logo, PAGE_W - M - 2 * mm, PAGE_H - HEADER_H + 15 * mm, 26 * mm, 14 * mm, align="right")
            right_w = max(right_w, 30 * mm)
            c.setFillColor(GOLD_LIGHT)
            c.setFont("Helvetica-Bold", 6.5)
            c.drawRightString(PAGE_W - M, PAGE_H - HEADER_H + 10 * mm, COMPANY_NAME)
        max_w = PAGE_W - text_x - M - max(right_w, 30 * mm) - 4 * mm
        c.setFillColor(WHITE)
        size = _fit(c, company, "Helvetica-Bold", 20, max_w)
        c.setFont("Helvetica-Bold", size)
        c.drawString(text_x, top - 12 * mm, company)
        c.setFillColor(GOLD_LIGHT)
        tsize = _fit(c, title, "Helvetica-Bold", 11.5, max_w)
        c.setFont("Helvetica-Bold", tsize)
        c.drawString(text_x, top - 19.5 * mm, title)
        c.setFillColor(colors.HexColor("#C9D3DD"))
        c.setFont("Helvetica", 8)
        c.drawString(text_x, top - 25 * mm, f"Booking & delivery network  ·  {len(states)} states  ·  {total} stations")

    def footer():
        c.setFillColor(GOLD)
        c.rect(0, 0, PAGE_W, FOOTER_H, fill=1, stroke=0)
        c.setFillColor(NAVY)
        c.rect(0, FOOTER_H, PAGE_W, 1.2 * mm, fill=1, stroke=0)
        c.setFont("Helvetica-Bold", 8.5)
        c.drawString(M, FOOTER_H / 2 + 0.5 * mm, "Call: " + COMPANY_PHONE)
        c.drawRightString(PAGE_W - M, FOOTER_H / 2 + 0.5 * mm, COMPANY_WEBSITE)
        c.setFillColor(NAVY)
        c.circle(PAGE_W / 2, FOOTER_H / 2 + 1.5, 4.6 * mm, fill=1, stroke=0)
        c.setFillColor(GOLD_LIGHT)
        c.setFont("Helvetica-Bold", 9)
        c.drawCentredString(PAGE_W / 2, FOOTER_H / 2 - 1.2, str(state["page"]))

    def new_page():
        if state["page"]:
            footer()
            c.showPage()
        state["page"] += 1
        c.setFillColor(WHITE)
        c.rect(0, 0, PAGE_W, PAGE_H, fill=1, stroke=0)
        if watermark:
            wm = 120 * mm
            _draw_fit(c, watermark, PAGE_W / 2, (PAGE_H - wm) / 2, wm, wm, align="center")
        header()
        state["y"] = PAGE_H - HEADER_H - 8 * mm

    def need(h):
        if state["y"] - h < FOOTER_H + 6 * mm:
            new_page()
            return True
        return False

    def banner(text):
        y = state["y"] - banner_h
        c.setFillColor(NAVY)
        c.roundRect(M, y, PAGE_W - 2 * M, banner_h, 2 * mm, fill=1, stroke=0)
        c.setFillColor(GOLD)
        c.rect(M, y, 3 * mm, banner_h, fill=1, stroke=0)
        c.setFillColor(WHITE)
        c.setFont("Helvetica-Bold", banner_fs)
        c.drawString(M + 6 * mm, y + (banner_h - banner_fs * 0.7) / 2, text)
        state["y"] = y - 4 * mm

    def major_band():
        rows_needed = (len(majors) + 3) // 4
        band_h = 12 * mm + rows_needed * 9 * mm
        need(band_h + 7 * mm)
        c.setFillColor(GOLD_PALE)
        y = state["y"] - band_h
        c.roundRect(M, y, PAGE_W - 2 * M, band_h, 3 * mm, fill=1, stroke=0)
        c.setStrokeColor(GOLD)
        c.setLineWidth(1)
        c.roundRect(M, y, PAGE_W - 2 * M, band_h, 3 * mm, fill=0, stroke=1)
        c.setFillColor(NAVY)
        c.setFont("Helvetica-Bold", 11)
        c.drawString(M + 5 * mm, y + band_h - 8 * mm, "MAJOR CITIES")
        chip_w = (PAGE_W - 2 * M - 10 * mm - 3 * 4 * mm) / 4
        for i, name in enumerate(majors):
            r, col = divmod(i, 4)
            cx = M + 5 * mm + col * (chip_w + 4 * mm)
            cy = y + band_h - 18 * mm - r * 9 * mm
            c.setFillColor(NAVY)
            c.roundRect(cx, cy, chip_w, 7 * mm, 3.5 * mm, fill=1, stroke=0)
            c.setFillColor(GOLD_LIGHT)
            c.setFont("Helvetica-Bold", _fit(c, name, "Helvetica-Bold", 9.5, chip_w - 4 * mm))
            c.drawCentredString(cx + chip_w / 2, cy + 2.3 * mm, name)
        state["y"] = y - 7 * mm

    new_page()
    if majors and majors_top:
        major_band()

    # ── states ──
    col_w = (PAGE_W - 2 * M) / COLS
    for st in states:
        names = cities.get(st) or []
        need(banner_h + 4 * mm + row_h * min(3, max(1, len(names))))
        banner(f"{st.upper()}  —  {len(names)} STATION{'' if len(names) == 1 else 'S'}")
        if not names:
            c.setFillColor(GREY)
            c.setFont("Helvetica-Oblique", 9)
            c.drawString(M + 3 * mm, state["y"] - 3 * mm, "No stations listed for this state yet.")
            state["y"] -= row_h + 4 * mm
            continue
        i = 0
        while i < len(names):
            avail_rows = int((state["y"] - FOOTER_H - 6 * mm) // row_h)
            if avail_rows < 1:
                new_page()
                banner(f"{st.upper()}  (CONTD.)")
                continue
            chunk = names[i:i + avail_rows * COLS]
            per_col = (len(chunk) + COLS - 1) // COLS
            for j, nm in enumerate(chunk):
                col, row = divmod(j, per_col)
                x = M + 3 * mm + col * col_w
                yy = state["y"] - (row + 1) * row_h + (row_h - name_fs * 0.7) / 2
                c.setFillColor(GOLD)
                c.circle(x + 1 * mm, yy + name_fs * 0.33, 0.8 * mm * min(scale, 1.4), fill=1, stroke=0)
                c.setFillColor(DARK)
                c.setFont("Helvetica", _fit(c, nm, "Helvetica", name_fs, col_w - 7 * mm, 6))
                c.drawString(x + 3.5 * mm, yy, nm)
            state["y"] -= per_col * row_h + 5 * mm
            i += len(chunk)

    # ── major cities band (default: below the state lists) ──
    if majors and not majors_top:
        major_band()

    # ── closing line ──
    need(16 * mm)
    y = state["y"] - 12 * mm
    c.setFillColor(NAVY)
    c.roundRect(M, y, PAGE_W - 2 * M, 12 * mm, 2 * mm, fill=1, stroke=0)
    c.setFillColor(GOLD_LIGHT)
    closing = f"{COMPANY_NAME}  ·  {len(states)} states  ·  {total} stations  ·  {STATIONS_URL}"
    c.setFont("Helvetica-Bold", _fit(c, closing, "Helvetica-Bold", 10.5, PAGE_W - 2 * M - 8 * mm))
    c.drawCentredString(PAGE_W / 2, y + 4.5 * mm, closing)

    footer()
    c.showPage()
    c.save()
    return buf.getvalue(), state["page"]
