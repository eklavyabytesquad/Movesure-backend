"""
Station Catalog PDF
======================
Generates the SS MOVESECURE LOGISTICS PVT LTD station list catalog:
indigo/orange branded pages (matching the company logo's own colors), one
banner per state, cities in a 2-column list.

Rebuilt from the old top-level generate_catalog.py script into a proper,
customizable service:
  - City data comes live from the `cities` table (state_name), not a
    stale CSV export — new cities show up automatically.
  - STATES and FREIGHT_RATES below are the two things to edit to
    customize the catalog — which states are included, and what each
    state's freight rate line says.
  - Returns PDF bytes (generate_station_catalog_pdf()) so it can be
    served from an API route or saved to a file — see
    save_station_catalog_pdf() / `python -m services.catalog.station_catalog_service`.
"""
import io
import os
import qrcode
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.units import mm
from reportlab.pdfgen import canvas as rl_canvas
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.lib.utils import ImageReader
from PIL import Image as PILImage

from services.supabase_client import get_supabase

# ── Customize here ──────────────────────────────────────────────────────────

STATES = ["UTTAR PRADESH", "UTTARAKHAND", "DELHI", "BIHAR", "JHARKHAND", "ODISHA", "ASSAM", "MIZORAM", "MANIPUR"]

# Shown on the cover/header as "Prepared for: M/s <name>" — this catalog is
# handed to a specific customer. Pass a different consignor_name to
# generate_station_catalog_pdf() per customer; defaults to this sample.
SAMPLE_CONSIGNOR = "SPIDER METAL PRODUCTS PVT LTD"

# Freight rates are NOT shown in this catalog — see
# services/catalog/rate_list_service.py for the per-consignor rate list PDF.

STATIONS_URL = "https://www.ssmovesecure.com/#stations"

COMPANY_NAME = "SS MOVESECURE LOGISTICS PVT LTD"
COMPANY_WEBSITE = "ssmovesecure.com"

# ── Paths ─────────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
LOGO_PATH = os.path.join(BASE_DIR, "assets", "ss-movesecure-logo-transparent.png")
LOGO_ASPECT = 1325 / 800  # width / height of ss-movesecure-logo-transparent.png
CONSIGNOR_LOGO_PATH = os.path.join(BASE_DIR, "assets", "spider-metal-icon.png")
DEFAULT_OUTPUT_PATH = os.path.join(BASE_DIR, "station_catalog_v2.pdf")

# ── Fonts — fall back to built-ins if the Windows font files aren't there ──
_WIN_FONTS = "C:/Windows/Fonts"
_FONT_FILES = {
    "Georgia": "georgia.ttf", "Georgia-Bold": "georgiab.ttf",
    "Georgia-Italic": "georgiai.ttf", "Georgia-BoldItalic": "georgiaz.ttf",
    "Calibri": "calibri.ttf", "Calibri-Bold": "calibrib.ttf",
}
_FALLBACK = {
    "Georgia": "Times-Roman", "Georgia-Bold": "Times-Bold",
    "Georgia-Italic": "Times-Italic", "Georgia-BoldItalic": "Times-BoldItalic",
    "Calibri": "Helvetica", "Calibri-Bold": "Helvetica-Bold",
}


def _register_fonts() -> dict:
    """Returns the actual font-name map to use — real fonts if available, else built-ins."""
    names = {}
    for font_name, filename in _FONT_FILES.items():
        path = os.path.join(_WIN_FONTS, filename)
        if os.path.exists(path):
            try:
                pdfmetrics.registerFont(TTFont(font_name, path))
                names[font_name] = font_name
                continue
            except Exception:
                pass
        names[font_name] = _FALLBACK[font_name]
    return names


FONT = _register_fonts()

# ── Palette — matches the SS Movesecure logo's own colors ─────────────────
NAVY = colors.HexColor("#181888")        # logo's indigo-blue
NAVY_DARK = colors.HexColor("#0E0E52")
NAVY_LIGHT = colors.HexColor("#3535A8")
GOLD = colors.HexColor("#F7641F")        # logo's orange
GOLD_LIGHT = colors.HexColor("#FF8F52")
GOLD_PALE = colors.HexColor("#FDE7DA")
GOLD_DARK = colors.HexColor("#C94E12")
WHITE = colors.white
DARK = colors.HexColor("#1A1A1A")

# ── Layout constants ────────────────────────────────────────────────────────
# Page border — drawn on the canvas (replaces the old unused
# assets/_SS CATALOG BORDER.png image asset entirely).
BORDER_MARGIN = 6 * mm
BORDER_GAP = 1.2 * mm
PAGE_INSET = BORDER_MARGIN + BORDER_GAP

LOGO_H = 30 * mm
FOOTER_H = 20 * mm
ROW_H = 7 * mm          # was 6mm — bumped to fit the larger station-name font
CITY_FONT_SIZE = 12.5   # was 10.5
COL1_X = PAGE_INSET + 6 * mm
BANNER_H = 10 * mm


def _load_state_cities() -> dict:
    """Live from the `cities` table, grouped and sorted per STATES, above."""
    sb = get_supabase()
    result = {}
    for state in STATES:
        rows = (
            sb.table("cities")
            .select("city_name")
            .ilike("state_name", f"%{state}%")
            .execute()
            .data or []
        )
        names = sorted({r["city_name"].strip().upper() for r in rows if r.get("city_name")})
        if names:
            result[state] = names
    return result


# ── Watermark ─────────────────────────────────────────────────────────────
_WATERMARK = None


def _make_watermark(path: str, alpha: float = 0.09) -> ImageReader:
    img = PILImage.open(path).convert("RGBA")
    r, g, b, a = img.split()
    a = a.point(lambda x: int(x * alpha))
    img.putalpha(a)
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    return ImageReader(buf)


def draw_watermark(c, width, height):
    global _WATERMARK
    if _WATERMARK is None:
        _WATERMARK = _make_watermark(LOGO_PATH)
    wm = 130 * mm
    c.drawImage(_WATERMARK, (width - wm) / 2, (height - wm) / 2,
                width=wm, height=wm, preserveAspectRatio=True, mask="auto")


def draw_page_border(c, width, height):
    """Double-line border frame, drawn on the canvas — no image asset needed."""
    c.setStrokeColor(GOLD)
    c.setLineWidth(1.2)
    c.rect(BORDER_MARGIN, BORDER_MARGIN, width - 2 * BORDER_MARGIN, height - 2 * BORDER_MARGIN, fill=0, stroke=1)
    c.setStrokeColor(NAVY)
    c.setLineWidth(0.5)
    c.rect(PAGE_INSET, PAGE_INSET, width - 2 * PAGE_INSET, height - 2 * PAGE_INSET, fill=0, stroke=1)


# ── Header — plain white letterhead; a dark banner behind the logo's own
# blue half made the logo hard to read, so color now lives only in the
# thin accent rules, not a solid background block. ──────────────────────
def draw_header(c, width, height) -> float:
    logo_w = LOGO_H * LOGO_ASPECT
    logo_x = PAGE_INSET + 5 * mm
    logo_y = height - PAGE_INSET - 5 * mm - LOGO_H
    c.drawImage(LOGO_PATH, logo_x, logo_y, width=logo_w, height=LOGO_H,
                preserveAspectRatio=True, mask="auto")

    text_x = logo_x + logo_w + 5 * mm
    max_text_w = width - PAGE_INSET - 4 * mm - text_x
    name_y = logo_y + LOGO_H - 7 * mm

    name_font_size = 15
    while name_font_size > 9 and c.stringWidth(COMPANY_NAME, FONT["Georgia-Bold"], name_font_size) > max_text_w:
        name_font_size -= 0.5

    c.setFillColor(NAVY)
    c.setFont(FONT["Georgia-Bold"], name_font_size)
    c.drawString(text_x, name_y, COMPANY_NAME)

    cw = c.stringWidth(COMPANY_NAME, FONT["Georgia-Bold"], name_font_size)
    c.setStrokeColor(GOLD)
    c.setLineWidth(1.2)
    c.line(text_x, name_y - 2 * mm, text_x + cw, name_y - 2 * mm)

    cat_y = name_y - 10 * mm
    c.setFillColor(GOLD)
    c.setFont(FONT["Georgia-BoldItalic"], 15)
    c.drawString(text_x, cat_y, "Station List Catalog")

    c.setFillColor(DARK)
    c.setFont(FONT["Calibri"], 8.5)
    c.drawString(text_x, cat_y - 6 * mm, COMPANY_WEBSITE)

    rule_y = logo_y - 4 * mm
    c.setStrokeColor(GOLD)
    c.setLineWidth(1.3)
    c.line(PAGE_INSET + 4 * mm, rule_y, width - PAGE_INSET - 4 * mm, rule_y)
    c.setStrokeColor(NAVY)
    c.setLineWidth(0.4)
    c.line(PAGE_INSET + 4 * mm, rule_y - 1 * mm, width - PAGE_INSET - 4 * mm, rule_y - 1 * mm)

    return rule_y - 3 * mm


CONSIGNOR_BAND_H = 12 * mm


def draw_consignor_band(c, width, top_y, consignor_name: str) -> float:
    """"Prepared for" strip — this catalog is being handed to a specific customer."""
    band_y = top_y - CONSIGNOR_BAND_H
    c.setFillColor(GOLD_PALE)
    c.rect(PAGE_INSET, band_y, width - 2 * PAGE_INSET, CONSIGNOR_BAND_H, fill=1, stroke=0)
    c.setStrokeColor(GOLD)
    c.setLineWidth(1)
    c.line(PAGE_INSET, band_y + CONSIGNOR_BAND_H, width - PAGE_INSET, band_y + CONSIGNOR_BAND_H)
    c.line(PAGE_INSET, band_y, width - PAGE_INSET, band_y)

    c.setFillColor(NAVY)
    c.setFont(FONT["Calibri-Bold"], 10.5)
    c.drawCentredString(width / 2, band_y + CONSIGNOR_BAND_H / 2 - 3.5, f"PREPARED FOR:  M/s {consignor_name.upper()}")
    return band_y


# ── State banner ─────────────────────────────────────────────────────────
def draw_state_banner(c, width, top_y, label) -> float:
    band_y = top_y - BANNER_H - 4 * mm
    lx, rx = PAGE_INSET, width - PAGE_INSET

    c.setFillColor(GOLD_PALE)
    c.rect(lx, band_y, rx - lx, BANNER_H, fill=1, stroke=0)

    c.setStrokeColor(GOLD)
    c.setLineWidth(2)
    c.line(lx, band_y, lx, band_y + BANNER_H)
    c.line(rx, band_y, rx, band_y + BANNER_H)
    c.setLineWidth(0.8)
    c.line(lx, band_y, rx, band_y)
    c.line(lx, band_y + BANNER_H, rx, band_y + BANNER_H)

    c.setFillColor(NAVY)
    c.setFont(FONT["Georgia-BoldItalic"], 13)
    c.drawCentredString((lx + rx) / 2, band_y + 3 * mm, label)

    return band_y


def draw_column_divider(c, width, top_y):
    div_x = width / 2 + 2 * mm
    c.setStrokeColor(GOLD)
    c.setLineWidth(0.9)
    c.line(div_x, FOOTER_H + 1 * mm, div_x, top_y)
    c.setStrokeColor(GOLD_LIGHT)
    c.setLineWidth(0.3)
    c.line(div_x + 1, FOOTER_H + 1 * mm, div_x + 1, top_y)


def draw_footer(c, width, page_num=1):
    """Thin rule + contact line + page number — no solid color bars."""
    rule_y = PAGE_INSET + 9 * mm
    c.setStrokeColor(GOLD)
    c.setLineWidth(1)
    c.line(PAGE_INSET + 4 * mm, rule_y, width - PAGE_INSET - 4 * mm, rule_y)

    text_y = rule_y - 5.5 * mm
    c.setFillColor(DARK)
    c.setFont(FONT["Calibri-Bold"], 8.5)
    c.drawString(PAGE_INSET + 4 * mm, text_y, "+91-7902122230   |   +91-9695293140")
    c.drawRightString(width - PAGE_INSET - 4 * mm, text_y, COMPANY_WEBSITE)

    cx, cy, radius = width / 2, rule_y + 3 * mm, 4 * mm
    c.setFillColor(WHITE)
    c.setStrokeColor(NAVY)
    c.setLineWidth(1)
    c.circle(cx, cy, radius, fill=1, stroke=1)
    c.setFillColor(NAVY)
    c.setFont(FONT["Georgia-Bold"], 8)
    c.drawCentredString(cx, cy - 2.8, str(page_num))

    c.setFillColor(GOLD)
    c.setFont(FONT["Georgia-Italic"], 7.5)
    c.drawCentredString(width / 2, PAGE_INSET + 2 * mm, "Trusted Transport Partner Across India")


def draw_closing_banner(c, width, y, total_states, total_cities) -> float:
    banner_h = 16 * mm
    band_y = y - banner_h - 6 * mm
    lx, rx = PAGE_INSET, width - PAGE_INSET

    c.setFillColor(GOLD_PALE)
    c.rect(lx, band_y, rx - lx, banner_h, fill=1, stroke=0)
    c.setStrokeColor(GOLD)
    c.setLineWidth(1.5)
    c.line(lx, band_y + banner_h, rx, band_y + banner_h)
    c.line(lx, band_y, rx, band_y)

    c.setFillColor(NAVY)
    c.setFont(FONT["Georgia-Bold"], 11)
    c.drawCentredString(width / 2, band_y + 9.5 * mm, f"Covering {total_states} States  &  {total_cities} Cities")

    c.setFillColor(GOLD_DARK)
    c.setFont(FONT["Georgia-BoldItalic"], 9.5)
    c.drawCentredString(width / 2, band_y + 3.5 * mm, "We Help You Move Faster & Grow Faster")

    return band_y


_QR_IMAGE = None


def _make_qr_image(url: str) -> ImageReader:
    qr = qrcode.QRCode(border=1, box_size=10)
    qr.add_data(url)
    qr.make(fit=True)
    img = qr.make_image(fill_color="#181888", back_color="white").convert("RGB")
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    return ImageReader(buf)


QR_SECTION_H = 46 * mm


def draw_qr_section(c, width, top_y, url: str) -> float:
    """Closing QR block — scan to open this station list online."""
    global _QR_IMAGE
    if _QR_IMAGE is None:
        _QR_IMAGE = _make_qr_image(url)

    band_y = top_y - QR_SECTION_H
    lx, rx = PAGE_INSET, width - PAGE_INSET

    c.setFillColor(WHITE)  # opaque — covers the column divider line running behind it
    c.setStrokeColor(GOLD)
    c.setLineWidth(1.3)
    c.rect(lx, band_y, rx - lx, QR_SECTION_H, fill=1, stroke=1)

    qr_size = 28 * mm
    qr_x = width / 2 - qr_size / 2
    qr_y = band_y + QR_SECTION_H - qr_size - 6 * mm
    c.drawImage(_QR_IMAGE, qr_x, qr_y, width=qr_size, height=qr_size)

    c.setFillColor(NAVY)
    c.setFont(FONT["Calibri-Bold"], 10.5)
    c.drawCentredString(width / 2, qr_y - 6 * mm, "Scan QR Code for the Link to this Station List")

    c.setFillColor(GOLD_DARK)
    c.setFont(FONT["Calibri"], 9)
    c.drawCentredString(width / 2, qr_y - 11 * mm, url)

    return band_y


# ── Generator ─────────────────────────────────────────────────────────────
def generate_station_catalog_pdf(consignor_name: str = SAMPLE_CONSIGNOR) -> bytes:
    """Builds the catalog and returns it as PDF bytes (for an API response)."""
    state_cities = _load_state_cities()
    width, height = A4
    buf = io.BytesIO()
    c = rl_canvas.Canvas(buf, pagesize=A4)

    col2_x = width / 2 + 6 * mm
    page_started = False
    current_y = 0.0
    page_num = 0

    def begin_page():
        nonlocal current_y, page_started, page_num
        if page_started:
            draw_footer(c, width, page_num)
            c.showPage()
        page_num += 1
        c.setFillColor(WHITE)
        c.rect(0, 0, width, height, fill=1, stroke=0)
        draw_watermark(c, width, height)
        draw_page_border(c, width, height)
        header_bottom = draw_header(c, width, height)
        band_bottom = draw_consignor_band(c, width, header_bottom, consignor_name)
        draw_column_divider(c, width, band_bottom)
        current_y = band_bottom
        page_started = True

    begin_page()

    for state, cities in state_cities.items():
        n = len(cities)
        needed = BANNER_H + 4 * mm + 8 * mm + ROW_H
        if current_y - needed < FOOTER_H:
            begin_page()

        current_y = draw_state_banner(c, width, current_y, f"{state}  ({n} Stations)")
        current_y -= 8 * mm

        # Fill sequentially: left column top-to-bottom, then right column
        # continues the same running number where the left column left off
        # — 1-28 left / 29-56 right, not 1-28 left / 251-278 right.
        printed = 0
        while printed < n:
            rows_avail = max(1, int((current_y - FOOTER_H) // ROW_H))

            left_chunk = cities[printed: printed + rows_avail]
            right_chunk = cities[printed + len(left_chunk): printed + len(left_chunk) + rows_avail]

            # Fill+stroke render mode (mode=2) gives the bold font extra
            # visual weight — plain fill-only bold can still look thin at this size.
            c.setFont(FONT["Calibri-Bold"], CITY_FONT_SIZE)
            c.setFillColor(DARK)
            c.setStrokeColor(DARK)
            c.setLineWidth(0.35)
            for i, city in enumerate(left_chunk):
                c.drawString(COL1_X, current_y - i * ROW_H, f"{printed + i + 1}.  {city}", mode=2)
            right_start = printed + len(left_chunk)
            for i, city in enumerate(right_chunk):
                c.drawString(col2_x, current_y - i * ROW_H, f"{right_start + i + 1}.  {city}", mode=2)

            rows_used = max(len(left_chunk), len(right_chunk))
            current_y -= rows_used * ROW_H
            printed += len(left_chunk) + len(right_chunk)

            if printed < n:
                begin_page()
                current_y = draw_state_banner(c, width, current_y, f"{state}  (Contd.)")
                current_y -= 8 * mm

        current_y -= 5 * mm

    total_states = len(state_cities)
    total_cities = sum(len(v) for v in state_cities.values())
    if current_y - 20 * mm < FOOTER_H:
        begin_page()
    current_y = draw_closing_banner(c, width, current_y, total_states, total_cities)

    if current_y - QR_SECTION_H - 6 * mm < FOOTER_H:
        begin_page()
    draw_qr_section(c, width, current_y, STATIONS_URL)

    draw_footer(c, width, page_num)
    c.showPage()
    c.save()
    return buf.getvalue()


def save_station_catalog_pdf(consignor_name: str = SAMPLE_CONSIGNOR, output_path: str = None) -> str:
    """Convenience wrapper — writes the PDF to disk, returns the path."""
    output_path = output_path or DEFAULT_OUTPUT_PATH
    pdf_bytes = generate_station_catalog_pdf(consignor_name)
    with open(output_path, "wb") as f:
        f.write(pdf_bytes)
    return output_path


if __name__ == "__main__":
    path = save_station_catalog_pdf()
    print(f"Catalog saved to: {path}")
