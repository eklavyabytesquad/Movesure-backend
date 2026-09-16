"""
Consignment Tracking Card PDF
===============================
A single-page, good-looking card for one consignor: both brand logos up
top, a big "TRACK YOUR CONSIGNMENT HERE" heading with an arrow pointing
straight down at a large QR code, and "Stay connected with your order"
underneath. Same indigo/orange theme, border, and watermark as the rate
list and station catalog — this is the "hand it to the customer" version
of those, built around one QR code instead of a table or a city list.

Pass a different gstin/consignor_name per customer; both default to the
sample used to build this.
"""
import io
import os
import qrcode
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.units import cm, mm
from reportlab.pdfgen import canvas as rl_canvas
from reportlab.lib.utils import ImageReader
from PIL import Image as PILImage

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SS_LOGO_PATH = os.path.join(BASE_DIR, "assets", "ss-movesecure-logo-transparent.png")
SS_LOGO_ASPECT = 1325 / 800
SPIDER_LOGO_PATH = os.path.join(BASE_DIR, "assets", "spider-metal-transparent.png")
SPIDER_LOGO_ASPECT = 597 / 335

COMPANY_NAME = "SS MOVESECURE LOGISTICS PVT LTD"
COMPANY_CONTACT = "+91-7902122230   |   +91-9695293140   |   ssmovesecure.com"

TRACKING_BASE_URL = "https://www.ssmovesecure.com/tracking/"
SAMPLE_GSTIN = "09AAICS9681G1ZP"
SAMPLE_CONSIGNOR = "SPIDER METAL PRODUCTS PVT LTD"

NAVY = colors.HexColor("#181888")
GOLD = colors.HexColor("#F7641F")
GOLD_LIGHT = colors.HexColor("#FF8F52")
GOLD_PALE = colors.HexColor("#FDE7DA")
DARK = colors.HexColor("#1A1A1A")
GREY = colors.HexColor("#4A4A4A")
WHITE = colors.white

PAGE_W, PAGE_H = A4
BORDER_MARGIN = 0.9 * cm
BORDER_GAP = 0.12 * cm
INSET = BORDER_MARGIN + BORDER_GAP


def _make_qr_image(url: str) -> ImageReader:
    qr = qrcode.QRCode(border=1, box_size=10)
    qr.add_data(url)
    qr.make(fit=True)
    img = qr.make_image(fill_color="#181888", back_color="white").convert("RGB")
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    return ImageReader(buf)


def _make_watermark(path: str, alpha: float = 0.05) -> ImageReader:
    img = PILImage.open(path).convert("RGBA")
    r, g, b, a = img.split()
    a = a.point(lambda x: int(x * alpha))
    img.putalpha(a)
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    return ImageReader(buf)


def _draw_border(c):
    c.setStrokeColor(GOLD)
    c.setLineWidth(1.4)
    c.rect(BORDER_MARGIN, BORDER_MARGIN, PAGE_W - 2 * BORDER_MARGIN, PAGE_H - 2 * BORDER_MARGIN, fill=0, stroke=1)
    c.setStrokeColor(NAVY)
    c.setLineWidth(0.5)
    c.rect(INSET, INSET, PAGE_W - 2 * INSET, PAGE_H - 2 * INSET, fill=0, stroke=1)


def _draw_arrow(c, x: float, top_y: float, length: float):
    """Bold downward arrow — heading points down at the QR code below it."""
    bottom_y = top_y - length
    c.setStrokeColor(GOLD)
    c.setLineWidth(3.5)
    c.line(x, top_y, x, bottom_y + 6 * mm)

    c.setFillColor(GOLD)
    head_w = 5 * mm
    p = c.beginPath()
    p.moveTo(x - head_w, bottom_y + 6 * mm)
    p.lineTo(x + head_w, bottom_y + 6 * mm)
    p.lineTo(x, bottom_y)
    p.close()
    c.drawPath(p, fill=1, stroke=0)


def generate_tracking_card_pdf(gstin: str = SAMPLE_GSTIN, consignor_name: str = SAMPLE_CONSIGNOR) -> bytes:
    url = f"{TRACKING_BASE_URL}{gstin}"
    buf = io.BytesIO()
    c = rl_canvas.Canvas(buf, pagesize=A4)

    c.setFillColor(WHITE)
    c.rect(0, 0, PAGE_W, PAGE_H, fill=1, stroke=0)

    watermark = _make_watermark(SS_LOGO_PATH)
    wm = 12 * cm
    c.drawImage(watermark, (PAGE_W - wm) / 2, (PAGE_H - wm) / 2, width=wm, height=wm,
                preserveAspectRatio=True, mask="auto")

    _draw_border(c)

    # ── Header: both logos side by side ──
    logo_h = 2.6 * cm
    spider_h = 4.2 * cm
    row_h = max(logo_h, spider_h)
    row_y = PAGE_H - INSET - 0.8 * cm - row_h

    ss_w = logo_h * SS_LOGO_ASPECT
    ss_x = INSET + 0.8 * cm
    ss_y = row_y + (row_h - logo_h) / 2
    c.drawImage(SS_LOGO_PATH, ss_x, ss_y, width=ss_w, height=logo_h, preserveAspectRatio=True, mask="auto")

    spider_w = spider_h * SPIDER_LOGO_ASPECT
    spider_x = PAGE_W - INSET - 0.8 * cm - spider_w
    spider_y = row_y + (row_h - spider_h) / 2
    c.drawImage(SPIDER_LOGO_PATH, spider_x, spider_y, width=spider_w, height=spider_h,
                preserveAspectRatio=True, mask="auto")

    logo_y = row_y
    partner_line = f"A JOINT DELIVERY PARTNERSHIP WITH {consignor_name.upper()}"
    max_w = PAGE_W - 2 * INSET - 2 * cm
    partner_size = 13
    while partner_size > 8 and c.stringWidth(partner_line, "Helvetica-Bold", partner_size) > max_w:
        partner_size -= 0.5
    c.setFillColor(NAVY)
    c.setFont("Helvetica-Bold", partner_size)
    c.drawCentredString(PAGE_W / 2, logo_y - 0.5 * cm, partner_line)

    rule_y = logo_y - 1.2 * cm
    c.setStrokeColor(GOLD)
    c.setLineWidth(1.3)
    c.line(INSET + 0.8 * cm, rule_y, PAGE_W - INSET - 0.8 * cm, rule_y)

    # ── Heading ──
    heading_y = rule_y - 2.3 * cm
    c.setFillColor(NAVY)
    c.setFont("Helvetica-Bold", 25)
    c.drawCentredString(PAGE_W / 2, heading_y, "TRACK YOUR CONSIGNMENT")
    c.setFillColor(GOLD)
    c.setFont("Helvetica-Bold", 25)
    c.drawCentredString(PAGE_W / 2, heading_y - 1.1 * cm, "HERE")

    c.setFillColor(DARK)
    c.setFont("Helvetica-Oblique", 11)
    c.drawCentredString(PAGE_W / 2, heading_y - 2.1 * cm, "Scan the QR code below to see live status, in one place")

    # ── Arrow pointing down at the QR code ──
    _draw_arrow(c, PAGE_W / 2, heading_y - 2.8 * cm, 2.4 * cm)

    # ── QR code, boxed ──
    qr_size = 7.5 * cm
    qr_x = PAGE_W / 2 - qr_size / 2
    qr_y = heading_y - 2.8 * cm - 2.4 * cm - qr_size - 0.6 * cm
    box_pad = 0.6 * cm
    c.setFillColor(GOLD_PALE)
    c.rect(qr_x - box_pad, qr_y - box_pad, qr_size + 2 * box_pad, qr_size + 2 * box_pad, fill=1, stroke=0)
    c.setStrokeColor(GOLD)
    c.setLineWidth(1.5)
    c.rect(qr_x - box_pad, qr_y - box_pad, qr_size + 2 * box_pad, qr_size + 2 * box_pad, fill=0, stroke=1)
    c.drawImage(_make_qr_image(url), qr_x, qr_y, width=qr_size, height=qr_size)

    # ── Tagline + URL + GSTIN ──
    tag_y = qr_y - box_pad - 1.1 * cm
    c.setFillColor(NAVY)
    c.setFont("Helvetica-BoldOblique", 15)
    c.drawCentredString(PAGE_W / 2, tag_y, "Stay Connected With Your Order")

    c.setFillColor(GOLD)
    c.setFont("Helvetica", 9.5)
    c.drawCentredString(PAGE_W / 2, tag_y - 0.8 * cm, url)

    c.setFillColor(GREY)
    c.setFont("Helvetica", 8.5)
    c.drawCentredString(PAGE_W / 2, tag_y - 1.4 * cm, f"GSTIN: {gstin}   |   {consignor_name}")

    # ── Footer ──
    footer_rule_y = INSET + 1.6 * cm
    c.setStrokeColor(GOLD)
    c.setLineWidth(1)
    c.line(INSET + 0.8 * cm, footer_rule_y, PAGE_W - INSET - 0.8 * cm, footer_rule_y)
    c.setFillColor(DARK)
    c.setFont("Helvetica-Bold", 9)
    c.drawCentredString(PAGE_W / 2, footer_rule_y - 0.6 * cm, COMPANY_NAME)
    c.setFillColor(GREY)
    c.setFont("Helvetica", 8)
    c.drawCentredString(PAGE_W / 2, footer_rule_y - 1.05 * cm, COMPANY_CONTACT)

    c.showPage()
    c.save()
    return buf.getvalue()


def save_tracking_card_pdf(gstin: str = SAMPLE_GSTIN, consignor_name: str = SAMPLE_CONSIGNOR, output_path: str = None) -> str:
    output_path = output_path or os.path.join(BASE_DIR, "tracking_card.pdf")
    with open(output_path, "wb") as f:
        f.write(generate_tracking_card_pdf(gstin, consignor_name))
    return output_path


if __name__ == "__main__":
    print(f"Tracking card saved to: {save_tracking_card_pdf()}")
