"""
Freight Rate List PDF — per-consignor, from SS Movesecure Logistics Pvt Ltd
=========================================================================
A one-page, professional letterhead-style rate list: bordered page, logo
+ company block, addressed to a named consignor, a state-by-state rate
table, and an authorized-signatory block at the end. Separate from the
station catalog on purpose — that one lists every station with no rates;
this one is a short quotation document you'd actually hand or email to a
customer.

The letterhead (logo, company name, address, border, watermark, footer)
is drawn directly on the canvas at fixed coordinates — not as flowables —
specifically so the company name and address can never collide regardless
of font metrics. Everything below it (title, addressee, rate table,
signature) is a normal platypus flowable story.

Customize the rates in RATES below — add/edit a state's numbers and every
rate list generated after that reflects it. Pass a different
consignor_name to generate_rate_list_pdf() per customer; it defaults to
the sample used to build this ("SPIDER METALS PRODUCTS PVT LTD").
"""
import io
import os
from datetime import datetime

from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.units import cm
from reportlab.lib.utils import ImageReader
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from PIL import Image as PILImage

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
LOGO_PATH = os.path.join(BASE_DIR, "assets", "ss-movesecure-logo-transparent.png")
LOGO_ASPECT = 1325 / 800  # width / height of ss-movesecure-logo-transparent.png
CONSIGNOR_LOGO_PATH = os.path.join(BASE_DIR, "assets", "spider-metal-transparent.png")

COMPANY_NAME = "SS MOVESECURE LOGISTICS PVT LTD"
COMPANY_ADDRESS = "SHIVA PETROL PUMP, GT ROAD, ALIGARH - 202001"
COMPANY_CONTACT = "+91-7902122230   |   +91-9695293140   |   ssmovesecure.com"

# ── Palette — matches the SS Movesecure logo's own colors ─────────────────
NAVY = colors.HexColor("#181888")        # logo's indigo-blue
GOLD = colors.HexColor("#F7641F")        # logo's orange
GOLD_LIGHT = colors.HexColor("#FF8F52")
GOLD_PALE = colors.HexColor("#FDE7DA")
DARK = colors.HexColor("#1A1A1A")
GREY = colors.HexColor("#4A4A4A")        # logo's wheel grey

SAMPLE_CONSIGNOR = "SPIDER METALS PRODUCTS PVT LTD"

# ── Customize here ──────────────────────────────────────────────────────────
# Leave a field as None for a state you haven't quoted yet — it prints as
# "On Request" instead of a made-up number.
RATES = {
    "UTTAR PRADESH": {"godown": "Rs. 6 / kg", "door_above_200kg": "Rs. 7 / kg", "door_below_200kg": "Rs. 7.5 / kg"},
    "UTTARAKHAND":   {"godown": "Rs. 6 / kg", "door_above_200kg": "Rs. 7 / kg", "door_below_200kg": "Rs. 7.5 / kg"},
    "DELHI":         {"godown": "Rs. 3 / kg", "door_above_200kg": "Rs. 4 / kg", "door_below_200kg": "Rs. 4 / kg"},
    "BIHAR":         {"godown": "Rs. 9 / kg", "door_above_200kg": "Rs. 11 / kg", "door_below_200kg": "Rs. 11 / kg"},
    "JHARKHAND":     {"godown": "Rs. 10 / kg", "door_above_200kg": "Rs. 11.5 / kg", "door_below_200kg": "Rs. 11.5 / kg"},
    "ODISHA":        {"godown": "Rs. 10 / kg", "door_above_200kg": "Rs. 11.5 / kg", "door_below_200kg": "Rs. 11.5 / kg"},
    "ASSAM":         {"godown": "Rs. 12 / kg", "door_above_200kg": "Rs. 13.5 / kg", "door_below_200kg": "Rs. 13.5 / kg"},
    "MIZORAM":       {"godown": "Rs. 19 / kg", "door_above_200kg": None, "door_below_200kg": None},
    "MANIPUR":       {"godown": "Rs. 20 / kg", "door_above_200kg": None, "door_below_200kg": None},
}

# ── Letterhead layout (all canvas-drawn, fixed coordinates) ────────────────
PAGE_W, PAGE_H = A4
BORDER_MARGIN = 0.9 * cm     # outer border inset from the page edge
BORDER_GAP = 0.12 * cm       # gap between the two border lines
LOGO_H = 2.5 * cm
LOGO_W = LOGO_H * LOGO_ASPECT
LETTERHEAD_TOP_GAP = 0.5 * cm  # gap from inner border to the logo

_WATERMARK = None


def _make_watermark(path: str, alpha: float = 0.06) -> ImageReader:
    img = PILImage.open(path).convert("RGBA")
    r, g, b, a = img.split()
    a = a.point(lambda x: int(x * alpha))
    img.putalpha(a)
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    return ImageReader(buf)


def _cell(value):
    return value if value else "On Request"


def _draw_page_frame(canvas, _doc):
    """Border, watermark, logo + company letterhead block, and footer — all at fixed positions so nothing can overlap."""
    global _WATERMARK
    canvas.saveState()

    # Watermark, behind everything
    if _WATERMARK is None:
        _WATERMARK = _make_watermark(LOGO_PATH)
    wm = 11 * cm
    canvas.drawImage(_WATERMARK, (PAGE_W - wm) / 2, (PAGE_H - wm) / 2,
                      width=wm, height=wm, preserveAspectRatio=True, mask="auto")

    # Double-line border frame
    canvas.setStrokeColor(GOLD)
    canvas.setLineWidth(1.4)
    canvas.rect(BORDER_MARGIN, BORDER_MARGIN, PAGE_W - 2 * BORDER_MARGIN, PAGE_H - 2 * BORDER_MARGIN, fill=0, stroke=1)
    inset = BORDER_MARGIN + BORDER_GAP
    canvas.setStrokeColor(NAVY)
    canvas.setLineWidth(0.5)
    canvas.rect(inset, inset, PAGE_W - 2 * inset, PAGE_H - 2 * inset, fill=0, stroke=1)

    # Logo
    logo_x = inset + 0.5 * cm
    logo_y = PAGE_H - inset - LETTERHEAD_TOP_GAP - LOGO_H
    canvas.drawImage(LOGO_PATH, logo_x, logo_y, width=LOGO_W, height=LOGO_H,
                      preserveAspectRatio=True, mask="auto")

    # Company name / address / contact — each on its own explicit y, so the
    # gap between lines can never shrink to the point of overlapping.
    text_x = logo_x + LOGO_W + 0.5 * cm
    name_y = logo_y + LOGO_H - 0.55 * cm

    date_text = datetime.now().strftime("%d %B %Y")
    date_x = PAGE_W - inset - 0.5 * cm
    date_font_size = 9
    date_w = canvas.stringWidth(date_text, "Helvetica", date_font_size)
    name_max_w = (date_x - date_w - 0.4 * cm) - text_x  # never run into the date, whatever the name's length

    name_font_size = 16
    while name_font_size > 9 and canvas.stringWidth(COMPANY_NAME, "Helvetica-Bold", name_font_size) > name_max_w:
        name_font_size -= 0.5

    canvas.setFillColor(NAVY)
    canvas.setFont("Helvetica-Bold", name_font_size)
    canvas.drawString(text_x, name_y, COMPANY_NAME)

    canvas.setFillColor(GREY)
    canvas.setFont("Helvetica", 8.5)
    canvas.drawString(text_x, name_y - 0.55 * cm, COMPANY_ADDRESS)
    canvas.drawString(text_x, name_y - 0.95 * cm, COMPANY_CONTACT)

    # Date, top-right of the letterhead
    canvas.setFillColor(DARK)
    canvas.setFont("Helvetica", date_font_size)
    canvas.drawRightString(date_x, name_y, date_text)

    # Gold rule closing the letterhead
    rule_y = logo_y - 0.4 * cm
    canvas.setStrokeColor(GOLD)
    canvas.setLineWidth(1.3)
    canvas.line(inset + 0.4 * cm, rule_y, PAGE_W - inset - 0.4 * cm, rule_y)
    canvas.setStrokeColor(GOLD_LIGHT)
    canvas.setLineWidth(0.4)
    canvas.line(inset + 0.4 * cm, rule_y - 0.1 * cm, PAGE_W - inset - 0.4 * cm, rule_y - 0.1 * cm)

    # Footer
    footer_rule_y = inset + 1.3 * cm
    canvas.setStrokeColor(GOLD)
    canvas.setLineWidth(0.8)
    canvas.line(inset + 0.4 * cm, footer_rule_y, PAGE_W - inset - 0.4 * cm, footer_rule_y)
    canvas.setFont("Helvetica", 7.5)
    canvas.setFillColor(GREY)
    canvas.drawCentredString(PAGE_W / 2, inset + 0.9 * cm, f"{COMPANY_NAME}  ·  {COMPANY_CONTACT}")

    canvas.restoreState()

    # Content-start y, in points from the bottom — used to size topMargin
    return rule_y


def generate_rate_list_pdf(consignor_name: str = SAMPLE_CONSIGNOR, rates: dict = None) -> bytes:
    rates = rates or RATES
    buf = io.BytesIO()

    # Compute exactly where the letterhead ends so the story starts with a
    # clean gap below it, never guessed/approximated.
    logo_y = PAGE_H - (BORDER_MARGIN + BORDER_GAP) - LETTERHEAD_TOP_GAP - LOGO_H
    rule_y = logo_y - 0.4 * cm
    top_margin = PAGE_H - (rule_y - 0.9 * cm)

    doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=2.1 * cm, rightMargin=2.1 * cm,
        topMargin=top_margin, bottomMargin=2.8 * cm,
    )
    styles = getSampleStyleSheet()

    title_style = ParagraphStyle(
        "Title", parent=styles["Normal"], fontSize=17, fontName="Helvetica-Bold",
        textColor=NAVY, alignment=TA_CENTER, leading=17,
    )
    to_label_style = ParagraphStyle(
        "ToLabel", parent=styles["Normal"], fontSize=9.5, fontName="Helvetica",
        textColor=GREY, spaceAfter=2,
    )
    consignor_style = ParagraphStyle(
        "Consignor", parent=styles["Normal"], fontSize=13, fontName="Helvetica-Bold",
        textColor=DARK, spaceAfter=10,
    )
    body_style = ParagraphStyle(
        "Body", parent=styles["Normal"], fontSize=10, fontName="Helvetica",
        textColor=DARK, alignment=TA_LEFT, leading=15, spaceAfter=16,
    )
    sign_label_style = ParagraphStyle(
        "SignLabel", parent=styles["Normal"], fontSize=10.5, fontName="Helvetica-Bold",
        textColor=NAVY, alignment=TA_RIGHT,
    )
    sign_role_style = ParagraphStyle(
        "SignRole", parent=styles["Normal"], fontSize=9, fontName="Helvetica",
        textColor=DARK, alignment=TA_RIGHT,
    )

    story = []

    # ── Title banner + addressee ──
    title_band = Table([[Paragraph("FREIGHT RATE LIST", title_style)]], colWidths=[doc.width])
    title_band.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), GOLD_PALE),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 10),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
        ("BOX", (0, 0), (-1, -1), 0.8, GOLD),
    ]))
    story.append(title_band)
    story.append(Spacer(1, 0.5 * cm))

    addressee = [Paragraph("To,", to_label_style), Paragraph(f"M/s {consignor_name}", consignor_style)]
    if os.path.exists(CONSIGNOR_LOGO_PATH):
        # 597x335 source — keep its own aspect ratio, just bigger than before
        consignor_logo = Image(CONSIGNOR_LOGO_PATH, width=4.6 * cm, height=4.6 * cm * (335 / 597))
        consignor_logo.hAlign = "RIGHT"
        addressee_row = Table([[addressee, consignor_logo]], colWidths=[doc.width - 4.8 * cm, 4.8 * cm])
        addressee_row.setStyle(TableStyle([
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("ALIGN", (1, 0), (1, 0), "RIGHT"),
            ("LEFTPADDING", (0, 0), (-1, -1), 0),
            ("RIGHTPADDING", (0, 0), (-1, -1), 0),
        ]))
        story.append(addressee_row)
    else:
        story.extend(addressee)

    story.append(Paragraph(
        f"Please find below our freight rates for consignments booked through "
        f"<b>{COMPANY_NAME}</b>, applicable until revised:",
        body_style,
    ))

    # ── Rate table ──
    header_row = ["State", "Godown Delivery", "Door Delivery\n(Above 200 kg)", "Door Delivery\n(Below 200 kg)"]
    table_rows = [header_row]
    for state, r in rates.items():
        table_rows.append([state.title(), _cell(r.get("godown")), _cell(r.get("door_above_200kg")), _cell(r.get("door_below_200kg"))])

    rate_table = Table(table_rows, colWidths=[4.5 * cm, 4 * cm, 4.25 * cm, 4.25 * cm])
    rate_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), NAVY),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, 0), 9.5),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [GOLD_PALE, colors.white]),
        ("FONTNAME", (0, 1), (0, -1), "Helvetica-Bold"),
        ("FONTNAME", (1, 1), (-1, -1), "Helvetica"),
        ("FONTSIZE", (0, 1), (-1, -1), 10),
        ("GRID", (0, 0), (-1, -1), 0.6, GOLD),
        ("BOX", (0, 0), (-1, -1), 1.2, NAVY),
        ("TOPPADDING", (0, 0), (-1, -1), 8),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
    ]))
    story.append(rate_table)

    # ── Signature block ──
    story.append(Spacer(1, 0.8 * cm))
    sign_table = Table(
        [[Paragraph(COMPANY_NAME, sign_label_style)],
         [Spacer(1, 1.6 * cm)],
         [Paragraph("Authorized Signatory", sign_role_style)]],
        colWidths=[doc.width],
    )
    sign_table.setStyle(TableStyle([("ALIGN", (0, 0), (-1, -1), "RIGHT")]))
    story.append(sign_table)

    doc.build(story, onFirstPage=_draw_page_frame, onLaterPages=_draw_page_frame)
    return buf.getvalue()


def save_rate_list_pdf(consignor_name: str = SAMPLE_CONSIGNOR, rates: dict = None, output_path: str = None) -> str:
    output_path = output_path or os.path.join(BASE_DIR, "rate_list.pdf")
    pdf_bytes = generate_rate_list_pdf(consignor_name, rates)
    with open(output_path, "wb") as f:
        f.write(pdf_bytes)
    return output_path


if __name__ == "__main__":
    path = save_rate_list_pdf()
    print(f"Rate list saved to: {path}")
