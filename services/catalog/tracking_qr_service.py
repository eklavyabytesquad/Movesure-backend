"""
Consignor tracking QR code (PNG / SVG)
======================================
Builds the QR that opens https://www.ssmovesecure.com/tracking/<GSTIN> for any
consignor in the `consignors` table. Find the consignor by GSTIN, id or name.

Styles:
  - "card" (default): a print-ready image with the SS Movesecure logo, a
    "SCAN TO TRACK" heading, the QR, the consignor name, GSTIN and link.
  - "plain": only the QR code, with a small white margin.
"""
import io
import os
import re

import qrcode
import qrcode.image.svg
from qrcode.constants import ERROR_CORRECT_H
from PIL import Image, ImageDraw, ImageFont
import reportlab

from services.supabase_client import get_supabase

TRACKING_BASE_URL = "https://www.ssmovesecure.com/tracking/"
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SS_LOGO_PATH = os.path.join(BASE_DIR, "assets", "ss-movesecure-logo-transparent.png")
_FONT_DIR = os.path.join(os.path.dirname(reportlab.__file__), "fonts")

NAVY = "#161A8A"
ORANGE = "#F26522"
GREY = "#4A4A4A"
GSTIN_RE = re.compile(r"^[0-9]{2}[A-Z0-9]{10}[0-9A-Z]{3}$")


class ConsignorNotFound(Exception):
    pass


def _font(size: int, bold: bool = False):
    return ImageFont.truetype(os.path.join(_FONT_DIR, "VeraBd.ttf" if bold else "Vera.ttf"), size)


def find_consignor(gstin: str = None, consignor_id: str = None, name: str = None) -> dict:
    """Return {id, company_name, gst_num} for one consignor, or raise ConsignorNotFound / ValueError."""
    sb = get_supabase()
    q = sb.table("consignors").select("id, company_name, gst_num, company_add")
    if consignor_id:
        rows = q.eq("id", consignor_id).limit(1).execute().data
    elif gstin:
        rows = q.ilike("gst_num", gstin.strip()).limit(1).execute().data
    elif name:
        rows = q.ilike("company_name", f"%{name.strip()}%").limit(10).execute().data
        rows = [r for r in rows if r.get("gst_num")]
        exact = [r for r in rows if r["company_name"].strip().upper() == name.strip().upper()]
        if exact:
            rows = exact
        elif len(rows) > 1:
            raise ValueError("More than one consignor matches '%s': %s. Send gstin or consignor_id."
                             % (name, ", ".join(f"{r['company_name']} ({r['gst_num']})" for r in rows)))
    else:
        raise ValueError("Send gstin, consignor_id or name")
    if not rows:
        raise ConsignorNotFound("Consignor not found")
    row = rows[0]
    gst = (row.get("gst_num") or "").strip().upper()
    if not GSTIN_RE.match(gst):
        raise ValueError(f"Consignor '{row['company_name']}' has no valid GSTIN, so it has no tracking page")
    return {"id": row["id"], "company_name": row["company_name"].strip(), "gst_num": gst,
            "company_add": row.get("company_add")}


def tracking_url(gstin: str) -> str:
    return TRACKING_BASE_URL + gstin.strip().upper()


def _qr(url: str) -> qrcode.QRCode:
    qr = qrcode.QRCode(error_correction=ERROR_CORRECT_H, box_size=10, border=2)
    qr.add_data(url)
    qr.make(fit=True)
    return qr


def _fit_text(draw, text, max_w, size, bold):
    while size > 12:
        f = _font(size, bold)
        if draw.textlength(text, font=f) <= max_w:
            return f
        size -= 2
    return _font(12, bold)


def _plain_png(url: str, size: int) -> Image.Image:
    img = _qr(url).make_image(fill_color=NAVY, back_color="white").convert("RGB")
    return img.resize((size, size), Image.NEAREST)


def _card_png(url: str, consignor: dict, size: int) -> Image.Image:
    W = size
    pad = W // 14
    qr_side = W - 2 * pad
    qr_img = _qr(url).make_image(fill_color=NAVY, back_color="white").convert("RGB").resize((qr_side, qr_side), Image.NEAREST)

    logo = None
    if os.path.exists(SS_LOGO_PATH):
        logo = Image.open(SS_LOGO_PATH).convert("RGBA")
        lw = int(W * 0.42)
        logo = logo.resize((lw, int(lw * logo.height / logo.width)), Image.LANCZOS)

    head_f = _font(int(W * 0.072), True)
    sub_f = _font(int(W * 0.034))
    probe = ImageDraw.Draw(Image.new("RGB", (1, 1)))
    name_f = _fit_text(probe, consignor["company_name"].upper(), W - 2 * pad, int(W * 0.056), True)
    gst_f = _font(int(W * 0.04), True)
    url_f = _fit_text(probe, url.replace("https://", ""), W - 2 * pad, int(W * 0.03), False)

    band = int(W * 0.012)
    gap = int(W * 0.03)
    logo_h = logo.height if logo else 0
    H = (band + pad // 2 + logo_h + gap + head_f.size + gap // 2 + sub_f.size + gap + qr_side + gap
         + name_f.size + gap // 2 + gst_f.size + gap // 2 + url_f.size + pad // 2 + band)

    img = Image.new("RGB", (W, H), "white")
    d = ImageDraw.Draw(img)
    d.rectangle([0, 0, W, band], fill=ORANGE)
    d.rectangle([0, H - band, W, H], fill=ORANGE)
    border = max(2, W // 200)
    d.rectangle([border, band + border, W - border - 1, H - band - border - 1], outline=NAVY, width=border)

    y = band + pad // 2
    if logo:
        img.paste(logo, ((W - logo.width) // 2, y), logo)
        y += logo_h + gap

    def centred(text, font, fill, yy):
        d.text(((W - d.textlength(text, font=font)) / 2, yy), text, font=font, fill=fill)

    centred("SCAN TO TRACK", head_f, NAVY, y); y += head_f.size + gap // 2
    centred("your consignment, any time", sub_f, ORANGE, y); y += sub_f.size + gap
    img.paste(qr_img, (pad, y)); y += qr_side + gap
    centred(consignor["company_name"].upper(), name_f, NAVY, y); y += name_f.size + gap // 2
    centred(f"GSTIN: {consignor['gst_num']}", gst_f, GREY, y); y += gst_f.size + gap // 2
    centred(url.replace("https://", ""), url_f, GREY, y)
    return img


def generate_tracking_qr(gstin: str = None, consignor_id: str = None, name: str = None,
                         style: str = "card", fmt: str = "png", size: int = 1000):
    """Return (bytes, media_type, consignor_dict)."""
    style, fmt = (style or "card").lower(), (fmt or "png").lower()
    if style not in ("card", "plain"):
        raise ValueError("style must be 'card' or 'plain'")
    if fmt not in ("png", "svg"):
        raise ValueError("format must be 'png' or 'svg'")
    size = max(300, min(int(size or 1000), 3000))

    consignor = find_consignor(gstin, consignor_id, name)
    url = tracking_url(consignor["gst_num"])
    consignor["tracking_url"] = url

    if fmt == "svg":  # SVG is always the plain QR (scales to any print size)
        buf = io.BytesIO()
        _qr(url).make_image(image_factory=qrcode.image.svg.SvgPathImage).save(buf)
        return buf.getvalue(), "image/svg+xml", consignor

    img = _card_png(url, consignor, size) if style == "card" else _plain_png(url, size)
    buf = io.BytesIO()
    img.save(buf, format="PNG", dpi=(300, 300))
    return buf.getvalue(), "image/png", consignor
