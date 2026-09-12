"""
Bilty "First Booking" WhatsApp Notification
==============================================
Fired once, in the background, right after a NEW bilty is saved (never on
an edit — see the `if not bilty_id` guard at the call site in
bilty_save_service.py). Sends the same 7-variable template to whichever of
consignor_number / consignee_number is a usable Indian mobile number, in
one webhook call (the API's own "multiple" batch shape).

Best-effort only: a failure here must never fail the bilty save, so every
error is caught and logged, never raised.
"""
import re
import requests
from datetime import datetime

TEMPLATE_URL = "https://campaignadmin.backendprod.com/webhook/template/67f16e7d-787d-4399-9c2a-c1dfddccec15/process"
REQUEST_TIMEOUT = 10  # seconds — runs in the background thread, never blocks the bilty save response

_MOBILE_RE = re.compile(r"^[6-9]\d{9}$")


def _normalize_mobile(raw: str) -> str | None:
    """10-digit Indian mobile -> '91XXXXXXXXXX'. None if not a plausible mobile number."""
    if not raw:
        return None
    digits = re.sub(r"\D", "", str(raw))
    if len(digits) == 12 and digits.startswith("91"):
        digits = digits[2:]
    if not _MOBILE_RE.match(digits):
        return None
    return f"91{digits}"


def _format_date(bilty_date: str) -> str:
    """'2026-09-12' -> '12-09-2026'; falls back to the raw value if it doesn't parse."""
    if not bilty_date:
        return ""
    try:
        return datetime.strptime(str(bilty_date)[:10], "%Y-%m-%d").strftime("%d-%m-%Y")
    except ValueError:
        return str(bilty_date)


def send_first_booking_notification(bilty_row: dict, from_city: dict | None, to_city: dict | None) -> dict:
    """
    bilty_row: the saved bilty (gr_no, bilty_date, consignor_name,
    consignee_name, consignor_number, consignee_number, no_of_pkg).
    from_city / to_city: {city_name, ...} dicts already resolved by save_bilty(), or None.
    """
    values = {
        "1": bilty_row.get("gr_no") or "",
        "2": _format_date(bilty_row.get("bilty_date")),
        "3": bilty_row.get("consignor_name") or "",
        "4": bilty_row.get("consignee_name") or "",
        "5": (from_city or {}).get("city_name") or "",
        "6": (to_city or {}).get("city_name") or "",
        "7": str(bilty_row.get("no_of_pkg") or ""),
    }

    receivers = []
    seen = set()
    for raw_number in (bilty_row.get("consignor_number"), bilty_row.get("consignee_number")):
        mobile = _normalize_mobile(raw_number)
        if mobile and mobile not in seen:
            seen.add(mobile)
            receivers.append(mobile)

    if not receivers:
        return {"status": "skipped", "message": "No valid consignor/consignee mobile number to notify"}

    payload = {
        "type": "multiple",
        "numbers": [{"receiver": r, "values": values} for r in receivers],
    }

    try:
        resp = requests.post(TEMPLATE_URL, json=payload, timeout=REQUEST_TIMEOUT)
        if resp.status_code >= 400:
            return {"status": "error", "message": f"Webhook returned {resp.status_code}: {resp.text[:300]}"}
        return {"status": "success", "message": f"Notified {len(receivers)} number(s)", "receivers": receivers}
    except Exception as e:
        return {"status": "error", "message": f"Failed to send first-booking notification: {e}"}
