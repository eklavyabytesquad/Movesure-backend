"""
Bilty "First Booking" WhatsApp Notification
==============================================
Fired once, in the background, right after a NEW bilty is saved (never on
an edit — see the `if not bilty_id` guard at the call site in
bilty_save_service.py). Sends the same 8-variable "first booking" template to whichever of
consignor_number / consignee_number is a usable Indian mobile number, in
one webhook call (the API's own "multiple" batch shape).

Best-effort only: a failure here must never fail the bilty save, so every
error is caught and logged, never raised.
"""
import re
import requests
from datetime import datetime

TEMPLATE_URL = "https://campaignadmin.backendprod.com/webhook/template/67f16e7d-787d-4399-9c2a-c1dfddccec15/process"
COUNTRY_CODE = "+91"
REQUEST_TIMEOUT = 10  # seconds — runs in the background thread, never blocks the bilty save response

_MOBILE_RE = re.compile(r"^[6-9]\d{9}$")


def _normalize_mobile(raw: str) -> str | None:
    """Indian mobile -> 10 digits (no country code). None if not a plausible mobile number."""
    if not raw:
        return None
    digits = re.sub(r"\D", "", str(raw))
    if len(digits) == 12 and digits.startswith("91"):
        digits = digits[2:]
    if not _MOBILE_RE.match(digits):
        return None
    return digits


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
    # The template rejects an EMPTY value with 422 "Invalid values for keys: N" — so a 0
    # package count must go as "0" (not ""), and any other missing field as "-".
    pkg = bilty_row.get("no_of_pkg")

    def _v(x) -> str:
        s = str(x).strip() if x is not None else ""
        return s or "-"

    values = {
        "1": _v(bilty_row.get("gr_no")),
        "2": _v(_format_date(bilty_row.get("bilty_date"))),
        "3": _v(bilty_row.get("consignor_name")),
        "4": _v(bilty_row.get("consignee_name")),
        "5": _v((from_city or {}).get("city_name")),
        "6": _v((to_city or {}).get("city_name")),
        "7": _v(pkg if pkg is not None else "0"),
        "8": _v(bilty_row.get("gr_no")),
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
        "numbers": [{"countryCode": COUNTRY_CODE, "receiver": r, "values": values} for r in receivers],
    }

    try:
        resp = requests.post(TEMPLATE_URL, json=payload, timeout=REQUEST_TIMEOUT)
        if resp.status_code >= 400:
            return {"status": "error", "message": f"Webhook returned {resp.status_code}: {resp.text[:300]}"}
        return {"status": "success", "message": f"Notified {len(receivers)} number(s)", "receivers": receivers,
                "response": resp.text[:300]}
    except Exception as e:
        return {"status": "error", "message": f"Failed to send first-booking notification: {e}"}


def send_booking_message_for_gr(gr_no: str, numbers: list | None = None) -> dict:
    """
    For the frontend: send the 'first booking' WhatsApp for a saved bilty by GR number.
    The message is built server-side from the bilty itself, so it can never hit the
    template's 422s (empty / null / numeric values, missing keys, bad receiver).
    numbers (optional): send to these 10-digit mobiles instead of the bilty's own
    consignor / consignee numbers — e.g. a number typed on the screen.
    Returns which receivers were used and which numbers were skipped as invalid.
    """
    from services.supabase_client import get_supabase
    gr = (gr_no or "").strip().upper()
    if not gr:
        return {"status": "error", "message": "gr_no is required", "status_code": 400}
    sb = get_supabase()
    rows = sb.table("bilty").select("gr_no, bilty_date, consignor_name, consignee_name, consignor_number, "
                                    "consignee_number, no_of_pkg, from_city_id, to_city_id") \
        .eq("gr_no", gr).eq("is_active", True).limit(1).execute().data
    if not rows:
        return {"status": "error", "message": f"Bilty {gr} not found", "status_code": 404}
    b = rows[0]
    ids = [i for i in (b.get("from_city_id"), b.get("to_city_id")) if i]
    cities = {c["id"]: c for c in (sb.table("cities").select("id, city_name").in_("id", ids).execute().data or [])} if ids else {}
    skipped = []
    if numbers:
        raw = list(numbers)
        b = {**b, "consignor_number": None, "consignee_number": None}
        valid = [m for m in (_normalize_mobile(n) for n in raw) if m]
        skipped = [n for n in raw if not _normalize_mobile(n)]
        if not valid:
            return {"status": "error", "message": "None of the numbers is a valid 10-digit mobile", "skipped": skipped,
                    "status_code": 400}
        # send to each given number (reuse the two receiver slots, then the rest one by one)
        results = []
        for m in valid:
            results.append(send_first_booking_notification({**b, "consignor_number": m}, cities.get(b.get("from_city_id")),
                                                           cities.get(b.get("to_city_id"))))
        ok = [r for r in results if r["status"] == "success"]
        return {"status": "success" if ok else "error", "gr_no": gr,
                "message": f"Sent to {len(ok)} of {len(valid)} number(s)", "receivers": valid, "skipped": skipped,
                "errors": [r["message"] for r in results if r["status"] != "success"],
                **({} if ok else {"status_code": 502})}
    for label, n in (("consignor", b.get("consignor_number")), ("consignee", b.get("consignee_number"))):
        if not _normalize_mobile(n):
            skipped.append({"party": label, "number": n, "reason": "missing" if not n else "not a valid 10-digit mobile"})
    res = send_first_booking_notification(b, cities.get(b.get("from_city_id")), cities.get(b.get("to_city_id")))
    res.update({"gr_no": gr, "skipped": skipped})
    if res["status"] == "skipped":
        res.update({"status": "error", "status_code": 400})
    elif res["status"] == "error":
        res["status_code"] = 502
    return res
