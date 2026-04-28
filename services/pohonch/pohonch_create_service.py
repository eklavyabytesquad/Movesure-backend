"""
Pohonch Create Service
──────────────────────
Creates a single pohonch record from an explicit list of GR items.

Input (from API body):
  transport_name   str           required
  transport_gstin  str           optional
  challan_nos      list[str]     required  – one or more challan numbers
  gr_items         list[dict]    required  – [{gr_no, pohonch_bilty}]
  pohonch_prefix   str           optional  – custom prefix (e.g. "NIE")
                                             auto-derived from transport name if omitted
  created_by       str (uuid)    optional  – falls back to first active user

Auto-generates pohonch_number as <PREFIX><seq:04d>  e.g. NIE0001
Seq is determined by finding the highest existing number with the same prefix.

Bilty data is enriched from:
  bilty table           → freight_amount, wt, no_of_pkg, consignor/ee, date, etc.
  bilty_wise_kaat       → kaat, pf, dd_chrg, actual_kaat_rate
  cities                → destination name/code
"""
import re
from datetime import datetime, timezone
from collections import defaultdict
from services.supabase_client import get_supabase

# ── Common words to strip when auto-building prefix ──────────────────────────
_SKIP_WORDS = {"TRANSPORT", "CARRIER", "ROADLINES", "ROADWAYS", "LOGISTICS",
               "PVT", "LTD", "CO", "CORP", "CORPORATION", "EXPRESS",
               "SERVICE", "SERVICES", "AGENCY", "NEW", "AND", "THE"}


def _make_prefix(transport_name: str, hint: str | None = None) -> str:
    """
    Derive a 3-5 char prefix from transport name.
    E.g. "NEW INDIA EXPRESS TRANSPORT CO." → "NIE"
         "VISHWANATH EXPRESS TRANSPORT"    → "VET"
    hint overrides auto-derivation.
    """
    if hint:
        return re.sub(r"[^A-Z0-9]", "", hint.strip().upper())[:6] or "POH"

    words = re.sub(r"[^A-Z0-9 ]", "", transport_name.upper()).split()
    initials = "".join(w[0] for w in words if w not in _SKIP_WORDS and w)
    if not initials:
        # fallback: include all words
        initials = "".join(w[0] for w in words if w)
    return initials[:5] or "POH"


def _safe_float(val) -> float:
    try:
        return float(val or 0)
    except (TypeError, ValueError):
        return 0.0


def _chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def _next_seq(sb, prefix: str) -> int:
    """Return next integer seq for a given pohonch prefix."""
    res = (
        sb.table("pohonch")
        .select("pohonch_number")
        .ilike("pohonch_number", f"{prefix}%")
        .order("pohonch_number", desc=True)
        .limit(1)
        .execute()
    )
    rows = res.data or []
    if not rows:
        return 1
    last = rows[0]["pohonch_number"]
    m = re.search(r"(\d+)$", last)
    return (int(m.group(1)) + 1) if m else 1


def _get_default_user(sb) -> str:
    res = sb.table("users").select("id").eq("is_active", True).limit(1).execute()
    rows = res.data or []
    if not rows:
        raise RuntimeError("No active user found in users table")
    return rows[0]["id"]


# ─────────────────────────────────────────────────────────────────────────────
# Main service function
# ─────────────────────────────────────────────────────────────────────────────

def create_pohonch_from_gr_items(
    transport_name: str,
    transport_gstin: str | None,
    challan_nos: list[str],
    gr_items: list[dict],        # [{gr_no, pohonch_bilty}]
    pohonch_prefix: str | None,
    created_by: str | None,
) -> dict:
    """
    Creates one pohonch record from the supplied gr_items.
    Returns {"status": "success", "data": <inserted_pohonch_row>}
    or      {"status": "error",   "message": ..., "status_code": ...}
    """
    # ── Validate ──────────────────────────────────────────────────────────────
    if not transport_name:
        return {"status": "error", "message": "transport_name is required", "status_code": 400}
    if not challan_nos:
        return {"status": "error", "message": "challan_nos is required", "status_code": 400}
    if not gr_items:
        return {"status": "error", "message": "gr_items is required", "status_code": 400}

    # Validate each gr_item has gr_no
    for item in gr_items:
        if not item.get("gr_no"):
            return {"status": "error", "message": "Each gr_item must have gr_no", "status_code": 400}

    sb = get_supabase()

    # ── created_by fallback ────────────────────────────────────────────────────
    if not created_by:
        try:
            created_by = _get_default_user(sb)
        except RuntimeError as e:
            return {"status": "error", "message": str(e), "status_code": 500}

    # ── Build lookup maps ──────────────────────────────────────────────────────
    gr_nos = [item["gr_no"] for item in gr_items]
    pohonch_bilty_map = {item["gr_no"]: item.get("pohonch_bilty", "") for item in gr_items}

    # 1. bilty table (primary)
    bilty_map = {}
    for chunk in _chunks(gr_nos, 50):
        res = (
            sb.table("bilty")
            .select(
                "gr_no, bilty_date, wt, no_of_pkg, freight_amount, "
                "consignor_name, consignee_name, payment_mode, delivery_type, "
                "e_way_bill, to_city_id"
            )
            .in_("gr_no", chunk)
            .execute()
        )
        for b in res.data or []:
            gr = b.get("gr_no")
            if gr and gr not in bilty_map:
                bilty_map[gr] = b

    # 2. station_bilty_summary (fallback for GRs not in bilty table)
    missing_gr = [gr for gr in gr_nos if gr not in bilty_map]
    if missing_gr:
        for chunk in _chunks(missing_gr, 50):
            res = (
                sb.table("station_bilty_summary")
                .select(
                    "gr_no, created_at, weight, no_of_packets, amount, "
                    "consignor, consignee, payment_status, delivery_type, "
                    "e_way_bill, city_id"
                )
                .in_("gr_no", chunk)
                .execute()
            )
            for s in res.data or []:
                gr = s.get("gr_no")
                if gr and gr not in bilty_map:
                    # normalise to same shape as bilty table
                    bilty_map[gr] = {
                        "gr_no":          gr,
                        "bilty_date":     (s.get("created_at") or "")[:10],
                        "wt":             s.get("weight") or 0,
                        "no_of_pkg":      s.get("no_of_packets") or 0,
                        "freight_amount": s.get("amount") or 0,
                        "consignor_name": s.get("consignor") or "",
                        "consignee_name": s.get("consignee") or "",
                        "payment_mode":   s.get("payment_status") or "",
                        "delivery_type":  s.get("delivery_type") or "",
                        "e_way_bill":     s.get("e_way_bill") or "",
                        "to_city_id":     s.get("city_id") or "",
                        "_source":        "station",
                    }

    # bilty_wise_kaat  (match on gr_no AND one of our challan_nos)
    kaat_map = {}
    for chunk in _chunks(gr_nos, 50):
        res = (
            sb.table("bilty_wise_kaat")
            .select("gr_no, challan_no, kaat, pf, dd_chrg, actual_kaat_rate")
            .in_("gr_no", chunk)
            .execute()
        )
        for k in res.data or []:
            gr = k.get("gr_no")
            if gr and gr not in kaat_map:
                kaat_map[gr] = k

    # Cities
    city_ids = list({b["to_city_id"] for b in bilty_map.values() if b.get("to_city_id")})
    city_map = {}
    for chunk in _chunks(city_ids, 50):
        res = sb.table("cities").select("id, city_name, city_code").in_("id", chunk).execute()
        for c in res.data or []:
            city_map[c["id"]] = {"name": c.get("city_name", ""), "code": c.get("city_code", "")}

    # ── Build bilty_metadata & totals ─────────────────────────────────────────
    bilty_items = []
    total_amount = total_kaat = total_pf = total_dd = total_weight = 0.0
    total_packages = 0
    unmatched_gr = []

    for gr in gr_nos:
        b = bilty_map.get(gr, {})
        k = kaat_map.get(gr, {})
        city_info = city_map.get(b.get("to_city_id", ""), {})

        if not b:
            unmatched_gr.append(gr)  # not in bilty OR station_bilty_summary

        # Determine which challan this gr belongs to
        gr_challan = k.get("challan_no", "") or (challan_nos[0] if challan_nos else "")

        kaat_val = _safe_float(k.get("kaat"))
        pf_raw   = _safe_float(k.get("pf"))
        dd_val   = _safe_float(k.get("dd_chrg"))
        amt      = _safe_float(b.get("freight_amount"))
        wt       = _safe_float(b.get("wt"))
        pkgs     = int(b.get("no_of_pkg") or 0)
        rate     = _safe_float(k.get("actual_kaat_rate"))

        pf_val = round(amt - kaat_val, 2) if amt and kaat_val else round(pf_raw, 2)

        bilty_items.append({
            "gr_no":            gr,
            "date":             (b.get("bilty_date") or "")[:10],
            "challan_no":       gr_challan,
            "amount":           amt,
            "kaat":             kaat_val,
            "pf":               pf_val,
            "dd":               dd_val,
            "weight":           wt,
            "packages":         pkgs,
            "consignor":        b.get("consignor_name", ""),
            "consignee":        b.get("consignee_name", ""),
            "kaat_rate":        rate,
            "e_way_bill":       b.get("e_way_bill", "") or "",
            "destination":      city_info.get("name", ""),
            "destination_code": city_info.get("code", ""),
            "payment_mode":     b.get("payment_mode", ""),
            "delivery_type":    b.get("delivery_type", ""),
            "pohonch_bilty":    pohonch_bilty_map.get(gr, ""),
            "is_paid":          False,
        })

        total_amount   += amt
        total_kaat     += kaat_val
        total_pf       += pf_val
        total_dd       += dd_val
        total_weight   += wt
        total_packages += pkgs

    # ── Generate pohonch_number ────────────────────────────────────────────────
    prefix = _make_prefix(transport_name, pohonch_prefix)
    seq    = _next_seq(sb, prefix)
    pohonch_number = f"{prefix}{seq:04d}"

    # Check uniqueness
    dup = sb.table("pohonch").select("id").eq("pohonch_number", pohonch_number).execute()
    if dup.data:
        # Extremely unlikely but safe: increment until unique
        while dup.data:
            seq += 1
            pohonch_number = f"{prefix}{seq:04d}"
            dup = sb.table("pohonch").select("id").eq("pohonch_number", pohonch_number).execute()

    # ── Build & insert record ──────────────────────────────────────────────────
    record = {
        "pohonch_number":   pohonch_number,
        "transport_name":   transport_name.strip(),
        "transport_gstin":  (transport_gstin or "").strip().upper() or None,
        "challan_metadata": sorted(set(challan_nos)),
        "bilty_metadata":   bilty_items,
        "total_bilties":    len(bilty_items),
        "total_amount":     round(total_amount, 2),
        "total_kaat":       round(total_kaat, 2),
        "total_pf":         round(total_pf, 2),
        "total_dd":         round(total_dd, 2),
        "total_packages":   total_packages,
        "total_weight":     round(total_weight, 2),
        "is_signed":        False,
        "is_active":        True,
        "created_by":       created_by,
    }

    res = sb.table("pohonch").insert(record).execute()
    inserted = (res.data or [{}])[0]
    if not inserted:
        return {"status": "error", "message": "Insert failed — no data returned", "status_code": 500}

    response = {
        "status":         "success",
        "pohonch_number": pohonch_number,
        "data":           inserted,
    }
    if unmatched_gr:
        response["warnings"] = {
            "unmatched_gr_nos": unmatched_gr,
            "message": "These GR numbers were not found in bilty or station_bilty_summary tables. Amounts/weights set to 0.",
        }
    return response
