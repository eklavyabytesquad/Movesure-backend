"""
Pohonch Edit Service
─────────────────────
PATCH /api/pohonch/{pohonch_id}/edit

Supports three independent (combinable) operations in one call:
  1. add_gr_items   – enrich new GRs and append to bilty_metadata
  2. remove_gr_nos  – remove existing GRs from bilty_metadata
  3. new_pohonch_number – rename the pohonch_number (uniqueness checked)
  4. challan_nos    – replace the challan_metadata array

All totals are recalculated from the final bilty_metadata list.
A signed pohonch is blocked from edits unless force=True is passed.
"""
import re
from datetime import datetime, timezone
from services.supabase_client import get_supabase
from services.pohonch.pohonch_create_service import _chunks, _safe_float


def _now():
    return datetime.now(timezone.utc).isoformat()


def _enrich_gr_items(sb, gr_items: list[dict], challan_nos: list[str]) -> tuple[list[dict], list[str]]:
    """
    Given a list of {gr_no, challan_no, pohonch_bilty} dicts,
    return (enriched_bilty_items, unmatched_gr_nos).
    Mirrors the create-service lookup logic.
    """
    gr_nos = [item["gr_no"] for item in gr_items]
    pohonch_bilty_map = {item["gr_no"]: item.get("pohonch_bilty", "") for item in gr_items}
    challan_override  = {item["gr_no"]: item.get("challan_no", "") for item in gr_items}

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

    # 2. station_bilty_summary (fallback)
    missing = [g for g in gr_nos if g not in bilty_map]
    if missing:
        for chunk in _chunks(missing, 50):
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
                    }

    # 3. kaat
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

    # 4. cities
    city_ids = list({b["to_city_id"] for b in bilty_map.values() if b.get("to_city_id")})
    city_map = {}
    for chunk in _chunks(city_ids, 50):
        res = sb.table("cities").select("id, city_name, city_code").in_("id", chunk).execute()
        for c in res.data or []:
            city_map[c["id"]] = {"name": c.get("city_name", ""), "code": c.get("city_code", "")}

    # Build items
    enriched = []
    unmatched = []
    for gr in gr_nos:
        b = bilty_map.get(gr, {})
        k = kaat_map.get(gr, {})
        city_info = city_map.get(b.get("to_city_id", ""), {})

        if not b:
            unmatched.append(gr)

        kaat_val = _safe_float(k.get("kaat"))
        pf_raw   = _safe_float(k.get("pf"))
        dd_val   = _safe_float(k.get("dd_chrg"))
        amt      = _safe_float(b.get("freight_amount"))
        wt       = _safe_float(b.get("wt"))
        pkgs     = int(b.get("no_of_pkg") or 0)
        rate     = _safe_float(k.get("actual_kaat_rate"))
        pf_val   = round(amt - kaat_val, 2) if amt and kaat_val else round(pf_raw, 2)

        gr_challan = (
            challan_override.get(gr)
            or k.get("challan_no", "")
            or (challan_nos[0] if challan_nos else "")
        )

        enriched.append({
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

    return enriched, unmatched


def _recalculate_totals(bilty_items: list[dict]) -> dict:
    total_amount = total_kaat = total_pf = total_dd = total_weight = 0.0
    total_packages = 0
    for item in bilty_items:
        total_amount   += _safe_float(item.get("amount"))
        total_kaat     += _safe_float(item.get("kaat"))
        total_pf       += _safe_float(item.get("pf"))
        total_dd       += _safe_float(item.get("dd"))
        total_weight   += _safe_float(item.get("weight"))
        total_packages += int(item.get("packages") or 0)
    return {
        "total_bilties":  len(bilty_items),
        "total_amount":   round(total_amount, 2),
        "total_kaat":     round(total_kaat, 2),
        "total_pf":       round(total_pf, 2),
        "total_dd":       round(total_dd, 2),
        "total_packages": total_packages,
        "total_weight":   round(total_weight, 2),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Main edit function
# ─────────────────────────────────────────────────────────────────────────────

def edit_pohonch(
    pohonch_id: str,
    add_gr_items: list[dict] | None = None,     # [{gr_no, challan_no?, pohonch_bilty?}]
    remove_gr_nos: list[str] | None = None,      # ["22789", "22790"]
    new_pohonch_number: str | None = None,       # rename e.g. "NIE0001"
    challan_nos: list[str] | None = None,        # replace challan_metadata
    user_id: str | None = None,
    force: bool = False,                         # bypass signed-check
) -> dict:
    """
    Edit an existing pohonch — add/remove bilties and/or rename.
    All supplied operations are applied in a single DB update.
    """
    sb = get_supabase()

    # ── Fetch existing ────────────────────────────────────────────────────────
    res = (
        sb.table("pohonch")
        .select(
            "id, pohonch_number, is_signed, bilty_metadata, challan_metadata, "
            "transport_name, transport_gstin"
        )
        .eq("id", pohonch_id)
        .single()
        .execute()
    )
    if not res.data:
        return {"status": "error", "message": "Pohonch not found", "status_code": 404}

    row = res.data

    if row.get("is_signed") and not force:
        return {
            "status": "error",
            "message": "Pohonch is signed — unsign it before editing, or pass force=true",
            "status_code": 409,
        }

    # ── Current bilty list ────────────────────────────────────────────────────
    current_bilties: list[dict] = row.get("bilty_metadata") or []
    if isinstance(current_bilties, str):
        import json
        current_bilties = json.loads(current_bilties)

    # ── Remove ────────────────────────────────────────────────────────────────
    removed_count = 0
    if remove_gr_nos:
        remove_set = {str(g).strip() for g in remove_gr_nos}
        before = len(current_bilties)
        current_bilties = [b for b in current_bilties if str(b.get("gr_no", "")) not in remove_set]
        removed_count = before - len(current_bilties)

    # ── Add ───────────────────────────────────────────────────────────────────
    added_count = 0
    warnings = []
    if add_gr_items:
        # Prevent duplicates
        existing_grs = {str(b.get("gr_no", "")) for b in current_bilties}
        new_items = [i for i in add_gr_items if str(i.get("gr_no", "")) not in existing_grs]
        if new_items:
            eff_challan = challan_nos or row.get("challan_metadata") or []
            if isinstance(eff_challan, str):
                eff_challan = [eff_challan]
            enriched, unmatched = _enrich_gr_items(sb, new_items, eff_challan)
            current_bilties.extend(enriched)
            added_count = len(enriched)
            if unmatched:
                warnings.append(
                    f"GRs not found in bilty/station_bilty_summary (amounts=0): {', '.join(unmatched)}"
                )

    # ── Rename pohonch_number ─────────────────────────────────────────────────
    final_pohonch_number = row["pohonch_number"]
    if new_pohonch_number:
        new_num = new_pohonch_number.strip().upper()
        if new_num != row["pohonch_number"]:
            dup = sb.table("pohonch").select("id").eq("pohonch_number", new_num).execute()
            if dup.data:
                return {
                    "status": "error",
                    "message": f"pohonch_number '{new_num}' is already taken",
                    "status_code": 409,
                }
            final_pohonch_number = new_num

    # ── Build update payload ──────────────────────────────────────────────────
    totals = _recalculate_totals(current_bilties)

    payload = {
        **totals,
        "bilty_metadata":  current_bilties,
        "pohonch_number":  final_pohonch_number,
        "updated_at":      _now(),
    }
    if user_id:
        payload["updated_by"] = user_id
    if challan_nos is not None:
        payload["challan_metadata"] = sorted(set(challan_nos))

    # ── Persist ───────────────────────────────────────────────────────────────
    update_res = (
        sb.table("pohonch")
        .update(payload)
        .eq("id", pohonch_id)
        .execute()
    )
    updated = (update_res.data or [{}])[0]
    if not updated:
        return {"status": "error", "message": "Update failed", "status_code": 500}

    response = {
        "status":    "success",
        "message":   f"Pohonch updated: +{added_count} GRs added, -{removed_count} GRs removed",
        "pohonch_number": final_pohonch_number,
        "data":      updated,
    }
    if warnings:
        response["warnings"] = warnings
    return response
