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

        kaat_val     = _safe_float(k.get("kaat"))
        pf_raw       = _safe_float(k.get("pf"))
        dd_val       = _safe_float(k.get("dd_chrg"))
        amt          = _safe_float(b.get("freight_amount"))
        wt           = _safe_float(b.get("wt"))
        pkgs         = int(b.get("no_of_pkg") or 0)
        rate         = _safe_float(k.get("actual_kaat_rate"))
        payment_mode = str(b.get("payment_mode") or "").strip().lower()
        if payment_mode == "paid":
            pf_val = round(-kaat_val, 2)                             # -kaat (0 when kaat=0)
        elif kaat_val:
            pf_val = round(amt - kaat_val - dd_val, 2)              # to-pay: freight-kaat-dd
        else:
            pf_val = round(pf_raw, 2)                               # no kaat stored — keep as-is

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


def _fetch_city_map(sb, city_ids: list) -> dict:
    city_map = {}
    for chunk in _chunks(city_ids, 50):
        res = sb.table("cities").select("id, city_name, city_code").in_("id", chunk).execute()
        for c in res.data or []:
            city_map[c["id"]] = {"name": c.get("city_name", ""), "code": c.get("city_code", "")}
    return city_map


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


# ─────────────────────────────────────────────────────────────────────────────
# 2. Update individual GR fields inside bilty_metadata
# ─────────────────────────────────────────────────────────────────────────────

def update_gr_fields(
    pohonch_id: str,
    gr_no: str,
    updates: dict,          # any subset of the bilty_metadata entry fields
    user_id: str | None = None,
    force: bool = False,
) -> dict:
    """
    Patch one or more fields on a single GR entry inside pohonch.bilty_metadata,
    then recalculate all pohonch totals.

    Patchable fields per GR entry:
      destination      str   – display name  (also updates destination_code if city found)
      destination_code str   – short code
      kaat             float
      pf               float
      dd               float
      kaat_rate        float
      weight           float
      packages         int
      amount           float
      pohonch_bilty    str   – receipt/bilty number printed on pohonch
      e_way_bill       str
      is_paid          bool
      payment_mode     str
      delivery_type    str

    If `destination` is updated, the service looks up the city in the cities
    table and also sets destination_code automatically.
    """
    if not gr_no:
        return {"status": "error", "message": "gr_no is required", "status_code": 400}
    if not updates:
        return {"status": "error", "message": "No fields to update provided", "status_code": 400}

    sb = get_supabase()

    res = (
        sb.table("pohonch")
        .select("id, pohonch_number, is_signed, bilty_metadata")
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
            "message": "Pohonch is signed — unsign first or pass force=true",
            "status_code": 409,
        }

    bilty_meta: list[dict] = row.get("bilty_metadata") or []
    gr_index = next((i for i, b in enumerate(bilty_meta) if str(b.get("gr_no", "")) == str(gr_no)), None)
    if gr_index is None:
        return {"status": "error", "message": f"GR '{gr_no}' not found in this pohonch", "status_code": 404}

    entry = dict(bilty_meta[gr_index])

    # Allowed fields — prevents accidental overwrite of gr_no / date / consignor
    ALLOWED = {
        "destination", "destination_code", "kaat", "pf", "dd", "kaat_rate",
        "weight", "packages", "amount", "pohonch_bilty",
        "e_way_bill", "is_paid", "payment_mode", "delivery_type",
    }
    for field, value in updates.items():
        if field not in ALLOWED:
            continue
        # Numeric coercion
        if field in ("kaat", "pf", "dd", "kaat_rate", "weight", "amount"):
            try:
                value = float(value)
            except (TypeError, ValueError):
                value = 0.0
        if field == "packages":
            try:
                value = int(value)
            except (TypeError, ValueError):
                value = 0
        entry[field] = value

    # If destination name changed, auto-resolve destination_code from cities table
    if "destination" in updates and updates["destination"]:
        city_res = (
            sb.table("cities")
            .select("city_name, city_code")
            .ilike("city_name", f"%{updates['destination'].strip()}%")
            .limit(1)
            .execute()
        )
        if city_res.data:
            c = city_res.data[0]
            entry["destination"]      = c.get("city_name", updates["destination"])
            entry["destination_code"] = c.get("city_code", entry.get("destination_code", ""))

    bilty_meta[gr_index] = entry
    totals = _recalculate_totals(bilty_meta)

    update_payload = {
        **totals,
        "bilty_metadata": bilty_meta,
        "updated_at": _now(),
    }
    if user_id:
        update_payload["updated_by"] = user_id

    upd = sb.table("pohonch").update(update_payload).eq("id", pohonch_id).execute()
    if not upd.data:
        return {"status": "error", "message": "Update failed", "status_code": 500}

    return {
        "status": "success",
        "message": f"GR '{gr_no}' updated in pohonch {row['pohonch_number']}",
        "gr_no": gr_no,
        "updated_entry": entry,
        "new_totals": totals,
        "data": upd.data[0],
    }


# ─────────────────────────────────────────────────────────────────────────────
# 3. Full recalculation — re-fetch live data from all source tables
# ─────────────────────────────────────────────────────────────────────────────

def recalculate_pohonch(
    pohonch_id: str,
    user_id: str | None = None,
    force: bool = False,
) -> dict:
    """
    Re-fetch live data for every GR in bilty_metadata from:
      bilty / station_bilty_summary  →  weight, freight_amount, payment_mode
      bilty_wise_kaat                →  kaat, pf, dd_chrg, actual_kaat_rate
      cities                         →  destination name + code

    Rebuilds each bilty_metadata entry with fresh values, preserves
    fields that don't come from DB (pohonch_bilty, is_paid),
    then recalculates totals and persists.

    Returns diff summary (old vs new totals) + updated pohonch.
    """
    sb = get_supabase()

    res = (
        sb.table("pohonch")
        .select("id, pohonch_number, is_signed, bilty_metadata, challan_metadata, "
                "total_kaat, total_pf, total_amount, total_bilties")
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
            "message": "Pohonch is signed — unsign first or pass force=true",
            "status_code": 409,
        }

    bilty_meta: list[dict] = row.get("bilty_metadata") or []
    if not bilty_meta:
        return {"status": "error", "message": "Pohonch has no bilty entries", "status_code": 400}

    gr_nos = [str(b.get("gr_no", "")) for b in bilty_meta if b.get("gr_no")]

    # ── 1. Bilty table ────────────────────────────────────────────────────────
    bilty_map: dict[str, dict] = {}
    for chunk in _chunks(gr_nos, 50):
        r = (
            sb.table("bilty")
            .select("gr_no, bilty_date, wt, no_of_pkg, freight_amount, "
                    "consignor_name, consignee_name, payment_mode, delivery_type, "
                    "e_way_bill, to_city_id")
            .in_("gr_no", chunk)
            .execute()
        )
        for b in r.data or []:
            gr = b.get("gr_no")
            if gr and gr not in bilty_map:
                bilty_map[gr] = b

    # ── 2. station_bilty_summary fallback ────────────────────────────────────
    missing = [g for g in gr_nos if g not in bilty_map]
    if missing:
        for chunk in _chunks(missing, 50):
            r = (
                sb.table("station_bilty_summary")
                .select("gr_no, created_at, weight, no_of_packets, amount, "
                        "consignor, consignee, payment_status, delivery_type, "
                        "e_way_bill, city_id")
                .in_("gr_no", chunk)
                .execute()
            )
            for s in r.data or []:
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

    # ── 3. bilty_wise_kaat ────────────────────────────────────────────────────
    kaat_map: dict[str, dict] = {}
    for chunk in _chunks(gr_nos, 50):
        r = (
            sb.table("bilty_wise_kaat")
            .select("gr_no, kaat, pf, dd_chrg, actual_kaat_rate, challan_no")
            .in_("gr_no", chunk)
            .execute()
        )
        for k in r.data or []:
            gr = k.get("gr_no")
            if gr:
                kaat_map[gr] = k

    # ── 4. Cities ─────────────────────────────────────────────────────────────
    city_ids = list({b["to_city_id"] for b in bilty_map.values() if b.get("to_city_id")})
    city_map = _fetch_city_map(sb, city_ids)

    # ── 5. Rebuild bilty_metadata with fresh values ───────────────────────────
    old_totals = {
        "total_kaat":    _safe_float(row.get("total_kaat")),
        "total_pf":      _safe_float(row.get("total_pf")),
        "total_amount":  _safe_float(row.get("total_amount")),
        "total_bilties": row.get("total_bilties", 0),
    }

    new_meta = []
    not_found = []
    for old_entry in bilty_meta:
        gr = str(old_entry.get("gr_no", ""))
        b = bilty_map.get(gr)
        k = kaat_map.get(gr, {})

        if not b:
            # Keep old entry as-is — bilty was deleted or moved
            new_meta.append(old_entry)
            not_found.append(gr)
            continue

        city_info    = city_map.get(b.get("to_city_id", ""), {})
        amt          = _safe_float(b.get("freight_amount"))
        wt           = _safe_float(b.get("wt"))
        pkgs         = int(b.get("no_of_pkg") or 0)
        kaat_val     = _safe_float(k.get("kaat"))
        pf_raw       = _safe_float(k.get("pf"))
        dd_val       = _safe_float(k.get("dd_chrg"))
        rate         = _safe_float(k.get("actual_kaat_rate"))
        payment_mode = str(b.get("payment_mode") or "").strip().lower()
        if payment_mode == "paid":
            pf_val = round(-kaat_val, 2)                             # -kaat (0 when kaat=0)
        elif kaat_val:
            pf_val = round(amt - kaat_val - dd_val, 2)              # to-pay: freight-kaat-dd
        else:
            pf_val = round(pf_raw, 2)                               # no kaat stored — keep as-is

        new_entry = {
            # Preserve manual / non-DB fields
            "gr_no":            gr,
            "pohonch_bilty":    old_entry.get("pohonch_bilty", ""),
            "is_paid":          old_entry.get("is_paid", False),
            "challan_no":       k.get("challan_no") or old_entry.get("challan_no", ""),
            # Refreshed from DB
            "date":             (b.get("bilty_date") or "")[:10],
            "amount":           amt,
            "kaat":             kaat_val,
            "pf":               pf_val,
            "dd":               dd_val,
            "weight":           wt,
            "packages":         pkgs,
            "kaat_rate":        rate,
            "consignor":        b.get("consignor_name", ""),
            "consignee":        b.get("consignee_name", ""),
            "e_way_bill":       b.get("e_way_bill", "") or "",
            "destination":      city_info.get("name", old_entry.get("destination", "")),
            "destination_code": city_info.get("code", old_entry.get("destination_code", "")),
            "payment_mode":     b.get("payment_mode", ""),
            "delivery_type":    b.get("delivery_type", ""),
        }
        new_meta.append(new_entry)

        # Also fix bilty_wise_kaat.pf if it differs from the correct value
        if kaat_val and abs(pf_val - pf_raw) > 0.01:
            sb.table("bilty_wise_kaat").update({"pf": pf_val}).eq("gr_no", gr).execute()

    new_totals = _recalculate_totals(new_meta)

    update_payload = {
        **new_totals,
        "bilty_metadata": new_meta,
        "updated_at": _now(),
    }
    if user_id:
        update_payload["updated_by"] = user_id

    upd = sb.table("pohonch").update(update_payload).eq("id", pohonch_id).execute()
    if not upd.data:
        return {"status": "error", "message": "Update failed", "status_code": 500}

    return {
        "status": "success",
        "message": f"Pohonch {row['pohonch_number']} recalculated — bilty_wise_kaat.pf corrected where needed",
        "pohonch_number": row["pohonch_number"],
        "old_totals": old_totals,
        "new_totals": new_totals,
        "diff": {
            "kaat":   round(new_totals["total_kaat"]   - old_totals["total_kaat"], 2),
            "pf":     round(new_totals["total_pf"]     - old_totals["total_pf"], 2),
            "amount": round(new_totals["total_amount"] - old_totals["total_amount"], 2),
        },
        "not_refreshed_gr_nos": not_found,
        "data": upd.data[0],
    }


# ─────────────────────────────────────────────────────────────────────────────
# 4. Bulk recalculate — re-fetch live data for multiple pohonch at once
# ─────────────────────────────────────────────────────────────────────────────

def bulk_recalculate_pohonch(
    pohonch_ids: list[str] | None = None,
    pohonch_numbers: list[str] | None = None,
    transport_gstin: str | None = None,
    transport_name: str | None = None,
    user_id: str | None = None,
    force: bool = False,
) -> dict:
    """
    Recalculate multiple pohonch in one call by re-fetching live data from
    bilty, bilty_wise_kaat, station_bilty_summary and cities tables.

    Selection — supply ONE of:
      pohonch_ids      list[uuid]  — recalculate specific pohonch by UUID
      pohonch_numbers  list[str]   — recalculate by pohonch_number (e.g. ["NIE0001","NIE0002"])
      transport_gstin  str         — recalculate ALL active pohonch for this transport GSTIN
      transport_name   str         — recalculate ALL active pohonch for this transport name

    Returns a summary per-pohonch (old vs new totals + diff) and aggregate counts.
    Signed pohonch are skipped unless force=True.
    """
    sb = get_supabase()

    # ── Fetch target pohonch list ─────────────────────────────────────────────
    q = sb.table("pohonch").select(
        "id, pohonch_number, is_signed, transport_name, transport_gstin, "
        "bilty_metadata, total_kaat, total_pf, total_amount, total_bilties"
    ).eq("is_active", True)

    if pohonch_ids:
        q = q.in_("id", pohonch_ids)
    elif pohonch_numbers:
        q = q.in_("pohonch_number", pohonch_numbers)
    elif transport_gstin:
        q = q.ilike("transport_gstin", f"%{transport_gstin.strip()}%")
    elif transport_name:
        q = q.ilike("transport_name", f"%{transport_name.strip()}%")
    else:
        return {
            "status": "error",
            "message": "Provide one of: pohonch_ids, pohonch_numbers, transport_gstin, or transport_name",
            "status_code": 400,
        }

    pohonch_rows = q.execute().data or []
    if not pohonch_rows:
        return {"status": "success", "message": "No pohonch found matching criteria",
                "processed": 0, "skipped": 0, "results": []}

    # ── Collect ALL gr_nos from ALL pohonch in one pass ───────────────────────
    all_gr_nos: list[str] = []
    for row in pohonch_rows:
        meta = row.get("bilty_metadata") or []
        all_gr_nos.extend(str(b.get("gr_no", "")) for b in meta if b.get("gr_no"))
    all_gr_nos = list(dict.fromkeys(all_gr_nos))   # deduplicate, preserve order

    # ── Bulk-fetch bilty data ─────────────────────────────────────────────────
    bilty_map: dict[str, dict] = {}
    for chunk in _chunks(all_gr_nos, 100):
        r = (
            sb.table("bilty")
            .select("gr_no, bilty_date, wt, no_of_pkg, freight_amount, "
                    "consignor_name, consignee_name, payment_mode, delivery_type, "
                    "e_way_bill, to_city_id")
            .in_("gr_no", chunk)
            .execute()
        )
        for b in r.data or []:
            gr = b.get("gr_no")
            if gr and gr not in bilty_map:
                bilty_map[gr] = b

    missing_grs = [g for g in all_gr_nos if g not in bilty_map]
    if missing_grs:
        for chunk in _chunks(missing_grs, 100):
            r = (
                sb.table("station_bilty_summary")
                .select("gr_no, created_at, weight, no_of_packets, amount, "
                        "consignor, consignee, payment_status, delivery_type, "
                        "e_way_bill, city_id")
                .in_("gr_no", chunk)
                .execute()
            )
            for s in r.data or []:
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

    # ── Bulk-fetch kaat data ──────────────────────────────────────────────────
    kaat_map: dict[str, dict] = {}
    for chunk in _chunks(all_gr_nos, 100):
        r = (
            sb.table("bilty_wise_kaat")
            .select("gr_no, kaat, pf, dd_chrg, actual_kaat_rate, challan_no")
            .in_("gr_no", chunk)
            .execute()
        )
        for k in r.data or []:
            gr = k.get("gr_no")
            if gr:
                kaat_map[gr] = k

    # ── Bulk-fetch city data ──────────────────────────────────────────────────
    city_ids = list({b["to_city_id"] for b in bilty_map.values() if b.get("to_city_id")})
    city_map = _fetch_city_map(sb, city_ids)

    # ── Process each pohonch ──────────────────────────────────────────────────
    results = []
    processed = 0
    skipped_signed = 0
    skipped_empty = 0

    for row in pohonch_rows:
        pno = row["pohonch_number"]

        if row.get("is_signed") and not force:
            results.append({
                "pohonch_number": pno,
                "status": "skipped",
                "reason": "signed — pass force=true to override",
            })
            skipped_signed += 1
            continue

        bilty_meta: list[dict] = row.get("bilty_metadata") or []
        if not bilty_meta:
            results.append({"pohonch_number": pno, "status": "skipped", "reason": "no bilty entries"})
            skipped_empty += 1
            continue

        old_totals = {
            "total_kaat":   _safe_float(row.get("total_kaat")),
            "total_pf":     _safe_float(row.get("total_pf")),
            "total_amount": _safe_float(row.get("total_amount")),
        }

        new_meta = []
        not_found_grs = []
        for old_entry in bilty_meta:
            gr = str(old_entry.get("gr_no", ""))
            b = bilty_map.get(gr)
            k = kaat_map.get(gr, {})

            if not b:
                new_meta.append(old_entry)
                not_found_grs.append(gr)
                continue

            city_info    = city_map.get(b.get("to_city_id", ""), {})
            amt          = _safe_float(b.get("freight_amount"))
            wt           = _safe_float(b.get("wt"))
            pkgs         = int(b.get("no_of_pkg") or 0)
            kaat_val     = _safe_float(k.get("kaat"))
            pf_raw       = _safe_float(k.get("pf"))
            dd_val       = _safe_float(k.get("dd_chrg"))
            rate         = _safe_float(k.get("actual_kaat_rate"))
            payment_mode = str(b.get("payment_mode") or "").strip().lower()
            if kaat_val:
                pf_val = round(-kaat_val, 2) if payment_mode == "paid" else round(amt - kaat_val - dd_val, 2)
            else:
                pf_val = round(pf_raw, 2)

            new_meta.append({
                "gr_no":            gr,
                "pohonch_bilty":    old_entry.get("pohonch_bilty", ""),
                "is_paid":          old_entry.get("is_paid", False),
                "challan_no":       k.get("challan_no") or old_entry.get("challan_no", ""),
                "date":             (b.get("bilty_date") or "")[:10],
                "amount":           amt,
                "kaat":             kaat_val,
                "pf":               pf_val,
                "dd":               dd_val,
                "weight":           wt,
                "packages":         pkgs,
                "kaat_rate":        rate,
                "consignor":        b.get("consignor_name", ""),
                "consignee":        b.get("consignee_name", ""),
                "e_way_bill":       b.get("e_way_bill", "") or "",
                "destination":      city_info.get("name", old_entry.get("destination", "")),
                "destination_code": city_info.get("code", old_entry.get("destination_code", "")),
                "payment_mode":     b.get("payment_mode", ""),
                "delivery_type":    b.get("delivery_type", ""),
            })

            # Fix bilty_wise_kaat.pf if the stored value differs from the correct one
            if kaat_val and abs(pf_val - pf_raw) > 0.01:
                sb.table("bilty_wise_kaat").update({"pf": pf_val}).eq("gr_no", gr).execute()

        new_totals = _recalculate_totals(new_meta)
        update_payload = {
            **new_totals,
            "bilty_metadata": new_meta,
            "updated_at": _now(),
        }
        if user_id:
            update_payload["updated_by"] = user_id

        sb.table("pohonch").update(update_payload).eq("id", row["id"]).execute()
        processed += 1

        results.append({
            "pohonch_number": pno,
            "status": "updated",
            "old_totals": old_totals,
            "new_totals": {
                "total_kaat":   new_totals["total_kaat"],
                "total_pf":     new_totals["total_pf"],
                "total_amount": new_totals["total_amount"],
            },
            "diff": {
                "kaat":   round(new_totals["total_kaat"]   - old_totals["total_kaat"],   2),
                "pf":     round(new_totals["total_pf"]     - old_totals["total_pf"],     2),
                "amount": round(new_totals["total_amount"] - old_totals["total_amount"], 2),
            },
            "not_refreshed_gr_nos": not_found_grs,
        })

    return {
        "status":        "success",
        "total_found":   len(pohonch_rows),
        "processed":     processed,
        "skipped_signed": skipped_signed,
        "skipped_empty":  skipped_empty,
        "results":       results,
    }
