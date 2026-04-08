"""
Bilty Save Service
Server-side validated bilty creation/update.
Resolves city names from IDs on the server so the frontend
never has to rely on network during PDF generation.
Optimized with parallel DB calls for maximum speed.
"""
from concurrent.futures import ThreadPoolExecutor
from time import sleep
from services.supabase_client import get_supabase

# Shared thread pool for background tasks (rate save, bill book update)
_bg_pool = ThreadPoolExecutor(max_workers=4)


def _extract_gr_number(gr_no: str, prefix: str, postfix: str, digits: int) -> int | None:
    """Extract the raw numeric part from a formatted GR string.
    e.g. gr_no='A08044', prefix='A', digits=5 -> 8044
    """
    try:
        prefix_len = len(prefix or "")
        postfix_len = len(postfix or "")
        num_part = gr_no[prefix_len:] if not postfix_len else gr_no[prefix_len:-postfix_len]
        return int(num_part)
    except (ValueError, IndexError):
        return None


def _format_gr(prefix: str, number: int, digits: int, postfix: str = None) -> str:
    """Format a GR number string, e.g. prefix='A', number=8044, digits=5 -> 'A08044'."""
    return f"{prefix or ''}{str(number).zfill(digits)}{postfix or ''}"


def _get_highest_used_gr(sb, prefix: str, digits: int, postfix: str, from_num: int, to_num: int) -> int | None:
    """
    Safety checker: find the highest GR number (raw numeric part) that
    exists as a saved active bilty within this bill book's GR range.
    Returns None if no bilties exist yet.
    Uses string ordering on zero-padded GR numbers (safe because padding is fixed).
    """
    prefix_str = prefix or ""
    postfix_str = postfix or ""
    gr_start = f"{prefix_str}{str(from_num).zfill(digits)}{postfix_str}"
    gr_end = f"{prefix_str}{str(to_num).zfill(digits)}{postfix_str}"

    resp = (
        sb.table("bilty")
        .select("gr_no")
        .gte("gr_no", gr_start)
        .lte("gr_no", gr_end)
        .eq("is_active", True)
        .order("gr_no", desc=True)
        .limit(1)
        .execute()
    )
    if not resp.data:
        return None
    return _extract_gr_number(resp.data[0]["gr_no"], prefix, postfix, digits)


def _resolve_city(sb, city_id: str) -> dict | None:
    """Look up a city by ID. Returns {id, city_name, city_code} or None."""
    if not city_id:
        return None
    resp = (
        sb.table("cities")
        .select("id, city_name, city_code")
        .eq("id", city_id)
        .single()
        .execute()
    )
    return resp.data


def _ensure_party(sb, table: str, name: str, gst: str = None, number: str = None) -> str | None:
    """
    Auto-create a consignor/consignee if it doesn't exist.
    Returns the party ID.
    """
    if not name or not name.strip():
        return None

    name_clean = name.strip()

    # Check if already exists (exact name match, case-insensitive)
    existing = (
        sb.table(table)
        .select("id")
        .ilike("company_name", name_clean)
        .limit(1)
        .execute()
    )
    if existing.data:
        return existing.data[0]["id"]

    # Create new
    row = {"company_name": name_clean, "company_add": ""}
    if gst:
        row["gst_num"] = gst.strip()
    if number:
        row["number"] = number.strip()

    inserted = sb.table(table).insert(row).execute()
    if inserted.data:
        return inserted.data[0]["id"]
    return None


def _auto_save_rate(sb, branch_id, city_id, consignor_name, rate_value):
    """Save/update rate for this branch+city+consignor combination."""
    if not branch_id or not city_id or not rate_value or float(rate_value) <= 0:
        return

    # Find consignor ID
    consignor_id = None
    if consignor_name:
        resp = (
            sb.table("consignors")
            .select("id")
            .ilike("company_name", consignor_name.strip())
            .limit(1)
            .execute()
        )
        if resp.data:
            consignor_id = resp.data[0]["id"]

    if not consignor_id:
        return

    # Check existing rate
    existing = (
        sb.table("rates")
        .select("id, rate")
        .eq("branch_id", branch_id)
        .eq("city_id", city_id)
        .eq("consignor_id", consignor_id)
        .limit(1)
        .execute()
    )

    if existing.data:
        if float(existing.data[0]["rate"]) != float(rate_value):
            sb.table("rates").update({"rate": float(rate_value)}).eq(
                "id", existing.data[0]["id"]
            ).execute()
    else:
        sb.table("rates").insert(
            {
                "branch_id": branch_id,
                "city_id": city_id,
                "consignor_id": consignor_id,
                "rate": float(rate_value),
                "is_default": False,
            }
        ).execute()


def save_bilty(data: dict) -> dict:
    """
    Validate and save a bilty with server-side city resolution.
    
    Optimized flow:
    1. City resolution + GR dup check run in PARALLEL (~1 DB round-trip)
    2. Insert/Update bilty (~1 DB round-trip)
    3. Rate save + bill book update run in BACKGROUND (non-blocking)
    Total blocking time: ~2 DB round-trips (~100-200ms)
    """
    try:
        sb = get_supabase()

        bilty_id = data.get("bilty_id")  # Present for updates
        branch_id = data.get("branch_id")
        from_city_id = data.get("from_city_id")
        to_city_id = data.get("to_city_id")
        gr_no = data.get("gr_no")
        saving_option = data.get("saving_option", "SAVE")

        # === VALIDATION ===
        if not branch_id:
            return {"status": "error", "message": "branch_id is required", "status_code": 400}
        if not gr_no:
            return {"status": "error", "message": "gr_no is required", "status_code": 400}

        # === PARALLEL: City resolution + GR dup check + Invoice dup check ===
        from_city = None
        to_city = None
        dup_exists = False
        invoice_dup = None  # will hold gr_no of existing bilty if duplicate

        consignor_name_raw = data.get("consignor_name")
        invoice_no_raw = data.get("invoice_no")

        with ThreadPoolExecutor(max_workers=4) as pool:
            futures = {}
            if from_city_id:
                futures[pool.submit(_resolve_city, sb, from_city_id)] = "from_city"
            if to_city_id:
                futures[pool.submit(_resolve_city, sb, to_city_id)] = "to_city"
            if not bilty_id:
                def check_dup():
                    r = (
                        sb.table("bilty")
                        .select("id")
                        .eq("gr_no", gr_no)
                        .eq("branch_id", branch_id)
                        .eq("is_active", True)
                        .limit(1)
                        .execute()
                    )
                    return bool(r.data)
                futures[pool.submit(check_dup)] = "dup_check"

            # Invoice dedup: same consignor + same invoice_no should not exist
            if not bilty_id and consignor_name_raw and invoice_no_raw:
                def check_invoice_dup():
                    r = (
                        sb.table("bilty")
                        .select("gr_no")
                        .ilike("consignor_name", consignor_name_raw.strip())
                        .eq("invoice_no", invoice_no_raw.strip())
                        .eq("is_active", True)
                        .limit(1)
                        .execute()
                    )
                    if r.data:
                        return r.data[0]["gr_no"]
                    return None
                futures[pool.submit(check_invoice_dup)] = "invoice_dup"

            for future in futures:
                key = futures[future]
                if key == "from_city":
                    from_city = future.result()
                elif key == "to_city":
                    to_city = future.result()
                elif key == "dup_check":
                    dup_exists = future.result()
                elif key == "invoice_dup":
                    invoice_dup = future.result()

        if from_city_id and not from_city:
            return {
                "status": "error",
                "message": f"Invalid from_city_id: {from_city_id}. City not found.",
                "status_code": 400,
            }
        if to_city_id and not to_city:
            return {
                "status": "error",
                "message": f"Invalid to_city_id: {to_city_id}. City not found.",
                "status_code": 400,
            }
        if dup_exists:
            return {
                "status": "error",
                "message": f"GR number {gr_no} already exists for this branch",
                "status_code": 409,
            }
        if invoice_dup:
            print(
                f"\U0001f6ab Invoice dedup: blocked save of GR {gr_no} "
                f"(consignor '{consignor_name_raw}' + invoice '{invoice_no_raw}' "
                f"already used in GR {invoice_dup})"
            )
            return {
                "status": "error",
                "message": f"Duplicate invoice: consignor '{consignor_name_raw}' with invoice '{invoice_no_raw}' already exists in GR {invoice_dup}",
                "status_code": 409,
            }

        # === EXTRACT PARTY NAMES (needed for bilty row + background tasks) ===
        consignor_name = consignor_name_raw
        consignee_name = data.get("consignee_name")

        # === BUILD BILTY RECORD ===
        bilty_row = {
            "branch_id": branch_id,
            "staff_id": data.get("staff_id"),
            "gr_no": gr_no,
            "bilty_date": data.get("bilty_date"),
            "from_city_id": from_city_id,
            "to_city_id": to_city_id,
            "delivery_type": data.get("delivery_type"),
            "consignor_name": consignor_name,
            "consignor_gst": data.get("consignor_gst"),
            "consignor_number": data.get("consignor_number"),
            "consignee_name": consignee_name,
            "consignee_gst": data.get("consignee_gst"),
            "consignee_number": data.get("consignee_number"),
            "transport_name": data.get("transport_name"),
            "transport_gst": data.get("transport_gst"),
            "transport_number": data.get("transport_number"),
            "transport_id": data.get("transport_id"),
            "payment_mode": data.get("payment_mode"),
            "contain": data.get("contain"),
            "invoice_no": data.get("invoice_no"),
            "invoice_value": data.get("invoice_value"),
            "invoice_date": data.get("invoice_date"),
            "e_way_bill": data.get("e_way_bill"),
            "document_number": data.get("document_number"),
            "no_of_pkg": data.get("no_of_pkg"),
            "wt": data.get("wt"),
            "rate": data.get("rate"),
            "labour_rate": data.get("labour_rate"),
            "pvt_marks": data.get("pvt_marks"),
            "freight_amount": data.get("freight_amount"),
            "labour_charge": data.get("labour_charge"),
            "bill_charge": data.get("bill_charge"),
            "toll_charge": data.get("toll_charge"),
            "dd_charge": data.get("dd_charge"),
            "other_charge": data.get("other_charge"),
            "pf_charge": data.get("pf_charge"),
            "total": data.get("total"),
            "remark": data.get("remark"),
            "saving_option": saving_option,
        }

        # === SAVE ===
        if bilty_id:
            # UPDATE existing bilty
            # For updates, keep None values so fields can be CLEARED (e.g. removing e_way_bill).
            # Only strip keys that were not sent in the request at all.
            nullable_fields = [
                "e_way_bill", "invoice_no", "invoice_value", "invoice_date",
                "document_number", "remark", "pvt_marks", "transport_id",
                "consignor_gst", "consignor_number", "consignee_gst", "consignee_number",
                "transport_gst", "transport_number",
            ]
            update_row = {}
            for k, v in bilty_row.items():
                if v is not None:
                    update_row[k] = v
                elif k in nullable_fields and k in data:
                    # Field was explicitly sent as null/empty → set to None to clear it
                    update_row[k] = None
            result = (
                sb.table("bilty")
                .update(update_row)
                .eq("id", bilty_id)
                .execute()
            )
        else:
            # INSERT new bilty — remove None values to let DB defaults apply
            bilty_row = {k: v for k, v in bilty_row.items() if v is not None}
            result = sb.table("bilty").insert(bilty_row).execute()

        if not result.data:
            return {
                "status": "error",
                "message": "Failed to save bilty",
                "status_code": 500,
            }

        saved_bilty = result.data[0]

        # === SAFETY CHECK & ADVANCE current_number (server-owned, blocking) ===
        # The backend is the SOLE authority on current_number.
        # After inserting the bilty, we query the DB for the actual highest
        # used GR number and set current_number = highest + 1.  This prevents:
        #   - Double increment (save + complete_reservation both advancing)
        #   - Drift (current_number jumping ahead by more than 1)
        #   - Stale state (frontend sending a wrong value)
        #   - Same-as-last (current_number equalling an already-used GR)
        # Rule: current_number MUST always be exactly last_used_gr + 1.
        new_current_number = None
        bill_book_id = data.get("bill_book_id")
        if not bilty_id and bill_book_id:
            try:
                bb = sb.table("bill_books").select(
                    "id, prefix, postfix, digits, current_number, from_number, to_number, auto_continue"
                ).eq("id", bill_book_id).single().execute().data
                if bb:
                    highest = _get_highest_used_gr(
                        sb, bb["prefix"], bb["digits"], bb.get("postfix"),
                        bb["from_number"], bb["to_number"]
                    )
                    if highest is not None:
                        correct = highest + 1
                    else:
                        correct = bb["from_number"]

                    # Wraparound if auto_continue is enabled
                    if correct > bb["to_number"]:
                        if bb.get("auto_continue"):
                            correct = bb["from_number"]
                        else:
                            correct = bb["to_number"]

                    new_current_number = correct
                    old_current = bb["current_number"]
                    if new_current_number != old_current:
                        sb.table("bill_books").update(
                            {"current_number": new_current_number}
                        ).eq("id", bill_book_id).execute()
                        print(
                            f"\U0001f6e1 Safety check: corrected current_number "
                            f"{old_current} → {new_current_number} "
                            f"(highest used GR = {highest})"
                        )
            except Exception as bb_err:
                print(f"Bill book safety check error: {bb_err}")

        # NOTE: next_gr_no intentionally NOT returned in the response.
        # The frontend must call GET /api/bilty/gr/next-available instead.
        # Returning it here caused the frontend to auto-fill the next GR
        # and accidentally trigger a second save.

        # === POST-SAVE: Run in BACKGROUND (non-blocking, response returns immediately) ===
        def _post_save():
            for attempt in range(2):
                try:
                    _sb = get_supabase()
                    # Auto-create consignor/consignee if new
                    _ensure_party(_sb, "consignors", consignor_name, data.get("consignor_gst"), data.get("consignor_number"))
                    _ensure_party(_sb, "consignees", consignee_name, data.get("consignee_gst"), data.get("consignee_number"))
                    # Auto-save rate
                    if saving_option == "SAVE" and data.get("rate"):
                        _auto_save_rate(_sb, branch_id, to_city_id, consignor_name, data.get("rate"))
                    return  # success — exit retry loop
                except OSError as e:
                    if attempt == 0:
                        print(f"Background post-save: connection error, retrying in 1s... ({e})")
                        sleep(1)
                    else:
                        print(f"Background post-save error (after retry): {e}")
                except Exception as bg_err:
                    print(f"Background post-save error: {bg_err}")
                    return  # non-retriable — exit

        _bg_pool.submit(_post_save)

        # === RETURN RESPONSE WITH RESOLVED CITY DATA ===
        # Frontend gets city names back from server for PDF generation.
        # next_gr_no is NOT included — frontend calls /next-available.
        return {
            "status": "success",
            "message": "Bilty saved successfully",
            "data": {
                "bilty": saved_bilty,
                "from_city": from_city,
                "to_city": to_city,
            },
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to save bilty: {str(e)}",
            "status_code": 500,
        }


def get_bilty_with_cities(bilty_id: str) -> dict:
    """
    Fetch a bilty with resolved city names.
    Use this for PDF generation / reprinting — guaranteed correct city data.
    """
    try:
        sb = get_supabase()

        bilty_resp = (
            sb.table("bilty")
            .select("*")
            .eq("id", bilty_id)
            .single()
            .execute()
        )

        if not bilty_resp.data:
            return {"status": "error", "message": "Bilty not found", "status_code": 404}

        bilty = bilty_resp.data
        from_city = _resolve_city(sb, bilty.get("from_city_id"))
        to_city = _resolve_city(sb, bilty.get("to_city_id"))

        return {
            "status": "success",
            "data": {
                "bilty": bilty,
                "from_city": from_city,
                "to_city": to_city,
            },
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to fetch bilty: {str(e)}",
            "status_code": 500,
        }
