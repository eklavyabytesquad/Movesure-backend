"""
PARTY LEDGERS + CUSTOM BILTY LEDGERS
=====================================
Two kinds of ledger built from bilties (see migrations/024):

  party  — tied to one consignor or consignee (ANIL LOCKS PVT.LTD, SHINE PRODUCTS, …).
           A consignor ledger counts that party's PAID bilties; a consignee ledger
           counts its TO-PAY bilties. Spellings vary a lot in the data
           ("ANIL LOCKS PVT.LTD" / "Anil Locks Pvt.Ltd" / "ANIL LOCK PVT.LTD."), so a
           party matches on its name + match_names (compared ignoring case, spaces
           and punctuation) or its GSTIN. New regular bilties are added on booking
           (on_bilty_saved, called from bilty_save_service); manual/station bilties
           come in through the Sync button or POST /party-ledgers/auto-add.
  custom — any name (MRM RAMKOLA); search any regular or manual bilty and add it.

Every BILLED bilty in a ledger is ONE Sales voucher:
    Dr <party ledger>  (a bill, reference_no = GR no)  /  Cr 'Freight Income'
A paid bilty booked with total = 0 is "to be billed": it sits in the ledger with
no voucher (nothing owed yet) until its amount is entered — bill_bilties() —
after which it is owed like any other (migrations/025 records amount_source).
so balances, statements, bill-wise payments, Day Book and edit/cancel all work
exactly like every other ledger. Payments: POST /party-ledgers/{id}/collect.
A bilty can be in only one ledger at a time (unique index), so nothing is
counted twice. Bulk adds insert vouchers/bills/entries in a few batched calls.
"""
import re
from datetime import datetime, timezone
from services.supabase_client import get_supabase
from services.ledger.ledger_service import create_ledger, get_ledger, get_ledger_balance, get_ledger_statement
from services.ledger.bill_reference_service import list_bills
from services.ledger.voucher_service import (
    VOUCHER_PREFIX, source_supported, cancel_voucher, update_voucher, save_party_voucher, last_voucher_seq,
)
from services.ledger.ledger_helpers import (
    resolve_payment_ledger, get_or_create_named_ledger, get_or_create_group, today_ist,
)

PARTY_GROUP_NAME = "Booking Parties"
SUNDRY_DEBTORS_GROUP_NAME = "Sundry Debtors"
FREIGHT_INCOME_LEDGER_NAME = "Freight Income"
DIRECT_INCOMES_GROUP_NAME = "Direct Incomes"
CHUNK = 300
SYNC_BATCH = 500          # bilties posted per sync call — keeps each request well under the 30 s limit
STATEMENT_ROWS = 200      # newest statement rows returned with the ledger page

REGULAR_COLS = ("id, gr_no, bilty_date, branch_id, consignor_name, consignee_name, consignor_gst, "
                "consignee_gst, payment_mode, total, is_active, deleted_at")
MANUAL_COLS = "id, gr_no, created_at, branch_id, station, consignor, consignee, payment_status, amount"
LINK_COLS = ("id, ledger_id, bilty_type, bilty_id, gr_no, bilty_date, consignor, consignee, payment_mode, "
             "amount, voucher_id, bill_reference_id, added_via, is_active, added_by, created_at")
REQUIRED_PAYMENT = {"consignor": "paid", "consignee": "to-pay"}


_billing_supported: bool | None = None


def billing_supported(sb) -> bool:
    """ledger_bilties.amount_source / billed_at / billed_by exist after migration 025."""
    global _billing_supported
    if _billing_supported:
        return True
    try:
        sb.table("ledger_bilties").select("amount_source").limit(1).execute()
        _billing_supported = True
    except Exception:
        _billing_supported = False
    return _billing_supported


def _link_cols(sb) -> str:
    return LINK_COLS + (", amount_source, billed_at, billed_by" if billing_supported(sb) else "")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _err(msg: str, code: int = 400, **extra) -> dict:
    return {"status": "error", "message": msg, "status_code": code, **extra}


def norm(name) -> str:
    """'Anil Locks Pvt.Ltd' -> 'ANILLOCKSPVTLTD' — how party names are compared."""
    return re.sub(r"[^A-Z0-9]", "", str(name or "").upper())


def _chunks(lst, n=CHUNK):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


# ── Bilty rows (regular + manual) in one common shape ────────────────────────
def _pay(mode) -> str:
    m = str(mode or "").strip().lower()
    return "foc" if m in ("freeofcost", "foc", "free") else m


def _from_regular(r: dict) -> dict:
    return {
        "bilty_type": "regular", "bilty_id": r["id"], "gr_no": r["gr_no"],
        "bilty_date": r.get("bilty_date"), "branch_id": r.get("branch_id"), "station": None,
        "consignor": r.get("consignor_name"), "consignee": r.get("consignee_name"),
        "consignor_gst": r.get("consignor_gst"), "consignee_gst": r.get("consignee_gst"),
        "payment_mode": _pay(r.get("payment_mode")), "amount": round(float(r.get("total") or 0), 2),
        "is_deleted": (r.get("is_active") is False) or bool(r.get("deleted_at")),
    }


def _from_manual(r: dict) -> dict:
    return {
        "bilty_type": "manual", "bilty_id": r["id"], "gr_no": r["gr_no"],
        "bilty_date": (r.get("created_at") or "")[:10] or None, "branch_id": r.get("branch_id"),
        "station": r.get("station"), "consignor": r.get("consignor"), "consignee": r.get("consignee"),
        "consignor_gst": None, "consignee_gst": None,
        "payment_mode": _pay(r.get("payment_status")), "amount": round(float(r.get("amount") or 0), 2),
        "is_deleted": False,
    }


def _fetch_bilties(sb, keys: list[tuple[str, str]]) -> dict[tuple[str, str], dict]:
    """{(bilty_type, bilty_id): common row} for the given keys."""
    out = {}
    reg = [k[1] for k in keys if k[0] == "regular"]
    man = [k[1] for k in keys if k[0] == "manual"]
    for ids in _chunks(reg, 200):
        for r in sb.table("bilty").select(REGULAR_COLS).in_("id", ids).execute().data or []:
            out[("regular", r["id"])] = _from_regular(r)
    for ids in _chunks(man, 200):
        for r in sb.table("station_bilty_summary").select(MANUAL_COLS).in_("id", ids).execute().data or []:
            out[("manual", r["id"])] = _from_manual(r)
    return out


def _paged(query_fn) -> list[dict]:
    rows, off = [], 0
    while True:
        r = query_fn().range(off, off + 999).execute().data or []
        rows += r
        if len(r) < 1000:
            return rows
        off += 1000


# ── Profiles + matching ──────────────────────────────────────────────────────
def _get_profile(sb, ledger_id: str) -> dict | None:
    p = sb.table("party_ledger_profiles").select("*").eq("ledger_id", ledger_id).execute().data
    if not p:
        return None
    led = sb.table("ledgers").select("id, name, branch_id, is_active").eq("id", ledger_id).execute().data
    return {**p[0], "ledger": led[0] if led else None}


def _match_set(profile: dict) -> set[str]:
    names = {norm(profile.get("party_name"))} | {norm(n) for n in (profile.get("match_names") or [])}
    return {n for n in names if n}


def qualifies(profile: dict, b: dict) -> tuple[bool, str | None]:
    """Does bilty `b` belong in this ledger? Returns (ok, reason_if_not)."""
    if b.get("is_deleted"):
        return False, "bilty is deleted"
    if profile["kind"] == "custom":
        return True, None
    ledger = profile.get("ledger") or {}
    if profile.get("own_branch_only") and b.get("branch_id") != ledger.get("branch_id"):
        return False, "booked in another branch"
    side = profile["party_type"]
    need = REQUIRED_PAYMENT[side]
    if b["payment_mode"] != need:
        return False, f"{side} ledger counts only {need} bilties (this one is {b['payment_mode'] or 'unknown'})"
    gst = (profile.get("gstin") or "").strip().upper()
    if gst and (b.get(f"{side}_gst") or "").strip().upper() == gst:
        return True, None
    if norm(b.get(side)) in _match_set(profile):
        return True, None
    return False, f"{side} '{b.get(side)}' doesn't match this party (add it to match_names)"


def _candidate_bilties(sb, profile: dict, from_date=None, to_date=None) -> list[dict]:
    """Every bilty of this party that qualifies (paid for consignor / to-pay for consignee)."""
    side = profile["party_type"]
    need = REQUIRED_PAYMENT[side]
    patterns = set()
    for name in [profile.get("party_name")] + list(profile.get("match_names") or []):
        tokens = re.findall(r"[A-Za-z0-9]+", str(name or ""))
        if tokens:
            # wildcards around + between words; the exact match is then checked with norm()
            patterns.add("%" + "%".join(tokens) + "%")
    found: dict[tuple, dict] = {}
    reg_col, man_col = f"{side}_name", side
    reg_modes = ["paid"] if need == "paid" else ["to-pay"]

    def reg_q(col, op, val):
        def q():
            x = sb.table("bilty").select(REGULAR_COLS).in_("payment_mode", reg_modes).eq("is_active", True)
            x = x.ilike(col, val) if op == "ilike" else x.eq(col, val)
            if from_date:
                x = x.gte("bilty_date", from_date)
            if to_date:
                x = x.lte("bilty_date", to_date)
            return x.order("bilty_date")
        return q

    def man_q(val):
        def q():
            x = sb.table("station_bilty_summary").select(MANUAL_COLS).eq("payment_status", need).ilike(man_col, val)
            if from_date:
                x = x.gte("created_at", from_date)
            if to_date:
                x = x.lte("created_at", f"{to_date}T23:59:59")
            return x.order("created_at")
        return q

    for pat in patterns:
        for r in _paged(reg_q(reg_col, "ilike", pat)):
            found[("regular", r["id"])] = _from_regular(r)
        for r in _paged(man_q(pat)):
            found[("manual", r["id"])] = _from_manual(r)
    gst = (profile.get("gstin") or "").strip().upper()
    if gst:
        for r in _paged(reg_q(f"{side}_gst", "eq", gst)):
            found[("regular", r["id"])] = _from_regular(r)
    out = [b for b in found.values() if qualifies(profile, b)[0]]
    out.sort(key=lambda b: (b.get("bilty_date") or "", b["gr_no"]))
    return out


def _active_links(sb, keys: list[tuple[str, str]]) -> dict[tuple[str, str], dict]:
    """Active links for these bilties. A link whose voucher was cancelled from the
    Day Book is retired here, so that bilty can be added again."""
    out = {}
    by_type: dict[str, list] = {}
    for t, i in keys:
        by_type.setdefault(t, []).append(i)
    for t, ids in by_type.items():
        for part in _chunks(ids, 200):
            try:
                rows = sb.table("ledger_bilties").select(LINK_COLS).eq("bilty_type", t).in_("bilty_id", part) \
                    .eq("is_active", True).execute().data or []
            except Exception as exc:
                if "ledger_bilties" in str(exc) and "does not exist" in str(exc):
                    return {}          # migration 024 not run yet -> nothing is linked
                raise
            for r in rows:
                out[(r["bilty_type"], r["bilty_id"])] = r
    _retire_cancelled(sb, out)
    return out


def _retire_cancelled(sb, links: dict) -> None:
    v_ids = list({l["voucher_id"] for l in links.values() if l.get("voucher_id")})
    dead = set()
    for part in _chunks(v_ids, 200):
        dead |= {v["id"] for v in (sb.table("vouchers").select("id, is_active").in_("id", part).execute().data or [])
                 if not v["is_active"]}
    for key, l in list(links.items()):
        if l.get("voucher_id") in dead:
            sb.table("ledger_bilties").update({"is_active": False, "removed_at": _now(), "updated_at": _now()}) \
                .eq("id", l["id"]).execute()
            links.pop(key)


def _ledger_names(sb, ids) -> dict:
    ids = list({i for i in ids if i})
    if not ids:
        return {}
    return {r["id"]: r["name"] for r in sb.table("ledgers").select("id, name").in_("id", ids).execute().data or []}


# ── Posting (bulk) ───────────────────────────────────────────────────────────
def _last_sales_seq(sb, branch_id: str) -> int:
    return last_voucher_seq(sb, branch_id, "sales")


def _post_vouchers(sb, ledger: dict, freight_id: str, items: list[dict], user_id: str,
                   via: str, v_ids: list, bill_ids: list) -> tuple[dict, dict]:
    """Post Sales vouchers (Dr party bill / Cr Freight Income) for items with amount > 0,
    in 3 batched inserts. Appends created ids to v_ids / bill_ids so the caller can roll
    back. Returns ({key: voucher_id}, {key: bill_id}), key = (bilty_type, bilty_id)."""
    payable = [b for b in items if b["amount"] > 0]
    v_by_key, bill_by_key = {}, {}
    if not payable:
        return v_by_key, bill_by_key
    with_source = source_supported(sb)
    for attempt in range(2):
        seq = _last_sales_seq(sb, ledger["branch_id"])
        rows = []
        for i, b in enumerate(payable, start=1):
            row = {
                "branch_id": ledger["branch_id"], "voucher_type": "sales",
                "voucher_no": f"{VOUCHER_PREFIX['sales']}{str(seq + i).zfill(4)}",
                "voucher_date": b.get("voucher_date") or b.get("bilty_date") or today_ist(),
                "narration": f"Bilty {b['gr_no']} - {b.get('consignor') or ''} to {b.get('consignee') or ''}".strip(),
                "reference_no": b["gr_no"], "total_amount": b["amount"], "created_by": user_id,
            }
            if with_source:
                row["source"] = {"action": "bilty", "bilty_type": b["bilty_type"],
                                 "bilty_id": b["bilty_id"], "gr_no": b["gr_no"]}
            rows.append(row)
        try:
            ins = sb.table("vouchers").insert(rows).execute().data or []
            break
        except Exception as exc:
            if attempt == 0 and "vouchers_branch_type_no_unique" in str(exc):
                continue          # someone took the same numbers — renumber once
            raise
    no_to_key = {r["voucher_no"]: (b["bilty_type"], b["bilty_id"]) for r, b in zip(rows, payable)}
    for v in ins:
        v_by_key[no_to_key[v["voucher_no"]]] = v["id"]
        v_ids.append(v["id"])

    bills = sb.table("ledger_bill_references").insert([{
        "ledger_id": ledger["id"], "reference_no": b["gr_no"],
        "reference_date": b.get("voucher_date") or b.get("bilty_date") or today_ist(),
        "bill_amount": b["amount"], "balance_amount": b["amount"], "entry_type": "dr",
        "source_table": "bilty" if b["bilty_type"] == "regular" else "station_bilty_summary",
        "source_id": b["bilty_id"],
        "metadata": {"gr_no": b["gr_no"], "bilty_type": b["bilty_type"], "consignor": b.get("consignor"),
                     "consignee": b.get("consignee"), "payment_mode": b["payment_mode"]},
        "created_by": user_id,
    } for b in payable]).execute().data or []
    for bl in bills:
        key = ("regular" if bl["source_table"] == "bilty" else "manual", bl["source_id"])
        bill_by_key[key] = bl["id"]
        bill_ids.append(bl["id"])

    entries = []
    for b in payable:
        key = (b["bilty_type"], b["bilty_id"])
        entries += [
            {"voucher_id": v_by_key[key], "ledger_id": ledger["id"], "entry_type": "dr",
             "amount": b["amount"], "bill_reference_id": bill_by_key[key], "bill_allocation_type": "new_ref"},
            {"voucher_id": v_by_key[key], "ledger_id": freight_id, "entry_type": "cr", "amount": b["amount"]},
        ]
    sb.table("voucher_entries").insert(entries).execute()

    try:
        sb.table("ledger_audit_log").insert([{
            "entity_type": "voucher", "entity_id": vid, "action": "create", "old_data": None,
            "new_data": {"via": f"party_ledger/{via}", "ledger_id": ledger["id"]}, "changed_by": user_id,
        } for vid in v_by_key.values()]).execute()
    except Exception as exc:
        print(f"party ledger audit log failed: {exc}")
    return v_by_key, bill_by_key


def _rollback(sb, v_ids: list, bill_ids: list) -> None:
    for part in _chunks(v_ids, 200):
        sb.table("vouchers").delete().in_("id", part).execute()     # entries cascade
    for part in _chunks(bill_ids, 200):
        sb.table("ledger_bill_references").delete().in_("id", part).execute()


def _freight_ledger(ledger: dict, user_id: str) -> dict:
    return get_or_create_named_ledger(ledger["branch_id"], FREIGHT_INCOME_LEDGER_NAME, DIRECT_INCOMES_GROUP_NAME, user_id)


def _post_bilties(sb, profile: dict, bilties: list[dict], added_via: str, user_id: str | None) -> dict:
    """Add bilties to the ledger. Bilties with an amount are posted as Sales vouchers;
    zero-amount ones (paid but not billed yet) are linked as "to be billed".
    Batched per chunk; any failure rolls the chunk back."""
    ledger = profile["ledger"]
    user_id = user_id or profile.get("created_by")
    if not user_id:
        return _err("created_by / added_by is required")
    freight = _freight_ledger(ledger, user_id)
    if freight["status"] != "success":
        return freight
    added = []

    for chunk in _chunks(bilties):
        v_ids, bill_ids = [], []
        try:
            v_by_key, bill_by_key = _post_vouchers(sb, ledger, freight["data"]["id"], chunk, user_id,
                                                   added_via, v_ids, bill_ids)
            links = []
            for b in chunk:
                key = (b["bilty_type"], b["bilty_id"])
                links.append({
                    "ledger_id": ledger["id"], "bilty_type": b["bilty_type"], "bilty_id": b["bilty_id"],
                    "gr_no": b["gr_no"], "bilty_date": b.get("bilty_date"), "consignor": b.get("consignor"),
                    "consignee": b.get("consignee"), "payment_mode": b["payment_mode"], "amount": b["amount"],
                    "voucher_id": v_by_key.get(key), "bill_reference_id": bill_by_key.get(key),
                    "added_via": added_via, "added_by": user_id,
                })
            sb.table("ledger_bilties").insert(links).execute()
        except Exception as exc:
            _rollback(sb, v_ids, bill_ids)
            msg = str(exc)
            if "uq_ledger_bilties_active" in msg:
                msg = "one of these bilties was just added to a ledger by someone else — refresh and try again"
            return _err(f"Could not add bilties: {msg}", added=added)
        added += [b["gr_no"] for b in chunk]

    return {"status": "success", "added": added}


# ── Create / list / detail ───────────────────────────────────────────────────
def create_party_ledger(data: dict) -> dict:
    """
    data = { kind: 'party'|'custom', branch_id, created_by,
             party:  party_type 'consignor'|'consignee', party_id? (consignors/consignees id),
                     party_name? (defaults to that party's company_name), match_names?: [...],
                     gstin?, auto_add? (default true), own_branch_only? (default false),
                     import_existing? (default FALSE — old bilties are usually already paid;
                     send import_from_date = the date from which bills are still unpaid,
                     and put anything older in opening_balance), import_from_date?, import_to_date?
             custom: name (e.g. 'MRM RAMKOLA'),
             both:   name? (ledger name, defaults to party_name), opening_balance?, opening_balance_type?,
                     phone?, address? }
    """
    sb = get_supabase()
    kind = data.get("kind")
    branch_id = data.get("branch_id")
    created_by = data.get("created_by")
    if kind not in ("party", "custom") or not branch_id or not created_by:
        return _err("kind ('party'/'custom'), branch_id and created_by are required")

    party_type = data.get("party_type")
    party_name = (data.get("party_name") or "").strip()
    gstin = (data.get("gstin") or "").strip().upper() or None
    phone, address = data.get("phone"), data.get("address")
    if kind == "party":
        if party_type not in ("consignor", "consignee"):
            return _err("party_type must be 'consignor' or 'consignee'")
        if data.get("party_id"):
            table = "consignors" if party_type == "consignor" else "consignees"
            p = sb.table(table).select("id, company_name, gst_num, number, company_add") \
                .eq("id", data["party_id"]).execute().data
            if not p:
                return _err(f"{party_type} not found", 404)
            party_name = party_name or p[0]["company_name"]
            gstin = gstin or (p[0].get("gst_num") or "").strip().upper() or None
            phone, address = phone or p[0].get("number"), address or p[0].get("company_add")
        if not party_name:
            return _err("party_name (or party_id) is required")
    name = (data.get("name") or party_name).strip()
    if not name:
        return _err("name is required")
    if kind == "custom":
        party_name = party_name or name

    group_id = get_or_create_group(PARTY_GROUP_NAME, SUNDRY_DEBTORS_GROUP_NAME, "asset")
    if not group_id:
        return _err("Could not resolve the Booking Parties group", 500)
    created = create_ledger({
        "branch_id": branch_id, "name": name, "group_id": group_id, "gstin": gstin, "phone": phone,
        "address": address, "opening_balance": data.get("opening_balance", 0),
        "opening_balance_type": data.get("opening_balance_type", "dr"),
        "opening_balance_date": data.get("opening_balance_date"), "is_bill_wise": True,
        "created_by": created_by,
    }, created_by)
    if created["status"] != "success":
        return created
    ledger_id = created["data"]["id"]

    profile_row = {
        "ledger_id": ledger_id, "kind": kind,
        "party_type": party_type if kind == "party" else None,
        "party_id": data.get("party_id") if kind == "party" else None,
        "party_name": party_name,
        "match_names": [n.strip() for n in (data.get("match_names") or []) if str(n).strip()],
        "gstin": gstin, "auto_add": bool(data.get("auto_add", True)) if kind == "party" else False,
        "own_branch_only": bool(data.get("own_branch_only", False)),
        "created_by": created_by, "updated_by": created_by,
    }
    try:
        sb.table("party_ledger_profiles").insert(profile_row).execute()
    except Exception as exc:
        sb.table("ledgers").delete().eq("id", ledger_id).execute()
        if "party_ledger_profiles" in str(exc) and "does not exist" in str(exc):
            return _err("Party ledgers need migrations/024_party_bilty_ledgers.sql — run it first", 500)
        return _err(f"Could not create party ledger: {exc}")

    result = {"status": "success", "message": f"Ledger '{name}' created", "data": {"ledger_id": ledger_id}}
    if kind == "party" and data.get("import_existing", False):
        imp = sync_party_ledger(ledger_id, {
            "dry_run": False, "from_date": data.get("import_from_date"), "to_date": data.get("import_to_date"),
            "user_id": created_by, "added_via": "import",
        })
        result["import"] = {k: imp.get(k) for k in ("status", "message", "added_count", "remaining", "skipped")}
    detail = get_party_ledger(ledger_id)
    result["data"] = detail.get("data", result["data"])
    return result


def update_party_profile(ledger_id: str, data: dict) -> dict:
    """data: party_name?, match_names?, gstin?, auto_add?, own_branch_only?, updated_by.
    (Rename the ledger itself with PUT /api/ledger/ledgers/{id}.)"""
    sb = get_supabase()
    if not _get_profile(sb, ledger_id):
        return _err("Party ledger not found", 404)
    payload = {}
    if "party_name" in data:
        if not str(data.get("party_name") or "").strip():
            return _err("party_name can't be empty")
        payload["party_name"] = data["party_name"].strip()
    if "match_names" in data:
        payload["match_names"] = [n.strip() for n in (data.get("match_names") or []) if str(n).strip()]
    if "gstin" in data:
        payload["gstin"] = (data.get("gstin") or "").strip().upper() or None
    for f in ("auto_add", "own_branch_only"):
        if f in data:
            payload[f] = bool(data[f])
    if not payload:
        return _err("Nothing to update")
    payload.update({"updated_by": data.get("updated_by"), "updated_at": _now()})
    sb.table("party_ledger_profiles").update(payload).eq("ledger_id", ledger_id).execute()
    return get_party_ledger(ledger_id)


def _link_stats(sb, ledger_id: str) -> dict:
    rows = _paged(lambda: sb.table("ledger_bilties").select("amount, voucher_id").eq("ledger_id", ledger_id)
                  .eq("is_active", True))
    unbilled = [r for r in rows if not r.get("voucher_id")]
    return {"bilty_count": len(rows), "bilty_total": round(sum(float(r["amount"] or 0) for r in rows), 2),
            "to_be_billed_count": len(unbilled)}


def list_party_ledgers(branch_id: str | None = None, kind: str | None = None) -> dict:
    sb = get_supabase()
    q = sb.table("party_ledger_profiles").select("*")
    if kind:
        q = q.eq("kind", kind)
    profiles = q.execute().data or []
    ledgers = {l["id"]: l for l in (sb.table("ledgers").select("id, name, branch_id, is_active, phone, gstin")
                                    .in_("id", [p["ledger_id"] for p in profiles]).execute().data or [])} if profiles else {}
    out = []
    for p in profiles:
        l = ledgers.get(p["ledger_id"])
        if not l or not l["is_active"] or (branch_id and l["branch_id"] != branch_id):
            continue
        bal = get_ledger_balance(l["id"])["data"]
        out.append({**p, "name": l["name"], "branch_id": l["branch_id"], "phone": l.get("phone"),
                    "balance": bal["balance"], "balance_type": bal["balance_type"], **_link_stats(sb, l["id"])})
    if out and not branch_id:
        from services.branch_service import get_branch_name_map
        bmap = get_branch_name_map([r["branch_id"] for r in out])
        for r in out:
            r["branch_name"] = bmap.get(r["branch_id"])
    out.sort(key=lambda r: r["name"])
    return {"status": "success", "data": out}


def get_party_ledger(ledger_id: str) -> dict:
    """Everything for the party ledger page: ledger, profile, balance, open bills, statement, bilty totals."""
    sb = get_supabase()
    profile = _get_profile(sb, ledger_id)
    if not profile:
        return _err("Party ledger not found", 404)
    ledger_res = get_ledger(ledger_id)
    if ledger_res["status"] != "success":
        return ledger_res
    profile.pop("ledger", None)
    statement = get_ledger_statement(ledger_id)["data"]["entries"]
    return {
        "status": "success",
        "data": {
            "ledger": ledger_res["data"],
            "profile": profile,
            "balance": get_ledger_balance(ledger_id)["data"],
            "open_bills": list_bills(ledger_id, is_settled=False)["data"],
            # newest STATEMENT_ROWS rows (running balances stay exact); full history:
            # GET /api/ledger/ledgers/{id}/statement?from_date=&to_date=
            "statement": statement[-STATEMENT_ROWS:],
            "statement_total_rows": len(statement),
            "bilty_summary": _link_stats(sb, ledger_id),
        },
    }


BILLING_FILTERS = {
    "to_be_billed": {"to_be_billed"}, "unpaid": {"unpaid"}, "part_paid": {"part_paid"}, "paid": {"paid"},
    "open": {"unpaid", "part_paid"}, "settled": {"paid"}, "billed": {"unpaid", "part_paid", "paid"},
}


def list_ledger_bilties(ledger_id: str, status: str | None = None, q: str | None = None,
                        page: int = 1, page_size: int = 50) -> dict:
    """Bilties in a ledger with their billing state:
         billing_status = to_be_billed (no amount yet) | unpaid | part_paid | paid
       status filter: to_be_billed | unpaid | part_paid | paid | open (unpaid + part_paid) | settled | billed"""
    sb = get_supabase()
    page, page_size = max(1, page), min(max(1, page_size), 200)
    term = _safe_term(q)
    cols = _link_cols(sb)

    def base():
        x = sb.table("ledger_bilties").select(cols).eq("ledger_id", ledger_id).eq("is_active", True)
        if term:
            x = x.or_(f"gr_no.ilike.%{term}%,consignor.ilike.%{term}%,consignee.ilike.%{term}%")
        return x.order("bilty_date", desc=True)

    rows = _paged(base)
    bills = {}
    for part in _chunks([r["bill_reference_id"] for r in rows if r.get("bill_reference_id")], 200):
        for bl in sb.table("ledger_bill_references").select("id, bill_amount, balance_amount, is_settled") \
                .in_("id", part).execute().data or []:
            bills[bl["id"]] = bl
    void = _void_bills(sb, list(bills))
    for r in rows:
        bl = bills.get(r.get("bill_reference_id"))
        if not r.get("voucher_id") or not bl or bl["id"] in void:
            r.update({"billing_status": "to_be_billed", "billed_amount": 0.0, "balance_amount": 0.0,
                      "paid_amount": 0.0, "is_settled": False})
            continue
        billed, bal = float(bl["bill_amount"]), float(bl["balance_amount"])
        r.update({"billed_amount": billed, "balance_amount": bal, "paid_amount": round(billed - bal, 2),
                  "is_settled": bal <= 0,
                  "billing_status": "paid" if bal <= 0 else ("unpaid" if bal >= billed else "part_paid")})
    if status:
        wanted = BILLING_FILTERS.get(status)
        if wanted is None:
            return _err(f"status must be one of {sorted(BILLING_FILTERS)}")
        rows = [r for r in rows if r["billing_status"] in wanted]
    total = len(rows)
    return {"status": "success", "data": {
        "rows": rows[(page - 1) * page_size: page * page_size], "total": total, "page": page, "page_size": page_size,
        "open_total": round(sum(r["balance_amount"] for r in rows if r["billing_status"] in ("unpaid", "part_paid")), 2),
        "to_be_billed_count": sum(1 for r in rows if r["billing_status"] == "to_be_billed"),
    }}


def _void_bills(sb, bill_ids: list[str]) -> set[str]:
    from services.ledger.bill_reference_service import _void_bill_ids
    return _void_bill_ids(sb, bill_ids)


def _safe_term(q) -> str:
    """Search text that can't break a PostgREST or_() filter."""
    return re.sub(r"[,()%*\\]", " ", str(q or "")).strip()


# ── Add / remove / sync / auto ───────────────────────────────────────────────
def add_bilties(ledger_id: str, data: dict) -> dict:
    """data = { items: [{bilty_type: 'regular'|'manual', bilty_id}], added_by }"""
    sb = get_supabase()
    profile = _get_profile(sb, ledger_id)
    if not profile:
        return _err("Party ledger not found", 404)
    if not (profile.get("ledger") or {}).get("is_active"):
        return _err("Ledger is inactive")
    items = data.get("items") or []
    keys = list(dict.fromkeys((i.get("bilty_type"), i.get("bilty_id")) for i in items
                              if i.get("bilty_type") in ("regular", "manual") and i.get("bilty_id")))
    if not keys:
        return _err("items: [{bilty_type: 'regular'|'manual', bilty_id}] is required")

    rows = _fetch_bilties(sb, keys)
    links = _active_links(sb, keys)
    names = _ledger_names(sb, [l["ledger_id"] for l in links.values()])
    to_add, skipped = [], []
    for k in keys:
        b = rows.get(k)
        if not b:
            skipped.append({"bilty_type": k[0], "bilty_id": k[1], "reason": "bilty not found"})
            continue
        if k in links:
            l = links[k]
            where = "this ledger" if l["ledger_id"] == ledger_id else f"ledger '{names.get(l['ledger_id'])}'"
            skipped.append({"gr_no": b["gr_no"], "reason": f"already in {where}"})
            continue
        ok, why = qualifies(profile, b)
        if not ok:
            skipped.append({"gr_no": b["gr_no"], "reason": why})
            continue
        to_add.append(b)

    if not to_add:
        return {"status": "success", "message": "Nothing added", "added_count": 0, "added": [], "skipped": skipped}
    res = _post_bilties(sb, profile, to_add, "manual", data.get("added_by"))
    if res["status"] != "success":
        return {**res, "skipped": skipped}
    return {"status": "success", "message": f"{len(res['added'])} bilty(s) added",
            "added_count": len(res["added"]), "added": res["added"], "skipped": skipped}


def remove_bilty(ledger_id: str, link_id: str, user_id: str | None = None, reason: str | None = None) -> dict:
    """Takes a bilty out of the ledger by cancelling its Sales voucher (refused while
    payments are applied to that bilty's bill — cancel those first)."""
    sb = get_supabase()
    l = sb.table("ledger_bilties").select(LINK_COLS).eq("id", link_id).eq("ledger_id", ledger_id).execute().data
    if not l or not l[0]["is_active"]:
        return _err("Bilty not found in this ledger", 404)
    link = l[0]
    if link.get("voucher_id"):
        v = sb.table("vouchers").select("is_active").eq("id", link["voucher_id"]).execute().data
        if v and v[0]["is_active"]:
            c = cancel_voucher(link["voucher_id"], user_id, reason or f"Bilty {link['gr_no']} removed from ledger")
            if c["status"] != "success":
                return c
    sb.table("ledger_bilties").update({"is_active": False, "removed_by": user_id, "removed_at": _now(),
                                       "updated_at": _now()}).eq("id", link_id).execute()
    return {"status": "success", "message": f"Bilty {link['gr_no']} removed from the ledger"}


def _refresh_link(sb, link: dict, b: dict, user_id: str | None) -> dict:
    """The bilty's own total changed after it was added — follow it, unless the
    amount was entered at billing time in the ledger (amount_source = 'manual')."""
    if link.get("amount_source") == "manual":
        return {"status": "skipped", "message": "amount was entered at billing — not taken from the bilty"}
    return _set_link_amount(sb, link, b["amount"], user_id, source="bilty",
                            reason=f"Bilty {link['gr_no']} amount changed", bilty=b)


def _set_link_amount(sb, link: dict, new_amount: float, user_id: str | None, source: str,
                     reason: str, bilty: dict | None = None, voucher_date: str | None = None) -> dict:
    """Move one ledger bilty to `new_amount`:
         no voucher yet + amount > 0 -> post its Sales voucher (it is now billed / owed)
         voucher + amount > 0        -> edit the voucher; its bill follows (refused below what's paid)
         voucher + amount = 0        -> cancel the voucher, back to "to be billed" (refused if paid)"""
    new_amount = round(float(new_amount or 0), 2)
    if new_amount < 0:
        return _err("amount can't be negative")
    voucher_active = False
    if link.get("voucher_id"):
        v = sb.table("vouchers").select("is_active").eq("id", link["voucher_id"]).execute().data
        voucher_active = bool(v and v[0]["is_active"])

    update = {"amount": new_amount, "updated_at": _now()}
    if bilty:
        update.update({"payment_mode": bilty["payment_mode"], "consignor": bilty.get("consignor"),
                       "consignee": bilty.get("consignee")})
    if billing_supported(sb):
        update["amount_source"] = source
        if source == "manual":
            update.update({"billed_at": _now(), "billed_by": user_id})

    if voucher_active and new_amount > 0:
        entries = sb.table("voucher_entries").select("id, ledger_id, entry_type, bill_reference_id, bill_allocation_type") \
            .eq("voucher_id", link["voucher_id"]).execute().data or []
        body = {"entries": [{**e, "amount": new_amount} for e in entries], "updated_by": user_id, "edit_reason": reason}
        if voucher_date:
            body["voucher_date"] = voucher_date
        res = update_voucher(link["voucher_id"], body)
        if res["status"] != "success":
            return res
    elif voucher_active and new_amount == 0:
        res = cancel_voucher(link["voucher_id"], user_id, reason)
        if res["status"] != "success":
            return res
        update.update({"voucher_id": None, "bill_reference_id": None})
    elif new_amount > 0:
        led = sb.table("ledgers").select("id, name, branch_id").eq("id", link["ledger_id"]).execute().data
        if not led:
            return _err("Ledger not found", 404)
        user_id = user_id or link.get("added_by")
        freight = _freight_ledger(led[0], user_id)
        if freight["status"] != "success":
            return freight
        item = {k: link.get(k) for k in ("bilty_type", "bilty_id", "gr_no", "bilty_date", "consignor",
                                         "consignee", "payment_mode")}
        item.update({"amount": new_amount, "voucher_date": voucher_date})
        v_ids, bill_ids = [], []
        try:
            v_by_key, bill_by_key = _post_vouchers(sb, led[0], freight["data"]["id"], [item], user_id,
                                                   "billing", v_ids, bill_ids)
            key = (link["bilty_type"], link["bilty_id"])
            update.update({"voucher_id": v_by_key[key], "bill_reference_id": bill_by_key[key]})
            sb.table("ledger_bilties").update(update).eq("id", link["id"]).execute()
        except Exception as exc:
            _rollback(sb, v_ids, bill_ids)
            return _err(f"Could not bill {link['gr_no']}: {exc}")
        return {"status": "success", "gr_no": link["gr_no"], "amount": new_amount}

    sb.table("ledger_bilties").update(update).eq("id", link["id"]).execute()
    return {"status": "success", "gr_no": link["gr_no"], "amount": new_amount}


def bill_bilties(ledger_id: str, data: dict) -> dict:
    """
    Enter billed amounts on bilties in this ledger (paid bilties are "to be billed").
    data = { items: [{link_id, amount}], date? (bill date, default: each bilty's date), updated_by }
      amount > 0 on a "to be billed" bilty -> it's now owed (Sales voucher posted)
      amount > 0 on a billed bilty         -> amount corrected (refused below what's already paid)
      amount = 0                           -> back to "to be billed" (refused if anything is paid)
    Amounts entered here are never overwritten by the bilty's own total later.
    """
    sb = get_supabase()
    items = data.get("items") or []
    if not items:
        return _err("items: [{link_id, amount}] is required")
    user_id = data.get("updated_by")
    if not user_id:
        return _err("updated_by is required")
    if not billing_supported(sb):
        # without amount_source a later sync would see the bilty's own 0 total and undo the billing
        return _err("Billing needs migrations/025_ledger_bilties_billing.sql — run it first", 500)
    ids = [i.get("link_id") for i in items if i.get("link_id")]
    links = {}
    for part in _chunks(ids, 200):
        for l in sb.table("ledger_bilties").select(_link_cols(sb)).in_("id", part).eq("ledger_id", ledger_id) \
                .eq("is_active", True).execute().data or []:
            links[l["id"]] = l
    done, failed = [], []
    for it in items:
        l = links.get(it.get("link_id"))
        if not l:
            failed.append({"link_id": it.get("link_id"), "reason": "bilty not found in this ledger"})
            continue
        try:
            amt = float(it.get("amount"))
        except (TypeError, ValueError):
            failed.append({"gr_no": l["gr_no"], "reason": "amount must be a number"})
            continue
        r = _set_link_amount(sb, l, amt, user_id, source="manual",
                             reason=f"Billed {l['gr_no']} at ₹{amt:,.2f}", voucher_date=data.get("date"))
        if r["status"] == "success":
            done.append({"gr_no": l["gr_no"], "amount": round(amt, 2)})
        else:
            failed.append({"gr_no": l["gr_no"], "reason": r["message"]})
    return {"status": "success" if done or not failed else "error",
            "message": f"{len(done)} bilty(s) billed" + (f", {len(failed)} failed" if failed else ""),
            "billed": done, "failed": failed, "billed_total": round(sum(d["amount"] for d in done), 2),
            **({} if done or not failed else {"status_code": 400})}


def sync_party_ledger(ledger_id: str, data: dict) -> dict:
    """
    data = { dry_run? (default true), from_date?, to_date?, user_id / added_by,
             limit? (default 500 — bilties posted per call; repeat while `remaining` > 0) }
    Party ledgers: finds this party's qualifying bilties that are in NO ledger yet.
    All ledgers: finds bilties already in this ledger whose amount changed since,
    or that no longer qualify / were deleted.
    dry_run=true only reports; false adds the new ones and applies the amount changes.
    """
    sb = get_supabase()
    profile = _get_profile(sb, ledger_id)
    if not profile:
        return _err("Party ledger not found", 404)
    dry_run = data.get("dry_run", True)
    user_id = data.get("user_id") or data.get("added_by")

    new_items = []
    if profile["kind"] == "party":
        cands = _candidate_bilties(sb, profile, data.get("from_date"), data.get("to_date"))
        linked = _active_links(sb, [(b["bilty_type"], b["bilty_id"]) for b in cands])
        new_items = [b for b in cands if (b["bilty_type"], b["bilty_id"]) not in linked]

    mine = _paged(lambda: sb.table("ledger_bilties").select(_link_cols(sb)).eq("ledger_id", ledger_id).eq("is_active", True))
    current = _fetch_bilties(sb, [(l["bilty_type"], l["bilty_id"]) for l in mine])
    changed, not_matching = [], []
    for l in mine:
        b = current.get((l["bilty_type"], l["bilty_id"]))
        if not b:
            not_matching.append({"link_id": l["id"], "gr_no": l["gr_no"], "reason": "bilty no longer exists"})
            continue
        ok, why = qualifies(profile, b)
        if not ok:
            not_matching.append({"link_id": l["id"], "gr_no": l["gr_no"], "reason": why})
        if l.get("amount_source") != "manual" and round(float(l["amount"] or 0), 2) != b["amount"]:
            changed.append({"link_id": l["id"], "gr_no": l["gr_no"], "old_amount": float(l["amount"] or 0),
                            "new_amount": b["amount"], "_link": l, "_bilty": b})

    report = {
        "new_bilties": [{k: b[k] for k in ("bilty_type", "bilty_id", "gr_no", "bilty_date", "consignor",
                                            "consignee", "payment_mode", "amount")} for b in new_items],
        "new_total": round(sum(b["amount"] for b in new_items), 2),
        "amount_changed": [{k: v for k, v in c.items() if not k.startswith("_")} for c in changed],
        "no_longer_matching": not_matching,
    }
    if dry_run:
        return {"status": "success", "dry_run": True, "data": report}

    try:
        limit = max(1, min(int(data.get("limit") or SYNC_BATCH), 2000))
    except (TypeError, ValueError):
        limit = SYNC_BATCH
    batch = new_items[:limit]
    result = {"status": "success", "dry_run": False, "data": report, "added_count": 0, "skipped": [],
              "remaining": max(0, len(new_items) - len(batch))}
    if batch:
        res = _post_bilties(sb, profile, batch, data.get("added_via") or "sync", user_id)
        if res["status"] != "success":
            return {**res, "data": report}
        result["added_count"] = len(res["added"])
    fails = []
    for c in changed:
        r = _refresh_link(sb, c["_link"], c["_bilty"], user_id)
        if r["status"] == "error":
            fails.append({"gr_no": c["gr_no"], "reason": r["message"]})
    result["skipped"] = fails
    result["message"] = (f"{result['added_count']} bilty(s) added, {len(changed) - len(fails)} amount(s) updated"
                         + (f", {result['remaining']} more to add — run sync again" if result["remaining"] else ""))
    return result


def on_bilty_saved(bilty_type: str, bilty_id: str, user_id: str | None = None) -> dict:
    """Called after a bilty is booked or edited:
       - already in a ledger and its amount changed -> ledger follows the new amount
       - not in any ledger -> added to the matching auto_add party ledger, if any
    Never raises (it runs in the background after the bilty save)."""
    try:
        sb = get_supabase()
        try:
            profiles = sb.table("party_ledger_profiles").select("*").eq("kind", "party").eq("auto_add", True) \
                .execute().data or []
        except Exception:
            return {"status": "skipped", "message": "party ledgers not set up (migration 024)"}
        b = _fetch_bilties(sb, [(bilty_type, bilty_id)]).get((bilty_type, bilty_id))
        if not b:
            return {"status": "skipped", "message": "bilty not found"}

        link = _active_links(sb, [(bilty_type, bilty_id)]).get((bilty_type, bilty_id))
        if link:
            if round(float(link["amount"] or 0), 2) != b["amount"]:
                full = sb.table("ledger_bilties").select(_link_cols(sb)).eq("id", link["id"]).execute().data
                return _refresh_link(sb, full[0] if full else link, b, user_id)
            return {"status": "skipped", "message": "already in a ledger, unchanged"}
        if not profiles:
            return {"status": "skipped", "message": "no auto-add party ledgers"}

        ledgers = {l["id"]: l for l in (sb.table("ledgers").select("id, name, branch_id, is_active")
                                        .in_("id", [p["ledger_id"] for p in profiles]).execute().data or [])}
        matches = []
        for p in profiles:
            led = ledgers.get(p["ledger_id"])
            if not led or not led["is_active"]:
                continue
            prof = {**p, "ledger": led}
            if qualifies(prof, b)[0]:
                matches.append(prof)
        if not matches:
            return {"status": "skipped", "message": "no matching party ledger"}
        matches.sort(key=lambda p: p["ledger"]["branch_id"] != b.get("branch_id"))   # same branch first
        return _post_bilties(sb, matches[0], [b], "auto", user_id)
    except Exception as exc:
        print(f"party ledger auto-add failed for {bilty_type}/{bilty_id}: {exc}")
        return {"status": "error", "message": str(exc)}


# ── Payments ─────────────────────────────────────────────────────────────────
def collect_from_party(ledger_id: str, data: dict, voucher_id: str | None = None) -> dict:
    """
    Money received from this party.
    data = { branch_id, amount, payment_mode: 'cash'|'bank', bank_ledger_id?,
             allocations?: [{bill_reference_id, amount}]   (settle several bilties at once;
                            the rest of `amount`, if any, goes on account)
             bill_reference_id?  (single bilty — shortcut for one allocation),
             date?, narration?, created_by }
    Creates: Dr Cash/Bank  /  Cr this party (against each bilty bill, rest on account).
    Pass voucher_id to EDIT an existing 'collect' entry in place (same body as create).
    """
    branch_id = data.get("branch_id")
    payment_mode = data.get("payment_mode")
    created_by = data.get("created_by")
    try:
        amount = round(float(data.get("amount") or 0), 2)
    except (TypeError, ValueError):
        amount = 0
    if not branch_id or amount <= 0 or payment_mode not in ("cash", "bank") or not created_by:
        return _err("branch_id, a positive amount, payment_mode ('cash'/'bank') and created_by are required")

    allocations = list(data.get("allocations") or [])
    if not allocations and data.get("bill_reference_id"):
        allocations = [{"bill_reference_id": data["bill_reference_id"], "amount": amount}]
    lines, allocated = [], 0.0
    for a in allocations:
        try:
            a_amt = round(float(a.get("amount") or 0), 2)
        except (TypeError, ValueError):
            a_amt = 0
        if not a.get("bill_reference_id") or a_amt <= 0:
            return _err("every allocation needs bill_reference_id and a positive amount")
        allocated = round(allocated + a_amt, 2)
        lines.append({"ledger_id": ledger_id, "entry_type": "cr", "amount": a_amt,
                      "bill_allocation_type": "agst_ref", "bill_reference_id": a["bill_reference_id"]})
    if allocated > amount:
        return _err(f"allocations (₹{allocated:,.2f}) are more than the amount received (₹{amount:,.2f})")
    bill_ids = list({a["bill_reference_id"] for a in allocations})
    if bill_ids:
        sb = get_supabase()
        own = {b["id"]: b for b in (sb.table("ledger_bill_references").select("id, ledger_id, reference_no")
                                    .in_("id", bill_ids).execute().data or [])}
        bad = [i for i in bill_ids if i not in own or own[i]["ledger_id"] != ledger_id]
        if bad:
            return _err("one or more bills in allocations don't belong to this ledger")
    if amount - allocated > 0:
        lines.append({"ledger_id": ledger_id, "entry_type": "cr", "amount": round(amount - allocated, 2),
                      "bill_allocation_type": "on_account"})

    resolved = resolve_payment_ledger(branch_id, payment_mode, data.get("bank_ledger_id"))
    if resolved["status"] != "success":
        return resolved
    return save_party_voucher({
        "branch_id": branch_id,
        "voucher_type": "receipt",
        "voucher_date": data.get("date") or today_ist(),
        "narration": data.get("narration") or f"Payment received ({payment_mode})",
        "created_by": created_by,
        "entries": [{"ledger_id": resolved["data"]["id"], "entry_type": "dr", "amount": amount}, *lines],
    }, ledger_id, "collect", data, voucher_id)


# ── Search helpers for the UI ────────────────────────────────────────────────
def search_bilties(q: str | None = None, bilty_type: str = "both", from_date: str | None = None,
                   to_date: str | None = None, payment_mode: str | None = None,
                   branch_id: str | None = None, limit: int = 50, group_id: str | None = None) -> dict:
    """Search regular + manual bilties by GR no / consignor / consignee, each with the
    ledger it's already in (in_ledger) so the UI can grey it out."""
    sb = get_supabase()
    limit = min(max(1, limit), 200)
    q = _safe_term(q)
    out = []
    if group_id:
        bilty_type = "manual"          # groups (MRM, RONKEY METAL…) exist on manual bilties only
    if bilty_type in ("both", "regular"):
        x = sb.table("bilty").select(REGULAR_COLS).eq("is_active", True)
        if q:
            x = x.or_(f"gr_no.ilike.%{q}%,consignor_name.ilike.%{q}%,consignee_name.ilike.%{q}%")
        if from_date:
            x = x.gte("bilty_date", from_date)
        if to_date:
            x = x.lte("bilty_date", to_date)
        if payment_mode:
            x = x.in_("payment_mode", ["freeofcost"] if payment_mode == "foc" else [payment_mode])
        if branch_id:
            x = x.eq("branch_id", branch_id)
        out += [_from_regular(r) for r in x.order("bilty_date", desc=True).limit(limit).execute().data or []]
    if bilty_type in ("both", "manual"):
        x = sb.table("station_bilty_summary").select(MANUAL_COLS)
        if q:
            x = x.or_(f"gr_no.ilike.%{q}%,consignor.ilike.%{q}%,consignee.ilike.%{q}%")
        if from_date:
            x = x.gte("created_at", from_date)
        if to_date:
            x = x.lte("created_at", f"{to_date}T23:59:59")
        if payment_mode:
            x = x.eq("payment_status", payment_mode)
        if branch_id:
            x = x.eq("branch_id", branch_id)
        if group_id:
            x = x.eq("group_id", group_id)
        try:
            out += [_from_manual(r) for r in x.order("created_at", desc=True).limit(limit).execute().data or []]
        except Exception as exc:
            if group_id and "group_id" in str(exc):
                return _err("Bilty groups need migrations 020 and 021 — run them first", 500)
            raise

    links = _active_links(sb, [(b["bilty_type"], b["bilty_id"]) for b in out])
    names = _ledger_names(sb, [l["ledger_id"] for l in links.values()])
    for b in out:
        l = links.get((b["bilty_type"], b["bilty_id"]))
        b["in_ledger"] = {"ledger_id": l["ledger_id"], "ledger_name": names.get(l["ledger_id"]), "link_id": l["id"]} if l else None
        b.pop("consignor_gst", None), b.pop("consignee_gst", None)
    out.sort(key=lambda b: b.get("bilty_date") or "", reverse=True)
    return {"status": "success", "data": out[:limit * 2], "count": len(out)}


def search_parties(party_type: str, q: str, limit: int = 20) -> dict:
    """Consignor / consignee master rows for the 'New party ledger' picker."""
    if party_type not in ("consignor", "consignee"):
        return _err("type must be 'consignor' or 'consignee'")
    table = "consignors" if party_type == "consignor" else "consignees"
    rows = get_supabase().table(table).select("id, company_name, gst_num, number") \
        .ilike("company_name", f"%{(q or '').strip()}%").order("company_name").limit(min(limit, 100)).execute().data or []
    return {"status": "success", "data": rows}


def name_variants(party_type: str, q: str) -> dict:
    """Every spelling of a party name used on bilties (regular + manual) with counts —
    tick the ones that are the same party to fill match_names."""
    if party_type not in ("consignor", "consignee") or not (q or "").strip():
        return _err("type ('consignor'/'consignee') and q are required")
    sb = get_supabase()
    tokens = re.findall(r"[A-Za-z0-9]+", q)
    pat = "%" + "%".join(tokens) + "%"
    counts: dict[str, dict] = {}
    for table, col in (("bilty", f"{party_type}_name"), ("station_bilty_summary", party_type)):
        for r in _paged(lambda t=table, c=col: sb.table(t).select(c).ilike(c, pat)):
            name = (r.get(col) or "").strip()
            if name:
                e = counts.setdefault(name, {"name": name, "regular": 0, "manual": 0, "normalized": norm(name)})
                e["regular" if table == "bilty" else "manual"] += 1
    data = sorted(counts.values(), key=lambda e: -(e["regular"] + e["manual"]))
    return {"status": "success", "data": data}
