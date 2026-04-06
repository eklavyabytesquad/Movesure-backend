"""
Reference Data Preload Service
Single endpoint that returns ALL data the bilty page needs in one call.
Replaces 8 separate Supabase calls from the frontend.
Uses ThreadPoolExecutor for true parallel DB queries.
"""
from concurrent.futures import ThreadPoolExecutor, as_completed
from services.supabase_client import get_supabase


def get_reference_data(branch_id: str, user_id: str) -> dict:
    """
    Load all reference data needed for the bilty page in one shot.
    All 7 queries run in parallel via threads — typically completes in ~200-400ms.
    """
    try:
        sb = get_supabase()

        def fetch_branch():
            return (
                sb.table("branches")
                .select("id, branch_code, city_code, address, branch_name, default_bill_book_id")
                .eq("id", branch_id)
                .single()
                .execute()
            ).data

        def fetch_cities():
            return (
                sb.table("cities")
                .select("id, city_code, city_name")
                .order("city_name")
                .execute()
            ).data or []

        def fetch_transports():
            return (
                sb.table("transports")
                .select("id, transport_name, city_id, city_name, gst_number, mob_number, address, branch_owner_name, transport_admin_id, is_prior")
                .execute()
            ).data or []

        def fetch_consignors():
            return (
                sb.table("consignors")
                .select("id, company_name, gst_num, number")
                .order("company_name")
                .execute()
            ).data or []

        def fetch_consignees():
            return (
                sb.table("consignees")
                .select("id, company_name, gst_num, number")
                .order("company_name")
                .execute()
            ).data or []

        def fetch_rates():
            return (
                sb.table("rates")
                .select("id, branch_id, city_id, consignor_id, rate, is_default")
                .eq("branch_id", branch_id)
                .execute()
            ).data or []

        def fetch_bill_books():
            return (
                sb.table("bill_books")
                .select("id, prefix, from_number, to_number, digits, postfix, current_number, is_fixed, auto_continue, consignor_id")
                .eq("branch_id", branch_id)
                .eq("is_active", True)
                .eq("is_completed", False)
                .execute()
            ).data or []

        # Run ALL queries in parallel
        results = {}
        with ThreadPoolExecutor(max_workers=7) as pool:
            futures = {
                pool.submit(fetch_branch): "branch",
                pool.submit(fetch_cities): "cities",
                pool.submit(fetch_transports): "transports",
                pool.submit(fetch_consignors): "consignors",
                pool.submit(fetch_consignees): "consignees",
                pool.submit(fetch_rates): "rates",
                pool.submit(fetch_bill_books): "bill_books",
            }
            for future in as_completed(futures):
                key = futures[future]
                results[key] = future.result()

        # Build city lookup maps for the frontend to cache
        cities = results["cities"]
        city_by_id = {c["id"]: c for c in cities}
        city_by_code = {c["city_code"]: c for c in cities}

        # Build transport lookup by city_id
        # Key = city_id, Value = list of transports for that city
        # is_prior=true transport comes first (auto-selected by frontend)
        transports = results["transports"]
        transport_by_city_id = {}
        for t in transports:
            cid = t.get("city_id")
            if cid:
                transport_by_city_id.setdefault(cid, []).append(t)
        for cid in transport_by_city_id:
            transport_by_city_id[cid].sort(key=lambda t: not t.get("is_prior", False))

        return {
            "status": "success",
            "data": {
                "branch": results["branch"],
                "cities": cities,
                "city_by_id": city_by_id,
                "city_by_code": city_by_code,
                "transports": transports,
                "transport_by_city_id": transport_by_city_id,
                "consignors": results["consignors"],
                "consignees": results["consignees"],
                "rates": results["rates"],
                "bill_books": results["bill_books"],
            },
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to load reference data: {str(e)}",
            "status_code": 500,
        }
