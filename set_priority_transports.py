"""
One-time script: Set is_prior=true for the oldest transport per city.
Does NOT delete anything — only updates the is_prior flag.

Usage:
    python set_priority_transports.py

What it does:
    1. Fetches ALL transports (id, city_id, transport_name, created_at, is_prior)
    2. Groups them by city_id
    3. For each city, picks the OLDEST transport (earliest created_at)
    4. Sets is_prior=true for that transport (if not already)
    5. Prints a summary of all changes
"""
from services.supabase_client import get_supabase
from collections import defaultdict


def run():
    sb = get_supabase()

    # Step 1: Fetch all transports
    print("Fetching all transports...")
    resp = (
        sb.table("transports")
        .select("id, city_id, city_name, transport_name, is_prior")
        .order("id", desc=False)
        .execute()
    )
    transports = resp.data or []
    print(f"Total transports: {len(transports)}")

    if not transports:
        print("No transports found. Exiting.")
        return

    # Step 2: Group by city_id
    by_city = defaultdict(list)
    for t in transports:
        cid = t.get("city_id")
        if cid:
            by_city[cid].append(t)

    print(f"Cities with transports: {len(by_city)}")

    # Step 3: For each city, find the oldest transport
    to_set_prior = []      # IDs to set is_prior = true
    already_prior = 0

    for city_id, city_transports in by_city.items():
        # Sorted by id (ASC) from query — first entry is the oldest
        oldest = city_transports[0]
        city_name = oldest.get("city_name", "UNKNOWN")

        if oldest.get("is_prior") is True:
            already_prior += 1
            continue

        to_set_prior.append({
            "id": oldest["id"],
            "transport_name": oldest["transport_name"],
            "city_name": city_name,
            "city_id": city_id,
            "total_in_city": len(city_transports),
        })

    print(f"\nAlready has is_prior=true (oldest): {already_prior}")
    print(f"Need to set is_prior=true: {len(to_set_prior)}")

    if not to_set_prior:
        print("\nNothing to update. All cities already have priority set on oldest transport.")
        return

    # Step 4: Preview changes
    print(f"\n{'='*80}")
    print(f"{'CITY':<25} {'PRIORITY TRANSPORT':<40} {'TOTAL'}")
    print(f"{'='*80}")
    for item in sorted(to_set_prior, key=lambda x: x["city_name"]):
        print(
            f"{item['city_name']:<25} "
            f"{item['transport_name'][:38]:<40} "
            f"{item['total_in_city']}"
        )

    # Step 5: Confirm and update
    print(f"\nThis will set is_prior=true for {len(to_set_prior)} transports.")
    confirm = input("Proceed? (y/n): ").strip().lower()

    if confirm != "y":
        print("Aborted. No changes made.")
        return

    # Bulk update — one query per transport (Supabase doesn't support bulk UPDATE by different IDs in one call)
    success = 0
    failed = 0
    for item in to_set_prior:
        try:
            sb.table("transports").update({"is_prior": True}).eq("id", item["id"]).execute()
            success += 1
        except Exception as e:
            print(f"  FAILED: {item['transport_name']} in {item['city_name']} — {e}")
            failed += 1

    print(f"\nDone! Updated: {success}, Failed: {failed}")
    print("Priority transports have been set. The API will now return them first per city.")


if __name__ == "__main__":
    run()
