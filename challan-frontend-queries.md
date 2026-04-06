# Challan Page — Initial Load Breakdown

What exactly happens when the challan page loads for the first time.

---

## 4 Sequential API Requests

All requests are **sequential** (not parallel) to avoid Render thread pool exhaustion.

```
Step 1 → GET /api/challan/init?branch_id=X           (reference data)
Step 2 → GET /api/challan/list?branch_id=X&page_size=10000  (ALL challans)
Step 3 → GET /api/challan/transit/available?branch_id=X      (available bilties)
Step 4 → GET /api/challan/transit/bilties/{challan_no}       (transit bilties for auto-selected challan)
```

---

## Step 1: `GET /api/challan/init?branch_id={user.branch_id}`

**Single Supabase RPC call** (`get_challan_init`) — returns 5 datasets in 1 DB round-trip.

| Data | Filter | Stored In | Used By |
|------|--------|-----------|---------|
| `user_branch` | `id = branch_id` | `userBranch` | TransitHeader, PDF header |
| `branches` | All (no filter) | `branches` | ChallanSelector (branch names), BiltyList (branch filter) |
| `cities` | All (no filter) | `cities` | BiltyList (city filter) |
| `permanent_details` | All (company details) | `permanentDetails` | ChallanPDFPreview (company name, logo, address) |
| `challan_books` | `is_active=true`, `is_completed=false`, `branch_1/2/3 = branch_id` | `challanBooks` | ChallanSelector book dropdown — **only books assigned to user's branch** |

> **Note:** Init also returns `challans` (last 50) but we **ignore** those and fetch all challans in Step 2 instead.

---

## Step 2: `GET /api/challan/list?branch_id={user.branch_id}&page_size=10000`

Fetches **ALL challans** for this branch (active + dispatched). No 50-row limit.

| Data | Filter | Stored In | Used By |
|------|--------|-----------|---------|
| `rows` | `branch_id`, `is_active=true`, all pages | `challans` | ChallanSelector dropdown (Active + Dispatched sections) |

Each challan row includes resolved names via SQL JOINs:
- `truck_number` (from `trucks` table)
- `driver_name` (from `staff` table)
- `owner_name` (from `staff` table)
- `created_by` (user name from `users` table)
- `total_bilty_count` (maintained by server on add/remove)

**Transform:** Each row is passed through `transformChallanRow()` to create nested objects:
```
truck_number → truck.truck_number
driver_name  → driver.name
owner_name   → owner.name
```
This is needed because ChallanSelector/ChallanPDFPreview expect nested objects.

**After this step:** Auto-selects the most recent active (non-dispatched) challan. If none, falls back to the most recent dispatched challan.

---

## Step 3: `GET /api/challan/transit/available?branch_id={user.branch_id}&page=1&page_size=1000`

Fetches bilties **not assigned to any challan** for this branch.

| Data | Filter | Stored In | Used By |
|------|--------|-----------|---------|
| Regular bilties | `source_table = 'bilty'` | `bilties` | BiltyList "Available" tab (Reg section) |
| Station bilties | `source_table = 'station_bilty_summary'` | `stationBilties` | BiltyList "Available" tab (Mnl section) |
| `total` count | — | `totalAvailableCount` | TransitHeader stats |

Each row includes:
- `bilty_type`: `"reg"` (bilty table) or `"mnl"` (station_bilty_summary)
- `source_table`: `"bilty"` or `"station_bilty_summary"`
- Full bilty details: `gr_no`, `consignor_name`, `consignee_name`, `weight`, `packages`, etc.

**Server-side:** Uses RPC `get_available_gr_numbers` with `NOT EXISTS` against `transit_details`. Deduplicates — if same GR exists in both tables, prefers `bilty`.

---

## Step 4: `GET /api/challan/transit/bilties/{challan_no}`

Fetches bilties loaded into the **auto-selected challan**.

| Data | Filter | Stored In | Used By |
|------|--------|-----------|---------|
| Transit bilties | `challan_no` match | `transitBilties` | BiltyList "Transit" tab, ChallanSelector bilty counts |

Each row includes:
- `bilty_type`: `"reg"` or `"mnl"` — used for count breakdown in ChallanSelector
- `transit_id`: needed for remove operations
- `source_table`: `"bilty"` or `"station_bilty_summary"`
- Enriched bilty details from the appropriate source table

---

## After Load — Auto-Selection

| What | Logic |
|------|-------|
| **Challan** | First active (non-dispatched) challan. If none → first dispatched challan. |
| **Challan Book** | First available book (already branch-filtered by init). |

---

## What's Branch-Filtered vs Global

| Branch-Filtered (`user.branch_id`) | Global (all rows) |
|-------------------------------------|-------------------|
| `challan_books` — `branch_1/2/3 = branch_id` | `branches` — all branches |
| `challans` — `branch_id` match | `cities` — all cities |
| `available bilties` — `branch_id` match | `permanent_details` — company info |
| `user_branch` — user's own branch | |
| `transit bilties` — by `challan_no` (challan is already branch-scoped) | |

---

## Summary

```
┌────────────────────────────────────────────────────────────────┐
│  CHALLAN PAGE INITIAL LOAD                                     │
├────────────────────────────────────────────────────────────────┤
│                                                                │
│  Request 1: GET /api/challan/init                              │
│  ├─ user_branch        (branch-filtered)                       │
│  ├─ branches           (global)                                │
│  ├─ cities             (global)                                │
│  ├─ permanent_details  (global)                                │
│  └─ challan_books      (branch-filtered: branch_1/2/3)        │
│                                                                │
│  Request 2: GET /api/challan/list?page_size=10000              │
│  └─ ALL challans       (branch-filtered, active+dispatched)    │
│                                                                │
│  Request 3: GET /api/challan/transit/available                  │
│  └─ available bilties  (branch-filtered, not in any challan)   │
│                                                                │
│  Request 4: GET /api/challan/transit/bilties/{challan_no}      │
│  └─ transit bilties    (for auto-selected challan)             │
│                                                                │
│  Auto-select: first active challan + first challan book        │
└────────────────────────────────────────────────────────────────┘
```
Key differences:

Old (Supabase direct)	New (Render API)
Where it runs	Supabase PostgREST — unlimited capacity	Render free tier — limited thread pool
Limit	None — returns ALL challans	page_size=10000 through Render
JOINs	PostgREST embedded JOINs (native, fast)	Server-side SQL JOINs (extra layer)
Server load	Zero on Render	Heavy — Render processes 10K rows
The old approach had no Render load because Supabase handles the query directly. The new approach routes everything through Render with page_size=10000, which is heavy.

// OLD — goes straight to Supabase PostgREST (NOT through Render)
supabase
  .from('challan_details')
  .select(`id, challan_no, ..., 
    truck:trucks(...), 
    owner:staff!challan_details_owner_id_fkey(...), 
    driver:staff!challan_details_driver_id_fkey(...)
  `)
  .eq('branch_id', user.branch_id)
  .eq('is_active', true)
  .order('is_dispatched', { ascending: true })
  .order('created_at', { ascending: false })