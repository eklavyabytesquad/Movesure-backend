# Challan & Transit Management API

Manages the **full challan lifecycle**: challan book number series → challan creation → bilty loading → dispatch → hub receipt → delivery tracking.

**Base URL:** `/api/challan`

---

## Key Fixes (vs direct-from-frontend)

| Problem | Server-side Fix |
|---------|----------------|
| Bilties added to dispatched challans | `add_to_transit` validates `is_dispatched=false` before insert |
| Duplicate GR numbers in transit | Checks `transit_details` for existing GR before insert |
| Same GR in bilty + station_bilty_summary | Deduplication: prefers `bilty` source over station |
| Bilties removed from dispatched challans | `remove_from_transit` validates dispatch lock |
| Empty challan dispatched | `dispatch` rejects if `total_bilty_count=0` |
| Challan count out of sync | Auto-updates `total_bilty_count` on every add/remove |
| Available bilties performance (Render 500) | Uses Supabase RPC `get_available_gr_numbers` — single DB query instead of fetching all rows client-side |
| GR with mismatched bilty_id (recreated bilties) | SQL function matches on `gr_no` only, not `bilty_id`, so recreated bilties are correctly excluded |
| All bilties showing as "reg" type | Added `bilty_type` field: `"reg"` (from bilty table) / `"mnl"` (from station_bilty_summary) |
| 4 sequential requests on page load | Single RPC `get_challan_page_init` replaces init + list + available (4→2 requests) |
| Dispatched challans not visible on init | Returns ALL active challans (dispatched + non-dispatched), sorted non-dispatched first |
| Slow init (9+ DB round-trips) | Single Supabase RPC — 1 DB call, JOINs for name resolution, includes available bilties |
| Challan books from all branches showing | Filtered to `branch_1 = branch_id` — only books where user's branch is the sender |
| Only 50 challans returned | Now returns ALL challans (lightweight: no bilty details per challan) |
| Only 50 transit bilties returned | Transit bilties endpoint now defaults to `page_size=10000` — returns ALL bilties in a challan |
| Available bilties filtered by branch | Init RPC now returns available bilties from ALL branches (no `branch_id` filter) |
| `contain` field missing from available bilties | RPC now includes `b.contain` (goods description) for regular bilties |

---

## Endpoints Overview

### Challan Books
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/challan/books` | List challan books |
| GET | `/api/challan/books/{id}` | Get single book |
| POST | `/api/challan/books` | Create book |
| PUT | `/api/challan/books/{id}` | Update book |

### Challans
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/challan/init` | **Combined page-load fetch** (all reference data + ALL challans + available bilties) |
| GET | `/api/challan/list` | List challans (paginated, for search/filter) |
| GET | `/api/challan/{id}` | Get single challan |
| POST | `/api/challan/create` | Create challan (auto-generates number) |
| PUT | `/api/challan/{id}` | Update challan (blocked if dispatched) |
| POST | `/api/challan/{id}/dispatch` | Dispatch challan (locks it) |
| POST | `/api/challan/{id}/undispatch` | Reopen challan (if not at hub yet) |
| POST | `/api/challan/{id}/hub-received` | Mark received at hub |
| DELETE | `/api/challan/{id}` | Soft-delete challan (blocked if dispatched) |

### Transit (Bilty ↔ Challan)
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/challan/transit/available` | Available bilties (not in any challan) |
| GET | `/api/challan/transit/bilties/{challan_no}` | Bilties in a specific challan |
| GET | `/api/challan/transit/stats/{challan_no}` | Stats: weight, packages, EWBs, payments |
| POST | `/api/challan/transit/add` | Add bilties to challan (with all validations) |
| POST | `/api/challan/transit/remove/{transit_id}` | Remove single bilty from transit |
| POST | `/api/challan/transit/bulk-remove` | Remove multiple bilties from transit |
| PUT | `/api/challan/transit/delivery-status` | Bulk update delivery pipeline stages |

---

## API Details

### 0. Combined Page-Load Init (Single RPC)

One RPC call (`get_challan_page_init`) replaces 3 API requests — returns reference data + ALL challans + available bilties.

```js
const res = await fetch(`${API}/api/challan/init?branch_id=${user.branch_id}`);
const { data } = await res.json();
// data.user_branch        = { id, name, ... }           ← user's branch
// data.branches           = [{ id, name, ... }, ...]    ← all branches
// data.cities             = [{ id, name, ... }, ...]    ← all cities
// data.permanent_details  = [{ ... }]                   ← company details (PDF header)
// data.challan_books      = [{ id, prefix, ... }, ...]  ← active books for THIS branch only
// data.challans           = [{                          ← ALL active challans (non-dispatched first)
//     id, challan_no, truck_number, driver_name, owner_name,
//     is_dispatched, dispatch_date, total_bilty_count, ...
//   }]
// data.available_bilties  = [{ gr_no, source_table, bilty_type, ... }]  ← bilties not in any challan
// data.regular_count      = 120   ← count of "reg" bilties
// data.station_count      = 45    ← count of "mnl" bilties
```

**Key points:**
- **Challans** — ALL active challans (no limit), lightweight fields only (challan_no, dispatch details, bilty count, truck/driver/owner names). Sorted: non-dispatched first, then dispatched, both by `created_at DESC`.
- **Challan books** — filtered to `branch_1 = branch_id`. User only sees books where their branch is the sender (first branch).
- **Available bilties** — bilties not in any challan, from **ALL branches** (no branch filter). Each row has `bilty_type` (`"reg"` or `"mnl"`), `source_table`, and `contain` (goods description, regular bilties only).
- **Names resolved via SQL JOINs** — `truck_number`, `driver_name`, `owner_name`, `created_by` (user name) — no extra API calls.

#### Frontend Transform

Pass each challan through `transformChallanRow()` to create nested objects:
```js
// challan from init has flat fields:
//   truck_number, driver_name, owner_name
// Transform to nested objects for ChallanSelector/ChallanPDFPreview:
//   truck: { truck_number }, driver: { name }, owner: { name }
```

Auto-select: first active (non-dispatched) challan. If none → first dispatched. First challan book (already branch-filtered).

#### Separate Available Bilties

From the `available_bilties` array, split into reg and station:
```js
const bilties = data.available_bilties.filter(b => b.bilty_type === 'reg');
const stationBilties = data.available_bilties.filter(b => b.bilty_type === 'mnl');
```

**Before vs After:**
| Before (4 sequential requests) | After (2 requests) |
|---|---|
| GET /api/challan/init | **GET /api/challan/init** (1 RPC = everything) |
| GET /api/challan/list?page_size=10000 | *(included in init)* |
| GET /api/challan/transit/available | *(included in init)* |
| GET /api/challan/transit/bilties/{no} | GET /api/challan/transit/bilties/{no} |

**Result:** 4 sequential requests → 2 requests on page load (init + transit bilties for selected challan).

---

### 1. Challan Books

#### List Books
```js
// Get active books for a branch
const res = await fetch(`${API}/api/challan/books?branch_id=${branchId}&active_only=true`);
const { data } = await res.json();
// data.rows = [{ id, prefix, from_number, to_number, current_number, from_branch_id, to_branch_id, ... }]
```

#### Create Book
```js
await fetch(`${API}/api/challan/books`, {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({
    prefix: 'CH',
    from_number: 1,
    to_number: 500,
    digits: 4,
    postfix: '',
    from_branch_id: 'origin-branch-uuid',
    to_branch_id: 'destination-branch-uuid',
    branch_1: 'origin-branch-uuid',
    branch_2: null,  // optional
    branch_3: null,  // optional
    created_by: user.id,
  }),
});
```

#### Update Book
```js
await fetch(`${API}/api/challan/books/${bookId}`, {
  method: 'PUT',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({ is_active: false }),
});
```

---

### 2. Challans

#### List Challans
```js
// Active (non-dispatched) challans for a branch
const res = await fetch(`${API}/api/challan/list?branch_id=${branchId}&is_dispatched=false`);
const { data } = await res.json();
// data.rows = [{ id, challan_no, truck_number, driver_name, owner_name, total_bilty_count, ... }]

// All challans (active + dispatched)
const all = await fetch(`${API}/api/challan/list?branch_id=${branchId}`);

// Search by challan number
const search = await fetch(`${API}/api/challan/list?search=CH0001`);
```

**Response includes resolved names:**
```json
{
  "id": "uuid",
  "challan_no": "CH0042",
  "truck_id": "truck-uuid",
  "truck_number": "UP-32-AT-1234",
  "driver_id": "staff-uuid",
  "driver_name": "RAMESH",
  "owner_id": "staff-uuid",
  "owner_name": "SURESH",
  "total_bilty_count": 25,
  "is_dispatched": false,
  "created_by": "EKLAVYA SINGH"
}
```

#### Create Challan
```js
const res = await fetch(`${API}/api/challan/create`, {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({
    challan_book_id: 'book-uuid',   // REQUIRED — auto-generates challan_no
    branch_id: user.branch_id,       // REQUIRED
    truck_id: 'truck-uuid',          // REQUIRED
    driver_id: 'driver-uuid',        // REQUIRED
    owner_id: 'owner-uuid',          // optional
    date: '2026-04-06',              // optional, defaults to today
    remarks: 'Special delivery',     // optional
    created_by: user.id,             // REQUIRED
  }),
});
// Returns: { challan_no: "CH0042", ... }
// Auto-increments challan_books.current_number
```

#### Update Challan (blocked if dispatched)
```js
await fetch(`${API}/api/challan/${challanId}`, {
  method: 'PUT',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({
    truck_id: 'new-truck-uuid',
    driver_id: 'new-driver-uuid',
    remarks: 'Updated remarks',
  }),
});
// Returns 400 if challan is dispatched
```

#### Dispatch Challan
```js
await fetch(`${API}/api/challan/${challanId}/dispatch`, {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({ user_id: user.id }),
});
// Returns 400 if: already dispatched, or 0 bilties loaded
// After dispatch: challan is READ-ONLY (no add/remove/update)
```

#### Undispatch (Reopen)
```js
await fetch(`${API}/api/challan/${challanId}/undispatch`, {
  method: 'POST',
});
// Returns 400 if: not dispatched, or already received at hub
```

#### Hub Received
```js
await fetch(`${API}/api/challan/${challanId}/hub-received`, {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({ user_id: user.id }),
});
// Returns 400 if: not dispatched, or already received
```

#### Delete Challan
```js
await fetch(`${API}/api/challan/${challanId}`, { method: 'DELETE' });
// Returns 400 if dispatched
// Soft-deletes challan + removes all its transit_details
```

---

### 3. Transit — Available Bilties

Get bilties that are NOT assigned to any challan. Merges from `bilty` + `station_bilty_summary` tables.

Returns bilties from **all branches** by default. Pass `branch_id` to narrow down to a specific branch.

**Architecture:** Uses the Supabase RPC function `get_available_gr_numbers(p_limit, p_offset)` which runs a single SQL query with `NOT EXISTS` joins against `transit_details`. This avoids fetching all bilties client-side — critical for production (24K+ rows caused Render thread pool exhaustion before this fix).

**How it works:**
1. Calls RPC `get_available_gr_numbers` → returns `(source_table, gr_no)` pairs for all available bilties
2. Applies `source` filter if provided (bilty/station)
3. Fetches full details only for the available GR numbers (in chunks of 500)
4. Applies client-side filters: `branch_id`, `search`, `payment_mode`, `city_id`
5. Paginates the final result

```js
// All available bilties (all branches)
const res = await fetch(`${API}/api/challan/transit/available?page=1&page_size=50`);
const { data } = await res.json();
// data.rows = [{ gr_no, source_table, branch_id, consignor_name, ... }]
// data.regular_count = 120  (from bilty table)
// data.station_count = 45   (from station_bilty_summary table)
// data.total = 165

// Filter by branch
const branchRes = await fetch(
  `${API}/api/challan/transit/available?branch_id=${branchId}&page=1&page_size=50`
);
```

**Query Parameters:**
| Param | Required | Description |
|-------|----------|-------------|
| `branch_id` | No | Filter by branch (omit to get all branches) |
| `page` | No | Page number (default 1) |
| `page_size` | No | Rows per page (default 50) |
| `search` | No | Search gr_no, consignor, consignee, transport |
| `payment_mode` | No | Filter by payment mode (PAID, TO-PAY, etc.) |
| `city_id` | No | Filter by destination city |
| `source` | No | `"bilty"` or `"station"` (default: both) |

**Deduplication:** If same GR_NO exists in both `bilty` and `station_bilty_summary`, the `bilty` record is preferred.

Each row has `source_table` field: `"bilty"` or `"station_bilty_summary"`.

Each row also has `bilty_type` field: `"reg"` (from bilty table) or `"mnl"` (from station_bilty_summary).

---

### 4. Transit — Bilties in a Challan

Returns **ALL** bilties in a challan (default `page_size=10000` — no pagination needed for typical challans).

```js
const res = await fetch(
  `${API}/api/challan/transit/bilties/${challanNo}`
);
const { data } = await res.json();
// data.rows = [{ id, challan_no, gr_no, consignor_name, consignee_name, contain, source_table, ... }]
// data.total = 86  ← total bilties in this challan
```

Each transit row includes `bilty_type`: `"reg"` for bilty-table rows, `"mnl"` for station_bilty_summary rows.

Each transit row is enriched with bilty details (consignor, consignee, transport, weight, packages, **contain** (goods description), etc.) from the appropriate source table.

**`contain` field:** For regular bilties (`bilty_type: "reg"`), mapped from `bilty.contain`. For station bilties (`bilty_type: "mnl"`), mapped from `station_bilty_summary.contents`. Both are returned as `contain` in the response for consistent frontend usage.

---

### 5. Transit — Challan Stats

```js
const res = await fetch(`${API}/api/challan/transit/stats/${challanNo}`);
const { data } = await res.json();
```

**Response:**
```json
{
  "total": 25,
  "regular": 20,
  "station": 5,
  "total_weight": 1250.5,
  "total_packages": 340,
  "ewb_count": 18,
  "topay_amount": 45000.00,
  "paid_amount": 32000.00
}
```

---

### 6. Transit — Add Bilties to Challan

**The most important endpoint — contains ALL the validation logic.**

```js
const res = await fetch(`${API}/api/challan/transit/add`, {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({
    challan_id: 'challan-uuid',       // REQUIRED
    challan_book_id: 'book-uuid',     // REQUIRED (determines destination branch)
    user_id: user.id,                  // REQUIRED
    bilties: [                         // REQUIRED (non-empty array)
      {
        gr_no: 'GR0001',
        bilty_id: 'bilty-uuid',       // null for station bilties
        source_table: 'bilty',        // "bilty" or "station_bilty_summary"
      },
      {
        gr_no: 'GR0002',
        bilty_id: null,
        source_table: 'station_bilty_summary',
      },
    ],
  }),
});
```

**Server-side validations:**
1. Challan must exist, be active, and **NOT dispatched**
2. Challan book must exist (provides `to_branch_id`)
3. Input is deduplicated (same GR from both sources → prefer `bilty`)
4. Checks `transit_details` for existing GR numbers → skips duplicates
5. Auto-updates `challan_details.total_bilty_count`

**Response:**
```json
{
  "status": "success",
  "message": "8 bilties added to challan CH0042",
  "data": {
    "added": 8,
    "skipped": ["GR0099"],
    "total_bilty_count": 25
  }
}
```

---

### 7. Transit — Remove Bilties

#### Single Remove
```js
await fetch(`${API}/api/challan/transit/remove/${transitId}`, {
  method: 'POST',
});
// Returns 400 if challan is dispatched
// Auto-decrements total_bilty_count
```

#### Bulk Remove
```js
await fetch(`${API}/api/challan/transit/bulk-remove`, {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({
    transit_ids: ['transit-uuid-1', 'transit-uuid-2', 'transit-uuid-3'],
  }),
});
```

**Response:**
```json
{
  "status": "success",
  "message": "3 bilties removed from transit",
  "data": { "removed": 3, "total_bilty_count": 22 }
}
```

---

### 8. Transit — Delivery Status Update

Update delivery pipeline stages for multiple bilties at once.

```js
await fetch(`${API}/api/challan/transit/delivery-status`, {
  method: 'PUT',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({
    user_id: user.id,
    updates: [
      { id: 'transit-uuid-1', stage: 'out_from_branch1' },
      { id: 'transit-uuid-2', stage: 'delivered_at_branch2' },
      { id: 'transit-uuid-3', stage: 'out_from_branch2' },
      { id: 'transit-uuid-4', stage: 'delivered_at_destination' },
      {
        id: 'transit-uuid-5',
        stage: 'door_delivery',
        delivery_agent_name: 'RAMESH',
        delivery_agent_phone: '9876543210',
        vehicle_number: 'UP32-AT-1234',
        remarks: 'Delivered to guard',
      },
    ],
  }),
});
```

**Valid stages:**
| Stage Key | Sets Flag | Sets Date |
|-----------|-----------|-----------|
| `out_from_branch1` | `is_out_of_delivery_from_branch1` | `out_of_delivery_from_branch1_date` |
| `delivered_at_branch2` | `is_delivered_at_branch2` | `delivered_at_branch2_date` |
| `out_from_branch2` | `is_out_of_delivery_from_branch2` | `out_of_delivery_from_branch2_date` |
| `delivered_at_destination` | `is_delivered_at_destination` | `delivered_at_destination_date` |
| `door_delivery` | `out_for_door_delivery` | `out_for_door_delivery_date` |

**Response:**
```json
{
  "status": "success",
  "message": "Delivery status updated: 5 success, 0 failed",
  "data": { "success_count": 5, "failed_count": 0, "failed": [] }
}
```

---

## Complete Flow (Frontend → API)

```
1. PAGE LOAD (2 requests only)
   └─ GET /api/challan/init?branch_id=X                       → ALL data (1 RPC): branches, cities, books, ALL challans, available bilties
   └─ GET /api/challan/transit/bilties/{challan_no}            → transit bilties for auto-selected challan

2. SELECT A CHALLAN
   ├─ GET /api/challan/transit/bilties/{challan_no}            → transit bilties tab
   └─ GET /api/challan/transit/stats/{challan_no}              → stats header

3. ADD BILTIES
   └─ POST /api/challan/transit/add
      { challan_id, challan_book_id, user_id, bilties: [...] }
      → Server validates dispatch lock, deduplicates, inserts, updates count
      → Refresh available + transit lists

4. REMOVE BILTIES
   └─ POST /api/challan/transit/remove/{transit_id}
      or POST /api/challan/transit/bulk-remove { transit_ids }
      → Server validates dispatch lock, deletes, updates count
      → Bilty goes back to available pool

5. DISPATCH
   └─ POST /api/challan/{id}/dispatch { user_id }
      → Locks challan: no more add/remove/update

6. HUB RECEIPT
   └─ POST /api/challan/{id}/hub-received { user_id }

7. DELIVERY TRACKING
   └─ PUT /api/challan/transit/delivery-status { updates: [...] }
```

---

## Error Responses

```json
{ "status": "error", "message": "Cannot add bilties to a dispatched challan" }
```

| Code | When |
|------|------|
| 400 | Dispatched challan (add/remove/update blocked), empty challan (dispatch blocked), missing fields, invalid stage |
| 404 | Challan/book/transit record not found |
| 500 | Server error |

---

## Supabase SQL Function

The available bilties endpoint relies on a Supabase RPC function. If you need to recreate or update it:

```sql
CREATE OR REPLACE FUNCTION get_available_gr_numbers(p_limit INT, p_offset INT)
RETURNS TABLE(source_table VARCHAR(50), gr_no VARCHAR)
LANGUAGE sql STABLE
AS $$
  -- Bilties not in any transit
  SELECT 'bilty'::VARCHAR(50) AS source_table, b.gr_no::VARCHAR
  FROM bilty b
  WHERE NOT EXISTS (
    SELECT 1 FROM transit_details td WHERE td.gr_no = b.gr_no
  )
  UNION ALL
  -- Station bilties not in any transit (and not duplicated in bilty)
  SELECT 'station_bilty_summary'::VARCHAR(50) AS source_table, s.gr_no::VARCHAR
  FROM station_bilty_summary s
  WHERE NOT EXISTS (
    SELECT 1 FROM transit_details td WHERE td.gr_no = s.gr_no
  )
  AND NOT EXISTS (
    SELECT 1 FROM bilty b WHERE b.gr_no = s.gr_no
  )
  LIMIT p_limit OFFSET p_offset;
$$;
```

**Key design decisions:**
- Matches on `gr_no` only (not `bilty_id`) to handle cases where a bilty was deleted and recreated with a different UUID
- Station bilties are excluded if the same GR exists in the `bilty` table (deduplication)
- Returns `VARCHAR` types to match table column types (avoids TEXT vs VARCHAR mismatch)

### RPC: `get_challan_page_init`

The init endpoint uses this single RPC to fetch everything the page needs — reference data, ALL challans, and available bilties — in one DB call:

```sql
CREATE OR REPLACE FUNCTION get_challan_page_init(p_branch_id UUID)
RETURNS JSON
LANGUAGE plpgsql
AS $$
DECLARE
  result JSON;
BEGIN
  SELECT json_build_object(
    -- Challan books: active + incomplete, for this branch only
    'challan_books', COALESCE((
      SELECT json_agg(row_to_json(cb_sub))
      FROM (
        SELECT id, prefix, from_number, to_number, digits, postfix,
               current_number, from_branch_id, to_branch_id,
               branch_1, branch_2, branch_3,
               is_active, is_completed, is_fixed, auto_continue,
               created_by, created_at, updated_at
        FROM challan_books
        WHERE is_active = true
          AND is_completed = false
          AND branch_1 = p_branch_id
        ORDER BY created_at DESC
      ) cb_sub
    ), '[]'::json),

    -- ALL challans: lightweight, names resolved via JOINs, non-dispatched first
    'challans', COALESCE((
      SELECT json_agg(row_to_json(ch_sub))
      FROM (
        SELECT cd.id, cd.challan_no, cd.branch_id, cd.truck_id, cd.owner_id,
               cd.driver_id, cd.date, cd.total_bilty_count, cd.remarks,
               cd.is_active, cd.is_dispatched, cd.dispatch_date,
               cd.is_received_at_hub, cd.received_at_hub_timing, cd.received_by_user,
               COALESCE(u.name, cd.created_by::text) AS created_by,
               cd.created_at, cd.updated_at,
               t.truck_number,
               s_driver.name AS driver_name,
               s_owner.name AS owner_name
        FROM challan_details cd
        LEFT JOIN trucks t ON t.id = cd.truck_id
        LEFT JOIN staff s_driver ON s_driver.id = cd.driver_id
        LEFT JOIN staff s_owner ON s_owner.id = cd.owner_id
        LEFT JOIN users u ON u.id = cd.created_by
        WHERE cd.branch_id = p_branch_id
          AND cd.is_active = true
        ORDER BY cd.is_dispatched ASC, cd.created_at DESC
      ) ch_sub
    ), '[]'::json),

    -- Available regular bilties: ALL branches (no branch filter)
    'available_regular', COALESCE((
      SELECT json_agg(row_to_json(ar_sub))
      FROM (
        SELECT b.id, b.gr_no, b.branch_id, b.bilty_date, b.delivery_type,
               b.consignor_name, b.consignor_gst, b.consignor_number,
               b.consignee_name, b.consignee_gst, b.consignee_number,
               b.transport_name, b.transport_gst, b.transport_number, b.transport_id,
               b.payment_mode, b.no_of_pkg, b.wt, b.rate, b.freight_amount,
               b.labour_charge, b.bill_charge, b.toll_charge, b.dd_charge,
               b.other_charge, b.pf_charge, b.total,
               b.from_city_id, b.to_city_id, b.e_way_bill, b.pvt_marks,
               b.contain, b.remark, b.saving_option, b.is_active
        FROM bilty b
        WHERE b.is_active = true
          AND b.consignor_name != 'CANCEL BILTY'
          AND NOT EXISTS (
            SELECT 1 FROM transit_details td WHERE td.gr_no = b.gr_no
          )
        ORDER BY b.created_at DESC
      ) ar_sub
    ), '[]'::json),

    -- Available station bilties: ALL branches (no branch filter)
    'available_station', COALESCE((
      SELECT json_agg(row_to_json(as_sub))
      FROM (
        SELECT s.id, s.gr_no, s.station, s.consignor, s.consignee, s.contents,
               s.no_of_packets, s.weight, s.payment_status, s.amount, s.pvt_marks,
               s.delivery_type, s.staff_id, s.branch_id, s.e_way_bill,
               s.transport_id, s.transport_name, s.transport_gst, s.city_id,
               s.created_at, s.updated_at
        FROM station_bilty_summary s
        WHERE NOT EXISTS (
            SELECT 1 FROM transit_details td WHERE td.gr_no = s.gr_no
          )
          AND NOT EXISTS (
            SELECT 1 FROM bilty b WHERE b.gr_no = s.gr_no
          )
        ORDER BY s.created_at DESC
      ) as_sub
    ), '[]'::json),

    -- All branches
    'branches', COALESCE((
      SELECT json_agg(row_to_json(br)) FROM branches br
    ), '[]'::json),

    -- All cities
    'cities', COALESCE((
      SELECT json_agg(row_to_json(c)) FROM cities c
    ), '[]'::json),

    -- Permanent details
    'permanent_details', COALESCE((
      SELECT json_agg(row_to_json(pd)) FROM permanent_details pd
    ), '[]'::json)

  ) INTO result;

  RETURN result;
END;
$$;
```

**What the RPC does in 1 call:**
| Dataset | Filter | JOINs / Notes |
|---------|--------|---------------|
| `challan_books` | `is_active=true`, `is_completed=false`, `branch_1 = branch_id` | none |
| `challans` | `branch_id`, `is_active=true`, ALL (no limit), non-dispatched first | trucks → truck_number, staff → driver_name/owner_name, users → created_by name |
| `available_regular` | `is_active=true`, not CANCEL, NOT EXISTS transit_details (ALL branches) | bilty table — full row details incl. `contain` |
| `available_station` | NOT EXISTS transit_details, NOT EXISTS bilty (dedup) (ALL branches) | station_bilty_summary — full row details |
| `branches` | all | none |
| `cities` | all | none |
| `permanent_details` | all | none |
