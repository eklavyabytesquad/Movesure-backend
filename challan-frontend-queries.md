# Challan & Transit Management System

## Overview

The Challan system manages the **dispatch of bilties (consignment notes) via trucks**. It connects bilties to challans (loading manifests), tracks them through transit stages, and handles the full lifecycle from loading to delivery.

---

## Database Tables

### 1. `challan_books` — Number Series for Challans

Controls how challan numbers are generated for each route.

| Field | Purpose |
|-------|---------|
| `prefix` | Challan number prefix (e.g., `"CH"`) |
| `from_number` / `to_number` | Number range (e.g., 1–500) |
| `digits` | Zero-padding (e.g., 4 → `CH0001`) |
| `postfix` | Suffix after number |
| `current_number` | Next number to use (auto-incremented) |
| `from_branch_id` / `to_branch_id` | Route: origin → destination branch |
| `branch_1`, `branch_2`, `branch_3` | Which branches can use this book |
| `is_active` / `is_completed` | Book status |

**Challan Number Formula:** `prefix + padStart(current_number, digits, '0') + postfix`

### 2. `challan_details` — The Challan (Loading Manifest)

Each challan represents **one truck load** going from origin branch to a destination.

| Field | Purpose |
|-------|---------|
| `challan_no` | Unique challan number (from challan book) |
| `branch_id` | Origin branch |
| `truck_id` | Assigned truck (FK → `trucks`) |
| `owner_id` | Truck owner (FK → `staff`) |
| `driver_id` | Assigned driver (FK → `staff`) |
| `date` | Challan date |
| `total_bilty_count` | Number of bilties loaded |
| `is_dispatched` | Whether truck has left |
| `dispatch_date` | When dispatch happened |
| `is_received_at_hub` | Hub receipt status |
| `received_at_hub_timing` | Hub receipt timestamp |
| `is_active` | Soft delete flag |

### 3. `transit_details` — Bilty-to-Challan Link + Delivery Tracking

Each row = **one bilty assigned to one challan**, with its delivery pipeline status.

| Field | Purpose |
|-------|---------|
| `challan_no` | Which challan this bilty is on |
| `gr_no` | Bilty GR number |
| `bilty_id` | FK to `bilty` table (null for station bilties) |
| `challan_book_id` | Which book was used |
| `from_branch_id` / `to_branch_id` | Route |

**Delivery Pipeline (5 stages):**

| Stage | Fields | Meaning |
|-------|--------|---------|
| 1 | `is_out_of_delivery_from_branch1` + date | Left origin branch |
| 2 | `is_delivered_at_branch2` + date | Arrived at destination branch |
| 3 | `is_out_of_delivery_from_branch2` + date | Out for local delivery |
| 4 | `is_delivered_at_destination` + date | Delivered to consignee |
| 5 | `out_for_door_delivery` + date + agent info | Door delivery details |

---

## Complete Flow

```
┌──────────────────────────────────────────────────────────┐
│                    CHALLAN LIFECYCLE                       │
├──────────────────────────────────────────────────────────┤
│                                                           │
│  1. CREATE CHALLAN BOOK (challan-settings page)           │
│     └─ Define number series + route (branch A → B)       │
│                                                           │
│  2. CREATE CHALLAN (challan-settings page)                │
│     ├─ Pick challan book → auto-generate challan_no      │
│     ├─ Assign truck → auto-fills owner                   │
│     ├─ Assign driver                                      │
│     ├─ INSERT into challan_details (is_dispatched=false)  │
│     └─ INCREMENT challan_books.current_number             │
│                                                           │
│  3. LOAD BILTIES (challan page)                           │
│     ├─ Select active challan from dropdown                │
│     ├─ Select challan book (sets destination branch)      │
│     ├─ View available bilties (not yet in any transit)    │
│     ├─ Select bilties → click "Add to Transit"            │
│     └─ INSERT rows into transit_details                   │
│                                                           │
│  4. DISPATCH (challan-settings page)                      │
│     ├─ Mark is_dispatched = true                          │
│     ├─ Set dispatch_date                                  │
│     └─ Challan becomes READ-ONLY                          │
│                                                           │
│  5. HUB RECEIPT (hub-management page)                     │
│     └─ Mark is_received_at_hub = true                     │
│                                                           │
│  6. DELIVERY TRACKING (hub-management page)               │
│     ├─ Update transit_details delivery stages              │
│     └─ Uses bulk_update_transit_status RPC                │
│                                                           │
└──────────────────────────────────────────────────────────┘
```

---

## How Available Bilties Are Loaded

Available bilties = bilties that exist but are **NOT assigned to any challan yet**.

```
1. Call DB function: get_available_gr_count() → total count
2. Call DB function: get_available_gr_numbers(limit, offset) → list of GR numbers + source table
3. Each GR number has a source_table: 'bilty' or 'station_bilty_summary'
4. Fetch full details from respective tables using the GR numbers
5. Process and merge both lists with city/branch name resolution
6. Sort by GR number
```

**Two sources of bilties:**
- **`bilty` table** — Regular bilties created from the bilty page (full details: consignor, consignee, charges, etc.)
- **`station_bilty_summary` table** — Station/manual bilties received from other branches (lighter data)

**Filtering out already-in-transit bilties** is done at the database level by the `get_available_gr_numbers` function.

---

## Adding Bilties to Transit

When user clicks **"Add to Transit"**:

```
1. VALIDATE
   ├─ Challan must be selected (and NOT dispatched)
   ├─ Challan book must be selected
   └─ At least one bilty must be selected

2. DEDUPLICATE
   ├─ Group by gr_no
   └─ If same GR exists in both 'bilty' and 'station_bilty_summary',
      prefer the 'bilty' source (has more detail)

3. CHECK EXISTING
   ├─ Query transit_details for all selected GR numbers
   └─ Skip any that already exist (prevent duplicates)

4. INSERT
   ├─ Create transit_details row for each new bilty:
   │   ├─ challan_no = selected challan
   │   ├─ gr_no = bilty GR number
   │   ├─ bilty_id = bilty.id (only for 'bilty' source, null for station)
   │   ├─ from_branch_id = user's branch
   │   ├─ to_branch_id = challan book's destination
   │   └─ All delivery flags = false
   └─ UPDATE challan_details.total_bilty_count += inserted count

5. REFRESH
   ├─ Reload available bilties (removed ones disappear)
   ├─ Reload transit bilties (added ones appear)
   └─ Reload challans (updated count)
```

---

## Removing Bilties from Transit

### Single Remove
- Click the remove button on a transit bilty card
- Confirms with user → `DELETE FROM transit_details WHERE id = transit_id`
- Updates `challan_details.total_bilty_count -= 1`

### Bulk Remove
- Select multiple transit bilties via checkboxes
- Click "Remove Selected" → confirms with user
- `DELETE FROM transit_details WHERE id IN (selected_transit_ids)`
- Updates `challan_details.total_bilty_count -= removed_count`

**Note:** Removing from transit does NOT delete the bilty itself — it only removes the challan assignment. The bilty goes back to the "available" pool.

---

## Dispatch

Dispatch happens on the **challan-settings page** (not the challan loading page):

1. Toggle `is_dispatched = true` on `challan_details`
2. Set `dispatch_date = now()`
3. Once dispatched, the challan becomes **READ-ONLY**:
   - Cannot add bilties
   - Cannot remove bilties
   - Selection checkboxes are disabled
   - "Add to Transit" button is disabled

The challan page auto-selects the most recent **active** (non-dispatched) challan. If none exist, it falls back to the most recent dispatched challan (read-only view).

---

## Challan Selector (Left Panel)

Shows two dropdowns:

### Challan Dropdown
- **Active challans** — can add/remove bilties
- **Dispatched challans** — read-only view
- Searchable by: challan number, truck number, driver name, owner name
- Shows: date, bilty count, dispatch badge

### Challan Book Dropdown
- Only shows active, non-completed books for the user's branch
- Determines the **destination branch** when adding bilties
- Shows route info (destination branch name)

### Challan Overview (when selected)
- Bilty counts: Regular / Manual / Total
- Truck, Driver, Owner details
- Active vs Dispatched status badge

---

## Bilty Lists (Right Panel)

### Available Bilties Tab
- Shows all bilties not yet in any challan
- Filters: search text, payment mode, date, city, bilty type
- Excludes: cancelled bilties (`consignor_name === 'CANCEL BILTY'`)
- Click to select, double-click to add directly to challan
- Sorted by GR number (alphanumeric: prefix → number → suffix)

### Transit Bilties Tab
- Shows bilties assigned to the **currently selected challan**
- Same filter options as available list
- Sorted by **destination city alphabetically**, then by GR number
- Dispatched challan → selection disabled

---

## PDF Generation

Two PDF types available:

### Loading Challan PDF
- Shows all **available bilties** (loading manifest for what's ready to load)
- Portrait A4, 2-column × 20-row layout (40 bilties per page)
- Grouped by destination city → sorted by GR number
- Header: company name, challan number, date, truck/driver/owner
- Totals: bilty count (REG/MNL), packages, weight

### Challan Bilties PDF
- Shows bilties **in the selected challan** (what's already loaded)
- Detailed bilty listing for the specific challan

---

## Transit Header (Stats Dashboard)

Six stat cards displayed at the top:

| Card | Value |
|------|-------|
| Available | Count of filtered available bilties |
| In Transit | Count of bilties in selected challan |
| Weight | Total weight of transit bilties (KG + ton/quintal) |
| E-way Bills | Count of individual EWBs across transit bilties |
| Selected | Count of currently checked available bilties |
| Packages | Total packages in transit bilties |

Also shows payment mode breakdown (to-pay vs paid amounts).

---

## Key Technical Details

### Data Flow
- All database operations go **directly to Supabase** from the client (no backend API for challan operations)
- Available bilties use **database functions** (`get_available_gr_count`, `get_available_gr_numbers`) for efficient filtering
- Delivery tracking updates use `bulk_update_transit_status` RPC

### Deduplication Logic
- When adding bilties, if the same GR number exists in both `bilty` and `station_bilty_summary`, the `bilty` source is preferred (more complete data)
- Before inserting into `transit_details`, existing GR numbers are checked and skipped

### Sorting
- Available bilties: sorted by GR number (alphanumeric)
- Transit bilties: sorted by **destination city** (alphabetical), then GR number within same city

### Auto-Selection on Page Load
1. Auto-selects the most recent **active** challan (non-dispatched)
2. If no active challans, falls back to most recent **dispatched** challan
3. Auto-selects the first available challan book
