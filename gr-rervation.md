# GR Reservation System — How It Works & The `current_number` Problem

## Overview

The GR (Goods Receipt) Reservation System allows **multiple users from the same branch** to create bilties simultaneously without accidentally using the same GR number. It works through atomic database-level locks, real-time subscriptions, and a 30-minute TTL (time-to-live) expiry model.

---

## The Core Problem This Solves

Without the reservation system, two users opening the bilty page at the same time would both see **the same next GR number** (e.g., `A08044`). Whichever user saves first succeeds — the other user gets a **duplicate GR error** and loses their work. The reservation system prevents this by letting each user **lock (reserve) a GR number** before filling the form.

---

## How a GR Number Gets Created — Step by Step

### 1. Page Load
- The bilty page loads the **bill book** (e.g., prefix `A`, digits `5`, current_number `8044`, range `8001–9999`).
- The system calls `getNextAvailableGR()` which generates `A08044` from the bill book's `current_number`.
- It skips any numbers that are **already reserved** by other users or **already used** in saved bilties.
- The user sees the next available GR in the form (e.g., `A08044`).

### 2. User Clicks "Reserve"
- Calls the Supabase RPC function `reserve_next_gr()`.
- The SQL function **locks the bill_books row** (`SELECT ... FOR UPDATE`) to prevent race conditions.
- Finds the next number starting from `current_number` that is:
  - NOT used in the `bilty` table (no saved bilty with that GR).
  - NOT reserved in `gr_reservations` by another active reservation.
- Creates a row in `gr_reservations` with `status = 'reserved'` and `expires_at = NOW() + 30 minutes`.
- **Does NOT update `bill_books.current_number` yet** — this is critical (explained below).

### 3. User Fills the Form and Saves
- On save, the backend API inserts the bilty into the `bilty` table.
- Then the frontend calls `completeAndReserveNext()`:
  - Marks the reservation as `status = 'used'` via `complete_gr_reservation()`.
  - **NOW** advances `bill_books.current_number` using:
    ```sql
    UPDATE bill_books
    SET current_number = GREATEST(current_number, gr_number + 1)
    WHERE id = bill_book_id;
    ```
  - If the user has more pending reservations (from a batch/range reserve), it **switches to the next one**.
  - If no more pending reservations exist, the user must click "Reserve" again.

### 4. Form Resets
- After save, the form resets to a new bilty.
- If the user has another reserved GR, it populates automatically.
- Otherwise, `getNextAvailableGR()` calculates the next available number (skipping reserved/used ones).

---

## The `current_number` Problem — The Biggest Issue

### What is `current_number`?

Every bill book has a `current_number` field that represents **the next GR number to be issued**. For example:

| Field | Value |
|-------|-------|
| prefix | `A` |
| from_number | `8001` |
| to_number | `9999` |
| current_number | `8044` |
| digits | `5` |

This means the next GR will be `A08044`.

### When Does `current_number` Advance?

`current_number` advances **ONLY when a bilty is actually saved**, not when it is reserved. This is by design — if a user reserves `A08044` but then cancels or closes the page, the number should become available again, not be permanently wasted.

The advancement happens in two places:

1. **`complete_gr_reservation()` SQL function** — uses `GREATEST(current_number, gr_number + 1)` to ensure it only moves forward.
2. **Backend save API** — sends `bill_book_next_number` as `current_number + 1` to the backend, which also updates the bill book.

### The Problem: `current_number` Gets Stuck

This is the **single biggest problem** in the entire bilty system. Here's how it happens:

#### Scenario: `current_number` Doesn't Advance After Save

1. Bill book has `current_number = 8044`.
2. User A reserves and saves bilty with GR `A08044`.
3. The save goes through the **backend API** (`/api/bilty/save`), which saves the bilty.
4. The frontend then calls `completeAndReserveNext()` to advance `current_number`.
5. **But** — if the network drops, the browser closes, or any error occurs between step 3 and step 4, the bilty is saved **but** `current_number` is never updated.
6. Result: `current_number` is still `8044`, even though bilty `A08044` already exists.
7. Next user opens the page → sees `A08044` → tries to save → **DUPLICATE GR ERROR**.

#### Scenario: Backend Updates `current_number` But Reservation System Also Updates It

The save flow has **two independent paths** that both try to advance `current_number`:

- **Path 1 (Backend API):** The `handleSave()` function calculates `billBookNextNumber = selectedBillBook.current_number + 1` and sends it to the backend, which runs:
  ```sql
  UPDATE bill_books SET current_number = bill_book_next_number WHERE id = bill_book_id;
  ```
- **Path 2 (Reservation System):** After save completes, `completeAndReserveNext()` calls the SQL function `complete_gr_reservation()` which runs:
  ```sql
  UPDATE bill_books SET current_number = GREATEST(current_number, gr_number + 1) WHERE id = bill_book_id;
  ```

If Path 1 succeeds but Path 2 fails (or vice versa), the `current_number` may end up in an inconsistent state.

#### Scenario: Out-of-Order Saves With Multiple Users

1. User A reserves `A08044`, User B reserves `A08045`, User C reserves `A08046`.
2. User C saves first → `complete_gr_reservation` sets `current_number = GREATEST(8044, 8047) = 8047`.
3. User A saves second → `complete_gr_reservation` sets `current_number = GREATEST(8047, 8045) = 8047`. (No regression — `GREATEST` protects against this.)
4. This scenario is **actually handled correctly** by the `GREATEST` logic.

#### Scenario: Save Without Reservation (Legacy / Direct Save)

If the reservation system is not active (e.g., the user didn't click Reserve, or the system failed to connect), the bilty is saved **without** a reservation. In this case:
- Only the backend API path advances `current_number`.
- The backend calculates `next = current_number + 1` from the **client's local state**, which may be stale if another user saved in between.
- Result: `current_number` can go backward or skip numbers.

### How to Detect the Problem

The bilty page has a **GR Sequence Validation** (`useEffect` in `page.js`) that runs whenever the selected bill book or existing bilties change:

```javascript
// Check if any existing bilty already has this GR number
const duplicateGR = existingBilties.find(b => 
  b.gr_no.trim().toLowerCase() === currentGR.trim().toLowerCase()
);
```

If a duplicate is found, it sets `grSequenceError` which:
- Shows a **red warning banner** on the bilty page.
- **Blocks the save button** — prevents creating a duplicate bilty.
- Tells the user to fix the bill book sequence.

### How to Fix It

The bilty page provides a **Fix GR Sequence** button (via `onFixGRSequence`) that lets the user manually set `current_number` to the correct value. The `fixGRSequence()` function:

1. Validates the number is within the bill book's range.
2. Updates `bill_books.current_number` directly in Supabase.
3. Recalculates the next available GR.
4. Clears the sequence error.

**Manual fix:** If the UI fix doesn't work, run this SQL directly:
```sql
-- Find what the current_number should be (one more than the highest used GR)
SELECT MAX(gr_number) + 1 as correct_current_number
FROM gr_reservations
WHERE bill_book_id = '<bill-book-uuid>' AND status = 'used';

-- Or check from the bilty table directly
-- (need to parse the numeric part from gr_no)

-- Then update
UPDATE bill_books
SET current_number = <correct_number>
WHERE id = '<bill-book-uuid>';
```

---

## Why `current_number` Doesn't Update — Root Causes

| Cause | Explanation | Frequency |
|-------|-------------|-----------|
| **Network failure after save** | Bilty saved via backend API, but `completeAndReserveNext()` fails due to network drop | Common |
| **Browser closed mid-save** | User closes tab after bilty INSERT but before reservation completion | Occasional |
| **No reservation active** | User saves without reserving first (reservation system disabled or failed) — only backend path runs | Common when reservation system has errors |
| **Backend updates stale value** | Client sends `current_number + 1` based on its local state, which may be outdated if another user saved between page load and save | Occasional with multiple users |
| **Reservation system RPC failure** | `complete_gr_reservation()` throws an error (DB timeout, connection issue) | Rare |
| **Dual update conflict** | Backend API and reservation system both try to update `current_number` — if they race, one may overwrite the other with a lower value | Rare (GREATEST mitigates this) |

---

## Architecture of the Reservation System

### Database Table: `gr_reservations`

| Column | Type | Description |
|--------|------|-------------|
| `id` | UUID | Primary key |
| `bill_book_id` | UUID | Which bill book this reservation belongs to |
| `branch_id` | UUID | Branch (for multi-branch isolation) |
| `gr_no` | VARCHAR(50) | Full formatted GR string (e.g., `A08044`) |
| `gr_number` | INTEGER | Raw numeric part (e.g., `8044`) — for comparison |
| `user_id` | UUID | Who reserved this number |
| `user_name` | VARCHAR(100) | Cached display name |
| `status` | VARCHAR(20) | `reserved` / `used` / `released` / `expired` |
| `reserved_at` | TIMESTAMPTZ | When reserved |
| `expires_at` | TIMESTAMPTZ | When the lock expires (30 min from reserve/heartbeat) |
| `used_at` | TIMESTAMPTZ | When the bilty was saved |
| `released_at` | TIMESTAMPTZ | When cancelled or expired |

**Key constraint:** A partial unique index ensures only ONE active reservation per GR number per branch:
```sql
CREATE UNIQUE INDEX idx_gr_reservations_active_unique 
  ON gr_reservations (gr_no, branch_id) 
  WHERE status = 'reserved';
```

### SQL Functions

| Function | Purpose |
|----------|---------|
| `reserve_next_gr()` | Atomically find and reserve the next available GR number |
| `reserve_specific_gr()` | Reserve a user-chosen GR number |
| `reserve_gr_range()` | Batch-reserve a range of consecutive GR numbers |
| `release_gr_reservation()` | Cancel a reservation (user doesn't want to save) |
| `complete_gr_reservation()` | Mark reservation as used + advance `current_number` |
| `extend_gr_reservation()` | Heartbeat — extend the 30-minute TTL |
| `get_branch_gr_status()` | Get all active reservations + recent bilties for live display |
| `release_all_user_reservations()` | Clean up all user's reservations (logout) |
| `find_unused_gr_numbers()` | Find gaps — GR numbers that were skipped and never saved |
| `cleanup_expired_reservations()` | Mark expired reservations + delete old historical rows |

### Frontend Hook: `useGRReservation`

Located in `src/utils/grReservation.js`. Provides:

| Property / Method | Description |
|---|---|
| `reservation` | Current active reservation object |
| `reservedGRNo` | The GR string of the active reservation (e.g., `A08044`) |
| `hasReservation` | Boolean — is there an active reservation? |
| `reserveNext()` | Reserve the next available GR |
| `reserveSpecific(grNumber)` | Reserve a specific GR number |
| `reserveRange(from, to)` | Batch reserve a range |
| `release()` | Release the current active reservation |
| `releaseById(id)` | Release a specific reservation |
| `complete()` | Mark current reservation as used |
| `completeAndReserveNext()` | Complete current + switch to next pending one |
| `switchToReservation(resData)` | Switch active reservation to a different pending one |
| `branchReservations` | All active reservations in this branch (for live display) |
| `recentBilties` | Recently saved bilties (last 1 hour) |
| `myPendingReservations` | This user's other pending reservations (not the active one) |
| `unusedGRNumbers` | Gaps/holes in the GR sequence |

### Real-Time Updates

The system subscribes to **Supabase Realtime** on the `gr_reservations` table, filtered by `branch_id`. Any INSERT, UPDATE, or DELETE on the table triggers a refresh of `branchReservations`, so all users see live status updates.

### Heartbeat

Every **5 minutes**, the hook calls `extend_gr_reservation()` to push the `expires_at` forward by 30 minutes. This keeps the reservation alive as long as the user has the bilty page open. If the user closes the tab, the reservation expires naturally after 30 minutes.

### Auto-Restore on Page Refresh

When the page reloads:
1. The hook fetches `branchReservations` via `get_branch_gr_status()`.
2. If the user already has a reservation in the list, it **auto-restores** it as the active reservation.
3. The user sees their same GR number — no duplication, no loss.

---

## Smart GR Calculation: `getNextAvailableGR()`

When the user does NOT have an active reservation, the form shows a **suggested** next GR calculated by `getNextAvailableGR()`:

```
Start from bill_book.current_number
  → Skip numbers reserved by anyone (from branchReservations)
  → Skip numbers already used in bilty table (from existingBilties)
  → Return the first available GR string
```

This ensures the user sees a number they can actually reserve, not one that's already taken by another user or saved as a bilty.

---

## Lifecycle of a GR Number

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  AVAILABLE   │────▶│   RESERVED   │────▶│     USED     │
│              │     │  (30min TTL) │     │ (bilty saved)│
└──────────────┘     └──────┬───────┘     └──────────────┘
                            │
                            │ (user cancels or TTL expires)
                            ▼
                     ┌──────────────┐
                     │   RELEASED   │
                     │  / EXPIRED   │
                     └──────────────┘
                            │
                            │ (number becomes available again)
                            ▼
                     ┌──────────────┐
                     │  AVAILABLE   │
                     └──────────────┘
```

---

## Common Issues & Troubleshooting

### Issue: "GR SEQUENCE ERROR" banner appears
**Cause:** `bill_books.current_number` is behind the last saved bilty. The generated GR already exists.  
**Fix:** Click the "Fix" button on the banner and enter the correct `current_number`. Or manually update in the database.

### Issue: Two users get the same GR number
**Cause:** Both users opened the page before the reservation system loaded, or one saved without reserving.  
**Fix:** The second save will fail with a duplicate error. The user should reset the form and try again. The sequence error detection will also block saves with duplicate GRs.

### Issue: GR numbers have gaps (e.g., A08044, A08046 — where is A08045?)
**Cause:** A08045 was reserved but released without saving (user cancelled). The `find_unused_gr_numbers()` function detects these gaps and shows them in the UI so they can be reused.  
**Fix:** Use "Reserve Specific" to manually reserve and use the gap number.

### Issue: Reservation expired while filling the form
**Cause:** User took longer than 30 minutes AND the heartbeat failed (e.g., network issue).  
**Fix:** The heartbeat runs every 5 minutes. If the network is stable, this shouldn't happen. If it does, the user needs to reserve again — another user may have grabbed the number in the meantime.

### Issue: `current_number` jumped far ahead
**Cause:** A user did a range reservation (e.g., reserved 8044–8054) and saved the last number first. `GREATEST` advanced `current_number` to 8055.  
**Fix:** This is correct behavior. The gap numbers will be detected by `find_unused_gr_numbers()`.

---

## Files Involved

| File | Role |
|------|------|
| `src/app/bilty/page.js` | Main bilty form — GR sequence validation, save flow, `getNextAvailableGR()` |
| `src/utils/grReservation.js` | React hook — reservation lifecycle, realtime subscriptions, heartbeat |
| `src/components/bilty/grnumber-manager.js` | GR number display, bill book selector, edit mode, sequence error UI |
| `src/components/bilty/gr-live-status.js` | Live panel showing who has reserved which GR numbers |
| `gr_reservations_system.sql` | All SQL: table, indexes, RLS, all RPC functions |

---

## Summary

The reservation system works well for **preventing duplicate GRs in a multi-user environment**. The `current_number` problem is the Achilles' heel — it stems from the fact that the bilty save and the `current_number` advancement are **not in the same atomic transaction**. The bilty is saved via a backend API call, and `current_number` is updated separately via either the backend payload or the `complete_gr_reservation()` RPC. If anything interrupts between those two steps, `current_number` gets stuck and every subsequent user sees a duplicate GR error until it's manually fixed.

**The ideal fix** would be to make the entire save atomic: insert the bilty, advance `current_number`, and complete the reservation — all in a **single database transaction** on the backend. Until then, the sequence error detection and manual fix UI serve as a safety net.
