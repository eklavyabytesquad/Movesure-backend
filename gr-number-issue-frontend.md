# Bilty Save Flow & GR Number / Bill Book `current_number` Management

## Overview

When a bilty is saved, the GR number and bill book `current_number` are managed by **BOTH** the frontend and the backend, creating a **dual-update problem**. The frontend calculates the next number and sends it to the backend, the backend uses it to update the bill book, and then the GR reservation system **also** advances `current_number` via a separate `/complete` API call.

---

## What Happens When the User Clicks "Save"

### Step-by-Step Flow (`handleSave()` in `page.js`)

```
User clicks "Save"
       │
       ▼
┌─────────────────────────────────────┐
│ 1. FRONTEND: Pre-save checks        │
│    - Is already saving? (debounce)   │
│    - GR sequence error? (block save) │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────────────┐
│ 2. FRONTEND: Save new consignor/consignee to DB     │
│    (Direct Supabase calls — if typed, not selected)  │
└──────────────┬──────────────────────────────────────┘
               │
               ▼
┌───────────────────────────────────────────────────────────────┐
│ 3. FRONTEND: Calculate next bill book number ⚠️ ISSUE HERE   │
│                                                               │
│    let billBookNextNumber = null;                             │
│    if (selectedBillBook && !isEditMode) {                     │
│      billBookNextNumber = selectedBillBook.current_number + 1 │
│      if (billBookNextNumber > to_number) {                    │
│        billBookNextNumber = auto_continue ? from_number : null│
│      }                                                        │
│    }                                                          │
└──────────────┬────────────────────────────────────────────────┘
               │
               ▼
┌──────────────────────────────────────────────────────┐
│ 4. FRONTEND → BACKEND: POST /api/bilty/save          │
│                                                      │
│    Payload includes:                                  │
│    - All bilty fields (gr_no, consignor, charges...)  │
│    - bill_book_id: selectedBillBook.id                │
│    - bill_book_next_number: calculated above ⚠️       │
│    - bilty_id: (only if editing existing bilty)       │
│                                                      │
│    Backend does:                                      │
│    a) Validate cities (parallel)                      │
│    b) Check GR duplicate for branch (parallel)        │
│    c) INSERT or UPDATE bilty                          │
│    d) Update bill_books.current_number =              │
│       bill_book_next_number (background, non-blocking)│
│    e) Save rate (background, non-blocking)            │
└──────────────┬───────────────────────────────────────┘
               │
               ▼
┌──────────────────────────────────────────────────────────────┐
│ 5. FRONTEND: Post-save actions (after backend returns)       │
│                                                              │
│    a) Update city names from backend response                │
│    b) ⚠️ GR RESERVATION COMPLETION (if user had reservation)│
│       → grReservation.completeAndReserveNext()               │
│       → This calls POST /api/bilty/gr/complete/{id}          │
│       → Backend ALSO advances current_number here!           │
│    c) Update local selectedBillBook.current_number           │
│       → setSelectedBillBook(prev => ({                       │
│           ...prev, current_number: billBookNextNumber        │
│         }))                                                  │
│    d) Refresh next-available GR list from backend            │
│    e) Refresh existing bilties list (Supabase query)         │
│    f) Upload PDF in background (non-blocking)                │
│    g) Show print modal (or reset form if draft)              │
└──────────────────────────────────────────────────────────────┘
```

---

## The `current_number` Dual-Update Problem

### Where `current_number` Gets Updated

There are **THREE places** that modify `bill_books.current_number`:

| # | Where | How | When |
|---|-------|-----|------|
| 1 | **Backend `/api/bilty/save`** | Sets `current_number = bill_book_next_number` (value calculated by frontend) | During bilty save (background, non-blocking) |
| 2 | **Backend `/api/bilty/gr/complete/{id}`** | Advances `current_number` as part of reservation completion | After save succeeds, frontend calls `completeAndReserveNext()` |
| 3 | **Frontend local state** | `setSelectedBillBook(prev => ({ ...prev, current_number: billBookNextNumber }))` | After backend save returns success |

### The Race Condition

```
Time ─────────────────────────────────────────────────────────►

Frontend:  POST /api/bilty/save (includes bill_book_next_number = 8045)
                │
Backend:        └──► INSERT bilty ──► UPDATE bill_books SET current_number = 8045 (background)
                                                  │
Frontend:  (save returned) ──► POST /api/bilty/gr/complete/{id}
                                      │
Backend:                              └──► UPDATE bill_books SET current_number = ??? (may +1 again)
                                                  │
Frontend:  setSelectedBillBook({ current_number: 8045 })  ◄── local state update
```

**Potential issue:** If the `/complete` endpoint also increments `current_number`, it could advance it to 8046 when it should be 8045 (double-increment).

---

## Detailed Data Sent to `/api/bilty/save`

### Full Payload

```javascript
const savePayload = {
  // ── Identity ──
  branch_id: user.branch_id,          // UUID
  staff_id: user.id,                   // UUID
  gr_no: formData.gr_no.trim(),        // e.g., "A08044" — the GR string shown on bilty

  // ── Dates ──
  bilty_date: "2026-04-06",
  invoice_date: "2026-04-06" || null,

  // ── Location ──
  from_city_id: UUID || null,
  to_city_id: UUID || null,
  delivery_type: "godown-delivery",

  // ── Parties ──
  consignor_name, consignor_gst, consignor_number,
  consignee_name, consignee_gst, consignee_number,
  transport_name, transport_gst, transport_number, transport_id,

  // ── Payment ──
  payment_mode: "to-pay",

  // ── Goods ──
  contain, invoice_no, invoice_value, e_way_bill, document_number,
  no_of_pkg, wt, rate, labour_rate, pvt_marks,

  // ── Charges ──
  freight_amount, labour_charge, bill_charge, toll_charge,
  dd_charge, other_charge, pf_charge, total,

  // ── Meta ──
  remark,
  saving_option: "SAVE" | "DRAFT",

  // ── Bill Book Management ⚠️ ──
  bill_book_id: selectedBillBook.id,           // Which bill book to update
  bill_book_next_number: 8045,                 // Frontend-calculated next number

  // ── Edit Mode (optional) ──
  bilty_id: UUID                               // Only present when editing existing bilty
};
```

### Key Point About GR Number

The `gr_no` sent is the **formatted string** (e.g., `"A08044"`), NOT the raw number. It's generated from:

```javascript
const generateGRNumber = (billBook) => {
  const { prefix, current_number, digits, postfix } = billBook;
  const paddedNumber = String(current_number).padStart(digits, '0');
  return `${prefix || ''}${paddedNumber}${postfix || ''}`;
};
```

So if `prefix="A"`, `current_number=8044`, `digits=5` → GR = `"A08044"`.

---

## How `current_number` Is Determined at Different Stages

### 1. Page Load

```
Frontend loads → calls GET /api/bilty/reference-data
                 → bill_books array returned with current_number from DB
                 
GR Reservation hook activates:
  → GET /api/bilty/gr/validate/{bill_book_id}
    → Backend checks: is current_number consistent with last saved bilty?
    → If not, auto-fixes current_number in DB and returns corrected value
  → GET /api/bilty/gr/next-available?bill_book_id=...&count=5
    → Returns next 5 unreserved GR numbers
  → GET /api/bilty/gr/status/{branch_id}
    → Returns all active reservations (for showing who has which GR)
```

### 2. GR Number Assignment to Form

Priority order:
1. **Reserved GR** (from reservation system) — if user has an active reservation
2. **First from next-available list** (from backend `/next-available`) — if no reservation
3. **Local `generateGRNumber()`** — fallback if backend hasn't responded yet

### 3. During Save

Frontend sends `bill_book_next_number = selectedBillBook.current_number + 1` to the `/save` API. The backend updates `bill_books.current_number` to this value.

### 4. After Save (Reservation Completion)

If user had a GR reservation:
- Frontend calls `grReservation.completeAndReserveNext()`
- This calls `POST /api/bilty/gr/complete/{reservation_id}`
- **Backend may also advance `current_number` here** (see issue below)

---

## ⚠️ Identified Issues

### Issue 1: Double `current_number` Update (Frontend + Reservation System)

**The frontend calculates `billBookNextNumber` and sends it to `/api/bilty/save`, which updates the bill book. Then immediately after, `completeAndReserveNext()` calls `/api/bilty/gr/complete/{id}` which may ALSO advance `current_number`.**

This is the core architectural confusion:
- The `/save` endpoint updates `current_number` using the frontend-supplied value
- The `/complete` endpoint may also update `current_number` as part of reservation cleanup
- The frontend also locally updates `selectedBillBook.current_number`

**Risk:** `current_number` could be incremented twice for a single bilty save, causing a GR number to be skipped.

### Issue 2: Frontend Calculates Next Number Based on Potentially Stale State

```javascript
billBookNextNumber = selectedBillBook.current_number + 1;
```

`selectedBillBook.current_number` is the local React state. If another user saved a bilty simultaneously (multi-user scenario), this local value could be outdated. The GR reservation system was built to solve this, but the save payload still sends the frontend-calculated number.

### Issue 3: Mixed Responsibility — Who Owns `current_number`?

| Component | Reads `current_number` | Writes `current_number` |
|-----------|----------------------|------------------------|
| Frontend `page.js` | ✅ For GR generation, next number calc | ✅ Local state update after save |
| Backend `/api/bilty/save` | ❌ Uses frontend-supplied value | ✅ Updates DB to frontend value |
| Backend `/api/bilty/gr/validate` | ✅ Reads from DB | ✅ Auto-fixes if inconsistent |
| Backend `/api/bilty/gr/complete` | ✅ Reads from DB | ✅ May advance it |
| Backend `/api/bilty/gr/fix-sequence` | ✅ Reads from DB | ✅ Corrects it |
| Danger Helper (Admin UI) | ✅ Displays | ✅ Direct Supabase update |

**There is no single source of truth.** The frontend calculates the next number, sends it to the backend, and the backend blindly uses it. Meanwhile the reservation system has its own logic for advancing the number.

### Issue 4: `bill_book_next_number` Sent Even When Reservation System Is Active

When the GR reservation system is active, the reservation `/complete` endpoint should be the **sole** manager of `current_number`. But the `/save` endpoint still receives and applies `bill_book_next_number` from the frontend.

---

## Recommended Fix

The backend should **own** the `current_number` advancement entirely:

1. **Remove** `bill_book_next_number` from the frontend save payload
2. **Backend `/api/bilty/save`** should:
   - Look up the bill book
   - Determine the correct next number itself (current_number + 1, with wraparound logic)
   - Update it atomically
3. **OR** if reservation system is active, **only** `/api/bilty/gr/complete` should advance `current_number`, and `/api/bilty/save` should NOT touch it
4. Frontend should only update local state from the **backend response**, never calculate it independently

This eliminates the race condition, the stale state issue, and the double-update problem.

---

## File Locations

| File | Purpose |
|------|---------|
| `src/app/bilty/page.js` (lines 600-695) | `handleSave()` — builds payload, sends to backend, post-save actions |
| `src/app/bilty/page.js` (lines 475-480) | `generateGRNumber()` — formats GR string from bill book |
| `src/app/bilty/page.js` (lines 690-695) | Local state update of `selectedBillBook.current_number` |
| `src/utils/grReservation.js` (lines 300-320) | `complete()` — calls backend `/complete`, which may advance number |
| `src/utils/grReservation.js` (lines 325-380) | `completeAndReserveNext()` — complete + switch to next pending |
| `src/utils/grReservation.js` (lines 117-135) | `validateBillBook()` — auto-fix current_number on load |
| `src/utils/grReservation.js` (lines 395-415) | `fixSequence()` — manual fix via backend |
| `src/components/danger/danger-helper.js` (lines 75-160) | Admin UI to directly edit `current_number` via Supabase |
| `src/app/bilty/bilty-page-backend.md` | API documentation for all endpoints |
