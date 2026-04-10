# GR Number Auto-Advance — Backend Changes Required

## Problem
After saving a bilty, the frontend GR number does not automatically advance to the next one. The user has to manually fetch/refresh to get the next GR number.

## Root Cause
The frontend relies on `next_gr_no` from the `/api/bilty/save` response to instantly set the next GR number (stored in a synchronous ref). If the backend doesn't return this field, the frontend falls back to async sources (reservations, next-available API) which may not have updated yet when the form resets.

---

## Required Backend Change: `/api/bilty/save` Response

### Current Response (assumed)
```json
{
  "status": "success",
  "data": {
    "bilty": { ... },
    "from_city": { ... },
    "to_city": { ... },
    "new_current_number": 1235
  }
}
```

### Required Response — Add `next_gr_no` field
```json
{
  "status": "success",
  "data": {
    "bilty": { ... },
    "from_city": { ... },
    "to_city": { ... },
    "new_current_number": 1235,
    "next_gr_no": "SS1235"
  }
}
```

### How to Generate `next_gr_no`
After saving the bilty and advancing `current_number`, format the next GR number using the bill book's prefix/postfix:

```python
# In the /api/bilty/save endpoint, after updating bill_books.current_number:

# 1. Get the updated bill book
bill_book = get_bill_book(bill_book_id)  # fetch after current_number was advanced

# 2. Format the next GR number
new_current = bill_book['current_number']  # This is already the NEXT number after save
prefix = bill_book.get('prefix', '') or ''
postfix = bill_book.get('postfix', '') or ''
digits = bill_book.get('digits', 0) or 0

if digits > 0:
    padded = str(new_current).zfill(digits)
else:
    padded = str(new_current)

next_gr_no = f"{prefix}{padded}{postfix}"

# 3. Include in response
response_data['next_gr_no'] = next_gr_no
```

### Important Notes
- `new_current_number` is the numeric value of the next GR (already returned by backend)
- `next_gr_no` is the **formatted string** with prefix + padded number + postfix (e.g., `"SS1235"`)
- The backend already advances `current_number = highest_used + 1` on save — just need to format and return it
- This field should ONLY be included for **new bilty saves** (not edits). For edits, omit or set to `null`
- Skip reserved GR numbers when calculating the next available number (check `gr_reservations` table for active reservations on the same bill book)

---

## Frontend Changes Already Applied

### 1. `src/app/bilty/page.js` — `handleSave`
- After save, if no `next_gr_no` from server AND no pending reservation, **auto-reserves** the next GR number via `/api/bilty/gr/reserve`
- Stores the auto-reserved GR in `serverNextGrNoRef` for instant use in `resetForm`
- `refreshNextAvailable()` is now **awaited** (was fire-and-forget before)

### 2. `src/utils/grReservation.js` — `completeAndReserveNext`
- When no pending reservations exist after completing one, now **auto-reserves** the next available GR instead of just refreshing the list
- Returns the new reservation so the caller can use the GR number immediately

---

## Flow After Fix

```
Save Bilty
  ├── Backend saves bilty, advances current_number
  ├── Backend returns next_gr_no (formatted) ← NEW
  ├── Frontend stores next_gr_no in serverNextGrNoRef (sync ref)
  ├── completeAndReserveNext() → completes reservation
  │   ├── If pending reservation exists → switch to it ✓
  │   └── If no pending → auto-reserve next GR ← NEW
  ├── If still no next GR → frontend calls reserveNext() ← NEW  
  ├── await refreshNextAvailable() ← was not awaited before
  └── Show Print Modal
       └── User closes modal → resetForm()
            └── Uses serverNextGrNoRef (instant, sync) → correct next GR ✓
```
