# GR Reservation API

Atomic GR number reservation system for multi-user bilty creation. Prevents duplicate GR numbers and auto-corrects `bill_books.current_number`.

---

## Base URL

```
https://your-domain.com  (or http://localhost:5000 locally)
```

All endpoints are under `/api/bilty/gr/` — no auth required (skipped via `/api/bilty` prefix).

---

## Endpoints

### 1. Get Next Available GR Numbers

Show the next N available GR numbers (skips reserved + used). **Use this to display upcoming GR numbers in the UI.**

```
GET /api/bilty/gr/next-available?bill_book_id=XXX&branch_id=YYY&count=5
```

| Param | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `bill_book_id` | UUID | Yes | — | Bill book ID |
| `branch_id` | UUID | Yes | — | Branch ID |
| `count` | int | No | 5 | How many to return |

**Response:**
```json
{
  "status": "success",
  "data": {
    "bill_book_id": "2f990473-...",
    "available": [
      { "number": 9293, "gr_no": "A09293" },
      { "number": 9294, "gr_no": "A09294" },
      { "number": 9295, "gr_no": "A09295" },
      { "number": 9296, "gr_no": "A09296" },
      { "number": 9297, "gr_no": "A09297" }
    ],
    "current_number": 9293,
    "reserved_count": 0
  }
}
```

---

### 2. Reserve a GR Number

Reserve the **next available** or a **specific** GR number. Auto-fixes `current_number` if it points to an already-used GR.

```
POST /api/bilty/gr/reserve
```

**Body:**
```json
{
  "bill_book_id": "2f990473-...",
  "branch_id": "e47a517f-...",
  "user_id": "fffaa44f-...",
  "user_name": "Eklavya",
  "gr_number": 9295          // optional — omit to get next available
}
```

**Response:**
```json
{
  "status": "success",
  "data": {
    "reservation": {
      "id": "746e70d4-...",
      "gr_no": "A09293",
      "gr_number": 9293,
      "user_name": "Eklavya",
      "status": "reserved",
      "expires_at": "2026-04-05T22:48:14+00:00"
    },
    "gr_no": "A09293",
    "gr_number": 9293,
    "expires_at": "2026-04-05T22:48:14+00:00",
    "auto_fixed_current_number": false
  }
}
```

**Errors:** `409` if GR is already reserved/used, `404` if bill book not found.

---

### 3. Release a Reservation

User cancels — number becomes available again.

```
POST /api/bilty/gr/release/{reservation_id}
```

**Body:**
```json
{ "user_id": "fffaa44f-..." }
```

**Response:**
```json
{
  "status": "success",
  "data": {
    "reservation_id": "746e70d4-...",
    "gr_no": "A09293",
    "released_at": "2026-04-05T22:18:23+00:00"
  }
}
```

---

### 4. Complete a Reservation (After Bilty Save)

Mark reservation as `used` and advance `current_number`. **Call this immediately after saving a bilty.**

```
POST /api/bilty/gr/complete/{reservation_id}
```

**Body:**
```json
{ "user_id": "fffaa44f-..." }
```

**Response:**
```json
{
  "status": "success",
  "data": {
    "reservation_id": "720c7302-...",
    "gr_no": "A09295",
    "used_at": "2026-04-05T22:17:06+00:00",
    "current_number_advanced_to": 9296
  }
}
```

Uses `GREATEST` logic — `current_number` only moves forward.

---

### 5. Extend Reservation (Heartbeat)

Keep a reservation alive. Call every 5 minutes while the bilty form is open.

```
POST /api/bilty/gr/extend/{reservation_id}
```

**Body:**
```json
{ "user_id": "fffaa44f-..." }
```

**Response:**
```json
{
  "status": "success",
  "data": {
    "reservation_id": "746e70d4-...",
    "expires_at": "2026-04-05T22:48:59+00:00"
  }
}
```

---

### 6. Branch GR Status (Live View)

Get all active reservations + recent bilties for a branch. **Poll this for the live GR panel.**

```
GET /api/bilty/gr/status/{branch_id}?bill_book_id=XXX
```

| Param | Type | Required | Description |
|-------|------|----------|-------------|
| `branch_id` | UUID (path) | Yes | Branch ID |
| `bill_book_id` | UUID (query) | No | Filter by bill book |

**Response:**
```json
{
  "status": "success",
  "data": {
    "reservations": [
      {
        "id": "746e70d4-...",
        "gr_no": "A09293",
        "user_name": "Eklavya",
        "status": "reserved",
        "expires_at": "..."
      }
    ],
    "recent_bilties": [
      { "id": "...", "gr_no": "A09292", "consignor_name": "XYZ", "created_at": "..." }
    ],
    "bill_book": { "id": "...", "prefix": "A", "current_number": 9293, ... },
    "reservation_count": 1
  }
}
```

---

### 7. Release All User Reservations

Clean up all reservations for a user (logout / page close).

```
POST /api/bilty/gr/release-all
```

**Body:**
```json
{
  "user_id": "fffaa44f-...",
  "branch_id": "e47a517f-..."
}
```

**Response:**
```json
{
  "status": "success",
  "data": { "released_count": 3, "released_grs": ["A09293", "A09294", "A09295"] }
}
```

---

### 8. Fix GR Sequence

Manually or auto-fix `current_number`. Auto mode finds the highest used GR and sets `current_number = max + 1`.

```
POST /api/bilty/gr/fix-sequence
```

**Body:**
```json
{
  "bill_book_id": "2f990473-...",
  "correct_number": 9300          // optional — omit for auto-detect
}
```

**Response:**
```json
{
  "status": "success",
  "data": {
    "bill_book_id": "2f990473-...",
    "old_current_number": 9296,
    "new_current_number": 9293,
    "fixed": true
  }
}
```

---

### 9. Cleanup Expired Reservations

Expire stale reservations (TTL > 30 min). Can be called as a cron job or on-demand.

```
POST /api/bilty/gr/cleanup
```

**Body (optional):**
```json
{ "branch_id": "e47a517f-..." }
```

**Response:**
```json
{ "status": "success", "data": { "expired_count": 2 } }
```

---

### 10. Validate & Auto-Correct Bill Book

**Call this every time a bill book is loaded or edited.** Checks if `current_number` points to an already-used bilty and auto-corrects it. Also returns the next 5 available GR numbers.

```
GET /api/bilty/gr/validate/{bill_book_id}
```

**Response:**
```json
{
  "status": "success",
  "data": {
    "bill_book_id": "2f990473-...",
    "old_current_number": 9293,
    "new_current_number": 9295,
    "fixed": true,
    "exhausted": false,
    "next_available": [
      { "number": 9295, "gr_no": "A09295" },
      { "number": 9296, "gr_no": "A09296" },
      { "number": 9297, "gr_no": "A09297" },
      { "number": 9298, "gr_no": "A09298" },
      { "number": 9299, "gr_no": "A09299" }
    ],
    "reserved_count": 1,
    "used_count": 42,
    "bill_book": { "id": "...", "prefix": "A", "current_number": 9295, ... }
  }
}
```

**When `exhausted: true`**, the bill book has no numbers left.

---

## Frontend Integration Guide

### On Bilty Page Load

```js
// 1. Validate & auto-correct the selected bill book
const { data } = await api.get(`/api/bilty/gr/validate/${billBookId}`);
// data.fixed === true → current_number was corrected
// data.next_available → show these 5 GR options to the user

// 2. Get branch live status
const status = await api.get(`/api/bilty/gr/status/${branchId}?bill_book_id=${billBookId}`);
// status.data.reservations → show who reserved which GR
```

### User Selects a GR & Clicks "Reserve"

```js
// Reserve next available
const res = await api.post('/api/bilty/gr/reserve', {
  bill_book_id: billBookId,
  branch_id: branchId,
  user_id: userId,
  user_name: userName,
});
// res.data.gr_no → the reserved GR number to use in the form

// OR reserve a specific number (user clicked on one of the 5 shown)
const res = await api.post('/api/bilty/gr/reserve', {
  bill_book_id: billBookId,
  branch_id: branchId,
  user_id: userId,
  user_name: userName,
  gr_number: 9295,  // the specific number they picked
});
```

### Heartbeat (Every 5 Minutes)

```js
setInterval(async () => {
  if (reservationId) {
    await api.post(`/api/bilty/gr/extend/${reservationId}`, { user_id: userId });
  }
}, 5 * 60 * 1000);
```

### After Bilty Save

```js
// 1. Save bilty via /api/bilty/save
const bilty = await api.post('/api/bilty/save', biltyData);

// 2. Immediately complete the reservation (advances current_number)
await api.post(`/api/bilty/gr/complete/${reservationId}`, { user_id: userId });

// 3. Validate bill book again to get next available
const fresh = await api.get(`/api/bilty/gr/validate/${billBookId}`);
```

### User Cancels / Closes Page

```js
// On beforeunload or cancel button
await api.post('/api/bilty/gr/release-all', {
  user_id: userId,
  branch_id: branchId,
});
```

---

## GR Number Lifecycle

```
AVAILABLE → RESERVED (30min TTL) → USED (bilty saved)
                ↓
         RELEASED / EXPIRED (number becomes available again)
```

## Key Behaviors

| Behavior | How It Works |
|----------|-------------|
| **Auto-fix on reserve** | If `current_number` points to a used GR, it auto-advances before reserving |
| **Auto-fix on validate** | `/validate/{id}` checks and corrects `current_number` every time it's called |
| **Skip logic** | `next-available` skips both reserved and used GR numbers |
| **GREATEST protection** | `complete` uses `max(current, gr+1)` — never goes backward |
| **30min TTL** | Reservations auto-expire; heartbeat extends them |
| **Ownership check** | Only the reserving user can release/complete/extend |
| **Multi-user safe** | Different users get different numbers; the UI shows who has what |
