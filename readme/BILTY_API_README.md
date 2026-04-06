# Bilty API - Backend Endpoints for Fast Bilty Creation

## Base URL
```
http://localhost:5000
```
On Render: `https://your-app.onrender.com`

**Swagger Docs:** `http://localhost:5000/docs`

---

## Setup

```bash
pip install -r requirements.txt
```

Create `.env` file (already done — contains `SUPABASE_URL` and `SUPABASE_SERVICE_KEY`).  
MastersGST auth works as before (hardcoded, no env needed).

```bash
python app.py
```

---

## Endpoints

### 1. `GET /api/bilty/reference-data` — Preload ALL Data (Single Call)

Replaces all 8 separate Supabase queries the frontend makes on page load.  
All 7 DB queries run **in parallel** — typically completes in **200-400ms**.

**Query Parameters:**
| Param | Required | Description |
|-------|----------|-------------|
| `branch_id` | Yes | User's branch UUID |
| `user_id` | Yes | User's UUID |

**Example Request:**
```js
const res = await fetch(
  `${API_URL}/api/bilty/reference-data?branch_id=${user.branch_id}&user_id=${user.id}`
);
const { data } = await res.json();
```

**Response:**
```json
{
  "status": "success",
  "data": {
    "branch": {
      "id": "uuid",
      "branch_code": "ALG",
      "city_code": "ALG",
      "address": "...",
      "branch_name": "ALIGARH",
      "default_bill_book_id": "uuid"
    },
    "cities": [
      { "id": "uuid", "city_code": "ALG", "city_name": "ALIGARH" },
      { "id": "uuid", "city_code": "KNP", "city_name": "KANPUR" }
    ],
    "city_by_id": {
      "uuid-1": { "id": "uuid-1", "city_code": "ALG", "city_name": "ALIGARH" }
    },
    "city_by_code": {
      "ALG": { "id": "uuid-1", "city_code": "ALG", "city_name": "ALIGARH" }
    },
    "transports": [
      { "id": "uuid", "transport_name": "S S TRANSPORT", "city_id": "uuid", "city_name": "KANPUR", "gst_number": "09COVPS5556J1ZT", "mob_number": "7668291228", "address": "TRANSPORT NAGAR", "branch_owner_name": "RAJEEV", "is_prior": true }
    ],
    "transport_by_city_id": {
      "city-uuid-1": [
        { "id": "uuid", "transport_name": "S S TRANSPORT", "city_id": "city-uuid-1", "city_name": "KANPUR", "gst_number": "...", "mob_number": "...", "is_prior": true },
        { "id": "uuid", "transport_name": "VIJAY TRANSPORT", "city_id": "city-uuid-1", "city_name": "KANPUR", "gst_number": "...", "mob_number": "...", "is_prior": false }
      ]
    },
    "consignors": [
      { "id": "uuid", "company_name": "ABC CO", "gst_num": "...", "number": "..." }
    ],
    "consignees": [
      { "id": "uuid", "company_name": "XYZ CO", "gst_num": "...", "number": "..." }
    ],
    "rates": [
      { "id": "uuid", "branch_id": "uuid", "city_id": "uuid", "consignor_id": "uuid", "rate": 120, "is_default": false }
    ],
    "bill_books": [
      { "id": "uuid", "prefix": "A", "from_number": 1, "to_number": 9999, "digits": 5, "current_number": 8044 }
    ]
  }
}
```

**Frontend usage — replace `loadInitialData()`:**
```js
// BEFORE: 8 separate await calls to Supabase
// AFTER: 1 call
const API_URL = 'http://localhost:5000'; // or your Render URL

async function loadInitialData() {
  const res = await fetch(
    `${API_URL}/api/bilty/reference-data?branch_id=${user.branch_id}&user_id=${user.id}`
  );
  const { data } = await res.json();
  
  setCities(data.cities);
  setTransports(data.transports);
  setConsignors(data.consignors);
  setConsignees(data.consignees);
  setRates(data.rates);
  setBillBooks(data.bill_books);
  setBranch(data.branch);
  
  // Cache city lookups — use these for PDF generation
  cityByIdRef.current = data.city_by_id;
  cityByCodeRef.current = data.city_by_code;
  
  // Cache transport lookup by city — use for auto-filling transport on city select
  transportByCityIdRef.current = data.transport_by_city_id;
}

// When user types a city code (e.g. "KNP"):
function onToCityCodeChange(cityCode) {
  const city = cityByCodeRef.current[cityCode];
  if (!city) return;
  
  setFormData(prev => ({ ...prev, to_city_id: city.id }));
  
  // Auto-fill transports for this city
  // NOTE: Array is sorted by priority — is_prior=true transport comes FIRST
  const cityTransports = transportByCityIdRef.current[city.id] || [];
  setFilteredTransports(cityTransports);
  
  // Auto-select priority transport (first in array) or only transport
  if (cityTransports.length >= 1) {
    const t = cityTransports[0]; // Priority transport (is_prior=true) is always first
    setFormData(prev => ({
      ...prev,
      to_city_id: city.id,
      transport_name: t.transport_name,
      transport_gst: t.gst_number,
      transport_number: t.mob_number,
      transport_id: t.id,
    }));
  }
}
```

---

### 2. `POST /api/bilty/save` — Save Bilty (Server-Validated)

**This is the key endpoint.** Saves bilty AND returns resolved city names.  
The frontend uses the **response city data** for PDF generation — never hardcoded fallbacks.

**Speed:** ~100-200ms total  
- City validation + GR dup check run **in parallel** (~1 round-trip)  
- INSERT/UPDATE bilty (~1 round-trip)  
- Safety check & advance `current_number` (~1 round-trip, blocking — returns new value)  
- Rate save runs **in background** (non-blocking)

> **`current_number` Safety Checker:** The backend is the **sole authority** on
> `bill_books.current_number`. After inserting the bilty, it queries the DB for
> the actual highest used GR number and sets `current_number = highest + 1`.
> This auto-corrects drift in **either direction** — if `current_number` somehow
> jumped ahead by +4, it snaps back to `last_bilty + 1`. The frontend should
> **never** calculate or send `bill_book_next_number`.

**Request Body:**
```json
{
  "branch_id": "uuid (required)",
  "staff_id": "uuid",
  "gr_no": "A08044 (required)",
  "bilty_date": "2026-04-06",
  "from_city_id": "uuid",
  "to_city_id": "uuid",
  "delivery_type": "godown-delivery",
  "consignor_name": "MODERN LOCK MFG CO",
  "consignor_gst": "09AABFM8846M1ZM",
  "consignor_number": "9876543210",
  "consignee_name": "M.A ENTERPRISES",
  "consignee_gst": "09ASMPS6146H1Z5",
  "consignee_number": "9876543211",
  "transport_name": "S S TRANSPORT",
  "transport_gst": "09COVPS5556J1ZT",
  "transport_number": "9876543212",
  "transport_id": "uuid",
  "payment_mode": "to-pay",
  "contain": "LOCKS",
  "invoice_no": "INV-001",
  "invoice_value": 50000,
  "invoice_date": "2026-04-06",
  "e_way_bill": "431698629992",
  "document_number": "DOC-001",
  "no_of_pkg": 10,
  "wt": 500,
  "rate": 120,
  "labour_rate": 20,
  "pvt_marks": "ABC",
  "freight_amount": 6000,
  "labour_charge": 200,
  "bill_charge": 50,
  "toll_charge": 20,
  "dd_charge": 0,
  "other_charge": 0,
  "pf_charge": 0,
  "total": 6270,
  "remark": "",
  "saving_option": "SAVE",
  "bill_book_id": "uuid (backend uses this to advance current_number safely)"
}
```

For **editing** an existing bilty, add:
```json
{
  "bilty_id": "uuid-of-existing-bilty",
  ...rest of fields
}
```

**Response:**
```json
{
  "status": "success",
  "message": "Bilty saved successfully",
  "data": {
    "bilty": {
      "id": "new-uuid",
      "gr_no": "A08044",
      "branch_id": "uuid",
      "from_city_id": "uuid",
      "to_city_id": "uuid",
      "consignor_name": "MODERN LOCK MFG CO",
      "total": 6270,
      "...all bilty fields..."
    },
    "from_city": {
      "id": "uuid",
      "city_name": "ALIGARH",
      "city_code": "ALG"
    },
    "to_city": {
      "id": "uuid",
      "city_name": "KANPUR",
      "city_code": "KNP"
    },
    "new_current_number": 8045,
    "next_gr_no": "A08045"
  }
}
```

**Frontend usage — replace `handleSave()`:**
```js
async function handleSave() {
  if (isSaving) return;
  setIsSaving(true);

  try {
    const res = await fetch(`${API_URL}/api/bilty/save`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        // Pass all form fields + IDs
        branch_id: user.branch_id,
        staff_id: user.id,
        gr_no: formData.gr_no,
        bilty_date: formData.bilty_date,
        from_city_id: formData.from_city_id,
        to_city_id: formData.to_city_id,
        consignor_name: formData.consignor_name,
        consignor_gst: formData.consignor_gst,
        consignor_number: formData.consignor_number,
        consignee_name: formData.consignee_name,
        consignee_gst: formData.consignee_gst,
        consignee_number: formData.consignee_number,
        transport_name: formData.transport_name,
        transport_gst: formData.transport_gst,
        transport_number: formData.transport_number,
        transport_id: formData.transport_id,
        payment_mode: formData.payment_mode,
        contain: formData.contain,
        invoice_no: formData.invoice_no,
        invoice_value: formData.invoice_value ? parseFloat(formData.invoice_value) : null,
        invoice_date: formData.invoice_date || null,
        e_way_bill: formData.e_way_bill,
        document_number: formData.document_number,
        no_of_pkg: formData.no_of_pkg ? parseInt(formData.no_of_pkg) : null,
        wt: formData.wt ? parseFloat(formData.wt) : null,
        rate: formData.rate ? parseFloat(formData.rate) : null,
        labour_rate: formData.labour_rate ? parseFloat(formData.labour_rate) : null,
        pvt_marks: formData.pvt_marks,
        freight_amount: formData.freight_amount ? parseFloat(formData.freight_amount) : null,
        labour_charge: formData.labour_charge ? parseFloat(formData.labour_charge) : null,
        bill_charge: formData.bill_charge ? parseFloat(formData.bill_charge) : null,
        toll_charge: formData.toll_charge ? parseFloat(formData.toll_charge) : null,
        dd_charge: formData.dd_charge ? parseFloat(formData.dd_charge) : null,
        other_charge: formData.other_charge ? parseFloat(formData.other_charge) : null,
        pf_charge: formData.pf_charge ? parseFloat(formData.pf_charge) : null,
        total: formData.total ? parseFloat(formData.total) : null,
        remark: formData.remark,
        delivery_type: formData.delivery_type,
        saving_option: 'SAVE',
        // For bill book — backend auto-advances current_number
        bill_book_id: selectedBillBook?.id,
        // For edit mode
        ...(editMode && formData.bilty_id ? { bilty_id: formData.bilty_id } : {}),
      }),
    });

    const result = await res.json();

    if (result.status !== 'success') {
      alert(result.message);
      return;
    }

    // ✅ USE SERVER RESPONSE for city data — NEVER hardcoded defaults
    const savedBilty = result.data.bilty;
    const fromCity = result.data.from_city;  // { id, city_name, city_code }
    const toCity = result.data.to_city;      // { id, city_name, city_code }

    // ✅ Sync local bill book state from server (NEVER calculate locally)
    const serverCurrentNumber = result.data.new_current_number;
    if (serverCurrentNumber != null) {
      setSelectedBillBook(prev => ({
        ...prev,
        current_number: serverCurrentNumber
      }));
    }

    // ✅ Use next_gr_no from save response — eliminates need for /next-available call
    // This makes rapid bilty creation reliable even under connection pressure.
    const nextGrNo = result.data.next_gr_no;
    if (nextGrNo && !editMode) {
      setFormData(prev => ({ ...prev, gr_no: nextGrNo }));
    }

    // Pass to PDF generation — guaranteed correct
    // No more || 'DEORIA' fallback needed!
    showPrintModal({
      bilty: savedBilty,
      fromCity,   // { city_name: "ALIGARH", city_code: "ALG" }
      toCity,     // { city_name: "KANPUR", city_code: "KNP" }
    });

    // Reset form for next bilty
    if (!editMode) {
      resetForm();
    }
  } catch (err) {
    alert('Save failed: ' + err.message);
  } finally {
    setIsSaving(false);
  }
}
```

**CRITICAL PDF FIX — in `pdf-generation.js`:**
```js
// BEFORE (BUG — hardcoded fallback causes wrong city):
const fromCityName = fromCity?.city_name || 'ALIGARH';
const toCityName = toCity?.city_name || 'DEORIA';
const toCityCode = toCity?.city_code || 'DRO';

// AFTER (use server-provided data, fail safely):
const fromCityName = fromCity?.city_name;
const toCityName = toCity?.city_name;
const toCityCode = toCity?.city_code;

if (!fromCityName || !toCityName) {
  console.error('City data missing from server response');
  alert('Error: City data missing. Cannot generate PDF.');
  return;
}
```

---

### 3. `GET /api/bilty/<bilty_id>` — Fetch Bilty with Cities (for Reprint)

Use this when reprinting a bilty — guaranteed correct city names from database.

**Example Request:**
```js
const res = await fetch(`${API_URL}/api/bilty/${biltyId}`);
const { data } = await res.json();
// data.bilty = full bilty record
// data.from_city = { id, city_name, city_code }
// data.to_city = { id, city_name, city_code }
```

**Response:**
```json
{
  "status": "success",
  "data": {
    "bilty": { "...all bilty fields..." },
    "from_city": { "id": "uuid", "city_name": "ALIGARH", "city_code": "ALG" },
    "to_city": { "id": "uuid", "city_name": "KANPUR", "city_code": "KNP" }
  }
}
```

---

### 4. `GET /api/bilty/rates/consignor/{consignor_id}` — Consignor Rate Profiles

Fetch all active rates from `consignor_bilty_profile` for a specific consignor.  
Returns destination-wise rate, labour, DD charges, transport info, etc.

**Path Parameter:**
| Param | Required | Description |
|-------|----------|-------------|
| `consignor_id` | Yes | Consignor UUID |

**Example Request:**
```js
const res = await fetch(`${API_URL}/api/bilty/rates/consignor/${consignorId}`);
const { data } = await res.json();
```

**Response:**
```json
{
  "status": "success",
  "data": {
    "rates": [
      {
        "id": "uuid",
        "consignor_id": "uuid",
        "destination_station_id": "city-uuid",
        "city_code": "AZM",
        "city_name": "AZAMGARH",
        "transport_name": "NEW INDIA EXPRESS TRANSPORT CO.",
        "transport_gst": "09AABFN1234A1Z1",
        "rate": 3.0,
        "rate_unit": "PER_KG",
        "minimum_weight_kg": 0,
        "labour_rate": 6.0,
        "labour_unit": "PER_KG",
        "dd_charge_per_kg": 0,
        "dd_charge_per_nag": 0,
        "receiving_slip_charge": 0,
        "bilty_charge": 0,
        "is_no_charge": false,
        "effective_from": "2025-01-01",
        "effective_to": null,
        "is_active": true,
        "dd_print_charge_per_kg": null,
        "dd_print_charge_per_nag": null,
        "is_toll_tax_applicable": false,
        "toll_tax_amount": 0,
        "freight_minimum_amount": 0
      }
    ],
    "rates_by_city": {
      "city-uuid-1": [ { "...rate object..." } ],
      "city-uuid-2": [ { "...rate object..." } ]
    },
    "count": 33
  }
}
```

**Frontend usage — auto-fill rate when consignor + city selected:**
```js
// Load when consignor changes
async function onConsignorChange(consignorId) {
  const res = await fetch(`${API_URL}/api/bilty/rates/consignor/${consignorId}`);
  const { data } = await res.json();
  consignorRatesRef.current = data.rates_by_city;
}

// When city is selected, look up rate instantly from cache
function onToCityChange(cityId) {
  const cityRates = consignorRatesRef.current[cityId];
  if (cityRates && cityRates.length > 0) {
    const r = cityRates[0];
    setFormData(prev => ({
      ...prev,
      rate: r.rate,
      labour_rate: r.labour_rate,
      transport_name: r.transport_name || prev.transport_name,
      transport_gst: r.transport_gst || prev.transport_gst,
      dd_charge: r.rate_unit === 'PER_KG'
        ? (r.dd_charge_per_kg * prev.wt)
        : (r.dd_charge_per_nag * prev.no_of_pkg),
      bill_charge: r.bilty_charge,
      toll_charge: r.is_toll_tax_applicable ? r.toll_tax_amount : 0,
    }));
  }
}
```

---

### 5. `GET /api/bilty/rates/default` — Default Branch Rates

Fetch default city-wise rates for a branch from the `rates` table.  
Used as fallback when no `consignor_bilty_profile` exists for a consignor+city.

**Query Parameters:**
| Param | Required | Description |
|-------|----------|-------------|
| `branch_id` | Yes | Branch UUID |

**Example Request:**
```js
const res = await fetch(`${API_URL}/api/bilty/rates/default?branch_id=${branchId}`);
const { data } = await res.json();
```

**Response:**
```json
{
  "status": "success",
  "data": {
    "rates": [
      { "id": "uuid", "branch_id": "uuid", "city_id": "uuid", "rate": 4.5, "is_default": true }
    ],
    "rate_by_city_id": {
      "city-uuid-1": 4.5,
      "city-uuid-2": 3.5,
      "city-uuid-3": 5.0
    },
    "count": 439
  }
}
```

**Frontend usage — fallback rate lookup:**
```js
// Pre-load default rates on page load
async function loadDefaultRates(branchId) {
  const res = await fetch(`${API_URL}/api/bilty/rates/default?branch_id=${branchId}`);
  const { data } = await res.json();
  defaultRateByCity.current = data.rate_by_city_id;
}

// When consignor has no profile for this city, use default
function getRate(cityId) {
  const consignorRates = consignorRatesRef.current[cityId];
  if (consignorRates && consignorRates.length > 0) {
    return consignorRates[0].rate;  // Consignor-specific rate
  }
  return defaultRateByCity.current[cityId] || 0;  // Default branch rate
}
```

---

### 6. `GET /api/bilty/rates/all` — Both Rates in One Call (Parallel)

Fetches **both** consignor-specific AND default branch rates in parallel.  
Use this when you need both at once (e.g., when consignor changes).

**Query Parameters:**
| Param | Required | Description |
|-------|----------|-------------|
| `consignor_id` | Yes | Consignor UUID |
| `branch_id` | Yes | Branch UUID |

**Example Request:**
```js
const res = await fetch(
  `${API_URL}/api/bilty/rates/all?consignor_id=${consignorId}&branch_id=${branchId}`
);
const { data } = await res.json();
```

**Response:**
```json
{
  "status": "success",
  "data": {
    "consignor_rates": [ { "...consignor_bilty_profile rows..." } ],
    "consignor_rates_by_city": {
      "city-uuid": [ { "...rate objects..." } ]
    },
    "default_rates": [ { "...rates table rows..." } ],
    "default_rate_by_city_id": {
      "city-uuid": 4.5
    }
  }
}
```

**Frontend usage — smart rate resolution:**
```js
async function onConsignorChange(consignorId) {
  const res = await fetch(
    `${API_URL}/api/bilty/rates/all?consignor_id=${consignorId}&branch_id=${user.branch_id}`
  );
  const { data } = await res.json();

  consignorRatesRef.current = data.consignor_rates_by_city;
  defaultRateByCity.current = data.default_rate_by_city_id;
}

function getSmartRate(cityId) {
  // Priority: consignor profile rate > default branch rate > 0
  const profile = consignorRatesRef.current[cityId];
  if (profile && profile.length > 0) {
    return {
      rate: profile[0].rate,
      rate_unit: profile[0].rate_unit,
      labour_rate: profile[0].labour_rate,
      transport_name: profile[0].transport_name,
      source: 'consignor_profile'
    };
  }
  const defRate = defaultRateByCity.current[cityId];
  if (defRate) {
    return { rate: defRate, rate_unit: 'PER_KG', labour_rate: 0, transport_name: null, source: 'default' };
  }
  return { rate: 0, rate_unit: 'PER_KG', labour_rate: 0, transport_name: null, source: 'none' };
}
```

---

## Error Responses

All endpoints return errors in the same format:
```json
{
  "status": "error",
  "message": "Description of what went wrong"
}
```

| HTTP Code | Meaning |
|-----------|---------|
| 400 | Missing required fields or invalid city_id |
| 409 | Duplicate GR number for this branch |
| 500 | Server error |

---

## Speed Summary

| Operation | Before (Frontend) | After (Backend API) |
|-----------|-------------------|---------------------|
| Page load data | 8 Supabase calls sequential ~2-3s | 1 API call, 7 parallel queries ~200-400ms |
| Save bilty | Direct Supabase + re-fetch cities | 1 API call, parallel validation ~100-200ms |
| PDF city data | Re-fetches from Supabase (fails on slow network) | Returned in save response (guaranteed) |
| Next GR after save | Separate `/next-available` call (can fail under load) | `next_gr_no` returned in save response (guaranteed) |
| Rate save | Blocking (adds ~100ms to save) | Background (non-blocking, retries on connection error) |
| Bill book update | Frontend-calculated (stale, can drift) | Server safety-checked: always `highest_used + 1` |
| Consignor rates | Frontend fetches one by one | 1 API call with city lookup map |
| Default rates | Loaded in reference-data | Dedicated endpoint with city-id map |
| Both rates | 2 sequential calls | 1 call, 2 parallel queries |

---

## Quick Test with cURL

**Health check:**
```bash
curl http://localhost:5000/api/health
```

**Load reference data:**
```bash
curl "http://localhost:5000/api/bilty/reference-data?branch_id=YOUR_BRANCH_UUID&user_id=YOUR_USER_UUID"
```

**Save a bilty:**
```bash
curl -X POST http://localhost:5000/api/bilty/save \
  -H "Content-Type: application/json" \
  -d '{
    "branch_id": "your-branch-uuid",
    "staff_id": "your-user-uuid",
    "gr_no": "A08044",
    "bilty_date": "2026-04-06",
    "from_city_id": "from-city-uuid",
    "to_city_id": "to-city-uuid",
    "consignor_name": "TEST CONSIGNOR",
    "consignee_name": "TEST CONSIGNEE",
    "no_of_pkg": 5,
    "wt": 100,
    "total": 500,
    "saving_option": "DRAFT"
  }'
```

**Fetch bilty for reprint:**
```bash
curl "http://localhost:5000/api/bilty/YOUR_BILTY_UUID"
```

**Consignor rates:**
```bash
curl "http://localhost:5000/api/bilty/rates/consignor/YOUR_CONSIGNOR_UUID"
```

**Default rates:**
```bash
curl "http://localhost:5000/api/bilty/rates/default?branch_id=YOUR_BRANCH_UUID"
```

**Both rates (parallel):**
```bash
curl "http://localhost:5000/api/bilty/rates/all?consignor_id=YOUR_CONSIGNOR_UUID&branch_id=YOUR_BRANCH_UUID"
```


  // 1. STOP calculating billBookNextNumber locally
- let billBookNextNumber = selectedBillBook.current_number + 1;

  // 2. STOP sending it in the payload
- bill_book_next_number: billBookNextNumber,

  // 3. READ from server response
+ const serverCurrentNumber = result.data.new_current_number;

  // 4. SYNC local state from server
+ if (serverCurrentNumber != null) {
+   setSelectedBillBook(prev => ({
+     ...prev, current_number: serverCurrentNumber
+   }));
+ }
- setSelectedBillBook(prev => ({
-   ...prev, current_number: billBookNextNumber
- }));