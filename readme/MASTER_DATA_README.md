# Master Data CRUD API

Manage **Cities, Transports, Transport Admin, Consignors, Consignees, Rates** — Create, Read, Update, Delete + Bulk operations.

All endpoints are under `/api/bilty/master/{entity}` where `entity` is one of:
- `cities`
- `transports`
- `transport_admin`
- `consignors`
- `consignees`
- `rates`

> **Note:** `created_by` and `updated_by` fields automatically resolve to **user names** (not UUIDs) in all responses.

## API Endpoints

### 1. `GET /api/bilty/master/{entity}` — List (Paginated)

Returns **40 rows per page** by default. Use `page` param for load-more.

**Query Parameters:**
| Param | Required | Default | Description |
|-------|----------|---------|-------------|
| `page` | No | 1 | Page number (1-based) |
| `page_size` | No | 40 | Rows per page (max recommended 100) |
| `search` | No | — | Text search across name/code/GST columns |
| `branch_id` | No | — | Filter by branch (for rates) |
| `city_id` | No | — | Filter by city (for transports, rates) |
| `consignor_id` | No | — | Filter by consignor (for rates) |

**Example — Load cities page 1:**
```js
const res = await fetch(`${API_URL}/api/bilty/master/cities?page=1`);
const { data } = await res.json();
// data.rows = [...40 cities]
// data.total = 350
// data.has_more = true
// data.page = 1
```

**Example — Search transports:**
```js
const res = await fetch(`${API_URL}/api/bilty/master/transports?search=KANPUR&page=1`);
```

**Example — Load more (page 2):**
```js
const res = await fetch(`${API_URL}/api/bilty/master/consignors?page=2`);
```

**Example — Rates filtered by branch:**
```js
const res = await fetch(`${API_URL}/api/bilty/master/rates?branch_id=${branchId}&page=1`);
```

**Response:**
```json
{
  "status": "success",
  "data": {
    "rows": [
      { "id": "uuid", "city_code": "ALG", "city_name": "ALIGARH", "created_by": "Eklav", "updated_by": "Eklav", "created_at": "2026-04-06T...", "updated_at": "2026-04-06T..." }
    ],
    "page": 1,
    "page_size": 40,
    "total": 350,
    "has_more": true
  }
}
```

> `created_by` / `updated_by` show the **user name** (resolved from `users.name`), not the UUID.

**Frontend — Infinite scroll / load-more:**
```js
const [rows, setRows] = useState([]);
const [page, setPage] = useState(1);
const [hasMore, setHasMore] = useState(true);
const [search, setSearch] = useState('');

async function loadData(entity, pageNum = 1, searchText = '') {
  const params = new URLSearchParams({ page: pageNum, page_size: 40 });
  if (searchText) params.set('search', searchText);
  
  const res = await fetch(`${API_URL}/api/bilty/master/${entity}?${params}`);
  const { data } = await res.json();
  
  if (pageNum === 1) {
    setRows(data.rows);          // Fresh load
  } else {
    setRows(prev => [...prev, ...data.rows]); // Append
  }
  setPage(pageNum);
  setHasMore(data.has_more);
}

// Initial load
useEffect(() => { loadData('cities'); }, []);

// Search (debounced)
function onSearch(text) {
  setSearch(text);
  loadData('cities', 1, text);   // Reset to page 1
}

// Load more button
function onLoadMore() {
  if (hasMore) loadData('cities', page + 1, search);
}
```

---

### 2. `GET /api/bilty/master/{entity}/{id}` — Get Single Record

```js
const res = await fetch(`${API_URL}/api/bilty/master/transports/${transportId}`);
const { data } = await res.json();
// data = { id, transport_name, city_id, city_name, ... }
```

---

### 3. `POST /api/bilty/master/{entity}` — Create

**Request:**
```js
const res = await fetch(`${API_URL}/api/bilty/master/cities`, {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({
    user_id: user.id,        // for audit trail
    city_code: 'NEW',
    city_name: 'NEW CITY',
  }),
});
```

**Create Transport:**
```js
await fetch(`${API_URL}/api/bilty/master/transports`, {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({
    user_id: user.id,
    transport_name: 'XYZ TRANSPORT',
    city_id: 'city-uuid',
    city_name: 'KANPUR',
    gst_number: '09XXXXX',
    mob_number: '9876543210',
    address: 'TRANSPORT NAGAR',
    branch_owner_name: 'RAJESH',
    is_prior: false,
  }),
});
```

**Create Consignor:**
```js
await fetch(`${API_URL}/api/bilty/master/consignors`, {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({
    user_id: user.id,
    company_name: 'ABC INDUSTRIES',
    company_add: '123 MAIN ROAD, KANPUR',
    number: '9876543210',
    gst_num: '09AABFM8846M1ZM',
    adhar: '1234-5678-9012',
    pan: 'ABCDE1234F',
  }),
});
```

**Create Rate:**
```js
await fetch(`${API_URL}/api/bilty/master/rates`, {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({
    user_id: user.id,
    branch_id: user.branch_id,
    city_id: 'city-uuid',
    consignor_id: 'consignor-uuid',
    rate: 4.5,
    is_default: false,
  }),
});
```

**Create Transport Admin:**
```js
await fetch(`${API_URL}/api/bilty/master/transport_admin`, {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({
    user_id: user.id,
    transport_name: 'XYZ LOGISTICS HUB',
    gstin: '09XXXXX',
    hub_mobile_number: '9876543210',
    owner_name: 'RAJESH KUMAR',
    website: 'https://example.com',
    address: 'TRANSPORT NAGAR, KANPUR',
  }),
});
```

**Response:**
```json
{
  "status": "success",
  "message": "City created",
  "data": { "id": "new-uuid", "city_code": "NEW", "city_name": "NEW CITY", "created_at": "..." }
}
```

---

### 4. `PUT /api/bilty/master/{entity}/{id}` — Update

Only send fields you want to change. `updated_at` is set automatically.

```js
await fetch(`${API_URL}/api/bilty/master/cities/${cityId}`, {
  method: 'PUT',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({
    user_id: user.id,
    city_name: 'UPDATED CITY NAME',
  }),
});
```

**Update transport priority:**
```js
await fetch(`${API_URL}/api/bilty/master/transports/${transportId}`, {
  method: 'PUT',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({
    user_id: user.id,
    is_prior: true,
  }),
});
```

---

### 5. `DELETE /api/bilty/master/{entity}/{id}` — Delete

```js
await fetch(`${API_URL}/api/bilty/master/consignees/${consigneeId}`, {
  method: 'DELETE',
});
```

**Response:**
```json
{ "status": "success", "message": "Consignee deleted" }
```

---

### 6. `POST /api/bilty/master/{entity}/bulk-create` — Bulk Create

Create multiple records in a single call.

```js
await fetch(`${API_URL}/api/bilty/master/cities/bulk-create`, {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({
    user_id: user.id,
    records: [
      { city_code: 'AA1', city_name: 'CITY ONE' },
      { city_code: 'AA2', city_name: 'CITY TWO' },
      { city_code: 'AA3', city_name: 'CITY THREE' },
    ],
  }),
});
```

**Response:**
```json
{
  "status": "success",
  "message": "3 cities created",
  "data": [ { "id": "uuid-1", ... }, { "id": "uuid-2", ... }, { "id": "uuid-3", ... } ]
}
```

---

### 7. `PUT /api/bilty/master/{entity}/bulk-update` — Bulk Update (Edit Multiple)

Each item must have `id` + the fields to change.

```js
await fetch(`${API_URL}/api/bilty/master/transports/bulk-update`, {
  method: 'PUT',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({
    user_id: user.id,
    updates: [
      { id: 'uuid-1', is_prior: true },
      { id: 'uuid-2', mob_number: '9999999999' },
      { id: 'uuid-3', address: 'NEW ADDRESS' },
    ],
  }),
});
```

**Bulk edit rates:**
```js
await fetch(`${API_URL}/api/bilty/master/rates/bulk-update`, {
  method: 'PUT',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({
    user_id: user.id,
    updates: [
      { id: 'rate-uuid-1', rate: 5.0 },
      { id: 'rate-uuid-2', rate: 3.5 },
      { id: 'rate-uuid-3', rate: 7.0, is_default: true },
    ],
  }),
});
```

**Response:**
```json
{
  "status": "success",
  "message": "Bulk update: 3 updated, 0 failed",
  "data": { "success_count": 3, "failed_count": 0, "failed": [] }
}
```

---

### 8. `POST /api/bilty/master/{entity}/bulk-delete` — Bulk Delete

```js
await fetch(`${API_URL}/api/bilty/master/cities/bulk-delete`, {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({
    ids: ['uuid-1', 'uuid-2', 'uuid-3'],
  }),
});
```

**Response:**
```json
{
  "status": "success",
  "message": "3 cities deleted",
  "data": { "deleted_count": 3 }
}
```

---

## Table Schemas (after ALTER)

### Cities
| Column | Type | Description |
|--------|------|-------------|
| id | uuid | PK |
| city_code | text | e.g. "ALG" |
| city_name | text | e.g. "ALIGARH" |
| created_by | uuid | Resolved to user name in response |
| updated_by | uuid | Resolved to user name in response |
| created_at | timestamptz | When created |
| updated_at | timestamptz | When last updated |

### Transports
| Column | Type | Description |
|--------|------|-------------|
| id | uuid | PK |
| transport_name | text | e.g. "S S TRANSPORT" |
| city_id | uuid | FK → cities |
| city_name | text | Denormalized city name |
| address | text | Address |
| gst_number | text | GST number |
| mob_number | text | Mobile |
| branch_owner_name | text | Branch owner |
| website | text | Website URL |
| transport_admin_id | uuid | FK → transport_admin |
| is_prior | boolean | Priority transport for city |
| created_by | uuid | Resolved to user name in response |
| updated_by | uuid | Resolved to user name in response |
| created_at | timestamptz | When created |
| updated_at | timestamptz | When last updated |

### Transport Admin
| Column | Type | Description |
|--------|------|-------------|
| transport_id | uuid | PK (auto-generated) |
| transport_name | text | Admin transport name |
| gstin | text | GST number |
| hub_mobile_number | text | Hub mobile number |
| owner_name | text | Owner name |
| website | text | Website URL |
| address | text | Address |
| sample_ref_image | text | Reference image URL |
| sample_challan_image | text | Challan image URL |
| created_by | uuid | Resolved to user name in response |
| updated_by | uuid | Resolved to user name in response |
| created_at | timestamptz | When created |
| updated_at | timestamptz | When last updated |

> **Note:** `transport_admin` uses `transport_id` as its primary key (not `id`). Pass `transport_id` in GET/PUT/DELETE URLs.

### Consignors
| Column | Type | Description |
|--------|------|-------------|
| id | uuid | PK |
| company_name | text | Company name |
| company_add | text | Address |
| number | text | Phone |
| gst_num | text | GST number |
| adhar | text | Aadhaar |
| pan | text | PAN |
| created_by | uuid | Resolved to user name in response |
| updated_by | uuid | Resolved to user name in response |
| created_at | timestamptz | When created |
| updated_at | timestamptz | When last updated |

### Consignees
| Column | Type | Description |
|--------|------|-------------|
| id | uuid | PK |
| company_name | text | Company name |
| company_add | text | Address |
| number | text | Phone |
| gst_num | text | GST number |
| adhar | text | Aadhaar |
| pan | text | PAN |
| created_by | uuid | Resolved to user name in response |
| updated_by | uuid | Resolved to user name in response |
| created_at | timestamptz | When created |
| updated_at | timestamptz | When last updated |

### Rates
| Column | Type | Description |
|--------|------|-------------|
| id | uuid | PK |
| branch_id | uuid | FK → branches |
| city_id | uuid | FK → cities |
| consignor_id | uuid | FK → consignors |
| rate | numeric | Rate value |
| is_default | boolean | Default rate flag |
| created_by | uuid | Resolved to user name in response |
| updated_by | uuid | Resolved to user name in response |
| created_at | timestamptz | When created |
| updated_at | timestamptz | When last updated |

---

## Searchable Columns

| Entity | Search works on |
|--------|----------------|
| cities | city_code, city_name |
| transports | transport_name, city_name, gst_number |
| transport_admin | transport_name, gstin, owner_name |
| consignors | company_name, gst_num, number |
| consignees | company_name, gst_num, number |
| rates | *(no text search — use filters: branch_id, city_id, consignor_id)* |

---

## Error Responses

```json
{ "status": "error", "message": "Description" }
```

| Code | Meaning |
|------|---------|
| 400 | Invalid entity name or missing fields |
| 404 | Record not found |
| 409 | Duplicate entry |
| 500 | Server error |

---

## Frontend Integration — Full CRUD Page Example

```jsx
const ENTITY = 'consignors'; // or cities, transports, consignees, rates

function MasterDataPage() {
  const [rows, setRows] = useState([]);
  const [page, setPage] = useState(1);
  const [hasMore, setHasMore] = useState(true);
  const [search, setSearch] = useState('');
  const [loading, setLoading] = useState(false);
  const [editingRow, setEditingRow] = useState(null);

  // ── LOAD (paginated) ──
  async function loadData(pg = 1, searchText = search) {
    setLoading(true);
    const params = new URLSearchParams({ page: pg, page_size: 40 });
    if (searchText) params.set('search', searchText);
    
    const res = await fetch(`${API_URL}/api/bilty/master/${ENTITY}?${params}`);
    const { data } = await res.json();
    
    setRows(pg === 1 ? data.rows : prev => [...prev, ...data.rows]);
    setPage(pg);
    setHasMore(data.has_more);
    setLoading(false);
  }

  useEffect(() => { loadData(); }, []);

  // ── SEARCH (debounced) ──
  const debouncedSearch = useMemo(
    () => debounce((text) => loadData(1, text), 300),
    []
  );
  function onSearch(e) {
    setSearch(e.target.value);
    debouncedSearch(e.target.value);
  }

  // ── CREATE ──
  async function onCreate(formData) {
    const res = await fetch(`${API_URL}/api/bilty/master/${ENTITY}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ...formData, user_id: user.id }),
    });
    const result = await res.json();
    if (result.status === 'success') {
      loadData(1);  // Refresh
    }
    return result;
  }

  // ── UPDATE ──
  async function onUpdate(id, changes) {
    const res = await fetch(`${API_URL}/api/bilty/master/${ENTITY}/${id}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ...changes, user_id: user.id }),
    });
    const result = await res.json();
    if (result.status === 'success') {
      setRows(prev => prev.map(r => r.id === id ? { ...r, ...result.data } : r));
    }
    return result;
  }

  // ── DELETE ──
  async function onDelete(id) {
    if (!confirm('Are you sure?')) return;
    const res = await fetch(`${API_URL}/api/bilty/master/${ENTITY}/${id}`, {
      method: 'DELETE',
    });
    const result = await res.json();
    if (result.status === 'success') {
      setRows(prev => prev.filter(r => r.id !== id));
    }
  }

  // ── BULK UPDATE (e.g. inline table edits) ──
  async function onBulkSave(changedRows) {
    // changedRows = [{ id: 'uuid', field: 'newValue' }, ...]
    const res = await fetch(`${API_URL}/api/bilty/master/${ENTITY}/bulk-update`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ user_id: user.id, updates: changedRows }),
    });
    return await res.json();
  }

  // ── BULK DELETE ──
  async function onBulkDelete(selectedIds) {
    if (!confirm(`Delete ${selectedIds.length} items?`)) return;
    const res = await fetch(`${API_URL}/api/bilty/master/${ENTITY}/bulk-delete`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ids: selectedIds }),
    });
    const result = await res.json();
    if (result.status === 'success') {
      setRows(prev => prev.filter(r => !selectedIds.includes(r.id)));
    }
  }

  return (
    <div>
      <input placeholder="Search..." value={search} onChange={onSearch} />
      <button onClick={() => setEditingRow({})}>+ Add New</button>
      
      <table>
        {rows.map(row => (
          <tr key={row.id}>
            {/* render columns */}
            <td><button onClick={() => onDelete(row.id)}>Delete</button></td>
          </tr>
        ))}
      </table>
      
      {hasMore && (
        <button onClick={() => loadData(page + 1)} disabled={loading}>
          {loading ? 'Loading...' : 'Load More'}
        </button>
      )}
    </div>
  );
}
```
