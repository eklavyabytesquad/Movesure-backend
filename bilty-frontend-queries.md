# Bilty Page – Complete Documentation

## Overview

The **Bilty Page** (`/bilty`) is the core form in the movesure.io transport management system used to create, edit, and print **bilties** (consignment notes / goods receipts). It is a feature-rich, multi-section form built with **Next.js (App Router)** and **Supabase** as the backend.

A bilty is the primary document in a transport company — it records the shipment of goods from a consignor (sender) to a consignee (receiver) via a specific route, with all associated charges.

---

## Page Location

```
src/app/bilty/page.js          → Main page component (BiltyForm)
src/components/bilty/           → All sub-components (23 files)
src/utils/grReservation.js      → GR reservation system hook
src/utils/biltyPdfUpload.js     → Background PDF generation & upload
```

---

## How the Page Works (Flow)

### 1. Page Load (`loadInitialData`)
When the page mounts, it checks for an authenticated user and loads **all reference data in parallel** from Supabase:

| Data Loaded         | Table              | Filter                          |
|---------------------|--------------------|----------------------------------|
| Branch info         | `branches`         | `id = user.branch_id`           |
| Cities              | `cities`           | All, ordered by name             |
| Transports          | `transports`       | All                              |
| Rates               | `rates`            | `branch_id = user.branch_id`    |
| Consignors          | `consignors`       | All, ordered by company name     |
| Consignees          | `consignees`       | All, ordered by company name     |
| Bill Books          | `bill_books`       | Active, not completed, by branch |
| Existing Bilties    | `bilty`            | Active, by branch, newest first  |

After loading, it:
- Sets the **from city** based on the user's branch
- Selects the **default bill book** and generates the first GR number
- Checks `localStorage` for **edit data** (if user came from bilty list to edit)

### 2. GR Number Generation
The GR (Goods Receipt) number is auto-generated from the selected **bill book**:
```
GR = prefix + padded(current_number, digits) + postfix
Example: A08044 → prefix="A", current_number=8044, digits=5
```

A **smart GR system** (`getNextAvailableGR`) skips numbers that are:
- Already reserved by other users (via GR reservation system)
- Already used in existing bilties

### 3. GR Reservation System (Multi-User)
The `useGRReservation` hook enables **concurrent bilty creation** by multiple users on the same branch:

- Uses **Supabase RPC functions** (`reserve_next_gr`, `reserve_specific_gr`, `release_gr_reservation`, etc.) for atomic operations
- **Realtime subscriptions** keep all users in sync about who has reserved which GR numbers
- **Heartbeat** (every 5 minutes) keeps reservations alive
- Auto-releases reservations on page close/unmount
- Supports **range reservations** (batch reserve multiple GRs)
- `completeAndReserveNext()` atomically completes a reservation and grabs the next one on save

### 4. Form Filling
The form is divided into **5 visual sections**, each rendered by a dedicated component:

| Section | Component | What It Captures |
|---------|-----------|------------------|
| **Row 1** | `GRNumberSection` | GR number, bill book selection, edit mode toggle, existing bilty search |
| **Live Status** | `GRLiveStatus` | Shows all GR reservations in the branch (who has what) |
| **Row 2** | `CityTransportSection` | From/To city, transport/transporter details |
| **Row 3** | `ConsignorConsigneeSection` | Consignor & consignee with autocomplete, GST, phone |
| **Row 4** | `InvoiceDetailsSection` | Invoice number, value, date, e-way bill, content, payment mode |
| **Row 5** | `PackageChargesSection` | Packages, weight, rate, freight, labour, tolls, total + Save/Draft buttons |

### 5. Smart Rate & Charge Calculation
When the user fills in consignor, destination city, and weight:

1. **Rate lookup** follows a 3-tier priority:
   - **Tier 1:** Consignor + Consignee + City specific rate (from `bilty` history)
   - **Tier 2:** Consignor + City specific rate (from `consignor_bilty_profile` or `bilty` history)
   - **Tier 3:** Default city rate (from `rates` table)

2. **Freight calculation** uses rate unit (PER_KG or FIXED) with minimum freight support
3. **Labour charge** = labour_rate × no_of_packages (rate from consignor profile or default)
4. **DD charge** applied based on consignor profile settings
5. **Total** = freight + labour + bill_charge + toll_charge + dd_charge + other_charge + pf_charge

### 6. Consignor/Consignee Auto-Save
When a bilty is saved, **new consignors/consignees** typed into the form (not selected from dropdown) are **automatically created** in the database:
- Checks for duplicates before saving
- Saves company name, GST number, and phone number
- Refreshes in-memory lists after creation

### 7. Save Process (`handleSave`)
The save process is comprehensive:

```
1. Block if saving already in progress (prevents double-submit)
2. Block if GR sequence error exists (for new bilties)
3. Auto-save new consignor/consignee if typed
4. Prepare saveData with type conversion and null handling
5. If EDIT MODE:
   a. Fetch old PDF URL for deletion
   b. Check GR conflicts with OTHER bilties
   c. UPDATE the bilty row
6. If NEW MODE:
   a. Check for existing GR number (prevent duplicates)
   b. INSERT new bilty row
   c. Auto-save rate to rates table (if rate > 0)
   d. Update bill book current_number (only for genuinely new bilties)
   e. Complete GR reservation and reserve next
7. Upload PDF to Supabase Storage (background, non-blocking)
8. Show print modal (for SAVE) or reset form (for DRAFT)
```

### 8. Automatic Rate Saving
After saving a non-draft bilty with a valid rate:
- Looks up the consignor in the `consignors` table (exact then partial match)
- Checks if a rate already exists for this branch + city + consignor combination
- **Updates** the rate if it differs, or **creates** a new rate entry
- Validates all foreign keys before insertion
- Refreshes the in-memory rates list

### 9. PDF Generation & Upload
After a successful save:
- `uploadBiltyPdf()` runs in the **background** (non-blocking)
- Uses **jsPDF** to generate a PDF with QR code, header, charges table, etc.
- Uploads to Supabase Storage bucket `pdf_bilty`
- Updates the `pdf_bucket` column on the bilty row with the public URL
- On edit: deletes the old PDF before uploading the new one

### 10. Print & WhatsApp Flow
After saving (non-draft):
1. **Print Modal** appears with options:
   - **Print + WhatsApp** (Enter key) – opens PDF viewer and sends WhatsApp notification
   - **Save Only** (Tab key) – skips printing, resets form
2. **Print Component** renders the bilty in a printable format
3. **WhatsApp Notification** sends a 9-variable template message with bilty details
4. After printing, form **always resets** to new bilty mode

### 11. Edit Mode
Users can edit existing bilties in two ways:
- **From bilty list page:** Data stored in `localStorage` as `editBiltyData`, loaded on page mount
- **From GR search:** Click an existing bilty in the GR number dropdown

In edit mode:
- GR reservation system is disabled
- Bill book current_number is NOT updated on save
- GR sequence validation is skipped
- Form shows an amber "EDITING: {gr_no}" indicator

### 12. Form Reset
`resetForm()` clears all fields to defaults while:
- Keeping the user's reserved GR (if any) or computing the next available GR
- Preserving the from_city (branch city)
- Resetting all charge amounts to defaults (bill_charge=50, toll_charge=20, total=70)
- Incrementing `resetKey` to force child component re-renders
- Clearing `localStorage` edit data

---

## Keyboard Shortcuts

| Shortcut | Action |
|----------|--------|
| `Ctrl+S` | Save bilty |
| `Ctrl+D` | Save as draft |
| `Ctrl+N` | New bilty (reset form) |
| `Ctrl+E` | Toggle edit mode |
| `Alt+N` | New bill |
| `Alt+C` | Go to challan page |
| `Alt+R` | Open rate search modal |
| `Enter` | Move to next field (via InputNavigationManager) |
| `Tab` | Standard tab navigation |

---

## Component Details

### Core Form Components

| File | Component | Purpose |
|------|-----------|---------|
| `grnumber-manager.js` | `GRNumberSection` | GR number selection, bill book dropdown, existing bilty search with pagination, rate search modal trigger |
| `city-transport.js` | `CityTransportSection` | From/To city dropdowns, transport name/GST/phone auto-fill |
| `consignor-consignee.js` | `ConsignorConsigneeSection` | Consignor & consignee autocomplete inputs with GST and phone fields, fires `consignorSelected` event for rate updates |
| `invoice.js` | `InvoiceDetailsSection` | Invoice number, value, date, e-way bill, document number, content, payment mode & delivery type (auto-fetched from consignor history) |
| `charges.js` | `PackageChargesSection` | Package count, weight, rate, all charge calculations, save/draft/reset buttons |

### Helper Modules

| File | Export | Purpose |
|------|--------|---------|
| `consignor-consignee-helper.js` | `addNewConsignor`, `addNewConsignee`, `checkDuplicateConsignor`, `checkDuplicateConsignee`, `getSimilarConsignors`, `findConsignorByGST`, etc. | CRUD operations and search for consignors/consignees |
| `consignor-profile-helper.js` | `useConsignorBiltyProfile`, `useConsignorBiltyProfileByName`, `calculateFreightWithMinimum`, `calculateLabourCharge`, `calculateDDCharge` | Consignor-specific billing profile with rate, labour, DD charge logic |
| `rate-helper.js` | `useHistoricalRate` | 3-tier historical rate lookup from bilty history and rates table |
| `labour-rate-helper.js` | `useConsignorLabourRate`, `LabourRateInfo` | Fetches most recent labour rate from consignor's bilty history |
| `payment-mode-helper.js` | `useConsignorPaymentMode` | Determines most common payment mode from last 10 bilties |
| `input-navigation.js` | `InputNavigationManager`, `useInputNavigation` | Keyboard navigation (Enter/Tab) between form fields with section support |

### Display & Print Components

| File | Component | Purpose |
|------|-----------|---------|
| `print-model.js` | `PrintModal` | Post-save modal with Print+WhatsApp / Save Only options |
| `print-bilty.js` | `PrintBilty` | Printable bilty document renderer |
| `pdf-generation.js` | `PDFGenerator` | jsPDF-based PDF generation with QR codes, signatures, city data |
| `pdf-viewer-ui.js` | `PDFViewerUI` | Responsive PDF viewer with download/print/refresh actions |
| `bilty-detail-popup.js` | `BiltyDetailPopup` | Modal showing full bilty details (used in search/history) |

### Rate & Stats Components

| File | Component | Purpose |
|------|-----------|---------|
| `rate-search-modal.js` | `RateSearchModal` | Modal to search historical rates by consignor, consignee, city |
| `consignor-rate-stats.js` | `ConsignorRateStats` | Statistical summary (total bilties, common rates, top cities) |
| `consignor-rate-table.js` | `ConsignorRateTable` | Tabular rate history with copy-to-clipboard and detail popup |
| `consignor-autocomplete.js` | `ConsignorAutocomplete` | Reusable autocomplete input for consignor selection |

### Other

| File | Component | Purpose |
|------|-----------|---------|
| `gr-live-status.js` | `GRLiveStatus` | Live panel showing all GR reservations in the branch |
| `whatsapp-notification.js` | `WhatsAppNotification` | Sends bilty confirmation via WhatsApp API with 9-variable template |
| `action.js` | `ActionButtonsSection` | Draft and reset buttons with shortcut hints |

---

## Supabase Database Tables Used

### Primary Tables

| Table | Purpose | Operations |
|-------|---------|------------|
| **`bilty`** | Core bilty records | INSERT, UPDATE, SELECT (search, history, rates, payment modes) |
| **`bill_books`** | GR number series management | SELECT, UPDATE (current_number, is_completed) |
| **`branches`** | Branch/office information | SELECT (user's branch) |

### Reference Tables

| Table | Purpose | Operations |
|-------|---------|------------|
| **`cities`** | City master list | SELECT (dropdowns, name lookup) |
| **`transports`** | Transporter master list | SELECT (dropdown) |
| **`consignors`** | Consignor (sender) master | SELECT, INSERT (autocomplete, auto-create) |
| **`consignees`** | Consignee (receiver) master | SELECT, INSERT (autocomplete, auto-create) |
| **`rates`** | City-wise rate configuration | SELECT, INSERT, UPDATE (rate lookup, auto-save) |

### Profile & Reservation Tables

| Table | Purpose | Operations |
|-------|---------|------------|
| **`consignor_bilty_profile`** | Consignor-specific billing config (rates, labour, DD charges) | SELECT |
| **`gr_reservations`** | Multi-user GR number locking | INSERT, UPDATE, DELETE via RPC functions |

### Storage

| Bucket | Purpose |
|--------|---------|
| **`pdf_bilty`** | Supabase Storage bucket for generated bilty PDFs |

---

## Bilty Table Schema (Key Fields)

| Column | Type | Description |
|--------|------|-------------|
| `id` | UUID | Primary key |
| `gr_no` | Text | Goods Receipt number (unique per branch) |
| `branch_id` | UUID | Branch that created the bilty |
| `staff_id` | UUID | Staff member who created it |
| `from_city_id` | UUID | Origin city |
| `to_city_id` | UUID | Destination city |
| `bilty_date` | Date | Date of bilty creation |
| `delivery_type` | Text | `godown-delivery` or `door-delivery` |
| `consignor_name` | Text | Sender company name |
| `consignor_gst` | Text | Sender GST number |
| `consignor_number` | Text | Sender phone |
| `consignee_name` | Text | Receiver company name |
| `consignee_gst` | Text | Receiver GST number |
| `consignee_number` | Text | Receiver phone |
| `transport_name` | Text | Transporter company name |
| `transport_gst` | Text | Transporter GST |
| `transport_number` | Text | Transporter phone |
| `transport_id` | UUID | FK to transports table |
| `payment_mode` | Text | `to-pay`, `paid`, `tbb`, etc. |
| `contain` | Text | Content/goods description |
| `invoice_no` | Text | Invoice number |
| `invoice_value` | Numeric | Invoice value |
| `invoice_date` | Date | Invoice date |
| `e_way_bill` | Text | E-Way Bill number |
| `document_number` | Text | Additional document reference |
| `no_of_pkg` | Integer | Number of packages |
| `wt` | Numeric | Weight (kg) |
| `rate` | Numeric | Rate per unit |
| `labour_rate` | Numeric | Labour rate per package (default: 20) |
| `pvt_marks` | Text | Private marks |
| `freight_amount` | Numeric | Calculated freight |
| `labour_charge` | Numeric | Calculated labour charge |
| `bill_charge` | Numeric | Bill charge (default: 50) |
| `toll_charge` | Numeric | Toll charge (default: 20) |
| `dd_charge` | Numeric | Door delivery charge |
| `other_charge` | Numeric | Other charges |
| `pf_charge` | Numeric | Platform charge |
| `total` | Numeric | Grand total |
| `remark` | Text | Remarks |
| `saving_option` | Text | `SAVE` or `DRAFT` |
| `is_active` | Boolean | Soft delete flag |
| `pdf_bucket` | Text | URL to generated PDF in Supabase Storage |
| `created_at` | Timestamp | Record creation time |

---

## Bill Books Table Schema

| Column | Type | Description |
|--------|------|-------------|
| `id` | UUID | Primary key |
| `branch_id` | UUID | Branch this book belongs to |
| `prefix` | Text | GR number prefix (e.g., "A") |
| `postfix` | Text | GR number postfix |
| `digits` | Integer | Number of digits to pad (e.g., 5) |
| `from_number` | Integer | Starting number in series |
| `to_number` | Integer | Ending number in series |
| `current_number` | Integer | Next number to be used |
| `is_active` | Boolean | Whether the book is active |
| `is_completed` | Boolean | Whether all numbers are used |
| `auto_continue` | Boolean | Whether to restart from `from_number` when `to_number` is reached |

---

## Rates Table Schema

| Column | Type | Description |
|--------|------|-------------|
| `id` | UUID | Primary key |
| `branch_id` | UUID | Branch |
| `city_id` | UUID | Destination city |
| `consignor_id` | UUID | Consignor (null for default rate) |
| `rate` | Numeric | Rate value |
| `is_default` | Boolean | True if no consignor (city default) |

---

## GR Reservation RPC Functions

| Function | Purpose |
|----------|---------|
| `reserve_next_gr` | Atomically reserve the next available GR number |
| `reserve_specific_gr` | Reserve a specific GR number by value |
| `release_gr_reservation` | Release a held reservation |
| `complete_gr_reservation` | Mark a reservation as completed (bilty saved) |
| `reserve_gr_range` | Batch reserve a range of GR numbers |

---

## Internal State Flags (formData)

These are **not saved to the database** but control UI behavior:

| Field | Purpose |
|-------|---------|
| `_rate_unit` | `PER_KG` or `FIXED` — how rate is applied |
| `_rate_unit_override` | Whether user manually overrode the rate unit |
| `_labour_unit` | `PER_NAG` — labour rate calculation mode |
| `_minimum_freight` | Minimum freight amount from consignor profile |
| `_is_minimum_applied` | Whether minimum freight was applied |
| `_dd_charge_applied` | DD charge from consignor profile |
| `_rs_charge_applied` | RS charge from consignor profile |

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────┐
│                  BiltyForm (page.js)             │
│                                                  │
│  ┌─────────────┐  ┌──────────────────────────┐  │
│  │ Auth Check   │  │ loadInitialData()        │  │
│  │ (useAuth)    │  │ → 8 parallel Supabase    │  │
│  └─────────────┘  │   queries                 │  │
│                    └──────────────────────────┘  │
│                                                  │
│  ┌──────────────────────────────────────────┐   │
│  │         useGRReservation Hook             │   │
│  │  • Realtime subscriptions                 │   │
│  │  • Heartbeat (5 min)                      │   │
│  │  • Reserve / Release / Complete           │   │
│  └──────────────────────────────────────────┘   │
│                                                  │
│  ┌────────────────────────────────────────────┐ │
│  │              FORM SECTIONS                  │ │
│  │                                             │ │
│  │  1. GRNumberSection                         │ │
│  │  2. GRLiveStatus                            │ │
│  │  3. CityTransportSection                    │ │
│  │  4. ConsignorConsigneeSection               │ │
│  │  5. InvoiceDetailsSection                   │ │
│  │  6. PackageChargesSection                   │ │
│  └────────────────────────────────────────────┘ │
│                                                  │
│  ┌──────────────┐  ┌────────────────────────┐   │
│  │ handleSave() │  │ resetForm()            │   │
│  │ → Validate   │  │ → Clear all fields     │   │
│  │ → Insert/Upd │  │ → Keep branch & GR     │   │
│  │ → Rate save  │  │ → Increment resetKey   │   │
│  │ → PDF upload │  └────────────────────────┘   │
│  │ → Print modal│                               │
│  └──────────────┘                               │
│                                                  │
│  ┌──────────────────────────────────────────┐   │
│  │           POST-SAVE FLOW                  │   │
│  │  PrintModal → PrintBilty/PDFGenerator     │   │
│  │            → WhatsAppNotification         │   │
│  │            → uploadBiltyPdf (background)  │   │
│  └──────────────────────────────────────────┘   │
└─────────────────────────────────────────────────┘
```

---

## Key Design Decisions

1. **No form validation on save** — the system allows saving with minimal data to avoid blocking workflow speed
2. **Background PDF upload** — PDF generation doesn't block the save flow; runs async after save completes
3. **Automatic rate learning** — every saved bilty teaches the system new rates for faster future entry
4. **Reset key pattern** — child components use a `resetKey` prop to force full re-render on form reset
5. **Event-based communication** — consignor selection fires a custom DOM event (`consignorSelected`) to update rates across components
6. **GR reservation via RPC** — atomic database functions prevent race conditions in multi-user scenarios
7. **Soft delete** — bilties are never hard-deleted; `is_active=false` marks them as deleted
