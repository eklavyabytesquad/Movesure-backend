-- Migration: Add Payment Tracking to Bilty and Station Summary Tables
-- Date: 2026-05-15
-- Purpose: Track payment details (mode, advance, remaining, status) without affecting existing data
-- Safe: All columns are DEFAULT NULL and use conditional logic for backwards compatibility

-- ========================================================================
-- 1. ALTER bilty TABLE
-- ========================================================================

-- Add payment tracking JSONB column to store complete payment history
ALTER TABLE public.bilty
ADD COLUMN IF NOT EXISTS payment_details JSONB DEFAULT NULL;

-- Add payment status for quick filtering (PENDING, PARTIAL, PAID, FOC)
ALTER TABLE public.bilty
ADD COLUMN IF NOT EXISTS payment_status VARCHAR DEFAULT NULL;

-- Add advance amount column for quick access (partial payments)
ALTER TABLE public.bilty
ADD COLUMN IF NOT EXISTS advance_amount NUMERIC DEFAULT NULL;

-- Add remaining amount column for quick access
ALTER TABLE public.bilty
ADD COLUMN IF NOT EXISTS remaining_amount NUMERIC DEFAULT NULL;

-- Create index for faster payment_status queries
CREATE INDEX IF NOT EXISTS idx_bilty_payment_status
ON public.bilty(payment_status)
WHERE payment_status IS NOT NULL;

-- Create index for payment_details queries if needed
CREATE INDEX IF NOT EXISTS idx_bilty_payment_details
ON public.bilty USING GIN(payment_details);

-- ========================================================================
-- 2. ALTER station_bilty_summary TABLE
-- ========================================================================

-- Add payment tracking JSONB column to store complete payment history
ALTER TABLE public.station_bilty_summary
ADD COLUMN IF NOT EXISTS payment_details JSONB DEFAULT NULL;

-- Add payment status for quick filtering
ALTER TABLE public.station_bilty_summary
ADD COLUMN IF NOT EXISTS payment_status VARCHAR DEFAULT NULL;

-- Add advance amount column for quick access
ALTER TABLE public.station_bilty_summary
ADD COLUMN IF NOT EXISTS advance_amount NUMERIC DEFAULT NULL;

-- Add remaining amount column for quick access
ALTER TABLE public.station_bilty_summary
ADD COLUMN IF NOT EXISTS remaining_amount NUMERIC DEFAULT NULL;

-- Create index for faster payment_status queries
CREATE INDEX IF NOT EXISTS idx_station_bilty_summary_payment_status
ON public.station_bilty_summary(payment_status)
WHERE payment_status IS NOT NULL;

-- Create index for payment_details queries if needed
CREATE INDEX IF NOT EXISTS idx_station_bilty_summary_payment_details
ON public.station_bilty_summary USING GIN(payment_details);

-- ========================================================================
-- 3. OPTIONAL: UPDATE EXISTING RECORDS (if migration from payment_mode)
-- ========================================================================

-- For bilties where payment_mode is already set, initialize payment_details
-- This ensures backward compatibility with existing payment_mode field
-- NOTE: Uncomment if you have existing data in payment_mode field and want to migrate

/*
UPDATE public.bilty
SET payment_details = jsonb_build_object(
    'payment_mode', payment_mode,
    'advance_amount', 0,
    'remaining_amount', COALESCE(total, 0),
    'paid_amount', 0,
    'payment_date', NULL,
    'notes', NULL,
    'created_at', NOW()
),
payment_status = CASE
    WHEN payment_mode = 'PAID' THEN 'PAID'
    WHEN payment_mode = 'TO-PAY' THEN 'PENDING'
    WHEN payment_mode = 'PARTIAL' THEN 'PARTIAL'
    WHEN payment_mode = 'FOC' THEN 'FOC'
    ELSE NULL
END,
remaining_amount = COALESCE(total, 0)
WHERE payment_details IS NULL AND payment_mode IS NOT NULL;
*/

-- ========================================================================
-- COMMENTS FOR PAYMENT_DETAILS JSONB STRUCTURE
-- ========================================================================

/*
payment_details JSONB structure:
{
  "payment_mode": "cash|online|partial|foc",
  "advance_amount": <numeric>,          -- Amount paid in advance
  "remaining_amount": <numeric>,        -- Amount still pending
  "paid_amount": <numeric>,             -- Total paid so far
  "payment_date": "2026-05-15",         -- Date of last payment
  "payment_method": "cash|cheque|bank_transfer|upi",
  "reference_number": "CHQ123456",      -- Cheque/transaction reference
  "notes": "partial payment collected", -- Additional notes
  "transactions": [                     -- Payment history
    {
      "date": "2026-05-10",
      "amount": 5000,
      "method": "cash",
      "reference": "RECEIPT-001",
      "notes": "advance payment"
    }
  ],
  "created_at": "2026-05-15T10:30:00Z",
  "updated_at": "2026-05-15T11:45:00Z"
}

payment_status values:
- PENDING    : No payment received yet
- PARTIAL    : Some amount received, balance pending
- PAID       : Full amount paid
- FOC        : Free of Charge
- DISPUTED   : Payment disputed / under investigation
- CANCELLED  : Bill cancelled, payment not required
*/
