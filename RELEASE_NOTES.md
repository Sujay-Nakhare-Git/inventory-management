# Release Notes

## v3.0.0 - 2026-08-21

### Rentals
- Added a dedicated Rentals tab in Create New Bill.
- Rental bills capture:
  - Customer name and phone number
  - Number of rental days
  - Daily rent amount
  - Refundable deposit amount
- Rental charges are calculated as `Number of Days × Daily Rent`.
- Total payable is calculated as `Rental Charges + Refundable Deposit`.
- Rental bills are available in bill history with rental-specific details and totals.

### Deposit Returns
- Added a one-time Return Deposit action for rental bills.
- Deposit returns are recorded in the refund history with the return timestamp.
- Added a dedicated printable deposit-return receipt showing:
  - Rental date
  - Deposit return date
  - Number of rental days
  - Daily rent and rental charges
  - Deposit paid, deposit returned, and net rental amount

### Compatibility
- Existing Sales billing, inventory deduction, refunds, and exchanges remain unchanged.
- Existing rental records continue to work with a default rental duration of one day.

## v2.0.0 - 2026-07-13

### Security
- Added Admin auto-logout after 20 minutes of inactivity.
- Admin login now requires password plus either:
  - fingerprint verification (WebAuthn-capable browser), or
  - Admin PIN.

### Reporting
- Daily Summary now shows:
  - Profit % (Without Expense)
  - Profit % (Including Expense)
- Profit & Loss now shows:
  - Profit % (Without Expense)
  - Profit % (Including Expense)

### Notes
- Fingerprint verification is browser/device dependent.
- PIN remains available as fallback (Default value set to 1234).

## v1.9.0 - 2026-06-15
- Modularized routing and shared core architecture.
- Refined Bill History vs bill detail access behavior.
