# Release Notes

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
