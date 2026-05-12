"""
Bulk-attach product images from a local folder to the local inventory database.

What it does
------------
1. Reads product SKUs from the local SQLite DB.
2. Tries to detect SKU for each image:
   - filename first (fast, most reliable when SKU is in file name),
   - optional OCR from image text using pytesseract.
3. Optimizes and copies matched images into static/product_images.
4. Updates products.image_filename for the matched SKU.

Usage
-----
python tools/bulk_attach_images_local.py \
  --folder "/path/to/images" \
  --db "boutique.db" \
  --images-dir "static/product_images"

Optional OCR mode:
python tools/bulk_attach_images_local.py --folder "/path/to/images" --ocr tesseract

Requirements for OCR mode:
- pip install pytesseract
- install tesseract binary (e.g. brew install tesseract on macOS)
"""

from __future__ import annotations

import argparse
import re
import shutil
import sqlite3
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from PIL import Image, UnidentifiedImageError
from werkzeug.utils import secure_filename

ALLOWED_IMAGE_EXTENSIONS = {"png", "jpg", "jpeg", "webp", "gif"}
MAX_IMAGE_SIZE = (1600, 1600)
FORMAT_MAP = {"jpg": "JPEG", "jpeg": "JPEG", "png": "PNG", "webp": "WEBP", "gif": "GIF"}


def normalize(text: str) -> str:
    """Strip non-alphanumeric chars and uppercase."""
    return re.sub(r"[^A-Za-z0-9]", "", text or "").upper()


def sku_filename_base(sku: str) -> str:
    """Format SKU for file naming, forcing dash style like CS-001 when possible."""
    norm = normalize(sku)
    match = re.match(r"^([A-Z]+)(\d+)$", norm)
    if match:
        return f"{match.group(1)}-{match.group(2)}"
    return secure_filename(sku) or norm or "SKU"


def extract_candidates(text: str) -> List[str]:
    """Extract SKU-like candidates from any text block."""
    candidates: List[str] = []

    # Explicitly capture patterns like "CS001", "CS-001", and "CS 001".
    for alpha, digits in re.findall(r"\b([A-Za-z]{1,5})\s*[-_/]?\s*(\d{2,5})\b", text):
        candidates.append(normalize(f"{alpha}{digits}"))

    for raw in re.findall(r"[A-Za-z0-9\-_/]{3,20}", text):
        norm = normalize(raw)
        if not norm:
            continue
        has_alpha = any(c.isalpha() for c in norm)
        has_digit = any(c.isdigit() for c in norm)
        if (has_alpha and has_digit) or (norm.isdigit() and len(norm) >= 3):
            candidates.append(norm)

    seen = set()
    out = []
    for c in candidates:
        if c not in seen:
            seen.add(c)
            out.append(c)
    return out


def match_sku(candidates: List[str], sku_index: Dict[str, str]) -> Optional[str]:
    """Return SKU as stored in DB for first matched candidate."""
    for cand in candidates:
        if cand in sku_index:
            return sku_index[cand]
        for norm_sku, real_sku in sku_index.items():
            if len(norm_sku) >= 4 and norm_sku in cand:
                return real_sku
    return None


def ocr_image_tesseract(path: Path) -> str:
    try:
        import pytesseract
    except ImportError as exc:
        raise RuntimeError("pytesseract is not installed. Run: pip install pytesseract") from exc

    with Image.open(path) as img:
        gray = img.convert("L")
        return pytesseract.image_to_string(gray)


def optimize_image(src: Path, dst: Path, extension: str) -> None:
    image_format = FORMAT_MAP.get(extension, "JPEG")
    try:
        with Image.open(src) as img:
            resample = Image.Resampling.LANCZOS if hasattr(Image, "Resampling") else Image.LANCZOS
            img.thumbnail(MAX_IMAGE_SIZE, resample)

            save_kwargs = {}
            if image_format == "JPEG":
                if img.mode not in ("RGB", "L"):
                    img = img.convert("RGB")
                save_kwargs = {"quality": 82, "optimize": True}
            elif image_format == "WEBP":
                if img.mode not in ("RGB", "RGBA"):
                    img = img.convert("RGBA" if "A" in img.getbands() else "RGB")
                save_kwargs = {"quality": 80, "method": 6}
            elif image_format == "PNG":
                save_kwargs = {"optimize": True, "compress_level": 7}
            elif image_format == "GIF" and img.mode not in ("P", "L"):
                img = img.convert("P", palette=Image.ADAPTIVE)
                save_kwargs = {"optimize": True}

            img.save(dst, format=image_format, **save_kwargs)
    except (UnidentifiedImageError, OSError, ValueError):
        shutil.copyfile(src, dst)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Attach product images to inventory using SKU detection.")
    parser.add_argument("--folder", required=True, help="Folder containing product images.")
    parser.add_argument("--db", default="boutique.db", help="Path to local SQLite DB (default: boutique.db).")
    parser.add_argument(
        "--images-dir",
        default="static/product_images",
        help="Destination directory for product images (default: static/product_images).",
    )
    parser.add_argument(
        "--ocr",
        choices=["off", "tesseract"],
        default="off",
        help="Use OCR for SKU extraction when filename match fails.",
    )
    parser.add_argument("--overwrite", action="store_true", help="Replace existing product image mapping.")
    parser.add_argument("--dry-run", action="store_true", help="Show updates without writing files/DB.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    folder = Path(args.folder).expanduser().resolve()
    db_path = Path(args.db).expanduser().resolve()
    images_dir = Path(args.images_dir).expanduser().resolve()

    if not folder.is_dir():
        print(f"ERROR: folder not found: {folder}", file=sys.stderr)
        return 2
    if not db_path.exists():
        print(f"ERROR: database not found: {db_path}", file=sys.stderr)
        return 2

    images = sorted(
        path for path in folder.iterdir()
        if path.is_file() and path.suffix.lower().lstrip(".") in ALLOWED_IMAGE_EXTENSIONS
    )
    if not images:
        print(f"No supported images found in {folder}")
        return 0

    if not args.dry_run:
        images_dir.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    rows = conn.execute(
        "SELECT id, name, sku, image_filename FROM products WHERE sku IS NOT NULL AND sku != ''"
    ).fetchall()

    sku_index: Dict[str, str] = {}
    products_by_sku: Dict[str, sqlite3.Row] = {}
    for row in rows:
        norm = normalize(row["sku"])
        if norm:
            sku_index[norm] = row["sku"]
            products_by_sku[row["sku"]] = row

    print(f"Found {len(images)} images in {folder}")
    print(f"Loaded {len(products_by_sku)} products with SKU from {db_path}")

    updated: List[str] = []
    skipped_existing: List[Tuple[str, str]] = []
    unmatched: List[Tuple[str, List[str]]] = []

    for img_path in images:
        ext = img_path.suffix.lower().lstrip(".")

        filename_candidates = extract_candidates(img_path.stem)
        sku = match_sku(filename_candidates, sku_index)

        ocr_candidates: List[str] = []
        if not sku and args.ocr == "tesseract":
            try:
                ocr_text = ocr_image_tesseract(img_path)
                ocr_candidates = extract_candidates(ocr_text)
                sku = match_sku(ocr_candidates, sku_index)
            except Exception as exc:  # noqa: BLE001
                print(f"  ! OCR failed for {img_path.name}: {exc}")

        if not sku:
            candidates = filename_candidates + [c for c in ocr_candidates if c not in filename_candidates]
            print(f"  ? {img_path.name}: no SKU match. candidates={candidates[:8]}")
            unmatched.append((img_path.name, candidates))
            continue

        product = products_by_sku[sku]
        if product["image_filename"] and not args.overwrite:
            print(
                f"  - {img_path.name} -> {sku}: skipped (already has image '{product['image_filename']}')"
            )
            skipped_existing.append((img_path.name, sku))
            continue

        filename_sku = sku_filename_base(sku)
        new_filename = f"{filename_sku}.{ext}"
        target = images_dir / new_filename

        if target.exists() and not args.overwrite:
            index = 2
            while True:
                candidate_name = f"{filename_sku}_{index}.{ext}"
                candidate_path = images_dir / candidate_name
                if not candidate_path.exists():
                    new_filename = candidate_name
                    target = candidate_path
                    break
                index += 1

        if args.dry_run:
            print(f"  [dry-run] {img_path.name} -> {sku}: would save {target.name}")
        else:
            optimize_image(img_path, target, ext)
            conn.execute(
                "UPDATE products SET image_filename = ?, updated_at = datetime('now','localtime') WHERE id = ?",
                (new_filename, product["id"]),
            )
            print(f"  + {img_path.name} -> {sku}: attached {target.name}")

        updated.append(sku)

    if not args.dry_run:
        conn.commit()
    conn.close()

    print("\n=== Summary ===")
    print(f"Images processed:        {len(images)}")
    print(f"Products updated:        {len(updated)}")
    print(f"Skipped (existing img):  {len(skipped_existing)}")
    print(f"Unmatched images:        {len(unmatched)}")

    if unmatched:
        print("\nUnmatched images:")
        for name, cands in unmatched:
            print(f"  - {name} candidates={cands[:6]}")

    not_updated = sorted(set(products_by_sku.keys()) - set(updated))
    print(f"\nInventory SKUs NOT updated in this run ({len(not_updated)}):")
    for sku in not_updated:
        current = products_by_sku[sku]["image_filename"] or ""
        marker = " (already had image)" if current else ""
        print(f"  - {sku}{marker}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
