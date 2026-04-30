"""
Bulk-attach product images from a local folder to the inventory database
hosted on PythonAnywhere.

Workflow
--------
1. Download the remote SQLite DB via the PythonAnywhere Files API.
2. OCR each image in the local folder with pytesseract and extract SKU
   candidates from the recognised text.
3. Match candidates against ``products.sku`` (case-insensitive, ignoring
   non-alphanumeric characters such as the dash in ``CS-008`` vs ``CS008``).
4. For matched images: optimise the image, upload it to the remote
   ``static/product_images`` folder, and set ``products.image_filename``
   in the local copy of the DB.
5. Upload the modified DB back to PythonAnywhere.
6. Print a report listing:
   - Images that could not be matched to any SKU.
   - Inventory SKUs that were *not* updated (no image attached in this run).

Requirements
------------
- ``pip install pillow pytesseract requests``
- ``brew install tesseract`` (macOS) or equivalent for your platform.
- Environment variable ``PA_API_TOKEN`` set to a PythonAnywhere API token
  (Account -> API token).

Usage
-----
    export PA_API_TOKEN=...
    python tools/bulk_attach_images_pa.py \
        --folder "/Users/sujay/Documents/Personal/Gulmohar/untitled folder/untitled folder" \
        --username SujayNakhare \
        --remote-db /home/SujayNakhare/inventory-management/boutique.db \
        --remote-images-dir /home/SujayNakhare/inventory-management/static/product_images

Add ``--dry-run`` to see what would happen without writing anything to PA.
Add ``--overwrite`` to replace images on products that already have one.
"""
from __future__ import annotations

import argparse
import base64
import io
import json
import os
import re
import shutil
import sqlite3
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
from PIL import Image, UnidentifiedImageError
from werkzeug.utils import secure_filename


PA_HOST = "https://www.pythonanywhere.com"
ALLOWED_IMAGE_EXTENSIONS = {"png", "jpg", "jpeg", "webp", "gif"}
MAX_IMAGE_SIZE = (1600, 1600)
FORMAT_MAP = {"jpg": "JPEG", "jpeg": "JPEG", "png": "PNG", "webp": "WEBP", "gif": "GIF"}


# ---------------------------------------------------------------------------
# PythonAnywhere Files API helpers
# ---------------------------------------------------------------------------
class PAClient:
    def __init__(self, username: str, token: str):
        self.username = username
        self.session = requests.Session()
        self.session.headers["Authorization"] = f"Token {token}"

    def _files_url(self, remote_path: str) -> str:
        # remote_path must be absolute, e.g. /home/user/foo.db
        return f"{PA_HOST}/api/v0/user/{self.username}/files/path{remote_path}"

    def download(self, remote_path: str, local_path: Path) -> None:
        resp = self.session.get(self._files_url(remote_path), stream=True)
        resp.raise_for_status()
        with open(local_path, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=64 * 1024):
                if chunk:
                    fh.write(chunk)

    def upload(self, remote_path: str, local_path: Path) -> None:
        with open(local_path, "rb") as fh:
            resp = self.session.post(
                self._files_url(remote_path),
                files={"content": (os.path.basename(remote_path), fh)},
            )
        if resp.status_code not in (200, 201):
            raise RuntimeError(
                f"Upload failed for {remote_path}: {resp.status_code} {resp.text}"
            )


# ---------------------------------------------------------------------------
# OCR + SKU matching
# ---------------------------------------------------------------------------
def normalize(text: str) -> str:
    """Strip everything that isn't a letter/digit and uppercase."""
    return re.sub(r"[^A-Za-z0-9]", "", text or "").upper()


def ocr_image_tesseract(path: Path) -> str:
    """Run tesseract on the image and return raw text."""
    try:
        import pytesseract
    except ImportError:
        print("ERROR: pytesseract not installed. Run: pip install pytesseract", file=sys.stderr)
        sys.exit(1)
    with Image.open(path) as img:
        gray = img.convert("L")
        return pytesseract.image_to_string(gray)


def _downscale_to_jpeg_b64(path: Path, max_side: int = 1280, quality: int = 80) -> str:
    """Downscale image and return base64 JPEG — keeps API payloads small."""
    with Image.open(path) as img:
        if img.mode not in ("RGB", "L"):
            img = img.convert("RGB")
        resample = Image.Resampling.LANCZOS if hasattr(Image, "Resampling") else Image.LANCZOS
        img.thumbnail((max_side, max_side), resample)
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=quality, optimize=True)
    return base64.b64encode(buf.getvalue()).decode("ascii")


def _vision_prompt(allowed_skus: List[str]) -> str:
    sku_hint = ", ".join(allowed_skus[:200])
    return (
        "You are reading a clothing price tag. Find the SKU / style code on the tag. "
        "It is usually handwritten near the bottom (e.g. 'CS008', 'CS-008', 'K-012', 'KS-031'). "
        "Ignore the size, price, style name, and brand address.\n"
        f"The SKU MUST be one of these known SKUs (case-insensitive, dashes optional): {sku_hint}.\n"
        "Respond with ONLY a compact JSON object: "
        '{"sku": "<best match or empty string>", "raw": "<text you actually saw>"}. '
        "No prose, no code fences."
    )


def _parse_vision_response(content: str) -> str:
    try:
        m = re.search(r"\{.*\}", content, re.DOTALL)
        data = json.loads(m.group(0)) if m else {"sku": "", "raw": content}
    except (json.JSONDecodeError, AttributeError):
        data = {"sku": "", "raw": content}
    return f"{data.get('sku', '')} {data.get('raw', '')}"


def ocr_image_github(path: Path, allowed_skus: List[str], model: str) -> str:
    """Use GitHub Models (free with a PAT that has 'models:read' scope)."""
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    if not token:
        raise RuntimeError("GITHUB_TOKEN env var not set. Create a PAT with 'models:read' scope.")
    b64 = _downscale_to_jpeg_b64(path)
    payload = {
        "model": model,
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": _vision_prompt(allowed_skus)},
                    {
                        "type": "image_url",
                        "image_url": {"url": f"data:image/jpeg;base64,{b64}"},
                    },
                ],
            }
        ],
        "temperature": 0,
        "max_tokens": 80,
    }
    resp = requests.post(
        "https://models.github.ai/inference/chat/completions",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        json=payload,
        timeout=60,
    )
    resp.raise_for_status()
    content = resp.json()["choices"][0]["message"]["content"].strip()
    return _parse_vision_response(content)


def ocr_image_openai(path: Path, allowed_skus: List[str], model: str,
                     azure_endpoint: Optional[str] = None,
                     azure_deployment: Optional[str] = None,
                     azure_api_version: str = "2024-08-01-preview") -> str:
    """Ask an OpenAI (or Azure OpenAI) vision model to read the SKU.

    Returns whitespace-separated candidate tokens so extract_candidates / match_sku
    keep working.
    """
    use_azure = bool(azure_endpoint and azure_deployment)
    if use_azure:
        api_key = os.environ.get("AZURE_OPENAI_API_KEY") or os.environ.get("OPENAI_API_KEY")
        if not api_key:
            raise RuntimeError("AZURE_OPENAI_API_KEY (or OPENAI_API_KEY) env var is not set.")
        url = (
            f"{azure_endpoint.rstrip('/')}/openai/deployments/{azure_deployment}"
            f"/chat/completions?api-version={azure_api_version}"
        )
        headers = {"api-key": api_key, "Content-Type": "application/json"}
    else:
        api_key = os.environ.get("OPENAI_API_KEY")
        if not api_key:
            raise RuntimeError("OPENAI_API_KEY env var is not set.")
        url = "https://api.openai.com/v1/chat/completions"
        headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

    b64 = _downscale_to_jpeg_b64(path)
    payload = {
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": _vision_prompt(allowed_skus)},
                    {
                        "type": "image_url",
                        "image_url": {"url": f"data:image/jpeg;base64,{b64}"},
                    },
                ],
            }
        ],
        "temperature": 0,
        "max_tokens": 80,
    }
    if not use_azure:
        payload["model"] = model

    resp = requests.post(url, headers=headers, json=payload, timeout=60)
    resp.raise_for_status()
    content = resp.json()["choices"][0]["message"]["content"].strip()
    return _parse_vision_response(content)


def extract_candidates(text: str) -> List[str]:
    """
    Pull plausible SKU-shaped tokens from OCR text.
    Includes both the raw token and a normalized form.
    """
    # Tokens of length 3-15 that contain at least one letter and one digit
    # OR fully-numeric of length >= 3.
    candidates: List[str] = []
    for raw in re.findall(r"[A-Za-z0-9\-_/]{3,20}", text):
        norm = normalize(raw)
        if not norm:
            continue
        has_alpha = any(c.isalpha() for c in norm)
        has_digit = any(c.isdigit() for c in norm)
        if (has_alpha and has_digit) or (norm.isdigit() and len(norm) >= 3):
            candidates.append(norm)
    # Preserve order, dedupe.
    seen = set()
    out = []
    for c in candidates:
        if c not in seen:
            seen.add(c)
            out.append(c)
    return out


def match_sku(candidates: List[str], sku_index: Dict[str, str]) -> Optional[str]:
    """Return the original SKU (as stored in DB) that matches a candidate."""
    for cand in candidates:
        if cand in sku_index:
            return sku_index[cand]
        # Substring match: OCR may glue extra characters around the SKU.
        for norm_sku, real_sku in sku_index.items():
            if len(norm_sku) >= 4 and norm_sku in cand:
                return real_sku
    return None


# ---------------------------------------------------------------------------
# Image optimisation (mirrors app.save_optimized_image)
# ---------------------------------------------------------------------------
def optimize_image(src: Path, dst: Path, extension: str) -> None:
    image_format = FORMAT_MAP.get(extension, "JPEG")
    try:
        with Image.open(src) as img:
            resample = (
                Image.Resampling.LANCZOS
                if hasattr(Image, "Resampling")
                else Image.LANCZOS
            )
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


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--folder", required=True, help="Local folder with tag images.")
    p.add_argument("--username", required=True, help="PythonAnywhere username.")
    p.add_argument(
        "--remote-db",
        default="/home/SujayNakhare/inventory-management/boutique.db",
        help="Absolute path to boutique.db on PythonAnywhere.",
    )
    p.add_argument(
        "--remote-images-dir",
        default="/home/SujayNakhare/inventory-management/static/product_images",
        help="Absolute path to the product_images dir on PythonAnywhere.",
    )
    p.add_argument("--dry-run", action="store_true", help="Don't upload or modify anything on PA.")
    p.add_argument("--overwrite", action="store_true", help="Replace existing image_filename values.")
    p.add_argument(
        "--ocr",
        choices=["tesseract", "openai", "github"],
        default="tesseract",
        help="OCR backend. 'github' uses GitHub Models (free, needs GITHUB_TOKEN with models:read).",
    )
    p.add_argument(
        "--openai-model",
        default="gpt-4o-mini",
        help="Vision model to use. For --ocr=github try 'openai/gpt-4o' or 'openai/gpt-4o-mini'.",
    )
    p.add_argument("--azure-endpoint", default=None,
                   help="Azure OpenAI endpoint, e.g. https://myres.openai.azure.com")
    p.add_argument("--azure-deployment", default=None,
                   help="Azure OpenAI deployment name (vision-capable, e.g. gpt-4o).")
    p.add_argument("--azure-api-version", default="2024-08-01-preview")
    p.add_argument(
        "--rename-files",
        action="store_true",
        help="After OCR, rename each matched local source image to <SKU>.<ext> in-place.",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()

    token = os.environ.get("PA_API_TOKEN")
    if not token:
        print("ERROR: PA_API_TOKEN env var is not set.", file=sys.stderr)
        return 2

    folder = Path(args.folder).expanduser()
    if not folder.is_dir():
        print(f"ERROR: folder not found: {folder}", file=sys.stderr)
        return 2

    images = sorted(
        p for p in folder.iterdir()
        if p.is_file() and p.suffix.lower().lstrip(".") in ALLOWED_IMAGE_EXTENSIONS
    )
    if not images:
        print(f"No supported images found in {folder}.")
        return 0

    print(f"Found {len(images)} image(s) in {folder}.")
    pa = PAClient(args.username, token)

    workdir = Path(tempfile.mkdtemp(prefix="pa_bulk_images_"))
    local_db = workdir / "boutique.db"
    print(f"Downloading remote DB -> {local_db} ...")
    pa.download(args.remote_db, local_db)

    conn = sqlite3.connect(local_db)
    conn.row_factory = sqlite3.Row
    cur = conn.execute("SELECT id, name, sku, image_filename FROM products WHERE sku IS NOT NULL AND sku != ''")
    rows = cur.fetchall()

    # normalized SKU -> real SKU
    sku_index: Dict[str, str] = {}
    products_by_sku: Dict[str, sqlite3.Row] = {}
    for row in rows:
        norm = normalize(row["sku"])
        if norm:
            sku_index[norm] = row["sku"]
            products_by_sku[row["sku"]] = row
    print(f"Loaded {len(products_by_sku)} products with SKUs from remote DB.")

    updated_skus: List[str] = []
    skipped_existing: List[Tuple[str, str]] = []  # (image_name, sku)
    unmatched_images: List[Tuple[str, List[str]]] = []  # (image_name, candidates)

    allowed_skus = sorted(products_by_sku.keys())
    for img_path in images:
        ext = img_path.suffix.lower().lstrip(".")
        try:
            if args.ocr == "openai":
                text = ocr_image_openai(
                    img_path, allowed_skus, args.openai_model,
                    azure_endpoint=args.azure_endpoint,
                    azure_deployment=args.azure_deployment,
                    azure_api_version=args.azure_api_version,
                )
            elif args.ocr == "github":
                text = ocr_image_github(img_path, allowed_skus, args.openai_model)
            else:
                text = ocr_image_tesseract(img_path)
        except Exception as exc:
            print(f"  ! OCR failed for {img_path.name}: {exc}")
            unmatched_images.append((img_path.name, []))
            continue

        candidates = extract_candidates(text)
        sku = match_sku(candidates, sku_index)
        if not sku:
            print(f"  ? {img_path.name}: no SKU match. candidates={candidates[:8]}")
            unmatched_images.append((img_path.name, candidates))
            continue

        product = products_by_sku[sku]
        if product["image_filename"] and not args.overwrite:
            print(f"  - {img_path.name} -> {sku}: skipped (already has image '{product['image_filename']}')")
            skipped_existing.append((img_path.name, sku))
            continue

        safe_name = secure_filename(product["name"] or sku) or "product"
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
        new_filename = f"{safe_name}_{timestamp}.{ext}"
        local_optimized = workdir / new_filename
        optimize_image(img_path, local_optimized, ext)

        remote_image_path = f"{args.remote_images_dir.rstrip('/')}/{new_filename}"
        if args.dry_run:
            print(f"  [dry-run] would upload {local_optimized.name} -> {remote_image_path}")
            print(f"  [dry-run] would set products.image_filename='{new_filename}' for SKU {sku}")
        else:
            print(f"  + {img_path.name} -> {sku} : uploading {new_filename}")
            pa.upload(remote_image_path, local_optimized)
            conn.execute(
                "UPDATE products SET image_filename = ?, updated_at = datetime('now','localtime') WHERE id = ?",
                (new_filename, product["id"]),
            )

        updated_skus.append(sku)

        if args.rename_files:
            safe_sku = secure_filename(sku) or sku.replace("/", "_")
            target = img_path.with_name(f"{safe_sku}.{ext}")
            if target.exists() and target.resolve() != img_path.resolve():
                # Avoid clobbering: append a counter.
                i = 2
                while True:
                    alt = img_path.with_name(f"{safe_sku}_{i}.{ext}")
                    if not alt.exists():
                        target = alt
                        break
                    i += 1
            if target.resolve() != img_path.resolve():
                # Renaming is local-only; do it even in --dry-run so the user
                # can verify SKU detection without touching PythonAnywhere.
                img_path.rename(target)
                print(f"  ~ renamed {img_path.name} -> {target.name}")

    conn.commit()
    conn.close()

    if not args.dry_run and updated_skus:
        print(f"Uploading modified DB back to {args.remote_db} ...")
        pa.upload(args.remote_db, local_db)

    # ---- Report ----
    print("\n=== Summary ===")
    print(f"Images processed:        {len(images)}")
    print(f"Products updated:        {len(updated_skus)}")
    print(f"Skipped (existing img):  {len(skipped_existing)}")
    print(f"Unmatched images:        {len(unmatched_images)}")

    if unmatched_images:
        print("\nUnmatched images (no SKU detected):")
        for name, cands in unmatched_images:
            print(f"  - {name}   candidates={cands[:6]}")

    not_updated = sorted(set(products_by_sku.keys()) - set(updated_skus))
    print(f"\nInventory SKUs NOT updated in this run ({len(not_updated)}):")
    for sku in not_updated:
        cur_img = products_by_sku[sku]["image_filename"] or ""
        marker = " (already had image)" if cur_img else ""
        print(f"  - {sku}{marker}")

    print(f"\nWorkdir kept at: {workdir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
