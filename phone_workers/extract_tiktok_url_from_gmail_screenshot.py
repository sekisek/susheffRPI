import re
import sys
from pathlib import Path

from PIL import Image, ImageOps, ImageEnhance
import pytesseract


URL_REGEX = re.compile(
    r"https?://(?:www\.)?tiktok\.com/[A-Za-z0-9/_\-\.\?=&]+",
    re.IGNORECASE
)


def preprocess(img: Image.Image) -> Image.Image:
    # grayscale
    img = ImageOps.grayscale(img)

    # crop middle-lower content area where Gmail body usually appears
    w, h = img.size
    crop = img.crop((0, int(h * 0.35), w, int(h * 0.78)))

    # upscale for OCR
    crop = crop.resize((crop.width * 2, crop.height * 2))

    # increase contrast
    crop = ImageEnhance.Contrast(crop).enhance(2.0)
    crop = ImageEnhance.Sharpness(crop).enhance(2.0)

    # simple threshold
    crop = crop.point(lambda p: 255 if p > 150 else 0)

    return crop


def normalize_ocr_text(text: str) -> str:
    text = text.replace("\n", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def main():
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python extract_tiktok_url_from_gmail_screenshot.py <image_path>")
        sys.exit(1)

    image_path = Path(sys.argv[1])
    if not image_path.exists():
        print(f"File not found: {image_path}")
        sys.exit(1)

    img = Image.open(image_path)
    processed = preprocess(img)

    text = pytesseract.image_to_string(processed, config="--psm 6")
    normalized = normalize_ocr_text(text)

    print("=== OCR TEXT ===")
    print(normalized)
    print()

    match = URL_REGEX.search(normalized)
    if match:
        print("=== FOUND URL ===")
        print(match.group(0))
        sys.exit(0)

    # fallback: OCR sometimes inserts spaces inside URL
    squashed = normalized.replace(" ", "")
    match = URL_REGEX.search(squashed)
    if match:
        print("=== FOUND URL ===")
        print(match.group(0))
        sys.exit(0)

    print("No TikTok URL found.")
    sys.exit(2)


if __name__ == "__main__":
    main()
