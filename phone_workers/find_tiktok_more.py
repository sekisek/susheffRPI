#!/usr/bin/env python3
import argparse
import csv
import io
import os
import re
import subprocess
import sys
import tempfile
import unicodedata
import xml.etree.ElementTree as ET
from pathlib import Path

from PIL import Image, ImageEnhance, ImageFilter, ImageOps

SCALE = 3
MIN_X_FRAC = 0.12  # reject bogus far-left matches like x=75 on 1080px screen
TOP_K_DEFAULT = max(1, int(os.getenv("TIKTOK_MORE_TOP_K", "3") or "3"))
OCR_LANGS = os.getenv(
    "TIKTOK_MORE_OCR_LANGS",
    "eng+spa+deu+fra+ita+por+heb+ara+rus+jpn+kor+chi_sim+chi_tra",
)

FAST_OCR_LANGS = os.getenv(
    "TIKTOK_MORE_FAST_OCR_LANGS",
    "eng+spa+deu+fra+ita+por+heb+ara+rus",
)
FAST_VARIANTS = ("gray", "bw_white_on_black")
FAST_OCR_PSMS = ("6",)

MORE_PHRASES = [
    # English
    "more",
    "see more",
    "show more",
    "read more",
    # Spanish
    "más",
    "ver más",
    "mostrar más",
    "leer más",
    # German
    "mehr",
    "mehr anzeigen",
    "mehr lesen",
    "weiterlesen",
    # French
    "plus",
    "voir plus",
    "afficher plus",
    "lire plus",
    # Italian
    "di più",
    "mostra di più",
    "leggi di più",
    "mostra altro",
    "altro",
    # Portuguese
    "mais",
    "ver mais",
    "mostrar mais",
    "ler mais",
    # Hebrew
    "עוד",
    "הצג עוד",
    "קרא עוד",
    "קראו עוד",
    # Arabic
    "المزيد",
    "عرض المزيد",
    "اقرأ المزيد",
    # Russian
    "ещё",
    "еще",
    "подробнее",
    "больше",
    # Japanese
    "もっと",
    "続きを見る",
    "さらに表示",
    # Korean
    "더보기",
    "자세히 보기",
    # Chinese Simplified / Traditional
    "更多",
    "查看更多",
    "显示更多",
    "顯示更多",
    "展开",
    "展開",
]

LATIN_OCR_FIXES = {
    "rnore": "more",
    "rn0re": "more",
    "m0re": "more",
    "seernore": "seemore",
    "sh0w": "show",
    "rea0": "read",
}

PRIMARY_OCR_VARIANTS = ("gray", "bw_white_on_black", "bw_black_on_white")
PRIMARY_OCR_PSMS = ("11", "6")


def strip_diacritics(text: str) -> str:
    decomposed = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in decomposed if not unicodedata.combining(ch))


def normalize_for_match(text: str) -> str:
    t = unicodedata.normalize("NFKC", str(text or "").strip().lower())
    if not t:
        return ""

    # Common OCR confusions that mostly matter for Latin-script variants.
    t = t.replace("0", "o").replace("|", "l")
    for bad, good in LATIN_OCR_FIXES.items():
        t = t.replace(bad, good)

    t = strip_diacritics(t).replace("ё", "е")

    chars = []
    for ch in t:
        category = unicodedata.category(ch)
        if ch.isspace() or category.startswith("L") or category.startswith("N"):
            chars.append(ch)
        else:
            chars.append(" ")

    normalized = re.sub(r"\s+", " ", "".join(chars)).strip()
    return normalized


NORMALIZED_MORE_PHRASES = {normalize_for_match(phrase) for phrase in MORE_PHRASES if phrase}
NORMALIZED_MORE_PHRASES.discard("")
NORMALIZED_MORE_COLLAPSED = {phrase.replace(" ", "") for phrase in NORMALIZED_MORE_PHRASES}
NORMALIZED_SINGLE_TOKEN_MORE = {phrase for phrase in NORMALIZED_MORE_PHRASES if " " not in phrase}


def _split_norm_tokens(text: str) -> list[str]:
    norm = normalize_for_match(text)
    return [token for token in norm.split() if token]


def _looks_like_more_suffix(tokens: list[str]) -> bool:
    if not tokens:
        return False
    last = tokens[-1]
    if last in NORMALIZED_SINGLE_TOKEN_MORE:
        return True
    collapsed_last = last.replace(" ", "")
    if collapsed_last in NORMALIZED_MORE_COLLAPSED:
        return True
    return False


def token_looks_like_more(text: str) -> bool:
    norm = normalize_for_match(text)
    if not norm:
        return False

    collapsed = norm.replace(" ", "")
    if norm in NORMALIZED_SINGLE_TOKEN_MORE:
        return True
    if collapsed in NORMALIZED_MORE_COLLAPSED:
        return True

    tokens = _split_norm_tokens(text)
    if _looks_like_more_suffix(tokens):
        return True

    # Useful for OCR that glues words together like "seemore" / "showmore" or
    # attaches the more marker to a previous token like "concentrate...more".
    latin_more_markers = {
        "more",
        "seemore",
        "showmore",
        "readmore",
        "vermas",
        "mostrarmas",
        "leermas",
        "vermais",
        "mostrarmais",
        "lermais",
        "mehranzeigen",
        "mehrlesen",
        "weiterlesen",
        "voirplus",
        "afficherplus",
        "lireplus",
    }
    if collapsed in latin_more_markers:
        return True

    for marker in NORMALIZED_SINGLE_TOKEN_MORE:
        if collapsed.endswith(marker) and len(collapsed) > len(marker):
            return True

    return False


def phrase_looks_like_more(text: str) -> bool:
    norm = normalize_for_match(text)
    if not norm:
        return False

    collapsed = norm.replace(" ", "")
    if norm in NORMALIZED_MORE_PHRASES or collapsed in NORMALIZED_MORE_COLLAPSED:
        return True

    tokens = _split_norm_tokens(text)
    if _looks_like_more_suffix(tokens):
        return True

    return False


def phrase_specificity(text: str) -> int:
    norm = normalize_for_match(text)
    if not norm:
        return 0
    tokens = norm.split()
    if len(tokens) >= 3:
        return 3
    if len(tokens) == 2:
        return 2
    return 1


def run_tesseract_tsv(img_path: str, psm: str, langs: str | None = None) -> str:
    return subprocess.check_output(
        [
            "tesseract",
            img_path,
            "stdout",
            "-l",
            langs or OCR_LANGS,
            "--psm",
            psm,
            "tsv",
        ],
        text=True,
        stderr=subprocess.DEVNULL,
    )


def make_variants(crop: Image.Image):
    gray = ImageOps.grayscale(crop)
    gray = ImageOps.autocontrast(gray)
    gray = ImageEnhance.Contrast(gray).enhance(3.0)
    gray = gray.filter(ImageFilter.SHARPEN)
    gray = gray.resize((gray.width * SCALE, gray.height * SCALE))

    v1 = gray
    v2 = gray.point(lambda p: 255 if p > 155 else 0)
    v3 = ImageOps.invert(v2)

    return [
        ("gray", v1),
        ("bw_white_on_black", v2),
        ("bw_black_on_white", v3),
    ]


def load_tsv_rows(tsv_text: str):
    rows = []
    reader = csv.DictReader(io.StringIO(tsv_text), delimiter="\t")

    for row in reader:
        raw_text = (row.get("text") or "").strip()
        if not raw_text:
            continue

        try:
            left = int(float(row.get("left") or 0))
            top = int(float(row.get("top") or 0))
            width = int(float(row.get("width") or 0))
            height = int(float(row.get("height") or 0))
            conf = float(row.get("conf") or -1)
        except Exception:
            continue

        if width <= 0 or height <= 0:
            continue

        rows.append(
            {
                "raw_text": raw_text,
                "norm_text": normalize_for_match(raw_text),
                "conf": conf,
                "left": left,
                "top": top,
                "width": width,
                "height": height,
                "right": left + width,
                "bottom": top + height,
                "page_num": row.get("page_num") or "0",
                "block_num": row.get("block_num") or "0",
                "par_num": row.get("par_num") or "0",
                "line_num": row.get("line_num") or "0",
                "word_num": row.get("word_num") or "0",
            }
        )

    return rows


def line_key(row):
    return (
        row["page_num"],
        row["block_num"],
        row["par_num"],
        row["line_num"],
    )


def pick_tap_token_bounds(tokens):
    if not tokens:
        return None

    # For multi-token phrases like "see more", tap on the last token ("more").
    if len(tokens) >= 2 and token_looks_like_more(tokens[-1]["raw_text"]):
        last = tokens[-1]
        return last["left"], last["top"], last["right"], last["bottom"]

    # For merged OCR tokens like "concentrate...more", bias the tap to the right side.
    if len(tokens) == 1:
        token = tokens[0]
        norm_tokens = _split_norm_tokens(token["raw_text"])
        if len(norm_tokens) >= 2 and _looks_like_more_suffix(norm_tokens):
            left = token["left"] + int(max(1, token["width"]) * 0.72)
            return left, token["top"], token["right"], token["bottom"]

    return (
        min(token["left"] for token in tokens),
        min(token["top"] for token in tokens),
        max(token["right"] for token in tokens),
        max(token["bottom"] for token in tokens),
    )


def build_hit(tokens, left, top, region_name, variant_name, psm, min_x):
    if not tokens:
        return None

    tap_bounds = pick_tap_token_bounds(tokens)
    if tap_bounds:
        tap_left, tap_top, tap_right, tap_bottom = tap_bounds
    else:
        tap_left = min(token["left"] for token in tokens)
        tap_top = min(token["top"] for token in tokens)
        tap_right = max(token["right"] for token in tokens)
        tap_bottom = max(token["bottom"] for token in tokens)

    x_center = tap_left + (tap_right - tap_left) // 2
    y_center = tap_top + (tap_bottom - tap_top) // 2

    orig_x = left + x_center // SCALE
    orig_y = top + y_center // SCALE
    confidences = [token["conf"] for token in tokens if token["conf"] >= 0]
    conf = sum(confidences) / len(confidences) if confidences else -1
    raw_text = " ".join(token["raw_text"] for token in tokens).strip()
    norm_text = normalize_for_match(raw_text)

    return {
        "conf": conf,
        "x": orig_x,
        "y": orig_y,
        "text": raw_text,
        "norm": norm_text,
        "region": region_name,
        "variant": variant_name,
        "psm": psm,
        "accepted": orig_x >= min_x,
    }


def extract_hits_from_rows(rows, left, top, region_name, variant_name, psm, min_x):
    hits = []

    # Single-token matches.
    for row in rows:
        if token_looks_like_more(row["raw_text"]):
            hit = build_hit([row], left, top, region_name, variant_name, psm, min_x)
            if hit:
                hits.append(hit)

    # Multi-token phrase matches.
    grouped = {}
    for row in rows:
        grouped.setdefault(line_key(row), []).append(row)

    for line_rows in grouped.values():
        sequences = []

        # OCR order.
        sequences.append(line_rows)
        # Visual left-to-right order.
        sequences.append(sorted(line_rows, key=lambda item: (item["left"], item["top"])))
        # Visual right-to-left order.
        sequences.append(sorted(line_rows, key=lambda item: (item["left"], item["top"]), reverse=True))

        seen_sequences = set()
        unique_sequences = []
        for sequence in sequences:
            key = tuple(
                (item["left"], item["top"], item["width"], item["height"], item["raw_text"])
                for item in sequence
            )
            if key in seen_sequences:
                continue
            seen_sequences.add(key)
            unique_sequences.append(sequence)

        for sequence in unique_sequences:
            for n in (2, 3):
                if len(sequence) < n:
                    continue
                for start in range(0, len(sequence) - n + 1):
                    gram = sequence[start : start + n]
                    phrase = " ".join(token["raw_text"] for token in gram)
                    if phrase_looks_like_more(phrase):
                        hit = build_hit(gram, left, top, region_name, variant_name, psm, min_x)
                        if hit:
                            hits.append(hit)

    # Deduplicate similar hits.
    deduped = {}
    for hit in hits:
        key = (
            hit["norm"],
            hit["x"],
            hit["y"],
            hit["region"],
            hit["variant"],
            hit["psm"],
        )
        existing = deduped.get(key)
        if existing is None or hit["conf"] > existing["conf"]:
            deduped[key] = hit

    return list(deduped.values())


def parse_android_bounds(bounds: str):
    match = re.match(r"\[(\d+),(\d+)\]\[(\d+),(\d+)\]", str(bounds or "").strip())
    if not match:
        return None
    left, top, right, bottom = [int(value) for value in match.groups()]
    if right <= left or bottom <= top:
        return None
    return {
        "left": left,
        "top": top,
        "right": right,
        "bottom": bottom,
        "x": left + (right - left) // 2,
        "y": top + (bottom - top) // 2,
    }


def extract_hits_from_xml(xml_path: Path, min_x: int):
    hits = []
    if not xml_path or not xml_path.exists():
        return hits

    try:
        root = ET.parse(str(xml_path)).getroot()
    except Exception:
        return hits

    for node in root.iter("node"):
        text_value = str(node.attrib.get("text") or "").strip()
        desc_value = str(node.attrib.get("content-desc") or "").strip()
        merged = " ".join(part for part in [text_value, desc_value] if part).strip()
        if not merged:
            continue
        if not phrase_looks_like_more(merged):
            continue

        bounds = parse_android_bounds(node.attrib.get("bounds") or "")
        if not bounds:
            continue

        norm = normalize_for_match(merged)
        hit = {
            "conf": 999.0,
            "x": bounds["x"],
            "y": bounds["y"],
            "text": merged,
            "norm": norm,
            "region": "uiautomator",
            "variant": "xml",
            "psm": "xml",
            "accepted": bounds["x"] >= min_x,
            "debug_image": str(xml_path),
            "ocr_langs": "uiautomator",
        }
        hits.append(hit)

    deduped = {}
    for hit in hits:
        key = (hit["norm"], hit["x"], hit["y"], hit["region"], hit["variant"], hit["psm"])
        existing = deduped.get(key)
        if existing is None or hit["conf"] > existing["conf"]:
            deduped[key] = hit

    return list(deduped.values())


def region_rank(region_name: str) -> int:
    if region_name == "uiautomator":
        return 100
    if region_name == "r3":
        return 3
    if region_name == "r2":
        return 2
    if region_name == "r1":
        return 1
    return 0


def hit_score(hit, image_width: int):
    x_ratio = (float(hit.get("x") or 0) / max(1, image_width))
    conf = max(0.0, float(hit.get("conf") or 0.0))
    specificity = phrase_specificity(hit.get("text") or "")
    xml_bonus = 100000 if hit.get("region") == "uiautomator" else 0
    accepted_bonus = 10000 if hit.get("accepted") else -10000
    region_bonus = region_rank(str(hit.get("region") or "")) * 100
    specificity_bonus = specificity * 50
    x_bonus = x_ratio * 100
    return xml_bonus + accepted_bonus + region_bonus + specificity_bonus + conf + x_bonus


def dedupe_ranked_hits(hits):
    deduped = []
    seen = set()
    for hit in hits:
        key = (int(hit["x"] // 6), int(hit["y"] // 6), hit["norm"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(hit)
    return deduped


def rank_hits(valid_hits, image_width: int):
    ranked = []
    for hit in valid_hits:
        cloned = dict(hit)
        cloned["score"] = hit_score(hit, image_width)
        ranked.append(cloned)
    ranked.sort(key=lambda item: (item["score"], item["x"], item["conf"]), reverse=True)
    return dedupe_ranked_hits(ranked)


def write_debug(debug_dir: Path, all_hits, ranked_hits, min_x: int):
    all_lines = []
    for idx, hit in enumerate(sorted(all_hits, key=lambda item: (-(1 if item["accepted"] else 0), -float(item.get("x") or 0), -float(item.get("conf") or 0)))):
        score = hit.get("score")
        all_lines.append(
            f'{idx + 1}. text={hit["text"]!r} norm={hit["norm"]!r} conf={hit["conf"]} '
            f'x={hit["x"]} y={hit["y"]} region={hit["region"]} variant={hit["variant"]} '
            f'psm={hit["psm"]} accepted={hit["accepted"]} '
            f'score={score if score is not None else "n/a"} '
            f'ocr_langs={hit.get("ocr_langs", "")} debug_image={hit.get("debug_image", "")}'
        )
    (debug_dir / "_debug_more_all_hits.txt").write_text("\n".join(all_lines), encoding="utf-8")

    ranked_lines = []
    for idx, hit in enumerate(ranked_hits):
        ranked_lines.append(
            f'{idx + 1}. text={hit["text"]!r} norm={hit["norm"]!r} conf={hit["conf"]} '
            f'x={hit["x"]} y={hit["y"]} region={hit["region"]} variant={hit["variant"]} '
            f'psm={hit["psm"]} accepted={hit["accepted"]} '
            f'score={hit["score"]} '
            f'ocr_langs={hit.get("ocr_langs", "")} debug_image={hit.get("debug_image", "")}'
        )
    (debug_dir / "_debug_more_ranked.txt").write_text("\n".join(ranked_lines), encoding="utf-8")

    if ranked_hits:
        best = ranked_hits[0]
        (debug_dir / "_debug_more_best.txt").write_text(
            f'text={best["text"]}\n'
            f'norm={best["norm"]}\n'
            f'conf={best["conf"]}\n'
            f'x={best["x"]}\n'
            f'y={best["y"]}\n'
            f'region={best["region"]}\n'
            f'variant={best["variant"]}\n'
            f'psm={best["psm"]}\n'
            f'debug_image={best.get("debug_image", "")}\n'
            f'ocr_langs={best.get("ocr_langs", "")}\n'
            f'min_x={min_x}\n'
            f'score={best["score"]}\n',
            encoding="utf-8",
        )


def collect_hits(img_path: Path, xml_path: Path | None):
    img = Image.open(img_path).convert("RGB")
    w, h = img.size
    min_x = int(w * MIN_X_FRAC)
    debug_dir = img_path.parent

    fast_regions = [
        ("f1", (0, int(h * 0.80), int(w * 0.88), int(h * 0.95))),
        ("f2", (0, int(h * 0.84), int(w * 0.90), int(h * 0.97))),
    ]
    regions = [
        ("r1", (0, int(h * 0.60), int(w * 0.82), int(h * 0.90))),
        ("r2", (0, int(h * 0.66), int(w * 0.82), int(h * 0.94))),
        ("r3", (0, int(h * 0.72), int(w * 0.82), int(h * 0.96))),
    ]

    all_hits = []
    valid_hits = []

    xml_hits = extract_hits_from_xml(xml_path, min_x) if xml_path and xml_path.exists() else []
    for hit in xml_hits:
        all_hits.append(hit)
        if hit["accepted"]:
            valid_hits.append(hit)

    # Strong preference for UIAutomator hits: if we have any accepted XML hit, skip OCR fallback entirely.
    xml_ranked = rank_hits(valid_hits, w) if valid_hits else []
    if xml_ranked:
        write_debug(debug_dir, all_hits, xml_ranked, min_x)
        return all_hits, xml_ranked, min_x

    # Fast bottom-caption OCR pass: much cheaper and it catches visible "...more"
    # cases even when UIAutomator dump fails with idle-state errors.
    for region_name, (left, top, right, bottom) in fast_regions:
        crop = img.crop((left, top, right, bottom))
        crop.save(debug_dir / f"_debug_more_crop_{region_name}.png")

        for variant_name, variant in make_variants(crop):
            if variant_name not in FAST_VARIANTS:
                continue
            debug_variant_path = debug_dir / f"_debug_more_{region_name}_{variant_name}.png"
            variant.save(debug_variant_path)

            tmp = tempfile.NamedTemporaryFile(suffix=".png", delete=False)
            tmp_path = tmp.name
            tmp.close()
            variant.save(tmp_path)

            try:
                for psm in FAST_OCR_PSMS:
                    out = run_tesseract_tsv(tmp_path, psm, langs=FAST_OCR_LANGS)
                    rows = load_tsv_rows(out)
                    hits = extract_hits_from_rows(rows, left, top, region_name, variant_name, psm, min_x)

                    for hit in hits:
                        hit["debug_image"] = str(debug_variant_path)
                        hit["ocr_langs"] = FAST_OCR_LANGS
                        all_hits.append(hit)
                        if hit["accepted"]:
                            valid_hits.append(hit)
            finally:
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)

    fast_ranked = rank_hits(valid_hits, w) if valid_hits else []
    if fast_ranked:
        write_debug(debug_dir, all_hits, fast_ranked, min_x)
        return all_hits, fast_ranked, min_x

    # Full OCR fallback if the fast bottom pass still found nothing.
    for region_name, (left, top, right, bottom) in regions:
        crop = img.crop((left, top, right, bottom))
        crop.save(debug_dir / f"_debug_more_crop_{region_name}.png")

        for variant_name, variant in make_variants(crop):
            if variant_name not in PRIMARY_OCR_VARIANTS:
                continue
            debug_variant_path = debug_dir / f"_debug_more_{region_name}_{variant_name}.png"
            variant.save(debug_variant_path)

            tmp = tempfile.NamedTemporaryFile(suffix=".png", delete=False)
            tmp_path = tmp.name
            tmp.close()
            variant.save(tmp_path)

            try:
                for psm in PRIMARY_OCR_PSMS:
                    out = run_tesseract_tsv(tmp_path, psm, langs=OCR_LANGS)
                    rows = load_tsv_rows(out)
                    hits = extract_hits_from_rows(rows, left, top, region_name, variant_name, psm, min_x)

                    for hit in hits:
                        hit["debug_image"] = str(debug_variant_path)
                        hit["ocr_langs"] = OCR_LANGS
                        all_hits.append(hit)
                        if hit["accepted"]:
                            valid_hits.append(hit)
            finally:
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)

    ranked_hits = rank_hits(valid_hits, w) if valid_hits else []
    write_debug(debug_dir, all_hits, ranked_hits, min_x)
    return all_hits, ranked_hits, min_x


def emit_line(text: str):
    try:
        print(text)
    except BrokenPipeError:
        raise SystemExit(0)


def parse_args(argv):
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--all", action="store_true", dest="all_hits")
    parser.add_argument("--exists", action="store_true", dest="exists_only")
    parser.add_argument("--top-k", type=int, default=TOP_K_DEFAULT)
    parser.add_argument("image")
    parser.add_argument("xml", nargs="?")
    return parser.parse_args(argv)


def main():
    args = parse_args(sys.argv[1:])
    img_path = Path(args.image)
    xml_path = Path(args.xml) if args.xml else None

    if not img_path.exists():
        emit_line("NOT_FOUND")
        sys.exit(2)

    _all_hits, ranked_hits, _min_x = collect_hits(img_path, xml_path)

    if args.exists_only:
        emit_line("FOUND" if ranked_hits else "NOT_FOUND")
        sys.exit(0 if ranked_hits else 2)

    if not ranked_hits:
        emit_line("NOT_FOUND")
        sys.exit(2)

    if args.all_hits:
        top_k = max(1, int(args.top_k or TOP_K_DEFAULT))
        for hit in ranked_hits[:top_k]:
            emit_line(f'{hit["x"]} {hit["y"]}')
        return

    best = ranked_hits[0]
    emit_line(f'{best["x"]} {best["y"]}')


if __name__ == "__main__":
    main()
