import asyncio
import hashlib
import html as html_lib
import json
import os
import re
import subprocess
import time
from datetime import datetime, timezone
import unicodedata
import urllib.error
import urllib.request
from pathlib import Path
from urllib.parse import parse_qs, quote, quote_plus, urljoin, urlparse, unquote

from dotenv import load_dotenv
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright

from bot_api import (
    BOT_SECRET,
    DEVICE_NAME,
    append_job_debug_log,
    touch_job_heartbeat,
    write_investigation_history,
    claim_next_job,
    create_confirmation_job,
    fail_job,
    get_job,
    get_recipe,
    submit_bot_evidence,
    update_job,
    update_recipe,
    update_recipe_debug,
    update_recipe_from_job,
    upload_bot_screenshot,
)

load_dotenv(Path.home() / "social-bot" / ".env", override=True)

BASE = Path.home() / "social-bot"
SCREENSHOT_DIR = BASE / "screenshots"
SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)
PROFILE_ROOT = BASE / "profiles"

HEADLESS = os.getenv("HEADLESS", "true").strip().lower() == "true"
PAGE_HTML_MAX_LEN = 20000

PI_ANALYZER_BUILD = os.getenv("PI_ANALYZER_BUILD", "pi-runtime-pilot-2026-03-25a").strip() or "pi-runtime-pilot-2026-03-25a"
SUPABASE_FUNCTIONS_URL = os.getenv("SUPABASE_URL", "").strip()
if SUPABASE_FUNCTIONS_URL:
    SUPABASE_FUNCTIONS_URL = f"{SUPABASE_FUNCTIONS_URL}/functions/v1"
ANALYZER_RUNTIME_INTERNAL_SECRET = (
    os.getenv("ANALYZER_RUNTIME_INTERNAL_SECRET", "").strip()
    or os.getenv("SCRAPER_SHARED_SECRET", "").strip()
    or os.getenv("PROCESS_RECIPE_INTERNAL_SECRET", "").strip()
)
ANALYZER_RUNTIME_FETCH_TIMEOUT_SECONDS = float(os.getenv("ANALYZER_RUNTIME_FETCH_TIMEOUT_SECONDS", "8").strip() or "8")
ANALYZER_RUNTIME_CACHE_TTL_SECONDS = int(os.getenv("ANALYZER_RUNTIME_CACHE_TTL_SECONDS", "300").strip() or "300")
ANALYZER_RUNTIME_CACHE = {
    "loaded_at": 0.0,
    "snapshot": None,
    "version": "builtin-empty-v1",
    "source": "builtin",
    "error": None,
    "url": None,
    "url_source": None,
}
BUILTIN_ANALYZER_RUNTIME = {
    "schema_version": 1,
    "published_version": "builtin-empty-v1",
    "scenarios": {
        "instagram.caption": {"enabled": True, "rules": {}, "notes": ""},
        "instagram.external_site": {"enabled": True, "rules": {}, "notes": ""},
        "instagram.low_signal": {"enabled": True, "rules": {}, "notes": ""},
        "tiktok.external_site": {"enabled": True, "rules": {}, "notes": ""},
        "youtube.rendered": {"enabled": True, "rules": {}, "notes": ""},
        "youtube.linked_recipe": {"enabled": True, "rules": {}, "notes": ""},
        "youtube.mixed_language": {"enabled": True, "rules": {}, "notes": ""},
        "web.clue_follow": {"enabled": True, "rules": {}, "notes": ""},
        "web.recipe_page": {"enabled": True, "rules": {}, "notes": ""},
    },
}


class JobLeaseLostError(RuntimeError):
    pass


def get_job_lock_token(job: dict | None) -> str:
    if not isinstance(job, dict):
        return ""
    return str(job.get("lock_token") or "").strip()


def assert_job_claim_is_current(job_id: str, expected_lock_token: str, stage: str):
    expected = str(expected_lock_token or "").strip()
    if not job_id or not expected:
        return

    current_job = get_job(job_id)
    current_token = get_job_lock_token(current_job)
    current_status = str(current_job.get("status") or "").strip().lower()
    current_finished_at = current_job.get("finished_at")

    if current_token != expected:
        raise JobLeaseLostError(
            f"BotJob lease lost at {stage}: expected lock_token={expected or 'empty'} current_lock_token={current_token or 'empty'}"
        )

    if current_status in {"done", "failed"} or current_finished_at:
        raise JobLeaseLostError(
            f"BotJob no longer active at {stage}: status={current_status or 'empty'} finished_at={current_finished_at or 'empty'}"
        )


def _normalize_public_url(value: str) -> str:
    url = str(value or "").strip()
    if not url:
        return ""
    if not re.match(r"^https?://", url, flags=re.IGNORECASE):
        url = f"https://{url.lstrip('/')}"
    return url.rstrip('/')


def _append_function_endpoint(base_url: str, function_name: str) -> str:
    base = _normalize_public_url(base_url)
    if not base:
        return ""
    if re.search(rf"/functions/{re.escape(function_name)}(?:$|[/?#])", base, flags=re.IGNORECASE):
        return base
    if re.search(r"/functions/[^/?#]+/?$", base, flags=re.IGNORECASE):
        return re.sub(r"/functions/[^/?#]+/?$", f"/functions/{function_name}", base, flags=re.IGNORECASE)
    return f"{base}/functions/{function_name}"


def _derive_function_url_from_known_endpoint(value: str, target_name: str) -> str:
    endpoint = _normalize_public_url(value)
    if not endpoint:
        return ""
    replaced = re.sub(r"/functions/[^/?#]+", f"/functions/{target_name}", endpoint, flags=re.IGNORECASE)
    if replaced != endpoint:
        return replaced
    if endpoint.endswith('/functions'):
        return f"{endpoint}/{target_name}"
    return ""


def resolve_analyzer_runtime_function_url() -> tuple[str, str]:
    explicit_env_names = [
        "ANALYZER_RUNTIME_FUNCTION_URL",
        "GET_ANALYZER_RUNTIME_URL",
        "GET_ANALYZER_RUNTIME_FUNCTION_URL",
        "ANALYZER_RUNTIME_URL",
    ]
    for env_name in explicit_env_names:
        value = os.getenv(env_name, "").strip()
        if value:
            return _append_function_endpoint(value, "getAnalyzerRuntime"), f"env:{env_name}"

    app_domain_env_names = [
        "BASE44_APP_DOMAIN",
        "BASE44_APP_URL",
        "BASE44_PUBLIC_APP_URL",
        "PUBLIC_APP_URL",
        "APP_BASE_URL",
        "APP_URL",
        "PREVIEW_APP_URL",
        "PREVIEW_BASE_URL",
    ]
    for env_name in app_domain_env_names:
        value = os.getenv(env_name, "").strip()
        if value:
            return _append_function_endpoint(value, "getAnalyzerRuntime"), f"env:{env_name}"

    related_function_env_names = [
        "PROCESS_RECIPE_URL",
        "PROCESS_RECIPE_FUNCTION_URL",
        "UPDATE_ANALYZER_RUNTIME_URL",
        "UPDATE_ANALYZER_RUNTIME_FUNCTION_URL",
        "ANALYZE_BOT_EVIDENCE_ASYNC_URL",
        "ANALYZE_BOT_EVIDENCE_ASYNC_FUNCTION_URL",
    ]
    for env_name in related_function_env_names:
        value = os.getenv(env_name, "").strip()
        if value:
            derived = _derive_function_url_from_known_endpoint(value, "getAnalyzerRuntime")
            if derived:
                return derived, f"derived:{env_name}"

    # Fallback: derive from SUPABASE_URL
    if SUPABASE_FUNCTIONS_URL:
        return f"{SUPABASE_FUNCTIONS_URL}/getAnalyzerRuntime", "derived:SUPABASE_URL"

    # Legacy fallback: try production domain
    production_url = "https://susheff.com/functions/getAnalyzerRuntime"
    return production_url, "production:fallback"


ANALYZER_RUNTIME_FUNCTION_URL, ANALYZER_RUNTIME_FUNCTION_URL_SOURCE = resolve_analyzer_runtime_function_url()

# Keep Pi payloads conservative so submitBotEvidence/Base44 never chokes on merged text.
RAW_PAGE_TEXT_SUBMIT_MAX = int(os.getenv("RAW_PAGE_TEXT_SUBMIT_MAX", "8000").strip() or "8000")
EXPANDED_CAPTION_SUBMIT_MAX = int(os.getenv("EXPANDED_CAPTION_SUBMIT_MAX", "6000").strip() or "6000")
VISIBLE_TEXT_BEFORE_SUBMIT_MAX = int(os.getenv("VISIBLE_TEXT_BEFORE_SUBMIT_MAX", "4000").strip() or "4000")
VISIBLE_TEXT_AFTER_SUBMIT_MAX = int(os.getenv("VISIBLE_TEXT_AFTER_SUBMIT_MAX", "8000").strip() or "8000")
META_DESCRIPTION_SUBMIT_MAX = int(os.getenv("META_DESCRIPTION_SUBMIT_MAX", "3000").strip() or "3000")
PAGE_TITLE_SUBMIT_MAX = int(os.getenv("PAGE_TITLE_SUBMIT_MAX", "500").strip() or "500")
SUBMIT_TRANSCRIPT_MAX_LEN = int(os.getenv("BOT_SUBMIT_TRANSCRIPT_MAX_LEN", "12000").strip() or "12000")

LINKED_RECIPE_GOTO_TIMEOUT_MS = int(os.getenv("LINKED_RECIPE_GOTO_TIMEOUT_MS", "20000").strip() or "20000")
LINKED_RECIPE_WAIT_MS = int(os.getenv("LINKED_RECIPE_WAIT_MS", "2500").strip() or "2500")
LINKED_RECIPE_SCREENSHOT = os.getenv("LINKED_RECIPE_SCREENSHOT", "true").strip().lower() == "true"
INSTAGRAM_SITE_ROOT_GOTO_TIMEOUT_MS = int(os.getenv("INSTAGRAM_SITE_ROOT_GOTO_TIMEOUT_MS", "12000").strip() or "12000")
INSTAGRAM_SITE_ROOT_WAIT_MS = int(os.getenv("INSTAGRAM_SITE_ROOT_WAIT_MS", "900").strip() or "900")

PHONE_WORKERS_DIR = BASE / "app" / "phone_workers"
PHONE_CAPTURE_ROOT = PHONE_WORKERS_DIR / "captures"
PHONE_WORKER_ENABLED = os.getenv("PHONE_WORKER_ENABLED", "true").strip().lower() == "true"
PHONE_FALLBACK_PLATFORMS = {
    x.strip().lower()
    for x in os.getenv("PHONE_FALLBACK_PLATFORMS", "tiktok").split(",")
    if x.strip()
}
TESSERACT_BIN = os.getenv("TESSERACT_BIN", "tesseract").strip() or "tesseract"

PHONE_WORKER_SCRIPTS = {
    "tiktok": PHONE_WORKERS_DIR / "tiktok_phone_evidence.sh",
}


ANALYZER_BUILD_VERSION = os.getenv("ANALYZER_BUILD_VERSION", "pi-analyzer-hotfix-2026-03-25-1").strip() or "pi-analyzer-hotfix-2026-03-25-1"
INVESTIGATION_PATCH_VERSION = os.getenv("INVESTIGATION_PATCH_VERSION", "investigator-v10-phone-merged-evidence-friendly-2026-04-06").strip() or "investigator-v10-phone-merged-evidence-friendly-2026-04-06"
INVESTIGATION_ENGINE_VERSION = INVESTIGATION_PATCH_VERSION
INVESTIGATION_HISTORY_WRITER_VERSION = os.getenv("INVESTIGATION_HISTORY_WRITER_VERSION", "history-writer-v14-2026-04-05").strip() or "history-writer-v14-2026-04-05"
INVESTIGATION_HISTORY_ENABLED = os.getenv("INVESTIGATION_HISTORY_ENABLED", "true").strip().lower() == "true"
INVESTIGATION_HISTORY_SUPPORTED_MODES = {
    mode.strip().lower()
    for mode in os.getenv("INVESTIGATION_HISTORY_SUPPORTED_MODES", "instagram.external_site,tiktok.external_site").split(",")
    if mode.strip()
}
INVESTIGATION_HISTORY_MAX_BREADCRUMBS = int(os.getenv("INVESTIGATION_HISTORY_MAX_BREADCRUMBS", "40").strip() or "40")
INVESTIGATION_HISTORY_MAX_CANDIDATES = int(os.getenv("INVESTIGATION_HISTORY_MAX_CANDIDATES", "24").strip() or "24")
INVESTIGATION_HISTORY_TEXT_PREVIEW_MAX = int(os.getenv("INVESTIGATION_HISTORY_TEXT_PREVIEW_MAX", "280").strip() or "280")
SUBMIT_DEBUG_DATA_MAX_JSON_LEN = int(os.getenv("SUBMIT_DEBUG_DATA_MAX_JSON_LEN", "12000").strip() or "12000")
SUBMIT_DEBUG_TEXT_MAX_LEN = int(os.getenv("SUBMIT_DEBUG_TEXT_MAX_LEN", "600").strip() or "600")
SUBMIT_DEBUG_LIST_MAX_ITEMS = int(os.getenv("SUBMIT_DEBUG_LIST_MAX_ITEMS", "8").strip() or "8")



def _parse_bool_env(name: str, default: bool) -> bool:
    raw = str(os.getenv(name, "")).strip().lower()
    if not raw:
        return bool(default)
    if raw in {"1", "true", "yes", "y", "on"}:
        return True
    if raw in {"0", "false", "no", "n", "off"}:
        return False
    return bool(default)


def _parse_csv_env_list(*names: str, default: str = "") -> list[str]:
    raw = ""
    for name in names:
        candidate = str(os.getenv(name, "")).strip()
        if candidate:
            raw = candidate
            break
    if not raw:
        raw = default or ""

    seen = set()
    out = []
    for item in str(raw).replace("\n", ",").split(","):
        normalized = str(item or "").strip().lower()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        out.append(normalized)
    return out


COLLECTOR_NODE_ID = os.getenv("COLLECTOR_NODE_ID", "").strip() or DEVICE_NAME
COLLECTOR_PROFILE_ID = (
    os.getenv("COLLECTOR_PROFILE_ID", "").strip()
    or os.getenv("SOCIAL_COLLECTOR_PROFILE_ID", "").strip()
)
COLLECTOR_ACCOUNT_LABEL = (
    os.getenv("COLLECTOR_ACCOUNT_LABEL", "").strip()
    or os.getenv("SOCIAL_COLLECTOR_ACCOUNT_LABEL", "").strip()
    or COLLECTOR_PROFILE_ID
    or DEVICE_NAME
)
COLLECTOR_PLATFORMS = _parse_csv_env_list(
    "COLLECTOR_PLATFORMS",
    "SOCIAL_COLLECTOR_PLATFORMS",
    "PLATFORM_ALLOWLIST",
)
COLLECTOR_CAPABILITIES = _parse_csv_env_list(
    "COLLECTOR_CAPABILITIES",
    "SOCIAL_COLLECTOR_CAPABILITIES",
)
CAN_CLAIM_DEFAULT_JOBS = _parse_bool_env("CAN_CLAIM_DEFAULT_JOBS", True)
CAN_CLAIM_CONFIRMATION_JOBS = _parse_bool_env("CAN_CLAIM_CONFIRMATION_JOBS", False)


def get_current_collector_identity() -> dict:
    return {
        "collector_node_id": COLLECTOR_NODE_ID,
        "collector_profile_id": COLLECTOR_PROFILE_ID,
        "collector_account_label": COLLECTOR_ACCOUNT_LABEL,
        "collector_platforms": list(COLLECTOR_PLATFORMS),
        "collector_capabilities": list(COLLECTOR_CAPABILITIES),
        "can_claim_default_jobs": bool(CAN_CLAIM_DEFAULT_JOBS),
        "can_claim_confirmation_jobs": bool(CAN_CLAIM_CONFIRMATION_JOBS),
    }


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def normalize_text_preserve_lines(text: str) -> str:
    if not text:
        return ""

    lines = []
    for line in str(text).replace("\r", "\n").split("\n"):
        line = re.sub(r"[ \t]+", " ", line).strip()
        if line:
            lines.append(line)
    return "\n".join(lines)


def trim_text(text: str, max_len: int) -> str:
    text = str(text or "")
    return text if len(text) <= max_len else text[:max_len]


def combine_text_blocks(blocks) -> str:
    combined = []
    seen = set()

    for block in blocks or []:
        normalized = normalize_text_preserve_lines(block)
        if not normalized:
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        combined.append(normalized)

    return "\n\n".join(combined)


def choose_first_non_empty(*values) -> str:
    for value in values:
        if isinstance(value, bool):
            continue
        normalized = normalize_text_preserve_lines(value)
        if normalized:
            return normalized
    return ""


def count_non_empty_lines(text: str) -> int:
    if not text:
        return 0
    return len([line for line in normalize_text_preserve_lines(text).split("\n") if line.strip()])


def count_regex_matches(text: str, regex: str) -> int:
    source = str(text or "")
    if not source:
        return 0

    count = 0
    for _ in re.finditer(regex, source, flags=re.IGNORECASE):
        count += 1
        if count > 200:
            break
    return count


def count_likely_measurement_signals(text: str) -> int:
    source = str(text or "")
    if not source:
        return 0

    patterns = [
        r'\b\d+(?:[.,]\d+)?\s*(?:g|gr|gram|grams|kg|ml|l|tbsp|tsp|cup|cups|oz|lb)\b',
        r'\d+(?:[.,]\d+)?\s*(?:כף|כפות|כפית|כפיות|גרם|ק["״]?ג|קילו|מל|מ["״]?ל|ליטר|ליטרים)',
    ]

    total = 0
    for pattern in patterns:
        total += count_regex_matches(source, pattern)
    return total


def count_recipe_verb_signals(text: str) -> int:
    lower = str(text or "").lower()
    if not lower:
        return 0

    keywords = [
        "mix", "add", "stir", "cook", "boil", "bake", "fry", "roast", "soak", "blend",
        "chop", "serve", "heat", "simmer",
        "משרים", "מבשלים", "מערבבים", "מוסיפים", "מטגנים", "טוחנים", "קוצצים", "מגישים", "אופים",
    ]

    hits = 0
    for keyword in keywords:
        if keyword in lower:
            hits += 1
    return hits


def has_food_context(text: str) -> bool:
    lower = str(text or "").lower()
    if not lower:
        return False

    food_keywords = [
        "recipe", "ingredient", "ingredients", "cook", "bake", "prepare", "dish", "food",
        "meal", "salt", "flour", "butter", "oil", "sugar", "egg", "water", "heat", "mix",
        "add", "stir", "מתכון", "רכיבים", "מצרכים", "מלח", "שמן", "לימון", "שום", "בצל",
        "חומוס", "טחינה",
    ]
    return any(keyword in lower for keyword in food_keywords)


def evaluate_evidence_text(text: str) -> dict:
    normalized = normalize_text_preserve_lines(text) or ""
    line_count = count_non_empty_lines(normalized)
    measurement_signal_count = count_likely_measurement_signals(normalized[:15000])
    recipe_verb_signal_count = count_recipe_verb_signals(normalized[:15000])

    has_strong_text_evidence = (
        len(normalized) >= 220 or line_count >= 6 or measurement_signal_count >= 3
    )
    looks_recipe_dense = (
        measurement_signal_count >= 3
        or (line_count >= 6 and recipe_verb_signal_count >= 2)
        or (measurement_signal_count >= 2 and recipe_verb_signal_count >= 1)
    )

    return {
        "length": len(normalized),
        "lineCount": line_count,
        "measurementSignalCount": measurement_signal_count,
        "recipeVerbSignalCount": recipe_verb_signal_count,
        "hasStrongTextEvidence": has_strong_text_evidence,
        "looksRecipeDense": looks_recipe_dense,
        "hasFoodContext": has_food_context(normalized),
    }


def deep_clone_json(value):
    return json.loads(json.dumps(value))



def _as_plain_dict(value):
    if not value:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            return {}
    return {}


def _parse_string_list(value) -> list[str]:
    if isinstance(value, list):
        items = value
    elif isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return []
        try:
            parsed = json.loads(stripped)
            items = parsed if isinstance(parsed, list) else stripped.split(",")
        except Exception:
            items = stripped.split(",")
    else:
        return []

    seen = set()
    out = []
    for item in items:
        normalized = str(item or "").strip().lower()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        out.append(normalized)
    return out


def sanitize_collector_profile_segment(value: str) -> str:
    raw = unicodedata.normalize("NFKC", str(value or "").strip().lower())
    if not raw:
        return ""
    collapsed = re.sub(r"[^a-z0-9._-]+", "-", raw).strip("-._")
    return collapsed[:80]


SOCIAL_CONFIRMATION_NOISE_MARKERS = [
    "podcast",
    "episode",
    "full episode",
    "available on all platforms",
    "spotify",
    "apple podcast",
    "youtube",
    "comment ",
    "dm ",
    "views and counting",
    "subscribe",
    "out now",
]


def detect_script_groups(text: str) -> list[str]:
    source = str(text or "")
    groups = set()
    if re.search(r"[A-Za-z]", source):
        groups.add("latin")
    if re.search(r"[\u0590-\u05FF]", source):
        groups.add("hebrew")
    if re.search(r"[\u0600-\u06FF]", source):
        groups.add("arabic")
    if re.search(r"[\u0400-\u04FF]", source):
        groups.add("cyrillic")
    if re.search(r"[\u3040-\u30FF\u4E00-\u9FFF\uAC00-\uD7AF]", source):
        groups.add("cjk")
    return sorted(groups)


def count_social_noise_markers(text: str) -> int:
    source = normalize_text_preserve_lines(text).lower()
    if not source:
        return 0
    hits = 0
    for marker in SOCIAL_CONFIRMATION_NOISE_MARKERS:
        if marker in source:
            hits += 1
    return hits


def extract_confirmation_job_context(job: dict | None) -> dict:
    job = job or {}
    debug_data = _as_plain_dict(job.get("debug_data"))
    routing = _as_plain_dict(
        job.get("confirmation_routing")
        or debug_data.get("confirmation_routing")
        or debug_data.get("confirmation_request")
        or debug_data.get("investigation_confirmation")
        or {}
    )
    confirmation_reason = normalize_text(job.get("confirmation_reason") or routing.get("confirmation_reason") or "").lower()
    require_different_profile_from = normalize_text(
        job.get("require_different_profile_from") or routing.get("require_different_profile_from") or ""
    ).lower()
    allowed_profile_ids = _parse_string_list(
        job.get("allowed_collector_profile_ids") or routing.get("allowed_collector_profile_ids")
    )
    excluded_profile_ids = _parse_string_list(
        job.get("excluded_collector_profile_ids") or routing.get("excluded_collector_profile_ids")
    )
    required_capabilities = _parse_string_list(
        job.get("required_collector_capabilities") or routing.get("required_collector_capabilities")
    )
    job_type = normalize_text(job.get("job_type") or routing.get("job_type") or job.get("claim_kind") or routing.get("claim_kind") or "").lower()
    parent_job_id = str(job.get("parent_job_id") or routing.get("parent_job_id") or "").strip()
    requested_by_collector_profile_id = normalize_text(
        job.get("requested_by_collector_profile_id") or routing.get("requested_by_collector_profile_id") or ""
    ).lower()
    baseline_evidence = _as_plain_dict(
        debug_data.get("confirmation_baseline_evidence")
        or routing.get("baseline_evidence")
        or {}
    )

    is_confirmation_job = bool(
        confirmation_reason
        or require_different_profile_from
        or allowed_profile_ids
        or excluded_profile_ids
        or required_capabilities
        or job_type in {"confirmation", "investigation_confirmation", "confirmation_request"}
    )

    return {
        "is_confirmation_job": is_confirmation_job,
        "confirmation_reason": confirmation_reason,
        "require_different_profile_from": require_different_profile_from,
        "allowed_collector_profile_ids": allowed_profile_ids,
        "excluded_collector_profile_ids": excluded_profile_ids,
        "required_collector_capabilities": required_capabilities,
        "parent_job_id": parent_job_id,
        "requested_by_collector_profile_id": requested_by_collector_profile_id,
        "job_type": job_type,
        "baseline_evidence": baseline_evidence,
    }


def _extract_comparison_lines_from_evidence(evidence: dict | None) -> list[str]:
    payload = evidence or {}
    text = combine_text_blocks([
        payload.get("page_title"),
        payload.get("meta_description"),
        payload.get("expanded_caption_text"),
        payload.get("visible_text_after_expand"),
        payload.get("raw_page_text"),
        payload.get("transcript_text"),
    ])
    lines = []
    seen = set()
    for raw_line in normalize_text_preserve_lines(text).split("\n"):
        line = normalize_text(raw_line)
        if len(line) < 8:
            continue
        if line in seen:
            continue
        seen.add(line)
        lines.append(line)
        if len(lines) >= 120:
            break
    return lines


def build_confirmation_overlap_summary(current_evidence: dict, baseline_evidence: dict | None) -> dict:
    baseline = baseline_evidence or {}
    if not baseline:
        return {}

    current_lines = _extract_comparison_lines_from_evidence(current_evidence)
    baseline_lines = _extract_comparison_lines_from_evidence(baseline)
    if not current_lines and not baseline_lines:
        return {}

    baseline_set = set(baseline_lines)
    current_set = set(current_lines)
    shared = [line for line in current_lines if line in baseline_set]
    current_only = [line for line in current_lines if line not in baseline_set]
    baseline_only = [line for line in baseline_lines if line not in current_set]

    return {
        "shared_line_count": len(shared),
        "current_only_line_count": len(current_only),
        "baseline_only_line_count": len(baseline_only),
        "shared_text_preview": trim_text("\n".join(shared[:8]), 1200),
        "current_only_preview": trim_text("\n".join(current_only[:8]), 1200),
        "baseline_only_preview": trim_text("\n".join(baseline_only[:8]), 1200),
    }


def evaluate_social_confirmation_suspicion(
    platform: str,
    evidence: dict,
    source_metadata: dict,
    investigation_result: dict,
) -> dict:
    normalized_platform = normalize_text(platform).lower()
    if normalized_platform not in {"instagram", "facebook", "tiktok"}:
        return {
            "recommended": False,
            "score": 0,
            "reason": "",
            "reasons": [],
            "noise_marker_count": 0,
            "hashtag_count": 0,
            "script_groups": [],
            "current_metrics_summary": {},
            "stable_metrics_summary": {},
        }

    current_text = combine_text_blocks([
        evidence.get("page_title"),
        evidence.get("meta_description"),
        evidence.get("expanded_caption_text"),
        evidence.get("visible_text_after_expand"),
        evidence.get("raw_page_text"),
        evidence.get("transcript_text"),
    ])
    merged_evidence = _as_plain_dict((investigation_result or {}).get("merged_evidence"))
    stable_text = combine_text_blocks([
        merged_evidence.get("page_title"),
        merged_evidence.get("meta_description"),
        merged_evidence.get("expanded_caption_text"),
        merged_evidence.get("raw_page_text"),
        merged_evidence.get("transcript_text"),
    ])

    current_metrics = evaluate_evidence_text(current_text[:15000])
    stable_metrics = evaluate_evidence_text(stable_text[:15000]) if stable_text else {}

    noise_marker_count = count_social_noise_markers(current_text[:12000])
    hashtag_count = min(
        count_regex_matches(current_text[:8000], r"(?:^|[\s])#[\w\-]+"),
        30,
    )
    script_groups = detect_script_groups(current_text[:6000])

    score = 0
    reasons = []

    if (
        bool((investigation_result or {}).get("linked_recipe_used"))
        and stable_metrics.get("looksRecipeDense")
        and not current_metrics.get("looksRecipeDense")
    ):
        score += 2
        reasons.append("linked_recipe_cleaner_than_social_source")

    if noise_marker_count >= 2:
        score += 2
        reasons.append("social_noise_markers")
    elif noise_marker_count == 1:
        score += 1
        reasons.append("one_social_noise_marker")

    if hashtag_count >= 5:
        score += 1
        reasons.append("many_hashtags")

    if len(script_groups) >= 2 and noise_marker_count >= 1:
        score += 1
        reasons.append("mixed_scripts_with_noise")

    if (
        stable_metrics.get("measurementSignalCount", 0) >= 3
        and current_metrics.get("measurementSignalCount", 0) == 0
    ):
        score += 1
        reasons.append("measurements_only_in_stable_text")

    if (
        has_food_context(stable_text)
        and not has_food_context(current_text)
        and noise_marker_count >= 1
    ):
        score += 1
        reasons.append("stable_text_food_context_stronger")

    recommended = bool(COLLECTOR_PROFILE_ID) and score >= 3
    return {
        "recommended": recommended,
        "score": score,
        "reason": "social_account_contamination" if recommended else "",
        "reasons": reasons,
        "noise_marker_count": noise_marker_count,
        "hashtag_count": hashtag_count,
        "script_groups": script_groups,
        "current_metrics_summary": {
            "lineCount": current_metrics.get("lineCount"),
            "measurementSignalCount": current_metrics.get("measurementSignalCount"),
            "looksRecipeDense": current_metrics.get("looksRecipeDense"),
            "hasFoodContext": current_metrics.get("hasFoodContext"),
        },
        "stable_metrics_summary": {
            "lineCount": stable_metrics.get("lineCount"),
            "measurementSignalCount": stable_metrics.get("measurementSignalCount"),
            "looksRecipeDense": stable_metrics.get("looksRecipeDense"),
            "hasFoodContext": stable_metrics.get("hasFoodContext"),
        },
    }


def build_confirmation_debug_payload(
    job: dict,
    platform: str,
    evidence: dict,
    source_metadata: dict,
    investigation_result: dict,
) -> dict:
    confirmation_context = extract_confirmation_job_context(job)
    suspicion = evaluate_social_confirmation_suspicion(
        platform,
        evidence,
        source_metadata,
        investigation_result,
    )
    overlap_summary = build_confirmation_overlap_summary(
        evidence,
        confirmation_context.get("baseline_evidence") or {},
    )

    return {
        **get_current_collector_identity(),
        "confirmation_job": bool(confirmation_context.get("is_confirmation_job")),
        "confirmation_job_type": confirmation_context.get("job_type") or "",
        "confirmation_reason": confirmation_context.get("confirmation_reason") or "",
        "confirmation_parent_job_id": confirmation_context.get("parent_job_id") or "",
        "confirmation_requested_by_collector_profile_id": confirmation_context.get("requested_by_collector_profile_id") or "",
        "require_different_profile_from": confirmation_context.get("require_different_profile_from") or "",
        "allowed_collector_profile_ids": list(confirmation_context.get("allowed_collector_profile_ids") or [])[:12],
        "excluded_collector_profile_ids": list(confirmation_context.get("excluded_collector_profile_ids") or [])[:12],
        "required_collector_capabilities": list(confirmation_context.get("required_collector_capabilities") or [])[:12],
        "contamination_confirmation_recommended": bool(suspicion.get("recommended")),
        "contamination_confirmation_score": suspicion.get("score", 0),
        "contamination_confirmation_reason": suspicion.get("reason") or "",
        "contamination_confirmation_reasons": list(suspicion.get("reasons") or [])[:12],
        "contamination_noise_marker_count": suspicion.get("noise_marker_count", 0),
        "contamination_hashtag_count": suspicion.get("hashtag_count", 0),
        "contamination_script_groups": list(suspicion.get("script_groups") or []),
        "confirmation_current_metrics": suspicion.get("current_metrics_summary") or {},
        "confirmation_stable_metrics": suspicion.get("stable_metrics_summary") or {},
        "confirmation_comparison_shared_line_count": overlap_summary.get("shared_line_count", 0),
        "confirmation_comparison_current_only_line_count": overlap_summary.get("current_only_line_count", 0),
        "confirmation_comparison_baseline_only_line_count": overlap_summary.get("baseline_only_line_count", 0),
        "confirmation_shared_text_preview": overlap_summary.get("shared_text_preview", ""),
        "confirmation_current_only_preview": overlap_summary.get("current_only_preview", ""),
        "confirmation_baseline_only_preview": overlap_summary.get("baseline_only_preview", ""),
    }




def build_confirmation_evidence_snapshot(evidence: dict, source_metadata: dict, linked_recipe_used: bool, effective_analysis_platform: str) -> dict:
    payload = evidence or {}
    return {
        "effective_page_url": trim_text(payload.get("effective_page_url") or "", 1000),
        "page_title": trim_text(payload.get("page_title") or "", 500),
        "meta_description": trim_text(payload.get("meta_description") or "", 2000),
        "expanded_caption_text": trim_text(payload.get("expanded_caption_text") or "", 4000),
        "visible_text_after_expand": trim_text(payload.get("visible_text_after_expand") or "", 4000),
        "raw_page_text": trim_text(payload.get("raw_page_text") or "", 4000),
        "transcript_text": trim_text(payload.get("transcript_text") or "", 4000),
        "linked_recipe_used": bool(linked_recipe_used),
        "effective_analysis_platform": normalize_text(effective_analysis_platform or "").lower(),
        "source_platform": trim_text(source_metadata.get("source_platform") or "", 100),
        "source_profile_url": trim_text(source_metadata.get("source_profile_url") or "", 1000),
        "source_creator_handle": trim_text(source_metadata.get("source_creator_handle") or "", 200),
        "source_creator_name": trim_text(source_metadata.get("source_creator_name") or "", 300),
    }


def choose_confirmation_merged_preview(overlap_summary: dict, baseline_snapshot: dict, current_snapshot: dict) -> str:
    shared_preview = normalize_text_preserve_lines(overlap_summary.get("shared_text_preview") or "")
    if shared_preview:
        return trim_text(shared_preview, 1600)

    baseline_text = combine_text_blocks([
        baseline_snapshot.get("page_title"),
        baseline_snapshot.get("meta_description"),
        baseline_snapshot.get("expanded_caption_text"),
        baseline_snapshot.get("visible_text_after_expand"),
        baseline_snapshot.get("raw_page_text"),
    ])
    current_text = combine_text_blocks([
        current_snapshot.get("page_title"),
        current_snapshot.get("meta_description"),
        current_snapshot.get("expanded_caption_text"),
        current_snapshot.get("visible_text_after_expand"),
        current_snapshot.get("raw_page_text"),
    ])
    baseline_metrics = evaluate_evidence_text(baseline_text[:12000]) if baseline_text else {}
    current_metrics = evaluate_evidence_text(current_text[:12000]) if current_text else {}
    chosen = baseline_text
    if current_metrics.get("looksRecipeDense") and not baseline_metrics.get("looksRecipeDense"):
        chosen = current_text
    elif current_metrics.get("measurementSignalCount", 0) > baseline_metrics.get("measurementSignalCount", 0):
        chosen = current_text
    return trim_text(normalize_text_preserve_lines(chosen), 1600)


def finalize_confirmation_job(
    job_id: str,
    recipe_id: str | None,
    confirmation_context: dict,
    collector_identity: dict,
    evidence: dict,
    source_metadata: dict,
    linked_recipe_used: bool,
    effective_analysis_platform: str,
    primary_screenshot_url: str = "",
    description_screenshot_url: str = "",
):
    baseline_snapshot = confirmation_context.get("baseline_evidence") or {}
    current_snapshot = build_confirmation_evidence_snapshot(
        evidence,
        source_metadata,
        linked_recipe_used,
        effective_analysis_platform,
    )
    overlap_summary = build_confirmation_overlap_summary(current_snapshot, baseline_snapshot)
    merged_preview = choose_confirmation_merged_preview(overlap_summary, baseline_snapshot, current_snapshot)

    comparison_debug = {
        **collector_identity,
        "confirmation_collection_only": True,
        "confirmation_parent_job_id": confirmation_context.get("parent_job_id") or "",
        "confirmation_reason": confirmation_context.get("confirmation_reason") or "",
        "confirmation_baseline_profile_id": confirmation_context.get("requested_by_collector_profile_id") or confirmation_context.get("require_different_profile_from") or "",
        "confirmation_current_profile_id": collector_identity.get("collector_profile_id") or "",
        "evidence_by_collector": {
            "baseline": {
                "collector_profile_id": confirmation_context.get("requested_by_collector_profile_id") or confirmation_context.get("require_different_profile_from") or "",
                "snapshot": baseline_snapshot,
            },
            "current": {
                "collector_profile_id": collector_identity.get("collector_profile_id") or "",
                "snapshot": current_snapshot,
            },
        },
        "shared_stable_text_preview": overlap_summary.get("shared_text_preview", ""),
        "conflicting_text": {
            "current_only_preview": overlap_summary.get("current_only_preview", ""),
            "baseline_only_preview": overlap_summary.get("baseline_only_preview", ""),
        },
        "chosen_merged_evidence_preview": merged_preview,
        "confirmation_primary_screenshot_url": primary_screenshot_url or "",
        "confirmation_description_screenshot_url": description_screenshot_url or "",
    }

    append_job_debug_log(
        job_id,
        "Confirmation collector evidence captured; analyzer submit skipped.",
        debug_status="confirmation_complete",
        debug_last_step="confirmation_completed",
        debug_data=comparison_debug,
    )
    update_job(job_id, {
        "status": "done",
        "decision": "confirmation_collected",
        "finished_at": utc_now_iso(),
        "debug_status": "confirmation_complete",
        "debug_last_step": "confirmation_completed",
    })

    parent_job_id = confirmation_context.get("parent_job_id") or ""
    if parent_job_id:
        try:
            append_job_debug_log(
                parent_job_id,
                f"Confirmation BotJob {job_id} completed by collector_profile={collector_identity.get('collector_profile_id') or 'none'}",
                debug_last_step="confirmation_child_completed",
                debug_data={
                    "confirmation_child_job_id": job_id,
                    "confirmation_child_collector_profile_id": collector_identity.get("collector_profile_id") or "",
                    "confirmation_child_collector_node_id": collector_identity.get("collector_node_id") or "",
                    "confirmation_result": comparison_debug,
                },
            )
        except Exception:
            pass

    print("CONFIRMATION_JOB_COMPLETED = True")
    print("JOB_ID =", job_id)
    return True


def maybe_create_confirmation_job(
    job: dict,
    job_id: str,
    recipe_id: str | None,
    target_url: str,
    platform: str,
    collector_identity: dict,
    evidence: dict,
    source_metadata: dict,
    linked_recipe_used: bool,
    effective_analysis_platform: str,
    confirmation_debug: dict,
):
    if extract_confirmation_job_context(job).get("is_confirmation_job"):
        return None
    if not confirmation_debug.get("contamination_confirmation_recommended"):
        return None

    existing_debug = _as_plain_dict(job.get("debug_data"))
    if existing_debug.get("confirmation_child_job_id"):
        return {"id": existing_debug.get("confirmation_child_job_id")}

    requested_profile = normalize_text(collector_identity.get("collector_profile_id") or "").lower()
    if not requested_profile:
        return None

    baseline_snapshot = build_confirmation_evidence_snapshot(
        evidence,
        source_metadata,
        linked_recipe_used,
        effective_analysis_platform,
    )
    child_debug = {
        "confirmation_baseline_evidence": baseline_snapshot,
        "confirmation_request": {
            "confirmation_reason": confirmation_debug.get("contamination_confirmation_reason") or "social_account_contamination",
            "require_different_profile_from": requested_profile,
            "requested_by_collector_profile_id": requested_profile,
            "parent_job_id": job_id,
            "job_type": "confirmation",
            "claim_kind": "confirmation",
            "required_collector_platforms": [platform],
        },
        "confirmation_parent_source_url": target_url,
        "confirmation_parent_collector": collector_identity,
        "confirmation_parent_debug": {
            "score": confirmation_debug.get("contamination_confirmation_score", 0),
            "reason": confirmation_debug.get("contamination_confirmation_reason") or "",
            "reasons": confirmation_debug.get("contamination_confirmation_reasons") or [],
        },
    }

    child = create_confirmation_job(
        parent_job_id=job_id,
        recipe_id=recipe_id,
        target_url=target_url,
        platform=platform,
        confirmation_reason=confirmation_debug.get("contamination_confirmation_reason") or "social_account_contamination",
        require_different_profile_from=requested_profile,
        requested_by_collector_profile_id=requested_profile,
        excluded_collector_profile_ids=[requested_profile],
        required_collector_platforms=[platform],
        debug_data=child_debug,
    )
    child_id = str(child.get("id") or "").strip()
    if child_id:
        append_job_debug_log(
            job_id,
            (
                f"Queued confirmation BotJob {child_id} for a different collector profile. "
                f"requested_profile={requested_profile or 'none'} requested_node={collector_identity.get('collector_node_id') or 'none'} "
                f"note=different_node_alone_is_not_enough"
            ),
            debug_last_step="confirmation_job_created",
            debug_data={
                "confirmation_child_job_id": child_id,
                "confirmation_request_created": True,
                "confirmation_request": child_debug.get("confirmation_request") or {},
                "confirmation_requested_by_collector_node_id": collector_identity.get("collector_node_id") or "",
                "confirmation_requested_by_collector_profile_id": requested_profile,
                "confirmation_requires_different_profile": True,
                "confirmation_routing_note": "different_node_alone_is_not_enough",
            },
        )
    return child


def normalize_analyzer_runtime_snapshot(snapshot):
    if not isinstance(snapshot, dict):
        return deep_clone_json(BUILTIN_ANALYZER_RUNTIME)

    scenarios = snapshot.get("scenarios") if isinstance(snapshot.get("scenarios"), dict) else {}
    merged_scenarios = deep_clone_json(BUILTIN_ANALYZER_RUNTIME["scenarios"])
    for key, value in scenarios.items():
        if not isinstance(value, dict):
            continue
        previous = merged_scenarios.get(key) if isinstance(merged_scenarios.get(key), dict) else {"enabled": True, "rules": {}, "notes": ""}
        merged_scenarios[key] = {
            "enabled": value.get("enabled") is not False,
            "rules": value.get("rules") if isinstance(value.get("rules"), dict) else previous.get("rules", {}),
            "notes": str(value.get("notes") or previous.get("notes") or ""),
        }

    return {
        "schema_version": int(snapshot.get("schema_version") or 1),
        "published_version": str(snapshot.get("published_version") or BUILTIN_ANALYZER_RUNTIME["published_version"]),
        "updated_at": snapshot.get("updated_at"),
        "updated_by": snapshot.get("updated_by"),
        "notes": str(snapshot.get("notes") or ""),
        "fixtures": snapshot.get("fixtures") if isinstance(snapshot.get("fixtures"), list) else [],
        "scenarios": merged_scenarios,
    }


def load_published_analyzer_runtime(force: bool = False) -> dict:
    global ANALYZER_RUNTIME_CACHE

    now = time.time()
    if (
        not force
        and ANALYZER_RUNTIME_CACHE.get("snapshot") is not None
        and (now - float(ANALYZER_RUNTIME_CACHE.get("loaded_at") or 0.0)) < ANALYZER_RUNTIME_CACHE_TTL_SECONDS
    ):
        return ANALYZER_RUNTIME_CACHE

    cache = {
        "loaded_at": now,
        "snapshot": deep_clone_json(BUILTIN_ANALYZER_RUNTIME),
        "version": BUILTIN_ANALYZER_RUNTIME["published_version"],
        "source": "builtin",
        "error": None,
        "url": ANALYZER_RUNTIME_FUNCTION_URL or None,
        "url_source": ANALYZER_RUNTIME_FUNCTION_URL_SOURCE,
    }

    if ANALYZER_RUNTIME_FUNCTION_URL:
        request_attempts = []

        internal_body = {"runtime_only": True}
        internal_headers = {"Content-Type": "application/json"}
        if BOT_SECRET:
            internal_headers["x-bot-secret"] = BOT_SECRET
            internal_body["bot_secret"] = BOT_SECRET
        if ANALYZER_RUNTIME_INTERNAL_SECRET:
            internal_body.update({
                "internal_invocation": True,
                "internal_secret": ANALYZER_RUNTIME_INTERNAL_SECRET,
                "internal_caller": f"pi:{DEVICE_NAME}",
                "scraper_secret": ANALYZER_RUNTIME_INTERNAL_SECRET,
            })
            internal_headers["x-internal-secret"] = ANALYZER_RUNTIME_INTERNAL_SECRET
            internal_headers["x-scraper-secret"] = ANALYZER_RUNTIME_INTERNAL_SECRET
        request_attempts.append((internal_headers, internal_body, "header+body_internal"))

        if BOT_SECRET:
            request_attempts.append((
                {"Content-Type": "application/json", "x-bot-secret": BOT_SECRET},
                {"runtime_only": True, "bot_secret": BOT_SECRET},
                "bot_secret_only",
            ))

        request_attempts.append((
            {"Content-Type": "application/json"},
            {"runtime_only": True},
            "runtime_only_public",
        ))

        last_error = None
        for headers, request_body, attempt_label in request_attempts:
            request = urllib.request.Request(
                ANALYZER_RUNTIME_FUNCTION_URL,
                data=json.dumps(request_body).encode("utf-8"),
                headers=headers,
                method="POST",
            )

            try:
                with urllib.request.urlopen(request, timeout=ANALYZER_RUNTIME_FETCH_TIMEOUT_SECONDS) as response:
                    payload = json.loads(response.read().decode("utf-8") or "{}")
                    published = payload.get("published") if isinstance(payload, dict) else None
                    if isinstance(published, dict):
                        cache["snapshot"] = normalize_analyzer_runtime_snapshot(published)
                        cache["version"] = str(payload.get("runtime_version") or cache["snapshot"].get("published_version") or BUILTIN_ANALYZER_RUNTIME["published_version"])
                        cache["source"] = str(payload.get("runtime_source") or "settings")
                        cache["error"] = payload.get("runtime_error")
                        cache["auth_mode"] = payload.get("auth_mode") or attempt_label
                        break
                    last_error = f"runtime_missing_published_snapshot:{attempt_label}"
            except urllib.error.HTTPError as runtime_http_err:
                response_preview = ""
                try:
                    response_preview = (runtime_http_err.read() or b"").decode("utf-8", errors="ignore")[:500]
                except Exception:
                    response_preview = ""
                last_error = f"HTTPError:{runtime_http_err.code}:{attempt_label}:{response_preview or runtime_http_err.reason}"
            except Exception as runtime_err:
                last_error = f"{type(runtime_err).__name__}:{attempt_label}:{runtime_err}"

        if cache.get("source") == "builtin":
            cache["error"] = last_error or cache.get("error") or "runtime_fetch_failed"
    else:
        cache["error"] = "runtime_fetch_disabled_no_function_url"

    ANALYZER_RUNTIME_CACHE = cache
    return ANALYZER_RUNTIME_CACHE


def get_runtime_scenario_config(scenario_key: str) -> dict:
    runtime = load_published_analyzer_runtime()
    snapshot = runtime.get("snapshot") if isinstance(runtime.get("snapshot"), dict) else BUILTIN_ANALYZER_RUNTIME
    scenarios = snapshot.get("scenarios") if isinstance(snapshot.get("scenarios"), dict) else {}
    scenario = scenarios.get(scenario_key) if isinstance(scenarios.get(scenario_key), dict) else {}
    rules = scenario.get("rules") if isinstance(scenario.get("rules"), dict) else {}
    return {
        "enabled": scenario.get("enabled") is not False,
        "rules": rules,
        "runtime_version": runtime.get("version") or BUILTIN_ANALYZER_RUNTIME["published_version"],
        "runtime_source": runtime.get("source") or "builtin",
        "runtime_error": runtime.get("error"),
        "runtime_url": runtime.get("url"),
        "runtime_url_source": runtime.get("url_source"),
    }


INVESTIGATION_RULE_DEFAULTS = {
    "clue_phrases": [],
    "blocked_hosts": [],
    "profile_follow_rules": {},
    "search_phrases": [],
    "candidate_limits": {},
    "ranking_weights": {},
    "creator_overrides": {},
    "domain_overrides": {},
    "language_overrides": {},
    "stop_thresholds": {},
    "fixture_ids": [],
}

INVESTIGATION_FIXTURE_SEED_PACK = [
    {
        "fixture_id": "instagram.external_site.batia.winner",
        "scenario": "instagram.external_site",
        "expected_outcome": "winner",
        "source_url": "https://www.instagram.com/reels/C5lgUa-IQ5-/",
        "expected_winner_url": "https://just-batia.com/",
        "notes": "Known good Instagram external-site winner path.",
    },
    {
        "fixture_id": "instagram.external_site.cookful.winner",
        "scenario": "instagram.external_site",
        "expected_outcome": "winner",
        "source_url": "https://www.instagram.com/reels/DPEu8Ydke5r/",
        "expected_winner_url": "https://thecookful.com/evaporated-milk-mac-n-cheese/",
        "notes": "Direct clue/search handoff to TheCookful.",
    },
    {
        "fixture_id": "instagram.external_site.victoria.acceptable_partial",
        "scenario": "instagram.external_site",
        "expected_outcome": "acceptable_partial",
        "source_url": "https://www.instagram.com/reels/C_glNy1hmd2/",
        "expected_winner_url": "https://recipesbyvictoria.substack.com/",
        "notes": "Linked page is acceptable even when the public page is a paywalled preview.",
    },
    {
        "fixture_id": "instagram.external_site.cucinabyelena.no_winner",
        "scenario": "instagram.external_site",
        "expected_outcome": "no_winner",
        "source_url": "https://www.instagram.com/p/DWHMkyGFEec/?hl=en&img_index=1",
        "expected_winner_url": "",
        "notes": "Clue-follow attempt should record a no-winner path cleanly.",
    },
]

INVESTIGATION_LEGACY_LIMIT_KEYS = {
    "profile": "profile_candidate_limit",
    "external_site": "external_site_candidate_limit",
    "internal_page": "internal_page_candidate_limit",
}
INVESTIGATION_LEGACY_THRESHOLD_KEYS = {
    "minimum_winner_score": "minimum_winner_score",
}


def _merge_investigation_rule_value(default_value, incoming_value):
    if isinstance(default_value, dict):
        merged = dict(default_value)
        if isinstance(incoming_value, dict):
            for key, value in incoming_value.items():
                merged[key] = value
        return merged

    if isinstance(default_value, list):
        if not isinstance(incoming_value, list):
            return list(default_value)
        out = []
        seen = set()
        for item in list(default_value) + list(incoming_value):
            marker = json.dumps(item, ensure_ascii=False, sort_keys=True) if isinstance(item, (dict, list)) else str(item)
            if marker in seen:
                continue
            seen.add(marker)
            out.append(item)
        return out

    return incoming_value if incoming_value is not None else default_value


def resolve_investigation_rules(rules: dict | None) -> dict:
    if not isinstance(rules, dict):
        return dict(INVESTIGATION_RULE_DEFAULTS)

    flat_rules = {key: value for key, value in rules.items() if key != "investigation_rules"}
    nested_rules = rules.get("investigation_rules") if isinstance(rules.get("investigation_rules"), dict) else {}

    resolved = {}
    for key, default_value in INVESTIGATION_RULE_DEFAULTS.items():
        resolved[key] = _merge_investigation_rule_value(default_value, None)

    for source in (flat_rules, nested_rules):
        for key, value in source.items():
            default_value = INVESTIGATION_RULE_DEFAULTS.get(key)
            if key in INVESTIGATION_RULE_DEFAULTS:
                resolved[key] = _merge_investigation_rule_value(default_value, value)
            else:
                resolved[key] = value

    candidate_limits = resolved.get("candidate_limits") if isinstance(resolved.get("candidate_limits"), dict) else {}
    candidate_limits = dict(candidate_limits)
    for limit_name, legacy_key in INVESTIGATION_LEGACY_LIMIT_KEYS.items():
        if limit_name not in candidate_limits and rules.get(legacy_key) is not None:
            candidate_limits[limit_name] = rules.get(legacy_key)
    resolved["candidate_limits"] = candidate_limits

    stop_thresholds = resolved.get("stop_thresholds") if isinstance(resolved.get("stop_thresholds"), dict) else {}
    stop_thresholds = dict(stop_thresholds)
    for threshold_name, legacy_key in INVESTIGATION_LEGACY_THRESHOLD_KEYS.items():
        if threshold_name not in stop_thresholds and rules.get(legacy_key) is not None:
            stop_thresholds[threshold_name] = rules.get(legacy_key)
    resolved["stop_thresholds"] = stop_thresholds

    return resolved


def get_runtime_investigation_config(scenario_key: str) -> dict:
    config = get_runtime_scenario_config(scenario_key)
    resolved_rules = resolve_investigation_rules(config.get("rules"))
    return {
        **config,
        "rules": resolved_rules,
        "investigation_engine_version": INVESTIGATION_ENGINE_VERSION,
        "fixture_seed_matches": match_investigation_fixture_seeds(scenario_key, ""),
        "rules_summary": summarize_investigation_rules(resolved_rules),
    }


def get_investigation_candidate_limit(rules: dict | None, limit_name: str, default: int) -> int:
    try:
        if isinstance(rules, dict):
            candidate_limits = rules.get("candidate_limits") if isinstance(rules.get("candidate_limits"), dict) else {}
            if candidate_limits.get(limit_name) is not None:
                return max(int(candidate_limits.get(limit_name) or default), 1)

            legacy_key = INVESTIGATION_LEGACY_LIMIT_KEYS.get(limit_name)
            if legacy_key and rules.get(legacy_key) is not None:
                return max(int(rules.get(legacy_key) or default), 1)
    except Exception:
        pass
    return max(int(default or 1), 1)


def get_investigation_stop_threshold(rules: dict | None, threshold_name: str, default: int) -> int:
    try:
        if isinstance(rules, dict):
            stop_thresholds = rules.get("stop_thresholds") if isinstance(rules.get("stop_thresholds"), dict) else {}
            if stop_thresholds.get(threshold_name) is not None:
                return int(stop_thresholds.get(threshold_name) or default)

            legacy_key = INVESTIGATION_LEGACY_THRESHOLD_KEYS.get(threshold_name)
            if legacy_key and rules.get(legacy_key) is not None:
                return int(rules.get(legacy_key) or default)
    except Exception:
        pass
    return int(default or 0)


def get_investigation_blocked_hosts(rules: dict | None) -> list[str]:
    blocked = []
    seen = set()
    for host in runtime_string_list(rules or {}, "blocked_hosts"):
        normalized = canonical_domain(host)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        blocked.append(normalized)
    return blocked


def summarize_investigation_rules(rules: dict | None) -> dict:
    rule_set = rules if isinstance(rules, dict) else {}
    return {
        "clue_phrase_count": len(runtime_string_list(rule_set, "clue_phrases")),
        "blocked_host_count": len(get_investigation_blocked_hosts(rule_set)),
        "search_phrase_count": len(runtime_string_list(rule_set, "search_phrases")),
        "fixture_id_count": len(runtime_string_list(rule_set, "fixture_ids")),
        "candidate_limits": {
            "profile": get_investigation_candidate_limit(rule_set, "profile", 1),
            "external_site": get_investigation_candidate_limit(rule_set, "external_site", 2),
            "internal_page": get_investigation_candidate_limit(rule_set, "internal_page", 4),
        },
        "stop_thresholds": {
            "minimum_winner_score": get_investigation_stop_threshold(rule_set, "minimum_winner_score", 80),
        },
    }


def match_investigation_fixture_seeds(scenario_key: str, target_url: str) -> list[str]:
    normalized_target = strip_url_query_fragment(normalize_profile_url(target_url or "", target_url or "")).lower()
    if not normalized_target:
        return []

    matches = []
    seen = set()
    for fixture in INVESTIGATION_FIXTURE_SEED_PACK:
        if normalize_text(fixture.get("scenario") or "").lower() != normalize_text(scenario_key).lower():
            continue
        fixture_url = strip_url_query_fragment(normalize_profile_url(fixture.get("source_url") or "", fixture.get("source_url") or "")).lower()
        if not fixture_url or fixture_url != normalized_target:
            continue
        fixture_id = normalize_text(fixture.get("fixture_id") or "")
        if fixture_id and fixture_id not in seen:
            seen.add(fixture_id)
            matches.append(fixture_id)
    return matches


def filter_runtime_blocked_investigation_candidates(result: dict, candidates: list[dict], rules: dict | None, *, source: str) -> list[dict]:
    blocked_hosts = get_investigation_blocked_hosts(rules or {})
    if not blocked_hosts:
        return list(candidates or [])

    kept = []
    for candidate in candidates or []:
        if not isinstance(candidate, dict):
            continue
        candidate_url = normalize_profile_url(candidate.get("url") or "", candidate.get("url") or "")
        candidate_host = canonical_domain(candidate_url)
        if candidate_host and any(candidate_host == blocked or candidate_host.endswith(f".{blocked}") for blocked in blocked_hosts):
            add_investigation_candidate(
                result,
                candidate_url,
                source=source,
                score=candidate.get("score"),
                usable=False,
                reason="runtime_blocked_host",
                extra={"blocked_host": candidate_host},
            )
            continue
        kept.append(candidate)
    return kept


def score_linked_page_candidate(base_score: int | float | None, linked_metrics: dict | None, *, strong_bonus: int = 60, dense_bonus: int = 120, food_context_bonus: int = 0) -> int:
    metrics = linked_metrics or {}
    score = int(base_score or 0)
    if metrics.get("looksRecipeDense"):
        score += dense_bonus
    elif metrics.get("hasStrongTextEvidence"):
        score += strong_bonus
    if food_context_bonus and metrics.get("hasFoodContext"):
        score += food_context_bonus
    return score


def build_linked_page_candidate_extra(linked_evidence: dict | None, linked_metrics: dict | None) -> dict:
    evidence = linked_evidence or {}
    metrics = linked_metrics or {}
    return {
        "page_title": evidence.get("page_title") or "",
        "line_count": metrics.get("lineCount"),
        "measurement_signal_count": metrics.get("measurementSignalCount"),
        "recipe_verb_signal_count": metrics.get("recipeVerbSignalCount"),
    }


def finalize_linked_investigation_result(
    result: dict,
    *,
    base_evidence: dict,
    linked_evidence: dict,
    winner_url: str,
    winner_score: int | float | None,
    winner_source_metadata: dict | None,
    merge_callback,
    explicit_recipe_link: str = "",
    breadcrumb_append=None,
    analysis_platform_hint: str = "linked_recipe_page",
    effective_analysis_platform: str = "web",
    debug_updates: dict | None = None,
):
    result["winner_url"] = winner_url
    result["winner_score"] = winner_score
    result["winner_source_metadata"] = winner_source_metadata or {}
    result["merged_evidence"] = merge_callback()
    result["linked_recipe_used"] = True
    result["analysis_platform_hint"] = analysis_platform_hint
    result["effective_analysis_platform"] = effective_analysis_platform
    result["explicit_recipe_link"] = explicit_recipe_link or winner_url

    if breadcrumb_append:
        breadcrumb = result.setdefault("breadcrumb", [])
        if isinstance(breadcrumb_append, (list, tuple)):
            breadcrumb.extend([str(item) for item in breadcrumb_append if str(item or "")])
        else:
            breadcrumb.append(str(breadcrumb_append))

    if isinstance(debug_updates, dict):
        result.setdefault("debug", {}).update(debug_updates)

    return result


def build_instagram_investigation_context(*, target_url: str, evidence: dict, source_metadata: dict) -> dict:
    runtime = get_instagram_external_site_runtime_rules()
    runtime_rules = runtime.get("rules") if isinstance(runtime, dict) else {}

    instagram_query_text = build_instagram_discovery_query_text(evidence)
    instagram_discovery_text = combine_text_blocks([
        instagram_query_text,
        evidence.get("raw_page_text"),
        evidence.get("meta_description"),
    ])
    instagram_owner_hint = extract_instagram_owner_hint(instagram_discovery_text)
    instagram_hint_tokens = extract_instagram_hint_tokens(instagram_query_text or instagram_discovery_text)
    instagram_mentions = extract_instagram_mentions(instagram_discovery_text)
    instagram_query_info = extract_instagram_primary_query_info(instagram_query_text or instagram_discovery_text)
    instagram_clue_text = combine_text_blocks([
        evidence.get("expanded_caption_text"),
        evidence.get("visible_text_after_expand"),
        evidence.get("raw_page_text"),
        evidence.get("meta_description"),
        instagram_query_text,
    ])
    detected_clues = detect_investigation_clues(instagram_clue_text or instagram_discovery_text, runtime_rules)
    should_discover_instagram, instagram_discovery_reasons = should_try_instagram_discovery(evidence, source_metadata)

    source_metadata_updates = {}
    owner_hint_handle = source_safe_handle(instagram_owner_hint.get("handle") or "")
    if owner_hint_handle:
        owner_hint_name = clean_instagram_display_name(instagram_owner_hint.get("display_name") or "") or source_safe_text(owner_hint_handle.lstrip('@'))
        source_metadata_updates = enrich_source_metadata({
            **(source_metadata or {}),
            "source_creator_name": owner_hint_name or source_metadata.get("source_creator_name") or source_metadata.get("source_channel_name") or owner_hint_handle.lstrip('@'),
            "source_channel_name": owner_hint_name or source_metadata.get("source_channel_name") or source_metadata.get("source_creator_name") or owner_hint_handle.lstrip('@'),
            "source_creator_handle": owner_hint_handle,
            "source_profile_url": normalize_instagram_profile_root(f"https://www.instagram.com/{owner_hint_handle.lstrip('@')}/"),
            "source_page_domain": "instagram.com",
        }, "instagram", target_url)

    merged_source_metadata = source_metadata_updates or source_metadata or {}
    instagram_domain_affinity_tokens = extract_instagram_domain_affinity_tokens(
        merged_source_metadata,
        instagram_owner_hint,
        instagram_query_info,
    )

    return {
        "runtime": runtime,
        "runtime_rules": runtime_rules,
        "instagram_query_text": instagram_query_text,
        "instagram_discovery_text": instagram_discovery_text,
        "instagram_owner_hint": instagram_owner_hint,
        "instagram_hint_tokens": instagram_hint_tokens,
        "instagram_mentions": instagram_mentions,
        "instagram_query_info": instagram_query_info,
        "should_discover": should_discover_instagram,
        "discovery_reasons": list(instagram_discovery_reasons or []),
        "detected_clues": list(detected_clues or []),
        "source_metadata_updates": source_metadata_updates,
        "instagram_domain_affinity_tokens": instagram_domain_affinity_tokens,
        "fixture_seed_matches": match_investigation_fixture_seeds("instagram.external_site", target_url),
        "rules_summary": summarize_investigation_rules(runtime_rules),
    }


def runtime_string_list(rules: dict, key: str) -> list[str]:
    value = rules.get(key) if isinstance(rules, dict) else None
    if not isinstance(value, list):
        return []
    out = []
    seen = set()
    for item in value:
        text = normalize_text(item)
        lower = text.lower()
        if not text or lower in seen:
            continue
        seen.add(lower)
        out.append(text)
    return out


def apply_runtime_noise_phrases(text: str, rules: dict) -> str:
    cleaned = str(text or "")
    for phrase in runtime_string_list(rules, "noise_phrases"):
        cleaned = re.sub(re.escape(phrase), " ", cleaned, flags=re.IGNORECASE)
    return normalize_text_preserve_lines(cleaned)


def get_instagram_external_site_runtime_rules() -> dict:
    config = get_runtime_investigation_config("instagram.external_site")
    if config.get("enabled") is False:
        return {"rules": {}, "runtime_version": config.get("runtime_version"), "runtime_source": config.get("runtime_source"), "runtime_error": config.get("runtime_error"), "enabled": False}
    return config


SOCIAL_PROFILE_RESERVED_PATHS = {
    "", "reel", "reels", "p", "tv", "stories", "explore", "accounts", "about", "legal",
    "watch", "shorts", "channel", "user", "c", "playlist", "post", "posts", "share",
    "pin", "video", "videos", "photo", "photos", "hashtag", "tags", "login", "signup",
}

GENERIC_SOURCE_LABELS = {
    "instagram", "facebook", "learn more", "more", "view profile", "follow", "message", "messages",
    "original audio", "see translation", "see more", "show more", "about", "help", "help center",
    "privacy", "terms", "meta", "open app", "open in instagram",
}

SOCIAL_EXTERNAL_HOST_BLOCKLIST = {
    "instagram.com", "facebook.com", "fb.watch", "youtube.com", "youtu.be", "tiktok.com", "pinterest.com",
    "accounts.google.com", "support.google.com", "developers.google.com", "google.com", "linktr.ee",
    "shopmy.us", "shopltk.com", "liketk.it", "threads.com", "threads.net", "meta.ai", "meta.com", "cloudflare.com",
}

NON_PAGE_EXTERNAL_HOST_BLOCKLIST = {
    "cdninstagram.com",
    "fbcdn.net",
    "ytimg.com",
    "googlevideo.com",
    "ggpht.com",
    "googleusercontent.com",
    "storage.googleapis.com",
    "tiktokcdn.com",
    "byteimg.com",
    "pinimg.com",
    "fonts.googleapis.com",
    "fonts.gstatic.com",
    "gstatic.com",
    "starling.tiktokv.us",
    "lf16-config.tiktokcdn-us.com",
}

NON_PAGE_EXTERNAL_HOST_PREFIXES = (
    "scontent-",
    "static.",
    "image.",
    "images.",
    "video.",
    "videos.",
)

INSTAGRAM_EXTERNAL_MEETING_HOST_BLOCKLIST = {
    "bluejeans.com",
    "calendly.com",
    "zoom.us",
    "meet.google.com",
    "teams.microsoft.com",
    "discord.gg",
    "discord.com",
    "wa.me",
    "whatsapp.com",
    "telegram.me",
    "t.me",
}

INSTAGRAM_EXTERNAL_SHORTENER_HOSTS = {
    "tinyurl.com",
    "bit.ly",
    "buff.ly",
    "cutt.ly",
    "ow.ly",
    "shorturl.at",
    "rebrand.ly",
    "short.gy",
    "tg-need.com",
    "sn.tg-need.com",
    "stecu.short.gy",
    "lnk.bio",
    "beacons.ai",
    "stan.store",
    "hoo.be",
    "msha.ke",
    "bio.site",
}

INSTAGRAM_EXTERNAL_SPAM_HOSTS = {
    "vip788-hoki.org",
    "sinarabadi.xyz",
}

INSTAGRAM_EXTERNAL_SPAM_TOKENS = {
    "vip",
    "hoki",
    "slot",
    "slots",
    "casino",
    "bet",
    "bets",
    "bonus",
    "register",
    "signup",
    "login",
    "daftar",
    "judi",
    "togel",
    "gacor",
    "poker",
}

INSTAGRAM_EXTERNAL_SUSPICIOUS_TLDS = {
    "xyz",
    "top",
    "click",
    "rest",
    "sbs",
    "cfd",
    "quest",
}

INSTAGRAM_RECIPE_NEWSLETTER_TOKENS = {
    "recipe",
    "recipes",
    "cook",
    "cooking",
    "kitchen",
    "food",
}

INSTAGRAM_DOMAIN_AFFINITY_STOPWORDS = {
    "recipe", "recipes", "food", "blog", "site", "website", "official", "home", "link", "bio",
    "instagram", "facebook", "youtube", "tiktok", "channel", "creator", "kitchen", "cooking",
    "good", "easy", "full", "written", "page", "pages", "the", "and", "for", "with",
    "מתכון", "מתכונים", "בלוג", "אתר", "ביו", "קישור", "אינסטגרם", "פייסבוק", "יוטיוב",
}


INSTAGRAM_DISCOVERY_HINTS = [
    "link in bio", "bio", "blog", "website", "site", "story", "full recipe", "written recipe",
    "מתכון בבלוג", "לינק בביו", "בבלוג", "ביו", "באתר", "בסטורי", "לאתר", "בלוג", "אתר",
]

INSTAGRAM_SITE_POSITIVE_HINTS = [
    "recipe", "recipes", "blog", "food", "kitchen", "cook", "meal", "dish",
    "מתכון", "מתכונים", "בלוג", "מטבח", "ארטישוק", "ממולא",
]

INSTAGRAM_SITE_NEGATIVE_HINTS = [
    "privacy", "terms", "contact", "about", "policy", "cart", "checkout", "account", "login",
    "wp-login", "tag/", "author/", "feed", "category/", "product-category",
]

HEBREW_STOPWORDS = {
    "של", "עם", "גם", "אבל", "היא", "הוא", "אני", "אתם", "אתן", "שזה", "שאני", "בכל", "כמו",
    "הכל", "הזה", "זאת", "היום", "כאן", "לכם", "לכן", "המתכון", "מתכון", "מצרכים", "המצרכים",
    "הכנה", "היום", "אפשר", "טעים", "ממש", "עוד", "הרי", "ביו", "בלוג", "אתר",
}

ENGLISH_STOPWORDS = {
    "recipe", "recipes", "instagram", "follow", "message", "more", "show", "view", "profile",
    "link", "bio", "blog", "site", "website", "story", "with", "from", "this", "that", "your",
    "just", "have", "will", "into", "over", "then", "here", "there", "full", "written",
}


def is_generic_source_label(value: str) -> bool:
    normalized = normalize_text(value).lower()
    if not normalized:
        return True
    return normalized in GENERIC_SOURCE_LABELS


def strip_url_query_fragment(url: str) -> str:
    try:
        parsed = urlparse(str(url or ""))
        return f"{parsed.scheme}://{parsed.netloc}{parsed.path}" if parsed.scheme and parsed.netloc else str(url or "")
    except Exception:
        return str(url or "")


def urlsRoughlyEqual(a: str, b: str) -> bool:
    left = normalize_profile_url(a or "", a or "")
    right = normalize_profile_url(b or "", b or "")
    if not left or not right:
        return False

    def _normalize_compare_url(value: str) -> str:
        try:
            parsed = urlparse(value)
            query = parse_qs(parsed.query or "")
            filtered_query = []
            for key in sorted(query):
                if key.lower().startswith('utm_'):
                    continue
                filtered_query.append((key, tuple(v for v in query.get(key, []) if v)))
            return json.dumps({
                'scheme': (parsed.scheme or 'https').lower(),
                'host': canonical_domain(value),
                'path': decode_url_path(parsed.path or '').rstrip('/') or '/',
                'query': filtered_query,
            }, ensure_ascii=False, sort_keys=True)
        except Exception:
            return strip_url_query_fragment(str(value or '')).rstrip('/').lower()

    return _normalize_compare_url(left) == _normalize_compare_url(right)


def investigation_candidate_dedupe_key(url: str) -> str:
    normalized = normalize_profile_url(url or "", url or "")
    if not normalized:
        return ""
    try:
        parsed = urlparse(normalized)
        host = canonical_domain(normalized)
        path = decode_url_path(parsed.path or "").rstrip("/") or "/"
        return f"{host}{path}"
    except Exception:
        cleaned = strip_url_query_fragment(normalized).lower()
        return cleaned.replace("https://", "").replace("http://", "")


def decode_url_path(path: str) -> str:
    try:
        return unquote(str(path or "")).lower()
    except Exception:
        return str(path or "").lower()


def is_homepage_like_url(url: str) -> bool:
    normalized = normalize_profile_url(url or '')
    if not normalized:
        return False
    try:
        parsed = urlparse(normalized)
        path = decode_url_path(parsed.path or '').strip('/')
        return path == ''
    except Exception:
        return False


def page_contains_instagram_reference(linked_evidence: dict, original_target_url: str) -> bool:
    source = '\n'.join([
        str(linked_evidence.get('page_html') or ''),
        str(linked_evidence.get('raw_page_text') or ''),
        str(linked_evidence.get('structured_html_text') or ''),
        str(linked_evidence.get('effective_page_url') or ''),
    ]).lower()
    stripped_target = strip_url_query_fragment(original_target_url).lower()
    media_code = extract_instagram_media_code(original_target_url).lower()
    return (bool(stripped_target) and stripped_target in source) or (bool(media_code) and media_code in source)


def extract_instagram_media_code(url: str) -> str:
    try:
        parsed = urlparse(str(url or ""))
        parts = [part for part in (parsed.path or "").split("/") if part]
        if len(parts) >= 2 and parts[0].lower() in {"reel", "reels", "p", "tv"}:
            return parts[1]
    except Exception:
        pass
    return ""


def extract_instagram_mentions(text: str, max_handles: int = 12) -> list[str]:
    source = str(text or "")
    if not source:
        return []
    out = []
    seen = set()
    for match in re.finditer(r'@([A-Za-z0-9._]{2,30})', source):
        handle = source_safe_handle(match.group(1))
        if not handle:
            continue
        slug = handle.lstrip('@').lower()
        if slug in seen or slug in SOCIAL_PROFILE_RESERVED_PATHS:
            continue
        seen.add(slug)
        out.append(handle)
        if len(out) >= max_handles:
            break
    return out


def looks_like_instagram_discovery_hint(text: str) -> bool:
    lower = str(text or "").lower()
    if not lower:
        return False
    return any(hint in lower for hint in INSTAGRAM_DISCOVERY_HINTS)


def extract_longest_quoted_block(text: str, min_len: int = 80, max_len: int = 5000) -> str:
    source = normalize_text_preserve_lines(text)
    if not source:
        return ""

    best = ""
    for pattern in [
        re.compile(r'["“](.{20,5000}?)["”]', flags=re.DOTALL),
        re.compile(r'“(.{20,5000}?)”', flags=re.DOTALL),
    ]:
        for match in pattern.finditer(source):
            candidate = trim_text(normalize_text_preserve_lines(match.group(1)), max_len)
            if len(candidate) >= min_len and len(candidate) > len(best):
                best = candidate
    return best


def extract_instagram_caption_quote_block(text: str, min_len: int = 40, max_len: int = 5000) -> str:
    source = normalize_text_preserve_lines(text)
    if not source:
        return ""

    semitic_present = bool(re.search(r'[֐-׿؀-ۿ]', source))
    best = ""
    best_score = float('-inf')
    patterns = [
        re.compile(r'on\s+instagram[^\n\"“”]{0,120}[:：]\s*[\"“](.{20,5000}?)[\"”]', flags=re.IGNORECASE | re.DOTALL),
        re.compile(r'instagram[^\n\"“”]{0,120}[:：]\s*[\"“](.{20,5000}?)[\"”]', flags=re.IGNORECASE | re.DOTALL),
    ]
    noise_words = {
        'suns', 'window', 'season', 'choir', 'prayer', 'stroke', 'saved', 'version',
        'learn', 'olivia', 'dean', 'easy', 'love', 'sorry', 'parents', 'tired',
        'yes', 'saved', 'vibe', 'glow'
    }
    for pattern in patterns:
        for match in pattern.finditer(source):
            candidate = trim_text(normalize_text_preserve_lines(match.group(1)), max_len)
            if len(candidate) < min_len:
                continue
            metrics = evaluate_evidence_text(candidate)
            score = 0
            score += min(metrics.get('measurementSignalCount', 0), 10) * 30
            score += min(metrics.get('recipeVerbSignalCount', 0), 8) * 25
            score += 120 if metrics.get('hasFoodContext') else 0
            score += min(len(candidate), 600) / 20
            candidate_has_semitic = bool(re.search(r'[֐-׿؀-ۿ]', candidate))
            if semitic_present and candidate_has_semitic:
                score += 80
            if semitic_present and not candidate_has_semitic and not (
                metrics.get('hasFoodContext')
                or metrics.get('measurementSignalCount', 0) > 0
                or metrics.get('recipeVerbSignalCount', 0) > 0
            ):
                score -= 250
            lowered = candidate.lower()
            if any(word in lowered for word in noise_words):
                score -= 120
            if metrics.get('measurementSignalCount', 0) == 0 and metrics.get('recipeVerbSignalCount', 0) == 0 and not metrics.get('hasFoodContext'):
                score -= 80
            if score > best_score:
                best_score = score
                best = candidate
    if best_score < 40:
        return ""
    return best


def extract_instagram_reliable_caption_text(text: str) -> str:
    source = normalize_text_preserve_lines(text)
    if not source:
        return ""

    caption_quote = extract_instagram_caption_quote_block(source)
    if caption_quote:
        return caption_quote

    longest_quote = normalize_text_preserve_lines(extract_longest_quoted_block(source) or "")
    sanitized = normalize_text_preserve_lines(sanitize_instagram_visible_text_for_caption_fallback(source) or "")

    if longest_quote:
        quote_metrics = evaluate_evidence_text(longest_quote)
        if (
            quote_metrics.get("hasFoodContext")
            or quote_metrics.get("measurementSignalCount", 0) > 0
            or quote_metrics.get("recipeVerbSignalCount", 0) > 0
        ):
            return longest_quote

    if sanitized:
        return sanitized

    return longest_quote or source


def extract_instagram_meta_caption_text(meta_description: str) -> str:
    source = normalize_text_preserve_lines(meta_description)
    if not source:
        return ""

    candidate = extract_instagram_reliable_caption_text(source)
    if candidate:
        return trim_text(candidate, EXPANDED_CAPTION_SUBMIT_MAX)

    quoted = extract_longest_quoted_block(source, min_len=20, max_len=EXPANDED_CAPTION_SUBMIT_MAX)
    if quoted:
        return trim_text(normalize_text_preserve_lines(quoted), EXPANDED_CAPTION_SUBMIT_MAX)

    colon_match = re.search(r':\s*(.+)$', source, flags=re.DOTALL)
    if colon_match and colon_match.group(1):
        return trim_text(normalize_text_preserve_lines(colon_match.group(1)), EXPANDED_CAPTION_SUBMIT_MAX)

    return trim_text(source, EXPANDED_CAPTION_SUBMIT_MAX)


def detect_instagram_feed_contamination(evidence: dict, source_metadata: dict | None = None) -> dict:
    payload = evidence or {}
    metadata = source_metadata or {}

    raw_text = normalize_text_preserve_lines(payload.get("raw_page_text") or "")
    expanded_text = normalize_text_preserve_lines(payload.get("expanded_caption_text") or "")
    visible_after = normalize_text_preserve_lines(payload.get("visible_text_after_expand") or "")
    meta_description = normalize_text_preserve_lines(payload.get("meta_description") or "")
    combined = combine_text_blocks([raw_text, expanded_text, visible_after])

    target_handle = normalize_text(metadata.get("source_creator_handle") or "").lstrip('@').lower()
    handle_candidates = []
    seen_handles = set()
    audio_line_count = 0
    numeric_row_count = 0

    for line in combined.split("\n"):
        normalized_line = normalize_text(line)
        lower = normalized_line.lower()
        if not normalized_line:
            continue
        if re.fullmatch(r'[0-9][0-9.,kmbKM]*', normalized_line):
            numeric_row_count += 1
            continue
        if ' · ' in normalized_line and len(normalized_line) <= 120:
            audio_line_count += 1
        if re.fullmatch(r'[A-Za-z0-9._]{3,30}', normalized_line):
            if lower in GENERIC_SOURCE_LABELS:
                continue
            if lower == target_handle:
                continue
            if lower in {'follow', 'following', 'message', 'messages', 'instagram', 'meta'}:
                continue
            if lower not in seen_handles:
                seen_handles.add(lower)
                handle_candidates.append(normalized_line)

    score = 0
    reasons = []
    error_banner = 'trouble playing this video' in combined.lower()
    if error_banner:
        score += 2
        reasons.append('video_error_banner')

    messages_ui = 'messages' in raw_text.lower() or 'messages' in expanded_text.lower()
    if messages_ui:
        score += 1
        reasons.append('messages_ui')

    if len(handle_candidates) >= 2:
        score += 2
        reasons.append('multiple_creator_handles')
    elif len(handle_candidates) == 1:
        score += 1
        reasons.append('one_other_creator_handle')

    if audio_line_count >= 2:
        score += 1
        reasons.append('multiple_audio_lines')

    if numeric_row_count >= 6:
        score += 1
        reasons.append('feed_engagement_rows')

    clean_caption = extract_instagram_meta_caption_text(meta_description)
    if clean_caption and combined and len(combined) > max(len(clean_caption) * 2, 500) and (len(handle_candidates) >= 1 or audio_line_count >= 1):
        score += 1
        reasons.append('combined_text_much_noisier_than_meta_caption')

    return {
        'recommended': score >= 3 and bool(clean_caption),
        'score': score,
        'reasons': reasons,
        'error_banner': error_banner,
        'messages_ui': messages_ui,
        'other_handle_count': len(handle_candidates),
        'other_handles': handle_candidates[:8],
        'audio_line_count': audio_line_count,
        'numeric_row_count': numeric_row_count,
        'clean_meta_caption': clean_caption,
        'meta_description_length': len(meta_description),
        'combined_length': len(combined),
    }


def maybe_apply_instagram_contamination_guard(
    evidence: dict,
    source_metadata: dict | None = None,
    *,
    linked_recipe_used: bool = False,
    effective_analysis_platform: str = '',
) -> tuple[dict, dict]:
    payload = dict(evidence or {})
    metadata = source_metadata or {}

    if linked_recipe_used or normalize_text(effective_analysis_platform).lower() not in {'', 'instagram'}:
        return payload, {'applied': False, 'reason': 'not_pure_instagram'}

    contamination = detect_instagram_feed_contamination(payload, metadata)
    if not contamination.get('recommended'):
        contamination.update({'applied': False, 'reason': 'not_recommended'})
        return payload, contamination

    clean_caption = normalize_text_preserve_lines(contamination.get('clean_meta_caption') or '')
    if not clean_caption:
        contamination.update({'applied': False, 'reason': 'no_clean_meta_caption'})
        return payload, contamination

    creator_label = choose_first_non_empty(
        metadata.get('source_creator_name'),
        metadata.get('source_channel_name'),
        metadata.get('source_creator_handle'),
    )
    clean_page_title = choose_first_non_empty(
        metadata.get('source_creator_name'),
        metadata.get('source_creator_handle'),
        payload.get('page_title'),
    )

    payload['page_title'] = trim_text(clean_page_title or payload.get('page_title') or '', PAGE_TITLE_SUBMIT_MAX)
    payload['expanded_caption_text'] = trim_text(clean_caption, EXPANDED_CAPTION_SUBMIT_MAX)
    payload['visible_text_after_expand'] = trim_text(clean_caption, VISIBLE_TEXT_AFTER_SUBMIT_MAX)
    payload['visible_text_before_expand'] = trim_text(clean_caption, VISIBLE_TEXT_BEFORE_SUBMIT_MAX)
    payload['meta_description'] = trim_text(clean_caption, META_DESCRIPTION_SUBMIT_MAX)
    payload['raw_page_text'] = trim_text(
        combine_text_blocks([
            creator_label,
            clean_caption,
        ]),
        RAW_PAGE_TEXT_SUBMIT_MAX,
    )

    contamination.update({
        'applied': True,
        'reason': 'meta_caption_anchor_only',
        'guard_page_title': payload.get('page_title') or '',
        'guard_raw_page_text_length': len(payload.get('raw_page_text') or ''),
        'guard_caption_length': len(clean_caption),
    })
    return payload, contamination


def build_instagram_discovery_query_text(evidence: dict) -> str:
    meta = normalize_text_preserve_lines(evidence.get("meta_description") or "")
    expanded_caption = normalize_text_preserve_lines(evidence.get("expanded_caption_text") or "")
    visible_after_expand = normalize_text_preserve_lines(evidence.get("visible_text_after_expand") or "")
    raw_text = normalize_text_preserve_lines(evidence.get("raw_page_text") or "")

    preferred_caption_text = combine_text_blocks([
        expanded_caption,
        visible_after_expand,
        meta,
    ])
    reliable_caption = extract_instagram_reliable_caption_text(preferred_caption_text)
    primary_query_text = combine_text_blocks([
        reliable_caption,
        meta,
        expanded_caption,
        visible_after_expand,
    ])

    primary_metrics = evaluate_evidence_text(primary_query_text[:8000])
    if (
        primary_query_text
        and (
            primary_metrics.get("hasFoodContext")
            or primary_metrics.get("measurementSignalCount", 0) > 0
            or primary_metrics.get("recipeVerbSignalCount", 0) > 0
            or len(primary_query_text) >= 180
        )
    ):
        return primary_query_text

    return combine_text_blocks([
        primary_query_text,
        raw_text,
    ])


def clean_instagram_display_name(value: str) -> str:
    text = normalize_text_preserve_lines(value)
    if not text:
        return ""
    text = re.sub(r'^.*?see instagram photos and videos from\s+', '', text, flags=re.IGNORECASE)
    text = re.sub(r'^.*?view the profile of\s+', '', text, flags=re.IGNORECASE)
    text = re.sub(r'^\s*[\d.,kmbKM]+\s+followers?,\s*[\d.,kmbKM]+\s+following?,\s*[\d.,kmbKM]+\s+posts?\s*[-–—]\s*', '', text, flags=re.IGNORECASE)
    text = re.sub(r'\s*[•·\-–—]\s*instagram.*$', '', text, flags=re.IGNORECASE)
    text = re.sub(r'\bon instagram\b.*$', '', text, flags=re.IGNORECASE)
    text = re.sub(r'^[\s\-–—•·"“”]+|[\s\-–—•·"“”]+$', '', text)
    cleaned = source_safe_text(text, 160)
    if is_generic_source_label(cleaned):
        return ""
    return cleaned


def looks_like_instagram_avatar_url(url: str) -> bool:
    lower = str(url or '').lower()
    if not lower:
        return False
    if 't51.71878-15' in lower:
        return False
    return 'profile_pic' in lower or 't51.2885-19' in lower or 'scontent' in lower


def looks_like_youtube_avatar_url(url: str) -> bool:
    lower = str(url or '').lower()
    if not lower:
        return False
    return 'yt3.ggpht.com' in lower or 'ggpht.com' in lower or 'googleusercontent.com' in lower


async def get_instagram_avatar_url_from_page(page, base_url: str = '', handle: str = '') -> str:
    handle_value = source_safe_handle(handle).lstrip('@').lower()
    try:
        candidates = await page.evaluate(
            """
            (handle) => {
              const out = [];
              const push = (value) => {
                if (!value || typeof value !== 'string') return;
                const trimmed = value.trim();
                if (!trimmed) return;
                if (!out.includes(trimmed)) out.push(trimmed);
              };
              const handleNeedle = handle ? `/${handle}/` : '';
              for (const anchor of Array.from(document.querySelectorAll('a[href]'))) {
                const href = (anchor.href || anchor.getAttribute('href') || '').toLowerCase();
                if (!handleNeedle || href.includes(handleNeedle)) {
                  const img = anchor.querySelector('img');
                  if (img) {
                    push(img.currentSrc || img.src || img.getAttribute('src') || '');
                    push(img.getAttribute('srcset') || '');
                  }
                }
              }
              for (const selector of ['article header img', 'header img', 'main header img']) {
                for (const img of Array.from(document.querySelectorAll(selector)).slice(0, 6)) {
                  push(img.currentSrc || img.src || img.getAttribute('src') || '');
                  push(img.getAttribute('srcset') || '');
                }
              }
              return out;
            }
            """,
            handle_value,
        )
    except Exception:
        candidates = []

    normalized_candidates = []
    seen = set()
    for raw in candidates or []:
        candidate = str(raw or '').strip()
        if ',' in candidate and ' ' in candidate:
            candidate = candidate.split(',')[0].strip().split(' ')[0].strip()
        normalized = normalize_profile_url(candidate, base_url)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        normalized_candidates.append(normalized)

    preferred = [url for url in normalized_candidates if looks_like_instagram_avatar_url(url)]
    if preferred:
        strong = [url for url in preferred if 't51.2885-19' in url.lower() or 'profile_pic' in url.lower()]
        return (strong[0] if strong else preferred[0])
    return ''


async def get_youtube_avatar_url_from_page(page, base_url: str = 'https://www.youtube.com') -> str:
    selectors = [
        '#owner img',
        'ytd-watch-metadata #owner img',
        'ytd-video-owner-renderer img',
        'ytd-channel-name img',
        'yt-img-shadow img',
    ]
    candidates = []
    seen = set()
    for selector in selectors:
        try:
            locator = page.locator(selector)
            count = min(await locator.count(), 8)
            for idx in range(count):
                value = ''
                try:
                    node = locator.nth(idx)
                    value = await node.get_attribute('src', timeout=600) or await node.get_attribute('srcset', timeout=600) or ''
                except Exception:
                    value = ''
                candidate = str(value or '').strip()
                if ',' in candidate and ' ' in candidate:
                    candidate = candidate.split(',')[0].strip().split(' ')[0].strip()
                normalized = normalize_profile_url(candidate, base_url)
                if not normalized or normalized in seen:
                    continue
                seen.add(normalized)
                candidates.append(normalized)
        except Exception:
            continue
    preferred = [url for url in candidates if looks_like_youtube_avatar_url(url)]
    if preferred:
        return preferred[0]
    return ''


def extract_instagram_owner_hint(text: str) -> dict:
    source = normalize_text_preserve_lines(text)
    if not source:
        return {"handle": "", "display_name": ""}

    candidates = []
    rich_patterns = [
        (re.compile(r'([^\n"]{2,140})\s*\(@([A-Za-z0-9._]{2,30})\)\s*[•·\-–—]?\s*Instagram', flags=re.IGNORECASE), 320),
        (re.compile(r'([^\n"]{2,140})\s*\(@([A-Za-z0-9._]{2,30})\)', flags=re.IGNORECASE), 220),
        (re.compile(r'likes?,?.{0,120}?[-–—]\s*([A-Za-z0-9._]{2,30})\s+on\b', flags=re.IGNORECASE | re.DOTALL), 260),
        (re.compile(r'comments?\s*[-–—]\s*([A-Za-z0-9._]{2,30})\s+on\b', flags=re.IGNORECASE), 240),
    ]

    for pattern, base_score in rich_patterns:
        for match in pattern.finditer(source):
            groups = match.groups()
            if len(groups) == 2:
                display_name = clean_instagram_display_name(groups[0])
                handle = source_safe_handle(groups[1])
            else:
                display_name = ""
                handle = source_safe_handle(groups[0])

            if not handle:
                continue

            score = base_score + (20 if display_name else 0)
            context = source[max(0, match.start() - 40): match.end() + 40].lower()
            if 'instagram reel' in context or 'on instagram' in context:
                score += 40

            candidates.append({
                "handle": handle,
                "display_name": display_name,
                "score": score,
            })

    if not candidates:
        return {"handle": "", "display_name": ""}

    best = max(candidates, key=lambda item: item["score"])
    return {
        "handle": best.get("handle") or "",
        "display_name": best.get("display_name") or "",
    }


def should_prefer_semitic_tokens(source: str, override=None) -> bool:
    if override is not None:
        return bool(override)

    text = normalize_text_preserve_lines(source)
    if not text:
        return False

    semitic_tokens = re.findall(r'[֐-׿؀-ۿ]{2,}', text)
    latin_tokens = re.findall(r'[A-Za-z]{3,}', text)

    if not semitic_tokens:
        return False
    if not latin_tokens:
        return True

    return len(semitic_tokens) >= max(4, int(len(latin_tokens) * 1.5))


def normalize_instagram_profile_root(url: str) -> str:
    normalized = normalize_profile_url(url or '', 'https://www.instagram.com')
    if not normalized:
        return ''
    try:
        parsed = urlparse(normalized)
        host = canonical_domain(normalized)
        if host != 'instagram.com':
            return normalized
        parts = [part for part in (parsed.path or '').split('/') if part]
        if not parts:
            return normalized
        handle = parts[0]
        if handle.lower() in SOCIAL_PROFILE_RESERVED_PATHS:
            return normalized
        return f'https://www.instagram.com/{handle}/'
    except Exception:
        return normalized


def sanitize_instagram_visible_text_for_caption_fallback(text: str) -> str:
    cleaned_lines = []
    seen = set()
    for line in normalize_text_preserve_lines(text).split('\n'):
        normalized_line = normalize_text(line)
        if not normalized_line:
            continue
        lower = normalized_line.lower()
        if lower in GENERIC_SOURCE_LABELS:
            continue
        if lower.startswith('about ') or lower.startswith('privacy ') or lower.startswith('terms '):
            continue
        if 'trouble playing this video' in lower:
            continue
        if lower in {"open app", "instagram", "meta", "likes", "comments"}:
            continue
        if re.fullmatch(r'[\d.,kmb]+', lower):
            continue
        if re.fullmatch(r'\d+\s+people', lower):
            continue
        if re.fullmatch(r'@?[a-z0-9._]{2,30}', lower):
            continue
        key = lower
        if key in seen:
            continue
        seen.add(key)
        cleaned_lines.append(normalized_line)
    return '\n'.join(cleaned_lines)


def extract_instagram_hint_tokens(text: str, max_tokens: int = 12) -> list[str]:
    runtime_config = get_instagram_external_site_runtime_rules()
    runtime_rules = runtime_config.get("rules") if isinstance(runtime_config, dict) else {}
    source = apply_runtime_noise_phrases(extract_instagram_reliable_caption_text(text), runtime_rules)
    if not source:
        return []

    prefer_semitic_override = runtime_rules.get("prefer_semitic_tokens") if isinstance(runtime_rules, dict) else None
    prefer_semitic_tokens = should_prefer_semitic_tokens(source, prefer_semitic_override)
    filtered_lines = []
    for line in source.split('\n'):
        normalized_line = normalize_text(line)
        lower = normalized_line.lower()
        if not normalized_line:
            continue
        if 'trouble playing this video' in lower:
            continue
        if lower in {'likes', 'comments', 'instagram reel'}:
            continue
        has_hebrew_or_arabic = bool(re.search(r'[\u0590-\u05FF\u0600-\u06FF]', normalized_line))
        line_metrics = evaluate_evidence_text(normalized_line)
        if re.search(r'[A-Za-z]', normalized_line) and not has_hebrew_or_arabic and not (
            line_metrics.get('hasFoodContext')
            or line_metrics.get('measurementSignalCount', 0) > 0
            or line_metrics.get('recipeVerbSignalCount', 0) > 0
        ):
            continue
        filtered_lines.append(normalized_line)
    source = '\n'.join(filtered_lines)

    tokens = []
    seen = set()
    token_pattern = re.compile(r'[A-Za-z\u0590-\u05FF\u0600-\u06FF][A-Za-z0-9_\-\u0590-\u05FF\u0600-\u06FF]{2,}')
    hard_stopwords = {
        'sorry', 'having', 'trouble', 'playing', 'video', 'likes', 'comments',
        'instagram', 'reel', 'people', 'shared', 'post', 'april',
        'suns', 'window', 'season', 'choir', 'prayer', 'stroke', 'saved', 'version',
        'learn', 'olivia', 'dean', 'easy', 'love', 'yes', 'parents', 'tired',
    }
    hard_stopwords.update(token.lower() for token in runtime_string_list(runtime_rules, 'token_blacklist'))

    for match in token_pattern.finditer(source):
        token = match.group(0).strip('._-').lower()
        if not token or token.startswith('@'):
            continue
        if prefer_semitic_tokens and re.fullmatch(r'[A-Za-z0-9_\-]+', token):
            continue
        if token in ENGLISH_STOPWORDS or token in HEBREW_STOPWORDS or token in hard_stopwords:
            continue
        if token.isdigit() or len(token) < 3:
            continue
        if '_' in token and re.fullmatch(r'[a-z0-9_]+', token):
            continue
        if token in seen:
            continue
        seen.add(token)
        tokens.append(token)
        if len(tokens) >= max_tokens:
            break
    return tokens


def extract_google_recipe_query(text: str) -> str:
    source = normalize_text_preserve_lines(text)
    if not source:
        return ""

    patterns = [
        re.compile(r"\bg(?:oogle|00gle)\b\s*[:;,]?\s*([^\n\r]+)", flags=re.IGNORECASE),
    ]
    stop_tokens = ['http://', 'https://', '#', '…', '… more']
    trailing_patterns = [
        re.compile(r'\band\s+my\s+recipes?\b.*$', flags=re.IGNORECASE),
        re.compile(r'\band\s+my\s+recipe\b.*$', flags=re.IGNORECASE),
        re.compile(r'\band\s+the\s+recipes?\b.*$', flags=re.IGNORECASE),
        re.compile(r'\bor\s+search\b.*$', flags=re.IGNORECASE),
        re.compile(r'\bwill\s+appear\b.*$', flags=re.IGNORECASE),
        re.compile(r'\bat\s+the\s+top\b.*$', flags=re.IGNORECASE),
        re.compile(r'\bfor\s+the\s+recipe\b.*$', flags=re.IGNORECASE),
    ]

    for pattern in patterns:
        for match in pattern.finditer(source):
            candidate = normalize_text(match.group(1) or '')
            if not candidate:
                continue

            quoted = re.search(r'["“`\'](.+?)["”\']', candidate)
            if quoted and quoted.group(1):
                candidate = quoted.group(1)

            for token in stop_tokens:
                if token in candidate:
                    candidate = candidate.split(token, 1)[0]
            for trailing_pattern in trailing_patterns:
                candidate = trailing_pattern.sub('', candidate)

            candidate = re.split(r'[|•\n\r]', candidate)[0]
            candidate = re.sub(r'^["“”\':,;\-\s]+', '', candidate)
            candidate = re.sub(r'[💛❤️♥️🩷💙💚🧡💜✨🥖👀🥺😘🤍]+.*$', '', candidate).strip()
            candidate = normalize_text(candidate)
            if len(candidate) < 6:
                continue
            return candidate
    return ""

def extract_instagram_primary_query_info(text: str) -> dict:
    runtime_config = get_instagram_external_site_runtime_rules()
    runtime_rules = runtime_config.get("rules") if isinstance(runtime_config, dict) else {}
    forced_query_phrases = runtime_string_list(runtime_rules, 'forced_query_phrases')
    if forced_query_phrases:
        primary_phrase = normalize_text(forced_query_phrases[0])
        extended_phrase = normalize_text(forced_query_phrases[1] if len(forced_query_phrases) > 1 else forced_query_phrases[0])
        core_tokens = extract_match_tokens(extended_phrase or primary_phrase)[:4]
        return {"primary_phrase": primary_phrase, "extended_phrase": extended_phrase, "core_tokens": core_tokens}

    google_query = extract_google_recipe_query(text)
    if google_query:
        primary_phrase = normalize_text(google_query)
        extended_phrase = primary_phrase
        core_tokens = extract_match_tokens(primary_phrase)[:4]
        return {"primary_phrase": primary_phrase, "extended_phrase": extended_phrase, "core_tokens": core_tokens}

    source = apply_runtime_noise_phrases(extract_instagram_reliable_caption_text(text), runtime_rules)
    if not source:
        return {"primary_phrase": "", "extended_phrase": "", "core_tokens": []}

    prefer_semitic_override = runtime_rules.get("prefer_semitic_tokens") if isinstance(runtime_rules, dict) else None
    prefer_semitic_tokens = should_prefer_semitic_tokens(source, prefer_semitic_override)
    token_pattern = re.compile(r'[A-Za-z֐-׿؀-ۿ][A-Za-z0-9_\-֐-׿؀-ۿ]{1,}')
    query_stopwords = {
        'day', 'days', 'episode', 'episodes', 'part', 'parts', 'pt', 'full', 'recipe', 'recipes',
        'website', 'site', 'blog', 'link', 'bio', 'google', 'g00gle', 'comment', 'comments',
        'high', 'low', 'protein', 'calorie', 'calories', 'food', 'meal', 'meals', 'written',
    }
    query_stopwords.update(item.lower() for item in runtime_string_list(runtime_rules, 'token_blacklist'))

    for raw_line in source.split('\n'):
        line = normalize_text(raw_line)
        if not line:
            continue
        line = re.sub(r'@[A-Za-z0-9._]{2,30}', ' ', line)
        line = re.sub(r'#', ' ', line)
        line = re.sub(r'^\s*(?:day|episode|part|pt)\s*\d+\s*[:\-–—]\s*', '', line, flags=re.IGNORECASE)
        line = re.split(r'[!?…]', line)[0]
        line = normalize_text(line)
        if not line:
            continue
        has_hebrew_or_arabic = bool(re.search(r'[\u0590-\u05FF\u0600-\u06FF]', line))
        line_metrics = evaluate_evidence_text(line)
        if re.search(r'[A-Za-z]', line) and not has_hebrew_or_arabic and not (
            line_metrics.get('hasFoodContext')
            or line_metrics.get('measurementSignalCount', 0) > 0
            or line_metrics.get('recipeVerbSignalCount', 0) > 0
            or len(line) >= 18
        ):
            continue

        tokens = []
        seen = set()
        for token_match in token_pattern.finditer(line):
            token = token_match.group(0).strip('._-').lower()
            if prefer_semitic_tokens and re.fullmatch(r'[A-Za-z0-9_\-]+', token):
                continue
            if not token or token in ENGLISH_STOPWORDS or token in HEBREW_STOPWORDS:
                continue
            if token in query_stopwords:
                continue
            if token in {'הכי', 'הייתי', 'חייבת', 'לצלם', 'אימוש', 'שלי', 'שלה', 'נסעתי', 'במיוחד', 'מלא', 'מחכה', 'קישור'}:
                continue
            if token in seen:
                continue
            seen.add(token)
            tokens.append(token)
            if len(tokens) >= 5:
                break

        if len(tokens) >= 2:
            primary_phrase = ' '.join(tokens[:2])
            extended_phrase = ' '.join(tokens[:3]) if len(tokens) >= 3 else primary_phrase
            return {
                "primary_phrase": primary_phrase,
                "extended_phrase": extended_phrase,
                "core_tokens": tokens[:4],
            }

    fallback_tokens = [
        token for token in extract_instagram_hint_tokens(source, max_tokens=6)
        if token not in query_stopwords and token not in {'day', 'days', 'comment', 'comments'}
    ]
    if len(fallback_tokens) >= 2:
        primary_phrase = ' '.join(fallback_tokens[:2])
        extended_phrase = ' '.join(fallback_tokens[:3]) if len(fallback_tokens) >= 3 else primary_phrase
        return {
            "primary_phrase": primary_phrase,
            "extended_phrase": extended_phrase,
            "core_tokens": fallback_tokens[:4],
        }

    return {"primary_phrase": "", "extended_phrase": "", "core_tokens": []}

def build_instagram_site_search_requests(site_url: str, query_info: dict) -> list[dict]:
    normalized_site_url = normalize_profile_url(site_url or '')
    if not normalized_site_url:
        return []
    parsed = urlparse(normalized_site_url)
    root = f"{parsed.scheme}://{parsed.netloc}/" if parsed.scheme and parsed.netloc else normalized_site_url

    runtime_rules = get_instagram_external_site_runtime_rules().get('rules') or {}

    requests = []
    seen = set()
    ordered_phrases = runtime_string_list(runtime_rules, 'forced_query_phrases') + [
        query_info.get('extended_phrase') or '',
        query_info.get('primary_phrase') or '',
    ]
    for phrase in ordered_phrases:
        phrase = normalize_text(phrase)
        if not phrase:
            continue
        search_url = f"{root}?s={quote_plus(phrase)}"
        key = search_url.lower()
        if key in seen:
            continue
        seen.add(key)
        requests.append({"query": phrase, "url": search_url})
    return requests



def extract_anchor_like_items_from_html(raw_html: str, base_url: str = '', limit: int = 240) -> list[dict]:
    source = str(raw_html or '')
    if not source:
        return []

    items: list[dict] = []
    seen: set[str] = set()

    def add_item(raw_url: str, text: str = '') -> None:
        normalized_url = normalize_profile_url(raw_url or '', base_url or raw_url or '')
        if not normalized_url:
            return
        dedupe_key = investigation_candidate_dedupe_key(normalized_url) or normalized_url.lower()
        if dedupe_key in seen:
            return
        seen.add(dedupe_key)
        items.append({'href': normalized_url, 'text': source_safe_text(text or '', 240)})
        if len(items) >= limit:
            return

    anchor_pattern = re.compile(r'<a\b[^>]*href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', re.IGNORECASE | re.DOTALL)
    tag_strip = re.compile(r'<[^>]+>')
    for match in anchor_pattern.finditer(source):
        raw_href = html_lib.unescape(match.group(1) or '').strip()
        inner_html = match.group(2) or ''
        inner_text = normalize_text_preserve_lines(html_lib.unescape(tag_strip.sub(' ', inner_html)))
        add_item(raw_href, inner_text)
        if len(items) >= limit:
            break

    if len(items) < limit:
        for extracted in extract_urls_with_context(decode_htmlish(source)):
            add_item(extracted.get('url') or '', extracted.get('context') or '')
            if len(items) >= limit:
                break

    return items


def normalize_recipe_title_for_match(value: str) -> str:
    text = normalize_text(value or '').lower()
    if not text:
        return ''
    parts = [
        part.strip()
        for part in re.split(r'\s+[-–—]\s+|\s*\|\s*', text)
        if part.strip()
    ]
    return parts[0] if parts else text


def extract_match_tokens(value: str) -> list[str]:
    normalized = normalize_recipe_title_for_match(value)
    if not normalized:
        return []
    token_pattern = re.compile(r'[A-Za-z\u0590-\u05FF\u0600-\u06FF][A-Za-z0-9_\-\u0590-\u05FF\u0600-\u06FF]{1,}')
    tokens = []
    seen = set()
    for match in token_pattern.finditer(normalized):
        token = match.group(0).strip('._-').lower()
        if not token:
            continue
        if token in ENGLISH_STOPWORDS or token in HEBREW_STOPWORDS:
            continue
        if token in seen:
            continue
        seen.add(token)
        tokens.append(token)
    return tokens


def compare_query_to_candidate_phrase(query_phrase: str, candidate_phrase: str) -> dict:
    query_tokens = extract_match_tokens(query_phrase)
    candidate_tokens = extract_match_tokens(candidate_phrase)
    result = {
        'query_tokens': query_tokens,
        'candidate_tokens': candidate_tokens,
        'exact': False,
        'prefix': False,
        'extra_tokens': [],
        'overlap': 0,
    }
    if not query_tokens or not candidate_tokens:
        return result

    result['exact'] = candidate_tokens == query_tokens
    result['prefix'] = candidate_tokens[:len(query_tokens)] == query_tokens
    result['overlap'] = len([token for token in query_tokens if token in candidate_tokens])
    if result['prefix'] and len(candidate_tokens) > len(query_tokens):
        result['extra_tokens'] = candidate_tokens[len(query_tokens):]
    return result


def extract_url_path_phrase(url: str) -> str:
    try:
        parsed = urlparse(str(url or ''))
        parts = [part for part in decode_url_path(parsed.path or '').split('/') if part]
        if not parts:
            return ''
        last = parts[-1].replace('-', ' ')
        return normalize_text(last)
    except Exception:
        return ''


def build_query_match_debug(query_phrase: str, candidate_phrase: str, candidate_url: str = '') -> dict:
    title_match = compare_query_to_candidate_phrase(query_phrase, candidate_phrase)
    slug_phrase = extract_url_path_phrase(candidate_url)
    slug_match = compare_query_to_candidate_phrase(query_phrase, slug_phrase)
    return {
        'title_phrase': normalize_recipe_title_for_match(candidate_phrase),
        'title_exact': title_match['exact'],
        'title_prefix': title_match['prefix'],
        'title_extra_tokens': title_match['extra_tokens'],
        'title_overlap': title_match['overlap'],
        'slug_phrase': normalize_recipe_title_for_match(slug_phrase),
        'slug_exact': slug_match['exact'],
        'slug_prefix': slug_match['prefix'],
        'slug_extra_tokens': slug_match['extra_tokens'],
        'slug_overlap': slug_match['overlap'],
    }


TIKTOK_TITLE_ALIGNMENT_GENERIC_TOKENS = {
    'recipe', 'recipes', 'full', 'written', 'easy', 'food', 'dish', 'meal', 'stew',
    'מתכון', 'מתכונים', 'המתכון', 'תבשיל', 'מנה', 'מנות', 'אוכל', 'ארוחה',
}
TIKTOK_HEBREW_PREFIX_CHARS = 'ובה'


def normalize_tiktok_alignment_token(token: str) -> str:
    value = normalize_text(token or '').lower().strip('._- ')
    if not value:
        return ''
    if re.fullmatch(r'[֐-׿]+', value):
        while len(value) >= 4 and value[0] in TIKTOK_HEBREW_PREFIX_CHARS and value[1:2] not in TIKTOK_HEBREW_PREFIX_CHARS:
            candidate = value[1:]
            if len(candidate) < 3:
                break
            value = candidate
    return value


def extract_tiktok_alignment_tokens(value: str) -> list[str]:
    normalized = normalize_recipe_title_for_match(value)
    if not normalized:
        return []
    token_pattern = re.compile(r'[A-Za-z֐-׿؀-ۿ][A-Za-z0-9_\-֐-׿؀-ۿ]{1,}')
    out = []
    seen = set()
    for match in token_pattern.finditer(normalized):
        raw_token = match.group(0)
        for token_part in re.split(r'[_\-]+', raw_token):
            token = normalize_tiktok_alignment_token(token_part)
            if not token or token in ENGLISH_STOPWORDS or token in HEBREW_STOPWORDS:
                continue
            if token in seen:
                continue
            seen.add(token)
            out.append(token)
    return out


def select_tiktok_alignment_query_tokens(query_tokens: list[str]) -> list[str]:
    non_generic = [token for token in (query_tokens or []) if token not in TIKTOK_TITLE_ALIGNMENT_GENERIC_TOKENS]
    if len(non_generic) >= 2:
        return non_generic
    return list(query_tokens or [])


def evaluate_tiktok_same_host_query_match(query_phrase: str, candidate_title: str, candidate_url: str = '') -> dict:
    query = normalize_text_preserve_lines(query_phrase or '')
    if not query:
        return {
            'query_phrase': '',
            'query_tokens': [],
            'query_token_count': 0,
            'query_ignored_generic_tokens': [],
            'query_match_allowed': True,
            'query_match_exact': False,
            'query_match_prefix': False,
            'query_match_overlap': 0,
            'query_match_overlap_ratio': 0.0,
            'query_match_missing_tokens': [],
            'query_match_guard_reasons': ['no_query_tokens'],
            'query_match_strict_guard': False,
            'title_phrase': normalize_recipe_title_for_match(candidate_title or ''),
            'title_tokens': extract_tiktok_alignment_tokens(candidate_title or '')[:12],
            'title_exact': False,
            'title_prefix': False,
            'title_extra_tokens': [],
            'title_overlap': 0,
            'slug_phrase': normalize_recipe_title_for_match(extract_url_path_phrase(candidate_url or '')),
            'slug_tokens': extract_tiktok_alignment_tokens(extract_url_path_phrase(candidate_url or ''))[:12],
            'slug_exact': False,
            'slug_prefix': False,
            'slug_extra_tokens': [],
            'slug_overlap': 0,
        }

    raw_query_tokens = extract_tiktok_alignment_tokens(query)
    query_tokens = select_tiktok_alignment_query_tokens(raw_query_tokens)
    ignored_tokens = [token for token in raw_query_tokens if token not in query_tokens]
    title_phrase = normalize_recipe_title_for_match(candidate_title or '')
    slug_phrase = normalize_recipe_title_for_match(extract_url_path_phrase(candidate_url or ''))
    title_tokens = extract_tiktok_alignment_tokens(title_phrase)
    slug_tokens = extract_tiktok_alignment_tokens(slug_phrase)

    title_exact = bool(query_tokens and title_tokens == query_tokens)
    slug_exact = bool(query_tokens and slug_tokens == query_tokens)
    title_prefix = bool(query_tokens and title_tokens[:len(query_tokens)] == query_tokens)
    slug_prefix = bool(query_tokens and slug_tokens[:len(query_tokens)] == query_tokens)
    title_overlap_tokens = [token for token in query_tokens if token in title_tokens]
    slug_overlap_tokens = [token for token in query_tokens if token in slug_tokens]
    title_overlap = len(title_overlap_tokens)
    slug_overlap = len(slug_overlap_tokens)
    best_overlap = max(title_overlap, slug_overlap)
    overlap_ratio = (best_overlap / max(len(query_tokens), 1)) if query_tokens else 0.0
    exact = title_exact or slug_exact
    prefix = title_prefix or slug_prefix
    missing_tokens = [token for token in query_tokens if token not in (title_tokens if title_overlap >= slug_overlap else slug_tokens)]

    allowed = False
    guard_reasons = []
    if not query_tokens:
        allowed = True
        guard_reasons.append('no_query_tokens')
    elif exact:
        allowed = True
        guard_reasons.append('exact_title_or_slug')
    elif prefix and best_overlap >= max(2, len(query_tokens) - 1):
        allowed = True
        guard_reasons.append('prefix_title_or_slug')
    else:
        query_count = len(query_tokens)
        if query_count <= 2:
            allowed = best_overlap >= query_count
        elif query_count == 3:
            allowed = best_overlap >= 2 and overlap_ratio >= 0.66
        elif query_count == 4:
            allowed = best_overlap >= 3 and overlap_ratio >= 0.75
        else:
            allowed = best_overlap >= 4 and overlap_ratio >= 0.67
        guard_reasons.append('strong_overlap' if allowed else 'insufficient_overlap')

    return {
        'query_phrase': normalize_text(query),
        'query_tokens': query_tokens[:12],
        'query_token_count': len(query_tokens),
        'query_ignored_generic_tokens': ignored_tokens[:12],
        'query_match_allowed': allowed,
        'query_match_exact': exact,
        'query_match_prefix': prefix,
        'query_match_overlap': best_overlap,
        'query_match_overlap_ratio': round(overlap_ratio, 3),
        'query_match_missing_tokens': missing_tokens[:12],
        'query_match_guard_reasons': guard_reasons[:6],
        'query_match_strict_guard': not allowed,
        'title_phrase': title_phrase,
        'title_tokens': title_tokens[:12],
        'title_exact': title_exact,
        'title_prefix': title_prefix,
        'title_extra_tokens': title_tokens[len(query_tokens):][:12] if title_prefix and len(title_tokens) > len(query_tokens) else [],
        'title_overlap': title_overlap,
        'slug_phrase': slug_phrase,
        'slug_tokens': slug_tokens[:12],
        'slug_exact': slug_exact,
        'slug_prefix': slug_prefix,
        'slug_extra_tokens': slug_tokens[len(query_tokens):][:12] if slug_prefix and len(slug_tokens) > len(query_tokens) else [],
        'slug_overlap': slug_overlap,
    }

def is_instagram_source_metadata_suspicious(metadata: dict) -> bool:
    metadata = metadata or {}
    handle = str(metadata.get('source_creator_handle') or '').lstrip('@').strip().lower()
    profile_url = str(metadata.get('source_profile_url') or '').strip()
    profile_domain = canonical_domain(profile_url)
    creator_name = metadata.get('source_creator_name') or metadata.get('source_channel_name') or ''

    if profile_domain and profile_domain != 'instagram.com':
        return True
    if handle and handle in SOCIAL_PROFILE_RESERVED_PATHS:
        return True
    if handle in {'instagram', 'learn_more', 'learn.more'}:
        return True
    if is_generic_source_label(creator_name):
        return True
    if not handle and not profile_url:
        return True
    return False


def has_explicit_instagram_offpage_recipe_clue(text: str) -> bool:
    source = normalize_text_preserve_lines(text or '')
    if not source:
        return False
    lowered = source.lower()

    has_recipe_word = bool(re.search(r"\brecipe\b|מתכון|מתכונים", lowered, flags=re.IGNORECASE))
    has_bio_or_site = bool(re.search(
        r"\blink\s+in\s+bio\b|\bbio\s+link\b|\bsubstack\b|\bwebsite\b|\bblog\b|לינק\s+בביו|קישור\s+בביו|בביו|באתר|בבלוג",
        lowered,
        flags=re.IGNORECASE,
    ))
    has_recipe_redirect_phrase = bool(re.search(
        r"\brecipe\b.{0,80}\b(?:bio|website|blog|substack)\b|\bfull\s+(?:written\s+)?recipe\b.{0,80}\b(?:website|blog|bio|substack)\b|\brecipe\s+will\s+be\s+up\b.{0,80}\b(?:today|bio|substack|website|blog)\b|מתכון.{0,40}(?:בביו|באתר|בבלוג)",
        source,
        flags=re.IGNORECASE | re.DOTALL,
    ))

    return has_recipe_redirect_phrase or (has_bio_or_site and (has_recipe_word or 'substack' in lowered))


def should_try_instagram_discovery(evidence: dict, source_metadata: dict) -> tuple[bool, list[str]]:
    reasons = []
    caption = normalize_text_preserve_lines(
        choose_first_non_empty(
            extract_longest_quoted_block(evidence.get('expanded_caption_text') or ''),
            evidence.get('expanded_caption_text') or '',
            evidence.get('visible_text_after_expand') or '',
        )
    )
    raw_text = normalize_text_preserve_lines(evidence.get('raw_page_text') or '')
    meta = normalize_text_preserve_lines(evidence.get('meta_description') or '')
    combined = combine_text_blocks([caption, raw_text, meta])
    metrics = evaluate_evidence_text(combined[:15000])
    page_title = normalize_text(evidence.get('page_title') or '').lower()
    generic_title = (not page_title) or page_title == 'instagram' or page_title.startswith('instagram ')
    hint_present = looks_like_instagram_discovery_hint(combined)
    mention_count = len(extract_instagram_mentions(combined))
    owner_hint = extract_instagram_owner_hint(combined)
    source_suspicious = is_instagram_source_metadata_suspicious(source_metadata)
    no_measurements = metrics.get('measurementSignalCount', 0) == 0
    weak_caption = len(caption) < 120 or count_non_empty_lines(caption) < 4
    explicit_offpage_recipe_clue = has_explicit_instagram_offpage_recipe_clue(combined)

    runtime_rules = {}
    try:
        runtime_rules = (get_instagram_external_site_runtime_rules() or {}).get('rules') or {}
    except Exception:
        runtime_rules = {}
    detected_clues = [normalize_text(clue).lower() for clue in detect_investigation_clues(combined, runtime_rules) if normalize_text(clue)]
    has_google_recipe_clue = 'google_creator_recipe' in detected_clues
    has_strong_site_clue = any(clue in {'link_in_bio', 'full_recipe_on_website'} for clue in detected_clues)
    has_comment_recipe_offer = 'comment_for_dm_recipe' in detected_clues
    strong_identity = bool(owner_hint.get('handle') or mention_count > 0)

    if source_suspicious:
        reasons.append('source_metadata_suspicious')
    if generic_title:
        reasons.append('generic_page_title')
    if hint_present:
        reasons.append('website_hint_present')
    if mention_count > 0:
        reasons.append('mentions_present')
    if owner_hint.get('handle'):
        reasons.append('owner_text_hint_present')
    if weak_caption:
        reasons.append('caption_text_weak')
    if not metrics.get('looksRecipeDense'):
        reasons.append('recipe_text_not_dense')
    if no_measurements:
        reasons.append('no_measurements')
    if explicit_offpage_recipe_clue:
        reasons.append('explicit_offpage_recipe_clue')
    if has_google_recipe_clue:
        reasons.append('google_recipe_clue')
    if has_strong_site_clue:
        reasons.append('strong_site_clue')
    if has_comment_recipe_offer:
        reasons.append('comment_recipe_offer')

    should_try = False
    if explicit_offpage_recipe_clue and (strong_identity or hint_present):
        should_try = True
    elif has_google_recipe_clue and (strong_identity or hint_present):
        should_try = True
    elif has_strong_site_clue and (generic_title or strong_identity or hint_present):
        should_try = True
    elif has_comment_recipe_offer and has_google_recipe_clue and (strong_identity or hint_present):
        should_try = True
    elif explicit_offpage_recipe_clue and (not metrics.get('looksRecipeDense') or no_measurements or weak_caption or source_suspicious):
        should_try = True
    elif hint_present and generic_title and (not metrics.get('looksRecipeDense') or no_measurements):
        should_try = True
    elif hint_present and source_suspicious:
        should_try = True
    elif source_suspicious and owner_hint.get('handle') and not metrics.get('looksRecipeDense'):
        should_try = True

    return should_try, list(dict.fromkeys(reasons))
def is_allowed_instagram_profile_href(href: str) -> bool:
    normalized = normalize_instagram_profile_root(href or '')
    if not normalized:
        return False
    parsed = urlparse(normalized)
    host = canonical_domain(normalized)
    if host != 'instagram.com':
        return False
    parts = [part for part in (parsed.path or '').split('/') if part]
    if len(parts) != 1:
        return False
    return parts[0].lower() not in SOCIAL_PROFILE_RESERVED_PATHS
def looks_like_non_page_asset_host(host: str, url: str = "") -> bool:
    normalized_host = str(host or '').replace('www.', '').lower().strip()
    lower_url = str(url or '').lower()

    if not normalized_host:
        return False

    if any(
        normalized_host == blocked or normalized_host.endswith(f'.{blocked}')
        for blocked in NON_PAGE_EXTERNAL_HOST_BLOCKLIST
    ):
        return True

    if normalized_host.startswith(NON_PAGE_EXTERNAL_HOST_PREFIXES):
        return True

    if normalized_host.startswith('scontent-') or '.cdninstagram.com' in normalized_host:
        return True

    if any(token in lower_url for token in [
        'cdninstagram.com/',
        'fbcdn.net/',
        'ytimg.com/',
        'googlevideo.com/',
        'ggpht.com/',
        'googleusercontent.com/',
        'storage.googleapis.com/',
        'tiktokcdn.com/',
        'byteimg.com/',
        'pinimg.com/',
    ]):
        return True

    return False


def is_external_site_host(host: str) -> bool:
    host = str(host or '').replace('www.', '').lower().strip()
    if not host:
        return False
    if looks_like_non_page_asset_host(host):
        return False
    return all(host != blocked and not host.endswith(f'.{blocked}') for blocked in SOCIAL_EXTERNAL_HOST_BLOCKLIST)


def extract_external_site_candidates_from_items(items, base_url: str = '', preferred_host: str = '') -> list[dict]:
    candidates = []
    seen = set()
    preferred_host = canonical_domain(preferred_host)

    for item in items or []:
        raw_href = item.get('href') or ''
        href = normalize_profile_url(raw_href, base_url)
        href = unwrap_known_redirect_url(href)
        text = source_safe_text(item.get('text') or '')
        if not href:
            continue
        parsed = urlparse(href)
        host = canonical_domain(href)
        if not is_external_site_host(host):
            continue
        if parsed.scheme not in {'http', 'https'}:
            continue
        if not looks_like_fetchable_external_page(href):
            continue
        dedupe_key = investigation_candidate_dedupe_key(href)
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        score = 0
        if preferred_host and host == preferred_host:
            score += 40
        if text and not is_generic_source_label(text):
            score += 15
        path = (parsed.path or '').lower()
        if path and path not in {'/', ''}:
            score += 8
        runtime_rules = get_instagram_external_site_runtime_rules().get('rules') or {}
        positive_hints = INSTAGRAM_SITE_POSITIVE_HINTS + [item.lower() for item in runtime_string_list(runtime_rules, 'positive_hints')]
        if any(keyword in href.lower() or keyword in text.lower() for keyword in positive_hints):
            score += 20
        candidates.append({
            'url': href,
            'text': text,
            'host': host,
            'score': score,
        })

    return sorted(candidates, key=lambda item: item['score'], reverse=True)


def _host_matches_any_domain(host: str, domains) -> bool:
    normalized_host = str(host or '').replace('www.', '').lower().strip()
    if not normalized_host:
        return False
    for domain in domains or []:
        blocked = str(domain or '').replace('www.', '').lower().strip()
        if not blocked:
            continue
        if normalized_host == blocked or normalized_host.endswith(f'.{blocked}'):
            return True
    return False


def extract_instagram_domain_affinity_tokens(source_metadata: dict, owner_hint: dict, query_info: dict | None = None) -> list[str]:
    query_info = query_info or {}
    raw_values = [
        source_metadata.get('source_creator_handle') if isinstance(source_metadata, dict) else '',
        source_metadata.get('source_creator_name') if isinstance(source_metadata, dict) else '',
        source_metadata.get('source_channel_name') if isinstance(source_metadata, dict) else '',
        source_metadata.get('creator_group_key') if isinstance(source_metadata, dict) else '',
        owner_hint.get('handle') if isinstance(owner_hint, dict) else '',
        owner_hint.get('display_name') if isinstance(owner_hint, dict) else '',
        query_info.get('primary_phrase') if isinstance(query_info, dict) else '',
        query_info.get('extended_phrase') if isinstance(query_info, dict) else '',
    ]

    tokens = []
    seen = set()
    for raw_value in raw_values:
        value = normalize_text_preserve_lines(raw_value).lower()
        if not value:
            continue
        compact = re.sub(r'[^a-z0-9֐-׿؀-ۿ]+', '', value)
        if compact and len(compact) >= 4 and compact not in seen and compact not in INSTAGRAM_DOMAIN_AFFINITY_STOPWORDS:
            seen.add(compact)
            tokens.append(compact)
        split_tokens = re.findall(r'[a-z0-9]{4,}|[֐-׿؀-ۿ]{2,}', value)
        for token in split_tokens:
            if token in seen or token in INSTAGRAM_DOMAIN_AFFINITY_STOPWORDS:
                continue
            seen.add(token)
            tokens.append(token)
    tokens.sort(key=lambda item: (-len(item), item))
    return tokens[:16]


def score_instagram_candidate_domain_affinity(url: str, affinity_tokens: list[str]) -> int:
    normalized = normalize_profile_url(url or '', url or '')
    if not normalized:
        return 0
    parsed = urlparse(normalized)
    host = canonical_domain(normalized)
    haystack = f"{host.replace('.', ' ')} {decode_url_path(parsed.path or '')}".lower()
    best = 0
    for token in affinity_tokens or []:
        if not token or token not in haystack:
            continue
        if re.search(r'[֐-׿؀-ۿ]', token):
            best = max(best, 90)
        elif len(token) >= 10:
            best = max(best, 170)
        elif len(token) >= 6:
            best = max(best, 140)
        else:
            best = max(best, 110)
    return best


def rerank_instagram_external_site_candidates(candidates, affinity_tokens: list[str]) -> list[dict]:
    reranked = []
    for item in candidates or []:
        candidate = dict(item or {})
        url = candidate.get('url') or ''
        host = canonical_domain(url)
        score = int(candidate.get('score') or 0)
        if _host_matches_any_domain(host, INSTAGRAM_EXTERNAL_MEETING_HOST_BLOCKLIST):
            score -= 260
        elif _host_matches_any_domain(host, INSTAGRAM_EXTERNAL_SHORTENER_HOSTS):
            score -= 140
        score += score_instagram_candidate_domain_affinity(url, affinity_tokens)
        score += score_instagram_external_candidate_risk(url, affinity_tokens)
        candidate['score'] = score
        reranked.append(candidate)
    reranked.sort(key=lambda item: item.get('score') or 0, reverse=True)
    return reranked


def looks_like_instagram_spam_candidate(url: str, affinity_tokens: list[str] | None = None) -> bool:
    normalized = normalize_profile_url(url or "", url or "")
    if not normalized:
        return False

    affinity_tokens = affinity_tokens or []
    if score_instagram_candidate_domain_affinity(normalized, affinity_tokens) > 0:
        return False

    try:
        parsed = urlparse(normalized)
        host = canonical_domain(normalized)
        path = decode_url_path(parsed.path or "")
    except Exception:
        return False

    if _host_matches_any_domain(host, INSTAGRAM_EXTERNAL_SPAM_HOSTS):
        return True

    tld = host.rsplit(".", 1)[-1] if "." in host else ""
    haystack = f"{host} {path}".lower()

    if any(token in haystack for token in INSTAGRAM_EXTERNAL_SPAM_TOKENS):
        return True

    if tld in INSTAGRAM_EXTERNAL_SUSPICIOUS_TLDS and len(path.strip("/")) <= 32:
        return True

    if re.search(r"(?:^|[-.])(vip|slot|bet|bonus|hoki|togel|judi)(?:[-.]|$)", host):
        return True

    return False


def score_instagram_external_candidate_risk(url: str, affinity_tokens: list[str] | None = None) -> int:
    if not looks_like_instagram_spam_candidate(url, affinity_tokens):
        return 0

    normalized = normalize_profile_url(url or "", url or "")
    try:
        parsed = urlparse(normalized)
        path = decode_url_path(parsed.path or "")
    except Exception:
        path = ""

    penalty = -260
    if "/register" in path or "/signup" in path or "/login" in path:
        penalty -= 80
    return penalty


def _extract_instagram_recipe_newsletter_slugs(source_metadata: dict, owner_hint: dict) -> list[str]:
    raw_values = [
        owner_hint.get("handle") if isinstance(owner_hint, dict) else "",
        source_metadata.get("source_creator_handle") if isinstance(source_metadata, dict) else "",
        source_metadata.get("creator_group_key") if isinstance(source_metadata, dict) else "",
        source_metadata.get("source_channel_key") if isinstance(source_metadata, dict) else "",
    ]

    slugs = []
    seen = set()
    for raw in raw_values:
        value = normalize_text(raw or "").lower()
        if not value:
            continue
        value = value.lstrip("@")
        if ":" in value:
            value = value.split(":")[-1]
        value = re.sub(r"[^a-z0-9_-]+", "", value)
        if not value or value in seen:
            continue
        seen.add(value)
        slugs.append(value)
    return slugs


def _slugify_instagram_query_phrase(value: str) -> str:
    tokens = [
        token for token in re.findall(r"[a-z0-9]{2,}", normalize_text(value or "").lower())
        if token not in ENGLISH_STOPWORDS and token not in {"day", "days", "recipe", "recipes"}
    ]
    deduped = []
    for token in tokens:
        if token not in deduped:
            deduped.append(token)
    return "-".join(deduped[:8])


def should_generate_instagram_recipe_newsletter_candidates(
    source_metadata: dict,
    owner_hint: dict,
    clue_list,
    existing_candidates,
    affinity_tokens: list[str],
) -> bool:
    normalized_clues = {normalize_text(clue).lower() for clue in (clue_list or []) if normalize_text(clue)}
    if not ({"full_recipe_on_website", "website_hint"} & normalized_clues):
        return False

    if any(canonical_domain(item.get("url") or "").endswith("substack.com") for item in (existing_candidates or [])):
        return False

    for item in existing_candidates or []:
        url = item.get("url") or ""
        if score_instagram_candidate_domain_affinity(url, affinity_tokens) >= 120 and int(item.get("score") or 0) >= 90:
            return False

    newsletter_slugs = _extract_instagram_recipe_newsletter_slugs(source_metadata, owner_hint)
    if not newsletter_slugs:
        return False

    return any(any(token in slug for token in INSTAGRAM_RECIPE_NEWSLETTER_TOKENS) for slug in newsletter_slugs)


def build_instagram_recipe_newsletter_candidates(
    source_metadata: dict,
    owner_hint: dict,
    query_info: dict | None,
    affinity_tokens: list[str],
) -> list[dict]:
    query_info = query_info or {}
    newsletter_slugs = _extract_instagram_recipe_newsletter_slugs(source_metadata, owner_hint)
    if not newsletter_slugs:
        return []

    phrase_slugs = []
    for phrase in [
        " ".join(query_info.get("core_tokens") or []),
        query_info.get("extended_phrase") or "",
        query_info.get("primary_phrase") or "",
    ]:
        slug = _slugify_instagram_query_phrase(phrase)
        if slug and slug not in phrase_slugs:
            phrase_slugs.append(slug)
            viral_slug = f"viral-{slug}"
            if viral_slug not in phrase_slugs:
                phrase_slugs.append(viral_slug)

    candidates = []
    seen = set()
    for newsletter_slug in newsletter_slugs[:2]:
        root = f"https://{newsletter_slug}.substack.com/"
        affinity_bonus = score_instagram_candidate_domain_affinity(root, affinity_tokens)
        seeded = []
        if phrase_slugs:
            seeded.append((f"{root}p/{phrase_slugs[0]}", 310 + affinity_bonus, "newsletter_exact_recipe_candidate"))
        if len(phrase_slugs) > 1:
            seeded.append((f"{root}p/{phrase_slugs[1]}", 295 + affinity_bonus, "newsletter_alt_recipe_candidate"))
        for phrase_slug in phrase_slugs[2:4]:
            seeded.append((f"{root}p/{phrase_slug}", 245 + affinity_bonus, "newsletter_recipe_candidate"))
        seeded.extend([
            (f"{root}archive", 190 + affinity_bonus, "newsletter_archive_candidate"),
            (f"{root}p/recipe-index", 175 + affinity_bonus, "newsletter_recipe_index_candidate"),
        ])

        for url, score, label in seeded:
            if not url:
                continue
            dedupe_key = investigation_candidate_dedupe_key(url)
            if not dedupe_key or dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            candidates.append({
                "url": url,
                "text": label,
                "host": canonical_domain(url),
                "score": score,
            })

    candidates.sort(key=lambda item: item.get("score") or 0, reverse=True)
    return candidates


def inject_instagram_creator_affine_external_site_candidates(
    existing_candidates,
    *,
    source_metadata: dict,
    owner_hint: dict,
    clue_list,
    query_info: dict | None,
    affinity_tokens: list[str],
):
    candidates = [dict(item or {}) for item in (existing_candidates or [])]
    if not should_generate_instagram_recipe_newsletter_candidates(
        source_metadata,
        owner_hint,
        clue_list,
        candidates,
        affinity_tokens,
    ):
        return candidates

    seen = {investigation_candidate_dedupe_key(item.get("url") or "") for item in candidates}
    for candidate in build_instagram_recipe_newsletter_candidates(source_metadata, owner_hint, query_info, affinity_tokens):
        dedupe_key = investigation_candidate_dedupe_key(candidate.get("url") or "")
        if not dedupe_key or dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        candidates.append(candidate)

    return rerank_instagram_external_site_candidates(candidates, affinity_tokens)


INSTAGRAM_CREATOR_DOMAIN_SEED_STOPWORDS = {
    'instagram', 'reels', 'reel', 'recipe', 'recipes', 'official', 'channel', 'account', 'blog', 'bio', 'link',
    'home', 'page', 'food', 'kitchen', 'cooking', 'cook', 'chef', 'the', 'and', 'with', 'from', 'for', 'www', 'com',
}


def _extract_instagram_creator_domain_seed_parts(source_metadata: dict, owner_hint: dict) -> list[dict]:
    raw_values = [
        owner_hint.get('handle') if isinstance(owner_hint, dict) else '',
        source_metadata.get('source_creator_handle') if isinstance(source_metadata, dict) else '',
        source_metadata.get('creator_group_key') if isinstance(source_metadata, dict) else '',
        source_metadata.get('source_channel_key') if isinstance(source_metadata, dict) else '',
    ]

    parts = []
    seen = set()
    for raw in raw_values:
        value = normalize_text(raw or '').lower()
        if not value:
            continue
        value = value.lstrip('@')
        if ':' in value:
            value = value.split(':')[-1]
        tokens = [token for token in re.findall(r'[a-z0-9]{3,}', value) if token not in INSTAGRAM_CREATOR_DOMAIN_SEED_STOPWORDS]
        if not tokens:
            continue
        joined = ''.join(tokens)
        hyphenated = '-'.join(tokens)
        first = tokens[0]
        key = (joined, hyphenated, first)
        if key in seen:
            continue
        seen.add(key)
        parts.append({'tokens': tokens, 'joined': joined, 'hyphenated': hyphenated, 'first': first})
    return parts


def should_generate_instagram_creator_domain_seed_candidates(
    source_metadata: dict,
    owner_hint: dict,
    clue_list,
    existing_candidates,
    affinity_tokens: list[str],
) -> bool:
    normalized_clues = {normalize_text(clue).lower() for clue in (clue_list or []) if normalize_text(clue)}
    if not ({'full_recipe_on_website', 'website_hint', 'link_in_bio'} & normalized_clues):
        return False

    for item in existing_candidates or []:
        url = item.get('url') or ''
        if score_instagram_candidate_domain_affinity(url, affinity_tokens) >= 120 and int(item.get('score') or 0) >= 90:
            return False

    return bool(_extract_instagram_creator_domain_seed_parts(source_metadata, owner_hint))


def build_instagram_creator_domain_seed_candidates(
    source_metadata: dict,
    owner_hint: dict,
    clue_list,
    affinity_tokens: list[str],
) -> list[dict]:
    normalized_clues = {normalize_text(clue).lower() for clue in (clue_list or []) if normalize_text(clue)}
    clue_bonus = 20 if 'full_recipe_on_website' in normalized_clues else 0
    clue_bonus += 10 if 'link_in_bio' in normalized_clues else 0

    candidates = []
    seen = set()
    for part in _extract_instagram_creator_domain_seed_parts(source_metadata, owner_hint)[:3]:
        joined = part.get('joined') or ''
        hyphenated = part.get('hyphenated') or ''
        first = part.get('first') or ''

        seeded = []
        if joined and len(joined) >= 6:
            joined_url = f'https://{joined}.com/'
            seeded.append((joined_url, 140 + clue_bonus + score_instagram_candidate_domain_affinity(joined_url, affinity_tokens), 'creator_domain_seed'))
        if hyphenated and hyphenated != joined and len(hyphenated.replace('-', '')) >= 6:
            hyphen_url = f'https://{hyphenated}.com/'
            seeded.append((hyphen_url, 132 + clue_bonus + score_instagram_candidate_domain_affinity(hyphen_url, affinity_tokens), 'creator_domain_seed_hyphen'))
        if first and len(first) >= 4:
            just_url = f'https://just-{first}.com/'
            seeded.append((just_url, 205 + clue_bonus + score_instagram_candidate_domain_affinity(just_url, affinity_tokens), 'creator_domain_seed_just'))

        for url, score, label in seeded:
            if not looks_like_fetchable_external_page(url):
                continue
            dedupe_key = investigation_candidate_dedupe_key(url)
            if not dedupe_key or dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            candidates.append({'url': url, 'text': label, 'host': canonical_domain(url), 'score': score})

    candidates.sort(key=lambda item: item.get('score') or 0, reverse=True)
    return candidates


def should_force_instagram_creator_domain_just_seed_candidates(
    source_metadata: dict,
    owner_hint: dict,
    clue_list,
    existing_candidates,
) -> bool:
    normalized_clues = {normalize_text(clue).lower() for clue in (clue_list or []) if normalize_text(clue)}
    if not ({'full_recipe_on_website', 'website_hint', 'link_in_bio'} & normalized_clues):
        return False

    existing_domains = {
        canonical_domain((item or {}).get('url') or '')
        for item in (existing_candidates or [])
        if canonical_domain((item or {}).get('url') or '')
    }
    if not existing_domains or any(domain.startswith('just-') for domain in existing_domains):
        return False

    seed_parts = _extract_instagram_creator_domain_seed_parts(source_metadata, owner_hint)
    predicted_domains = set()
    for part in seed_parts[:3]:
        joined = canonical_domain(f"https://{part.get('joined') or ''}.com/")
        hyphenated = canonical_domain(f"https://{part.get('hyphenated') or ''}.com/")
        if joined:
            predicted_domains.add(joined)
        if hyphenated:
            predicted_domains.add(hyphenated)

    return bool(predicted_domains) and existing_domains.issubset(predicted_domains)


def inject_instagram_creator_domain_seed_candidates(
    existing_candidates,
    *,
    source_metadata: dict,
    owner_hint: dict,
    clue_list,
    affinity_tokens: list[str],
):
    candidates = [dict(item or {}) for item in (existing_candidates or [])]
    should_generate = should_generate_instagram_creator_domain_seed_candidates(
        source_metadata,
        owner_hint,
        clue_list,
        candidates,
        affinity_tokens,
    )
    force_just_seed = should_force_instagram_creator_domain_just_seed_candidates(
        source_metadata,
        owner_hint,
        clue_list,
        candidates,
    )
    if not should_generate and not force_just_seed:
        return candidates

    seen = {investigation_candidate_dedupe_key(item.get('url') or '') for item in candidates}
    for candidate in build_instagram_creator_domain_seed_candidates(source_metadata, owner_hint, clue_list, affinity_tokens):
        candidate_url = candidate.get('url') or ''
        candidate_domain = canonical_domain(candidate_url)
        if force_just_seed and candidate_domain and not candidate_domain.startswith('just-'):
            if investigation_candidate_dedupe_key(candidate_url) not in seen:
                # keep forced-just rescue focused and low-risk
                continue
        dedupe_key = investigation_candidate_dedupe_key(candidate_url)
        if not dedupe_key or dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        candidates.append(candidate)

    return rerank_instagram_external_site_candidates(candidates, affinity_tokens)


def extract_instagram_profile_external_link_items(profile_html: str, base_url: str = '') -> list[dict]:
    source = str(profile_html or '')
    if not source:
        return []

    decoded_source = decode_htmlish(source)
    items = []
    seen = set()

    def normalize_candidate_url(raw_url: str) -> str:
        value = decode_htmlish(raw_url or '').strip()
        if not value:
            return ''
        if value.startswith('//'):
            value = f'https:{value}'
        elif not re.match(r'^https?://', value, flags=re.IGNORECASE):
            if re.match(r'^(?:www\.)?[A-Za-z0-9.-]+\.[A-Za-z]{2,}(?:[/?#]|$)', value):
                value = f"https://{value.lstrip('/')}"
            elif base_url:
                value = urljoin(base_url, value)
        if not re.match(r'^https?://', value, flags=re.IGNORECASE):
            return ''
        return unwrap_known_redirect_url(value)

    def add_candidate(raw_url: str, text_label: str = '') -> None:
        normalized = normalize_candidate_url(raw_url)
        if not normalized or not looks_like_fetchable_external_page(normalized):
            return
        dedupe_key = strip_url_query_fragment(normalized).lower()
        if dedupe_key in seen:
            return
        seen.add(dedupe_key)
        items.append({
            'href': normalized,
            'text': source_safe_text(text_label or 'bio link'),
        })

    targeted_patterns = [
        (re.compile(r'"bio_links?"\s*:\s*\[[\s\S]{0,4000}?"url"\s*:\s*"((?:\\.|[^"\\])*)"', flags=re.IGNORECASE), 'bio link'),
        (re.compile(r'"bioLinks?"\s*:\s*\[[\s\S]{0,4000}?"url"\s*:\s*"((?:\\.|[^"\\])*)"', flags=re.IGNORECASE), 'bio link'),
        (re.compile(r'"bioLink"\s*:\s*\{[\s\S]{0,1200}?"(?:url|link)"\s*:\s*"((?:\\.|[^"\\])*)"', flags=re.IGNORECASE), 'bio link'),
        (re.compile(r'"external_url"\s*:\s*"((?:\\.|[^"\\])*)"', flags=re.IGNORECASE), 'external url'),
        (re.compile(r'"externalUrl"\s*:\s*"((?:\\.|[^"\\])*)"', flags=re.IGNORECASE), 'external url'),
        (re.compile(r'"website"\s*:\s*"((?:\\.|[^"\\])*)"', flags=re.IGNORECASE), 'website'),
        (re.compile(r'"websiteUrl"\s*:\s*"((?:\\.|[^"\\])*)"', flags=re.IGNORECASE), 'website'),
        (re.compile(r'"website_url"\s*:\s*"((?:\\.|[^"\\])*)"', flags=re.IGNORECASE), 'website'),
        (re.compile(r'"link_url"\s*:\s*"((?:\\.|[^"\\])*)"', flags=re.IGNORECASE), 'bio link'),
        (re.compile(r'"outbound_link"\s*:\s*"((?:\\.|[^"\\])*)"', flags=re.IGNORECASE), 'bio link'),
        (re.compile(r'https?://l\.instagram\.com/\?u=([^"\'\s<>]+)', flags=re.IGNORECASE), 'bio link'),
    ]

    for pattern, label in targeted_patterns:
        for match in pattern.finditer(source):
            if match and match.group(1):
                add_candidate(match.group(1), label)
        for match in pattern.finditer(decoded_source):
            if match and match.group(1):
                add_candidate(match.group(1), label)

    positive_context_markers = ('link in bio', 'bio link', 'website', 'external url', 'external_url', 'my website', 'food blog')
    for extracted in extract_urls_with_context(decoded_source, window=220):
        context = extracted.get('context') or ''
        lower_context = context.lower()
        if 'l.instagram.com/?u=' in (extracted.get('url') or '').lower() or any(marker in lower_context for marker in positive_context_markers):
            add_candidate(extracted.get('url') or '', context)

    return items


def build_instagram_profile_url_items_from_html(profile_html: str, base_url: str, affinity_tokens: list[str] | None = None) -> list[dict]:
    affinity_tokens = affinity_tokens or []
    items = []
    seen = set()

    def add_item(raw_url: str, text_label: str = '') -> None:
        normalized = normalize_profile_url(unwrap_known_redirect_url(raw_url or ''), base_url)
        if not normalized or not looks_like_fetchable_external_page(normalized):
            return
        dedupe_key = strip_url_query_fragment(normalized).lower()
        if dedupe_key in seen:
            return
        seen.add(dedupe_key)
        items.append({'href': normalized, 'text': text_label or ''})

    decoded_html = decode_htmlish(profile_html or "")
    positive_context_markers = ('link in bio', 'bio link', 'website', 'external url', 'my website', 'food blog', 'blog')
    for extracted in extract_urls_with_context(decoded_html, window=220):
        extracted_url = extracted.get("url") or ""
        extracted_host = canonical_domain(extracted_url)
        lower_context = (extracted.get("context") or "").lower()
        if (
            score_instagram_candidate_domain_affinity(extracted_url, affinity_tokens) > 0
            or extracted_host.endswith("substack.com")
            or 'l.instagram.com/?u=' in extracted_url.lower()
            or any(marker in lower_context for marker in positive_context_markers)
        ):
            add_item(extracted_url, extracted.get('context') or '')

    # Broader raw/decoded HTML fallback: keep any fetchable external page and let later reranking/filtering decide.
    for source_text in [profile_html or '', decoded_html]:
        for extracted_url in extract_urls_from_text(source_text):
            if looks_like_fetchable_external_page(extracted_url):
                add_item(extracted_url, '')

    for extracted in extract_urls_with_context(profile_html or '', window=180):
        extracted_url = extracted.get('url') or ''
        if looks_like_fetchable_external_page(extracted_url):
            add_item(extracted_url, extracted.get('context') or '')

    for extracted in extract_instagram_profile_external_link_items(profile_html, base_url):
        add_item(extracted.get('href') or '', extracted.get('text') or '')

    return items


def score_instagram_site_anchor_candidate(url: str, text: str, site_host: str, hint_tokens: list[str], query_info: dict | None = None) -> int:
    href = unwrap_known_redirect_url(normalize_profile_url(url or ''))
    if not href:
        return -1000
    host = canonical_domain(href)
    if not host or host != canonical_domain(site_host):
        return -1000
    if not is_external_site_host(host):
        return -1000

    query_info = query_info or {}
    primary_phrase = (query_info.get('primary_phrase') or '').lower()
    extended_phrase = (query_info.get('extended_phrase') or '').lower()
    core_tokens = [str(token or '').lower() for token in (query_info.get('core_tokens') or []) if str(token or '').strip()]

    parsed = urlparse(href)
    path = decode_url_path(parsed.path or '')
    lower_text = source_safe_text(text).lower()
    haystack = f"{path} {lower_text}"
    score = 0

    if path and path not in {'/', ''}:
        score += 10
    runtime_rules = get_instagram_external_site_runtime_rules().get('rules') or {}
    positive_hints = INSTAGRAM_SITE_POSITIVE_HINTS + [item.lower() for item in runtime_string_list(runtime_rules, 'positive_hints')]
    negative_hints = INSTAGRAM_SITE_NEGATIVE_HINTS + [item.lower() for item in runtime_string_list(runtime_rules, 'negative_hints')]

    if any(hint in path or hint in lower_text for hint in positive_hints):
        score += 45
    if any(hint in path for hint in negative_hints):
        score -= 120
    if is_homepage_like_url(href):
        score -= 25

    phrase_hit = False
    for phrase in [extended_phrase, primary_phrase]:
        if phrase and phrase in haystack:
            phrase_hit = True
            score += 180 if phrase == extended_phrase else 140
            break

    core_hits = 0
    for token in core_tokens:
        if token and token in haystack:
            core_hits += 1
    score += min(core_hits * 45, 180)

    token_hits = 0
    for token in hint_tokens or []:
        if token and (token in path or token in lower_text):
            token_hits += 1
    score += min(token_hits * 18, 72)

    query_match = build_query_match_debug(primary_phrase or extended_phrase, lower_text or path, href)
    if query_match['slug_exact']:
        score += 220
    elif query_match['slug_prefix']:
        score += 70 - (110 * len(query_match['slug_extra_tokens']))

    if query_match['title_exact']:
        score += 180
    elif query_match['title_prefix']:
        score += 50 - (90 * len(query_match['title_extra_tokens']))

    if core_tokens and core_hits == 0 and not phrase_hit:
        score -= 90
    if lower_text and not is_generic_source_label(lower_text):
        score += 10

    return score


def score_instagram_linked_page_match(linked_evidence: dict, original_target_url: str, hint_tokens: list[str], source_handle: str = '', query_info: dict | None = None) -> int:
    source_handle = str(source_handle or '').lstrip('@').lower()
    query_info = query_info or {}
    primary_phrase = (query_info.get('primary_phrase') or '').lower()
    extended_phrase = (query_info.get('extended_phrase') or '').lower()
    core_tokens = [str(token or '').lower() for token in (query_info.get('core_tokens') or []) if str(token or '').strip()]

    page_title = normalize_text_preserve_lines(linked_evidence.get('page_title') or '').lower()
    meta = normalize_text_preserve_lines(linked_evidence.get('meta_description') or '').lower()
    raw_text = normalize_text_preserve_lines(linked_evidence.get('raw_page_text') or '').lower()
    page_html = str(linked_evidence.get('page_html') or '').lower()
    effective_url = str(linked_evidence.get('effective_page_url') or '').lower()
    title_haystack = '\n'.join([page_title, effective_url, meta[:1000]])
    content_haystack = '\n'.join([page_title, meta, raw_text[:12000], page_html[:12000], effective_url])

    score = 0
    stripped_target = strip_url_query_fragment(original_target_url).lower()
    media_code = extract_instagram_media_code(original_target_url).lower()

    if stripped_target and stripped_target in page_html:
        score += 120
    if media_code and media_code in page_html:
        score += 90
    if source_handle and source_handle in content_haystack:
        score += 25

    phrase_hit = False
    for phrase in [extended_phrase, primary_phrase]:
        if phrase and phrase in title_haystack:
            phrase_hit = True
            score += 260 if phrase == extended_phrase else 220
            break
    if not phrase_hit:
        for phrase in [extended_phrase, primary_phrase]:
            if phrase and phrase in content_haystack:
                phrase_hit = True
                score += 120 if phrase == extended_phrase else 90
                break

    core_title_hits = 0
    core_content_hits = 0
    for token in core_tokens:
        if token and token in title_haystack:
            core_title_hits += 1
        if token and token in content_haystack:
            core_content_hits += 1
    score += min(core_title_hits * 90, 270)
    score += min(core_content_hits * 16, 64)

    token_hits = 0
    for token in hint_tokens or []:
        if token and token in content_haystack:
            token_hits += 1
    score += min(token_hits * 10, 50)

    query_phrase = primary_phrase or extended_phrase
    query_match = build_query_match_debug(query_phrase, page_title, effective_url)
    if query_match['title_exact'] or query_match['slug_exact']:
        score += 260
    elif query_match['title_prefix'] or query_match['slug_prefix']:
        score += 80
        extra_tokens = (query_match['title_extra_tokens'] or []) + (query_match['slug_extra_tokens'] or [])
        unique_extra = []
        for token in extra_tokens:
            token = str(token or '').lower()
            if token and token not in unique_extra:
                unique_extra.append(token)
        score -= 150 * len(unique_extra)

    if core_tokens and core_title_hits == 0 and not phrase_hit:
        score -= 140
    elif core_tokens and core_title_hits == 0:
        score -= 40

    return score


def extract_prefixed_structured_lines(text: str, prefix: str) -> list[str]:
    prefix = str(prefix or '').strip()
    if not text or not prefix:
        return []
    out = []
    seen = set()
    needle = f"{prefix}:"
    for raw_line in normalize_text_preserve_lines(text).split("\n"):
        line = str(raw_line or '').strip()
        if not line.startswith(needle):
            continue
        value = normalize_text_preserve_lines(line[len(needle):])
        if not value or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def line_looks_like_ingredient_candidate(line: str) -> bool:
    lowered = str(line or '').lower()
    if not lowered:
        return False
    if count_likely_measurement_signals(lowered) > 0:
        return True
    if re.match(
        r'^\s*(?:כ[-–]?\s*)?\d+[\/\d¼½¾⅓⅔⅛⅜⅝⅞.,-]*\s+[\u0590-\u05FF][\u0590-\u05FF"\'׳״-]*(?:\s+[\u0590-\u05FF][\u0590-\u05FF"\'׳״-]*){0,6}\s*$',
        str(line or ''),
        flags=re.UNICODE,
    ) and not re.search(r'\b(?:דקה|דקות|שעה|שעות|יום|ימים|שלב|חלק)\b', str(line or ''), flags=re.UNICODE):
        return True
    return any(token in lowered for token in [
        'salt', 'pepper', 'sugar', 'flour', 'butter', 'oil', 'garlic', 'onion', 'water', 'milk',
        'מלח', 'פלפל', 'סוכר', 'קמח', 'שמן', 'שום', 'בצל', 'מים', 'חלב', 'ביצה', 'כוסברה', 'פטרוזיליה', 'סלרי',
    ])


def line_looks_like_instruction_candidate(line: str) -> bool:
    lowered = str(line or '').lower()
    if not lowered:
        return False
    return any(token in lowered for token in [
        'mix', 'add', 'stir', 'cook', 'bake', 'bring', 'boil', 'simmer', 'serve', 'heat', 'place',
        'מערבבים', 'מוסיפים', 'מבשלים', 'מטגנים', 'מניחים', 'טועמים', 'מפזרים', 'ממשיכים', 'מפשירים',
    ])


def normalize_visible_recipe_component_heading(line: str) -> str:
    cleaned = normalize_text_preserve_lines(line).replace('：', ':').strip()
    if not cleaned:
        return ''
    cleaned = re.sub(r'\s*:\s*$', '', cleaned).strip()
    if not cleaned:
        return ''
    if re.match(r'^(ingredients?|ingredient|מצרכים|רכיבים)$', cleaned, flags=re.IGNORECASE):
        return ''
    if re.match(r'^(מלית|מילוי|רוטב|בצק|ציפוי|קרם|filling|stuffing|sauce|dough|topping|cream)$', cleaned, flags=re.IGNORECASE):
        return cleaned
    return ''


def line_is_generic_ingredient_heading(line: str) -> bool:
    cleaned = normalize_text_preserve_lines(line).replace('：', ':').strip()
    cleaned = re.sub(r'\s*:\s*$', '', cleaned).strip()
    return bool(cleaned and re.match(r'^(ingredients?|ingredient|מצרכים|רכיבים)$', cleaned, flags=re.IGNORECASE))


def line_is_generic_instruction_heading(line: str) -> bool:
    cleaned = normalize_text_preserve_lines(line).replace('：', ':').strip()
    cleaned = re.sub(r'\s*:\s*$', '', cleaned).strip()
    return bool(cleaned and re.match(r'^(instructions?|directions?|method|אופן הכנה|הכנה)$', cleaned, flags=re.IGNORECASE))


def normalize_visible_recipe_component_prefix(section: str) -> str:
    cleaned = normalize_visible_recipe_component_heading(section)
    if not cleaned:
        return ''
    hebrew_prefix_map = {
        'מלית': 'למלית',
        'מילוי': 'למילוי',
        'רוטב': 'לרוטב',
        'בצק': 'לבצק',
        'קרם': 'לקרם',
        'ציפוי': 'לציפוי',
    }
    if cleaned in hebrew_prefix_map:
        return hebrew_prefix_map[cleaned]
    if re.match(r'^ל[\u0590-\u05FF]', cleaned):
        return cleaned
    return cleaned


def build_section_scoped_ingredient_line(line: str, section: str | None = None) -> str:
    cleaned = normalize_text_preserve_lines(line)
    if not cleaned:
        return ''
    prefix = normalize_visible_recipe_component_prefix(section or '')
    if not prefix:
        return cleaned
    if re.match(rf'^{re.escape(prefix)}\s*:', cleaned, flags=re.IGNORECASE):
        return cleaned
    return f'{prefix}: {cleaned}'


def line_has_section_scoped_prefix(line: str) -> bool:
    cleaned = normalize_text_preserve_lines(line)
    if not cleaned:
        return False
    return bool(re.match(r'^(?:ל(?:מלית|מילוי|רוטב|בצק|קרם|ציפוי)|(?:filling|stuffing|sauce|dough|topping|cream))\s*:', cleaned, flags=re.IGNORECASE))


def line_is_instruction_section_heading(line: str) -> bool:
    cleaned = normalize_text_preserve_lines(line).replace('：', ':').strip()
    if not cleaned:
        return False
    if line_is_generic_instruction_heading(cleaned):
        return False
    if re.match(r'^הכנת\s+.+:\s*$', cleaned):
        return True
    return bool(re.match(r'^(?:for the .+|assembly|serving|to serve|sauce|filling|stuffing|dough|topping|cream)\s*:\s*$', cleaned, flags=re.IGNORECASE))


def extract_section_lines_from_visible_text(text: str, start_markers: list[str], stop_markers: list[str], max_lines: int = 40, kind: str = 'generic') -> list[str]:
    lines = [normalize_text_preserve_lines(line) for line in str(text or '').split('\n')]
    lines = [line for line in lines if line]
    if not lines:
        return []

    lowered_start = [marker.lower() for marker in (start_markers or []) if marker]
    lowered_stop = [marker.lower() for marker in (stop_markers or []) if marker]

    collecting = False
    out = []
    seen = set()
    seen_by_section = {}
    current_section = None

    def should_begin_from_pre_section(idx: int) -> bool:
        if kind != 'ingredient':
            return False
        line = lines[idx]
        if not line_looks_like_ingredient_candidate(line):
            return False
        lookahead = lines[idx + 1: min(len(lines), idx + 7)]
        for candidate in lookahead:
            lower_candidate = candidate.lower()
            if normalize_visible_recipe_component_heading(candidate):
                return True
            if any(marker in lower_candidate for marker in lowered_start):
                return True
            if any(marker in lower_candidate for marker in lowered_stop):
                return True
        return False

    for idx, line in enumerate(lines):
        lower = line.lower()

        if collecting and any(marker in lower for marker in lowered_stop):
            break

        component_heading = normalize_visible_recipe_component_heading(line) if kind == 'ingredient' else ''

        if not collecting:
            start_hit = any(marker in lower for marker in lowered_start)
            if start_hit:
                collecting = True
                if kind == 'ingredient':
                    if component_heading:
                        current_section = component_heading
                        continue
                    if line_is_generic_ingredient_heading(line):
                        continue
                    if line_looks_like_ingredient_candidate(line):
                        scoped = build_section_scoped_ingredient_line(line, current_section)
                        out.append(scoped)
                        seen.add(scoped)
                        seen_by_section.setdefault(current_section or '__none__', set()).add(scoped)
                    continue
                if kind == 'instruction':
                    if line_is_generic_instruction_heading(line):
                        continue
                    if line_is_instruction_section_heading(line):
                        out.append(line)
                        seen.add(line)
                        continue
                if kind == 'generic':
                    if line in seen:
                        continue
                    seen.add(line)
                    out.append(line)
                    continue
            elif should_begin_from_pre_section(idx):
                collecting = True
            else:
                continue

        if len(out) >= max_lines:
            break

        if kind == 'ingredient':
            if line_is_generic_ingredient_heading(line):
                continue
            if component_heading:
                current_section = component_heading
                continue
            if not line_looks_like_ingredient_candidate(line):
                if len(out) >= 2:
                    continue
            scoped = build_section_scoped_ingredient_line(line, current_section)
            section_key = current_section or '__none__'
            scoped_seen = seen_by_section.setdefault(section_key, set())
            if scoped in scoped_seen:
                continue
            scoped_seen.add(scoped)
            out.append(scoped)
            continue

        if kind == 'instruction':
            if line_is_generic_instruction_heading(line):
                continue
            if not line_looks_like_instruction_candidate(line) and len(out) >= 2 and not line_is_instruction_section_heading(line):
                continue
            if line in seen:
                continue
            seen.add(line)
            out.append(line)
            continue

        if line in seen:
            continue
        seen.add(line)
        out.append(line)

    return out



def extract_loose_instruction_lines_from_visible_text(text: str, max_lines: int = 40) -> list[str]:
    lines = [normalize_text_preserve_lines(line) for line in str(text or '').split('\n')]
    lines = [line for line in lines if line]
    if not lines:
        return []

    out = []
    seen = set()
    after_ingredients = False

    stop_markers = [
        'continue reading this post for free',
        'claim my free post',
        'purchase a paid subscription',
        'subscribe sign in',
    ]

    for line in lines:
        lowered = line.lower()

        if any(marker in lowered for marker in stop_markers):
            break

        if line_is_generic_instruction_heading(line):
            after_ingredients = True
            continue

        if line_is_generic_ingredient_heading(line):
            after_ingredients = True
            continue

        if line_looks_like_ingredient_candidate(line) and not after_ingredients:
            after_ingredients = True
            continue

        if not after_ingredients:
            continue

        if line_looks_like_ingredient_candidate(line) and len(out) < 1:
            continue

        if (
            line_looks_like_instruction_candidate(line)
            or re.match(r'^\s*(?:\d+[\).]|step\s*\d+)', line, flags=re.IGNORECASE)
        ):
            if line not in seen:
                seen.add(line)
                out.append(line)

        if len(out) >= max_lines:
            break

    return out


def is_substack_preview_like_evidence(explicit_link: str, linked_meta_description: str, linked_raw_page_text: str, linked_page_title: str = '') -> bool:
    host = canonical_domain(explicit_link or '')
    if not host.endswith('substack.com'):
        return False
    combined = normalize_text_preserve_lines(
        combine_text_blocks([
            linked_page_title,
            linked_meta_description,
            linked_raw_page_text,
        ])
    )
    if not combined:
        return False
    if 'SUBSTACK_FREE_POST_CLAIM_' in combined:
        return True
    return looks_like_substack_claim_gate_text(combined)


def strip_substack_preview_noise(text: str) -> str:
    cleaned_lines = []
    for raw_line in normalize_text_preserve_lines(text or '').split('\n'):
        line = str(raw_line or '').strip()
        if not line:
            continue
        lower = line.lower()
        if lower.startswith('substack_free_post_claim_'):
            continue
        if any(marker in lower for marker in [
            'subscribe to receive access',
            'purchase a paid subscription',
            'claim my free post',
            'continue reading this post for free',
            'continue reading',
            'subscribe sign in',
            'paid subscription',
        ]):
            continue
        cleaned_lines.append(line)
    return trim_text(combine_text_blocks(cleaned_lines), RAW_PAGE_TEXT_SUBMIT_MAX)


def build_compact_recipe_block(linked_evidence: dict, explicit_link: str, explicit_link_label: str = 'LINKED_RECIPE_PAGE_URL') -> str:
    linked_page_title = normalize_text_preserve_lines(decode_htmlish(linked_evidence.get('page_title') or ''))
    linked_meta_description = normalize_text_preserve_lines(decode_htmlish(linked_evidence.get('meta_description') or ''))
    linked_structured_html = normalize_text_preserve_lines(decode_htmlish(linked_evidence.get('structured_html_text') or ''))
    linked_visible_text = normalize_text_preserve_lines(
        decode_htmlish(linked_evidence.get('visible_page_text') or linked_evidence.get('visible_text_after_expand') or '')
    )
    linked_raw_page_text = normalize_text_preserve_lines(decode_htmlish(linked_evidence.get('raw_page_text') or ''))
    substack_preview = is_substack_preview_like_evidence(
        explicit_link,
        linked_meta_description,
        linked_raw_page_text,
        linked_page_title,
    )
    if substack_preview:
        linked_raw_page_text = strip_substack_preview_noise(linked_raw_page_text)
        linked_visible_text = strip_substack_preview_noise(linked_visible_text)

    structured_ingredients = extract_prefixed_structured_lines(linked_structured_html, 'LDJSON_INGREDIENT')
    structured_instructions = extract_prefixed_structured_lines(linked_structured_html, 'LDJSON_INSTRUCTION')

    fallback_text = linked_visible_text or linked_raw_page_text
    sectioned_ingredients = extract_section_lines_from_visible_text(
        fallback_text,
        ['ingredients', 'ingredient', 'מצרכים', 'רכיבים', 'מלית', 'מילוי', 'רוטב', 'בצק', 'ציפוי', 'קרם', 'filling', 'stuffing', 'sauce', 'dough', 'topping', 'cream'],
        ['instructions', 'directions', 'method', 'אופן הכנה', 'הכנה'],
        max_lines=60,
        kind='ingredient',
    )
    fallback_instructions = extract_section_lines_from_visible_text(
        fallback_text,
        ['instructions', 'directions', 'method', 'אופן הכנה', 'הכנה', 'הכנת'],
        ['notes', 'הערות', 'comments', 'שתפו', 'אהבתם'],
        max_lines=60,
        kind='instruction',
    )

    sectioned_scope_count = sum(1 for line in sectioned_ingredients if line_has_section_scoped_prefix(line))
    if sectioned_ingredients and (
        not structured_ingredients
        or sectioned_scope_count >= 2
        or len(sectioned_ingredients) >= max(8, int(len(structured_ingredients) * 0.75))
    ):
        ingredients = sectioned_ingredients
    else:
        ingredients = structured_ingredients or sectioned_ingredients

    fallback_instruction_heading_count = sum(1 for line in fallback_instructions if line_is_instruction_section_heading(line))
    structured_instruction_heading_count = sum(1 for line in structured_instructions if line_is_instruction_section_heading(line))
    if fallback_instructions and (
        not structured_instructions
        or fallback_instruction_heading_count > structured_instruction_heading_count
        or len(fallback_instructions) >= len(structured_instructions) + 2
    ):
        instructions = fallback_instructions
    else:
        instructions = structured_instructions or fallback_instructions

    if not instructions and canonical_domain(explicit_link or '').endswith('substack.com'):
        loose_instructions = extract_loose_instruction_lines_from_visible_text(fallback_text, max_lines=40)
        if loose_instructions:
            instructions = loose_instructions

    compact_lines = [f"{explicit_link_label}: {explicit_link}"]
    if linked_page_title:
        compact_lines.append(linked_page_title)
    if linked_meta_description and not substack_preview:
        compact_lines.append(linked_meta_description)

    if ingredients:
        compact_lines.append('INGREDIENTS:')
        compact_lines.extend(ingredients[:60])
    if instructions:
        compact_lines.append('INSTRUCTIONS:')
        compact_lines.extend(instructions[:80])

    if not ingredients and not instructions:
        compact_lines.append(trim_text(linked_structured_html or linked_raw_page_text, 5000))

    return trim_text(combine_text_blocks(compact_lines), RAW_PAGE_TEXT_SUBMIT_MAX)


def merge_linked_page_evidence(
    base_evidence: dict,
    linked_evidence: dict,
    explicit_link: str,
    explicit_link_label: str = "LINKED_RECIPE_PAGE_URL",
) -> dict:
    linked_page_title = linked_evidence.get('page_title')
    linked_meta_description = linked_evidence.get('meta_description')
    linked_structured_html = linked_evidence.get('structured_html_text')
    linked_raw_page_text = linked_evidence.get('raw_page_text')

    compact_recipe_text = build_compact_recipe_block(linked_evidence, explicit_link, explicit_link_label)
    linked_context_text = trim_text(
        combine_text_blocks([
            linked_page_title,
            linked_meta_description,
            linked_structured_html,
            linked_raw_page_text,
        ]),
        RAW_PAGE_TEXT_SUBMIT_MAX,
    )

    merged_raw_page_text = trim_text(
        combine_text_blocks([
            compact_recipe_text,
            linked_context_text,
        ]),
        RAW_PAGE_TEXT_SUBMIT_MAX,
    )

    merged_expanded_caption_text = trim_text(
        compact_recipe_text,
        EXPANDED_CAPTION_SUBMIT_MAX,
    )

    substack_preview = is_substack_preview_like_evidence(
        explicit_link,
        linked_meta_description,
        linked_raw_page_text,
        linked_page_title,
    )

    merged_meta_description = trim_text(
        choose_first_non_empty(base_evidence.get('meta_description'), '' if substack_preview else linked_meta_description),
        META_DESCRIPTION_SUBMIT_MAX,
    )

    merged_visible_before_expand = trim_text(
        choose_first_non_empty(linked_page_title, base_evidence.get('visible_text_before_expand')),
        VISIBLE_TEXT_BEFORE_SUBMIT_MAX,
    )

    merged_visible_after_expand = trim_text(
        compact_recipe_text,
        VISIBLE_TEXT_AFTER_SUBMIT_MAX,
    )

    merged_transcript_text = trim_text(
        compact_recipe_text,
        SUBMIT_TRANSCRIPT_MAX_LEN,
    )

    merged_page_title = trim_text(
        choose_first_non_empty(linked_page_title, base_evidence.get('page_title')),
        PAGE_TITLE_SUBMIT_MAX,
    )

    merged_page_html = trim_text(
        choose_first_non_empty(
            linked_structured_html,
            decode_htmlish(linked_evidence.get('page_html') or ''),
            base_evidence.get('page_html') or '',
        ),
        PAGE_HTML_MAX_LEN,
    )

    merged = dict(base_evidence)
    merged.update({
        'page_title': merged_page_title,
        'raw_page_text': merged_raw_page_text,
        'expanded_caption_text': merged_expanded_caption_text,
        'transcript_text': merged_transcript_text,
        'meta_description': merged_meta_description,
        'page_html': merged_page_html,
        'page_image_url': choose_first_non_empty(linked_evidence.get('page_image_url'), base_evidence.get('page_image_url')),
        'media_type_guess': 'page',
        'is_video': False,
        'video_url': '',
        'effective_page_url': explicit_link,
        'visible_text_before_expand': merged_visible_before_expand,
        'visible_text_after_expand': merged_visible_after_expand,
        'visible_page_text': linked_evidence.get('visible_page_text') or base_evidence.get('visible_page_text') or '',
        'structured_html_text': linked_structured_html or base_evidence.get('structured_html_text') or '',
        'structured_html_text_len': len(linked_structured_html or ''),
        'visible_page_text_len': len(linked_evidence.get('visible_page_text') or ''),
        'page_html_was_skipped': linked_evidence.get('page_html_was_skipped', False),
        'page_html_raw_len': linked_evidence.get('page_html_raw_len', 0),
        'explicit_recipe_link': explicit_link,
        'linked_recipe_used': True,
        'current_page_is_youtube_shorts': False,
    })
    return merged


def merge_instagram_linked_page_evidence(instagram_evidence: dict, linked_evidence: dict, explicit_link: str) -> dict:
    return merge_linked_page_evidence(
        instagram_evidence,
        linked_evidence,
        explicit_link,
        explicit_link_label="EXTERNAL_RECIPE_PAGE_URL",
    )


def merge_tiktok_linked_page_evidence(tiktok_evidence: dict, linked_evidence: dict, explicit_link: str) -> dict:
    return merge_linked_page_evidence(
        tiktok_evidence,
        linked_evidence,
        explicit_link,
        explicit_link_label="EXTERNAL_RECIPE_PAGE_URL",
    )


def source_safe_text(value: str, max_len: int = 500) -> str:
    return trim_text(normalize_text_preserve_lines(value), max_len)


def source_safe_handle(value: str) -> str:
    handle = str(value or "").strip()
    if not handle:
        return ""
    handle = handle.lstrip("@")
    if not handle:
        return ""
    return trim_text(f"@{handle}", 200)


def source_slug(value: str) -> str:
    raw = unicodedata.normalize("NFKC", str(value or "")).lower().strip()
    raw = re.sub(r"[@#]+", " ", raw)
    raw = re.sub(r"[^\w\s-]", " ", raw, flags=re.UNICODE)
    raw = re.sub(r"[\s_-]+", "_", raw).strip("_")
    return raw


def canonical_domain(url_or_host: str) -> str:
    value = str(url_or_host or "").strip()
    if not value:
        return ""
    try:
        parsed = urlparse(value if "://" in value else f"https://{value}")
        host = (parsed.netloc or parsed.path or "").lower()
    except Exception:
        host = value.lower()
    host = host.replace("www.", "").strip().strip(".")
    return host.split(":")[0]


def normalize_profile_url(url: str, base_url: str = "") -> str:
    raw = str(url or "").strip()
    if not raw:
        return ""
    try:
        if raw.startswith("//"):
            raw = f"https:{raw}"
        elif raw.startswith("/") and base_url:
            raw = urljoin(base_url, raw)
        elif not re.match(r"^https?://", raw, flags=re.IGNORECASE) and base_url:
            raw = urljoin(base_url, raw)
        if re.match(r"^https?://", raw, flags=re.IGNORECASE):
            return trim_text(raw, 2000)
    except Exception:
        return ""
    return ""


def build_source_channel_key(platform: str, handle: str, channel_name: str, page_domain: str) -> str:
    platform = (platform or "").strip().lower()
    handle_slug = source_slug(str(handle or "").lstrip("@"))
    channel_slug = source_slug(channel_name)
    domain_slug = source_slug(str(page_domain or "").split(".")[0])
    if platform and handle_slug:
        return f"{platform}:{handle_slug}"
    if platform and channel_slug:
        return f"{platform}:{channel_slug}"
    if platform and domain_slug:
        return f"{platform}:{domain_slug}"
    return ""


def build_creator_group_key(handle: str, creator_name: str, channel_name: str, page_domain: str, platform: str) -> str:
    handle_slug = source_slug(str(handle or "").lstrip("@"))
    creator_slug = source_slug(creator_name)
    channel_slug = source_slug(channel_name)
    domain_slug = source_slug(str(page_domain or "").split(".")[0])
    if handle_slug:
        return handle_slug
    if creator_slug:
        return creator_slug
    if channel_slug:
        return channel_slug
    if domain_slug and (platform or "").lower() == "web":
        return domain_slug
    return ""


def enrich_source_metadata(metadata: dict, platform_hint: str = "", target_url: str = "") -> dict:
    metadata = dict(metadata or {})
    source_platform = (metadata.get("source_platform") or platform_hint or detect_platform_from_url(target_url)).strip().lower()
    source_profile_url = normalize_profile_url(metadata.get("source_profile_url") or "", target_url)
    source_page_domain = canonical_domain(metadata.get("source_page_domain") or source_profile_url or target_url)
    source_creator_handle = source_safe_handle(metadata.get("source_creator_handle") or "")
    source_channel_name = source_safe_text(metadata.get("source_channel_name") or metadata.get("source_creator_name") or "")
    source_creator_name = source_safe_text(metadata.get("source_creator_name") or source_channel_name)
    source_channel_key = source_safe_text(
        metadata.get("source_channel_key") or build_source_channel_key(source_platform, source_creator_handle, source_channel_name, source_page_domain),
        300,
    )
    creator_group_key = source_safe_text(
        metadata.get("creator_group_key") or build_creator_group_key(source_creator_handle, source_creator_name, source_channel_name, source_page_domain, source_platform),
        300,
    )
    source_avatar_url = normalize_profile_url(metadata.get("source_avatar_url") or "", target_url)
    return {
        "source_platform": source_platform or "",
        "source_creator_name": source_creator_name or "",
        "source_creator_handle": source_creator_handle or "",
        "source_channel_name": source_channel_name or "",
        "source_channel_key": source_channel_key or "",
        "source_profile_url": source_profile_url or "",
        "source_page_domain": source_page_domain or "",
        "creator_group_key": creator_group_key or "",
        "source_avatar_url": source_avatar_url or "",
    }


async def extract_anchor_candidates(page, selectors=None, limit: int = 120):
    selectors = selectors or ["a[href]"]
    out = []
    seen = set()
    for selector in selectors:
        try:
            locator = page.locator(selector)
            count = min(await locator.count(), limit)
            for i in range(count):
                candidate = locator.nth(i)
                try:
                    href = await candidate.get_attribute("href", timeout=500)
                except Exception:
                    href = None
                if not href:
                    continue
                try:
                    text = await candidate.inner_text(timeout=500)
                except Exception:
                    try:
                        text = await candidate.text_content(timeout=500)
                    except Exception:
                        text = ""
                key = f"{href}|||{normalize_text(text)}"
                if key in seen:
                    continue
                seen.add(key)
                out.append({"href": href, "text": normalize_text(text)})
                if len(out) >= limit:
                    return out
        except Exception:
            continue
    return out


def choose_profile_candidate(platform: str, candidates, base_url: str = "") -> dict:
    best = {}
    best_score = -1
    for candidate in candidates or []:
        href = normalize_profile_url(candidate.get("href") or "", base_url)
        text = source_safe_text(candidate.get("text") or "")
        if not href:
            continue
        parsed = urlparse(href)
        parts = [part for part in (parsed.path or "").split("/") if part]
        first = parts[0].lower() if parts else ""
        score = 0
        if platform == "youtube":
            if first.startswith("@"):
                score += 120
            elif first in {"channel", "user", "c"} and len(parts) >= 2:
                score += 100
            if text and text.lower() != "youtube":
                score += 30
        elif platform == "instagram":
            if canonical_domain(href) != "instagram.com":
                continue
            if len(parts) == 1 and first not in SOCIAL_PROFILE_RESERVED_PATHS:
                score += 120
            if text and not is_generic_source_label(text):
                score += 20
        elif platform == "tiktok":
            if first.startswith("@"):
                score += 120
            if text and text.lower() != "tiktok":
                score += 20
        elif platform == "facebook":
            if (len(parts) == 1 and first not in SOCIAL_PROFILE_RESERVED_PATHS) or first == "profile.php":
                score += 90
            if text and text.lower() != "facebook":
                score += 20
        else:
            if text:
                score += 10
        if score > best_score:
            best_score = score
            best = {"href": href, "text": text}
    return best


def extract_meta_tag(html: str, keys) -> str:
    source = str(html or "")
    if not source:
        return ""
    for key in keys:
        patterns = [
            re.compile(rf'<meta[^>]+property=["\']{re.escape(key)}["\'][^>]+content=["\']([^"\']+)["\']', re.IGNORECASE),
            re.compile(rf'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']{re.escape(key)}["\']', re.IGNORECASE),
            re.compile(rf'<meta[^>]+name=["\']{re.escape(key)}["\'][^>]+content=["\']([^"\']+)["\']', re.IGNORECASE),
            re.compile(rf'<meta[^>]+content=["\']([^"\']+)["\'][^>]+name=["\']{re.escape(key)}["\']', re.IGNORECASE),
        ]
        for pattern in patterns:
            match = pattern.search(source)
            if match and match.group(1):
                return source_safe_text(html_lib.unescape(match.group(1)))
    return ""


def decode_htmlish(value: str) -> str:
    return html_lib.unescape(str(value or "")).replace('\\u0026', '&').replace('\\u003d', '=').replace('\\u002F', '/').replace('\\/', '/')


def parse_json_ld_names(html: str) -> dict:
    source = str(html or "")
    result = {"author_name": "", "publisher_name": "", "profile_url": ""}
    if not source:
        return result
    scripts = re.findall(r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>([\s\S]*?)</script>', source, flags=re.IGNORECASE)
    for raw_script in scripts[:20]:
        raw_script = raw_script.strip()
        if not raw_script:
            continue
        try:
            parsed = json.loads(raw_script)
        except Exception:
            continue
        nodes = parsed if isinstance(parsed, list) else [parsed]
        for node in nodes[:40]:
            if not isinstance(node, dict):
                continue
            for key, out_key in (("author", "author_name"), ("publisher", "publisher_name")):
                value = node.get(key)
                if isinstance(value, dict):
                    if not result[out_key] and value.get("name"):
                        result[out_key] = source_safe_text(value.get("name"))
                    if not result["profile_url"] and value.get("url"):
                        result["profile_url"] = normalize_profile_url(value.get("url"))
                elif isinstance(value, list):
                    for item in value:
                        if isinstance(item, dict):
                            if not result[out_key] and item.get("name"):
                                result[out_key] = source_safe_text(item.get("name"))
                            if not result["profile_url"] and item.get("url"):
                                result["profile_url"] = normalize_profile_url(item.get("url"))
            if not result["profile_url"] and isinstance(node.get("mainEntityOfPage"), dict):
                result["profile_url"] = normalize_profile_url(node.get("mainEntityOfPage", {}).get("@id") or node.get("mainEntityOfPage", {}).get("url") or "")
    return result


def parse_youtube_channel_from_title(page_title: str) -> str:
    title = source_safe_text(page_title)
    if not title:
        return ""
    title = re.sub(r'\s*-\s*youtube\s*$', '', title, flags=re.IGNORECASE)
    parts = [part.strip() for part in title.split('|') if part.strip()]
    if len(parts) >= 2:
        return source_safe_text(parts[-1])
    return ""


def parse_youtube_html_source(html: str) -> dict:
    source = str(html or "")
    out = {"channel_name": "", "profile_url": "", "handle": "", "avatar_url": ""}
    for pattern, key in [
        (r'"ownerChannelName":"((?:\\.|[^"\\])*)"', 'channel_name'),
        (r'"channelName":"((?:\\.|[^"\\])*)"', 'channel_name'),
        (r'"ownerProfileUrl":"((?:\\.|[^"\\])*)"', 'profile_url'),
        (r'"canonicalBaseUrl":"((?:\\.|[^"\\])*)"', 'profile_url'),
        (r'"vanityChannelUrl":"((?:\\.|[^"\\])*)"', 'profile_url'),
    ]:
        match = re.search(pattern, source)
        if match and match.group(1):
            value = decode_htmlish(match.group(1))
            if key == 'profile_url':
                out[key] = normalize_profile_url(value, 'https://www.youtube.com') or out[key]
            else:
                out[key] = source_safe_text(value) or out[key]
    if out["profile_url"]:
        parts = [part for part in urlparse(out["profile_url"]).path.split('/') if part]
        if parts and parts[0].startswith('@'):
            out["handle"] = source_safe_handle(parts[0])

    decoded_source = decode_htmlish(source)
    avatar_patterns = [
        r'https://yt3\.ggpht\.com/[^"\'\s<>()]+',
        r'"avatar\":\{[^\{]{0,400}?"thumbnails\":\[\{"url":"((?:\\.|[^"\\])*)"',
        r'"channelAvatar(?:_thumb)?":"((?:\\.|[^"\\])*)"',
    ]
    for pattern in avatar_patterns:
        match = re.search(pattern, decoded_source) or re.search(pattern, source)
        if match:
            value = match.group(1) if match.groups() else match.group(0)
            avatar_url = normalize_profile_url(decode_htmlish(value), 'https://www.youtube.com')
            if avatar_url:
                out['avatar_url'] = avatar_url
                break

    return out


def extract_instagram_profile_from_any_url(url: str) -> str:
    normalized = normalize_profile_url(url or '', 'https://www.instagram.com')
    if not normalized:
        return ''
    try:
        parsed = urlparse(normalized)
        host = canonical_domain(normalized)
        if host != 'instagram.com':
            return ''
        parts = [part for part in (parsed.path or '').split('/') if part]
        if not parts:
            return ''
        handle = parts[0]
        if handle.lower() in SOCIAL_PROFILE_RESERVED_PATHS:
            return ''
        return f'https://www.instagram.com/{handle}/'
    except Exception:
        return ''


def parse_instagram_html_source(html: str) -> dict:
    source = str(html or "")
    out = {"handle": "", "channel_name": "", "profile_url": "", "avatar_url": ""}

    for pattern in [
        r'"owner_username":"([^"\\]+)"',
        r'"username":"([^"\\]+)"',
        r'instagram://user\?username=([^"&\\]+)',
    ]:
        match = re.search(pattern, source)
        if match and match.group(1):
            handle = source_safe_handle(match.group(1))
            if handle and handle.lstrip('@').lower() != 'instagram':
                out["handle"] = handle
                out["channel_name"] = source_safe_text(handle.lstrip('@'))
                out["profile_url"] = f"https://www.instagram.com/{handle.lstrip('@')}/"
                break

    meta_profile_url = extract_instagram_profile_from_any_url(extract_meta_tag(source, ['og:url']))
    if meta_profile_url:
        out['profile_url'] = meta_profile_url or out['profile_url']
        if not out['handle']:
            parts = [part for part in urlparse(meta_profile_url).path.split('/') if part]
            if parts:
                out['handle'] = source_safe_handle(parts[0])
        if out['handle'] and not out['channel_name']:
            out['channel_name'] = source_safe_text(out['handle'].lstrip('@'))

    # Meta description often contains the owner/footer in a more trustworthy form than noisy anchors.
    meta_description = extract_meta_tag(source, ['og:description', 'twitter:description', 'description'])
    owner_hint = extract_instagram_owner_hint(meta_description)
    if owner_hint.get('handle'):
        out['handle'] = source_safe_handle(owner_hint.get('handle')) or out['handle']
        out['profile_url'] = f"https://www.instagram.com/{out['handle'].lstrip('@')}/"
        if owner_hint.get('display_name'):
            out['channel_name'] = source_safe_text(owner_hint.get('display_name')) or out['channel_name']
        elif out['handle'] and not out['channel_name']:
            out['channel_name'] = source_safe_text(out['handle'].lstrip('@'))

    avatar_match = re.search(r'\"profile_pic_url(?:_hd)?\":\"((?:\\\.|[^\"\\])*)\"', source)
    if avatar_match and avatar_match.group(1):
        out['avatar_url'] = normalize_profile_url(decode_htmlish(avatar_match.group(1))) or ''

    return out


def parse_tiktok_html_source(html: str, target_url: str) -> dict:
    source = str(html or "")
    out = {
        "handle": "",
        "channel_name": "",
        "creator_name": "",
        "profile_url": "",
        "avatar_url": "",
        "external_site_url": "",
        "bio_text": "",
    }

    candidate_pairs = [
        (r'"uniqueId":"((?:\\.|[^"\\])*)"', "handle"),
        (r'"ownerUniqueId":"((?:\\.|[^"\\])*)"', "handle"),
        (r'"nickname":"((?:\\.|[^"\\])*)"', "creator_name"),
        (r'"ownerNickname":"((?:\\.|[^"\\])*)"', "creator_name"),
        (r'"signature":"((?:\\.|[^"\\])*)"', "bio_text"),
    ]
    for pattern, key in candidate_pairs:
        match = re.search(pattern, source)
        if match and match.group(1):
            out[key] = source_safe_text(decode_htmlish(match.group(1))) if key != "handle" else source_safe_handle(decode_htmlish(match.group(1)))

    meta_profile_url = normalize_profile_url(extract_meta_tag(source, ['og:url']) or '', 'https://www.tiktok.com')
    if meta_profile_url:
        out["profile_url"] = meta_profile_url

    if out["handle"] and not out["profile_url"]:
        out["profile_url"] = f"https://www.tiktok.com/@{out['handle'].lstrip('@')}"

    if not out["handle"]:
        parts = [part for part in urlparse(str(target_url or "")).path.split('/') if part]
        if parts and parts[0].startswith('@'):
            out["handle"] = source_safe_handle(parts[0])
            out["profile_url"] = f"https://www.tiktok.com/{parts[0]}"

    avatar_patterns = [
        r'"avatarLarger":"((?:\\.|[^"\\])*)"',
        r'"avatarMedium":"((?:\\.|[^"\\])*)"',
        r'"avatarThumb":"((?:\\.|[^"\\])*)"',
    ]
    avatar_candidates = []
    for pattern in avatar_patterns:
        avatar_candidates.extend([decode_htmlish(m.group(1)) for m in re.finditer(pattern, source) if m.group(1)])
    meta_avatar = extract_meta_tag(source, ['image_src', 'og:image', 'og:image:secure_url', 'twitter:image', 'twitter:image:src'])
    if meta_avatar:
        avatar_candidates.append(meta_avatar)
    out["avatar_url"] = normalize_profile_url(pick_best_source_avatar_url(avatar_candidates) or '', 'https://www.tiktok.com') or ''

    # Prefer dedicated bio-link fields when present.
    bio_link_patterns = [
        r'"bioLink"\s*:\s*\{[^{}]{0,1200}?"link"\s*:\s*"((?:\\.|[^"\\])*)"',
        r'"link"\s*:\s*"((?:https?:)?\\/\\/(?:\\.|[^"\\])*)"',
        r'"website"\s*:\s*"((?:https?:)?\\/\\/(?:\\.|[^"\\])*)"',
    ]
    link_candidates = []
    for pattern in bio_link_patterns:
        for match in re.finditer(pattern, source, flags=re.IGNORECASE):
            if match and match.group(1):
                url = normalize_profile_url(unwrap_known_redirect_url(decode_htmlish(match.group(1))), out["profile_url"] or target_url)
                if url:
                    link_candidates.append(url)

    decoded_source = decode_htmlish(source)
    for url in extract_urls_from_text(decoded_source):
        normalized = normalize_profile_url(unwrap_known_redirect_url(url), out["profile_url"] or target_url)
        if normalized:
            link_candidates.append(normalized)

    deduped = []
    seen = set()
    for candidate in link_candidates:
        candidate = normalize_profile_url(candidate, out["profile_url"] or target_url)
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        deduped.append(candidate)

    for candidate in deduped:
        if looks_like_fetchable_external_page(candidate):
            out["external_site_url"] = candidate
            break

    out["channel_name"] = out["creator_name"] or source_safe_text(out["handle"].lstrip('@'))
    return out




def pick_best_source_avatar_url(candidates) -> str:
    cleaned = []
    seen = set()
    for raw in candidates or []:
        candidate = normalize_profile_url(raw or "", "https://www.tiktok.com")
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        cleaned.append(candidate)

    if not cleaned:
        return ""

    preferred_tokens = ["avatar", "avatarlarger", "avatarmedium", "avatarthumb", "profile", "tiktokcdn", "byteimg"]
    for candidate in cleaned:
        lower = candidate.lower()
        if any(token in lower for token in preferred_tokens):
            return candidate

    return cleaned[0] if cleaned else ""


def looks_like_fetchable_external_page(url: str) -> bool:
    normalized = normalize_profile_url(url or "", url or "")
    if not normalized:
        return False
    try:
        parsed = urlparse(normalized)
        host = canonical_domain(normalized)
        if not is_external_site_host(host):
            return False
        if looks_like_non_page_asset_host(host, normalized):
            return False
        path = (parsed.path or "").lower()
        if re.search(r'\.(?:jpg|jpeg|png|webp|gif|svg|mp4|m3u8|js|css|woff2?|ttf|ico|json|xml)(?:$|\?)', path):
            return False
        if looks_like_non_recipe_internal_page_url(normalized):
            return False
        if not path.strip('/') and looks_like_non_page_asset_host(host, normalized):
            return False
        return True
    except Exception:
        return False


def fetch_remote_html_document(url: str, timeout: int = 20) -> dict:
    normalized = normalize_profile_url(url or "", url or "")
    if not normalized:
        return {"ok": False, "url": "", "html": "", "error": "missing_url"}

    try:
        request = urllib.request.Request(
            normalized,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
            },
        )
        with urllib.request.urlopen(request, timeout=timeout) as response:
            payload = response.read()
            final_url = response.geturl() or normalized
        html = payload.decode("utf-8", errors="ignore")
        return {"ok": True, "url": final_url, "html": html, "error": ""}
    except Exception as err:
        return {"ok": False, "url": normalized, "html": "", "error": f"{type(err).__name__}: {err}"}


def extract_html_title_tag(html: str) -> str:
    source = str(html or "")
    match = re.search(r"<title[^>]*>([\s\S]*?)</title>", source, flags=re.IGNORECASE)
    if match and match.group(1):
        return source_safe_text(html_lib.unescape(match.group(1)), PAGE_TITLE_SUBMIT_MAX)
    return ""



def extract_tiktok_external_site_candidates(
    html: str,
    base_url: str = "",
    page_title: str = "",
    context_prefix: str = "",
) -> list[dict]:
    source = str(html or "")
    if not source:
        return []

    candidates = []
    seen = set()

    def add_candidate(raw_url: str, context: str = "", source_name: str = "html") -> None:
        normalized = normalize_investigation_candidate_url(raw_url or "", base_url)
        if not normalized:
            return
        dedupe_key = investigation_candidate_dedupe_key(normalized)
        if dedupe_key in seen:
            return
        seen.add(dedupe_key)
        if not looks_like_fetchable_external_page(normalized):
            return

        combined_context = normalize_text_preserve_lines(
            combine_text_blocks([context_prefix, context])
        )
        score = score_recipe_link_candidate(normalized, combined_context, page_title, source=source_name)
        if score <= -1000:
            return

        parsed = urlparse(normalized)
        lower_context = combined_context.lower()
        if "bio" in lower_context or "website" in lower_context or "link in bio" in lower_context:
            score += 25
        if "full recipe" in lower_context or "recipe linked" in lower_context:
            score += 20
        if source_name == "meta_candidate":
            score += 20
        if parsed.path and parsed.path not in {"/", ""}:
            score += 8

        candidates.append({
            "url": normalized,
            "score": score,
            "source": source_name,
            "context": combined_context,
            "host": canonical_domain(normalized),
        })

    for pattern in [
        r'"bioLink"\s*:\s*\{[^{}]{0,2000}?"link"\s*:\s*"((?:\\.|[^"\\])*)"',
        r'"website"\s*:\s*"((?:https?:)?\\/\\/(?:\\.|[^"\\])*)"',
        r'"externalUrl"\s*:\s*"((?:https?:)?\\/\\/(?:\\.|[^"\\])*)"',
        r'"bioUrl"\s*:\s*"((?:https?:)?\\/\\/(?:\\.|[^"\\])*)"',
    ]:
        for match in re.finditer(pattern, source, flags=re.IGNORECASE):
            if match and match.group(1):
                add_candidate(decode_htmlish(match.group(1)), "bio link", "description_marker")

    decoded = decode_htmlish(source)
    for href_match in re.finditer(r'href=["\']([^"\']+)["\']', decoded, flags=re.IGNORECASE):
        if href_match and href_match.group(1):
            add_candidate(href_match.group(1), "href", "dom_anchor")

    for extracted in extract_urls_with_context(decoded):
        add_candidate(extracted.get("url") or "", extracted.get("context") or "", "html_anchor")

    return sorted(candidates, key=lambda item: item["score"], reverse=True)


TIKTOK_DOMAIN_AFFINITY_STOPWORDS = {
    "tiktok",
    "recipe",
    "recipes",
    "official",
    "profile",
    "home",
    "blog",
    "link",
    "bio",
    "food",
    "kitchen",
}


TIKTOK_INTERMEDIATE_LANDING_HOSTS = set(INSTAGRAM_EXTERNAL_SHORTENER_HOSTS) | {
    "bitly.com",
    "linktr.ee",
    "mcs.tiktokw.us",
    "tiktokw.us",
    "tiktokv.us",
    "tiktokcdn-us.com",
    "tiktokcdn.com",
    "googleapis.com",
    "gstatic.com",
}

TIKTOK_INTERMEDIATE_LANDING_TITLE_MARKERS = (
    "landing page",
    "all my links",
    "link in bio",
    "my website",
    "website",
)


TIKTOK_RENDERED_LINKHUB_GOTO_TIMEOUT_MS = int(
    os.getenv("TIKTOK_RENDERED_LINKHUB_GOTO_TIMEOUT_MS", "12000").strip() or "12000"
)
TIKTOK_RENDERED_LINKHUB_WAIT_MS = int(
    os.getenv("TIKTOK_RENDERED_LINKHUB_WAIT_MS", "1800").strip() or "1800"
)


TIKTOK_HARD_NON_RECIPE_PATH_MARKERS = (
    "/wp-admin/admin-ajax.php",
    "/product/",
    "/products/",
    "/shop",
    "/cart",
    "/cart1",
    "/checkout",
    "/my-account",
    "/account",
    "/wishlist",
)

TIKTOK_SOFT_OFFER_PATH_MARKERS = (
    "/course",
    "/courses",
    "/class",
    "/classes",
    "/workshop",
    "/academy",
    "/subscribe",
    "/subscription",
    "/subscriptions",
    "/membership",
    "/memberships",
    "/member",
    "/members",
    "/community",
    "/join",
    "/club",
    "/vip",
    "/book",
    "/books",
    "/ebook",
    "/weekly-recipes",
    "/weeklyrecipes",
    "קורס",
)

TIKTOK_SOFT_OFFER_TEXT_MARKERS = (
    "weekly recipes",
    "subscription",
    "subscribe",
    "membership",
    "member",
    "members only",
    "community",
    "join the community",
    "join our community",
    "join now",
    "vip",
    "club",
    "buy now",
    "purchase",
    "shop now",
    "cookbook",
    "ebook",
    "book",
    "course",
    "class",
    "workshop",
    "academy",
    "מנוי",
    "מנויים",
    "הרשמה",
    "רכישה",
    "ספר מתכונים",
    "שבוע עם",
    "הצטרפות",
    "הצטרפו",
    "קהילה",
    "קהילת",
    "מועדון",
)

TIKTOK_RECIPE_STRUCTURE_MARKERS = (
    "ingredients",
    "instructions",
    "directions",
    "method",
    "how to make",
    "recipe",
    "מצרכים",
    "רכיבים",
    "אופן הכנה",
    "הוראות",
)


TIKTOK_RECIPE_FOCUS_STOPWORDS = {
    "tiktok",
    "recipe",
    "recipes",
    "full",
    "website",
    "link",
    "bio",
    "blog",
    "video",
    "hen",
    "heninthekitchen",
    "kitchen",
    "kitchencom",
    "food",
    "cooking",
    "home",
    "community",
    "join",
    "weekly",
    "weeklyrecipes",
    "membership",
    "subscribe",
    "club",
    "vip",
    "book",
    "books",
    "course",
    "courses",
    "class",
    "classes",
    "workshop",
    "academy",
    "מתכון",
    "מתכונים",
    "המתכון",
    "המלא",
    "מלא",
    "באתר",
    "באתר שלי",
    "בביו",
    "לינק",
    "קישור",
    "שלי",
    "חן",
    "במטבח",
    "קהילה",
    "קהילת",
    "הצטרפות",
    "מועדון",
    "שבוע",
    "שבוע עם",
    "ביו",
}

def looks_like_tiktok_hard_non_recipe_candidate_url(url: str) -> bool:
    normalized = normalize_profile_url(url or "", url or "")
    if not normalized:
        return False
    try:
        parsed = urlparse(normalized)
        host = canonical_domain(normalized)
        path = (parsed.path or "").lower().rstrip("/")
    except Exception:
        return False
    if any(
        host == blocked or host.endswith(f".{blocked}")
        for blocked in (
            "mcs.tiktokw.us",
            "tiktokw.us",
            "tiktokv.us",
            "tiktokcdn-us.com",
            "tiktokcdn.com",
            "googleapis.com",
            "gstatic.com",
        )
    ):
        return True
    if not path:
        return False
    if any(marker in path for marker in TIKTOK_HARD_NON_RECIPE_PATH_MARKERS):
        return True
    return False


def looks_like_tiktok_offer_like_page(
    url: str,
    linked_evidence: dict | None = None,
    linked_metrics: dict | None = None,
) -> bool:
    normalized = normalize_profile_url(url or "", url or "")
    if not normalized:
        return False

    if looks_like_tiktok_hard_non_recipe_candidate_url(normalized):
        return True

    evidence = linked_evidence or {}
    metrics = linked_metrics or {}

    try:
        parsed = urlparse(normalized)
        host = canonical_domain(normalized)
        path = (parsed.path or "").lower().rstrip("/")
    except Exception:
        host = canonical_domain(normalized)
        path = ""

    title = normalize_text_preserve_lines(evidence.get("page_title") or "").lower()
    meta = normalize_text_preserve_lines(evidence.get("meta_description") or "").lower()
    raw = normalize_text_preserve_lines(evidence.get("raw_page_text") or "").lower()
    combined = "\n".join([title, meta, raw[:2500]])

    strong_recipe_structure = sum(1 for marker in TIKTOK_RECIPE_STRUCTURE_MARKERS if marker in combined) >= 2
    dense_recipe_signal = bool(metrics.get("looksRecipeDense")) or (
        int(metrics.get("measurementSignalCount") or 0) >= 2 and int(metrics.get("recipeVerbSignalCount") or 0) >= 2
    )

    if host.startswith("book.") and not dense_recipe_signal:
        return True

    if any(marker in path for marker in TIKTOK_SOFT_OFFER_PATH_MARKERS):
        if not dense_recipe_signal or not strong_recipe_structure:
            return True

    if any(marker in combined for marker in TIKTOK_SOFT_OFFER_TEXT_MARKERS):
        if not dense_recipe_signal and not strong_recipe_structure:
            return True

    if looks_like_non_recipe_offer_page(
        normalized,
        evidence.get("page_title") or "",
        evidence.get("meta_description") or "",
        raw,
    ):
        return True

    return False


def get_tiktok_candidate_final_url(candidate_url: str, linked_evidence: dict | None = None) -> str:
    candidate = normalize_investigation_candidate_url(candidate_url or "", candidate_url or "")
    evidence = linked_evidence or {}
    effective = normalize_investigation_candidate_url(
        evidence.get("effective_page_url") or candidate or "",
        candidate or candidate_url or "",
    )
    return effective or candidate or ""


def extract_tiktok_domain_affinity_tokens(source_metadata: dict | None, creator_name: str = "", handle: str = "") -> list[str]:
    raw_values = [
        handle or "",
        creator_name or "",
        source_metadata.get("source_creator_handle") if isinstance(source_metadata, dict) else "",
        source_metadata.get("source_creator_name") if isinstance(source_metadata, dict) else "",
        source_metadata.get("source_channel_name") if isinstance(source_metadata, dict) else "",
        source_metadata.get("source_profile_url") if isinstance(source_metadata, dict) else "",
    ]

    tokens = []
    seen = set()
    for raw_value in raw_values:
        value = normalize_text_preserve_lines(raw_value).lower()
        if not value:
            continue

        compact = re.sub(r'[^a-z0-9]+', '', value)
        if compact and len(compact) >= 4 and compact not in seen and compact not in TIKTOK_DOMAIN_AFFINITY_STOPWORDS:
            seen.add(compact)
            tokens.append(compact)

        for token in re.findall(r'[a-z0-9]{4,}', value):
            if token in seen or token in TIKTOK_DOMAIN_AFFINITY_STOPWORDS:
                continue
            seen.add(token)
            tokens.append(token)

    return tokens


def extract_tiktok_recipe_focus_tokens(
    source_text: str,
    affinity_tokens: list[str] | None = None,
    max_tokens: int = 8,
) -> list[str]:
    affinity_seen = {str(token or '').strip().lower() for token in (affinity_tokens or []) if token}
    for token in list(affinity_seen):
        if len(token) >= 6:
            affinity_seen.add(token[-7:])
            affinity_seen.add(token[-6:])
            affinity_seen.add(token[-5:])
    source = decode_htmlish(normalize_text_preserve_lines(source_text or '').lower())
    if not source:
        return []

    source = re.sub(r'https?://\S+', ' ', source)
    source = re.sub(r'[#@]', ' ', source)

    extra_stopwords = {
        'delicious', 'yummy', 'amazing', 'tasty', 'easy', 'quick', 'simple',
        'kitchen', 'ekitchen', 'inthekitchen', 'heninthe', 'websitehint', 'biohint',
    }

    hebrew_scored = []
    latin_scored = []
    seen = set()
    for token in re.findall(r'[a-z]{3,}|[֐-׿]{2,}', source):
        cleaned = str(token or '').strip().lower()
        if not cleaned:
            continue
        if cleaned in seen or cleaned in affinity_seen or cleaned in TIKTOK_RECIPE_FOCUS_STOPWORDS or cleaned in extra_stopwords:
            continue
        if cleaned.isdigit():
            continue
        seen.add(cleaned)
        score = len(cleaned)
        is_hebrew = bool(re.search(r'[֐-׿]', cleaned))
        if is_hebrew:
            score += 8
        if len(cleaned) >= 5:
            score += 2
        if len(cleaned) >= 8:
            score += 1
        bucket = hebrew_scored if is_hebrew else latin_scored
        bucket.append((score, cleaned))

    hebrew_scored.sort(key=lambda item: (-item[0], item[1]))
    latin_scored.sort(key=lambda item: (-item[0], item[1]))

    limit = max(1, int(max_tokens or 0))
    if len(hebrew_scored) >= 2:
        return [token for _, token in hebrew_scored[:limit]]

    merged = hebrew_scored + latin_scored
    merged.sort(key=lambda item: (-item[0], item[1]))
    return [token for _, token in merged[:limit]]


TIKTOK_RECIPE_TITLE_CUE_MARKERS = [
    "full recipe",
    "recipe on my website",
    "recipe on my site",
    "see the link in my bio",
    "link in my bio",
    "website",
    "blog",
    "comment",
    "dm me",
    "message me",
    "email me",
    "בקישור",
    "בביו",
    "באתר",
    "בבלוג",
    "המתכון",
    "הסרטון",
    "למתכון",
]

TIKTOK_TITLE_LINE_DROP_PREFIXES = (
    "how ",
    "i ",
    "and ",
    "it ",
    "the ",
    "a ",
    "an ",
    "this ",
    "that ",
)

TIKTOK_TITLE_LINE_DROP_SUBSTRINGS = (
    "free gifts",
    "link in bio",
    "full recipe",
    "website",
    "my website",
    "full video",
    "see original",
    "add comment",
)

TIKTOK_TITLE_LINE_DROP_TIME_PATTERNS = (
    r'\b\d+\s*(?:d|h|m|s|day|days|week|weeks|hour|hours|min|mins|minute|minutes|sec|secs|second|seconds)\b',
    r'\bago\b',
)

TIKTOK_TITLE_FOOD_MARKERS = (
    "stew", "meat", "potato", "pomegranate", "cake", "bread", "soup", "salad", "pasta", "chicken", "beef", "recipe",
    "תבשיל", "בשר", "תפוח", "אדמה", "רימונים", "עוף", "עוג", "פסטה", "סלט", "מרק", "מתכון",
)

def clean_tiktok_title_line(value: str) -> str:
    line = normalize_text_preserve_lines(decode_htmlish(value or ''))
    if not line:
        return ''
    line = re.sub(r'https?://\S+', ' ', line)
    line = re.sub(r'[#@][^\s]+', ' ', line)
    line = re.sub(r'\b(?:heninthekitchen|hen in the kitchen|tiktok|instagram|see original|add comment)\b', ' ', line, flags=re.IGNORECASE)
    for pattern in TIKTOK_TITLE_LINE_DROP_TIME_PATTERNS:
        line = re.sub(pattern, ' ', line, flags=re.IGNORECASE)
    line = re.sub(r'\b\d+\b', ' ', line)
    line = re.sub(r'[|•·]+', ' ', line)
    line = re.sub(r'\s+', ' ', line).strip(" \t-–—|•:;.,()[]{}")
    return line

def looks_like_tiktok_title_line(value: str) -> bool:
    line = clean_tiktok_title_line(value)
    if not line:
        return False
    lowered = line.lower()
    if any(marker in lowered for marker in TIKTOK_TITLE_LINE_DROP_SUBSTRINGS):
        return False
    if any(re.search(pattern, lowered, flags=re.IGNORECASE) for pattern in TIKTOK_TITLE_LINE_DROP_TIME_PATTERNS):
        return False
    if any(lowered.startswith(prefix) for prefix in TIKTOK_TITLE_LINE_DROP_PREFIXES):
        return False
    token_count = len(re.findall(r'[A-Za-z0-9]+|[֐-׿]+', line))
    if token_count < 1 or len(line) < 4:
        return False
    alphaish = len(re.findall(r'[A-Za-z֐-׿]', line))
    if alphaish < max(3, int(len(line) * 0.35)):
        return False
    return True

def score_tiktok_title_line(value: str) -> int:
    line = clean_tiktok_title_line(value)
    if not line:
        return -1000
    lowered = line.lower()
    token_count = len(re.findall(r'[A-Za-z0-9]+|[֐-׿]+', line))
    score = min(len(line), 90)
    if re.search(r'[֐-׿]', line):
        score += 60
    if token_count >= 3:
        score += 25
    if token_count >= 5:
        score += 15
    if any(marker in lowered for marker in TIKTOK_TITLE_FOOD_MARKERS):
        score += 40
    if any(lowered.startswith(prefix) for prefix in TIKTOK_TITLE_LINE_DROP_PREFIXES):
        score -= 120
    return score

def extract_tiktok_preclue_title_phrase(source_text: str) -> str:
    text = normalize_text_preserve_lines(decode_htmlish(source_text or ''))
    if not text:
        return ''

    lines = [line for line in (clean_tiktok_title_line(part) for part in re.split(r'[\r\n]+', text)) if line]
    if not lines:
        return ''

    clue_index = None
    lowered_lines = [line.lower() for line in lines]
    for idx, lowered in enumerate(lowered_lines):
        if any(marker in lowered for marker in TIKTOK_RECIPE_TITLE_CUE_MARKERS):
            clue_index = idx
            break

    candidate_lines = []
    if clue_index is not None:
        for idx in range(max(0, clue_index - 3), clue_index + 1):
            line = lines[idx]
            stripped = strip_tiktok_recipe_title_clue_suffix(line)
            if looks_like_tiktok_title_line(stripped):
                candidate_lines.append(stripped)
    else:
        for line in lines[:4]:
            stripped = strip_tiktok_recipe_title_clue_suffix(line)
            if looks_like_tiktok_title_line(stripped):
                candidate_lines.append(stripped)

    if not candidate_lines:
        scored = sorted(
            ((score_tiktok_title_line(line), strip_tiktok_recipe_title_clue_suffix(line)) for line in lines),
            key=lambda item: (-item[0], item[1]),
        )
        for score, phrase in scored:
            if score <= 0 or not looks_like_tiktok_title_line(phrase):
                continue
            candidate_lines = [phrase]
            break

    if not candidate_lines:
        return ''

    merged = ' '.join([part for part in candidate_lines if part]).strip()
    merged = re.sub(r'\s+', ' ', merged).strip(" \t-–—|•:;.,()[]{}")
    if len(merged) > 140:
        merged = merged[:140].rsplit(' ', 1)[0].strip()
    return merged


def recipe_title_hint_from_recipe_row(recipe: dict | None) -> str:
    if not isinstance(recipe, dict):
        return ''

    for key in ('title', 'ai_extracted_dish_name', 'dish_name'):
        value = clean_tiktok_title_line(str(recipe.get(key) or ''))
        if value:
            return strip_tiktok_recipe_title_clue_suffix(value)

    return ''


def pick_tiktok_same_host_title_hint(title_hint: str, source_text: str = "") -> str:
    direct_hint = strip_tiktok_recipe_title_clue_suffix(title_hint or "")
    phrase_source = combine_text_blocks([source_text])
    inferred = extract_tiktok_preclue_title_phrase(phrase_source)

    direct_valid = looks_like_tiktok_title_line(direct_hint)
    inferred_valid = looks_like_tiktok_title_line(inferred)

    if inferred_valid and direct_valid:
        inferred_match = compare_query_to_candidate_phrase(inferred, direct_hint)
        direct_match = compare_query_to_candidate_phrase(direct_hint, inferred)
        same_title = (
            inferred_match.get('exact')
            or direct_match.get('exact')
            or (inferred_match.get('prefix') and inferred_match.get('overlap', 0) >= 2)
            or (direct_match.get('prefix') and direct_match.get('overlap', 0) >= 2)
        )
        if same_title:
            return inferred if len(inferred) >= len(direct_hint) else direct_hint
        return inferred

    if inferred_valid:
        return inferred
    if direct_valid:
        return direct_hint

    return direct_hint or inferred or ''

def strip_tiktok_recipe_title_clue_suffix(text: str) -> str:
    candidate = normalize_text_preserve_lines(decode_htmlish(text or ''))
    if not candidate:
        return ''

    candidate = re.sub(r'(?i)^day\s*\d+\s*[:\-–—]?\s*', '', candidate).strip()
    lowered = candidate.lower()
    cut_at = len(candidate)

    paren_idx = candidate.find('(')
    if paren_idx != -1:
        paren_tail = lowered[paren_idx:]
        if any(marker in paren_tail for marker in TIKTOK_RECIPE_TITLE_CUE_MARKERS):
            cut_at = min(cut_at, paren_idx)

    for marker in TIKTOK_RECIPE_TITLE_CUE_MARKERS:
        idx = lowered.find(marker)
        if idx > 8:
            cut_at = min(cut_at, idx)

    candidate = candidate[:cut_at].strip(" \t-–—|•:;.,()[]{}")
    candidate = re.sub(r'\s+', ' ', candidate).strip()
    return candidate


def extract_tiktok_recipe_focus_phrases(source_text: str, max_phrases: int = 4) -> list[str]:
    text = normalize_text_preserve_lines(decode_htmlish(source_text or ''))
    if not text:
        return []

    phrase_candidates = []
    seen = set()

    exact_title_phrase = extract_tiktok_preclue_title_phrase(text)
    if exact_title_phrase:
        lowered_title = exact_title_phrase.lower()
        seen.add(lowered_title)
        phrase_candidates.append((1000 + score_tiktok_title_line(exact_title_phrase), exact_title_phrase))

    for raw_line in re.split(r'[\r\n]+', text):
        line = strip_tiktok_recipe_title_clue_suffix(raw_line)
        line = clean_tiktok_title_line(line)
        if not line:
            continue
        if any(re.search(pattern, line, flags=re.IGNORECASE) for pattern in TIKTOK_TITLE_LINE_DROP_TIME_PATTERNS):
            continue

        token_count = len(re.findall(r'[A-Za-z0-9]+|[֐-׿]+', line))
        if token_count < 2 or len(line) < 8:
            continue

        lowered = line.lower()
        if lowered in seen:
            continue
        seen.add(lowered)

        score = score_tiktok_title_line(line)
        if score <= 0:
            continue
        phrase_candidates.append((score, line))

    phrase_candidates.sort(key=lambda item: (-item[0], item[1]))

    out = []
    for _score, phrase in phrase_candidates:
        lowered = phrase.lower()
        if any(existing.lower() == lowered for existing in out):
            continue
        out.append(phrase)
        if len(out) >= max(1, int(max_phrases or 0)):
            break

    return out


def tiktok_text_has_hebrew(value: str) -> bool:
    return bool(re.search(r'[֐-׿]', normalize_text_preserve_lines(value or '')))


def normalize_tiktok_same_host_search_base(site_url: str, affinity_tokens: list[str] | None = None) -> str:
    normalized = normalize_profile_url(site_url or '', site_url or '')
    if not normalized:
        return ''

    try:
        parsed = urlparse(normalized)
    except Exception:
        return ''

    host = canonical_domain(normalized)
    if not host:
        return ''

    host_compact = re.sub(r'[^a-z0-9]+', '', host)
    affinity_tokens = [str(token or '').strip().lower() for token in (affinity_tokens or []) if token]
    if host.startswith('book.'):
        stripped_host = host[5:]
        if stripped_host:
            host = stripped_host
            host_compact = re.sub(r'[^a-z0-9]+', '', host)

    if any(
        blocked in host
        for blocked in (
            'tiktok.com',
            'tiktokw.us',
            'tiktokcdn',
            'tiktokv.us',
            'muscdn',
            'byteoversea',
            'ibytedtos',
            'mcs.',
            'lf16-',
            'googleapis.com',
            'gstatic.com',
        )
    ):
        return ''

    if affinity_tokens and not any(token in host_compact for token in affinity_tokens if len(token) >= 3):
        return ''

    scheme = parsed.scheme or 'https'
    return f"{scheme}://{host}"


def build_tiktok_same_host_index_urls(site_root: str) -> list[str]:
    root = normalize_tiktok_same_host_search_base(site_root or '', [])
    if not root:
        return []
    try:
        parsed = urlparse(root)
    except Exception:
        return [root]
    origin = f"{parsed.scheme or 'https'}://{parsed.netloc}"
    candidates = [
        origin,
        urljoin(origin.rstrip('/') + '/', 'blog/'),
        urljoin(origin.rstrip('/') + '/', 'recipes/'),
        urljoin(origin.rstrip('/') + '/', 'recipe/'),
        urljoin(origin.rstrip('/') + '/', 'מתכונים/'),
    ]
    out = []
    seen = set()
    for candidate in candidates:
        normalized = normalize_profile_url(candidate or '', origin) or ''
        if not normalized:
            continue
        if canonical_domain(normalized) != canonical_domain(origin):
            continue
        key = investigation_candidate_dedupe_key(normalized)
        if key in seen:
            continue
        seen.add(key)
        out.append(normalized)
    return out



def _encode_same_host_archive_slug(value: str) -> str:
    cleaned = normalize_text_preserve_lines(value or '').strip()
    if not cleaned:
        return ''
    cleaned = re.sub(r'\s+', '-', cleaned)
    cleaned = re.sub(r'-{2,}', '-', cleaned).strip('-')
    return quote(cleaned, safe='-')


def build_tiktok_same_host_archive_urls(
    site_root: str,
    title_hint: str = "",
    focus_tokens: list[str] | None = None,
    source_text: str = "",
) -> list[str]:
    root = normalize_tiktok_same_host_search_base(site_root or '', [])
    if not root:
        return []
    try:
        parsed = urlparse(root)
    except Exception:
        return []

    origin = f"{parsed.scheme or 'https'}://{parsed.netloc}"
    phrase_source = combine_text_blocks([source_text, title_hint])
    exact_title_phrase = pick_tiktok_same_host_title_hint(title_hint, source_text=phrase_source)
    focus_tokens = [str(token or '').strip() for token in (focus_tokens or []) if token]

    seeds: list[str] = []
    seen_seed_values: set[str] = set()

    def add_seed(value: str) -> None:
        normalized = normalize_text_preserve_lines(value or '').strip()
        if not normalized:
            return
        key = normalized.lower()
        if key in seen_seed_values:
            return
        seen_seed_values.add(key)
        seeds.append(normalized)

    if exact_title_phrase and tiktok_text_has_hebrew(exact_title_phrase):
        add_seed(exact_title_phrase)
        title_tokens = [part for part in re.findall(r'[֐-׿]+(?:-[֐-׿]+)?', exact_title_phrase) if part]
        if len(title_tokens) >= 2:
            add_seed(' '.join(title_tokens[:2]))
        for token in title_tokens:
            token_clean = token.strip()
            if len(token_clean) >= 3 and token_clean not in {"המתכון", "המלא", "בקישור", "בביו", "באתר", "שלי", "עם"}:
                add_seed(token_clean)

    if not seeds:
        for token in focus_tokens:
            if re.search(r'[֐-׿]', token) and len(token) >= 3:
                add_seed(token)

    candidates = [
        urljoin(origin.rstrip('/') + '/', 'latest/'),
        urljoin(origin.rstrip('/') + '/', 'blog/latest/'),
    ]
    for seed in seeds[:6]:
        slug = _encode_same_host_archive_slug(seed)
        if not slug:
            continue
        candidates.extend([
            urljoin(origin.rstrip('/') + '/', f'blog/tag/{slug}/'),
            urljoin(origin.rstrip('/') + '/', f'blog/category/{slug}/'),
        ])

    out: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        normalized = normalize_profile_url(candidate or '', origin) or ''
        if not normalized:
            continue
        if canonical_domain(normalized) != canonical_domain(origin):
            continue
        key = investigation_candidate_dedupe_key(normalized)
        if key in seen:
            continue
        seen.add(key)
        out.append(normalized)
    return out


def build_tiktok_same_host_sitemap_urls(site_root: str) -> list[str]:
    root = normalize_tiktok_same_host_search_base(site_root or '', [])
    if not root:
        return []
    try:
        parsed = urlparse(root)
    except Exception:
        return []
    origin = f"{parsed.scheme or 'https'}://{parsed.netloc}"
    candidates = [
        urljoin(origin.rstrip('/') + '/', 'sitemap_index.xml'),
        urljoin(origin.rstrip('/') + '/', 'sitemap.xml'),
        urljoin(origin.rstrip('/') + '/', 'post-sitemap.xml'),
        urljoin(origin.rstrip('/') + '/', 'post-sitemap1.xml'),
        urljoin(origin.rstrip('/') + '/', 'page-sitemap.xml'),
        urljoin(origin.rstrip('/') + '/', 'blog-sitemap.xml'),
        urljoin(origin.rstrip('/') + '/', 'category-sitemap.xml'),
    ]
    out: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        normalized = normalize_profile_url(candidate or '', origin) or ''
        if not normalized:
            continue
        if canonical_domain(normalized) != canonical_domain(origin):
            continue
        key = investigation_candidate_dedupe_key(normalized)
        if key in seen:
            continue
        seen.add(key)
        out.append(normalized)
    return out


def extract_xml_loc_urls(xml_text: str) -> list[str]:
    source = str(xml_text or '')
    if not source:
        return []
    urls: list[str] = []
    seen: set[str] = set()
    for match in re.finditer(r'<loc>\s*([^<]+?)\s*</loc>', source, flags=re.IGNORECASE):
        value = source_safe_text(html_lib.unescape(match.group(1) or ''), 4000)
        if not value:
            continue
        key = value.strip()
        if not key or key in seen:
            continue
        seen.add(key)
        urls.append(key)
    return urls


def select_tiktok_sitemap_loc_samples(
    locs: list[str],
    exact_title_phrase: str = "",
    focus_tokens: list[str] | None = None,
    max_candidates: int = 520,
) -> list[str]:
    normalized_locs: list[str] = []
    seen: set[str] = set()
    for loc in locs or []:
        value = normalize_text_preserve_lines(loc or '').strip()
        if not value or value in seen:
            continue
        seen.add(value)
        normalized_locs.append(value)

    if len(normalized_locs) <= max(1, int(max_candidates or 0)):
        return normalized_locs

    preferred_tokens: list[str] = []
    preferred_seen: set[str] = set()

    def add_preferred_token(raw_value: str) -> None:
        token = normalize_tiktok_alignment_token(raw_value or '')
        if not token:
            return
        if token in TIKTOK_TITLE_ALIGNMENT_GENERIC_TOKENS or token in preferred_seen:
            return
        if len(token) < 3:
            return
        preferred_seen.add(token)
        preferred_tokens.append(token)

    for token in extract_tiktok_alignment_tokens(exact_title_phrase or ''):
        add_preferred_token(token)
    for token in focus_tokens or []:
        add_preferred_token(token)

    target_limit = max(1, int(max_candidates or 0))
    head_limit = min(220, target_limit)
    tail_limit = min(220, target_limit)
    ranked_matches: list[tuple[int, int, str]] = []
    if preferred_tokens:
        for index, loc in enumerate(normalized_locs):
            decoded_loc = normalize_text_preserve_lines(decode_htmlish(unquote(loc))).lower()
            hit_count = sum(1 for token in preferred_tokens if token in decoded_loc)
            if hit_count <= 0:
                continue
            ranked_matches.append((hit_count, -index, loc))
        ranked_matches.sort(key=lambda item: (-item[0], item[1], item[2]))

    out: list[str] = []
    out_seen: set[str] = set()

    def add_loc(value: str) -> None:
        if not value or value in out_seen:
            return
        out_seen.add(value)
        out.append(value)

    for _hits, _neg_index, loc in ranked_matches[:160]:
        add_loc(loc)
        if len(out) >= target_limit:
            return out

    for loc in normalized_locs[:head_limit]:
        add_loc(loc)
        if len(out) >= target_limit:
            return out

    for loc in normalized_locs[-tail_limit:]:
        add_loc(loc)
        if len(out) >= target_limit:
            return out

    for loc in normalized_locs:
        add_loc(loc)
        if len(out) >= target_limit:
            break

    return out


def extract_tiktok_sitemap_page_candidates(
    site_root: str,
    title_hint: str = "",
    affinity_tokens: list[str] | None = None,
    focus_tokens: list[str] | None = None,
    source_text: str = "",
) -> list[dict]:
    normalized_root = normalize_tiktok_same_host_search_base(site_root or '', affinity_tokens) or ''
    if not normalized_root:
        return []
    try:
        parsed = urlparse(normalized_root)
    except Exception:
        return []
    origin = f"{parsed.scheme or 'https'}://{parsed.netloc}"
    origin_host = canonical_domain(origin)
    affinity_tokens = [token for token in (affinity_tokens or []) if token]
    focus_tokens = [token for token in (focus_tokens or []) if token]
    phrase_source = combine_text_blocks([source_text, title_hint])
    focus_phrases = extract_tiktok_recipe_focus_phrases(phrase_source, max_phrases=3)
    exact_title_phrase = extract_tiktok_preclue_title_phrase(phrase_source)
    exact_title_lower = normalize_text_preserve_lines(exact_title_phrase).lower() if exact_title_phrase else ''

    visited_xml: set[str] = set()
    candidate_map: dict[str, dict] = {}

    def score_and_add(raw_url: str, source_name: str, context: str = "") -> None:
        normalized = normalize_investigation_candidate_url(raw_url or '', origin)
        if not normalized:
            return
        if canonical_domain(normalized) != origin_host:
            return
        if not looks_like_fetchable_external_page(normalized):
            return
        if looks_like_non_recipe_internal_page_url(normalized):
            return
        if looks_like_tiktok_hard_non_recipe_candidate_url(normalized):
            return
        if looks_like_tiktok_search_result_page(normalized):
            return
        if is_homepage_like_url(normalized):
            return

        normalized_lower = normalized.lower()
        normalized_decoded_lower = normalize_text_preserve_lines(decode_htmlish(unquote(normalized))).lower()
        context_norm = normalize_text_preserve_lines(context or "")
        context_lower = context_norm.lower()

        score = score_recipe_link_candidate(
            normalized,
            context_norm,
            phrase_source or title_hint,
            source='sitemap_loc',
        )
        if score <= -1000:
            return
        score += 30
        if re.search(r'/blog/\d{4}/\d{2}/\d{2}/', normalized_lower):
            score += 80
        elif '/blog/' in normalized_lower:
            score += 30
        if any(marker in normalized_lower for marker in ('/recipe', '/recipes', '/מתכון', '/מתכונים')):
            score += 30
        if exact_title_lower:
            title_tokens = [token for token in re.findall(r'[\w֐-׿]+', exact_title_lower) if len(token) >= 2]
            matched_title_tokens = sum(
                1 for token in title_tokens
                if token in normalized_decoded_lower or token in context_lower
            )
            if matched_title_tokens:
                score += min(30 * matched_title_tokens, 180)
            if exact_title_lower in normalized_decoded_lower or exact_title_lower in context_lower:
                score += 220
        focus_hits = 0
        for token in focus_tokens:
            token_lower = normalize_text_preserve_lines(token).lower()
            if not token_lower:
                continue
            if token_lower in normalized_decoded_lower or token_lower in context_lower:
                score += 25
                focus_hits += 1
        if focus_hits:
            score += min(focus_hits * 10, 40)
        for phrase in focus_phrases:
            phrase_lower = normalize_text_preserve_lines(phrase).lower()
            if phrase_lower and (phrase_lower in normalized_decoded_lower or phrase_lower in context_lower):
                score += 55
                break
        if any(marker in normalized_lower for marker in TIKTOK_SOFT_OFFER_PATH_MARKERS):
            score -= 160
        if canonical_domain(normalized).startswith('book.'):
            score -= 100

        if score <= 0:
            return

        key = investigation_candidate_dedupe_key(normalized)
        existing = candidate_map.get(key)
        payload = {
            'url': normalized,
            'score': score,
            'context': context_norm,
            'source': source_name,
        }
        if existing is None or int(payload.get('score') or 0) >= int(existing.get('score') or 0):
            candidate_map[key] = payload

    def fetch_sitemap(xml_url: str, depth: int = 0) -> None:
        normalized_xml = normalize_profile_url(xml_url or '', origin) or ''
        if not normalized_xml:
            return
        xml_key = investigation_candidate_dedupe_key(normalized_xml)
        if xml_key in visited_xml:
            return
        visited_xml.add(xml_key)

        doc = fetch_remote_html_document(normalized_xml, timeout=20)
        if not doc.get('ok'):
            return
        final_url = normalize_profile_url(doc.get('url') or normalized_xml, normalized_xml) or normalized_xml
        if canonical_domain(final_url) != origin_host:
            return

        locs = extract_xml_loc_urls(doc.get('html') or '')
        if not locs:
            return

        prioritized_locs = select_tiktok_sitemap_loc_samples(
            locs,
            exact_title_phrase=exact_title_phrase,
            focus_tokens=focus_tokens,
            max_candidates=520,
        )

        for loc in prioritized_locs:
            normalized_loc = normalize_profile_url(loc or '', final_url) or ''
            if not normalized_loc:
                continue
            if canonical_domain(normalized_loc) != origin_host:
                continue
            if normalized_loc.lower().endswith('.xml'):
                if depth < 1 and re.search(r'(?:sitemap|post|page|category|tag)', normalized_loc, flags=re.IGNORECASE):
                    fetch_sitemap(normalized_loc, depth + 1)
                continue
            score_and_add(normalized_loc, 'tiktok_same_host_sitemap_candidate', context=unquote(urlparse(normalized_loc).path or ''))

    for xml_url in build_tiktok_same_host_sitemap_urls(origin):
        fetch_sitemap(xml_url, depth=0)

    return sorted(candidate_map.values(), key=lambda item: int(item.get('score') or 0), reverse=True)
def build_tiktok_same_host_search_urls(
    site_url: str,
    focus_tokens: list[str] | None = None,
    title_hint: str = "",
    source_text: str = "",
    affinity_tokens: list[str] | None = None,
) -> list[dict]:
    normalized = normalize_tiktok_same_host_search_base(site_url or '', affinity_tokens)
    if not normalized:
        return []

    try:
        parsed = urlparse(normalized)
    except Exception:
        return []

    host = canonical_domain(normalized)
    if not host or any(host == blocked or host.endswith(f'.{blocked}') for blocked in TIKTOK_INTERMEDIATE_LANDING_HOSTS):
        return []

    def tokenize_phrase(value: str) -> list[str]:
        return [part for part in re.findall(r'[A-Za-z0-9]+|[֐-׿]+', normalize_text_preserve_lines(value or '')) if part]

    def query_is_strong(text: str) -> bool:
        lowered = normalize_text_preserve_lines(text).strip().lower()
        if not lowered:
            return False
        tokens = [part for part in lowered.split() if part]
        if re.search(r'[֐-׿]', lowered):
            return len(tokens) >= 1 and len(lowered) >= 4
        if len(tokens) >= 3:
            return True
        return len(tokens) >= 2 and any(marker in lowered for marker in TIKTOK_TITLE_FOOD_MARKERS)

    origin = f"{parsed.scheme or 'https'}://{parsed.netloc}"
    focus = [str(token or '').strip() for token in (focus_tokens or []) if token]
    queries = []

    phrase_source = combine_text_blocks([source_text, title_hint])
    exact_title_phrase = pick_tiktok_same_host_title_hint(title_hint, source_text=phrase_source)
    prefer_hebrew = tiktok_text_has_hebrew(exact_title_phrase) or tiktok_text_has_hebrew(title_hint)

    if exact_title_phrase and query_is_strong(exact_title_phrase):
        queries.append(exact_title_phrase)
        exact_tokens = tokenize_phrase(exact_title_phrase)
        if prefer_hebrew:
            if len(exact_tokens) >= 5:
                shortened_exact = ' '.join(exact_tokens[:6]).strip()
                if shortened_exact and shortened_exact != exact_title_phrase and query_is_strong(shortened_exact):
                    queries.append(shortened_exact)
        else:
            if len(exact_tokens) >= 5:
                shortened_exact = ' '.join(exact_tokens[:6]).strip()
                if shortened_exact and shortened_exact != exact_title_phrase and query_is_strong(shortened_exact):
                    queries.append(shortened_exact)
            if len(exact_tokens) >= 3:
                shorter_exact = ' '.join(exact_tokens[:4]).strip()
                if shorter_exact and shorter_exact not in queries and query_is_strong(shorter_exact):
                    queries.append(shorter_exact)

    focus_phrases = extract_tiktok_recipe_focus_phrases(phrase_source, max_phrases=4)
    for phrase in focus_phrases:
        if prefer_hebrew and not tiktok_text_has_hebrew(phrase):
            continue
        if query_is_strong(phrase):
            queries.append(phrase)

        phrase_tokens = tokenize_phrase(phrase)
        if len(phrase_tokens) >= 4:
            shortened = ' '.join(phrase_tokens[:5]).strip()
            if shortened and shortened != phrase and query_is_strong(shortened):
                queries.append(shortened)

    if not prefer_hebrew:
        if len(focus) >= 3:
            queries.append(' '.join(focus[:4]).strip())
            queries.append(' '.join(focus[:3]).strip())
        elif len(focus) >= 2:
            joined_focus = ' '.join(focus[:2]).strip()
            if query_is_strong(joined_focus):
                queries.append(joined_focus)

        if title_hint:
            title_tokens = extract_tiktok_recipe_focus_tokens(title_hint, focus, max_tokens=4)
            if len(title_tokens) >= 2:
                joined_title = ' '.join(title_tokens).strip()
                if query_is_strong(joined_title):
                    queries.append(joined_title)

    out = []
    seen = set()
    for query in queries:
        cleaned = normalize_text_preserve_lines(query)
        if not cleaned or not query_is_strong(cleaned):
            continue
        lowered = cleaned.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        out.append({
            'url': f"{origin}/?s={quote_plus(cleaned)}",
            'query': cleaned,
            'source': 'tiktok_same_host_search',
        })

    return out
def rerank_tiktok_external_site_candidates(candidates: list[dict], affinity_tokens: list[str]) -> list[dict]:
    reranked = []
    affinity_tokens = [token for token in (affinity_tokens or []) if token]
    for candidate in candidates or []:
        if not isinstance(candidate, dict):
            continue
        url = normalize_profile_url(candidate.get("url") or "", candidate.get("url") or "")
        if not url:
            continue
        item = dict(candidate)
        score = int(item.get("score") or 0)
        host = canonical_domain(url)
        path = (urlparse(url).path or "").lower()
        host_compact = re.sub(r'[^a-z0-9]+', '', host)

        if any(token in host_compact for token in affinity_tokens):
            score += 35
        if any(token in path for token in affinity_tokens):
            score += 25
        if any(host == blocked or host.endswith(f".{blocked}") for blocked in TIKTOK_INTERMEDIATE_LANDING_HOSTS):
            score -= 35
        if looks_like_tiktok_hard_non_recipe_candidate_url(url):
            score -= 180
        elif any(marker in path for marker in TIKTOK_SOFT_OFFER_PATH_MARKERS):
            score -= 120
        if host.startswith("book."):
            score -= 90
        if is_homepage_like_url(url):
            score -= 8

        item["url"] = url
        item["host"] = host
        item["score"] = score
        reranked.append(item)

    return sorted(reranked, key=lambda item: item.get("score") or 0, reverse=True)


def looks_like_tiktok_intermediate_landing_page(
    url: str,
    linked_evidence: dict | None,
    linked_metrics: dict | None,
) -> bool:
    normalized = normalize_profile_url(url or "", url or "")
    if not normalized:
        return False

    host = canonical_domain(normalized)
    title = normalize_text_preserve_lines((linked_evidence or {}).get("page_title") or "").lower()
    meta_description = normalize_text_preserve_lines((linked_evidence or {}).get("meta_description") or "").lower()
    raw_text = normalize_text_preserve_lines((linked_evidence or {}).get("raw_page_text") or "").lower()
    metrics = linked_metrics or {}

    weak_recipe_signal = (
        not metrics.get("looksRecipeDense")
        and metrics.get("measurementSignalCount", 0) == 0
        and metrics.get("recipeVerbSignalCount", 0) == 0
    )
    if is_wordpress_oembed_endpoint(normalized):
        return True

    host_is_intermediate = any(host == blocked or host.endswith(f".{blocked}") for blocked in TIKTOK_INTERMEDIATE_LANDING_HOSTS)
    title_looks_intermediate = any(marker in title for marker in TIKTOK_INTERMEDIATE_LANDING_TITLE_MARKERS)
    text_looks_intermediate = any(marker in raw_text[:4000] or marker in meta_description for marker in TIKTOK_INTERMEDIATE_LANDING_TITLE_MARKERS)

    if title_looks_intermediate and weak_recipe_signal:
        return True
    if host_is_intermediate and weak_recipe_signal:
        return True
    if host_is_intermediate and text_looks_intermediate:
        return True

    return False





async def extract_tiktok_rendered_linkhub_candidates(
    site_url: str,
    title_hint: str = "",
    affinity_tokens: list[str] | None = None,
) -> tuple[list[dict], dict]:
    affinity_tokens = [token for token in (affinity_tokens or []) if token]
    normalized_site = normalize_profile_url(site_url or "", site_url or "")
    if not normalized_site:
        return [], {"ok": False, "rendered_url": "", "title": "", "candidate_count": 0, "error": "missing_url"}

    landing_key = investigation_candidate_dedupe_key(normalized_site)
    landing_host = canonical_domain(normalized_site)
    candidate_map: dict[str, dict] = {}
    debug = {
        "ok": False,
        "rendered_url": normalized_site,
        "title": "",
        "candidate_count": 0,
        "error": "",
    }

    def upsert_candidate(raw_url: str, score: int, source_name: str, context: str = "") -> None:
        normalized = normalize_investigation_candidate_url(raw_url or "", normalized_site)
        if not normalized:
            return
        dedupe_key = investigation_candidate_dedupe_key(normalized)
        if dedupe_key == landing_key:
            return
        if not looks_like_fetchable_external_page(normalized):
            return
        if looks_like_non_recipe_internal_page_url(normalized):
            return
        if looks_like_tiktok_hard_non_recipe_candidate_url(normalized):
            return

        host = canonical_domain(normalized)
        path = (urlparse(normalized).path or "").lower()
        patched_score = int(score or 0)
        host_compact = re.sub(r'[^a-z0-9]+', '', host)

        if host == landing_host and path not in {"", "/"}:
            patched_score += 35
        if any(token in host_compact for token in affinity_tokens):
            patched_score += 35
        if any(token in path for token in affinity_tokens):
            patched_score += 25
        if any(marker in path for marker in TIKTOK_SOFT_OFFER_PATH_MARKERS):
            patched_score -= 120
        if host.startswith("book."):
            patched_score -= 90
        if is_homepage_like_url(normalized):
            patched_score -= 8

        payload = {
            "url": normalized,
            "score": patched_score,
            "source": source_name,
            "context": normalize_text_preserve_lines(context),
            "host": host,
        }
        existing = candidate_map.get(dedupe_key)
        if existing is None or int(payload.get("score") or 0) >= int(existing.get("score") or 0):
            candidate_map[dedupe_key] = payload

    browser = None
    context = None
    page = None
    try:
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=HEADLESS)
            context = await browser.new_context(ignore_https_errors=True)
            page = await context.new_page()
            await page.goto(
                normalized_site,
                wait_until="domcontentloaded",
                timeout=max(int(TIKTOK_RENDERED_LINKHUB_GOTO_TIMEOUT_MS or 0), 4000),
            )
            await page.wait_for_timeout(max(int(TIKTOK_RENDERED_LINKHUB_WAIT_MS or 0), 600))

            current_url = page.url or normalized_site
            rendered_title = source_safe_text(await page.title(), PAGE_TITLE_SUBMIT_MAX)
            html = await page.content()
            try:
                rendered_text = await page.evaluate(
                    "() => { const body = document.body; return body ? ((body.innerText || body.textContent || '').slice(0, 12000)) : ''; }"
                )
            except Exception:
                rendered_text = ""

            try:
                dom_items = await page.evaluate(
                    """
                    () => {
                      const out = [];
                      const push = (href, text, source) => {
                        if (!href) return;
                        const normalizedText = String(text || '').replace(/\\s+/g, ' ').trim().slice(0, 300);
                        out.push({ href: String(href), text: normalizedText, source });
                      };
                      const parseOnclick = (value) => {
                        const s = String(value || '');
                        const match = s.match(/https?:\/\/[^"'\s)]+/i);
                        return match ? match[0] : '';
                      };
                      for (const el of Array.from(document.querySelectorAll('a[href], area[href], button, [role="link"], [data-url], [data-href], [href]'))) {
                        const tag = String(el.tagName || '').toLowerCase();
                        const text = el.innerText || el.textContent || '';
                        const href =
                          el.getAttribute('href') ||
                          el.getAttribute('data-url') ||
                          el.getAttribute('data-href') ||
                          el.getAttribute('formaction') ||
                          parseOnclick(el.getAttribute('onclick'));
                        if (href) {
                          push(href, text, tag === 'a' || tag === 'area' ? 'rendered_dom_anchor' : 'rendered_dom_link');
                        }
                      }
                      return out;
                    }
                    """
                )
            except Exception:
                dom_items = []

            debug.update({
                "ok": True,
                "rendered_url": current_url,
                "title": rendered_title,
            })

            for item in dom_items or []:
                raw_href = item.get("href") or ""
                context_text = item.get("text") or ""
                source_name = item.get("source") or "rendered_dom_anchor"
                seed_score = score_recipe_link_candidate(
                    raw_href,
                    context_text,
                    rendered_title or title_hint,
                    source="dom_anchor",
                )
                if "recipe" in context_text.lower() or "blog" in context_text.lower():
                    seed_score += 10
                upsert_candidate(raw_href, seed_score, source_name, context_text)

            for candidate in extract_tiktok_external_site_candidates(
                html,
                current_url,
                rendered_title or title_hint,
                context_prefix="rendered_page_html",
            ):
                upsert_candidate(
                    candidate.get("url") or "",
                    int(candidate.get("score") or 0),
                    candidate.get("source") or "rendered_page_html",
                    candidate.get("context") or "",
                )

            rendered_combined = combine_text_blocks([rendered_text, html])
            for extracted in extract_urls_with_context(rendered_combined):
                seed_score = score_recipe_link_candidate(
                    extracted.get("url") or "",
                    extracted.get("context") or "",
                    rendered_title or title_hint,
                    source="description",
                )
                upsert_candidate(
                    extracted.get("url") or "",
                    seed_score,
                    "rendered_page_text",
                    extracted.get("context") or "",
                )

    except Exception as rendered_err:
        debug["error"] = f"{type(rendered_err).__name__}: {rendered_err}"
    finally:
        await close_page_and_context(page, None)
        if context is not None:
            try:
                await context.close()
            except Exception:
                pass
        if browser is not None:
            try:
                await browser.close()
            except Exception:
                pass

    reranked = rerank_tiktok_external_site_candidates(list(candidate_map.values()), affinity_tokens)
    final = []
    for item in reranked:
        score = int(item.get("score") or 0)
        lower_url = (item.get("url") or "").lower()
        context_lower = (item.get("context") or "").lower()
        if "/recipe" in lower_url or "/recipes" in lower_url:
            score += 15
        if "recipe" in context_lower or "blog" in context_lower:
            score += 10
        patched = dict(item)
        patched["score"] = score
        final.append(patched)

    final = sorted(final, key=lambda item: item.get("score") or 0, reverse=True)
    debug["candidate_count"] = len(final)
    return final, debug


def extract_tiktok_site_root_external_candidates(
    linked_evidence: dict,
    site_url: str,
    title_hint: str = "",
    affinity_tokens: list[str] | None = None,
) -> list[dict]:
    affinity_tokens = [token for token in (affinity_tokens or []) if token]
    effective_url = normalize_profile_url(linked_evidence.get("effective_page_url") or site_url, site_url) or site_url
    effective_host = canonical_domain(effective_url)

    page_html = linked_evidence.get("page_html") or ""
    visible_text = linked_evidence.get("visible_page_text") or ""
    raw_text = linked_evidence.get("raw_page_text") or ""
    candidate_map: dict[str, dict] = {}

    def upsert_candidate(url: str, score: int, source_name: str, context: str = "") -> None:
        normalized = normalize_investigation_candidate_url(url or "", effective_url)
        if not normalized or not looks_like_fetchable_external_page(normalized):
            return
        if looks_like_non_recipe_internal_page_url(normalized):
            return
        if looks_like_tiktok_hard_non_recipe_candidate_url(normalized):
            return
        host = canonical_domain(normalized)
        if host == effective_host:
            return
        key = investigation_candidate_dedupe_key(normalized)
        payload = {
            "url": normalized,
            "score": int(score or 0),
            "source": source_name,
            "context": normalize_text_preserve_lines(context),
            "host": host,
        }
        existing = candidate_map.get(key)
        if existing is None or int(payload.get("score") or 0) >= int(existing.get("score") or 0):
            candidate_map[key] = payload

    for candidate in extract_tiktok_external_site_candidates(
        page_html,
        effective_url,
        title_hint,
        context_prefix="site_root_html",
    ):
        upsert_candidate(
            candidate.get("url") or "",
            int(candidate.get("score") or 0),
            candidate.get("source") or "site_root_html",
            candidate.get("context") or "",
        )

    combined_text = combine_text_blocks([visible_text, raw_text])
    for extracted in extract_urls_with_context(combined_text):
        seed_score = score_recipe_link_candidate(
            extracted.get("url") or "",
            extracted.get("context") or "",
            title_hint,
            source="description",
        )
        upsert_candidate(extracted.get("url") or "", seed_score, "site_root_text", extracted.get("context") or "")

    if not candidate_map:
        raw_fetch = fetch_remote_html_document(effective_url)
        raw_html = raw_fetch.get("html") or ""
        if raw_html:
            for candidate in extract_tiktok_external_site_candidates(
                raw_html,
                raw_fetch.get("url") or effective_url,
                title_hint,
                context_prefix="site_root_raw_html",
            ):
                upsert_candidate(
                    candidate.get("url") or "",
                    int(candidate.get("score") or 0),
                    candidate.get("source") or "site_root_raw_html",
                    candidate.get("context") or "",
                )

    reranked = rerank_tiktok_external_site_candidates(list(candidate_map.values()), affinity_tokens)
    final = []
    for item in reranked:
        host = canonical_domain(item.get("url") or "")
        score = int(item.get("score") or 0)
        if not any(host == blocked or host.endswith(f".{blocked}") for blocked in TIKTOK_INTERMEDIATE_LANDING_HOSTS):
            score += 10
        if not is_homepage_like_url(item.get("url") or ""):
            score += 8
        lower_url = (item.get("url") or "").lower()
        if looks_like_tiktok_hard_non_recipe_candidate_url(item.get("url") or ""):
            score -= 180
        elif any(marker in lower_url for marker in TIKTOK_SOFT_OFFER_PATH_MARKERS):
            score -= 120
        if host.startswith("book."):
            score -= 90
        if "/recipe" in lower_url or "/recipes" in lower_url:
            score += 15
        patched = dict(item)
        patched["score"] = score
        final.append(patched)

    return sorted(final, key=lambda item: item.get("score") or 0, reverse=True)


def looks_like_tiktok_search_result_page(
    url: str,
    linked_evidence: dict | None = None,
) -> bool:
    normalized = normalize_profile_url(url or '', url or '')
    if not normalized:
        return False
    try:
        parsed = urlparse(normalized)
        path = (parsed.path or '').lower().rstrip('/')
        query_lower = (parsed.query or '').lower()
    except Exception:
        path = ''
        query_lower = ''
    if path.startswith('/search'):
        return True
    if '/search/' in path:
        return True
    if any(marker in query_lower for marker in ('s=', 'search=', 'q=')) and not any(marker in path for marker in ('/recipe', '/recipes', '/category/')):
        return True
    evidence = linked_evidence or {}
    page_title = normalize_text_preserve_lines(evidence.get('page_title') or '').lower()
    if page_title.startswith('חיפוש עבור') or page_title.startswith('search results for') or page_title.startswith('search for'):
        return True
    return False


def extract_tiktok_internal_page_candidates(
    linked_evidence: dict,
    site_url: str,
    title_hint: str = "",
    affinity_tokens: list[str] | None = None,
    focus_tokens: list[str] | None = None,
    source_text: str = "",
) -> list[dict]:
    affinity_tokens = [token for token in (affinity_tokens or []) if token]
    focus_tokens = [token for token in (focus_tokens or []) if token]
    phrase_source = combine_text_blocks([source_text, title_hint])
    focus_phrases = extract_tiktok_recipe_focus_phrases(phrase_source, max_phrases=3)
    exact_title_phrase = extract_tiktok_preclue_title_phrase(phrase_source)
    page_html = linked_evidence.get("page_html") or ""
    visible_text = linked_evidence.get("visible_page_text") or ""
    effective_url = normalize_profile_url(linked_evidence.get("effective_page_url") or site_url, site_url) or site_url
    preferred_host = canonical_domain(effective_url)
    effective_key = investigation_candidate_dedupe_key(effective_url)

    raw_items = []
    for anchor_match in re.finditer(r'<a\b[^>]*href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', page_html, flags=re.IGNORECASE | re.DOTALL):
        href = anchor_match.group(1) or ''
        inner = re.sub(r'<[^>]+>', ' ', anchor_match.group(2) or '')
        inner = html_lib.unescape(inner)
        raw_items.append({
            "href": href,
            "text": normalize_text_preserve_lines(inner),
            "source": "dom_anchor_text",
        })

    for href_match in re.finditer(r'href=["\']([^"\']+)["\']', page_html, flags=re.IGNORECASE):
        if href_match and href_match.group(1):
            raw_items.append({
                "href": href_match.group(1),
                "text": "",
                "source": "dom_anchor",
            })

    for extracted in extract_urls_with_context(visible_text):
        raw_items.append({
            "href": extracted.get("url") or "",
            "text": extracted.get("context") or "",
            "source": "visible_text",
        })

    for extracted in extract_urls_with_context(page_html):
        raw_items.append({
            "href": extracted.get("url") or "",
            "text": extracted.get("context") or "",
            "source": "html_text",
        })

    candidates = []
    seen = set()
    for item in raw_items:
        normalized = normalize_investigation_candidate_url(
            item.get("href") or "",
            effective_url,
        )
        if not normalized:
            continue
        if canonical_domain(normalized) != preferred_host:
            continue
        if not looks_like_fetchable_external_page(normalized):
            continue
        if looks_like_non_recipe_internal_page_url(normalized):
            continue
        if looks_like_tiktok_hard_non_recipe_candidate_url(normalized):
            continue
        if looks_like_tiktok_search_result_page(normalized):
            continue
        if is_homepage_like_url(normalized):
            continue
        dedupe_key = investigation_candidate_dedupe_key(normalized)
        if dedupe_key == effective_key:
            continue
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)

        context = normalize_text_preserve_lines(item.get("text") or "")
        score = score_recipe_link_candidate(
            normalized,
            context,
            phrase_source or title_hint,
            source="dom_anchor" if item.get("source") in {"dom_anchor", "dom_anchor_text"} else "html_anchor",
        )
        if score <= -1000:
            continue
        score += 25
        normalized_lower = normalized.lower()
        normalized_decoded_lower = normalize_text_preserve_lines(decode_htmlish(unquote(normalized))).lower()
        context_lower = context.lower()
        if item.get("source") == "dom_anchor_text" and context:
            score += 8
            if looks_like_tiktok_title_line(context):
                score += 20
            if any(marker in context_lower for marker in TIKTOK_TITLE_FOOD_MARKERS):
                score += 25
        if any(token in normalized_lower or token in normalized_decoded_lower for token in affinity_tokens):
            score += 20
        focus_hits = 0
        for token in focus_tokens:
            if token in normalized_lower or token in normalized_decoded_lower:
                score += 35
                focus_hits += 1
            elif token in context_lower:
                score += 18
                focus_hits += 1
        if focus_hits:
            score += min(focus_hits * 10, 30)
        if any(marker in normalized_lower for marker in ("/recipe", "/recipes", "/מתכון", "/מתכונים")):
            score += 20
        if re.search(r'/blog/\d{4}/\d{2}/\d{2}/', normalized_lower):
            score += 45
        elif '/blog/' in normalized_lower:
            score += 20
        for phrase in focus_phrases:
            phrase_lower = phrase.lower()
            if phrase_lower and (phrase_lower in context_lower or phrase_lower in normalized_lower or phrase_lower in normalized_decoded_lower):
                score += 45
                break
        if exact_title_phrase:
            exact_lower = exact_title_phrase.lower()
            if exact_lower and (exact_lower in context_lower or exact_lower in normalized_decoded_lower):
                score += 70
        if any(marker in normalized_lower for marker in TIKTOK_SOFT_OFFER_PATH_MARKERS):
            score -= 120
        if canonical_domain(normalized).startswith("book."):
            score -= 90

        candidates.append({
            "url": normalized,
            "score": score,
            "context": context,
            "source": item.get("source") or "internal_page",
        })

    return sorted(candidates, key=lambda item: item["score"], reverse=True)

def find_tiktok_same_host_search_winner(
    result: dict,
    site_url: str,
    title_hint: str,
    affinity_tokens: list[str] | None,
    focus_tokens: list[str] | None,
    minimum_winner_score: int,
    searched_bases: set[str] | None = None,
    source_text: str = "",
) -> dict | None:
    normalized_site = normalize_profile_url(site_url or '', site_url or '')
    if not normalized_site:
        return None

    search_base_site = normalize_tiktok_same_host_search_base(normalized_site, affinity_tokens) or normalized_site
    host = canonical_domain(search_base_site)
    if not host or any(host == blocked or host.endswith(f'.{blocked}') for blocked in TIKTOK_INTERMEDIATE_LANDING_HOSTS):
        return None

    dedupe_key = f"{host}:{search_base_site.rstrip('/')}"
    if searched_bases is not None:
        if dedupe_key in searched_bases:
            return None
        searched_bases.add(dedupe_key)

    phrase_source = combine_text_blocks([source_text, title_hint])
    internal_title_hint = pick_tiktok_same_host_title_hint(title_hint, source_text=phrase_source) or title_hint

    # First pass: mine the normalized same-host root/home/category/blog pages directly before site-search.
    root_index_urls = build_tiktok_same_host_index_urls(search_base_site)
    evaluated_root_indexes: set[str] = set()
    for root_index_url in root_index_urls:
        root_index_key = investigation_candidate_dedupe_key(root_index_url)
        if not root_index_url or root_index_key in evaluated_root_indexes:
            continue
        evaluated_root_indexes.add(root_index_key)

        base_evidence, _base_source_metadata, _base_metrics = fetch_remote_page_metadata(root_index_url)
        if not base_evidence:
            continue

        base_internal_candidates = extract_tiktok_internal_page_candidates(
            base_evidence,
            root_index_url,
            internal_title_hint,
            affinity_tokens,
            focus_tokens,
            source_text=phrase_source,
        )
        for internal_candidate in base_internal_candidates[:10]:
            add_investigation_candidate(
                result,
                internal_candidate.get('url') or '',
                source='tiktok_same_host_root_internal_candidate',
                score=internal_candidate.get('score'),
                usable=None,
                reason='same_host_root_internal_candidate_ranked',
                extra={'query': internal_title_hint or '', 'root_index_url': root_index_url},
            )

        for internal_candidate in base_internal_candidates[:10]:
            internal_url = internal_candidate.get('url') or ''
            internal_seed_score = int(internal_candidate.get('score') or 0)
            candidate_evidence, candidate_source_metadata, candidate_metrics = fetch_remote_page_metadata(internal_url)
            candidate_score = score_linked_page_candidate(
                internal_seed_score,
                candidate_metrics,
                strong_bonus=80,
                dense_bonus=140,
                food_context_bonus=20,
            )
            candidate_effective_url = normalize_investigation_candidate_url(
                candidate_evidence.get('effective_page_url') or internal_url,
                internal_url,
            ) or internal_url
            candidate_offer_like = looks_like_tiktok_offer_like_page(
                candidate_effective_url,
                candidate_evidence,
                candidate_metrics,
            )
            candidate_search_result = looks_like_tiktok_search_result_page(candidate_effective_url, candidate_evidence)
            candidate_same_as_root = urlsRoughlyEqual(candidate_effective_url, root_index_url) or urlsRoughlyEqual(internal_url, root_index_url)
            usable_candidate = linked_page_looks_usable(candidate_evidence, candidate_metrics) and not candidate_offer_like and not candidate_search_result and not candidate_same_as_root
            if candidate_offer_like:
                candidate_score -= 120
            if candidate_search_result or candidate_same_as_root:
                candidate_score -= 180
            candidate_extra = build_linked_page_candidate_extra(candidate_evidence, candidate_metrics)
            candidate_query_match = evaluate_tiktok_same_host_query_match(
                internal_title_hint,
                candidate_evidence.get('page_title') or '',
                candidate_effective_url or internal_url,
            )
            candidate_extra.update(candidate_query_match)
            if internal_title_hint and not candidate_query_match.get('query_match_allowed', True):
                candidate_score -= 260
                usable_candidate = False
                candidate_extra['query_mismatch_guard'] = True
            candidate_extra['offer_like_page'] = candidate_offer_like
            candidate_extra['search_result_page'] = candidate_search_result
            candidate_extra['same_as_site_root'] = candidate_same_as_root
            candidate_extra['same_host_root_scan'] = True
            candidate_extra['root_index_url'] = root_index_url
            candidate_extra['query'] = internal_title_hint or ''
            add_investigation_candidate(
                result,
                internal_url,
                source='tiktok_same_host_root_internal_candidate',
                score=candidate_score,
                usable=usable_candidate,
                reason='same_host_root_internal_page_evaluated',
                extra=candidate_extra,
            )
            if usable_candidate and candidate_score >= minimum_winner_score and not candidate_offer_like and not candidate_search_result and not candidate_same_as_root:
                return {
                    'evidence': candidate_evidence,
                    'source_metadata': candidate_source_metadata,
                    'url': candidate_effective_url or internal_url,
                    'score': candidate_score,
                    'site_root': root_index_url,
                }


    sitemap_candidates = extract_tiktok_sitemap_page_candidates(
        search_base_site,
        internal_title_hint,
        affinity_tokens,
        focus_tokens,
        source_text=phrase_source,
    )
    for internal_candidate in sitemap_candidates[:12]:
        add_investigation_candidate(
            result,
            internal_candidate.get('url') or '',
            source='tiktok_same_host_sitemap_candidate',
            score=internal_candidate.get('score'),
            usable=None,
            reason='same_host_sitemap_candidate_ranked',
            extra={'query': internal_title_hint or '', 'site_root': search_base_site},
        )

    sitemap_eval_limit = 10
    if len(extract_tiktok_alignment_tokens(internal_title_hint or '')) >= 4:
        sitemap_eval_limit = 18

    for internal_candidate in sitemap_candidates[:sitemap_eval_limit]:
        internal_url = internal_candidate.get('url') or ''
        internal_seed_score = int(internal_candidate.get('score') or 0)
        candidate_evidence, candidate_source_metadata, candidate_metrics = fetch_remote_page_metadata(internal_url)
        candidate_score = score_linked_page_candidate(
            internal_seed_score,
            candidate_metrics,
            strong_bonus=80,
            dense_bonus=140,
            food_context_bonus=20,
        )
        candidate_effective_url = normalize_investigation_candidate_url(
            candidate_evidence.get('effective_page_url') or internal_url,
            internal_url,
        ) or internal_url
        candidate_offer_like = looks_like_tiktok_offer_like_page(
            candidate_effective_url,
            candidate_evidence,
            candidate_metrics,
        )
        candidate_search_result = looks_like_tiktok_search_result_page(candidate_effective_url, candidate_evidence)
        candidate_same_as_root = urlsRoughlyEqual(candidate_effective_url, search_base_site) or urlsRoughlyEqual(internal_url, search_base_site)
        usable_candidate = linked_page_looks_usable(candidate_evidence, candidate_metrics) and not candidate_offer_like and not candidate_search_result and not candidate_same_as_root
        if candidate_offer_like:
            candidate_score -= 120
        if candidate_search_result or candidate_same_as_root:
            candidate_score -= 180
        candidate_extra = build_linked_page_candidate_extra(candidate_evidence, candidate_metrics)
        candidate_query_match = evaluate_tiktok_same_host_query_match(
            internal_title_hint,
            candidate_evidence.get('page_title') or '',
            candidate_effective_url or internal_url,
        )
        candidate_extra.update(candidate_query_match)
        if internal_title_hint and not candidate_query_match.get('query_match_allowed', True):
            candidate_score -= 260
            usable_candidate = False
            candidate_extra['query_mismatch_guard'] = True
        candidate_extra['offer_like_page'] = candidate_offer_like
        candidate_extra['search_result_page'] = candidate_search_result
        candidate_extra['same_as_site_root'] = candidate_same_as_root
        candidate_extra['same_host_sitemap_scan'] = True
        candidate_extra['query'] = internal_title_hint or ''
        add_investigation_candidate(
            result,
            internal_url,
            source='tiktok_same_host_sitemap_candidate',
            score=candidate_score,
            usable=usable_candidate,
            reason='same_host_sitemap_page_evaluated',
            extra=candidate_extra,
        )
        if usable_candidate and candidate_score >= minimum_winner_score and not candidate_offer_like and not candidate_search_result and not candidate_same_as_root:
            return {
                'evidence': candidate_evidence,
                'source_metadata': candidate_source_metadata,
                'url': candidate_effective_url or internal_url,
                'score': candidate_score,
                'site_root': search_base_site,
            }

    archive_urls = build_tiktok_same_host_archive_urls(
        search_base_site,
        internal_title_hint,
        focus_tokens,
        source_text=phrase_source,
    )
    evaluated_archive_indexes: set[str] = set()
    for archive_url in archive_urls[:8]:
        archive_key = investigation_candidate_dedupe_key(archive_url)
        if not archive_url or archive_key in evaluated_archive_indexes:
            continue
        evaluated_archive_indexes.add(archive_key)

        archive_evidence, _archive_source_metadata, _archive_metrics = fetch_remote_page_metadata(archive_url)
        if not archive_evidence:
            continue

        archive_internal_candidates = extract_tiktok_internal_page_candidates(
            archive_evidence,
            archive_url,
            internal_title_hint,
            affinity_tokens,
            focus_tokens,
            source_text=phrase_source,
        )
        for internal_candidate in archive_internal_candidates[:10]:
            add_investigation_candidate(
                result,
                internal_candidate.get('url') or '',
                source='tiktok_same_host_archive_internal_candidate',
                score=internal_candidate.get('score'),
                usable=None,
                reason='same_host_archive_internal_candidate_ranked',
                extra={'query': internal_title_hint or '', 'archive_url': archive_url},
            )

        for internal_candidate in archive_internal_candidates[:10]:
            internal_url = internal_candidate.get('url') or ''
            internal_seed_score = int(internal_candidate.get('score') or 0)
            candidate_evidence, candidate_source_metadata, candidate_metrics = fetch_remote_page_metadata(internal_url)
            candidate_score = score_linked_page_candidate(
                internal_seed_score,
                candidate_metrics,
                strong_bonus=80,
                dense_bonus=140,
                food_context_bonus=20,
            )
            candidate_effective_url = normalize_investigation_candidate_url(
                candidate_evidence.get('effective_page_url') or internal_url,
                internal_url,
            ) or internal_url
            candidate_offer_like = looks_like_tiktok_offer_like_page(
                candidate_effective_url,
                candidate_evidence,
                candidate_metrics,
            )
            candidate_search_result = looks_like_tiktok_search_result_page(candidate_effective_url, candidate_evidence)
            candidate_same_as_archive = urlsRoughlyEqual(candidate_effective_url, archive_url) or urlsRoughlyEqual(internal_url, archive_url)
            usable_candidate = linked_page_looks_usable(candidate_evidence, candidate_metrics) and not candidate_offer_like and not candidate_search_result and not candidate_same_as_archive
            if candidate_offer_like:
                candidate_score -= 120
            if candidate_search_result or candidate_same_as_archive:
                candidate_score -= 180
            candidate_extra = build_linked_page_candidate_extra(candidate_evidence, candidate_metrics)
            candidate_query_match = evaluate_tiktok_same_host_query_match(
                internal_title_hint,
                candidate_evidence.get('page_title') or '',
                candidate_effective_url or internal_url,
            )
            candidate_extra.update(candidate_query_match)
            if internal_title_hint and not candidate_query_match.get('query_match_allowed', True):
                candidate_score -= 260
                usable_candidate = False
                candidate_extra['query_mismatch_guard'] = True
            candidate_extra['offer_like_page'] = candidate_offer_like
            candidate_extra['search_result_page'] = candidate_search_result
            candidate_extra['same_as_archive_url'] = candidate_same_as_archive
            candidate_extra['same_host_archive_scan'] = True
            candidate_extra['archive_url'] = archive_url
            candidate_extra['query'] = internal_title_hint or ''
            add_investigation_candidate(
                result,
                internal_url,
                source='tiktok_same_host_archive_internal_candidate',
                score=candidate_score,
                usable=usable_candidate,
                reason='same_host_archive_internal_page_evaluated',
                extra=candidate_extra,
            )
            if usable_candidate and candidate_score >= minimum_winner_score and not candidate_offer_like and not candidate_search_result and not candidate_same_as_archive:
                return {
                    'evidence': candidate_evidence,
                    'source_metadata': candidate_source_metadata,
                    'url': candidate_effective_url or internal_url,
                    'score': candidate_score,
                    'site_root': archive_url,
                }

    search_urls = build_tiktok_same_host_search_urls(search_base_site, focus_tokens, internal_title_hint, source_text=phrase_source, affinity_tokens=affinity_tokens)
    if not search_urls:
        return None

    for search_item in search_urls[:3]:
        search_url = search_item.get('url') or ''
        add_investigation_candidate(
            result,
            search_url,
            source='tiktok_same_host_search',
            score=90,
            usable=None,
            reason='same_host_search_candidate_ranked',
            extra={'query': search_item.get('query') or ''},
        )

        search_evidence, _search_source_metadata, search_metrics = fetch_remote_page_metadata(search_url)
        search_extra = build_linked_page_candidate_extra(search_evidence, search_metrics)
        search_extra['query'] = search_item.get('query') or ''
        search_extra['search_result_page'] = True
        add_investigation_candidate(
            result,
            search_url,
            source='tiktok_same_host_search',
            score=score_linked_page_candidate(60, search_metrics, strong_bonus=40, dense_bonus=80, food_context_bonus=20),
            usable=False,
            reason='same_host_search_results_evaluated',
            extra=search_extra,
        )

        internal_candidates = extract_tiktok_internal_page_candidates(
            search_evidence,
            search_url,
            search_item.get('query') or internal_title_hint,
            affinity_tokens,
            focus_tokens,
            source_text=phrase_source,
        )
        for internal_candidate in internal_candidates[:8]:
            add_investigation_candidate(
                result,
                internal_candidate.get('url') or '',
                source='tiktok_same_host_search_internal_candidate',
                score=internal_candidate.get('score'),
                usable=None,
                reason='same_host_search_internal_candidate_ranked',
                extra={'query': search_item.get('query') or ''},
            )

        for internal_candidate in internal_candidates[:8]:
            internal_url = internal_candidate.get('url') or ''
            internal_seed_score = int(internal_candidate.get('score') or 0)
            candidate_evidence, candidate_source_metadata, candidate_metrics = fetch_remote_page_metadata(internal_url)
            candidate_score = score_linked_page_candidate(
                internal_seed_score,
                candidate_metrics,
                strong_bonus=80,
                dense_bonus=140,
                food_context_bonus=20,
            )
            candidate_effective_url = normalize_investigation_candidate_url(
                candidate_evidence.get('effective_page_url') or internal_url,
                internal_url,
            ) or internal_url
            candidate_offer_like = looks_like_tiktok_offer_like_page(
                candidate_effective_url,
                candidate_evidence,
                candidate_metrics,
            )
            candidate_search_result = looks_like_tiktok_search_result_page(candidate_effective_url, candidate_evidence)
            candidate_same_as_search = urlsRoughlyEqual(candidate_effective_url, search_url) or urlsRoughlyEqual(internal_url, search_url)
            usable_candidate = linked_page_looks_usable(candidate_evidence, candidate_metrics) and not candidate_offer_like and not candidate_search_result and not candidate_same_as_search
            if candidate_offer_like:
                candidate_score -= 120
            if candidate_search_result or candidate_same_as_search:
                candidate_score -= 180
            candidate_extra = build_linked_page_candidate_extra(candidate_evidence, candidate_metrics)
            candidate_query_match = evaluate_tiktok_same_host_query_match(
                search_item.get('query') or internal_title_hint,
                candidate_evidence.get('page_title') or '',
                candidate_effective_url or internal_url,
            )
            candidate_extra.update(candidate_query_match)
            if (search_item.get('query') or internal_title_hint) and not candidate_query_match.get('query_match_allowed', True):
                candidate_score -= 260
                usable_candidate = False
                candidate_extra['query_mismatch_guard'] = True
            candidate_extra['offer_like_page'] = candidate_offer_like
            candidate_extra['search_result_page'] = candidate_search_result
            candidate_extra['same_as_search_url'] = candidate_same_as_search
            candidate_extra['query'] = search_item.get('query') or ''
            candidate_extra['same_host_search'] = True
            add_investigation_candidate(
                result,
                internal_url,
                source='tiktok_same_host_search_internal_candidate',
                score=candidate_score,
                usable=usable_candidate,
                reason='same_host_search_internal_page_evaluated',
                extra=candidate_extra,
            )
            if usable_candidate and candidate_score >= minimum_winner_score and not candidate_offer_like and not candidate_search_result and not candidate_same_as_search:
                return {
                    'evidence': candidate_evidence,
                    'source_metadata': candidate_source_metadata,
                    'url': candidate_effective_url or internal_url,
                    'score': candidate_score,
                    'site_root': search_url,
                }

    return None

def extract_tiktok_external_site_url(html: str, base_url: str = "", page_title: str = "") -> str:
    candidates = extract_tiktok_external_site_candidates(html, base_url, page_title)
    if not candidates:
        return ""
    best = candidates[0]
    return best["url"] if int(best.get("score") or 0) >= 20 else ""


def fetch_remote_page_metadata(url: str) -> tuple[dict, dict, dict]:
    fetched = fetch_remote_html_document(url)
    final_url = fetched.get("url") or normalize_profile_url(url or "", url or "")
    html = fetched.get("html") or ""

    page_title = extract_meta_tag(html, ['og:title', 'twitter:title']) or extract_html_title_tag(html)
    meta_description = extract_meta_tag(html, ['og:description', 'twitter:description', 'description'])
    page_image_url = normalize_profile_url(
        extract_meta_tag(html, ['image_src', 'og:image', 'og:image:secure_url', 'twitter:image', 'twitter:image:src']) or '',
        final_url,
    )
    structured_html_text = extract_structured_text_from_html(html)
    visible_html_text = normalize_text_preserve_lines(re.sub(r'<[^>]+>', '\n', decode_htmlish(html)))
    raw_page_text = combine_text_blocks([
        page_title,
        meta_description,
        structured_html_text,
        visible_html_text,
    ])

    evidence = {
        'page_title': trim_text(page_title, PAGE_TITLE_SUBMIT_MAX),
        'raw_page_text': trim_text(raw_page_text, RAW_PAGE_TEXT_SUBMIT_MAX),
        'visible_page_text': trim_text(visible_html_text, RAW_PAGE_TEXT_SUBMIT_MAX),
        'visible_text_before_expand': '',
        'visible_text_after_expand': '',
        'structured_html_text': structured_html_text,
        'expanded_caption_text': trim_text(meta_description, EXPANDED_CAPTION_SUBMIT_MAX),
        'meta_description': trim_text(meta_description, META_DESCRIPTION_SUBMIT_MAX),
        'page_html': trim_text(html, PAGE_HTML_MAX_LEN),
        'page_image_url': trim_text(page_image_url, 2000),
        'media_type_guess': 'page',
        'caption_expanded': False,
        'expand_attempted': False,
        'expand_method': 'remote_html_fetch',
        'expand_success': False,
        'caption_before_len': 0,
        'caption_after_len': len(meta_description or ''),
        'caption_before_lines': 0,
        'caption_after_lines': count_non_empty_lines(meta_description),
        'is_youtube_shorts': False,
        'current_page_is_youtube_shorts': False,
        'youtube_watch_fallback_used': False,
        'effective_page_url': final_url,
        'is_video': False,
        'video_url': '',
        'page_html_was_skipped': False,
        'page_html_raw_len': len(html or ''),
        'structured_html_text_len': len(structured_html_text or ''),
        'visible_page_text_len': len(visible_html_text or ''),
        'transcript_text': '',
        'transcript_attempted': False,
        'transcript_success': False,
        'transcript_method': '',
        'transcript_line_count': 0,
        'linked_recipe_used': False,
        'explicit_recipe_link': '',
    }
    source_metadata = parse_web_source_metadata(page_title, html, final_url)
    metrics = build_linked_page_metrics(evidence)
    return evidence, source_metadata, metrics



def build_tiktok_phone_source_context(target_url: str, evidence: dict) -> tuple[dict, str, dict, list[dict]]:
    initial_text = combine_text_blocks([
        evidence.get("visible_text_before_expand"),
        evidence.get("visible_text_after_expand"),
        evidence.get("expanded_caption_text"),
        evidence.get("raw_page_text"),
    ])
    ocr_handle_match = re.search(r'@([A-Za-z0-9._]{2,40})', initial_text or '')
    ocr_handle = source_safe_handle(ocr_handle_match.group(1)) if ocr_handle_match else ''

    video_doc = fetch_remote_html_document(target_url)
    video_meta = parse_tiktok_html_source(video_doc.get("html") or "", video_doc.get("url") or target_url)

    profile_url = normalize_profile_url(video_meta.get("profile_url") or '', video_doc.get("url") or target_url)
    if not profile_url and ocr_handle:
        profile_url = f"https://www.tiktok.com/{ocr_handle}"

    profile_doc = fetch_remote_html_document(profile_url) if profile_url else {"ok": False, "url": "", "html": "", "error": "missing_profile_url"}
    profile_meta = parse_tiktok_html_source(profile_doc.get("html") or "", profile_doc.get("url") or profile_url or target_url)

    handle = source_safe_handle(profile_meta.get("handle") or video_meta.get("handle") or ocr_handle)
    final_profile_url = normalize_profile_url(profile_doc.get("url") or profile_meta.get("profile_url") or profile_url or '', target_url)
    if not final_profile_url and handle:
        final_profile_url = f"https://www.tiktok.com/{handle}"

    creator_name = source_safe_text(
        profile_meta.get("creator_name")
        or profile_meta.get("channel_name")
        or video_meta.get("creator_name")
        or video_meta.get("channel_name")
        or handle.lstrip('@')
    )

    avatar_url = normalize_profile_url(
        profile_meta.get("avatar_url") or video_meta.get("avatar_url") or '',
        final_profile_url or target_url,
    )

    source_metadata = enrich_source_metadata({
        'source_platform': 'tiktok',
        'source_creator_name': creator_name,
        'source_channel_name': creator_name,
        'source_creator_handle': handle,
        'source_profile_url': final_profile_url,
        'source_page_domain': canonical_domain(final_profile_url or target_url),
        'source_avatar_url': avatar_url,
    }, 'tiktok', target_url)

    affinity_tokens = extract_tiktok_domain_affinity_tokens(source_metadata, creator_name, handle)

    candidate_map = {}

    def upsert_candidate(url: str, score: int, source_name: str, context: str = "") -> None:
        normalized = normalize_profile_url(unwrap_known_redirect_url(url or ""), final_profile_url or target_url)
        if not normalized or not looks_like_fetchable_external_page(normalized):
            return
        key = investigation_candidate_dedupe_key(normalized)
        payload = {
            "url": normalized,
            "score": int(score or 0),
            "source": source_name,
            "context": normalize_text_preserve_lines(context),
            "host": canonical_domain(normalized),
        }
        existing = candidate_map.get(key)
        if existing is None or int(payload.get("score") or 0) >= int(existing.get("score") or 0):
            candidate_map[key] = payload

    for candidate in extract_tiktok_external_site_candidates(
        profile_doc.get("html") or "",
        final_profile_url or target_url,
        creator_name,
        context_prefix="profile_html",
    ):
        upsert_candidate(candidate.get("url") or "", int(candidate.get("score") or 0), candidate.get("source") or "profile_html", candidate.get("context") or "")

    for candidate in extract_tiktok_external_site_candidates(
        video_doc.get("html") or "",
        video_doc.get("url") or target_url,
        creator_name,
        context_prefix="video_html",
    ):
        upsert_candidate(candidate.get("url") or "", int(candidate.get("score") or 0), candidate.get("source") or "video_html", candidate.get("context") or "")

    for url_value, source_name in [
        (profile_meta.get("external_site_url") or "", "profile_meta"),
        (video_meta.get("external_site_url") or "", "video_meta"),
    ]:
        if url_value:
            seed_score = score_recipe_link_candidate(url_value, "bio link", creator_name, source="description_marker") + 25
            upsert_candidate(url_value, seed_score, source_name, "bio link")

    for extracted in extract_urls_with_context(initial_text):
        seed_score = score_recipe_link_candidate(
            extracted.get("url") or "",
            extracted.get("context") or "",
            creator_name,
            source="description",
        )
        upsert_candidate(extracted.get("url") or "", seed_score, "phone_text", extracted.get("context") or "")

    external_candidates = rerank_tiktok_external_site_candidates(list(candidate_map.values()), affinity_tokens)
    external_site_url = external_candidates[0]["url"] if external_candidates else ""

    debug = {
        'tiktok_video_page_url': video_doc.get("url") or target_url,
        'tiktok_video_fetch_ok': bool(video_doc.get("ok")),
        'tiktok_video_fetch_error': video_doc.get("error") or '',
        'tiktok_profile_url': final_profile_url,
        'tiktok_profile_fetch_ok': bool(profile_doc.get("ok")),
        'tiktok_profile_fetch_error': profile_doc.get("error") or '',
        'tiktok_external_site_url': external_site_url,
        'tiktok_external_site_candidates': [
            {
                "url": candidate.get("url") or "",
                "score": int(candidate.get("score") or 0),
                "source": candidate.get("source") or "",
            }
            for candidate in external_candidates[:8]
        ],
        'tiktok_bio_text': source_safe_text(profile_meta.get("bio_text") or video_meta.get("bio_text") or '', 1000),
        'tiktok_domain_affinity_tokens': affinity_tokens[:10],
    }
    return source_metadata, external_site_url, debug, external_candidates


def parse_web_source_metadata(page_title: str, page_html: str, target_url: str) -> dict:
    page_domain = canonical_domain(target_url)
    site_name = extract_meta_tag(page_html, ['og:site_name'])
    ldjson_names = parse_json_ld_names(page_html)
    publisher_name = ldjson_names.get('publisher_name') or ''
    author_name = ldjson_names.get('author_name') or ''
    profile_url = ldjson_names.get('profile_url') or ''
    channel_name = source_safe_text(site_name or publisher_name or author_name or page_domain.split('.')[0])
    creator_name = source_safe_text(author_name or publisher_name or channel_name)
    return enrich_source_metadata({
        'source_platform': 'web',
        'source_creator_name': creator_name,
        'source_channel_name': channel_name,
        'source_profile_url': profile_url or (f'https://{page_domain}' if page_domain else ''),
        'source_page_domain': page_domain,
    }, 'web', target_url)


def avatar_file_suffix_from_url(url: str) -> str:
    lower = str(url or '').lower()
    for suffix in ('.png', '.webp', '.jpeg', '.jpg'):
        if suffix in lower:
            return suffix
    return '.jpg'


def download_remote_file(url: str, dest_path: Path, timeout: int = 20) -> bool:
    try:
        request = urllib.request.Request(str(url or '').strip(), headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(request, timeout=timeout) as response:
            payload = response.read()
        if not payload or len(payload) < 64:
            return False
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        dest_path.write_bytes(payload)
        return True
    except Exception:
        return False


async def fetch_source_avatar_from_profile(context, platform: str, profile_url: str, current_platform: str = "") -> str:
    normalized_profile_url = normalize_profile_url(profile_url or '', profile_url or '')
    if not normalized_profile_url or context is None:
        return ''

    page = None
    owner_context = None
    try:
        page, owner_context = await open_page_with_context_policy(
            context,
            current_platform=current_platform or platform or detect_platform_from_url(normalized_profile_url),
            url=normalized_profile_url,
            platform_hint=platform or detect_platform_from_url(normalized_profile_url),
        )
        await page.goto(normalized_profile_url, wait_until='domcontentloaded', timeout=LINKED_RECIPE_GOTO_TIMEOUT_MS)
        await page.wait_for_timeout(max(LINKED_RECIPE_WAIT_MS, 1500))
        try:
            profile_html_raw = await page.content()
        except Exception:
            profile_html_raw = ''

        avatar_url = ''
        if platform == 'instagram':
            avatar_url = normalize_profile_url(parse_instagram_html_source(profile_html_raw).get('avatar_url') or '', normalized_profile_url)
            if not avatar_url:
                avatar_url = await get_instagram_avatar_url_from_page(page, normalized_profile_url)
            if not avatar_url:
                meta_avatar = extract_meta_tag(profile_html_raw, ['image_src', 'og:image', 'og:image:secure_url', 'twitter:image', 'twitter:image:src'])
                meta_avatar = normalize_profile_url(meta_avatar or '', normalized_profile_url)
                if looks_like_instagram_avatar_url(meta_avatar):
                    avatar_url = meta_avatar
        elif platform == 'youtube':
            avatar_url = normalize_profile_url(parse_youtube_html_source(profile_html_raw).get('avatar_url') or '', normalized_profile_url)
            if not avatar_url:
                avatar_url = await get_youtube_avatar_url_from_page(page, normalized_profile_url)
            if not avatar_url:
                meta_avatar = extract_meta_tag(profile_html_raw, ['image_src', 'og:image', 'og:image:secure_url', 'twitter:image', 'twitter:image:src'])
                meta_avatar = normalize_profile_url(meta_avatar or '', normalized_profile_url)
                if looks_like_youtube_avatar_url(meta_avatar):
                    avatar_url = meta_avatar
        else:
            meta_avatar = extract_meta_tag(profile_html_raw, ['image_src', 'og:image', 'og:image:secure_url', 'twitter:image', 'twitter:image:src'])
            avatar_url = normalize_profile_url(meta_avatar or '', normalized_profile_url)

        return avatar_url or ''
    except Exception:
        return ''
    finally:
        await close_page_and_context(page, owner_context)


async def materialize_source_avatar(context, job_id: str, source_metadata: dict, current_platform: str = "") -> str:
    platform = (source_metadata.get('source_platform') or '').strip().lower()
    avatar_url = normalize_profile_url(source_metadata.get('source_avatar_url') or '', source_metadata.get('source_profile_url') or '')
    if not avatar_url and source_metadata.get('source_profile_url'):
        avatar_url = await fetch_source_avatar_from_profile(
            context,
            platform,
            source_metadata.get('source_profile_url') or '',
            current_platform=current_platform,
        )
    if not avatar_url:
        return ''

    avatar_path = SCREENSHOT_DIR / f"{job_id}_source_avatar{avatar_file_suffix_from_url(avatar_url)}"
    if not download_remote_file(avatar_url, avatar_path):
        return avatar_url

    upload_result = upload_bot_screenshot(
        job_id=job_id,
        image_path=str(avatar_path),
        debug_last_step='source_avatar_uploaded',
    )
    uploaded_url = str(upload_result.get('url') or '').strip()
    return uploaded_url or avatar_url


async def extract_source_metadata(page, platform: str, target_url: str, page_title: str = '', page_html: str = '', page_image_url: str = '') -> dict:
    platform = (platform or detect_platform_from_url(target_url) or 'web').lower()
    metadata = {
        'source_platform': platform,
        'source_creator_name': '',
        'source_creator_handle': '',
        'source_channel_name': '',
        'source_channel_key': '',
        'source_profile_url': '',
        'source_page_domain': canonical_domain(target_url),
        'creator_group_key': '',
    }

    if platform == 'youtube':
        anchor = choose_profile_candidate('youtube', await extract_anchor_candidates(page, [
            'ytd-watch-metadata ytd-channel-name a[href]', '#owner ytd-channel-name a[href]',
            'ytd-video-owner-renderer ytd-channel-name a[href]', 'ytd-channel-name a[href]', 'a[href^="/@"]'
        ], limit=40), target_url)
        html_meta = parse_youtube_html_source(page_html)
        channel_name = source_safe_text(anchor.get('text') or html_meta.get('channel_name') or parse_youtube_channel_from_title(page_title))
        handle = source_safe_handle(html_meta.get('handle') or '')
        profile_url = normalize_profile_url(anchor.get('href') or html_meta.get('profile_url') or '', 'https://www.youtube.com')
        if not handle and profile_url:
            parts = [part for part in urlparse(profile_url).path.split('/') if part]
            if parts and parts[0].startswith('@'):
                handle = source_safe_handle(parts[0])
        avatar_url = await get_youtube_avatar_url_from_page(page, 'https://www.youtube.com')
        if not avatar_url:
            avatar_url = html_meta.get('avatar_url') or ''
        metadata.update({
            'source_creator_name': channel_name,
            'source_channel_name': channel_name,
            'source_creator_handle': handle,
            'source_profile_url': profile_url,
            'source_page_domain': canonical_domain(profile_url or target_url),
            'source_avatar_url': avatar_url,
        })
        return enrich_source_metadata(metadata, 'youtube', target_url)

    if platform == 'instagram':
        anchor = choose_profile_candidate('instagram', await extract_anchor_candidates(page, ['header a[href]', 'article a[href]', 'main a[href]'], limit=80), target_url)
        html_meta = parse_instagram_html_source(page_html)
        meta_description = extract_meta_tag(page_html, ['og:description', 'twitter:description', 'description'])
        owner_hint = extract_instagram_owner_hint(combine_text_blocks([meta_description, page_title]))

        profile_url = normalize_profile_url(
            html_meta.get('profile_url')
            or (f"https://www.instagram.com/{str(owner_hint.get('handle') or '').lstrip('@')}/" if owner_hint.get('handle') else '')
            or anchor.get('href')
            or '',
            'https://www.instagram.com'
        )
        handle = source_safe_handle(owner_hint.get('handle') or html_meta.get('handle') or '')
        if not handle and profile_url:
            parts = [part for part in urlparse(profile_url).path.split('/') if part]
            if len(parts) == 1 and parts[0].lower() not in SOCIAL_PROFILE_RESERVED_PATHS:
                handle = source_safe_handle(parts[0])
        if handle and (not profile_url or is_instagram_source_metadata_suspicious({'source_profile_url': profile_url, 'source_creator_handle': handle})):
            profile_url = f"https://www.instagram.com/{handle.lstrip('@')}/"

        anchor_text = source_safe_text(anchor.get('text') or '')
        channel_name = source_safe_text(owner_hint.get('display_name') or html_meta.get('channel_name') or '')
        if not channel_name and anchor_text and not is_generic_source_label(anchor_text):
            channel_name = anchor_text
        if not channel_name:
            channel_name = source_safe_text(handle.lstrip('@'))
        avatar_url = await get_instagram_avatar_url_from_page(page, target_url, handle)
        if not avatar_url:
            avatar_url = html_meta.get('avatar_url') or ''
        metadata.update({
            'source_creator_name': channel_name,
            'source_channel_name': channel_name,
            'source_creator_handle': handle,
            'source_profile_url': profile_url,
            'source_page_domain': canonical_domain(profile_url or target_url),
            'source_avatar_url': avatar_url,
        })
        return enrich_source_metadata(metadata, 'instagram', target_url)

    if platform == 'tiktok':
        html_meta = parse_tiktok_html_source(page_html, target_url)
        metadata.update({
            'source_creator_name': html_meta.get('creator_name') or html_meta.get('channel_name') or '',
            'source_channel_name': html_meta.get('channel_name') or html_meta.get('creator_name') or '',
            'source_creator_handle': html_meta.get('handle') or '',
            'source_profile_url': html_meta.get('profile_url') or '',
            'source_page_domain': canonical_domain(html_meta.get('profile_url') or target_url),
            'source_avatar_url': html_meta.get('avatar_url') or '',
        })
        return enrich_source_metadata(metadata, 'tiktok', target_url)

    if platform == 'facebook':
        anchor = choose_profile_candidate('facebook', await extract_anchor_candidates(page, ['main a[href]', 'header a[href]', 'a[href]'], limit=120), target_url)
        title_name = source_safe_text(re.sub(r'\s*-\s*facebook\s*$', '', page_title or '', flags=re.IGNORECASE))
        channel_name = source_safe_text(anchor.get('text') or title_name)
        metadata.update({
            'source_creator_name': channel_name,
            'source_channel_name': channel_name,
            'source_profile_url': normalize_profile_url(anchor.get('href') or '', target_url),
            'source_page_domain': canonical_domain(anchor.get('href') or target_url),
        })
        return enrich_source_metadata(metadata, 'facebook', target_url)

    return parse_web_source_metadata(page_title, page_html, target_url)


def merge_source_metadata(primary: dict, fallback: dict, platform_hint: str = '', target_url: str = '') -> dict:
    primary = primary or {}
    fallback = fallback or {}
    merged = {}
    for key in ['source_platform', 'source_creator_name', 'source_creator_handle', 'source_channel_name', 'source_channel_key', 'source_profile_url', 'source_page_domain', 'creator_group_key', 'source_avatar_url']:
        merged[key] = primary.get(key) or fallback.get(key) or ''
    return enrich_source_metadata(merged, platform_hint or merged.get('source_platform') or '', target_url)

def get_platform_allowlist():
    raw = os.getenv("PLATFORM_ALLOWLIST", "").strip()
    if not raw:
        return None
    return [x.strip().lower() for x in raw.split(",") if x.strip()]


def detect_platform_from_url(url: str) -> str:
    host = (urlparse(url).netloc or "").lower()
    if "instagram.com" in host:
        return "instagram"
    if "facebook.com" in host or "fb.watch" in host:
        return "facebook"
    if "tiktok.com" in host:
        return "tiktok"
    if "youtube.com" in host or "youtu.be" in host:
        return "youtube"
    return "web"


def normalize_platform(platform: str | None, target_url: str) -> str:
    detected = detect_platform_from_url(target_url)
    p = (platform or "").strip().lower()

    if detected in {"instagram", "facebook", "tiktok", "youtube"}:
        return detected
    if p in {"instagram", "facebook", "tiktok", "youtube", "web", "general"}:
        return p
    return detected or "web"


def profile_key_for(platform: str, collector_profile_id: str = "") -> str:
    base = platform if platform in {"instagram", "facebook", "tiktok", "youtube"} else "web"
    profile_segment = sanitize_collector_profile_segment(collector_profile_id)
    if profile_segment and base in {"instagram", "facebook", "tiktok", "youtube"}:
        return f"{base}__{profile_segment}"
    return base


def persistent_profile_dir_for(platform: str, collector_profile_id: str = "") -> Path:
    return PROFILE_ROOT / profile_key_for(platform, collector_profile_id)


SOCIAL_SESSION_PLATFORMS = {"instagram", "facebook", "tiktok", "youtube"}


def session_platform_for_url(url: str = "", platform_hint: str = "") -> str:
    platform = normalize_platform(platform_hint or None, url or "")
    return platform if platform in SOCIAL_SESSION_PLATFORMS else ""


async def open_page_with_context_policy(
    base_context,
    *,
    current_platform: str,
    url: str,
    platform_hint: str = "",
    ignore_https_errors: bool = True,
):
    current = normalize_text(current_platform).lower()
    target = session_platform_for_url(url, platform_hint)

    if target and target == current:
        page = await base_context.new_page()
        return page, None

    browser = getattr(base_context, "browser", None)
    if callable(browser):
        browser = browser()

    if browser is not None:
        owner_context = await browser.new_context(ignore_https_errors=ignore_https_errors)
        page = await owner_context.new_page()
        return page, owner_context

    page = await base_context.new_page()
    return page, None


async def close_page_and_context(page, owner_context=None):
    if page is not None:
        try:
            await page.close()
        except Exception:
            pass
    if owner_context is not None:
        try:
            await owner_context.close()
        except Exception:
            pass


def is_youtube_shorts_url(url: str) -> bool:
    return "youtube.com/shorts/" in str(url or "").lower()


def extract_youtube_video_id(url: str) -> str | None:
    if not url:
        return None

    try:
        parsed = urlparse(url)

        if "youtu.be" in parsed.netloc.lower():
            video_id = parsed.path.strip("/").split("/")[0]
            return video_id or None

        if "youtube.com" in parsed.netloc.lower():
            if parsed.path.startswith("/shorts/"):
                parts = [p for p in parsed.path.split("/") if p]
                if len(parts) >= 2:
                    return parts[1]

            query = parse_qs(parsed.query or "")
            if query.get("v"):
                return query["v"][0]
    except Exception:
        return None

    return None


def build_youtube_watch_url(url: str) -> str | None:
    video_id = extract_youtube_video_id(url)
    if not video_id:
        return None
    return f"https://www.youtube.com/watch?v={video_id}"


def add_structured_line(lines, seen, label: str, value):
    if value is None:
        return

    if isinstance(value, (int, float, bool)):
        text = str(value)
    else:
        text = normalize_text_preserve_lines(value)

    if not text:
        return

    key = f"{label}:{text}"
    if key in seen:
        return

    seen.add(key)
    lines.append(f"{label}: {text}")


def extract_instruction_lines(value, lines, seen, depth: int = 0):
    if depth > 8 or value is None:
        return

    if isinstance(value, str):
        add_structured_line(lines, seen, "LDJSON_INSTRUCTION", value)
        return

    if isinstance(value, list):
        for item in value[:80]:
            extract_instruction_lines(item, lines, seen, depth + 1)
        return

    if isinstance(value, dict):
        if value.get("text"):
            add_structured_line(lines, seen, "LDJSON_INSTRUCTION", value.get("text"))
        elif value.get("name"):
            add_structured_line(lines, seen, "LDJSON_INSTRUCTION", value.get("name"))

        for nested_key in ["itemListElement", "item", "steps", "step", "recipeInstructions"]:
            if nested_key in value:
                extract_instruction_lines(value.get(nested_key), lines, seen, depth + 1)


def extract_structured_text_from_json_ld_node(node, lines, seen, depth: int = 0):
    if depth > 8 or node is None:
        return

    if isinstance(node, list):
        for item in node[:80]:
            extract_structured_text_from_json_ld_node(item, lines, seen, depth + 1)
        return

    if not isinstance(node, dict):
        return

    type_value = node.get("@type")
    if isinstance(type_value, list):
        add_structured_line(lines, seen, "LDJSON_TYPE", ", ".join(str(x) for x in type_value if x))
    elif type_value:
        add_structured_line(lines, seen, "LDJSON_TYPE", type_value)

    simple_fields = {
        "name": "LDJSON_NAME",
        "headline": "LDJSON_HEADLINE",
        "description": "LDJSON_DESCRIPTION",
        "caption": "LDJSON_CAPTION",
        "text": "LDJSON_TEXT",
        "articleBody": "LDJSON_ARTICLE_BODY",
        "recipeYield": "LDJSON_RECIPE_YIELD",
        "prepTime": "LDJSON_PREP_TIME",
        "cookTime": "LDJSON_COOK_TIME",
        "totalTime": "LDJSON_TOTAL_TIME",
        "keywords": "LDJSON_KEYWORDS",
        "recipeCategory": "LDJSON_RECIPE_CATEGORY",
        "recipeCuisine": "LDJSON_RECIPE_CUISINE",
    }

    for field, label in simple_fields.items():
        value = node.get(field)
        if isinstance(value, list):
            for item in value[:40]:
                add_structured_line(lines, seen, label, item)
        else:
            add_structured_line(lines, seen, label, value)

    ingredients = node.get("recipeIngredient")
    if isinstance(ingredients, list):
        for ingredient in ingredients[:120]:
            add_structured_line(lines, seen, "LDJSON_INGREDIENT", ingredient)

    if "recipeInstructions" in node:
        extract_instruction_lines(node.get("recipeInstructions"), lines, seen, depth + 1)

    for key, value in node.items():
        if key in simple_fields or key in {"recipeIngredient", "recipeInstructions"}:
            continue
        if isinstance(value, (dict, list)):
            extract_structured_text_from_json_ld_node(value, lines, seen, depth + 1)


def extract_structured_text_from_html(html: str) -> str:
    if not html:
        return ""

    lines = []
    seen = set()

    scripts = re.findall(
        r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>([\s\S]*?)</script>',
        html,
        flags=re.IGNORECASE,
    )

    for raw_script in scripts[:20]:
        raw_script = raw_script.strip()
        if not raw_script:
            continue

        try:
            parsed = json.loads(raw_script)
        except Exception:
            continue

        nodes = parsed[:40] if isinstance(parsed, list) else [parsed]
        for node in nodes:
            extract_structured_text_from_json_ld_node(node, lines, seen)

    return "\n".join(lines)


def clean_extracted_url(url: str | None) -> str | None:
    cleaned = str(url or "").strip()
    if not cleaned:
        return None

    cleaned = re.sub(r"[)\]}>,]+$", "", cleaned)
    cleaned = re.sub(r"[!?]+$", "", cleaned)
    cleaned = re.sub(r"\.\.\.$", "", cleaned)
    cleaned = cleaned.strip()
    return cleaned or None


def extract_urls_from_text(text: str) -> list[str]:
    source = str(text or "")
    if not source:
        return []

    matches = re.findall(r'https?://[^\s<>"\')\]}]+', source, flags=re.IGNORECASE)
    deduped = []
    seen = set()
    for match in matches:
        cleaned = clean_extracted_url(match)
        if cleaned and cleaned not in seen:
            seen.add(cleaned)
            deduped.append(cleaned)
    return deduped


def extract_urls_with_context(text: str, window: int = 180) -> list[dict]:
    source = str(text or "")
    if not source:
        return []

    results = []
    seen = set()
    pattern = re.compile(r'https?://[^\s<>"\')\]}]+', flags=re.IGNORECASE)

    for match in pattern.finditer(source):
        cleaned = clean_extracted_url(match.group(0))
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)

        line_start = source.rfind("\n", 0, match.start()) + 1
        line_end = source.find("\n", match.end())
        if line_end == -1:
            line_end = len(source)
        context = source[line_start:line_end].strip()

        if len(normalize_text(context)) < 20:
            ctx_start = max(0, match.start() - window)
            ctx_end = min(len(source), match.end() + window)
            context = source[ctx_start:ctx_end].strip()

        results.append({
            "url": cleaned,
            "context": normalize_text_preserve_lines(context),
        })

    return results


def is_blocked_recipe_link_host(url: str) -> bool:
    try:
        host = urlparse(url).netloc.replace("www.", "").lower()
        blocked_hosts = [
            "youtube.com",
            "youtu.be",
            "instagram.com",
            "facebook.com",
            "fb.watch",
            "tiktok.com",
            "pinterest.com",
            "amazon.com",
            "a.co",
            "barnesandnoble.com",
            "bookshop.org",
            "booksamillion.com",
            "linktr.ee",
            "shopmy.us",
            "shopltk.com",
            "liketk.it",
            "google.com",
            "developers.google.com",
            "support.google.com",
            "accounts.google.com",
        ]
        if looks_like_non_page_asset_host(host, url):
            return True
        return any(host == blocked or host.endswith(f".{blocked}") for blocked in blocked_hosts)
    except Exception:
        return True


def unwrap_known_redirect_url(url: str) -> str:
    cleaned = clean_extracted_url(url)
    if not cleaned:
        return ""

    try:
        parsed = urlparse(cleaned)
        host = parsed.netloc.replace("www.", "").lower()

        if host.endswith("youtube.com") and parsed.path == "/redirect":
            query = parse_qs(parsed.query or "")
            for key in ["q", "url", "target", "u", "redir"]:
                values = query.get(key) or []
                if values and values[0]:
                    nested = clean_extracted_url(values[0])
                    if nested and nested.startswith("http"):
                        return nested

        if host.endswith("google.com") and parsed.path.startswith("/url"):
            query = parse_qs(parsed.query or "")
            for key in ["q", "url"]:
                values = query.get(key) or []
                if values and values[0]:
                    nested = clean_extracted_url(values[0])
                    if nested and nested.startswith("http"):
                        return nested

        if host in {"l.instagram.com", "instagram.com", "www.instagram.com"}:
            query = parse_qs(parsed.query or "")
            for key in ["u", "url", "target"]:
                values = query.get(key) or []
                if values and values[0]:
                    nested = clean_extracted_url(values[0])
                    if nested and nested.startswith("http"):
                        return nested

    except Exception:
        return cleaned

    return cleaned



def is_wordpress_oembed_endpoint(url: str) -> bool:
    cleaned = clean_extracted_url(url)
    if not cleaned:
        return False
    try:
        parsed = urlparse(cleaned)
        path = (parsed.path or "").lower()
        return "/wp-json/oembed/" in path or path.endswith("/wp-json/oembed/1.0/embed")
    except Exception:
        return False


def normalize_investigation_candidate_url(url: str, base_url: str = "") -> str:
    normalized = normalize_profile_url(unwrap_known_redirect_url(url or ""), base_url)
    if not normalized:
        return ""

    try:
        parsed = urlparse(normalized)
        path = (parsed.path or "").lower()
        if "/wp-json/oembed/" in path or path.endswith("/wp-json/oembed/1.0/embed"):
            query = parse_qs(parsed.query or "")
            for key in ("url", "target", "href", "u"):
                values = query.get(key) or []
                if values and values[0]:
                    nested = normalize_profile_url(unwrap_known_redirect_url(values[0]), normalized)
                    if nested and nested != normalized:
                        return nested
    except Exception:
        return normalized

    return normalized


def looks_like_non_recipe_internal_page_url(url: str) -> bool:
    normalized = normalize_profile_url(url or "", url or "")
    if not normalized:
        return False

    try:
        parsed = urlparse(normalized)
        path = (parsed.path or "").lower().rstrip("/")
    except Exception:
        return False

    if not path:
        return False

    if path.endswith("/feed") or path.endswith("/comments") or path.endswith("/comments/feed"):
        return True
    if "/wp-json/" in path and not is_wordpress_oembed_endpoint(normalized):
        return True
    if re.search(r"/(?:tag|category|author|comments)(?:/|$)", path):
        return True

    return False


def looks_like_non_recipe_offer_page(
    url: str,
    page_title: str = "",
    meta_description: str = "",
    raw_text: str = "",
) -> bool:
    normalized = normalize_profile_url(url or "", url or "")
    if not normalized:
        return False

    try:
        path = (urlparse(normalized).path or "").lower()
    except Exception:
        path = ""

    title = normalize_text_preserve_lines(page_title).lower()
    meta = normalize_text_preserve_lines(meta_description).lower()
    raw = normalize_text_preserve_lines(raw_text).lower()
    combined = "\n".join([title, meta, raw[:2500]])

    recipe_markers = [
        "ingredients",
        "instructions",
        "recipe",
        "recipes",
        "מצרכים",
        "רכיבים",
        "אופן הכנה",
        "הוראות",
    ]
    offer_markers = [
        "course",
        "courses",
        "class",
        "classes",
        "workshop",
        "masterclass",
        "academy",
        "community",
        "join",
        "membership",
        "subscribe",
        "weekly recipes",
        "הצטרפות",
        "קהילה",
        "קהילת",
        "מועדון",
        "קורס",
    ]

    if any(marker in title for marker in offer_markers):
        if not any(marker in combined for marker in recipe_markers):
            return True

    if any(token in path for token in ["/course", "/courses", "/class", "/classes", "/workshop", "/academy", "קורס"]):
        if not any(marker in combined for marker in recipe_markers):
            return True

    return False


def line_looks_like_recipe_link(line: str) -> bool:
    lower = str(line or "").lower()
    return (
        "recipe:" in lower
        or "full recipe" in lower
        or "printable recipe" in lower
        or "written recipe" in lower
        or "get the recipe" in lower
        or "recipe link" in lower
        or "recipe here" in lower
        or "for the recipe" in lower
        or "google:" in lower
        or "g00gle" in lower
        or "link in bio" in lower
        or "blog" in lower
    )


def is_suspiciously_truncated_recipe_url(url: str, context_line: str = "") -> bool:
    try:
        parsed = urlparse(url)
        host = parsed.netloc.replace("www.", "").lower()
        path = (parsed.path or "").lower().rstrip("/")
        lower_context = str(context_line or "").lower()

        if "..." in lower_context or "…" in lower_context:
            return True

        # allrecipes and similar recipe URLs should not end right after the numeric id
        if host.endswith("allrecipes.com") and re.search(r"/recipe/\d+$", path):
            return True
    except Exception:
        return False

    return False


def score_recipe_link_candidate(url: str, context_line: str = "", title: str = "", source: str = "description") -> int:
    cleaned_url = unwrap_known_redirect_url(url)
    if not cleaned_url or is_blocked_recipe_link_host(cleaned_url):
        return -1000

    lower_context = str(context_line or "").lower()
    lower_title = str(title or "").lower()

    try:
        parsed = urlparse(cleaned_url)
        path = f"{parsed.path}{('?' + parsed.query) if parsed.query else ''}".lower()
        slug_parts = [part for part in re.split(r'[-_/]+', (parsed.path or '').lower()) if part]
    except Exception:
        return -1000

    score = 0

    if line_looks_like_recipe_link(lower_context):
        score += 120
    if "google:" in lower_context or "g00gle" in lower_context:
        score += 80
    if "comment" in lower_context and "recipe" in lower_context:
        score += 35
    if "/recipe" in path:
        score += 100
    elif "/recipes" in path:
        score += 90
    elif "recipe=" in path:
        score += 30

    if len(path) > 5:
        score += 10
    if len([part for part in slug_parts if len(part) >= 3]) >= 4:
        score += 25

    title_tokens = [
        token
        for token in re.sub(r"[^a-z0-9\s]", " ", lower_title).split()
        if len(token) >= 4
    ]
    token_hits = sum(1 for token in title_tokens if token in path)
    score += min(token_hits * 10, 40)

    if source == "dom_anchor" and ("/recipe" in path or "/recipes" in path):
        score += 35
    if source == "description_marker":
        score += 25

    if source in {"html_anchor", "dom_anchor"} and not line_looks_like_recipe_link(lower_context) and "/recipe" not in path and "/recipes" not in path:
        score -= 50

    if is_suspiciously_truncated_recipe_url(cleaned_url, lower_context):
        score -= 999

    if is_wordpress_oembed_endpoint(cleaned_url):
        score -= 120
    if looks_like_non_recipe_internal_page_url(cleaned_url):
        score -= 120
    if any(marker in path for marker in ["/course", "/courses", "/class", "/classes", "/workshop", "/academy", "קורס"]):
        score -= 90

    if "order my new book" in lower_context:
        score -= 120
    if "amazon" in lower_context:
        score -= 60
    if "barnes and noble" in lower_context:
        score -= 60
    if "bookshop" in lower_context:
        score -= 60
    if "booksamillion" in lower_context:
        score -= 60

    return score


def extract_explicit_recipe_link_from_youtube(description: str, title: str, candidate_urls=None) -> dict | None:
    candidates = []

    for line in normalize_text_preserve_lines(description).split("\n"):
        line = line.strip()
        if not line:
            continue
        for url in extract_urls_from_text(line):
            normalized_url = unwrap_known_redirect_url(url)
            if is_suspiciously_truncated_recipe_url(normalized_url, line):
                continue
            score = score_recipe_link_candidate(normalized_url, line, title, source="description")
            if score >= 40:
                candidates.append({
                    "url": normalized_url,
                    "source": "description",
                    "context": line,
                    "score": score,
                })

    recipe_line_regex = re.compile(
        r'(?:^|\b)(?:recipe|full recipe|printable recipe|written recipe|get the recipe|recipe link)\s*[:\-–—]?\s*(https?://[^\s<>"\')\]}]+)',
        flags=re.IGNORECASE,
    )
    for match in recipe_line_regex.finditer(str(description or "")):
        normalized_url = unwrap_known_redirect_url(match.group(1))
        if not normalized_url or is_suspiciously_truncated_recipe_url(normalized_url, match.group(0)):
            continue
        score = score_recipe_link_candidate(normalized_url, match.group(0), title, source="description_marker")
        if score >= 40:
            candidates.append({
                "url": normalized_url,
                "source": "description_marker",
                "context": match.group(0),
                "score": score,
            })

    for candidate_url in candidate_urls or []:
        normalized_url = unwrap_known_redirect_url(candidate_url)
        if not normalized_url or is_suspiciously_truncated_recipe_url(normalized_url):
            continue
        score = score_recipe_link_candidate(normalized_url, "", title, source="dom_anchor")
        if score >= 90:
            candidates.append({
                "url": normalized_url,
                "source": "dom_anchor",
                "context": "",
                "score": score,
            })

    if not candidates:
        return None

    deduped = {}
    for candidate in candidates:
        existing = deduped.get(candidate["url"])
        if not existing or candidate["score"] > existing["score"]:
            deduped[candidate["url"]] = candidate

    final_candidates = sorted(deduped.values(), key=lambda item: item["score"], reverse=True)
    return final_candidates[0]


def linked_page_looks_usable(linked_evidence: dict, linked_metrics: dict) -> bool:
    has_any_content = bool(
        choose_first_non_empty(
            linked_evidence.get("page_title"),
            linked_evidence.get("meta_description"),
            linked_evidence.get("raw_page_text"),
            linked_evidence.get("structured_html_text"),
        )
    )
    if not has_any_content:
        return False

    title = (linked_evidence.get("page_title") or "").lower()
    raw_text = normalize_text_preserve_lines(linked_evidence.get("raw_page_text")).lower()
    meta_description = normalize_text_preserve_lines(linked_evidence.get("meta_description")).lower()

    error_markers = [
        "page not found",
        "error 404",
        "404",
        "we couldn't find",
        "couldn't find that page",
        "requested page could not be found",
        "something went wrong",
    ]
    combined = "\n".join([title, meta_description, raw_text[:4000]])
    if any(marker in combined for marker in error_markers):
        return False

    effective_page_url = normalize_profile_url(
        linked_evidence.get("effective_page_url") or "",
        linked_evidence.get("effective_page_url") or "",
    )
    if is_wordpress_oembed_endpoint(effective_page_url):
        return False
    if looks_like_non_recipe_offer_page(
        effective_page_url,
        linked_evidence.get("page_title") or "",
        linked_evidence.get("meta_description") or "",
        raw_text,
    ) and not linked_metrics.get("looksRecipeDense"):
        return False

    structured_html_text = normalize_text_preserve_lines(linked_evidence.get("structured_html_text"))
    if any(token in structured_html_text for token in [
        "LDJSON_INGREDIENT",
        "LDJSON_INSTRUCTION",
        "LDJSON_RECIPE_YIELD",
        "LDJSON_PREP_TIME",
        "LDJSON_COOK_TIME",
    ]):
        return True

    if linked_metrics["looksRecipeDense"]:
        return True

    if linked_metrics["hasStrongTextEvidence"] and linked_metrics.get("hasFoodContext"):
        return True

    if "recipe" in title and len(raw_text) >= 120 and linked_metrics.get("hasFoodContext"):
        return True

    return False


def merge_youtube_linked_page_evidence(youtube_evidence: dict, linked_evidence: dict, explicit_link: str) -> dict:
    return merge_linked_page_evidence(
        youtube_evidence,
        linked_evidence,
        explicit_link,
        explicit_link_label="EXPLICIT_LINKED_RECIPE_URL",
    )


async def extract_candidate_anchor_urls(page) -> list[str]:
    try:
        urls = await page.evaluate(
            """
            () => {
              return Array.from(document.querySelectorAll('a[href]'))
                .map((a) => a.href || a.getAttribute('href') || '')
                .filter(Boolean)
                .slice(0, 500);
            }
            """
        )
        if not isinstance(urls, list):
            return []
        return [str(url).strip() for url in urls if str(url).strip()]
    except Exception:
        return []


async def click_first_matching(page, selectors=None, text_candidates=None):
    selectors = selectors or []
    text_candidates = text_candidates or []

    for selector in selectors:
        try:
            locator = page.locator(selector)
            count = await locator.count()

            for i in range(min(count, 5)):
                candidate = locator.nth(i)

                try:
                    if not await candidate.is_visible():
                        continue
                except Exception:
                    continue

                try:
                    await candidate.scroll_into_view_if_needed(timeout=1500)
                except Exception:
                    pass

                await candidate.click(timeout=2000)
                await page.wait_for_timeout(1200)
                return True, f"selector:{selector}"
        except Exception:
            pass

    for text_value in text_candidates:
        try:
            locator = page.get_by_text(text_value, exact=False)
            count = await locator.count()

            for i in range(min(count, 5)):
                candidate = locator.nth(i)

                try:
                    if not await candidate.is_visible():
                        continue
                except Exception:
                    continue

                try:
                    await candidate.scroll_into_view_if_needed(timeout=1500)
                except Exception:
                    pass

                await candidate.click(timeout=2000)
                await page.wait_for_timeout(1200)
                return True, f"text:{text_value}"
        except Exception:
            pass

    return False, None


async def expand_platform_content(page, platform: str):
    common_texts = [
        "See more", "see more", "Show more", "show more", "Read more", "read more",
        "More", "more", "Expand", "expand", "Show recipe", "show recipe",
        "עוד", "הצג עוד", "קראו עוד", "Mehr", "mehr",
    ]

    if platform == "instagram":
        return await click_first_matching(page, selectors=[], text_candidates=common_texts)

    if platform == "facebook":
        return await click_first_matching(
            page,
            selectors=[
                'div[role="button"][aria-label="See more"]',
                'div[role="button"][aria-label="Show more"]',
            ],
            text_candidates=common_texts,
        )

    if platform == "tiktok":
        return await click_first_matching(
            page,
            selectors=[
                '[data-e2e="browse-video-desc"] button',
                '[data-e2e="video-desc"] button',
            ],
            text_candidates=common_texts,
        )

    if platform == "youtube":
        current_url = page.url or ""

        if is_youtube_shorts_url(current_url):
            return await click_first_matching(
                page,
                selectors=[
                    'ytd-reel-player-overlay-renderer #description-inline-expander #expand',
                    'ytd-reel-player-overlay-renderer ytd-text-inline-expander #expand',
                    'ytd-reel-player-overlay-renderer #expand',
                ],
                text_candidates=["Show more", "More", "more", "See more", "Read more", "עוד"],
            )

        return await click_first_matching(
            page,
            selectors=[
                'ytd-watch-metadata ytd-text-inline-expander tp-yt-paper-button#expand',
                'ytd-watch-metadata #description-inline-expander tp-yt-paper-button#expand',
                'ytd-watch-metadata #expand',
                '#description-inline-expander #expand',
                'tp-yt-paper-button#expand',
                '#expand',
            ],
            text_candidates=["Show more", "More", "more", "See more", "Read more", "עוד"],
        )

    return await click_first_matching(page, selectors=[], text_candidates=common_texts)



SUBSTACK_FREE_POST_TEXT_CANDIDATES = [
    "Claim my free post",
    "claim my free post",
    "Continue reading this post for free",
    "continue reading this post for free",
    "Continue reading",
    "continue reading",
]

SUBSTACK_FREE_POST_SELECTORS = [
    'button:has-text("Claim my free post")',
    'a:has-text("Claim my free post")',
    'button:has-text("Continue reading this post for free")',
    'a:has-text("Continue reading this post for free")',
    'button:has-text("Continue reading")',
    'a:has-text("Continue reading")',
]

def looks_like_substack_claim_gate_text(text: str) -> bool:
    lower = normalize_text_preserve_lines(text or '').lower()
    if not lower:
        return False

    gate_markers = [
        'continue reading this post for free',
        'claim my free post',
        'purchase a paid subscription',
        'or purchase a paid subscription',
        'subscribe sign in',
    ]
    return any(marker in lower for marker in gate_markers)


def substack_text_has_recipe_body(text: str, baseline_text: str = '') -> bool:
    normalized = normalize_text_preserve_lines(text or '')
    lower = normalized.lower()
    if not normalized:
        return False
    if not looks_like_substack_claim_gate_text(normalized):
        return True
    growth = max(0, len(normalized) - len(normalize_text_preserve_lines(baseline_text or '')))
    if growth < 220:
        return False
    instruction_markers = [
        'mix', 'add', 'stir', 'combine', 'spread', 'roll', 'slice', 'serve',
        'מערבבים', 'מוסיפים', 'מגלגלים', 'מגישים', 'קוצצים', 'פורסים',
    ]
    if any(marker in lower for marker in instruction_markers):
        return True
    if re.search(r'^\s*(?:\d+[\).]|step\s*\d+)', normalized, flags=re.IGNORECASE | re.MULTILINE):
        return True
    return False


async def get_substack_visible_text(page, timeout_ms: int = 4000) -> str:
    candidates = []
    for selector in ['article', 'main', 'body']:
        try:
            value = await page.locator(selector).inner_text(timeout=timeout_ms)
            value = normalize_text_preserve_lines(value)
            if value:
                candidates.append(value)
        except Exception:
            pass
    if not candidates:
        return ''
    return max(candidates, key=len)


async def maybe_unlock_substack_free_post(page):
    current_url = page.url or ""
    current_host = canonical_domain(current_url)
    if not current_host.endswith("substack.com"):
        return False, False, None

    try:
        body_text = await get_substack_visible_text(page, timeout_ms=3000)
    except Exception:
        body_text = ""

    if not looks_like_substack_claim_gate_text(body_text):
        return False, False, None

    clicked, via = await click_first_matching(
        page,
        selectors=SUBSTACK_FREE_POST_SELECTORS,
        text_candidates=SUBSTACK_FREE_POST_TEXT_CANDIDATES,
    )
    if not clicked:
        try:
            clicked = await page.evaluate(
                """
                () => {
                  const needles = [/claim my free post/i, /continue reading this post for free/i, /continue reading/i];
                  const nodes = Array.from(document.querySelectorAll('button, a, [role="button"]'));
                  for (const node of nodes) {
                    const text = (node.innerText || node.textContent || '').trim();
                    if (!text) continue;
                    if (needles.some((rx) => rx.test(text))) {
                      try { node.click(); return true; } catch (e) {}
                    }
                  }
                  return false;
                }
                """
            )
            if clicked:
                via = 'js:text:substack_free_post_gate'
        except Exception:
            clicked = False
    if not clicked:
        return True, False, None

    before_text = body_text
    after_text = before_text

    for _ in range(2):
        try:
            await page.wait_for_load_state("networkidle", timeout=4000)
        except Exception:
            pass
        await page.wait_for_timeout(2400)
        try:
            await page.evaluate("window.scrollBy(0, Math.max(window.innerHeight * 0.85, 700))")
        except Exception:
            pass
        await page.wait_for_timeout(1200)
        try:
            after_text = await get_substack_visible_text(page, timeout_ms=3500)
        except Exception:
            after_text = ""
        if not looks_like_substack_claim_gate_text(after_text) or substack_text_has_recipe_body(after_text, before_text):
            return True, True, via

    try:
        await page.reload(wait_until="domcontentloaded", timeout=10000)
        await page.wait_for_timeout(2600)
        try:
            await page.evaluate("window.scrollBy(0, Math.max(window.innerHeight * 0.85, 700))")
        except Exception:
            pass
        await page.wait_for_timeout(1200)
        try:
            after_text = await get_substack_visible_text(page, timeout_ms=4000)
        except Exception:
            after_text = ""
    except Exception:
        pass

    unlocked = (not looks_like_substack_claim_gate_text(after_text)) or substack_text_has_recipe_body(after_text, before_text)
    return True, unlocked, via


async def get_meta_content(page, selectors) -> str:
    for selector in selectors:
        try:
            value = await page.locator(selector).get_attribute("content")
            if value:
                return normalize_text_preserve_lines(value)
        except Exception:
            pass
    return ""


async def get_meta_description(page) -> str:
    return await get_meta_content(
        page,
        [
            'meta[property="og:description"]',
            'meta[name="description"]',
            'meta[name="twitter:description"]',
        ],
    )


def normalize_possible_image_url(value: str | None, base_url: str = "") -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""

    if raw.startswith("//"):
        raw = f"https:{raw}"
    elif raw.startswith("/") and base_url:
        raw = urljoin(base_url, raw)
    elif raw and not re.match(r'^https?://', raw, flags=re.IGNORECASE):
        if base_url:
            raw = urljoin(base_url, raw)

    if not re.match(r'^https?://', raw, flags=re.IGNORECASE):
        return ""

    return raw


async def get_meta_image_url(page, base_url: str = "") -> str:
    selectors = [
        'meta[property="og:image"]',
        'meta[name="twitter:image"]',
    ]

    for selector in selectors:
        try:
            value = await page.locator(selector).get_attribute("content")
            normalized = normalize_possible_image_url(value, base_url)
            if normalized:
                return normalized
        except Exception:
            pass

    return ""


async def get_video_meta_url(page) -> str | None:
    candidates = [
        'meta[property="og:video"]',
        'meta[property="og:video:url"]',
        'meta[property="og:video:secure_url"]',
        'meta[name="twitter:player:stream"]',
    ]
    for selector in candidates:
        try:
            value = await page.locator(selector).get_attribute("content")
            if value:
                return value
        except Exception:
            pass
    return None


async def extract_expanded_caption_text(page, platform: str, target_url: str = "") -> str:
    current_url = page.url or target_url

    if platform == "youtube" and is_youtube_shorts_url(current_url):
        selectors = [
            'ytd-reel-player-overlay-renderer #description-inline-expander',
            'ytd-reel-player-overlay-renderer #description',
            '#description-inline-expander',
            '#description',
            'ytd-reel-player-overlay-renderer',
        ]
    else:
        selector_map = {
            "instagram": ["article h1", 'article span[dir="auto"]', "article", "main article"],
            "facebook": [
                '[data-ad-preview="message"]',
                '[data-ad-comet-preview="message"]',
                '[data-ad-rendering-role="story_message"]',
                'div[role="article"]',
                "main",
            ],
            "tiktok": ['[data-e2e="browse-video-desc"]', '[data-e2e="video-desc"]', "h1", "main"],
            "youtube": [
                'ytd-watch-metadata #description-inline-expander',
                'ytd-watch-metadata ytd-text-inline-expander',
                '#description-inline-expander',
                '#description',
                'ytd-watch-metadata',
                'main',
            ],
            "web": ["article", "main", "body"],
            "general": ["article", "main", "body"],
        }
        selectors = selector_map.get(platform, ["article", "main", "body"])

    best = ""

    for selector in selectors:
        try:
            locator = page.locator(selector)
            if await locator.count() == 0:
                continue

            candidate = locator.first

            try:
                if not await candidate.is_visible():
                    continue
            except Exception:
                continue

            txt = normalize_text_preserve_lines(await candidate.inner_text(timeout=2500))
            if len(txt) > len(best):
                best = txt
        except Exception:
            pass

    return best


async def extract_page_text(page) -> str:
    parts = []

    try:
        body = await page.locator("body").inner_text(timeout=8000)
        if body:
            parts.append(normalize_text_preserve_lines(body))
    except Exception:
        pass

    for selector in [
        'meta[property="og:title"]',
        'meta[property="og:description"]',
        'meta[name="description"]',
        'meta[name="twitter:title"]',
        'meta[name="twitter:description"]',
    ]:
        try:
            value = await page.locator(selector).get_attribute("content")
            if value:
                parts.append(normalize_text_preserve_lines(value))
        except Exception:
            pass

    return "\n\n".join([part for part in parts if part])


async def guess_media_type(page, target_url: str, platform: str) -> str:
    url = str(target_url or "").lower()

    if platform == "youtube":
        return "video"

    if platform == "tiktok":
        if "/photo/" in url:
            return "image"
        return "video"

    if platform == "instagram":
        if "/reel/" in url or "/reels/" in url:
            return "reel"

        try:
            if await page.locator("video").count() > 0:
                return "video"
        except Exception:
            pass

        try:
            img_count = await page.locator("img").count()
            if img_count > 1:
                return "carousel"
            if img_count == 1:
                return "image"
        except Exception:
            pass

        return "post"

    if platform == "facebook":
        try:
            if await page.locator("video").count() > 0:
                return "video"
        except Exception:
            pass

        try:
            img_count = await page.locator("img").count()
            if img_count > 1:
                return "carousel"
            if img_count == 1:
                return "image"
        except Exception:
            pass

        return "post"

    try:
        if await page.locator("video").count() > 0:
            return "video"
    except Exception:
        pass

    try:
        img_count = await page.locator("img").count()
        if img_count > 1:
            return "image_gallery"
        if img_count == 1:
            return "image"
    except Exception:
        pass

    return "page"


async def detect_video_info(page, target_url: str, platform: str, media_type_guess: str) -> tuple[bool, str | None]:
    video_url = None

    try:
        video_url = await page.evaluate(
            """
            () => {
              const videos = Array.from(document.querySelectorAll('video'));
              for (const v of videos) {
                const src = v.currentSrc || v.src || (v.querySelector('source') ? v.querySelector('source').src : null);
                if (src) return src;
              }
              return null;
            }
            """
        )
    except Exception:
        pass

    if not video_url:
        video_url = await get_video_meta_url(page)

    if video_url and str(video_url).startswith("blob:"):
        video_url = None

    is_video = bool(video_url) or media_type_guess in {"video", "reel"} or platform == "youtube"
    return is_video, video_url


def should_use_phone_worker(platform: str) -> bool:
    return (
        PHONE_WORKER_ENABLED
        and platform in PHONE_FALLBACK_PLATFORMS
        and platform in PHONE_WORKER_SCRIPTS
    )


def ocr_image_text(image_path: str) -> str:
    path = Path(image_path)
    if not path.exists():
        return ""

    try:
        result = subprocess.run(
            [TESSERACT_BIN, str(path), "stdout", "--psm", "6"],
            capture_output=True,
            text=True,
            timeout=60,
        )
        if result.returncode != 0:
            return ""
        return normalize_text_preserve_lines(result.stdout)
    except Exception:
        return ""


def run_phone_worker_job(job_id: str, platform: str, target_url: str) -> dict:
    script_path = PHONE_WORKER_SCRIPTS.get(platform)

    if not script_path:
        raise RuntimeError(f"No phone worker configured for platform={platform}")
    if not script_path.exists():
        raise RuntimeError(f"Phone worker script not found: {script_path}")

    result = subprocess.run(
        [str(script_path), target_url, job_id],
        capture_output=True,
        text=True,
        cwd=str(PHONE_WORKERS_DIR),
        timeout=180,
    )

    if result.returncode != 0:
        raise RuntimeError(
            f"Phone worker failed for {platform}. "
            f"stdout={trim_text(result.stdout or '', 2000)} "
            f"stderr={trim_text(result.stderr or '', 2000)}"
        )

    bundle_dir = PHONE_CAPTURE_ROOT / job_id
    primary_path = bundle_dir / "01_open.png"
    description_path = bundle_dir / "02_expanded.png"

    if not primary_path.exists():
        raise RuntimeError(f"Primary phone screenshot not found: {primary_path}")

    visible_before = ocr_image_text(str(primary_path))
    visible_after = ocr_image_text(str(description_path)) if description_path.exists() else ""
    media_type_guess = "video" if platform in {"tiktok", "youtube"} else "post"

    return {
        "collection_method": "android_phone_worker",
        "effective_page_url": target_url,
        "bundle_dir": str(bundle_dir),
        "primary_screenshot_path": str(primary_path),
        "description_screenshot_path": str(description_path) if description_path.exists() else None,
        "visible_text_before_expand": visible_before,
        "visible_text_after_expand": visible_after,
        "expanded_caption_text": visible_after or visible_before,
        "raw_page_text": combine_text_blocks([visible_before, visible_after]),
        "page_title": "",
        "meta_description": "",
        "page_html": "",
        "page_image_url": "",
        "media_type_guess": media_type_guess,
        "caption_expanded": description_path.exists(),
        "expand_attempted": True,
        "expand_success": description_path.exists(),
        "expand_method": "android_phone_worker_ocr_more_tap",
        "is_youtube_shorts": False,
        "current_page_is_youtube_shorts": False,
        "youtube_watch_fallback_used": False,
        "is_video": platform in {"tiktok", "youtube"},
        "video_url": target_url if platform in {"tiktok", "youtube"} else "",
        "page_html_was_skipped": True,
        "page_html_raw_len": 0,
        "structured_html_text_len": 0,
        "structured_html_text": "",
        "visible_page_text": visible_before,
        "visible_page_text_len": len(visible_before),
        "caption_before_len": len(visible_before),
        "caption_after_len": len(visible_after),
        "caption_before_lines": count_non_empty_lines(visible_before),
        "caption_after_lines": count_non_empty_lines(visible_after),
        "worker_stdout": trim_text(result.stdout or "", 4000),
        "worker_stderr": trim_text(result.stderr or "", 4000),
        "linked_recipe_used": False,
        "explicit_recipe_link": "",
    }


async def get_youtube_transcript_panel_text(page) -> str:
    try:
        transcript_text = await page.evaluate(
            """
            () => {
              const panel =
                document.querySelector('ytd-engagement-panel-section-list-renderer[target-id="engagement-panel-searchable-transcript"]') ||
                document.querySelector('ytd-transcript-search-panel-renderer') ||
                document.querySelector('ytd-transcript-renderer');

              if (!panel) return '';

              const lines = [];
              const segments = Array.from(panel.querySelectorAll('ytd-transcript-segment-renderer'));

              for (const segment of segments) {
                const textEl =
                  segment.querySelector('#segment-text') ||
                  segment.querySelector('.segment-text') ||
                  segment.querySelector('yt-formatted-string#segment-text');

                const text = (textEl?.textContent || '').replace(/\\s+/g, ' ').trim();
                if (text) lines.push(text);
              }

              if (lines.length > 0) return lines.join('\n');

              const fallbackText = (panel.textContent || '').replace(/\\s+/g, ' ').trim();
              return fallbackText || '';
            }
            """
        )
    except Exception:
        transcript_text = ""

    return normalize_text_preserve_lines(transcript_text)


async def extract_youtube_transcript_text(page) -> dict:
    result = {
        "text": "",
        "attempted": False,
        "success": False,
        "method": "",
        "line_count": 0,
    }

    current_url = page.url or ""
    if not current_url or "youtube.com/watch" not in current_url.lower():
        return result

    if is_youtube_shorts_url(current_url):
        return result

    existing = await get_youtube_transcript_panel_text(page)
    if existing:
        result["text"] = existing
        result["success"] = True
        result["method"] = "already_open"
        result["line_count"] = count_non_empty_lines(existing)
        return result

    transcript_text_candidates = [
        "Show transcript",
        "show transcript",
        "Transcript",
        "Open transcript",
        "View transcript",
        "Transkript anzeigen",
        "Transkript",
        "Mostrar transcripción",
        "Afficher la transcription",
    ]

    clicked, method = await click_first_matching(
        page,
        selectors=[
            'ytd-video-description-transcript-section-renderer button',
            'ytd-video-description-transcript-section-renderer tp-yt-paper-button',
            'button[aria-label*="transcript" i]',
            'tp-yt-paper-button[aria-label*="transcript" i]',
        ],
        text_candidates=transcript_text_candidates,
    )

    result["attempted"] = True

    if not clicked:
        menu_clicked, menu_method = await click_first_matching(
            page,
            selectors=[
                'ytd-watch-metadata button[aria-label*="More actions" i]',
                'ytd-watch-metadata yt-button-shape button[aria-label*="More actions" i]',
                'ytd-menu-renderer button[aria-label*="More actions" i]',
                'button[aria-label="More actions"]',
            ],
            text_candidates=["More actions", "more actions", "Actions", "Aktionen"],
        )

        if menu_clicked:
            clicked, method = await click_first_matching(
                page,
                selectors=[
                    'ytd-menu-service-item-renderer',
                    'tp-yt-paper-item',
                ],
                text_candidates=transcript_text_candidates,
            )
            if clicked and menu_method:
                method = f"{menu_method} -> {method or 'transcript_menu_item'}"

    if clicked:
        await page.wait_for_timeout(1800)
        transcript_text = await get_youtube_transcript_panel_text(page)
        result["text"] = transcript_text
        result["success"] = bool(transcript_text and count_non_empty_lines(transcript_text) >= 3)
        result["method"] = method or ""
        result["line_count"] = count_non_empty_lines(transcript_text)

    return result


async def collect_evidence(page, platform: str, target_url: str, original_target_url: str = "", youtube_watch_fallback_used: bool = False):
    await page.wait_for_timeout(4000)

    current_page_url = page.url or target_url
    original_is_youtube_shorts = platform == "youtube" and is_youtube_shorts_url(original_target_url or target_url)
    current_page_is_youtube_shorts = platform == "youtube" and is_youtube_shorts_url(current_page_url)

    caption_before = ""
    if platform in {"youtube", "instagram"} or current_page_is_youtube_shorts:
        caption_before = await extract_expanded_caption_text(page, platform, current_page_url)

    caption_expanded, expand_method = await expand_platform_content(page, platform)
    if caption_expanded:
        await page.wait_for_timeout(1500)

    page_title = trim_text(await page.title(), PAGE_TITLE_SUBMIT_MAX)
    visible_page_text = await extract_page_text(page)
    expanded_caption_text = await extract_expanded_caption_text(page, platform, current_page_url)
    if platform == "instagram":
        instagram_caption_fallback = sanitize_instagram_visible_text_for_caption_fallback(visible_page_text)
        if len(normalize_text_preserve_lines(expanded_caption_text)) < 80 and len(instagram_caption_fallback) > len(normalize_text_preserve_lines(expanded_caption_text)):
            expanded_caption_text = instagram_caption_fallback
    meta_description = await get_meta_description(page)
    page_image_url = await get_meta_image_url(page, current_page_url)

    try:
        page_html_raw = await page.content()
    except Exception:
        page_html_raw = ""

    structured_html_text = extract_structured_text_from_html(page_html_raw)
    raw_page_text = combine_text_blocks([visible_page_text, structured_html_text])

    page_html = trim_text(page_html_raw, PAGE_HTML_MAX_LEN) if page_html_raw else ""
    media_type_guess = await guess_media_type(page, current_page_url, platform)
    is_video, video_url = await detect_video_info(page, current_page_url, platform, media_type_guess)

    caption_before_len = len(caption_before or "")
    caption_after_len = len(expanded_caption_text or "")
    caption_before_lines = count_non_empty_lines(caption_before)
    caption_after_lines = count_non_empty_lines(expanded_caption_text)

    if platform == "youtube":
        expand_success = bool(caption_expanded) and (
            caption_after_len > (caption_before_len + 40)
            or caption_after_lines > (caption_before_lines + 2)
        )
    elif current_page_is_youtube_shorts:
        expand_success = bool(caption_expanded) and caption_after_len > (caption_before_len + 40)
    else:
        expand_success = bool(caption_expanded)

    return {
        "page_title": page_title,
        "raw_page_text": raw_page_text,
        "visible_page_text": visible_page_text,
        "visible_text_before_expand": caption_before,
        "visible_text_after_expand": expanded_caption_text,
        "structured_html_text": structured_html_text,
        "expanded_caption_text": expanded_caption_text,
        "meta_description": meta_description,
        "page_html": page_html,
        "page_image_url": page_image_url,
        "media_type_guess": media_type_guess,
        "caption_expanded": caption_expanded,
        "expand_attempted": True,
        "expand_method": expand_method or "",
        "expand_success": expand_success,
        "caption_before_len": caption_before_len,
        "caption_after_len": caption_after_len,
        "caption_before_lines": caption_before_lines,
        "caption_after_lines": caption_after_lines,
        "is_youtube_shorts": original_is_youtube_shorts,
        "current_page_is_youtube_shorts": current_page_is_youtube_shorts,
        "youtube_watch_fallback_used": youtube_watch_fallback_used,
        "effective_page_url": current_page_url,
        "is_video": is_video,
        "video_url": video_url or "",
        "page_html_was_skipped": False,
        "page_html_raw_len": len(page_html_raw or ""),
        "structured_html_text_len": len(structured_html_text or ""),
        "visible_page_text_len": len(visible_page_text or ""),
        "transcript_text": "",
        "transcript_attempted": False,
        "transcript_success": False,
        "transcript_method": "",
        "transcript_line_count": 0,
        "linked_recipe_used": False,
        "explicit_recipe_link": "",
    }


def build_linked_page_metrics(evidence: dict) -> dict:
    return evaluate_evidence_text(
        combine_text_blocks([
            evidence.get("page_title"),
            evidence.get("meta_description"),
            evidence.get("structured_html_text"),
            evidence.get("raw_page_text"),
        ])
    )


async def open_web_candidate_page(
    context,
    current_platform: str,
    url: str,
    *,
    goto_timeout_ms: int | None = None,
    wait_ms: int | None = None,
    platform_hint: str = "web",
):
    page = None
    owner_context = None
    goto_timeout = int(goto_timeout_ms or LINKED_RECIPE_GOTO_TIMEOUT_MS)
    settle_wait = max(int(wait_ms or LINKED_RECIPE_WAIT_MS), 600)

    try:
        page, owner_context = await open_page_with_context_policy(
            context,
            current_platform=current_platform,
            url=url,
            platform_hint=platform_hint,
        )
        await page.goto(url, wait_until="domcontentloaded", timeout=goto_timeout)
        await page.wait_for_timeout(settle_wait)

        try:
            gate_attempted, gate_unlocked, gate_via = await maybe_unlock_substack_free_post(page)
            if gate_attempted:
                await page.wait_for_timeout(1200)
            else:
                gate_unlocked = False
                gate_via = None
        except Exception:
            gate_attempted = False
            gate_unlocked = False
            gate_via = None

        evidence = await collect_evidence(
            page,
            "web",
            url,
            original_target_url=url,
            youtube_watch_fallback_used=False,
        )
        if gate_attempted:
            gate_note = f"SUBSTACK_FREE_POST_CLAIM_ATTEMPTED: {'success' if gate_unlocked else 'failed'}"
            evidence["raw_page_text"] = trim_text(combine_text_blocks([gate_note, evidence.get("raw_page_text") or ""]), RAW_PAGE_TEXT_SUBMIT_MAX)
            evidence["expanded_caption_text"] = trim_text(combine_text_blocks([gate_note, evidence.get("expanded_caption_text") or ""]), EXPANDED_CAPTION_SUBMIT_MAX)
            if gate_via:
                evidence["meta_description"] = trim_text(
                    combine_text_blocks([f"SUBSTACK_FREE_POST_CLAIM_VIA: {gate_via}", evidence.get("meta_description") or ""]),
                    META_DESCRIPTION_SUBMIT_MAX,
                )

        source_metadata = await extract_source_metadata(
            page,
            "web",
            url,
            page_title=evidence.get("page_title") or "",
            page_html=evidence.get("page_html") or "",
            page_image_url=evidence.get("page_image_url") or "",
        )
        metrics = build_linked_page_metrics(evidence)
        return page, evidence, source_metadata, metrics, owner_context
    except Exception:
        await close_page_and_context(page, owner_context)
        raise


async def evaluate_instagram_direct_candidate(
    *,
    context,
    result: dict,
    target_url: str,
    source_metadata: dict,
    instagram_hint_tokens: list[str],
    instagram_query_info: dict,
    direct_candidate_url: str,
    minimum_winner_score: int,
    direct_anchor_score: int = 0,
):
    page = None
    page_owner_context = None
    try:
        page, candidate_evidence, candidate_source_metadata, candidate_metrics, page_owner_context = await open_web_candidate_page(
            context,
            "instagram",
            direct_candidate_url,
        )
        candidate_match_score = score_instagram_linked_page_match(
            candidate_evidence,
            target_url,
            instagram_hint_tokens,
            source_metadata.get('source_creator_handle') or '',
            instagram_query_info,
        )
        has_reference = page_contains_instagram_reference(candidate_evidence, target_url)
        is_homepage = is_homepage_like_url(direct_candidate_url)
        slug_parts = [part for part in re.split(r'[-_/]+', urlparse(direct_candidate_url).path.lower()) if part]
        strong_direct_recipe_url = (
            not is_homepage
            and (
                "/recipe" in direct_candidate_url.lower()
                or "/recipes" in direct_candidate_url.lower()
                or len([part for part in slug_parts if len(part) >= 3]) >= 4
            )
        )
        effective_score = max(candidate_match_score, int(direct_anchor_score or 0))
        usable_candidate = linked_page_looks_usable(candidate_evidence, candidate_metrics) and (
            candidate_match_score >= minimum_winner_score
            or (strong_direct_recipe_url and effective_score >= 60)
        )
        add_investigation_candidate(
            result,
            direct_candidate_url,
            source='instagram_current_page_url',
            score=effective_score,
            usable=usable_candidate,
            reason='direct_url_evaluated',
            extra={
                'page_title': candidate_evidence.get('page_title') or '',
                'direct_site_has_reference': has_reference,
                'direct_site_is_homepage': is_homepage,
                'direct_anchor_score': int(direct_anchor_score or 0),
            },
        )
        if usable_candidate and (has_reference or not is_homepage):
            return {
                'page': page,
                'evidence': candidate_evidence,
                'source_metadata': candidate_source_metadata,
                'url': direct_candidate_url,
                'score': effective_score,
                'site_root': direct_candidate_url,
            }
    except Exception as direct_err:
        add_investigation_candidate(
            result,
            direct_candidate_url,
            source='instagram_current_page_url',
            score=int(direct_anchor_score or 0),
            usable=False,
            reason='direct_url_open_failed',
            extra={'error': f'{type(direct_err).__name__}: {direct_err}'},
        )
        return None
    finally:
        if page is not None:
            try:
                await page.close()
            except Exception:
                pass
    return None


def prepare_submission_evidence(evidence: dict) -> dict:
    prepared = dict(evidence)
    prepared["page_title"] = trim_text(prepared.get("page_title", ""), PAGE_TITLE_SUBMIT_MAX)
    prepared["raw_page_text"] = trim_text(prepared.get("raw_page_text", ""), RAW_PAGE_TEXT_SUBMIT_MAX)
    prepared["expanded_caption_text"] = trim_text(prepared.get("expanded_caption_text", ""), EXPANDED_CAPTION_SUBMIT_MAX)
    prepared["transcript_text"] = trim_text(prepared.get("transcript_text", ""), SUBMIT_TRANSCRIPT_MAX_LEN)
    prepared["meta_description"] = trim_text(prepared.get("meta_description", ""), META_DESCRIPTION_SUBMIT_MAX)
    prepared["page_image_url"] = trim_text(prepared.get("page_image_url", ""), 2000)
    prepared["visible_text_before_expand"] = trim_text(prepared.get("visible_text_before_expand", ""), VISIBLE_TEXT_BEFORE_SUBMIT_MAX)
    prepared["visible_text_after_expand"] = trim_text(prepared.get("visible_text_after_expand", ""), VISIBLE_TEXT_AFTER_SUBMIT_MAX)
    prepared["page_html"] = trim_text(prepared.get("page_html", ""), PAGE_HTML_MAX_LEN)
    return prepared


def extract_existing_job_submission_evidence(job_row: dict | None) -> dict:
    row = job_row if isinstance(job_row, dict) else {}
    return {
        "page_title": normalize_text_preserve_lines(row.get("page_title") or ""),
        "raw_page_text": normalize_text_preserve_lines(row.get("raw_page_text") or ""),
        "expanded_caption_text": normalize_text_preserve_lines(row.get("expanded_caption_text") or ""),
        "meta_description": normalize_text_preserve_lines(row.get("meta_description") or ""),
        "visible_text_before_expand": normalize_text_preserve_lines(row.get("visible_text_before_expand") or ""),
        "visible_text_after_expand": normalize_text_preserve_lines(row.get("visible_text_after_expand") or ""),
        "page_html": str(row.get("page_html") or ""),
        "page_image_url": normalize_text(row.get("page_image_url") or row.get("debug_screenshot_url") or row.get("screenshot_url") or ""),
        "media_type_guess": normalize_text(row.get("media_type_guess") or ""),
        "video_url": normalize_text(row.get("video_url") or row.get("target_url") or ""),
    }


def has_existing_job_submission_evidence(evidence: dict | None) -> bool:
    payload = evidence if isinstance(evidence, dict) else {}
    keys = [
        "page_title",
        "raw_page_text",
        "expanded_caption_text",
        "meta_description",
        "visible_text_before_expand",
        "visible_text_after_expand",
        "page_html",
        "page_image_url",
    ]
    return any(str(payload.get(key) or "").strip() for key in keys)


def build_phone_worker_fallback_evidence(
    *,
    job_id: str,
    platform: str,
    target_url: str,
    current_job: dict | None = None,
    error_text: str = "",
) -> tuple[dict, dict]:
    existing_job = current_job if isinstance(current_job, dict) else None
    existing_snapshot = extract_existing_job_submission_evidence(existing_job)

    if not has_existing_job_submission_evidence(existing_snapshot):
        try:
            existing_job = get_job(job_id)
            existing_snapshot = extract_existing_job_submission_evidence(existing_job)
        except Exception:
            existing_job = existing_job or {}
            existing_snapshot = extract_existing_job_submission_evidence(existing_job)

    if not has_existing_job_submission_evidence(existing_snapshot):
        return {}, {
            "fallback_used": False,
            "reason": "no_existing_job_submission_evidence",
            "job_id": job_id,
            "platform": platform,
            "error": trim_text(error_text or "", 500),
        }

    row = existing_job if isinstance(existing_job, dict) else {}
    primary_screenshot_url = normalize_text(
        row.get("debug_screenshot_url")
        or row.get("screenshot_url")
        or existing_snapshot.get("page_image_url")
        or ""
    )

    visible_before = normalize_text_preserve_lines(
        existing_snapshot.get("visible_text_before_expand")
        or existing_snapshot.get("expanded_caption_text")
        or existing_snapshot.get("meta_description")
        or existing_snapshot.get("raw_page_text")
        or ""
    )
    visible_after = normalize_text_preserve_lines(
        existing_snapshot.get("visible_text_after_expand")
        or existing_snapshot.get("expanded_caption_text")
        or ""
    )
    expanded_caption_text = trim_text(
        combine_text_blocks([
            existing_snapshot.get("expanded_caption_text") or "",
            existing_snapshot.get("visible_text_after_expand") or "",
            existing_snapshot.get("visible_text_before_expand") or "",
            existing_snapshot.get("meta_description") or "",
        ]),
        max(EXPANDED_CAPTION_SUBMIT_MAX, 12000),
    )
    raw_page_text = trim_text(
        combine_text_blocks([
            existing_snapshot.get("raw_page_text") or "",
            expanded_caption_text,
            visible_before,
            visible_after,
            existing_snapshot.get("meta_description") or "",
        ]),
        max(RAW_PAGE_TEXT_SUBMIT_MAX, 16000),
    )

    evidence = {
        "collection_method": "server_first_pass_fallback",
        "effective_page_url": normalize_text(row.get("target_url") or target_url),
        "bundle_dir": "",
        "primary_screenshot_path": "",
        "description_screenshot_path": None,
        "primary_screenshot_url": primary_screenshot_url,
        "visible_text_before_expand": visible_before,
        "visible_text_after_expand": visible_after,
        "expanded_caption_text": expanded_caption_text,
        "raw_page_text": raw_page_text,
        "page_title": existing_snapshot.get("page_title") or "",
        "meta_description": existing_snapshot.get("meta_description") or "",
        "page_html": existing_snapshot.get("page_html") or "",
        "page_image_url": existing_snapshot.get("page_image_url") or primary_screenshot_url,
        "media_type_guess": existing_snapshot.get("media_type_guess") or ("video" if platform in {"tiktok", "youtube"} else "post"),
        "caption_expanded": bool(visible_after or expanded_caption_text),
        "expand_attempted": False,
        "expand_success": False,
        "expand_method": "server_first_pass_fallback",
        "is_youtube_shorts": False,
        "current_page_is_youtube_shorts": False,
        "youtube_watch_fallback_used": False,
        "is_video": platform in {"tiktok", "youtube"},
        "video_url": normalize_text(existing_snapshot.get("video_url") or row.get("target_url") or target_url),
        "page_html_was_skipped": not bool(existing_snapshot.get("page_html")),
        "page_html_raw_len": len(str(existing_snapshot.get("page_html") or "")),
        "structured_html_text_len": 0,
        "structured_html_text": "",
        "visible_page_text": visible_before,
        "visible_page_text_len": len(visible_before),
        "caption_before_len": len(visible_before),
        "caption_after_len": len(visible_after),
        "caption_before_lines": count_non_empty_lines(visible_before),
        "caption_after_lines": count_non_empty_lines(visible_after),
        "worker_stdout": "",
        "worker_stderr": trim_text(error_text or "", 2000),
        "linked_recipe_used": False,
        "explicit_recipe_link": "",
    }

    return evidence, {
        "fallback_used": True,
        "reason": "server_first_pass_fallback",
        "job_id": job_id,
        "platform": platform,
        "error": trim_text(error_text or "", 500),
        "reused_screenshot_url": bool(primary_screenshot_url),
        "raw_page_text_len": len(raw_page_text),
        "expanded_caption_len": len(expanded_caption_text),
    }


def merge_phone_worker_evidence_with_existing_job(
    *,
    job_id: str,
    evidence: dict,
    current_job: dict | None = None,
) -> tuple[dict, dict]:
    base_evidence = dict(evidence or {})
    existing_job = current_job if isinstance(current_job, dict) else None
    existing_snapshot = extract_existing_job_submission_evidence(existing_job)

    if not has_existing_job_submission_evidence(existing_snapshot):
        try:
            existing_job = get_job(job_id)
            existing_snapshot = extract_existing_job_submission_evidence(existing_job)
        except Exception:
            existing_job = existing_job or {}
            existing_snapshot = extract_existing_job_submission_evidence(existing_job)

    if not has_existing_job_submission_evidence(existing_snapshot):
        return base_evidence, {
            "merged": False,
            "reason": "no_existing_job_submission_evidence",
            "job_id": job_id,
        }

    merged = dict(base_evidence)
    before_lengths = {
        "raw_page_text": len(str(merged.get("raw_page_text") or "")),
        "expanded_caption_text": len(str(merged.get("expanded_caption_text") or "")),
        "meta_description": len(str(merged.get("meta_description") or "")),
        "visible_text_before_expand": len(str(merged.get("visible_text_before_expand") or "")),
        "visible_text_after_expand": len(str(merged.get("visible_text_after_expand") or "")),
        "page_html": len(str(merged.get("page_html") or "")),
    }
    existing_lengths = {
        "raw_page_text": len(str(existing_snapshot.get("raw_page_text") or "")),
        "expanded_caption_text": len(str(existing_snapshot.get("expanded_caption_text") or "")),
        "meta_description": len(str(existing_snapshot.get("meta_description") or "")),
        "visible_text_before_expand": len(str(existing_snapshot.get("visible_text_before_expand") or "")),
        "visible_text_after_expand": len(str(existing_snapshot.get("visible_text_after_expand") or "")),
        "page_html": len(str(existing_snapshot.get("page_html") or "")),
    }

    merged["page_title"] = choose_first_non_empty(
        merged.get("page_title"),
        existing_snapshot.get("page_title"),
    )
    merged["page_html"] = trim_text(
        choose_first_non_empty(merged.get("page_html"), existing_snapshot.get("page_html")),
        PAGE_HTML_MAX_LEN,
    )
    merged["meta_description"] = trim_text(
        combine_text_blocks([
            merged.get("meta_description"),
            existing_snapshot.get("meta_description"),
        ]),
        max(META_DESCRIPTION_SUBMIT_MAX, 6000),
    )
    merged["visible_text_before_expand"] = trim_text(
        combine_text_blocks([
            merged.get("visible_text_before_expand"),
            existing_snapshot.get("visible_text_before_expand"),
        ]),
        max(VISIBLE_TEXT_BEFORE_SUBMIT_MAX, 6000),
    )
    merged["visible_text_after_expand"] = trim_text(
        combine_text_blocks([
            merged.get("visible_text_after_expand"),
            existing_snapshot.get("visible_text_after_expand"),
            existing_snapshot.get("expanded_caption_text"),
        ]),
        max(VISIBLE_TEXT_AFTER_SUBMIT_MAX, 12000),
    )
    merged["expanded_caption_text"] = trim_text(
        combine_text_blocks([
            merged.get("expanded_caption_text"),
            existing_snapshot.get("expanded_caption_text"),
            existing_snapshot.get("visible_text_after_expand"),
            existing_snapshot.get("visible_text_before_expand"),
            existing_snapshot.get("meta_description"),
        ]),
        max(EXPANDED_CAPTION_SUBMIT_MAX, 12000),
    )
    merged["raw_page_text"] = trim_text(
        combine_text_blocks([
            merged.get("raw_page_text"),
            merged.get("expanded_caption_text"),
            existing_snapshot.get("raw_page_text"),
            existing_snapshot.get("expanded_caption_text"),
            existing_snapshot.get("meta_description"),
            existing_snapshot.get("visible_text_after_expand"),
            existing_snapshot.get("visible_text_before_expand"),
        ]),
        max(RAW_PAGE_TEXT_SUBMIT_MAX, 16000),
    )
    merged["page_image_url"] = choose_first_non_empty(
        merged.get("page_image_url"),
        existing_snapshot.get("page_image_url"),
    )
    merged["media_type_guess"] = choose_first_non_empty(
        merged.get("media_type_guess"),
        existing_snapshot.get("media_type_guess"),
    )
    merged["video_url"] = choose_first_non_empty(
        merged.get("video_url"),
        existing_snapshot.get("video_url"),
    )

    after_lengths = {
        "raw_page_text": len(str(merged.get("raw_page_text") or "")),
        "expanded_caption_text": len(str(merged.get("expanded_caption_text") or "")),
        "meta_description": len(str(merged.get("meta_description") or "")),
        "visible_text_before_expand": len(str(merged.get("visible_text_before_expand") or "")),
        "visible_text_after_expand": len(str(merged.get("visible_text_after_expand") or "")),
        "page_html": len(str(merged.get("page_html") or "")),
    }

    merged_fields = []
    for field_name in [
        "page_title",
        "raw_page_text",
        "expanded_caption_text",
        "meta_description",
        "visible_text_before_expand",
        "visible_text_after_expand",
        "page_html",
        "page_image_url",
    ]:
        before_value = str(base_evidence.get(field_name) or "").strip()
        after_value = str(merged.get(field_name) or "").strip()
        existing_value = str(existing_snapshot.get(field_name) or "").strip()
        if not existing_value:
            continue
        if not before_value and after_value:
            merged_fields.append(field_name)
            continue
        if field_name in after_lengths and after_lengths[field_name] > before_lengths.get(field_name, 0):
            merged_fields.append(field_name)

    return merged, {
        "merged": bool(merged_fields),
        "job_id": job_id,
        "reason": "merged_existing_job_submission_evidence" if merged_fields else "existing_evidence_already_present",
        "merged_fields": merged_fields,
        "before_lengths": before_lengths,
        "existing_lengths": existing_lengths,
        "after_lengths": after_lengths,
    }


TERMINAL_JOB_STATUSES = {"done", "failed", "completed", "cancelled", "canceled"}
TERMINAL_DEBUG_STATUSES = {"completed", "failed", "analysis_completed", "done"}
TERMINAL_DECISIONS = {"usable", "needs_ai_review", "needs_review", "dismissed", "failed"}
VISUAL_IMAGE_APPLY_WAIT_SECONDS = int(os.getenv("VISUAL_IMAGE_APPLY_WAIT_SECONDS", "60").strip() or "60")
VISUAL_IMAGE_APPLY_POLL_SECONDS = float(os.getenv("VISUAL_IMAGE_APPLY_POLL_SECONDS", "2").strip() or "2")


async def wait_for_job_analysis_completion(job_id: str, timeout_seconds: int = VISUAL_IMAGE_APPLY_WAIT_SECONDS):
    deadline = asyncio.get_running_loop().time() + max(timeout_seconds, 1)
    last_job = None

    while asyncio.get_running_loop().time() < deadline:
        try:
            last_job = await asyncio.to_thread(get_job, job_id)
        except Exception:
            last_job = last_job or None

        if isinstance(last_job, dict):
            status = str(last_job.get("status") or "").strip().lower()
            debug_status = str(last_job.get("debug_status") or "").strip().lower()
            decision = str(last_job.get("decision") or "").strip().lower()
            if status in TERMINAL_JOB_STATUSES or debug_status in TERMINAL_DEBUG_STATUSES or decision in TERMINAL_DECISIONS:
                return last_job

        await asyncio.sleep(max(VISUAL_IMAGE_APPLY_POLL_SECONDS, 0.5))

    return last_job


def switch_job_platform_for_analysis(job_id: str, original_platform: str, effective_analysis_platform: str, linked_recipe_used: bool) -> str:
    original = str(original_platform or "").strip().lower()
    effective = str(effective_analysis_platform or "").strip().lower()

    if not linked_recipe_used or not effective or effective == original:
        return original or effective

    try:
        update_job(job_id, {"platform": effective})
        append_job_debug_log(
            job_id,
            f"Switching BotJob platform from {original or 'unknown'} to {effective} for analyzer routing; source_platform metadata remains unchanged.",
            debug_last_step="analysis_platform_switched",
        )
        return effective
    except Exception as switch_err:
        append_job_debug_log(
            job_id,
            f"Failed switching BotJob platform to {effective}: {type(switch_err).__name__}: {switch_err}",
            debug_last_step="analysis_platform_switch_failed",
        )
        return original or effective


async def restore_job_platform_after_analysis(job_id: str, submission_platform: str, original_platform: str):
    submission = str(submission_platform or "").strip().lower()
    original = str(original_platform or "").strip().lower()
    if not submission or not original or submission == original:
        return

    await wait_for_job_analysis_completion(job_id)

    try:
        update_job(job_id, {"platform": original})
        append_job_debug_log(
            job_id,
            f"Restored BotJob platform from {submission} to {original} after analyzer routing completed.",
            debug_last_step="analysis_platform_restored",
        )
    except Exception as restore_err:
        append_job_debug_log(
            job_id,
            f"Failed restoring BotJob platform to {original}: {type(restore_err).__name__}: {restore_err}",
            debug_last_step="analysis_platform_restore_failed",
        )


async def apply_visual_recipe_image_after_analysis(job_id: str, recipe_id: str, visual_image_url: str, primary_screenshot_url: str = ""):
    visual_image_url = choose_first_non_empty(visual_image_url)
    if not recipe_id or not visual_image_url:
        return

    final_job = await wait_for_job_analysis_completion(job_id)
    decision = str((final_job or {}).get("decision") or "").strip().lower()

    if decision == "failed":
        append_job_debug_log(
            job_id,
            "Skipping visual recipe image apply because job finished as failed.",
            debug_last_step="visual_image_skipped",
        )
        return

    try:
        recipe = await asyncio.to_thread(get_recipe, recipe_id)
    except Exception as recipe_err:
        append_job_debug_log(
            job_id,
            f"Could not load recipe before visual image apply: {type(recipe_err).__name__}: {recipe_err}",
            debug_last_step="visual_image_apply_failed",
        )
        return

    current_image_url = choose_first_non_empty(recipe.get("image_url"), recipe.get("debug_image_url"))

    if current_image_url == visual_image_url:
        append_job_debug_log(
            job_id,
            f"Visual recipe image already applied: {visual_image_url}",
            debug_last_step="visual_image_already_applied",
        )
        return

    looks_like_uploaded_debug_image = "base44.app/api/apps/" in str(current_image_url or "") or "supabase.co/storage/" in str(current_image_url or "")
    should_replace = not current_image_url or looks_like_uploaded_debug_image or current_image_url == primary_screenshot_url

    if not should_replace:
        append_job_debug_log(
            job_id,
            f"Skipping visual recipe image apply because recipe already has a non-debug image: {current_image_url}",
            debug_last_step="visual_image_preserved",
        )
        return

    try:
        await asyncio.to_thread(update_recipe, recipe_id, {"image_url": visual_image_url})
        append_job_debug_log(
            job_id,
            f"Applied visual recipe image to recipe {recipe_id}: {visual_image_url}",
            debug_last_step="visual_image_applied",
        )
        update_recipe_debug(recipe_id, debug_log_append=f"Applied visual recipe image from linked page for BotJob {job_id}")
    except Exception as update_err:
        append_job_debug_log(
            job_id,
            f"Failed applying visual recipe image: {type(update_err).__name__}: {update_err}",
            debug_last_step="visual_image_apply_failed",
        )


INVESTIGATION_CLUE_PATTERNS = [
    ("link_in_bio", re.compile(r"\blink\s+in\s+bio\b|\bbio\s+link\b|לינק\s+בביו|בביו", flags=re.IGNORECASE)),
    ("full_recipe_on_website", re.compile(r"full\s+(?:written\s+)?recipe.*(?:website|blog)|recipe.*(?:website|blog)|באתר|בבלוג|באתר\s+מתוק\s+בריא", flags=re.IGNORECASE)),
    ("google_creator_recipe", re.compile(r"\bg(?:oogle|00gle)\b", flags=re.IGNORECASE)),
    ("comment_for_dm_recipe", re.compile(r"comment\s+[\"'“”A-Za-z0-9_ -]{0,40}(?:recipe|save)|dm\s+you\s+the\s+recipe|אשלח\s+קישור\s+בהודעה\s+פרטית|הודעה\s+פרטית", flags=re.IGNORECASE)),
    ("website_hint", re.compile(r"website|blog|site|recipe\s+here|written\s+recipe|full\s+recipe|אתר|בלוג", flags=re.IGNORECASE)),
]


def detect_investigation_clues(text: str, rules: dict | None = None) -> list[str]:
    source = normalize_text_preserve_lines(text)
    if not source:
        return []

    clues = []
    seen = set()
    for clue_key, pattern in INVESTIGATION_CLUE_PATTERNS:
        if pattern.search(source):
            clues.append(clue_key)
            seen.add(clue_key)

    for phrase in runtime_string_list(rules or {}, "clue_phrases"):
        if phrase and re.search(re.escape(phrase), source, flags=re.IGNORECASE):
            clue_key = f"runtime:{phrase}"
            if clue_key not in seen:
                clues.append(clue_key)
                seen.add(clue_key)

    return clues


def make_investigation_result(mode: str = "", trigger: str = "inline_after_first_pass") -> dict:
    return {
        "attempted": False,
        "mode": mode,
        "trigger": trigger,
        "clues": [],
        "reasons": [],
        "candidates": [],
        "winner_url": "",
        "winner_score": None,
        "winner_source_metadata": {},
        "source_metadata_updates": {},
        "merged_evidence": None,
        "analysis_platform_hint": "",
        "effective_analysis_platform": "",
        "breadcrumb": [],
        "no_winner_reason": "",
        "linked_recipe_used": False,
        "explicit_recipe_link": "",
        "primary_submission_screenshot_path": "",
        "secondary_submission_screenshot_path": "",
        "debug": {},
    }


def add_investigation_candidate(
    result: dict,
    url: str,
    *,
    source: str = "",
    score: int | None = None,
    usable: bool | None = None,
    reason: str = "",
    extra: dict | None = None,
) -> None:
    normalized_url = normalize_profile_url(url or "", url or "")
    if not normalized_url:
        return

    dedupe_key = strip_url_query_fragment(normalized_url).lower()
    candidates = result.setdefault("candidates", [])
    existing = None
    for candidate in candidates:
        if candidate.get("dedupe_key") == dedupe_key:
            existing = candidate
            break

    payload = {
        "url": normalized_url,
        "source": source or "",
        "score": int(score) if isinstance(score, (int, float)) else score,
        "usable": usable,
        "reason": reason or "",
        "dedupe_key": dedupe_key,
    }
    if isinstance(extra, dict):
        for key, value in extra.items():
            if key == "dedupe_key":
                continue
            payload[key] = value

    if existing is None:
        candidates.append(payload)
        return

    existing_score = existing.get("score")
    incoming_score = payload.get("score")
    if isinstance(incoming_score, (int, float)) and (not isinstance(existing_score, (int, float)) or incoming_score >= existing_score):
        existing.update(payload)
    else:
        for key, value in payload.items():
            if value not in (None, "", []):
                existing[key] = value


def build_investigation_debug_data(result: dict) -> dict:
    result = result or {}
    debug = dict(result.get("debug") or {})
    candidates = []
    for candidate in result.get("candidates") or []:
        if not isinstance(candidate, dict):
            continue
        candidates.append({
            "url": candidate.get("url") or "",
            "source": candidate.get("source") or "",
            "score": candidate.get("score"),
            "usable": candidate.get("usable"),
            "reason": candidate.get("reason") or "",
        })
        if len(candidates) >= 12:
            break

    outreach_handoff = dict(debug.get("friendly_outreach_handoff") or {})

    debug.update({
        "investigation_attempted": bool(result.get("attempted")),
        "investigation_mode": result.get("mode") or "",
        "investigation_trigger": result.get("trigger") or "",
        "investigation_clues": list(result.get("clues") or [])[:20],
        "investigation_reasons": list(result.get("reasons") or [])[:20],
        "investigation_candidates": candidates,
        "investigation_winner_url": result.get("winner_url") or "",
        "investigation_winner_score": result.get("winner_score"),
        "investigation_breadcrumb": list(result.get("breadcrumb") or [])[:20],
        "investigation_no_winner_reason": result.get("no_winner_reason") or "",
        "investigation_linked_recipe_used": bool(result.get("linked_recipe_used")),
        "investigation_analysis_platform_hint": result.get("analysis_platform_hint") or "",
        "investigation_effective_analysis_platform": result.get("effective_analysis_platform") or "",
        "friendly_outreach_needed": bool(debug.get("friendly_outreach_needed")),
        "friendly_outreach_handoff": outreach_handoff if outreach_handoff else {},
    })
    return debug


def _truncate_debug_text(value: str, max_len: int = SUBMIT_DEBUG_TEXT_MAX_LEN) -> str:
    text = str(value or "")
    if len(text) <= max_len:
        return text
    if max_len <= 32:
        return text[:max_len]
    head = max_len // 2
    tail = max_len - head - 24
    removed = max(0, len(text) - (head + tail))
    return f"{text[:head]}...[trimmed {removed} chars]...{text[-max(tail, 0):]}"


def _sanitize_debug_value(value, *, depth: int = 0):
    if value is None or isinstance(value, (int, float, bool)):
        return value
    if isinstance(value, str):
        return _truncate_debug_text(value)
    if isinstance(value, list):
        items = []
        for item in value[:SUBMIT_DEBUG_LIST_MAX_ITEMS]:
            items.append(_sanitize_debug_value(item, depth=depth + 1))
        if len(value) > SUBMIT_DEBUG_LIST_MAX_ITEMS:
            items.append(f"...[{len(value) - SUBMIT_DEBUG_LIST_MAX_ITEMS} more]")
        return items
    if isinstance(value, dict):
        compact = {}
        for idx, (key, nested) in enumerate(value.items()):
            if idx >= 40:
                compact["_truncated_keys"] = f"{len(value) - 40} more"
                break
            if key in {"worker_stdout", "worker_stderr"}:
                raw = str(nested or "")
                compact[f"{key}_len"] = len(raw)
                compact[f"{key}_tail"] = _truncate_debug_text(raw, 400)
                continue
            compact[key] = _sanitize_debug_value(nested, depth=depth + 1)
        return compact
    return _truncate_debug_text(value)


def sanitize_debug_data_for_submit(debug_data: dict | None) -> dict:
    sanitized = _sanitize_debug_value(debug_data or {})
    if not isinstance(sanitized, dict):
        return {}

    phone_worker = sanitized.get("phone_worker")
    if isinstance(phone_worker, dict):
        sanitized["phone_worker"] = {
            "bundle_dir": phone_worker.get("bundle_dir") or "",
            "primary_screenshot_path": phone_worker.get("primary_screenshot_path") or "",
            "description_screenshot_path": phone_worker.get("description_screenshot_path") or "",
            "worker_stdout_len": phone_worker.get("worker_stdout_len") or 0,
            "worker_stderr_len": phone_worker.get("worker_stderr_len") or 0,
            "worker_stdout_tail": phone_worker.get("worker_stdout_tail") or "",
            "worker_stderr_tail": phone_worker.get("worker_stderr_tail") or "",
        }

    candidates = sanitized.get("investigation_candidates")
    if isinstance(candidates, list):
        compact_candidates = []
        for item in candidates[:SUBMIT_DEBUG_LIST_MAX_ITEMS]:
            if isinstance(item, dict):
                compact_candidates.append({
                    "url": item.get("url") or "",
                    "source": item.get("source") or "",
                    "score": item.get("score"),
                    "usable": item.get("usable"),
                    "reason": _truncate_debug_text(item.get("reason") or "", 180),
                })
            else:
                compact_candidates.append(_sanitize_debug_value(item, depth=1))
        sanitized["investigation_candidates"] = compact_candidates

    def _json_len(value) -> int:
        try:
            return len(json.dumps(value, ensure_ascii=False))
        except Exception:
            return 10**9

    drop_order = [
        "phone_worker",
        "friendly_outreach_handoff",
        "investigation_candidates",
        "investigation_breadcrumb",
        "investigation_clues",
        "investigation_reasons",
        "instagram_hint_tokens",
        "instagram_mentions",
    ]
    for key in drop_order:
        if _json_len(sanitized) <= SUBMIT_DEBUG_DATA_MAX_JSON_LEN:
            break
        if key in sanitized:
            sanitized[key] = {"trimmed": True} if isinstance(sanitized[key], dict) else []

    if _json_len(sanitized) > SUBMIT_DEBUG_DATA_MAX_JSON_LEN:
        sanitized = {
            "platform": sanitized.get("platform") or "",
            "original_platform": sanitized.get("original_platform") or "",
            "collection_method": sanitized.get("collection_method") or "",
            "execution_actor": sanitized.get("execution_actor") or "",
            "execution_path": sanitized.get("execution_path") or "",
            "effective_page_url": sanitized.get("effective_page_url") or "",
            "investigation_mode": sanitized.get("investigation_mode") or "",
            "investigation_winner_url": sanitized.get("investigation_winner_url") or "",
            "investigation_no_winner_reason": sanitized.get("investigation_no_winner_reason") or "",
            "linked_recipe_used": bool(sanitized.get("linked_recipe_used")),
            "source_platform": sanitized.get("source_platform") or "",
            "source_creator_handle": sanitized.get("source_creator_handle") or "",
            "source_profile_url": sanitized.get("source_profile_url") or "",
            "_debug_data_trimmed": True,
        }

    return sanitized


FRIENDLY_OUTREACH_SUPPORTED_PLATFORMS = {"instagram", "tiktok", "facebook"}
FRIENDLY_OUTREACH_MAX_CLUE_TEXT = int(os.getenv("FRIENDLY_OUTREACH_MAX_CLUE_TEXT", "500") or "500")
FRIENDLY_OUTREACH_DETECTION_VERSION = os.getenv("FRIENDLY_OUTREACH_DETECTION_VERSION", "friendly-outreach-v2-paywalled-preview-2026-04-08").strip() or "friendly-outreach-v1-2026-04-05"


FRIENDLY_OUTREACH_PATTERN_SPECS = [
    {
        "clue_type": "comment_send_offer",
        "patterns": [
            r"\bcomment\b.{0,90}\b(?:and|&)\b.{0,30}\b(?:i(?:['’]?ll)?|i will)\b.{0,30}\b(?:send|dm|message)\b.{0,90}\b(?:recipe|link|ingredients|method)\b",
            r"\bcomment\b.{0,90}\b(?:to get|for)\b.{0,90}\b(?:recipe|link)\b.{0,90}\b(?:sent to your dms?|in your dms?|i(?:['’]?ll)?\s*(?:send|dm|message))",
            r"(?:if you (?:don[’']?t|do not) see the link).*?\bcomment\b.{0,90}\b(?:i(?:['’]?ll)?|i will)\b.{0,30}\b(?:send|dm|message)\b",
            r"תגיב(?:ו|י)?[^\n]{0,100}(?:ואני|ואשלח|אשלח)[^\n]{0,100}(?:קישור|מתכון|בהודעה|בפרטי)",
            r"לא בא[^\n]{0,40}לחפש[^\n]{0,100}תגיב(?:ו|י)?[^\n]{0,100}(?:אשלח|ואשלח)[^\n]{0,100}(?:קישור|מתכון|בהודעה|בפרטי)",
        ],
    },
    {
        "clue_type": "email_offer",
        "patterns": [
            r"\bemail me\b.{0,100}\b(?:full recipe|recipe|link|ingredients|method)\b",
            r"\bmail me\b.{0,100}\b(?:recipe|link|ingredients|method)\b",
        ],
    },
    {
        "clue_type": "dm_offer",
        "patterns": [
            r"\bdm me\b.{0,100}\b(?:recipe|link|ingredients|method)\b",
            r"\bi(?:['’]?ll| will)?\s*dm you\b.{0,100}\b(?:recipe|link|ingredients|method)\b",
            r"(?:אשלח|ואשלח)[^\n]{0,100}(?:בהודעה פרטית|בפרטי)",
        ],
    },
    {
        "clue_type": "message_send_offer",
        "patterns": [
            r"\bmessage me\b.{0,100}\b(?:and|&)\b.{0,30}\b(?:i(?:['’]?ll)?|i will)\b.{0,30}\b(?:send|share)\b.{0,100}\b(?:recipe|link|ingredients|method)\b",
            r"\bwrite to me\b.{0,100}\b(?:and|&)\b.{0,30}\b(?:i(?:['’]?ll)?|i will)\b.{0,30}\b(?:send|share)\b.{0,100}\b(?:recipe|link|ingredients|method)\b",
            r"(?:שלח(?:ו|י)? לי הודעה|כתבו לי|תכתבו לי|תכתבי לי|תשלחו לי הודעה)[^\n]{0,100}(?:קישור|מתכון)",
        ],
    },
]


def friendly_outreach_suggested_channel(platform: str, clue_type: str) -> str:
    normalized_platform = normalize_text(platform).lower()
    normalized_type = normalize_text(clue_type).lower()
    if normalized_type == "email_offer":
        return "email"
    if normalized_platform == "instagram":
        return "instagram_comment" if normalized_type == "comment_send_offer" else "instagram_dm"
    if normalized_platform == "tiktok":
        return "tiktok_dm"
    if normalized_platform == "facebook":
        return "instagram_dm"
    return ""


def get_friendly_outreach_text_sources(evidence: dict) -> list[tuple[str, str]]:
    if not isinstance(evidence, dict):
        return []
    ordered_fields = [
        ("caption", evidence.get("expanded_caption_text") or ""),
        ("caption", evidence.get("visible_text_after_expand") or ""),
        ("caption", evidence.get("visible_text_before_expand") or ""),
        ("description", evidence.get("transcript_text") or ""),
        ("description", evidence.get("raw_page_text") or ""),
        ("description", evidence.get("meta_description") or ""),
    ]
    seen = set()
    out = []
    for location, raw_text in ordered_fields:
        normalized = normalize_text_preserve_lines(unicodedata.normalize("NFKC", str(raw_text or "")))
        marker = (location, normalized)
        if not normalized or marker in seen:
            continue
        seen.add(marker)
        out.append((location, normalized))
    return out


def extract_friendly_outreach_clue_text(text: str, pattern: str) -> str:
    normalized = normalize_text_preserve_lines(text or "")
    if not normalized:
        return ""
    try:
        compiled = re.compile(pattern, flags=re.IGNORECASE)
    except re.error:
        return ""

    for line in [normalize_text_preserve_lines(line) for line in normalized.splitlines() if normalize_text(line)]:
        if compiled.search(line):
            return trim_text(line, FRIENDLY_OUTREACH_MAX_CLUE_TEXT)

    match = compiled.search(normalized)
    if not match:
        return ""
    start = max(match.start() - 120, 0)
    end = min(match.end() + 120, len(normalized))
    snippet = normalized[start:end].strip(" ,.;:-\n\r\t")
    return trim_text(snippet, FRIENDLY_OUTREACH_MAX_CLUE_TEXT)


def detect_friendly_outreach_offer(platform: str, evidence: dict) -> dict:
    normalized_platform = normalize_text(platform).lower()
    if normalized_platform not in FRIENDLY_OUTREACH_SUPPORTED_PLATFORMS:
        return {}

    for location, source_text in get_friendly_outreach_text_sources(evidence):
        for spec in FRIENDLY_OUTREACH_PATTERN_SPECS:
            clue_type = spec.get("clue_type") or ""
            for pattern in spec.get("patterns") or []:
                clue_text = extract_friendly_outreach_clue_text(source_text, pattern)
                if not clue_text:
                    continue
                return {
                    "clue_text": clue_text,
                    "clue_type": clue_type,
                    "clue_location": location,
                    "suggested_channel": friendly_outreach_suggested_channel(normalized_platform, clue_type),
                }
    return {}


def detect_friendly_outreach_offer_from_investigation_result(platform: str, investigation_result: dict, evidence: dict) -> dict:
    normalized_platform = normalize_text(platform).lower()
    if normalized_platform not in FRIENDLY_OUTREACH_SUPPORTED_PLATFORMS:
        return {}

    clue_labels = []
    for clue in list((investigation_result or {}).get("clues") or []):
        normalized_clue = normalize_text(clue).lower()
        if normalized_clue:
            clue_labels.append(normalized_clue)

    mapping = {
        "comment_for_dm_recipe": "comment_send_offer",
        "comment_send_offer": "comment_send_offer",
        "dm_offer": "dm_offer",
        "message_send_offer": "message_send_offer",
        "email_offer": "email_offer",
    }

    chosen_label = ""
    chosen_type = ""
    for label in clue_labels:
        if label in mapping:
            chosen_label = label
            chosen_type = mapping[label]
            break
    if not chosen_type:
        return {}

    clue_text = ""
    normalized_sources = [source_text for _location, source_text in get_friendly_outreach_text_sources(evidence)]
    weak_hint_patterns = []
    if chosen_type == "comment_send_offer":
        weak_hint_patterns = [r"\bcomment\b", r"תגיב(?:ו|י)?"]
    elif chosen_type == "dm_offer":
        weak_hint_patterns = [r"\bdm\b", r"בפרטי", r"בהודעה פרטית"]
    elif chosen_type == "message_send_offer":
        weak_hint_patterns = [r"\bmessage\b", r"כתבו לי", r"שלחו לי הודעה"]
    elif chosen_type == "email_offer":
        weak_hint_patterns = [r"\bemail\b", r"\bmail\b"]

    for source_text in normalized_sources:
        for pattern in weak_hint_patterns:
            clue_text = extract_friendly_outreach_clue_text(source_text, pattern)
            if clue_text:
                break
        if clue_text:
            break

    if not clue_text:
        clue_text = trim_text(chosen_label.replace("_", " "), FRIENDLY_OUTREACH_MAX_CLUE_TEXT)

    return {
        "clue_text": clue_text,
        "clue_type": chosen_type,
        "clue_location": "caption",
        "suggested_channel": friendly_outreach_suggested_channel(normalized_platform, chosen_type),
        "detected_via": "investigation_clue",
    }


def compute_friendly_outreach_reason_normal_path_insufficient(result: dict, evidence: dict) -> str:
    no_winner_reason = normalize_text((result or {}).get("no_winner_reason") or "")
    if no_winner_reason:
        return no_winner_reason

    combined_text = combine_text_blocks([
        evidence.get("expanded_caption_text") or "",
        evidence.get("visible_text_after_expand") or "",
        evidence.get("visible_text_before_expand") or "",
        evidence.get("transcript_text") or "",
        evidence.get("raw_page_text") or "",
        evidence.get("meta_description") or "",
    ])
    normalized_combined = normalize_text_preserve_lines(combined_text).lower()

    paywall_markers = [
        "claim my free post",
        "continue reading this post for free",
        "purchase a paid subscription",
        "subscribe and unlock",
        "unlock this post",
        "maximum one post unlock per account",
        "substack_free_post_claim_attempted: failed",
    ]
    if any(marker in normalized_combined for marker in paywall_markers):
        return "linked_page_paywalled_preview"

    if bool((result or {}).get("attempted")) and not bool((result or {}).get("linked_recipe_used")):
        return "normal_discovery_no_viable_external_recipe_page"

    metrics = evaluate_evidence_text(combined_text)
    if (
        metrics.get("looksRecipeDense")
        and int(metrics.get("measurementSignalCount") or 0) >= 3
        and int(metrics.get("recipeVerbSignalCount") or 0) >= 2
    ):
        return ""
    return "normal_discovery_path_insufficient"


def maybe_prepare_friendly_outreach_handoff(
    *,
    job_id: str,
    target_url: str,
    platform: str,
    evidence: dict,
    source_metadata: dict,
    investigation_result: dict,
    confirmation_context: dict | None = None,
) -> dict:
    if (confirmation_context or {}).get("is_confirmation_job"):
        return {}

    result = investigation_result or {}

    try:
        current_job = get_job(job_id)
        existing_debug = _as_plain_dict(current_job.get("debug_data"))
        existing_handoff = _as_plain_dict(existing_debug.get("friendly_outreach_handoff"))
        if existing_handoff.get("friendly_outreach_needed") or existing_debug.get("friendly_outreach_needed"):
            return existing_handoff
    except Exception:
        pass

    reason_normal_path_insufficient = compute_friendly_outreach_reason_normal_path_insufficient(result, evidence)
    linked_winner_exists = bool(result.get("linked_recipe_used")) or bool(normalize_text(result.get("winner_url") or ""))
    if linked_winner_exists and reason_normal_path_insufficient != "linked_page_paywalled_preview":
        return {}

    detected_offer = detect_friendly_outreach_offer(platform, evidence)
    if not detected_offer:
        detected_offer = detect_friendly_outreach_offer_from_investigation_result(platform, result, evidence)
    if not detected_offer:
        return {}

    if not reason_normal_path_insufficient:
        return {}

    handoff = {
        "friendly_outreach_needed": True,
        "source_url": target_url,
        "normalized_source_url": strip_url_query_fragment(normalize_profile_url(target_url or "", target_url or "")),
        "platform": normalize_text(platform).lower(),
        "creator_profile_url": source_metadata.get("source_profile_url") or "",
        "creator_name": source_metadata.get("source_creator_name") or source_metadata.get("source_channel_name") or "",
        "creator_handle": source_metadata.get("source_creator_handle") or "",
        "clue_text": detected_offer.get("clue_text") or "",
        "clue_type": detected_offer.get("clue_type") or "",
        "clue_location": detected_offer.get("clue_location") or "",
        "reason_normal_path_insufficient": reason_normal_path_insufficient,
        "investigation_run_id": "",
        "suggested_channel": detected_offer.get("suggested_channel") or "",
        "detector_version": FRIENDLY_OUTREACH_DETECTION_VERSION,
    }
    return handoff


def finalize_friendly_outreach_handoff(
    *,
    job_id: str,
    recipe_id: str | None,
    handoff: dict,
    investigation_history_write: dict | None,
    confirmation_context: dict | None = None,
) -> dict:
    handoff_payload = dict(handoff or {})
    run_id = str((investigation_history_write or {}).get("run_id") or handoff_payload.get("investigation_run_id") or "")
    if run_id:
        handoff_payload["investigation_run_id"] = run_id

    append_job_debug_log(
        job_id,
        (
            f"Friendly outreach handoff emitted. clue_type={handoff_payload.get('clue_type') or 'unknown'} "
            f"channel={handoff_payload.get('suggested_channel') or 'unknown'} "
            f"reason={handoff_payload.get('reason_normal_path_insufficient') or 'unknown'}"
        ),
        debug_last_step="friendly_outreach_emitted",
        debug_data={
            "friendly_outreach_needed": True,
            "friendly_outreach_handoff": handoff_payload,
        },
    )

    if recipe_id and not (confirmation_context or {}).get("is_confirmation_job"):
        update_recipe_debug(
            recipe_id,
            debug_log_append=(
                f"Friendly outreach handoff emitted for BotJob {job_id}: "
                f"{handoff_payload.get('clue_type') or 'unknown'} -> {handoff_payload.get('suggested_channel') or 'unknown'}"
            ),
        )

    return handoff_payload


def investigation_history_mode_supported(mode: str) -> bool:
    if not INVESTIGATION_HISTORY_ENABLED:
        return False
    normalized_mode = normalize_text(mode).lower()
    if not normalized_mode:
        return False
    allowed = INVESTIGATION_HISTORY_SUPPORTED_MODES or {"instagram.external_site"}
    return normalized_mode in allowed


def infer_investigation_candidate_type(candidate: dict) -> str:
    source = normalize_text(candidate.get("source") or "").lower()
    url = normalize_profile_url(candidate.get("url") or "", candidate.get("url") or "")
    host = canonical_domain(url)

    if "profile" in source or (host.endswith("instagram.com") and re.search(r"instagram\.com/[^/]+/?$", strip_url_query_fragment(url), flags=re.IGNORECASE)):
        return "profile"
    if "current_page" in source or "direct" in source:
        return "direct_url"
    if "search" in source:
        return "search_result"
    if "site_root" in source or source.endswith("site_root_candidate"):
        return "site_root"
    if "internal_page" in source:
        return "internal_page"
    if "bio" in source:
        return "link_in_bio"
    if host.endswith("instagram.com"):
        return "instagram"
    return "website"


def summarize_source_identity_for_history(source_metadata: dict | None) -> dict:
    metadata = source_metadata or {}
    return {
        "source_platform": metadata.get("source_platform") or "",
        "source_creator_name": metadata.get("source_creator_name") or "",
        "source_creator_handle": metadata.get("source_creator_handle") or "",
        "source_channel_name": metadata.get("source_channel_name") or "",
        "source_channel_key": metadata.get("source_channel_key") or "",
        "source_profile_url": metadata.get("source_profile_url") or "",
        "source_page_domain": metadata.get("source_page_domain") or "",
        "source_avatar_url": metadata.get("source_avatar_url") or "",
        "creator_group_key": metadata.get("creator_group_key") or "",
    }


def summarize_text_for_history(value: str, *, preview_max: int | None = None) -> dict:
    normalized = normalize_text_preserve_lines(value or "")
    preview_limit = preview_max or INVESTIGATION_HISTORY_TEXT_PREVIEW_MAX
    return {
        "length": len(normalized),
        "line_count": count_non_empty_lines(normalized),
        "preview": trim_text(normalized, preview_limit),
    }


def summarize_investigation_evidence_for_history(evidence: dict, merged_evidence: dict | None) -> dict:
    merged = merged_evidence or {}
    return {
        "target_url": evidence.get("effective_page_url") or evidence.get("target_url") or "",
        "page_title": evidence.get("page_title") or "",
        "media_type_guess": evidence.get("media_type_guess") or "",
        "raw_page_text": summarize_text_for_history(evidence.get("raw_page_text") or ""),
        "expanded_caption_text": summarize_text_for_history(evidence.get("expanded_caption_text") or ""),
        "transcript_text": summarize_text_for_history(evidence.get("transcript_text") or ""),
        "visible_text_before_expand": summarize_text_for_history(evidence.get("visible_text_before_expand") or ""),
        "visible_text_after_expand": summarize_text_for_history(evidence.get("visible_text_after_expand") or ""),
        "meta_description": summarize_text_for_history(evidence.get("meta_description") or ""),
        "merged_raw_page_text": summarize_text_for_history(merged.get("raw_page_text") or ""),
        "merged_expanded_caption_text": summarize_text_for_history(merged.get("expanded_caption_text") or ""),
        "merged_transcript_text": summarize_text_for_history(merged.get("transcript_text") or ""),
        "merged_meta_description": summarize_text_for_history(merged.get("meta_description") or ""),
        "merged_page_title": merged.get("page_title") or "",
        "merged_page_image_url": merged.get("page_image_url") or "",
    }


def build_investigation_history_payload(
    *,
    job_id: str,
    recipe_id: str | None,
    target_url: str,
    platform: str,
    original_platform: str,
    collection_method: str,
    evidence: dict,
    source_metadata: dict,
    investigation_result: dict,
    primary_screenshot_url: str,
    description_screenshot_url: str,
    visual_recipe_image_url: str,
    effective_analysis_platform: str,
) -> dict:
    result = investigation_result or {}
    debug = dict(result.get("debug") or {})
    winner_url = normalize_profile_url(result.get("winner_url") or "", target_url)
    mode = normalize_text(result.get("mode") or "")
    clues = list(result.get("clues") or [])[:20]
    reasons = list(result.get("reasons") or [])[:20]
    breadcrumbs_raw = list(result.get("breadcrumb") or [])[: max(INVESTIGATION_HISTORY_MAX_BREADCRUMBS, 1)]
    candidates_raw = [candidate for candidate in (result.get("candidates") or []) if isinstance(candidate, dict)]

    status = "winner_selected" if winner_url else "no_winner"
    summary_outcome = (
        f"winner_selected:{winner_url}"
        if winner_url
        else f"no_winner:{result.get('no_winner_reason') or ', '.join(reasons) or 'none'}"
    )

    source_identity = {
        **summarize_source_identity_for_history(source_metadata),
        **get_current_collector_identity(),
    }
    evidence_summary = summarize_investigation_evidence_for_history(evidence, result.get("merged_evidence") or {})
    run_recorded_at = utc_now_iso()

    breadcrumbs = []
    sequence = 1
    for clue in clues:
        breadcrumbs.append({
            "recipe_id": recipe_id or None,
            "bot_job_id": job_id,
            "sequence": sequence,
            "event_type": "clue_detected",
            "label": str(clue),
            "url": target_url,
            "status": "seen",
            "reason": "detected_clue",
            "payload_json": {"clue": clue},
            "occurred_at": run_recorded_at,
        })
        sequence += 1

    for crumb in breadcrumbs_raw:
        crumb_text = normalize_text(str(crumb or ""))
        crumb_url = crumb_text if crumb_text.startswith("http://") or crumb_text.startswith("https://") else ""
        breadcrumbs.append({
            "recipe_id": recipe_id or None,
            "bot_job_id": job_id,
            "sequence": sequence,
            "event_type": "breadcrumb",
            "label": crumb_text[:500],
            "url": crumb_url or None,
            "status": "visited",
            "reason": "breadcrumb",
            "payload_json": {"breadcrumb": crumb_text},
            "occurred_at": run_recorded_at,
        })
        sequence += 1

    breadcrumbs.append({
        "recipe_id": recipe_id or None,
        "bot_job_id": job_id,
        "sequence": sequence,
        "event_type": "winner_chosen" if winner_url else "no_winner",
        "label": winner_url or (result.get("no_winner_reason") or "no_winner"),
        "url": winner_url or None,
        "status": status,
        "reason": result.get("no_winner_reason") or ", ".join(reasons) or "",
        "payload_json": {
            "winner_url": winner_url or "",
            "winner_score": result.get("winner_score"),
            "linked_recipe_used": bool(result.get("linked_recipe_used")),
        },
        "occurred_at": run_recorded_at,
    })

    winner_dedupe = strip_url_query_fragment(winner_url).lower() if winner_url else ""
    candidates = []
    for index, candidate in enumerate(candidates_raw[: max(INVESTIGATION_HISTORY_MAX_CANDIDATES, 1)], 1):
        candidate_url = normalize_profile_url(candidate.get("url") or "", target_url)
        dedupe = strip_url_query_fragment(candidate_url).lower() if candidate_url else ""
        score_breakdown = {
            "score": candidate.get("score"),
            "title_exact": candidate.get("title_exact"),
            "slug_exact": candidate.get("slug_exact"),
            "title_extra_tokens": list(candidate.get("title_extra_tokens") or [])[:12],
            "slug_extra_tokens": list(candidate.get("slug_extra_tokens") or [])[:12],
        }
        evidence_quality = {
            "page_title": candidate.get("page_title") or "",
            "usable": candidate.get("usable"),
            "reason": candidate.get("reason") or "",
            "source": candidate.get("source") or "",
        }
        candidates.append({
            "recipe_id": recipe_id or None,
            "bot_job_id": job_id,
            "sequence": index,
            "candidate_url": candidate_url,
            "candidate_domain": canonical_domain(candidate_url),
            "candidate_type": infer_investigation_candidate_type(candidate),
            "candidate_source": candidate.get("source") or "",
            "score": candidate.get("score"),
            "usable": bool(candidate.get("usable")) if candidate.get("usable") is not None else False,
            "chosen": bool(winner_dedupe and dedupe == winner_dedupe),
            "reason": candidate.get("reason") or "",
            "page_title": candidate.get("page_title") or "",
            "score_breakdown_json": score_breakdown,
            "evidence_quality_summary_json": evidence_quality,
            "payload_json": dict(candidate),
            "created_at_snapshot": run_recorded_at,
        })

    evidence_snapshot = {
        "recipe_id": recipe_id or None,
        "bot_job_id": job_id,
        "source_url": target_url,
        "winner_url": winner_url or None,
        "explicit_recipe_link": result.get("explicit_recipe_link") or None,
        "analysis_platform_hint": result.get("analysis_platform_hint") or None,
        "effective_analysis_platform": effective_analysis_platform or None,
        "linked_recipe_used": bool(result.get("linked_recipe_used")),
        "evidence_summary_json": evidence_summary,
        "source_metadata_json": source_identity,
        "blob_refs_json": {
            "primary_screenshot_url": primary_screenshot_url or "",
            "description_screenshot_url": description_screenshot_url or "",
            "visual_recipe_image_url": visual_recipe_image_url or "",
            "page_image_url": evidence.get("page_image_url") or "",
            "linked_submission_screenshot_path": result.get("primary_submission_screenshot_path") or "",
            "secondary_submission_screenshot_path": result.get("secondary_submission_screenshot_path") or "",
        },
        "merged_evidence_summary": trim_text(
            choose_first_non_empty(
                (result.get("merged_evidence") or {}).get("page_title"),
                (result.get("merged_evidence") or {}).get("meta_description"),
                result.get("winner_url"),
                target_url,
            ),
            500,
        ),
        "created_at_snapshot": run_recorded_at,
    }

    run = {
        "recipe_id": recipe_id or None,
        "bot_job_id": job_id,
        "source_url": target_url,
        "normalized_source_url": strip_url_query_fragment(target_url),
        "platform": platform,
        "original_platform": original_platform or platform,
        "scenario_mode": mode,
        "trigger_reason": result.get("trigger") or "inline_after_first_pass",
        "collection_method": collection_method,
        "runtime_version": debug.get("runtime_version") or "",
        "runtime_source": debug.get("runtime_source") or "",
        "investigator_patch_version": INVESTIGATION_PATCH_VERSION,
        "started_at": debug.get("attempt_started_at") or None,
        "recorded_at": run_recorded_at,
        "status": status,
        "summary_outcome": summary_outcome,
        "winner_url": winner_url or None,
        "winner_score": result.get("winner_score"),
        "linked_recipe_used": bool(result.get("linked_recipe_used")),
        "analysis_platform_hint": result.get("analysis_platform_hint") or None,
        "effective_analysis_platform": effective_analysis_platform or None,
        "source_profile_url": source_identity.get("source_profile_url") or None,
        "source_creator_name": source_identity.get("source_creator_name") or None,
        "source_creator_handle": source_identity.get("source_creator_handle") or None,
        "source_channel_name": source_identity.get("source_channel_name") or None,
        "source_channel_key": source_identity.get("source_channel_key") or None,
        "source_page_domain": source_identity.get("source_page_domain") or None,
        "candidate_count": len(candidates),
        "breadcrumb_count": len(breadcrumbs),
        "clues_json": clues,
        "reasons_json": reasons,
        "source_identity_json": source_identity,
        "summary_json": {
            "no_winner_reason": result.get("no_winner_reason") or "",
            "reasons": reasons,
            "winner_score": result.get("winner_score"),
            "linked_recipe_used": bool(result.get("linked_recipe_used")),
            "effective_analysis_platform": effective_analysis_platform or "",
            "contamination_confirmation_recommended": bool(debug.get("contamination_confirmation_recommended")),
            "contamination_confirmation_score": debug.get("contamination_confirmation_score"),
            "contamination_confirmation_reasons": list(debug.get("contamination_confirmation_reasons") or [])[:12],
            "confirmation_job": bool(debug.get("confirmation_job")),
            "confirmation_reason": debug.get("confirmation_reason") or "",
            "friendly_outreach_needed": bool(debug.get("friendly_outreach_needed")),
            "friendly_outreach_handoff": dict(debug.get("friendly_outreach_handoff") or {}),
        },
    }

    return {
        "run": run,
        "breadcrumbs": breadcrumbs,
        "candidates": candidates,
        "evidence": evidence_snapshot,
    }


def persist_investigation_history_if_needed(
    *,
    job_id: str,
    recipe_id: str | None,
    target_url: str,
    platform: str,
    original_platform: str,
    collection_method: str,
    evidence: dict,
    source_metadata: dict,
    investigation_result: dict,
    primary_screenshot_url: str,
    description_screenshot_url: str,
    visual_recipe_image_url: str,
    effective_analysis_platform: str,
) -> dict:
    result = investigation_result or {}
    if not bool(result.get("attempted")):
        return {"ok": False, "skipped": True, "reason": "not_attempted"}
    if not investigation_history_mode_supported(result.get("mode") or ""):
        return {"ok": False, "skipped": True, "reason": "mode_not_enabled"}

    try:
        payload = build_investigation_history_payload(
            job_id=job_id,
            recipe_id=recipe_id,
            target_url=target_url,
            platform=platform,
            original_platform=original_platform,
            collection_method=collection_method,
            evidence=evidence,
            source_metadata=source_metadata,
            investigation_result=result,
            primary_screenshot_url=primary_screenshot_url,
            description_screenshot_url=description_screenshot_url,
            visual_recipe_image_url=visual_recipe_image_url,
            effective_analysis_platform=effective_analysis_platform,
        )
        write_result = write_investigation_history(payload)
    except Exception as history_err:
        return {
            "ok": False,
            "skipped": True,
            "reason": f"write_failed:{type(history_err).__name__}",
            "error": f"{type(history_err).__name__}: {history_err}",
        }

    run_id = str(write_result.get("run_id") or "")
    if run_id:
        result.setdefault("debug", {})["investigation_history_run_id"] = run_id
        result.setdefault("debug", {})["investigation_history_write"] = {
            "breadcrumbs_written": write_result.get("breadcrumbs_written"),
            "candidates_written": write_result.get("candidates_written"),
            "candidate_errors": write_result.get("candidate_errors") or [],
            "breadcrumb_errors": write_result.get("breadcrumb_errors") or [],
            "evidence_error": write_result.get("evidence_error"),
        }
    return write_result


def log_investigation_result(job_id: str, result: dict) -> None:
    mode = str(result.get("mode") or "none")
    attempted = bool(result.get("attempted"))
    winner_url = str(result.get("winner_url") or "")
    no_winner_reason = str(result.get("no_winner_reason") or "")
    clue_summary = ", ".join(list(result.get("clues") or [])[:6]) or "none"
    reason_summary = ", ".join(list(result.get("reasons") or [])[:6]) or "none"
    if attempted and winner_url:
        message = (
            f"Investigation completed. mode={mode} winner={winner_url} "
            f"winner_score={result.get('winner_score')} clues={clue_summary}"
        )
        debug_last_step = "investigation_completed"
    elif attempted:
        message = (
            f"Investigation attempted with no winner. mode={mode} "
            f"reason={no_winner_reason or reason_summary} clues={clue_summary}"
        )
        debug_last_step = "investigation_no_winner"
    else:
        message = (
            f"Investigation skipped. mode={mode} "
            f"reason={no_winner_reason or reason_summary} clues={clue_summary}"
        )
        debug_last_step = "investigation_skipped"

    append_job_debug_log(
        job_id,
        message,
        debug_last_step=debug_last_step,
        debug_data=build_investigation_debug_data(result),
    )


def build_instagram_profile_candidate_urls(source_metadata: dict, owner_hint: dict) -> list[str]:
    candidates = []
    owner_hint_handle = str(owner_hint.get("handle") or "").lstrip("@").strip()
    if owner_hint_handle:
        candidates.append(f"https://www.instagram.com/{owner_hint_handle}/")

    source_profile_url = normalize_instagram_profile_root(source_metadata.get("source_profile_url") or "")
    if is_allowed_instagram_profile_href(source_profile_url):
        candidates.append(source_profile_url)

    source_handle = str(source_metadata.get("source_creator_handle") or "").lstrip("@").strip()
    if source_handle:
        candidates.append(f"https://www.instagram.com/{source_handle}/")

    deduped = []
    seen = set()
    for candidate in candidates:
        normalized = normalize_profile_url(candidate, "https://www.instagram.com")
        if not is_allowed_instagram_profile_href(normalized):
            continue
        dedupe_key = strip_url_query_fragment(normalized).lower()
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        deduped.append(normalized)
    return deduped


async def investigate_youtube_linked_recipe(
    *,
    context,
    page,
    job_id: str,
    target_url: str,
    evidence: dict,
    source_metadata: dict,
    original_submission_screenshot_path: str,
    linked_submission_screenshot_path: str,
) -> dict:
    result = make_investigation_result("youtube.linked_recipe")
    runtime = get_runtime_investigation_config("youtube.linked_recipe")
    result["debug"].update({
        "runtime_version": runtime.get("runtime_version"),
        "runtime_source": runtime.get("runtime_source"),
        "runtime_error": runtime.get("runtime_error"),
        "runtime_rules_summary": summarize_investigation_rules(runtime.get("rules") if isinstance(runtime, dict) else {}),
    })

    description = combine_text_blocks([evidence.get("expanded_caption_text"), evidence.get("meta_description")])
    result["clues"] = detect_investigation_clues(description, runtime.get("rules"))
    anchor_candidate_urls = await extract_candidate_anchor_urls(page)
    explicit_recipe_link = extract_explicit_recipe_link_from_youtube(
        description=description,
        title=evidence.get("page_title") or "",
        candidate_urls=anchor_candidate_urls,
    )
    if not explicit_recipe_link or not explicit_recipe_link.get("url"):
        result["no_winner_reason"] = "no_explicit_recipe_link"
        return result

    result["attempted"] = True
    result["clues"] = list(dict.fromkeys((result.get("clues") or []) + ["explicit_recipe_link"]))
    result["explicit_recipe_link"] = explicit_recipe_link["url"]
    result["breadcrumb"] = ["first_pass", "youtube_explicit_link"]
    add_investigation_candidate(
        result,
        explicit_recipe_link["url"],
        source=explicit_recipe_link.get("source") or "description",
        score=explicit_recipe_link.get("score"),
        usable=None,
        reason="explicit_recipe_link_detected",
    )

    linked_page = None
    linked_page_owner_context = None
    try:
        linked_page, linked_evidence, linked_source_metadata, linked_metrics, linked_page_owner_context = await open_web_candidate_page(
            context,
            "youtube",
            explicit_recipe_link["url"],
            platform_hint="web",
        )
        linked_score = score_linked_page_candidate(
            explicit_recipe_link.get("score"),
            linked_metrics,
            strong_bonus=60,
            dense_bonus=120,
        )

        usable = linked_page_looks_usable(linked_evidence, linked_metrics)
        add_investigation_candidate(
            result,
            explicit_recipe_link["url"],
            source=explicit_recipe_link.get("source") or "description",
            score=linked_score,
            usable=usable,
            reason="linked_page_evaluated",
            extra=build_linked_page_candidate_extra(linked_evidence, linked_metrics),
        )

        if not usable:
            result["no_winner_reason"] = "linked_page_not_usable"
            return result

        if LINKED_RECIPE_SCREENSHOT:
            try:
                await linked_page.evaluate("window.scrollTo(0, 0)")
                await linked_page.wait_for_timeout(400)
                await linked_page.screenshot(path=linked_submission_screenshot_path, full_page=False)
                result["primary_submission_screenshot_path"] = linked_submission_screenshot_path
                result["secondary_submission_screenshot_path"] = original_submission_screenshot_path
            except Exception:
                result["primary_submission_screenshot_path"] = original_submission_screenshot_path
        if not result.get("primary_submission_screenshot_path"):
            result["primary_submission_screenshot_path"] = original_submission_screenshot_path

        finalize_linked_investigation_result(
            result,
            base_evidence=evidence,
            linked_evidence=linked_evidence,
            winner_url=explicit_recipe_link["url"],
            winner_score=linked_score,
            winner_source_metadata=linked_source_metadata,
            merge_callback=lambda: merge_youtube_linked_page_evidence(
                evidence,
                linked_evidence,
                explicit_recipe_link["url"],
            ),
            explicit_recipe_link=explicit_recipe_link["url"],
            breadcrumb_append="linked_recipe_page",
            debug_updates={
                "linked_recipe_page_title": linked_evidence.get("page_title") or "",
                "linked_recipe_metrics": linked_metrics,
            },
        )
        return result
    except Exception as linked_err:
        result["no_winner_reason"] = f"linked_page_fetch_failed:{type(linked_err).__name__}"
        result["debug"]["error"] = f"{type(linked_err).__name__}: {linked_err}"
        return result
    finally:
        await close_page_and_context(linked_page, linked_page_owner_context)



def prepare_tiktok_phone_investigation(
    *,
    result: dict,
    target_url: str,
    evidence: dict,
):
    runtime = get_runtime_investigation_config("tiktok.external_site")
    runtime_rules = runtime.get("rules") if isinstance(runtime.get("rules"), dict) else {}
    result["debug"].update({
        "runtime_version": runtime.get("runtime_version"),
        "runtime_source": runtime.get("runtime_source"),
        "runtime_error": runtime.get("runtime_error"),
        "runtime_rules_summary": summarize_investigation_rules(runtime_rules),
    })

    combined_text = combine_text_blocks([
        evidence.get("expanded_caption_text"),
        evidence.get("raw_page_text"),
        evidence.get("visible_text_after_expand"),
    ])
    result["clues"] = detect_investigation_clues(combined_text, runtime_rules)

    limits = resolve_investigation_runtime_limits(
        runtime_rules,
        external_default=3,
        internal_default=4,
        minimum_winner_score_default=80,
    )

    (
        source_metadata_updates,
        external_site_url,
        tiktok_source_debug,
        external_site_candidates,
    ) = build_tiktok_phone_source_context(
        target_url,
        evidence,
    )
    result["source_metadata_updates"] = source_metadata_updates
    result["debug"].update(tiktok_source_debug)

    if external_site_candidates:
        result["attempted"] = True
        result["breadcrumb"] = ["first_pass", "tiktok_profile", "external_site_candidate"]

    return {
        "runtime": runtime,
        "runtime_rules": runtime_rules,
        "limits": limits,
        "source_metadata_updates": source_metadata_updates,
        "external_site_url": external_site_url,
        "tiktok_source_debug": tiktok_source_debug,
        "external_site_candidates": external_site_candidates,
    }


async def resolve_tiktok_phone_title_focus_context(
    *,
    result: dict,
    evidence: dict,
    recipe_id: str | None,
    source_metadata_updates: dict,
    tiktok_source_debug: dict,
):
    recipe_title_hint = ""
    if recipe_id:
        try:
            current_recipe = await asyncio.to_thread(get_recipe, recipe_id)
            recipe_title_hint = recipe_title_hint_from_recipe_row(current_recipe)
        except Exception:
            recipe_title_hint = ""
    if recipe_title_hint:
        result["debug"]["tiktok_recipe_row_title_hint"] = recipe_title_hint

    evidence_title_source_text = combine_text_blocks([
        evidence.get("page_title"),
        evidence.get("expanded_caption_text"),
        evidence.get("visible_text_after_expand"),
        evidence.get("raw_page_text"),
    ])
    evidence_preclue_title_hint = extract_tiktok_preclue_title_phrase(evidence_title_source_text)
    raw_title_hint = recipe_title_hint or evidence.get("page_title") or source_metadata_updates.get("source_creator_name") or ""
    if evidence_preclue_title_hint and recipe_title_hint:
        normalized_recipe_title_hint = normalize_text(strip_tiktok_recipe_title_clue_suffix(recipe_title_hint)).lower()
        normalized_evidence_title_hint = normalize_text(evidence_preclue_title_hint).lower()
        if normalized_recipe_title_hint and normalized_evidence_title_hint and normalized_recipe_title_hint != normalized_evidence_title_hint:
            recipe_tokens = set(extract_match_tokens(normalized_recipe_title_hint))
            evidence_tokens = set(extract_match_tokens(normalized_evidence_title_hint))
            overlap_ratio = (len(recipe_tokens & evidence_tokens) / max(len(recipe_tokens), 1)) if recipe_tokens else 0.0
            if overlap_ratio < 0.34:
                raw_title_hint = evidence_preclue_title_hint
                result["debug"]["tiktok_same_host_title_hint_override"] = evidence_preclue_title_hint
                result["debug"]["tiktok_recipe_row_title_hint_deprioritized"] = recipe_title_hint
    title_hint = pick_tiktok_same_host_title_hint(raw_title_hint, source_text=evidence_title_source_text) or raw_title_hint
    if title_hint and title_hint != raw_title_hint:
        result["debug"]["tiktok_same_host_title_hint_override"] = title_hint
    affinity_tokens = list((tiktok_source_debug or {}).get("tiktok_domain_affinity_tokens") or [])
    focus_source_text = combine_text_blocks([
        title_hint,
        evidence.get("page_title"),
        evidence.get("expanded_caption_text"),
        evidence.get("visible_text_after_expand"),
        evidence.get("raw_page_text"),
    ])
    focus_tokens = extract_tiktok_recipe_focus_tokens(
        focus_source_text,
        affinity_tokens,
        max_tokens=8,
    )
    focus_phrases = extract_tiktok_recipe_focus_phrases(focus_source_text, max_phrases=3)
    if focus_tokens:
        result["debug"]["tiktok_recipe_focus_tokens"] = focus_tokens[:8]
    if focus_phrases:
        result["debug"]["tiktok_recipe_focus_phrases"] = focus_phrases[:3]

    return {
        "recipe_title_hint": recipe_title_hint,
        "title_hint": title_hint,
        "affinity_tokens": affinity_tokens,
        "focus_source_text": focus_source_text,
        "focus_tokens": focus_tokens,
        "focus_phrases": focus_phrases,
    }


async def choose_tiktok_phone_external_site_winner(
    *,
    result: dict,
    target_url: str,
    evidence: dict,
    source_metadata_updates: dict,
    external_site_candidates: list[dict],
    max_external_site_candidates: int,
    max_internal_page_candidates: int,
    minimum_winner_score: int,
    title_hint: str,
    affinity_tokens: list[str],
    focus_tokens: list[str],
    focus_source_text: str,
):
    chosen_evidence = None
    chosen_source_metadata = {}
    chosen_url = ""
    chosen_score = None
    chosen_site_root = ""
    searched_same_host_bases: set[str] = set()

    for site_candidate in external_site_candidates[:max_external_site_candidates]:
        candidate_url = normalize_investigation_candidate_url(site_candidate.get("url") or "", target_url)
        candidate_seed_score = int(site_candidate.get("score") or 0)

        add_investigation_candidate(
            result,
            candidate_url,
            source=site_candidate.get("source") or "tiktok_profile_external_site",
            score=candidate_seed_score,
            usable=None,
            reason="external_site_candidate_ranked",
        )

        linked_evidence, linked_source_metadata, linked_metrics = fetch_remote_page_metadata(candidate_url)
        linked_score = score_linked_page_candidate(
            candidate_seed_score,
            linked_metrics,
            strong_bonus=80,
            dense_bonus=140,
            food_context_bonus=20,
        )
        direct_effective_url = get_tiktok_candidate_final_url(candidate_url, linked_evidence) or candidate_url
        direct_requires_follow = looks_like_tiktok_intermediate_landing_page(
            candidate_url,
            linked_evidence,
            linked_metrics,
        )
        direct_offer_like = looks_like_tiktok_offer_like_page(
            direct_effective_url,
            linked_evidence,
            linked_metrics,
        )
        direct_usable = linked_page_looks_usable(linked_evidence, linked_metrics) and not direct_offer_like
        if direct_offer_like:
            linked_score -= 120

        direct_extra = build_linked_page_candidate_extra(linked_evidence, linked_metrics)
        direct_extra["intermediate_landing_page"] = direct_requires_follow
        direct_extra["offer_like_page"] = direct_offer_like
        add_investigation_candidate(
            result,
            candidate_url,
            source=site_candidate.get("source") or "tiktok_profile_external_site",
            score=linked_score,
            usable=direct_usable,
            reason="linked_page_evaluated",
            extra=direct_extra,
        )

        if direct_usable and linked_score >= minimum_winner_score and not is_homepage_like_url(direct_effective_url) and not direct_requires_follow and not direct_offer_like:
            chosen_evidence = linked_evidence
            chosen_source_metadata = linked_source_metadata
            chosen_url = direct_effective_url or candidate_url
            chosen_score = linked_score
            chosen_site_root = candidate_url
            break

        rendered_linkhub_candidates = []
        rendered_linkhub_debug = {}
        if direct_requires_follow:
            rendered_linkhub_candidates, rendered_linkhub_debug = await extract_tiktok_rendered_linkhub_candidates(
                candidate_url,
                title_hint,
                affinity_tokens,
            )
            if rendered_linkhub_debug:
                result["debug"].update({
                    "tiktok_rendered_linkhub_url": rendered_linkhub_debug.get("rendered_url") or candidate_url,
                    "tiktok_rendered_linkhub_title": rendered_linkhub_debug.get("title") or "",
                    "tiktok_rendered_linkhub_error": rendered_linkhub_debug.get("error") or "",
                    "tiktok_rendered_linkhub_candidate_count": int(rendered_linkhub_debug.get("candidate_count") or 0),
                    "tiktok_rendered_linkhub_candidates": [
                        {
                            "url": candidate.get("url") or "",
                            "score": int(candidate.get("score") or 0),
                            "source": candidate.get("source") or "",
                        }
                        for candidate in rendered_linkhub_candidates[:8]
                    ],
                })

            for rendered_candidate in rendered_linkhub_candidates[: max(max_external_site_candidates * 2, 4)]:
                add_investigation_candidate(
                    result,
                    rendered_candidate.get("url") or "",
                    source="tiktok_rendered_linkhub_candidate",
                    score=rendered_candidate.get("score"),
                    usable=None,
                    reason="rendered_linkhub_candidate_ranked",
                )

            for rendered_candidate in rendered_linkhub_candidates[: max(max_external_site_candidates * 2, 4)]:
                rendered_url = normalize_investigation_candidate_url(rendered_candidate.get("url") or "", candidate_url)
                rendered_seed_score = int(rendered_candidate.get("score") or 0)
                rendered_evidence, rendered_source_metadata, rendered_metrics = fetch_remote_page_metadata(rendered_url)
                rendered_score = score_linked_page_candidate(
                    rendered_seed_score,
                    rendered_metrics,
                    strong_bonus=80,
                    dense_bonus=140,
                    food_context_bonus=20,
                )
                rendered_effective_url = get_tiktok_candidate_final_url(rendered_url, rendered_evidence) or rendered_url
                rendered_requires_follow = looks_like_tiktok_intermediate_landing_page(
                    rendered_url,
                    rendered_evidence,
                    rendered_metrics,
                )
                rendered_offer_like = looks_like_tiktok_offer_like_page(
                    rendered_effective_url,
                    rendered_evidence,
                    rendered_metrics,
                )
                rendered_usable = linked_page_looks_usable(rendered_evidence, rendered_metrics) and not rendered_offer_like
                if rendered_offer_like:
                    rendered_score -= 120
                rendered_extra = build_linked_page_candidate_extra(rendered_evidence, rendered_metrics)
                rendered_extra["intermediate_landing_page"] = rendered_requires_follow
                rendered_extra["offer_like_page"] = rendered_offer_like
                add_investigation_candidate(
                    result,
                    rendered_url,
                    source="tiktok_rendered_linkhub_candidate",
                    score=rendered_score,
                    usable=rendered_usable,
                    reason="rendered_linkhub_evaluated",
                    extra=rendered_extra,
                )
                if rendered_usable and rendered_score >= minimum_winner_score and not rendered_requires_follow and not is_homepage_like_url(rendered_effective_url) and not rendered_offer_like:
                    chosen_evidence = rendered_evidence
                    chosen_source_metadata = rendered_source_metadata
                    chosen_url = rendered_effective_url or rendered_url
                    chosen_score = rendered_score
                    chosen_site_root = candidate_url
                    break

                rendered_internal_page_candidates = extract_tiktok_internal_page_candidates(
                    rendered_evidence,
                    rendered_url,
                    title_hint,
                    affinity_tokens,
                    focus_tokens,
                )
                for internal_candidate in rendered_internal_page_candidates[:max_internal_page_candidates]:
                    add_investigation_candidate(
                        result,
                        internal_candidate.get("url") or "",
                        source="tiktok_rendered_internal_page_candidate",
                        score=internal_candidate.get("score"),
                        usable=None,
                        reason="rendered_internal_page_candidate_ranked",
                    )

                for internal_candidate in rendered_internal_page_candidates[:max_internal_page_candidates]:
                    internal_url = normalize_investigation_candidate_url(internal_candidate.get("url") or "", candidate_url)
                    internal_seed_score = int(internal_candidate.get("score") or 0)
                    candidate_evidence, candidate_source_metadata, candidate_metrics = fetch_remote_page_metadata(internal_url)
                    candidate_score = score_linked_page_candidate(
                        internal_seed_score,
                        candidate_metrics,
                        strong_bonus=80,
                        dense_bonus=140,
                        food_context_bonus=20,
                    )
                    candidate_effective_url = get_tiktok_candidate_final_url(internal_url, candidate_evidence) or internal_url
                    candidate_offer_like = looks_like_tiktok_offer_like_page(
                        candidate_effective_url,
                        candidate_evidence,
                        candidate_metrics,
                    )
                    usable_candidate = linked_page_looks_usable(candidate_evidence, candidate_metrics) and not candidate_offer_like
                    if candidate_offer_like:
                        candidate_score -= 120
                    candidate_extra = build_linked_page_candidate_extra(candidate_evidence, candidate_metrics)
                    candidate_extra["offer_like_page"] = candidate_offer_like
                    add_investigation_candidate(
                        result,
                        internal_url,
                        source="tiktok_rendered_internal_page_candidate",
                        score=candidate_score,
                        usable=usable_candidate,
                        reason="rendered_internal_page_evaluated",
                        extra=candidate_extra,
                    )
                    if usable_candidate and candidate_score >= minimum_winner_score and not candidate_offer_like:
                        chosen_evidence = candidate_evidence
                        chosen_source_metadata = candidate_source_metadata
                        chosen_url = candidate_effective_url or internal_url
                        chosen_score = candidate_score
                        chosen_site_root = rendered_url or candidate_url
                        break

                if not chosen_url and (
                    rendered_requires_follow
                    or is_homepage_like_url(rendered_effective_url)
                    or rendered_offer_like
                ):
                    same_host_winner = find_tiktok_same_host_search_winner(
                        result,
                        rendered_effective_url or rendered_url,
                        title_hint,
                        affinity_tokens,
                        focus_tokens,
                        minimum_winner_score,
                        searched_same_host_bases,
                        source_text=focus_source_text,
                    )
                    if same_host_winner:
                        chosen_evidence = same_host_winner.get("evidence")
                        chosen_source_metadata = same_host_winner.get("source_metadata") or {}
                        chosen_url = same_host_winner.get("url") or rendered_effective_url or rendered_url
                        chosen_score = same_host_winner.get("score")
                        chosen_site_root = same_host_winner.get("site_root") or rendered_effective_url or rendered_url
                        break

                if chosen_url:
                    break

        if chosen_url:
            break

        site_root_external_candidates = [] if rendered_linkhub_candidates else extract_tiktok_site_root_external_candidates(
            linked_evidence,
            candidate_url,
            title_hint,
            affinity_tokens,
        )
        for root_candidate in site_root_external_candidates[:max_external_site_candidates]:
            add_investigation_candidate(
                result,
                root_candidate.get("url") or "",
                source="tiktok_site_root_external_candidate",
                score=root_candidate.get("score"),
                usable=None,
                reason="site_root_external_candidate_ranked",
            )

        for root_candidate in site_root_external_candidates[:max_external_site_candidates]:
            root_url = normalize_investigation_candidate_url(root_candidate.get("url") or "", candidate_url)
            root_seed_score = int(root_candidate.get("score") or 0)
            root_evidence, root_source_metadata, root_metrics = fetch_remote_page_metadata(root_url)
            root_score = score_linked_page_candidate(
                root_seed_score,
                root_metrics,
                strong_bonus=80,
                dense_bonus=140,
                food_context_bonus=20,
            )
            root_effective_url = get_tiktok_candidate_final_url(root_url, root_evidence) or root_url
            root_requires_follow = looks_like_tiktok_intermediate_landing_page(
                root_url,
                root_evidence,
                root_metrics,
            )
            root_offer_like = looks_like_tiktok_offer_like_page(
                root_effective_url,
                root_evidence,
                root_metrics,
            )
            root_usable = linked_page_looks_usable(root_evidence, root_metrics) and not root_offer_like
            if root_offer_like:
                root_score -= 120
            root_extra = build_linked_page_candidate_extra(root_evidence, root_metrics)
            root_extra["intermediate_landing_page"] = root_requires_follow
            root_extra["offer_like_page"] = root_offer_like
            add_investigation_candidate(
                result,
                root_url,
                source="tiktok_site_root_external_candidate",
                score=root_score,
                usable=root_usable,
                reason="site_root_external_evaluated",
                extra=root_extra,
            )
            if root_usable and root_score >= minimum_winner_score and not root_requires_follow and not is_homepage_like_url(root_effective_url) and not root_offer_like:
                chosen_evidence = root_evidence
                chosen_source_metadata = root_source_metadata
                chosen_url = root_effective_url or root_url
                chosen_score = root_score
                chosen_site_root = candidate_url
                break

        if chosen_url:
            break

        internal_page_candidates = extract_tiktok_internal_page_candidates(
            linked_evidence,
            candidate_url,
            title_hint,
            affinity_tokens,
            focus_tokens,
        )
        for internal_candidate in internal_page_candidates[:max_internal_page_candidates]:
            add_investigation_candidate(
                result,
                internal_candidate.get("url") or "",
                source="tiktok_internal_page_candidate",
                score=internal_candidate.get("score"),
                usable=None,
                reason="internal_page_candidate_ranked",
            )

        for internal_candidate in internal_page_candidates[:max_internal_page_candidates]:
            internal_url = internal_candidate.get("url") or ""
            internal_seed_score = int(internal_candidate.get("score") or 0)
            candidate_evidence, candidate_source_metadata, candidate_metrics = fetch_remote_page_metadata(internal_url)
            candidate_score = score_linked_page_candidate(
                internal_seed_score,
                candidate_metrics,
                strong_bonus=80,
                dense_bonus=140,
                food_context_bonus=20,
            )
            candidate_effective_url = normalize_investigation_candidate_url(
                candidate_evidence.get("effective_page_url") or internal_url,
                internal_url,
            ) or internal_url
            candidate_offer_like = looks_like_tiktok_offer_like_page(
                candidate_effective_url,
                candidate_evidence,
                candidate_metrics,
            )
            usable_candidate = linked_page_looks_usable(candidate_evidence, candidate_metrics) and not candidate_offer_like
            if candidate_offer_like:
                candidate_score -= 120
            candidate_extra = build_linked_page_candidate_extra(candidate_evidence, candidate_metrics)
            candidate_extra["offer_like_page"] = candidate_offer_like
            add_investigation_candidate(
                result,
                internal_url,
                source="tiktok_internal_page_candidate",
                score=candidate_score,
                usable=usable_candidate,
                reason="internal_page_evaluated",
                extra=candidate_extra,
            )
            if usable_candidate and candidate_score >= minimum_winner_score and not candidate_offer_like:
                chosen_evidence = candidate_evidence
                chosen_source_metadata = candidate_source_metadata
                chosen_url = internal_url
                chosen_score = candidate_score
                chosen_site_root = candidate_url
                break

        if not chosen_url and (
            direct_requires_follow
            or is_homepage_like_url(direct_effective_url)
            or direct_offer_like
        ):
            same_host_winner = find_tiktok_same_host_search_winner(
                result,
                direct_effective_url or candidate_url,
                title_hint,
                affinity_tokens,
                focus_tokens,
                minimum_winner_score,
                searched_same_host_bases,
                source_text=focus_source_text,
            )
            if same_host_winner:
                chosen_evidence = same_host_winner.get("evidence")
                chosen_source_metadata = same_host_winner.get("source_metadata") or {}
                chosen_url = same_host_winner.get("url") or direct_effective_url or candidate_url
                chosen_score = same_host_winner.get("score")
                chosen_site_root = same_host_winner.get("site_root") or direct_effective_url or candidate_url
                break

        if chosen_url:
            break

        if direct_usable and linked_score >= minimum_winner_score and not is_homepage_like_url(direct_effective_url) and not direct_offer_like and (
            not direct_requires_follow
            or linked_metrics.get("looksRecipeDense")
            or linked_metrics.get("measurementSignalCount", 0) >= 2
            or linked_metrics.get("recipeVerbSignalCount", 0) >= 2
        ):
            chosen_evidence = linked_evidence
            chosen_source_metadata = linked_source_metadata
            chosen_url = direct_effective_url or candidate_url
            chosen_score = linked_score
            chosen_site_root = candidate_url
            break

    if not chosen_url or not chosen_evidence:
        return None

    return {
        "evidence": chosen_evidence,
        "source_metadata": chosen_source_metadata,
        "url": chosen_url,
        "score": chosen_score,
        "site_root": chosen_site_root,
    }


def finalize_tiktok_phone_external_site_result(
    *,
    result: dict,
    evidence: dict,
    chosen_evidence: dict,
    chosen_source_metadata: dict,
    chosen_url: str,
    chosen_score,
    chosen_site_root: str,
    source_metadata_updates: dict,
    external_site_candidates: list[dict],
):
    result["breadcrumb"] = [
        "first_pass",
        source_metadata_updates.get("source_profile_url") or "tiktok_profile",
        chosen_site_root or "external_site_root",
        chosen_url,
    ]
    finalize_linked_investigation_result(
        result,
        base_evidence=evidence,
        linked_evidence=chosen_evidence,
        winner_url=chosen_url,
        winner_score=chosen_score,
        winner_source_metadata=chosen_source_metadata,
        merge_callback=lambda: merge_tiktok_linked_page_evidence(
            evidence,
            chosen_evidence,
            chosen_url,
        ),
        explicit_recipe_link=chosen_url,
        breadcrumb_append="linked_recipe_page",
        debug_updates={
            "tiktok_external_site_url": chosen_url,
            "tiktok_external_site_candidates": [
                {
                    "url": candidate.get("url") or "",
                    "score": int(candidate.get("score") or 0),
                    "source": candidate.get("source") or "",
                }
                for candidate in external_site_candidates[:8]
            ],
        },
    )


async def investigate_tiktok_phone_external_site(
    *,
    job_id: str,
    target_url: str,
    evidence: dict,
    recipe_id: str | None = None,
) -> dict:
    result = make_investigation_result("tiktok.external_site")

    try:
        context_summary = prepare_tiktok_phone_investigation(
            result=result,
            target_url=target_url,
            evidence=evidence,
        )
        limits = context_summary["limits"]
        source_metadata_updates = context_summary["source_metadata_updates"]
        tiktok_source_debug = context_summary["tiktok_source_debug"]
        external_site_candidates = context_summary["external_site_candidates"]

        if not external_site_candidates:
            result["no_winner_reason"] = "no_external_site_url"
            return result

        title_focus = await resolve_tiktok_phone_title_focus_context(
            result=result,
            evidence=evidence,
            recipe_id=recipe_id,
            source_metadata_updates=source_metadata_updates,
            tiktok_source_debug=tiktok_source_debug,
        )

        winner_selection = await choose_tiktok_phone_external_site_winner(
            result=result,
            target_url=target_url,
            evidence=evidence,
            source_metadata_updates=source_metadata_updates,
            external_site_candidates=external_site_candidates,
            max_external_site_candidates=limits["max_external_site_candidates"],
            max_internal_page_candidates=limits["max_internal_page_candidates"],
            minimum_winner_score=limits["minimum_winner_score"],
            title_hint=title_focus["title_hint"],
            affinity_tokens=title_focus["affinity_tokens"],
            focus_tokens=title_focus["focus_tokens"],
            focus_source_text=title_focus["focus_source_text"],
        )

        if winner_selection is None:
            result["no_winner_reason"] = "no_viable_external_recipe_page"
            return result

        finalize_tiktok_phone_external_site_result(
            result=result,
            evidence=evidence,
            chosen_evidence=winner_selection["evidence"],
            chosen_source_metadata=winner_selection["source_metadata"] or {},
            chosen_url=winner_selection["url"] or "",
            chosen_score=winner_selection.get("score"),
            chosen_site_root=winner_selection.get("site_root") or "",
            source_metadata_updates=source_metadata_updates,
            external_site_candidates=external_site_candidates,
        )
        return result
    except Exception as investigation_err:
        result["attempted"] = bool(result.get("attempted"))
        result["no_winner_reason"] = f"investigation_failed:{type(investigation_err).__name__}"
        result["debug"]["error"] = f"{type(investigation_err).__name__}: {investigation_err}"
        return result



def begin_inline_investigation_attempt(result: dict, job_id: str, *, breadcrumb: list[str] | None = None) -> None:
    result["attempted"] = True
    result.setdefault("debug", {})["attempt_started_at"] = utc_now_iso()
    try:
        touch_job_heartbeat(job_id, debug_last_step="investigation_started")
    except Exception:
        pass
    if breadcrumb is not None:
        result["breadcrumb"] = [str(item) for item in breadcrumb if str(item or "")]


def resolve_investigation_runtime_limits(
    runtime_rules: dict,
    *,
    profile_default: int = 1,
    external_default: int = 2,
    internal_default: int = 4,
    minimum_winner_score_default: int = 80,
) -> dict:
    return {
        "max_profile_candidates": get_investigation_candidate_limit(runtime_rules, "profile", profile_default),
        "max_external_site_candidates": get_investigation_candidate_limit(runtime_rules, "external_site", external_default),
        "max_internal_page_candidates": get_investigation_candidate_limit(runtime_rules, "internal_page", internal_default),
        "minimum_winner_score": get_investigation_stop_threshold(
            runtime_rules,
            "minimum_winner_score",
            minimum_winner_score_default,
        ),
    }


def collect_instagram_direct_url_candidates(
    *,
    result: dict,
    evidence: dict,
    target_url: str,
    instagram_query_info: dict,
) -> list[dict]:
    direct_url_candidates: list[dict] = []
    direct_seen = set()
    direct_candidate_text = combine_text_blocks([
        evidence.get("expanded_caption_text"),
        evidence.get("meta_description"),
        evidence.get("raw_page_text"),
        evidence.get("visible_text_after_expand"),
    ])

    for direct_match in extract_urls_with_context(direct_candidate_text):
        extracted_url = direct_match.get("url") or ""
        context_line = direct_match.get("context") or direct_candidate_text
        normalized_url = unwrap_known_redirect_url(normalize_profile_url(extracted_url, target_url))
        if not normalized_url or normalized_url in direct_seen or not looks_like_fetchable_external_page(normalized_url):
            continue
        direct_seen.add(normalized_url)
        direct_anchor_score = score_recipe_link_candidate(
            normalized_url,
            context_line,
            instagram_query_info.get("primary_phrase") or instagram_query_info.get("extended_phrase") or evidence.get("page_title") or "",
            source="description_marker",
        )
        if direct_anchor_score < 20:
            continue
        direct_url_candidates.append({"url": normalized_url, "score": direct_anchor_score})
        add_investigation_candidate(
            result,
            normalized_url,
            source="instagram_current_page_url",
            score=direct_anchor_score,
            usable=None,
            reason="direct_url_candidate",
        )

    direct_url_candidates.sort(key=lambda item: item.get("score") or 0, reverse=True)
    return direct_url_candidates


async def choose_instagram_direct_candidate_winner(
    *,
    context,
    job_id: str,
    result: dict,
    target_url: str,
    source_metadata: dict,
    instagram_hint_tokens: list[str],
    instagram_query_info: dict,
    direct_url_candidates: list[dict],
    max_external_site_candidates: int,
    minimum_winner_score: int,
):
    for direct_candidate in direct_url_candidates[:max_external_site_candidates]:
        try:
            touch_job_heartbeat(job_id, debug_last_step="investigation_opening_direct_candidate")
        except Exception:
            pass

        winner = await evaluate_instagram_direct_candidate(
            context=context,
            result=result,
            target_url=target_url,
            source_metadata=source_metadata,
            instagram_hint_tokens=instagram_hint_tokens,
            instagram_query_info=instagram_query_info,
            direct_candidate_url=direct_candidate["url"],
            minimum_winner_score=minimum_winner_score,
            direct_anchor_score=int(direct_candidate.get("score") or 0),
        )
        if winner:
            return {
                "page": None,
                "owner_context": None,
                "evidence": winner["evidence"],
                "source_metadata": winner["source_metadata"],
                "url": winner["url"],
                "score": winner["score"],
                "profile_url": "",
                "site_root": winner["site_root"],
            }
    return None


async def capture_linked_investigation_winner_screenshot(
    *,
    result: dict,
    chosen_page,
    linked_submission_screenshot_path: str,
    original_submission_screenshot_path: str,
) -> None:
    if LINKED_RECIPE_SCREENSHOT and chosen_page is not None:
        try:
            await chosen_page.evaluate("window.scrollTo(0, 0)")
            await chosen_page.wait_for_timeout(400)
            await chosen_page.screenshot(path=linked_submission_screenshot_path, full_page=False)
            result["primary_submission_screenshot_path"] = linked_submission_screenshot_path
            result["secondary_submission_screenshot_path"] = original_submission_screenshot_path
        except Exception:
            result["primary_submission_screenshot_path"] = original_submission_screenshot_path

    if not result.get("primary_submission_screenshot_path"):
        result["primary_submission_screenshot_path"] = original_submission_screenshot_path


async def choose_instagram_profile_candidate_winner(
    *,
    context,
    job_id: str,
    result: dict,
    target_url: str,
    evidence: dict,
    source_metadata: dict,
    profile_candidate_urls: list[str],
    instagram_owner_hint: dict,
    instagram_hint_tokens: list[str],
    instagram_query_info: dict,
    instagram_domain_affinity_tokens: list[str],
    runtime_rules: dict,
    max_profile_candidates: int,
    max_external_site_candidates: int,
    max_internal_page_candidates: int,
    minimum_winner_score: int,
):
    chosen_page = None
    chosen_page_owner_context = None
    chosen_evidence = None
    chosen_source_metadata = {}
    chosen_url = ""
    chosen_match_score = -1
    chosen_profile_url = ""
    chosen_site_root = ""

    for profile_candidate_url in profile_candidate_urls[:max_profile_candidates]:
        if chosen_url:
            break
        try:
            touch_job_heartbeat(job_id, debug_last_step="investigation_opening_profile")
        except Exception:
            pass
        profile_page = await context.new_page()
        try:
            await profile_page.goto(
                profile_candidate_url,
                wait_until="domcontentloaded",
                timeout=LINKED_RECIPE_GOTO_TIMEOUT_MS,
            )
            await profile_page.wait_for_timeout(max(LINKED_RECIPE_WAIT_MS, 1500))

            profile_page_title = trim_text(await profile_page.title(), PAGE_TITLE_SUBMIT_MAX)
            try:
                profile_page_html_raw = await profile_page.content()
            except Exception:
                profile_page_html_raw = ""
            profile_page_html = trim_text(profile_page_html_raw, PAGE_HTML_MAX_LEN)

            profile_source_metadata = await extract_source_metadata(
                profile_page,
                "instagram",
                profile_candidate_url,
                page_title=profile_page_title,
                page_html=profile_page_html,
                page_image_url="",
            )
            if not is_instagram_source_metadata_suspicious(profile_source_metadata):
                result["source_metadata_updates"] = merge_source_metadata(
                    profile_source_metadata,
                    result.get("source_metadata_updates") or source_metadata,
                    "instagram",
                    target_url,
                )

            profile_visible_text = await extract_page_text(profile_page)
            profile_anchor_items = await extract_anchor_candidates(
                profile_page,
                ["header a[href]", "main a[href]", "a[href]"],
                limit=200,
            )
            profile_url_items = list(profile_anchor_items)
            for extracted in extract_urls_with_context(profile_visible_text):
                profile_url_items.append({"href": extracted.get("url") or "", "text": extracted.get("context") or ""})
            profile_url_items.extend(
                build_instagram_profile_url_items_from_html(
                    profile_page_html_raw,
                    profile_candidate_url,
                    instagram_domain_affinity_tokens,
                )
            )

            external_site_candidates = extract_external_site_candidates_from_items(
                profile_url_items,
                profile_candidate_url,
            )
            external_site_candidates = rerank_instagram_external_site_candidates(
                external_site_candidates,
                instagram_domain_affinity_tokens,
            )
            external_site_candidates = inject_instagram_creator_affine_external_site_candidates(
                external_site_candidates,
                source_metadata=result.get("source_metadata_updates") or source_metadata or {},
                owner_hint=instagram_owner_hint,
                clue_list=result.get("clues") or [],
                query_info=instagram_query_info,
                affinity_tokens=instagram_domain_affinity_tokens,
            )
            external_site_candidates = inject_instagram_creator_domain_seed_candidates(
                external_site_candidates,
                source_metadata=result.get("source_metadata_updates") or source_metadata or {},
                owner_hint=instagram_owner_hint,
                clue_list=result.get("clues") or [],
                affinity_tokens=instagram_domain_affinity_tokens,
            )
            external_site_candidates = filter_runtime_blocked_investigation_candidates(
                result,
                external_site_candidates,
                runtime_rules,
                source="instagram_profile_external_site",
            )

            for site_candidate in external_site_candidates[:max_external_site_candidates]:
                add_investigation_candidate(
                    result,
                    site_candidate["url"],
                    source="instagram_profile_external_site",
                    score=site_candidate.get("score"),
                    usable=None,
                    reason="external_site_candidate",
                )

            if not external_site_candidates:
                remote_profile = fetch_remote_html_document(profile_candidate_url)
                remote_profile_html = remote_profile.get("html") or ""
                remote_profile_url = remote_profile.get("url") or profile_candidate_url
                if remote_profile_html:
                    profile_url_items.extend(
                        build_instagram_profile_url_items_from_html(
                            remote_profile_html,
                            remote_profile_url,
                            instagram_domain_affinity_tokens,
                        )
                    )
                    external_site_candidates = extract_external_site_candidates_from_items(
                        profile_url_items,
                        remote_profile_url,
                    )
                    external_site_candidates = rerank_instagram_external_site_candidates(
                        external_site_candidates,
                        instagram_domain_affinity_tokens,
                    )
                    external_site_candidates = inject_instagram_creator_affine_external_site_candidates(
                        external_site_candidates,
                        source_metadata=result.get("source_metadata_updates") or source_metadata or {},
                        owner_hint=instagram_owner_hint,
                        clue_list=result.get("clues") or [],
                        query_info=instagram_query_info,
                        affinity_tokens=instagram_domain_affinity_tokens,
                    )
                    external_site_candidates = inject_instagram_creator_domain_seed_candidates(
                        external_site_candidates,
                        source_metadata=result.get("source_metadata_updates") or source_metadata or {},
                        owner_hint=instagram_owner_hint,
                        clue_list=result.get("clues") or [],
                        affinity_tokens=instagram_domain_affinity_tokens,
                    )
                    external_site_candidates = filter_runtime_blocked_investigation_candidates(
                        result,
                        external_site_candidates,
                        runtime_rules,
                        source="instagram_profile_external_site_raw_html",
                    )
                    if external_site_candidates:
                        append_job_debug_log(
                            job_id,
                            "Instagram profile raw-HTML fallback recovered external site candidates: "
                            + ", ".join(f"{candidate['url']}(score={candidate.get('score', 0)})" for candidate in external_site_candidates[:3]),
                            debug_last_step="instagram_profile_raw_html_recovered",
                        )
                        for site_candidate in external_site_candidates[:max_external_site_candidates]:
                            add_investigation_candidate(
                                result,
                                site_candidate["url"],
                                source="instagram_profile_external_site_raw_html",
                                score=site_candidate.get("score"),
                                usable=None,
                                reason="external_site_candidate_raw_html",
                            )
                    else:
                        append_job_debug_log(
                            job_id,
                            f"Instagram profile raw-HTML fallback found no external site links on profile {remote_profile_url}.",
                            debug_last_step="instagram_profile_raw_html_no_external_site",
                        )
                elif remote_profile.get("error"):
                    append_job_debug_log(
                        job_id,
                        f"Instagram profile raw-HTML fallback failed for {profile_candidate_url}: {remote_profile.get('error')}",
                        debug_last_step="instagram_profile_raw_html_failed",
                    )

            if not external_site_candidates:
                continue

            site_candidate_limit = max_external_site_candidates
            if any((canonical_domain((item or {}).get("url") or "").startswith("just-")) for item in external_site_candidates):
                site_candidate_limit = max(site_candidate_limit, 3)

            for site_candidate in external_site_candidates[:site_candidate_limit]:
                if looks_like_instagram_spam_candidate(site_candidate.get("url") or "", instagram_domain_affinity_tokens):
                    add_investigation_candidate(
                        result,
                        site_candidate["url"],
                        source="instagram_profile_external_site",
                        score=site_candidate.get("score"),
                        usable=False,
                        reason="external_site_candidate_rejected_as_spam",
                    )
                    continue
                site_page = None
                site_page_owner_context = None
                try:
                    try:
                        touch_job_heartbeat(job_id, debug_last_step="investigation_opening_site_root")
                    except Exception:
                        pass
                    try:
                        site_page, site_evidence, site_source_metadata, site_metrics, site_page_owner_context = await open_web_candidate_page(
                            context,
                            "instagram",
                            site_candidate["url"],
                            goto_timeout_ms=min(LINKED_RECIPE_GOTO_TIMEOUT_MS, INSTAGRAM_SITE_ROOT_GOTO_TIMEOUT_MS),
                            wait_ms=min(LINKED_RECIPE_WAIT_MS, INSTAGRAM_SITE_ROOT_WAIT_MS),
                            platform_hint="web",
                        )
                    except Exception as site_root_err:
                        add_investigation_candidate(
                            result,
                            site_candidate["url"],
                            source="instagram_site_root",
                            score=site_candidate.get("score"),
                            usable=False,
                            reason="direct_site_open_failed",
                            extra={"error": f"{type(site_root_err).__name__}: {site_root_err}"},
                        )
                        continue
                    effective_site_url = normalize_profile_url(
                        site_evidence.get("effective_page_url") or site_candidate["url"],
                        site_candidate["url"],
                    ) or site_candidate["url"]
                    parsed_effective_site = urlparse(effective_site_url)
                    effective_site_host = canonical_domain(effective_site_url) or site_candidate["host"]
                    effective_site_root_url = (
                        f"{parsed_effective_site.scheme}://{parsed_effective_site.netloc}/"
                        if parsed_effective_site.scheme and parsed_effective_site.netloc
                        else effective_site_url
                    )

                    direct_site_match_score = score_instagram_linked_page_match(
                        site_evidence,
                        target_url,
                        instagram_hint_tokens,
                        (result.get("source_metadata_updates") or source_metadata).get("source_creator_handle") or "",
                        instagram_query_info,
                    )
                    direct_site_has_reference = page_contains_instagram_reference(site_evidence, target_url)
                    direct_site_is_homepage = is_homepage_like_url(effective_site_url)
                    add_investigation_candidate(
                        result,
                        effective_site_url,
                        source="instagram_site_root",
                        score=direct_site_match_score,
                        usable=linked_page_looks_usable(site_evidence, site_metrics),
                        reason="direct_site_page_evaluated",
                        extra={
                            "page_title": site_evidence.get("page_title") or "",
                            "direct_site_has_reference": direct_site_has_reference,
                            "direct_site_is_homepage": direct_site_is_homepage,
                            "original_candidate_url": site_candidate["url"],
                            "effective_site_host": effective_site_host,
                        },
                    )

                    if linked_page_looks_usable(site_evidence, site_metrics) and direct_site_match_score >= minimum_winner_score and (direct_site_has_reference or not direct_site_is_homepage):
                        chosen_page = site_page
                        chosen_page_owner_context = site_page_owner_context
                        chosen_evidence = site_evidence
                        chosen_source_metadata = site_source_metadata
                        chosen_url = effective_site_url
                        chosen_match_score = direct_site_match_score
                        chosen_profile_url = profile_candidate_url
                        chosen_site_root = effective_site_url
                        site_page = None
                        site_page_owner_context = None
                        break

                    if site_page is None:
                        continue

                    site_anchor_items = await extract_anchor_candidates(site_page, ["a[href]"], limit=220)
                    raw_html_anchor_items = extract_anchor_like_items_from_html(site_evidence.get("page_html") or "", effective_site_url, limit=220)
                    if raw_html_anchor_items:
                        site_anchor_items.extend(raw_html_anchor_items)
                    site_root_url_items = list(site_anchor_items)
                    for extracted in extract_urls_with_context(site_evidence.get("visible_page_text") or ""):
                        site_root_url_items.append({"href": extracted.get("url") or "", "text": extracted.get("context") or ""})
                    for extracted_url in extract_urls_from_text(site_evidence.get("page_html") or ""):
                        site_root_url_items.append({"href": extracted_url, "text": ""})

                    site_root_external_candidates = extract_external_site_candidates_from_items(
                        site_root_url_items,
                        effective_site_url,
                        preferred_host=effective_site_host,
                    )
                    site_root_external_candidates = rerank_instagram_external_site_candidates(
                        site_root_external_candidates,
                        instagram_domain_affinity_tokens,
                    )

                    for root_external_candidate in site_root_external_candidates[:max_external_site_candidates]:
                        if canonical_domain(root_external_candidate.get("url") or "") == effective_site_host:
                            continue
                        add_investigation_candidate(
                            result,
                            root_external_candidate["url"],
                            source="instagram_site_root_external_candidate",
                            score=root_external_candidate.get("score"),
                            usable=None,
                            reason="site_root_external_candidate",
                        )

                    for root_external_candidate in site_root_external_candidates[:max_external_site_candidates]:
                        if canonical_domain(root_external_candidate.get("url") or "") == effective_site_host:
                            continue
                        chosen_direct_external = await evaluate_instagram_direct_candidate(
                            context=context,
                            result=result,
                            target_url=target_url,
                            source_metadata=result.get("source_metadata_updates") or source_metadata,
                            instagram_hint_tokens=instagram_hint_tokens,
                            instagram_query_info=instagram_query_info,
                            direct_candidate_url=root_external_candidate["url"],
                            minimum_winner_score=minimum_winner_score,
                            direct_anchor_score=int(root_external_candidate.get("score") or 0),
                        )
                        if chosen_direct_external:
                            chosen_evidence = chosen_direct_external["evidence"]
                            chosen_source_metadata = chosen_direct_external["source_metadata"]
                            chosen_url = chosen_direct_external["url"]
                            chosen_match_score = chosen_direct_external["score"]
                            chosen_profile_url = profile_candidate_url
                            chosen_site_root = effective_site_url
                            break

                    if chosen_url:
                        site_page = None
                        break

                    search_requests = build_instagram_site_search_requests(effective_site_root_url or effective_site_url, instagram_query_info)
                    search_anchor_items = []

                    for search_request in search_requests[:2]:
                        search_page = None
                        search_page_owner_context = None
                        try:
                            try:
                                touch_job_heartbeat(job_id, debug_last_step="investigation_site_search")
                            except Exception:
                                pass
                            search_page, search_page_owner_context = await open_page_with_context_policy(
                                context,
                                current_platform="instagram",
                                url=search_request["url"],
                                platform_hint="web",
                            )
                            try:
                                await search_page.goto(search_request["url"], wait_until="domcontentloaded", timeout=min(LINKED_RECIPE_GOTO_TIMEOUT_MS, INSTAGRAM_SITE_ROOT_GOTO_TIMEOUT_MS))
                                await search_page.wait_for_timeout(max(900, min(1200, LINKED_RECIPE_WAIT_MS)))
                                discovered = await extract_anchor_candidates(search_page, ["a[href]"], limit=220)
                                search_anchor_items.extend(discovered)
                            except Exception as search_err:
                                add_investigation_candidate(
                                    result,
                                    search_request["url"],
                                    source="instagram_site_search",
                                    score=None,
                                    usable=False,
                                    reason="site_search_open_failed",
                                    extra={"error": f"{type(search_err).__name__}: {search_err}", "query": search_request.get("query") or ""},
                                )
                        finally:
                            await close_page_and_context(search_page, search_page_owner_context)

                    ranked_site_candidates = []
                    seen_site_urls = set()
                    for anchor_item in (search_anchor_items + site_anchor_items):
                        normalized_candidate_url = unwrap_known_redirect_url(
                            normalize_profile_url(anchor_item.get("href") or "", effective_site_url or site_candidate["url"])
                        )
                        score = score_instagram_site_anchor_candidate(
                            normalized_candidate_url,
                            anchor_item.get("text") or "",
                            effective_site_host,
                            instagram_hint_tokens,
                            instagram_query_info,
                        )
                        if score < 40:
                            continue
                        dedupe_key = strip_url_query_fragment(normalized_candidate_url).lower()
                        if dedupe_key in seen_site_urls:
                            continue
                        seen_site_urls.add(dedupe_key)
                        ranked_site_candidates.append({
                            "url": normalized_candidate_url,
                            "text": anchor_item.get("text") or "",
                            "score": score,
                        })

                    ranked_site_candidates.sort(key=lambda item: item["score"], reverse=True)
                    for candidate_item in ranked_site_candidates[:max_internal_page_candidates]:
                        add_investigation_candidate(
                            result,
                            candidate_item["url"],
                            source="instagram_internal_page_candidate",
                            score=candidate_item.get("score"),
                            usable=None,
                            reason="internal_page_candidate_ranked",
                        )

                    evaluated_candidates = []
                    for candidate_item in ranked_site_candidates[:max_internal_page_candidates]:
                        candidate_page = None
                        candidate_page_owner_context = None
                        try:
                            try:
                                touch_job_heartbeat(job_id, debug_last_step="investigation_opening_internal_candidate")
                            except Exception:
                                pass
                            try:
                                candidate_page, candidate_evidence, candidate_source_metadata, candidate_metrics, candidate_page_owner_context = await open_web_candidate_page(
                                    context,
                                    "instagram",
                                    candidate_item["url"],
                                    platform_hint="web",
                                )
                            except Exception as candidate_open_err:
                                add_investigation_candidate(
                                    result,
                                    candidate_item["url"],
                                    source="instagram_internal_page_candidate",
                                    score=candidate_item.get("score"),
                                    usable=False,
                                    reason="internal_page_open_failed",
                                    extra={"error": f"{type(candidate_open_err).__name__}: {candidate_open_err}"},
                                )
                                continue
                            candidate_match_score = score_instagram_linked_page_match(
                                candidate_evidence,
                                target_url,
                                instagram_hint_tokens,
                                (result.get("source_metadata_updates") or source_metadata).get("source_creator_handle") or "",
                                instagram_query_info,
                            )
                            match_debug = build_query_match_debug(
                                (instagram_query_info.get('primary_phrase') or instagram_query_info.get('extended_phrase') or ''),
                                candidate_evidence.get('page_title') or '',
                                candidate_item["url"],
                            )
                            usable_candidate = linked_page_looks_usable(candidate_evidence, candidate_metrics) and candidate_match_score >= minimum_winner_score
                            add_investigation_candidate(
                                result,
                                candidate_item["url"],
                                source="instagram_internal_page_candidate",
                                score=candidate_match_score,
                                usable=usable_candidate,
                                reason="internal_page_evaluated",
                                extra={
                                    "page_title": candidate_evidence.get("page_title") or "",
                                    "title_exact": match_debug.get("title_exact"),
                                    "slug_exact": match_debug.get("slug_exact"),
                                    "title_extra_tokens": match_debug.get("title_extra_tokens") or [],
                                    "slug_extra_tokens": match_debug.get("slug_extra_tokens") or [],
                                },
                            )
                            evaluated_candidates.append({
                                'usable': usable_candidate,
                                'score': candidate_match_score,
                                'anchor_score': candidate_item.get('score', 0),
                                'match_debug': match_debug,
                                'page': candidate_page,
                                'owner_context': candidate_page_owner_context,
                                'evidence': candidate_evidence,
                                'source_metadata': candidate_source_metadata,
                                'url': candidate_item['url'],
                            })
                            candidate_page = None
                            candidate_page_owner_context = None
                        finally:
                            await close_page_and_context(candidate_page, candidate_page_owner_context)

                    viable_candidates = [item for item in evaluated_candidates if item['usable']]
                    if viable_candidates:
                        viable_candidates.sort(
                            key=lambda item: (
                                item['match_debug'].get('title_exact') or item['match_debug'].get('slug_exact'),
                                -len(item['match_debug'].get('title_extra_tokens') or []) - len(item['match_debug'].get('slug_extra_tokens') or []),
                                item['score'],
                                item['anchor_score'],
                            ),
                            reverse=True,
                        )
                        best_candidate = viable_candidates[0]
                        chosen_page = best_candidate['page']
                        chosen_page_owner_context = best_candidate.get('owner_context')
                        chosen_evidence = best_candidate['evidence']
                        chosen_source_metadata = best_candidate['source_metadata']
                        chosen_url = best_candidate['url']
                        chosen_match_score = best_candidate['score']
                        chosen_profile_url = profile_candidate_url
                        chosen_site_root = site_candidate["url"]

                    for loser in evaluated_candidates:
                        if loser.get('page') is not None and loser.get('page') is not chosen_page:
                            await close_page_and_context(loser.get('page'), loser.get('owner_context'))

                    if chosen_url:
                        site_page = None
                        break
                finally:
                    await close_page_and_context(site_page, site_page_owner_context)

            if chosen_url:
                break
        finally:
            try:
                await profile_page.close()
            except Exception:
                pass

    if not chosen_url or not chosen_evidence:
        return None

    return {
        "page": chosen_page,
        "owner_context": chosen_page_owner_context,
        "evidence": chosen_evidence,
        "source_metadata": chosen_source_metadata,
        "url": chosen_url,
        "score": chosen_match_score,
        "profile_url": chosen_profile_url,
        "site_root": chosen_site_root,
    }



async def investigate_instagram_external_site(
    *,
    context,
    page,
    job_id: str,
    target_url: str,
    evidence: dict,
    source_metadata: dict,
    original_submission_screenshot_path: str,
    linked_submission_screenshot_path: str,
) -> dict:
    result = make_investigation_result("instagram.external_site")
    context_summary = build_instagram_investigation_context(
        target_url=target_url,
        evidence=evidence,
        source_metadata=source_metadata,
    )
    runtime = context_summary["runtime"]
    runtime_rules = context_summary["runtime_rules"]
    instagram_owner_hint = context_summary["instagram_owner_hint"]
    instagram_hint_tokens = context_summary["instagram_hint_tokens"]
    instagram_mentions = context_summary["instagram_mentions"]
    instagram_query_info = context_summary["instagram_query_info"]
    should_discover_instagram = context_summary["should_discover"]
    instagram_discovery_reasons = context_summary["discovery_reasons"]
    result["clues"] = list(dict.fromkeys(context_summary["detected_clues"] + list(instagram_hint_tokens[:8])))
    result["reasons"] = list(instagram_discovery_reasons or [])
    if context_summary["source_metadata_updates"]:
        result["source_metadata_updates"] = context_summary["source_metadata_updates"]
    instagram_domain_affinity_tokens = context_summary["instagram_domain_affinity_tokens"]

    result["debug"].update({
        "investigation_engine_version": INVESTIGATION_ENGINE_VERSION,
        "fixture_seed_matches": context_summary["fixture_seed_matches"],
        "runtime_version": runtime.get("runtime_version"),
        "runtime_source": runtime.get("runtime_source"),
        "runtime_error": runtime.get("runtime_error"),
        "runtime_rules_summary": context_summary["rules_summary"],
        "instagram_owner_hint": instagram_owner_hint,
        "instagram_hint_tokens": instagram_hint_tokens[:12],
        "instagram_mentions": instagram_mentions[:12],
        "instagram_query_info": instagram_query_info,
        "instagram_domain_affinity_tokens": instagram_domain_affinity_tokens[:12],
    })

    if not should_discover_instagram and context_summary["fixture_seed_matches"]:
        should_discover_instagram = True
        result["reasons"] = list(dict.fromkeys(list(result.get("reasons") or []) + ["fixture_seed_match"]))
        result.setdefault("debug", {})["forced_by_fixture_seed"] = True

    if not should_discover_instagram:
        result["no_winner_reason"] = "clues_not_strong_enough"
        return result

    begin_inline_investigation_attempt(
        result,
        job_id,
        breadcrumb=["first_pass", "instagram_profile"],
    )
    profile_candidate_urls = build_instagram_profile_candidate_urls(
        result.get("source_metadata_updates") or source_metadata,
        instagram_owner_hint,
    )
    if not profile_candidate_urls:
        result["no_winner_reason"] = "no_trustworthy_profile_candidate"
        return result

    limits = resolve_investigation_runtime_limits(runtime_rules)
    max_profile_candidates = limits["max_profile_candidates"]
    max_external_site_candidates = limits["max_external_site_candidates"]
    max_internal_page_candidates = limits["max_internal_page_candidates"]
    minimum_winner_score = limits["minimum_winner_score"]

    winner_selection = None
    direct_url_candidates = collect_instagram_direct_url_candidates(
        result=result,
        evidence=evidence,
        target_url=target_url,
        instagram_query_info=instagram_query_info,
    )
    if direct_url_candidates:
        winner_selection = await choose_instagram_direct_candidate_winner(
            context=context,
            job_id=job_id,
            result=result,
            target_url=target_url,
            source_metadata=(result.get("source_metadata_updates") or source_metadata or {}),
            instagram_hint_tokens=instagram_hint_tokens,
            instagram_query_info=instagram_query_info,
            direct_url_candidates=direct_url_candidates,
            max_external_site_candidates=max_external_site_candidates,
            minimum_winner_score=minimum_winner_score,
        )

    if winner_selection is None:
        winner_selection = await choose_instagram_profile_candidate_winner(
            context=context,
            job_id=job_id,
            result=result,
            target_url=target_url,
            evidence=evidence,
            source_metadata=source_metadata,
            profile_candidate_urls=profile_candidate_urls,
            instagram_owner_hint=instagram_owner_hint,
            instagram_hint_tokens=instagram_hint_tokens,
            instagram_query_info=instagram_query_info,
            instagram_domain_affinity_tokens=instagram_domain_affinity_tokens,
            runtime_rules=runtime_rules,
            max_profile_candidates=max_profile_candidates,
            max_external_site_candidates=max_external_site_candidates,
            max_internal_page_candidates=max_internal_page_candidates,
            minimum_winner_score=minimum_winner_score,
        )

    if winner_selection is None:
        result["no_winner_reason"] = "no_viable_external_recipe_page"
        return result

    chosen_page = winner_selection.get("page")
    chosen_page_owner_context = winner_selection.get("owner_context")
    chosen_evidence = winner_selection.get("evidence")
    chosen_source_metadata = winner_selection.get("source_metadata") or {}
    chosen_url = winner_selection.get("url") or ""
    chosen_match_score = winner_selection.get("score")
    chosen_profile_url = winner_selection.get("profile_url") or ""
    chosen_site_root = winner_selection.get("site_root") or ""

    result["breadcrumb"] = ["first_pass", chosen_profile_url or "instagram_profile", chosen_site_root or "external_site_root", chosen_url]
    finalize_linked_investigation_result(
        result,
        base_evidence=evidence,
        linked_evidence=chosen_evidence,
        winner_url=chosen_url,
        winner_score=chosen_match_score,
        winner_source_metadata=chosen_source_metadata,
        merge_callback=lambda: merge_instagram_linked_page_evidence(
            evidence,
            chosen_evidence,
            chosen_url,
        ),
        explicit_recipe_link=chosen_url,
        debug_updates={
            "instagram_profile_url": chosen_profile_url,
            "instagram_external_site_url": chosen_url,
        },
    )

    await capture_linked_investigation_winner_screenshot(
        result=result,
        chosen_page=chosen_page,
        linked_submission_screenshot_path=linked_submission_screenshot_path,
        original_submission_screenshot_path=original_submission_screenshot_path,
    )

    await close_page_and_context(chosen_page, chosen_page_owner_context)
    return result






def get_investigation_handler(platform: str, collection_method: str):
    normalized_platform = normalize_platform(platform or "", "")
    normalized_method = normalize_text(collection_method or "").lower()
    handlers = {
        ("youtube", "browser"): ("youtube.linked_recipe", investigate_youtube_linked_recipe),
        ("instagram", "browser"): ("instagram.external_site", investigate_instagram_external_site),
        ("tiktok", "phone"): ("tiktok.external_site", investigate_tiktok_phone_external_site),
    }
    return handlers.get((normalized_platform, normalized_method))

async def run_inline_investigation(
    *,
    job_id: str,
    platform: str,
    target_url: str,
    evidence: dict,
    recipe_id: str | None = None,
    source_metadata: dict | None = None,
    page=None,
    context=None,
    original_submission_screenshot_path: str = "",
    linked_submission_screenshot_path: str = "",
    collection_method: str = "browser",
) -> dict:
    source_metadata = source_metadata or enrich_source_metadata({}, platform, target_url)
    handler_info = get_investigation_handler(platform, collection_method)

    if not handler_info:
        result = make_investigation_result(f"{platform}.inline")
        result["debug"].update({
            "investigation_engine_version": INVESTIGATION_ENGINE_VERSION,
            "handler_key": f"{normalize_platform(platform or '', target_url)}:{normalize_text(collection_method or '').lower()}",
            "fixture_seed_matches": match_investigation_fixture_seeds(f"{normalize_platform(platform or '', target_url)}.inline", target_url),
        })
        result["no_winner_reason"] = "no_investigation_handler_for_platform"
        return result

    scenario_key, handler = handler_info
    if collection_method == "phone":
        result = await handler(
            job_id=job_id,
            target_url=target_url,
            evidence=evidence,
            recipe_id=recipe_id,
        )
    else:
        if page is None or context is None:
            result = make_investigation_result(scenario_key)
            result["no_winner_reason"] = "missing_browser_context"
        else:
            result = await handler(
                context=context,
                page=page,
                job_id=job_id,
                target_url=target_url,
                evidence=evidence,
                source_metadata=source_metadata,
                original_submission_screenshot_path=original_submission_screenshot_path,
                linked_submission_screenshot_path=linked_submission_screenshot_path,
            )

    result.setdefault("debug", {}).update({
        "investigation_engine_version": INVESTIGATION_ENGINE_VERSION,
        "handler_key": f"{normalize_platform(platform or '', target_url)}:{normalize_text(collection_method or '').lower()}",
    })
    result["debug"].setdefault("fixture_seed_matches", match_investigation_fixture_seeds(result.get("mode") or scenario_key, target_url))
    return result


async def main():
    platform_allowlist = get_platform_allowlist()
    job = claim_next_job(platform_allowlist=platform_allowlist)

    if not job:
        print("JOBS_FOUND = 0")
        print("NO_PENDING_JOBS")
        return

    print("JOBS_FOUND = 1")

    job_id = job["id"]
    recipe_id = job.get("recipe_id")
    target_url = job["target_url"]
    claim_lock_token = get_job_lock_token(job)
    original_platform = (job.get("platform") or "").lower()
    platform = normalize_platform(original_platform, target_url)
    effective_analysis_platform = platform

    print("CLAIMED_JOB_ID =", job_id)
    print("CLAIM_LOCK_TOKEN =", claim_lock_token or "")

    assert_job_claim_is_current(job_id, claim_lock_token, "after_claim")

    if platform != original_platform:
        try:
            update_job(job_id, {"platform": platform})
        except Exception:
            pass

    append_job_debug_log(
        job_id,
        f"Claimed job for URL: {target_url}",
        debug_status="processing",
        debug_last_step="job_claimed",
    )
    append_job_debug_log(
        job_id,
        f"Resolved platform = {platform} (original job platform = {original_platform or 'empty'})",
        debug_last_step="platform_resolved",
    )
    append_job_debug_log(
        job_id,
        f"Pi build = {PI_ANALYZER_BUILD}",
        debug_last_step="pi_build_info",
        debug_data={"pi_build": PI_ANALYZER_BUILD},
    )
    append_job_debug_log(
        job_id,
        f"Analyzer build version: {ANALYZER_BUILD_VERSION}",
        debug_last_step="build_version_logged",
    )
    append_job_debug_log(
        job_id,
        f"Investigation patch version: {INVESTIGATION_PATCH_VERSION}",
        debug_last_step="patch_version_logged",
        debug_data={"investigation_patch_version": INVESTIGATION_PATCH_VERSION},
    )

    collector_identity = get_current_collector_identity()
    confirmation_context = extract_confirmation_job_context(job)

    append_job_debug_log(
        job_id,
        (
            f"Collector identity = node={collector_identity.get('collector_node_id') or 'none'} "
            f"profile={collector_identity.get('collector_profile_id') or 'none'} "
            f"label={collector_identity.get('collector_account_label') or 'none'} "
            f"default_claim={collector_identity.get('can_claim_default_jobs')} "
            f"confirmation_claim={collector_identity.get('can_claim_confirmation_jobs')}"
        ),
        debug_last_step="collector_identity_logged",
        debug_data=collector_identity,
    )

    if confirmation_context.get("is_confirmation_job"):
        append_job_debug_log(
            job_id,
            (
                f"Confirmation job context detected. reason={confirmation_context.get('confirmation_reason') or 'none'} "
                f"require_different_profile_from={confirmation_context.get('require_different_profile_from') or 'none'} "
                f"parent_job_id={confirmation_context.get('parent_job_id') or 'none'}"
            ),
            debug_last_step="confirmation_job_context",
            debug_data={
                "confirmation_job": True,
                "confirmation_reason": confirmation_context.get("confirmation_reason") or "",
                "confirmation_parent_job_id": confirmation_context.get("parent_job_id") or "",
                "require_different_profile_from": confirmation_context.get("require_different_profile_from") or "",
                "allowed_collector_profile_ids": confirmation_context.get("allowed_collector_profile_ids") or [],
                "excluded_collector_profile_ids": confirmation_context.get("excluded_collector_profile_ids") or [],
                "required_collector_capabilities": confirmation_context.get("required_collector_capabilities") or [],
            },
        )

        current_profile_id = str(COLLECTOR_PROFILE_ID or "").strip().lower()
        required_other_profile = str(confirmation_context.get("require_different_profile_from") or "").strip().lower()

        if not current_profile_id:
            fail_job(job_id, "Confirmation job requires COLLECTOR_PROFILE_ID on this worker.")
            print("CONFIRMATION_JOB_SKIPPED = missing_collector_profile_id")
            return

        if required_other_profile and current_profile_id == required_other_profile:
            fail_job(job_id, "Confirmation job was claimed by the same collector_profile_id that it must avoid.")
            print("CONFIRMATION_JOB_SKIPPED = same_profile_forbidden")
            return

    if recipe_id and not confirmation_context.get("is_confirmation_job"):
        update_recipe_debug(
            recipe_id,
            debug_log_append=(
                f"Claimed BotJob {job_id} | Runner versions: "
                f"pi={PI_ANALYZER_BUILD} analyzer={ANALYZER_BUILD_VERSION} investigator={INVESTIGATION_PATCH_VERSION} history_writer={INVESTIGATION_HISTORY_WRITER_VERSION}"
            ),
        )

    screenshot_path = str(SCREENSHOT_DIR / f"{job_id}.png")
    linked_screenshot_path = str(SCREENSHOT_DIR / f"{job_id}_linked.png")
    profile_dir = persistent_profile_dir_for(platform, COLLECTOR_PROFILE_ID)
    profile_dir.mkdir(parents=True, exist_ok=True)

    try:
        if should_use_phone_worker(platform):
            append_job_debug_log(
                job_id,
                f"Routing {platform} job to Android phone worker",
                debug_last_step="phone_worker_start",
            )

            assert_job_claim_is_current(job_id, claim_lock_token, "before_phone_worker")
            phone_worker_fallback_summary = None
            try:
                evidence = run_phone_worker_job(job_id, platform, target_url)
            except Exception as phone_worker_err:
                phone_worker_error_text = f"{type(phone_worker_err).__name__}: {phone_worker_err}"
                append_job_debug_log(
                    job_id,
                    f"Phone worker failed for {platform}; attempting server_first_pass fallback. error={trim_text(phone_worker_error_text, 500)}",
                    debug_last_step="phone_worker_failed",
                    debug_data={"platform": platform, "error": trim_text(phone_worker_error_text, 1000)},
                )
                evidence, phone_worker_fallback_summary = build_phone_worker_fallback_evidence(
                    job_id=job_id,
                    platform=platform,
                    target_url=target_url,
                    current_job=job,
                    error_text=phone_worker_error_text,
                )
                if not evidence:
                    raise
                append_job_debug_log(
                    job_id,
                    f"Phone worker fallback loaded. method={evidence.get('collection_method') or 'unknown'} raw_text_len={len(evidence.get('raw_page_text') or '')} caption_len={len(evidence.get('expanded_caption_text') or '')}",
                    debug_last_step="phone_worker_fallback_loaded",
                    debug_data=phone_worker_fallback_summary or {},
                )
            screenshot_path = evidence.get("primary_screenshot_path") or screenshot_path

            append_job_debug_log(
                job_id,
                (
                    f"Phone evidence collected. platform={platform} "
                    f"bundle_dir={evidence['bundle_dir']} "
                    f"media_type_guess={evidence['media_type_guess']} "
                    f"is_video={evidence['is_video']} "
                    f"caption_expanded={evidence['caption_expanded']} "
                    f"expand_method={evidence['expand_method']} "
                    f"expand_success={evidence['expand_success']} "
                    f"raw_text_len={len(evidence['raw_page_text'])} "
                    f"caption_len={len(evidence['expanded_caption_text'])}"
                ),
                debug_last_step="phone_evidence_collected",
                debug_data={
                    "platform": platform,
                    "collection_method": evidence["collection_method"],
                    "execution_actor": "phone",
                    "assigned_device": "phone_auto_queue",
                    "source_device": DEVICE_NAME,
                    "execution_device": "android_phone_worker",
                    "execution_path": "phone",
                    "runner": "phone",
                    "effective_page_url": evidence["effective_page_url"],
                    "bundle_dir": evidence["bundle_dir"],
                    "primary_screenshot_path": evidence["primary_screenshot_path"],
                    "description_screenshot_path": evidence["description_screenshot_path"],
                    "caption_expanded": evidence["caption_expanded"],
                    "expand_attempted": evidence["expand_attempted"],
                    "expand_success": evidence["expand_success"],
                    "expand_method": evidence["expand_method"],
                    "caption_before_len": evidence["caption_before_len"],
                    "caption_after_len": evidence["caption_after_len"],
                    "transcript_text_len": len(evidence.get("transcript_text") or ""),
                    "transcript_attempted": bool(evidence.get("transcript_attempted")),
                    "transcript_success": bool(evidence.get("transcript_success")),
                    "transcript_method": evidence.get("transcript_method") or "",
                    "worker_stdout": evidence["worker_stdout"],
                    "worker_stderr": evidence["worker_stderr"],
                },
            )

            description_screenshot_url = ""
            if evidence.get("description_screenshot_path"):
                description_upload = upload_bot_screenshot(
                    job_id=job_id,
                    image_path=evidence["description_screenshot_path"],
                    debug_last_step="description_screenshot_uploaded",
                )
                description_screenshot_url = description_upload.get("url", "")
                append_job_debug_log(
                    job_id,
                    f"Description screenshot uploaded to Base44: {description_screenshot_url}",
                    debug_last_step="description_screenshot_uploaded",
                )

            screenshot_url = ""
            primary_screenshot_path = evidence.get("primary_screenshot_path") or ""
            if primary_screenshot_path and Path(primary_screenshot_path).exists():
                upload_result = upload_bot_screenshot(
                    job_id=job_id,
                    image_path=primary_screenshot_path,
                    debug_last_step="screenshot_uploaded",
                )
                screenshot_url = upload_result.get("url", "")
                append_job_debug_log(
                    job_id,
                    f"Primary screenshot uploaded to Base44: {screenshot_url}",
                    debug_last_step="screenshot_uploaded",
                )
            else:
                screenshot_url = normalize_text(
                    evidence.get("primary_screenshot_url")
                    or evidence.get("page_image_url")
                    or ""
                )
                if screenshot_url:
                    append_job_debug_log(
                        job_id,
                        f"Reused existing screenshot URL for phone fallback: {screenshot_url}",
                        debug_last_step="screenshot_uploaded",
                    )

            if recipe_id and not confirmation_context.get("is_confirmation_job") and screenshot_url:
                update_recipe_debug(
                    recipe_id,
                    debug_log_append=f"Phone worker screenshot uploaded for BotJob {job_id}",
                    debug_screenshot_url=screenshot_url,
                )

            confirmation_source_evidence = dict(evidence)

            if not confirmation_context.get("is_confirmation_job"):
                try:
                    merged_phone_evidence, merge_summary = merge_phone_worker_evidence_with_existing_job(
                        job_id=job_id,
                        evidence=confirmation_source_evidence,
                        current_job=job,
                    )
                    if merge_summary.get("merged"):
                        evidence = merged_phone_evidence
                        confirmation_source_evidence = dict(merged_phone_evidence)
                        append_job_debug_log(
                            job_id,
                            (
                                "Merged existing BotJob submission evidence into phone worker evidence before investigation. "
                                f"fields={', '.join(merge_summary.get('merged_fields') or []) or 'none'} "
                                f"raw_text_len={len(evidence.get('raw_page_text') or '')} "
                                f"caption_len={len(evidence.get('expanded_caption_text') or '')}"
                            ),
                            debug_last_step="phone_evidence_merged_with_existing",
                            debug_data=merge_summary,
                        )
                except Exception as phone_merge_err:
                    append_job_debug_log(
                        job_id,
                        f"Could not merge existing BotJob evidence into phone worker evidence: {type(phone_merge_err).__name__}: {phone_merge_err}",
                        debug_last_step="phone_evidence_merge_failed",
                    )

            tiktok_external_site_url = ""
            linked_recipe_used = False
            linked_recipe_url = ""
            source_metadata = enrich_source_metadata({}, platform, target_url)
            investigation_result = await run_inline_investigation(
                job_id=job_id,
                platform=platform,
                target_url=target_url,
                evidence=evidence,
                recipe_id=recipe_id,
                source_metadata=source_metadata,
                collection_method="phone",
            )
            source_metadata = merge_source_metadata(
                investigation_result.get("source_metadata_updates") or {},
                source_metadata,
                platform,
                target_url,
            )
            if investigation_result.get("winner_source_metadata"):
                source_metadata = merge_source_metadata(
                    source_metadata,
                    investigation_result.get("winner_source_metadata") or {},
                    platform,
                    target_url,
                )
            if investigation_result.get("merged_evidence"):
                evidence = investigation_result.get("merged_evidence") or evidence
            linked_recipe_used = bool(investigation_result.get("linked_recipe_used"))
            linked_recipe_url = investigation_result.get("explicit_recipe_link") or investigation_result.get("winner_url") or ""
            effective_analysis_platform = investigation_result.get("effective_analysis_platform") or effective_analysis_platform
            tiktok_external_site_url = str(
                investigation_result.get("winner_url")
                or (investigation_result.get("debug") or {}).get("tiktok_external_site_url")
                or ""
            )
            log_investigation_result(job_id, investigation_result)

            materialized_avatar_url = await materialize_source_avatar(None, job_id, source_metadata, current_platform=platform)
            if materialized_avatar_url:
                source_metadata["source_avatar_url"] = materialized_avatar_url
                append_job_debug_log(
                    job_id,
                    f"Source avatar uploaded to Base44: {materialized_avatar_url}",
                    debug_last_step="source_avatar_uploaded",
                )

            append_job_debug_log(
                job_id,
                f"Derived source metadata: platform={source_metadata.get('source_platform') or 'none'} creator={source_metadata.get('source_creator_name') or 'none'} handle={source_metadata.get('source_creator_handle') or 'none'} channel={source_metadata.get('source_channel_name') or 'none'} profile={source_metadata.get('source_profile_url') or 'none'}",
                debug_last_step="source_metadata_derived",
                debug_data=source_metadata,
            )

            assert_job_claim_is_current(job_id, claim_lock_token, "before_phone_submit")
            evidence = prepare_submission_evidence(evidence)
            submission_platform = switch_job_platform_for_analysis(
                job_id,
                platform,
                effective_analysis_platform,
                linked_recipe_used,
            )

            confirmation_debug = build_confirmation_debug_payload(
                job,
                platform,
                confirmation_source_evidence,
                source_metadata,
                investigation_result,
            )
            investigation_result.setdefault("debug", {}).update(confirmation_debug)

            friendly_outreach_handoff = maybe_prepare_friendly_outreach_handoff(
                job_id=job_id,
                target_url=evidence.get("effective_page_url") or target_url,
                platform=platform,
                evidence=evidence,
                source_metadata=source_metadata,
                investigation_result=investigation_result,
                confirmation_context=confirmation_context,
            )
            if friendly_outreach_handoff:
                investigation_result.setdefault("debug", {}).update({
                    "friendly_outreach_needed": True,
                    "friendly_outreach_handoff": friendly_outreach_handoff,
                })

            if confirmation_debug.get("contamination_confirmation_recommended"):
                append_job_debug_log(
                    job_id,
                    (
                        f"Selective confirmation recommended. score={confirmation_debug.get('contamination_confirmation_score')} "
                        f"reason={confirmation_debug.get('contamination_confirmation_reason') or 'none'} "
                        f"collector_profile={confirmation_debug.get('collector_profile_id') or 'none'}"
                    ),
                    debug_last_step="confirmation_recommended",
                    debug_data=confirmation_debug,
                )
                try:
                    maybe_create_confirmation_job(
                        job=job,
                        job_id=job_id,
                        recipe_id=recipe_id,
                        target_url=target_url,
                        platform=platform,
                        collector_identity=collector_identity,
                        evidence=confirmation_source_evidence,
                        source_metadata=source_metadata,
                        linked_recipe_used=linked_recipe_used,
                        effective_analysis_platform=effective_analysis_platform,
                        confirmation_debug=confirmation_debug,
                    )
                except Exception as confirmation_err:
                    append_job_debug_log(
                        job_id,
                        f"Failed to queue confirmation BotJob: {type(confirmation_err).__name__}: {confirmation_err}",
                        debug_last_step="confirmation_job_create_failed",
                    )
            elif confirmation_debug.get("confirmation_job"):
                append_job_debug_log(
                    job_id,
                    (
                        f"Confirmation collector run active. parent_job_id={confirmation_debug.get('confirmation_parent_job_id') or 'none'} "
                        f"shared_lines={confirmation_debug.get('confirmation_comparison_shared_line_count', 0)}"
                    ),
                    debug_last_step="confirmation_collecting",
                    debug_data=confirmation_debug,
                )
                finalize_confirmation_job(
                    job_id=job_id,
                    recipe_id=recipe_id,
                    confirmation_context=confirmation_context,
                    collector_identity=collector_identity,
                    evidence=evidence,
                    source_metadata=source_metadata,
                    linked_recipe_used=linked_recipe_used,
                    effective_analysis_platform=effective_analysis_platform,
                    primary_screenshot_url=screenshot_url or "",
                    description_screenshot_url=description_screenshot_url or "",
                )
                return

            phone_debug_data = {
                "platform": submission_platform,
                "original_platform": original_platform or platform,
                "collection_method": evidence["collection_method"],
                "execution_actor": "phone",
                "assigned_device": "phone_auto_queue",
                "source_device": DEVICE_NAME,
                "execution_device": "android_phone_worker",
                "execution_path": "phone",
                "runner": "phone",
                "effective_page_url": evidence["effective_page_url"],
                "analysis_platform_hint": investigation_result.get("analysis_platform_hint") or effective_analysis_platform,
                "effective_analysis_platform": effective_analysis_platform,
                "tiktok_external_site_url": tiktok_external_site_url,
                "linked_recipe_used": linked_recipe_used,
                "explicit_recipe_link": linked_recipe_url,
                **confirmation_debug,
                **build_investigation_debug_data(investigation_result),
                **source_metadata,
                "phone_worker": {
                    "bundle_dir": evidence["bundle_dir"],
                    "primary_screenshot_path": evidence["primary_screenshot_path"],
                    "description_screenshot_path": evidence["description_screenshot_path"],
                    "worker_stdout": evidence["worker_stdout"],
                    "worker_stderr": evidence["worker_stderr"],
                },
            }

            investigation_history_write = persist_investigation_history_if_needed(
                job_id=job_id,
                recipe_id=recipe_id,
                target_url=evidence.get("effective_page_url") or target_url,
                platform=platform,
                original_platform=original_platform or platform,
                collection_method=evidence.get("collection_method") or "phone_worker_collector",
                evidence=evidence,
                source_metadata=source_metadata,
                investigation_result=investigation_result,
                primary_screenshot_url=screenshot_url or "",
                description_screenshot_url=description_screenshot_url or "",
                visual_recipe_image_url=evidence.get("page_image_url") or screenshot_url or "",
                effective_analysis_platform=effective_analysis_platform,
            )
            history_run_id = str(investigation_history_write.get("run_id") or "")
            if history_run_id:
                append_job_debug_log(
                    job_id,
                    f"Investigation history recorded. run_id={history_run_id} breadcrumbs={investigation_history_write.get('breadcrumbs_written')} candidates={investigation_history_write.get('candidates_written')}",
                    debug_last_step="investigation_history_recorded",
                    debug_data={"investigation_history_run_id": history_run_id},
                )
                if recipe_id and not confirmation_context.get("is_confirmation_job"):
                    update_recipe_debug(recipe_id, debug_log_append=f"Investigation history recorded for BotJob {job_id}: {history_run_id}")
            else:
                append_job_debug_log(
                    job_id,
                    f"Investigation history write result. writer={investigation_history_write.get('writer_version') or INVESTIGATION_HISTORY_WRITER_VERSION} ok={bool(investigation_history_write.get('ok'))} skipped={bool(investigation_history_write.get('skipped'))} reason={investigation_history_write.get('reason') or 'unknown'} mode={(investigation_result or {}).get('mode') or 'unknown'}",
                    debug_last_step="investigation_history_skipped",
                    debug_data={"investigation_history_write": investigation_history_write},
                )
                if recipe_id and not confirmation_context.get("is_confirmation_job"):
                    update_recipe_debug(
                        recipe_id,
                        debug_log_append=(
                            f"Investigation history not recorded for BotJob {job_id}: "
                            f"{investigation_history_write.get('reason') or 'unknown'} "
                            f"(writer={investigation_history_write.get('writer_version') or INVESTIGATION_HISTORY_WRITER_VERSION})"
                        ),
                    )


            if friendly_outreach_handoff:
                finalized_friendly_outreach = finalize_friendly_outreach_handoff(
                    job_id=job_id,
                    recipe_id=recipe_id,
                    handoff=friendly_outreach_handoff,
                    investigation_history_write=investigation_history_write,
                    confirmation_context=confirmation_context,
                )
                investigation_result.setdefault("debug", {}).update({
                    "friendly_outreach_needed": True,
                    "friendly_outreach_handoff": finalized_friendly_outreach,
                })

            submit_result = submit_bot_evidence(
                job_id=job_id,
                recipe_id=recipe_id,
                target_url=evidence.get("effective_page_url") or target_url,
                screenshot_url=screenshot_url or "",
                primary_screenshot_url=screenshot_url or "",
                description_screenshot_url=description_screenshot_url or "",
                raw_page_text=evidence["raw_page_text"],
                page_title=evidence["page_title"],
                media_type_guess=evidence["media_type_guess"] or "",
                page_html=evidence["page_html"] or "",
                expanded_caption_text=evidence["expanded_caption_text"],
                transcript_text=evidence.get("transcript_text") or "",
                meta_description=evidence["meta_description"] or "",
                is_video=bool(evidence["is_video"]),
                video_url=evidence["video_url"] or "",
                visible_text_before_expand=evidence["visible_text_before_expand"],
                visible_text_after_expand=evidence["visible_text_after_expand"],
                expand_attempted=bool(evidence["expand_attempted"]),
                expand_success=bool(evidence["expand_success"]),
                expand_method=evidence["expand_method"] or "",
                is_youtube_shorts=bool(evidence["is_youtube_shorts"]),
                caption_before_len=evidence["caption_before_len"],
                caption_after_len=evidence["caption_after_len"],
                execution_actor="phone",
                assigned_device="phone_auto_queue",
                execution_path="phone",
                source_device=DEVICE_NAME,
                runner="phone",
                device="android_phone_worker",
                execution_device="android_phone_worker",
                controller_device=DEVICE_NAME,
                client_device=DEVICE_NAME,
                source_platform=source_metadata.get("source_platform") or "",
                source_creator_name=source_metadata.get("source_creator_name") or "",
                source_creator_handle=source_metadata.get("source_creator_handle") or "",
                source_channel_name=source_metadata.get("source_channel_name") or "",
                source_channel_key=source_metadata.get("source_channel_key") or "",
                source_profile_url=source_metadata.get("source_profile_url") or "",
                source_page_domain=source_metadata.get("source_page_domain") or "",
                creator_group_key=source_metadata.get("creator_group_key") or "",
                source_avatar_url=source_metadata.get("source_avatar_url") or "",
                visual_recipe_image_url=evidence.get("page_image_url") or "",
                page_image_url=evidence.get("page_image_url") or "",
                debug_data=sanitize_debug_data_for_submit(phone_debug_data),
            )

            append_job_debug_log(
                job_id,
                f"Phone evidence submitted to Base44. status={submit_result.get('status')}",
                debug_status="evidence_submitted",
                debug_last_step="evidence_submitted",
            )

            if recipe_id and not confirmation_context.get("is_confirmation_job"):
                update_recipe_debug(recipe_id, debug_log_append=f"Phone evidence submitted for BotJob {job_id}")

            if submission_platform != platform:
                await restore_job_platform_after_analysis(job_id, submission_platform, platform)

            print("SUBMIT_OK =", submit_result.get("ok"))
            print("SUBMIT_STATUS =", submit_result.get("status"))
            print("JOB_ID =", submit_result.get("job_id"))
            print("PLATFORM =", submission_platform)
            print("JOB_TYPE =", submit_result.get("job_type"))
            print("IS_VIDEO =", evidence["is_video"])
            print("VIDEO_URL =", evidence["video_url"])
            print("LINKED_RECIPE_USED =", linked_recipe_used)
            print("PHONE_WORKER = True")
            return

        append_job_debug_log(
            job_id,
            f"Launching {platform} persistent browser context",
            debug_last_step="launching_browser",
        )

        original_screenshot_path = screenshot_path
        primary_submission_screenshot_path = original_screenshot_path
        secondary_submission_screenshot_path = None
        linked_recipe_url = ""
        linked_recipe_used = False
        youtube_watch_fallback_url = None
        effective_analysis_platform = platform
        source_metadata = enrich_source_metadata({}, platform, target_url)
        linked_source_metadata = {}
        instagram_discovery_attempted = False
        instagram_discovery_reasons = []
        instagram_discovery_profile_url = ""
        instagram_external_site_url = ""
        instagram_hint_tokens = []
        instagram_mentions = []
        instagram_owner_hint = {"handle": "", "display_name": ""}
        investigation_result = make_investigation_result()

        async with async_playwright() as p:
            context = await p.chromium.launch_persistent_context(
                user_data_dir=str(profile_dir),
                headless=HEADLESS,
                viewport={"width": 1440, "height": 1000},
                args=["--start-maximized"],
            )

            try:
                page = context.pages[0] if context.pages else await context.new_page()

                append_job_debug_log(
                    job_id,
                    "Opening target URL",
                    debug_last_step="opening_target_url",
                )
                await page.goto(target_url, wait_until="domcontentloaded", timeout=45000)

                append_job_debug_log(
                    job_id,
                    f"Opened URL successfully: {page.url}",
                    debug_last_step="url_opened",
                )

                youtube_watch_fallback_used = False
                effective_target_url = page.url or target_url

                if platform == "youtube" and is_youtube_shorts_url(page.url or target_url):
                    watch_url = build_youtube_watch_url(page.url or target_url)

                    if watch_url and watch_url != (page.url or ""):
                        append_job_debug_log(
                            job_id,
                            f"YouTube Shorts detected. Switching to watch page for richer description extraction: {watch_url}",
                            debug_last_step="youtube_watch_fallback_start",
                        )

                        try:
                            await page.goto(watch_url, wait_until="domcontentloaded", timeout=45000)
                            await page.wait_for_timeout(3500)

                            youtube_watch_fallback_used = True
                            youtube_watch_fallback_url = page.url or watch_url
                            effective_target_url = page.url or watch_url

                            append_job_debug_log(
                                job_id,
                                f"YouTube watch fallback opened successfully: {effective_target_url}",
                                debug_last_step="youtube_watch_fallback_opened",
                            )
                        except Exception as watch_err:
                            append_job_debug_log(
                                job_id,
                                f"YouTube watch fallback failed: {type(watch_err).__name__}: {watch_err}",
                                debug_last_step="youtube_watch_fallback_failed",
                            )
                            effective_target_url = page.url or target_url

                evidence = await collect_evidence(
                    page,
                    platform,
                    effective_target_url,
                    original_target_url=target_url,
                    youtube_watch_fallback_used=youtube_watch_fallback_used,
                )

                source_metadata = await extract_source_metadata(
                    page,
                    platform,
                    effective_target_url,
                    page_title=evidence.get("page_title") or "",
                    page_html=evidence.get("page_html") or "",
                    page_image_url=evidence.get("page_image_url") or "",
                )

                if platform == "instagram":
                    instagram_source_text = combine_text_blocks([
                        evidence.get("visible_text_after_expand") or "",
                        evidence.get("expanded_caption_text") or "",
                        evidence.get("raw_page_text") or "",
                        evidence.get("meta_description") or "",
                    ])
                    instagram_owner_hint = extract_instagram_owner_hint(instagram_source_text)
                    owner_hint_handle = source_safe_handle(instagram_owner_hint.get("handle") or "")
                    if owner_hint_handle:
                        owner_hint_name = clean_instagram_display_name(instagram_owner_hint.get("display_name") or "") or source_safe_text(owner_hint_handle.lstrip('@'))
                        current_handle = source_safe_handle(source_metadata.get("source_creator_handle") or "")
                        if current_handle.lower() != owner_hint_handle.lower() or is_instagram_source_metadata_suspicious(source_metadata):
                            source_metadata = enrich_source_metadata({
                                **source_metadata,
                                "source_creator_name": owner_hint_name or source_metadata.get("source_creator_name") or source_metadata.get("source_channel_name") or owner_hint_handle.lstrip('@'),
                                "source_channel_name": owner_hint_name or source_metadata.get("source_channel_name") or source_metadata.get("source_creator_name") or owner_hint_handle.lstrip('@'),
                                "source_creator_handle": owner_hint_handle,
                                "source_profile_url": normalize_instagram_profile_root(f"https://www.instagram.com/{owner_hint_handle.lstrip('@')}/"),
                                "source_page_domain": "instagram.com",
                            }, "instagram", target_url)

                append_job_debug_log(
                    job_id,
                    (
                        f"Evidence collected. platform={platform} "
                        f"effective_page_url={evidence['effective_page_url']} "
                        f"youtube_watch_fallback_used={evidence['youtube_watch_fallback_used']} "
                        f"current_page_is_youtube_shorts={evidence['current_page_is_youtube_shorts']} "
                        f"media_type_guess={evidence['media_type_guess']} "
                        f"is_video={evidence['is_video']} "
                        f"video_url_present={bool(evidence['video_url'])} "
                        f"caption_expanded={evidence['caption_expanded']} "
                        f"expand_method={evidence['expand_method']} "
                        f"expand_success={evidence['expand_success']} "
                        f"shorts={evidence['is_youtube_shorts']} "
                        f"raw_text_len={len(evidence['raw_page_text'])} "
                        f"visible_text_len={evidence['visible_page_text_len']} "
                        f"structured_html_text_len={evidence['structured_html_text_len']} "
                        f"caption_len={len(evidence['expanded_caption_text'])} "
                        f"caption_before_len={evidence['caption_before_len']} "
                        f"caption_after_len={evidence['caption_after_len']} "
                        f"caption_before_lines={evidence['caption_before_lines']} "
                        f"caption_after_lines={evidence['caption_after_lines']} "
                        f"meta_len={len(evidence['meta_description'])} "
                        f"html_len={len(evidence['page_html'])} "
                        f"html_raw_len={evidence['page_html_raw_len']} "
                        f"html_skipped={evidence['page_html_was_skipped']}"
                    ),
                    debug_last_step="evidence_collected",
                    debug_data={
                        "platform": platform,
                        "collection_method": "pi_browser_collector",
                        "execution_actor": "pi",
                        "assigned_device": DEVICE_NAME,
                        "source_device": DEVICE_NAME,
                        "execution_device": DEVICE_NAME,
                        "execution_path": "pi",
                        "runner": "pi",
                        "effective_page_url": evidence["effective_page_url"],
                        "youtube_watch_fallback_used": evidence["youtube_watch_fallback_used"],
                        "youtube_watch_fallback_url": youtube_watch_fallback_url,
                        "media_type_guess": evidence["media_type_guess"],
                        "is_video": evidence["is_video"],
                        "video_url_present": bool(evidence["video_url"]),
                        "caption_expanded": evidence["caption_expanded"],
                        "expand_method": evidence["expand_method"],
                        "expand_success": evidence["expand_success"],
                        "is_youtube_shorts": evidence["is_youtube_shorts"],
                        "current_page_is_youtube_shorts": evidence["current_page_is_youtube_shorts"],
                        "raw_text_len": len(evidence["raw_page_text"]),
                        "visible_page_text_len": evidence["visible_page_text_len"],
                        "structured_html_text_len": evidence["structured_html_text_len"],
                        "caption_len": len(evidence["expanded_caption_text"]),
                        "caption_before_len": evidence["caption_before_len"],
                        "caption_after_len": evidence["caption_after_len"],
                        "caption_before_lines": evidence["caption_before_lines"],
                        "caption_after_lines": evidence["caption_after_lines"],
                        "meta_len": len(evidence["meta_description"]),
                        "html_len": len(evidence["page_html"]),
                        "html_raw_len": evidence["page_html_raw_len"],
                        "html_skipped": evidence["page_html_was_skipped"],
                        "page_title": evidence["page_title"],
                        "page_image_url": evidence.get("page_image_url") or "",
                    },
                )

                await page.evaluate("window.scrollTo(0, 0)")
                await page.wait_for_timeout(400)
                await page.screenshot(path=original_screenshot_path, full_page=False)
                append_job_debug_log(
                    job_id,
                    f"Screenshot saved locally: {original_screenshot_path}",
                    debug_last_step="screenshot_saved",
                )

                assert_job_claim_is_current(job_id, claim_lock_token, "before_inline_investigation")

                if platform == "youtube" and not evidence.get("current_page_is_youtube_shorts"):
                    transcript_info = await extract_youtube_transcript_text(page)
                    evidence["transcript_text"] = transcript_info.get("text") or ""
                    evidence["transcript_attempted"] = bool(transcript_info.get("attempted"))
                    evidence["transcript_success"] = bool(transcript_info.get("success"))
                    evidence["transcript_method"] = transcript_info.get("method") or ""
                    evidence["transcript_line_count"] = int(transcript_info.get("line_count") or 0)

                    if evidence["transcript_attempted"] or evidence["transcript_text"]:
                        append_job_debug_log(
                            job_id,
                            (
                                f"YouTube transcript extraction: "
                                f"attempted={evidence['transcript_attempted']} "
                                f"success={evidence['transcript_success']} "
                                f"method={evidence['transcript_method'] or 'none'} "
                                f"lines={evidence['transcript_line_count']} "
                                f"len={len(evidence['transcript_text'])}"
                            ),
                            debug_last_step="youtube_transcript_extracted",
                            debug_data={
                                "transcript_text_len": len(evidence["transcript_text"]),
                                "transcript_attempted": evidence["transcript_attempted"],
                                "transcript_success": evidence["transcript_success"],
                                "transcript_method": evidence["transcript_method"],
                                "transcript_line_count": evidence["transcript_line_count"],
                            },
                        )

                investigation_result = await run_inline_investigation(
                    job_id=job_id,
                    platform=platform,
                    target_url=target_url,
                    evidence=evidence,
                    source_metadata=source_metadata,
                    page=page,
                    context=context,
                    original_submission_screenshot_path=original_screenshot_path,
                    linked_submission_screenshot_path=linked_screenshot_path,
                    collection_method="browser",
                )
                source_metadata = merge_source_metadata(
                    investigation_result.get("source_metadata_updates") or {},
                    source_metadata,
                    platform,
                    target_url,
                )
                if investigation_result.get("winner_source_metadata"):
                    source_metadata = merge_source_metadata(
                        source_metadata,
                        investigation_result.get("winner_source_metadata") or {},
                        platform,
                        target_url,
                    )
                if investigation_result.get("merged_evidence"):
                    evidence = investigation_result.get("merged_evidence") or evidence

                linked_recipe_url = investigation_result.get("explicit_recipe_link") or investigation_result.get("winner_url") or linked_recipe_url
                linked_recipe_used = bool(investigation_result.get("linked_recipe_used"))
                effective_analysis_platform = investigation_result.get("effective_analysis_platform") or effective_analysis_platform
                primary_submission_screenshot_path = investigation_result.get("primary_submission_screenshot_path") or primary_submission_screenshot_path
                secondary_submission_screenshot_path = investigation_result.get("secondary_submission_screenshot_path") or secondary_submission_screenshot_path

                investigation_debug = investigation_result.get("debug") or {}
                if investigation_result.get("mode") == "instagram.external_site":
                    instagram_discovery_attempted = bool(investigation_result.get("attempted"))
                    instagram_discovery_reasons = list(investigation_result.get("reasons") or [])
                    instagram_discovery_profile_url = str(investigation_debug.get("instagram_profile_url") or "")
                    instagram_external_site_url = str(investigation_result.get("winner_url") or investigation_debug.get("instagram_external_site_url") or "")
                    instagram_hint_tokens = list(investigation_debug.get("instagram_hint_tokens") or [])
                    instagram_mentions = list(investigation_debug.get("instagram_mentions") or [])
                    instagram_owner_hint = dict(investigation_debug.get("instagram_owner_hint") or {"handle": "", "display_name": ""})

                log_investigation_result(job_id, investigation_result)

            finally:
                pass

        if linked_recipe_used and effective_analysis_platform != platform:
            append_job_debug_log(
                job_id,
                f"Retaining BotJob platform as {platform}; linked recipe page analysis hint={effective_analysis_platform}.",
                debug_last_step="platform_retained_for_linked_recipe",
            )

        source_metadata = enrich_source_metadata(source_metadata, platform, evidence.get("effective_page_url") or target_url)

        materialized_avatar_url = await materialize_source_avatar(context, job_id, source_metadata, current_platform=platform)
        if materialized_avatar_url:
            source_metadata['source_avatar_url'] = materialized_avatar_url
            append_job_debug_log(
                job_id,
                f"Source avatar uploaded to Base44: {materialized_avatar_url}",
                debug_last_step="source_avatar_uploaded",
            )

        try:
            await context.close()
        except Exception:
            pass

        append_job_debug_log(
            job_id,
            f"Derived source metadata: platform={source_metadata.get('source_platform') or 'none'} creator={source_metadata.get('source_creator_name') or 'none'} handle={source_metadata.get('source_creator_handle') or 'none'} channel={source_metadata.get('source_channel_name') or 'none'} profile={source_metadata.get('source_profile_url') or 'none'} channel_key={source_metadata.get('source_channel_key') or 'none'} creator_group_key={source_metadata.get('creator_group_key') or 'none'}",
            debug_last_step="source_metadata_derived",
            debug_data=source_metadata,
        )

        assert_job_claim_is_current(job_id, claim_lock_token, "before_primary_screenshot_upload")
        primary_upload_result = upload_bot_screenshot(
            job_id=job_id,
            image_path=primary_submission_screenshot_path,
            debug_last_step="screenshot_uploaded",
        )
        primary_screenshot_url = primary_upload_result.get("url", "")

        append_job_debug_log(
            job_id,
            f"Primary screenshot uploaded to Base44: {primary_screenshot_url}",
            debug_last_step="screenshot_uploaded",
        )

        description_screenshot_url = ""
        if secondary_submission_screenshot_path and Path(secondary_submission_screenshot_path).exists():
            secondary_upload_result = upload_bot_screenshot(
                job_id=job_id,
                image_path=secondary_submission_screenshot_path,
                debug_last_step="description_screenshot_uploaded",
            )
            description_screenshot_url = secondary_upload_result.get("url", "")

            append_job_debug_log(
                job_id,
                f"Secondary screenshot uploaded to Base44: {description_screenshot_url}",
                debug_last_step="description_screenshot_uploaded",
            )

        if recipe_id and not confirmation_context.get("is_confirmation_job"):
            update_recipe_debug(
                recipe_id,
                debug_log_append=f"Screenshot uploaded for BotJob {job_id}",
                debug_screenshot_url=primary_screenshot_url,
            )

        assert_job_claim_is_current(job_id, claim_lock_token, "before_browser_submit")
        confirmation_source_evidence = dict(evidence)
        contamination_guard = {"applied": False, "reason": "not_instagram"}
        if platform == "instagram":
            evidence, contamination_guard = maybe_apply_instagram_contamination_guard(
                evidence,
                source_metadata,
                linked_recipe_used=linked_recipe_used,
                effective_analysis_platform=effective_analysis_platform,
            )
            if contamination_guard.get("applied"):
                append_job_debug_log(
                    job_id,
                    (
                        f"Instagram contamination guard applied. strategy={contamination_guard.get('reason')} "
                        f"score={contamination_guard.get('score', 0)} other_handles={contamination_guard.get('other_handle_count', 0)} "
                        f"audio_lines={contamination_guard.get('audio_line_count', 0)}"
                    ),
                    debug_last_step="instagram_contamination_guard_applied",
                    debug_data={
                        "instagram_contamination_guard": contamination_guard,
                    },
                )
        evidence = prepare_submission_evidence(evidence)
        submission_platform = switch_job_platform_for_analysis(
            job_id,
            platform,
            effective_analysis_platform,
            linked_recipe_used,
        )
        confirmation_debug = build_confirmation_debug_payload(
            job,
            platform,
            confirmation_source_evidence,
            source_metadata,
            investigation_result,
        )
        if contamination_guard.get("applied"):
            confirmation_debug["instagram_contamination_guard"] = contamination_guard
        investigation_result.setdefault("debug", {}).update(confirmation_debug)

        if (
            platform == "instagram"
            and not confirmation_debug.get("contamination_confirmation_recommended")
            and confirmation_debug.get("contamination_confirmation_score", 0) > 0
        ):
            append_job_debug_log(
                job_id,
                (
                    f"Contamination signals observed but confirmation not queued. score={confirmation_debug.get('contamination_confirmation_score', 0)} "
                    f"reasons={','.join(confirmation_debug.get('contamination_confirmation_reasons') or []) or 'none'}"
                ),
                debug_last_step="confirmation_not_recommended",
                debug_data={
                    "confirmation_decision": "not_recommended",
                    "confirmation_debug": confirmation_debug,
                },
            )

        friendly_outreach_handoff = maybe_prepare_friendly_outreach_handoff(
            job_id=job_id,
            target_url=evidence.get("effective_page_url") or target_url,
            platform=platform,
            evidence=evidence,
            source_metadata=source_metadata,
            investigation_result=investigation_result,
            confirmation_context=confirmation_context,
        )
        if friendly_outreach_handoff:
            investigation_result.setdefault("debug", {}).update({
                "friendly_outreach_needed": True,
                "friendly_outreach_handoff": friendly_outreach_handoff,
            })

        if confirmation_debug.get("contamination_confirmation_recommended"):
            append_job_debug_log(
                job_id,
                (
                    f"Selective confirmation recommended. score={confirmation_debug.get('contamination_confirmation_score')} "
                    f"reason={confirmation_debug.get('contamination_confirmation_reason') or 'none'} "
                    f"collector_profile={confirmation_debug.get('collector_profile_id') or 'none'}"
                ),
                debug_last_step="confirmation_recommended",
                debug_data=confirmation_debug,
            )
        elif confirmation_debug.get("confirmation_job"):
            append_job_debug_log(
                job_id,
                (
                    f"Confirmation collector run active. parent_job_id={confirmation_debug.get('confirmation_parent_job_id') or 'none'} "
                    f"shared_lines={confirmation_debug.get('confirmation_comparison_shared_line_count', 0)}"
                ),
                debug_last_step="confirmation_collecting",
                debug_data=confirmation_debug,
            )

        submission_visual_url = choose_first_non_empty(
            evidence.get("page_image_url"),
            primary_screenshot_url,
        )

        if submission_visual_url != primary_screenshot_url:
            append_job_debug_log(
                job_id,
                f"Captured visual recipe image candidate for post-analysis apply: {submission_visual_url}",
                debug_last_step="visual_image_selected",
            )

        if len(evidence["raw_page_text"]) >= RAW_PAGE_TEXT_SUBMIT_MAX:
            append_job_debug_log(
                job_id,
                f"raw_page_text trimmed to {len(evidence['raw_page_text'])} bytes before submit to avoid Base44 field-limit failures.",
                debug_last_step="raw_text_trimmed",
            )

        investigation_history_write = persist_investigation_history_if_needed(
            job_id=job_id,
            recipe_id=recipe_id,
            target_url=evidence.get("effective_page_url") or target_url,
            platform=platform,
            original_platform=original_platform or platform,
            collection_method="pi_browser_collector",
            evidence=evidence,
            source_metadata=source_metadata,
            investigation_result=investigation_result,
            primary_screenshot_url=primary_screenshot_url,
            description_screenshot_url=description_screenshot_url or "",
            visual_recipe_image_url=submission_visual_url,
            effective_analysis_platform=effective_analysis_platform,
        )
        history_run_id = str(investigation_history_write.get("run_id") or "")
        if history_run_id:
            append_job_debug_log(
                job_id,
                f"Investigation history recorded. run_id={history_run_id} breadcrumbs={investigation_history_write.get('breadcrumbs_written')} candidates={investigation_history_write.get('candidates_written')}",
                debug_last_step="investigation_history_recorded",
                debug_data={"investigation_history_run_id": history_run_id},
            )
            if recipe_id and not confirmation_context.get("is_confirmation_job"):
                update_recipe_debug(recipe_id, debug_log_append=f"Investigation history recorded for BotJob {job_id}: {history_run_id}")
        else:
            append_job_debug_log(
                job_id,
                f"Investigation history write result. writer={investigation_history_write.get('writer_version') or INVESTIGATION_HISTORY_WRITER_VERSION} ok={bool(investigation_history_write.get('ok'))} skipped={bool(investigation_history_write.get('skipped'))} reason={investigation_history_write.get('reason') or 'unknown'} mode={(investigation_result or {}).get('mode') or 'unknown'}",
                debug_last_step="investigation_history_skipped",
                debug_data={"investigation_history_write": investigation_history_write},
            )
            if recipe_id and not confirmation_context.get("is_confirmation_job"):
                update_recipe_debug(
                    recipe_id,
                    debug_log_append=(
                        f"Investigation history not recorded for BotJob {job_id}: "
                        f"{investigation_history_write.get('reason') or 'unknown'} "
                        f"(writer={investigation_history_write.get('writer_version') or INVESTIGATION_HISTORY_WRITER_VERSION})"
                    ),
                )

        if friendly_outreach_handoff:
            finalized_friendly_outreach = finalize_friendly_outreach_handoff(
                job_id=job_id,
                recipe_id=recipe_id,
                handoff=friendly_outreach_handoff,
                investigation_history_write=investigation_history_write,
                confirmation_context=confirmation_context,
            )
            investigation_result.setdefault("debug", {}).update({
                "friendly_outreach_needed": True,
                "friendly_outreach_handoff": finalized_friendly_outreach,
            })

        if confirmation_debug.get("contamination_confirmation_recommended"):
            try:
                maybe_create_confirmation_job(
                    job=job,
                    job_id=job_id,
                    recipe_id=recipe_id,
                    target_url=target_url,
                    platform=platform,
                    collector_identity=collector_identity,
                    evidence=evidence,
                    source_metadata=source_metadata,
                    linked_recipe_used=linked_recipe_used,
                    effective_analysis_platform=effective_analysis_platform,
                    confirmation_debug=confirmation_debug,
                )
            except Exception as confirmation_err:
                append_job_debug_log(
                    job_id,
                    f"Failed to queue confirmation BotJob: {type(confirmation_err).__name__}: {confirmation_err}",
                    debug_last_step="confirmation_job_create_failed",
                )
        elif confirmation_debug.get("confirmation_job"):
            finalize_confirmation_job(
                job_id=job_id,
                recipe_id=recipe_id,
                confirmation_context=confirmation_context,
                collector_identity=collector_identity,
                evidence=evidence,
                source_metadata=source_metadata,
                linked_recipe_used=linked_recipe_used,
                effective_analysis_platform=effective_analysis_platform,
                primary_screenshot_url=primary_screenshot_url or "",
                description_screenshot_url=description_screenshot_url or "",
            )
            return

        submit_result = submit_bot_evidence(
            job_id=job_id,
            recipe_id=recipe_id,
            target_url=evidence.get("effective_page_url") or target_url,
            screenshot_url=submission_visual_url,
            primary_screenshot_url=primary_screenshot_url,
            description_screenshot_url=description_screenshot_url or "",
            raw_page_text=evidence["raw_page_text"],
            page_title=evidence["page_title"],
            media_type_guess=evidence["media_type_guess"],
            page_html=evidence["page_html"],
            expanded_caption_text=evidence["expanded_caption_text"],
            transcript_text=evidence.get("transcript_text") or "",
            meta_description=evidence["meta_description"],
            is_video=evidence["is_video"],
            video_url=evidence["video_url"],
            visible_text_before_expand=evidence.get("visible_text_before_expand", ""),
            visible_text_after_expand=evidence.get("visible_text_after_expand", ""),
            expand_attempted=True,
            expand_success=bool(evidence["expand_success"]),
            expand_method=evidence["expand_method"],
            is_youtube_shorts=bool(evidence["is_youtube_shorts"]),
            caption_before_len=evidence["caption_before_len"],
            caption_after_len=evidence["caption_after_len"],
            execution_actor="pi",
            assigned_device=DEVICE_NAME,
            execution_path="pi",
            source_device=DEVICE_NAME,
            runner="pi",
            device=DEVICE_NAME,
            execution_device=DEVICE_NAME,
            controller_device=DEVICE_NAME,
            client_device=DEVICE_NAME,
            source_platform=source_metadata.get("source_platform") or "",
            source_creator_name=source_metadata.get("source_creator_name") or "",
            source_creator_handle=source_metadata.get("source_creator_handle") or "",
            source_channel_name=source_metadata.get("source_channel_name") or "",
            source_channel_key=source_metadata.get("source_channel_key") or "",
            source_profile_url=source_metadata.get("source_profile_url") or "",
            source_page_domain=source_metadata.get("source_page_domain") or "",
            creator_group_key=source_metadata.get("creator_group_key") or "",
            source_avatar_url=source_metadata.get("source_avatar_url") or "",
            visual_recipe_image_url=submission_visual_url,
            page_image_url=evidence.get("page_image_url") or "",
            debug_data=sanitize_debug_data_for_submit({
                "platform": submission_platform,
                "analysis_platform_hint": (
                    investigation_result.get("analysis_platform_hint")
                    if linked_recipe_used
                    else None
                ),
                "original_platform": original_platform or platform,
                "collection_method": "pi_browser_collector",
                "execution_actor": "pi",
                "assigned_device": DEVICE_NAME,
                "source_device": DEVICE_NAME,
                "execution_device": DEVICE_NAME,
                "execution_path": "pi",
                "runner": "pi",
                "effective_page_url": evidence["effective_page_url"],
                "youtube_watch_fallback_used": evidence["youtube_watch_fallback_used"],
                "youtube_watch_fallback_url": youtube_watch_fallback_url,
                "caption_expanded": evidence["caption_expanded"],
                "expand_method": evidence["expand_method"],
                "expand_success": evidence["expand_success"],
                "is_youtube_shorts": evidence["is_youtube_shorts"],
                "current_page_is_youtube_shorts": evidence["current_page_is_youtube_shorts"],
                "caption_before_len": evidence["caption_before_len"],
                "caption_after_len": evidence["caption_after_len"],
                "caption_before_lines": evidence["caption_before_lines"],
                "caption_after_lines": evidence["caption_after_lines"],
                "visible_page_text_len": evidence["visible_page_text_len"],
                "structured_html_text_len": evidence["structured_html_text_len"],
                "visual_recipe_image_url": submission_visual_url,
                "page_image_url": evidence.get("page_image_url") or "",
                "pi_build": PI_ANALYZER_BUILD,
                **confirmation_debug,
                **build_investigation_debug_data(investigation_result),
                **source_metadata,
                "instagram_discovery_attempted": instagram_discovery_attempted,
                "instagram_discovery_reasons": instagram_discovery_reasons,
                "instagram_discovery_profile_url": instagram_discovery_profile_url,
                "instagram_external_site_url": instagram_external_site_url,
                "instagram_hint_tokens": instagram_hint_tokens[:12],
                "instagram_mentions": instagram_mentions[:12],
                "instagram_owner_hint": instagram_owner_hint,
                "local_screenshot_path": primary_submission_screenshot_path,
                "secondary_screenshot_path": secondary_submission_screenshot_path or "",
                "page_html_was_skipped": evidence["page_html_was_skipped"],
                "page_html_raw_len": evidence["page_html_raw_len"],
                "explicit_recipe_link": linked_recipe_url,
                "linked_recipe_used": linked_recipe_used,
            }),
        )

        append_job_debug_log(
            job_id,
            f"Evidence submitted to Base44. status={submit_result.get('status')}",
            debug_status="evidence_submitted",
            debug_last_step="evidence_submitted",
        )

        if recipe_id and not confirmation_context.get("is_confirmation_job"):
            update_recipe_debug(recipe_id, debug_log_append=f"Evidence submitted for BotJob {job_id}")

        if linked_recipe_used and recipe_id and not confirmation_context.get("is_confirmation_job") and submission_visual_url and submission_visual_url != primary_screenshot_url:
            await apply_visual_recipe_image_after_analysis(
                job_id=job_id,
                recipe_id=recipe_id,
                visual_image_url=submission_visual_url,
                primary_screenshot_url=primary_screenshot_url,
            )

        if submission_platform != platform:
            await restore_job_platform_after_analysis(job_id, submission_platform, platform)

        print("SUBMIT_OK =", submit_result.get("ok"))
        print("SUBMIT_STATUS =", submit_result.get("status"))
        print("JOB_ID =", submit_result.get("job_id"))
        print("PLATFORM =", submission_platform)
        print("JOB_TYPE =", submit_result.get("job_type"))
        print("IS_VIDEO =", evidence["is_video"])
        print("VIDEO_URL =", evidence["video_url"])
        print("LINKED_RECIPE_USED =", linked_recipe_used)

    except JobLeaseLostError as e:
        error_text = f"{type(e).__name__}: {e}"
        print("LEASE_LOST_JOB_ID =", job_id)
        print("LEASE_LOST =", error_text)
        return

    except PlaywrightTimeoutError as e:
        error_text = f"Timeout: {e}"

        append_job_debug_log(
            job_id,
            error_text,
            debug_status="failed",
            debug_last_step="timeout_error",
        )
        failed = fail_job(job_id, error_text)
        print("FAILED_JOB_ID =", failed["id"])
        print("ERROR =", error_text)

        if recipe_id and not confirmation_context.get("is_confirmation_job"):
            recipe = update_recipe_from_job(
                recipe_id=recipe_id,
                bot_decision="failed",
                bot_result="Processing failed due to timeout",
                error=error_text,
            )
            print("UPDATED_RECIPE_ID =", recipe["id"])
            update_recipe_debug(recipe_id, debug_log_append=f"Timeout failure on BotJob {job_id}: {error_text}")

    except Exception as e:
        error_text = f"{type(e).__name__}: {e}"

        append_job_debug_log(
            job_id,
            error_text,
            debug_status="failed",
            debug_last_step="exception",
        )
        failed = fail_job(job_id, error_text)
        print("FAILED_JOB_ID =", failed["id"])
        print("ERROR =", error_text)

        if recipe_id and not confirmation_context.get("is_confirmation_job"):
            recipe = update_recipe_from_job(
                recipe_id=recipe_id,
                bot_decision="failed",
                bot_result="Processing failed",
                error=error_text,
            )
            print("UPDATED_RECIPE_ID =", recipe["id"])
            update_recipe_debug(recipe_id, debug_log_append=f"Exception on BotJob {job_id}: {error_text}")


if __name__ == "__main__":
    asyncio.run(main())
                                                                                                                                                                                                                                                             