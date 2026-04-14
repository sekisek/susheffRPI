import base64
import json
import os
import socket
import time
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from requests.exceptions import ConnectionError, ReadTimeout, RequestException, Timeout

load_dotenv()

API_KEY = os.getenv("API_KEY", "").strip()
BOT_SECRET = os.getenv("BOT_SECRET", "").strip()
DEVICE_NAME = os.getenv("DEVICE_NAME", "").strip() or socket.gethostname()
UPLOAD_BOT_SCREENSHOT_URL = os.getenv("UPLOAD_BOT_SCREENSHOT_URL", "").strip()
SUBMIT_BOT_EVIDENCE_URL = os.getenv("SUBMIT_BOT_EVIDENCE_URL", "").strip()
HEARTBEAT_URL = os.getenv("HEARTBEAT_URL", "").strip()
CLAIM_NEXT_JOB_URL = os.getenv("CLAIM_NEXT_JOB_URL", "").strip()

APP_ID = os.getenv("BASE44_APP_ID", "").strip()
if not APP_ID:
    raise RuntimeError("Missing BASE44_APP_ID in .env")

ENTITY_BASE_URL = f"https://app.base44.com/api/apps/{APP_ID}/entities"
BOTJOB_URL = f"{ENTITY_BASE_URL}/BotJob"
RECIPE_URL = f"{ENTITY_BASE_URL}/Recipe"
BOTALERT_URL = f"{ENTITY_BASE_URL}/BotAlert"

INVESTIGATION_RUN_URL = f"{ENTITY_BASE_URL}/InvestigationRun"
INVESTIGATION_BREADCRUMB_URL = f"{ENTITY_BASE_URL}/InvestigationBreadcrumb"
INVESTIGATION_CANDIDATE_SNAPSHOT_URL = f"{ENTITY_BASE_URL}/InvestigationCandidateSnapshot"
INVESTIGATION_EVIDENCE_SNAPSHOT_URL = f"{ENTITY_BASE_URL}/InvestigationEvidenceSnapshot"

INVESTIGATION_HISTORY_ENABLED = os.getenv("INVESTIGATION_HISTORY_ENABLED", "true").strip().lower() == "true"
INVESTIGATION_HISTORY_SUPPORTED_MODES = {
    mode.strip().lower()
    for mode in os.getenv("INVESTIGATION_HISTORY_SUPPORTED_MODES", "instagram.external_site,tiktok.external_site").split(",")
    if mode.strip()
}
INVESTIGATION_HISTORY_MAX_BREADCRUMBS = int(os.getenv("INVESTIGATION_HISTORY_MAX_BREADCRUMBS", "40") or "40")
INVESTIGATION_HISTORY_MAX_CANDIDATES = int(os.getenv("INVESTIGATION_HISTORY_MAX_CANDIDATES", "24") or "24")
INVESTIGATION_HISTORY_JSON_MAX_LEN = int(os.getenv("INVESTIGATION_HISTORY_JSON_MAX_LEN", "16000") or "16000")

MAX_EVIDENCE_HISTORY = 10
SUBMIT_RAW_PAGE_TEXT_MAX_LEN = int(os.getenv("BOT_SUBMIT_RAW_PAGE_TEXT_MAX_LEN", "12000") or "12000")
SUBMIT_EXPANDED_CAPTION_MAX_LEN = int(os.getenv("BOT_SUBMIT_EXPANDED_CAPTION_MAX_LEN", "12000") or "12000")
SUBMIT_META_DESCRIPTION_MAX_LEN = int(os.getenv("BOT_SUBMIT_META_DESCRIPTION_MAX_LEN", "4000") or "4000")
SUBMIT_VISIBLE_BEFORE_MAX_LEN = int(os.getenv("BOT_SUBMIT_VISIBLE_BEFORE_MAX_LEN", "8000") or "8000")
SUBMIT_VISIBLE_AFTER_MAX_LEN = int(os.getenv("BOT_SUBMIT_VISIBLE_AFTER_MAX_LEN", "12000") or "12000")
SUBMIT_TRANSCRIPT_MAX_LEN = int(os.getenv("BOT_SUBMIT_TRANSCRIPT_MAX_LEN", "12000") or "12000")
SUBMIT_PAGE_TITLE_MAX_LEN = int(os.getenv("BOT_SUBMIT_PAGE_TITLE_MAX_LEN", "500") or "500")
SUBMIT_VIDEO_URL_MAX_LEN = int(os.getenv("BOT_SUBMIT_VIDEO_URL_MAX_LEN", "1000") or "1000")
SUBMIT_EXPAND_METHOD_MAX_LEN = int(os.getenv("BOT_SUBMIT_EXPAND_METHOD_MAX_LEN", "500") or "500")
SUBMIT_SOURCE_TEXT_MAX_LEN = int(os.getenv("BOT_SUBMIT_SOURCE_TEXT_MAX_LEN", "500") or "500")
SUBMIT_SOURCE_HANDLE_MAX_LEN = int(os.getenv("BOT_SUBMIT_SOURCE_HANDLE_MAX_LEN", "200") or "200")
SUBMIT_SOURCE_KEY_MAX_LEN = int(os.getenv("BOT_SUBMIT_SOURCE_KEY_MAX_LEN", "300") or "300")
SUBMIT_SOURCE_URL_MAX_LEN = int(os.getenv("BOT_SUBMIT_SOURCE_URL_MAX_LEN", "2000") or "2000")
SUBMIT_SOURCE_DOMAIN_MAX_LEN = int(os.getenv("BOT_SUBMIT_SOURCE_DOMAIN_MAX_LEN", "200") or "200")
HTTP_RETRY_ATTEMPTS = int(os.getenv("BASE44_HTTP_RETRY_ATTEMPTS", "4") or "4")
HTTP_RETRY_BACKOFF_SECONDS = float(os.getenv("BASE44_HTTP_RETRY_BACKOFF_SECONDS", "2") or "2")
RETRYABLE_STATUS_CODES = {405, 408, 425, 429, 500, 502, 503, 504}

BOT_API_PATCH_VERSION = os.getenv("BOT_API_PATCH_VERSION", "bot_api-v14-debugdata-guard-2026-04-07").strip() or "bot_api-v14-debugdata-guard-2026-04-07"
INVESTIGATION_HISTORY_WRITER_VERSION = os.getenv("INVESTIGATION_HISTORY_WRITER_VERSION", "history-writer-v13-trace-2026-04-05").strip() or "history-writer-v13-trace-2026-04-05"


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


def get_collector_identity():
    return {
        "collector_node_id": COLLECTOR_NODE_ID,
        "collector_profile_id": COLLECTOR_PROFILE_ID,
        "collector_account_label": COLLECTOR_ACCOUNT_LABEL,
        "collector_platforms": list(COLLECTOR_PLATFORMS),
        "collector_capabilities": list(COLLECTOR_CAPABILITIES),
        "can_claim_default_jobs": bool(CAN_CLAIM_DEFAULT_JOBS),
        "can_claim_confirmation_jobs": bool(CAN_CLAIM_CONFIRMATION_JOBS),
    }



def get_current_collector_identity():
    return get_collector_identity()


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def _checked_json_response(resp):
    if not resp.ok:
        raise RuntimeError(f"Base44 API error {resp.status_code}: {resp.text[:2000]}")
    return resp.json()


def _response_text_preview(resp):
    try:
        return (resp.text or "")[:2000]
    except Exception:
        return ""


def _is_retryable_http_response(resp):
    if resp is None:
        return False

    if resp.status_code in RETRYABLE_STATUS_CODES:
        return True

    try:
        body = (resp.text or "").upper()
    except Exception:
        body = ""

    return resp.status_code == 502 and "TIME_LIMIT" in body


def _sleep_before_retry(attempt: int):
    time.sleep(max(HTTP_RETRY_BACKOFF_SECONDS * attempt, HTTP_RETRY_BACKOFF_SECONDS))


def _request_json(method: str, url: str, *, headers: dict, timeout: int, json_body=None, retries: int | None = None):
    attempts = max(int(retries or HTTP_RETRY_ATTEMPTS or 1), 1)
    last_error = None
    last_response = None

    for attempt in range(1, attempts + 1):
        try:
            resp = requests.request(method, url, headers=headers, json=json_body, timeout=timeout)
            if resp.ok:
                return resp.json()

            last_response = resp
            if _is_retryable_http_response(resp) and attempt < attempts:
                _sleep_before_retry(attempt)
                continue

            raise RuntimeError(f"Base44 API error {resp.status_code}: {_response_text_preview(resp)}")
        except (ReadTimeout, Timeout, ConnectionError, RequestException) as e:
            last_error = e
            if attempt < attempts:
                _sleep_before_retry(attempt)
                continue
            raise RuntimeError(
                f"Base44 request failed after {attempt} attempts: {type(e).__name__}: {e}"
            )

    if last_response is not None:
        raise RuntimeError(
            f"Base44 API error {last_response.status_code}: {_response_text_preview(last_response)}"
        )

    raise RuntimeError(f"Base44 request failed: {last_error}")


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


def _unique_non_empty_strings(values):
    seen = set()
    out = []
    for value in values or []:
        if value is None:
            continue
        s = str(value).strip()
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def _trim_text(value, max_len):
    text = str(value or "")
    return text if len(text) <= max_len else text[:max_len]


BASE44_DEBUG_DATA_MAX_LEN = int(os.getenv("BASE44_DEBUG_DATA_MAX_LEN", "8500") or "8500")
BASE44_DEBUG_URL_MAX_LEN = int(os.getenv("BASE44_DEBUG_URL_MAX_LEN", "240") or "240")
BASE44_DEBUG_TEXT_MAX_LEN = int(os.getenv("BASE44_DEBUG_TEXT_MAX_LEN", "400") or "400")
BASE44_DEBUG_STDOUT_MAX_LEN = int(os.getenv("BASE44_DEBUG_STDOUT_MAX_LEN", "1200") or "1200")
BASE44_DEBUG_STDERR_MAX_LEN = int(os.getenv("BASE44_DEBUG_STDERR_MAX_LEN", "800") or "800")
BASE44_DEBUG_HISTORY_ITEMS = int(os.getenv("BASE44_DEBUG_HISTORY_ITEMS", "1") or "1")
BASE44_DEBUG_SCREENSHOT_HISTORY_ITEMS = int(os.getenv("BASE44_DEBUG_SCREENSHOT_HISTORY_ITEMS", "2") or "2")


def _debug_json_len(value):
    try:
        return len(json.dumps(value, ensure_ascii=False, separators=(",", ":")))
    except Exception:
        return len(str(value or ""))


def _debug_pick_keys(obj, keys):
    source = _as_plain_dict(obj)
    out = {}
    for key in keys:
        if key in source:
            out[key] = source.get(key)
    return out


def _debug_fit_scalar(key, value):
    if not isinstance(value, str):
        return value

    lower = str(key or "").strip().lower()

    if lower in {"worker_stdout", "stdout"}:
        return _trim_text(value, BASE44_DEBUG_STDOUT_MAX_LEN)

    if lower in {"worker_stderr", "stderr"}:
        return _trim_text(value, BASE44_DEBUG_STDERR_MAX_LEN)

    if lower.endswith("_url") or lower in {"target_url", "effective_page_url", "video_url"}:
        return _trim_text(value, BASE44_DEBUG_URL_MAX_LEN)

    if lower.endswith("_path") or lower in {"bundle_dir", "local_screenshot_path", "secondary_screenshot_path"}:
        return _trim_text(value, 160)

    if lower in {"page_title", "title"}:
        return _trim_text(value, 240)

    if lower in {
        "collection_method",
        "expand_method",
        "execution_actor",
        "assigned_device",
        "source_device",
        "execution_device",
        "execution_path",
        "runner",
        "device",
        "controller_device",
        "client_device",
        "platform",
        "source_platform",
        "source_page_domain",
    }:
        return _trim_text(value, 120)

    if len(value) > BASE44_DEBUG_TEXT_MAX_LEN:
        return _trim_text(value, BASE44_DEBUG_TEXT_MAX_LEN)

    return value


def _sanitize_debug_data_value(value, key=None):
    if isinstance(value, dict):
        out = {}
        for child_key, child_value in value.items():
            lower = str(child_key or "").strip().lower()

            if lower in {"page_html", "raw_html", "raw_page_text", "visible_text", "expanded_caption_text", "transcript_text"}:
                if isinstance(child_value, str):
                    out[f"{child_key}_len"] = len(child_value)
                elif child_value is not None:
                    out[f"{child_key}_len"] = len(str(child_value))
                continue

            if lower in {"screenshot_urls", "additional_screenshot_urls"}:
                urls = _unique_non_empty_strings(child_value or [])
                out[child_key] = [_trim_text(url, BASE44_DEBUG_URL_MAX_LEN) for url in urls[-2:]]
                continue

            if lower == "evidence_history":
                items = list(child_value or [])[-max(BASE44_DEBUG_HISTORY_ITEMS, 1):]
                out[child_key] = [_sanitize_debug_data_value(item, child_key) for item in items]
                continue

            if lower == "screenshot_upload_history":
                items = list(child_value or [])[-max(BASE44_DEBUG_SCREENSHOT_HISTORY_ITEMS, 1):]
                out[child_key] = [_sanitize_debug_data_value(item, child_key) for item in items]
                continue

            if lower in {
                "collector_platforms",
                "collector_capabilities",
                "evidence_actor_tags",
                "allowed_collector_profile_ids",
                "excluded_collector_profile_ids",
                "required_collector_platforms",
                "required_collector_capabilities",
            }:
                out[child_key] = _unique_non_empty_strings(child_value or [])[:10]
                continue

            out[child_key] = _sanitize_debug_data_value(child_value, child_key)
        return out

    if isinstance(value, list):
        items = value[-5:] if len(value) > 5 else value
        return [_sanitize_debug_data_value(item, key) for item in items]

    return _debug_fit_scalar(key, value)


def _summarize_evidence_submission(value):
    summary = _debug_pick_keys(
        value,
        [
            "received_at",
            "target_url",
            "platform_hint",
            "job_type_hint",
            "collection_method",
            "execution_actor",
            "assigned_device",
            "execution_path",
            "source_device",
            "runner",
            "page_title",
            "media_type_guess",
            "is_video",
            "video_url",
            "raw_page_text_length",
            "expanded_caption_text_length",
            "meta_description_length",
            "page_html_length",
            "transcript_text_length",
            "visible_text_before_expand_length",
            "visible_text_after_expand_length",
            "source_platform",
            "source_creator_handle",
            "source_channel_key",
            "source_profile_url",
            "source_page_domain",
        ],
    )
    if summary:
        summary["trimmed"] = True
    return _sanitize_debug_data_value(summary, "evidence_submission")


def _summarize_evidence_history(value):
    items = list(value or [])[-1:]
    out = []
    for item in items:
        summary = _debug_pick_keys(
            item,
            [
                "received_at",
                "execution_actor",
                "assigned_device",
                "execution_path",
                "source_device",
                "runner",
                "target_url",
                "platform_hint",
                "job_type_hint",
                "collection_method",
                "page_title",
                "media_type_guess",
                "is_video",
                "raw_page_text_length",
                "expanded_caption_text_length",
                "meta_description_length",
                "page_html_length",
                "transcript_text_length",
                "visible_text_before_expand_length",
                "visible_text_after_expand_length",
            ],
        )
        if summary:
            summary["trimmed"] = True
        out.append(_sanitize_debug_data_value(summary, "evidence_history"))
    return out


def _summarize_server_first_pass(value):
    summary = _debug_pick_keys(
        value,
        [
            "target_url",
            "final_url",
            "page_title",
            "media_type_guess",
            "raw_page_text_length",
            "meta_description_length",
            "page_html_length",
            "platform",
            "extraction_confidence",
            "metadata_only",
            "parsed_script_found",
            "browser_fallback_used",
            "visible_text_length",
            "structured_text_length",
            "fetch_status",
        ],
    )
    if summary:
        summary["trimmed"] = True
    return _sanitize_debug_data_value(summary, "server_first_pass")


def _summarize_screenshot_upload_history(value):
    items = list(value or [])[-1:]
    out = []
    for item in items:
        summary = _debug_pick_keys(item, ["uploaded_at", "url", "filename", "kind", "device", "debug_last_step"])
        if summary:
            summary["trimmed"] = True
        out.append(_sanitize_debug_data_value(summary, "screenshot_upload_history"))
    return out


def _summarize_collector_claim(value):
    summary = _debug_pick_keys(
        value,
        [
            "collector_node_id",
            "collector_profile_id",
            "collector_account_label",
            "collector_platforms",
            "collector_capabilities",
            "can_claim_default_jobs",
            "can_claim_confirmation_jobs",
            "device_name",
            "claimed_at",
            "claim_update_mode",
            "is_confirmation_job",
            "confirmation_reason",
            "require_different_profile_from",
            "requested_by_collector_profile_id",
            "parent_job_id",
            "claim_kind",
            "allowed_collector_profile_ids",
            "excluded_collector_profile_ids",
            "required_collector_platforms",
            "required_collector_capabilities",
        ],
    )
    if summary:
        summary["trimmed"] = True
    return _sanitize_debug_data_value(summary, "claim")


def _build_minimal_debug_data(value, original_len):
    source = _as_plain_dict(value)
    minimal = _debug_pick_keys(
        source,
        [
            "source_platform",
            "source_creator_name",
            "source_creator_handle",
            "source_channel_name",
            "source_channel_key",
            "source_profile_url",
            "source_page_domain",
            "creator_group_key",
            "source_avatar_url",
            "collection_method",
            "execution_actor",
            "assigned_device",
            "source_device",
            "execution_device",
            "execution_path",
            "runner",
            "platform",
            "effective_page_url",
            "caption_expanded",
            "expand_attempted",
            "expand_success",
            "expand_method",
            "collector_node_id",
            "collector_profile_id",
            "collector_account_label",
            "collector_platforms",
            "collector_capabilities",
            "can_claim_default_jobs",
            "can_claim_confirmation_jobs",
            "claimed_by_collector_node_id",
            "claimed_by_collector_profile_id",
            "claimed_by_collector_account_label",
            "pi_build",
            "investigation_patch_version",
            "device",
            "upload_timestamp",
        ],
    )
    if "claim_next_job" in source:
        minimal["claim_next_job"] = _summarize_collector_claim(source.get("claim_next_job"))
    if "claim_confirmation_routing" in source:
        minimal["claim_confirmation_routing"] = _summarize_collector_claim(source.get("claim_confirmation_routing"))
    if "claimed_by_collector" in source:
        minimal["claimed_by_collector"] = _summarize_collector_claim(source.get("claimed_by_collector"))
    minimal["debug_data_trimmed"] = True
    minimal["debug_data_original_len"] = int(original_len or 0)
    return _sanitize_debug_data_value(minimal, "debug_data")


def _fit_debug_data_for_base44(value):
    working = _sanitize_debug_data_value(_as_plain_dict(value), "debug_data")
    original_len = _debug_json_len(working)

    if original_len <= BASE44_DEBUG_DATA_MAX_LEN:
        return working

    if "worker_stdout" in working:
        working["worker_stdout"] = _trim_text(str(working.get("worker_stdout") or ""), 600)
    if "worker_stderr" in working:
        working["worker_stderr"] = _trim_text(str(working.get("worker_stderr") or ""), 300)
    if _debug_json_len(working) <= BASE44_DEBUG_DATA_MAX_LEN:
        working["debug_data_trimmed"] = True
        working["debug_data_original_len"] = original_len
        return working

    if "evidence_submission" in working:
        working["evidence_submission"] = _summarize_evidence_submission(working.get("evidence_submission"))
    if "evidence_history" in working:
        working["evidence_history"] = _summarize_evidence_history(working.get("evidence_history"))
    if "server_first_pass" in working:
        working["server_first_pass"] = _summarize_server_first_pass(working.get("server_first_pass"))
    if "screenshot_upload_history" in working:
        working["screenshot_upload_history"] = _summarize_screenshot_upload_history(working.get("screenshot_upload_history"))
    if "claimed_by_collector" in working:
        working["claimed_by_collector"] = _summarize_collector_claim(working.get("claimed_by_collector"))
    if "claim_next_job" in working:
        working["claim_next_job"] = _summarize_collector_claim(working.get("claim_next_job"))
    if "claim_confirmation_routing" in working:
        working["claim_confirmation_routing"] = _summarize_collector_claim(working.get("claim_confirmation_routing"))

    for key in ["bundle_dir", "primary_screenshot_path", "description_screenshot_path", "local_screenshot_path", "secondary_screenshot_path"]:
        working.pop(key, None)

    if _debug_json_len(working) <= BASE44_DEBUG_DATA_MAX_LEN:
        working["debug_data_trimmed"] = True
        working["debug_data_original_len"] = original_len
        return working

    return _build_minimal_debug_data(working, original_len)


def _serialize_debug_data_for_base44(value):
    return json.dumps(_fit_debug_data_for_base44(value), ensure_ascii=False)

def _normalize_actor_tag(value):
    s = str(value or "").strip().lower()
    if not s:
        return None
    if any(token in s for token in ["iphone", "ios", "android", "mobile", "phone", "adb", "phone_worker"]):
        return "phone"
    if (
        any(token in s for token in ["raspberry", "pi_auto_queue", "pi_browser", "pi_browser_collector", "rpi", "raspberry-pi", " pi "])
        or s == "pi"
        or s.startswith("pi_")
        or s.startswith("pi-")
    ):
        return "pi"
    if any(token in s for token in ["server", "playwright", "web_runner", "server_first_pass"]):
        return "server"
    return None


def _extract_actor_values(blob):
    obj = _as_plain_dict(blob)
    if not obj:
        return []

    out = []
    interesting_keys = [
        "execution_actor",
        "assigned_device",
        "source_device",
        "execution_device",
        "execution_path",
        "runner",
        "device",
        "controller_device",
        "client_device",
        "collection_method",
        "created_from",
    ]
    for key in interesting_keys:
        if key in obj:
            out.append(obj.get(key))

    for key in ["evidence_submission", "shorts", "phone_worker", "extra"]:
        nested = obj.get(key)
        if isinstance(nested, dict):
            out.extend(_extract_actor_values(nested))

    return out


def _build_actor_tags(*blobs):
    tags = []
    for blob in blobs:
        for value in _extract_actor_values(blob):
            tag = _normalize_actor_tag(value)
            if tag:
                tags.append(tag)
    return _unique_non_empty_strings(tags)


def _merge_debug_data(existing, incoming):
    existing = _as_plain_dict(existing)
    incoming = _as_plain_dict(incoming)
    merged = dict(existing)

    for key, value in incoming.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _merge_debug_data(merged.get(key), value)
            continue

        if key in {"screenshot_urls", "additional_screenshot_urls", "evidence_actor_tags"}:
            merged[key] = _unique_non_empty_strings(list(merged.get(key) or []) + list(value or []))
            continue

        if key == "evidence_history":
            existing_history = merged.get(key) or []
            incoming_history = value or []
            if isinstance(existing_history, list) and isinstance(incoming_history, list):
                merged[key] = (existing_history + incoming_history)[-MAX_EVIDENCE_HISTORY:]
                continue

        merged[key] = value

    existing_submission = _as_plain_dict(existing.get("evidence_submission"))
    incoming_submission = _as_plain_dict(incoming.get("evidence_submission"))

    if existing_submission or incoming_submission:
        merged_submission = dict(existing_submission)
        for key, value in incoming_submission.items():
            if isinstance(value, dict) and isinstance(merged_submission.get(key), dict):
                merged_submission[key] = _merge_debug_data(merged_submission.get(key), value)
                continue

            if key in {"screenshot_urls", "additional_screenshot_urls", "evidence_actor_tags"}:
                merged_submission[key] = _unique_non_empty_strings(list(merged_submission.get(key) or []) + list(value or []))
                continue

            merged_submission[key] = value

        merged_submission["screenshot_urls"] = _unique_non_empty_strings(
            list(merged_submission.get("screenshot_urls") or [])
            + list(merged_submission.get("additional_screenshot_urls") or [])
            + [
                merged_submission.get("screenshot_url"),
                merged_submission.get("primary_screenshot_url"),
                merged_submission.get("description_screenshot_url"),
            ]
        )

        merged_submission["evidence_actor_tags"] = _build_actor_tags(merged_submission, existing_submission, incoming_submission)
        merged["evidence_submission"] = merged_submission

    merged["evidence_actor_tags"] = _build_actor_tags(existing, incoming, merged)
    return merged


def api_headers():
    if not API_KEY:
        raise RuntimeError("Missing API_KEY in .env")
    return {"api_key": API_KEY, "Content-Type": "application/json"}


def bot_function_headers():
    if not BOT_SECRET:
        raise RuntimeError("Missing BOT_SECRET in .env")
    return {"Content-Type": "application/json", "x-bot-secret": BOT_SECRET}


def get_job(job_id: str):
    return _request_json("GET", f"{BOTJOB_URL}/{job_id}", headers=api_headers(), timeout=30)


def update_job(job_id: str, payload: dict):
    return _request_json("PUT", f"{BOTJOB_URL}/{job_id}", headers=api_headers(), json_body=payload, timeout=30)


def get_recipe(recipe_id: str):
    return _request_json("GET", f"{RECIPE_URL}/{recipe_id}", headers=api_headers(), timeout=30)


def update_recipe(recipe_id: str, payload: dict):
    return _request_json("PUT", f"{RECIPE_URL}/{recipe_id}", headers=api_headers(), json_body=payload, timeout=30)


def _entity_collection_url(entity_name: str) -> str:
    return f"{ENTITY_BASE_URL}/{entity_name}"


def entity_url(entity_name: str):
    return _entity_collection_url(str(entity_name or "").strip())


def create_entity(entity_name: str, payload: dict, *, timeout: int = 30):
    return _request_json(
        "POST",
        _entity_collection_url(entity_name),
        headers=api_headers(),
        json_body=payload,
        timeout=timeout,
    )


def create_entity_record(entity_name: str, payload: dict, *, timeout: int = 30):
    return create_entity(entity_name, payload, timeout=timeout)


def update_entity(entity_name: str, entity_id: str, payload: dict, *, timeout: int = 30):
    return _request_json(
        "PUT",
        f"{_entity_collection_url(entity_name)}/{entity_id}",
        headers=api_headers(),
        json_body=payload,
        timeout=timeout,
    )


def _drop_none_values(payload: dict):
    return {key: value for key, value in (payload or {}).items() if value is not None}


def _json_stringify_compact(value, max_len: int | None = None):
    if isinstance(value, str):
        return _trim_text(value, max_len or INVESTIGATION_HISTORY_JSON_MAX_LEN)
    try:
        serialized = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        serialized = json.dumps({"error": "json_stringify_failed"}, ensure_ascii=False)
    return _trim_text(serialized, max_len or INVESTIGATION_HISTORY_JSON_MAX_LEN)


def _prepare_investigation_row(payload: dict, *, json_fields=None, json_max_len: int | None = None):
    row = _drop_none_values(dict(payload or {}))
    for field in (json_fields or []):
        if field in row:
            row[field] = _json_stringify_compact(row.get(field), json_max_len)
    return row


def _history_now_iso():
    return utc_now_iso()


def _history_build_payload_from_direct_args(
    job_id: str,
    recipe_id: str | None,
    start_url: str,
    platform: str,
    investigation_payload: dict,
    source_metadata: dict | None = None,
    primary_screenshot_url: str | None = None,
    secondary_screenshot_url: str | None = None,
):
    payload = investigation_payload if isinstance(investigation_payload, dict) else {}
    source_metadata = _as_plain_dict(source_metadata)
    now = _history_now_iso()
    start_url_text = str(start_url or '').strip()
    winner_url = str(payload.get('winner_url') or '').strip()
    clues = _unique_non_empty_strings(payload.get('clues') or [])
    reasons = _unique_non_empty_strings(payload.get('reasons') or [])
    breadcrumbs = list(payload.get('breadcrumbs') or [])[: max(INVESTIGATION_HISTORY_MAX_BREADCRUMBS, 1)]
    candidates = list(payload.get('candidates') or [])[: max(INVESTIGATION_HISTORY_MAX_CANDIDATES, 1)]
    evidence_summary = payload.get('evidence_summary') if isinstance(payload.get('evidence_summary'), dict) else {}
    source_identity = {
        'source_platform': source_metadata.get('source_platform') or platform or '',
        'source_creator_name': source_metadata.get('source_creator_name') or '',
        'source_creator_handle': source_metadata.get('source_creator_handle') or '',
        'source_channel_name': source_metadata.get('source_channel_name') or '',
        'source_channel_key': source_metadata.get('source_channel_key') or '',
        'source_profile_url': source_metadata.get('source_profile_url') or '',
        'source_page_domain': source_metadata.get('source_page_domain') or '',
        'creator_group_key': source_metadata.get('creator_group_key') or '',
        'source_avatar_url': source_metadata.get('source_avatar_url') or '',
    }

    run_payload = {
        'recipe_id': recipe_id or None,
        'bot_job_id': job_id or None,
        'source_url': start_url_text or None,
        'normalized_source_url': start_url_text.lower() if start_url_text else None,
        'platform': platform or None,
        'original_platform': payload.get('original_platform') or platform or None,
        'scenario_mode': payload.get('scenario_mode') or payload.get('mode') or None,
        'trigger_reason': payload.get('trigger_reason') or None,
        'collection_method': payload.get('collection_method') or None,
        'runtime_version': payload.get('runtime_version') or None,
        'runtime_source': payload.get('runtime_source') or None,
        'investigator_patch_version': payload.get('investigator_patch_version') or BOT_API_PATCH_VERSION,
        'started_at': payload.get('started_at') or now,
        'recorded_at': now,
        'status': payload.get('status') or ('winner_selected' if winner_url else 'no_winner'),
        'summary_outcome': payload.get('summary_outcome') or (('winner_selected:' + winner_url) if winner_url else ('no_winner:' + str(payload.get('no_winner_reason') or ''))),
        'winner_url': winner_url or None,
        'winner_score': payload.get('winner_score'),
        'linked_recipe_used': bool(payload.get('linked_recipe_used')),
        'analysis_platform_hint': payload.get('analysis_platform_hint') or None,
        'effective_analysis_platform': payload.get('effective_analysis_platform') or None,
        'source_profile_url': source_identity.get('source_profile_url') or None,
        'source_creator_name': source_identity.get('source_creator_name') or None,
        'source_creator_handle': source_identity.get('source_creator_handle') or None,
        'source_channel_name': source_identity.get('source_channel_name') or None,
        'source_channel_key': source_identity.get('source_channel_key') or None,
        'source_page_domain': source_identity.get('source_page_domain') or None,
        'candidate_count': len(candidates),
        'breadcrumb_count': len(breadcrumbs),
        'clues_json': clues,
        'reasons_json': reasons,
        'source_identity_json': source_identity,
        'summary_json': {
            'start_url': start_url_text,
            'winner_url': winner_url,
            'summary_outcome': payload.get('summary_outcome') or '',
            'no_winner_reason': payload.get('no_winner_reason') or '',
            'linked_recipe_used': bool(payload.get('linked_recipe_used')),
            'expected_platform_handoff': payload.get('effective_analysis_platform') or payload.get('analysis_platform_hint') or '',
            'debug': payload.get('debug') or {},
        },
    }

    breadcrumb_rows = []
    for index, crumb in enumerate(breadcrumbs, start=1):
        crumb = crumb if isinstance(crumb, dict) else {}
        breadcrumb_rows.append({
            'recipe_id': recipe_id or None,
            'bot_job_id': job_id or None,
            'sequence': index,
            'event_type': crumb.get('event_type') or None,
            'label': crumb.get('label') or None,
            'url': crumb.get('url') or None,
            'status': crumb.get('status') or None,
            'reason': crumb.get('reason') or None,
            'payload_json': crumb.get('payload') or {},
            'occurred_at': crumb.get('occurred_at') or now,
        })

    candidate_rows = []
    for index, candidate in enumerate(candidates, start=1):
        candidate = candidate if isinstance(candidate, dict) else {}
        candidate_rows.append({
            'recipe_id': recipe_id or None,
            'bot_job_id': job_id or None,
            'sequence': index,
            'candidate_url': candidate.get('candidate_url') or candidate.get('url') or None,
            'candidate_domain': candidate.get('candidate_domain') or candidate.get('domain') or None,
            'candidate_type': candidate.get('candidate_type') or None,
            'candidate_source': candidate.get('candidate_source') or candidate.get('source') or None,
            'score': candidate.get('score'),
            'usable': bool(candidate.get('usable')),
            'chosen': bool(candidate.get('chosen')),
            'reason': candidate.get('reason') or None,
            'page_title': candidate.get('page_title') or None,
            'score_breakdown_json': candidate.get('score_breakdown') or {},
            'evidence_quality_summary_json': candidate.get('evidence_quality_summary') or {},
            'payload_json': candidate.get('payload') or {},
            'created_at_snapshot': now,
        })

    evidence_payload = {
        'recipe_id': recipe_id or None,
        'bot_job_id': job_id or None,
        'source_url': start_url_text or None,
        'winner_url': winner_url or None,
        'explicit_recipe_link': payload.get('explicit_recipe_link') or winner_url or None,
        'analysis_platform_hint': payload.get('analysis_platform_hint') or None,
        'effective_analysis_platform': payload.get('effective_analysis_platform') or None,
        'linked_recipe_used': bool(payload.get('linked_recipe_used')),
        'evidence_summary_json': evidence_summary,
        'source_metadata_json': source_identity,
        'blob_refs_json': {
            'primary_screenshot_url': primary_screenshot_url or '',
            'secondary_screenshot_url': secondary_screenshot_url or '',
            'source_avatar_url': source_metadata.get('source_avatar_url') or '',
        },
        'merged_evidence_summary': _trim_text(str(payload.get('merged_evidence_summary') or evidence_summary.get('page_title') or ''), 1000),
        'created_at_snapshot': now,
    }

    return {
        'run': run_payload,
        'breadcrumbs': breadcrumb_rows,
        'candidates': candidate_rows,
        'evidence': evidence_payload,
    }


def _history_counts_from_body(body):
    plain = _as_plain_dict(body)
    return {
        'breadcrumbs_requested': len(list(plain.get('breadcrumbs') or [])),
        'candidates_requested': len(list(plain.get('candidates') or [])),
        'has_evidence_payload': bool(_as_plain_dict(plain.get('evidence'))),
    }


def _history_log_non_success(job_id, recipe_id, result):
    job_id_text = str(job_id or '').strip()
    if not job_id_text:
        return

    result_obj = _as_plain_dict(result)
    scenario_mode = str(result_obj.get('scenario_mode') or '').strip().lower()
    reason = str(result_obj.get('reason') or result_obj.get('error') or 'unknown').strip()
    error_text = str(result_obj.get('error') or '').strip()
    supported_modes = list(result_obj.get('supported_modes') or [])
    counts = _as_plain_dict(result_obj.get('counts'))
    writer_version = str(result_obj.get('writer_version') or INVESTIGATION_HISTORY_WRITER_VERSION or '').strip()

    summary_parts = [
        f"scenario={scenario_mode or 'unknown'}",
        f"reason={reason or 'unknown'}",
    ]
    if supported_modes:
        summary_parts.append(f"supported={','.join([str(x) for x in supported_modes])}")
    if counts:
        summary_parts.append(
            f"counts=b:{counts.get('breadcrumbs_requested', 0)} c:{counts.get('candidates_requested', 0)} e:{1 if counts.get('has_evidence_payload') else 0}"
        )
    if writer_version:
        summary_parts.append(f"writer={writer_version}")
    if error_text:
        summary_parts.append(f"error={_trim_text(error_text, 300)}")

    debug_data = {
        'investigation_history_write_result': result_obj,
        'investigation_history_writer_version': writer_version or None,
    }

    try:
        append_job_debug_log(
            job_id_text,
            f"Investigation history not recorded. {' '.join(summary_parts)}",
            debug_last_step='investigation_history_skipped',
            debug_data=debug_data,
        )
    except Exception:
        pass

    recipe_id_text = str(recipe_id or '').strip()
    if recipe_id_text:
        try:
            update_recipe_debug(
                recipe_id_text,
                debug_log_append=f"Investigation history not recorded for BotJob {job_id_text}: {reason or 'unknown'}",
            )
        except Exception:
            pass


def write_investigation_history(*args, **kwargs):
    if len(args) == 1 and isinstance(args[0], dict) and not kwargs:
        body = _as_plain_dict(args[0])
        run_for_log = _as_plain_dict(body.get('run'))
        job_id_for_log = str(run_for_log.get('bot_job_id') or '').strip()
        recipe_id_for_log = run_for_log.get('recipe_id')
    else:
        job_id = kwargs.get('job_id', args[0] if len(args) > 0 else '')
        recipe_id = kwargs.get('recipe_id', args[1] if len(args) > 1 else None)
        start_url = kwargs.get('start_url', kwargs.get('source_url', args[2] if len(args) > 2 else ''))
        platform = kwargs.get('platform', args[3] if len(args) > 3 else '')
        investigation_payload = kwargs.get('investigation_payload', kwargs.get('payload', args[4] if len(args) > 4 else {}))
        source_metadata = kwargs.get('source_metadata', args[5] if len(args) > 5 else None)
        primary_screenshot_url = kwargs.get('primary_screenshot_url', args[6] if len(args) > 6 else None)
        secondary_screenshot_url = kwargs.get('secondary_screenshot_url', args[7] if len(args) > 7 else None)
        body = _history_build_payload_from_direct_args(
            job_id=str(job_id or ''),
            recipe_id=recipe_id,
            start_url=str(start_url or ''),
            platform=str(platform or ''),
            investigation_payload=investigation_payload if isinstance(investigation_payload, dict) else {},
            source_metadata=source_metadata if isinstance(source_metadata, dict) else _as_plain_dict(source_metadata),
            primary_screenshot_url=primary_screenshot_url,
            secondary_screenshot_url=secondary_screenshot_url,
        )
        job_id_for_log = str(job_id or '').strip()
        recipe_id_for_log = recipe_id

    run_payload = _as_plain_dict(body.get('run'))
    scenario_mode = str(run_payload.get('scenario_mode') or run_payload.get('scenario') or '').strip().lower()
    counts = _history_counts_from_body(body)

    if not INVESTIGATION_HISTORY_ENABLED:
        result = {
            'ok': False,
            'skipped': True,
            'reason': 'disabled',
            'scenario_mode': scenario_mode,
            'counts': counts,
            'patch_version': BOT_API_PATCH_VERSION,
            'writer_version': INVESTIGATION_HISTORY_WRITER_VERSION,
        }
        _history_log_non_success(job_id_for_log, recipe_id_for_log, result)
        return result

    if not run_payload:
        result = {
            'ok': False,
            'skipped': True,
            'reason': 'missing_run_payload',
            'scenario_mode': scenario_mode,
            'counts': counts,
            'patch_version': BOT_API_PATCH_VERSION,
            'writer_version': INVESTIGATION_HISTORY_WRITER_VERSION,
        }
        _history_log_non_success(job_id_for_log, recipe_id_for_log, result)
        return result

    if INVESTIGATION_HISTORY_SUPPORTED_MODES and scenario_mode and scenario_mode not in INVESTIGATION_HISTORY_SUPPORTED_MODES:
        result = {
            'ok': False,
            'skipped': True,
            'reason': 'unsupported_mode',
            'scenario_mode': scenario_mode,
            'supported_modes': sorted(INVESTIGATION_HISTORY_SUPPORTED_MODES),
            'counts': counts,
            'patch_version': BOT_API_PATCH_VERSION,
            'writer_version': INVESTIGATION_HISTORY_WRITER_VERSION,
        }
        _history_log_non_success(job_id_for_log, recipe_id_for_log, result)
        return result

    run_row = _prepare_investigation_row(
        run_payload,
        json_fields=['clues_json', 'reasons_json', 'summary_json', 'source_identity_json'],
    )
    try:
        created_run = create_entity_record('InvestigationRun', run_row, timeout=45)
    except Exception as err:
        result = {
            'ok': False,
            'skipped': True,
            'reason': 'write_failed:run',
            'scenario_mode': scenario_mode,
            'counts': counts,
            'error': f'{type(err).__name__}: {err}',
            'patch_version': BOT_API_PATCH_VERSION,
            'writer_version': INVESTIGATION_HISTORY_WRITER_VERSION,
        }
        _history_log_non_success(job_id_for_log, recipe_id_for_log, result)
        return result

    run_id = str(created_run.get('id') or '')
    if not run_id:
        result = {
            'ok': False,
            'skipped': True,
            'reason': 'missing_run_id',
            'scenario_mode': scenario_mode,
            'counts': counts,
            'created_run_preview': _trim_text(_json_stringify_compact(created_run, 1200), 1200),
            'patch_version': BOT_API_PATCH_VERSION,
            'writer_version': INVESTIGATION_HISTORY_WRITER_VERSION,
        }
        _history_log_non_success(job_id_for_log, recipe_id_for_log, result)
        return result

    breadcrumb_errors = []
    breadcrumbs_written = 0
    for idx, breadcrumb in enumerate(list(body.get('breadcrumbs') or [])[: max(INVESTIGATION_HISTORY_MAX_BREADCRUMBS, 1)], 1):
        try:
            row = _prepare_investigation_row(
                {
                    'investigation_run_id': run_id,
                    **_as_plain_dict(breadcrumb),
                },
                json_fields=['payload_json'],
                json_max_len=min(INVESTIGATION_HISTORY_JSON_MAX_LEN, 12000),
            )
            create_entity_record('InvestigationBreadcrumb', row, timeout=30)
            breadcrumbs_written += 1
        except Exception as err:
            breadcrumb_errors.append(f'breadcrumb[{idx}] {type(err).__name__}: {err}')

    candidate_errors = []
    candidates_written = 0
    for idx, candidate in enumerate(list(body.get('candidates') or [])[: max(INVESTIGATION_HISTORY_MAX_CANDIDATES, 1)], 1):
        try:
            row = _prepare_investigation_row(
                {
                    'investigation_run_id': run_id,
                    **_as_plain_dict(candidate),
                },
                json_fields=['score_breakdown_json', 'evidence_quality_summary_json', 'payload_json'],
                json_max_len=min(INVESTIGATION_HISTORY_JSON_MAX_LEN, 12000),
            )
            create_entity_record('InvestigationCandidateSnapshot', row, timeout=30)
            candidates_written += 1
        except Exception as err:
            candidate_errors.append(f'candidate[{idx}] {type(err).__name__}: {err}')

    evidence_error = None
    evidence_payload = _as_plain_dict(body.get('evidence'))
    if evidence_payload:
        try:
            row = _prepare_investigation_row(
                {
                    'investigation_run_id': run_id,
                    **evidence_payload,
                },
                json_fields=['evidence_summary_json', 'source_metadata_json', 'blob_refs_json'],
            )
            create_entity_record('InvestigationEvidenceSnapshot', row, timeout=45)
        except Exception as err:
            evidence_error = f'{type(err).__name__}: {err}'

    return {
        'ok': True,
        'run_id': run_id,
        'scenario_mode': scenario_mode,
        'counts': counts,
        'breadcrumbs_written': breadcrumbs_written,
        'candidates_written': candidates_written,
        'breadcrumb_errors': breadcrumb_errors,
        'candidate_errors': candidate_errors,
        'evidence_error': evidence_error,
        'patch_version': BOT_API_PATCH_VERSION,
        'writer_version': INVESTIGATION_HISTORY_WRITER_VERSION,
    }



def create_bot_job(payload: dict):
    return _request_json("POST", BOTJOB_URL, headers=api_headers(), json_body=payload, timeout=60)


def create_confirmation_job(
    parent_job_id: str,
    recipe_id: str | None,
    target_url: str,
    platform: str,
    confirmation_reason: str = "social_account_contamination",
    require_different_profile_from: str = "",
    requested_by_collector_profile_id: str = "",
    allowed_collector_profile_ids=None,
    excluded_collector_profile_ids=None,
    required_collector_platforms=None,
    required_collector_capabilities=None,
    debug_data=None,
):
    routing = {
        "job_type": "confirmation",
        "claim_kind": "confirmation",
        "confirmation_reason": str(confirmation_reason or "social_account_contamination").strip(),
        "require_different_profile_from": str(require_different_profile_from or requested_by_collector_profile_id or COLLECTOR_PROFILE_ID).strip().lower(),
        "requested_by_collector_profile_id": str(requested_by_collector_profile_id or COLLECTOR_PROFILE_ID).strip().lower(),
        "allowed_collector_profile_ids": _unique_non_empty_strings(allowed_collector_profile_ids or []),
        "excluded_collector_profile_ids": _unique_non_empty_strings(excluded_collector_profile_ids or []),
        "required_collector_platforms": _unique_non_empty_strings(required_collector_platforms or ([platform] if platform else [])),
        "required_collector_capabilities": _unique_non_empty_strings(required_collector_capabilities or []),
        "parent_job_id": str(parent_job_id or "").strip(),
    }

    merged_debug_data = _merge_debug_data(
        _as_plain_dict(debug_data),
        {
            "confirmation_request": routing,
            "confirmation_routing": routing,
            "requested_by_collector": get_collector_identity(),
        },
    )

    payload = {
        "status": "pending",
        "platform": str(platform or "").strip().lower(),
        "target_url": str(target_url or "").strip(),
        "job_type": "confirmation",
        "claim_kind": "confirmation",
        "confirmation_reason": routing["confirmation_reason"],
        "require_different_profile_from": routing["require_different_profile_from"],
        "requested_by_collector_profile_id": routing["requested_by_collector_profile_id"],
        "parent_job_id": routing["parent_job_id"],
        "debug_status": "pending",
        "debug_last_step": "confirmation_job_created",
        "assigned_device": "",
        "decision": "pending_confirmation",
        "last_error": "",
        "debug_data": _serialize_debug_data_for_base44(merged_debug_data),
    }
    if routing["allowed_collector_profile_ids"]:
        payload["allowed_collector_profile_ids"] = routing["allowed_collector_profile_ids"]
    if routing["excluded_collector_profile_ids"]:
        payload["excluded_collector_profile_ids"] = routing["excluded_collector_profile_ids"]
    if routing["required_collector_platforms"]:
        payload["required_collector_platforms"] = routing["required_collector_platforms"]
    if routing["required_collector_capabilities"]:
        payload["required_collector_capabilities"] = routing["required_collector_capabilities"]
    if recipe_id:
        payload["recipe_id"] = recipe_id

    return create_bot_job(payload)


def claim_next_job(platform_allowlist=None):
    if not CLAIM_NEXT_JOB_URL:
        raise RuntimeError("Missing CLAIM_NEXT_JOB_URL in .env")

    payload = {
        "device_name": DEVICE_NAME,
        "collector_node_id": COLLECTOR_NODE_ID,
        "collector_profile_id": COLLECTOR_PROFILE_ID,
        "collector_account_label": COLLECTOR_ACCOUNT_LABEL,
        "collector_platforms": list(COLLECTOR_PLATFORMS),
        "collector_capabilities": list(COLLECTOR_CAPABILITIES),
        "can_claim_default_jobs": bool(CAN_CLAIM_DEFAULT_JOBS),
        "can_claim_confirmation_jobs": bool(CAN_CLAIM_CONFIRMATION_JOBS),
    }
    if platform_allowlist:
        payload["platform_allowlist"] = platform_allowlist

    data = _request_json(
        "POST",
        CLAIM_NEXT_JOB_URL,
        headers=bot_function_headers(),
        json_body=payload,
        timeout=60,
    )
    return data.get("job")


def list_pending_supported_jobs(limit=100):
    return []


def list_pending_instagram_jobs(limit=100):
    return []


def claim_job(job_id: str):
    payload = {
        "status": "processing",
        "assigned_device": DEVICE_NAME,
        "started_at": utc_now_iso(),
        "lock_token": f"{DEVICE_NAME}-{utc_now_iso()}",
        "debug_status": "processing",
        "debug_last_step": "job_claimed",
    }
    return update_job(job_id, payload)


def fail_job(job_id: str, error_message: str):
    payload = {
        "status": "failed",
        "decision": "failed",
        "last_error": error_message,
        "finished_at": utc_now_iso(),
        "debug_status": "failed",
        "debug_last_step": "job_failed",
    }
    return update_job(job_id, payload)


def update_recipe_from_job(recipe_id: str, bot_decision: str, bot_result: str, screenshot_path=None, error=None):
    payload = {
        "bot_status": "check_failed" if error else "checked",
        "bot_last_checked_at": utc_now_iso(),
        "bot_decision": bot_decision,
        "bot_result": bot_result,
    }
    if screenshot_path:
        payload["bot_screenshot_path"] = screenshot_path
    if error:
        payload["bot_last_error"] = error
    return update_recipe(recipe_id, payload)


def append_job_debug_log(job_id: str, message: str, debug_status=None, debug_last_step=None, debug_data=None):
    job = get_job(job_id)
    existing_log = job.get("debug_log") or ""
    line = f"{utc_now_iso()} | {message}"
    new_log = f"{existing_log}\n{line}".strip()

    payload = {
        "debug_log": new_log,
        "last_heartbeat_at": utc_now_iso(),
    }
    if debug_status is not None:
        payload["debug_status"] = debug_status
    if debug_last_step is not None:
        payload["debug_last_step"] = debug_last_step

    if debug_data is not None:
        existing_debug_data = _as_plain_dict(job.get("debug_data"))
        incoming_debug_data = _as_plain_dict(debug_data)
        merged_debug_data = _merge_debug_data(existing_debug_data, incoming_debug_data)
        payload["debug_data"] = _serialize_debug_data_for_base44(merged_debug_data)

    return update_job(job_id, payload)


def touch_job_heartbeat(job_id: str, debug_last_step=None):
    payload = {"last_heartbeat_at": utc_now_iso()}
    if debug_last_step is not None:
        payload["debug_last_step"] = debug_last_step
    return update_job(job_id, payload)


def update_recipe_debug(recipe_id: str, debug_log_append=None, debug_screenshot_url=None):
    payload = {}
    if debug_screenshot_url is not None:
        payload["bot_debug_screenshot_url"] = debug_screenshot_url

    if debug_log_append is not None:
        try:
            recipe = get_recipe(recipe_id)
            existing = recipe.get("bot_debug_log") or ""
        except Exception:
            existing = ""

        line = f"{utc_now_iso()} | {debug_log_append}"
        payload["bot_debug_log"] = f"{existing}\n{line}".strip()

    if not payload:
        return None
    return update_recipe(recipe_id, payload)


def upload_bot_screenshot(job_id: str, image_path: str, device=None, debug_last_step=None):
    if not UPLOAD_BOT_SCREENSHOT_URL:
        raise RuntimeError("Missing UPLOAD_BOT_SCREENSHOT_URL in .env")

    with open(image_path, "rb") as f:
        image_b64 = base64.b64encode(f.read()).decode("utf-8")

    return _request_json(
        "POST",
        UPLOAD_BOT_SCREENSHOT_URL,
        headers=bot_function_headers(),
        json_body={
            "job_id": job_id,
            "image_b64": image_b64,
            "filename": os.path.basename(image_path) or f"bot_{job_id}.png",
            "device": device or DEVICE_NAME,
            "debug_last_step": debug_last_step or "screenshot_taken",
        },
        timeout=60,
        retries=max(HTTP_RETRY_ATTEMPTS, 4),
    )


def submit_bot_evidence(
    job_id: str,
    target_url: str,
    screenshot_url: str,
    recipe_id: str | None = None,
    raw_page_text: str | None = None,
    page_title: str | None = None,
    media_type_guess: str | None = None,
    page_html: str | None = None,
    expanded_caption_text: str | None = None,
    meta_description: str | None = None,
    is_video: bool | None = None,
    video_url: str | None = None,
    primary_screenshot_url: str | None = None,
    description_screenshot_url: str | None = None,
    transcript_text: str | None = None,
    visible_text_before_expand: str | None = None,
    visible_text_after_expand: str | None = None,
    expand_attempted: bool | None = None,
    expand_success: bool | None = None,
    expand_method: str | None = None,
    is_youtube_shorts: bool | None = None,
    caption_before_len: int | None = None,
    caption_after_len: int | None = None,
    execution_actor: str | None = None,
    assigned_device: str | None = None,
    execution_path: str | None = None,
    source_device: str | None = None,
    runner: str | None = None,
    device: str | None = None,
    execution_device: str | None = None,
    controller_device: str | None = None,
    client_device: str | None = None,
    source_platform: str | None = None,
    source_creator_name: str | None = None,
    source_creator_handle: str | None = None,
    source_channel_name: str | None = None,
    source_channel_key: str | None = None,
    source_profile_url: str | None = None,
    source_page_domain: str | None = None,
    creator_group_key: str | None = None,
    source_avatar_url: str | None = None,
    visual_recipe_image_url: str | None = None,
    page_image_url: str | None = None,
    debug_data=None,
):
    if not SUBMIT_BOT_EVIDENCE_URL:
        raise RuntimeError("Missing SUBMIT_BOT_EVIDENCE_URL in .env")

    payload = {
        "job_id": job_id,
        "target_url": target_url,
        "screenshot_url": screenshot_url,
    }

    if recipe_id:
        payload["recipe_id"] = recipe_id
    if raw_page_text is not None:
        payload["raw_page_text"] = _trim_text(raw_page_text, SUBMIT_RAW_PAGE_TEXT_MAX_LEN)
    if page_title is not None:
        payload["page_title"] = _trim_text(page_title, SUBMIT_PAGE_TITLE_MAX_LEN)
    if media_type_guess is not None:
        payload["media_type_guess"] = media_type_guess
    if page_html is not None:
        payload["page_html"] = page_html
    if expanded_caption_text is not None:
        payload["expanded_caption_text"] = _trim_text(expanded_caption_text, SUBMIT_EXPANDED_CAPTION_MAX_LEN)
    if meta_description is not None:
        payload["meta_description"] = _trim_text(meta_description, SUBMIT_META_DESCRIPTION_MAX_LEN)
    if is_video is not None:
        payload["is_video"] = bool(is_video)
    if video_url is not None:
        payload["video_url"] = _trim_text(video_url, SUBMIT_VIDEO_URL_MAX_LEN)
    if primary_screenshot_url is not None:
        payload["primary_screenshot_url"] = primary_screenshot_url
    if description_screenshot_url is not None:
        payload["description_screenshot_url"] = description_screenshot_url
    if transcript_text is not None:
        payload["transcript_text"] = _trim_text(transcript_text, SUBMIT_TRANSCRIPT_MAX_LEN)
    if visible_text_before_expand is not None:
        payload["visible_text_before_expand"] = _trim_text(visible_text_before_expand, SUBMIT_VISIBLE_BEFORE_MAX_LEN)
    if visible_text_after_expand is not None:
        payload["visible_text_after_expand"] = _trim_text(visible_text_after_expand, SUBMIT_VISIBLE_AFTER_MAX_LEN)
    if expand_attempted is not None:
        payload["expand_attempted"] = bool(expand_attempted)
    if expand_success is not None:
        payload["expand_success"] = bool(expand_success)
    if expand_method is not None:
        payload["expand_method"] = _trim_text(expand_method, SUBMIT_EXPAND_METHOD_MAX_LEN)
    if is_youtube_shorts is not None:
        payload["is_youtube_shorts"] = bool(is_youtube_shorts)
    if caption_before_len is not None:
        payload["caption_before_len"] = int(caption_before_len)
    if caption_after_len is not None:
        payload["caption_after_len"] = int(caption_after_len)
    if execution_actor is not None:
        payload["execution_actor"] = execution_actor
    if assigned_device is not None:
        payload["assigned_device"] = assigned_device
    if execution_path is not None:
        payload["execution_path"] = execution_path
    if source_device is not None:
        payload["source_device"] = source_device
    if runner is not None:
        payload["runner"] = runner
    if device is not None:
        payload["device"] = device
    if execution_device is not None:
        payload["execution_device"] = execution_device
    if controller_device is not None:
        payload["controller_device"] = controller_device
    if client_device is not None:
        payload["client_device"] = client_device
    if source_platform is not None:
        payload["source_platform"] = _trim_text(source_platform, SUBMIT_SOURCE_TEXT_MAX_LEN)
    if source_creator_name is not None:
        payload["source_creator_name"] = _trim_text(source_creator_name, SUBMIT_SOURCE_TEXT_MAX_LEN)
    if source_creator_handle is not None:
        payload["source_creator_handle"] = _trim_text(source_creator_handle, SUBMIT_SOURCE_HANDLE_MAX_LEN)
    if source_channel_name is not None:
        payload["source_channel_name"] = _trim_text(source_channel_name, SUBMIT_SOURCE_TEXT_MAX_LEN)
    if source_channel_key is not None:
        payload["source_channel_key"] = _trim_text(source_channel_key, SUBMIT_SOURCE_KEY_MAX_LEN)
    if source_profile_url is not None:
        payload["source_profile_url"] = _trim_text(source_profile_url, SUBMIT_SOURCE_URL_MAX_LEN)
    if source_page_domain is not None:
        payload["source_page_domain"] = _trim_text(source_page_domain, SUBMIT_SOURCE_DOMAIN_MAX_LEN)
    if creator_group_key is not None:
        payload["creator_group_key"] = _trim_text(creator_group_key, SUBMIT_SOURCE_KEY_MAX_LEN)
    if source_avatar_url is not None:
        payload["source_avatar_url"] = _trim_text(source_avatar_url, SUBMIT_SOURCE_URL_MAX_LEN)
    if visual_recipe_image_url is not None:
        payload["visual_recipe_image_url"] = _trim_text(visual_recipe_image_url, SUBMIT_SOURCE_URL_MAX_LEN)
    if page_image_url is not None:
        payload["page_image_url"] = _trim_text(page_image_url, SUBMIT_SOURCE_URL_MAX_LEN)
    if debug_data is not None:
        payload["debug_data"] = _fit_debug_data_for_base44(debug_data)

    return _request_json(
        "POST",
        SUBMIT_BOT_EVIDENCE_URL,
        headers=bot_function_headers(),
        json_body=payload,
        timeout=120,
        retries=max(HTTP_RETRY_ATTEMPTS, 4),
    )


def send_alert(service: str, status: str, reason: str, message: str, screenshot_path: str | None = None, extra: dict | None = None):
    payload = {
        "service": service,
        "status": status,
        "reason": reason,
        "message": message,
        "screenshot_path": screenshot_path or "",
        "timestamp": utc_now_iso(),
        "device": DEVICE_NAME,
        "hostname": socket.gethostname(),
        "extra": extra or {},
    }

    return _request_json("POST", BOTALERT_URL, headers=api_headers(), json_body=payload, timeout=30)

