import asyncio
import os
import re
from pathlib import Path

from dotenv import load_dotenv
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

from alerts import send_alert

load_dotenv(Path.home() / "social-bot" / ".env", override=True)

BASE = Path.home() / "social-bot"
PROFILE_ROOT = BASE / "profiles"
SCREENSHOT_DIR = BASE / "screenshots"
SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)

HEADLESS = os.getenv("HEADLESS", "true").strip().lower() == "true"
COLLECTOR_PROFILE_ID = (
    os.getenv("COLLECTOR_PROFILE_ID", "").strip()
    or os.getenv("SOCIAL_COLLECTOR_PROFILE_ID", "").strip()
)
CHECK_URL = "https://www.facebook.com/settings/"


def sanitize_profile_segment(value: str) -> str:
    raw = str(value or "").strip().lower()
    raw = re.sub(r"[^a-z0-9._-]+", "-", raw).strip("-._")
    return raw[:80]


def profile_key_for(platform: str, collector_profile_id: str = "") -> str:
    base = platform if platform in {"instagram", "facebook", "tiktok", "youtube"} else "web"
    profile_segment = sanitize_profile_segment(collector_profile_id)
    if profile_segment and base in {"instagram", "facebook", "tiktok", "youtube"}:
        return f"{base}__{profile_segment}"
    return base


PROFILE_KEY = profile_key_for("facebook", COLLECTOR_PROFILE_ID)
PROFILE_DIR = PROFILE_ROOT / PROFILE_KEY


async def is_logged_in(page) -> tuple[bool, str]:
    try:
        await page.goto(CHECK_URL, wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(2500)

        url = (page.url or "").lower()

        bad_parts = [
            "/login",
            "/checkpoint/",
            "/recover/",
            "/two_step_verification/",
            "/device-based/regular/login/",
        ]
        for part in bad_parts:
            if part in url:
                return False, f"redirected_to_{part.strip('/').replace('/', '_')}"

        body_text = ""
        try:
            body_text = (await page.locator("body").inner_text(timeout=3000)).lower()
        except Exception:
            pass

        if "log in" in body_text and "create new account" in body_text:
            return False, "logged_out_ui_visible"

        for selector in [
            'input[name="email"]',
            'input[name="pass"]',
            'button[name="login"]',
        ]:
            try:
                if await page.locator(selector).count() > 0:
                    return False, "login_form_visible"
            except Exception:
                pass

        good_selectors = [
            '[aria-label="Your profile"]',
            '[aria-label="Home"]',
            '[role="feed"]',
            '[aria-label="Facebook"]',
        ]
        for selector in good_selectors:
            try:
                await page.wait_for_selector(selector, timeout=2000)
                return True, "ok_logged_in_shell"
            except Exception:
                pass

        if "facebook.com/settings" in url or "/settings" in url:
            return True, "ok_settings"

        return False, "unknown_not_logged_in_state"

    except PlaywrightTimeoutError:
        return False, "timeout"
    except Exception as e:
        return False, f"exception_{type(e).__name__}"


async def run_check_once() -> tuple[bool, str, str | None]:
    PROFILE_DIR.mkdir(parents=True, exist_ok=True)

    async with async_playwright() as p:
        context = await p.chromium.launch_persistent_context(
            user_data_dir=str(PROFILE_DIR),
            headless=HEADLESS,
            viewport={"width": 1440, "height": 1000},
            args=["--start-maximized"],
        )

        try:
            page = context.pages[0] if context.pages else await context.new_page()
            ok, reason = await is_logged_in(page)

            if ok:
                return True, reason, None

            screenshot_path = SCREENSHOT_DIR / f"{PROFILE_KEY}_check_failed.png"
            await page.screenshot(path=str(screenshot_path), full_page=True)
            return False, reason, str(screenshot_path)
        finally:
            await context.close()


async def main():
    ok, reason, screenshot_path = await run_check_once()

    if ok:
        print("FACEBOOK_STATUS=LOGGED_IN")
        print(f"REASON={reason}")
        return

    print("FACEBOOK_STATUS=NOT_LOGGED_IN")
    print(f"REASON={reason}")
    print(f"SCREENSHOT={screenshot_path}")
    print("ALERT_RESULT_1=skipped_first_attempt")
    print("FACEBOOK_RETRYING=1")

    await asyncio.sleep(5)

    ok2, reason2, screenshot_path2 = await run_check_once()

    if ok2:
        print("FACEBOOK_STATUS=RECOVERED_AFTER_RETRY")
        print("ALERT_RESULT_RECOVERY=not_sent_no_final_failure")
        return

    second_alert = send_alert(
        service="facebook",
        status="failure",
        reason=reason2,
        message="Facebook login/session check failed again after retry.",
        screenshot_path=screenshot_path2,
        extra={"check": "facebook_login", "attempt": 2, "profile_key": PROFILE_KEY},
    )

    print("FACEBOOK_STATUS=FAILED_AFTER_RETRY")
    print(f"REASON_2={reason2}")
    print(f"SCREENSHOT_2={screenshot_path2}")
    print(f"ALERT_RESULT_2={second_alert}")


if __name__ == "__main__":
    asyncio.run(main())
