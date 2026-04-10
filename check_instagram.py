import asyncio
import os
import re
import unicodedata
from pathlib import Path

from dotenv import load_dotenv
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright

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
CHECK_URL = "https://www.instagram.com/accounts/edit/"


def sanitize_profile_segment(value: str) -> str:
    raw = unicodedata.normalize("NFKC", str(value or "").strip().lower())
    if not raw:
        return ""
    collapsed = re.sub(r"[^a-z0-9._-]+", "-", raw).strip("-._")
    return collapsed[:80]


def profile_key_for(platform: str, collector_profile_id: str = "") -> str:
    base = platform if platform in {"instagram", "facebook", "tiktok", "youtube"} else "web"
    profile_segment = sanitize_profile_segment(collector_profile_id)
    if profile_segment and base in {"instagram", "facebook", "tiktok", "youtube"}:
        return f"{base}__{profile_segment}"
    return base


def profile_dir_for_instagram() -> Path:
    return PROFILE_ROOT / profile_key_for("instagram", COLLECTOR_PROFILE_ID)


async def is_logged_in(page) -> tuple[bool, str]:
    try:
        await page.goto(CHECK_URL, wait_until="domcontentloaded", timeout=30000)

        try:
            await page.wait_for_load_state("networkidle", timeout=5000)
        except Exception:
            pass

        await page.wait_for_timeout(2500)

        url = (page.url or "").lower()

        bad_parts = [
            "/accounts/login",
            "/challenge/",
            "/checkpoint/",
            "/accounts/suspended",
        ]
        for part in bad_parts:
            if part in url:
                return False, f"redirected_to_{part.strip('/').replace('/', '_')}"

        body_text = ""
        try:
            body_text = (await page.locator("body").inner_text(timeout=3000)).lower()
        except Exception:
            body_text = ""

        if "log in" in body_text and "sign up" in body_text:
            return False, "logged_out_ui_visible"

        password_like_selectors = [
            'input[name="password"]',
            'input[name="enc_password"]',
            'input[type="password"]',
        ]
        for selector in password_like_selectors:
            try:
                if await page.locator(selector).count() > 0:
                    return False, "login_form_visible"
            except Exception:
                pass

        if "/accounts/edit" in url:
            return True, "ok_accounts_edit"

        logged_in_shell_selectors = [
            'svg[aria-label="Home"]',
            'a[href="/direct/inbox/"]',
            'a[href="/accounts/edit/"]',
        ]
        for selector in logged_in_shell_selectors:
            try:
                await page.wait_for_selector(selector, timeout=2000)
                return True, "ok_logged_in_shell"
            except Exception:
                pass

        return False, "unknown_not_logged_in_state"

    except PlaywrightTimeoutError:
        return False, "timeout"
    except Exception as e:
        return False, f"exception_{type(e).__name__}"


async def run_check_once() -> tuple[bool, str, str | None]:
    profile_dir = profile_dir_for_instagram()
    profile_dir.mkdir(parents=True, exist_ok=True)

    async with async_playwright() as p:
        context = await p.chromium.launch_persistent_context(
            user_data_dir=str(profile_dir),
            headless=HEADLESS,
            viewport={"width": 1440, "height": 1000},
            args=["--start-maximized"],
        )

        try:
            page = context.pages[0] if context.pages else await context.new_page()
            ok, reason = await is_logged_in(page)

            if ok:
                return True, reason, None

            screenshot_path = SCREENSHOT_DIR / f"{profile_key_for('instagram', COLLECTOR_PROFILE_ID)}_check_failed.png"
            await page.screenshot(path=str(screenshot_path), full_page=True)
            return False, reason, str(screenshot_path)
        finally:
            await context.close()


async def main():
    ok, reason, screenshot_path = await run_check_once()

    if ok:
        print("INSTAGRAM_STATUS=LOGGED_IN")
        print(f"REASON={reason}")
        return

    first_alert = send_alert(
        service="instagram",
        status="failure",
        reason=reason,
        message="Instagram login/session check failed on first attempt.",
        screenshot_path=screenshot_path,
        extra={
            "check": "instagram_login",
            "attempt": 1,
            "profile_key": profile_key_for("instagram", COLLECTOR_PROFILE_ID),
        },
    )

    print("INSTAGRAM_STATUS=NOT_LOGGED_IN")
    print(f"REASON={reason}")
    print(f"SCREENSHOT={screenshot_path}")
    print(f"ALERT_RESULT_1={first_alert}")
    print("INSTAGRAM_RETRYING=1")

    await asyncio.sleep(5)

    ok2, reason2, screenshot_path2 = await run_check_once()

    if ok2:
        recovery_alert = send_alert(
            service="instagram",
            status="recovered",
            reason="recovered_after_retry",
            message="Instagram check failed first, but recovered on retry.",
            screenshot_path=screenshot_path2,
            extra={
                "check": "instagram_login",
                "attempt": 2,
                "profile_key": profile_key_for("instagram", COLLECTOR_PROFILE_ID),
            },
        )
        print("INSTAGRAM_STATUS=RECOVERED_AFTER_RETRY")
        print(f"REASON_2={reason2}")
        print(f"ALERT_RESULT_RECOVERY={recovery_alert}")
        return

    second_alert = send_alert(
        service="instagram",
        status="failure",
        reason=reason2,
        message="Instagram login/session check failed again after retry.",
        screenshot_path=screenshot_path2,
        extra={
            "check": "instagram_login",
            "attempt": 2,
            "profile_key": profile_key_for("instagram", COLLECTOR_PROFILE_ID),
        },
    )

    print("INSTAGRAM_STATUS=FAILED_AFTER_RETRY")
    print(f"REASON_2={reason2}")
    print(f"SCREENSHOT_2={screenshot_path2}")
    print(f"ALERT_RESULT_2={second_alert}")


if __name__ == "__main__":
    asyncio.run(main())
