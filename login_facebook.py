import asyncio
import os
import re
from pathlib import Path

from dotenv import load_dotenv
from playwright.async_api import async_playwright

load_dotenv(Path.home() / "social-bot" / ".env", override=True)

BASE = Path.home() / "social-bot"
PROFILE_ROOT = BASE / "profiles"
COLLECTOR_PROFILE_ID = (
    os.getenv("COLLECTOR_PROFILE_ID", "").strip()
    or os.getenv("SOCIAL_COLLECTOR_PROFILE_ID", "").strip()
)
OPEN_URL = "https://www.facebook.com/"


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


async def main():
    PROFILE_DIR.mkdir(parents=True, exist_ok=True)

    async with async_playwright() as p:
        context = await p.chromium.launch_persistent_context(
            user_data_dir=str(PROFILE_DIR),
            headless=False,
            no_viewport=True,
            args=["--start-maximized"],
        )

        page = context.pages[0] if context.pages else await context.new_page()
        await page.goto(OPEN_URL, wait_until="domcontentloaded")

        print(f"\nManual Facebook login mode. profile_key={PROFILE_KEY}")
        print("Log in, solve anything needed, and make sure Facebook home loads.")
        print("Then run: python check_facebook.py")
        print("Press Enter here in the terminal to save and close.\n")
        input()

        await context.close()


if __name__ == "__main__":
    asyncio.run(main())
