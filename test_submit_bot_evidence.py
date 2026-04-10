import os
import json
import requests
from dotenv import load_dotenv

load_dotenv("/home/bamanio/social-bot/.env")

url = os.getenv("SUBMIT_BOT_EVIDENCE_URL")
secret = os.getenv("BOT_SECRET")

resp = requests.post(
    url,
    headers={
        "x-bot-secret": secret,
        "Content-Type": "application/json",
    },
    json={
        "job_id": "69af6cecd096c787cf0f81be",
        "recipe_id": "69af6c6cc8b3349ba6b347a9",
        "target_url": "https://www.instagram.com/p/DVba2rhDAT0/",
        "screenshot_url": "https://base44.app/api/apps/69ad9b8c06689adb44280cf2/files/public/69ad9b8c06689adb44280cf2/e93420c54_69af6cecd096c787cf0f81be.png",
        "raw_page_text": "test",
        "page_title": "Instagram",
        "media_type_guess": "image",
        "debug_data": json.dumps({"test": True}),
    },
    timeout=20,
)

print(resp.status_code)
print(resp.text[:2000])
