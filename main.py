from pathlib import Path
from dotenv import load_dotenv
import os
import time

load_dotenv()

BASE = Path.home() / "social-bot"
for name in ["app", "profiles", "logs", "screenshots", "tmp"]:
    (BASE / name).mkdir(parents=True, exist_ok=True)

print("social-bot base setup OK")
print("API_BASE_URL =", os.getenv("API_BASE_URL"))
print("HEADLESS =", os.getenv("HEADLESS"))
print("CHECK_INTERVAL_SECONDS =", os.getenv("CHECK_INTERVAL_SECONDS"))
print("JOB_POLL_SECONDS =", os.getenv("JOB_POLL_SECONDS"))
