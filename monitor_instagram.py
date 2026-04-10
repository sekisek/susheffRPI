import asyncio
import os
from dotenv import load_dotenv
from check_instagram import main as check_main

load_dotenv()
CHECK_INTERVAL_SECONDS = int(os.getenv("CHECK_INTERVAL_SECONDS", "1500"))


async def loop_forever():
    while True:
        print("INSTAGRAM_MONITOR_RUN_START")
        try:
            await check_main()
        except Exception as e:
            print(f"INSTAGRAM_MONITOR_EXCEPTION={type(e).__name__}: {e}")

        print(f"INSTAGRAM_MONITOR_SLEEP={CHECK_INTERVAL_SECONDS}")
        try:
            await asyncio.sleep(CHECK_INTERVAL_SECONDS)
        except asyncio.CancelledError:
            print("INSTAGRAM_MONITOR_STOPPED")
            break


if __name__ == "__main__":
    try:
        asyncio.run(loop_forever())
    except KeyboardInterrupt:
        print("INSTAGRAM_MONITOR_EXIT")
