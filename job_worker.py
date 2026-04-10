import os
import time
import subprocess
from dotenv import load_dotenv

load_dotenv("/home/bamanio/social-bot/.env")

SHORT_SLEEP_SECONDS = int(os.getenv("JOB_SHORT_SLEEP_SECONDS", "5"))
LONG_SLEEP_SECONDS = int(os.getenv("JOB_LONG_SLEEP_SECONDS", "30"))
MAX_JOBS_PER_CYCLE = int(os.getenv("MAX_JOBS_PER_CYCLE", "1"))

PROCESS_ONE_JOB = "/home/bamanio/social-bot/app/process_one_job.py"
PYTHON_BIN = "/home/bamanio/social-bot/.venv/bin/python"


def run_one_job():
    result = subprocess.run(
        [PYTHON_BIN, PROCESS_ONE_JOB],
        capture_output=True,
        text=True,
        cwd="/home/bamanio/social-bot/app",
    )

    stdout = (result.stdout or "").strip()
    stderr = (result.stderr or "").strip()

    if stdout:
        print(stdout, flush=True)
    if stderr:
        print(stderr, flush=True)

    return stdout, stderr, result.returncode


def main():
    while True:
        jobs_processed = 0

        while jobs_processed < MAX_JOBS_PER_CYCLE:
            print("JOB_WORKER_RUN_START", flush=True)
            stdout, stderr, code = run_one_job()

            combined = f"{stdout}\n{stderr}"

            if "NO_PENDING_JOBS" in combined:
                break

            jobs_processed += 1

            if jobs_processed < MAX_JOBS_PER_CYCLE:
                print(f"JOB_WORKER_SLEEP={SHORT_SLEEP_SECONDS}", flush=True)
                time.sleep(SHORT_SLEEP_SECONDS)

        if jobs_processed == 0:
            print(f"JOB_WORKER_SLEEP={LONG_SLEEP_SECONDS}", flush=True)
            time.sleep(LONG_SLEEP_SECONDS)
        else:
            print(f"JOB_WORKER_SLEEP={SHORT_SLEEP_SECONDS}", flush=True)
            time.sleep(SHORT_SLEEP_SECONDS)


if __name__ == "__main__":
    main()
