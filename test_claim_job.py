from bot_api import list_pending_instagram_jobs, claim_job

jobs = list_pending_instagram_jobs()

print("JOBS_FOUND =", len(jobs))

if not jobs:
    print("NO_PENDING_JOBS")
    raise SystemExit(0)

job = jobs[0]
print("FIRST_JOB =", job)

claimed = claim_job(job["id"])
print("CLAIMED_JOB =", claimed)

