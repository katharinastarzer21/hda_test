"""
Delete all Pushgateway groups where service=hda.
"""

import os
import requests

URL  = os.environ["PUSHGATEWAY_URL"].rstrip("/")
AUTH = (os.environ["PUSHGATEWAY_USERNAME"], os.environ["PUSHGATEWAY_PASSWORD"])

r = requests.get(f"{URL}/api/v1/metrics", auth=AUTH, allow_redirects=True, timeout=20)
r.raise_for_status()

groups = r.json().get("data", [])
hda_groups = [g for g in groups if g.get("labels", {}).get("service") == "hda"]

print(f"Found {len(hda_groups)} hda groups to delete")

for group in hda_groups:
    labels = group.get("labels", {})
    job    = labels.get("job", "hda_monitor")
    path   = "/".join(f"{k}/{v}" for k, v in sorted(labels.items()) if k != "job")
    url    = f"{URL}/metrics/job/{job}/{path}"

    resp = requests.delete(url, auth=AUTH, allow_redirects=True, timeout=15)
    print(f"  {resp.status_code}  {path}")

print("Done.")
