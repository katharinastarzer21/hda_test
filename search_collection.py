import random
import requests
from urllib.parse import urljoin

BASE = "https://eodag.dev.services.eodc.eu"


def get_all_collections():
    url = f"{BASE}/collections"
    collections = []

    while url:
        response = requests.get(url, timeout=60)
        response.raise_for_status()

        data = response.json()
        collections.extend(data.get("collections", []))

        url = None

        for link in data.get("links", []):
            if link.get("rel") == "next":
                url = urljoin(BASE, link["href"])
                break

    return collections


collections = get_all_collections()

print(f"Collections gefunden: {len(collections)}")

with open("random_assets.txt", "w", encoding="utf-8") as f:
    for collection in collections:
        cid = collection["id"]

        try:
            response = requests.get(
                f"{BASE}/collections/{cid}/items",
                params={"limit": 2},
                timeout=60,
            )
            response.raise_for_status()
        except requests.RequestException as exc:
            print(f"Fehler bei {cid}: {exc}")
            continue

        items = response.json().get("features", [])

        if not items:
            print(f"Keine Items in {cid}")
            continue

        f.write(f"=== {cid} ===\n")

        for item in items:
            assets = [
                (name, asset)
                for name, asset in item.get("assets", {}).items()
                if asset.get("href")
            ]

            if not assets:
                print(f"Keine Assets für Item {item.get('id')}")
                continue

            # irgendein Asset aus diesem Item auswählen
            asset_name, asset = random.choice(assets)

            f.write(f"Item : {item['id']}\n")
            f.write(f"Asset: {asset_name}\n")
            f.write(f"Href : {asset['href']}\n\n")

        print(f"{cid}: {len(items)} Items verarbeitet")

print("Gespeichert in random_assets.txt")