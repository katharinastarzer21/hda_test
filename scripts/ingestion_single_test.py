from pystac_client import Client
import random

API_URL = "https://eodag.dev.services.eodc.eu"
COLLECTION = "OPERA_L2_RTC-S1_V1"

catalog = Client.open(API_URL)

search = catalog.search(
    collections=[COLLECTION]
)

# Gesamtzahl, falls die API das unterstützt
try:
    print("Anzahl gefundener Items:", search.matched())
except Exception as exc:
    print("API liefert keine direkte Gesamtzahl:", exc)

# Alle Seiten laden
items = list(search.items())

print("Tatsächlich geladene Items:", len(items))

# Höchstens 20 zufällige, unterschiedliche Items
selected = random.sample(items, min(20, len(items)))

for item in selected:
    print(item.id)