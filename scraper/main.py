import requests
import time
from utils import parse_card

ITEMS_PER_PAGE = 200
API_URL = "http://127.0.0.1:8000/insert"
BATCH_SIZE = 50

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "application/json",
}

def get_cards_from_page(base_url, page):
    params = {
        "_tab": "shop",
        "_pgn": page,
        "_ipg": ITEMS_PER_PAGE,
        "_ajax": "itemFilter",
    }

    r = requests.get(base_url, headers=HEADERS, params=params, timeout=20)
    r.raise_for_status()

    data = r.json()
    containers = data.get("modules", {}) \
                     .get("LISTINGS_MODULE", {}) \
                     .get("containers", [])

    return [card for c in containers for card in c.get("cards", [])]

def send_batches(items):
    for i in range(0, len(items), BATCH_SIZE):
        batch = items[i:i + BATCH_SIZE]
        try:
            r = requests.post(API_URL, json=batch, timeout=120)
            if r.status_code == 200:
                print(f"✅ Batch envoyé : {len(batch)}")
            else:
                print(f"❌ API error {r.status_code} : {r.text}")
        except Exception as e:
            print(f"❌ API exception : {e}")

def scrape_and_stream(base_url):
    page = 1
    total = 0
    start = time.time()

    while True:
        print(f"\n🔎 Scraping page {page}")
        t0 = time.time()

        cards = get_cards_from_page(base_url, page)
        if not cards:
            print("🏁 Fin du scraping")
            break

        items = [parse_card(card) for card in cards]
        send_batches(items)

        total += len(items)
        print(f"📦 Page {page} | {len(items)} items | {time.time() - t0:.2f}s")

        if len(cards) < ITEMS_PER_PAGE:
            break

        page += 1
        time.sleep(0.3)

    print("\n===== RÉSUMÉ =====")
    print(f"Total items : {total}")
    print(f"Durée totale : {time.time() - start:.2f}s")

if __name__ == "__main__":
    url = input("Entrez l'URL du store eBay : ").strip()
    scrape_and_stream(url)
