import requests
import time
from scraper.parser import parse_card

ITEMS_PER_PAGE = 200
BATCH_SIZE = 50

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "application/json",
    "Referer": "https://www.ebay.com/",
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
    containers = data.get("modules", {}).get("LISTINGS_MODULE", {}).get("containers", [])
    return [card for c in containers for card in c.get("cards", [])]

def scrape_store(base_url, on_batch):
    page = 1
    total = 0

    while True:
        print(f"🔎 Page {page}")
        try:
            cards = get_cards_from_page(base_url, page)
        except Exception as e:
            print(f"❌ Scraping error: {e}")
            break

        if not cards:
            break

        items = [parse_card(card) for card in cards]

        # envoyer par batch
        for i in range(0, len(items), BATCH_SIZE):
            batch = items[i:i + BATCH_SIZE]
            on_batch(batch)

        total += len(items)
        print(f"📦 Page {page} | {len(items)} items")
        if len(cards) < ITEMS_PER_PAGE:
            break

        page += 1
        time.sleep(0.5)

    print(f"✅ Scraping terminé | Total items: {total}")
