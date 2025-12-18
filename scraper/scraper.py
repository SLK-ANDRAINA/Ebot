import requests
import time
import random
from scraper.parser import parse_card

ITEMS_PER_PAGE = 200
BATCH_SIZE = 50
TIMEOUT = 20
PROXY_SWITCH_ITEMS = 5000
WEBSHARE_PROXY_URL = "https://proxy.webshare.io/api/v2/proxy/list/download/orvijslzdksyvlmhxmbqvrbtwxceszerdgkjuhps/-/any/username/direct/-/?plan_id=12426655"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "application/json",
    "Referer": "https://www.ebay.com/",
}

# ========================
# Charger les proxies Webshare
# ========================
def load_proxies_from_webshare():
    proxies = []
    try:
        r = requests.get(WEBSHARE_PROXY_URL, timeout=10)
        r.raise_for_status()
        for line in r.text.strip().split("\n"):
            if not line:
                continue
            ip, port, user, pwd = line.strip().split(":")
            proxy_url = f"http://{user}:{pwd}@{ip}:{port}"
            proxies.append({"http": proxy_url, "https": proxy_url})
    except Exception as e:
        print(f"⚠️ Impossible de charger proxies Webshare: {e}")
    return proxies

PROXIES = load_proxies_from_webshare()

def get_random_proxy():
    if not PROXIES:
        return None
    return random.choice(PROXIES)

# ========================
# Récupérer les cartes d'une page
# ========================
def get_cards_from_page(base_url, page, proxy=None):
    params = {
        "_tab": "shop",
        "_pgn": page,
        "_ipg": ITEMS_PER_PAGE,
        "_ajax": "itemFilter",
    }
    try:
        r = requests.get(base_url, headers=HEADERS, params=params, timeout=TIMEOUT, proxies=proxy)
        r.raise_for_status()
        data = r.json()
        containers = data.get("modules", {}).get("LISTINGS_MODULE", {}).get("containers", [])
        return [card for c in containers for card in c.get("cards", [])]
    except Exception as e:
        raise RuntimeError(f"❌ Impossible de scraper la page {page} avec proxy={proxy}: {e}")

# ========================
# Scraping principal
# ========================
def scrape_store(base_url, on_batch):
    page = 1
    total = 0
    proxy = get_random_proxy()
    use_proxy = True

    # === Test du proxy sur la première page ===
    if proxy:
        print(f"🔎 Test page 1 avec proxy: {proxy}")
        try:
            cards = get_cards_from_page(base_url, page, proxy)
        except Exception as e:
            print(f"⚠️ Proxy ne fonctionne pas, on continue sans proxy: {e}")
            proxy = None
            use_proxy = False
            cards = get_cards_from_page(base_url, page, None)
    else:
        cards = get_cards_from_page(base_url, page, None)

    while True:
        if page > 1:
            try:
                cards = get_cards_from_page(base_url, page, proxy if use_proxy else None)
            except Exception as e:
                print(f"❌ Scraping error à la page {page}: {e}")
                break

        if not cards:
            break

        items = [parse_card(card) for card in cards]

        # Envoyer par batch
        for i in range(0, len(items), BATCH_SIZE):
            batch = items[i:i + BATCH_SIZE]
            on_batch(batch)

        total += len(items)
        print(f"📦 Page {page} | {len(items)} items | Total: {total}")

        # === Changement/retest de proxy tous les PROXY_SWITCH_ITEMS ===
        if total % PROXY_SWITCH_ITEMS == 0:
            if not use_proxy:
                test_proxy = get_random_proxy()
                if test_proxy:
                    print(f"🔄 Retest proxy après {total} items: {test_proxy}")
                    try:
                        _ = get_cards_from_page(base_url, page, test_proxy)
                        proxy = test_proxy
                        use_proxy = True
                        print("✅ Proxy valide, on continue avec proxy")
                    except Exception:
                        print("⚠️ Proxy toujours non valide, on continue sans proxy")
                        proxy = None
                        use_proxy = False
            else:
                proxy = get_random_proxy()
                print(f"🔄 Changement de proxy après {total} items")

        if len(cards) < ITEMS_PER_PAGE:
            break

        page += 1
        time.sleep(random.uniform(0.4, 0.8))

    print(f"✅ Scraping terminé | Total items: {total}")
