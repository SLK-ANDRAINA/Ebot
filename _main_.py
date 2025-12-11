import requests
import time
import pandas as pd
import os
import re
import psycopg2
from datetime import datetime

# ===========================
# CONFIGURATION
# ===========================
ITEMS_PER_PAGE = 200
CSV_FILE = "ebay_items_page_by_page.csv"
DATABASE_URL = "postgresql://neondb_owner:npg_cGFD6UTO2Mfe@ep-quiet-morning-ahibb1kr-pooler.c-3.us-east-1.aws.neon.tech/neondb?sslmode=require"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "application/json",
}

# ===========================
# FONCTIONS UTILES
# ===========================
def safe_truncate(s, length):
    if not s:
        return ""
    return str(s)[:length]

# ===========================
# CREATION DES TABLES
# ===========================
def create_tables_if_not_exists():
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    
    cur.execute("""
    CREATE TABLE IF NOT EXISTS sellers (
        seller_id VARCHAR(50) PRIMARY KEY,
        last_scan TIMESTAMP
    );
    """)
    
    cur.execute("""
    CREATE TABLE IF NOT EXISTS products (
        item_id VARCHAR(50) PRIMARY KEY,
        seller_id VARCHAR(50) REFERENCES sellers(seller_id),
        title TEXT,
        mpn_oem VARCHAR(100),
        category VARCHAR(100),
        url TEXT,
        image_url TEXT,
        created_at TIMESTAMP DEFAULT NOW()
    );
    """)
    
    cur.execute("""
    CREATE TABLE IF NOT EXISTS daily_logs (
        id SERIAL PRIMARY KEY,
        item_id VARCHAR(50) REFERENCES products(item_id),
        log_date DATE NOT NULL,
        price DECIMAL(10,2),
        shipping DECIMAL(10,2),
        stock_level INT,
        sales_estimated INT DEFAULT 0,
        UNIQUE(item_id, log_date)
    );
    """)
    
    conn.commit()
    cur.close()
    conn.close()
    print("✅ Tables créées ou vérifiées avec succès.")

# ===========================
# FONCTIONS SCRAPING
# ===========================
def get_cards_from_page(base_url, page=1):
    params = {
        "_tab": "shop",
        "_pgn": page,
        "_ipg": ITEMS_PER_PAGE,
        "_trksid": "p2334524.m570.l113337",
        "_ajax": "itemFilter",
        "_tabName": "shop"
    }
    response = requests.get(base_url, headers=HEADERS, params=params)
    data = response.json()
    containers = data.get("modules", {}).get("LISTINGS_MODULE", {}).get("containers", [])
    all_cards = [card for container in containers for card in container.get("cards", [])]
    return all_cards

def parse_card(card):
    item_id = card.get("listingId") or card.get("id") or card.get("presentityId") or f"item_{int(time.time()*1000)}"
    item_id = safe_truncate(item_id, 50)

    seller_text = card.get("__search", {}).get("sellerInfo", {}).get("text", {}).get("textSpans", [])
    seller_id = seller_text[0].get("text", "").split(" ")[0] if seller_text else "unknown_seller"
    seller_id = safe_truncate(seller_id, 50)

    title_spans = card.get("title", {}).get("textSpans", [])
    title = title_spans[0].get("text", "") if title_spans else ""
    title = safe_truncate(title, 300)

    mpn = ""
    if "itemSpecifics" in card:
        for spec in card["itemSpecifics"]:
            if spec.get("name", "").lower() in ["mpn", "oem"]:
                mpn = spec.get("value", "")
                break
    mpn = safe_truncate(mpn, 100)

    display = card.get("displayPrice", {}).get("value", {})
    price = display.get("value") if display else 0.0

    delivery_spans = card.get("logisticsCost", {}).get("textSpans", [])
    delivery_text = delivery_spans[0].get("text", "") if delivery_spans else ""
    delivery = float(re.sub(r"[^\d\.]", "", delivery_text)) if delivery_text else 0.0

    qty_spans = card.get("quantity", {}).get("textSpans", [])
    qty_text = qty_spans[0].get("text", "") if qty_spans else ""
    quantity = int(re.sub(r"[^\d]", "", qty_text)) if qty_text else 0

    category = card.get("category", {}).get("displayName", "")
    category = safe_truncate(category, 100)

    link = card.get("action", {}).get("URL", "")
    image = card.get("image", {}).get("URL", "")

    return {
        "item_id": item_id,
        "seller_id": seller_id,
        "title": title,
        "mpn": mpn,
        "price": price,
        "delivery": delivery,
        "quantity": quantity,
        "category": category,
        "link": link,
        "image": image
    }

def scrape_pages_and_save(base_url):
    page = 1
    all_items = []
    page_times = []

    scraping_start = time.time()
    while True:
        start_page = time.time()
        print(f"Scraping page {page}...")
        cards = get_cards_from_page(base_url, page)
        if not cards:
            print("✅ No more items found. Finished scraping.")
            break
        items = [parse_card(card) for card in cards]
        all_items.extend(items)

        df = pd.DataFrame(items)
        if page == 1 or not os.path.exists(CSV_FILE):
            df.to_csv(CSV_FILE, index=False)
        else:
            df.to_csv(CSV_FILE, mode='a', index=False, header=False)

        page_duration = time.time() - start_page
        page_times.append(page_duration)
        print(f"Saved {len(items)} items from page {page} to '{CSV_FILE}' | Duration: {page_duration:.2f}s")

        if len(cards) < ITEMS_PER_PAGE:
            print("✅ Last page reached.")
            break
        page += 1
        time.sleep(0.5)

    scraping_duration = time.time() - scraping_start
    return pd.DataFrame(all_items), page_times, scraping_duration

# ===========================
# INSERTION DANS NEON PAR LOTS
# ===========================
def insert_into_db(df, batch_size=500):
    start_db = time.time()
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    # Insert sellers
    sellers = [(s, datetime.now()) for s in df['seller_id'].unique()]
    cur.executemany("""
        INSERT INTO sellers (seller_id, last_scan)
        VALUES (%s, %s)
        ON CONFLICT (seller_id) DO UPDATE
        SET last_scan = EXCLUDED.last_scan
    """, sellers)

    # Insert products in batches
    product_values = df[["item_id","seller_id","title","mpn","category","link","image"]].values.tolist()
    for i in range(0, len(product_values), batch_size):
        batch = product_values[i:i+batch_size]
        cur.executemany("""
            INSERT INTO products (item_id, seller_id, title, mpn_oem, category, url, image_url)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (item_id) DO NOTHING
        """, batch)

    # Insert daily_logs in batches
    daily_values = [(row["item_id"], datetime.now().date(), row["price"], row["delivery"], row["quantity"]) 
                    for _, row in df.iterrows()]
    for i in range(0, len(daily_values), batch_size):
        batch = daily_values[i:i+batch_size]
        cur.executemany("""
            INSERT INTO daily_logs (item_id, log_date, price, shipping, stock_level)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (item_id, log_date) DO UPDATE
            SET price = EXCLUDED.price,
                shipping = EXCLUDED.shipping,
                stock_level = EXCLUDED.stock_level
        """, batch)

    conn.commit()
    cur.close()
    conn.close()
    db_duration = time.time() - start_db
    return db_duration

# ===========================
# SCRIPT PRINCIPAL
# ===========================
if __name__ == "__main__":
    create_tables_if_not_exists()
    base_url = input("Entrez l'URL du store eBay : ").strip()

    df, page_times, scraping_duration = scrape_pages_and_save(base_url)
    db_duration = insert_into_db(df)

    total_items = len(df)
    print("\n===== RÉSUMÉ DU SCRAPING =====")
    print(f"Total items scrapped: {total_items}")
    for i, t in enumerate(page_times, 1):
        print(f"  Page {i}: {t:.2f}s")
    print(f"Total scraping duration: {scraping_duration:.2f}s")
    print(f"Database update duration: {db_duration:.2f}s")
    print(f"Total duration (scraping + DB): {scraping_duration + db_duration:.2f}s")
