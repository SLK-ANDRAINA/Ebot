from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import List, Optional
import asyncpg
import ssl, certifi
from datetime import datetime
from scraper.scraper import scrape_store

app = FastAPI(title="Ebay Scraper API")

DATABASE_URL = "postgresql://neondb_owner:npg_cGFD6UTO2Mfe@ep-quiet-morning-ahibb1kr-pooler.c-3.us-east-1.aws.neon.tech/neondb?sslmode=require"

# --------------------- MODELS ---------------------
class ScrapeRequest(BaseModel):
    url: str

class Item(BaseModel):
    item_id: str
    seller_id: str
    title: str
    mpn: str = ""
    price: float
    delivery: float
    quantity: int
    category: str = ""
    link: str = ""
    image: str = ""

class ItemUpdate(BaseModel):
    title: Optional[str]
    mpn: Optional[str]
    price: Optional[float]
    delivery: Optional[float]
    quantity: Optional[int]
    category: Optional[str]
    link: Optional[str]
    image: Optional[str]

# --------------------- DB ---------------------
@app.on_event("startup")
async def startup():
    ssl_ctx = ssl.create_default_context(cafile=certifi.where())
    app.state.pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10, ssl=ssl_ctx)
    print("🔌 DB connectée")

@app.on_event("shutdown")
async def shutdown():
    await app.state.pool.close()
    print("🔌 DB fermée")

async def insert_items_db(items: List[Item]):
    pool = app.state.pool
    now = datetime.now()
    today = now.date()

    sellers = [(i.seller_id, now) for i in items]
    products = [(i.item_id, i.seller_id, i.title, i.mpn, i.category, i.link, i.image) for i in items]
    logs = [(i.item_id, today, i.price, i.delivery, i.quantity) for i in items]

    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.executemany("""
                INSERT INTO sellers (seller_id, last_scan)
                VALUES ($1,$2)
                ON CONFLICT (seller_id) DO UPDATE SET last_scan = EXCLUDED.last_scan
            """, sellers)
            await conn.executemany("""
                INSERT INTO products
                (item_id, seller_id, title, mpn_oem, category, url, image_url)
                VALUES ($1,$2,$3,$4,$5,$6,$7)
                ON CONFLICT (item_id) DO NOTHING
            """, products)
            await conn.executemany("""
                INSERT INTO daily_logs
                (item_id, log_date, price, shipping, stock_level)
                VALUES ($1,$2,$3,$4,$5)
                ON CONFLICT (item_id, log_date)
                DO UPDATE SET price = EXCLUDED.price, shipping = EXCLUDED.shipping, stock_level = EXCLUDED.stock_level
            """, logs)

# --------------------- ROUTES ---------------------
@app.get("/status")
def status():
    return {"status": "ok"}

@app.post("/scrape")
def scrape(req: ScrapeRequest, background_tasks: BackgroundTasks):
    """
    Lance le scraping en arrière-plan
    """
    import asyncio

    async def run_scraping(url: str):
        async def on_batch(batch):
            await insert_items_db([Item(**i) for i in batch])

        loop = asyncio.get_event_loop()
        def callback(batch):
            asyncio.run_coroutine_threadsafe(on_batch(batch), loop)

        scrape_store(url, callback)

    background_tasks.add_task(run_scraping, req.url)
    return {"status": "scraping_started"}

# --------------------- CRUD ITEMS ---------------------
@app.get("/items", response_model=List[Item])
async def list_items():
    pool = app.state.pool
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM products")
        return [Item(
            item_id=r["item_id"], seller_id=r["seller_id"], title=r["title"],
            mpn=r["mpn_oem"], price=0, delivery=0, quantity=0,
            category=r["category"], link=r["url"], image=r["image_url"]
        ) for r in rows]

@app.get("/items/{item_id}", response_model=Item)
async def get_item(item_id: str):
    pool = app.state.pool
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM products WHERE item_id=$1", item_id)
        if not row:
            raise HTTPException(status_code=404, detail="Item not found")
        return Item(
            item_id=row["item_id"], seller_id=row["seller_id"], title=row["title"],
            mpn=row["mpn_oem"], price=0, delivery=0, quantity=0,
            category=row["category"], link=row["url"], image=row["image_url"]
        )

@app.post("/items", response_model=Item)
async def create_item(item: Item):
    pool = app.state.pool
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO products (item_id, seller_id, title, mpn_oem, category, url, image_url)
            VALUES ($1,$2,$3,$4,$5,$6,$7)
            ON CONFLICT (item_id) DO NOTHING
        """, item.item_id, item.seller_id, item.title, item.mpn, item.category, item.link, item.image)
    return item

@app.put("/items/{item_id}", response_model=Item)
async def update_item(item_id: str, item: ItemUpdate):
    pool = app.state.pool
    async with pool.acquire() as conn:
        # Build dynamic set clause
        set_clause = []
        values = []
        for field, val in item.dict(exclude_unset=True).items():
            set_clause.append(f"{field if field != 'mpn' else 'mpn_oem'} = ${len(values)+1}")
            values.append(val)
        if not set_clause:
            raise HTTPException(status_code=400, detail="No fields to update")
        values.append(item_id)
        query = f"UPDATE products SET {', '.join(set_clause)} WHERE item_id = ${len(values)}"
        await conn.execute(query, *values)

        row = await conn.fetchrow("SELECT * FROM products WHERE item_id=$1", item_id)
        return Item(
            item_id=row["item_id"], seller_id=row["seller_id"], title=row["title"],
            mpn=row["mpn_oem"], price=0, delivery=0, quantity=0,
            category=row["category"], link=row["url"], image=row["image_url"]
        )

@app.delete("/items/{item_id}")
async def delete_item(item_id: str):
    pool = app.state.pool
    async with pool.acquire() as conn:
        result = await conn.execute("DELETE FROM products WHERE item_id=$1", item_id)
    return {"status": "deleted", "item_id": item_id}
