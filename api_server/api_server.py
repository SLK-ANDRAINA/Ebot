from fastapi import FastAPI, BackgroundTasks
from pydantic import BaseModel
from typing import List
import asyncpg
from datetime import datetime
import ssl
import certifi

app = FastAPI(title="Ebay Scraper API")

DATABASE_URL = "postgresql://neondb_owner:npg_cGFD6UTO2Mfe@ep-quiet-morning-ahibb1kr-pooler.c-3.us-east-1.aws.neon.tech/neondb?sslmode=require"

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

@app.on_event("startup")
async def startup():
    ssl_ctx = ssl.create_default_context(cafile=certifi.where())
    app.state.pool = await asyncpg.create_pool(
        DATABASE_URL,
        min_size=1,
        max_size=10,
        ssl=ssl_ctx,
    )
    print("🔌 Pool connecté")

@app.on_event("shutdown")
async def shutdown():
    await app.state.pool.close()
    print("🔌 Pool fermé")

@app.post("/insert")
async def insert_items(items: List[Item], background_tasks: BackgroundTasks):
    background_tasks.add_task(insert_items_task, items)
    return {"status": "accepted", "count": len(items)}

async def insert_items_task(items: List[Item]):
    pool = app.state.pool
    now = datetime.now()
    today = now.date()

    sellers = [(i.seller_id, now) for i in items]
    products = [
        (i.item_id, i.seller_id, i.title, i.mpn, i.category, i.link, i.image)
        for i in items
    ]
    logs = [
        (i.item_id, today, i.price, i.delivery, i.quantity)
        for i in items
    ]

    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.executemany("""
                INSERT INTO sellers (seller_id, last_scan)
                VALUES ($1,$2)
                ON CONFLICT (seller_id)
                DO UPDATE SET last_scan = EXCLUDED.last_scan
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
                DO UPDATE SET
                    price = EXCLUDED.price,
                    shipping = EXCLUDED.shipping,
                    stock_level = EXCLUDED.stock_level
            """, logs)
