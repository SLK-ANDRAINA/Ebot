import re
import time

def safe_truncate(v, n):
    if not v:
        return ""
    return str(v)[:n]

def parse_card(card):
    item_id = (
        card.get("listingId")
        or card.get("id")
        or card.get("presentityId")
        or f"item_{int(time.time()*1000)}"
    )
    item_id = safe_truncate(item_id, 50)

    seller_spans = card.get("__search", {}) \
        .get("sellerInfo", {}) \
        .get("text", {}) \
        .get("textSpans", [])

    seller_id = seller_spans[0]["text"].split(" ")[0] if seller_spans else "unknown"
    seller_id = safe_truncate(seller_id, 50)

    title_spans = card.get("title", {}).get("textSpans", [])
    title = safe_truncate(title_spans[0]["text"] if title_spans else "", 300)

    mpn = ""
    for s in card.get("itemSpecifics", []):
        if s.get("name", "").lower() in ("mpn", "oem"):
            mpn = s.get("value", "")
            break
    mpn = safe_truncate(mpn, 100)

    price = card.get("displayPrice", {}).get("value", {}).get("value", 0.0)

    delivery_text = ""
    d_spans = card.get("logisticsCost", {}).get("textSpans", [])
    if d_spans:
        delivery_text = d_spans[0]["text"]
    delivery = float(re.sub(r"[^\d.]", "", delivery_text)) if delivery_text else 0.0

    qty_text = ""
    q_spans = card.get("quantity", {}).get("textSpans", [])
    if q_spans:
        qty_text = q_spans[0]["text"]
    quantity = int(re.sub(r"[^\d]", "", qty_text)) if qty_text else 0

    category = safe_truncate(card.get("category", {}).get("displayName", ""), 100)
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
        "image": image,
    }
