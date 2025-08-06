import requests
import os
from libsql import connect

db_url = os.getenv("TURSO_DB_URL")
db_token = os.getenv("TURSO_AUTH_TOKEN")
ALGOLIA_ID = os.getenv("ALGOLIA_ID")
ALGOLIA_API_KEY = os.getenv("ALGOLIA_API_KEY")

conn = connect(sync_url=db_url, auth_token=db_token, database="gamedeals")

def parse_discount(discount_str):
    return float(discount_str.replace("%", "").strip()) / 100

def fetch_sales_page(page: int):
    body = {
      "filters": "(topLevelFilters:\"Promoções\")",
      "hitsPerPage": 50,
      "analytics": False,
      "facetingAfterDistinct": False,
      "clickAnalytics": False,
      "maxValuesPerFacet": 1,
      "page": page
    }
    response = requests.post(f"https://{ALGOLIA_ID}-dsn.algolia.net/1/indexes/store_game_pt_br/query", headers={"X-Algolia-Api-Key": ALGOLIA_API_KEY, "X-Algolia-Application-Id": ALGOLIA_ID}, json=body)

    res = response.json()

    return res

def upload_data(_data):
    values = []

    for hit in _data['hits']:
        values.append((
            hit['title'],
            hit['topLevelCategory'],
            parse_discount(f"{-hit['price']['percentOff']}%"),
            int(round(hit['price']['finalPrice'] * 100)),
            int(round(hit['price']['regPrice'] * 100)),
            f"https://assets.nintendo.com/image/upload/ar_16:9,c_lpad,w_656/b_white/f_auto/q_auto/{hit['productImage']}",
            hit['platform'],
            f"https://www.nintendo.com{hit['url']}"
        ))

    if values:
        conn.executemany(
            """
            INSERT INTO nintendo (title, type, discount, price, original_price, art, platforms, link)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
            ON CONFLICT(title, price, link) DO UPDATE SET
                discount = excluded.discount,
                original_price = excluded.original_price,
                art = excluded.art,
                platforms = excluded.platforms;
            """,
            values
        )
        conn.commit()

if __name__ == "__main__":
    initial = fetch_sales_page(0)

    pages = initial['nbPages']

    for i in range(pages-1):
        if i == 0:
            upload_data(initial)
            print(f'Done Page {i+1}')
            continue
        data = fetch_sales_page(i)
        upload_data(data)
        print(f'Done Page {i+1}')

    print("End")


