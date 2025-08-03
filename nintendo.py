import requests
from supabase import create_client, Client
import os

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY")
ALGOLIA_ID = os.getenv("ALGOLIA_ID")
ALGOLIA_API_KEY = os.getenv("ALGOLIA_API_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)

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
            -hit['price']['percentOff'],
            int(round(hit['price']['finalPrice'] * 100)),
            int(round(hit['price']['regPrice'] * 100)),
            f"https://assets.nintendo.com/image/upload/ar_16:9,c_lpad,w_656/b_white/f_auto/q_auto/{hit['productImage']}",
            hit['platform'],
            f"https://www.nintendo.com{hit['url']}"
        ))

    cols = ['title', 'type', 'discount', 'price', 'original_price', 'art', 'platforms', 'link']
    records = [dict(zip(cols, row)) for row in values]
    unique_records = [dict(t) for t in {tuple(sorted(d.items())) for d in records}]
    supabase.table('nintendo').upsert(unique_records, on_conflict="title,price,link").execute()

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


