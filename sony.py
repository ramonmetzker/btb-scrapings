import requests
from bs4 import BeautifulSoup
from supabase import create_client, Client
import os

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)

def convert_price(price):
    normalized = price.replace("R$", "").replace(".", "").replace(",", ".").strip()
    return int(float(normalized) * 100)

def parse_discount(discount_str):
    return float(discount_str.replace("%", "").strip()) / 100

url = "https://store.playstation.com/pt-br/category/3f772501-f6f8-49b7-abac-874a88ca4897/1?FULL_GAME=storeDisplayClassification"
store = "https://store.playstation.com"

response = requests.get(url)

if response.status_code == 200:
    soup = BeautifulSoup(response.text, 'html.parser')
    total_pages = 1
    ols = soup.find_all('ol')

    for ol in ols:
        lis = ol.find_all('li')
        li = lis[-1]

        span = li.find('span')

        total_pages = int(span.getText())
        print(f"[TOTAL PAGES]: {total_pages}")
        print(f"==================================")

    for i in range(1,total_pages + 1):
        page = requests.get(f"https://store.playstation.com/pt-br/category/3f772501-f6f8-49b7-abac-874a88ca4897/{i}?FULL_GAME=storeDisplayClassification")

        soup = BeautifulSoup(page.text, 'html.parser')

        grid = soup.select('.psw-grid-list')[0]

        cards = grid.find_all('li')

        values = []

        for card in cards:
            title = card.select_one('[data-qa$="#product-name"]').getText()
            game_art = card.select_one('[data-qa$="#image#image-no-js"]').attrs.get('src')
            game_type = card.select_one('[data-qa$="#product-type"]').getText() if card.select_one('[data-qa$="#product-type"]') else "Jogo completo"
            discount_price = card.select_one('[data-qa$="#price#display-price"]').getText()
            original_price = card.select_one('[data-qa$="#price-strikethrough"]').getText() if card.select_one('[data-qa$="#price-strikethrough"]') else None
            discount = card.select_one('[data-qa$="#discount-badge#text"]').getText()
            platforms = card.select_one('[data-qa$="#game-art"]').select('span.psw-platform-tag')
            all_platforms = ", ".join(span.getText() for span in platforms)
            link = store + card.select_one('a.psw-link').attrs.get('href')
            values.append((title,game_type,parse_discount(discount),convert_price(discount_price),convert_price(original_price),game_art,all_platforms, link))

        cols = ['title','type','discount','price','original_price','art','platforms','link']
        records = [dict(zip(cols, row)) for row in values]
        supabase.table('sony').upsert(records, on_conflict=["title", "price"],ignore_duplicates=True).execute()

        print(f"Done Page {i}")

    print("End")