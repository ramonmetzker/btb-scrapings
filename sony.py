import requests
from bs4 import BeautifulSoup
import os
import libsql

def convert_price(price):
    normalized = price.replace("R$", "").replace(".", "").replace(",", ".").strip()
    return int(float(normalized) * 100)

def parse_discount(discount_str):
    return float(discount_str.replace("%", "").strip()) / 100

db_url = os.getenv("TURSO_DB_URL")
db_token = os.getenv("TURSO_AUTH_TOKEN")

conn = libsql.connect(
    syncurl=db_url,
    auth_token=db_token
)

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
        print("==================================")

    for i in range(1, total_pages + 1):
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
            original_price = card.select_one('[data-qa$="#price-strikethrough"]').getText() if card.select_one('[data-qa$="#price-strikethrough"]') else discount_price
            discount = card.select_one('[data-qa$="#discount-badge#text"]').getText()
            platforms = card.select_one('[data-qa$="#game-art"]').select('span.psw-platform-tag')
            all_platforms = ", ".join(span.getText() for span in platforms)
            link = store + card.select_one('a.psw-link').attrs.get('href')

            values.append((title, game_type, parse_discount(discount), convert_price(discount_price), convert_price(original_price), game_art, all_platforms, link))

        print(f"[PAGE {i}] Total: {len(values)} games")

        for record in values:
            conn.execute(
                """
                INSERT INTO sony (title, type, discount, price, original_price, art, platforms, link)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                ON CONFLICT(title, price, link) DO UPDATE SET
                    id=excluded.id,
                    discount=excluded.discount,
                    original_price=excluded.original_price,
                    art=excluded.art,
                    platforms=excluded.platforms;
                """,
                record
            )

        print(f"✔️ Done Page {i}")

    print("✅ All Done")
