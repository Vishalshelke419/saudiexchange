from playwright.sync_api import sync_playwright
from urllib.parse import urljoin
import pandas as pd

pd.set_option("display.max_colwidth", None)

URL = "https://www.saudiexchange.sa/wps/portal/saudiexchange/ourmarkets/main-market-watch?locale=en"
BASE = "https://www.saudiexchange.sa"

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page(user_agent="Mozilla/5.0")
    page.goto(URL, wait_until="domcontentloaded")
    page.wait_for_selector("#marketWatchTable1 tbody tr")

    # scroll until row count stabilizes
    last_count = -1
    stable = 0
    while stable < 2:
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(800)
        rows = page.locator("#marketWatchTable1 tbody tr")
        count = rows.count()
        if count == last_count:
            stable += 1
        else:
            stable = 0
            last_count = count

    companies = []
    rows = page.locator("#marketWatchTable1 tbody tr")

    name = ""
    code = ""
    sector = ""
    url = ""

    for i in range(rows.count()):
        row = rows.nth(i)
        txt = row.inner_text().strip()  
        txt = txt.replace('\t', '\n')  
              
        # print(f"Row {i+1}:\n{txt}\n" + "-"*40)        # printing all the text inside each row  

        row_text_content_list = txt.splitlines()

        # print(row_text_content_list)

        if len(row_text_content_list)==1:
            sector = row_text_content_list[0]
        else:
            name = row_text_content_list[0]
            code = row_text_content_list[1]

        link = row.locator("a.ellipsis")  
        if link.count() == 0:
            continue

        href = link.first.get_attribute("href") or ""

        
        # print(href)     # printing the name and href of each row

        full_url = urljoin(BASE, href)

    #     first_td = row.locator("td").first
    #     cell_text = first_td.inner_text().strip()
    #     parts = [ln.strip() for ln in cell_text.splitlines() if ln.strip()]
    #     code = parts[-1] if parts else ""

        if name and code:
            companies.append({"code": code, "name": name, "sector": sector, "url": full_url})

    browser.close()

df = pd.DataFrame(companies).drop_duplicates(subset=["code", "name"])
# print(df.to_string(index=False))
# print("Total companies:", len(df))
df.to_csv("list of company urls.csv", index=False)
print("Saved to list of company urls.csv")
