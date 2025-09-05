# capture_everything.py
# Captures ALL network responses (json/html/text), clicking Annually/Quarterly to trigger loads.
# Dumps bodies to ./financials_json/netdump/ + index.csv so we can mine them later.

import os, re, json, time, base64, csv
from pathlib import Path
from urllib.parse import urlparse

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Use env variable if available, else fallback to local folder

# SYMBOL = "1111"  # <-- change if needed
# OUT_ROOT = Path(r"C:\Users\Vishal\Desktop\Int\financials_json")

from pathlib import Path
import os


# Use env variable if available, else fallback to local folder
ROOT = Path(os.getenv("FINJSON_ROOT", "./financials_json")).resolve()
NETDUMP = ROOT / "netdump"
ROOT.mkdir(parents=True, exist_ok=True)
NETDUMP.mkdir(parents=True, exist_ok=True)

SYMBOL = "1111"  # <-- change if needed

URL = ("https://www.saudiexchange.sa/wps/portal/saudiexchange/hidden/company-profile-main/"
       "!ut/p/z1/04_Sj9CPykssy0xPLMnMz0vMAfIjo8ziTR3NDIw8LAz83d2MXA0C3SydAl1c3Q0NvE30I4EKzBEKDMKcTQzMDPxN3H19LAzdTU31w8syU8v1wwkpK8hOMgUA-oskdg!!/"
       f"?companySymbol={SYMBOL}&locale=en")

HEADLESS = False
CAPTURE_INITIAL = 6      # seconds pre-click
CAPTURE_AFTER_CLICK = 5  # seconds after each tab click
SCROLL_PAUSES = 6        # how many scroll steps in the panel/page

TOK_HITS = [  # strings to help you eyeball hits in console
    "Balance Sheet", "Statement Of Income", "Cash Flow",
    "Assets", "Liabilities", "Equity", "Revenue", "Profit",
    "الميزانية", "الدخل", "التدفقات"
]

NOISY_URL_BITS = [
    "collect?", "recaptcha", "analytics", "ChartGenerator", "TickerServlet",
    "Theme", "dojo", "font", ".js?", "icon", "manifest", "bootstrap"
]

def start_driver():
    opts = webdriver.ChromeOptions()
    if HEADLESS: opts.add_argument("--headless=new")
    opts.add_argument("--start-maximized")
    opts.set_capability("goog:loggingPrefs", {"performance":"ALL"})
    drv = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
    # enable CDP Network
    drv.execute_cdp_cmd("Network.enable", {"maxResourceBufferSize": 50_000_000, "maxTotalBufferSize": 100_000_000})
    drv.execute_cdp_cmd("Network.setCacheDisabled", {"cacheDisabled": True})
    return drv

def click_tab(driver, label):
    wait = WebDriverWait(driver, 10)
    driver.switch_to.default_content()
    # crawl iframes to find the widget
    def dfs(depth=0, max_depth=10):
        if depth > max_depth:
            return False
        # if the tab text is in current frame
        try:
            if driver.find_elements(By.XPATH, f"//*[normalize-space()='Annually' or normalize-space()='Quarterly' or contains(.,'FINANCIAL INFORMATION')]"):
                return True
        except: pass
        for f in driver.find_elements(By.TAG_NAME, "iframe"):
            try:
                driver.switch_to.frame(f)
                if dfs(depth+1, max_depth): return True
                driver.switch_to.parent_frame()
            except:
                driver.switch_to.parent_frame()
        return False
    if not dfs(): return False

    variants = {"Annually":["Annually","Annual","Yearly","سنوي"],
                "Quarterly":["Quarterly","Quarter","ربع سنوي","Quarter"]}[label]
    for v in variants:
        for xp in [
            f"//*[@role='tab' and normalize-space()='{v}']",
            f"//button[normalize-space()='{v}']",
            f"//a[normalize-space()='{v}']",
            f"//*[contains(@class,'tab') and normalize-space()='{v}']",
            f"//*[normalize-space()='{v}']",
        ]:
            els = driver.find_elements(By.XPATH, xp)
            if not els: continue
            el = els[0]
            try:
                wait.until(EC.element_to_be_clickable(el)).click()
                return True
            except:
                try:
                    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
                    driver.execute_script("arguments[0].click();", el)
                    return True
                except: pass
    return False

def save_body(root, seq, url, mime, text, base64_flag):
    # decode if base64
    if base64_flag:
        try:
            text = base64.b64decode(text).decode("utf-8","ignore")
        except Exception:
            # not text → skip
            return None
    # ext
    m = (mime or "").lower()
    ext = ".json" if "json" in m else ".html" if "html" in m else ".txt"
    # filename
    host = urlparse(url).netloc.replace(":","_")
    path = root / f"{seq:04d}_{host}{ext}"
    path.write_text(text, encoding="utf-8", errors="ignore")
    return path

def capture_all(drv, seconds, csv_writer, seen):
    """poll performance logs; on loadingFinished, pull body & dump"""
    t0 = time.time(); seq = len(seen)+1
    pending = {}  # reqId -> (url, mime)
    while time.time() - t0 < seconds:
        for e in drv.get_log("performance"):
            try:
                msg = json.loads(e["message"])["message"]
            except Exception:
                continue
            method = msg.get("method",""); params = msg.get("params",{})
            if method == "Network.responseReceived":
                resp = params.get("response", {})
                req_id = params.get("requestId")
                url = resp.get("url") or ""
                mime = resp.get("mimeType") or ""
                if any(x in url for x in NOISY_URL_BITS):
                    continue
                pending[req_id] = (url, mime)
            elif method == "Network.loadingFinished":
                req_id = params.get("requestId")
                if req_id in seen: 
                    continue
                if req_id not in pending:
                    continue
                url, mime = pending.pop(req_id)
                try:
                    body = drv.execute_cdp_cmd("Network.getResponseBody", {"requestId": req_id})
                    text = body.get("body") or ""
                    path = save_body(NETDUMP, seq, url, mime, text, body.get("base64Encoded"))
                    if path:
                        seq += 1
                        seen.add(req_id)
                        # write index
                        csv_writer.writerow({"file": path.name, "url": url, "mime": mime})
                        # quick hit print
                        low = text.lower()
                        if any(t.lower() in low for t in TOK_HITS):
                            print("[HIT]", path.name, "←", url)
                except Exception:
                    pass
        time.sleep(0.15)


if __name__ == "__main__":
    index_csv = NETDUMP / "index.csv"
    fcsv = open(index_csv, "w", newline="", encoding="utf-8")
    writer = csv.DictWriter(fcsv, fieldnames=["file", "url", "mime"])
    writer.writeheader()

    drv = start_driver()
    try:
        print("Target:", URL)
        drv.get(URL)
        time.sleep(1.2)

        seen = set()
        # capture while idle
        capture_all(drv, CAPTURE_INITIAL, writer, seen)

        # click Annually and capture
        if click_tab(drv, "Annually"):
            capture_all(drv, CAPTURE_AFTER_CLICK, writer, seen)

        # scroll a bit (some widgets lazy-load)
        try:
            drv.switch_to.default_content()
            for _ in range(SCROLL_PAUSES):
                drv.execute_script("window.scrollBy(0, 800);")
                time.sleep(0.3)
                capture_all(drv, 1.0, writer, seen)
        except:
            pass

        # click Quarterly and capture
        if click_tab(drv, "Quarterly"):
            capture_all(drv, CAPTURE_AFTER_CLICK, writer, seen)

        print(f"[ok] saved bodies in {NETDUMP}")
        print(f"[ok] index -> {index_csv}")

    finally:
        try:
            fcsv.close()
        except:
            pass
        drv.quit()
