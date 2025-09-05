# extract_financials_quarterly_only.py
import json, re
from pathlib import Path
from bs4 import BeautifulSoup
import pandas as pd

# ROOT = Path(r"C:\Users\Vishal\Desktop\Internship\financials_json")
# NETDUMP = ROOT / "netdump"
# SYMBOL = "1111"  # change if needed

import os

# ---------- CONFIG ----------
ROOT = Path(os.getenv("FINJSON_ROOT", "./financials_json")).resolve()
NETDUMP = ROOT / "netdump"
SYMBOL  = "1111"   # change if needed
VERBOSE = True
# ----------------------------

# Make sure folders exist
ROOT.mkdir(parents=True, exist_ok=True)
NETDUMP.mkdir(parents=True, exist_ok=True)


RE_Y   = re.compile(r"^(?:19|20)\d{2}$")
RE_ISO = re.compile(r"^(?:19|20)\d{2}-\d{2}-\d{2}$")
RE_S   = re.compile(r"^\d{1,2}/\d{1,2}/(?:19|20)\d{2}$")

def dateish(s):
    s=(str(s) or "").strip()
    return bool(RE_Y.match(s) or RE_ISO.match(s) or RE_S.match(s))

def norm_date(s):
    s=(str(s) or "").replace("\u00A0"," ").strip()
    for dayfirst in (False, True):
        dt = pd.to_datetime(s, dayfirst=dayfirst, errors="coerce")
        if not pd.isna(dt):
            return dt.strftime("%Y-%m-%d")
    return s

def to_number(x):
    if x is None: return None
    s = str(x).replace("\u00A0"," ").strip()
    if s in {"","-","—","–"}: return None
    s = re.sub(r"^\((.*)\)$", r"-\1", s)  # (1,234) -> -1234
    s = s.replace(",", "")
    try: return float(s)
    except: return None

LABEL_KEYS = ["metric","name","label","account","item","description","heading","field","title","lineItem","accountName","caption","displayName","line_name","LineItem","Line_Name"]
NOISE_METRICS = {"All Currency In","All Currency in","All Figures in","All Figures In","Announced Date","Eligibility Date","Distribution Date","Last Update Date","Name","Price","Change %"}

def walk(obj, path=()):
    yield path, obj
    if isinstance(obj, dict):
        for k,v in obj.items():
            yield from walk(v, path+(k,))
    elif isinstance(obj, list):
        for i,v in enumerate(obj):
            yield from walk(v, path+(i,))

def shape_json(node):
    # list of dicts
    if isinstance(node, list) and node and isinstance(node[0], dict):
        keys = set().union(*(r.keys() for r in node))
        date_cols = [k for k in keys if isinstance(k,str) and dateish(k)]
        if len(date_cols) >= 2:
            label = next((k for k in LABEL_KEYS if k in keys), None)
            if label:
                table=[]
                for r in node:
                    rec={"Metric": str(r.get(label,"")).strip()}
                    for d in date_cols:
                        rec[d]=r.get(d)
                    table.append(rec)
                return table, date_cols
    # columns/rows
    if isinstance(node, dict):
        cols = node.get("columns") or node.get("headers") or node.get("dates") or node.get("Dates")
        rows = node.get("rows") or node.get("data") or node.get("items")
        if isinstance(cols, list) and isinstance(rows, list):
            date_cols = [c for c in cols if isinstance(c,str) and dateish(c)]
            if len(date_cols) >= 2:
                label = next((k for k in LABEL_KEYS if any(isinstance(r,dict) and k in r for r in rows)), None) or "name"
                table=[]
                for r in rows:
                    if isinstance(r, dict):
                        vals = r.get("values") or r.get("data") or []
                        rec={"Metric": str(r.get(label,"")).strip()}
                        if isinstance(vals, list) and vals:
                            for i,d in enumerate(date_cols):
                                rec[d] = vals[i] if i < len(vals) else None
                        else:
                            for d in date_cols:
                                rec[d] = r.get(d)
                        table.append(rec)
                return table, date_cols
    return None

def parse_html_table(tbl):
    header = []
    thead = tbl.find("thead")
    if thead:
        header = [c.get_text(strip=True) for c in thead.find_all(["th","td"])]
    if not header:
        first_tr = tbl.find("tr")
        if first_tr:
            header = [c.get_text(strip=True) for c in first_tr.find_all(["th","td"])]

    date_cols = [h for h in header if dateish(h)]
    if len(date_cols) < 2:
        return None

    metric_idx = 0
    while metric_idx < len(header) and dateish(header[metric_idx]): metric_idx += 1

    rows = []
    for tr in tbl.find_all("tr"):
        cells = [c.get_text(strip=True).replace("\u00A0"," ") for c in tr.find_all(["th","td"])]
        if not cells: continue
        if cells == header: continue
        if sum(1 for c in cells if c not in ("","-","—","–")) == 1 and (cells[0] not in ("","-","—","–")):
            continue
        metric = cells[metric_idx] if metric_idx < len(cells) else (cells[0] if cells else "")
        rec = {"Metric": metric}
        for i, h in enumerate(header):
            if dateish(h):
                rec[h] = cells[i] if i < len(cells) else None
        rows.append(rec)

    return rows, date_cols

def scrape_html_file(path: Path):
    html = path.read_text(encoding="utf-8", errors="ignore")
    soup = BeautifulSoup(html, "lxml")
    tables = soup.find_all("table")
    best = None
    for t in tables:
        parsed = parse_html_table(t)
        if not parsed: continue
        rows, dates = parsed
        score = len(rows) + 3*len(dates)
        if not best or score > best[0]:
            best = (score, rows, dates)
    if not best: return None
    _, rows, dates = best
    return rows, dates

def score_table(date_cols):
    # quarterly-ish if months subset of {3,6,9,12} and enough columns
    try:
        months = [pd.to_datetime(d, dayfirst=True, errors="coerce").month for d in date_cols]
        months = [m for m in months if pd.notna(m)]
        if months and set(months).issubset({3,6,9,12}) and len(months) >= 4:
            return True
    except: 
        pass
    return False

def to_json(table, date_cols, symbol):
    iso_dates = [norm_date(d) for d in date_cols]

    def infer_section(metric):
        m = (metric or "").lower()
        if any(x in m for x in ["equity","assets","liabil","inventory","payable","receivable","balance"]): return "Balance Sheet"
        if any(x in m for x in ["revenue","sales","profit","loss","income","operat","eps","expenses","cost"]): return "Statement Of Income"
        if any(x in m for x in ["cash","operating","financing","investing","free cash"]): return "Cash Flows"
        return None

    sections = {"Balance Sheet": [], "Statement Of Income": [], "Cash Flows": []}
    for row in table:
        metric = str(row.get("Metric","")).strip()
        if not metric or metric in NOISE_METRICS: 
            continue
        sec = infer_section(metric)
        if sec not in sections: 
            continue
        values = {d_iso: to_number(row.get(d_raw)) for d_raw, d_iso in zip(date_cols, iso_dates)}
        if any(v is not None for v in values.values()):
            sections[sec].append({"metric": metric, "values": values})
    sections = {k:v for k,v in sections.items() if v}
    return {"symbol": symbol, "frequency": "quarterly", "sections": sections}

def main():
    net = NETDUMP
    if not net.exists():
        raise SystemExit("Run your capture first. netdump/ is missing.")

    candidates = []
    for p in sorted(net.iterdir()):
        if p.suffix.lower() not in {".json",".html",".txt"}:
            continue
        try:
            if p.suffix.lower()==".json":
                payload = json.loads(p.read_text(encoding="utf-8", errors="ignore"))
                obj = payload.get("json", payload)
                for _, node in walk(obj):
                    shaped = shape_json(node)
                    if shaped:
                        table, date_cols = shaped
                        if score_table([norm_date(d) for d in date_cols]):
                            score = len(table) + 3*len(date_cols)
                            candidates.append((score, table, date_cols, p.name))
            else:
                parsed = scrape_html_file(p)
                if parsed:
                    rows, date_cols = parsed
                    if score_table([norm_date(d) for d in date_cols]):
                        score = len(rows) + 3*len(date_cols)
                        candidates.append((score, rows, date_cols, p.name))
        except Exception:
            continue

    if not candidates:
        raise SystemExit("No quarterly-looking tables found. Open a clear file in netdump/ and try again.")

    candidates.sort(key=lambda x: x[0], reverse=True)
    score, table, date_cols, src = candidates[0]

    out = ROOT / f"{SYMBOL}_quarterly.json"
    q_js = to_json(table, date_cols, SYMBOL)
    out.write_text(json.dumps(q_js, ensure_ascii=False, indent=2), encoding="utf-8")
    print("[ok] quarterly ->", out, "(from", src, ")")

if __name__ == "__main__":
    main()
