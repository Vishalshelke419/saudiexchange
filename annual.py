# annual_only.py
import json, re, sys
from pathlib import Path
from bs4 import BeautifulSoup
import pandas as pd

# ---------- CONFIG ----------
# ROOT    = Path(r"C:\Users\Vishal\Desktop\Internship\financials_json")
# NETDUMP = ROOT / "netdump"
# SYMBOL  = "1111"   # change if needed
# VERBOSE = True
# ---------------------------
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


# date-ish patterns (way more permissive)
RE_YEAR        = re.compile(r"(19|20)\d{2}")
RE_PURE_YEAR   = re.compile(r"^(?:19|20)\d{2}$")
RE_ISO         = re.compile(r"^(?:19|20)\d{2}-\d{2}-\d{2}$")
RE_SLASH_DATE  = re.compile(r"^\d{1,2}/\d{1,2}/(?:19|20)\d{2}$")
RE_FY_PREFIX   = re.compile(r"^(?:FY|F/Y|FYE)\s*(?:19|20)\d{2}$", re.I)
RE_YEAR_SUFFIX = re.compile(r"^(?:19|20)\d{2}\s*(?:FY|Y|Annual|\(12M\))$", re.I)
RE_YEAR_SLASH  = re.compile(r"^(?:19|20)\d{2}/(?:19|20)\d{2}$")

def dbg(*a):
    if VERBOSE:
        print(*a, file=sys.stderr)

def clean_text(s):
    return (str(s) or "").replace("\u00A0", " ").strip()

def looks_like_date_header(s: str) -> bool:
    s = clean_text(s)
    return bool(
        RE_PURE_YEAR.match(s)
        or RE_ISO.match(s)
        or RE_SLASH_DATE.match(s)
        or RE_FY_PREFIX.match(s)
        or RE_YEAR_SUFFIX.match(s)
        or RE_YEAR_SLASH.match(s)
        or RE_YEAR.search(s)  # as a last resort, any 4-digit year somewhere
    )

def first_year(s: str):
    m = RE_YEAR.search(clean_text(s))
    return int(m.group()) if m else None

def norm_date_header(s: str) -> str:
    s = clean_text(s)
    if RE_PURE_YEAR.match(s):
        return f"{s}-12-31"
    if RE_YEAR_SLASH.match(s):
        y2 = int(s.split("/")[-1])
        return f"{y2}-12-31"
    if RE_FY_PREFIX.match(s) or RE_YEAR_SUFFIX.match(s):
        y = first_year(s)
        return f"{y}-12-31" if y else s

    # ↓ add these two lines to parse 31/12/2023 style without warnings
    if RE_SLASH_DATE.match(s):
        dt = pd.to_datetime(s, format="%d/%m/%Y", errors="coerce")
        if not pd.isna(dt):
            return dt.strftime("%Y-%m-%d")

    # fallback attempts
    for dayfirst in (False, True):
        dt = pd.to_datetime(s, dayfirst=dayfirst, errors="coerce")
        if not pd.isna(dt):
            return dt.strftime("%Y-%m-%d")
    y = first_year(s)
    return f"{y}-12-31" if y else s


def to_number(x):
    if x is None:
        return None
    s = clean_text(x)
    if s in {"", "-", "—", "–"}:
        return None
    s = re.sub(r"^\((.*)\)$", r"-\1", s)  # (1,234) -> -1234
    s = s.replace(",", "")
    try:
        return float(s)
    except:
        return None

LABEL_KEYS = ["metric","name","label","account","item","description","heading","field","title","lineItem","accountName","caption","displayName","line_name","LineItem","Line_Name"]
NOISE_METRICS = {"All Currency In","All Currency in","All Figures in","All Figures In","Announced Date","Eligibility Date","Distribution Date","Last Update Date","Name","Price","Change %"}

def walk(obj, path=()):
    yield path, obj
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield from walk(v, path + (k,))
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            yield from walk(v, path + (i,))

def shape_json(node):
    # 1) list[dict] style
    if isinstance(node, list) and node and isinstance(node[0], dict):
        keys = set().union(*(r.keys() for r in node))
        date_cols = [k for k in keys if isinstance(k, str) and looks_like_date_header(k)]
        if len(date_cols) >= 2:
            label = next((k for k in LABEL_KEYS if k in keys), None)
            if label:
                table = []
                for r in node:
                    rec = {"Metric": clean_text(r.get(label, ""))}
                    for d in date_cols:
                        rec[d] = r.get(d)
                    table.append(rec)
                return table, date_cols

    # 2) columns/rows style dict
    if isinstance(node, dict):
        cols = node.get("columns") or node.get("headers") or node.get("dates") or node.get("Dates")
        rows = node.get("rows") or node.get("data") or node.get("items")
        if isinstance(cols, list) and isinstance(rows, list):
            date_cols = [c for c in cols if isinstance(c, str) and looks_like_date_header(c)]
            if len(date_cols) >= 2:
                label = next((k for k in LABEL_KEYS if any(isinstance(r, dict) and k in r for r in rows)), None) or "name"
                table = []
                for r in rows:
                    if isinstance(r, dict):
                        vals = r.get("values") or r.get("data") or []
                        rec = {"Metric": clean_text(r.get(label, ""))}
                        if isinstance(vals, list) and vals:
                            for i, d in enumerate(date_cols):
                                rec[d] = vals[i] if i < len(vals) else None
                        else:
                            for d in date_cols:
                                rec[d] = r.get(d)
                        table.append(rec)
                return table, date_cols

    # 3) list[list] style: first row headers, rest rows
    if isinstance(node, list) and node and isinstance(node[0], list):
        header = node[0]
        if any(looks_like_date_header(h) for h in header):
            date_cols = [h for h in header if looks_like_date_header(h)]
            if len(date_cols) >= 2:
                table = []
                for row in node[1:]:
                    if not row:
                        continue
                    metric = clean_text(row[0]) if len(row) > 0 else ""
                    rec = {"Metric": metric}
                    for i, h in enumerate(header):
                        if i < len(row) and looks_like_date_header(h):
                            rec[h] = row[i]
                    table.append(rec)
                return table, date_cols

    return None

def choose_header_row_for_html(tbl):
    """Pick the row that actually contains the date headers (supports multi-row thead)."""
    # prefer thead rows
    thead = tbl.find("thead")
    header_rows = []
    if thead:
        header_rows.extend(thead.find_all("tr"))
    # also consider first 3 rows of tbody if thead is useless
    body_rows = tbl.find_all("tr")
    header_rows.extend(body_rows[:3])
    best = None
    for tr in header_rows:
        cells = [clean_text(c.get_text()) for c in tr.find_all(["th", "td"])]
        if sum(looks_like_date_header(c) for c in cells) >= 2:
            score = sum(looks_like_date_header(c) for c in cells)
            if not best or score > best[0]:
                best = (score, cells)
    return best[1] if best else None

def parse_html_table(tbl):
    header = choose_header_row_for_html(tbl)
    if not header:
        return None
    date_cols = [h for h in header if looks_like_date_header(h)]
    if len(date_cols) < 2:
        return None

    # metric col index = first non-date header, else 0
    metric_idx = 0
    while metric_idx < len(header) and looks_like_date_header(header[metric_idx]):
        metric_idx += 1

    rows = []
    for tr in tbl.find_all("tr"):
        cells = [clean_text(c.get_text()) for c in tr.find_all(["th", "td"])]
        if not cells:
            continue
        if cells == header:
            continue
        # skip section-only rows
        if sum(1 for c in cells if c not in ("", "-", "—", "–")) == 1 and (cells[0] not in ("", "-", "—", "–")):
            continue
        metric = cells[metric_idx] if metric_idx < len(cells) else (cells[0] if cells else "")
        rec = {"Metric": metric}
        for i, h in enumerate(header):
            if i < len(cells) and looks_like_date_header(h):
                rec[h] = cells[i]
        rows.append(rec)

    return rows, date_cols

def soup_for_file(path: Path) -> BeautifulSoup:
    text = path.read_text(encoding="utf-8", errors="ignore")
    head = text.lstrip()[:200].lower()
    # quiet the XML warning by using the XML parser when appropriate
    if head.startswith("<?xml") or "<workbook" in head or "<xml" in head:
        return BeautifulSoup(text, "xml")
    return BeautifulSoup(text, "lxml")

def is_annual(date_cols):
    """More forgiving annual detector:
       - >=3 distinct years in headers, OR
       - >=3 parsed timestamps roughly 12 months apart, OR
       - headers map to the SAME month each year (any month), >=3 cols.
    """
    normed = [norm_date_header(d) for d in date_cols]
    years  = [first_year(d) for d in date_cols if first_year(d)]
    years  = [y for y in years if y]
    if len(set(years)) >= 3:
        return True

    ts = [pd.to_datetime(d, errors="coerce") for d in normed]
    ts = [t for t in ts if pd.notna(t)]
    if len(ts) >= 3:
        ts_sorted = sorted(ts)
        diffs = [(b - a).days for a, b in zip(ts_sorted, ts_sorted[1:])]
        if diffs and pd.Series(diffs).median() >= 300:  # ~ yearly cadence
            return True
        months = {t.month for t in ts_sorted}
        if len(months) == 1 and len(ts_sorted) >= 3:    # same month every year
            return True
    return False

def to_json(table, date_cols, symbol):
    iso_dates = [norm_date_header(d) for d in date_cols]

    def infer_section(metric):
        m = (metric or "").lower()
        if any(x in m for x in ["equity","assets","liabil","inventory","payable","receivable","balance"]): return "Balance Sheet"
        if any(x in m for x in ["revenue","sales","profit","loss","income","operat","eps","expenses","cost"]): return "Statement Of Income"
        if any(x in m for x in ["cash","operating","financing","investing","free cash"]): return "Cash Flows"
        return None

    sections = {"Balance Sheet": [], "Statement Of Income": [], "Cash Flows": []}
    for row in table:
        metric = clean_text(row.get("Metric",""))
        if not metric or metric in NOISE_METRICS:
            continue
        sec = infer_section(metric)
        if sec not in sections:
            continue
        values = {d_iso: to_number(row.get(d_raw)) for d_raw, d_iso in zip(date_cols, iso_dates)}
        if any(v is not None for v in values.values()):
            sections[sec].append({"metric": metric, "values": values})
    sections = {k:v for k,v in sections.items() if v}
    return {"symbol": symbol, "frequency": "annual", "sections": sections}

def main():
    if not NETDUMP.exists():
        raise SystemExit(f"Missing folder: {NETDUMP}")

    dbg("[info] scanning:", NETDUMP)
    candidates = []
    files = sorted([p for p in NETDUMP.iterdir() if p.suffix.lower() in {".json",".html",".txt"}])
    if not files:
        raise SystemExit("netdump/ is empty. Save your captured files there.")

    for p in files:
        try:
            dbg("\n[file]", p.name)
            if p.suffix.lower() == ".json":
                payload = json.loads(p.read_text(encoding="utf-8", errors="ignore"))
                obj = payload.get("json", payload)
                found = 0
                for _, node in walk(obj):
                    shaped = shape_json(node)
                    if shaped:
                        table, date_cols = shaped
                        dbg("  - json table cols:", [clean_text(c) for c in date_cols][:8], "...")
                        if is_annual(date_cols):
                            score = len(table) + 3*len(date_cols)
                            candidates.append((score, table, date_cols, p.name))
                            found += 1
                dbg("  json candidates:", found)
            else:
                soup = soup_for_file(p)
                tables = soup.find_all("table")
                dbg("  html tables found:", len(tables))
                for t in tables:
                    parsed = parse_html_table(t)
                    if not parsed:
                        continue
                    rows, date_cols = parsed
                    dbg("  - html table cols:", [clean_text(c) for c in date_cols][:8], "...")
                    if is_annual(date_cols):
                        score = len(rows) + 3*len(date_cols)
                        candidates.append((score, rows, date_cols, p.name))
        except Exception as e:
            dbg("  [warn] error parsing", p.name, "->", e)
            continue

    if not candidates:
        raise SystemExit("No annual-looking tables found. Tip: open the ANNUAL financials page, export/copy its HTML or network JSON into netdump/, then rerun.")

    candidates.sort(key=lambda x: x[0], reverse=True)
    score, table, date_cols, src = candidates[0]
    dbg("\n[best] from", src, "| score:", score, "| cols:", [clean_text(c) for c in date_cols])

    out = ROOT / f"{SYMBOL}_annual.json"
    js = to_json(table, date_cols, SYMBOL)
    if not js.get("sections"):
        raise SystemExit("Found a table, but all rows were empty after cleaning. Try another capture.")
    out.write_text(json.dumps(js, ensure_ascii=False, indent=2), encoding="utf-8")
    print("[ok] annual  ->", out, "(from", src, ")")

if __name__ == "__main__":
    main()
