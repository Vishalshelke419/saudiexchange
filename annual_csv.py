# json_to_csv_financials_annual_only.py
import json
from pathlib import Path
import pandas as pd

# ---------- CONFIG ----------
IN_DIR = Path(r"C:\Users\Vishal\Desktop\Internship\financials_json")
FILES  = ["1111_annual.json"]  # only annual now
EXPORT_PER_SECTION = False
# ----------------------------

def load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

def to_wide_df(js: dict) -> pd.DataFrame:
    sections = js.get("sections", {})
    all_dates = set()
    for items in sections.values():
        for it in items:
            all_dates.update((it.get("values") or {}).keys())

    dates_sorted = sorted(
        list(all_dates),
        key=lambda d: pd.to_datetime(d, dayfirst=True, errors="coerce"),
        reverse=True,
    )

    rows = []
    for section, items in sections.items():
        for it in items:
            metric = it.get("metric", "")
            vals = it.get("values", {}) or {}
            row = [section, metric] + [vals.get(d) for d in dates_sorted]
            rows.append(row)

    cols = ["Section", "Metric"] + dates_sorted
    return pd.DataFrame(rows, columns=cols)

def to_long_df(df: pd.DataFrame) -> pd.DataFrame:
    long = df.melt(id_vars=["Section", "Metric"], var_name="Date", value_name="Value")
    long["__ts"] = pd.to_datetime(long["Date"], dayfirst=True, errors="coerce")
    long = long.sort_values(["Section", "Metric", "__ts"], ascending=[True, True, False]).drop(columns="__ts")
    return long.reset_index(drop=True)

def main():
    IN_DIR.mkdir(parents=True, exist_ok=True)

    for fname in FILES:
        src = IN_DIR / fname
        if not src.exists():
            print(f"[skip] {src} not found")
            continue

        js = load_json(src)
        symbol = js.get("symbol", Path(fname).stem.split("_")[0])
        freq   = js.get("frequency", "unknown")

        df_wide = to_wide_df(js)
        if df_wide.empty:
            print(f"[warn] No rows for {fname}")
            continue

        out_wide = IN_DIR / f"{symbol}_{freq}_wide.csv"
        out_long = IN_DIR / f"{symbol}_{freq}_long.csv"
        df_wide.to_csv(out_wide, index=False, encoding="utf-8-sig")
        to_long_df(df_wide).to_csv(out_long, index=False, encoding="utf-8-sig")
        print(f"[ok] {out_wide}")
        print(f"[ok] {out_long}")

        if EXPORT_PER_SECTION:
            for sec, g in df_wide.groupby("Section", dropna=False):
                if not sec:
                    continue
                slug = sec.lower().replace(" ", "_")
                (IN_DIR / f"{symbol}_{freq}_wide_{slug}.csv").write_text(
                    g.to_csv(index=False, encoding="utf-8-sig"), encoding="utf-8-sig"
                )

if __name__ == "__main__":
    main()
