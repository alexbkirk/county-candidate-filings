import base64
import io
import os
import re
import sys
import tempfile
from pathlib import Path
from typing import Dict, Optional

import pandas as pd
from playwright.sync_api import sync_playwright

# ----------------- CONFIG -----------------
ELECTION_URL = "https://web.sos.ky.gov/CandidateFilings/countyfilings.aspx?elecid=86"

# Files will be written to this folder in your repo (create automatically).
# Use "" for repo root, or "counties/" to keep things organized.
REPO_PATH_PREFIX = "counties/"

# "csv" or "xlsx"
OUTPUT_EXT = "csv"

# GitHub target (pre-filled for your repo)
GITHUB_OWNER = os.environ.get("GITHUB_OWNER", "alexbkirk")
GITHUB_REPO  = os.environ.get("GITHUB_REPO",  "county-candidate-filings")
GH_TOKEN     = os.environ.get("GH_TOKEN")  # required
# ------------------------------------------


def github_api(path: str, method: str = "GET", json_body: Optional[dict] = None):
    import requests
    url = f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/contents/{path}"
    headers = {
        "Authorization": f"Bearer {GH_TOKEN}",
        "Accept": "application/vnd.github+json"
    }
    resp = requests.request(method, url, headers=headers, json=json_body)
    return resp


def get_existing_sha(path: str) -> Optional[str]:
    resp = github_api(path, "GET")
    if resp.status_code == 200:
        return resp.json().get("sha")
    return None


def put_file(path: str, content_bytes: bytes, message: str, sha: Optional[str]):
    encoded = base64.b64encode(content_bytes).decode("utf-8")
    body = {"message": message, "content": encoded}
    if sha:
        body["sha"] = sha
    resp = github_api(path, "PUT", json_body=body)
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"GitHub PUT failed for {path}: {resp.status_code} {resp.text}")


def ensure_folder(prefix: str):
    """Create a .keep file so the folder exists in the repo."""
    if not prefix:
        return
    keep_path = prefix.rstrip("/") + "/.keep"
    sha = get_existing_sha(keep_path)
    if sha is None:
        put_file(keep_path, b"", f"Create {prefix} folder", None)


def playwright_download_xlsx(dest_dir: Path) -> Path:
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(accept_downloads=True)
        page.goto(ELECTION_URL, wait_until="domcontentloaded")

        selectors = [
            'text=/Download All Candidates/i',
            'text=/Download|Export|Excel|CSV/i',
            "a:has-text('Download')",
            "a:has-text('Export')",
            "button:has-text('Download')",
            "button:has-text('Export')",
            "input[type=submit]",
            "input[type=button]"
        ]

        handle = None
        for sel in selectors:
            try:
                handle = page.wait_for_selector(sel, timeout=3000)
                if handle:
                    break
            except Exception:
                pass

        with page.expect_download() as dlinfo:
            if handle:
                handle.click()
            else:
                page.evaluate("""
                    () => {
                      const els = [...document.querySelectorAll('a,button,input[type=submit],input[type=button]')];
                      const btn = els.find(el => /download|export|excel|csv/i.test((el.textContent||'')+(el.value||'')));
                      if (btn) btn.click();
                    }
                """)

        download = dlinfo.value
        suggested = download.suggested_filename
        out_path = dest_dir / (suggested or "AllCandidates.xlsx")
        download.save_as(str(out_path))
        browser.close()
        return out_path


def load_dataframe_from_file(path: Path) -> pd.DataFrame:
    """
    Robustly load the KY export which can be:
      - Real XLSX/XLSM (zip-based; openpyxl)
      - Legacy XLS (BIFF8; xlrd==1.2.0)
      - Excel-HTML ('.xls' but actually HTML with a <table>)
      - CSV (fallback)
    """
    suffix = path.suffix.lower()

    def is_probably_html(p: Path) -> bool:
        try:
            with open(p, "rb") as f:
                head = f.read(4096).strip().lower()
            # Heuristics: HTML-ish or XML-ish with table
            return (head.startswith(b"<") and (b"<html" in head or b"<table" in head or b"<meta" in head)) \
                   or b"content-type" in head and b"text/html" in head
        except Exception:
            return False

    def pick_table_with_county(dfs):
        if not dfs:
            return None
        # Prefer a table with a County column
        for df in dfs:
            cols = [str(c).strip().lower() for c in df.columns]
            if "county" in cols:
                return df
        # else largest-ish table
        return max(dfs, key=lambda d: (len(d.columns), len(d)))

    print(f"[loader] filename={path.name} suffix={suffix}")

    # 0) If it looks like HTML (common for ".xls" exports), parse as HTML first
    if is_probably_html(path):
        print("[loader] Detected HTML-styled file; parsing with read_html")
        try:
            dfs = pd.read_html(str(path))  # needs lxml/html5lib
            df = pick_table_with_county(dfs)
            if df is not None:
                df.columns = [str(c).strip() for c in df.columns]
                return df
            # If we got here, read_html worked but didn't find a good table → fall through
        except Exception as e:
            print(f"[loader] read_html failed: {type(e).__name__}: {e}")

    # 1) True XLSX-like formats → openpyxl
    if suffix in (".xlsx", ".xlsm", ".xltx", ".xltm"):
        try:
            print("[loader] Trying openpyxl for xlsx/xlsm")
            return pd.read_excel(path, engine="openpyxl")
        except Exception as e:
            print(f"[loader] openpyxl failed: {type(e).__name__}: {e}")

    # 2) Legacy XLS → xlrd==1.2.0
    if suffix == ".xls":
        try:
            print("[loader] Trying xlrd for legacy .xls")
            return pd.read_excel(path, engine="xlrd")
        except Exception as e:
            print(f"[loader] xlrd failed: {type(e).__name__}: {e}")
        # If xlrd failed and it wasn't detected as HTML above, try HTML again as fallback
        try:
            print("[loader] Fallback: trying read_html on .xls")
            dfs = pd.read_html(str(path))
            df = pick_table_with_county(dfs)
            if df is not None:
                df.columns = [str(c).strip() for c in df.columns]
                return df
        except Exception as e:
            print(f"[loader] read_html fallback failed: {type(e).__name__}: {e}")

    # 3) CSV fallback
    try:
        print("[loader] Fallback: trying CSV")
        return pd.read_csv(path)
    except Exception as e:
        raise RuntimeError(
            f"Could not parse downloaded file {path.name} as html/xlsx/xls/csv. "
            f"Last error: {type(e).__name__}: {e}"
        )




def split_by_county(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    candidates = ["County", "county", "COUNTY"]
    col = next((c for c in df.columns if c in candidates), None)
    if not col:
        matches = [c for c in df.columns if c.lower() == "county"]
        if matches:
            col = matches[0]
        else:
            raise ValueError(f"Couldn't find a 'County' column. Columns: {list(df.columns)}")

    df[col] = df[col].astype(str).str.strip()
    groups: Dict[str, pd.DataFrame] = {}
    for county, sub in df.groupby(col, dropna=True):
        c = county.strip()
        if not c:
            continue
        groups[c] = sub.reset_index(drop=True)
    return groups


def dataframe_to_bytes(df: pd.DataFrame) -> bytes:
    if OUTPUT_EXT.lower() == "xlsx":
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            df.to_excel(writer, index=False)
        return buf.getvalue()
    return df.to_csv(index=False).encode("utf-8")


def name_to_filename(county: str) -> str:
    # Keep readable/stable names (e.g., "Laurel.csv")
    base = re.sub(r"\s+", " ", county.strip())
    base = re.sub(r"[^A-Za-z0-9 \-']", "", base).strip()
    fname = f"{base}.{OUTPUT_EXT}"
    if REPO_PATH_PREFIX:
        return f"{REPO_PATH_PREFIX.rstrip('/')}/{fname}"
    return fname


def main():
    if not GH_TOKEN:
        print("ERROR: GH_TOKEN is required (repo contents write permissions).", file=sys.stderr)
        sys.exit(1)

    if REPO_PATH_PREFIX:
        ensure_folder(REPO_PATH_PREFIX)

    with tempfile.TemporaryDirectory() as td:
        tmpdir = Path(td)
        print("Downloading master spreadsheet…")
        master_path = playwright_download_xlsx(tmpdir)
        print(f"Downloaded: {master_path.name}")

        print("Reading and splitting by County…")
        df = load_dataframe_from_file(master_path)
        groups = split_by_county(df)
        if not groups:
            print("No county groups found—nothing to upload.")
            return

        for county, subdf in groups.items():
            target_path = name_to_filename(county)
            content = dataframe_to_bytes(subdf)
            sha = get_existing_sha(target_path)
            msg = f"Update {target_path} from latest KY SOS export"
            put_file(target_path, content, msg, sha)
            print(f"Upserted {target_path} ({len(subdf)} rows)")

    print("Done.")


if __name__ == "__main__":
    main()
