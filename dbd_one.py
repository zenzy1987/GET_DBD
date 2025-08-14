# -*- coding: utf-8 -*-
import os, re, time, random, json, argparse, math, sys, traceback
from datetime import datetime, timezone
from urllib.parse import urljoin
from typing import Callable, Iterable, Optional, Dict, Any, List

from bs4 import BeautifulSoup

# Selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, ElementClickInterceptedException, StaleElementReferenceException,
    WebDriverException, NoSuchWindowException
)

# Google Sheets
import gspread
from google.oauth2.service_account import Credentials

PAGE_LOAD_TIMEOUT = 60
BASE = "https://datawarehouse.dbd.go.th"

# ======== Retry helpers ========
def backoff_sleep(attempt: int, base: float = 0.8, cap: float = 8.0):
    # exponential backoff + jitter
    delay = min(cap, base * (2 ** attempt)) + random.uniform(0.05, 0.35)
    time.sleep(delay)

def with_retry(fn: Callable[[], Any], tries: int = 3, on_fail: Optional[Callable[[Exception,int], None]] = None):
    last = None
    for i in range(tries):
        try:
            return fn()
        except Exception as e:
            last = e
            if on_fail: 
                try: on_fail(e, i)
                except Exception: pass
            if i < tries - 1:
                backoff_sleep(i)
            else:
                raise last

# ========== Utils ==========
def canon_tax_id(x: str) -> str:
    t = re.sub(r"\D", "", str(x or ""))
    return t.zfill(13) if t else ""

def remove_ids_from_txt(path: str, tax_ids: Iterable[str]):
    if not os.path.exists(path): return
    targets = {canon_tax_id(t) for t in tax_ids if t}
    tmp = path + ".tmp"
    with open(path, "r", encoding="utf-8") as src, open(tmp, "w", encoding="utf-8") as dst:
        for line in src:
            raw = line.split("#", 1)[0].strip()
            cur = canon_tax_id(re.sub(r"\D", "", raw)) if raw else ""
            if not cur or cur not in targets:
                dst.write(line)
    os.replace(tmp, path)

def append_log(log_path: str, text: str):
    os.makedirs(os.path.dirname(log_path) or ".", exist_ok=True)
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(text + "\n")

def safe_write_json(fp: str, obj: Dict[str, Any]):
    os.makedirs(os.path.dirname(fp) or ".", exist_ok=True)
    tmp = fp + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    os.replace(tmp, fp)

def now_utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

# ========== Selenium helpers ==========
def build_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-software-rasterizer")
    opts.add_argument("--window-size=1365,900")
    opts.add_argument("--remote-debugging-port=9222")
    ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125 Safari/537.36"
    opts.add_argument(f"--user-agent={ua}")
    # honor explicit chrome path from workflow
    chrome_path = os.getenv("CHROME_PATH") or os.getenv("GOOGLE_CHROME_BIN") or os.getenv("CHROME_BIN")
    if chrome_path:
        opts.binary_location = chrome_path

    driver = webdriver.Chrome(options=opts)
    driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
    return driver

def stop_loading(driver):
    try:
        driver.execute_script("window.stop();")
    except Exception:
        pass

def close_popup_if_any(driver):
    sels = [
        '#btnWarning', '.modal [data-bs-dismiss="modal"]', '.modal .btn-close',
        '.swal2-confirm', '.swal2-container .swal2-confirm'
    ]
    for sel in sels:
        els = driver.find_elements(By.CSS_SELECTOR, sel)
        for el in els:
            try:
                if el.is_displayed() and el.is_enabled():
                    el.click()
                    time.sleep(0.25)
            except Exception:
                pass

def robust_get(driver, url: str, tries: int = 3):
    def _get():
        try:
            driver.get(url)
            return True
        except TimeoutException:
            stop_loading(driver)
            return True
    return with_retry(lambda: _get(), tries=tries)

def safe_open_link(driver, el):
    try:
        href = el.get_attribute("href")
    except StaleElementReferenceException:
        href = None
    if href:
        return robust_get(driver, urljoin(BASE, href))
    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
        time.sleep(0.1)
        el.click()
        return True
    except ElementClickInterceptedException:
        try:
            driver.execute_script("arguments[0].click();", el)
            return True
        except Exception:
            return False
    except Exception:
        return False

def find_any_xpath(driver, labels: List[str]) -> List:
    out=[]
    for lb in labels:
        out += driver.find_elements(By.XPATH, f"//a[contains(normalize-space(.),'{lb}')]")
    return out

def go_home_and_search(driver, tax_id: str):
    robust_get(driver, f"{BASE}/index")
    wait = WebDriverWait(driver, 30)
    close_popup_if_any(driver)

    def _type_and_submit():
        sb = wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "input#key-word.form-control")))
        sb.clear(); sb.send_keys(tax_id); time.sleep(0.15); sb.send_keys(Keys.ENTER)
        return True

    with_retry(_type_and_submit, tries=3)

    # ถ้าเข้าหน้าโปรไฟล์ได้เลย
    try:
        WebDriverWait(driver, 12).until(EC.presence_of_element_located((By.XPATH, "//h4[contains(.,'เลขทะเบียนนิติบุคคล')]")))
        return
    except TimeoutException:
        pass

    # หน้า list → คลิกรายละเอียด (รองรับข้อความหลายแบบ)
    labels = ["รายละเอียด", "ดูรายละเอียด", "ข้อมูลนิติบุคคล", "Detail", "View"]
    links = find_any_xpath(driver, labels)
    if links:
        close_popup_if_any(driver)
        if safe_open_link(driver, links[0]):
            return
        # fallback เผื่อ href
        try:
            href = links[0].get_attribute("href")
            if href:
                robust_get(driver, urljoin(BASE, href))
                return
        except Exception:
            pass

def wait_profile_loaded(driver):
    # รอทั้ง h4 และกลุ่มข้อมูลหลัก ๆ
    wait = WebDriverWait(driver, 30)
    wait.until(EC.presence_of_element_located((By.XPATH, "//h4[contains(.,'เลขทะเบียนนิติบุคคล')]")))
    # ให้ layout โหลด
    time.sleep(0.5)

def looks_blocked_or_empty(html: str) -> bool:
    text = re.sub(r"\s+", " ", html).lower()
    bad_keys = ["access denied", "too many requests", "rate limit", "captcha"]
    return any(k in text for k in bad_keys)

# ========== Parsing ==========
def extract_text_after_label(soup: BeautifulSoup, label: str) -> str:
    label_div = soup.find(lambda t: t.name == "div" and t.get_text(strip=True) == label)
    if not label_div: return ""
    parent = label_div.parent
    if parent:
        cols = parent.find_all("div", recursive=False)
        for i, c in enumerate(cols):
            if c is label_div and i + 1 < len(cols):
                return cols[i + 1].get_text(" ", strip=True)
    nxt = label_div.find_next_sibling("div")
    return nxt.get_text(" ", strip=True) if nxt else ""

def parse_profile_html(html: str):
    soup = BeautifulSoup(html, "html.parser")
    h3 = soup.find("h3"); h4 = soup.find("h4")
    name = re.sub(r"^ชื่อนิติบุคคล\s*:\s*", "", h3.get_text(" ", strip=True) if h3 else "") or "-"
    reg  = re.sub(r"^เลขทะเบียนนิติบุคคล:\s*", "", h4.get_text(" ", strip=True) if h4 else "") or "-"
    status_text = extract_text_after_label(soup, "สถานะนิติบุคคล") or "-"
    reg_date    = extract_text_after_label(soup, "วันที่จดทะเบียนจัดตั้ง") or "-"
    capital     = extract_text_after_label(soup, "ทุนจดทะเบียน") or "-"
    biz_group   = extract_text_after_label(soup, "กลุ่มธุรกิจ") or "-"
    biz_size    = extract_text_after_label(soup, "ขนาดธุรกิจ") or "-"
    address     = extract_text_after_label(soup, "ที่ตั้งสำนักงานแห่งใหญ่") or "-"

    directors = "-"
    h5_list = soup.find_all("h5", string=lambda s: s and "รายชื่อกรรมการ" in s)
    if h5_list:
        body = h5_list[0].find_next("div", class_="card-body")
        if body:
            lis = [li.get_text(" ", strip=True).strip(" /") for li in body.find_all("li")]
            if lis: directors = ", ".join(lis)

    tsic1_code_name, tsic1_obj = "-", "-"
    h5_tsic1 = soup.find("h5", string=lambda s: s and "ประเภทธุรกิจตอนจดทะเบียน" in s)
    if h5_tsic1:
        body = h5_tsic1.find_next("div", class_="card-body")
        if body:
            lab = body.find(lambda t: t.name=="div" and t.get_text(strip=True)=="ประเภทธุรกิจ")
            if lab and lab.find_next_sibling("div"): tsic1_code_name = lab.find_next_sibling("div").get_text(" ", strip=True)
            lab = body.find(lambda t: t.name=="div" and t.get_text(strip=True)=="วัตถุประสงค์")
            if lab and lab.find_next_sibling("div"): tsic1_obj = lab.find_next_sibling("div").get_text(" ", strip=True)

    tsic2_code_name, tsic2_obj = "-", "-"
    h5_tsic2 = soup.find("h5", string=lambda s: s and "ประเภทธุรกิจที่ส่งงบการเงินปีล่าสุด" in s)
    if h5_tsic2:
        body = h5_tsic2.find_next("div", class_="card-body")
        if body:
            lab = body.find(lambda t: t.name=="div" and t.get_text(strip=True)=="ประเภทธุรกิจ")
            if lab and lab.find_next_sibling("div"): tsic2_code_name = lab.find_next_sibling("div").get_text(" ", strip=True)
            lab = body.find(lambda t: t.name=="div" and t.get_text(strip=True)=="วัตถุประสงค์")
            if lab and lab.find_next_sibling("div"): tsic2_obj = lab.find_next_sibling("div").get_text(" ", strip=True)

    return {
        "ชื่อ": name, "เลขทะเบียน": reg, "สถานะ": status_text, "วันที่จดทะเบียน": reg_date,
        "ทุนจดทะเบียน": capital, "กลุ่มธุรกิจ": biz_group, "ขนาดธุรกิจ": biz_size,
        "ที่ตั้งสำนักงานใหญ่": address, "กรรมการ": directors,
        "TSIC ตอนจดทะเบียน": tsic1_code_name, "วัตถุประสงค์ตอนจดทะเบียน": tsic1_obj,
        "TSIC ล่าสุด": tsic2_code_name, "วัตถุประสงค์ล่าสุด": tsic2_obj,
    }

def scrape_one_id(driver, tax_id: str, out_dir: str, logs_dir: str):
    # 1) ไปหน้าแรก + ค้นหา (มี retry ชั้นใน)
    with_retry(lambda: go_home_and_search(driver, tax_id), tries=3)

    # 2) รอหน้าโปรไฟล์ (retry ภายนอกเผื่อโหลดไม่ครบ/โดนบล็อก)
    for attempt in range(3):
        try:
            wait_profile_loaded(driver)
            break
        except TimeoutException:
            if attempt == 2:
                # เก็บหลักฐานก่อนสรุป NOT_FOUND
                save_debug(driver, tax_id, out_dir, logs_dir, tag="timeout-profile")
                return None
            close_popup_if_any(driver)
            driver.refresh()
            backoff_sleep(attempt, base=0.6)

    # 3) ดึง HTML แบบ robust
    def _grab_html():
        try:
            WebDriverWait(driver, 8).until(EC.presence_of_element_located((By.CSS_SELECTOR, ".tab1")))
            return driver.find_element(By.CSS_SELECTOR, ".tab1").get_attribute("innerHTML") or driver.page_source
        except Exception:
            return driver.page_source

    html = with_retry(_grab_html, tries=2)

    if looks_blocked_or_empty(html):
        # รีเฟรชหนึ่งทีแล้วลองอีกครั้ง
        driver.refresh(); time.sleep(0.8)
        html = _grab_html()

    data = parse_profile_html(html)
    # ตรวจว่าได้เลขทะเบียนจริงไหม
    reg_ok = data.get("เลขทะเบียน") and data["เลขทะเบียน"] not in ("-","")
    if not reg_ok:
        save_debug(driver, tax_id, out_dir, logs_dir, tag="no-reg")
        return None
    return data

def save_debug(driver, tax_id: str, out_dir: str, logs_dir: str, tag: str):
    canon = canon_tax_id(tax_id)
    dbg_dir = os.path.join(out_dir, "_debug")
    os.makedirs(dbg_dir, exist_ok=True)
    # screenshot
    try:
        png = os.path.join(dbg_dir, f"{canon}.{tag}.png")
        driver.get_screenshot_as_file(png)
    except Exception:
        pass
    # page source
    try:
        html_fp = os.path.join(dbg_dir, f"{canon}.{tag}.html")
        with open(html_fp, "w", encoding="utf-8") as f:
            f.write(driver.page_source or "")
    except Exception:
        pass
    append_log(os.path.join(logs_dir, "debug_ids.txt"), f"{canon}\t{tag}\t{now_utc_iso()}")

# ========== Data / Sheets helpers ==========
HEADERS = [
    "tax_id","ชื่อ","เลขทะเบียน","สถานะ","วันที่จดทะเบียน","ทุนจดทะเบียน",
    "กลุ่มธุรกิจ","ขนาดธุรกิจ","ที่ตั้งสำนักงานใหญ่","กรรมการ",
    "TSIC ตอนจดทะเบียน","วัตถุประสงค์ตอนจดทะเบียน","TSIC ล่าสุด","วัตถุประสงค์ล่าสุด",
    "fetched_at_utc"
]

def read_tax_ids(path: str):
    ids = []
    if path and os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.split("#", 1)[0].strip()
                if not line:
                    continue
                m = re.search(r"\d{12,13}", line)
                if m:
                    ids.append(canon_tax_id(m.group(0)))
    # unique keep order
    seen=set(); out=[]
    for x in ids:
        if x and x not in seen: seen.add(x); out.append(x)
    return out

def open_sheet():
    creds_json = os.getenv("GOOGLE_CREDENTIALS_JSON")
    if not creds_json:
        raise RuntimeError("GOOGLE_CREDENTIALS_JSON is empty.")
    info = json.loads(creds_json)
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    gc = gspread.authorize(creds)
    sheet_id = os.getenv("SHEET_ID")
    if not sheet_id:
        raise RuntimeError("SHEET_ID not set.")
    sh = gc.open_by_key(sheet_id)
    ws = sh.get_worksheet(0)
    first = ws.row_values(1)
    if first != HEADERS:
        ws.resize(1)
        ws.update("A1", [HEADERS])
    return sh, ws

def sheet_index(ws):
    idx = {}
    col = ws.col_values(1)  # A
    for i, v in enumerate(col[1:], start=2):
        t = canon_tax_id(v)
        if t: idx[t] = i
    return idx

def _chunk(it: List[Any], n: int):
    for i in range(0, len(it), n):
        yield it[i:i+n]

def batch_upsert_rows(sh, ws, row_dicts, chunk_size: int = 200):
    if not row_dicts: return
    last_col_letter = "O"  # 15 cols
    existing = sheet_index(ws)

    updates = []   # (row_index, values)
    appends = []   # values

    for rd in row_dicts:
        tax_id = canon_tax_id(rd["tax_id"])
        values = [rd.get(h, "") for h in HEADERS]
        values[0] = f"'{tax_id}"  # keep leading zero
        if tax_id in existing:
            updates.append((existing[tax_id], values))
        else:
            appends.append(values)

    # batched updates
    for part in _chunk(updates, chunk_size):
        data_payload = []
        for row_idx, vals in part:
            rng = f"A{row_idx}:{last_col_letter}{row_idx}"
            data_payload.append({"range": rng, "values": [vals]})
        def _do():
            sh.values_batch_update(body={"valueInputOption":"RAW","data": data_payload})
        with_retry(_do, tries=3)

    # batched appends
    for part in _chunk(appends, chunk_size):
        def _do():
            ws.append_rows(part, value_input_option="RAW")
        with_retry(_do, tries=3)

# ========== Main ==========
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tax-id", default=os.getenv("TAX_ID", "0135563016845"))
    ap.add_argument("--list-file", default="tax_ids.txt")
    ap.add_argument("--out-dir", default="data")
    ap.add_argument("--limit", type=int, default=20, help="จำนวนต่อรอบ (0=ทั้งหมด)")
    ap.add_argument("--skip-existing", choices=["none","sheet","json","both"], default="sheet")
    ap.add_argument("--logs-dir", default="logs")
    args = ap.parse_args()

    ids_all = read_tax_ids(args.list_file)
    if not ids_all:
        t = canon_tax_id(args.tax_id)
        if not re.fullmatch(r"\d{13}", t):
            print("❌ ใส่เลขผู้เสียภาษี 13 หลักให้ถูกต้อง"); return
        ids_all = [t]

    os.makedirs(args.out_dir, exist_ok=True)
    os.makedirs(args.logs_dir, exist_ok=True)

    # Sheets อาจล่มชั่วคราว → เปิดด้วย retry
    sh, ws = with_retry(open_sheet, tries=3)

    done_found = set()
    if args.skip_existing in ("sheet","both"):
        done_found |= set(sheet_index(ws).keys())
    if args.skip_existing in ("json","both"):
        if os.path.isdir(args.out_dir):
            done_found |= {canon_tax_id(fn[:-5]) for fn in os.listdir(args.out_dir) if fn.endswith(".json")}

    remaining = [t for t in (canon_tax_id(x) for x in ids_all) if t and t not in done_found]
    ids = remaining[: (args.limit if args.limit and args.limit > 0 else None)]
    print(f"เหลือในคิว (หลังกรอง FOUND เดิม) {len(remaining)} รายการ → รอบนี้จะทำ {len(ids)} รายการ")

    rows_to_upsert = []
    found_ids, not_found_ids, fail_ids = [], [], []

    driver = build_driver()
    try:
        total = len(ids)
        for i, tax_id in enumerate(ids, start=1):
            print(f"\n🔎 [{i}/{total}] ค้นหาเลขภาษี: {tax_id}")
            try:
                data = scrape_one_id(driver, tax_id, args.out_dir, args.logs_dir)
            except (NoSuchWindowException, WebDriverException) as e:
                # รีสตาร์ทไดรเวอร์ 1 ครั้งแล้วลองใหม่เลขเดิม
                append_log(os.path.join(args.logs_dir, "driver_restart.txt"), f"{now_utc_iso()}\t{tax_id}\t{repr(e)}")
                try:
                    driver.quit()
                except Exception:
                    pass
                driver = build_driver()
                try:
                    data = scrape_one_id(driver, tax_id, args.out_dir, args.logs_dir)
                except Exception as e2:
                    append_log(os.path.join(args.logs_dir, "fail_ids.txt"), f"{tax_id}\t{repr(e2)}")
                    fail_ids.append(tax_id)
                    continue
            except Exception as e:
                append_log(os.path.join(args.logs_dir, "fail_ids.txt"), f"{tax_id}\t{repr(e)}")
                fail_ids.append(tax_id)
                continue

            if not data:
                print("⚠️  NOT_FOUND (จะลบออกจากไฟล์คิวหลังจบรอบ)")
                append_log(os.path.join(args.logs_dir, "not_found_ids.txt"), tax_id)
                not_found_ids.append(tax_id)
            else:
                canon = canon_tax_id(tax_id)
                row = {
                    "tax_id": canon,
                    **data,
                    "fetched_at_utc": now_utc_iso(),
                }
                fp = os.path.join(args.out_dir, f"{canon}.json")
                safe_write_json(fp, row)
                print(f"💾 saved: {fp}")

                rows_to_upsert.append(row)
                found_ids.append(tax_id)

            # คุม rate: ช่วงสั้น ๆ และสุ่ม
            time.sleep(random.uniform(0.9, 1.9))
    finally:
        try: driver.quit()
        except Exception: pass

    # ===== หลังจบรอบ: อัปเดต Google Sheets ทีเดียว =====
    if rows_to_upsert:
        print(f"\n📝 อัปเดต Google Sheets แบบ batch: {len(rows_to_upsert)} แถว …")
        with_retry(lambda: batch_upsert_rows(sh, ws, rows_to_upsert), tries=3)
        print("✅ อัปเดตชีตเสร็จ")

    # ===== ลบรายการที่สำเร็จออกจากไฟล์คิวทีเดียว =====
    done_ids = found_ids + not_found_ids
    if done_ids:
        remove_ids_from_txt(args.list_file, done_ids)
        print(f"🧹 ลบสำเร็จออกจากคิว: {len(done_ids)} รายการ (FOUND={len(found_ids)}, NOT_FOUND={len(not_found_ids)})")

    # ===== สรุปผลรอบ =====
    print("\n===== SUMMARY =====")
    print(f"FOUND      : {len(found_ids)}")
    print(f"NOT_FOUND  : {len(not_found_ids)}")
    print(f"FAILED     : {len(fail_ids)}")
    if fail_ids:
        print("ดู artifacts _debug/*.png และ .html เพื่อไล่ปัญหา")

if __name__ == "__main__":
    # ลดโอกาส burst พร้อมกันเวลารันตาม cron
    time.sleep(random.uniform(1.0, 2.5))
    main()
