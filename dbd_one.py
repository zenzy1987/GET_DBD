# -*- coding: utf-8 -*-
import os, re, time, random, json, argparse
from datetime import datetime, timezone
from urllib.parse import urljoin
from bs4 import BeautifulSoup

# Selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, ElementClickInterceptedException, StaleElementReferenceException
)

# Google Sheets
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.errors import HttpError

PAGE_LOAD_TIMEOUT = 90
BASE = "https://datawarehouse.dbd.go.th"

# ========== Utils ==========
def canon_tax_id(x: str) -> str:
    """ทำเลขผู้เสียภาษีให้เป็นรูปแบบมาตรฐาน: เก็บเฉพาะตัวเลขและเติม 0 ซ้ายให้ครบ 13 หลัก"""
    t = re.sub(r"\D", "", str(x or ""))
    return t.zfill(13) if t else ""

def retry_backoff(fn, max_attempts=5, retriable_status=(429, 403)):
    """รันฟังก์ชันที่เรียก Google API พร้อม backoff อัตโนมัติ"""
    for attempt in range(max_attempts):
        try:
            return fn()
        except HttpError as e:
            code = getattr(e.resp, "status", None)
            if code in retriable_status:
                sleep_time = (2 ** attempt) + random.random()
                print(f"⏳ quota/backoff (HTTP {code}) → sleep {sleep_time:.1f}s")
                time.sleep(sleep_time)
                continue
            raise

# ========== Selenium helpers ==========
def build_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--window-size=1365,900")
    ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125 Safari/537.36"
    opts.add_argument(f"--user-agent={ua}")
    chrome_path = os.getenv("CHROME_PATH") or os.getenv("GOOGLE_CHROME_BIN")
    if chrome_path:
        opts.binary_location = chrome_path
    driver = webdriver.Chrome(options=opts)  # Selenium Manager จะจัดการ chromedriver ให้เอง
    driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
    return driver

def close_popup_if_any(driver):
    try:
        time.sleep(0.6)
        sels = ['#btnWarning', '.modal [data-bs-dismiss="modal"]', '.modal .btn-close', '.swal2-confirm']
        for sel in sels:
            for el in driver.find_elements(By.CSS_SELECTOR, sel):
                try:
                    if el.is_displayed() and el.is_enabled():
                        el.click(); time.sleep(0.3)
                except Exception:
                    pass
    except Exception:
        pass

def safe_open_link(driver, el):
    """พยายามเปิดลิงก์แม้มี overlay"""
    href = None
    try:
        href = el.get_attribute("href")
    except StaleElementReferenceException:
        pass
    if href:
        driver.get(urljoin(BASE, href))
        return True
    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
        time.sleep(0.15)
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

def go_home_and_search(driver, tax_id: str):
    driver.get(f"{BASE}/index")
    wait = WebDriverWait(driver, 40)
    close_popup_if_any(driver)
    sb = wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "input#key-word.form-control")))
    sb.clear(); sb.send_keys(tax_id); time.sleep(0.2); sb.send_keys(Keys.ENTER)

    # ถ้าเข้าหน้าโปรไฟล์ได้เลย
    try:
        wait.until(EC.presence_of_element_located((By.XPATH, "//h4[contains(.,'เลขทะเบียนนิติบุคคล')]")))
        return
    except TimeoutException:
        pass

    # หน้า list → คลิกรายละเอียด
    for label in ["รายละเอียด", "ดูรายละเอียด", "ข้อมูลนิติบุคคล"]:
        links = driver.find_elements(By.XPATH, f"//a[contains(.,'{label}')]")
        if links:
            close_popup_if_any(driver)
            if safe_open_link(driver, links[0]):
                return
            try:
                href = links[0].get_attribute("href")
                if href:
                    driver.get(urljoin(BASE, href))
                    return
            except Exception:
                pass

def wait_profile_loaded(driver):
    wait = WebDriverWait(driver, 40)
    wait.until(EC.presence_of_element_located((By.XPATH, "//h4[contains(.,'เลขทะเบียนนิติบุคคล')]")))
    time.sleep(0.6)

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

def scrape_one_id(driver, tax_id: str):
    for attempt in range(2):
        go_home_and_search(driver, tax_id)
        try:
            wait_profile_loaded(driver)
            break
        except TimeoutException:
            if attempt == 1: raise
            time.sleep(1.2)

    try:
        WebDriverWait(driver, 12).until(EC.presence_of_element_located((By.CSS_SELECTOR, ".tab1")))
        html = driver.find_element(By.CSS_SELECTOR, ".tab1").get_attribute("innerHTML") or driver.page_source
    except Exception:
        html = driver.page_source
    data = parse_profile_html(html)
    return None if not data.get("เลขทะเบียน") or data["เลขทะเบียน"] in ("-","") else data

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
                m = re.search(r"\d{12,13}", line)  # ยอม 12–13 หลักแล้ว normalize ต่อ
                if m:
                    ids.append(canon_tax_id(m.group(0)))
    # unique (keep order)
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
    ws = sh.get_worksheet(0)  # gid=0
    first = ws.row_values(1)
    if first != HEADERS:
        ws.resize(1)
        retry_backoff(lambda: ws.update("A1", [HEADERS]))
    return sh, ws

def build_taxid_rowindex(ws):
    """อ่านคอลัมน์ A ครั้งเดียวแล้วสร้าง map: tax_id → row_index"""
    col = ws.col_values(1)  # รวม header
    idx = {}
    for i, v in enumerate(col[1:], start=2):
        t = canon_tax_id(v)
        if t:
            idx[t] = i
    return idx, len(col)  # last_row+1 สำหรับ append

class SheetBuffer:
    """
    เก็บรายการที่จะ upsert เป็น batch:
      - updates: รายการที่มีแถวอยู่แล้ว → ใช้ values_batch_update
      - inserts: รายการใหม่ → ใช้ values_append ทีละก้อน
    """
    def __init__(self, sh, ws, row_index_map, batch_size=50):
        self.sh = sh
        self.ws = ws
        self.title = ws.title
        self.idx = row_index_map  # tax_id → row
        self.batch_size = batch_size
        self.updates = []  # list of dict: {'range': 'Sheet1!A5:O5', 'values': [[...]]}
        self.inserts = []  # list of values rows (no range needed for append)
        self.next_append_row = ws.row_count + 1  # ไม่ใช้จริง แต่เก็บไว้เผื่อ

    def add(self, row_dict):
        tax_id = canon_tax_id(row_dict["tax_id"])
        values = [row_dict.get(h, "") for h in HEADERS]
        values[0] = f"'{tax_id}"  # กันศูนย์หาย
        if tax_id in self.idx:
            r = self.idx[tax_id]
            a1 = f"{self.title}!A{r}:{chr(ord('A')+len(HEADERS)-1)}{r}"
            self.updates.append({"range": a1, "values": [values]})
        else:
            self.inserts.append(values)
        # flush อัตโนมัติเมื่อเกินก้อน
        if len(self.updates) + len(self.inserts) >= self.batch_size:
            self.flush()

    def flush(self):
        # 1) เขียนอัปเดตที่มี range (ทับแถวเดิม)
        if self.updates:
            data = [{"range": u["range"], "values": u["values"]} for u in self.updates]
            def do_update():
                return self.sh.values_batch_update(
                    data=data,
                    value_input_option="RAW"
                )
            retry_backoff(do_update)
            self.updates.clear()

        # 2) append แถวใหม่เป็นก้อนเดียว
        if self.inserts:
            def do_append():
                return self.sh.values_append(
                    range=f"{self.title}!A1",
                    params={"valueInputOption": "RAW", "insertDataOption": "INSERT_ROWS"},
                    body={"values": self.inserts}
                )
            retry_backoff(do_append)
            # อัปเดต index map สำหรับแถวใหม่ (ประมาณตำแหน่งปลายตาราง)
            # เคสนี้ไม่รู้เลข row ที่แน่ชัดจนกว่าจะ re-scan; แต่พอสำหรับลด API calls แล้ว
            self.inserts.clear()

# ========== Main ==========
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tax-id", default=os.getenv("TAX_ID", "0135563016845"))
    ap.add_argument("--list-file", default="tax_ids.txt")
    ap.add_argument("--out-dir", default="data")
    ap.add_argument("--limit", type=int, default=40, help="จำนวนต่อรอบ (0=ทั้งหมด)")
    ap.add_argument("--offset", type=int, default=0, help="ข้ามกี่รายการแรก (ปกติปล่อย 0 เมื่อใช้ skip)")
    ap.add_argument("--skip-existing", choices=["none","sheet","json","both"], default="sheet",
                    help="ข้ามเลขที่มีอยู่แล้วใน sheet/json")
    ap.add_argument("--batch-size", type=int, default=50, help="ขนาด batch ต่อการเขียน Sheets หนึ่งครั้ง")
    args = ap.parse_args()

    ids_all = read_tax_ids(args.list_file)
    if not ids_all:
        t = canon_tax_id(args.tax_id)
        if not re.fullmatch(r"\d{13}", t):
            print("❌ ใส่เลขผู้เสียภาษี 13 หลักให้ถูกต้อง"); return
        ids_all = [t]

    os.makedirs(args.out_dir, exist_ok=True)
    sh, ws = open_sheet()

    # กรองรายการที่ทำแล้วก่อน → slice ตาม limit/offset
    done = set()
    if args.skip_existing in ("sheet","both"):
        idx_map, _ = build_taxid_rowindex(ws)
        done |= set(idx_map.keys())
    else:
        idx_map, _ = build_taxid_rowindex(ws)

    from_json = set()
    if args.skip_existing in ("json","both"):
        if os.path.isdir(args.out_dir):
            from_json = {canon_tax_id(fn[:-5]) for fn in os.listdir(args.out_dir) if fn.endswith(".json")}
        done |= from_json

    remaining = [t for t in (canon_tax_id(x) for x in ids_all) if t not in done]
    start = max(args.offset, 0)
    end = (start + args.limit) if args.limit and args.limit > 0 else None
    ids = remaining[start:end]
    print(f"เหลือในคิว {len(remaining)} รายการ → รอบนี้จะทำ {len(ids)} รายการ")

    buf = SheetBuffer(sh, ws, idx_map, batch_size=args.batch_size)

    driver = build_driver()
    try:
        total = len(ids)
        for i, tax_id in enumerate(ids, start=1):
            print(f"\n🔎 [{i}/{total}] ค้นหาเลขภาษี: {tax_id}")
            try:
                data = scrape_one_id(driver, tax_id)
            except Exception as e:
                print(f"❌ error: {e}")
                time.sleep(1.0)
                continue

            if not data:
                print("⚠️  ไม่พบข้อมูล/อ่านไม่สำเร็จ")
            else:
                canon = canon_tax_id(tax_id)
                row = {
                    "tax_id": canon,
                    **data,
                    "fetched_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                }
                fp = os.path.join(args.out_dir, f"{canon}.json")
                with open(fp, "w", encoding="utf-8") as f:
                    json.dump(row, f, ensure_ascii=False, indent=2)
                print(f"💾 saved: {fp}")

                # ===== ใช้ batch buffer แทนการ upsert ทีละแถว =====
                buf.add(row)
                print("📝 queued for batch write")

            time.sleep(random.uniform(1.0, 2.0))
    finally:
        try:
            driver.quit()
        except Exception:
            pass
        # flush งานค้างทั้งหมดก่อนจบ
        print("\n🚀 flush batch to Google Sheets ...")
        buf.flush()
        print("✅ done")

if __name__ == "__main__":
    time.sleep(random.uniform(1.2, 2.5))
    main()
