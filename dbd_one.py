# -*- coding: utf-8 -*-
import os, re, time, random, json, argparse
from datetime import datetime, timezone
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

# ---- Google Sheets ----
import gspread
from google.oauth2.service_account import Credentials

PAGE_LOAD_TIMEOUT = 90

# ===== Selenium =====
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
    driver = webdriver.Chrome(options=opts)      # Selenium Manager เลือก driver ให้เอง
    driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
    return driver

def close_popup_if_any(driver):
    try:
        time.sleep(0.8)
        for sel in ['#btnWarning', '.modal [data-bs-dismiss="modal"]', '.modal .btn-close', '.swal2-confirm']:
            for el in driver.find_elements(By.CSS_SELECTOR, sel):
                try:
                    if el.is_displayed() and el.is_enabled():
                        el.click(); time.sleep(0.4)
                except: pass
    except: pass

def go_home_and_search(driver, tax_id: str):
    driver.get("https://datawarehouse.dbd.go.th/index")
    wait = WebDriverWait(driver, 40)
    close_popup_if_any(driver)
    sb = wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "input#key-word.form-control")))
    sb.clear(); sb.send_keys(tax_id); time.sleep(0.2); sb.send_keys(Keys.ENTER)
    try:
        wait.until(EC.presence_of_element_located((By.XPATH, "//h4[contains(.,'เลขทะเบียนนิติบุคคล')]")))
        return
    except TimeoutException:
        pass
    for txt in ["รายละเอียด", "ดูรายละเอียด", "ข้อมูลนิติบุคคล"]:
        links = driver.find_elements(By.XPATH, f"//a[contains(.,'{txt}')]")
        if links:
            links[0].click()
            break

def wait_profile_loaded(driver):
    wait = WebDriverWait(driver, 40)
    wait.until(EC.presence_of_element_located((By.XPATH, "//h4[contains(.,'เลขทะเบียนนิติบุคคล')]")))
    time.sleep(0.8)

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
    go_home_and_search(driver, tax_id)
    wait_profile_loaded(driver)
    try:
        WebDriverWait(driver, 12).until(EC.presence_of_element_located((By.CSS_SELECTOR, ".tab1")))
        html = driver.find_element(By.CSS_SELECTOR, ".tab1").get_attribute("innerHTML") or driver.page_source
    except:
        html = driver.page_source
    data = parse_profile_html(html)
    return None if not data.get("เลขทะเบียน") or data["เลขทะเบียน"] in ("-","") else data

# ===== Helpers =====
HEADERS = [
    "tax_id","ชื่อ","เลขทะเบียน","สถานะ","วันที่จดทะเบียน","ทุนจดทะเบียน",
    "กลุ่มธุรกิจ","ขนาดธุรกิจ","ที่ตั้งสำนักงานใหญ่","กรรมการ",
    "TSIC ตอนจดทะเบียน","วัตถุประสงค์ตอนจดทะเบียน","TSIC ล่าสุด","วัตถุประสงค์ล่าสุด",
    "fetched_at_utc"
]

def read_tax_ids(path: str):
    ids = []
    if not path or not os.path.exists(path): return ids
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.split("#", 1)[0].strip()
            if not line: continue
            m = re.search(r"\b\d{13}\b", line)
            if m: ids.append(m.group(0))
    seen=set(); out=[]
    for x in ids:
        if x not in seen: seen.add(x); out.append(x)
    return out

# ===== Google Sheets =====
def open_sheet():
    creds_json = os.getenv("GOOGLE_CREDENTIALS_JSON")
    if not creds_json:
        raise RuntimeError("GOOGLE_CREDENTIALS_JSON is empty. Add repository secret.")
    info = json.loads(creds_json)
    scopes = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    gc = gspread.authorize(creds)
    sheet_id = os.getenv("SHEET_ID")
    if not sheet_id:
        raise RuntimeError("SHEET_ID not set.")
    sh = gc.open_by_key(sheet_id)
    ws = sh.get_worksheet(0)  # gid=0
    # ensure headers
    first = ws.row_values(1)
    if first != HEADERS:
        ws.resize(1)
        ws.update("A1", [HEADERS])
    return ws

def upsert_row(ws, row_dict):
    # key = tax_id
    tax_id = row_dict["tax_id"]
    # หา index คอลัมน์ tax_id
    headers = HEADERS
    col_idx = headers.index("tax_id") + 1
    col_vals = ws.col_values(col_idx)
    row_index = None
    for i, v in enumerate(col_vals[1:], start=2):  # ข้ามหัวตาราง
        if v == tax_id:
            row_index = i
            break
    values = [row_dict.get(h, "") for h in headers]
    if row_index:
        ws.update(f"A{row_index}", [values])
    else:
        ws.append_row(values, value_input_option="USER_ENTERED")

# ===== Main =====
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tax-id", default=os.getenv("TAX_ID", "0135563016845"))
    ap.add_argument("--list-file", default="tax_ids.txt")
    ap.add_argument("--out-dir", default="data")
    args = ap.parse_args()

    ids = read_tax_ids(args.list_file)
    if not ids:
        t = re.sub(r"\D","", args.tax_id)
        if not re.fullmatch(r"\d{13}", t):
            print("❌ ใส่เลขผู้เสียภาษี 13 หลักให้ถูกต้อง"); return
        ids = [t]

    os.makedirs(args.out_dir, exist_ok=True)
    ws = open_sheet()

    driver = build_driver()
    try:
        for i, tax_id in enumerate(ids, start=1):
            print(f"\n🔎 [{i}/{len(ids)}] ค้นหาเลขภาษี: {tax_id}")
            data = scrape_one_id(driver, tax_id)
            if not data:
                print("⚠️  ไม่พบข้อมูล/อ่านไม่สำเร็จ")
                continue
            # enrich + save file
            row = {"tax_id": tax_id, **data, "fetched_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}
            print("✅ พบข้อมูล:"); print(json.dumps(row, ensure_ascii=False, indent=2))
            fp = os.path.join(args.out_dir, f"{tax_id}.json")
            with open(fp, "w", encoding="utf-8") as f:
                json.dump(row, f, ensure_ascii=False, indent=2)
            print(f"💾 saved: {fp}")
            # upsert to Google Sheets
            upsert_row(ws, row)
            print("⬆️  updated Google Sheets")
            time.sleep(random.uniform(1.0, 2.0))
    finally:
        try: driver.quit()
        except: pass

if __name__ == "__main__":
    time.sleep(random.uniform(1.2, 2.5))
    main()
