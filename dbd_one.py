# -*- coding: utf-8 -*-
import os, re, time, random, json, argparse, signal
from datetime import datetime, timezone
from typing import Any, Dict, List
import requests
import atexit

# Google Sheets
import gspread
from google.oauth2.service_account import Credentials

API_URL = "https://openapi.dbd.go.th/api/v1/juristic_person/{}"

# ========= Single-instance lock =========
LOCK_FILE = os.getenv("LOCK_FILE", ".open_dbd.lock")
LOCK_TTL_SEC = int(os.getenv("LOCK_TTL_SEC", "7200"))  # 2 ชม.

def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def acquire_lock(lock_path: str = LOCK_FILE, ttl_sec: int = LOCK_TTL_SEC):
    """สร้างไฟล์ล็อกแบบ atomic; ถ้ามีอยู่และยังไม่หมดอายุให้หยุดรัน"""
    while True:
        try:
            fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                info = {"pid": os.getpid(), "started_utc": _now_utc_iso(), "cwd": os.getcwd()}
                f.write(json.dumps(info, ensure_ascii=False))
            # ลงทะเบียนลบล็อกอัตโนมัติเมื่อจบโปรเซส
            atexit.register(release_lock, lock_path)
            for sig in (signal.SIGINT, signal.SIGTERM):
                signal.signal(sig, lambda *_: exit(0))
            return True
        except FileExistsError:
            # ตรวจ TTL
            try:
                with open(lock_path, "r", encoding="utf-8") as f:
                    meta = json.load(f)
                # ถ้าไฟล์ล็อกเก่ากว่า TTL ให้ลบ (stale lock)
                mtime = os.path.getmtime(lock_path)
                if time.time() - mtime > ttl_sec:
                    try: os.remove(lock_path)
                    except OSError: pass
                    # ลูปใหม่เพื่อพยายามครอบล็อกอีกครั้ง
                    continue
                else:
                    print(f"⛔ พบการทำงานอยู่แล้ว (lock: {lock_path}) — ป้องกันรันซ้อน จบการทำงาน")
                    return False
            except Exception:
                # ถ้าอ่านไม่ได้ ให้พยายามเคลียร์แล้วลองใหม่
                try: os.remove(lock_path)
                except OSError: pass
                continue

def release_lock(lock_path: str = LOCK_FILE):
    try:
        if os.path.exists(lock_path):
            os.remove(lock_path)
    except OSError:
        pass

# ========= Utils =========
def canon_tax_id(x: str) -> str:
    t = re.sub(r"\D", "", str(x or ""))
    return t.zfill(13) if t else ""

def now_utc_iso() -> str:
    return _now_utc_iso()

def read_tax_ids(path: str):
    ids = []
    if path and os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.split("#", 1)[0].strip()
                if not line: continue
                m = re.search(r"\d{12,13}", line)
                if m: ids.append(canon_tax_id(m.group(0)))
    seen, out = set(), []
    for x in ids:
        if x and x not in seen:
            seen.add(x); out.append(x)
    return out

def remove_ids_from_txt(path: str, tax_ids):
    if not os.path.exists(path): return
    targets = {canon_tax_id(t) for t in tax_ids if t}
    tmp = path + ".tmp"
    with open(path, "r", encoding="utf-8") as src, open(tmp, "w", encoding="utf-8") as dst:
        for line in src:
            raw = line.split("#", 1)[0].strip()
            cur = canon_tax_id(re.sub(r"\D", "", raw)) if raw else ""
            if not cur or cur not in targets:
                dst.write(line if line.endswith("\n") else line + "\n")
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

# ========= Sheets helpers =========
HEADERS = [
    "tax_id","ชื่อ","เลขทะเบียน","สถานะ","วันที่จดทะเบียน","ทุนจดทะเบียน",
    "กลุ่มธุรกิจ","ขนาดธุรกิจ","ที่ตั้งสำนักงานใหญ่","กรรมการ",
    "TSIC ตอนจดทะเบียน","วัตถุประสงค์ตอนจดทะเบียน","TSIC ล่าสุด","วัตถุประสงค์ล่าสุด",
    "fetched_at_utc"
]

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
        sh.values_batch_update(body={"valueInputOption":"RAW","data": data_payload})

    # batched appends
    for part in _chunk(appends, chunk_size):
        ws.append_rows(part, value_input_option="RAW")

# ========= Open-DBD API (free) =========
def pick(obj: Dict[str, Any], *paths, default: str = "") -> str:
    for p in paths:
        cur = obj; ok = True
        for k in p.split("."):
            if isinstance(cur, dict) and k in cur:
                cur = cur[k]
            else:
                ok = False; break
        if ok and cur not in (None, ""):
            return str(cur)
    return default

def pick_obj(base: Dict[str, Any], keys: List[str], fallback: Dict[str, Any]):
    if not isinstance(base, dict): return fallback
    for k in keys:
        if k in base and isinstance(base[k], dict):
            return base[k]
    return fallback

def fetch_open_dbd(juristic_id: str, timeout=15, max_retries=3):
    last_err = None
    for i in range(max_retries):
        try:
            r = requests.get(API_URL.format(juristic_id), timeout=timeout, headers={"User-Agent": "Mozilla/5.0 (API client)"})
            if r.status_code == 200:
                data = r.json()
                status_code = pick(data, "status.code", default="")
                if status_code and status_code != "1000":
                    return {"ok": False, "reason": f"status.code={status_code}", "raw": data}
                core = None
                if isinstance(data.get("data"), list) and data["data"]:
                    core = data["data"][0].get("cd:OrganizationJuristicPerson") or data["data"][0]
                elif isinstance(data.get("data"), dict):
                    core = data["data"]
                else:
                    return {"ok": False, "reason": "no data", "raw": data}

                addr = pick_obj(core, ["cd:OrganizationJuristicAddress","organization_juristic_address","address"], {})
                addr_type = pick_obj(addr, ["cr:AddressType","address_type"], {})
                objv = pick_obj(core, ["cd:OrganizationJuristicObjective","organization_juristic_objective","objective"], {})
                objv1 = pick_obj(objv, ["td:JuristicObjective","juristic_objective"], {})

                parsed = {
                    "ชื่อ": pick(core, "cd:OrganizationJuristicNameTH","organization_juristic_name_th","name_th", default=""),
                    "เลขทะเบียน": pick(core, "cd:OrganizationJuristicID","organization_juristic_id","juristic_id", default=juristic_id),
                    "สถานะ": pick(core, "cd:OrganizationJuristicStatus","organization_juristic_status","status", default=""),
                    "วันที่จดทะเบียน": pick(core, "cd:OrganizationJuristicRegisterDate","organization_juristic_register_date","register_date", default=""),
                    "ทุนจดทะเบียน": pick(core, "cd:OrganizationJuristicRegisterCapital","organization_juristic_register_capital","register_capital", default=""),
                    "กลุ่มธุรกิจ": pick(core, "cd:OrganizationJuristicBusinessGroup","business_group", default=""),
                    "ขนาดธุรกิจ": pick(core, "cd:OrganizationJuristicBusinessSize","business_size", default=""),
                    "ที่ตั้งสำนักงานใหญ่": pick(addr_type, "cd:Address","address", default=""),
                    "กรรมการ": "",
                    "TSIC ตอนจดทะเบียน": "",
                    "วัตถุประสงค์ตอนจดทะเบียน": pick(objv1, "td:JuristicObjectiveTextTH","juristic_objective_text_th", default=""),
                    "TSIC ล่าสุด": "",
                    "วัตถุประสงค์ล่าสุด": "",
                }
                return {"ok": True, "data": parsed, "raw": data}
            elif r.status_code in (401, 403):
                return {"ok": False, "reason": f"HTTP {r.status_code} (อาจต้องใช้ key หรือถูกจำกัดชั่วคราว)"}
            elif r.status_code == 404:
                return {"ok": False, "reason": "NOT_FOUND"}
            else:
                last_err = f"HTTP {r.status_code}: {r.text[:200]}"
        except requests.RequestException as e:
            last_err = repr(e)
        # backoff + jitter
        time.sleep(min(6, 0.8 * (2 ** i)) + random.uniform(0.05, 0.35))
    return {"ok": False, "reason": last_err or "request failed"}

# ========= Main =========
def main():
    # ---- single-instance guard ----
    if not acquire_lock():
        return

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
            print("❌ ใส่เลขผู้เสียภาษี/เลขทะเบียน 13 หลักให้ถูกต้อง"); return
        ids_all = [t]

    os.makedirs(args.out_dir, exist_ok=True)
    os.makedirs(args.logs_dir, exist_ok=True)

    # เปิด Google Sheet
    sh, ws = open_sheet()

    done_found = set()
    if args.skip_existing in ("sheet","both"):
        done_found |= set(sheet_index(ws).keys())
    if args.skip_existing in ("json","both"):
        if os.path.isdir(args.out_dir):
            done_found |= {canon_tax_id(fn[:-5]) for fn in os.listdir(args.out_dir) if fn.endswith(".json")}

    remaining = [t for t in (canon_tax_id(x) for x in ids_all) if t and t not in done_found]
    ids = remaining[: (args.limit if args.limit and args.limit > 0 else None)]
    print(f"เหลือในคิว (หลังกรอง FOUND เดิม) {len(remaining)} รายการ → รอบนี้จะทำ {len(ids)} รายการ")

    rows_to_upsert, found_ids, not_found_ids, fail_ids = [], [], [], []

    for i, tax_id in enumerate(ids, start=1):
        print(f"\n🔎 [{i}/{len(ids)}] Open-DBD: {tax_id}")
        res = fetch_open_dbd(tax_id)
        if res.get("ok"):
            parsed = res["data"]
            row = {"tax_id": tax_id, **parsed, "fetched_at_utc": now_utc_iso()}
            fp = os.path.join(args.out_dir, f"{tax_id}.json")
            safe_write_json(fp, {"parsed": row, "raw": res.get("raw")})
            print(f"💾 saved: {fp}")
            rows_to_upsert.append(row)
            found_ids.append(tax_id)
        else:
            reason = res.get("reason", "")
            append_log(os.path.join(args.logs_dir, "fail_openapi.txt"), f"{tax_id}\t{reason}\t{now_utc_iso()}")
            if reason == "NOT_FOUND":
                not_found_ids.append(tax_id); print("⚠️  NOT_FOUND")
            else:
                fail_ids.append(tax_id); print(f"❌ FAIL: {reason}")

        # rate-limit: สุภาพกับเซิร์ฟเวอร์
        time.sleep(random.uniform(0.7, 1.4))

    # อัปเดต Google Sheets (batch)
    if rows_to_upsert:
        print(f"\n📝 อัปเดต Google Sheets แบบ batch: {len(rows_to_upsert)} แถว …")
        try:
            batch_upsert_rows(sh, ws, rows_to_upsert)
            print("✅ อัปเดตชีตเสร็จ")
        except Exception as e:
            print(f"❌ อัปเดตชีตล้มเหลว: {e}")

    # ลบรายการที่สำเร็จ/ไม่พบ ออกจากคิว
    done_ids = found_ids + not_found_ids
    if done_ids:
        remove_ids_from_txt(args.list_file, done_ids)
        print(f"🧹 ลบสำเร็จออกจากคิว: {len(done_ids)} รายการ (FOUND={len(found_ids)}, NOT_FOUND={len(not_found_ids)})")

    # สรุป
    print("\n===== SUMMARY =====")
    print(f"FOUND      : {len(found_ids)}")
    print(f"NOT_FOUND  : {len(not_found_ids)}")
    print(f"FAILED     : {len(fail_ids)}")

if __name__ == "__main__":
    # กัน burst เวลา cron
    time.sleep(random.uniform(0.8, 2.0))
    main()