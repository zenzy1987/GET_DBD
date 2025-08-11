# -*- coding: utf-8 -*-
import argparse, sys, urllib.parse, requests

def fetch_summary(term: str, lang: str = "th"):
    title = urllib.parse.quote(term.strip())
    url = f"https://{lang}.wikipedia.org/api/rest_v1/page/summary/{title}"
    r = requests.get(url, timeout=30, headers={"User-Agent": "GH-Actions-Demo/1.0"})
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return r.json()

def main():
    p = argparse.ArgumentParser(description="Print Wikipedia summary to terminal")
    p.add_argument("--term", default="ประเทศไทย", help="คำที่ต้องค้นหา (เช่น ประเทศไทย, Thailand)")
    p.add_argument("--lang", default="th", help="ภาษา (th หรือ en)")
    args = p.parse_args()

    data = fetch_summary(args.term, args.lang)
    if not data:
        print(f"❌ ไม่พบหน้าในวิกิพีเดีย: {args.term} ({args.lang})")
        sys.exit(1)

    title = data.get("title", "")
    desc = data.get("description") or ""
    extract = (data.get("extract") or "").replace("\n", " ").strip()
    link = (data.get("content_urls", {}).get("desktop", {}) or {}).get("page", "")

    print("✅ ผลการค้นหา")
    print(f"  ชื่อเรื่อง : {title}")
    if desc:
        print(f"  คำอธิบาย : {desc}")
    if link:
        print(f"  ลิงก์     : {link}")
    print("  สรุปย่อ  :")
    print(" ", extract[:600] + ("…" if len(extract) > 600 else ""))

if __name__ == "__main__":
    main()
