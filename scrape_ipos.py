"""
US IPO Calendar Scraper v2
- 1企業=1レコード（CIKベース）
- 全届出はイベント履歴として時系列管理
- NASDAQステータスを最優先（実態反映）
"""
import asyncio
import json
import os
import re
import random
import time
from datetime import datetime, timedelta
from pathlib import Path

import httpx
from playwright.async_api import async_playwright

WORKER_URL = os.environ.get("WORKER_URL", "")
ADMIN_TOKEN = os.environ.get("CF_API_TOKEN", "")
GH_TOKEN = os.environ.get("GITHUB_TOKEN", "")

SEC_HEADERS = {
    "User-Agent": "usaipocalendarapp miguoipomiguo@gmail.com",
    "Accept": "application/json",
}

NASDAQ_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Accept": "application/json",
}

GITHUB_MODELS_URL = "https://models.inference.ai.azure.com/chat/completions"


# ============================================================
# SEC EDGAR
# ============================================================

async def fetch_sec_filings() -> list:
    """SEC EDGAR APIからIPO関連書類を取得"""
    print("=== SEC EDGAR ===")
    filings = []

    async with httpx.AsyncClient(timeout=30, headers=SEC_HEADERS) as client:
        for form_type in ["S-1", "F-1", "S-1/A", "F-1/A", "424B4", "RW"]:
            try:
                start_date = (datetime.now() - timedelta(days=120)).strftime("%Y-%m-%d")
                end_date = datetime.now().strftime("%Y-%m-%d")

                resp = await client.get(
                    "https://efts.sec.gov/LATEST/search-index",
                    params={
                        "q": '"initial public offering"',
                        "forms": form_type,
                        "dateRange": "custom",
                        "startdt": start_date,
                        "enddt": end_date,
                    }
                )

                if resp.status_code != 200:
                    print(f"  [{form_type}] HTTP {resp.status_code}")
                    continue

                hits = resp.json().get("hits", {}).get("hits", [])
                print(f"  [{form_type}] {len(hits)} filings")

                for hit in hits[:30]:
                    source = hit.get("_source", {})
                    ciks = source.get("ciks", [])
                    cik = ciks[0] if ciks else ""
                    names = source.get("display_names", [])
                    name = names[0] if names else source.get("entity_name", "")

                    if not cik or not name:
                        continue

                    filings.append({
                        "cik": cik,
                        "company_name": name,
                        "filing_type": form_type,
                        "filing_date": source.get("file_date"),
                        "sec_accession": source.get("file_num"),
                        "event_type": {
                            "S-1": "filing", "F-1": "filing",
                            "S-1/A": "amendment", "F-1/A": "amendment",
                            "424B4": "pricing", "RW": "withdrawal",
                        }.get(form_type, "filing"),
                    })

                await asyncio.sleep(0.3)
            except Exception as e:
                print(f"  [{form_type}] Error: {e}")

    print(f"  SEC total: {len(filings)} filings")
    return filings


# ============================================================
# NASDAQ JSON API
# ============================================================

async def fetch_nasdaq_data() -> dict:
    """NASDAQ APIからupcoming/priced/filed/withdrawnを取得"""
    print("\n=== NASDAQ API ===")
    result = {"upcoming": [], "priced": [], "filed": [], "withdrawn": []}

    async with httpx.AsyncClient(timeout=30, headers=NASDAQ_HEADERS) as client:
        now = datetime.now()
        months = [now.strftime("%Y-%m"), (now + timedelta(days=31)).strftime("%Y-%m")]

        for month in months:
            try:
                resp = await client.get(f"https://api.nasdaq.com/api/ipo/calendar?date={month}")
                if resp.status_code != 200:
                    continue

                data = resp.json().get("data", {})

                # Upcoming (upcomingTable.rows)
                for row in data.get("upcoming", {}).get("upcomingTable", {}).get("rows", []):
                    result["upcoming"].append({
                        "ticker": row.get("proposedTickerSymbol"),
                        "company_name": row.get("companyName"),
                        "exchange": row.get("proposedExchange"),
                        "expected_date": parse_date(row.get("expectedPriceDate")),
                        "price_range": row.get("proposedSharePrice"),
                        "shares_offered": parse_shares(row.get("sharesOffered")),
                    })

                # Priced (direct rows)
                for row in data.get("priced", {}).get("rows", []):
                    result["priced"].append({
                        "ticker": row.get("proposedTickerSymbol"),
                        "company_name": row.get("companyName"),
                        "exchange": row.get("proposedExchange"),
                        "actual_date": parse_date(row.get("pricedDate")),
                        "offer_price": parse_single_price(row.get("proposedSharePrice")),
                        "shares_offered": parse_shares(row.get("sharesOffered")),
                    })

                # Filed
                for row in data.get("filed", {}).get("rows", []):
                    result["filed"].append({
                        "ticker": row.get("proposedTickerSymbol"),
                        "company_name": row.get("companyName"),
                        "filing_date": parse_date(row.get("filedDate")),
                    })

                # Withdrawn
                for row in data.get("withdrawn", {}).get("rows", []):
                    result["withdrawn"].append({
                        "ticker": row.get("proposedTickerSymbol"),
                        "company_name": row.get("companyName"),
                        "exchange": row.get("proposedExchange"),
                        "withdraw_date": parse_date(row.get("withdrawDate")),
                    })

                up_count = len(data.get("upcoming", {}).get("upcomingTable", {}).get("rows", []))
                pr_count = len(data.get("priced", {}).get("rows", []))
                print(f"  [{month}] upcoming={up_count}, priced={pr_count}")
            except Exception as e:
                print(f"  [{month}] Error: {e}")

    total = sum(len(v) for v in result.values())
    print(f"  NASDAQ total: {total}")
    return result


# ============================================================
# マージ（1企業=1レコード）
# ============================================================

def merge_all(sec_filings: list, nasdaq_data: dict) -> list:
    """CIKベースで1企業1レコード。NASDAQステータス最優先"""
    print("\n=== Merge ===")

    # Step 1: CIKごとにSEC filingを集約
    companies = {}
    for f in sec_filings:
        cik = f["cik"]
        if cik not in companies:
            companies[cik] = {
                "id": cik,
                "cik": cik,
                "company_name": f["company_name"],
                "filing_type": f["filing_type"],
                "filing_date": f["filing_date"],
                "events": [],
                "nasdaq_status": None,
            }
        companies[cik]["events"].append({
            "event_type": f["event_type"],
            "filing_type": f["filing_type"],
            "event_date": f["filing_date"],
            "sec_accession": f.get("sec_accession"),
        })
        # 最新filing_typeで更新
        if f["filing_date"] and (not companies[cik].get("_latest_date") or f["filing_date"] > companies[cik]["_latest_date"]):
            companies[cik]["filing_type"] = f["filing_type"]
            companies[cik]["_latest_date"] = f["filing_date"]

    print(f"  SEC: {len(companies)} unique companies")

    # Step 2: NASDAQデータ → ルックアップ作成
    nasdaq_lookup = {}  # normalized_name → info

    for item in nasdaq_data["upcoming"]:
        key = normalize_name(item.get("company_name", ""))
        nasdaq_lookup[key] = {**item, "nasdaq_status": "upcoming"}
        if item.get("ticker"):
            nasdaq_lookup[item["ticker"].upper()] = {**item, "nasdaq_status": "upcoming"}

    for item in nasdaq_data["priced"]:
        key = normalize_name(item.get("company_name", ""))
        nasdaq_lookup[key] = {**item, "nasdaq_status": "listed"}
        if item.get("ticker"):
            nasdaq_lookup[item["ticker"].upper()] = {**item, "nasdaq_status": "listed"}

    for item in nasdaq_data["withdrawn"]:
        key = normalize_name(item.get("company_name", ""))
        if key not in nasdaq_lookup:
            nasdaq_lookup[key] = {**item, "nasdaq_status": "withdrawn"}

    # Step 3: SEC企業にNASDAQマッチング
    matched_keys = set()
    for cik, company in companies.items():
        name_key = normalize_name(company["company_name"])
        match = find_nasdaq_match(name_key, nasdaq_lookup)
        if match:
            apply_nasdaq_data(company, match)
            matched_keys.add(id(match))

    # Step 4: NASDAQのみの銘柄を追加
    for key, ninfo in nasdaq_lookup.items():
        if id(ninfo) in matched_keys:
            continue
        if not ninfo.get("company_name"):
            continue
        ticker = ninfo.get("ticker")
        if not ticker:
            continue
        # 既にtickerで追加済みか確認
        if any(c.get("ticker") == ticker for c in companies.values()):
            continue

        new_id = ticker
        companies[f"nq_{new_id}"] = build_from_nasdaq(ninfo)
        matched_keys.add(id(ninfo))

    # Step 5: ステータス確定
    for company in companies.values():
        company["status"] = determine_status(company)
        company.pop("nasdaq_status", None)
        company.pop("_latest_date", None)

    result = list(companies.values())
    upcoming_count = sum(1 for c in result if c["status"] == "upcoming")
    listed_count = sum(1 for c in result if c["status"] == "listed")
    print(f"  Final: {len(result)} IPOs (upcoming={upcoming_count}, listed={listed_count})")
    return result


def find_nasdaq_match(name_key: str, nasdaq_lookup: dict):
    """企業名の部分一致でNASDAQデータを探す"""
    if name_key in nasdaq_lookup:
        return nasdaq_lookup[name_key]
    # 部分一致
    for nkey, ninfo in nasdaq_lookup.items():
        if len(nkey) < 4:
            continue
        if nkey in name_key or name_key in nkey:
            return ninfo
    return None


def apply_nasdaq_data(company: dict, nasdaq_info: dict):
    """NASDAQデータを企業レコードに適用"""
    company["ticker"] = nasdaq_info.get("ticker") or company.get("ticker")
    company["exchange"] = nasdaq_info.get("exchange") or company.get("exchange")
    company["expected_date"] = nasdaq_info.get("expected_date") or company.get("expected_date")
    company["actual_date"] = nasdaq_info.get("actual_date") or company.get("actual_date")
    company["shares_offered"] = nasdaq_info.get("shares_offered") or company.get("shares_offered")
    company["offer_price"] = nasdaq_info.get("offer_price") or company.get("offer_price")
    company["nasdaq_status"] = nasdaq_info["nasdaq_status"]

    # 価格レンジ
    price_str = nasdaq_info.get("price_range")
    if price_str:
        low, high = parse_price_range(price_str)
        company["price_range_low"] = low
        company["price_range_high"] = high

    # IDをtickerに更新
    if nasdaq_info.get("ticker"):
        company["id"] = nasdaq_info["ticker"]

    # イベント追加
    company["events"].append({
        "event_type": "exchange_" + nasdaq_info["nasdaq_status"],
        "filing_type": None,
        "event_date": nasdaq_info.get("expected_date") or nasdaq_info.get("actual_date"),
    })


def build_from_nasdaq(ninfo: dict) -> dict:
    """NASDAQのみのデータから企業レコードを生成"""
    price_str = ninfo.get("price_range")
    low, high = parse_price_range(price_str) if price_str else (None, None)

    return {
        "id": ninfo.get("ticker") or normalize_name(ninfo["company_name"])[:20],
        "cik": None,
        "company_name": ninfo["company_name"],
        "ticker": ninfo.get("ticker"),
        "exchange": ninfo.get("exchange"),
        "expected_date": ninfo.get("expected_date"),
        "actual_date": ninfo.get("actual_date"),
        "price_range_low": low,
        "price_range_high": high,
        "offer_price": ninfo.get("offer_price"),
        "shares_offered": ninfo.get("shares_offered"),
        "filing_type": None,
        "filing_date": ninfo.get("filing_date"),
        "nasdaq_status": ninfo["nasdaq_status"],
        "events": [{"event_type": "exchange_" + ninfo["nasdaq_status"], "filing_type": None, "event_date": ninfo.get("expected_date") or ninfo.get("actual_date")}],
    }


def determine_status(company: dict) -> str:
    """ステータス判定: NASDAQ最優先"""
    ns = company.get("nasdaq_status")
    if ns == "listed":
        return "listed"
    if ns == "upcoming":
        return "upcoming"
    if ns == "withdrawn":
        return "withdrawn"

    ft = company.get("filing_type")
    if ft == "RW":
        return "withdrawn"
    if ft == "424B4":
        return "priced"
    if ft in ("S-1/A", "F-1/A"):
        return "amended"
    return "filed"


# ============================================================
# Worker更新
# ============================================================

async def update_worker(ipos: list):
    if not WORKER_URL or not ADMIN_TOKEN:
        print("\n[SKIP] No WORKER_URL/ADMIN_TOKEN")
        return

    payload = []
    for ipo in ipos:
        payload.append({k: v for k, v in ipo.items() if k not in ("nasdaq_status", "_latest_date")})

    print(f"\n=== Updating Worker ({len(payload)} IPOs) ===")
    async with httpx.AsyncClient(timeout=60) as client:
        try:
            resp = await client.post(
                f"{WORKER_URL}/api/admin/update",
                json={"ipos": payload},
                headers={"Authorization": f"Bearer {ADMIN_TOKEN}"}
            )
            print(f"  Worker: {resp.status_code}")
        except Exception as e:
            print(f"  Worker error: {e}")


# ============================================================
# LLM
# ============================================================

async def enrich_with_llm(ipos: list) -> list:
    if not GH_TOKEN:
        return ipos
    print("\n=== LLM ===")
    enriched = 0
    priority = [i for i in ipos if i.get("status") in ("upcoming", "listed") and not i.get("description")]
    async with httpx.AsyncClient(timeout=60) as client:
        for ipo in priority[:20]:
            try:
                resp = await client.post(GITHUB_MODELS_URL,
                    json={"model": "gpt-4o-mini", "messages": [{"role": "user", "content": f'Brief 2-sentence description for IPO company: {ipo["company_name"]}. JSON only: {{"description":"...","products_services":"...","industry":"..."}}'}], "temperature": 0.1, "max_tokens": 200},
                    headers={"Authorization": f"Bearer {GH_TOKEN}", "Content-Type": "application/json"})
                if resp.status_code == 200:
                    c = resp.json()["choices"][0]["message"]["content"]
                    if "```" in c: c = c.split("```json")[-1].split("```")[0] if "```json" in c else c.split("```")[1].split("```")[0]
                    d = json.loads(c.strip())
                    ipo.update({k: v for k, v in d.items() if v})
                    enriched += 1
                await asyncio.sleep(1)
            except: pass
    print(f"  Enriched: {enriched}")
    return ipos


# ============================================================
# ユーティリティ
# ============================================================

def normalize_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", name.lower().strip())[:40]

def parse_date(s: str) -> str:
    if not s: return None
    for fmt in ["%m/%d/%Y", "%Y-%m-%d", "%b %d, %Y"]:
        try: return datetime.strptime(s.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError: continue
    return None

def parse_price_range(s: str) -> tuple:
    if not s: return None, None
    m = re.search(r"([\d.]+)\s*[-–]\s*([\d.]+)", s)
    if m: return float(m.group(1)), float(m.group(2))
    single = re.search(r"([\d.]+)", s)
    if single:
        v = float(single.group(1))
        return v, v
    return None, None

def parse_single_price(s: str) -> float:
    if not s: return None
    m = re.search(r"([\d.]+)", s)
    return float(m.group(1)) if m else None

def parse_shares(s: str) -> int:
    if not s: return None
    try: return int(s.replace(",", ""))
    except: return None


# ============================================================
# メイン
# ============================================================

async def main():
    print(f"{'='*60}")
    print(f"US IPO Calendar v2 - {datetime.now().isoformat()}")
    print(f"{'='*60}")

    sec = await fetch_sec_filings()
    nasdaq = await fetch_nasdaq_data()
    merged = merge_all(sec, nasdaq)
    enriched = await enrich_with_llm(merged)

    with open("ipo_data.json", "w") as f:
        json.dump([{k:v for k,v in i.items() if k not in ("nasdaq_status","_latest_date")} for i in enriched], f, ensure_ascii=False, indent=2, default=str)

    await update_worker(enriched)
    print(f"\nDone: {len(merged)} IPOs")

if __name__ == "__main__":
    asyncio.run(main())
