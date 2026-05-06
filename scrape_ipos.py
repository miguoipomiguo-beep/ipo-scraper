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
    """
    SEC EDGAR 2段階取得:
    Step 1: Worker Proxy経由でS-1/F-1の新規申請を検索→CIKリストを取得
    Step 2: 各CIKに対してSubmissions APIでフルタイムラインを取得
    """
    print("=== SEC EDGAR ===")

    # Step 1: 新規IPO申請のCIKを検索（Worker Proxy経由）
    cik_set = set()
    async with httpx.AsyncClient(timeout=60, headers=SEC_HEADERS) as client:
        proxy_url = WORKER_URL + "/api/admin/sec-proxy" if WORKER_URL else None

        for form_type in ["S-1", "F-1"]:
            try:
                start_date = (datetime.now() - timedelta(days=120)).strftime("%Y-%m-%d")
                end_date = datetime.now().strftime("%Y-%m-%d")

                if proxy_url:
                    resp = await client.get(proxy_url, params={
                        "forms": form_type, "startdt": start_date, "enddt": end_date,
                    })
                else:
                    resp = await client.get(
                        "https://efts.sec.gov/LATEST/search-index",
                        params={"q": '"initial public offering"', "forms": form_type,
                                "dateRange": "custom", "startdt": start_date, "enddt": end_date}
                    )

                if resp.status_code != 200:
                    print(f"  [{form_type}] HTTP {resp.status_code}")
                    continue

                hits = resp.json().get("hits", {}).get("hits", [])
                for hit in hits[:50]:
                    source = hit.get("_source", {})
                    ciks = source.get("ciks", [])
                    if ciks:
                        cik_set.add(ciks[0])

                print(f"  [{form_type}] {len(hits)} hits → {len(cik_set)} CIKs so far")
                await asyncio.sleep(0.3)
            except Exception as e:
                print(f"  [{form_type}] Error: {e}")

    print(f"  Step 1 complete: {len(cik_set)} unique CIKs")

    # Step 2: 各CIKのSubmissions APIでフルタイムライン取得
    print(f"  Step 2: Fetching timelines...")
    companies = []
    ipo_forms = {"S-1", "F-1", "S-1/A", "F-1/A", "RW", "424B4", "EFFECT"}
    event_type_map = {
        "S-1": "filing", "F-1": "filing",
        "S-1/A": "amendment", "F-1/A": "amendment",
        "424B4": "pricing", "RW": "withdrawal", "EFFECT": "effective",
    }

    async with httpx.AsyncClient(timeout=30, headers=SEC_HEADERS) as client:
        for cik in list(cik_set)[:80]:  # 最大80社（レート制限考慮）
            try:
                # Worker Proxy経由 or 直接
                if WORKER_URL:
                    url = f"{WORKER_URL}/api/admin/sec-submissions?cik={cik}"
                else:
                    cik_padded = cik.lstrip("0").zfill(10)
                    url = f"https://data.sec.gov/submissions/CIK{cik_padded}.json"
                resp = await client.get(url)

                if resp.status_code != 200:
                    continue

                data = resp.json()
                company_name = data.get("name", "")
                tickers = data.get("tickers", [])
                ticker = tickers[0] if tickers else None

                # IPO関連のfiling履歴を抽出
                recent = data.get("filings", {}).get("recent", {})
                forms = recent.get("form", [])
                dates = recent.get("filingDate", [])
                accessions = recent.get("accessionNumber", [])
                descs = recent.get("primaryDocDescription", [])

                events = []
                latest_form = None
                latest_date = None

                for i in range(len(forms)):
                    if forms[i] in ipo_forms:
                        event_date = dates[i] if i < len(dates) else None
                        events.append({
                            "event_type": event_type_map.get(forms[i], "filing"),
                            "filing_type": forms[i],
                            "event_date": event_date,
                            "sec_accession": accessions[i] if i < len(accessions) else None,
                            "details": descs[i] if i < len(descs) else None,
                        })
                        # 最新のfiling
                        if event_date and (not latest_date or event_date > latest_date):
                            latest_date = event_date
                            latest_form = forms[i]

                if not events:
                    continue

                # 最初のS-1/F-1の日付をfiling_dateとする
                first_filing_date = None
                for ev in reversed(events):
                    if ev["filing_type"] in ("S-1", "F-1"):
                        first_filing_date = ev["event_date"]
                        break

                companies.append({
                    "cik": cik,
                    "company_name": company_name,
                    "ticker": ticker,
                    "filing_type": latest_form,
                    "filing_date": first_filing_date,
                    "events": events,
                })

                await asyncio.sleep(0.15)  # SEC rate limit: ~10 req/sec
            except Exception as e:
                pass  # 個別エラーはスキップ

    print(f"  Step 2 complete: {len(companies)} companies with timelines")
    return companies


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

def merge_all(sec_companies: list, nasdaq_data: dict) -> list:
    """CIKベースで1企業1レコード。NASDAQステータス最優先"""
    print("\n=== Merge ===")

    # Step 1: SEC companiesをそのままCIK辞書に（Submissions API由来で既にCIK単位）
    companies = {}
    for c in sec_companies:
        cik = c["cik"]
        companies[cik] = {
            "id": c.get("ticker") or cik,  # ticker優先
            "cik": cik,
            "company_name": c["company_name"],
            "ticker": c.get("ticker"),
            "filing_type": c.get("filing_type"),
            "filing_date": c.get("filing_date"),
            "events": c.get("events", []),
            "nasdaq_status": None,
        }

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
        match = None
        # まずtickerで直接マッチ
        if company.get("ticker"):
            ticker_upper = company["ticker"].upper()
            if ticker_upper in nasdaq_lookup:
                match = nasdaq_lookup[ticker_upper]
        # tickerで見つからなければ企業名で部分一致
        if not match:
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

    # バッチ分割（15件ずつ、Worker CPU制限対策）
    batch_size = 15
    batches = [payload[i:i+batch_size] for i in range(0, len(payload), batch_size)]
    print(f"\n=== Updating Worker ({len(payload)} IPOs in {len(batches)} batches) ===")

    async with httpx.AsyncClient(timeout=90) as client:
        # まず全データ削除
        try:
            resp = await client.post(
                f"{WORKER_URL}/api/admin/update",
                json={"ipos": [], "clean": True},
                headers={"Authorization": f"Bearer {ADMIN_TOKEN}"}
            )
            print(f"  Clean: {resp.status_code}")
            await asyncio.sleep(1)
        except Exception as e:
            print(f"  Clean error: {e}")

        # 各バッチをINSERT
        for idx, batch in enumerate(batches):
            try:
                resp = await client.post(
                    f"{WORKER_URL}/api/admin/update",
                    json={"ipos": batch},
                    headers={"Authorization": f"Bearer {ADMIN_TOKEN}"}
                )
                print(f"  Batch {idx+1}/{len(batches)}: {resp.status_code}")
                await asyncio.sleep(1)
            except Exception as e:
                print(f"  Batch {idx+1} error: {e}")


# ============================================================
# LLM
# ============================================================

async def enrich_with_llm(ipos: list) -> list:
    if not GH_TOKEN:
        return ipos
    print("\n=== LLM ===")
    enriched = 0
    # upcoming/listed優先、その後undated（全銘柄に企業概要を付与）
    priority = [i for i in ipos if i.get("status") in ("upcoming", "listed") and not i.get("description")]
    priority += [i for i in ipos if i.get("status") not in ("upcoming", "listed") and not i.get("description")]
    async with httpx.AsyncClient(timeout=60) as client:
        for ipo in priority[:30]:  # 最大30件（タイムアウト回避）
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

async def supplement_nasdaq_ciks(sec_companies: list, nasdaq_data: dict) -> list:
    """NASDAQ Upcoming銘柄でSECに見つからないものをCIK補完取得"""
    print("\n=== Supplement NASDAQ CIKs ===")
    existing_tickers = {c.get("ticker") for c in sec_companies if c.get("ticker")}
    existing_ciks = {c.get("cik") for c in sec_companies}

    supplemented = 0
    async with httpx.AsyncClient(timeout=30, headers=SEC_HEADERS) as client:
        for item in nasdaq_data.get("upcoming", []) + nasdaq_data.get("priced", []):
            ticker = item.get("ticker")
            company_name = item.get("company_name", "")
            if not ticker or ticker in existing_tickers:
                continue

            try:
                # 企業名でSEC検索してCIKを取得
                search_term = " ".join(company_name.split()[:3])
                if WORKER_URL:
                    resp = await client.get(f"{WORKER_URL}/api/admin/sec-proxy",
                        params={"q": f'"{search_term}"', "forms": "F-1,S-1",
                                "startdt": "2024-01-01", "enddt": datetime.now().strftime("%Y-%m-%d")})
                else:
                    resp = await client.get("https://efts.sec.gov/LATEST/search-index",
                        params={"q": f'"{search_term}"', "forms": "F-1,S-1",
                                "dateRange": "custom", "startdt": "2024-01-01",
                                "enddt": datetime.now().strftime("%Y-%m-%d")})

                if resp.status_code != 200:
                    continue

                hits = resp.json().get("hits", {}).get("hits", [])
                # tickerまたは企業名で一致するCIKを探す
                for hit in hits:
                    src = hit.get("_source", {})
                    names = src.get("display_names", [])
                    ciks = src.get("ciks", [])
                    name_str = " ".join(names).lower()
                    if (ticker.lower() in name_str or normalize_name(company_name) in normalize_name(name_str)) and ciks:
                        cik = ciks[0]
                        if cik not in existing_ciks:
                            # Submissions APIでタイムライン取得
                            if WORKER_URL:
                                sub_resp = await client.get(f"{WORKER_URL}/api/admin/sec-submissions?cik={cik}")
                            else:
                                cik_padded = cik.lstrip("0").zfill(10)
                                sub_resp = await client.get(f"https://data.sec.gov/submissions/CIK{cik_padded}.json")

                            if sub_resp.status_code == 200:
                                data = sub_resp.json()
                                recent = data.get("filings", {}).get("recent", {})
                                forms = recent.get("form", [])
                                dates = recent.get("filingDate", [])
                                accessions = recent.get("accessionNumber", [])
                                descs = recent.get("primaryDocDescription", [])

                                ipo_forms = {"S-1", "F-1", "S-1/A", "F-1/A", "RW", "424B4", "EFFECT"}
                                event_type_map = {"S-1": "filing", "F-1": "filing", "S-1/A": "amendment", "F-1/A": "amendment", "424B4": "pricing", "RW": "withdrawal", "EFFECT": "effective"}

                                events = []
                                latest_form = None
                                latest_date = None
                                first_filing_date = None

                                for i in range(len(forms)):
                                    if forms[i] in ipo_forms:
                                        ed = dates[i] if i < len(dates) else None
                                        events.append({"event_type": event_type_map.get(forms[i], "filing"), "filing_type": forms[i], "event_date": ed, "sec_accession": accessions[i] if i < len(accessions) else None, "details": descs[i] if i < len(descs) else None})
                                        if ed and (not latest_date or ed > latest_date):
                                            latest_date = ed
                                            latest_form = forms[i]
                                        if forms[i] in ("S-1", "F-1") and not first_filing_date:
                                            first_filing_date = ed

                                if events:
                                    sec_companies.append({
                                        "cik": cik,
                                        "company_name": data.get("name", company_name),
                                        "ticker": ticker,
                                        "filing_type": latest_form,
                                        "filing_date": first_filing_date,
                                        "events": events,
                                    })
                                    existing_ciks.add(cik)
                                    existing_tickers.add(ticker)
                                    supplemented += 1
                                    print(f"  + {ticker} (CIK: {cik}) - {len(events)} events")
                        break
                await asyncio.sleep(0.3)
            except Exception as e:
                pass

    print(f"  Supplemented: {supplemented} companies")
    return sec_companies


async def main():
    print(f"{'='*60}")
    print(f"US IPO Calendar v3 - {datetime.now().isoformat()}")
    print(f"{'='*60}")

    sec = await fetch_sec_filings()
    nasdaq = await fetch_nasdaq_data()

    # NASDAQ Upcoming銘柄のCIK補完
    sec = await supplement_nasdaq_ciks(sec, nasdaq)

    merged = merge_all(sec, nasdaq)
    enriched = await enrich_with_llm(merged)

    with open("ipo_data.json", "w") as f:
        json.dump([{k:v for k,v in i.items() if k not in ("nasdaq_status","_latest_date")} for i in enriched], f, ensure_ascii=False, indent=2, default=str)

    await update_worker(enriched)
    print(f"\nDone: {len(merged)} IPOs")

if __name__ == "__main__":
    asyncio.run(main())
