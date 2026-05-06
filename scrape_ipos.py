"""
US IPO Calendar Scraper
- SEC EDGAR: S-1, F-1, S-1/A, F-1/A, RW, 424B4
- NASDAQ: Upcoming IPOs
- NYSE: IPO filings
"""
import asyncio
import json
import os
import re
import time
from datetime import datetime, timedelta

import httpx
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

WORKER_URL = os.environ.get("WORKER_URL", "")
ADMIN_TOKEN = os.environ.get("CF_API_TOKEN", "")
GH_TOKEN = os.environ.get("GITHUB_TOKEN", "")

SEC_BASE = "https://efts.sec.gov/LATEST/search-index?q=%22initial+public+offering%22&dateRange=custom"
SEC_FULL_TEXT = "https://efts.sec.gov/LATEST/search-index"
SEC_SUBMISSIONS = "https://data.sec.gov/submissions"
SEC_EDGAR_SEARCH = "https://efts.sec.gov/LATEST/search-index"
SEC_RSS = "https://www.sec.gov/cgi-bin/browse-edgar"
SEC_FULL_SEARCH = "https://efts.sec.gov/LATEST/search-index"

# SEC requires User-Agent
SEC_HEADERS = {
    "User-Agent": "usaipocalendarapp miguoipomiguo@gmail.com",
    "Accept": "application/json",
}

GITHUB_MODELS_URL = "https://models.inference.ai.azure.com/chat/completions"

# 監視対象のフォームタイプ
IPO_FORM_TYPES = ["S-1", "F-1", "S-1/A", "F-1/A", "424B4", "RW"]


async def fetch_sec_filings() -> list:
    """SEC EDGAR Full-Text Search APIでIPO関連書類を取得"""
    print("=== SEC EDGAR Filings ===")

    all_filings = []

    async with httpx.AsyncClient(timeout=30, headers=SEC_HEADERS) as client:
        for form_type in ["S-1", "F-1", "S-1/A", "F-1/A", "424B4", "RW"]:
            try:
                # EDGAR full-text search API
                params = {
                    "q": '"initial public offering"',
                    "forms": form_type,
                    "dateRange": "custom",
                    "startdt": (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d"),
                    "enddt": datetime.now().strftime("%Y-%m-%d"),
                }
                resp = await client.get(
                    "https://efts.sec.gov/LATEST/search-index",
                    params=params,
                )

                if resp.status_code != 200:
                    # フォールバック: EDGAR full text search
                    resp = await client.get(
                        f"https://efts.sec.gov/LATEST/search-index?q=%22initial+public+offering%22&forms={form_type}&dateRange=custom&startdt={(datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')}&enddt={datetime.now().strftime('%Y-%m-%d')}",
                    )

                if resp.status_code != 200:
                    print(f"  [{form_type}] API error: {resp.status_code}")
                    continue

                data = resp.json()
                hits = data.get("hits", {}).get("hits", [])
                print(f"  [{form_type}] {len(hits)} filings")

                for hit in hits[:20]:  # 各タイプ最大20件
                    source = hit.get("_source", {})
                    filing = {
                        "company_name": source.get("display_names", [None])[0] if source.get("display_names") else source.get("entity_name"),
                        "cik": str(source.get("entity_id", "")),
                        "filing_type": form_type,
                        "filing_date": source.get("file_date"),
                        "sec_accession": source.get("file_num"),
                        "sec_url": f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={source.get('entity_id')}&type={form_type}&dateb=&owner=include&count=10",
                    }

                    # ステータス判定
                    if form_type == "RW":
                        filing["status"] = "withdrawn"
                        filing["event_type"] = "withdrawal"
                    elif form_type == "424B4":
                        filing["status"] = "priced"
                        filing["event_type"] = "pricing"
                    elif form_type in ("S-1/A", "F-1/A"):
                        filing["status"] = "amended"
                        filing["event_type"] = "amendment"
                    else:
                        filing["status"] = "filed"
                        filing["event_type"] = "filing"

                    all_filings.append(filing)

                await asyncio.sleep(0.5)  # SEC rate limit

            except Exception as e:
                print(f"  [{form_type}] Error: {e}")

    # フォールバック: RSS feed
    if len(all_filings) < 5:
        print("  → RSS fallback...")
        rss_filings = await fetch_sec_rss()
        all_filings.extend(rss_filings)

    print(f"  Total SEC filings: {len(all_filings)}")
    return all_filings


async def fetch_sec_rss() -> list:
    """SEC EDGAR RSS feedからS-1/F-1を取得"""
    filings = []
    async with httpx.AsyncClient(timeout=30, headers=SEC_HEADERS) as client:
        for form_type in ["S-1", "F-1"]:
            try:
                resp = await client.get(
                    f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&type={form_type}&dateb=&owner=include&count=40&search_text=&action=getcompany&output=atom"
                )
                if resp.status_code == 200:
                    soup = BeautifulSoup(resp.text, "xml")
                    entries = soup.find_all("entry")
                    for entry in entries[:20]:
                        title = entry.find("title").text if entry.find("title") else ""
                        link = entry.find("link")["href"] if entry.find("link") else ""
                        updated = entry.find("updated").text[:10] if entry.find("updated") else ""

                        # CIKを抽出
                        cik_match = re.search(r"CIK=(\d+)", link)
                        cik = cik_match.group(1) if cik_match else ""

                        # 会社名を抽出
                        name_match = re.search(r"^(.*?)\s*\(", title)
                        name = name_match.group(1).strip() if name_match else title.split(" - ")[0].strip()

                        if name and cik:
                            filings.append({
                                "company_name": name,
                                "cik": cik,
                                "filing_type": form_type,
                                "filing_date": updated,
                                "status": "filed",
                                "event_type": "filing",
                                "sec_url": link,
                            })

                await asyncio.sleep(0.5)
            except Exception as e:
                print(f"  [RSS {form_type}] Error: {e}")

    print(f"  RSS filings: {len(filings)}")
    return filings


async def fetch_nasdaq_ipos() -> list:
    """NASDAQ Internal JSON APIからIPOデータ取得"""
    print("\n=== NASDAQ IPOs (JSON API) ===")
    ipos = []

    async with httpx.AsyncClient(timeout=30, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        "Accept": "application/json",
    }) as client:
        # 今月と来月のデータを取得
        now = datetime.now()
        months = [now.strftime("%Y-%m"), (now + timedelta(days=31)).strftime("%Y-%m")]

        for month in months:
            try:
                resp = await client.get(f"https://api.nasdaq.com/api/ipo/calendar?date={month}")
                if resp.status_code != 200:
                    print(f"  [{month}] HTTP {resp.status_code}")
                    continue

                data = resp.json().get("data", {})

                for category in ["upcoming", "priced", "filed", "withdrawn"]:
                    cat_data = data.get(category, {})
                    # upcomingは upcomingTable.rows、他は直接 rows
                    if category == "upcoming":
                        rows = cat_data.get("upcomingTable", {}).get("rows", [])
                    else:
                        rows = cat_data.get("rows", [])
                    for row in rows:
                        status_map = {"upcoming": "filed", "priced": "priced", "filed": "filed", "withdrawn": "withdrawn"}

                        # 日付パース
                        expected_date = None
                        date_str = row.get("expectedPriceDate") or row.get("pricedDate") or row.get("filedDate")
                        if date_str:
                            for fmt in ["%m/%d/%Y", "%Y-%m-%d"]:
                                try:
                                    expected_date = datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
                                    break
                                except ValueError:
                                    continue

                        # 価格パース
                        price_low = price_high = offer_price = None
                        price_str = row.get("proposedSharePrice", "")
                        if price_str:
                            price_match = re.search(r"([\d.]+)\s*[-–]\s*([\d.]+)", price_str)
                            if price_match:
                                price_low = float(price_match.group(1))
                                price_high = float(price_match.group(2))
                            else:
                                single_match = re.search(r"([\d.]+)", price_str)
                                if single_match:
                                    offer_price = float(single_match.group(1))

                        # 株数パース
                        shares = None
                        shares_str = row.get("sharesOffered", "")
                        if shares_str:
                            shares_clean = shares_str.replace(",", "")
                            try:
                                shares = int(shares_clean)
                            except ValueError:
                                pass

                        ipos.append({
                            "company_name": row.get("companyName"),
                            "ticker": row.get("proposedTickerSymbol"),
                            "exchange": row.get("proposedExchange", "NASDAQ"),
                            "expected_date": expected_date,
                            "price_range_low": price_low,
                            "price_range_high": price_high,
                            "offer_price": offer_price,
                            "shares_offered": shares,
                            "status": status_map.get(category, "filed"),
                            "event_type": "exchange_filing",
                        })

                print(f"  [{month}] {len(data.get('upcoming',{}).get('rows',[]))} upcoming, {len(data.get('priced',{}).get('rows',[]))} priced, {len(data.get('filed',{}).get('rows',[]))} filed")

            except Exception as e:
                print(f"  [{month}] Error: {e}")

    print(f"  NASDAQ total: {len(ipos)} IPOs")
    return ipos


async def fetch_nyse_ipos() -> list:
    """NYSE IPO Centerからスクレイピング"""
    print("\n=== NYSE IPOs ===")
    ipos = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled", "--no-sandbox"]
        )
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080},
        )
        page = await context.new_page()
        await page.add_init_script("Object.defineProperty(navigator, 'webdriver', { get: () => undefined });")

        try:
            await page.goto("https://www.nyse.com/ipo-center/filings", wait_until="domcontentloaded", timeout=45000)
            await page.wait_for_timeout(5000)

            html = await page.content()
            soup = BeautifulSoup(html, "html.parser")

            tables = soup.find_all("table")
            for table in tables:
                trs = table.find_all("tr")
                for tr in trs[1:]:
                    tds = tr.find_all("td")
                    if len(tds) >= 3:
                        company = tds[0].get_text(strip=True)
                        symbol = tds[1].get_text(strip=True) if len(tds) > 1 else ""
                        exp_date = tds[2].get_text(strip=True) if len(tds) > 2 else ""

                        if company:
                            parsed_date = None
                            if exp_date:
                                for fmt in ["%m/%d/%Y", "%Y-%m-%d", "%b %d, %Y"]:
                                    try:
                                        parsed_date = datetime.strptime(exp_date, fmt).strftime("%Y-%m-%d")
                                        break
                                    except ValueError:
                                        continue

                            ipos.append({
                                "company_name": company,
                                "ticker": symbol if symbol else None,
                                "exchange": "NYSE",
                                "expected_date": parsed_date,
                                "status": "filed",
                                "event_type": "exchange_filing",
                            })

            print(f"  NYSE: {len(ipos)} IPOs")

        except Exception as e:
            print(f"  NYSE Error: {e}")
        finally:
            await browser.close()

    return ipos


def merge_data(sec_filings: list, nasdaq_ipos: list, nyse_ipos: list) -> list:
    """SEC、NASDAQ、NYSEのデータをマージ"""
    print("\n=== Merging Data ===")
    merged = {}

    # SECデータをベースに
    for f in sec_filings:
        key = f.get("cik") or f.get("company_name", "").lower().replace(" ", "")
        if key:
            if key not in merged:
                merged[key] = f
            else:
                # より新しいステータスで更新
                status_priority = {"withdrawn": 5, "priced": 4, "listed": 3, "amended": 2, "filed": 1}
                if status_priority.get(f["status"], 0) > status_priority.get(merged[key]["status"], 0):
                    merged[key].update({k: v for k, v in f.items() if v is not None})

    # NASDAQ/NYSEデータをマッチング・追加
    for ipo in nasdaq_ipos + nyse_ipos:
        matched = False
        company_lower = (ipo.get("company_name") or "").lower()

        for key, existing in merged.items():
            existing_lower = (existing.get("company_name") or "").lower()
            if (company_lower and company_lower in existing_lower) or \
               (existing_lower and existing_lower in company_lower) or \
               (ipo.get("ticker") and ipo["ticker"] == existing.get("ticker")):
                # マッチ: 取引所情報を追加
                existing["ticker"] = existing.get("ticker") or ipo.get("ticker")
                existing["exchange"] = existing.get("exchange") or ipo.get("exchange")
                existing["expected_date"] = existing.get("expected_date") or ipo.get("expected_date")
                existing["price_range_low"] = existing.get("price_range_low") or ipo.get("price_range_low")
                existing["price_range_high"] = existing.get("price_range_high") or ipo.get("price_range_high")
                existing["offer_price"] = existing.get("offer_price") or ipo.get("offer_price")
                existing["shares_offered"] = existing.get("shares_offered") or ipo.get("shares_offered")
                matched = True
                break

        if not matched and ipo.get("company_name"):
            key = ipo.get("ticker") or ipo["company_name"].lower().replace(" ", "")
            merged[key] = ipo

    result = list(merged.values())
    print(f"  Merged: {len(result)} unique IPOs")
    return result


async def enrich_with_llm(ipos: list) -> list:
    """GitHub Models LLMで企業概要を補完"""
    if not GH_TOKEN:
        return ipos

    print("\n=== LLM Enrichment ===")
    enriched = 0

    async with httpx.AsyncClient(timeout=60) as client:
        for ipo in ipos:
            if ipo.get("description"):
                continue
            if not ipo.get("company_name"):
                continue

            prompt = f"""Provide a brief 2-sentence description and main products/services for this company filing for IPO:
Company: {ipo['company_name']}
Filing Type: {ipo.get('filing_type', 'S-1')}

Output JSON only:
{{"description": "Brief company description", "products_services": "Main products or services", "industry": "Industry sector"}}"""

            try:
                resp = await client.post(
                    GITHUB_MODELS_URL,
                    json={
                        "model": "gpt-4o-mini",
                        "messages": [{"role": "user", "content": prompt}],
                        "temperature": 0.1,
                        "max_tokens": 200,
                    },
                    headers={"Authorization": f"Bearer {GH_TOKEN}", "Content-Type": "application/json"}
                )

                if resp.status_code == 200:
                    content = resp.json()["choices"][0]["message"]["content"]
                    if "```json" in content:
                        content = content.split("```json")[1].split("```")[0]
                    elif "```" in content:
                        content = content.split("```")[1].split("```")[0]
                    data = json.loads(content.strip())
                    ipo["description"] = data.get("description")
                    ipo["products_services"] = data.get("products_services")
                    ipo["industry"] = data.get("industry")
                    enriched += 1

                await asyncio.sleep(1)  # Rate limit
            except Exception as e:
                print(f"  LLM error for {ipo['company_name']}: {e}")

            if enriched >= 30:  # 1回あたり最大30件
                break

    print(f"  Enriched: {enriched} companies")
    return ipos


async def update_worker(ipos: list):
    """Cloudflare Workerにデータ送信"""
    if not WORKER_URL or not ADMIN_TOKEN:
        print("\n[SKIP] No WORKER_URL/ADMIN_TOKEN")
        return

    print(f"\n=== Updating Worker ({len(ipos)} IPOs) ===")
    async with httpx.AsyncClient(timeout=30) as client:
        try:
            resp = await client.post(
                f"{WORKER_URL}/api/admin/update",
                json={"ipos": ipos},
                headers={"Authorization": f"Bearer {ADMIN_TOKEN}"}
            )
            print(f"  Worker: {resp.status_code}")
        except Exception as e:
            print(f"  Worker error: {e}")


async def main():
    print(f"{'='*60}")
    print(f"US IPO Calendar Scraper - {datetime.now().isoformat()}")
    print(f"{'='*60}")

    # 1. SEC EDGAR
    sec_filings = await fetch_sec_filings()

    # 2. NASDAQ
    nasdaq_ipos = await fetch_nasdaq_ipos()

    # 3. NYSE
    nyse_ipos = await fetch_nyse_ipos()

    # 4. マージ
    merged = merge_data(sec_filings, nasdaq_ipos, nyse_ipos)

    # 5. LLMで企業情報補完
    enriched = await enrich_with_llm(merged)

    # 6. 保存
    with open("ipo_data.json", "w") as f:
        json.dump(enriched, f, ensure_ascii=False, indent=2, default=str)

    # 7. Worker更新
    await update_worker(enriched)

    print(f"\n{'='*60}")
    print(f"Done: {len(enriched)} IPOs processed")
    print(f"{'='*60}")


if __name__ == "__main__":
    asyncio.run(main())
