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

import anthropic
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
GITHUB_MODELS_ENABLED = False   # GitHub Models は 2026-07-30 に終了。呼ぶと毎回失敗するだけなので止める
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
OPENROUTER_KEY = os.environ.get("OPENROUTER_KEY", "")
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
def _worker_auth() -> dict:
    """Worker の管理用エンドポイント（SEC 中継を含む）に付ける認証ヘッダー。SEC へ直接送るときは付けない"""
    return {"Authorization": f"Bearer {ADMIN_TOKEN}"}


HAIKU_MODEL = "claude-haiku-5-5"   # 第一候補。OpenRouter の無料モデルは混雑と1日50回の上限で安定しないので予備に回す
FILING_FETCH_MAX = 60   # 説明文のために S-1 本文を取りにいく上限（SEC の待ち時間で1件約6秒）
S1_MAX_PER_RUN = 50 if ANTHROPIC_KEY else 15   # 無料モデルだけのときは1日の回数上限があるので、説明文の分を残す
LLM_INTERVAL_SEC = 3.5   # 無料モデルは1分20回まで
OPENROUTER_MODEL = "google/gemma-4-31b-it:free"   # 旧 google/gemini-2.0-flash-001 は提供終了（GitHub Models も 2026-07-30 に終了）
# 無料モデルは提供元が混むと 429 を返すので、OpenRouter 側で順に切り替えてもらう
# openrouter/free は考えるタイプのモデルに振られると本文が空になり、1日50回の無料枠を無駄にしたので入れない
# gemma は2つとも Google の提供で同時に混むので、別会社（NVIDIA）を間に挟む（OpenRouter の上限は3つ）
OPENROUTER_FALLBACKS = [OPENROUTER_MODEL, "nvidia/nemotron-3.5-lightning:free", "google/gemma-4-26b-a4b-it:free"]


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

        for form_type in ["S-1", "F-1", "S-11"]:
            try:
                start_date = (datetime.now() - timedelta(days=120)).strftime("%Y-%m-%d")
                end_date = datetime.now().strftime("%Y-%m-%d")

                if proxy_url:
                    resp = await client.get(proxy_url, headers=_worker_auth(), params={
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
    ipo_forms = {"S-1", "F-1", "S-1/A", "F-1/A", "S-11", "S-11/A", "RW", "424B4", "EFFECT"}
    event_type_map = {
        "S-1": "filing", "F-1": "filing", "S-11": "filing",
        "S-1/A": "amendment", "F-1/A": "amendment", "S-11/A": "amendment",
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
                resp = await client.get(url, headers=_worker_auth() if WORKER_URL else None)

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

                # 最新のS-1/S-1A/F-1/F-1AのaccessionNumberを取得（書類解析用）
                latest_s1_accession = None
                for ev in events:
                    if ev["filing_type"] in ("S-1", "S-1/A", "F-1", "F-1/A", "S-11", "S-11/A"):
                        latest_s1_accession = ev.get("sec_accession")
                        break  # 最新順なので最初の1件

                companies.append({
                    "cik": cik,
                    "company_name": company_name,
                    "ticker": ticker,
                    "filing_type": latest_form,
                    "filing_date": first_filing_date,
                    "events": events,
                    "latest_s1_accession": latest_s1_accession,
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
        # 過去3ヶ月+今月+来月（Listedの価格・日付を確実に取得）
        months = [
            (now - timedelta(days=90)).strftime("%Y-%m"),
            (now - timedelta(days=60)).strftime("%Y-%m"),
            (now - timedelta(days=30)).strftime("%Y-%m"),
            now.strftime("%Y-%m"),
            (now + timedelta(days=31)).strftime("%Y-%m"),
        ]
        months = list(dict.fromkeys(months))  # 重複除去

        for month in months:
            try:
                resp = await client.get(f"https://api.nasdaq.com/api/ipo/calendar?date={month}")
                if resp.status_code != 200:
                    continue

                data = resp.json().get("data", {})

                # Upcoming (upcomingTable.rows) - rows can be None in past months
                upcoming_rows = data.get("upcoming", {}).get("upcomingTable", {}).get("rows") or []
                for row in upcoming_rows:
                    result["upcoming"].append({
                        "ticker": row.get("proposedTickerSymbol"),
                        "company_name": row.get("companyName"),
                        "exchange": row.get("proposedExchange"),
                        "expected_date": parse_date(row.get("expectedPriceDate")),
                        "price_range": row.get("proposedSharePrice"),
                        "shares_offered": parse_shares(row.get("sharesOffered")),
                        "offer_amount": row.get("dollarValueOfSharesOffered"),
                    })

                # Priced (direct rows) - rows can be None
                for row in (data.get("priced", {}).get("rows") or []):
                    result["priced"].append({
                        "ticker": row.get("proposedTickerSymbol"),
                        "company_name": row.get("companyName"),
                        "exchange": row.get("proposedExchange"),
                        "actual_date": parse_date(row.get("pricedDate")),
                        "offer_price": parse_single_price(row.get("proposedSharePrice")),
                        "shares_offered": parse_shares(row.get("sharesOffered")),
                        "offer_amount": row.get("dollarValueOfSharesOffered"),
                    })

                # Filed
                for row in (data.get("filed", {}).get("rows") or []):
                    result["filed"].append({
                        "ticker": row.get("proposedTickerSymbol"),
                        "company_name": row.get("companyName"),
                        "filing_date": parse_date(row.get("filedDate")),
                    })

                # Withdrawn
                for row in (data.get("withdrawn", {}).get("rows") or []):
                    result["withdrawn"].append({
                        "ticker": row.get("proposedTickerSymbol"),
                        "company_name": row.get("companyName"),
                        "exchange": row.get("proposedExchange"),
                        "withdraw_date": parse_date(row.get("withdrawDate")),
                    })

                up_count = len(data.get("upcoming", {}).get("upcomingTable", {}).get("rows") or [])
                pr_count = len(data.get("priced", {}).get("rows") or [])
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
        # Also index withdrawn items by ticker (like upcoming/priced) so an SEC company can
        # match its withdrawal by ticker, not just by name. Without this the withdrawal fails
        # to link and gets added as a separate 'nq_' row (the withdrawn-doesn't-link symptom).
        tk = (item.get("ticker") or "").upper()
        if tk and tk not in nasdaq_lookup:
            nasdaq_lookup[tk] = {**item, "nasdaq_status": "withdrawn"}

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
        name_key = normalize_name(ninfo.get("company_name", ""))
        # 既にtickerで追加済みか確認
        if ticker and any(c.get("ticker") == ticker for c in companies.values()):
            continue
        # ticker無し(NASDAQ withdrawnは全てticker=None)でも、企業名で未登録なら追加する。
        # 以前は `if not ticker: continue` で全て捨てていたため、SEC未捕捉のwithdrawnが欠落し
        # NASDAQカレンダーと一致しなかった。企業名重複が無ければ name ベースidで取り込む。
        if any(normalize_name(c.get("company_name") or "") == name_key for c in companies.values()):
            continue

        companies[f"nq_{ticker or name_key[:24]}"] = build_from_nasdaq(ninfo)
        matched_keys.add(id(ninfo))

    # Step 5: ステータス確定
    for company in companies.values():
        company["status"] = determine_status(company)
        company.pop("nasdaq_status", None)
        company.pop("_latest_date", None)

    # excluded（REIT等）を除外
    result = [c for c in companies.values() if c.get("status") != "excluded"]
    excluded_count = sum(1 for c in companies.values() if c.get("status") == "excluded")
    upcoming_count = sum(1 for c in result if c["status"] == "upcoming")
    listed_count = sum(1 for c in result if c["status"] == "listed")
    print(f"  Final: {len(result)} IPOs (upcoming={upcoming_count}, listed={listed_count}, excluded={excluded_count})")
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
    company["offer_amount"] = nasdaq_info.get("offer_amount") or company.get("offer_amount")
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
        "offer_amount": ninfo.get("offer_amount"),
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

    # SEC filingベースの判定
    ft = company.get("filing_type")
    events = company.get("events", [])

    # REIT（S-11系）でNASDAQに未掲載 = 除外
    is_reit = any(e.get("filing_type") in ("S-11", "S-11/A") for e in events)
    if is_reit and not ns:
        return "excluded"

    # EFFECT + 424B4あり or EFFECT複数回 = 既に上場済み
    effect_count = sum(1 for e in events if e.get("filing_type") == "EFFECT")
    has_424 = any(e.get("filing_type") == "424B4" for e in events)
    if (effect_count >= 2) or (has_424 and effect_count >= 1):
        return "listed"

    # NASDAQのUpcomingにない + EFFECTのみ = 既上場
    if ft == "EFFECT" and effect_count >= 1:
        return "listed"

    if ft == "RW":
        return "withdrawn"
    if ft == "424B4":
        return "listed"  # 424B4 = 価格確定 = 上場完了
    if ft in ("S-1/A", "F-1/A", "S-11/A"):
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
        payload.append({k: v for k, v in ipo.items() if k not in ("nasdaq_status", "_latest_date", "ads_checked")})

    # バッチ分割（15件ずつ、Worker CPU制限対策）
    batch_size = 15
    batches = [payload[i:i+batch_size] for i in range(0, len(payload), batch_size)]
    print(f"\n=== Updating Worker ({len(payload)} IPOs in {len(batches)} batches) ===")

    async with httpx.AsyncClient(timeout=90) as client:
        # upsertのみ（clean削除は行わない — 途中でDBが空になるのを防止）
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


NOTIFY_STATE = "notify_state.json"   # 送った通知の記録（Actions がコミットして次回に引き継ぐ）


def _load_json(path, default):
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return default


def _save_json(path, data):
    with open(path, "w") as f:
        json.dump(data, f, ensure_ascii=False, indent=1, sort_keys=True)


BROADCAST_MAX = 3   # 1回の実行で個別に知らせる新規IPOの数。超えた分は種類ごとに1通にまとめる


def _summarize_broadcasts(broadcasts: list) -> list:
    if len(broadcasts) <= BROADCAST_MAX:
        return broadcasts
    out = []
    for kind, label, label_ja in (("new_upcoming", "new upcoming IPOs", "件の新しい上場予定"),
                                  ("new_pending", "new IPO filings", "件の新しいIPO届出")):
        items = [b for b in broadcasts if b["type"] == kind]
        if not items:
            continue
        if len(items) == 1:
            out.append(items[0])
            continue
        names = ", ".join(b["title"].split(": ", 1)[-1] for b in items[:3])
        more = f" and {len(items) - 3} more" if len(items) > 3 else ""
        out.append({
            "ipo_id": "", "type": kind,
            "title": f"{len(items)} {label}",
            "body": f"{names}{more}. Open the app to see them.",
            "title_ja": f"{len(items)}{label_ja}",
            "body_ja": f"{names}" + (f" ほか{len(items) - 3}件" if len(items) > 3 else "") + "。アプリで確認できます。",
        })
    return out


async def detect_and_notify(ipos: list):
    """前回のスナップショットと比較して変化を検知、プッシュ通知を送信"""
    if not WORKER_URL or not ADMIN_TOKEN:
        return

    print("\n=== Change Detection & Notifications ===")
    prev_snapshot = {}

    # 前回のスナップショットをWorkerから取得
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(f"{WORKER_URL}/api/admin/snapshot",
                                    headers={"Authorization": f"Bearer {ADMIN_TOKEN}"})
            if resp.status_code == 200:
                prev_snapshot = resp.json()
    except Exception:
        pass

    changes = []
    new_snapshot = {}
    sent_state = _load_json(NOTIFY_STATE, {})
    seen_ranges = sent_state.setdefault("ranges", {})

    for ipo in ipos:
        ipo_id = ipo.get("id", "")
        if not ipo_id:
            continue

        status = ipo.get("status", "")
        expected_date = ipo.get("expected_date")
        price_low = ipo.get("price_range_low")
        price_high = ipo.get("price_range_high")
        offer_price = ipo.get("offer_price")
        event_count = len(ipo.get("events", []))
        ticker = ipo.get("ticker") or ipo.get("company_name", "")[:20]

        new_snapshot[ipo_id] = {
            "status": status,
            "expected_date": expected_date,
            "price_low": price_low,
            "price_high": price_high,
            "offer_price": offer_price,
            "event_count": event_count,
        }

        prev = prev_snapshot.get(ipo_id)
        if not prev:
            continue  # 新規銘柄は通知しない

        # 1. ステータス変更 → withdrawn
        if status == "withdrawn" and prev.get("status") != "withdrawn":
            changes.append({
                "ipo_id": ipo_id, "type": "withdrawn",
                "title": f"{ticker} - IPO Withdrawn",
                "body": f"{ipo.get('company_name', '')} has withdrawn their IPO filing.",
                "title_ja": f"{ticker} - IPO撤回",
                "body_ja": f"{ipo.get('company_name', '')} がIPOの届出を撤回しました。",
            })

        # 2. 上場日決定（undated → dated）
        if expected_date and not prev.get("expected_date"):
            changes.append({
                "ipo_id": ipo_id, "type": "date_announced",
                "title": f"{ticker} - Date Announced",
                "body": f"{ipo.get('company_name', '')} IPO scheduled for {expected_date}.",
                "title_ja": f"{ticker} - 上場日決定",
                "body_ja": f"{ipo.get('company_name', '')} の上場予定日が {expected_date}（米国時間）に決まりました。",
            })

        # 3. 上場日変更（延期）
        if expected_date and prev.get("expected_date") and expected_date != prev["expected_date"]:
            changes.append({
                "ipo_id": ipo_id, "type": "postponed",
                "title": f"{ticker} - Date Changed",
                "body": f"IPO date changed from {prev['expected_date']} to {expected_date}.",
                "title_ja": f"{ticker} - 上場日変更",
                "body_ja": f"上場予定日が {prev['expected_date']} から {expected_date}（米国時間）に変わりました。",
            })

        # 4. 価格決定
        if offer_price and not prev.get("offer_price"):
            changes.append({
                "ipo_id": ipo_id, "type": "priced",
                "title": f"{ticker} - Priced at ${offer_price}",
                "body": f"{ipo.get('company_name', '')} IPO priced at ${offer_price} per share.",
                "title_ja": f"{ticker} - 公開価格 ${offer_price}",
                "body_ja": f"{ipo.get('company_name', '')} の公開価格が1株 ${offer_price} に決まりました。",
            })

        # 4b. 仮条件（価格レンジ）の変更。価格決定前のみ
        # データ元によって値が行き来しても何度も届かないよう、一度知らせた価格帯は覚えておく
        def _rk(a, b):
            return f"{float(a):g}-{float(b):g}"
        rng_key = _rk(price_low, price_high) if price_low and price_high else ""
        if not offer_price and price_low and price_high and prev.get("price_low") and prev.get("price_high") \
                and (float(price_low), float(price_high)) != (float(prev["price_low"]), float(prev["price_high"])) \
                and rng_key not in seen_ranges.setdefault(ipo_id, [_rk(prev["price_low"], prev["price_high"])]):
            seen_ranges[ipo_id].append(rng_key)
            rng = f"${float(price_low):g}-${float(price_high):g}"
            old_rng = f"${float(prev['price_low']):g}-${float(prev['price_high']):g}"
            changes.append({
                "ipo_id": ipo_id, "type": "priced",
                "title": f"{ticker} - Price Range {rng}",
                "body": f"{ipo.get('company_name', '')} changed its price range from {old_rng} to {rng}.",
                "title_ja": f"{ticker} - 仮条件 {rng}",
                "body_ja": f"{ipo.get('company_name', '')} の仮条件が {old_rng} から {rng} に変わりました。",
            })

        # 5. タイムライン更新（新しいイベントが追加された）
        if event_count > prev.get("event_count", 0):
            changes.append({
                "ipo_id": ipo_id, "type": "timeline_update",
                "title": f"{ticker} - New Filing",
                "body": f"{ipo.get('company_name', '')} has a new SEC filing update.",
                "title_ja": f"{ticker} - 新しい届出",
                "body_ja": f"{ipo.get('company_name', '')} がSECに新しい書類を提出しました。",
            })

    # 6. 上場前日通知（明日上場する銘柄）。実行は1日2回以上あるので、送った分は覚えておいて1回だけにする
    # 上場日は米国東部の日付。実行環境は UTC なので、東部時間の「明日」で比べる
    # （UTC のままだと東部の前々日の夜に「明日上場」が届いてしまう）
    from zoneinfo import ZoneInfo
    now_et = datetime.now(ZoneInfo("America/New_York"))
    tomorrow = (now_et + timedelta(days=1)).strftime("%Y-%m-%d")
    day_before = sent_state.get("day_before", {})
    for ipo in ipos:
        if ipo.get("expected_date") == tomorrow:
            ipo_id = ipo.get("id", "")
            if day_before.get(ipo_id) == tomorrow:
                continue
            day_before[ipo_id] = tomorrow
            ticker = ipo.get("ticker") or ipo.get("company_name", "")[:20]
            changes.append({
                "ipo_id": ipo_id, "type": "day_before",
                "title": f"{ticker} - IPO Tomorrow",
                "body": f"{ipo.get('company_name', '')} is scheduled to go public tomorrow ({tomorrow}).",
                "title_ja": f"{ticker} - 明日上場",
                "body_ja": f"{ipo.get('company_name', '')} は明日 {tomorrow}（米国時間）に上場予定です。",
            })

    today_s = now_et.strftime("%Y-%m-%d")
    sent_state["day_before"] = {k: v for k, v in day_before.items() if v >= today_s}
    live = {i.get("id") for i in ipos if i.get("status") not in ("listed", "withdrawn")}
    sent_state["ranges"] = {k: v for k, v in seen_ranges.items() if k in live}
    _save_json(NOTIFY_STATE, sent_state)

    # 7. 新規IPO追加通知（全ユーザー向け、無料）
    broadcasts = []
    for ipo in ipos:
        ipo_id = ipo.get("id", "")
        if not ipo_id or ipo_id in prev_snapshot:
            continue
        status = ipo.get("status", "")
        if status in ("listed", "excluded", "withdrawn"):
            continue
        ticker = ipo.get("ticker") or ""
        name = ipo.get("company_name", "")
        if ipo.get("expected_date"):
            broadcasts.append({
                "ipo_id": ipo_id, "type": "new_upcoming",
                "title": f"New IPO: {ticker or name[:20]}",
                "body": f"{name} has been added to upcoming IPOs ({ipo.get('expected_date')}).",
                "title_ja": f"新しい上場予定: {ticker or name[:20]}",
                "body_ja": f"{name} が上場予定に加わりました（{ipo.get('expected_date')}）。",
            })
        else:
            broadcasts.append({
                "ipo_id": ipo_id, "type": "new_pending",
                "title": f"New Filing: {ticker or name[:20]}",
                "body": f"{name} has filed for an IPO (date TBD).",
                "title_ja": f"新しいIPO届出: {ticker or name[:20]}",
                "body_ja": f"{name} がIPOを申請しました（上場日は未定）。",
            })

    # 新規IPO通知の安全装置。前回の状態が欠けていると既存の銘柄まで「新規」になり、
    # 全端末に何十通も届いてしまう（2026-11-11 に実際に起きた）
    if not prev_snapshot:
        print("  ! no previous snapshot — skipping new-IPO broadcasts this run")
        broadcasts = []
    elif len(broadcasts) > max(10, len(new_snapshot) // 10):
        print(f"  ! {len(broadcasts)} 'new' IPOs looks like a broken snapshot — skipping broadcasts")
        broadcasts = []
    else:
        broadcasts = _summarize_broadcasts(broadcasts)

    # スナップショットをWorkerに保存
    try:
        async with httpx.AsyncClient(timeout=120) as client:
            resp = await client.post(f"{WORKER_URL}/api/admin/snapshot",
                                     json=new_snapshot,
                                     headers={"Authorization": f"Bearer {ADMIN_TOKEN}"})
            print(f"  Snapshot saved: {resp.status_code} {resp.text[:80]}")
    except Exception as e:
        print(f"  ! snapshot save failed: {type(e).__name__}")

    if not changes and not broadcasts:
        print("  No changes detected")
        return

    print(f"  {len(changes)} changes, {len(broadcasts)} new IPO broadcasts")
    for c in changes:
        print(f"    {c['type']}: {c['title']}")
    for b in broadcasts[:5]:
        print(f"    [broadcast] {b['title']}")

    # Worker経由でAPNs通知送信。全端末向けは1通で数百台に送るので、1回の呼び出しに1通ずつ分ける
    calls = []
    if changes:
        calls.append({"changes": changes, "broadcasts": []})
    calls += [{"changes": [], "broadcasts": [b]} for b in broadcasts]
    try:
        async with httpx.AsyncClient(timeout=120) as client:
            for body in calls:
                resp = await client.post(
                    f"{WORKER_URL}/api/admin/notify",
                    json=body,
                    headers={"Authorization": f"Bearer {ADMIN_TOKEN}"}
                )
                print(f"  Notify response: {resp.status_code} {resp.text[:200]}")
    except Exception as e:
        print(f"  Notify error: {e}")


# ============================================================
# LLM
# ============================================================

_filing_texts = {}   # id → S-1 本文テキスト（この実行の間だけ持つ）


async def _fetch_filing_text(client, ipo: dict, accession_raw: str):
    """SEC の S-1/F-1 本文をテキストにして返す。取れなければ None（財務抽出と説明文の両方で使う）"""
    cik_raw = ipo["cik"].lstrip("0")
    accession_nodash = accession_raw.replace("-", "")

    # Submissions APIからprimaryDocumentファイル名を取得
    if WORKER_URL:
        sub_resp = await client.get(f"{WORKER_URL}/api/admin/sec-submissions", params={"cik": ipo["cik"]},
                                    headers=_worker_auth())
    else:
        cik_padded = cik_raw.zfill(10)
        sub_resp = await client.get(f"https://data.sec.gov/submissions/CIK{cik_padded}.json",
                                   headers={"User-Agent": "usaipocalendarapp miguoipomiguo@gmail.com"})
    if sub_resp.status_code != 200:
        print(f"  ! {ipo.get('ticker','?')}: submissions API {sub_resp.status_code}")
        return None

    sub_data = sub_resp.json()
    recent = sub_data.get("filings", {}).get("recent", {})
    accession_list = recent.get("accessionNumber", [])
    primary_docs = recent.get("primaryDocument", [])

    # accession番号でprimaryDocumentを探す
    primary_doc = None
    for idx, acc in enumerate(accession_list):
        if acc == accession_raw:
            primary_doc = primary_docs[idx] if idx < len(primary_docs) else None
            break
    if not primary_doc:
        print(f"  ! {ipo.get('ticker','?')}: primaryDocument not found for {accession_raw}")
        return None

    await asyncio.sleep(5)  # SEC rate limit対策

    # ドキュメントURL構築 & 取得（403/429ならリトライ）
    doc_url = f"https://www.sec.gov/Archives/edgar/data/{cik_raw}/{accession_nodash}/{primary_doc}"
    doc_resp = None
    for attempt in range(3):
        if WORKER_URL:
            doc_resp = await client.get(f"{WORKER_URL}/api/admin/sec-fetch", params={"url": doc_url},
                                        headers=_worker_auth())
        else:
            doc_resp = await client.get(doc_url, headers={"User-Agent": "usaipocalendarapp miguoipomiguo@gmail.com"})
        if doc_resp.status_code == 200:
            break
        if doc_resp.status_code in (403, 429) and attempt < 2:
            wait = 60 * (attempt + 1)
            print(f"  ! {ipo.get('ticker','?')}: {doc_resp.status_code}, retrying in {wait}s...")
            await asyncio.sleep(wait)
    if doc_resp.status_code != 200:
        print(f"  ! {ipo.get('ticker','?')}: doc page {doc_resp.status_code} ({doc_url[:80]})")
        return None

    import re
    from bs4 import BeautifulSoup
    import html2text
    # SGMLラッパーを除去してHTML部分のみ取得
    raw = doc_resp.text
    html_start = raw.find("<HTML")
    if html_start == -1:
        html_start = raw.find("<html")
    if html_start >= 0:
        raw = raw[html_start:]
    soup = BeautifulSoup(raw[:1500000], "html.parser")   # 50万だと大きい書類で財務サマリーまで届かない（S-1 分析で必要）
    for tag in soup.find_all(["script", "style"]):
        tag.decompose()
    converter = html2text.HTML2Text()
    converter.ignore_links = True
    converter.body_width = 0
    full_text = converter.handle(str(soup))
    return full_text


_UNKNOWN_DESC = re.compile(
    r"don't have|do not have|I can't|I cannot|unable to|not aware of|no (verified|reliable|specific|public) information"
    r"|knowledge base|please provide|not familiar|could not find|couldn't find", re.I)


def _is_unknown_answer(d: dict) -> bool:
    """「この会社は知らない」という返事を説明文として保存しないための判定"""
    desc = d.get("description") or ""
    if _UNKNOWN_DESC.search(desc):
        return True
    return str(d.get("products_services", "")).strip().lower() in ("unknown", "n/a", "none")


def _s1_accession(ipo: dict):
    accession = ipo.get("latest_s1_accession")
    if not accession:
        for ev in ipo.get("events", []):
            if ev.get("filing_type") in ("S-1", "S-1/A", "F-1", "F-1/A", "S-11", "S-11/A") and ev.get("sec_accession"):
                return ev["sec_accession"]
    return accession


def _business_excerpt(text: str) -> str:
    """S-1 本文から事業の説明（Prospectus Summary / Overview）あたりを抜き出す。目次の見出しは飛ばす"""
    for pat in (r"#*\s*\**\s*(Our Company|Overview|Our Business|Who We Are|Business Overview)\s*\**\s*\n",
                r"PROSPECTUS SUMMARY"):
        for m in re.finditer(pat, text, re.I):
            chunk = text[m.start():m.start() + 4000]
            if len(re.sub(r"\s+", " ", chunk)) > 1500 and chunk.count("|") < 40:   # 目次（表）ではなく本文
                return chunk
    return text[3000:7000]


# ADS（米国預託証券）で上場する海外企業は、発行済株式数が普通株の数、価格が1 ADS あたり。
# 1 ADS = 何株かを書類から読み取り、アプリは「株式数 ÷ 比率 × 価格」で時価総額を出す
# （比率を見ないと OPAY が 10 倍の $40B になっていた）
_NUM = {"one": 1, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6, "seven": 7, "eight": 8, "nine": 9,
        "ten": 10, "eleven": 11, "twelve": 12, "fifteen": 15, "twenty": 20, "twenty-five": 25, "thirty": 30,
        "forty": 40, "fifty": 50, "one hundred": 100, "hundred": 100}
_FRAC = {"half": 2, "one-half": 2, "third": 3, "one-third": 3, "quarter": 4, "one-quarter": 4, "fourth": 4,
         "one-fourth": 4, "fifth": 5, "one-fifth": 5, "tenth": 10, "one-tenth": 10}
_ADS_RE = re.compile(
    r"each\s+(?:ADS|American\s+depositary\s+share)\s+(?:will\s+)?represents?\s+"
    r"(?P<q>[\w\-\.]+(?:\s+hundred)?)(?:\s*\((?P<d>[\d\.,/]+)\))?\s+(?:of\s+(?:one|an?)\s+)?(?:of\s+our\s+)?"
    r"(?:Class\s+[A-Z]\s+)?(?:ordinary|common)\s+shares?", re.I)

def _ads_ratio(text: str):
    """Ordinary shares per ADS (e.g. 10.0, or 0.5 when one ADS is half a share). None when not an ADS offering."""
    for m in _ADS_RE.finditer(text[:400000]):
        d, q = m.group("d"), m.group("q").lower().strip(".")
        if d:
            try:
                return float(d.replace(",", "")) if "/" not in d else _eval_frac(d)
            except ValueError:
                pass
        if q in _NUM: return float(_NUM[q])
        if q in _FRAC: return 1.0 / _FRAC[q]
        if re.fullmatch(r"\d+(\.\d+)?", q): return float(q)
    return None

def _eval_frac(s):
    a, b = s.split("/"); return float(a) / float(b)


ADS_BACKFILL_MAX = 40


async def backfill_ads_ratio(ipos: list, cache: dict):
    """発行済株式数を持つ海外（F-1）銘柄のうち、まだ ADS 比率を調べていないものを調べる（正規表現だけ）"""
    todo = [i for i in ipos if i.get("shares_outstanding") and i.get("cik") and _s1_accession(i)
            and (i.get("filing_type") or "").startswith("F-1")
            and not (cache.get(i.get("id")) or {}).get("ads_checked")]
    if not todo:
        return
    print(f"\n=== ADS ratio backfill ({len(todo)} left, up to {ADS_BACKFILL_MAX}) ===")
    found = 0
    async with httpx.AsyncClient(timeout=60, headers=SEC_HEADERS) as client:
        for ipo in todo[:ADS_BACKFILL_MAX]:
            text = _filing_texts.get(ipo.get("id"))
            if not text:
                try:
                    text = await _fetch_filing_text(client, ipo, _s1_accession(ipo))
                except Exception as e:
                    print(f"  ! {ipo.get('ticker') or ipo.get('id')}: {type(e).__name__}")
                    continue
            if not text:
                continue
            r = _ads_ratio(text)
            entry = cache.setdefault(ipo["id"], {})
            entry["ads_checked"] = True
            if r and r != 1:
                ipo["ads_ratio"] = entry["ads_ratio"] = r
                found += 1
                print(f"  {ipo.get('ticker') or ipo.get('id')}: 1 ADS = {r:g} shares")
            await asyncio.sleep(0.5)
    print(f"  ADS ratios found: {found}")


async def extract_s1_financials(ipos: list) -> list:
    """S-1/F-1書類からProposed Offering PriceとShares Outstandingを抽出"""
    if not ANTHROPIC_KEY and not OPENROUTER_KEY:
        return ipos
    print("\n=== S-1 Financial Extraction ===")
    extracted = 0

    # shares_outstandingが未取得の全銘柄が対象（Upcoming含む）
    # latest_s1_accessionがなくてもeventsからaccessionを取得
    targets_upcoming = []
    targets_other = []
    for i in ipos:
        if i.get("shares_outstanding") or not i.get("cik"):
            continue
        # eventsから最新のS-1系accessionを取得
        accession = i.get("latest_s1_accession")
        if not accession:
            for ev in i.get("events", []):
                if ev.get("filing_type") in ("S-1", "S-1/A", "F-1", "F-1/A", "S-11", "S-11/A") and ev.get("sec_accession"):
                    accession = ev["sec_accession"]
                    break
        if accession:
            i["_accession_for_extract"] = accession
            # expected_dateありを優先（Upcoming）
            if i.get("expected_date"):
                targets_upcoming.append(i)
            else:
                targets_other.append(i)

    targets = targets_upcoming + targets_other
    print(f"  Targets: {len(targets)} companies ({len(targets_upcoming)} upcoming)")

    async with httpx.AsyncClient(timeout=60, headers=SEC_HEADERS) as client:
        for ipo in targets[:S1_MAX_PER_RUN]:
            try:
                accession_raw = ipo["_accession_for_extract"]
                full_text = await _fetch_filing_text(client, ipo, accession_raw)
                if not full_text:
                    continue
                _filing_texts[ipo.get("id")] = full_text   # 説明文づくりでも使う
                r = _ads_ratio(full_text)
                if r and r != 1:
                    ipo["ads_ratio"] = r
                ipo["ads_checked"] = True

                # テキスト先頭 + キーワード周辺を抽出してLLMに渡す
                excerpts = [full_text[:3000]]
                keywords = [r'\$[\d,.]+\s*per\s*(share|unit)', r'per\s*share\s*price\s*of\s*\$', r'shares?\s*(outstanding|to be outstanding)', r'(offering|IPO)\s*price', r'shares?\s*after\s*(this|the)\s*offering', r'shares?\s*of\s*(Class\s*[A-Z]\s*)?(common|ordinary)\s*stock']
                for kw in keywords:
                    for m in re.finditer(kw, full_text, re.IGNORECASE):
                        s = max(0, m.start() - 200)
                        e = min(len(full_text), m.end() + 200)
                        excerpts.append(full_text[s:e])
                combined = "\n---\n".join(excerpts)[:6000]

                # LLMで抽出
                if len(combined.strip()) < 100:
                    print(f"  ! {ipo.get('ticker','?')}: text too short ({len(combined)} chars)")
                    continue

                prompt = f"""From these excerpts of an SEC S-1/F-1 filing, extract the ACTUAL values:
1. Proposed offering price per share or price range (low and high). For SPACs, this is typically "$10.00 per unit".
2. Total shares that will be outstanding after the offering (post-IPO). Look for "shares outstanding after this offering", "shares to be outstanding", or in the Capitalization table.

IMPORTANT: Extract the REAL numbers from the document. Do NOT guess or use placeholder values.
If price is a single number (e.g. "$10.00 per unit" or "$20 per share"), use that for both low and high.
Return JSON only:
{{"proposed_price_low": <number or null>, "proposed_price_high": <number or null>, "shares_outstanding": <integer or null>}}

Filing excerpts:
{combined}"""

                result = await _call_haiku(prompt)
                if result is None and GITHUB_MODELS_ENABLED:
                    result = await _call_llm(client, GITHUB_MODELS_URL, "gpt-4o-mini", prompt,
                        {"Authorization": f"Bearer {GH_TOKEN}", "Content-Type": "application/json"})
                if result is None and OPENROUTER_KEY and not ANTHROPIC_KEY:
                    result = await _call_llm(client, OPENROUTER_URL, OPENROUTER_MODEL, prompt,
                        {"Authorization": f"Bearer {OPENROUTER_KEY}", "Content-Type": "application/json"})

                if result:
                    if result.get("proposed_price_low"):
                        ipo["proposed_price_low"] = result["proposed_price_low"]
                    if result.get("proposed_price_high"):
                        ipo["proposed_price_high"] = result["proposed_price_high"]
                    if result.get("shares_outstanding"):
                        ipo["shares_outstanding"] = result["shares_outstanding"]
                    extracted += 1
                    print(f"  + {ipo.get('ticker','?')}: ${result.get('proposed_price_low')}-${result.get('proposed_price_high')}, {result.get('shares_outstanding')} shares")
                else:
                    print(f"  - {ipo.get('ticker','?')}: no values")
                    if _last_llm_status.get("daily") and not ANTHROPIC_KEY:
                        print("  Free daily quota used up, stopping")
                        break

                await asyncio.sleep(1)
            except Exception as e:
                print(f"  ! {ipo.get('ticker','?')}: {e}")

    print(f"  Extracted: {extracted}")
    return ipos


async def enrich_with_llm(ipos: list) -> list:
    if not ANTHROPIC_KEY and not OPENROUTER_KEY:
        return ipos
    print("\n=== LLM ===")
    enriched = 0
    # 構築中: description未補完の全件を処理
    priority = [i for i in ipos if not i.get("description")]
    today = datetime.now().strftime("%Y-%m-%d")
    # 上場予定の近い銘柄を先に（無料枠が尽きても upcoming には付くように）
    priority.sort(key=lambda i: (not (i.get("expected_date") or "") >= today, i.get("expected_date") or "9999"))
    print(f"  Priority: {len(priority)} companies without description")
    github_failures = 0
    rate_limited = 0
    openrouter_off = False

    filing_fetches = 0
    from_filing = 0
    async with httpx.AsyncClient(timeout=60) as client, httpx.AsyncClient(timeout=60, headers=SEC_HEADERS) as sec_client:
        for ipo in priority:
            name = ipo["company_name"]
            text = _filing_texts.get(ipo.get("id"))
            accession = _s1_accession(ipo)
            if not text and ANTHROPIC_KEY and ipo.get("cik") and accession and filing_fetches < FILING_FETCH_MAX:
                filing_fetches += 1
                try:
                    text = await _fetch_filing_text(sec_client, ipo, accession)
                except Exception as e:
                    print(f"  ! {ipo.get('ticker','?')}: filing fetch {type(e).__name__}")
            if text:
                from_filing += 1
                prompt = f"""Below is an excerpt from the SEC registration statement (S-1/F-1) of {name}, which is planning an IPO.
Using ONLY this excerpt, write a 2-sentence description of the company, what it sells, and its industry.
If the excerpt does not say what the company does, use null. Respond with JSON only, no markdown:
{{"description":"...","products_services":"...","industry":"..."}}

Excerpt:
{_business_excerpt(text)}"""
            else:
                prompt = f'Brief 2-sentence description for IPO company: {name}. If you do not know this specific company, use null for every field instead of guessing or explaining. Respond with JSON only, no markdown: {{"description":"...","products_services":"...","industry":"..."}}'

            # Try 1: Claude Haiku
            result = await _call_haiku(prompt)
            # GitHub Models（終了済み・止めてある）
            if result is None and GITHUB_MODELS_ENABLED and GH_TOKEN and github_failures < 5:
                result = await _call_llm(client, GITHUB_MODELS_URL, "gpt-4o-mini",
                    prompt, {"Authorization": f"Bearer {GH_TOKEN}", "Content-Type": "application/json"})
                if result is None:
                    github_failures += 1

            # Try 2: OpenRouter fallback（Haiku が失敗したときだけ。無料枠が尽きたらこの実行ではもう呼ばない）
            used_openrouter = False
            if result is None and OPENROUTER_KEY and not openrouter_off:
                used_openrouter = True
                result = await _call_llm(client, OPENROUTER_URL, OPENROUTER_MODEL,
                    prompt, {"Authorization": f"Bearer {OPENROUTER_KEY}", "Content-Type": "application/json"})
                if not result:
                    if _last_llm_status.get("daily"):
                        print("  Free daily quota used up, OpenRouter off for this run")
                        openrouter_off = True
                    elif _last_llm_status.get("code") == 429:
                        rate_limited += 1
                        if rate_limited >= 10:
                            print("  Rate limited 10 times in a row, OpenRouter off for this run")
                            openrouter_off = True
                        else:
                            await asyncio.sleep(20)   # 提供元の混雑（temporarily rate-limited upstream）は少し待つと通ることがある
                else:
                    rate_limited = 0

            if result and _is_unknown_answer(result):
                print(f"  - {ipo.get('ticker') or name[:30]}: model does not know this company, skipped")
                result = None
            if result:
                ipo.update(result)
                enriched += 1
            elif openrouter_off and not ANTHROPIC_KEY:
                break
            await asyncio.sleep(LLM_INTERVAL_SEC if used_openrouter else 0.5)

    print(f"  Enriched: {enriched} (from filings: {from_filing}, filing fetches: {filing_fetches})")
    return ipos


def _parse_llm_json(c: str) -> dict:
    """LLM の返答から JSON を取り出し、空の値を落として返す（読めなければ例外）"""
    if "```" in c:
        c = c.split("```json")[-1].split("```")[0] if "```json" in c else c.split("```")[1].split("```")[0]
    c = c.strip()
    brace_start = c.find("{")
    brace_end = c.rfind("}") + 1
    if brace_start >= 0 and brace_end > brace_start:
        c = c[brace_start:brace_end]
    d = json.loads(c)
    return {k: v for k, v in d.items() if v}


_haiku_client = anthropic.AsyncAnthropic(api_key=ANTHROPIC_KEY, max_retries=3) if ANTHROPIC_KEY else None

async def _call_haiku(prompt: str):
    """Claude Haiku を呼び出して JSON を返す。キーが無い・失敗時は None（値が全部空なら {}）"""
    if not _haiku_client:
        return None
    try:
        # temperature は Claude では送れない（2026-10-21 以降 400）。短い抽出なので考える過程は使わない
        resp = await _haiku_client.messages.create(
            model=HAIKU_MODEL, max_tokens=1024,
            thinking={"type": "disabled"},
            messages=[{"role": "user", "content": prompt}])
        if resp.stop_reason == "refusal":
            print(f"  Haiku refusal")
            return None
        text = "".join(b.text for b in resp.content if b.type == "text")
        if not text:
            print(f"  Haiku empty content (stop={resp.stop_reason})")
            return None
        return _parse_llm_json(text)
    except anthropic.RateLimitError as e:
        print(f"  Haiku 429: {str(e)[:200]}")
    except anthropic.APIStatusError as e:
        print(f"  Haiku {e.status_code}: {str(e)[:200]}")
    except anthropic.APIConnectionError as e:
        print(f"  Haiku connection error: {str(e)[:120]}")
    except Exception as e:
        print(f"  Haiku error: {type(e).__name__}: {str(e)[:120]}")
    return None


_last_llm_status = {}
_empty_counts = {}
_dropped_models = set()

async def _call_llm(client, url: str, model: str, prompt: str, headers: dict) -> dict:
    """LLM APIを呼び出してJSONを返す。失敗時はNone"""
    _last_llm_status["code"] = None
    _last_llm_status["daily"] = False
    _last_llm_status["answered_by"] = None
    try:
        body = {"model": model, "messages": [{"role": "user", "content": prompt}], "temperature": 0.1, "max_tokens": 1000}
        if url == OPENROUTER_URL:
            body["models"] = [m for m in OPENROUTER_FALLBACKS if m not in _dropped_models]
            body["reasoning"] = {"enabled": False}   # NVIDIA は考える過程だけで上限を使い切り、本文が空になっていた
        resp = await client.post(url, json=body, headers=headers)
        _last_llm_status["code"] = resp.status_code
        _last_llm_status["daily"] = resp.status_code == 429 and "free-models-per-day" in resp.text
        if resp.status_code == 200:
            j = resp.json()
            c = j["choices"][0]["message"]["content"]
            if not c:
                print(f"  LLM empty content ({j.get('model')}, finish={j['choices'][0].get('finish_reason')})")
                # 空で返すモデルは無料枠を無駄にするので、3回続いたらこの実行では使わない
                m = j.get("model") or ""
                _empty_counts[m] = _empty_counts.get(m, 0) + 1
                if _empty_counts[m] >= 3 and m in OPENROUTER_FALLBACKS and m != OPENROUTER_MODEL:
                    _dropped_models.add(m)
                    print(f"  Dropping {m} for this run")
                return None
            _last_llm_status["answered_by"] = j.get("model")
            return _parse_llm_json(c)
        else:
            print(f"  LLM {resp.status_code} ({model}): {resp.text[:300]}")   # 失敗の理由をログに残す
    except Exception as e:
        print(f"  LLM error ({_last_llm_status.get('answered_by') or model}): {type(e).__name__}: {str(e)[:120]}")
    return None


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
                    resp = await client.get(f"{WORKER_URL}/api/admin/sec-proxy", headers=_worker_auth(),
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
                                sub_resp = await client.get(f"{WORKER_URL}/api/admin/sec-submissions?cik={cik}",
                                                            headers=_worker_auth())
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

                                ipo_forms = {"S-1", "F-1", "S-1/A", "F-1/A", "S-11", "S-11/A", "RW", "424B4", "EFFECT"}
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


ENRICHMENT_CACHE = Path(__file__).parent / "enrichment_cache.json"


JA_MAX_PER_RUN = 400   # 1回に訳す社数（20社ずつまとめて Haiku に送る。約530社で数セント）
JA_CHUNK = 20


def _ja_src(ipo: dict) -> str:
    """訳した元の英文の指紋。英文が書き換わったら訳し直す"""
    import hashlib
    src = (ipo.get("description") or "") + "\n" + (ipo.get("products_services") or "")
    return hashlib.sha1(src.encode()).hexdigest()[:12]


async def translate_ja(ipos: list):
    """description / products_services を日本語に訳して description_ja / products_services_ja に入れる"""
    print("\n=== Japanese Translation ===")
    if not _haiku_client:
        print("  [SKIP] no ANTHROPIC_API_KEY")
        return
    todo, seen = [], {}
    for ipo in ipos:
        if not ipo.get("description"):
            continue
        src = _ja_src(ipo)
        if ipo.get("description_ja") and ipo.get("ja_src") == src:
            continue
        seen.setdefault(src, []).append(ipo)
    todo = list(seen.items())   # 同じ英文の銘柄（ティッカー違いなど）は1回だけ訳す
    print(f"  {len(todo)} texts need translation (up to {JA_MAX_PER_RUN} this run)")
    todo = todo[:JA_MAX_PER_RUN]
    stats = {"done": 0, "in": 0, "out": 0}
    sem = asyncio.Semaphore(5)   # 20社分の訳は1回30秒ほどかかるので、5つ並べて送る

    async def one(start: int):
        chunk = todo[start:start + JA_CHUNK]
        items = {str(n): {"description": group[0].get("description"),
                          "products_services": group[0].get("products_services")}
                 for n, (_, group) in enumerate(chunk)}
        prompt = (
            "Translate each company's description and products_services into natural Japanese "
            "for Japanese retail investors reading an IPO app. Keep company names, product names, "
            "drug codes and tickers in their original spelling. Keep numbers exact. "
            "Write concise Japanese in plain form (常体; 体言止め is fine). "
            "If a field is null, return null for it. Respond with JSON only, no markdown, "
            'in the form {"0": {"description": "...", "products_services": "..."}, ...}.\n\n'
            + json.dumps(items, ensure_ascii=False))
        async with sem:
            try:
                resp = await _haiku_client.messages.create(
                    model=HAIKU_MODEL, max_tokens=16000,
                    thinking={"type": "disabled"},
                    messages=[{"role": "user", "content": prompt}])
            except Exception as e:
                print(f"  ! chunk {start // JA_CHUNK}: {type(e).__name__}: {str(e)[:150]}")
                return
        stats["in"] += resp.usage.input_tokens
        stats["out"] += resp.usage.output_tokens
        text = "".join(b.text for b in resp.content if b.type == "text")
        try:
            out = _parse_llm_json(text) or {}
        except Exception:
            out = {}
        if resp.stop_reason != "end_turn" or not isinstance(out, dict) or not out:
            print(f"  ! chunk {start // JA_CHUNK}: stop={resp.stop_reason}, unparsed")
            return
        for n, (src, group) in enumerate(chunk):
            t = out.get(str(n))
            if not isinstance(t, dict) or not isinstance(t.get("description"), str) or not t["description"].strip():
                continue
            p = t.get("products_services") if isinstance(t.get("products_services"), str) else None
            for ipo in group:
                ipo["description_ja"] = t["description"].strip()
                if p and p.strip():
                    ipo["products_services_ja"] = p.strip()
                else:
                    ipo.pop("products_services_ja", None)
                ipo["ja_src"] = src
            stats["done"] += 1

    await asyncio.gather(*(one(st) for st in range(0, len(todo), JA_CHUNK)))
    done, tokens_in, tokens_out = stats["done"], stats["in"], stats["out"]
    cost = tokens_in / 1e6 * 0.10 + tokens_out / 1e6 * 0.50
    print(f"  translated {done} texts, tokens in={tokens_in} out={tokens_out}, ~${cost:.4f}")


def load_enrichment_cache() -> dict:
    """過去のLLM補完結果を読み込む（id → {description, products_services, industry}）"""
    if ENRICHMENT_CACHE.exists():
        try:
            with open(ENRICHMENT_CACHE, "r") as f:
                return json.load(f)
        except:
            pass
    return {}


def save_enrichment_cache(cache: dict):
    with open(ENRICHMENT_CACHE, "w") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


def apply_enrichment_cache(ipos: list, cache: dict):
    """キャッシュからdescription, shares_outstanding等を復元"""
    applied = 0
    junk = 0
    for entry in cache.values():
        if entry.get("description") and _is_unknown_answer(entry):
            for key in ("description", "products_services", "industry", "description_ja", "products_services_ja", "ja_src"):
                entry.pop(key, None)
            junk += 1
    for ipo in ipos:
        if ipo.get("description") and _is_unknown_answer(ipo):
            for key in ("description", "products_services", "industry"):
                ipo.pop(key, None)
    if junk:
        print(f"  Removed {junk} 'unknown company' descriptions from cache")
    for ipo in ipos:
        keys = [ipo.get("id"), ipo.get("ticker"), ipo.get("cik")]
        ipo_id = next((k for k in keys if k and k in cache and cache[k].get("description")), None) \
            or next((k for k in keys if k and k in cache), None)
        if ipo_id:
            cached = cache[ipo_id]
            changed = False
            for key in ("description", "products_services", "industry"):
                if not ipo.get(key) and cached.get(key):
                    ipo[key] = cached[key]
                    changed = True
            for key in ("shares_outstanding", "proposed_price_low", "proposed_price_high", "ads_ratio"):
                if not ipo.get(key) and cached.get(key):
                    ipo[key] = cached[key]
                    changed = True
            # 日本語訳は、訳した元の英文が今の英文と同じときだけ使う
            if cached.get("description_ja") and cached.get("ja_src") == _ja_src(ipo):
                for key in ("description_ja", "products_services_ja", "ja_src"):
                    if cached.get(key):
                        ipo[key] = cached[key]
            if changed:
                applied += 1
    print(f"  Cache applied: {applied} companies")


async def main():
    print(f"{'='*60}")
    print(f"US IPO Calendar v3 - {datetime.now().isoformat()}")
    print(f"{'='*60}")

    sec = await fetch_sec_filings()
    nasdaq = await fetch_nasdaq_data()

    # NASDAQ Upcoming銘柄のCIK補完
    sec = await supplement_nasdaq_ciks(sec, nasdaq)

    merged = merge_all(sec, nasdaq)

    # 前回データから消失した銘柄を補完（SEC API一時障害対策）
    try:
        with open("ipo_data.json") as f:
            prev_data = json.load(f)
        merged_ids = {i.get("id") for i in merged}
        restored = 0
        for prev in prev_data:
            pid = prev.get("id")
            if pid and pid not in merged_ids and prev.get("status") not in ("listed", "excluded"):
                merged.append(prev)
                restored += 1
        if restored:
            print(f"  Restored {restored} companies from previous data")
    except Exception:
        pass

    # 過去のLLM結果をキャッシュから復元
    cache = load_enrichment_cache()
    if cache:
        print(f"\n=== Enrichment Cache ({len(cache)} entries) ===")
        apply_enrichment_cache(merged, cache)

    # S-1書類から価格レンジ・発行済株式数を抽出
    print("\n⏳ Waiting 120s before SEC document fetch (rate limit cooldown)...")
    await asyncio.sleep(120)
    merged = await extract_s1_financials(merged)
    try:
        await backfill_ads_ratio(merged, cache)
    except Exception as e:
        print(f"  ! ADS backfill failed: {type(e).__name__}: {str(e)[:150]}")

    # proposed_priceでprice_rangeを補完（NASDAQに価格がない場合）
    for ipo in merged:
        if not ipo.get("price_range_low") and ipo.get("proposed_price_low"):
            ipo["price_range_low"] = ipo["proposed_price_low"]
        if not ipo.get("price_range_high") and ipo.get("proposed_price_high"):
            ipo["price_range_high"] = ipo["proposed_price_high"]

    # LLMで未補完分を補完
    enriched = await enrich_with_llm(merged) or merged

    # 企業概要・製品の日本語訳（アプリの日本語表示用）
    try:
        await translate_ja(enriched)
    except Exception as e:
        print(f"  ! translate_ja failed: {type(e).__name__}: {str(e)[:200]}")

    # キャッシュを更新保存
    for ipo in enriched:
        ipo_id = ipo.get("id") or ipo.get("ticker") or ipo.get("cik")
        if ipo_id and (ipo.get("description") or ipo.get("shares_outstanding")):
            entry = cache.get(ipo_id, {})
            for key in ("description", "products_services", "industry"):
                if ipo.get(key):
                    entry[key] = ipo[key]
            for key in ("shares_outstanding", "proposed_price_low", "proposed_price_high",
                        "description_ja", "products_services_ja", "ja_src", "ads_ratio", "ads_checked"):
                if ipo.get(key):
                    entry[key] = ipo[key]
            cache[ipo_id] = entry
    save_enrichment_cache(cache)

    with open("ipo_data.json", "w") as f:
        json.dump([{k:v for k,v in i.items() if k not in ("nasdaq_status","_latest_date")} for i in enriched], f, ensure_ascii=False, indent=2, default=str)

    await update_worker(enriched)
    await detect_and_notify(enriched)

    # S-1 分析（Pro 機能）。失敗してもほかの処理には影響させない
    try:
        import s1_analysis
        async with httpx.AsyncClient(timeout=60, headers=SEC_HEADERS) as sec_client:
            await s1_analysis.run(enriched, _fetch_filing_text, _s1_accession, sec_client,
                                  ANTHROPIC_KEY, WORKER_URL, ADMIN_TOKEN)
    except Exception as e:
        print(f"  S-1 analysis error: {type(e).__name__}: {str(e)[:200]}")
    print(f"\nDone: {len(merged)} IPOs")

if __name__ == "__main__":
    asyncio.run(main())
