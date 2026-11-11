"""
S-1 分析（アプリの Pro 機能）
- S-1/F-1 の本文から「表紙・The Offering・財務サマリー・リスク要約」だけを抜き出し（約2,000トークン）、
  Claude Haiku に決まった形の JSON で答えさせる。
- Message Batches API で送る（料金が半額）。この実行で送り、次の実行（約12時間後）で受け取る。
- 結果は書類番号（accession）ごとに s1_analysis_cache.json に残し、同じ書類は二度と分析しない。
- 受け取った結果は Worker の /api/admin/s1 に送る。アプリは詳細画面を開いたときだけ読む。
"""
import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import anthropic
import httpx

HERE = Path(__file__).parent
CACHE_PATH = HERE / "s1_analysis_cache.json"     # {accession: {"ipo_id", "data", "analyzed_at", "pushed_to", "empty"}}
STATE_PATH = HERE / "s1_batch_state.json"        # 送ったまま結果を受け取っていないバッチ
SCHEMA_VERSION = 3          # 3: 届出書の本来の通貨で記録し、見出しの通貨記号で確かめる（v2 は換算欄の選び間違いがあった）
MAX_PER_RUN = 200         # 1回のバッチに入れる件数（SEC からの取得が1件約6秒。200件で約20分。ワークフローの制限は90分）
EXCERPT_LIMIT = 9000      # Haiku に渡す文字数の上限（約2,300トークン）
LISTED_LOOKBACK_DAYS = 120
BATCH_GIVE_UP_HOURS = 30  # バッチは最長24時間。これを過ぎても終わらなければ捨てて送り直す

# ------------------------------------------------------------
# 本文の抜き出し（LLM を使わない）
# ------------------------------------------------------------

_ZW = re.compile(r"[​‌‍﻿\xa0]")


def _squeeze(s: str) -> str:
    """表の区切りや空白を詰めて、トークンを減らす"""
    s = _ZW.sub(" ", s)
    s = re.sub(r"(?m)^[ \t|\-]*-{3}[ \t|\-]*$", "", s)   # 表の --- 行
    s = re.sub(r"[ \t]*\|[ \t]*-{3}", "", s)
    s = re.sub(r"([ \t]*\|[ \t]*)+", " | ", s)           # | | | → |（改行はまたがない）
    s = re.sub(r"(?m)^ \| | \|$", "", s)
    s = re.sub(r"!\[[^\]]*\]\([^)]*\)", "", s)            # 画像
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\s*\n\s*", "\n", s)
    s = re.sub(r"[*#_]{2,}", "", s)
    return s.strip()


def _is_toc_line(line: str) -> bool:
    """目次の行（「見出し | ページ番号」）"""
    return bool(re.search(r"\|\s*\d{1,3}\s*(\|\s*)*$", _ZW.sub(" ", line)))


def _section(text: str, heading: str, length: int):
    for m in re.finditer(rf"^[#*\s]*({heading})[*\s:|]*$", text, re.I | re.M):
        line_end = text.find("\n", m.start())
        if _is_toc_line(text[m.start():line_end if line_end > 0 else None]):
            continue
        return text[m.start():m.start() + length]
    return None


def _offering_lines(text: str, max_chars: int = 3000):
    """「The Offering」の表から、資金使途・ロックアップ・上場先の項目だけ拾う（見出しだけの行なら続きも取る）"""
    m = None
    for m in re.finditer(r"^[#*\s]*(The Offering|THE OFFERING)[*\s:|]*$", text, re.M):
        if not _is_toc_line(text[m.start():text.find("\n", m.start())]):
            break
    block = text[m.start():m.start() + 20000] if m else text[:60000]
    keys = re.compile(r"^[#*\s|]*(use of proceeds|lock-?up|listing|(proposed )?(nasdaq|nyse)[\w ]*symbol|trading symbol|directed share)",
                      re.I | re.M)
    out, n, last_end = [], 0, -1
    for k in keys.finditer(block):
        if k.start() < last_end:
            continue
        piece = block[k.start():k.start() + 800]
        last_end = k.start() + 800
        out.append(piece)
        n += len(piece)
        if n > max_chars:
            break
    return "\n".join(out) or None


def _cover(text: str):
    """表紙：募集の概要（先頭）と主幹事の名前（末尾）"""
    start = re.search(r"PRELIMINARY PROSPECTUS|SUBJECT TO COMPLETION|^\s*PROSPECTUS\s*$", text, re.M)
    s0 = start.start() if start else 0
    end = re.search(r"Prospectus dated|The date of this prospectus is|prospectus is dated", text[s0:s0 + 20000], re.I)
    cover = text[s0:s0 + end.end() + 40] if end else text[s0:s0 + 5000]
    if len(cover) > 2600:
        cover = cover[:1500] + "\n...\n" + cover[-1000:]
    return cover


def excerpt(text: str) -> str:
    parts = []
    for label, chunk in (
        ("COVER", _cover(text)),
        ("OFFERING", _offering_lines(text)),
        ("SUMMARY FINANCIAL DATA", _section(
            text, r"Summary (Consolidated |Historical |Combined )?(Financial|Operating|Historical)[\w ,]*(Data|Information)", 4500)),
        ("RISK SUMMARY", _section(
            text, r"Risk Factors Summary|Summary (of )?Risk Factors|Summary Risk Factors|Risks Related to Our Business", 2200)),
    ):
        if chunk:
            parts.append(f"[{label}]\n{_squeeze(chunk)}")
    return "\n\n".join(parts)[:EXCERPT_LIMIT]


_LOCKUP_WORD = re.compile(r"lock-?up", re.I)
_LOCKUP_DAYS = re.compile(r"(?:period of|for)\s+(\d{2,3})\s+days|(\d{2,3})[- ]day\b", re.I)


def lockup_days_hint(text: str):
    """ロックアップ日数を正規表現で拾う（本文全体から。LLM の答えが無いときの予備）。
    「lock-up」の直後500文字にある「◯日間」だけを見る（追加購入の30日やディーラーの25日を拾わないため）"""
    for m in _LOCKUP_WORD.finditer(text):
        for d in _LOCKUP_DAYS.finditer(text[m.end():m.end() + 500]):
            n = int(d.group(1) or d.group(2))
            if 60 <= n <= 720:
                return n
    return None


# ------------------------------------------------------------
# Haiku へのリクエスト
# ------------------------------------------------------------

MODEL = "claude-haiku-5-5"

_num = {"anyOf": [{"type": "number"}, {"type": "null"}]}
_str = {"anyOf": [{"type": "string"}, {"type": "null"}]}
_int = {"anyOf": [{"type": "integer"}, {"type": "null"}]}
_strs = {"type": "array", "items": {"type": "string"}}

SCHEMA = {
    "type": "object",
    "properties": {
        "unit": {"type": "string", "enum": ["units", "thousands", "millions"]},
        "currency": {"type": "string"},  # ISO 4217（USD, CAD, MYR, CNY, HKD など）
        "fiscal_year": _str,             # 例 "FY2025"
        "revenue": _num,
        "revenue_prior_year": _num,
        "net_income": _num,              # 赤字はマイナス
        "net_income_prior_year": _num,
        "interim_period": _str,          # 例 "6M 2026"
        "interim_revenue": _num,
        "interim_net_income": _num,
        "net_proceeds_usd": _num,        # こちらは常にドル（単位換算しない）
        "use_of_proceeds": _strs,
        "underwriters": _strs,
        "lockup_days": _int,
        "key_risks": _strs,
        "use_of_proceeds_ja": _strs,     # 日本語版（アプリの日本語表示用）
        "key_risks_ja": _strs,
    },
    "required": ["unit", "currency", "fiscal_year", "revenue", "revenue_prior_year", "net_income", "net_income_prior_year",
                 "interim_period", "interim_revenue", "interim_net_income", "net_proceeds_usd",
                 "use_of_proceeds", "underwriters", "lockup_days", "key_risks",
                 "use_of_proceeds_ja", "key_risks_ja"],
    "additionalProperties": False,
}

# 指示は固定文にして、全リクエストで同じ先頭（キャッシュが効く形）にしておく
SYSTEM = """You extract facts from excerpts of SEC IPO registration statements (S-1/F-1). Use ONLY the excerpt. Never guess; use null or [] when the excerpt does not state it.
- Financials: from the summary financial data table. Report numbers exactly as printed. Losses are negative. revenue = total revenue (null for pre-revenue companies). Latest full fiscal year and the one before (match each number to its column header); interim = latest partial period shown, if any.
- Report the figures in the company's reporting currency (the main columns), NOT any US$ "convenience translation" column. "currency" = its ISO 4217 code: USD, CAD, MYR ("RM"), CNY ("RMB"), HKD, SGD, TWD ("NT$"), JPY, EUR, GBP, AUD, etc.
- "unit": "thousands" only if the table explicitly says "in thousands" (or "$000", "'000"); "millions" only if it says "in millions"; otherwise "units" (amounts printed in full).
- net_proceeds_usd: estimated net proceeds to the company in US dollars (full number, e.g. 181000000), assuming no over-allotment.
- use_of_proceeds: up to 3 items, each under 12 words.
- underwriters: lead/book-running managers only, short names (e.g. "Goldman Sachs"), up to 6.
- lockup_days: lock-up period in days, if stated.
- key_risks: the 3 most company-specific risks, each under 15 words.
- use_of_proceeds_ja / key_risks_ja: natural Japanese translations of use_of_proceeds / key_risks, same order and count, each under 40 characters."""


def build_request(custom_id: str, company: str, text_excerpt: str) -> dict:
    return {
        "custom_id": custom_id,
        "params": {
            "model": MODEL,
            "max_tokens": 1500,
            # 短い抽出なので考える過程は使わない（Haiku 5.5 は effort high 以下なら disabled を受け付ける）
            "thinking": {"type": "disabled"},
            "system": SYSTEM,
            "output_config": {"format": {"type": "json_schema", "schema": SCHEMA}},
            "messages": [{"role": "user", "content": f"Company: {company}\n\n{text_excerpt}"}],
        },
    }


def _custom_id(accession: str) -> str:
    return "s1-" + re.sub(r"[^A-Za-z0-9_-]", "", accession)[:60]


# ------------------------------------------------------------
# 結果の整形（アプリに渡す形）
# ------------------------------------------------------------

_MULT = {"units": 1, "thousands": 1_000, "millions": 1_000_000}


_UNIT_THOUSANDS = re.compile(r"in\s+thousands|thousands\s+of|\(\s*\$?\s*0{3}s?\s*\)|\$\s*0{3}s?\b|'0{3}", re.I)
_UNIT_MILLIONS = re.compile(r"in\s+millions|millions\s+of\s+(?:U\.?S\.?\s*)?dollars", re.I)


_CCY_TOKENS = [
    (r"\bRM\b|\bMYR\b|Ringgit", "MYR"), (r"\bRMB\b|\bCNY\b|Renminbi", "CNY"), (r"HK\$|\bHKD\b", "HKD"),
    (r"CAD\$|\bC\$|\bCAD\b|Canadian dollars?", "CAD"), (r"\bS\$|\bSGD\b", "SGD"), (r"NT\$|\bTWD\b", "TWD"),
    (r"\bJPY\b|¥|Japanese yen", "JPY"), (r"€|\bEUR\b", "EUR"), (r"£|\bGBP\b", "GBP"), (r"\bA\$|\bAUD\b", "AUD"),
    (r"\bTHB\b|Baht", "THB"), (r"\bINR\b|₹", "INR"), (r"\bKRW\b|₩", "KRW"), (r"\bIDR\b|Rupiah", "IDR"),
    (r"\bPHP\b|₱", "PHP"), (r"\bVND\b", "VND"), (r"\bCHF\b", "CHF"), (r"\bILS\b|NIS\b", "ILS"),
]
_ISO_ALIASES = {"RM": "MYR", "RMB": "CNY", "NT$": "TWD", "HK$": "HKD", "US$": "USD", "C$": "CAD", "S$": "SGD"}


def _financial_section(excerpt_text: str) -> str:
    m = re.search(r"\[SUMMARY FINANCIAL DATA\](.*?)(?:\n\[[A-Z ]+\]|\Z)", excerpt_text, re.S)
    return m.group(1) if m else excerpt_text


def currency_hint(excerpt_text: str):
    """財務の表の見出しに3回以上出てくるドル以外の通貨。無ければ None（米ドルとみなす）"""
    sec = _financial_section(excerpt_text)
    best, n = None, 0
    for pat, code in _CCY_TOKENS:
        k = len(re.findall(pat, sec))
        if k > n:
            best, n = code, k
    return best if n >= 3 else None


def unit_hint(excerpt_text: str):
    """財務の表に単位の記載があるかを機械的に見る。記載がなければ金額は1単位で書かれている。
    （Haiku が「千単位」と読み違え、1単位の表を1000倍した例があった：Devonian, Kainan）"""
    sec = _financial_section(excerpt_text)
    if _UNIT_MILLIONS.search(sec):
        return "millions"
    if _UNIT_THOUSANDS.search(sec):
        return "thousands"
    return "units"


def normalize(raw: dict, hints: dict) -> dict:
    unit = raw.get("unit") or "units"
    # 表に単位の記載が無いのに千・百万と答えたときは、記載のほうを信じる
    if hints.get("unit") == "units" and unit != "units":
        unit = "units"
    mult = _MULT.get(unit, 1)
    currency = (raw.get("currency") or "USD").strip().upper()
    currency = _ISO_ALIASES.get(currency, currency)
    if not re.fullmatch(r"[A-Z]{3}", currency):
        currency = "USD"
    # 見出しがリンギットなのに「USD」と答えた例があった（Kainan）。見出しの通貨を信じる
    if currency == "USD" and hints.get("currency"):
        currency = hints["currency"]

    def money(v):
        return round(v * mult) if isinstance(v, (int, float)) else None

    def short_list(v, n, max_len=120):
        return [s.strip()[:max_len] for s in (v or []) if isinstance(s, str) and s.strip()][:n]

    lockup = raw.get("lockup_days")
    if not (isinstance(lockup, int) and 30 <= lockup <= 720):
        lockup = hints.get("lockup_days")
    proceeds = raw.get("net_proceeds_usd")
    return {
        "v": SCHEMA_VERSION,
        "currency": currency,           # revenue / net_income の通貨（net_proceeds は常に USD）
        "fiscal_year": raw.get("fiscal_year"),
        "revenue": money(raw.get("revenue")),
        "revenue_prior_year": money(raw.get("revenue_prior_year")),
        "net_income": money(raw.get("net_income")),
        "net_income_prior_year": money(raw.get("net_income_prior_year")),
        "interim_period": raw.get("interim_period"),
        "interim_revenue": money(raw.get("interim_revenue")),
        "interim_net_income": money(raw.get("interim_net_income")),
        "net_proceeds": round(proceeds) if isinstance(proceeds, (int, float)) and proceeds > 0 else None,
        "use_of_proceeds": short_list(raw.get("use_of_proceeds"), 3),
        "underwriters": short_list(raw.get("underwriters"), 6, 60),
        "lockup_days": lockup,
        "key_risks": short_list(raw.get("key_risks"), 3),
        "use_of_proceeds_ja": short_list(raw.get("use_of_proceeds_ja"), 3, 80),
        "key_risks_ja": short_list(raw.get("key_risks_ja"), 3, 80),
    }


def _has_content(d: dict) -> bool:
    return any(d.get(k) for k in ("revenue", "net_income", "net_proceeds", "use_of_proceeds", "underwriters", "key_risks"))


# ------------------------------------------------------------
# キャッシュと状態ファイル
# ------------------------------------------------------------

def _load(path: Path, default):
    try:
        return json.loads(path.read_text())
    except Exception:
        return default


def _save(path: Path, data):
    path.write_text(json.dumps(data, ensure_ascii=False, indent=1, sort_keys=True))


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# ------------------------------------------------------------
# 対象選び
# ------------------------------------------------------------

def _targets(ipos: list, cache: dict, accession_of) -> list:
    today = datetime.now().strftime("%Y-%m-%d")
    recent = (datetime.now() - timedelta(days=LISTED_LOOKBACK_DAYS)).strftime("%Y-%m-%d")
    analyzed_ids = {v.get("ipo_id") for v in cache.values()}
    seen = set()
    rows = []
    for ipo in ipos:
        status = ipo.get("status")
        if status in ("withdrawn", "excluded") or not ipo.get("cik"):
            continue
        if status == "listed" and (ipo.get("actual_date") or ipo.get("expected_date") or "") < recent:
            continue
        acc = accession_of(ipo)
        if not acc or acc in seen:
            continue
        done = cache.get(acc)
        if done and (done.get("data") or {}).get("v", 0) >= SCHEMA_VERSION:
            continue   # 古い版の結果は分析し直す（その間は古い結果を出し続ける）
        seen.add(acc)   # 同じ書類が ID 違いで2銘柄に入っていることがある（バッチは custom_id の重複を受け付けない）
        # 並び：一度も分析していない銘柄 → 上場予定日が近い順 → 申請日が新しい順
        rows.append(((ipo.get("id") in analyzed_ids),
                     not ((ipo.get("expected_date") or "") >= today),
                     ipo.get("expected_date") or "9999",
                     "".join(chr(255 - ord(c)) for c in (ipo.get("filing_date") or "")),
                     ipo, acc))
    rows.sort(key=lambda r: r[:4])
    return [(r[4], r[5]) for r in rows]


# ------------------------------------------------------------
# 本体
# ------------------------------------------------------------

async def run(ipos: list, fetch_text, accession_of, sec_client, api_key: str, worker_url: str, admin_token: str):
    """1回の実行でやること：前回のバッチを受け取る → 新しいバッチを送る → 未送信の結果を Worker に送る"""
    print("\n=== S-1 Analysis ===")
    cache = _load(CACHE_PATH, {})
    state = _load(STATE_PATH, {})
    client = anthropic.AsyncAnthropic(api_key=api_key, max_retries=3) if api_key else None

    if client and state.get("batch_id"):
        done = await _collect(client, state, cache)
        if done:
            state = {}
            _save(STATE_PATH, state)
        _save(CACHE_PATH, cache)

    if client and not state.get("batch_id"):
        state = await _submit(client, ipos, cache, fetch_text, accession_of, sec_client)
        if state:
            _save(STATE_PATH, state)

    if worker_url and admin_token:
        await _push(ipos, accession_of, cache, worker_url, admin_token)
        _save(CACHE_PATH, cache)


async def _collect(client, state: dict, cache: dict) -> bool:
    batch_id = state["batch_id"]
    try:
        batch = await client.messages.batches.retrieve(batch_id)
    except anthropic.NotFoundError:
        print(f"  batch {batch_id} not found, dropping it")
        return True
    except anthropic.APIError as e:
        print(f"  batch retrieve failed: {type(e).__name__} {str(e)[:150]}")
        return False

    if batch.processing_status != "ended":
        age_h = (datetime.now(timezone.utc) - datetime.fromisoformat(state["submitted_at"])).total_seconds() / 3600
        print(f"  batch {batch_id}: {batch.processing_status} ({age_h:.1f}h old)")
        if age_h > BATCH_GIVE_UP_HOURS:
            print("  giving up on it; the items will be sent again")
            return True
        return False

    items = state.get("items", {})
    ok = empty = failed = 0
    usage_in = usage_out = 0
    async for result in await client.messages.batches.results(batch_id):
        item = items.get(result.custom_id)
        if not item:
            continue
        if result.result.type != "succeeded":
            failed += 1
            print(f"  - {item['ipo_id']}: {result.result.type}")
            continue
        msg = result.result.message
        usage_in += msg.usage.input_tokens
        usage_out += msg.usage.output_tokens
        if msg.stop_reason in ("refusal", "max_tokens"):
            failed += 1
            print(f"  - {item['ipo_id']}: stop={msg.stop_reason}")
            continue
        text = next((b.text for b in msg.content if b.type == "text"), "")
        try:
            data = normalize(json.loads(text), item.get("hints", {}))
        except (json.JSONDecodeError, TypeError, AttributeError) as e:
            failed += 1
            print(f"  - {item['ipo_id']}: bad JSON ({type(e).__name__})")
            continue
        if not _has_content(data):
            empty += 1   # 抜き出しに何も載っていなかった。記録して二度と送らない
        cache[item["accession"]] = {"ipo_id": item["ipo_id"], "data": data, "analyzed_at": _now(),
                                    "pushed_to": [], "empty": not _has_content(data)}
        ok += 1
    # バッチは半額。Haiku 5.5 は入力 $0.10 / 出力 $0.50（100万トークンあたり）
    cost = (usage_in * 0.10 + usage_out * 0.50) / 1_000_000 / 2
    print(f"  collected {batch_id}: {ok} ok ({empty} empty), {failed} failed, tokens in={usage_in} out={usage_out}, ~${cost:.4f}")
    return True


async def _submit(client, ipos, cache, fetch_text, accession_of, sec_client) -> dict:
    targets = _targets(ipos, cache, accession_of)
    print(f"  targets: {len(targets)} filings not analyzed yet (sending up to {MAX_PER_RUN})")
    requests, items = [], {}
    for ipo, acc in targets[:MAX_PER_RUN]:
        try:
            text = await fetch_text(sec_client, ipo, acc)
        except Exception as e:
            print(f"  ! {ipo.get('ticker') or ipo.get('id')}: fetch {type(e).__name__}")
            continue
        if not text:
            continue
        ex = excerpt(text)
        if len(ex) < 800:
            print(f"  ! {ipo.get('ticker') or ipo.get('id')}: excerpt too short ({len(ex)})")
            continue
        cid = _custom_id(acc)
        requests.append(build_request(cid, ipo.get("company_name", ""), ex))
        items[cid] = {"ipo_id": ipo.get("id"), "accession": acc,
                      "hints": {"lockup_days": lockup_days_hint(text), "unit": unit_hint(ex),
                                "currency": currency_hint(ex)}}
    if not requests:
        return {}
    try:
        batch = await client.messages.batches.create(requests=requests)
    except anthropic.APIError as e:
        print(f"  batch create failed: {type(e).__name__} {str(e)[:200]}")
        return {}
    chars = sum(len(r["params"]["messages"][0]["content"]) for r in requests)
    print(f"  submitted batch {batch.id}: {len(requests)} filings, ~{chars // 4} input tokens")
    return {"batch_id": batch.id, "submitted_at": _now(), "items": items}


async def _push(ipos, accession_of, cache: dict, worker_url: str, admin_token: str):
    """銘柄ごとに、今の書類の分析（なければその銘柄の一番新しい分析）を送る。
    同じ書類がティッカーと CIK の2つの ID で入っていることがあるので、ID ごとに送る"""
    by_ipo = {}
    for acc, entry in cache.items():
        if entry.get("empty"):
            continue
        cur = by_ipo.get(entry["ipo_id"])
        if not cur or entry["analyzed_at"] > cur[1]["analyzed_at"]:
            by_ipo[entry["ipo_id"]] = (acc, entry)
    todo = []
    for ipo in ipos:
        ipo_id = ipo.get("id")
        if not ipo_id:
            continue
        acc = accession_of(ipo)
        entry = cache.get(acc)
        pick = (acc, entry) if entry and not entry.get("empty") else by_ipo.get(ipo_id)
        if not pick:
            continue
        if ipo_id in pick[1].setdefault("pushed_to", []):
            continue
        todo.append((ipo_id, pick[0], pick[1]))
    if not todo:
        return
    print(f"  pushing {len(todo)} analyses to Worker")
    async with httpx.AsyncClient(timeout=60) as client:
        for i in range(0, len(todo), 20):
            chunk = todo[i:i + 20]
            body = {"items": [{"ipo_id": ipo_id, "accession": acc, "data": e["data"], "analyzed_at": e["analyzed_at"]}
                              for ipo_id, acc, e in chunk]}
            try:
                resp = await client.post(f"{worker_url}/api/admin/s1", json=body,
                                         headers={"Authorization": f"Bearer {admin_token}"})
            except httpx.HTTPError as e:
                print(f"  push error: {type(e).__name__}")
                return
            if resp.status_code != 200:
                print(f"  push {resp.status_code}: {resp.text[:150]}")
                return
            for ipo_id, _, e in chunk:
                e["pushed_to"].append(ipo_id)
