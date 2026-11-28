"""1.3 で足した処理。scrape_ipos.py から呼ぶ。どれも失敗してもほかの処理は止めない。

1. ロックアップ解除の暦（lockups）
   NASDAQ の IPO カレンダー（過去8か月）の priced から、各 IPO の詳細（overview）を1回だけ読み、
   ロックアップ解除日・日数・クワイエット期間明けを lockup_cache.json に残す（日付は上場時に決まり変わらない）。
   Worker の /api/admin/lockups に送る。SPAC は除く。
2. ロックアップ解除の3日前通知（ウォッチリストの銘柄、Pro）
3. 韓国語・ドイツ語訳（会社概要・製品、S-1 分析の資金使途・リスク）
"""
import asyncio, hashlib, json, re
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

LOCKUP_CACHE = "lockup_cache.json"
LOCKUP_MONTHS = 8          # ロックアップは多くが180日。過去8か月分の上場を見れば解除前の銘柄はそろう
LANGS = {"ko": "Korean", "de": "German"}
LANG_STYLE = {
    "ko": "Write concise Korean for Korean retail investors (plain declarative style, e.g. ~한다/~이다 or noun endings).",
    "de": "Write concise German for German-speaking retail investors (neutral, factual; use standard financial terms).",
}
TR_MAX_PER_RUN = 400
TR_CHUNK = 20

_last_lockups = []         # 通知で使う（update_lockups が入れる）


def _load(path, default):
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return default


def _save(path, data):
    with open(path, "w") as f:
        json.dump(data, f, ensure_ascii=False, indent=1, sort_keys=True)


def _mdy(s):
    try:
        return datetime.strptime(s.strip(), "%m/%d/%Y").strftime("%Y-%m-%d")
    except Exception:
        return None


def _num(s):
    s = re.sub(r"[^\d.]", "", str(s or ""))
    try:
        return float(s) if s else None
    except ValueError:
        return None


def _is_spac(row):
    name = (row.get("companyName") or "").lower()
    t = row.get("proposedTickerSymbol") or ""
    price = _num(row.get("proposedSharePrice"))
    if re.search(r"acquisition|blank check|\bspac\b|merger corp", name):
        return True
    return price == 10.0 and len(t) == 5 and t.endswith("U")


# ------------------------------------------------------------------ 1. lockups

async def update_lockups(httpx, headers, worker_url, admin_token):
    global _last_lockups
    print("\n=== Lockups ===")
    cache = _load(LOCKUP_CACHE, {})
    now = datetime.now()
    months = sorted({(now - timedelta(days=30 * i)).strftime("%Y-%m") for i in range(LOCKUP_MONTHS + 1)})
    priced = {}
    async with httpx.AsyncClient(timeout=30, headers=headers) as client:
        for m in months:
            try:
                r = await client.get(f"https://api.nasdaq.com/api/ipo/calendar?date={m}")
                rows = ((r.json().get("data") or {}).get("priced") or {}).get("rows") or []
            except Exception as e:
                print(f"  [{m}] calendar error: {type(e).__name__}")
                continue
            for row in rows:
                if row.get("dealID") and not _is_spac(row):
                    priced[row["dealID"]] = row
        new = 0
        for did, row in priced.items():
            if did in cache and cache[did].get("checked"):
                continue
            try:
                r = await client.get("https://api.nasdaq.com/api/ipo/overview/", params={"dealId": did})
                po = ((r.json().get("data") or {}).get("poOverview")) or {}
            except Exception as e:
                print(f"  {row.get('proposedTickerSymbol')}: overview error {type(e).__name__}")
                continue
            v = lambda k: (po.get(k) or {}).get("value")
            cache[did] = {
                "deal_id": did,
                "ticker": row.get("proposedTickerSymbol"),
                "company_name": row.get("companyName"),
                "exchange": row.get("proposedExchange"),
                "priced_date": _mdy(row.get("pricedDate") or ""),
                "offer_price": _num(row.get("proposedSharePrice")),
                "shares_offered": _num(row.get("sharesOffered")),
                "shares_outstanding": _num(v("SharesOutstanding")),
                "lockup_days": _num(v("LockupPeriodNumberofDays")),
                "lockup_date": _mdy(v("LockupPeriodExpirationDate") or ""),
                "quiet_period_date": _mdy(v("QuietPeriodExpirationDate") or ""),
                "checked": now.strftime("%Y-%m-%d"),
            }
            new += 1
            await asyncio.sleep(0.4)
    # 解除から60日を過ぎたものは暦から外す（キャッシュも軽く保つ）
    cutoff = (now - timedelta(days=60)).strftime("%Y-%m-%d")
    cache = {k: v for k, v in cache.items() if (v.get("lockup_date") or v.get("priced_date") or "9999") >= cutoff}
    _save(LOCKUP_CACHE, cache)
    items = [v for v in cache.values() if v.get("lockup_date") and v.get("ticker")]
    _last_lockups = items
    print(f"  priced deals {len(priced)}, new overviews {new}, with lockup date {len(items)}")
    if not worker_url or not admin_token or not items:
        return items
    async with httpx.AsyncClient(timeout=90) as client:
        for i in range(0, len(items), 40):
            try:
                r = await client.post(f"{worker_url}/api/admin/lockups", json={"items": items[i:i + 40]},
                                      headers={"Authorization": f"Bearer {admin_token}"})
                print(f"  push {i // 40 + 1}: {r.status_code} {r.text[:80]}")
            except Exception as e:
                print(f"  push error: {type(e).__name__}")
        # 通知の宛先は D1 の ipos.id。Worker がティッカーで引いた id を使う（古い上場は ipos がこの実行に無い）
        try:
            r = await client.get(f"{worker_url}/api/lockups")
            ids = {x["deal_id"]: x.get("ipo_id") for x in r.json() if isinstance(x, dict)}
            for x in items:
                if ids.get(x["deal_id"]):
                    x["ipo_id"] = ids[x["deal_id"]]
        except Exception as e:
            print(f"  lockup id lookup error: {type(e).__name__}")
    return items


# ------------------------------------------------------------------ 2. lockup notice

def lockup_notices(ipos, sent_state):
    """解除の3日前（米国東部の日付）にウォッチリストの銘柄へ1回だけ。ipo_id は D1 の ipos.id（ティッカー一致）"""
    today = datetime.now(ZoneInfo("America/New_York")).date()
    soon = (today + timedelta(days=3)).isoformat()
    by_ticker = {}
    for ipo in ipos:
        if ipo.get("ticker") and ipo.get("id"):
            by_ticker.setdefault(ipo["ticker"], ipo["id"])
    sent = sent_state.get("lockup", {})
    out = []
    for x in _last_lockups:
        d = x.get("lockup_date")
        if not d or not (today.isoformat() < d <= soon):
            continue
        ipo_id = x.get("ipo_id") or by_ticker.get(x["ticker"]) or x["ticker"]
        if sent.get(ipo_id) == d:
            continue
        sent[ipo_id] = d
        t, name = x["ticker"], x.get("company_name") or x["ticker"]
        out.append({
            "ipo_id": ipo_id, "type": "lockup",
            "title": f"{t} - Lockup expires {d}",
            "body": f"The IPO lockup for {name} ends on {d}. Insiders may be able to sell shares after this date.",
            "title_ja": f"{t} - ロックアップ解除 {d}",
            "body_ja": f"{name} のロックアップが {d}（米国時間）に明けます。この日から既存株主が売れるようになります。",
            "title_ko": f"{t} - 보호예수 해제 {d}",
            "body_ko": f"{name}의 보호예수(락업)가 {d}(미국 시간)에 해제됩니다. 이날부터 기존 주주가 주식을 팔 수 있습니다.",
            "title_de": f"{t} - Lock-up endet am {d}",
            "body_de": f"Die Lock-up-Frist von {name} endet am {d} (US-Zeit). Danach dürfen Altaktionäre verkaufen.",
        })
    sent_state["lockup"] = {k: v for k, v in sent.items() if v >= today.isoformat()}
    if out:
        print(f"  lockup notices: {len(out)}")
    return out


# ------------------------------------------------------------------ 3. translations

def src_hash(ipo):
    s = (ipo.get("description") or "") + "\n" + (ipo.get("products_services") or "")
    return hashlib.sha1(s.encode()).hexdigest()[:12]


async def translate_more(ipos, client, model, parse_json):
    """description / products_services を韓国語・ドイツ語に。<lang>_src が今の英文と同じなら飛ばす"""
    print("\n=== Korean / German Translation ===")
    if not client:
        print("  [SKIP] no ANTHROPIC_API_KEY")
        return
    stats = {"done": 0, "in": 0, "out": 0}
    sem = asyncio.Semaphore(5)
    for lang, lang_name in LANGS.items():
        seen = {}
        for ipo in ipos:
            if not ipo.get("description"):
                continue
            h = src_hash(ipo)
            if ipo.get(f"description_{lang}") and ipo.get(f"{lang}_src") == h:
                continue
            seen.setdefault(h, []).append(ipo)
        todo = list(seen.items())[:TR_MAX_PER_RUN]
        print(f"  {lang}: {len(seen)} texts need translation (up to {TR_MAX_PER_RUN} this run)")

        async def one(start, lang=lang, lang_name=lang_name, todo=todo):
            chunk = todo[start:start + TR_CHUNK]
            items = {str(n): {"description": g[0].get("description"), "products_services": g[0].get("products_services")}
                     for n, (_, g) in enumerate(chunk)}
            prompt = (
                f"Translate each company's description and products_services into natural {lang_name} "
                "for retail investors reading an IPO app. Keep company names, product names, drug codes and "
                f"tickers in their original spelling. Keep numbers exact. {LANG_STYLE[lang]} "
                "If a field is null, return null for it. Respond with JSON only, no markdown, "
                'in the form {"0": {"description": "...", "products_services": "..."}, ...}.\n\n'
                + json.dumps(items, ensure_ascii=False))
            async with sem:
                try:
                    resp = await client.messages.create(model=model, max_tokens=16000, thinking={"type": "disabled"},
                                                        messages=[{"role": "user", "content": prompt}])
                except Exception as e:
                    print(f"  ! {lang} chunk {start // TR_CHUNK}: {type(e).__name__}: {str(e)[:150]}")
                    return
            stats["in"] += resp.usage.input_tokens
            stats["out"] += resp.usage.output_tokens
            text = "".join(b.text for b in resp.content if b.type == "text")
            try:
                out = parse_json(text) or {}
            except Exception:
                out = {}
            if resp.stop_reason != "end_turn" or not isinstance(out, dict) or not out:
                print(f"  ! {lang} chunk {start // TR_CHUNK}: stop={resp.stop_reason}, unparsed")
                return
            for n, (h, group) in enumerate(chunk):
                t = out.get(str(n))
                if not isinstance(t, dict) or not isinstance(t.get("description"), str) or not t["description"].strip():
                    continue
                p = t.get("products_services") if isinstance(t.get("products_services"), str) else None
                for ipo in group:
                    ipo[f"description_{lang}"] = t["description"].strip()
                    if p and p.strip():
                        ipo[f"products_services_{lang}"] = p.strip()
                    else:
                        ipo.pop(f"products_services_{lang}", None)
                    ipo[f"{lang}_src"] = h
                stats["done"] += 1

        await asyncio.gather(*(one(st) for st in range(0, len(todo), TR_CHUNK)))
    cost = stats["in"] / 1e6 * 0.10 + stats["out"] / 1e6 * 0.50
    print(f"  translated {stats['done']} texts, tokens in={stats['in']} out={stats['out']}, ~${cost:.4f}")


CACHE_KEYS = [f"{k}_{l}" for l in LANGS for k in ("description", "products_services")] + [f"{l}_src" for l in LANGS]


def apply_cache(ipos, cache):
    """enrichment_cache から韓国語・ドイツ語訳を戻す（英文が同じときだけ）"""
    for ipo in ipos:
        keys = [ipo.get("id"), ipo.get("ticker"), ipo.get("cik")]
        k = next((k for k in keys if k and k in cache), None)
        if not k:
            continue
        c, h = cache[k], src_hash(ipo)
        for lang in LANGS:
            if c.get(f"description_{lang}") and c.get(f"{lang}_src") == h:
                for key in (f"description_{lang}", f"products_services_{lang}", f"{lang}_src"):
                    if c.get(key):
                        ipo[key] = c[key]


async def translate_s1_lists(cache_path, client, model, parse_json, max_items=60):
    """S-1 分析の資金使途・リスク（英語の短い箇条書き）を韓国語・ドイツ語に。結果は s1_analysis_cache.json に足す。
    足した分は s1_analysis._push が次に Worker へ送る（data に入るだけなので D1 の列は増やさない）。"""
    print("\n=== S-1 list translation (ko/de) ===")
    if not client:
        return 0
    cache = _load(cache_path, {})
    todo = []
    for key, entry in cache.items():
        if not isinstance(entry, dict) or entry.get("empty") or not isinstance(entry.get("data"), dict):
            continue
        d = entry["data"]
        if any(d.get(f) and d.get(f"{f}_{l}") is None for f in ("use_of_proceeds", "key_risks") for l in LANGS):
            todo.append((entry, d))
    todo = todo[:max_items]
    print(f"  {len(todo)} analyses to translate")
    done, tin, tout = 0, 0, 0
    for start in range(0, len(todo), 10):
        chunk = todo[start:start + 10]
        items = {str(n): {"use_of_proceeds": d.get("use_of_proceeds") or [], "key_risks": d.get("key_risks") or []}
                 for n, (_, d) in enumerate(chunk)}
        prompt = (
            "Translate these short bullet lists from an IPO prospectus summary into Korean (ko) and German (de). "
            "Same order and count as the English lists. Each item under 40 characters for Korean and under 70 for German. "
            "Keep company names, product names and numbers exact. Respond with JSON only, no markdown, in the form "
            '{"0": {"use_of_proceeds_ko": [...], "key_risks_ko": [...], "use_of_proceeds_de": [...], "key_risks_de": [...]}, ...}.\n\n'
            + json.dumps(items, ensure_ascii=False))
        try:
            resp = await client.messages.create(model=model, max_tokens=8000, thinking={"type": "disabled"},
                                                messages=[{"role": "user", "content": prompt}])
        except Exception as e:
            print(f"  ! s1 chunk: {type(e).__name__}: {str(e)[:150]}")
            continue
        tin += resp.usage.input_tokens
        tout += resp.usage.output_tokens
        try:
            out = parse_json("".join(b.text for b in resp.content if b.type == "text")) or {}
        except Exception:
            out = {}
        for n, (entry, d) in enumerate(chunk):
            t = out.get(str(n)) if isinstance(out, dict) else None
            if not isinstance(t, dict):
                continue
            ok = False
            for field in ("use_of_proceeds", "key_risks"):
                src = d.get(field) or []
                for l in LANGS:
                    v = t.get(f"{field}_{l}")
                    if isinstance(v, list) and len(v) == len(src) and all(isinstance(x, str) for x in v):
                        d[f"{field}_{l}"] = [x.strip()[:120] for x in v]
                        ok = True
            if ok:
                entry["pushed_to"] = []      # 訳を足したので Worker に送り直す
                done += 1
    _save_s1(cache_path, cache)
    print(f"  translated {done} analyses, tokens in={tin} out={tout}, ~${tin / 1e6 * 0.10 + tout / 1e6 * 0.50:.4f}")
    return done


def _save_s1(path, data):
    # s1_analysis._save と同じ書き方（差分を小さく保つ）
    with open(path, "w") as f:
        f.write(json.dumps(data, ensure_ascii=False, indent=1, sort_keys=True))
