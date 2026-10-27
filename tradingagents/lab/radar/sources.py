"""Where the radar looks: headlines, news about public figures, and disclosed trades.

Each collector takes a ``fetch(url) -> bytes`` (injectable for tests) and returns
signal dicts: ``source, uid, ts, actor, title, url, body`` and, for the structured
sources (disclosed trades), ``hits`` already filled in, since no model is needed
to read them.
"""

from __future__ import annotations

import json
import math
import time
import xml.etree.ElementTree as ET
from collections.abc import Callable
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from urllib.parse import quote_plus

Fetch = Callable[[str], bytes]

BLOOMBERG_FEEDS = ("markets", "politics", "technology", "economics")
CONGRESS_URL = "https://www.bargo.ai/free-apis/congress/v1/trades?limit=100"
SEC_CURRENT_FORM4 = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&count={n}&output=atom"
ATOM = "{http://www.w3.org/2005/Atom}"


def http_fetch(url: str) -> bytes:
    import requests

    from tradingagents.dataflows.vendors.sec_edgar import _user_agent

    response = requests.get(url, headers={"User-Agent": _user_agent()}, timeout=20)
    response.raise_for_status()
    return response.content


def _utc(dt: datetime) -> str:
    """Stored timestamps are naive UTC; a zone-less one is taken as UTC already."""
    if dt.tzinfo:
        dt = dt.astimezone(UTC)
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


def _iso(text: str | None) -> str:
    """RSS pubDate (RFC 822) or ISO 8601 -> UTC; unreadable -> now."""
    if text:
        for parse in (parsedate_to_datetime, datetime.fromisoformat):
            try:
                return _utc(parse(text.strip()))
            except (TypeError, ValueError):
                pass
    return _utc(datetime.now(UTC))


def _rss(raw: bytes, source: str, actor: str | None = None) -> list[dict]:
    out = []
    for item in ET.fromstring(raw).iter("item"):
        link = (item.findtext("link") or "").strip()
        title = (item.findtext("title") or "").strip()
        if not title:
            continue
        out.append({"source": source, "uid": f"{source}:{link or title}", "ts": _iso(item.findtext("pubDate")),
                    "actor": actor, "title": title, "url": link,
                    "body": (item.findtext("description") or "").strip()[:1500]})
    return out


def bloomberg(fetch: Fetch) -> list[dict]:
    """Public headline feeds (articles themselves are paywalled)."""
    out = []
    for feed in BLOOMBERG_FEEDS:
        out += _rss(fetch(f"https://feeds.bloomberg.com/{feed}/news.rss"), "bloomberg")
    return out


def figure_news(fetch: Fetch, figures: list[dict]) -> list[dict]:
    """Google News coverage of what each public figure said, last two days."""
    out = []
    for f in figures:
        name = f["name"]
        if f.get("lang") == "pt":
            q, loc = f'"{name}" (Ibovespa OR "na bolsa" OR "ações da" OR "ações de" OR mercado) when:2d', "hl=pt-BR&gl=BR&ceid=BR:pt-419"
        else:
            q, loc = f'"{name}" (stock OR shares OR market OR tariff OR company) when:2d', "hl=en-US&gl=US&ceid=US:en"
        out += _rss(fetch(f"https://news.google.com/rss/search?q={quote_plus(q)}&{loc}"), "figure_news", name)
    return out


def _amount_strength(value: float) -> float:
    """Bigger disclosed amounts weigh more: $15k ~ 0.6, $1M ~ 0.86, $10M+ = 1."""
    return round(min(1.0, max(0.1, math.log10(max(value, 1)) / 7)), 2)


def congress(fetch: Fetch) -> list[dict]:
    """Disclosed trades by members of the US Congress (filed up to ~45 days late)."""
    data = json.loads(fetch(CONGRESS_URL))
    out = []
    for t in data.get("trades", []):
        try:
            out.append(_congress_trade(t))
        except (TypeError, ValueError):  # one odd row must not drop the whole source
            continue
    return [s for s in out if s]


def _amount(value) -> float:
    if isinstance(value, str):
        value = value.replace("$", "").replace(",", "").strip()
    return float(value or 0)


def _congress_trade(t: dict) -> dict | None:
    ticker = (t.get("ticker") or "").strip().upper()
    kind = (t.get("type") or "").lower()
    day = t.get("disclosure_date") or t.get("transaction_date")
    if not ticker or kind not in ("purchase", "sale") or not day:
        return None
    datetime.fromisoformat(str(day)[:10])   # ValueError -> row skipped
    direction = 1 if kind == "purchase" else -1
    verb = "comprou" if direction > 0 else "vendeu"
    return {
        "source": "congress", "ts": f"{str(day)[:10]}T00:00:00",
        "uid": f"congress:{t.get('member_slug')}:{ticker}:{t.get('transaction_date')}:{kind}:{t.get('amount_range')}",
        "actor": t.get("member"), "url": t.get("filing_portal") or "",
        "title": f"{t.get('member')} ({t.get('chamber')}) {verb} {ticker} {t.get('amount_range') or ''}".strip(),
        "body": f"transação em {t.get('transaction_date')}, declarada em {t.get('disclosure_date')}; {t.get('asset') or ''}",
        "hits": [{"ticker": ticker, "direction": direction,
                  "strength": _amount_strength(_amount(t.get("amount_high") or t.get("amount_low"))),
                  "reason": f"congressista {verb}"}],
    }


def _form4(xml: bytes) -> dict | None:
    root = ET.fromstring(xml)
    ticker = (root.findtext("issuer/issuerTradingSymbol") or "").strip().upper()
    owner = (root.findtext("reportingOwner/reportingOwnerId/rptOwnerName") or "").strip()
    title = (root.findtext("reportingOwner/reportingOwnerRelationship/officerTitle") or "").strip()
    bought = sold = 0.0
    for tx in root.iter("nonDerivativeTransaction"):
        code = (tx.findtext("transactionCoding/transactionCode") or "").strip()
        shares = float(tx.findtext("transactionAmounts/transactionShares/value") or 0)
        price = float(tx.findtext("transactionAmounts/transactionPricePerShare/value") or 0)
        if code == "P":
            bought += shares * price
        elif code == "S":
            sold += shares * price
    if not ticker or ticker == "NONE" or (bought == 0 and sold < 1_000_000):
        return None     # grants and small sales are routine; open-market buys and big sales say something
    return {"ticker": ticker, "owner": owner, "title": title, "bought": bought, "sold": sold}


def insiders(fetch: Fetch, limit: int = 40, pause: float = 0.15) -> list[dict]:
    """Latest SEC Form 4 filings: executives buying (or selling big) their own company's stock."""
    feed = ET.fromstring(fetch(SEC_CURRENT_FORM4.format(n=limit)))
    out, seen = [], set()
    for entry in feed.iter(f"{ATOM}entry"):
        link = entry.find(f"{ATOM}link").get("href", "") if entry.find(f"{ATOM}link") is not None else ""
        folder = link.rsplit("/", 1)[0]
        accession = folder.rsplit("/", 1)[-1]   # the same filing is listed under the issuer and the owner CIK
        if not link or accession in seen:
            continue
        seen.add(accession)
        try:
            time.sleep(pause)   # SEC fair-access: well under 10 requests per second
            files = json.loads(fetch(folder + "/index.json"))["directory"]["item"]
            xml_name = next(f["name"] for f in files if f["name"].endswith(".xml") and "index" not in f["name"])
            time.sleep(pause)
            parsed = _form4(fetch(f"{folder}/{xml_name}"))
        except Exception:  # one odd filing must not stop the sweep
            continue
        if not parsed:
            continue
        buy = parsed["bought"] > 0
        value = parsed["bought"] if buy else parsed["sold"]
        who = f"{parsed['owner']}{' (' + parsed['title'] + ')' if parsed['title'] else ''}"
        out.append({
            "source": "insider", "uid": f"insider:{accession}", "ts": _iso(entry.findtext(f"{ATOM}updated")),
            "actor": parsed["owner"], "url": link,
            "title": f"{who} {'comprou' if buy else 'vendeu'} US$ {value:,.0f} de {parsed['ticker']}",
            "body": "Form 4 (SEC)",
            "hits": [{"ticker": parsed["ticker"], "direction": 1 if buy else -1,
                      "strength": _amount_strength(value) * (1 if buy else 0.5),
                      "reason": "executivo comprou no mercado" if buy else "executivo vendeu muito"}],
        })
    return out
