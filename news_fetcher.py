import os
import hashlib
import logging
import requests
import feedparser
import psycopg2
import yfinance as yf
from datetime import datetime, timezone
from dotenv import load_dotenv
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")
FINNHUB_KEY  = os.getenv("FINNHUB_API_KEY", "")

RSS_FEEDS = [
    ("reuters_business", None, "https://feeds.reuters.com/reuters/businessNews"),
    ("cnbc_markets",     None, "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=15839069"),
    ("cnbc_economy",     None, "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=20910258"),
    ("marketwatch",      None, "https://feeds.content.dowjones.io/public/rss/mw_topstories"),
    ("yahoo_finance",    None, "https://finance.yahoo.com/news/rssindex"),
    ("investing_com",    None, "https://www.investing.com/rss/news_25.rss"),
]

YFINANCE_TICKERS = ["SPY", "QQQ", "^GSPC", "^VIX"]


# ── helpers ───────────────────────────────────────────────────────────────────

def make_hash(url: str) -> str:
    return hashlib.sha256(url.encode()).hexdigest()[:64]


def insert_articles(articles: list[dict]) -> int:
    if not articles:
        return 0
    saved = 0
    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn:
            with conn.cursor() as cur:
                for a in articles:
                    try:
                        cur.execute(
                            """
                            INSERT INTO news_raw
                                (url_hash, source, symbol, title, summary, url, published_at)
                            VALUES (%s,%s,%s,%s,%s,%s,%s)
                            ON CONFLICT (url_hash) DO NOTHING
                            """,
                            (
                                make_hash(a["url"]),
                                a["source"],
                                a.get("symbol"),
                                a["title"][:500],
                                (a.get("summary") or "")[:2000],
                                a["url"][:1000],
                                a.get("published_at"),
                            ),
                        )
                        if cur.rowcount:
                            saved += 1
                    except Exception as e:
                        log.warning("Insert hatası: %s | %s", e, a.get("title", "")[:60])
    finally:
        conn.close()
    return saved


# ── fetchers ──────────────────────────────────────────────────────────────────

def fetch_rss() -> list[dict]:
    articles = []
    for source, symbol, url in RSS_FEEDS:
        try:
            feed  = feedparser.parse(url)
            count = 0
            for entry in feed.entries[:25]:
                title = entry.get("title", "").strip()
                link  = entry.get("link", url)
                if not title or not link:
                    continue
                pub = None
                if hasattr(entry, "published_parsed") and entry.published_parsed:
                    pub = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
                articles.append({
                    "source":       source,
                    "symbol":       symbol,
                    "title":        title,
                    "summary":      entry.get("summary", "").strip(),
                    "url":          link,
                    "published_at": pub,
                })
                count += 1
            log.info("RSS [%s]: %d haber", source, count)
        except Exception as e:
            log.warning("RSS hatası [%s]: %s", source, e)
    return articles


def fetch_yfinance() -> list[dict]:
    articles = []
    for ticker in YFINANCE_TICKERS:
        try:
            t     = yf.Ticker(ticker)
            news  = t.news or []
            count = 0
            for item in news[:15]:
                content = item.get("content", {}) or {}
                title   = content.get("title") or item.get("title", "")
                url     = (content.get("canonicalUrl") or {}).get("url") or item.get("link", "")
                summary = content.get("summary") or ""
                if not title or not url:
                    continue
                pub_ts = item.get("providerPublishTime")
                pub    = datetime.fromtimestamp(pub_ts, tz=timezone.utc) if pub_ts else None
                articles.append({
                    "source":       f"yfinance_{ticker.lower().replace('^','')}",
                    "symbol":       ticker if ticker in ["SPY", "QQQ"] else None,
                    "title":        title.strip(),
                    "summary":      summary.strip(),
                    "url":          url,
                    "published_at": pub,
                })
                count += 1
            log.info("yfinance [%s]: %d haber", ticker, count)
        except Exception as e:
            log.warning("yfinance hatası [%s]: %s", ticker, e)
    return articles


def fetch_finnhub() -> list[dict]:
    if not FINNHUB_KEY:
        return []
    articles = []
    try:
        resp = requests.get(
            "https://finnhub.io/api/v1/news",
            params={"category": "general", "token": FINNHUB_KEY},
            timeout=10,
        )
        resp.raise_for_status()
        for item in resp.json()[:30]:
            url = item.get("url", "")
            if not url:
                continue
            pub = datetime.fromtimestamp(item["datetime"], tz=timezone.utc) if item.get("datetime") else None
            articles.append({
                "source":       "finnhub",
                "symbol":       None,
                "title":        item.get("headline", "").strip(),
                "summary":      item.get("summary", "").strip(),
                "url":          url,
                "published_at": pub,
            })
        log.info("Finnhub: %d haber", len(articles))
    except Exception as e:
        log.warning("Finnhub hatası: %s", e)
    return articles


# ── main job ──────────────────────────────────────────────────────────────────

def run_fetch():
    log.info("══════ Haber çekme başlıyor ══════")
    all_articles: list[dict] = []
    all_articles += fetch_rss()
    all_articles += fetch_yfinance()
    all_articles += fetch_finnhub()
    log.info("Toplam çekilen: %d haber", len(all_articles))
    saved = insert_articles(all_articles)
    log.info("Yeni kaydedilen: %d | Tekrar (atlandı): %d", saved, len(all_articles) - saved)
    log.info("══════ Tamamlandı ══════")


if __name__ == "__main__":
    run_fetch()  # Başlangıçta hemen bir kez çalıştır

    scheduler = BlockingScheduler(timezone="Europe/Istanbul")

    # Hafta içi 15:00–24:00 → her 10 dakikada bir
    scheduler.add_job(
        run_fetch,
        CronTrigger(day_of_week="mon-fri", hour="15-23", minute="*/10", timezone="Europe/Istanbul"),
        id="news_fetch_market",
    )

    # Hafta içi 00:00–15:00 → her 30 dakikada bir
    scheduler.add_job(
        run_fetch,
        CronTrigger(day_of_week="mon-fri", hour="0-14", minute="*/30", timezone="Europe/Istanbul"),
        id="news_fetch_offhours",
    )

    # Hafta sonu → her 60 dakikada bir
    scheduler.add_job(
        run_fetch,
        CronTrigger(day_of_week="sat,sun", minute="0", timezone="Europe/Istanbul"),
        id="news_fetch_weekend",
    )

    log.info("Zamanlayıcı aktif:")
    log.info("  Hafta içi 15:00–24:00 → her 10 dakikada bir")
    log.info("  Hafta içi 00:00–15:00 → her 30 dakikada bir")
    log.info("  Hafta sonu            → her 60 dakikada bir")

    try:
        scheduler.start()
    except KeyboardInterrupt:
        log.info("Durduruluyor...")