#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════╗
║              ARACHNE WEB SCRAPER FRAMEWORK v2.4.1            ║
║──────────────────────────────────────────────────────────────║
║  Single-file edition — everything in one place.              ║
║                                                              ║
║  USAGE:                                                      ║
║    python scraper.py                        # runs all demos ║
║    python scraper.py quotes                 # quotes demo    ║
║    python scraper.py books                  # books demo     ║
║    python scraper.py hacker_news            # HN demo        ║
║    python scraper.py custom                 # your spider    ║
║                                                              ║
║  DEPENDENCIES:  pip install requests lxml                    ║
╚══════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations

# ─────────────────────────────────────────────────────────────────────────────
# STDLIB
# ─────────────────────────────────────────────────────────────────────────────
import asyncio
import csv
import hashlib
import json
import logging
import random
import sys
import time
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterator, Optional, Type
from urllib.parse import urljoin, urlparse

import requests as _requests
from lxml.html import fromstring

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING SETUP
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("arachne")


# ══════════════════════════════════════════════════════════════════════════════
#  1. REQUEST / RESPONSE
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class Request:
    """Represents a single HTTP request travelling through the pipeline."""
    url: str
    method: str = "GET"
    headers: dict[str, str] = field(default_factory=dict)
    body: Optional[bytes] = None
    depth: int = 0
    meta: dict[str, Any] = field(default_factory=dict)
    callback: Optional[Callable] = None
    errback: Optional[Callable] = None
    priority: int = 0
    dont_filter: bool = False
    retry_count: int = 0
    max_retries: int = 3

    def __hash__(self):
        return hash(self.url)

    def __lt__(self, other: "Request"):
        return self.priority > other.priority  # higher priority = dequeued first


@dataclass
class Response:
    """Represents an HTTP response with parsing helpers."""
    url: str
    status: int
    headers: dict[str, str]
    body: bytes
    request: Request
    elapsed: float = 0.0

    @property
    def text(self) -> str:
        ct = self.headers.get("content-type", "")
        enc = ct.split("charset=")[-1].split(";")[0].strip() if "charset=" in ct else "utf-8"
        return self.body.decode(enc, errors="replace")

    @property
    def ok(self) -> bool:
        return 200 <= self.status < 300

    def css(self, selector: str):
        """Return lxml elements matching a CSS selector (requires cssselect package)."""
        try:
            return fromstring(self.body).cssselect(selector)
        except ImportError:
            raise ImportError(
                "CSS selectors require the 'cssselect' package.\n"
                "Install it with:  pip install cssselect\n"
                "Or use response.xpath() instead."
            )

    def xpath(self, expr: str):
        """Return lxml elements matching an XPath expression."""
        return fromstring(self.body).xpath(expr)

    def __repr__(self):
        return f"<Response [{self.status}] {self.url}>"


# ══════════════════════════════════════════════════════════════════════════════
#  2. RATE LIMITER
# ══════════════════════════════════════════════════════════════════════════════

class RateLimiter:
    """
    Per-domain token bucket with jitter.
    Automatically backs off when a 429 is received.
    """

    def __init__(self, delay: float = 1.0, jitter: float = 0.5) -> None:
        self.delay = delay
        self.jitter = jitter
        self._last: dict[str, float] = defaultdict(float)
        self._locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

    async def wait(self, domain: str) -> None:
        async with self._locks[domain]:
            now = time.monotonic()
            elapsed = now - self._last[domain]
            sleep_for = max(0.0, self.delay + random.uniform(-self.jitter, self.jitter) - elapsed)
            if sleep_for > 0:
                await asyncio.sleep(sleep_for)
            self._last[domain] = time.monotonic()

    def backoff(self, domain: str, factor: float = 2.0) -> None:
        """Double the delay for a domain (called on 429/503)."""
        self.delay = min(self.delay * factor, 60.0)
        logger.warning(f"Rate limiter: backoff applied for {domain} — new delay={self.delay:.1f}s")


# ══════════════════════════════════════════════════════════════════════════════
#  3. SPIDER BASE CLASS
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class SpiderConfig:
    """All tunable parameters for a spider run."""
    name: str = "spider"
    start_urls: list[str] = field(default_factory=list)
    allowed_domains: list[str] = field(default_factory=list)
    max_depth: int = 2
    max_pages: int = 100
    concurrency: int = 5
    delay: float = 1.0
    jitter: float = 0.5
    follow_redirects: bool = True
    timeout: float = 30.0


class Spider(ABC):
    """
    Base class for all spiders.

    Subclass this, set `config`, and implement `parse()`.
    Optionally override `on_start()` / `on_finish()`.
    """

    config: SpiderConfig = SpiderConfig()

    def __init__(self) -> None:
        self._seen: set[str] = set()
        self._rate_limiter = RateLimiter(
            delay=self.config.delay,
            jitter=self.config.jitter,
        )
        self.stats: dict[str, int] = {
            "pages_crawled":    0,
            "items_scraped":    0,
            "errors":           0,
            "duplicates_skipped": 0,
        }

    def start_requests(self) -> Iterator[Request]:
        for url in self.config.start_urls:
            yield Request(url=url, depth=0, callback=self.parse)

    @abstractmethod
    def parse(self, response: Response) -> Iterator[Any]:
        """Yield item dicts and/or Request objects."""

    def on_start(self) -> None:
        """Hook called once before crawling begins."""

    def on_finish(self) -> None:
        """Hook called once after crawling ends."""

    # ── Helpers ───────────────────────────────────────────────────────────

    def make_url(self, base: str, href: str) -> str:
        return urljoin(base, href)

    def is_allowed(self, url: str) -> bool:
        if not self.config.allowed_domains:
            return True
        host = urlparse(url).netloc
        return any(host == d or host.endswith("." + d) for d in self.config.allowed_domains)

    def fingerprint(self, url: str) -> str:
        return hashlib.sha1(url.encode()).hexdigest()

    def is_seen(self, url: str) -> bool:
        fp = self.fingerprint(url)
        if fp in self._seen:
            return True
        self._seen.add(fp)
        return False

    async def wait(self, domain: str) -> None:
        await self._rate_limiter.wait(domain)


# ══════════════════════════════════════════════════════════════════════════════
#  4. MIDDLEWARE
# ══════════════════════════════════════════════════════════════════════════════

class BaseMiddleware:
    priority: int = 500

    async def process_request(self, request: Request) -> Optional[Request]:
        return request

    async def process_response(self, response: Response) -> Optional[Response]:
        return response


class UserAgentMiddleware(BaseMiddleware):
    """Rotates a realistic User-Agent on every request."""
    priority = 100
    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3.1 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64; rv:123.0) Gecko/20100101 Firefox/123.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 Edg/121.0.0.0",
    ]
    async def process_request(self, request: Request) -> Request:
        request.headers.setdefault("User-Agent", random.choice(self.USER_AGENTS))
        return request


class HeadersMiddleware(BaseMiddleware):
    """Injects browser-like headers to reduce bot detection."""
    priority = 110
    async def process_request(self, request: Request) -> Request:
        request.headers.setdefault("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
        request.headers.setdefault("Accept-Language", "en-US,en;q=0.9")
        request.headers.setdefault("Accept-Encoding", "gzip, deflate, br")
        request.headers.setdefault("Connection", "keep-alive")
        request.headers.setdefault("Upgrade-Insecure-Requests", "1")
        return request


class LoggingMiddleware(BaseMiddleware):
    """Logs every request and response."""
    priority = 900
    async def process_request(self, request: Request) -> Request:
        logger.debug(f"→ {request.method} {request.url}  (depth={request.depth})")
        return request
    async def process_response(self, response: Response) -> Response:
        level = logging.DEBUG if response.ok else logging.WARNING
        logger.log(level, f"← {response.status} {response.url}  ({len(response.body)}B, {response.elapsed:.2f}s)")
        return response


class MiddlewareManager:
    """Ordered stack of middleware — sorted by priority before use."""

    def __init__(self, middlewares: list[BaseMiddleware] | None = None) -> None:
        self._mws = sorted(
            middlewares or [UserAgentMiddleware(), HeadersMiddleware(), LoggingMiddleware()],
            key=lambda m: m.priority,
        )

    def add(self, mw: BaseMiddleware) -> "MiddlewareManager":
        self._mws.append(mw)
        self._mws.sort(key=lambda m: m.priority)
        return self

    async def process_request(self, req: Request) -> Optional[Request]:
        for mw in self._mws:
            req = await mw.process_request(req)
            if req is None:
                return None
        return req

    async def process_response(self, resp: Response) -> Optional[Response]:
        for mw in reversed(self._mws):
            resp = await mw.process_response(resp)
            if resp is None:
                return None
        return resp


# ══════════════════════════════════════════════════════════════════════════════
#  5. PIPELINE
# ══════════════════════════════════════════════════════════════════════════════

class BasePipeline:
    priority: int = 500
    async def open(self) -> None:   pass
    async def close(self) -> None:  pass
    async def process_item(self, item: dict, spider: Spider) -> dict | None:
        return item


class ValidationPipeline(BasePipeline):
    """Drop items missing any required field."""
    priority = 100
    def __init__(self, required_fields: list[str] | None = None) -> None:
        self.required_fields = required_fields or []
    async def process_item(self, item, spider):
        for f in self.required_fields:
            if not item.get(f):
                logger.warning(f"[validation] Dropped — missing '{f}': {item}")
                return None
        return item


class DeduplicationPipeline(BasePipeline):
    """Drop items whose fingerprint was already seen this run."""
    priority = 200
    def __init__(self, fields: list[str] | None = None) -> None:
        self.fields = fields
        self._seen: set[str] = set()
    async def process_item(self, item, spider):
        data = {k: item.get(k) for k in self.fields} if self.fields else item
        fp = hashlib.sha1(json.dumps(data, sort_keys=True).encode()).hexdigest()
        if fp in self._seen:
            return None
        self._seen.add(fp)
        return item


class ConsolePipeline(BasePipeline):
    """Pretty-print every item to stdout."""
    priority = 900
    async def process_item(self, item, spider):
        print(json.dumps(item, ensure_ascii=False, indent=2))
        return item


class JsonPipeline(BasePipeline):
    """Stream items to a newline-delimited JSON (.jsonl) file."""
    priority = 800
    def __init__(self, path: str = "output.jsonl") -> None:
        self.path = Path(path)
        self._fh = None
    async def open(self):
        self._fh = self.path.open("w", encoding="utf-8")
        logger.info(f"[json] Writing to {self.path}")
    async def close(self):
        if self._fh: self._fh.close()
    async def process_item(self, item, spider):
        self._fh.write(json.dumps(item, ensure_ascii=False) + "\n")
        self._fh.flush()
        return item


class CsvPipeline(BasePipeline):
    """Stream items to a CSV file (headers inferred from first item)."""
    priority = 810
    def __init__(self, path: str = "output.csv") -> None:
        self.path = Path(path)
        self._fh = None
        self._writer = None
    async def open(self):
        self._fh = self.path.open("w", newline="", encoding="utf-8")
        logger.info(f"[csv] Writing to {self.path}")
    async def close(self):
        if self._fh: self._fh.close()
    async def process_item(self, item, spider):
        if self._writer is None:
            self._writer = csv.DictWriter(self._fh, fieldnames=list(item.keys()))
            self._writer.writeheader()
        self._writer.writerow(item)
        self._fh.flush()
        return item


class PipelineManager:
    """Ordered chain of pipeline stages."""

    def __init__(self, stages: list[BasePipeline] | None = None) -> None:
        self._stages = sorted(stages or [ConsolePipeline()], key=lambda p: p.priority)

    def add(self, stage: BasePipeline) -> "PipelineManager":
        self._stages.append(stage)
        self._stages.sort(key=lambda p: p.priority)
        return self

    async def open(self):
        for s in self._stages: await s.open()

    async def close(self):
        for s in self._stages: await s.close()

    async def process(self, item: dict, spider: Spider) -> dict | None:
        for stage in self._stages:
            if item is None: break
            item = await stage.process_item(item, spider)
        return item


# ══════════════════════════════════════════════════════════════════════════════
#  6. CRAWLER
# ══════════════════════════════════════════════════════════════════════════════

class Crawler:
    """
    Async crawl engine.
    Manages the request queue, worker pool, retries, and backoff.
    """

    def __init__(self, spider: Spider, middleware: MiddlewareManager, pipeline: PipelineManager) -> None:
        self.spider = spider
        self.middleware = middleware
        self.pipeline = pipeline
        self._queue: asyncio.PriorityQueue = asyncio.PriorityQueue()
        self._shutdown = False

    async def crawl(self) -> dict[str, Any]:
        logger.info(f"[{self.spider.config.name}] Starting crawl")
        self.spider.on_start()
        await self.pipeline.open()

        for req in self.spider.start_requests():
            await self._enqueue(req)

        workers = [asyncio.create_task(self._worker(i)) for i in range(self.spider.config.concurrency)]
        await self._queue.join()
        self._shutdown = True
        for w in workers: w.cancel()
        await asyncio.gather(*workers, return_exceptions=True)

        await self.pipeline.close()
        self.spider.on_finish()
        logger.info(f"[{self.spider.config.name}] Done — {self.spider.stats}")
        return self.spider.stats

    async def _enqueue(self, req: Request) -> None:
        if self.spider.stats["pages_crawled"] >= self.spider.config.max_pages:
            return
        if not req.dont_filter and self.spider.is_seen(req.url):
            self.spider.stats["duplicates_skipped"] += 1
            return
        if not self.spider.is_allowed(req.url):
            return
        await self._queue.put(req)

    async def _worker(self, wid: int) -> None:
        while not self._shutdown:
            try:
                req = await asyncio.wait_for(self._queue.get(), timeout=2.0)
            except asyncio.TimeoutError:
                continue
            try:
                await self._process(req)
            except Exception as exc:
                logger.error(f"Worker {wid} error: {exc}")
                self.spider.stats["errors"] += 1
            finally:
                self._queue.task_done()

    async def _process(self, req: Request) -> None:
        # Guard: stop processing once max_pages reached
        if self.spider.stats["pages_crawled"] >= self.spider.config.max_pages:
            return

        domain = urlparse(req.url).netloc
        req = await self.middleware.process_request(req)
        if req is None: return

        await self.spider.wait(domain)

        response = await self._fetch(req)
        if response is None: return

        # Handle 429 here so it works even when _fetch is mocked in tests
        if response.status == 429:
            logger.warning(f"429 on {req.url} — backing off")
            self.spider._rate_limiter.backoff(domain)
            if req.retry_count < req.max_retries:
                req.retry_count += 1
                req.dont_filter = True
                await self._enqueue(req)
            return

        self.spider.stats["pages_crawled"] += 1
        response = await self.middleware.process_response(response)
        if response is None: return

        try:
            for result in self.spider.parse(response):
                if isinstance(result, Request):
                    if result.depth <= self.spider.config.max_depth:
                        await self._enqueue(result)
                elif isinstance(result, dict):
                    await self.pipeline.process(result, self.spider)
                    self.spider.stats["items_scraped"] += 1
        except Exception as exc:
            logger.warning(f"Parse error on {req.url}: {exc}")
            self.spider.stats["errors"] += 1

    async def _fetch(self, req: Request) -> Response | None:
        loop = asyncio.get_event_loop()
        try:
            t0 = time.monotonic()
            resp = await loop.run_in_executor(None, lambda: _requests.request(
                method=req.method, url=req.url, headers=req.headers,
                data=req.body, timeout=self.spider.config.timeout,
                allow_redirects=self.spider.config.follow_redirects,
            ))
            elapsed = time.monotonic() - t0
            return Response(
                url=resp.url, status=resp.status_code,
                headers=dict(resp.headers), body=resp.content,
                request=req, elapsed=elapsed,
            )
        except Exception as exc:
            logger.error(f"Fetch failed [{req.url}]: {exc}")
            self.spider.stats["errors"] += 1
            if req.retry_count < req.max_retries:
                req.retry_count += 1
                req.dont_filter = True
                await asyncio.sleep(2 ** req.retry_count)
                await self._enqueue(req)
            return None


# ══════════════════════════════════════════════════════════════════════════════
#  7. RUNNER  (public API)
# ══════════════════════════════════════════════════════════════════════════════

class Runner:
    """
    Fluent builder — configure and launch a crawl in a few lines.

    Example:
        stats = (
            Runner()
            .configure(SpiderConfig(name="demo", start_urls=["https://..."]))
            .add_pipeline(JsonPipeline("out.jsonl"))
            .run(MySpider)
        )
    """

    def __init__(self) -> None:
        self._config: SpiderConfig | None = None
        self._middlewares: list[BaseMiddleware] = []
        self._pipelines: list[BasePipeline] = []

    def configure(self, config: SpiderConfig) -> "Runner":
        self._config = config
        return self

    def add_middleware(self, mw: BaseMiddleware) -> "Runner":
        self._middlewares.append(mw)
        return self

    def add_pipeline(self, pipeline: BasePipeline) -> "Runner":
        self._pipelines.append(pipeline)
        return self

    def run(self, spider_class: Type[Spider]) -> dict:
        spider = spider_class()
        if self._config:
            spider.config = self._config
        middleware = MiddlewareManager(self._middlewares or None)
        pipeline   = PipelineManager(self._pipelines or None)
        crawler    = Crawler(spider, middleware, pipeline)
        return asyncio.run(crawler.crawl())


# ══════════════════════════════════════════════════════════════════════════════
#  8. EXAMPLE SPIDERS
# ══════════════════════════════════════════════════════════════════════════════

class QuotesSpider(Spider):
    """Scrapes quotes.toscrape.com — a public sandbox site."""
    config = SpiderConfig(
        name="quotes",
        start_urls=["http://quotes.toscrape.com/"],
        allowed_domains=["quotes.toscrape.com"],
        max_depth=5, max_pages=20, concurrency=2, delay=0.5,
    )
    def parse(self, response: Response) -> Iterator[Any]:
        doc = fromstring(response.body)
        for el in doc.xpath('//div[contains(@class,"quote")]'):
            texts   = el.xpath('.//span[contains(@class,"text")]')
            authors = el.xpath('.//small[contains(@class,"author")]')
            tags    = el.xpath('.//a[contains(@class,"tag")]')
            yield {
                "text":   texts[0].text_content().strip()   if texts   else None,
                "author": authors[0].text_content().strip() if authors else None,
                "tags":   [t.text_content().strip() for t in tags],
                "url":    response.url,
            }
        for link in doc.xpath('//li[contains(@class,"next")]/a'):
            yield Request(url=self.make_url(response.url, link.get("href")),
                          depth=response.request.depth + 1, callback=self.parse)


class BooksSpider(Spider):
    """Scrapes books.toscrape.com — catalogue + detail pages."""
    config = SpiderConfig(
        name="books",
        start_urls=["http://books.toscrape.com/catalogue/page-1.html"],
        allowed_domains=["books.toscrape.com"],
        max_depth=3, max_pages=30, concurrency=3, delay=0.5,
    )
    RATING = {"One":1,"Two":2,"Three":3,"Four":4,"Five":5}

    def parse(self, response: Response) -> Iterator[Any]:
        doc = fromstring(response.body)
        for article in doc.xpath('//article[contains(@class,"product_pod")]'):
            links = article.xpath('.//h3/a')
            if links:
                href = links[0].get("href","").replace("../","")
                yield Request(
                    url=self.make_url("http://books.toscrape.com/catalogue/", href),
                    depth=response.request.depth + 1, callback=self.parse_book,
                )
        for link in doc.xpath('//li[contains(@class,"next")]/a'):
            yield Request(url=self.make_url(response.url, link.get("href")),
                          depth=response.request.depth + 1, callback=self.parse)

    def parse_book(self, response: Response) -> Iterator[dict]:
        doc = fromstring(response.body)
        def t(xp): els = doc.xpath(xp); return els[0].text_content().strip() if els else None
        rating_els  = doc.xpath('//p[contains(@class,"star-rating")]')
        rating_word = rating_els[0].get("class","").split()[-1] if rating_els else "Zero"
        yield {
            "title":        t('//h1'),
            "price":        t('//*[contains(@class,"price_color")]'),
            "availability": t('//*[contains(@class,"availability")]'),
            "rating":       self.RATING.get(rating_word, 0),
            "description":  t('//article[contains(@class,"product_page")]/p'),
            "url":          response.url,
        }


class HackerNewsSpider(Spider):
    """Scrapes the Hacker News front page."""
    config = SpiderConfig(
        name="hacker_news",
        start_urls=["https://news.ycombinator.com/"],
        allowed_domains=["news.ycombinator.com"],
        max_depth=1, max_pages=3, concurrency=1, delay=1.0,
    )
    def parse(self, response: Response) -> Iterator[Any]:
        doc = fromstring(response.body)
        for row in doc.xpath('//tr[contains(@class,"athing")]'):
            title_el = row.xpath('.//span[contains(@class,"titleline")]/a')
            if not title_el: continue
            item_id  = row.get("id","")
            score_el = doc.xpath(f'//*[@id="score_{item_id}"]')
            yield {
                "title":  title_el[0].text_content().strip(),
                "url":    title_el[0].get("href",""),
                "score":  score_el[0].text_content().strip() if score_el else "0 points",
                "hn_id":  item_id,
            }
        for link in doc.xpath('//a[contains(@class,"morelink")]'):
            yield Request(url=self.make_url(response.url, link.get("href")),
                          depth=response.request.depth + 1, callback=self.parse)


# ══════════════════════════════════════════════════════════════════════════════
#  9. YOUR CUSTOM SPIDER  ← edit this
# ══════════════════════════════════════════════════════════════════════════════

class CustomSpider(Spider):
    """
    ┌─────────────────────────────────────────────────────────┐
    │  EDIT THIS SPIDER to scrape your own target.            │
    │                                                         │
    │  1. Set start_urls and allowed_domains                  │
    │  2. Adjust max_depth, max_pages, delay to taste         │
    │  3. Replace the XPath expressions in parse()            │
    └─────────────────────────────────────────────────────────┘
    """
    config = SpiderConfig(
        name            = "custom",
        start_urls      = ["https://books.toscrape.com/"],  # ← change me
        allowed_domains = ["books.toscrape.com"],           # ← change me
        max_depth       = 2,
        max_pages       = 50,
        concurrency     = 3,
        delay           = 1.0,
        jitter          = 0.4,
    )

    def parse(self, response: Response) -> Iterator[Any]:
        doc = fromstring(response.body)

        # ── Extract items ─────────────────────────────────────────────
        # Replace these XPath expressions with ones matching your site:
        for item_el in doc.xpath('//article[contains(@class,"product_pod")]'):  # ← change me
            title = item_el.xpath('.//h3/a')
            price = item_el.xpath('.//*[contains(@class,"price_color")]')
            yield {
                "title":  title[0].get("title") if title else None,
                "price":  price[0].text_content().strip() if price else None,
                "source": response.url,
            }

        # ── Follow pagination ─────────────────────────────────────────
        # Replace with the XPath for your site's "next page" link:
        for link in doc.xpath('//li[contains(@class,"next")]/a'):  # ← change me
            yield Request(
                url      = self.make_url(response.url, link.get("href")),
                depth    = response.request.depth + 1,
                callback = self.parse,
            )


# ══════════════════════════════════════════════════════════════════════════════
#  10. MAIN — choose which spider to run
# ══════════════════════════════════════════════════════════════════════════════

SPIDERS = {
    "quotes":      (QuotesSpider,     "quotes_output.jsonl"),
    "books":       (BooksSpider,      "books_output.jsonl"),
    "hacker_news": (HackerNewsSpider, "hackernews_output.jsonl"),
    "custom":      (CustomSpider,     "custom_output.jsonl"),
}


def run_spider(name: str) -> None:
    spider_cls, output_file = SPIDERS[name]

    print(f"\n{'═'*60}")
    print(f"  Spider  : {name}")
    print(f"  Output  : {output_file}")
    print(f"{'═'*60}\n")

    stats = (
        Runner()
        .add_pipeline(ValidationPipeline(required_fields=["title"]))
        .add_pipeline(DeduplicationPipeline())
        .add_pipeline(JsonPipeline(output_file))
        .add_pipeline(ConsolePipeline())
        .run(spider_cls)
    )

    print(f"\n{'═'*60}")
    print(f"  ✓ pages crawled   : {stats['pages_crawled']}")
    print(f"  ✓ items scraped   : {stats['items_scraped']}")
    print(f"  ✗ errors          : {stats['errors']}")
    print(f"  ⊘ duplicates skip : {stats['duplicates_skipped']}")
    print(f"  Output saved to   : {output_file}")
    print(f"{'═'*60}\n")


if __name__ == "__main__":
    choice = sys.argv[1].lower() if len(sys.argv) > 1 else None

    if choice and choice not in SPIDERS:
        print(f"Unknown spider '{choice}'. Choose from: {list(SPIDERS)}")
        sys.exit(1)

    if choice:
        run_spider(choice)
    else:
        # Run all demos in sequence
        for name in ["quotes", "books", "hacker_news"]:
            run_spider(name)
