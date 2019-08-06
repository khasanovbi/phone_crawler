import asyncio
import logging
import re
from collections import namedtuple

import aiohttp
import uvloop
from bs4 import BeautifulSoup

log = logging.getLogger(__name__)


class Parser:
    FORBIDDEN_TAGS = ("img", "input", "script", "svg")
    RUSSIA_COUNTRY_CODE = "8"
    MOSCOW_AREA_CODE = "495"
    DIGITS_RE = re.compile(r"\d+")
    LOCAL_PHONE_LENGTH = 7

    PHONE_RE = re.compile(
        r"""
        \b
        (
            # optional international part
            (?:
                # country code
                (?:
                    [+]?[\s]?7  # seven with optional + with optional whitespace
                    |
                    8  # eight
                )
                [\s]?  # skip whitespace
                # region
                (?:
                    \([\s]?\d{3}[\s]?\)  # wrapped with parenthesis
                    |
                    \-[\s]?\d{3}[\s]?\-  # wrapped with hyphens
                    |
                    \d{3}  # without wrappers
                )
                [\s]?  # skip whitespace
            )?
            # local phone part
            (?:
                \d{3}  # first three digits
                # last four digits
                (?:
                    [\s]?\-[\s]?\d{2}[\s]?\-[\s]?\d{2}  # divided with hyphens
                    |
                    [\s]?\d{2}[\s]?\d{2}  # divided with optional whitespaces
                )
            )
        )
        \b
        """,
        re.VERBOSE,
    )

    def _clean_page(self, page):
        soup = BeautifulSoup(page, "html.parser")
        for forbidden_tag in self.FORBIDDEN_TAGS:
            tags = soup.find_all(forbidden_tag)
            for tag in tags:
                tag.extract()
        return str(soup)

    def _normalize(self, raw_phone):
        digits_str = "".join(self.DIGITS_RE.findall(raw_phone))
        if len(digits_str) == self.LOCAL_PHONE_LENGTH:
            return f"{self.RUSSIA_COUNTRY_CODE}{self.MOSCOW_AREA_CODE}{digits_str}"

        if digits_str.startswith(self.RUSSIA_COUNTRY_CODE):
            return digits_str

        return f"{self.RUSSIA_COUNTRY_CODE}{digits_str[1:]}"

    def parse(self, page):
        page = self._clean_page(page)
        raw_phones = set(self.PHONE_RE.findall(page))
        log.debug("got raw phones: phones=%s", raw_phones)
        return {self._normalize(raw_phone) for raw_phone in raw_phones}


class WorkerGroup:
    def __init__(self):
        self._workers = []

    def add(self, worker):
        self._workers.append(worker)

    def add_many(self, workers):
        self._workers.extend(workers)

    async def clean(self):
        for worker in self._workers:
            worker.cancel()

        for worker in self._workers:
            try:
                await worker
            except asyncio.CancelledError:
                pass
            except Exception:
                log.exception("unexpected error at worker group cancel")
        self._workers.clear()


CrawlResult = namedtuple("CrawlResult", ("url", "phones"))


class PhoneCrawler:
    def __init__(
        self, result_queue, concurrency=100, timeout=aiohttp.ClientTimeout(total=5 * 60)
    ):
        self._session = None
        self._concurrency = concurrency
        self._timeout = timeout

        self._queue = asyncio.Queue(maxsize=concurrency)
        self._parser = Parser()
        self._is_run = False
        self._result_queue = result_queue
        self._worker_group = WorkerGroup()

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop()

    async def _fetch_page(self, session, url):
        log.info("fetch page: url='%s'", url)
        try:
            async with session.get(url) as response:
                # TODO: add retries and save error?
                if response.status != 200:
                    log.error(
                        "invalid response status: url='%s', status=%d",
                        url,
                        response.status,
                    )
                    return
                body = await response.text()
                phones = self._parser.parse(body)
                log.info("parsed phones: url='%s', phones=%s", url, phones)
                await self._result_queue.put(CrawlResult(url=url, phones=phones))
        except aiohttp.ClientError as e:
            log.error("client error at request: url='%s', error='%s'", url, e)
        except Exception:
            log.exception("unexpected exception at request: url='%s'", url)

    async def _worker(self, semaphore):
        url = await self._queue.get()
        try:
            async with semaphore:
                await self._fetch_page(self._session, url)
        finally:
            self._queue.task_done()

    async def start(self):
        if self._is_run:
            raise RuntimeError("crawler already started")

        # Use single session to reuse keepalive connections
        self._session = aiohttp.ClientSession(timeout=self._timeout)

        semaphore = asyncio.Semaphore(self._concurrency)
        self._worker_group.add_many(
            asyncio.ensure_future(self._worker(semaphore))
            for _ in range(self._concurrency)
        )

        self._is_run = True

    async def stop(self):
        if not self._is_run:
            return

        await self._queue.join()
        await self.cancel()

    async def cancel(self):
        if not self._is_run:
            return
        await asyncio.wait((self._session.close(), self._worker_group.clean()))

    async def feed_url(self, url):
        if not self._is_run:
            raise RuntimeError("crawler not started")
        await self._queue.put(url)

    async def feed_urls(self, urls):
        for url in urls:
            await self.feed_url(url)


async def main(urls):
    # can set max_size to prevent many queries to db.
    result_queue = asyncio.Queue()
    # use crawler as asynchronous pipeline
    async with PhoneCrawler(result_queue) as phone_crawler:
        await phone_crawler.feed_urls(urls)

    print(result_queue)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    urls = ["https://hands.ru/company/about", "https://repetitors.info/"]
    uvloop.install()
    asyncio.run(main(urls))
