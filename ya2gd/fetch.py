"""Скачивание файла с серверов хранения Яндекса (общая часть для API и веб-версии).

Ссылка Яндекса (downloader.disk.yandex.ru) ведёт редиректом на конкретный сервер
хранения (sNNN.storage.yandex.net). С октября 2026 часть таких серверов не отвечает
ни API, ни веб-версии. Главное правило: недоступный файл должен стоить потоку секунды, а не минуты.
- сервер, который недавно молчал, пропускается сразу;
- молчащий сервер распознаётся за STORAGE_TIMEOUT секунд, долгий ответ «первого» сервера —
  за FRONT_READ_TIMEOUT секунд (504 от Яндекса обычно приходит только через минуту);
- любой отказ отдать именно этот файл (молчит, 502/504/410/403, страница вместо файла) — StorageDown:
  файл уходит в «Отложенные», повторить его можно вручную;
- 503 «перегружен» — Overloaded: это сигнал притормозить, а не беда файла.
"""
import os
import threading
import time
from urllib.parse import urljoin, urlsplit

from .http_util import ApiError, NETWORK_ERRORS, RateLimited, session

STORAGE_TIMEOUT = float(os.environ.get("YA2GD_STORAGE_TIMEOUT", "10"))
FRONT_READ_TIMEOUT = float(os.environ.get("YA2GD_FRONT_TIMEOUT", "25"))
DEAD_HOST_TTL = float(os.environ.get("YA2GD_DEAD_HOST_TTL", "1800"))
STREAM_READ_TIMEOUT = float(os.environ.get("YA2GD_STREAM_TIMEOUT", "25"))
_REDIRECTS = (301, 302, 303, 307, 308)
_PAGE_TYPES = ("text/html", "application/json")
YANDEX_HOSTS = (".yandex.ru", ".yandex.net", ".yandex.com", ".yandex-team.ru")


class StorageDown(ApiError):
    """Яндекс сейчас не отдаёт этот файл — отложить (в «Отложенные»), попытку не тратить.

    network=True — это была сетевая ошибка, а не ответ сервера: прежде чем винить Яндекс,
    стоит проверить, есть ли интернет у самого компьютера."""

    def __init__(self, host, msg, network=False):
        super().__init__(msg, retryable=False, service="yandex")
        self.host = host
        self.network = network


class Overloaded(ApiError):
    """Яндекс ответил «перегружен» (503): притормозить; файл повторить позже."""

    def __init__(self, msg):
        super().__init__(msg, status=503, retryable=True, service="yandex")


class DeadHosts:
    def __init__(self):
        self._until = {}
        self._lock = threading.Lock()

    def mark(self, host):
        if host:
            with self._lock:
                self._until[host] = time.time() + DEAD_HOST_TTL

    def forget(self, host):
        with self._lock:
            self._until.pop(host, None)

    def is_dead(self, host):
        with self._lock:
            u = self._until.get(host)
            if u is None:
                return False
            if u <= time.time():
                del self._until[host]
                return False
            return True

    def active(self):
        now = time.time()
        with self._lock:
            return sorted(h for h, u in self._until.items() if u > now)

    def clear(self):
        with self._lock:
            self._until.clear()


DEAD = DeadHosts()


def is_yandex_host(url):
    host = (urlsplit(url).hostname or "").lower()
    return host == "localhost" or host.startswith("127.") or any(host.endswith(d) for d in YANDEX_HOSTS)


def open_download(url, offset=0, sess=None, reject_pages=False):
    """Открывает поток файла с байта offset. Возвращает (response, фактическое_смещение).

    reject_pages: оригинал фото/видео никогда не приходит HTML/JSON — это отказ Яндекса.
    """
    s = sess or session()
    headers = {"Accept-Encoding": "identity"}
    if offset:
        headers["Range"] = f"bytes={offset}-"
    host = ""
    try:
        r = s.get(url, headers=headers, stream=True, allow_redirects=False,
                  timeout=(STORAGE_TIMEOUT, FRONT_READ_TIMEOUT))
        hops = 0
        while r.status_code in _REDIRECTS and r.headers.get("Location") and hops < 5:
            loc = urljoin(r.url, r.headers["Location"])
            r.close()
            host = urlsplit(loc).hostname or ""
            if DEAD.is_dead(host):
                raise StorageDown(host, f"сервер хранения Яндекса {host} недавно не отвечал")
            # подключение — STORAGE_TIMEOUT; молчание дольше STREAM_READ_TIMEOUT посреди потока — обрыв
            r = s.get(loc, headers=headers, stream=True, allow_redirects=False,
                      timeout=(STORAGE_TIMEOUT, STREAM_READ_TIMEOUT))
            hops += 1
    except StorageDown:
        raise
    except NETWORK_ERRORS as e:
        if host:
            DEAD.mark(host)
            raise StorageDown(host, f"сервер хранения Яндекса {host} не отвечает ({type(e).__name__})",
                              network=True)
        raise StorageDown(None, f"Яндекс не начал отдавать файл ({type(e).__name__})", network=True)

    code = r.status_code
    if code in (200, 206):
        ctype = (r.headers.get("Content-Type") or "").lower()
        if reject_pages and any(t in ctype for t in _PAGE_TYPES):
            snippet = r.raw.read(300)
            r.close()
            raise StorageDown(host, f"Яндекс вместо файла прислал {ctype.split(';')[0]}: "
                                    f"{snippet[:120].decode('utf-8', 'replace')!r}")
        return r, (offset if code == 206 else 0)
    text = ""
    try:
        text = r.text[:200]
    except Exception:  # noqa
        pass
    r.close()
    where = f"сервер {host}" if host else "Яндекс"
    if code == 429:
        raise RateLimited("Яндекс: слишком частые скачивания (429)", status=429, retryable=True, service="yandex")
    if code == 503:
        raise Overloaded(f"{where}: перегружен (503)")
    if code == 404:
        raise ApiError("Яндекс: файл не найден при скачивании (удалён?)", status=404, service="yandex")
    if code == 416:
        raise ApiError("Яндекс: неверный диапазон (файл изменился?)", status=416, service="yandex")
    reason = {410: "файл сейчас недоступен (410)", 502: "не смог достать файл (502)",
              504: "не дождался сервера хранения (504)", 401: "отказал в доступе (401)",
              403: "отказал в доступе (403)"}.get(code, f"HTTP {code} {text[:80]!r}")
    raise StorageDown(host, f"{where}: {reason}")


def fetch_small(url, sess=None, headers=None, limit=50 * 2 ** 20):
    """Скачивает небольшой ресурс (превью) целиком: (bytes, content-type)."""
    s = sess or session()
    try:
        r = s.get(url, headers=headers or {}, timeout=(STORAGE_TIMEOUT, 20), stream=True)
    except NETWORK_ERRORS as e:
        raise ApiError(f"превью: сеть: {e}", retryable=True, service="yandex")
    try:
        if r.status_code != 200:
            raise ApiError(f"превью: HTTP {r.status_code}", status=r.status_code, service="yandex")
        data = r.raw.read(limit + 1, decode_content=True)
        if len(data) > limit:
            raise ApiError("превью слишком большое", service="yandex")
        return data, (r.headers.get("Content-Type") or "").split(";")[0].strip().lower()
    except NETWORK_ERRORS as e:
        raise ApiError(f"превью: обрыв: {e}", retryable=True, service="yandex")
    finally:
        r.close()
