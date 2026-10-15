"""Скачивание файла с серверов хранения Яндекса (общая часть для API и раздела «Фото»).

Ссылка Яндекса (downloader.disk.yandex.ru) ведёт редиректом на конкретный сервер
хранения (sNNN.storage.yandex.net). С октября 2026 часть таких серверов не отвечает
ни API, ни веб-версии; файлы при этом не удалены. Поэтому редирект разбирается вручную:
- сервер, который недавно молчал, пропускается сразу;
- новый молчащий сервер распознаётся за STORAGE_TIMEOUT секунд, а не за минуты;
- файл в таком случае откладывается (StorageDown), а не считается ошибкой.
"""
import os
import threading
import time
from urllib.parse import urljoin, urlsplit

from .http_util import ApiError, NETWORK_ERRORS, RateLimited, session

STORAGE_TIMEOUT = float(os.environ.get("YA2GD_STORAGE_TIMEOUT", "10"))
DEAD_HOST_TTL = float(os.environ.get("YA2GD_DEAD_HOST_TTL", "1800"))
STREAM_READ_TIMEOUT = float(os.environ.get("YA2GD_STREAM_TIMEOUT", "30"))
_REDIRECTS = (301, 302, 303, 307, 308)
_PAGE_TYPES = ("text/html", "application/json")


class StorageDown(ApiError):
    """Сервер хранения Яндекса не отвечает — файл отложить, попытку не тратить."""

    def __init__(self, host, msg):
        super().__init__(msg, retryable=False)
        self.host = host


class DeadHosts:
    def __init__(self):
        self._until = {}
        self._lock = threading.Lock()

    def mark(self, host):
        if host:
            with self._lock:
                self._until[host] = time.time() + DEAD_HOST_TTL

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
        r = s.get(url, headers=headers, stream=True, allow_redirects=False, timeout=(30, 60))
        hops = 0
        while r.status_code in _REDIRECTS and r.headers.get("Location") and hops < 5:
            loc = urljoin(r.url, r.headers["Location"])
            r.close()
            host = urlsplit(loc).hostname or ""
            if DEAD.is_dead(host):
                raise StorageDown(host, f"сервер хранения Яндекса {host} недавно не отвечал — файл отложен")
            # подключение — STORAGE_TIMEOUT; молчание дольше STREAM_READ_TIMEOUT посреди потока — обрыв
            r = s.get(loc, headers=headers, stream=True, allow_redirects=False,
                      timeout=(STORAGE_TIMEOUT, STREAM_READ_TIMEOUT))
            hops += 1
    except StorageDown:
        raise
    except NETWORK_ERRORS as e:
        if host:
            DEAD.mark(host)
            raise StorageDown(host, f"сервер хранения Яндекса {host} не отвечает ({type(e).__name__}) — "
                                    f"файл отложен, повтор через {int(DEAD_HOST_TTL // 60)} мин")
        raise ApiError(f"Яндекс: сеть при получении файла: {e}", retryable=True)

    code = r.status_code
    if code in (200, 206):
        ctype = (r.headers.get("Content-Type") or "").lower()
        if reject_pages and any(t in ctype for t in _PAGE_TYPES):
            snippet = r.raw.read(300)
            r.close()
            raise ApiError(f"Яндекс вместо файла прислал {ctype.split(';')[0]}: "
                           f"{snippet[:200].decode('utf-8', 'replace')!r}", retryable=True)
        return r, (offset if code == 206 else 0)
    text = ""
    try:
        text = r.text[:300]
    except Exception:  # noqa
        pass
    r.close()
    if code == 429:
        raise RateLimited("Яндекс: слишком частые скачивания (429)", status=429, retryable=True)
    if code == 404:
        raise ApiError("Яндекс: файл не найден при скачивании (удалён?)", status=404)
    if code == 416:
        raise ApiError("Яндекс: неверный диапазон (файл изменился?)", status=416)
    if code >= 500 and host:
        raise ApiError(f"Сервер хранения {host}: HTTP {code}", status=code, retryable=True)
    raise ApiError(f"Яндекс: скачивание HTTP {code}: {text}", status=code,
                   retryable=code >= 500 or code in (401, 403, 410))

