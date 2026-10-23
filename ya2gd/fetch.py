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

import requests

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
    """Серверы хранения, которые недавно молчали.

    Сервер считается «молчащим» только после нескольких отказов на РАЗНЫХ файлах за 10 минут
    (2 — если к нему не удалось даже подключиться, 3 — если подключились, но он не ответил):
    одна заминка — беда одного файла, а не сервера. Файлы с «молчащего» сервера не пропускаются
    без попытки, а пробуются с короткими таймаутами (PROBE_TIMEOUT): если сервер ожил — сразу видно."""

    WINDOW = 600.0

    def __init__(self):
        self._until = {}
        self._strikes = {}
        self._lock = threading.Lock()

    def strike(self, host, connect_failed):
        """Отказ сервера на очередном файле. True — сервер теперь считается молчащим."""
        if not host:
            return False
        now = time.time()
        with self._lock:
            hits = [(t, c) for t, c in self._strikes.get(host, []) if t > now - self.WINDOW]
            hits.append((now, connect_failed))
            self._strikes[host] = hits
            need = 2 if any(c for _, c in hits) else 3
            if len(hits) >= need:
                self._until[host] = now + DEAD_HOST_TTL
                return True
            return False

    def mark(self, host):
        if host:
            with self._lock:
                self._until[host] = time.time() + DEAD_HOST_TTL

    def forget(self, host):
        with self._lock:
            self._until.pop(host, None)
            self._strikes.pop(host, None)

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
            self._strikes.clear()


DEAD = DeadHosts()
PROBE_TIMEOUT = (3.0, 8.0)          # для «молчащего» сервера: подключение 3 с, первый байт 8 с

MIN_SPEED = float(os.environ.get("YA2GD_MIN_SPEED", str(8 * 1024)))       # байт/с
SPEED_WINDOW = float(os.environ.get("YA2GD_SPEED_WINDOW", "120"))


class SpeedGuard:
    """Сторож скорости: сервер, который отдаёт файл «по капле» (меньше MIN_SPEED за SPEED_WINDOW),
    держал бы поток часами — такой файл лучше отложить и взять доступные."""

    def __init__(self, name):
        self.name = name
        self.start = time.time()
        self.marks = [(self.start, 0)]
        self.total = 0

    def add(self, n):
        self.total += n
        now = time.time()
        if now - self.marks[-1][0] >= 5:
            self.marks.append((now, self.total))
            while len(self.marks) > 2 and self.marks[1][0] <= now - SPEED_WINDOW:
                self.marks.pop(0)
        t0, b0 = self.marks[0]
        if now - t0 >= SPEED_WINDOW and (self.total - b0) < MIN_SPEED * (now - t0):
            speed = (self.total - b0) / max(1.0, now - t0)
            raise StorageDown(None, f"Яндекс отдаёт «{self.name}» по капле ({speed / 1024:.1f} КБ/с) — файл отложен")


def is_yandex_host(url):
    host = (urlsplit(url).hostname or "").lower()
    return host == "localhost" or host.startswith("127.") or any(host.endswith(d) for d in YANDEX_HOSTS)


def open_download(url, offset=0, sess=None, reject_pages=False, length=None):
    """Открывает поток файла с байта offset. Возвращает (response, фактическое_смещение).

    reject_pages: оригинал фото/видео никогда не приходит HTML/JSON — это отказ Яндекса.
    length: нужны только первые length байт (замер, разметка) — сервер не начинает отдавать весь файл.
    """
    s = sess or session()
    headers = {"Accept-Encoding": "identity"}
    if length:
        headers["Range"] = f"bytes={offset}-{offset + length - 1}"
    elif offset:
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
            # подключение — STORAGE_TIMEOUT; молчание дольше STREAM_READ_TIMEOUT посреди потока — обрыв.
            # Недавно молчавший сервер пробуем с короткими таймаутами, а не пропускаем без попытки.
            timeout = PROBE_TIMEOUT if DEAD.is_dead(host) else (STORAGE_TIMEOUT, STREAM_READ_TIMEOUT)
            r = s.get(loc, headers=headers, stream=True, allow_redirects=False, timeout=timeout)
            hops += 1
    except NETWORK_ERRORS as e:
        if host:
            connect = isinstance(e, (requests.ConnectTimeout, requests.ConnectionError)) and \
                not isinstance(e, requests.ReadTimeout)
            DEAD.strike(host, connect)
            raise StorageDown(host, f"сервер хранения Яндекса {host} не отвечает ({type(e).__name__})",
                              network=True)
        raise StorageDown(None, f"Яндекс не начал отдавать файл ({type(e).__name__})", network=True)

    code = r.status_code
    if code in (200, 206) and host:
        DEAD.forget(host)                    # сервер ожил
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
