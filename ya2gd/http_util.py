"""Общие штуки для HTTP: сессии на поток, ошибки, повторы с паузой, ограничитель частоты."""
import os
import random
import threading
import time

import requests
import urllib3.exceptions

CONNECT_TIMEOUT = 20
READ_TIMEOUT = 180

_local = threading.local()


def session():
    s = getattr(_local, "s", None)
    if s is None:
        s = requests.Session()
        s.headers["User-Agent"] = "ya2gd/2.0"
        _local.s = s
    return s


class ApiError(Exception):
    """service: 'yandex' | 'google' | None — чтобы притормаживать именно ту сторону, что просит."""

    def __init__(self, msg, status=None, retryable=False, body=None, service=None):
        super().__init__(msg)
        self.status = status
        self.retryable = retryable
        self.body = body
        self.service = service


class AuthError(ApiError):
    """Токен недействителен/просрочен — дальше работать бессмысленно, нужна пауза."""


class WebAuthError(ApiError):
    """Куки веб-версии Яндекса недействительны. Останавливает только то, что идёт через веб-версию."""


class FatalPause(ApiError):
    """Ошибка, при которой весь перенос надо поставить на паузу (место на Drive кончилось и т.п.)."""


class RateLimited(ApiError):
    """Сервис просит притормозить."""


class LocalDiskError(ApiError):
    """Буфер на диске компьютера недоступен или переполнен — это не сеть, повторять бесполезно."""


NETWORK_ERRORS = (requests.ConnectionError, requests.Timeout, requests.exceptions.ChunkedEncodingError,
                  urllib3.exceptions.ProtocolError, urllib3.exceptions.ReadTimeoutError,
                  urllib3.exceptions.IncompleteRead, ConnectionError, TimeoutError, OSError)


def backoff(attempt, base=2.0, cap=120.0):
    return min(cap, base * (2 ** attempt)) * (0.7 + 0.6 * random.random())


def sleep_interruptible(seconds, stop_event=None):
    if stop_event is None:
        time.sleep(seconds)
        return False
    return stop_event.wait(seconds)


def env_url(name, default):
    return os.environ.get(name, default).rstrip("/")


class RateLimiter:
    """Не чаще одного запроса в interval секунд на весь процесс (запросы равномерно растягиваются)."""

    def __init__(self, interval):
        self.interval = interval
        self._next = 0.0
        self._lock = threading.Lock()

    def wait(self, stop_event=None):
        with self._lock:
            now = time.time()
            slot = max(now, self._next)
            self._next = slot + self.interval
        delay = slot - now
        if delay > 0:
            sleep_interruptible(delay, stop_event)
