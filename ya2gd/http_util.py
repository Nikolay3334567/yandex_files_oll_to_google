"""Общие штуки для HTTP: сессии на поток, ошибки, повторы с паузой."""
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
        s.headers["User-Agent"] = "ya2gd/1.0"
        _local.s = s
    return s


class ApiError(Exception):
    def __init__(self, msg, status=None, retryable=False, body=None):
        super().__init__(msg)
        self.status = status
        self.retryable = retryable
        self.body = body


class AuthError(ApiError):
    """Токен недействителен/просрочен — дальше работать бессмысленно, нужна пауза."""


class WebAuthError(ApiError):
    """Куки веб-версии Яндекса (раздел «Фото») недействительны. Останавливает только этот раздел."""


class FatalPause(ApiError):
    """Ошибка, при которой весь перенос надо поставить на паузу (место на Drive кончилось и т.п.)."""


class RateLimited(ApiError):
    """Сервис просит притормозить."""


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
