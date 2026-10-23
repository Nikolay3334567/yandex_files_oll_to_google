"""Доступ к Яндекс Диску через веб-API (как у сайта disk.yandex.ru).

Нужен для раздела «Фото» (безлимитная автозагрузка): публичный REST API эти файлы не отдаёт.
И как запасной канал для «Файлов»: с октября 2026 REST API у части людей отвечает 410 Gone
на чтение папок, а сайт продолжает работать. Веб-версия Диска получает файлы так:
- POST /models-v2?m=mpfs/list  {path: "/photounlim", offset, amount, meta: "size,md5,..."}
  — плоский список всех файлов раздела постранично, с размером и MD5;
- POST /models-v2?m=mpfs/url   {path: "/photounlim/<имя>" или "/disk/<путь>"}
  — одноразовая прямая ссылка на оригинал (так работает кнопка «Скачать»).
  Яндекс отключил на сайте скачивание папок и нескольких файлов сразу, но ссылка
  на один файл выдаётся — программа и качает строго по одному файлу.
Авторизация — куки вашей сессии в браузере (Session_id, sessionid2, yandexuid) и
CSRF-токен sk со страницы /client/disk. Способ неофициальный: так же работает открытый
проект github.com/PXSYS13/yadisk_backup.
"""
import json
import random
import re
import threading
import time
from urllib.parse import urlsplit

import requests

from .fetch import fetch_small
from .http_util import (ApiError, NETWORK_ERRORS, RateLimited, WebAuthError, backoff, env_url)
from .yandex import PREVIEW_SIZES, looks_like_image

BASE = env_url("YA2GD_YANDEX_WEB", "https://disk.yandex.ru")
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36")
REQUIRED = ("Session_id", "sessionid2", "yandexuid")
UNLIM_ROOT = "/photounlim"
DISK_ROOT = "/disk"
LIST_META = "size,md5,mimetype,mediatype,etime"
_tls = threading.local()       # одна HTTP-сессия на поток (переиспользует соединения)
MIN_INTERVAL = 0.12          # не чаще ~8 запросов к веб-API в секунду на всю программу


def parse_cookies(text):
    """Куки в любом из видов: экспорт Cookie-Editor (JSON-массив), JSON-объект,
    строка заголовка «Cookie: a=b; c=d»."""
    text = (text or "").strip()
    if not text:
        return {}
    if text[0] in "[{":
        data = json.loads(text)
        if isinstance(data, dict):
            return {str(k): str(v) for k, v in data.items() if v}
        return {c["name"]: c["value"] for c in data
                if isinstance(c, dict) and c.get("name") and isinstance(c.get("value"), str)}
    if text.lower().startswith("cookie:"):
        text = text[7:]
    out = {}
    for part in text.split(";"):
        if "=" in part:
            k, v = part.split("=", 1)
            if k.strip():
                out[k.strip()] = v.strip()
    return out


def to_mpfs(path):
    """photounlim:/a.jpg → /photounlim/a.jpg;  disk:/A/b.txt → /disk/A/b.txt"""
    for pref, root in (("photounlim:", UNLIM_ROOT), ("disk:", DISK_ROOT)):
        if path.startswith(pref):
            return root + "/" + path.split(":", 1)[1].lstrip("/")
    return path


def from_mpfs(path):
    """/photounlim/a.jpg → photounlim:/a.jpg;  /disk/A/b.txt → disk:/A/b.txt"""
    for pref, root in (("photounlim:/", UNLIM_ROOT), ("disk:/", DISK_ROOT)):
        if path.startswith(root + "/"):
            return pref + path[len(root):].lstrip("/")
    return path


def norm_item(it):
    meta = it.get("meta") or {}
    path = from_mpfs(it.get("path") or "")
    name = it.get("name") or path.rsplit("/", 1)[-1]

    def iso(ts):
        try:
            return time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime(int(ts)))
        except (TypeError, ValueError):
            return None
    return {
        "path": path, "source": "photounlim" if path.startswith("photounlim:") else "disk", "name": name,
        "size": int(meta.get("size") or 0), "md5": meta.get("md5"), "sha256": None,
        "modified": iso(it.get("mtime")), "created": iso(it.get("ctime")),
        "taken": iso(meta.get("etime")) if meta.get("etime") else None,
        "mime": meta.get("mimetype") or "application/octet-stream",
        "media_type": meta.get("mediatype"),
    }


class YandexWeb:
    _lock = threading.Lock()          # общий на процесс: sk и темп запросов
    _sk = None
    _uid = None
    _sk_key = None
    _last_call = 0.0

    def __init__(self, cookie_text, stop_event=None, log=None):
        self.cookies = parse_cookies(cookie_text)
        self.stop_event = stop_event
        self.log = log or (lambda *a, **k: None)
        self._key = tuple(sorted(self.cookies.items()))

    # ---------------------------------------------------------------- сессия
    def missing(self):
        return [n for n in REQUIRED if n not in self.cookies]

    def session(self):
        cached = getattr(_tls, "s", None)
        s = cached[1] if cached and cached[0] == self._key else None
        if s is None:
            s = requests.Session()
            s.headers.update({"User-Agent": UA, "Accept": "*/*", "Accept-Language": "ru,en;q=0.9",
                              "Origin": BASE, "Referer": f"{BASE}/client/photo",
                              "X-Requested-With": "XMLHttpRequest"})
            host = urlsplit(BASE).hostname or ""
            domain = ".yandex.ru" if host.endswith("yandex.ru") else host
            for k, v in self.cookies.items():
                s.cookies.set(k, v, domain=domain)
            _tls.s = (self._key, s)
        return s

    def _auth(self, force=False):
        key = tuple(sorted(self.cookies.items()))
        with YandexWeb._lock:
            if not force and YandexWeb._sk and YandexWeb._sk_key == key:
                return YandexWeb._sk, YandexWeb._uid
        if not self.cookies:
            raise WebAuthError("Куки Яндекса для раздела «Фото» не заданы")
        miss = self.missing()
        if miss:
            raise WebAuthError(f"В куках не хватает: {', '.join(miss)}. Скопируйте их на странице disk.yandex.ru")
        try:
            r = self.session().get(f"{BASE}/client/disk", timeout=(20, 60))
        except NETWORK_ERRORS as e:
            raise ApiError(f"Яндекс (веб) не ответил: {e}", retryable=True)
        if "passport." in r.url or r.status_code in (401, 403):
            raise WebAuthError("Куки устарели: Яндекс просит войти заново. Обновите куки в блоке «Раздел «Фото»».")
        if r.status_code != 200:
            raise ApiError(f"Яндекс (веб) HTTP {r.status_code}", status=r.status_code, retryable=True)
        sk = re.search(r'"sk"\s*:\s*"([^"]+)"', r.text)
        uid = re.search(r'"uid"\s*:\s*"?(\d+)"?', r.text)
        if not sk or not uid:
            raise WebAuthError("Не удалось войти по кукам (на странице нет sk/uid). Обновите куки.")
        with YandexWeb._lock:
            YandexWeb._sk, YandexWeb._uid, YandexWeb._sk_key = sk.group(1), uid.group(1), key
        return YandexWeb._sk, YandexWeb._uid

    def _throttle(self):
        with YandexWeb._lock:
            wait = YandexWeb._last_call + MIN_INTERVAL - time.time()
            YandexWeb._last_call = max(time.time(), YandexWeb._last_call + MIN_INTERVAL)
        if wait > 0:
            time.sleep(wait)

    def call(self, method, params=None, tries=6, timeout=(20, 120)):
        last = None
        refreshed = False
        for attempt in range(tries):
            sk, uid = self._auth()
            body = {"sk": sk, "connection_id": f"{uid}{int(time.time() * 1000)}{random.randint(100, 999)}",
                    "apiMethod": method}
            if params is not None:
                body["requestParams"] = params
            self._throttle()
            try:
                r = self.session().post(f"{BASE}/models-v2", params={"m": method}, json=body, timeout=timeout)
            except NETWORK_ERRORS as e:
                last = ApiError(f"Яндекс (веб) {method}: сеть: {e}", retryable=True)
            else:
                head = r.text[:3000].lower() if r.text else ""
                if "captcha" in head:
                    raise RateLimited("Яндекс показал капчу — слишком частые запросы к разделу «Фото»",
                                      status=r.status_code, retryable=True)
                if r.status_code in (401, 403):
                    if not refreshed:
                        refreshed = True
                        self._auth(force=True)
                        continue
                    raise WebAuthError(f"Яндекс отклонил куки (HTTP {r.status_code}). Обновите куки.")
                if r.status_code == 429:
                    raise RateLimited("Яндекс (веб): слишком много запросов (429)", status=429, retryable=True)
                if r.status_code == 200:
                    try:
                        data = r.json()
                    except ValueError:
                        last = ApiError(f"Яндекс (веб) {method}: ответ не JSON", retryable=True)
                    else:
                        if isinstance(data, dict) and data.get("error"):
                            err = data["error"]
                            txt = json.dumps(err, ensure_ascii=False)[:300]
                            if not refreshed and ("sk" in txt.lower() or "auth" in txt.lower()):
                                refreshed = True
                                self._auth(force=True)
                                continue
                            code = (err.get("code") if isinstance(err, dict) else None) or 0
                            raise ApiError(f"Яндекс (веб) {method}: {txt}",
                                           status=404 if "notfound" in txt.lower().replace(" ", "") else code,
                                           retryable=False)
                        return data
                else:
                    last = ApiError(f"Яндекс (веб) {method}: HTTP {r.status_code}", status=r.status_code,
                                    retryable=r.status_code >= 500)
                    if not last.retryable:
                        raise last
            if attempt == tries - 1:
                break                    # после последней попытки ждать незачем
            delay = backoff(attempt, base=2, cap=60)
            self.log(f"{last} — повтор через {delay:.0f} c", level="warn")
            if self.stop_event is not None:
                if self.stop_event.wait(delay):
                    break
            else:
                time.sleep(delay)
        raise last or ApiError(f"Яндекс (веб) {method}: не удалось")

    # ---------------------------------------------------------------- методы
    def list_page(self, path=UNLIM_ROOT, offset=0, amount=1000, meta=LIST_META):
        """(files, subdirs, raw_count) — одна страница плоского списка папки."""
        data = self.call("mpfs/list", {"path": path, "amount": amount, "offset": offset, "meta": meta})
        if not isinstance(data, list):
            raise ApiError(f"mpfs/list: неожиданный ответ {str(data)[:200]}")
        children = [x for x in data if (x.get("path") or "").rstrip("/") != path.rstrip("/")]
        files = [norm_item(x) for x in children if x.get("type") == "file"]
        dirs = [x["path"] for x in children if x.get("type") == "dir"]
        return files, dirs, len(children)

    def file_url(self, path):
        data = self.call("mpfs/url", {"path": to_mpfs(path)}, tries=2, timeout=(10, 30))
        url = data.get("file") if isinstance(data, dict) else None
        if not url:
            raise ApiError(f"Яндекс не выдал ссылку на файл: {str(data)[:200]}", retryable=True)
        return "https:" + url if url.startswith("//") else url

    def preview(self, path):
        """Самое крупное доступное превью файла: (bytes, content-type, размер).

        Веб-версия отдаёт ссылки на превью в meta.sizes (имена XXXL…S) — как в просмотрщике фото.
        """
        mp = to_mpfs(path)
        data = self.call("mpfs/list", {"path": mp, "amount": 1, "offset": 0, "meta": "sizes"}, tries=2)
        items = data if isinstance(data, list) else [data]
        sizes = []
        for it in items:
            if isinstance(it, dict) and (it.get("path") or "").rstrip("/") == mp.rstrip("/"):
                sizes = (it.get("meta") or {}).get("sizes") or []
        by_name = {s.get("name"): s.get("url") for s in sizes if isinstance(s, dict) and s.get("url")}
        last = ApiError("веб-версия не дала ссылок на превью")
        for name in PREVIEW_SIZES + ("DEFAULT",):
            url = by_name.get(name)
            if not url:
                continue
            url = "https:" + url if url.startswith("//") else url
            try:
                body, ctype = fetch_small(url, sess=self.session())
            except ApiError as e:
                last = e
                continue
            if looks_like_image(body, ctype):
                return body, ctype, name
            last = ApiError(f"превью {name}: пришло не изображение ({ctype})")
        raise last

    def check(self):
        sk, uid = self._auth(force=True)
        files, dirs, raw = self.list_page(UNLIM_ROOT, 0, 1)
        return {"uid": uid, "has_files": raw > 0}
