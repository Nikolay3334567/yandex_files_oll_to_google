"""Клиент REST API Яндекс Диска (https://yandex.ru/dev/disk-api/doc/ru/).

Используемые методы:
- GET /v1/disk                       — информация о Диске (проверка токена);
- GET /v1/disk/resources/files       — плоский список ВСЕХ файлов Диска (без обхода папок);
- GET /v1/disk/resources?path=...    — содержимое папки (обход дерева); с preview_size — ссылки на превью;
- GET /v1/disk/resources/download    — временная ссылка на скачивание конкретного файла.

Все запросы к API идут не чаще REQUEST_INTERVAL на всю программу — чтобы не создавать сервису лишней
нагрузки.
"""
import os
import time

from .fetch import fetch_small, is_yandex_host, open_download
from .http_util import (ApiError, AuthError, NETWORK_ERRORS, RateLimited, RateLimiter, backoff,
                        env_url, session, CONNECT_TIMEOUT, READ_TIMEOUT)

API = env_url("YA2GD_YANDEX_API", "https://cloud-api.yandex.net/v1/disk")
REQUEST_INTERVAL = float(os.environ.get("YA2GD_YANDEX_INTERVAL", "0.15"))   # ≈ 6-7 запросов в секунду
RATE = RateLimiter(REQUEST_INTERVAL)

ITEM_FIELDS = "path,name,type,size,md5,sha256,modified,created,mime_type,media_type,exif"
PREVIEW_SIZES = ("XXXL", "XXL", "XL", "L", "M")


def norm_item(it):
    path = it["path"]
    source = "photounlim" if path.startswith("photounlim:") else "disk"
    exif = it.get("exif") or {}
    return {
        "path": path,
        "source": source,
        "name": it.get("name") or path.rsplit("/", 1)[-1],
        "size": int(it.get("size") or 0),
        "md5": it.get("md5"),
        "sha256": it.get("sha256"),
        "modified": it.get("modified"),
        "created": it.get("created"),
        "taken": exif.get("date_time"),
        "mime": it.get("mime_type") or "application/octet-stream",
        "media_type": it.get("media_type"),
    }


class YandexClient:
    def __init__(self, token, stop_event=None, log=None):
        self.token = (token or "").strip()
        self.stop_event = stop_event
        self.log = log or (lambda *a, **k: None)

    def _headers(self):
        return {"Authorization": f"OAuth {self.token}", "Accept": "application/json"}

    def _get(self, url, params=None, tries=8, forbidden_is_auth=True):
        if not self.token:
            raise AuthError("Токен Яндекса не задан", service="yandex")
        last = None
        for attempt in range(tries):
            RATE.wait(self.stop_event)
            try:
                r = session().get(url, params=params, headers=self._headers(),
                                  timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
            except NETWORK_ERRORS as e:
                last = ApiError(f"Сеть (Яндекс): {e}", retryable=True, service="yandex")
            else:
                if r.status_code == 200:
                    return r.json()
                body = _safe_json(r)
                desc = body.get("description") or body.get("message") or r.text[:300]
                st = r.status_code
                if st == 401:
                    raise AuthError(f"Яндекс: токен не принят ({desc})", status=st, service="yandex")
                if st == 403 and not forbidden_is_auth:
                    raise ApiError(f"Яндекс: доступ запрещён ({desc})", status=403, service="yandex")
                if st == 403:
                    raise AuthError(f"Яндекс: нет доступа ({desc}). Проверьте права приложения "
                                    f"(нужно «Чтение всего Диска»).", status=403, service="yandex")
                if st == 404:
                    raise ApiError(f"Яндекс: не найдено ({desc})", status=404, service="yandex")
                if st == 410:
                    # С октября 2026 так отвечают на разделы, лежавшие в пострадавших дата-центрах
                    raise ApiError(f"Яндекс: ресурс недоступен (410 Gone: {desc})", status=410, service="yandex")
                if st == 429:
                    raise RateLimited("Яндекс: слишком много запросов (429)", status=429, retryable=True,
                                      service="yandex")
                if st >= 500:
                    last = ApiError(f"Яндекс: ошибка сервера {st} ({desc})", status=st, retryable=True,
                                    service="yandex")
                else:
                    raise ApiError(f"Яндекс: HTTP {st} ({desc})", status=st, body=body, service="yandex")
            if attempt == tries - 1:
                break
            delay = backoff(attempt, base=2, cap=60)
            self.log(f"{last} — повтор через {delay:.0f} c", level="warn")
            if self.stop_event is not None and self.stop_event.wait(delay):
                break
            elif self.stop_event is None:
                time.sleep(delay)
        raise last or ApiError("Яндекс: не удалось выполнить запрос", service="yandex")

    # ---- информация ----
    def disk_info(self):
        return self._get(API)

    # ---- плоский список всех файлов ----
    def files_page(self, offset, limit=1000):
        data = self._get(f"{API}/resources/files",
                         params={"limit": limit, "offset": offset,
                                 "fields": ",".join(f"items.{f}" for f in ITEM_FIELDS.split(",")) + ",limit,offset"})
        raw = data.get("items", [])
        return [norm_item(i) for i in raw if i.get("type", "file") == "file"], len(raw)

    # ---- обход папки ----
    def dir_page(self, path, offset, limit=1000, tries=8):
        """Возвращает (files, subdirs, total, raw)."""
        fields = ",".join(f"_embedded.items.{f}" for f in ITEM_FIELDS.split(",")) + \
            ",_embedded.total,_embedded.limit,_embedded.offset,type,path"
        data = self._get(f"{API}/resources",
                         params={"path": path, "limit": limit, "offset": offset, "fields": fields,
                                 "sort": "name"}, tries=tries,
                         forbidden_is_auth=not path.startswith("photounlim:"))
        emb = data.get("_embedded") or {}
        items = emb.get("items") or []
        files = [norm_item(i) for i in items if i.get("type") == "file"]
        dirs = [i["path"] for i in items if i.get("type") == "dir"]
        return files, dirs, int(emb.get("total") or 0), len(items)

    # ---- раздел «Фото»: безлимитная автозагрузка (photounlim) ----
    # Официально не документирован. Пробуем все известные варианты; какой сработал — запоминаем.
    def photounlim_flat_page(self, offset, limit=1000):
        """GET /v1/disk/photounlim/resources/last-modified (замечен в приложениях Яндекса)."""
        data = self._get(f"{API}/photounlim/resources/last-modified",
                         params={"limit": limit, "offset": offset}, tries=3, forbidden_is_auth=False)
        raw = data.get("items") or (data.get("_embedded") or {}).get("items") or []
        return [norm_item(i) for i in raw if i.get("type", "file") == "file"], len(raw)

    def probe_photounlim(self):
        """Проверяет, какими способами виден раздел безлимитных фото. Возвращает (метод|None, отчёт)."""
        report = []
        try:
            files, dirs, total, _ = self.dir_page("photounlim:/", 0, 1, tries=3)
            report.append(f"resources?path=photounlim:/ — работает, элементов: {total}")
            if total:
                return "tree", report
        except ApiError as e:
            report.append(f"resources?path=photounlim:/ — {e}")
        try:
            files, raw = self.photounlim_flat_page(0, 1)
            report.append(f"photounlim/resources/last-modified — работает, получено {raw}")
            if raw:
                return "flat", report
        except ApiError as e:
            report.append(f"photounlim/resources/last-modified — {e}")
        return None, report

    # ---- скачивание ----
    def download_href(self, path, tries=2):
        """Ссылка на один файл. Повторов мало: если API не отвечает, программа не ждёт на каждом файле
        минутами, а ставит паузу всему скачиванию (см. engine._open_source)."""
        if path.startswith("photounlim:"):
            try:
                data = self._get(f"{API}/resources/download", params={"path": path}, tries=tries,
                                 forbidden_is_auth=False)
            except ApiError as e:
                if e.retryable:
                    raise
                data = self._get(f"{API}/photounlim/resources/download", params={"path": path}, tries=tries,
                                 forbidden_is_auth=False)
        else:
            data = self._get(f"{API}/resources/download", params={"path": path}, tries=tries,
                             forbidden_is_auth=False)
        href = data.get("href")
        if not href:
            raise ApiError(f"Яндекс не выдал ссылку на скачивание: {data}", service="yandex")
        return href

    def open_stream(self, path, offset=0):
        """Поток скачивания с нужного байта: (response, фактическое_смещение)."""
        return open_download(self.download_href(path), offset)

    # ---- превью (если оригинал недоступен) ----
    def preview(self, path):
        """Самое крупное доступное превью: (bytes, content-type, размер) или исключение."""
        last = None
        for size in PREVIEW_SIZES:
            try:
                data = self._get(f"{API}/resources", tries=2, forbidden_is_auth=False,
                                 params={"path": path, "preview_size": size, "preview_crop": "false",
                                         "fields": "preview,sizes,media_type"})
            except AuthError:
                raise
            except ApiError as e:
                last = e
                continue
            urls = [data.get("preview")] + [s.get("url") for s in data.get("sizes") or []
                                            if s.get("name") == size]
            for url in filter(None, urls):
                url = "https:" + url if url.startswith("//") else url
                headers = self._headers() if is_yandex_host(url) else {}
                try:
                    body, ctype = fetch_small(url, headers=headers)
                except ApiError as e:
                    last = e
                    continue
                if looks_like_image(body, ctype):
                    return body, ctype, size
                last = ApiError(f"превью {size}: пришло не изображение ({ctype}, {len(body)} байт)")
        raise last or ApiError("превью недоступно")


def looks_like_image(body, ctype):
    if len(body) < 512:
        return False
    magic = (body.startswith(b"\xff\xd8\xff") or body.startswith(b"\x89PNG") or body[8:12] == b"WEBP"
             or body[4:12] in (b"ftypheic", b"ftypmif1", b"ftypavif"))
    return magic or ctype.startswith("image/")


def _safe_json(r):
    try:
        v = r.json()
        return v if isinstance(v, dict) else {}
    except ValueError:
        return {}
