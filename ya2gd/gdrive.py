"""Клиент Google Drive API v3: OAuth (refresh token), папки, резюмируемая загрузка.

Резюмируемая загрузка (https://developers.google.com/drive/api/guides/manage-uploads#resumable):
POST .../upload/drive/v3/files?uploadType=resumable → URI сессии (живёт ~неделю).
Куски отправляются PUT с Content-Range; после обрыва можно спросить у Google,
сколько байт он уже принял (PUT с "bytes */total"), и продолжить с этого места.
"""
import json
import os
import threading
import time
import urllib.parse

from .http_util import (ApiError, AuthError, FatalPause, NETWORK_ERRORS, RateLimited,
                        backoff, env_url, session, CONNECT_TIMEOUT, READ_TIMEOUT)

AUTH_URL = env_url("YA2GD_GOOGLE_AUTH", "https://accounts.google.com/o/oauth2/v2/auth")
TOKEN_URL = env_url("YA2GD_GOOGLE_TOKEN", "https://oauth2.googleapis.com/token")
API = env_url("YA2GD_GOOGLE_API", "https://www.googleapis.com/drive/v3")
UPLOAD = env_url("YA2GD_GOOGLE_UPLOAD", "https://www.googleapis.com/upload/drive/v3")

# drive.file — доступ только к файлам, которые создало само приложение. Этого хватает
# (мы всё создаём сами), и это «нечувствительная» область: Google не требует проверки приложения.
SCOPE = "https://www.googleapis.com/auth/drive.file"
FOLDER_MIME = "application/vnd.google-apps.folder"
APP_PROP = "ya2gd"

# Один замок на весь процесс: иначе параллельные потоки создают одинаковые папки-дубли.
_FOLDER_LOCK = threading.Lock()


class GoogleAuth:
    def __init__(self, client_id, client_secret, refresh_token):
        self.client_id = (client_id or "").strip()
        self.client_secret = (client_secret or "").strip()
        self.refresh_token = (refresh_token or "").strip()
        self._token = None
        self._exp = 0
        self._lock = threading.Lock()

    def auth_url(self, redirect_uri, state):
        return AUTH_URL + "?" + urllib.parse.urlencode({
            "client_id": self.client_id, "redirect_uri": redirect_uri, "response_type": "code",
            "scope": SCOPE + " openid email", "access_type": "offline", "prompt": "consent",
            "state": state, "include_granted_scopes": "true"})

    def exchange_code(self, code, redirect_uri):
        r = session().post(TOKEN_URL, data={
            "code": code, "client_id": self.client_id, "client_secret": self.client_secret,
            "redirect_uri": redirect_uri, "grant_type": "authorization_code"},
            timeout=(CONNECT_TIMEOUT, 60))
        data = _json(r)
        if r.status_code != 200:
            raise AuthError(f"Google: обмен кода не удался: {data.get('error_description') or data or r.text[:300]}")
        if not data.get("refresh_token"):
            raise AuthError("Google не выдал refresh_token. Удалите доступ приложения на "
                            "https://myaccount.google.com/permissions и авторизуйтесь снова.")
        self.refresh_token = data["refresh_token"]
        self._token = data.get("access_token")
        self._exp = time.time() + int(data.get("expires_in", 3600)) - 120
        return self.refresh_token

    def invalidate(self):
        with self._lock:
            self._token = None

    def access_token(self):
        with self._lock:
            if self._token and time.time() < self._exp:
                return self._token
            if not (self.client_id and self.client_secret):
                raise AuthError("Google: не заданы Client ID / Client secret", service="google")
            if not self.refresh_token:
                raise AuthError("Google: приложение не авторизовано — нажмите «Войти в Google»", service="google")
            last = None
            for attempt in range(6):
                try:
                    r = session().post(TOKEN_URL, data={
                        "client_id": self.client_id, "client_secret": self.client_secret,
                        "refresh_token": self.refresh_token, "grant_type": "refresh_token"},
                        timeout=(CONNECT_TIMEOUT, 60))
                except NETWORK_ERRORS as e:
                    last = e
                    time.sleep(backoff(attempt, cap=30))
                    continue
                data = _json(r)
                if r.status_code == 200:
                    self._token = data["access_token"]
                    self._exp = time.time() + int(data.get("expires_in", 3600)) - 120
                    return self._token
                if r.status_code in (400, 401):
                    err = data.get("error")
                    hint = ""
                    if err == "invalid_grant":
                        hint = (" Токен отозван или истёк. Если OAuth-приложение в режиме «Testing», "
                                "Google гасит токены через 7 дней — переведите его в «In production».")
                    raise AuthError(f"Google: обновление токена отклонено ({err}).{hint} "
                                    f"Нажмите «Войти в Google» ещё раз.", service="google")
                last = ApiError(f"HTTP {r.status_code}")
                time.sleep(backoff(attempt, cap=30))
            raise ApiError(f"Google: не удалось обновить токен: {last}", retryable=True, service="google")


class DriveClient:
    def __init__(self, auth, db=None, stop_event=None, log=None):
        self.auth = auth
        self.db = db
        self.stop_event = stop_event
        self.log = log or (lambda *a, **k: None)

    # ------------------------------------------------------------------ базовый запрос
    def _req(self, method, url, tries=8, ok=(200,), **kw):
        last = None
        base_headers = kw.pop("headers", None) or {}
        for attempt in range(tries):
            headers = dict(base_headers, Authorization=f"Bearer {self.auth.access_token()}")
            try:
                r = session().request(method, url, headers=headers,
                                      timeout=(CONNECT_TIMEOUT, READ_TIMEOUT), **kw)
            except NETWORK_ERRORS as e:
                last = ApiError(f"Сеть (Google): {e}", retryable=True, service="google")
            else:
                if r.status_code in ok:
                    return r
                err = classify(r)
                if isinstance(err, RateLimited):
                    raise err        # общую паузу Google ставит программа, а не повторы на месте
                if r.status_code == 401 and attempt == 0:
                    self.auth.invalidate()
                    if tries == 1:
                        raise ApiError("Google: токен устарел, обновляю", status=401, retryable=True,
                                       service="google")
                    continue
                if not err.retryable:
                    raise err
                last = err
            delay = backoff(attempt, base=2, cap=90)
            self.log(f"{last} — повтор через {delay:.0f} c", level="warn")
            if self._wait(delay):
                break
        raise last or ApiError("Google: не удалось выполнить запрос")

    def _wait(self, sec):
        if self.stop_event is not None:
            return self.stop_event.wait(sec)
        time.sleep(sec)
        return False

    # ------------------------------------------------------------------ информация
    def about(self):
        r = self._req("GET", f"{API}/about", params={"fields": "user(emailAddress,displayName),storageQuota"})
        return r.json()

    # ------------------------------------------------------------------ файлы/папки
    def get_file(self, fid):
        """dict файла или None, если его нет/он в корзине."""
        try:
            r = self._req("GET", f"{API}/files/{fid}", params={"fields": "id,name,trashed,md5Checksum,size"})
        except ApiError as e:
            if e.status == 404:
                return None
            raise
        d = r.json()
        return None if d.get("trashed") else d

    def find_in_parent(self, parent_id, name, folders=False):
        qname = name.replace("\\", "\\\\").replace("'", "\\'")
        q = f"name = '{qname}' and '{parent_id}' in parents and trashed = false"
        q += f" and mimeType {'=' if folders else '!='} '{FOLDER_MIME}'"
        r = self._req("GET", f"{API}/files", params={
            "q": q, "fields": "files(id,name,md5Checksum,size)", "pageSize": 100, "spaces": "drive"})
        return r.json().get("files", [])

    def create_folder(self, name, parent_id):
        r = self._req("POST", f"{API}/files", params={"fields": "id"},
                      json={"name": name, "mimeType": FOLDER_MIME, "parents": [parent_id],
                            "appProperties": {APP_PROP: "1"}})
        return r.json()["id"]

    def ensure_folder(self, full_path):
        """id папки по полному пути от «Мой диск» («Фото/2019/2019-05»), недостающие создаются.
        Кэш — в БД. Находятся только папки, созданные этой программой (доступ drive.file)."""
        key = "/".join(p for p in full_path.split("/") if p)
        if not key:
            return "root"
        with _FOLDER_LOCK:
            cached = self.db.folder_get(key) if self.db else None
            if cached:
                return cached
            parent, acc = "root", ""
            for part in key.split("/"):
                acc = f"{acc}/{part}" if acc else part
                fid = self.db.folder_get(acc) if self.db else None
                if not fid:
                    found = self.find_in_parent(parent, part, folders=True)
                    fid = found[0]["id"] if found else self.create_folder(part, parent)
                    if self.db:
                        self.db.folder_set(acc, fid)
                parent = fid
            return parent

    def create_empty(self, name, parent_id, mime, props, modified=None):
        meta = {"name": name, "parents": [parent_id], "appProperties": props}
        if modified:
            meta["modifiedTime"] = modified
        r = self._req("POST", f"{API}/files", params={"fields": "id,md5Checksum,size"}, json=meta)
        return r.json()

    def delete(self, fid):
        try:
            self._req("DELETE", f"{API}/files/{fid}", ok=(200, 204))
        except ApiError as e:
            if e.status != 404:
                raise

    def _list_all(self, q, fields):
        token = None
        while True:
            params = {"q": q, "fields": f"nextPageToken,files({fields})", "pageSize": 1000, "spaces": "drive"}
            if token:
                params["pageToken"] = token
            d = self._req("GET", f"{API}/files", params=params).json()
            yield from d.get("files", [])
            token = d.get("nextPageToken")
            if not token:
                return

    def list_app_files(self):
        """Все неудалённые файлы (не папки), созданные программой: id → {size, md5Checksum}."""
        q = (f"trashed = false and mimeType != '{FOLDER_MIME}' and "
             f"appProperties has {{ key='{APP_PROP}' and value='1' }}")
        return {f["id"]: f for f in self._list_all(q, "id,size,md5Checksum")}

    def list_children(self, parent_id):
        """Файлы (не папки) в папке — для проверки дублей."""
        q = f"'{parent_id}' in parents and trashed = false and mimeType != '{FOLDER_MIME}'"
        return list(self._list_all(q, "id,name,md5Checksum,size"))

    def upload_small(self, name, parent_id, data, mime, props, modified=None):
        """Загрузка одним запросом (multipart). Без внутренних повторов: при сбое вызывающий
        сначала проверяет, не создался ли файл, — иначе возможен дубль."""
        meta = {"name": name, "parents": [parent_id], "appProperties": props}
        if modified:
            meta["modifiedTime"] = modified
        boundary = "ya2gd" + os.urandom(12).hex()
        body = (f"--{boundary}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n".encode()
                + json.dumps(meta).encode("utf-8")
                + f"\r\n--{boundary}\r\nContent-Type: {mime or 'application/octet-stream'}\r\n\r\n".encode()
                + data + f"\r\n--{boundary}--\r\n".encode())
        r = self._req("POST", f"{UPLOAD}/files", tries=1,
                      params={"uploadType": "multipart", "fields": "id,md5Checksum,size"},
                      headers={"Content-Type": f"multipart/related; boundary={boundary}"}, data=body)
        return r.json()

    # ------------------------------------------------------------------ резюмируемая загрузка
    def start_upload(self, name, parent_id, size, mime, props, modified=None):
        meta = {"name": name, "parents": [parent_id], "appProperties": props}
        if modified:
            meta["modifiedTime"] = modified
        r = self._req("POST", f"{UPLOAD}/files",
                      params={"uploadType": "resumable", "fields": "id,md5Checksum,size"},
                      headers={"X-Upload-Content-Type": mime or "application/octet-stream",
                               "X-Upload-Content-Length": str(size),
                               "Content-Type": "application/json; charset=UTF-8"},
                      data=json.dumps(meta).encode("utf-8"))
        uri = r.headers.get("Location")
        if not uri:
            raise ApiError("Google не вернул адрес сессии загрузки")
        return uri

    def upload_status(self, uri, size):
        """('done', info) | ('partial', принято_байт) | ('gone', None)."""
        return self._put(uri, b"", f"bytes */{size}")

    def put_chunk(self, uri, data, start, size):
        end = start + len(data) - 1
        return self._put(uri, data, f"bytes {start}-{end}/{size}")

    def cancel_upload(self, uri):
        """Бросить незавершённую сессию загрузки (замер скорости): файл на Drive так и не появляется,
        а брошенная сессия Google сама истекает через неделю — отмена лишь освобождает её раньше."""
        try:
            session().delete(uri, headers={"Authorization": f"Bearer {self.auth.access_token()}"},
                             timeout=(CONNECT_TIMEOUT, 30)).close()
        except Exception:  # noqa
            pass

    def _put(self, uri, data, content_range):
        headers = {"Authorization": f"Bearer {self.auth.access_token()}",
                   "Content-Length": str(len(data)), "Content-Range": content_range}
        try:
            r = session().put(uri, data=data, headers=headers, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
        except NETWORK_ERRORS as e:
            raise ApiError(f"Сеть (Google, загрузка): {e}", retryable=True, service="google")
        if r.status_code in (200, 201):
            return "done", r.json()
        if r.status_code == 308:
            rng = r.headers.get("Range")
            return "partial", (int(rng.rsplit("-", 1)[1]) + 1) if rng else 0
        if r.status_code in (404, 410):
            return "gone", None
        if r.status_code == 401:
            self.auth.invalidate()
            raise ApiError("Google: токен устарел, обновляю", status=401, retryable=True, service="google")
        raise classify(r)


def classify(r):
    e = _classify(r)
    e.service = "google"
    return e


def _classify(r):
    data = _json(r)
    err = data.get("error") if isinstance(data.get("error"), dict) else {}
    reasons = [e.get("reason") for e in err.get("errors", []) if isinstance(e, dict)]
    msg = err.get("message") or r.text[:300]
    st = r.status_code
    if st == 401:
        return AuthError(f"Google: авторизация отклонена ({msg})", status=st)
    if st == 403 and ("storageQuotaExceeded" in reasons or "quotaExceeded" in reasons):
        return FatalPause("Google Drive: закончилось место", status=st)
    if st == 403 and ("insufficientPermissions" in reasons or "appNotAuthorizedToFile" in reasons):
        return AuthError(f"Google: недостаточно прав ({msg})", status=st)
    if st == 429 or (st == 403 and any(x and ("RateLimit" in x or "rateLimit" in x or "LimitExceeded" in x)
                                       for x in reasons)):
        return RateLimited(f"Google: лимит запросов ({', '.join(filter(None, reasons)) or st}: {msg})",
                           status=st, retryable=True)
    if st >= 500:
        return ApiError(f"Google: ошибка сервера {st}", status=st, retryable=True)
    return ApiError(f"Google: HTTP {st} ({msg})", status=st, body=data)


def _json(r):
    try:
        v = r.json()
        return v if isinstance(v, dict) else {}
    except ValueError:
        return {}
