"""Движок: получение списка файлов Яндекс Диска и потоковый перенос на Google Drive.

Источники (ничего не сохраняется на компьютер — байты идут Яндекс → память → Google):
- «Файлы» Диска: официальный REST API, плоский список /v1/disk/resources/files;
- раздел «Фото» (безлимитная автозагрузка): веб-API Диска (mpfs/list + mpfs/url) по кукам,
  либо REST, если Яндекс когда-нибудь откроет его там.

Надёжность:
- состояние в SQLite; после падения файлы «в работе» возвращаются в очередь;
- большой файл продолжается с байта, который подтвердил Google (резюмируемая сессия);
- маленький файл до загрузки сверяется по MD5 с Яндексом, после — Google сверяет MD5 сам;
- файлы на молчащих серверах Яндекса откладываются (попытки не тратятся) и повторяются позже;
- дубли исключены: перед загрузкой проверяется, нет ли уже в папке файла с тем же именем и MD5.
"""
import collections
import csv
import hashlib
import io
import os
import re
import threading
import time
import traceback

from .core import Log, Settings, data_dir
from .db import DB
from .fetch import DEAD, StorageDown, open_download
from .gdrive import APP_PROP, DriveClient, GoogleAuth
from .http_util import (ApiError, AuthError, FatalPause, NETWORK_ERRORS, RateLimited, WebAuthError,
                        backoff)
from .yandex import YandexClient
from .yaweb import UNLIM_ROOT, YandexWeb

DEFER_SEC = float(os.environ.get("YA2GD_DEAD_HOST_TTL", "1800"))
STOP_ERRORS = (AuthError, FatalPause, RateLimited, StorageDown, WebAuthError)


class Paused(Exception):
    pass


def name_date(name):
    """Дата из имени вида «2019-05-04 10-00-00.JPG» / «IMG_20190504_…»."""
    m = re.search(r"(?<!\d)((?:19|20)\d\d)[-_.]?(0[1-9]|1[0-2])[-_.]?(0[1-9]|[12]\d|3[01])", name or "")
    return f"{m.group(1)}-{m.group(2)}-{m.group(3)}" if m else None


def drive_dir_for(row, s):
    """Относительный путь папки на Drive для файла (структура как на Яндексе)."""
    path = row["path"]
    if row["source"] == "photounlim":
        rel = path.split(":", 1)[1].lstrip("/")
        sub = rel.rsplit("/", 1)[0] if "/" in rel else ""
        base = s["photounlim_folder"] or "Фото (безлимит)"
        date = (row.get("taken") or name_date(row.get("name")) or row.get("created") or
                row.get("modified") or "")[:10]
        split = s["photounlim_split"]
        if len(date) == 10 and date[4] == "-":
            if split == "year":
                base += "/" + date[:4]
            elif split == "month":
                base += f"/{date[:4]}/{date[:7]}"
        return f"{base}/{sub}" if sub else base
    rel = path.split(":", 1)[1].lstrip("/") if ":" in path else path.lstrip("/")
    return rel.rsplit("/", 1)[0] if "/" in rel else ""


def read_exact(raw, n, on_progress=None):
    buf = bytearray()
    while len(buf) < n:
        part = raw.read(min(1 << 20, n - len(buf)))
        if not part:
            break
        buf += part
        if on_progress:
            on_progress(len(buf))
    return bytes(buf)


class FolderIndex:
    """Кэш содержимого папок Drive: имя → [(id, md5, size)]. Один запрос на папку, а не на файл."""

    def __init__(self):
        self._data = {}
        self._lock = threading.Lock()
        self._folder_locks = collections.defaultdict(threading.Lock)

    def get(self, drive, folder_id):
        with self._lock:
            if folder_id in self._data:
                return self._data[folder_id]
            flock = self._folder_locks[folder_id]
        with flock:
            with self._lock:
                if folder_id in self._data:
                    return self._data[folder_id]
            idx = collections.defaultdict(list)
            for f in drive.list_children(folder_id):
                idx[f["name"]].append((f["id"], f.get("md5Checksum"), int(f.get("size") or -1)))
            with self._lock:
                self._data[folder_id] = idx
            return idx

    def add(self, folder_id, name, fid, md5, size):
        with self._lock:
            if folder_id in self._data:
                self._data[folder_id][name].append((fid, md5, size))

    def drop(self, folder_id):
        with self._lock:
            self._data.pop(folder_id, None)

    def clear(self):
        with self._lock:
            self._data.clear()


class Engine:
    def __init__(self, ddir=None):
        self.dir = ddir or data_dir()
        self.settings = Settings(os.path.join(self.dir, "settings.json"))
        self.log = Log(os.path.join(self.dir, "ya2gd.log"))
        self.db = DB(os.path.join(self.dir, "state.sqlite3"))
        n = self.db.reset_active()
        if n:
            self.log(f"Программа была остановлена во время переноса: {n} файл(ов) вернулись в очередь, "
                     f"незавершённые загрузки продолжатся с места обрыва", "warn")
        if self.db.get("scan.state") == "running":
            self.db.set("scan.state", "interrupted")

        self.lock = threading.RLock()
        self.stop_transfer = threading.Event()
        self.stop_scan = threading.Event()
        self.workers = []
        self.transfer_state = "idle"     # idle | running | pausing | paused | done
        self.pause_reason = ""
        self.active = {}
        self.cooldown_until = 0
        self.rate_hits = 0
        self.speed = collections.deque()
        self.scan_thread = None
        self.task = {"name": "", "state": "idle", "msg": ""}
        self.task_thread = None
        self.google_state = os.urandom(16).hex()
        self._auth = None
        self._auth_key = None
        self.web_problem = ""            # куки раздела «Фото» не работают — только этот раздел ждёт
        self.folder_index = FolderIndex()

    # ================================================================== клиенты
    def yandex(self, stop=None):
        return YandexClient(self.settings["yandex_token"], stop_event=stop, log=self.log)

    def yaweb(self, stop=None):
        return YandexWeb(self.settings["yandex_cookies"], stop_event=stop, log=self.log)

    def google_auth(self):
        s = self.settings
        key = (s["google_client_id"], s["google_client_secret"], s["google_refresh_token"])
        with self.lock:
            if self._auth is None or self._auth_key != key:
                self._auth = GoogleAuth(*key)
                self._auth_key = key
            return self._auth

    def drive(self, stop=None):
        return DriveClient(self.google_auth(), db=self.db, stop_event=stop, log=self.log)

    def photounlim_mode(self):
        """Как достаём раздел «Фото»: web (куки) | rest | '' (никак)."""
        if self.settings["yandex_cookies"]:
            return "web"
        return "rest" if self.db.get("photounlim.method") else ""

    # ================================================================== проверки
    def test_yandex(self):
        y = YandexClient(self.settings["yandex_token"], log=self.log)
        info = y.disk_info()
        user = info.get("user") or {}
        res = {"ok": True, "user": user.get("display_name") or user.get("login"),
               "total": info.get("total_space"), "used": info.get("used_space"),
               "photounlim_size": int(info.get("photounlim_size") or 0)}
        try:
            y.files_page(0, 1)
            res["files_api"] = "работает"
        except ApiError as e:
            res["files_api"] = f"ошибка: {e}"
        method, report = y.probe_photounlim()
        self.db.set("photounlim.method", method or "")
        self.db.set("photounlim.size", res["photounlim_size"])
        res["photounlim_rest"] = bool(method)
        res["photounlim_report"] = report
        self.log(f"Проверка Яндекса: {res}")
        return res

    def test_yandex_web(self):
        w = self.yaweb()
        try:
            info = w.check()
            msg = "Куки работают: список раздела «Фото» доступен" if info["has_files"] \
                else "Куки работают, но раздел «Фото» пуст"
            if info["has_files"]:
                files, _, _ = w.list_page(UNLIM_ROOT, 0, 1)
                if files:
                    w.file_url(files[0]["path"])
                    msg += ", ссылка на оригинал выдаётся"
        except WebAuthError as e:
            self.web_problem = str(e)
            raise
        self.web_problem = ""
        self.log(f"Проверка кук Яндекса: {msg}")
        return {"ok": True, "uid": info["uid"], "msg": msg}

    def test_google(self):
        d = DriveClient(self.google_auth(), db=self.db, log=self.log)
        about = d.about()
        root = d.ensure_folder("", self.settings["drive_root"])
        f = d.create_empty("_ya2gd_test.txt", root, "text/plain", {APP_PROP: "test"})
        d.delete(f["id"])
        q = about.get("storageQuota") or {}
        res = {"ok": True, "email": (about.get("user") or {}).get("emailAddress"),
               "limit": int(q["limit"]) if q.get("limit") else None,
               "usage": int(q.get("usage") or 0), "write": "запись в папку проверена"}
        if res["email"] and res["email"] != self.settings["google_email"]:
            self.settings.update(google_email=res["email"])
        self.log(f"Проверка Google: {res}")
        return res

    # ================================================================== список файлов
    def start_scan(self, fresh=False):
        with self.lock:
            if self.scan_thread and self.scan_thread.is_alive():
                return False
            if not self.settings["yandex_token"]:
                raise AuthError("Сначала укажите токен Яндекса")
            self.stop_scan.clear()
            self.scan_thread = threading.Thread(target=self._scan, args=(fresh,), daemon=True, name="scan")
            self.scan_thread.start()
            return True

    def _scan(self, fresh):
        db, s = self.db, self.settings
        state = db.get("scan.state")
        if fresh or state not in ("interrupted", "stopped"):
            scan_id = int(db.get("scan.id", "0")) + 1
            db.set("scan.id", scan_id)
            db.set("scan.phase", "main")
            db.set("scan.offset", 0)
            db.set("scan.found", 0)
            self.log(f"Получение списка файлов №{scan_id}")
        else:
            scan_id = int(db.get("scan.id", "1"))
            self.log(f"Получение списка продолжено с места остановки (этап {db.get('scan.phase')}, "
                     f"позиция {db.get('scan.offset')})")
        db.set("scan.state", "running")
        db.set("scan.error", "")
        y = YandexClient(s["yandex_token"], stop_event=self.stop_scan, log=self.log)
        try:
            if db.get("scan.phase") == "main":
                if s["scan_mode"] == "tree":
                    self._scan_tree(y, "disk:/", scan_id)
                else:
                    self._scan_flat(y, scan_id)
                if self.stop_scan.is_set():
                    raise Paused()
                db.set("scan.phase", "photounlim")
                db.set("scan.offset", 0)
            if db.get("scan.phase") == "photounlim":
                if s["include_photounlim"]:
                    self._scan_photounlim(y, scan_id)
                if self.stop_scan.is_set():
                    raise Paused()
                db.set("scan.phase", "done")
            db.set("scan.state", "done")
            st = db.stats()
            total = sum(v["count"] for v in st["by_status"].values())
            self.log(f"Список получен. Всего файлов: {total} "
                     f"({sum(v['bytes'] for v in st['by_status'].values()) / 2**30:.1f} ГБ)")
        except Paused:
            db.set("scan.state", "stopped")
            self.log("Получение списка остановлено, его можно продолжить", "warn")
        except Exception as e:  # noqa
            db.set("scan.state", "interrupted")
            db.set("scan.error", str(e))
            if isinstance(e, WebAuthError):
                self.web_problem = str(e)
            self.log(f"Получение списка прервано ошибкой: {e}", "error")
            self.log(traceback.format_exc(), "debug")

    def _bump_found(self, n):
        self.db.set("scan.found", int(self.db.get("scan.found", "0")) + n)

    def _scan_flat(self, y, scan_id):
        """«Файлы»: GET /v1/disk/resources/files — все файлы Диска одним плоским списком."""
        limit = 1000
        offset = int(self.db.get("scan.offset", "0"))
        while not self.stop_scan.is_set():
            files, raw = y.files_page(offset, limit)
            if files:
                self.db.upsert_files(files, scan_id)
                self._bump_found(len(files))
            offset += raw
            self.db.set("scan.offset", offset)
            if raw < limit:
                return
            self.log(f"Список «Файлы»: получено {offset}…")

    def _scan_tree(self, y, root, scan_id):
        stack, n = [root], 0
        while stack and not self.stop_scan.is_set():
            path = stack.pop()
            offset = 0
            while not self.stop_scan.is_set():
                files, dirs, total, raw = y.dir_page(path, offset, 1000)
                if files:
                    self.db.upsert_files(files, scan_id)
                    self._bump_found(len(files))
                    n += len(files)
                stack.extend(dirs)
                offset += raw
                if raw == 0 or offset >= total:
                    break
            self.log(f"Обход {root}: {path} — найдено {n}")

    def _scan_photounlim(self, y, scan_id):
        """Раздел «Фото» (безлимит): плоский список через веб-API (mpfs/list) или REST."""
        if self.settings["yandex_cookies"]:
            w = YandexWeb(self.settings["yandex_cookies"], stop_event=self.stop_scan, log=self.log)
            stack = [UNLIM_ROOT]
            offset = int(self.db.get("scan.offset", "0"))
            n = 0
            while stack and not self.stop_scan.is_set():
                path = stack.pop(0)
                while not self.stop_scan.is_set():
                    files, dirs, raw = w.list_page(path, offset, 1000)
                    if files:
                        self.db.upsert_files(files, scan_id)
                        self._bump_found(len(files))
                        n += len(files)
                    stack.extend(dirs)
                    offset += raw
                    if path == UNLIM_ROOT:
                        self.db.set("scan.offset", offset)
                    if raw < 1000:
                        break
                    self.log(f"Список раздела «Фото»: получено {n}…")
                offset = 0
            self.web_problem = ""
            self.log(f"Раздел «Фото»: в списке {n} файлов")
            return
        method, report = y.probe_photounlim()
        self.db.set("photounlim.method", method or "")
        if method == "tree":
            self._scan_tree(y, "photounlim:/", scan_id)
        elif method == "flat":
            offset = int(self.db.get("scan.offset", "0"))
            while not self.stop_scan.is_set():
                files, raw = y.photounlim_flat_page(offset, 1000)
                if files:
                    self.db.upsert_files(files, scan_id)
                    self._bump_found(len(files))
                offset += raw
                self.db.set("scan.offset", offset)
                if raw < 1000:
                    break
        else:
            size = int(self.db.get("photounlim.size") or 0)
            if size:
                self.log(f"Раздел «Фото» ({size / 2**30:.1f} ГБ) пропущен: официальный API Яндекса его не "
                         f"отдаёт. Добавьте куки в блоке «Раздел «Фото»» и получите список ещё раз.", "warn")

    def stop_scanning(self):
        self.stop_scan.set()

    def scanning(self):
        return bool(self.scan_thread and self.scan_thread.is_alive())

    # ================================================================== перенос
    def start_transfer(self):
        with self.lock:
            if any(t.is_alive() for t in self.workers):
                return False
            s = self.settings
            if not s["yandex_token"]:
                raise AuthError("Сначала укажите токен Яндекса")
            if not s["google_refresh_token"]:
                raise AuthError("Сначала авторизуйтесь в Google")
            self.stop_transfer.clear()
            self.transfer_state = "running"
            self.pause_reason = ""
            self.cooldown_until = 0
            self.web_problem = ""
            self.db.set("transfer.wanted", 1)
            n = s["workers"]
            self.workers = [threading.Thread(target=self._worker, args=(i,), daemon=True, name=f"w{i}")
                            for i in range(n)]
            for t in self.workers:
                t.start()
            threading.Thread(target=self._supervise, daemon=True).start()
            self.log(f"Перенос запущен, потоков: {n}")
            return True

    def pause_transfer(self, reason="", by_user=True):
        with self.lock:
            if by_user:
                self.db.set("transfer.wanted", 0)
            if self.transfer_state == "running":
                self.transfer_state = "pausing"
                self.pause_reason = reason
                self.stop_transfer.set()
                self.log(f"Пауза: {reason}" if reason else "Пауза по команде пользователя",
                         "warn" if reason else "info")

    def _supervise(self):
        for t in list(self.workers):
            t.join()
        with self.lock:
            if self.stop_transfer.is_set():
                self.transfer_state = "paused"
                self.log("Перенос остановлен. Нажмите «Старт», чтобы продолжить с того же места.")
            else:
                self.transfer_state = "done"
                st = self.db.stats()["by_status"]
                self.db.set("transfer.wanted", 0)
                self.log(f"Перенос завершён. Перенесено и сверено: {st.get('done', {}).get('count', 0)}, "
                         f"ошибок: {st.get('error', {}).get('count', 0)}")

    def _excluded_sources(self):
        mode = self.photounlim_mode()
        if mode == "" or (mode == "web" and self.web_problem):
            return ("photounlim",)
        return ()

    def _worker(self, wid):
        stop = self.stop_transfer
        while not stop.is_set():
            wait = self.cooldown_until - time.time()
            if wait > 0:
                stop.wait(min(wait, 5))
                continue
            excluded = self._excluded_sources()
            row = self.db.claim_next(excluded)
            if row is None:
                # Ждём, пока идёт получение списка или есть отложенные файлы (молчащие серверы Яндекса)
                if self.scanning() or self.db.has_waiting(excluded):
                    stop.wait(3)
                    continue
                return
            try:
                self._transfer_file(wid, row)
            except Paused:
                self.db.update_file(row["id"], status="pending")
            except StorageDown as e:
                self.db.update_file(row["id"], status="pending", next_try=time.time() + DEFER_SEC,
                                    last_error=str(e), note="ждёт сервер Яндекса")
                self.log(f"«{row['name']}»: {e}", "warn")
            except WebAuthError as e:
                self.db.update_file(row["id"], status="pending", last_error=str(e))
                if not self.web_problem:
                    self.web_problem = str(e)
                    self.log(f"Раздел «Фото» приостановлен: {e}. Остальные файлы продолжают переноситься.",
                             "error")
            except (AuthError, FatalPause) as e:
                self.db.update_file(row["id"], status="pending", last_error=str(e))
                self.pause_transfer(str(e), by_user=False)
            except RateLimited as e:
                self.db.update_file(row["id"], status="pending", last_error=str(e))
                self._rate_limited(e)
            except Exception as e:  # noqa
                attempts = row["attempts"] + 1
                fatal = isinstance(e, ApiError) and e.status in (404, 416)
                final = fatal or attempts >= self.settings["max_attempts"]
                delay = backoff(attempts, base=30, cap=1800)
                self.db.update_file(row["id"], status="error" if final else "pending",
                                    attempts=attempts, last_error=str(e)[:1000],
                                    next_try=time.time() + delay)
                self.log(f"Ошибка «{row['path']}»: {e}" +
                         ("" if final else f" — повтор через {delay / 60:.1f} мин (попытка {attempts})"),
                         "error" if final else "warn")
                if not isinstance(e, ApiError):
                    self.log(traceback.format_exc(), "debug")
            finally:
                self.active.pop(wid, None)

    def _rate_limited(self, e):
        with self.lock:
            self.rate_hits += 1
            delay = min(3600, 30 * (2 ** (self.rate_hits - 1)))
            self.cooldown_until = max(self.cooldown_until, time.time() + delay)
            extra = ""
            if delay >= 900 and "Google" in str(e):
                extra = (" Похоже на суточный лимит Google (≈750 ГБ загрузки в сутки на аккаунт) — "
                         "программа сама продолжит позже.")
            self.log(f"{e}. Общая пауза {delay // 60} мин {delay % 60} с.{extra}", "warn")

    def _add_speed(self, n):
        now = time.time()
        with self.lock:
            self.speed.append((now, n))
            while self.speed and self.speed[0][0] < now - 60:
                self.speed.popleft()
            self.rate_hits = 0

    def speed_bps(self):
        now = time.time()
        with self.lock:
            items = [x for x in self.speed if x[0] >= now - 30]
        if not items:
            return 0
        return sum(n for _, n in items) / max(5.0, now - items[0][0])

    # ---------------------------------------------------------------- один файл
    def _open_source(self, row, offset, y, w):
        if row["source"] == "photounlim" and self.photounlim_mode() == "web":
            return open_download(w.file_url(row["path"]), offset, sess=w.session(), reject_pages=True)
        return y.open_stream(row["path"], offset)

    def _find_existing(self, drive, parent, row, fresh=False):
        """Файл с тем же именем, размером и MD5 уже лежит в папке → его id."""
        if fresh:
            self.folder_index.drop(parent)
        for fid, md5, size in self.folder_index.get(drive, parent).get(row["name"], []):
            if size == row["size"] and (not row["md5"] or md5 == row["md5"]):
                return fid
        return None

    def _transfer_file(self, wid, row):
        s = self.settings
        size = row["size"]
        info = {"id": row["id"], "path": row["path"], "size": size, "done": row["bytes_done"],
                "stage": "подготовка", "started": time.time()}
        self.active[wid] = info
        drive = self.drive(self.stop_transfer)
        y = self.yandex(self.stop_transfer)
        w = self.yaweb(self.stop_transfer) if row["source"] == "photounlim" else None

        parent = drive.ensure_folder(drive_dir_for(row, s), s["drive_root"])
        props = {APP_PROP: "1"}
        if row["md5"]:
            props["ya_md5"] = row["md5"]

        if not row["upload_uri"] and s["skip_existing"]:
            info["stage"] = "проверка дублей"
            fid = self._find_existing(drive, parent, row, fresh=row["attempts"] > 0)
            if fid:
                self.db.update_file(row["id"], status="done", gd_id=fid, bytes_done=size,
                                    last_error=None, note="уже был на Drive",
                                    verified=time.time() if row["md5"] else None)
                return

        chunk = s["chunk_mb"] * 1024 * 1024
        if size <= chunk and not row["upload_uri"]:
            self._transfer_small(row, info, drive, parent, props, y, w)
        else:
            self._transfer_large(row, info, drive, parent, props, y, w, chunk)

    def _transfer_small(self, row, info, drive, parent, props, y, w):
        """Файл до размера куска: скачать в память, сверить MD5 с Яндексом, залить одним запросом."""
        size = row["size"]
        stop = self.stop_transfer
        for attempt in range(6):
            if stop.is_set():
                raise Paused()
            try:
                data = b""
                if size:
                    info["stage"] = "скачивание с Яндекса"
                    stream, _ = self._open_source(row, 0, y, w)
                    try:
                        data = read_exact(stream.raw, size, lambda n: info.__setitem__("done", n // 2))
                        extra = stream.raw.read(1)
                    finally:
                        stream.close()
                    if len(data) != size or extra:
                        raise ApiError(f"Яндекс отдал {len(data)}{'+' if extra else ''} байт вместо {size}",
                                       retryable=True)
                    if row["md5"] and hashlib.md5(data).hexdigest() != row["md5"]:
                        raise ApiError("MD5 скачанного не совпал с Яндексом — скачиваю заново", retryable=True)
                info["stage"] = "загрузка в Google"
                info["done"] = size // 2
                f = drive.upload_small(row["name"], parent, data, row["mime"], props, row["modified"])
                self._add_speed(size)
                self._finish(row, f, drive, parent)
                return
            except STOP_ERRORS:
                raise
            except (ApiError, *NETWORK_ERRORS) as e:
                if isinstance(e, ApiError) and not e.retryable:
                    raise
                # Ответ Google мог потеряться уже после создания файла — проверяем, чтобы не было дубля
                if info["stage"] == "загрузка в Google":
                    fid = self._find_existing(drive, parent, row, fresh=True)
                    if fid:
                        self.db.update_file(row["id"], status="done", gd_id=fid, bytes_done=size,
                                            last_error=None, note=None,
                                            verified=time.time() if row["md5"] else None)
                        return
                delay = backoff(attempt, base=3, cap=60)
                self.log(f"Сбой «{row['name']}»: {e} — повтор через {delay:.0f} c", "warn")
                if stop.wait(delay):
                    raise Paused()
        raise ApiError(f"«{row['name']}»: не удалось после нескольких попыток")

    def _transfer_large(self, row, info, drive, parent, props, y, w, chunk):
        """Большой файл: резюмируемая загрузка кусками; после сбоя — с подтверждённого Google байта."""
        size = row["size"]
        stop = self.stop_transfer
        uri = row["upload_uri"]
        if not uri:
            uri = drive.start_upload(row["name"], parent, size, row["mime"], props, row["modified"])
            self.db.update_file(row["id"], upload_uri=uri, bytes_done=0)
        offset = None
        stream = None
        errors = 0
        try:
            while True:
                if stop.is_set():
                    raise Paused()
                try:
                    if offset is None:
                        info["stage"] = "сверка с Google"
                        st, val = drive.upload_status(uri, size)
                        if st == "done":
                            self._finish(row, val, drive, parent)
                            return
                        if st == "gone":
                            self.log(f"Сессия загрузки истекла, начинаю файл заново: {row['path']}", "warn")
                            uri = drive.start_upload(row["name"], parent, size, row["mime"], props,
                                                     row["modified"])
                            val = 0
                        elif val:
                            self.log(f"Докачка «{row['name']}» с {val / 2**20:.1f} МБ")
                        offset = val
                        self.db.update_file(row["id"], upload_uri=uri, bytes_done=offset)
                        info["done"] = offset
                        if stream is not None:
                            stream.close()
                            stream = None
                    if stream is None:
                        info["stage"] = "открываю Яндекс"
                        stream, pos = self._open_source(row, offset, y, w)
                        if pos < offset:
                            skipped = read_exact(stream.raw, offset - pos)
                            if len(skipped) != offset - pos:
                                raise ApiError("Яндекс оборвал поток", retryable=True)
                    want = min(chunk, size - offset)
                    info["stage"] = "передача"
                    base = offset
                    buf = read_exact(stream.raw, want, lambda n: info.__setitem__("done", base + n))
                    if len(buf) != want:
                        raise ApiError(f"Яндекс оборвал поток ({len(buf)} из {want} байт)", retryable=True)
                    st, val = drive.put_chunk(uri, buf, offset, size)
                    if st == "done":
                        self._add_speed(len(buf))
                        self._finish(row, val, drive, parent)
                        return
                    if st == "gone":
                        offset = None
                        continue
                    if val - offset > 0:
                        self._add_speed(val - offset)
                    if val != offset + len(buf):
                        stream.close()
                        stream = None
                    offset = val
                    info["done"] = offset
                    self.db.update_file(row["id"], bytes_done=offset)
                    errors = 0
                except STOP_ERRORS:
                    raise
                except (ApiError, *NETWORK_ERRORS) as e:
                    if isinstance(e, ApiError) and not e.retryable:
                        raise
                    errors += 1
                    if errors > 8:
                        raise ApiError(f"Слишком много сбоев подряд: {e}")
                    delay = backoff(errors, base=3, cap=120)
                    self.log(f"Сбой при передаче «{row['name']}»: {e}. Продолжу с подтверждённого "
                             f"места через {delay:.0f} c", "warn")
                    if stream is not None:
                        stream.close()
                        stream = None
                    offset = None
                    if stop.wait(delay):
                        raise Paused()
        finally:
            if stream is not None:
                stream.close()

    def _finish(self, row, f, drive, parent):
        """Файл принят Google: сверяем MD5 и размер с Яндексом. Только после этого — «Готово»."""
        gmd5 = f.get("md5Checksum")
        if self.settings["verify_md5"] and row["md5"] and gmd5 and gmd5 != row["md5"]:
            drive.delete(f["id"])
            self.db.update_file(row["id"], upload_uri=None, bytes_done=0)
            raise ApiError(f"MD5 не совпал (Яндекс {row['md5']}, Google {gmd5}) — файл удалён с Drive, "
                           f"будет загружен заново", retryable=True)
        if f.get("size") is not None and int(f["size"]) != row["size"]:
            drive.delete(f["id"])
            self.db.update_file(row["id"], upload_uri=None, bytes_done=0)
            raise ApiError("Размер на Drive не совпал — файл будет загружен заново", retryable=True)
        self.folder_index.add(parent, row["name"], f["id"], gmd5, row["size"])
        self.db.update_file(row["id"], status="done", gd_id=f["id"], upload_uri=None,
                            bytes_done=row["size"], last_error=None, note=None,
                            verified=time.time() if (gmd5 and gmd5 == row["md5"]) else None)

    # ================================================================== служебные операции
    def busy(self):
        return any(t.is_alive() for t in self.workers) or bool(self.task_thread and self.task_thread.is_alive())

    def run_task(self, name, fn):
        with self.lock:
            if self.busy():
                raise ApiError("Сначала остановите перенос и дождитесь окончания текущей операции")
            self.task = {"name": name, "state": "running", "msg": ""}

            def wrap():
                try:
                    msg = fn()
                    self.task.update(state="done", msg=msg or "готово")
                    self.log(f"{name}: {msg or 'готово'}")
                except Exception as e:  # noqa
                    self.task.update(state="error", msg=str(e))
                    self.log(f"{name}: ошибка — {e}", "error")
            self.task_thread = threading.Thread(target=wrap, daemon=True)
            self.task_thread.start()

    def verify_drive(self):
        """Полная сверка: каждый «готовый» файл существует на Drive, не в корзине, и его размер
        и MD5 совпадают с Яндексом. Иначе — снова в очередь."""
        def fn():
            self.task["msg"] = "получаю с Google Drive список файлов с контрольными суммами…"
            files = self.drive().list_app_files()
            self.db.folders_clear()
            self.folder_index.clear()
            rows = self.db.q("SELECT id, gd_id, size, md5 FROM files WHERE status='done'")
            ok = missing = bad = 0
            now = time.time()
            for r in rows:
                f = files.get(r["gd_id"])
                if f is None:
                    missing += 1
                    self.db.update_file(r["id"], status="pending", gd_id=None, upload_uri=None, bytes_done=0,
                                        attempts=0, next_try=0, verified=None, note="не найден на Drive — повторно")
                elif int(f.get("size") or -1) != r["size"] or (r["md5"] and f.get("md5Checksum") != r["md5"]):
                    bad += 1
                    self.db.update_file(r["id"], status="pending", gd_id=None, upload_uri=None, bytes_done=0,
                                        attempts=0, next_try=0, verified=None,
                                        note="на Drive не совпал размер/MD5 — повторно")
                else:
                    ok += 1
                    self.db.update_file(r["id"], verified=now)
            return (f"сверено {len(rows)}: совпало по размеру и MD5 — {ok}, нет на Drive — {missing}, "
                    f"не совпало — {bad}. Несовпавшие и пропавшие возвращены в очередь")
        self.run_task("Сверка с Google Drive", fn)

    def report_csv(self):
        out = io.StringIO()
        wr = csv.writer(out, delimiter=";")
        wr.writerow(["Путь на Яндексе", "Размер, байт", "MD5 Яндекса", "Статус", "Проверено (MD5 совпал)",
                     "ID на Google Drive", "Ссылка", "Ошибка/примечание"])
        for r in self.db.q("SELECT path,size,md5,status,verified,gd_id,last_error,note FROM files ORDER BY path"):
            wr.writerow([r["path"], r["size"], r["md5"] or "", r["status"],
                         time.strftime("%Y-%m-%d %H:%M", time.localtime(r["verified"])) if r["verified"] else "",
                         r["gd_id"] or "",
                         f"https://drive.google.com/file/d/{r['gd_id']}/view" if r["gd_id"] else "",
                         r["last_error"] or r["note"] or ""])
        return "﻿" + out.getvalue()      # BOM — чтобы Excel открыл кириллицу

    def retry_errors(self):
        n = self.db.x("UPDATE files SET status='pending', attempts=0, next_try=0 WHERE status='error'")
        n2 = self.db.x("UPDATE files SET next_try=0 WHERE status='pending' AND next_try>?", (time.time(),))
        DEAD.clear()
        self.web_problem = ""
        self.log(f"В очередь возвращено: с ошибками {n}, отложенных {n2}")
        return n + n2

    def requeue_all(self):
        if self.busy():
            raise ApiError("Сначала остановите перенос")
        n = self.db.x("UPDATE files SET status='pending', attempts=0, next_try=0, gd_id=NULL, "
                      "upload_uri=NULL, bytes_done=0, last_error=NULL, note=NULL, verified=NULL")
        self.db.folders_clear()
        self.folder_index.clear()
        self.log(f"Все файлы ({n}) поставлены в очередь заново. Уже лежащие на Drive одинаковые файлы "
                 f"будут пропущены (сверка по имени, размеру и MD5).")
        return n

    def reset_db(self):
        if self.busy() or self.scanning():
            raise ApiError("Сначала остановите перенос и получение списка")
        self.db.x("DELETE FROM files")
        self.db.folders_clear()
        self.folder_index.clear()
        for k in ("scan.state", "scan.phase", "scan.offset", "scan.found", "scan.error"):
            self.db.set(k, None)
        self.log("База очищена. Нужно заново получить список файлов.")

    def file_action(self, fid, action):
        if action == "retry":
            self.db.update_file(fid, status="pending", attempts=0, next_try=0)
        elif action == "skip":
            self.db.x("UPDATE files SET status='skipped', note='пропущен вручную' "
                      "WHERE id=? AND status!='active'", (fid,))
        elif action == "requeue":
            self.db.x("UPDATE files SET status='pending', attempts=0, next_try=0, gd_id=NULL, "
                      "upload_uri=NULL, bytes_done=0, verified=NULL WHERE id=? AND status!='active'", (fid,))

    # ================================================================== статус для UI
    def status(self):
        st = self.db.stats()
        sp = self.speed_bps()
        bs = st["by_status"]
        total_b = sum(v["bytes"] for v in bs.values())
        done_b = bs.get("done", {}).get("bytes", 0) + bs.get("skipped", {}).get("bytes", 0)
        left = total_b - done_b - bs.get("error", {}).get("bytes", 0) - st["partial_bytes"]
        db = self.db
        return {
            "transfer": {"state": self.transfer_state, "reason": self.pause_reason,
                         "cooldown_until": self.cooldown_until if self.cooldown_until > time.time() else 0,
                         "workers": sum(t.is_alive() for t in self.workers)},
            "scan": {"running": self.scanning(), "state": db.get("scan.state") or "never",
                     "phase": db.get("scan.phase"), "offset": int(db.get("scan.offset") or 0),
                     "found": int(db.get("scan.found") or 0), "error": db.get("scan.error") or ""},
            "stats": st,
            "active": sorted(self.active.values(), key=lambda a: a["started"]),
            "speed": sp,
            "eta": (left / sp) if sp > 0 else None,
            "task": self.task,
            "configured": {"yandex": bool(self.settings["yandex_token"]),
                           "cookies": bool(self.settings["yandex_cookies"]),
                           "google": bool(self.settings["google_refresh_token"]),
                           "google_client": bool(self.settings["google_client_id"])},
            "google_email": self.settings["google_email"],
            "photounlim": {"mode": self.photounlim_mode(), "problem": self.web_problem,
                           "size": int(db.get("photounlim.size") or 0)},
            "dead_hosts": DEAD.active(),
        }
