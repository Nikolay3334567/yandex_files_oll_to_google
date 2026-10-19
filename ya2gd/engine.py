"""Движок: список файлов Яндекс Диска и перенос на Google Drive.

Два конвейера, связанных буфером:
  Яндекс → [скачивание] → буфер → [загрузка] → Google Drive
- «Скачивание» — потоки к Яндексу; их число подбирается само (throttle.AdaptiveGate), чтобы не
  перегружать серверы Яндекса, работающие в аварийном режиме, но брать максимум того, что они отдают.
- «Загрузка» — отдельные потоки к Google. Пока Google тормозит (суточный лимит ≈750 ГБ),
  скачивание с Яндекса продолжается, пока есть место в буфере.
- Буфер: без папки на диске — маленькие файлы ждут в памяти (до RAM_QUEUE_MB); с папкой на диске
  (например, D:\\буфер, лимит в ГБ) — файлы ждут там. Недокачанный файл докачивается с места обрыва,
  MD5 сверяется с Яндексом ДО загрузки в Google. Файл больше буфера идёт напрямую
  Яндекс → Google кусками с докачкой (как и все большие файлы без буфера).

Надёжность:
- состояние в SQLite (synchronous=FULL); после падения или выключения света всё продолжается;
- большой файл продолжается с байта, который подтвердил Google (резюмируемая сессия);
- «Готово» — только когда на Google тот же размер и MD5, что на Яндексе;
- молчащий сервер Яндекса: файл откладывается без траты попыток, позже повторяется сам;
  если оригинал долго недоступен — сохраняется хотя бы превью (отдельная папка на Drive);
- нет интернета: попытки не тратятся, программа ждёт и продолжает сама;
- дубли исключены: перед загрузкой проверяется, нет ли уже в папке файла с тем же именем и MD5.
"""
import collections
import csv
import hashlib
import io
import json
import os
import re
import shutil
import threading
import time
import traceback

from . import winsys
from .core import Log, Settings, data_dir
from .db import DB, FOREVER
from .fetch import DEAD, StorageDown, open_download
from .gdrive import API as GOOGLE_API, APP_PROP, DriveClient, GoogleAuth
from .http_util import (ApiError, AuthError, FatalPause, LocalDiskError, NETWORK_ERRORS, RateLimited,
                        WebAuthError, backoff, session)
from .throttle import AdaptiveGate
from .yandex import API as YANDEX_API, YandexClient
from .yaweb import DISK_ROOT, UNLIM_ROOT, YandexWeb

GB = 2 ** 30
MB = 2 ** 20
BUF_UNIT = int(os.environ.get("YA2GD_BUFFER_UNIT", str(GB)))        # «гигабайт» буфера (в тестах — меньше)
YA_COOLDOWN = float(os.environ.get("YA2GD_YA_COOLDOWN", "60"))      # первая пауза после «слишком часто»
DEFER_SEC = float(os.environ.get("YA2GD_DEAD_HOST_TTL", "1800"))
DEFER_CAP = 6 * 3600
PREVIEW_AFTER = int(os.environ.get("YA2GD_PREVIEW_AFTER", "2"))   # после стольких откладываний — превью
RAM_QUEUE_MB = int(os.environ.get("YA2GD_RAM_QUEUE_MB", "256"))
STOP_ERRORS = (AuthError, FatalPause, RateLimited, StorageDown, WebAuthError, LocalDiskError)
MEDIA_EXT = {"image/jpeg": ".jpg", "image/png": ".png", "image/webp": ".webp", "image/heic": ".heic",
             "image/avif": ".avif"}


class Paused(Exception):
    pass


class Mismatch(ApiError):
    """Google принял файл, но размер/MD5 не тот: файл удалён с Drive. Следующая попытка — с нуля,
    новой сессией загрузки (а не дозаливкой в старую)."""


def name_date(name):
    """Дата из имени вида «2019-05-04 10-00-00.JPG» / «IMG_20190504_…»."""
    m = re.search(r"(?<!\d)((?:19|20)\d\d)[-_.]?(0[1-9]|1[0-2])[-_.]?(0[1-9]|[12]\d|3[01])", name or "")
    return f"{m.group(1)}-{m.group(2)}-{m.group(3)}" if m else None


# Папка автозагрузки с телефона, когда безлимит выключен: эти фото занимают место на Диске,
# лежат в «Файлах», но на сайте Яндекса видны и в разделе «Фото».
CAMERA_DIRS = ("Фотокамера", "Camera Uploads")
MEDIA_EXTS = ("jpg", "jpeg", "png", "heic", "heif", "webp", "gif", "bmp", "tif", "tiff", "dng", "cr2", "nef",
              "arw", "raw", "mp4", "mov", "3gp", "avi", "mkv", "m4v", "mts", "m2ts", "wmv", "mpg", "mpeg")
PHOTOS_FROM_DISK = ("camera", "all", "none")


def _rel(row):
    path = row["path"]
    return path.split(":", 1)[1].lstrip("/") if ":" in path else path.lstrip("/")


def is_media(row):
    mt = (row.get("media_type") or "").lower()
    mime = (row.get("mime") or "").lower()
    name = (row.get("name") or "").lower()
    return mt in ("image", "video") or mime.startswith(("image/", "video/")) or \
        ("." in name and name.rsplit(".", 1)[1] in MEDIA_EXTS)


def goes_to_photos(row, s):
    """Куда файл: в «Фото» (по датам) или в «Файлы» (в своих папках).
    Безлимит — всегда в «Фото». Фото и видео из обычных папок — по настройке photos_from_disk:
    camera — из «Фотокамеры», all — отовсюду, none — никогда."""
    if row["source"] == "photounlim":
        return True
    mode = s["photos_from_disk"]
    if mode == "none" or not is_media(row):
        return False
    if mode == "all":
        return True
    rel = _rel(row)
    return "/" in rel and rel.split("/", 1)[0] in CAMERA_DIRS


def photos_sql(mode):
    """То же правило, что goes_to_photos, в виде условия SQL — для подсчёта прогресса по папкам."""
    media = ("(lower(IFNULL(media_type,'')) IN ('image','video') OR lower(IFNULL(mime,'')) LIKE 'image/%' "
             "OR lower(IFNULL(mime,'')) LIKE 'video/%' OR " +
             " OR ".join(f"lower(name) LIKE '%.{e}'" for e in MEDIA_EXTS) + ")")
    if mode == "none":
        disk = "0"
    elif mode == "all":
        disk = media
    else:
        # GLOB, а не LIKE: с учётом регистра, как и проверка в goes_to_photos
        disk = "(" + " OR ".join(f"path GLOB 'disk:/{d}/*'" for d in CAMERA_DIRS) + f") AND {media}"
    return f"(source='photounlim' OR (source='disk' AND {disk}))"


def drive_dir_for(row, s):
    """Полный путь папки на Drive для файла.
    В «Фото»: <photos_root>/<год>/<год-месяц>; в «Файлы»: <files_root>/<те же папки, что на Яндексе>."""
    rel = _rel(row)
    sub = rel.rsplit("/", 1)[0] if "/" in rel else ""
    if not goes_to_photos(row, s):
        return f"{s['files_root']}/{sub}" if sub else s["files_root"]
    if row["source"] == "disk":
        # из «Фотокамеры» — с подпапками внутри неё; из любых папок (режим all) — только по дате
        top, _, inner = sub.partition("/")
        sub = inner if top in CAMERA_DIRS else ""
    base = s["photos_root"]
    date = (row.get("taken") or name_date(row.get("name")) or row.get("created") or
            row.get("modified") or "")[:10]
    split = s["photos_split"]
    if len(date) == 10 and date[4] == "-":
        if split == "year":
            base += "/" + date[:4]
        elif split == "month":
            base += f"/{date[:4]}/{date[:7]}"
    return f"{base}/{sub}" if sub else base


def is_network(e):
    return isinstance(e, NETWORK_ERRORS) or (isinstance(e, ApiError) and e.retryable and e.status is None)


def read_exact(raw, n, on_progress=None):
    buf = bytearray()
    while len(buf) < n:
        part = raw.read(min(1 << 20, n - len(buf)))
        if not part:
            break
        buf += part
        if on_progress:
            on_progress(len(buf), len(part))
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


class MemQueue:
    """Очередь скачанных в память маленьких файлов, ограниченная по объёму."""

    def __init__(self, limit):
        self.limit = limit
        self.items = collections.deque()
        self.bytes = 0
        self.cond = threading.Condition()

    def put(self, item, n, stop, front=False, force=False):
        with self.cond:
            while not force and self.bytes and self.bytes + n > self.limit:
                if stop.is_set():
                    return False
                self.cond.wait(1.0)
            (self.items.appendleft if front else self.items.append)((item, n))
            self.bytes += n
            return True

    def get(self):
        with self.cond:
            if not self.items:
                return None
            item, n = self.items.popleft()
            self.bytes -= n
            self.cond.notify_all()
            return item

    def drain(self):
        with self.cond:
            out = [it for it, _ in self.items]
            self.items.clear()
            self.bytes = 0
            self.cond.notify_all()
            return out

    def __len__(self):
        return len(self.items)


class Buffer:
    """Папка буфера на диске компьютера. Файлы программы лежат в подпапке ya2gd-buffer."""

    def __init__(self, base):
        self.base = os.path.abspath(base)
        self.dir = os.path.join(self.base, "ya2gd-buffer")

    def prepare(self):
        try:
            os.makedirs(self.dir, exist_ok=True)
            probe = os.path.join(self.dir, ".probe")
            with open(probe, "wb") as f:
                f.write(b"ok")
            os.remove(probe)
        except OSError as e:
            raise LocalDiskError(f"Папка буфера «{self.base}» недоступна для записи: {e}")

    def part(self, fid):
        return os.path.join(self.dir, f"{fid}.part")

    def final(self, fid):
        return os.path.join(self.dir, f"{fid}.dat")

    def free(self):
        try:
            return shutil.disk_usage(self.dir).free
        except OSError:
            return 0

    def written(self):
        total = 0
        try:
            for e in os.scandir(self.dir):
                if e.is_file():
                    total += e.stat().st_size
        except OSError:
            pass
        return total

    def files(self):
        try:
            return [e.path for e in os.scandir(self.dir) if e.is_file() and e.name.endswith((".part", ".dat"))]
        except OSError:
            return []


def _date_of(v):
    """Дата 'ГГГГ-ММ-ДД' из строки ISO или секунд/миллисекунд Unix; None, если не разобрать."""
    if v in (None, ""):
        return None
    if isinstance(v, (int, float)):
        return time.strftime("%Y-%m-%d", time.gmtime(v / 1000 if v > 1e12 else v))
    m = re.match(r"\d{4}-\d\d-\d\d", str(v))
    return m.group(0) if m else None


def _dur(sec):
    sec = max(0, sec)
    if sec < 120:
        return f"{sec:.0f} с"
    if sec < 7200:
        return f"{sec / 60:.0f} мин"
    return f"{sec / 3600:.1f} ч"


def _remove(path):
    if path:
        try:
            os.remove(path)
        except OSError:
            pass


class Engine:
    def __init__(self, ddir=None):
        self.dir = ddir or data_dir()
        self.settings = Settings(os.path.join(self.dir, "settings.json"))
        self.log = Log(os.path.join(self.dir, "ya2gd.log"))
        self.db = DB(os.path.join(self.dir, "state.sqlite3"))
        if self.settings.migrated:
            self.log("Настройки прежней версии перенесены: папки на Drive остаются прежними")
        n = self.db.reset_active()
        if n:
            self.log(f"Программа была остановлена во время переноса: {n} файл(ов) вернулись в очередь, "
                     f"незавершённые загрузки продолжатся с места обрыва", "warn")
        if self.db.get("scan.state") == "running":
            self.db.set("scan.state", "interrupted")
        self._keep_started_layout()
        self.sync_sections()

        self.lock = threading.RLock()
        self.buffer_lock = threading.Lock()
        self.stop_transfer = threading.Event()
        self.stop_scan = threading.Event()
        self.downloaders, self.uploaders = [], []
        self.supervisor = None
        self.transfer_state = "idle"     # idle | running | pausing | paused | done | attention
        self.pause_reason = ""
        self.active = {}
        self.ya_cooldown_until = self.g_cooldown_until = 0
        self.ya_hits = self.g_hits = 0
        self.speed = {"yandex": collections.deque(), "google": collections.deque()}
        self.offline_since = 0
        self._net_checked = 0
        self.scan_thread = None
        self.task = {"name": "", "state": "idle", "msg": ""}
        self.task_thread = None
        self.google_state = os.urandom(16).hex()
        self._auth = None
        self._auth_key = None
        self.web_problem = ""            # куки веб-версии не работают — ждёт только то, что идёт через неё
        self.rest_broken = ""            # REST API не отдаёт файлы (410) — «Файлы» идут через веб-версию
        self.folder_index = FolderIndex()
        self.gate = AdaptiveGate(self.settings["yandex_streams_max"], adaptive=self.settings["yandex_adaptive"],
                                 log=self.log)
        self.memq = MemQueue(RAM_QUEUE_MB * MB)
        self.buffer = None
        self.g_quota = {}
        self._quota_at = 0

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
               "trash": info.get("trash_size"),
               "photounlim_size": int(info.get("photounlim_size") or 0),
               # Безлимит не занимает место только при тарифе Яндекс 360; без тарифа он входит в «занято».
               # Эти поля API отдаёт не всем — если их нет, просто не показываем.
               "is_paid": info.get("is_paid"),
               "unlim_end": info.get("free_photounlim_end_date"),
               "will_be_overdrawn": info.get("will_be_overdrawn")}
        self.db.set("disk.info", json.dumps({k: res[k] for k in ("total", "used", "photounlim_size", "is_paid",
                                                                  "unlim_end", "will_be_overdrawn")} |
                                            {"at": time.time()}))
        try:
            files, _ = y.files_page(0, 1)
            res["files_api"] = "работает"
            if files:
                y.download_href(files[0]["path"], tries=2)
                res["files_api"] += ", ссылки на скачивание выдаются"
            self.rest_broken = ""
        except ApiError as e:
            res["files_api"] = f"ошибка: {e}"
            if e.status == 410:
                res["files_api"] += (" — Яндекс не отдаёт «Файлы» через API. Добавьте куки (блок 3): "
                                     "программа возьмёт «Файлы» через веб-версию Диска")
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
            try:
                w.list_page(DISK_ROOT, 0, 1)
                msg += "; «Файлы» через веб-версию тоже видны"
            except ApiError as e:
                msg += f"; «Файлы» через веб-версию: {e}"
        except WebAuthError as e:
            self.web_problem = str(e)
            raise
        self.web_problem = ""
        self.log(f"Проверка кук Яндекса: {msg}")
        return {"ok": True, "uid": info["uid"], "msg": msg}

    def test_google(self):
        d = DriveClient(self.google_auth(), db=self.db, log=self.log)
        about = d.about()
        root = d.ensure_folder(self.settings["files_root"])
        f = d.create_empty("_ya2gd_test.txt", root, "text/plain", {APP_PROP: "test"})
        d.delete(f["id"])
        q = about.get("storageQuota") or {}
        res = {"ok": True, "email": (about.get("user") or {}).get("emailAddress"),
               "limit": int(q["limit"]) if q.get("limit") else None,
               "usage": int(q.get("usage") or 0), "write": "запись в папку проверена"}
        self.g_quota = {"limit": res["limit"], "usage": res["usage"], "at": time.time()}
        if res["email"] and res["email"] != self.settings["google_email"]:
            self.settings.update(google_email=res["email"])
        self.log(f"Проверка Google: {res}")
        return res

    # ================================================================== список файлов
    def start_scan(self, fresh=False):
        with self.lock:
            if self.scan_thread and self.scan_thread.is_alive():
                return False
            if not self.settings["yandex_token"] and not self.settings["yandex_cookies"]:
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
                if s["include_disk"]:
                    self._scan_disk(y, scan_id)
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
            st = self._stats()
            total = sum(v["count"] for v in st["by_status"].values())
            self.log(f"Список получен. Всего файлов: {total} "
                     f"({sum(v['bytes'] for v in st['by_status'].values()) / GB:.1f} ГБ)")
        except Paused:
            db.set("scan.state", "stopped")
            self.log("Получение списка остановлено, его можно продолжить", "warn")
        except Exception as e:  # noqa
            db.set("scan.state", "interrupted")
            db.set("scan.error", str(e))
            if isinstance(e, WebAuthError):
                self.web_problem = str(e)
            self.log(f"Получение списка прервано ошибкой: {e}. Нажмите «Получить список / продолжить» — "
                     f"он продолжится с того же места", "error")
            self.log(traceback.format_exc(), "debug")

    def _page(self, fn, *args):
        """Одна страница списка с терпеливыми повторами: Яндекс в аварийном режиме часто отвечает не сразу."""
        for attempt in range(12):
            if self.stop_scan.is_set():
                raise Paused()
            try:
                return fn(*args)
            except RateLimited as e:
                delay = min(1800, 60 * 2 ** attempt)
                self.log(f"{e}. Список: пауза {delay // 60} мин", "warn")
            except (AuthError, WebAuthError):
                raise
            except ApiError as e:
                if not e.retryable or attempt >= 6:
                    raise
                delay = backoff(attempt, base=10, cap=600)
                self.log(f"Список: {e} — повтор через {delay:.0f} c", "warn")
            if self.stop_scan.wait(delay):
                raise Paused()
        raise ApiError("Список: Яндекс не отвечает слишком долго")

    def _found(self, files, scan_id):
        if files:
            for f in files:
                f["section"] = 1 if goes_to_photos(f, self.settings) else 0
            self.db.upsert_files(files, scan_id)
            self.db.set("scan.found", int(self.db.get("scan.found", "0")) + len(files))

    def _scan_disk(self, y, scan_id):
        """«Файлы»: плоский список (REST), обход папок (REST) или через веб-версию; при 410 — сам на веб."""
        mode = self.settings["scan_mode"]
        if mode != "web" and self.settings["yandex_token"]:
            try:
                if mode == "tree":
                    self._scan_tree(y, "disk:/", scan_id)
                else:
                    self._scan_flat(y, scan_id)
                self.rest_broken = ""
                return
            except (Paused, AuthError):
                raise
            except ApiError as e:
                if not self.settings["yandex_cookies"]:
                    raise ApiError(f"{e}. Яндекс не отдаёт «Файлы» через API — добавьте куки (блок 3), "
                                   f"и программа возьмёт их через веб-версию Диска", status=e.status)
                self.rest_broken = str(e)
                self.log(f"API Яндекса не отдаёт список «Файлов» ({e}) — беру его через веб-версию Диска", "warn")
        self._scan_web(DISK_ROOT, scan_id, "«Файлы»", resume=False)

    def _scan_flat(self, y, scan_id):
        """GET /v1/disk/resources/files — все файлы Диска одним плоским списком."""
        limit = 1000
        offset = int(self.db.get("scan.offset", "0"))
        while not self.stop_scan.is_set():
            files, raw = self._page(y.files_page, offset, limit)
            self._found(files, scan_id)
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
                files, dirs, total, raw = self._page(y.dir_page, path, offset, 1000)
                self._found(files, scan_id)
                n += len(files)
                stack.extend(dirs)
                offset += raw
                if raw == 0 or offset >= total:
                    break
            self.log(f"Обход {root}: {path} — найдено {n}")

    def _scan_web(self, root, scan_id, title, resume=True):
        """Через веб-версию: mpfs/list постранично, вложенные папки — следом."""
        w = YandexWeb(self.settings["yandex_cookies"], stop_event=self.stop_scan, log=self.log)
        queue = [root]
        offset = int(self.db.get("scan.offset", "0")) if resume else 0
        n = 0
        while queue and not self.stop_scan.is_set():
            path = queue.pop(0)
            while not self.stop_scan.is_set():
                files, dirs, raw = self._page(w.list_page, path, offset, 1000)
                self._found(files, scan_id)
                n += len(files)
                queue.extend(dirs)
                offset += raw
                if path == root and resume:
                    self.db.set("scan.offset", offset)
                if raw < 1000:
                    break
                self.log(f"Список {title}: получено {n}…")
            offset = 0
        self.web_problem = ""
        self.log(f"{title}: в списке {n} файлов")

    def _scan_photounlim(self, y, scan_id):
        """Раздел «Фото» (безлимит): плоский список через веб-API (mpfs/list) или REST."""
        if self.settings["yandex_cookies"]:
            self._scan_web(UNLIM_ROOT, scan_id, "Раздел «Фото»")
            return
        method, report = y.probe_photounlim()
        self.db.set("photounlim.method", method or "")
        if method == "tree":
            self._scan_tree(y, "photounlim:/", scan_id)
        elif method == "flat":
            offset = int(self.db.get("scan.offset", "0"))
            while not self.stop_scan.is_set():
                files, raw = self._page(y.photounlim_flat_page, offset, 1000)
                self._found(files, scan_id)
                offset += raw
                self.db.set("scan.offset", offset)
                if raw < 1000:
                    break
        else:
            size = int(self.db.get("photounlim.size") or 0)
            if size:
                self.log(f"Раздел «Фото» ({size / GB:.1f} ГБ) пропущен: официальный API Яндекса его не "
                         f"отдаёт. Добавьте куки в блоке 3 и получите список ещё раз.", "warn")

    def stop_scanning(self):
        self.stop_scan.set()

    def scanning(self):
        return bool(self.scan_thread and self.scan_thread.is_alive())

    # ================================================================== перенос: запуск/остановка
    def start_transfer(self):
        with self.lock:
            if self.transferring():
                return False
            s = self.settings
            if not s["yandex_token"] and not s["yandex_cookies"]:
                raise AuthError("Сначала укажите токен Яндекса")
            if not s["google_refresh_token"]:
                raise AuthError("Сначала авторизуйтесь в Google")
            self.buffer = None
            if s["buffer_dir"]:
                b = Buffer(s["buffer_dir"])
                b.prepare()
                self.buffer = b
            self._recover_buffer()
            self.stop_transfer.clear()
            self.transfer_state = "running"
            self.pause_reason = ""
            self.ya_cooldown_until = self.g_cooldown_until = 0
            self.offline_since = 0
            self.web_problem = ""
            self.db.set("transfer.wanted", 1)
            self.gate.configure(s["yandex_streams_max"], s["yandex_adaptive"])
            self.downloaders = [threading.Thread(target=self._downloader, args=(i,), daemon=True, name=f"ya{i}")
                                for i in range(s["yandex_streams_max"])]
            self.uploaders = [threading.Thread(target=self._uploader, args=(i,), daemon=True, name=f"g{i}")
                              for i in range(s["google_streams"])]
            for t in self.downloaders + self.uploaders:
                t.start()
            self.supervisor = threading.Thread(target=self._supervise, daemon=True, name="supervisor")
            self.supervisor.start()
            buf = (f"буфер: {self.buffer.base} (до {s['buffer_gb']} ГБ)" if self.buffer
                   else f"без буфера на диске (до {RAM_QUEUE_MB} МБ в памяти)")
            self.log(f"Перенос запущен: к Яндексу до {s['yandex_streams_max']} потоков "
                     f"({'подбираются автоматически' if s['yandex_adaptive'] else 'фиксировано'}), "
                     f"к Google — {s['google_streams']}; {buf}")
            return True

    def transferring(self):
        return any(t.is_alive() for t in self.downloaders + self.uploaders) or \
            bool(self.supervisor and self.supervisor.is_alive())

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

    def _recover_buffer(self):
        """После перезапуска: файлы в буфере сверяются с базой, лишнее удаляется."""
        known = set()
        for r in self.db.q("SELECT id, status, local_path FROM files WHERE local_path IS NOT NULL"):
            lp = r["local_path"]
            if os.path.exists(lp):
                known.add(os.path.normcase(os.path.abspath(lp)))
                continue
            if r["status"] in ("buffered", "uploading"):
                self.db.update_file(r["id"], status="pending", local_path=None)
                self.log(f"Файл пропал из буфера ({lp}) — будет скачан заново", "warn")
            else:
                self.db.update_file(r["id"], local_path=None)
        if self.buffer:
            for p in self.buffer.files():
                if os.path.normcase(os.path.abspath(p)) not in known:
                    _remove(p)

    def _supervise(self):
        stop = self.stop_transfer
        last_keepalive = 0
        while any(t.is_alive() for t in self.downloaders + self.uploaders):
            now = time.time()
            try:
                self.gate.tick(self.db.has_ready(self._excluded_sources(), self._retry_errors()))
                if now - last_keepalive > 30:
                    winsys.keep_awake()
                    last_keepalive = now
                if now - self._quota_at > 900:
                    self._quota_at = now
                    self._refresh_quota()
                if winsys.WIN:
                    winsys.set_title(self._title())
            except Exception as e:  # noqa
                self.log(f"Наблюдатель: {e}", "debug")
            stop.wait(2)
        for row, _ in self.memq.drain():
            self.db.update_file(row["id"], status="pending")
        with self.lock:
            if stop.is_set():
                self.transfer_state = "paused"
                self.log("Перенос остановлен. Нажмите «Старт», чтобы продолжить с того же места.")
            else:
                st = self._stats()["by_status"]
                left = self.db.q1("SELECT COUNT(*) FROM files WHERE status IN ('pending','error') "
                                  "AND next_try<?", (FOREVER,))[0]
                if left and self._excluded_sources():
                    self.transfer_state = "attention"
                    self.pause_reason = (self.web_problem or "раздел «Фото» не настроен") + \
                        f" — {left} файл(ов) ждут. Обновите куки и нажмите «Старт»."
                    self.log(f"Перенесено всё, что доступно. {self.pause_reason}", "warn")
                else:
                    self.transfer_state = "done"
                    self.db.set("transfer.wanted", 0)
                    self.log(f"Перенос завершён. Перенесено и сверено: {st.get('done', {}).get('count', 0)}, "
                             f"ошибок: {st.get('error', {}).get('count', 0)}")
        winsys.allow_sleep()
        if winsys.WIN:
            winsys.set_title(self._title())

    def _stats(self):
        return self.db.stats()

    def _keep_started_layout(self):
        """Обновление с версии 2: тогда «Фотокамера» шла в «Файлы». Если оттуда уже что-то перенесено,
        прежнюю раскладку сохраняем, чтобы одна папка не оказалась разложена в два места."""
        keys = self.settings.stored_keys
        if keys is None or "photos_from_disk" in keys:
            return
        cam = " OR ".join(f"path GLOB 'disk:/{d}/*'" for d in CAMERA_DIRS)
        started = self.db.q1(f"SELECT 1 FROM files WHERE source='disk' AND status='done' AND ({cam}) LIMIT 1")
        self.settings.update(photos_from_disk="none" if started else self.settings["photos_from_disk"])
        self.settings.stored_keys.add("photos_from_disk")
        if started:
            self.log("Часть «Фотокамеры» уже перенесена в «Файлы» прежней версией — оставляю ту же раскладку. "
                     "Изменить можно в «Настройках».", "warn")

    def sync_sections(self):
        """Раздел («Фото»/«Файлы») каждого файла хранится в базе; пересчитать после смены настройки."""
        mode = self.settings["photos_from_disk"]
        if self.db.get("section.mode") != mode:
            n = self.db.recompute_sections(photos_sql(mode))
            self.db.set("section.mode", mode)
            if n:
                self.log(f"Раскладка фото пересчитана для {n} файлов")

    def _refresh_quota(self):
        try:
            q = self.drive(self.stop_transfer).about().get("storageQuota") or {}
            self.g_quota = {"limit": int(q["limit"]) if q.get("limit") else None,
                            "usage": int(q.get("usage") or 0), "at": time.time()}
        except Exception as e:  # noqa
            self.log(f"Не удалось узнать место на Google Drive: {e}", "debug")

    def _title(self):
        st = self._stats()
        bs = st["by_status"]
        total = sum(v["bytes"] for v in bs.values()) or 1
        done = bs.get("done", {}).get("bytes", 0) + bs.get("skipped", {}).get("bytes", 0)
        state = {"running": "идёт перенос", "paused": "пауза", "done": "готово", "pausing": "останавливаюсь",
                 "attention": "нужно внимание"}.get(self.transfer_state, "ожидание")
        return f"{done / total * 100:.1f}% · {done / GB:.1f} из {total / GB:.1f} ГБ · {state} — Яндекс → Google"

    # ================================================================== общие условия для потоков
    def _excluded_sources(self):
        out = []
        mode = self.photounlim_mode()
        if mode == "" or (mode == "web" and self.web_problem):
            out.append("photounlim")
        if self.rest_broken and self.web_problem:
            out.append("disk")
        if not self.settings["yandex_token"] and not self.settings["yandex_cookies"]:
            out.append("disk")
        return tuple(out)

    def _retry_errors(self):
        return self.settings["auto_retry_hours"] > 0

    def _work_left(self):
        if self.scanning() or len(self.memq):
            return True
        if self.db.count_status("active", "buffered", "uploading"):
            return True
        return self.db.has_waiting(self._excluded_sources(), self._retry_errors())

    def _hold(self, stop, side):
        """True — сейчас этой стороне работать нельзя (нет интернета, Яндекс/Google просил паузу); подождали."""
        now = time.time()
        if self.offline_since:
            if now - self._net_checked > 15:
                self._net_checked = now
                if self._online():
                    mins = (now - self.offline_since) / 60
                    self.offline_since = 0
                    self.log(f"Интернет снова есть (не было {mins:.0f} мин) — продолжаю")
                    return False
            stop.wait(3)
            return True
        cd = self.ya_cooldown_until if side == "yandex" else self.g_cooldown_until
        if cd > now:
            stop.wait(min(cd - now, 3))
            return True
        return False

    def _online(self):
        for url in (YANDEX_API, GOOGLE_API):
            try:
                session().get(url, timeout=(8, 8)).close()
                return True
            except Exception:  # noqa
                continue
        return False

    def _went_offline(self):
        with self.lock:
            if not self.offline_since:
                self.offline_since = time.time()
                self._net_checked = time.time()
                self.log("Похоже, пропал интернет. Жду, пока он появится; попытки при этом не тратятся.", "warn")

    def _yandex_limited(self, e):
        with self.lock:
            self.ya_hits += 1
            delay = int(min(1800, YA_COOLDOWN * 2 ** (self.ya_hits - 1)))
            self.ya_cooldown_until = max(self.ya_cooldown_until, time.time() + delay)
        self.gate.throttled()
        self.log(f"{e}. Скачивание с Яндекса — пауза {delay // 60} мин {delay % 60} с "
                 f"(загрузка в Google продолжается)", "warn")

    def _google_limited(self, e):
        with self.lock:
            self.g_hits += 1
            delay = min(3600, 30 * 2 ** (self.g_hits - 1))
            self.g_cooldown_until = max(self.g_cooldown_until, time.time() + delay)
        extra = ""
        if delay >= 900:
            extra = (" Похоже на суточный лимит Google (≈750 ГБ загрузки в сутки на аккаунт) — "
                     "программа сама продолжит позже.")
        self.log(f"{e}. Загрузка в Google — пауза {delay // 60} мин {delay % 60} с "
                 f"(скачивание с Яндекса продолжается, пока есть место в буфере).{extra}", "warn")

    def _add_speed(self, side, n):
        now = time.time()
        with self.lock:
            dq = self.speed[side]
            dq.append((now, n))
            while dq and dq[0][0] < now - 60:
                dq.popleft()
            if side == "yandex":
                self.ya_hits = 0
            else:
                self.g_hits = 0

    def speed_bps(self, side):
        now = time.time()
        with self.lock:
            items = [x for x in self.speed[side] if x[0] >= now - 30]
        if not items:
            return 0
        return sum(n for _, n in items) / max(5.0, now - items[0][0])

    def _defer_delay(self, defers):
        return min(DEFER_CAP, DEFER_SEC * 2 ** (defers // 2))

    # ================================================================== скачивание с Яндекса
    def _downloader(self, i):
        key = f"d{i}"
        stop = self.stop_transfer
        while not stop.is_set():
            if self._hold(stop, "yandex"):
                continue
            if not self.gate.acquire(stop):
                break
            row = None
            try:
                row = self._claim_download()
                if row is not None:
                    self._run(key, row, "yandex", self._download_one)
            finally:
                self.gate.release()           # слот занят только пока файл реально качается
            if row is None:
                if not self._work_left():
                    return
                stop.wait(3)

    def _claim_download(self):
        s = self.settings
        excluded, retry = self._excluded_sources(), self._retry_errors()
        if not self.buffer:
            return self.db.claim_download(excluded, s["order"], retry_errors=retry)
        with self.buffer_lock:
            avail, cap = self._buffer_avail()
            # Сначала — недокачанные в буфер: им нужно только недостающее место, а не весь размер
            for r in self.db.partials(excluded, retry):
                try:
                    have = os.path.getsize(r["local_path"])
                except OSError:
                    have = 0
                if r["size"] - have <= max(avail, 0) and r["size"] <= cap:
                    row = self.db.claim_id(r["id"])
                    if row:
                        return row
            row = self.db.claim_download(excluded, s["order"], fits=(avail, cap), retry_errors=retry)
            if row and row["size"] <= cap and not row["local_path"] and not row["upload_uri"]:
                row["local_path"] = self.buffer.part(row["id"])
                self.db.update_file(row["id"], local_path=row["local_path"])
            return row

    def _buffer_growth(self):
        """Сколько байт ещё допишут файлы, которые качаются в буфер прямо сейчас."""
        grow = 0
        for r in self.db.q("SELECT size, local_path FROM files WHERE status='active' AND local_path IS NOT NULL"):
            try:
                cur = os.path.getsize(r["local_path"])
            except OSError:
                cur = 0
            grow += max(0, r["size"] - cur)
        return grow

    def _buffer_avail(self):
        """(сколько ещё можно взять в буфер, размер буфера): с учётом лимита и свободного места на диске."""
        s = self.settings
        cap = s["buffer_gb"] * BUF_UNIT
        grow = self._buffer_growth()
        used = self.buffer.written() + grow
        free = self.buffer.free() - s["buffer_min_free_gb"] * BUF_UNIT - grow
        return min(cap - used, free), cap

    def _run(self, key, row, side, fn):
        info = {"id": row["id"], "path": row["path"], "size": row["size"], "done": 0, "side": side,
                "stage": "подготовка", "started": time.time()}
        self.active[key] = info
        try:
            fn(row, info)
        except Exception as e:  # noqa
            self._failed(row, e, side)
        finally:
            self.active.pop(key, None)

    def _failed(self, row, e, side):
        """Разбор неудачи: отложить, притормозить, поставить на паузу или посчитать попытку."""
        fid = row["id"]
        if isinstance(e, _Requeued):
            return
        back = "buffered" if side == "google" and (row.get("local_path") or "").endswith(".dat") else "pending"
        if isinstance(e, Paused):
            self.db.update_file(fid, status=back)
        elif isinstance(e, StorageDown):
            defers = (row.get("defers") or 0) + 1
            delay = self._defer_delay(defers)
            self.db.update_file(fid, status="pending", next_try=time.time() + delay, defers=defers,
                                last_error=str(e), note="ждёт сервер Яндекса")
            self.log(f"«{row['name']}»: {e} (повтор через {_dur(delay)})", "warn")
            if defers == PREVIEW_AFTER or (defers > PREVIEW_AFTER and defers % 6 == 0):
                self._try_preview(row)
        elif isinstance(e, WebAuthError):
            self.db.update_file(fid, status=back, last_error=str(e))
            if not self.web_problem:
                self.web_problem = str(e)
                self.log(f"Куки веб-версии Яндекса не работают: {e}. Раздел «Фото» ждёт новых кук, "
                         f"остальное продолжает переноситься.", "error")
        elif isinstance(e, (AuthError, FatalPause, LocalDiskError)):
            self.db.update_file(fid, status=back, last_error=str(e))
            self.pause_transfer(str(e), by_user=False)
        elif isinstance(e, RateLimited):
            self.db.update_file(fid, status=back, last_error=str(e))
            (self._google_limited if e.service == "google" else self._yandex_limited)(e)
        elif is_network(e) and not self._online():
            self.db.update_file(fid, status=back, last_error=f"нет интернета: {e}")
            self._went_offline()
        else:
            if side == "yandex" and getattr(e, "service", "yandex") != "google" and \
                    (is_network(e) or (isinstance(e, ApiError) and (e.status or 0) >= 500)):
                self.gate.error()
            attempts = row["attempts"] + 1
            permanent = isinstance(e, ApiError) and e.status in (404, 416)
            final = permanent or attempts >= self.settings["max_attempts"]
            if final:
                hours = self.settings["auto_retry_hours"]
                nxt = FOREVER if permanent or not hours else time.time() + hours * 3600
                status = "error"
            else:
                nxt = time.time() + backoff(attempts, base=30, cap=1800)
                status = back
            if final and row.get("local_path"):
                _remove(row["local_path"])          # не держать место в буфере до следующего повтора
                self.db.update_file(fid, local_path=None)
            self.db.update_file(fid, status=status, attempts=attempts, last_error=str(e)[:1000], next_try=nxt)
            if final:
                again = "" if nxt >= FOREVER else f" Повторю сам в {time.strftime('%H:%M', time.localtime(nxt))}."
                self.log(f"Ошибка «{row['path']}»: {e}.{again}", "error")
                if not permanent and is_media(row):
                    self._try_preview(row)
            else:
                self.log(f"Сбой «{row['path']}»: {e} — повтор через {_dur(nxt - time.time())} "
                         f"(попытка {attempts} из {self.settings['max_attempts']})", "warn")
            if not isinstance(e, ApiError):
                self.log(traceback.format_exc(), "debug")

    def _open_source(self, row, offset, y=None, w=None):
        """Поток файла с Яндекса с байта offset. «Файлы» — через API, при отказе API — через веб-версию."""
        stop = self.stop_transfer
        if row["source"] == "photounlim" and self.photounlim_mode() == "web":
            w = w or self.yaweb(stop)
            return open_download(w.file_url(row["path"]), offset, sess=w.session(), reject_pages=True)
        y = y or self.yandex(stop)
        cookies = self.settings["yandex_cookies"] and not self.web_problem
        if row["source"] == "disk" and cookies and (self.rest_broken or not self.settings["yandex_token"]):
            w = w or self.yaweb(stop)
            return open_download(w.file_url(row["path"]), offset, sess=w.session(), reject_pages=True)
        try:
            href = y.download_href(row["path"])
        except RateLimited:
            raise
        except ApiError as e:
            if e.status == 404:
                raise
            if row["source"] == "disk" and cookies:
                w = w or self.yaweb(stop)
                if e.status == 410 and not self.rest_broken:
                    self.rest_broken = str(e)
                    self.log(f"API Яндекса не отдаёт файлы ({e}) — дальше «Файлы» беру через веб-версию", "warn")
                elif e.status != 410:
                    self.log(f"«{row['name']}»: API Яндекса не дал ссылку ({e}) — беру через веб-версию", "warn")
                try:
                    return open_download(w.file_url(row["path"]), offset, sess=w.session(), reject_pages=True)
                except (WebAuthError, RateLimited, StorageDown):
                    raise
                except ApiError:
                    if e.status != 410:
                        raise
            if e.status == 410:
                raise StorageDown(None, f"Яндекс сейчас не отдаёт файл ({e}) — файл отложен")
            raise
        return open_download(href, offset)

    def _download_one(self, row, info):
        s = self.settings
        stop = self.stop_transfer
        chunk = s["chunk_mb"] * MB
        if row["local_path"] and (not self.buffer or row["size"] > s["buffer_gb"] * BUF_UNIT):
            _remove(row["local_path"])      # буфер выключили или уменьшили — файл пойдёт напрямую
            self.db.update_file(row["id"], local_path=None)
            row["local_path"] = None
        if s["skip_existing"] and not row["upload_uri"] and not row["local_path"]:
            if self._existing_check(row, info):
                return
        if row["local_path"]:
            self._download_to_buffer(row, info)
        elif row["size"] <= chunk and not row["upload_uri"]:
            data = self._download_to_memory(row, info)
            info["stage"] = "в памяти, ждёт загрузки в Google"
            if not self.memq.put((row, data), len(data) or 1, stop):
                raise Paused()
        else:
            while self._hold(stop, "google"):
                if stop.is_set():
                    raise Paused()
            self._transfer_direct(row, info, chunk)

    def _existing_check(self, row, info):
        """Если такой файл уже лежит на Drive — не качать его с Яндекса вовсе."""
        if self.g_cooldown_until > time.time():
            return False
        info["stage"] = "проверка: нет ли уже на Drive"
        try:
            drive = self.drive(self.stop_transfer)
            parent = drive.ensure_folder(drive_dir_for(row, self.settings))
            fid = self._find_existing(drive, parent, row, fresh=row["attempts"] > 0)
        except (AuthError, FatalPause):
            raise
        except RateLimited as e:
            self._google_limited(e)
            return False
        except ApiError as e:
            self.log(f"Проверка дублей «{row['name']}» не удалась ({e}) — проверю перед загрузкой", "debug")
            return False
        if fid:
            self._mark_existing(row, fid)
            return True
        return False

    def _download_to_memory(self, row, info):
        """Маленький файл целиком в память; MD5 сверяется с Яндексом."""
        size = row["size"]
        stop = self.stop_transfer
        for attempt in range(6):
            if stop.is_set():
                raise Paused()
            try:
                if not size:
                    return b""
                info["stage"] = "скачивание с Яндекса"
                stream, _ = self._open_source(row, 0)
                try:
                    data = read_exact(stream.raw, size, lambda n, d: (info.__setitem__("done", n),
                                                                      self._add_speed("yandex", d)))
                    extra = stream.raw.read(1)
                finally:
                    stream.close()
                if len(data) != size or extra:
                    raise ApiError(f"Яндекс отдал {len(data)}{'+' if extra else ''} байт вместо {size}",
                                   retryable=True)
                if row["md5"] and hashlib.md5(data).hexdigest() != row["md5"]:
                    raise ApiError("MD5 скачанного не совпал с Яндексом — скачиваю заново", retryable=True)
                return data
            except STOP_ERRORS:
                raise
            except (ApiError, *NETWORK_ERRORS) as e:
                if isinstance(e, ApiError) and not e.retryable:
                    raise
                if attempt == 5:
                    raise
                if is_network(e):
                    self.gate.error()
                delay = backoff(attempt, base=3, cap=60)
                self.log(f"Сбой скачивания «{row['name']}»: {e} — повтор через {delay:.0f} c", "warn")
                if stop.wait(delay):
                    raise Paused()

    def _download_to_buffer(self, row, info):
        """В буфер на диске с докачкой (Range) и сверкой MD5 до загрузки в Google."""
        size, part = row["size"], row["local_path"]
        stop = self.stop_transfer
        h = hashlib.md5()
        offset = 0
        try:
            if os.path.exists(part):
                offset = os.path.getsize(part)
                if offset > size:
                    os.truncate(part, 0)
                    offset = 0
                elif offset:
                    info["stage"] = "проверяю недокачанное"
                    with open(part, "rb") as f:
                        for block in iter(lambda: f.read(8 * MB), b""):
                            h.update(block)
                    self.log(f"Докачка в буфер «{row['name']}» с {offset / MB:.1f} МБ")
            f = open(part, "ab")
        except OSError as e:
            raise LocalDiskError(f"Буфер на диске: не удалось открыть {part}: {e}")
        errors, since_error = 0, 0
        info["done"] = offset
        try:
            while offset < size:
                if stop.is_set():
                    raise Paused()
                try:
                    info["stage"] = "запрашиваю файл у Яндекса"
                    stream, pos = self._open_source(row, offset)
                    try:
                        if pos != offset:            # сервер не умеет докачку — с начала
                            f.seek(0)
                            f.truncate()
                            h, offset = hashlib.md5(), 0
                        info["stage"] = "скачивание с Яндекса в буфер"
                        while offset < size:
                            if stop.is_set():
                                raise Paused()
                            block = stream.raw.read(min(MB, size - offset))
                            if not block:
                                break
                            try:
                                f.write(block)
                            except OSError as e:
                                raise LocalDiskError(f"Буфер на диске: запись не удалась ({e}). "
                                                     f"Проверьте место на диске и папку буфера.")
                            h.update(block)
                            offset += len(block)
                            since_error += len(block)
                            info["done"] = offset
                            self._add_speed("yandex", len(block))
                    finally:
                        stream.close()
                    if offset < size:
                        raise ApiError(f"Яндекс оборвал поток ({offset / MB:.1f} из {size / MB:.1f} МБ)",
                                       retryable=True)
                except STOP_ERRORS:
                    raise
                except (ApiError, *NETWORK_ERRORS) as e:
                    if isinstance(e, ApiError) and not e.retryable:
                        raise
                    if since_error >= 8 * MB:
                        errors = 0
                    since_error = 0
                    errors += 1
                    if errors > 8:
                        raise
                    self.gate.error()
                    delay = backoff(errors, base=3, cap=120)
                    self.log(f"Обрыв «{row['name']}» на {offset / MB:.1f} МБ: {e}. Докачаю с этого места "
                             f"через {delay:.0f} c", "warn")
                    if stop.wait(delay):
                        raise Paused()
            f.flush()
            os.fsync(f.fileno())
        finally:
            f.close()
        if row["md5"] and h.hexdigest() != row["md5"]:
            _remove(part)
            raise ApiError("MD5 скачанного в буфер не совпал с Яндексом — файл будет скачан заново",
                           retryable=True)
        final = part[:-5] + ".dat" if part.endswith(".part") else part
        try:
            os.replace(part, final)
        except OSError as e:
            raise LocalDiskError(f"Буфер на диске: {e}")
        self.db.update_file(row["id"], status="buffered", local_path=final, note=None, last_error=None,
                            next_try=0)
        info["stage"] = "в буфере, MD5 сверен"

    def _transfer_direct(self, row, info, chunk):
        """Большой файл напрямую Яндекс → Google кусками; после сбоя — с подтверждённого Google байта."""
        size = row["size"]
        stop = self.stop_transfer
        drive = self.drive(stop)
        parent = drive.ensure_folder(drive_dir_for(row, self.settings))
        props = self._props(row)
        uri = row["upload_uri"]
        if not uri:
            uri = drive.start_upload(row["name"], parent, size, row["mime"], props, self._mtime(row))
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
                                                     self._mtime(row))
                            val = 0
                        elif val:
                            self.log(f"Докачка «{row['name']}» с {val / MB:.1f} МБ")
                        offset = val
                        self.db.update_file(row["id"], upload_uri=uri, bytes_done=offset)
                        info["done"] = offset
                        if stream is not None:
                            stream.close()
                            stream = None
                    if stream is None:
                        info["stage"] = "запрашиваю файл у Яндекса"
                        stream, pos = self._open_source(row, offset)
                        if pos < offset:
                            skipped = read_exact(stream.raw, offset - pos)
                            if len(skipped) != offset - pos:
                                raise ApiError("Яндекс оборвал поток", retryable=True)
                    want = min(chunk, size - offset)
                    info["stage"] = "Яндекс → Google напрямую"
                    base = offset
                    buf = read_exact(stream.raw, want, lambda n, d: (info.__setitem__("done", base + n),
                                                                     self._add_speed("yandex", d)))
                    if len(buf) != want:
                        raise ApiError(f"Яндекс оборвал поток ({len(buf)} из {want} байт)", retryable=True)
                    st, val = drive.put_chunk(uri, buf, offset, size)
                    if st == "done":
                        self._add_speed("google", len(buf))
                        self._finish(row, val, drive, parent)
                        return
                    if st == "gone":
                        offset = None
                        continue
                    if val - offset > 0:
                        self._add_speed("google", val - offset)
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
                        raise ApiError(f"Слишком много сбоев подряд: {e}", retryable=True,
                                       service=getattr(e, "service", None))
                    if getattr(e, "service", None) != "google":
                        self.gate.error()
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

    # ================================================================== загрузка в Google
    def _uploader(self, i):
        key = f"u{i}"
        stop = self.stop_transfer
        while not stop.is_set():
            if self._hold(stop, "google"):
                continue
            item = self.memq.get()
            if item is not None:
                row, data = item
                self._run(key, row, "google", lambda r, inf: self._upload(r, inf, data))
                continue
            row = self.db.claim_upload()
            if row is not None:
                self._run(key, row, "google", lambda r, inf: self._upload(r, inf, None))
                continue
            if not self._work_left():
                return
            stop.wait(1)

    def _props(self, row):
        props = {APP_PROP: "1"}
        if row["md5"]:
            props["ya_md5"] = row["md5"]
        return props

    def _mtime(self, row):
        """Дата файла на Drive: у фото — дата съёмки, у остального — дата изменения на Яндексе."""
        t = row.get("taken") if goes_to_photos(row, self.settings) else None
        t = t or row.get("modified")
        return t if t and re.match(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d", t) else None

    def _upload(self, row, info, data):
        """Загрузка в Google из памяти (data) или из буфера на диске (row['local_path'])."""
        try:
            self._upload_inner(row, info, data)
        except (RateLimited, Paused) as e:
            if data is None or isinstance(e, Paused) or e.service != "google" or self.stop_transfer.is_set():
                raise
            # Google попросил паузу — скачанный в память файл не выбрасываем, он подождёт
            self._google_limited(e)
            self.memq.put((row, data), len(data) or 1, self.stop_transfer, front=True, force=True)
            raise _Requeued()

    def _upload_inner(self, row, info, data):
        s = self.settings
        size = row["size"]
        info["stage"] = "загрузка в Google"
        drive = self.drive(self.stop_transfer)
        parent = drive.ensure_folder(drive_dir_for(row, s))
        if not row["upload_uri"] and s["skip_existing"]:
            fid = self._find_existing(drive, parent, row, fresh=row["attempts"] > 0)
            if fid:
                self._mark_existing(row, fid)
                return
        chunk = s["chunk_mb"] * MB
        if data is None and row["local_path"] and not os.path.exists(row["local_path"]):
            self.db.update_file(row["id"], status="pending", local_path=None)
            self.log(f"«{row['name']}» пропал из буфера — скачаю заново", "warn")
            raise _Requeued()
        if size <= chunk and not row["upload_uri"]:
            if data is None:
                data = self._read_local(row["local_path"], 0, size)
            self._upload_small(row, info, drive, parent, data)
        else:
            self._upload_resumable(row, info, drive, parent, chunk)

    def _read_local(self, path, offset, n):
        try:
            with open(path, "rb") as f:
                f.seek(offset)
                return f.read(n)
        except OSError as e:
            raise LocalDiskError(f"Буфер на диске: не удалось прочитать {path}: {e}")

    def _upload_small(self, row, info, drive, parent, data):
        size = row["size"]
        stop = self.stop_transfer
        for attempt in range(6):
            if stop.is_set():
                raise Paused()
            try:
                info["stage"] = "загрузка в Google"
                f = drive.upload_small(row["name"], parent, data, row["mime"], self._props(row), self._mtime(row))
                info["done"] = size
                self._add_speed("google", size)
                self._finish(row, f, drive, parent)
                return
            except STOP_ERRORS:
                raise
            except (ApiError, *NETWORK_ERRORS) as e:
                if isinstance(e, ApiError) and not e.retryable:
                    raise
                # Ответ Google мог потеряться уже после создания файла — проверяем, чтобы не было дубля
                fid = self._find_existing(drive, parent, row, fresh=True)
                if fid:
                    self._mark_existing(row, fid, note=None, uploaded=size)
                    return
                if attempt == 5:
                    raise
                delay = backoff(attempt, base=3, cap=60)
                self.log(f"Сбой загрузки «{row['name']}» в Google: {e} — повтор через {delay:.0f} c", "warn")
                if stop.wait(delay):
                    raise Paused()

    def _upload_resumable(self, row, info, drive, parent, chunk):
        """Из буфера кусками; после сбоя — с байта, который подтвердил Google. Яндекс больше не нужен."""
        size, path = row["size"], row["local_path"]
        stop = self.stop_transfer
        uri = row["upload_uri"]
        if not uri:
            uri = drive.start_upload(row["name"], parent, size, row["mime"], self._props(row), self._mtime(row))
            self.db.update_file(row["id"], upload_uri=uri, bytes_done=0)
        offset, errors = None, 0
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
                        uri = drive.start_upload(row["name"], parent, size, row["mime"], self._props(row),
                                                 self._mtime(row))
                        val = 0
                    elif val:
                        self.log(f"Докачка в Google «{row['name']}» с {val / MB:.1f} МБ")
                    offset = val
                    self.db.update_file(row["id"], upload_uri=uri, bytes_done=offset)
                info["stage"] = "загрузка в Google"
                buf = self._read_local(path, offset, min(chunk, size - offset))
                st, val = drive.put_chunk(uri, buf, offset, size)
                if st == "done":
                    self._add_speed("google", len(buf))
                    self._finish(row, val, drive, parent)
                    return
                if st == "gone":
                    offset = None
                    continue
                if val > offset:
                    self._add_speed("google", val - offset)
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
                    raise ApiError(f"Слишком много сбоев подряд: {e}", retryable=True, service="google")
                delay = backoff(errors, base=3, cap=120)
                self.log(f"Сбой загрузки «{row['name']}» в Google: {e}. Продолжу с подтверждённого места "
                         f"через {delay:.0f} c", "warn")
                offset = None
                if stop.wait(delay):
                    raise Paused()

    def _find_existing(self, drive, parent, row, fresh=False):
        """Файл с тем же именем, размером и MD5 уже лежит в папке → его id."""
        if fresh:
            self.folder_index.drop(parent)
        for fid, md5, size in self.folder_index.get(drive, parent).get(row["name"], []):
            if size == row["size"] and (not row["md5"] or md5 == row["md5"]):
                return fid
        return None

    def _mark_existing(self, row, fid, note="уже был на Drive", uploaded=0):
        self._cleanup_after_done(row)
        self.db.update_file(row["id"], status="done", gd_id=fid, bytes_done=row["size"], upload_uri=None,
                            last_error=None, note=note, local_path=None, done_at=time.time(), uploaded=uploaded,
                            verified=time.time() if row["md5"] else None, preview_id=None)

    def _finish(self, row, f, drive, parent):
        """Файл принят Google: сверяем MD5 и размер с Яндексом. Только после этого — «Готово»."""
        gmd5 = f.get("md5Checksum")
        if self.settings["verify_md5"] and row["md5"] and gmd5 and gmd5 != row["md5"]:
            drive.delete(f["id"])
            self.db.update_file(row["id"], upload_uri=None, bytes_done=0)
            row["upload_uri"] = None
            raise Mismatch(f"MD5 не совпал (Яндекс {row['md5']}, Google {gmd5}) — файл удалён с Drive, "
                           f"будет загружен заново", service="google")
        if f.get("size") is not None and int(f["size"]) != row["size"]:
            drive.delete(f["id"])
            self.db.update_file(row["id"], upload_uri=None, bytes_done=0)
            row["upload_uri"] = None
            raise Mismatch("Размер на Drive не совпал — файл удалён с Drive, будет загружен заново",
                           service="google")
        self.folder_index.add(parent, row["name"], f["id"], gmd5, row["size"])
        self._cleanup_after_done(row, drive)
        self.db.update_file(row["id"], status="done", gd_id=f["id"], upload_uri=None, local_path=None,
                            bytes_done=row["size"], last_error=None, note=None, done_at=time.time(),
                            uploaded=row["size"], preview_id=None,
                            verified=time.time() if (gmd5 and gmd5 == row["md5"]) else None)

    def _cleanup_after_done(self, row, drive=None):
        _remove(row.get("local_path"))
        if row.get("preview_id"):
            try:
                (drive or self.drive()).delete(row["preview_id"])
                self.log(f"«{row['name']}»: оригинал перенесён, временное превью удалено")
            except Exception as e:  # noqa
                self.log(f"Не удалось удалить превью «{row['name']}»: {e}", "debug")

    # ================================================================== превью вместо недоступного оригинала
    def _try_preview(self, row):
        """Оригинал не отдаётся — сохранить хотя бы самое крупное превью в отдельную папку на Drive."""
        s = self.settings
        if not s["save_previews"] or row.get("preview_id") or not is_media(row):
            return False
        if self.g_cooldown_until > time.time() or not s["google_refresh_token"]:
            return False
        try:
            if row["source"] == "photounlim" or not s["yandex_token"]:
                body, ctype, size = self.yaweb(self.stop_transfer).preview(row["path"])
            else:
                try:
                    body, ctype, size = self.yandex(self.stop_transfer).preview(row["path"])
                except (AuthError, ApiError):
                    if not s["yandex_cookies"]:
                        raise
                    body, ctype, size = self.yaweb(self.stop_transfer).preview(row["path"])
            ext = MEDIA_EXT.get(ctype, ".jpg")
            name = f"{row['name']} (превью {size}){ext}"
            drive = self.drive(self.stop_transfer)
            parent = drive.ensure_folder(f"{s['previews_root']}/{drive_dir_for(row, s)}")
            fid = self._preview_on_drive(drive, parent, name)
            if not fid:
                try:
                    fid = drive.upload_small(name, parent, body, ctype if ctype.startswith("image/") else "image/jpeg",
                                             {APP_PROP: "preview"})["id"]
                except (ApiError, *NETWORK_ERRORS):
                    # ответ Google мог потеряться уже после создания файла — тогда превью уже лежит на Drive
                    fid = self._preview_on_drive(drive, parent, name)
                    if not fid:
                        raise
            self.db.update_file(row["id"], preview_id=fid,
                                note="оригинал пока недоступен — сохранено превью, оригинал продолжаю ждать")
            self.log(f"«{row['name']}»: оригинал не отдаётся — сохранено превью {size} "
                     f"({len(body) // 1024} КБ) в «{s['previews_root']}». Оригинал продолжаю ждать.", "warn")
            return True
        except Exception as e:  # noqa
            self.log(f"«{row['name']}»: превью тоже не получить ({e})", "warn")
            return False

    @staticmethod
    def _preview_on_drive(drive, parent, name):
        """id уже сохранённого превью с таким именем (лишние копии удаляются) или None."""
        found = drive.find_in_parent(parent, name)
        for extra in found[1:]:
            drive.delete(extra["id"])
        return found[0]["id"] if found else None

    # ================================================================== служебные операции
    def busy(self):
        return self.transferring() or bool(self.task_thread and self.task_thread.is_alive())

    def run_task(self, name, fn, allow_during_transfer=False):
        with self.lock:
            if (self.transferring() and not allow_during_transfer) or \
                    (self.task_thread and self.task_thread.is_alive()):
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
                reset = dict(status="pending", gd_id=None, upload_uri=None, bytes_done=0, attempts=0, next_try=0,
                             verified=None, done_at=None, uploaded=0)
                if f is None:
                    missing += 1
                    self.db.update_file(r["id"], note="не найден на Drive — повторно", **reset)
                elif int(f.get("size") or -1) != r["size"] or (r["md5"] and f.get("md5Checksum") != r["md5"]):
                    bad += 1
                    self.db.update_file(r["id"], note="на Drive не совпал размер/MD5 — повторно", **reset)
                else:
                    ok += 1
                    self.db.update_file(r["id"], verified=now)
            return (f"сверено {len(rows)}: совпало по размеру и MD5 — {ok}, нет на Drive — {missing}, "
                    f"не совпало — {bad}. Несовпавшие и пропавшие возвращены в очередь")
        self.run_task("Сверка с Google Drive", fn)

    def rescue_previews(self):
        """Для всех файлов, которые Яндекс сейчас не отдаёт, — сохранить превью."""
        def fn():
            rows = self.db.q("SELECT * FROM files WHERE preview_id IS NULL AND "
                             "((status='pending' AND next_try>?) OR status='error')", (time.time(),))
            rows = [dict(r) for r in rows if is_media(dict(r))]
            ok = 0
            for i, r in enumerate(rows):
                self.task["msg"] = f"{i + 1} из {len(rows)}…"
                ok += bool(self._try_preview(r))
            return f"превью сохранено для {ok} из {len(rows)} недоступных фото/видео"
        self.run_task("Спасение превью", fn, allow_during_transfer=True)

    def report_csv(self):
        out = io.StringIO()
        wr = csv.writer(out, delimiter=";")
        wr.writerow(["Путь на Яндексе", "Раздел", "Размер, байт", "MD5 Яндекса", "Статус", "Проверено (MD5 совпал)",
                     "Папка на Google Drive", "Ссылка", "Превью вместо оригинала", "Ошибка/примечание"])
        st_names = {"pending": "в очереди", "active": "в работе", "buffered": "в буфере", "uploading": "в работе",
                    "done": "готово", "error": "ошибка", "skipped": "пропущен"}
        for r in self.db.q("SELECT * FROM files ORDER BY source, path"):
            r = dict(r)
            wr.writerow([r["path"], "Фото" if r["source"] == "photounlim" else "Файлы", r["size"], r["md5"] or "",
                         st_names.get(r["status"], r["status"]),
                         time.strftime("%Y-%m-%d %H:%M", time.localtime(r["verified"])) if r["verified"] else "",
                         drive_dir_for(r, self.settings),
                         f"https://drive.google.com/file/d/{r['gd_id']}/view" if r["gd_id"] else "",
                         f"https://drive.google.com/file/d/{r['preview_id']}/view" if r["preview_id"] else "",
                         r["last_error"] or r["note"] or ""])
        return "\ufeff" + out.getvalue()      # BOM — чтобы Excel открыл кириллицу

    def retry_errors(self):
        n = self.db.x("UPDATE files SET status='pending', attempts=0, next_try=0 WHERE status='error'")
        n2 = self.db.x("UPDATE files SET next_try=0 WHERE status IN ('pending','buffered') AND next_try>?",
                       (time.time(),))
        DEAD.clear()
        self.web_problem = ""
        with self.lock:
            self.ya_cooldown_until = self.g_cooldown_until = 0
        self.log(f"В очередь возвращено: с ошибками {n}, отложенных {n2}")
        return n + n2

    def requeue_all(self):
        if self.busy():
            raise ApiError("Сначала остановите перенос")
        for r in self.db.q("SELECT local_path FROM files WHERE local_path IS NOT NULL"):
            _remove(r["local_path"])
        n = self.db.x("UPDATE files SET status='pending', attempts=0, next_try=0, gd_id=NULL, defers=0, "
                      "upload_uri=NULL, bytes_done=0, last_error=NULL, note=NULL, verified=NULL, "
                      "local_path=NULL, done_at=NULL, uploaded=0")
        self.db.folders_clear()
        self.folder_index.clear()
        self.log(f"Все файлы ({n}) поставлены в очередь заново. Уже лежащие на Drive одинаковые файлы "
                 f"будут пропущены (сверка по имени, размеру и MD5).")
        return n

    def reset_db(self):
        if self.busy() or self.scanning():
            raise ApiError("Сначала остановите перенос и получение списка")
        for r in self.db.q("SELECT local_path FROM files WHERE local_path IS NOT NULL"):
            _remove(r["local_path"])
        self.db.x("DELETE FROM files")
        self.db.folders_clear()
        self.folder_index.clear()
        for k in ("scan.state", "scan.phase", "scan.offset", "scan.found", "scan.error"):
            self.db.set(k, None)
        self.log("База очищена. Нужно заново получить список файлов.")

    def file_action(self, fid, action):
        if action == "retry":
            self.db.x("UPDATE files SET status='pending', attempts=0, next_try=0 "
                      "WHERE id=? AND status IN ('error','skipped','pending')", (fid,))
        elif action == "skip":
            r = self.db.q1("SELECT local_path FROM files WHERE id=? AND status IN ('pending','error','buffered')",
                           (fid,))
            if r:
                _remove(r["local_path"])
                self.db.x("UPDATE files SET status='skipped', note='пропущен вручную', local_path=NULL "
                          "WHERE id=?", (fid,))
        elif action == "requeue":
            self.db.x("UPDATE files SET status='pending', attempts=0, next_try=0, gd_id=NULL, "
                      "upload_uri=NULL, bytes_done=0, verified=NULL WHERE id=? AND status IN ('done','error')",
                      (fid,))

    # ================================================================== статус для UI
    def buffer_info(self):
        s = self.settings
        if not s["buffer_dir"]:
            return {"enabled": False, "ram_bytes": self.memq.bytes, "ram_items": len(self.memq),
                    "ram_limit": RAM_QUEUE_MB * MB}
        b = self.buffer if (self.buffer and self.buffer.base == os.path.abspath(s["buffer_dir"])) \
            else Buffer(s["buffer_dir"])
        free = total = None
        try:
            du = shutil.disk_usage(b.dir if os.path.isdir(b.dir) else b.base)
            free, total = du.free, du.total
        except OSError:
            pass
        return {"enabled": True, "dir": b.base, "cap": s["buffer_gb"] * BUF_UNIT, "reserved": self.db.buffer_reserved(),
                "written": b.written(), "free": free, "disk_total": total,
                "min_free": s["buffer_min_free_gb"] * BUF_UNIT,
                "files": self.db.count_status("buffered")}

    def disk_warnings(self):
        """Предупреждения о месте на Яндекс Диске: переполненный Диск Яндекс блокирует через 44 дня,
        а заблокированный дольше 90 дней — очищает (справка Яндекса «Диск переполнен»)."""
        try:
            d = json.loads(self.db.get("disk.info") or "{}")
        except ValueError:
            return []
        out = []
        total, used = d.get("total") or 0, d.get("used") or 0
        if total and used > total:
            out.append(("err", f"Яндекс Диск переполнен: занято {used / GB:.1f} ГБ из {total / GB:.1f} ГБ. По правилам "
                               f"Яндекса переполненный Диск через 44 дня блокируется (файлы недоступны даже для "
                               f"скачивания), а заблокированный дольше 90 дней — очищается. Перенос нужно закончить "
                               f"раньше; ничего не удаляйте и не перемещайте на Яндексе, пока он идёт"))
        elif total and used > 0.95 * total:
            out.append(("warn", f"Яндекс Диск почти заполнен: {used / GB:.1f} ГБ из {total / GB:.1f} ГБ. "
                                f"Переносу это не мешает — программа только читает с Яндекса"))
        end = _date_of(d.get("unlim_end"))
        if d.get("will_be_overdrawn") and end and end >= time.strftime("%Y-%m-%d"):
            out.append(("err", f"Бесплатный безлимит для фото заканчивается {end}: после этого "
                               f"фото из безлимита начнут занимать место, и Диск окажется переполнен"))
        return out

    def attention(self, st=None):
        """Что требует внимания человека — простыми словами."""
        s, out = self.settings, list(self.disk_warnings())
        if not s["yandex_token"]:
            out.append(("err", "Не указан токен Яндекса (блок 1)"))
        if not s["google_refresh_token"]:
            out.append(("err", "Не выполнен вход в Google (блок 2)"))
        pu = int(self.db.get("photounlim.size") or 0)
        if s["include_photounlim"] and pu and not s["yandex_cookies"] and self.db.get("photounlim.method") == "":
            out.append(("warn", f"Раздел «Фото» ({pu / GB:.1f} ГБ) не будет перенесён, пока не добавлены куки (блок 3)"))
        if self.web_problem:
            out.append(("err", f"Куки Яндекса не работают: {self.web_problem}"))
        if self.rest_broken:
            out.append(("warn", f"API Яндекса не отдаёт «Файлы» ({self.rest_broken}) — они идут через веб-версию"))
        if self.pause_reason and self.transfer_state in ("paused", "pausing", "attention"):
            out.append(("err", self.pause_reason))
        if self.offline_since:
            out.append(("err", f"Нет интернета с {time.strftime('%H:%M', time.localtime(self.offline_since))} — "
                               f"жду, продолжу сам"))
        now = time.time()
        if self.g_cooldown_until > now:
            out.append(("warn", f"Google попросил паузу до "
                                f"{time.strftime('%H:%M', time.localtime(self.g_cooldown_until))}; "
                                f"скачивание с Яндекса продолжается в буфер"))
        if self.ya_cooldown_until > now:
            out.append(("warn", f"Яндекс попросил паузу до "
                                f"{time.strftime('%H:%M', time.localtime(self.ya_cooldown_until))}"))
        q = self.g_quota
        if q.get("limit"):
            bs = (st or self._stats())["by_status"]
            left = sum(bs.get(k, {}).get("bytes", 0) for k in ("pending", "active", "buffered", "uploading", "error"))
            free = q["limit"] - q["usage"]
            if left > free:
                out.append(("err", f"На Google Drive не хватит места: свободно {free / GB:.1f} ГБ, "
                                   f"осталось перенести {left / GB:.1f} ГБ. Увеличьте тариф Google One"))
        if s["buffer_dir"]:
            bi = self.buffer_info()
            if bi["free"] is not None and bi["free"] < bi["min_free"] + 2 * GB:
                out.append(("warn", f"На диске с буфером мало места: свободно {bi['free'] / GB:.1f} ГБ"))
        return [{"level": lv, "msg": m} for lv, m in out]

    def status(self):
        st = self._stats()
        sy, sg = self.speed_bps("yandex"), self.speed_bps("google")
        bs = st["by_status"]
        total_b = sum(v["bytes"] for v in bs.values())
        done_b = bs.get("done", {}).get("bytes", 0) + bs.get("skipped", {}).get("bytes", 0)
        left = max(0, total_b - done_b - bs.get("error", {}).get("bytes", 0))
        sp = min(x for x in (sy, sg) if x) if (sy or sg) else 0
        db = self.db
        nxt = None
        if self.transfer_state == "running" and not self.active:
            nxt = db.next_wakeup(self._excluded_sources(), self._retry_errors())
        return {
            "transfer": {"state": self.transfer_state, "reason": self.pause_reason,
                         "ya_cooldown": self.ya_cooldown_until if self.ya_cooldown_until > time.time() else 0,
                         "g_cooldown": self.g_cooldown_until if self.g_cooldown_until > time.time() else 0,
                         "offline_since": self.offline_since, "next_wakeup": nxt,
                         "downloaders": sum(t.is_alive() for t in self.downloaders),
                         "uploaders": sum(t.is_alive() for t in self.uploaders)},
            "gate": self.gate.state(),
            "scan": {"running": self.scanning(), "state": db.get("scan.state") or "never",
                     "phase": db.get("scan.phase"), "offset": int(db.get("scan.offset") or 0),
                     "found": int(db.get("scan.found") or 0), "error": db.get("scan.error") or ""},
            "stats": st,
            "active": sorted(self.active.values(), key=lambda a: a["started"]),
            "speed": {"yandex": sy, "google": sg},
            "eta": (left / sp) if sp > 0 else None,
            "task": self.task,
            "buffer": self.buffer_info(),
            "quota": self.g_quota,
            "attention": self.attention(st),
            "configured": {"yandex": bool(self.settings["yandex_token"]),
                           "cookies": bool(self.settings["yandex_cookies"]),
                           "google": bool(self.settings["google_refresh_token"]),
                           "google_client": bool(self.settings["google_client_id"])},
            "google_email": self.settings["google_email"],
            "photounlim": {"mode": self.photounlim_mode(), "problem": self.web_problem,
                           "size": int(db.get("photounlim.size") or 0)},
            "folders": {"photos": self.settings["photos_root"], "files": self.settings["files_root"],
                        "previews": self.settings["previews_root"]},
            "dead_hosts": DEAD.active(),
        }


class _Requeued(Paused):
    """Файл уже возвращён в очередь (например, снова в память) — делать с ним больше ничего не нужно."""
