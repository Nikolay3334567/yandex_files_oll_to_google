"""Настройки и журнал."""
import collections
import json
import logging
import logging.handlers
import os
import sys
import threading
import time

DEFAULTS = {
    "yandex_token": "",
    "yandex_client_id": "",
    "yandex_cookies": "",                 # куки disk.yandex.ru — раздел «Фото» и запасной канал для «Файлов»
    "google_client_id": "",
    "google_client_secret": "",
    "google_refresh_token": "",
    "google_email": "",
    # Куда класть на Google Drive (пути от «Мой диск», можно вложенные: «Архив/Фото»)
    "photos_root": "Фото",                # раздел «Фото» (безлимитная автозагрузка)
    "files_root": "Файлы с Яндекс Диска",  # раздел «Файлы» — с той же иерархией папок
    "previews_root": "Превью — оригинал пока недоступен",
    "photos_split": "month",              # none | year | month — раскладка внутри «Фото»
    "photos_from_disk": "camera",         # фото/видео из «Файлов»: camera — из «Фотокамеры» в «Фото»;
                                          # all — все в «Фото»; none — всё остаётся в своих папках
    "scan_mode": "flat",                  # flat — плоский список; tree — обход папок; web — через веб-версию
    "include_photounlim": True,
    "include_disk": True,
    "order": "photos_first",              # photos_first | files_first | small_first | as_listed
    "yandex_streams_max": 8,              # потолок одновременных скачиваний с Яндекса
    "yandex_adaptive": True,              # подбирать число потоков к Яндексу автоматически
    "google_streams": 4,                  # одновременных загрузок в Google
    "buffer_dir": "",                     # папка буфера на диске ('' — без буфера, через память)
    "buffer_gb": 20,
    "buffer_min_free_gb": 3,              # сколько места на диске оставлять свободным всегда
    "chunk_mb": 32,
    "max_attempts": 2,                    # сбой не по вине Яндекса: одна быстрая повторная попытка
    "auto_retry_hours": 0,                # повторять ошибки автоматически (0 — только вручную)
    "skip_existing": True,                # не заливать повторно файл, который уже есть на Drive (имя+md5)
    "verify_md5": True,
    "save_previews": False,               # не используется: превью — только по кнопке (замедляло перенос)
    "autostart": False,                   # при запуске программы продолжать перенос
    "win_autorun": False,                 # запускать программу при входе в Windows
    "app_window": True,                   # открывать интерфейс отдельным окном (Edge/Chrome --app)
    "port": 8765,
    "terms_accepted": False,              # человек прочитал условия (лицензия MIT, без гарантий) и нажал «Принимаю»
    "settings_v": 3,
}

SECRET_KEYS = ("yandex_token", "yandex_cookies", "google_client_secret", "google_refresh_token")
LIMITS = {"yandex_streams_max": (1, 16), "google_streams": (1, 8), "chunk_mb": (1, 256),
          "max_attempts": (1, 100), "buffer_gb": (1, 10000), "buffer_min_free_gb": (0, 1000),
          "auto_retry_hours": (0, 168)}


def app_dir():
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _writable(d):
    try:
        os.makedirs(d, exist_ok=True)
        probe = os.path.join(d, ".write-test")
        with open(probe, "w") as f:
            f.write("ok")
        os.remove(probe)
        return True
    except OSError:
        return False


def data_dir():
    """Папка data рядом с программой. Если туда нельзя писать (Program Files) — в профиль пользователя."""
    d = os.environ.get("YA2GD_DATA")
    if d:
        os.makedirs(d, exist_ok=True)
        return d
    d = os.path.join(app_dir(), "data")
    if _writable(d):
        return d
    base = os.environ.get("LOCALAPPDATA") or os.path.expanduser("~")
    d = os.path.join(base, "YandexToGoogle", "data")
    os.makedirs(d, exist_ok=True)
    return d


class Settings:
    def __init__(self, path):
        self.path = path
        self._lock = threading.Lock()
        self.values = dict(DEFAULTS)
        self.migrated = False
        self.stored_keys = None              # None — файла настроек ещё не было (первый запуск)
        if os.path.exists(path):
            with open(path, encoding="utf-8") as f:
                stored = json.load(f)
            self.migrated = _migrate(stored)
            upgraded = _upgrade_v3(stored)
            self.stored_keys = set(stored)
            self.values.update({k: v for k, v in stored.items() if k in DEFAULTS})
            if self.migrated or upgraded:
                self.update()

    def __getitem__(self, k):
        return self.values.get(k, DEFAULTS.get(k))

    def update(self, **kw):
        with self._lock:
            for k, v in kw.items():
                if k not in DEFAULTS:
                    continue
                d = DEFAULTS[k]
                if isinstance(d, bool):
                    v = bool(v)
                elif isinstance(d, int):
                    v = int(float(v or 0))
                elif v is None:
                    v = ""
                elif isinstance(v, str):
                    v = v.strip()
                self.values[k] = v
            for k, (lo, hi) in LIMITS.items():
                self.values[k] = max(lo, min(hi, int(self.values[k])))
            for k, ok in (("photos_from_disk", ("camera", "all", "none")), ("photos_split", ("none", "year", "month")),
                          ("order", ("photos_first", "files_first", "small_first", "as_listed")),
                          ("scan_mode", ("flat", "tree", "web"))):
                if self.values[k] not in ok:
                    self.values[k] = DEFAULTS[k]
            for k in ("photos_root", "files_root", "previews_root"):
                self.values[k] = "/".join(p.strip() for p in str(self.values[k]).replace("\\", "/").split("/")
                                          if p.strip()) or DEFAULTS[k]
            tmp = self.path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self.values, f, ensure_ascii=False, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self.path)

    def public(self):
        out = {}
        for k, v in self.values.items():
            if k in SECRET_KEYS:
                out[k + "_set"] = bool(v)
                out[k] = _mask(v)
            else:
                out[k] = v
        return out


# Значения по умолчанию версии 2, которые в версии 3 поменялись: недоступные файлы больше не повторяются
# по кругу, а потоков к Яндексу и Google больше. Меняем, только если человек их сам не трогал.
V3_DEFAULTS = {"max_attempts": (6, 2), "auto_retry_hours": (3, 0), "yandex_streams_max": (6, 8),
               "google_streams": (3, 4), "save_previews": (True, False)}


def _upgrade_v3(stored):
    if int(stored.get("settings_v") or 0) >= 3:
        return False
    for k, (old, new) in V3_DEFAULTS.items():
        if stored.get(k, old) == old:
            stored[k] = new
    stored["settings_v"] = 3
    return True


def _migrate(stored):
    """Настройки версии 1 (одна корневая папка «Yandex Disk») → раздельные папки, раскладка та же."""
    if "drive_root" not in stored or "files_root" in stored:
        return False
    root = stored.pop("drive_root") or "Yandex Disk"
    stored["files_root"] = root
    stored["photos_root"] = f"{root}/{stored.pop('photounlim_folder', '') or 'Фото (безлимит)'}"
    if "photounlim_split" in stored:
        stored["photos_split"] = stored.pop("photounlim_split")
    stored["photos_from_disk"] = "none"     # в версии 1 «Фотокамера» шла в «Файлы» — не перекладываем
    if "workers" in stored:
        stored["yandex_streams_max"] = max(int(stored.pop("workers")), 4)
    return True


def _mask(v):
    if not v:
        return ""
    return v[:4] + "…" + v[-4:] if len(v) > 12 else "…"


class Log:
    """Журнал: в файл (с ротацией), кольцевой буфер для веб-интерфейса и консоль.

    В консоль пишет отдельный поток: если в окне Windows выделить текст мышью, вывод в консоль
    замирает, и рабочие потоки не должны замирать вместе с ним.
    """

    def __init__(self, path, size=3000):
        self.buf = collections.deque(maxlen=size)
        self.seq = 0
        self._lock = threading.Lock()
        self._console = collections.deque(maxlen=2000)
        self._console_ev = threading.Event()
        self.logger = logging.getLogger("ya2gd")
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            h = logging.handlers.RotatingFileHandler(path, maxBytes=10 * 1024 * 1024, backupCount=10,
                                                     encoding="utf-8")
            h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
            self.logger.addHandler(h)
        threading.Thread(target=self._printer, daemon=True, name="console").start()

    def __call__(self, msg, level="info"):
        if level == "debug":
            self.logger.info(msg)
            return
        with self._lock:
            self.seq += 1
            self.buf.append({"seq": self.seq, "ts": time.time(), "level": level, "msg": str(msg)})
        {"warn": self.logger.warning, "error": self.logger.error}.get(level, self.logger.info)(msg)
        self._console.append(f"{time.strftime('%H:%M:%S')} {({'warn': '!', 'error': 'X'}).get(level, ' ')} {msg}")
        self._console_ev.set()

    def _printer(self):
        while True:
            self._console_ev.wait()
            self._console_ev.clear()
            while self._console:
                line = self._console.popleft()
                try:
                    print(line, flush=True)
                except Exception:  # noqa — консоль без UTF-8 или её нет вовсе (exe без окна)
                    pass

    def since(self, seq, limit=500):
        with self._lock:
            items = [x for x in self.buf if x["seq"] > seq]
        return items[-limit:]
