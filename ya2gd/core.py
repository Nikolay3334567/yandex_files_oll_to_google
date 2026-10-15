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
    "google_client_id": "",
    "google_client_secret": "",
    "google_refresh_token": "",
    "google_email": "",
    "drive_root": "Yandex Disk",          # корневая папка на Google Drive
    "photounlim_folder": "Фото (безлимит)",
    "photounlim_split": "month",          # none | year | month — раскладка безлимитных фото
    "scan_mode": "flat",                  # flat — плоский список всех файлов; tree — обход папок
    "include_photounlim": True,
    "workers": 3,
    "chunk_mb": 32,
    "max_attempts": 6,
    "skip_existing": True,                # не заливать повторно файл, который уже есть на Drive (имя+md5)
    "verify_md5": True,
    "autostart": False,
    # Папки на компьютере (по одной на строку) — для фото из раздела «Фото», которые API не отдаёт
    "local_dirs": "",
    "local_folder": "Фото (безлимит)",
    "local_split": "month",               # keep — как в папке; year / month — по дате из имени файла
    "local_watch": True,                  # пока идёт перенос — ждать и подхватывать новые файлы
    "local_rescan_min": 5,
    "local_min_age_sec": 120,             # не трогать файлы, менявшиеся недавно (ещё скачиваются)
    "local_delete_after": False,          # удалять копию на компьютере после загрузки и сверки MD5                   # после запуска программы сразу продолжать перенос
    "port": 8765,
}

SECRET_KEYS = ("yandex_token", "google_client_secret", "google_refresh_token")


def app_dir():
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def data_dir():
    d = os.environ.get("YA2GD_DATA") or os.path.join(app_dir(), "data")
    os.makedirs(d, exist_ok=True)
    return d


class Settings:
    def __init__(self, path):
        self.path = path
        self._lock = threading.Lock()
        self.values = dict(DEFAULTS)
        if os.path.exists(path):
            with open(path, encoding="utf-8") as f:
                self.values.update(json.load(f))

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
                    v = int(v)
                elif v is None:
                    v = ""
                self.values[k] = v
            self.values["workers"] = max(1, min(16, self.values["workers"]))
            self.values["chunk_mb"] = max(1, min(256, self.values["chunk_mb"]))
            self.values["max_attempts"] = max(1, self.values["max_attempts"])
            tmp = self.path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self.values, f, ensure_ascii=False, indent=2)
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


def _mask(v):
    if not v:
        return ""
    return v[:4] + "…" + v[-4:] if len(v) > 12 else "…"


class Log:
    """Журнал: в файл (с ротацией) и кольцевой буфер для веб-интерфейса."""

    def __init__(self, path, size=2000):
        self.buf = collections.deque(maxlen=size)
        self.seq = 0
        self._lock = threading.Lock()
        self.logger = logging.getLogger("ya2gd")
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            h = logging.handlers.RotatingFileHandler(path, maxBytes=10 * 1024 * 1024, backupCount=5,
                                                     encoding="utf-8")
            h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
            self.logger.addHandler(h)

    def __call__(self, msg, level="info"):
        if level == "debug":
            self.logger.info(msg)
            return
        with self._lock:
            self.seq += 1
            self.buf.append({"seq": self.seq, "ts": time.time(), "level": level, "msg": str(msg)})
        {"warn": self.logger.warning, "error": self.logger.error}.get(level, self.logger.info)(msg)
        try:
            print(time.strftime("%H:%M:%S"), level.upper(), msg, flush=True)
        except Exception:  # noqa — консоль Windows без UTF-8 или её нет вовсе (exe без окна)
            pass

    def since(self, seq, limit=500):
        with self._lock:
            items = [x for x in self.buf if x["seq"] > seq]
        return items[-limit:]
