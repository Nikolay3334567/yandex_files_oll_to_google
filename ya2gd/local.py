"""Источник «папка на компьютере».

Нужен для фото из раздела «Фото» Яндекс Диска (безлимитная автозагрузка с телефона):
публичный API Яндекса их не отдаёт, а официальная программа «Яндекс Диск» для Windows
умеет копировать их на компьютер (Настройки → Автозагрузка → «Копировать фото и видео
на компьютер»). Программа забирает файлы из этой папки и заливает на Google Drive.
"""
import hashlib
import mimetypes
import os
import re
import time

from .http_util import ApiError

PREFIX = "local:"
# Файлы, которые ещё докачиваются/служебные — пропускаем.
SKIP_SUFFIXES = (".tmp", ".part", ".partial", ".crdownload", ".download", ".yadisk", ".!yd")
SKIP_NAMES = {"desktop.ini", "thumbs.db", ".ds_store"}
DATE_RE = re.compile(r"(?<!\d)((?:19|20)\d\d)[-_.]?(0[1-9]|1[0-2])[-_.]?(0[1-9]|[12]\d|3[01])")


def parse_dirs(text):
    return [d.strip().strip('"') for d in (text or "").splitlines() if d.strip()]


def root_for(path, dirs):
    """Корневая папка из настроек, в которой лежит файл (самая длинная подходящая)."""
    best = None
    norm = os.path.normcase(os.path.abspath(path))
    for d in dirs:
        nd = os.path.normcase(os.path.abspath(d))
        if norm.startswith(nd.rstrip("\\/") + os.sep) and (best is None or len(nd) > len(best[1])):
            best = (d, nd)
    return best[0] if best else None


def date_from_name(name):
    m = DATE_RE.search(name)
    return f"{m.group(1)}-{m.group(2)}-{m.group(3)}" if m else None


def scan_dir(root, min_age_sec=120, stop_event=None):
    """Генератор пачек файлов (по 500) из папки и всех подпапок."""
    now = time.time()
    batch, skipped_young = [], 0
    for dirpath, dirnames, filenames in os.walk(root):
        if stop_event is not None and stop_event.is_set():
            return
        dirnames[:] = [d for d in dirnames if not d.startswith(".")]
        for fn in filenames:
            low = fn.lower()
            if low in SKIP_NAMES or low.endswith(SKIP_SUFFIXES) or fn.startswith("~$"):
                continue
            full = os.path.join(dirpath, fn)
            try:
                st = os.stat(full)
            except OSError:
                continue
            if now - st.st_mtime < min_age_sec:
                skipped_young += 1      # файл, возможно, ещё пишется — возьмём при следующем проходе
                continue
            mtime = time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime(st.st_mtime))
            batch.append({
                "path": PREFIX + os.path.abspath(full),
                "source": "local",
                "name": fn,
                "size": st.st_size,
                "md5": None, "sha256": None,
                "modified": mtime, "created": None,
                "taken": date_from_name(fn),
                "mime": mimetypes.guess_type(fn)[0] or "application/octet-stream",
                "media_type": None,
            })
            if len(batch) >= 500:
                yield batch, skipped_young
                batch = []
    yield batch, skipped_young


class _Stream:
    """Обёртка, повторяющая интерфейс ответа requests (raw.read / close)."""

    def __init__(self, f):
        self.raw = f

    def close(self):
        self.raw.close()


def open_stream(path, offset=0):
    full = path[len(PREFIX):]
    try:
        f = open(full, "rb")
    except FileNotFoundError:
        raise ApiError(f"Файл больше не существует на компьютере: {full}", status=404)
    except OSError as e:
        raise ApiError(f"Не удалось открыть файл: {e}", retryable=True)
    if offset:
        f.seek(offset)
    return _Stream(f), offset


def file_md5(path):
    h = hashlib.md5()
    with open(path[len(PREFIX):], "rb") as f:
        for block in iter(lambda: f.read(4 << 20), b""):
            h.update(block)
    return h.hexdigest()
