"""Движок: сканирование Яндекс Диска и перенос файлов на Google Drive.

Устойчивость:
- всё состояние в SQLite; после падения «зависшие» файлы возвращаются в очередь;
- большой файл продолжается с байта, который Google подтвердил (резюмируемая сессия);
- сканирование продолжается с сохранённого смещения;
- ошибки авторизации/места на диске ставят перенос на паузу, а не «сжигают» попытки;
- лимиты Google (в т.ч. суточные 750 ГБ) → общая пауза с нарастающей задержкой.
"""
import collections
import os
import threading
import time
import traceback

from .core import Log, Settings, data_dir
from .db import DB
from .gdrive import APP_PROP, DriveClient, GoogleAuth
from .http_util import ApiError, AuthError, FatalPause, NETWORK_ERRORS, RateLimited, backoff
from .yandex import YandexClient
from . import local


class Paused(Exception):
    pass


def _with_date(base, row, split, fallback_dates=True):
    """base/<год>[/<год-месяц>] по дате съёмки (EXIF → имя файла → дата создания/изменения)."""
    date = (row.get("taken") or local.date_from_name(row.get("name") or "") or
            (row.get("created") or row.get("modified") if fallback_dates else None) or "").replace(":", "-")[:10]
    if len(date) == 10 and date[4] == "-":
        if split == "year":
            return f"{base}/{date[:4]}", True
        if split == "month":
            return f"{base}/{date[:4]}/{date[:7]}", True
    return base, False


def drive_dir_for(row, s):
    """Относительный путь папки на Drive для файла."""
    path = row["path"]
    if row["source"] == "photounlim":
        rel = path.split(":", 1)[1].lstrip("/")
        sub = rel.rsplit("/", 1)[0] if "/" in rel else ""
        base, _ = _with_date(s["photounlim_folder"] or "Фото (безлимит)", row, s["photounlim_split"])
        return f"{base}/{sub}" if sub else base
    if row["source"] == "local":
        full = path[len(local.PREFIX):]
        base = s["local_folder"] or "Фото (безлимит)"
        if s["local_split"] in ("year", "month"):
            # у копий на компьютере дата изменения = дата скачивания, поэтому её не используем
            dated, ok = _with_date(base, row, s["local_split"], fallback_dates=False)
            return dated if ok else f"{base}/Без даты"
        root = local.root_for(full, local.parse_dirs(s["local_dirs"]))
        parent = os.path.dirname(full)
        if root:
            rel = os.path.relpath(parent, root)
            parts = [os.path.basename(os.path.normpath(root))] + ([] if rel == "." else rel.split(os.sep))
        else:
            parts = [os.path.basename(parent)]
        return "/".join([base] + [p for p in parts if p])
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
        self.scan_info = {"found": 0}
        self.task = {"name": "", "state": "idle", "msg": ""}
        self.task_thread = None
        self.google_state = os.urandom(16).hex()
        self._auth = None

    # ================================================================== клиенты
    def yandex(self, stop=None):
        return YandexClient(self.settings["yandex_token"], stop_event=stop, log=self.log)

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

    # ================================================================== проверки
    def test_yandex(self):
        y = YandexClient(self.settings["yandex_token"], log=self.log)
        info = y.disk_info()
        user = info.get("user") or {}
        res = {"ok": True, "user": user.get("display_name") or user.get("login"),
               "total": info.get("total_space"), "used": info.get("used_space"),
               "photounlim_size": info.get("photounlim_size")}
        try:
            items, _ = y.files_page(0, 1)
            res["files_api"] = "работает"
        except ApiError as e:
            res["files_api"] = f"ошибка: {e}"
        method, report = y.probe_photounlim()
        res["photounlim_method"] = method
        res["photounlim_report"] = report
        size = int(info.get("photounlim_size") or 0)
        if method:
            res["photounlim"] = "фото из раздела «Фото» доступны через API — будут перенесены напрямую"
        elif size:
            res["photounlim"] = ("в разделе «Фото» (безлимитная автозагрузка) лежат файлы, но Яндекс НЕ отдаёт "
                                 "их через API. Используйте блок «Папки на компьютере» — инструкция там.")
        else:
            res["photounlim"] = "безлимитных фото не обнаружено"
        self.db.set("photounlim.method", method or "")
        self.log(f"Проверка Яндекса: {res}")
        return res

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

    # ================================================================== сканирование
    def start_scan(self, fresh=False, only_local=False):
        with self.lock:
            if self.scan_thread and self.scan_thread.is_alive():
                return False
            has_local = bool(local.parse_dirs(self.settings["local_dirs"]))
            if only_local and not has_local:
                raise ApiError("Не указаны папки на компьютере")
            if not only_local and not self.settings["yandex_token"] and not has_local:
                raise AuthError("Сначала укажите токен Яндекса (или папки на компьютере)")
            self.stop_scan.clear()
            target = self._scan_local_only if only_local else self._scan
            self.scan_thread = threading.Thread(target=target, args=(fresh,), daemon=True, name="scan")
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
            self.log(f"Сканирование Яндекс Диска №{scan_id} начато (режим: {s['scan_mode']})")
        else:
            scan_id = int(db.get("scan.id", "1"))
            self.log(f"Сканирование продолжено с места остановки (фаза {db.get('scan.phase')}, "
                     f"смещение {db.get('scan.offset')})")
        db.set("scan.state", "running")
        db.set("scan.error", "")
        y = YandexClient(s["yandex_token"], stop_event=self.stop_scan, log=self.log)
        has_ya = bool(s["yandex_token"])
        try:
            if db.get("scan.phase") == "main" and not has_ya:
                db.set("scan.phase", "local")
            if db.get("scan.phase") == "main":
                if s["scan_mode"] == "tree":
                    self._scan_tree(y, "disk:/", scan_id)
                else:
                    self._scan_flat(y, scan_id)
                db.set("scan.phase", "photounlim")
                db.set("scan.offset", 0)
            if db.get("scan.phase") == "photounlim":
                if s["include_photounlim"]:
                    self._scan_photounlim(y, scan_id)
                db.set("scan.phase", "local")
                db.set("scan.offset", 0)
            if db.get("scan.phase") == "local":
                self._scan_local(scan_id)
                db.set("scan.phase", "done")
            if self.stop_scan.is_set():
                raise Paused()
            db.set("scan.state", "done")
            st = db.stats()
            total = sum(v["count"] for v in st["by_status"].values())
            self.log(f"Сканирование завершено. Всего файлов в базе: {total}")
        except Paused:
            db.set("scan.state", "stopped")
            self.log("Сканирование остановлено, его можно продолжить", "warn")
        except Exception as e:  # noqa
            db.set("scan.state", "interrupted")
            db.set("scan.error", str(e))
            self.log(f"Сканирование прервано ошибкой: {e}", "error")
            self.log(traceback.format_exc(), "debug")

    def _scan_photounlim(self, y, scan_id):
        """Раздел «Фото» (безлимитная автозагрузка). Официального метода нет — пробуем известные."""
        method, report = y.probe_photounlim()
        self.db.set("photounlim.method", method or "")
        if method == "tree":
            self._scan_tree(y, "photounlim:/", scan_id)
            return
        if method == "flat":
            limit, offset = 1000, int(self.db.get("scan.offset", "0"))
            while not self.stop_scan.is_set():
                files, raw = y.photounlim_flat_page(offset, limit)
                if files:
                    self.db.upsert_files(files, scan_id)
                    self._bump_found(len(files))
                offset += raw
                self.db.set("scan.offset", offset)
                if raw < limit:
                    return
                self.log(f"Сканирование раздела «Фото»: получено {offset} записей…")
            return
        try:
            size = int(y.disk_info().get("photounlim_size") or 0)
        except ApiError:
            size = 0
        if size:
            self.log(f"В разделе «Фото» (безлимитная автозагрузка) лежит {size / 2**30:.1f} ГБ, но Яндекс НЕ "
                     f"отдаёт эти файлы через публичный API ({'; '.join(report)}). Перенесите их через блок "
                     f"«Папки на компьютере»: программа «Яндекс Диск» для Windows умеет копировать их на ПК.",
                     "warn")
        else:
            self.log("Безлимитных фото через API не найдено (" + "; ".join(report) + ")")

    def _scan_local(self, scan_id):
        dirs = local.parse_dirs(self.settings["local_dirs"])
        if not dirs:
            return
        total, young = 0, 0
        for d in dirs:
            if not os.path.isdir(d):
                self.log(f"Папка не найдена: {d}", "warn")
                continue
            for batch, y_cnt in local.scan_dir(d, self.settings["local_min_age_sec"], self.stop_scan):
                if batch:
                    self.db.upsert_files(batch, scan_id)
                    self._bump_found(len(batch))
                    total += len(batch)
                young = y_cnt
            self.log(f"Папка «{d}»: найдено файлов {total}")
        if young:
            self.log(f"Пропущено {young} файлов, которые менялись меньше "
                     f"{self.settings['local_min_age_sec']} с назад (ещё скачиваются) — возьму при следующем проходе")

    def check_local_dirs(self):
        out = []
        for d in local.parse_dirs(self.settings["local_dirs"]):
            if not os.path.isdir(d):
                out.append({"dir": d, "ok": False, "msg": "папка не найдена"})
                continue
            n = size = 0
            for dirpath, _, files in os.walk(d):
                for fn in files:
                    try:
                        size += os.path.getsize(os.path.join(dirpath, fn))
                        n += 1
                    except OSError:
                        pass
            out.append({"dir": d, "ok": True, "files": n, "bytes": size})
        return {"dirs": out}

    def _scan_local_only(self, fresh=False):
        try:
            self._scan_local(int(self.db.get("scan.id", "1")))
        except Exception as e:  # noqa
            self.log(f"Сканирование папок на компьютере: ошибка {e}", "error")

    def _bump_found(self, n):
        found = int(self.db.get("scan.found", "0")) + n
        self.db.set("scan.found", found)

    def _scan_flat(self, y, scan_id):
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
            self.log(f"Сканирование: получено {offset} записей…")

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
            self.log(f"Сканирование {root}: {path} — всего найдено {n}")

    def stop_scanning(self):
        self.stop_scan.set()

    # ================================================================== перенос
    def start_transfer(self):
        with self.lock:
            if any(t.is_alive() for t in self.workers):
                if self.transfer_state == "pausing":
                    return False
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
            self.db.set("transfer.wanted", 1)
            n = s["workers"]
            self.workers = [threading.Thread(target=self._worker, args=(i,), daemon=True, name=f"w{i}")
                            for i in range(n)]
            for t in self.workers:
                t.start()
            threading.Thread(target=self._supervise, daemon=True).start()
            if self._watching_local():
                threading.Thread(target=self._local_watcher, daemon=True).start()
            self.log(f"Перенос запущен, потоков: {n}")
            return True

    def pause_transfer(self, reason="", by_user=True):
        with self.lock:
            if by_user:
                self.db.set("transfer.wanted", 0)
            if self.transfer_state in ("running",):
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
                self.log(f"Перенос завершён. Готово: {st.get('done', {}).get('count', 0)}, "
                         f"ошибок: {st.get('error', {}).get('count', 0)}")

    def _watching_local(self):
        return bool(self.settings["local_watch"] and local.parse_dirs(self.settings["local_dirs"]))

    def _local_watcher(self):
        """Пока идёт перенос — периодически пересканирует папки (туда докачивает программа Яндекса)."""
        while not self.stop_transfer.wait(max(1, self.settings["local_rescan_min"]) * 60):
            if self._watching_local() and not self.scanning():
                try:
                    self.start_scan(only_local=True)
                except Exception as e:  # noqa
                    self.log(f"Пересканирование папок: {e}", "warn")

    def scanning(self):
        return bool(self.scan_thread and self.scan_thread.is_alive())

    def _worker(self, wid):
        stop = self.stop_transfer
        while not stop.is_set():
            wait = self.cooldown_until - time.time()
            if wait > 0:
                stop.wait(min(wait, 5))
                continue
            row = self.db.claim_next()
            if row is None:
                if self.scanning() or self.db.has_waiting_retries() or self._watching_local():
                    stop.wait(3)
                    continue
                return
            try:
                self._transfer_file(wid, row)
            except Paused:
                self.db.update_file(row["id"], status="pending")
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
            if delay >= 900:
                extra = (" Похоже на суточный лимит Google (≈750 ГБ загрузки в сутки на аккаунт) — "
                         "программа сама продолжит позже.")
            self.log(f"{e}. Общая пауза {delay // 60} мин {delay % 60} с.{extra}", "warn")

    def _add_speed(self, n):
        now = time.time()
        with self.lock:
            self.speed.append((now, n))
            while self.speed and self.speed[0][0] < now - 60:
                self.speed.popleft()

    def speed_bps(self):
        now = time.time()
        with self.lock:
            items = [x for x in self.speed if x[0] >= now - 30]
        if not items:
            return 0
        span = max(5.0, now - items[0][0])
        return sum(n for _, n in items) / span

    def _transfer_file(self, wid, row):
        s = self.settings
        stop = self.stop_transfer
        size = row["size"]
        info = {"id": row["id"], "path": row["path"], "size": size, "done": row["bytes_done"],
                "stage": "подготовка", "started": time.time()}
        self.active[wid] = info
        drive = self.drive(stop)
        y = self.yandex(stop)

        parent = drive.ensure_folder(drive_dir_for(row, s), s["drive_root"])
        props = {APP_PROP: "1"}
        if row["md5"]:
            props["ya_md5"] = row["md5"]
        uri = row["upload_uri"]

        if not uri:
            if s["skip_existing"]:
                info["stage"] = "проверка наличия на Drive"
                for f in drive.find_in_parent(parent, row["name"]):
                    if int(f.get("size") or -1) != size:
                        continue
                    if row["source"] == "local" and not row["md5"]:
                        row["md5"] = local.file_md5(row["path"])
                        self.db.update_file(row["id"], md5=row["md5"])
                    if not row["md5"] or f.get("md5Checksum") == row["md5"]:
                        self.db.update_file(row["id"], status="done", gd_id=f["id"], bytes_done=size,
                                            last_error=None, note="уже был на Drive")
                        self._maybe_delete_local(row)
                        return
            if size == 0:
                f = drive.create_empty(row["name"], parent, row["mime"], props, row["modified"])
                self._finish(row, f, drive)
                return
            uri = drive.start_upload(row["name"], parent, size, row["mime"], props, row["modified"])
            self.db.update_file(row["id"], upload_uri=uri, bytes_done=0)

        chunk = s["chunk_mb"] * 1024 * 1024
        offset = None          # сколько байт подтвердил Google (None — неизвестно, спросить)
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
                            self._finish(row, val, drive)
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
                        if row["source"] == "local":
                            stream, pos = local.open_stream(row["path"], offset)
                        else:
                            stream, pos = y.open_stream(row["path"], offset)
                        if pos < offset:
                            skipped = read_exact(stream.raw, offset - pos)
                            if len(skipped) != offset - pos:
                                raise ApiError("Яндекс оборвал поток", retryable=True)
                    want = min(chunk, size - offset)
                    info["stage"] = "передача"
                    base = offset
                    buf = read_exact(stream.raw, want,
                                     lambda n: info.__setitem__("done", base + n))
                    if len(buf) != want:
                        raise ApiError(f"Яндекс оборвал поток ({len(buf)} из {want} байт)", retryable=True)
                    st, val = drive.put_chunk(uri, buf, offset, size)
                    if st == "done":
                        self._add_speed(len(buf))
                        self._finish(row, val, drive)
                        return
                    if st == "gone":
                        offset = None
                        continue
                    accepted = val - offset
                    if accepted > 0:
                        self._add_speed(accepted)
                    if val != offset + len(buf):
                        # Google принял меньше, чем отправили — перечитываем с нужного места
                        stream.close()
                        stream = None
                    offset = val
                    info["done"] = offset
                    self.db.update_file(row["id"], bytes_done=offset)
                    errors = 0
                    with self.lock:
                        self.rate_hits = 0
                except (AuthError, FatalPause, RateLimited, Paused):
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

    def _maybe_delete_local(self, row):
        if row["source"] != "local" or not self.settings["local_delete_after"] or not row["md5"]:
            return
        try:
            os.remove(row["path"][len(local.PREFIX):])
            self.db.update_file(row["id"], note="загружен и сверен, копия на компьютере удалена")
        except OSError as e:
            self.log(f"Не удалось удалить локальную копию {row['path']}: {e}", "warn")

    def _finish(self, row, f, drive):
        s = self.settings
        gmd5 = f.get("md5Checksum")
        if row["source"] == "local":
            row["md5"] = local.file_md5(row["path"])   # MD5 считаем по локальному файлу
            self.db.update_file(row["id"], md5=row["md5"])
        if s["verify_md5"] and row["md5"] and gmd5 and gmd5 != row["md5"]:
            drive.delete(f["id"])
            self.db.update_file(row["id"], upload_uri=None, bytes_done=0)
            raise ApiError(f"MD5 не совпал (Яндекс {row['md5']}, Google {gmd5}) — файл удалён с Drive, "
                           f"будет загружен заново", retryable=True)
        if f.get("size") is not None and int(f["size"]) != row["size"]:
            drive.delete(f["id"])
            self.db.update_file(row["id"], upload_uri=None, bytes_done=0)
            raise ApiError("Размер на Drive не совпал — файл будет загружен заново", retryable=True)
        self.db.update_file(row["id"], status="done", gd_id=f["id"], upload_uri=None,
                            bytes_done=row["size"], last_error=None, note=None)
        if gmd5 and row["md5"] == gmd5:
            self._maybe_delete_local(row)

    # ================================================================== служебные операции
    def busy(self):
        return any(t.is_alive() for t in self.workers) or (self.task_thread and self.task_thread.is_alive())

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
        """Сверка с Drive: файлы, удалённые/убранные в корзину на Drive, снова ставятся в очередь."""
        def fn():
            self.task["msg"] = "получаю список файлов на Google Drive…"
            ids = self.drive().list_app_file_ids()
            self.db.folders_clear()
            rows = self.db.q("SELECT id, gd_id FROM files WHERE status='done'")
            missing = [r["id"] for r in rows if r["gd_id"] not in ids]
            for fid in missing:
                self.db.update_file(fid, status="pending", gd_id=None, upload_uri=None, bytes_done=0,
                                    attempts=0, next_try=0, note="не найден на Drive — повторно")
            return f"на Drive {len(ids)} файлов программы; перенесённых по базе {len(rows)}; " \
                   f"возвращено в очередь {len(missing)}"
        self.run_task("Сверка с Google Drive", fn)

    def retry_errors(self):
        n = self.db.x("UPDATE files SET status='pending', attempts=0, next_try=0 WHERE status='error'")
        self.log(f"В очередь возвращено файлов с ошибками: {n}")
        return n

    def requeue_all(self):
        if self.busy():
            raise ApiError("Сначала остановите перенос")
        n = self.db.x("UPDATE files SET status='pending', attempts=0, next_try=0, gd_id=NULL, "
                      "upload_uri=NULL, bytes_done=0, last_error=NULL, note=NULL")
        self.db.folders_clear()
        self.log(f"Все файлы ({n}) поставлены в очередь заново. Уже лежащие на Drive одинаковые файлы "
                 f"будут пропущены, если включено «Не заливать повторно».")
        return n

    def reset_db(self):
        if self.busy() or self.scanning():
            raise ApiError("Сначала остановите перенос и сканирование")
        self.db.x("DELETE FROM files")
        self.db.folders_clear()
        for k in ("scan.state", "scan.phase", "scan.offset", "scan.found", "scan.error"):
            self.db.set(k, None)
        self.log("База очищена. Нужно заново просканировать Яндекс Диск.")

    def file_action(self, fid, action):
        if action == "retry":
            self.db.update_file(fid, status="pending", attempts=0, next_try=0)
        elif action == "skip":
            self.db.x("UPDATE files SET status='skipped', note='пропущен вручную' "
                      "WHERE id=? AND status!='active'", (fid,))
        elif action == "requeue":
            self.db.x("UPDATE files SET status='pending', attempts=0, next_try=0, gd_id=NULL, "
                      "upload_uri=NULL, bytes_done=0 WHERE id=? AND status!='active'", (fid,))

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
                           "google": bool(self.settings["google_refresh_token"]),
                           "google_client": bool(self.settings["google_client_id"])},
            "google_email": self.settings["google_email"],
            "photounlim_method": db.get("photounlim.method"),
            "watching_local": self._watching_local(),
        }
