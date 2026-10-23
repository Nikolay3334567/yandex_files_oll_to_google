"""SQLite-хранилище состояния: список файлов, статусы, кэш папок Google Drive.

Всё, что нужно для продолжения после падения или выключения света, лежит здесь:
- статус каждого файла и количество подтверждённых Google байт;
- URI резюмируемой загрузки Google (живёт ~неделю), чтобы дозалить большой файл с места обрыва;
- путь к файлу в буфере на диске (недокачанный .part докачивается с места обрыва);
- смещение сканирования Яндекс Диска.

Статусы файла:
  pending   — ждёт (если next_try в будущем — ждёт повтора);
  active    — качается с Яндекса (или идёт напрямую Яндекс → Google);
  buffered  — скачан в буфер на диске, MD5 сверен, ждёт загрузки в Google;
  uploading — заливается в Google из буфера;
  deferred  — Яндекс не отдал файл: повтор только вручную;
  done      — на Google тот же размер и MD5; error — не вышло; skipped — пропущен вручную.

Метка разметки (probe): ok — Яндекс отдаёт файл, slow — отдаёт «по капле», dead — не отдаёт,
missing — файла нет на Яндексе, NULL — не проверялся (см. probe.py).
"""
import sqlite3
import threading
import time

FOREVER = 4102444800.0       # 2100-01-01: «больше не повторять автоматически»

SCHEMA = """
CREATE TABLE IF NOT EXISTS files (
    id            INTEGER PRIMARY KEY,
    path          TEXT NOT NULL UNIQUE,      -- путь на Яндексе: disk:/... или photounlim:/...
    source        TEXT NOT NULL,             -- disk | photounlim
    name          TEXT NOT NULL,
    size          INTEGER NOT NULL DEFAULT 0,
    md5           TEXT,
    sha256        TEXT,
    modified      TEXT,
    created       TEXT,
    taken         TEXT,                      -- дата съёмки, если есть
    mime          TEXT,
    media_type    TEXT,
    status        TEXT NOT NULL DEFAULT 'pending',
    attempts      INTEGER NOT NULL DEFAULT 0,
    last_error    TEXT,
    next_try      REAL NOT NULL DEFAULT 0,
    gd_id         TEXT,
    upload_uri    TEXT,
    bytes_done    INTEGER NOT NULL DEFAULT 0,
    note          TEXT,
    verified      REAL,                      -- когда подтверждено: на Drive тот же размер и MD5
    seen_scan     INTEGER NOT NULL DEFAULT 0,
    updated       REAL NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS folders (
    path   TEXT PRIMARY KEY,                 -- полный путь папки на Drive от «Мой диск»
    gd_id  TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS kv (
    key    TEXT PRIMARY KEY,
    value  TEXT
);
"""

EXTRA_COLS = {
    "verified": "REAL",
    "local_path": "TEXT",                    # файл в буфере (.part — качается, .dat — готов)
    "defers": "INTEGER NOT NULL DEFAULT 0",  # сколько раз откладывался из-за молчащего сервера
    "preview_id": "TEXT",                    # id превью на Drive, пока оригинал недоступен
    "done_at": "REAL",
    "uploaded": "INTEGER NOT NULL DEFAULT 0",  # байт реально залито в Google (для суточного лимита)
    "section": "INTEGER NOT NULL DEFAULT 0",   # 1 — идёт в папку «Фото», 0 — в «Файлы» (engine.goes_to_photos)
    "probe": "TEXT",                         # метка разметки: ok | slow | dead | missing
    "probe_at": "REAL",
    "probe_ms": "INTEGER",                   # задержка: ссылка + первый ответ Яндекса, мс
    "probe_bps": "REAL",                     # скорость при пробе, байт/с
    "probe_note": "TEXT",
}

FILE_COLS = ("path", "source", "name", "size", "md5", "sha256", "modified",
             "created", "taken", "mime", "media_type", "section")

# Раздел хранится готовым числом (section), а не вычисляется на каждый выбор файла:
# на сотнях тысяч строк вычисляемое условие делало выбор следующего файла в 20 раз медленнее.
ORDERS = {
    "photos_first": "section DESC, id",
    "files_first": "section, id",
    "small_first": "size, id",
    "as_listed": "id",
}


class DB:
    def __init__(self, path):
        self.path = path
        self._lock = threading.RLock()
        self.conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None, timeout=30)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        # FULL: прогресс переживает и внезапное выключение питания, а не только падение программы
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.executescript(SCHEMA)
        cols = {r[1] for r in self.conn.execute("PRAGMA table_info(files)")}
        for c, t in EXTRA_COLS.items():
            if c not in cols:
                self.conn.execute(f"ALTER TABLE files ADD COLUMN {c} {t}")
        self.conn.execute("CREATE INDEX IF NOT EXISTS files_status ON files(status, next_try, id)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS files_section ON files(status, section, id)")
        if self.get("folders.v") != "2":
            # версия 1 хранила пути папок относительно корня — кэш просто строится заново
            self.conn.execute("DELETE FROM folders")
            self.set("folders.v", 2)

    def close(self):
        with self._lock:
            self.conn.close()

    # ---------- низкоуровневое ----------
    def q(self, sql, args=()):
        with self._lock:
            return self.conn.execute(sql, args).fetchall()

    def q1(self, sql, args=()):
        with self._lock:
            return self.conn.execute(sql, args).fetchone()

    def x(self, sql, args=()):
        with self._lock:
            cur = self.conn.execute(sql, args)
            return cur.rowcount

    # ---------- kv ----------
    def get(self, key, default=None):
        r = self.q1("SELECT value FROM kv WHERE key=?", (key,))
        return r[0] if r and r[0] is not None else default

    def incr(self, key, n=1):
        with self._lock:
            self.set(key, int(self.get(key, "0")) + n)

    def set(self, key, value):
        self.x("INSERT INTO kv(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
               (key, None if value is None else str(value)))

    # ---------- файлы ----------
    def upsert_files(self, items, scan_id):
        """items: список dict с ключами FILE_COLS. Новые → pending.
        Если у уже перенесённого файла поменялись размер/md5 — ставим в очередь заново."""
        now = time.time()
        changed = ("files.size != excluded.size OR (excluded.md5 IS NOT NULL "
                   "AND IFNULL(files.md5,'') != excluded.md5)")
        with self._lock:
            self.conn.execute("BEGIN")
            try:
                for it in items:
                    it = dict(it)
                    if it.get("section") is None:     # раздел обычно считает engine; иначе безлимит — в «Фото»
                        it["section"] = 1 if it.get("source") == "photounlim" else 0
                    vals = [it.get(c) for c in FILE_COLS]
                    self.conn.execute(
                        f"""INSERT INTO files({",".join(FILE_COLS)}, seen_scan, updated)
                            VALUES({",".join("?" * len(FILE_COLS))}, ?, ?)
                            ON CONFLICT(path) DO UPDATE SET
                              status = CASE WHEN files.status IN ('done','skipped','error') AND ({changed})
                                            THEN 'pending' ELSE files.status END,
                              gd_id = CASE WHEN {changed} THEN NULL ELSE files.gd_id END,
                              upload_uri = CASE WHEN {changed} THEN NULL ELSE files.upload_uri END,
                              size=excluded.size, md5=COALESCE(excluded.md5, files.md5), sha256=excluded.sha256,
                              modified=excluded.modified, created=excluded.created,
                              taken=COALESCE(excluded.taken, files.taken), mime=excluded.mime,
                              media_type=excluded.media_type, name=excluded.name, section=excluded.section,
                              seen_scan=excluded.seen_scan""",
                        vals + [scan_id, now])
                self.conn.execute("COMMIT")
            except Exception:
                self.conn.execute("ROLLBACK")
                raise

    @staticmethod
    def _excl(excluded):
        if not excluded:
            return "", []
        return f" AND source NOT IN ({','.join('?' * len(excluded))})", list(excluded)

    def _ready(self, excluded, retry_errors):
        """Условие «файл можно брать в работу сейчас»."""
        ex, args = self._excl(excluded)
        st = "(status='pending' OR status='error')" if retry_errors else "status='pending'"
        return f"{st} AND next_try<=?{ex}", args

    def claim_download(self, excluded=(), order="photos_first", fits=None, retry_errors=True):
        """Атомарно забирает следующий файл для скачивания с Яндекса.

        fits=(свободно_в_буфере, размер_буфера): брать только то, что влезет в буфер сейчас,
        или то, что не влезет никогда (оно пойдёт напрямую Яндекс → Google).
        Недокачанный файл тоже ждёт, пока влезет целиком: его часть уже учтена как занятое место.
        Сначала — начатые: недокачанные в буфер и недозалитые напрямую.
        """
        now = time.time()
        cond, args = self._ready(excluded, retry_errors)
        if fits is not None:
            cond += " AND (size<=? OR size>?)"
            args += [max(0, int(fits[0])), int(fits[1])]
        with self._lock:
            row = self.conn.execute(
                f"SELECT * FROM files WHERE {cond} "
                f"ORDER BY (local_path IS NULL AND upload_uri IS NULL), (attempts > 0), "
                f"(IFNULL(probe,'') IN ('dead','missing')), {ORDERS.get(order, 'id')} LIMIT 1",
                [now] + args).fetchone()
            if row is None:
                return None
            self.conn.execute("UPDATE files SET status='active', updated=? WHERE id=?", (now, row["id"]))
            return dict(row)

    def partials(self, excluded=(), retry_errors=True):
        """Готовые к работе файлы с недокачанной частью в буфере."""
        cond, args = self._ready(excluded, retry_errors)
        return self.q(f"SELECT id, size, local_path FROM files WHERE {cond} AND local_path IS NOT NULL "
                      f"ORDER BY id", [time.time()] + args)

    def claim_id(self, fid):
        with self._lock:
            n = self.conn.execute("UPDATE files SET status='active', updated=? WHERE id=? "
                                  "AND status IN ('pending','error')", (time.time(), fid)).rowcount
            if not n:
                return None
            return dict(self.conn.execute("SELECT * FROM files WHERE id=?", (fid,)).fetchone())

    def claim_upload(self):
        now = time.time()
        with self._lock:
            row = self.conn.execute("SELECT * FROM files WHERE status='buffered' AND next_try<=? "
                                    "ORDER BY id LIMIT 1", (now,)).fetchone()
            if row is None:
                return None
            self.conn.execute("UPDATE files SET status='uploading', updated=? WHERE id=?", (now, row["id"]))
            return dict(row)

    def has_waiting(self, excluded=(), retry_errors=True):
        cond, args = self._ready(excluded, retry_errors)
        cond = cond.replace(" AND next_try<=?", " AND next_try<?")
        return self.q1(f"SELECT 1 FROM files WHERE {cond} LIMIT 1", [FOREVER] + args) is not None

    def has_ready(self, excluded=(), retry_errors=True):
        cond, args = self._ready(excluded, retry_errors)
        return self.q1(f"SELECT 1 FROM files WHERE {cond} LIMIT 1", [time.time()] + args) is not None

    def count_status(self, *statuses):
        return self.q1(f"SELECT COUNT(*) FROM files WHERE status IN ({','.join('?' * len(statuses))})",
                       statuses)[0]

    def next_wakeup(self, excluded=(), retry_errors=True):
        cond, args = self._ready(excluded, retry_errors)
        cond = cond.replace(" AND next_try<=?", " AND next_try<?")
        r = self.q1(f"SELECT MIN(next_try) FROM files WHERE {cond}", [FOREVER] + args)
        return r[0] if r else None

    def recompute_sections(self, photos_cond):
        """Пересчитать раздел всех файлов по правилу (один проход — при смене настройки)."""
        return self.x(f"UPDATE files SET section = CASE WHEN {photos_cond} THEN 1 ELSE 0 END")

    def set_probe(self, fid, result, ms=None, bps=None, note=None):
        """Метка разметки. Время изменения файла (updated) не трогаем — список «Файлы» не перемешивается."""
        self.x("UPDATE files SET probe=?, probe_at=?, probe_ms=?, probe_bps=?, probe_note=? WHERE id=?",
               (result, time.time() if result else None, ms, bps, note or None, fid))

    def update_file(self, fid, **kw):
        kw["updated"] = time.time()
        cols = ", ".join(f"{k}=?" for k in kw)
        self.x(f"UPDATE files SET {cols} WHERE id=?", list(kw.values()) + [fid])

    def reset_active(self):
        """После падения: «в работе» — снова в очередь (место загрузки и .part сохраняются),
        «заливается из буфера» — снова «в буфере»."""
        a = self.x("UPDATE files SET status='pending' WHERE status='active'")
        b = self.x("UPDATE files SET status='buffered' WHERE status='uploading'")
        return a + b

    def buffer_reserved(self):
        """Сколько байт буфера занято или обещано (каждый файл в буфере резервирует свой полный размер)."""
        return self.q1("SELECT IFNULL(SUM(size),0) FROM files WHERE local_path IS NOT NULL")[0]

    def stats(self):
        now = time.time()
        rows = self.q("SELECT status, COUNT(*) n, IFNULL(SUM(size),0) s FROM files GROUP BY status")
        out = {r["status"]: {"count": r["n"], "bytes": r["s"]} for r in rows}
        partial = self.q1("SELECT IFNULL(SUM(bytes_done),0) FROM files "
                          "WHERE status IN ('pending','active','buffered','uploading')")[0]
        w = self.q1("SELECT COUNT(*), IFNULL(SUM(size),0) FROM files WHERE status='pending' AND next_try>?",
                    (now,))
        ver = self.q1("SELECT COUNT(*) FROM files WHERE status='done' AND verified IS NOT NULL")[0]
        prev = self.q1("SELECT COUNT(*) FROM files WHERE preview_id IS NOT NULL AND status!='done'")[0]
        day = self.q1("SELECT IFNULL(SUM(uploaded),0) FROM files WHERE done_at>?", (now - 86400,))[0]
        src = {}
        for r in self.q("SELECT CASE WHEN section THEN 'photos' ELSE 'files' END sec, status, "
                        "COUNT(*) n, IFNULL(SUM(size),0) s FROM files GROUP BY sec, status"):
            d = src.setdefault(r["sec"], {"count": 0, "bytes": 0, "done": 0, "done_bytes": 0})
            d["count"] += r["n"]
            d["bytes"] += r["s"]
            if r["status"] in ("done", "skipped"):
                d["done"] += r["n"]
                d["done_bytes"] += r["s"]
        return {"by_status": out, "partial_bytes": partial, "waiting": {"count": w[0], "bytes": w[1]},
                "verified": ver, "previews": prev, "google_24h": day, "by_section": src}

    def list_files(self, status=None, query=None, offset=0, limit=100):
        where, args = [], []
        if status == "waiting":
            where.append("status='pending' AND next_try>?")
            args.append(time.time())
        elif status == "preview":
            where.append("preview_id IS NOT NULL AND status!='done'")
        elif status in ("probe_ok", "probe_dead", "probe_none"):
            where.append("status NOT IN ('done','skipped') AND " + {
                "probe_ok": "probe IN ('ok','slow')", "probe_dead": "probe IN ('dead','missing')",
                "probe_none": "probe IS NULL"}[status])
        elif status:
            where.append("status=?")
            args.append(status)
        if query:
            where.append("path LIKE ?")
            args.append(f"%{query}%")
        w = ("WHERE " + " AND ".join(where)) if where else ""
        total = self.q1(f"SELECT COUNT(*) FROM files {w}", args)[0]
        rows = self.q(f"SELECT id,path,source,size,status,attempts,last_error,bytes_done,gd_id,note,updated,"
                      f"verified,next_try,defers,preview_id,local_path,probe,probe_ms,probe_bps,probe_note "
                      f"FROM files {w} ORDER BY updated DESC, id LIMIT ? OFFSET ?",
                      args + [limit, offset])
        return total, [dict(r) for r in rows]

    # ---------- папки Drive ----------
    def folder_get(self, path):
        r = self.q1("SELECT gd_id FROM folders WHERE path=?", (path,))
        return r[0] if r else None

    def folder_set(self, path, gd_id):
        self.x("INSERT OR REPLACE INTO folders(path, gd_id) VALUES(?,?)", (path, gd_id))

    def folders_clear(self):
        self.x("DELETE FROM folders")
