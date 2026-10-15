"""SQLite-хранилище состояния: список файлов, статусы, кэш папок Google Drive.

Всё, что нужно для продолжения после падения, лежит здесь:
- статус каждого файла и количество подтверждённых Google байт;
- URI резюмируемой загрузки Google (живёт ~неделю), чтобы дозалить большой файл с места обрыва;
- смещение сканирования Яндекс Диска.
"""
import sqlite3
import threading
import time

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
    taken         TEXT,                      -- exif date_time, если есть
    mime          TEXT,
    media_type    TEXT,
    status        TEXT NOT NULL DEFAULT 'pending', -- pending|active|done|error|skipped
    attempts      INTEGER NOT NULL DEFAULT 0,
    last_error    TEXT,
    next_try      REAL NOT NULL DEFAULT 0,
    gd_id         TEXT,
    upload_uri    TEXT,
    bytes_done    INTEGER NOT NULL DEFAULT 0,
    note          TEXT,                      -- например «уже был на Drive»
    verified      REAL,                      -- когда подтверждено: на Drive тот же размер и MD5
    seen_scan     INTEGER NOT NULL DEFAULT 0,
    updated       REAL NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS files_status ON files(status, next_try, id);
CREATE TABLE IF NOT EXISTS folders (
    path   TEXT PRIMARY KEY,                 -- относительный путь папки на Drive ('' = корень)
    gd_id  TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS kv (
    key    TEXT PRIMARY KEY,
    value  TEXT
);
"""

FILE_COLS = ("path", "source", "name", "size", "md5", "sha256", "modified",
             "created", "taken", "mime", "media_type")


class DB:
    def __init__(self, path):
        self.path = path
        self._lock = threading.RLock()
        self.conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.executescript(SCHEMA)
        cols = {r[1] for r in self.conn.execute("PRAGMA table_info(files)")}
        if "verified" not in cols:
            self.conn.execute("ALTER TABLE files ADD COLUMN verified REAL")

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
        return r[0] if r else default

    def set(self, key, value):
        self.x("INSERT INTO kv(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
               (key, None if value is None else str(value)))

    # ---------- файлы ----------
    def upsert_files(self, items, scan_id):
        """items: список dict с ключами FILE_COLS. Новые → pending.
        Если у уже перенесённого файла поменялись размер/md5 — ставим в очередь заново."""
        now = time.time()
        with self._lock:
            self.conn.execute("BEGIN")
            try:
                for it in items:
                    vals = [it.get(c) for c in FILE_COLS]
                    self.conn.execute(
                        f"""INSERT INTO files({",".join(FILE_COLS)}, seen_scan, updated)
                            VALUES({",".join("?" * len(FILE_COLS))}, ?, ?)
                            ON CONFLICT(path) DO UPDATE SET
                              status = CASE WHEN files.status IN ('done','skipped')
                                              AND (files.size != excluded.size
                                                   OR (excluded.md5 IS NOT NULL AND IFNULL(files.md5,'') != excluded.md5))
                                            THEN 'pending' ELSE files.status END,
                              gd_id = CASE WHEN files.size != excluded.size
                                              OR (excluded.md5 IS NOT NULL AND IFNULL(files.md5,'') != excluded.md5)
                                           THEN NULL ELSE files.gd_id END,
                              upload_uri = CASE WHEN files.size != excluded.size
                                              OR (excluded.md5 IS NOT NULL AND IFNULL(files.md5,'') != excluded.md5)
                                           THEN NULL ELSE files.upload_uri END,
                              size=excluded.size, md5=COALESCE(excluded.md5, files.md5), sha256=excluded.sha256,
                              modified=excluded.modified, created=excluded.created,
                              taken=excluded.taken, mime=excluded.mime,
                              media_type=excluded.media_type, name=excluded.name,
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

    def claim_next(self, excluded=()):
        """Атомарно забирает следующий файл в работу (сначала недокачанные большие)."""
        now = time.time()
        ex, args = self._excl(excluded)
        with self._lock:
            row = self.conn.execute(
                f"SELECT * FROM files WHERE status='pending' AND next_try<=?{ex} "
                "ORDER BY (upload_uri IS NULL), id LIMIT 1", [now] + args).fetchone()
            if row is None:
                return None
            self.conn.execute("UPDATE files SET status='active', updated=? WHERE id=?", (now, row["id"]))
            return dict(row)

    def has_waiting(self, excluded=()):
        ex, args = self._excl(excluded)
        return self.q1(f"SELECT 1 FROM files WHERE status='pending'{ex} LIMIT 1", args) is not None

    def update_file(self, fid, **kw):
        kw["updated"] = time.time()
        cols = ", ".join(f"{k}=?" for k in kw)
        self.x(f"UPDATE files SET {cols} WHERE id=?", list(kw.values()) + [fid])

    def reset_active(self):
        """После падения: всё, что было «в работе», возвращаем в очередь (URI загрузки сохраняется)."""
        return self.x("UPDATE files SET status='pending' WHERE status='active'")

    def stats(self):
        rows = self.q("SELECT status, COUNT(*) n, IFNULL(SUM(size),0) s FROM files GROUP BY status")
        out = {r["status"]: {"count": r["n"], "bytes": r["s"]} for r in rows}
        partial = self.q1("SELECT IFNULL(SUM(bytes_done),0) FROM files WHERE status IN ('pending','active')")[0]
        src = self.q("SELECT source, COUNT(*) n, IFNULL(SUM(size),0) s FROM files GROUP BY source")
        w = self.q1("SELECT COUNT(*), IFNULL(SUM(size),0) FROM files WHERE status='pending' AND next_try>?",
                    (time.time(),))
        ver = self.q1("SELECT COUNT(*) FROM files WHERE status='done' AND verified IS NOT NULL")[0]
        return {"by_status": out, "partial_bytes": partial, "waiting": {"count": w[0], "bytes": w[1]},
                "verified": ver,
                "by_source": {r["source"]: {"count": r["n"], "bytes": r["s"]} for r in src}}

    def list_files(self, status=None, query=None, offset=0, limit=100):
        where, args = [], []
        if status == "waiting":
            where.append("status='pending' AND next_try>?")
            args.append(time.time())
        elif status:
            where.append("status=?")
            args.append(status)
        if query:
            where.append("path LIKE ?")
            args.append(f"%{query}%")
        w = ("WHERE " + " AND ".join(where)) if where else ""
        total = self.q1(f"SELECT COUNT(*) FROM files {w}", args)[0]
        rows = self.q(f"SELECT id,path,size,status,attempts,last_error,bytes_done,gd_id,note,updated,"
                      f"verified,next_try "
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
