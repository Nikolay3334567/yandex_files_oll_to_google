"""Имитация API Яндекс Диска и Google Drive для проверки программы без настоящих аккаунтов.

Запуск отдельно: python tests/mock_apis.py <port> <state.json>
"""
import hashlib
import json
import os
import random
import re
import sys
import threading
import urllib.parse
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def make_files(seed=1):
    rnd = random.Random(seed)
    files = {}

    def add(path, size):
        data = bytes(rnd.getrandbits(8) for _ in range(size)) if size < 200_000 else os.urandom(size)
        files[path] = data
    add("disk:/Документы/отчёт.pdf", 12345)
    add("disk:/Документы/пустой.txt", 0)
    add("disk:/Фотокамера/IMG_0001.jpg", 300_000)
    add("disk:/Фотокамера/IMG_0002.jpg", 450_000)
    add("disk:/Фотокамера/VID_0003.mp4", 5_500_000)   # несколько кусков по 1 МБ
    add("disk:/Фотокамера/it's \"quoted\".jpg", 1000)
    add("disk:/корень.txt", 10)
    for i in range(25):
        add(f"disk:/Много/Папка{i % 3}/file{i}.bin", rnd.randint(1, 50_000))
    add("photounlim:/2019-05-04 10-00-00.JPG", 200_000)
    add("photounlim:/2021-12-31 23-59-59.MP4", 2_100_000)
    add("photounlim:/2020-07-15 12-30-00.JPG", 150_000)
    return files


PREVIEW = b"\xff\xd8\xff\xe0" + b"preview-jpeg" * 200


class State:
    def __init__(self):
        self.lock = threading.Lock()
        self.ya = make_files()
        self.drive = {}       # id -> {name, parents, mimeType, data, appProperties, trashed}
        self.sessions = {}    # sid -> {meta, size, data}
        self.faults = {"ya_drop": 0.0, "g_500": 0.0, "slow": 0.0, "pu_hidden": False,
                       "dead": [],          # пути файлов, чей сервер хранения «молчит»
                       "e504": [],          # пути файлов, на которые Яндекс отвечает 504
                       "rest_410": False,   # REST API отвечает 410 Gone на «Файлы» (как в октябре 2026)
                       "ya_429": 0,         # сколько раз подряд ответить 429 на ссылку для скачивания
                       "hang": {},          # путь → сколько секунд «первый» сервер Яндекса молчит перед ответом
                       "rest_500": False,   # API Яндекса отвечает 500 на запрос ссылки (Яндекс лежит целиком)
                       "web_session": "good-session"}
        self.calls = {"chunks": 0, "mpfs_url": 0, "small_uploads": 0, "previews": 0, "ya_download_links": 0,
                      "ranges": 0, "head_ranges": 0, "max_parallel_dl": 0, "upload_cancel": 0}
        self.parallel = 0


S = State()


def md5(b):
    return hashlib.md5(b).hexdigest()


class H(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def send_json(self, code, obj, headers=None):
        b = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(b)))
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(b)

    def body(self):
        n = int(self.headers.get("Content-Length") or 0)
        return self.rfile.read(n) if n else b""

    # ------------------------------------------------------------------ Yandex
    def ya_item(self, path):
        d = S.ya[path]
        return {"path": path, "name": path.rsplit("/", 1)[-1], "type": "file", "size": len(d), "md5": md5(d),
                "sha256": hashlib.sha256(d).hexdigest(), "modified": "2020-01-02T03:04:05+00:00",
                "created": "2019-05-04T10:00:00+00:00", "mime_type": "application/octet-stream",
                "media_type": "image"}

    def ya(self, u, qs):
        if self.headers.get("Authorization") != "OAuth good":
            return self.send_json(401, {"error": "UnauthorizedError", "description": "Не авторизован."})
        p = u.path[len("/ya/v1/disk"):]
        if p in ("", "/"):
            return self.send_json(200, {"user": {"login": "tester", "display_name": "Тестер"},
                                        "total_space": 10 ** 12, "used_space": 12345, "trash_size": 0,
                                        "is_paid": False, "will_be_overdrawn": False,
                                        "photounlim_size": sum(len(d) for k, d in S.ya.items()
                                                               if k.startswith("photounlim:"))})
        if S.faults["pu_hidden"] and (p.startswith("/photounlim/") or qs.get("path", "").startswith("photounlim:")):
            return self.send_json(403, {"error": "ForbiddenError", "description": "Нет доступа"})
        if S.faults["rest_410"] and p in ("/resources/files", "/resources", "/resources/download") \
                and not qs.get("path", "disk:").startswith("photounlim:") and "preview_size" not in qs:
            return self.send_json(410, {"error": "GoneError", "description": "Ресурс больше недоступен."})
        if p == "/resources" and "preview_size" in qs:
            path = qs["path"]
            if path not in S.ya:
                return self.send_json(404, {"error": "DiskNotFoundError", "description": "Не найдено"})
            u = f"http://127.0.0.1:{self.server.server_address[1]}/preview?" + urllib.parse.urlencode(
                {"p": path, "size": qs["preview_size"]})
            return self.send_json(200, {"preview": u, "sizes": [{"name": qs["preview_size"], "url": u}],
                                        "media_type": "image"})
        if p == "/resources/files":
            off, lim = int(qs.get("offset", 0)), int(qs.get("limit", 20))
            paths = sorted(x for x in S.ya if x.startswith("disk:"))
            return self.send_json(200, {"items": [self.ya_item(x) for x in paths[off:off + lim]],
                                        "limit": lim, "offset": off})
        if p == "/resources":
            path = qs["path"].rstrip("/")
            if path.endswith(":"):
                path += "/"
            pref = path if path.endswith("/") else path + "/"
            children, dirs = [], set()
            for x in sorted(S.ya):
                if x.startswith(pref):
                    rest = x[len(pref):]
                    if "/" in rest:
                        dirs.add(pref + rest.split("/")[0])
                    else:
                        children.append(self.ya_item(x))
            items = [{"path": d, "name": d.rsplit("/", 1)[-1], "type": "dir"} for d in sorted(dirs)] + children
            if not items and path not in ("disk:/", "photounlim:/"):
                return self.send_json(404, {"error": "DiskNotFoundError", "description": "Не найдено"})
            off, lim = int(qs.get("offset", 0)), int(qs.get("limit", 20))
            return self.send_json(200, {"path": path, "type": "dir", "_embedded": {
                "items": items[off:off + lim], "total": len(items), "offset": off, "limit": lim}})
        if p == "/resources/download":
            path = qs["path"]
            S.calls["ya_download_links"] += 1
            if S.faults["rest_500"]:
                return self.send_json(500, {"error": "InternalError", "description": "Ошибка сервера"})
            if S.faults["ya_429"] > 0:
                S.faults["ya_429"] -= 1
                return self.send_json(429, {"error": "TooManyRequestsError", "description": "Слишком много"})
            if path not in S.ya:
                return self.send_json(404, {"error": "DiskNotFoundError", "description": "Не найдено"})
            return self.send_json(200, {"href": f"http://127.0.0.1:{self.server.server_address[1]}/dl?"
                                                + urllib.parse.urlencode({"p": path})})
        return self.send_json(404, {"error": "nf"})

    def downloader(self, qs):
        """Как downloader.disk.yandex.ru: редирект на сервер хранения."""
        port = self.server.server_address[1]
        p = qs["p"]
        hang = S.faults["hang"].get(p)
        if hang:
            import time
            time.sleep(hang)
            return self.send_json(504, {"error": "gateway timeout"})
        host = "localhost" if p in S.faults["dead"] else "127.0.0.1"   # «молчащий» сервер — другой хост
        self.send_response(302)
        self.send_header("Location", f"http://{host}:{port}/st?" + urllib.parse.urlencode({"p": p}))
        self.send_header("Content-Length", "0")
        self.end_headers()

    def preview(self, qs):
        ok = self.headers.get("Authorization") == "OAuth good" or self.web_ok()
        if not ok:
            return self.send_json(401, {"error": "unauthorized"})
        S.calls["previews"] += 1
        self.send_response(200)
        self.send_header("Content-Type", "image/jpeg")
        self.send_header("Content-Length", str(len(PREVIEW)))
        self.end_headers()
        self.wfile.write(PREVIEW)

    def dl(self, qs):
        if qs["p"] in S.faults["e504"]:
            return self.send_json(504, {"error": "gateway timeout"})
        with S.lock:
            S.parallel += 1
            S.calls["max_parallel_dl"] = max(S.calls["max_parallel_dl"], S.parallel)
        try:
            return self._dl(qs)
        finally:
            with S.lock:
                S.parallel -= 1

    def _dl(self, qs):
        if qs["p"] in S.faults["dead"]:
            import time
            time.sleep(3)          # сервер не отвечает (таймаут клиента меньше)
            self.close_connection = True
            return
        data = S.ya.get(qs["p"])
        if data is None:
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        start, end = 0, len(data) - 1
        rng = self.headers.get("Range")
        if rng:
            m = re.match(r"bytes=(\d+)-(\d*)", rng)
            start = int(m.group(1))
            if m.group(2):
                end = min(end, int(m.group(2)))
                S.calls["head_ranges"] += 1      # «только начало файла» — замер и разметка
            else:
                S.calls["ranges"] += 1
            self.send_response(206)
            self.send_header("Content-Range", f"bytes {start}-{end}/{len(data)}")
        else:
            self.send_response(200)
        part = data[start:end + 1]
        self.send_header("Content-Length", str(len(part)))
        self.end_headers()
        if part and random.random() < S.faults["ya_drop"]:
            self.wfile.write(part[:len(part) // 3])
            self.close_connection = True
            return
        step = 64 * 1024
        for i in range(0, len(part), step):
            self.wfile.write(part[i:i + step])
            if S.faults["slow"]:
                import time
                time.sleep(S.faults["slow"])

    # ------------------------------------------------------------------ Яндекс, веб-версия
    def web_ok(self):
        c = self.headers.get("Cookie") or ""
        need = {"Session_id": S.faults["web_session"], "sessionid2": "x", "yandexuid": "1"}
        got = dict(p.strip().split("=", 1) for p in c.split(";") if "=" in p)
        return all(got.get(k) == v for k, v in need.items())

    def web(self, method, u, qs):
        if u.path.startswith("/passport."):
            b = b"<html>login</html>"
            self.send_response(200)
            self.send_header("Content-Length", str(len(b)))
            self.end_headers()
            self.wfile.write(b)
            return
        if u.path == "/web/client/disk":
            if not self.web_ok():
                self.send_response(302)
                self.send_header("Location", "/passport.yandex/auth")
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            b = b'<html><script>var x={"sk":"sk-123","uid":"777"};</script></html>'
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.send_header("Content-Length", str(len(b)))
            self.end_headers()
            self.wfile.write(b)
            return
        if u.path == "/web/models-v2":
            body = json.loads(self.body() or b"{}")
            if not self.web_ok() or body.get("sk") != "sk-123":
                return self.send_json(401, {"error": "unauthorized"})
            m = qs.get("m")
            rp = body.get("requestParams") or {}
            if m == "mpfs/list":
                root = rp["path"].rstrip("/")
                yroot = to_ya(root)
                if yroot in S.ya:          # список по пути файла — сам файл (так берутся превью)
                    port = self.server.server_address[1]
                    url = f"//127.0.0.1:{port}/preview?" + urllib.parse.urlencode({"p": yroot})
                    return self.send_json(200, [{"path": root, "type": "file", "name": root.rsplit("/", 1)[-1],
                                                 "meta": {"sizes": [{"name": "DEFAULT", "url": url},
                                                                    {"name": "XXXL", "url": url}]}}])
                pref = yroot.rstrip("/") + "/"
                items, dirs = [], set()
                for p, d in sorted(S.ya.items()):
                    if not p.startswith(pref):
                        continue
                    rest = p[len(pref):]
                    if "/" in rest:
                        dirs.add(rest.split("/")[0])
                        continue
                    items.append({"path": root + "/" + rest, "name": rest, "type": "file",
                                  "ctime": 1556964000, "mtime": 1556964000,
                                  "meta": {"size": len(d), "md5": md5(d), "mimetype": "image/jpeg",
                                           "mediatype": "image"}})
                items = [{"path": f"{root}/{x}", "name": x, "type": "dir"} for x in sorted(dirs)] + items
                off, amount = int(rp.get("offset", 0)), int(rp.get("amount", 40))
                return self.send_json(200, [{"path": root, "type": "dir"}] + items[off:off + amount])
            if m == "mpfs/url":
                p = rp["path"]
                yp = to_ya(p)
                if yp not in S.ya:
                    return self.send_json(200, {"error": {"id": "resource not found"}})
                S.calls["mpfs_url"] += 1
                return self.send_json(200, {"file": f"http://127.0.0.1:{self.server.server_address[1]}/dl?"
                                                    + urllib.parse.urlencode({"p": yp})})
            return self.send_json(200, {"error": {"id": "unknown method"}})
        return self.send_json(404, {})

    # ------------------------------------------------------------------ Google
    def g_auth_ok(self):
        return self.headers.get("Authorization", "").startswith("Bearer at-")

    def g_file(self, fid, fields=None):
        f = S.drive[fid]
        out = {"id": fid, "name": f["name"], "mimeType": f["mimeType"], "trashed": f["trashed"]}
        if f["mimeType"] != "application/vnd.google-apps.folder":
            out["md5Checksum"] = md5(f["data"])
            out["size"] = str(len(f["data"]))
        return out

    def g_query(self, q):
        res = []
        for fid, f in S.drive.items():
            ok = True
            m = re.search(r"name = '((?:[^'\\]|\\.)*)'", q)
            if m and f["name"] != re.sub(r"\\(.)", r"\1", m.group(1)):
                ok = False
            m = re.search(r"'([^']+)' in parents", q)
            if m and m.group(1) not in f["parents"]:
                ok = False
            if "trashed = false" in q and f["trashed"]:
                ok = False
            if "mimeType = 'application/vnd.google-apps.folder'" in q and f["mimeType"] != "application/vnd.google-apps.folder":
                ok = False
            if "mimeType != 'application/vnd.google-apps.folder'" in q and f["mimeType"] == "application/vnd.google-apps.folder":
                ok = False
            if "appProperties has" in q and f.get("appProperties", {}).get("ya2gd") != "1":
                ok = False
            if ok:
                res.append(self.g_file(fid))
        return res

    def new_file(self, meta, data=b""):
        fid = "f" + uuid.uuid4().hex[:12]
        S.drive[fid] = {"name": meta["name"], "parents": meta.get("parents") or ["root"],
                        "mimeType": meta.get("mimeType", "application/octet-stream"), "data": data,
                        "appProperties": meta.get("appProperties", {}), "trashed": False}
        return fid

    def do_google(self, method, u, qs):
        if u.path == "/g/token":
            form = urllib.parse.parse_qs(self.body().decode())
            if form.get("refresh_token", [""])[0] == "rt-good" or form.get("code", [""])[0] == "code-good":
                return self.send_json(200, {"access_token": "at-" + uuid.uuid4().hex, "expires_in": 3600,
                                            "refresh_token": "rt-good"})
            return self.send_json(400, {"error": "invalid_grant"})
        if not self.g_auth_ok():
            return self.send_json(401, {"error": {"code": 401, "message": "Invalid Credentials"}})
        if random.random() < S.faults["g_500"] and u.path.startswith("/g/upload/"):
            self.body()
            return self.send_json(503, {"error": {"code": 503, "message": "backend"}})
        with S.lock:
            p = u.path
            if p == "/g/drive/v3/about":
                return self.send_json(200, {"user": {"emailAddress": "me@example.com"},
                                            "storageQuota": {"limit": str(15 * 2 ** 30), "usage": "100"}})
            if p == "/g/drive/v3/files" and method == "GET":
                return self.send_json(200, {"files": self.g_query(qs.get("q", ""))})
            if p == "/g/drive/v3/files" and method == "POST":
                meta = json.loads(self.body() or b"{}")
                fid = self.new_file(meta)
                return self.send_json(200, self.g_file(fid))
            m = re.match(r"/g/drive/v3/files/([^/]+)$", p)
            if m:
                fid = m.group(1)
                if fid not in S.drive:
                    return self.send_json(404, {"error": {"code": 404, "message": "File not found"}})
                if method == "DELETE":
                    del S.drive[fid]
                    self.send_response(204)
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                return self.send_json(200, self.g_file(fid))
            if p == "/g/upload/drive/v3/files" and method == "POST" and qs.get("uploadType") == "multipart":
                raw = self.body()
                bnd = re.search(r"boundary=(\S+)", self.headers["Content-Type"]).group(1).encode()
                parts = raw.split(b"--" + bnd)
                meta = json.loads(parts[1].split(b"\r\n\r\n", 1)[1].rsplit(b"\r\n", 1)[0])
                data = parts[2].split(b"\r\n\r\n", 1)[1][:-2]
                fail = [x for x in S.faults.get("fail_once", []) if x in meta["name"]]
                if fail:                   # временный сбой Google: файл НЕ создан
                    S.faults["fail_once"] = [x for x in S.faults["fail_once"] if x not in fail]
                    return self.send_json(503, {"error": {"code": 503, "message": "backend"}})
                fid = self.new_file(meta, data)
                S.calls["small_uploads"] += 1
                once = [x for x in S.faults.get("lose_once", []) if x in meta["name"]]
                if once:                   # «потерять» ответ на первую загрузку файла с таким именем
                    S.faults["lose_once"] = [x for x in S.faults["lose_once"] if x not in once]
                    return self.send_json(503, {"error": {"code": 503, "message": "lost"}})
                if random.random() < S.faults.get("g_lost", 0):
                    # файл создан, но ответ «потерялся» — клиент не должен залить дубль
                    return self.send_json(503, {"error": {"code": 503, "message": "lost"}})
                return self.send_json(200, self.g_file(fid))
            if p == "/g/upload/drive/v3/files" and method == "POST":
                meta = json.loads(self.body() or b"{}")
                sid = uuid.uuid4().hex
                S.sessions[sid] = {"meta": meta, "size": int(self.headers["X-Upload-Content-Length"]),
                                   "data": bytearray()}
                return self.send_json(200, {}, {"Location": f"http://127.0.0.1:{self.server.server_address[1]}"
                                                            f"/g/upload/session/{sid}"})
            m = re.match(r"/g/upload/session/(\w+)$", p)
            if m and method == "DELETE":                 # отмена незавершённой загрузки (замер скорости)
                S.sessions.pop(m.group(1), None)
                S.calls["upload_cancel"] += 1
                return self.send_json(499, {})
            if m and method == "PUT":
                sess = S.sessions.get(m.group(1))
                body = self.body()
                if sess is None:
                    return self.send_json(404, {"error": {"code": 404, "message": "session"}})
                cr = self.headers["Content-Range"]
                mm = re.match(r"bytes (\d+)-(\d+)/(\d+)", cr)
                if mm:
                    start = int(mm.group(1))
                    if start != len(sess["data"]):
                        return self.send_json(400, {"error": {"code": 400, "message": "bad offset"}})
                    sess["data"] += body
                    S.calls["chunks"] += 1
                if len(sess["data"]) >= sess["size"]:
                    fid = self.new_file(sess["meta"], bytes(sess["data"]))
                    sess["fid"] = fid
                    return self.send_json(200, self.g_file(fid))
                hdr = {"Range": f"bytes=0-{len(sess['data']) - 1}"} if sess["data"] else {}
                self.send_response(308)
                for k, v in hdr.items():
                    self.send_header(k, v)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
        return self.send_json(404, {"error": {"code": 404, "message": "nf " + u.path}})

    # ------------------------------------------------------------------ control (для тестов)
    def control(self, method, u):
        if u.path == "/ctl/faults":
            body = json.loads(self.body())
            if body.pop("reset_parallel", False):
                S.calls["max_parallel_dl"] = S.parallel
            S.faults.update(body)
            return self.send_json(200, S.faults)
        if u.path == "/ctl/drive":
            out = {}
            for fid, f in S.drive.items():
                out[fid] = {"name": f["name"], "parents": f["parents"], "trashed": f["trashed"],
                            "folder": f["mimeType"].endswith("folder"),
                            "md5": None if f["mimeType"].endswith("folder") else md5(f["data"])}
            return self.send_json(200, out)
        if u.path == "/ctl/ya":
            return self.send_json(200, {p: md5(d) for p, d in S.ya.items()})
        if u.path == "/ctl/trash" and method == "POST":
            fid = json.loads(self.body())["id"]
            S.drive[fid]["trashed"] = True
            return self.send_json(200, {})
        if u.path == "/ctl/stats":
            return self.send_json(200, S.calls)
        if u.path == "/ctl/add_files" and method == "POST":
            b = json.loads(self.body())
            for i in range(b["count"]):
                S.ya[f"{b['prefix']}{i:04d}.{b.get('ext', 'bin')}"] = os.urandom(b.get("size", 20000))
            return self.send_json(200, {"total": len(S.ya)})
        return self.send_json(404, {})

    def route(self, method):
        u = urllib.parse.urlparse(self.path)
        qs = {k: v[0] for k, v in urllib.parse.parse_qs(u.query).items()}
        if u.path.startswith("/ctl/"):
            return self.control(method, u)
        if u.path.startswith("/ya/"):
            return self.ya(u, qs)
        if u.path == "/dl":
            return self.downloader(qs)
        if u.path == "/st":
            return self.dl(qs)
        if u.path == "/preview":
            return self.preview(qs)
        if u.path.startswith("/web/") or u.path.startswith("/passport."):
            return self.web(method, u, qs)
        if u.path.startswith("/g/"):
            return self.do_google(method, u, qs)
        self.send_json(404, {})

    def do_GET(self):
        self.route("GET")

    def do_POST(self):
        self.route("POST")

    def do_PUT(self):
        self.route("PUT")

    def do_DELETE(self):
        self.route("DELETE")


def to_ya(p):
    """/photounlim/a → photounlim:/a, /disk/A/b → disk:/A/b"""
    for root, pref in (("/photounlim", "photounlim:/"), ("/disk", "disk:/")):
        if p == root or p.startswith(root + "/"):
            return pref + p[len(root):].lstrip("/")
    return p


def start(port=0):
    srv = ThreadingHTTPServer(("127.0.0.1", port), H)
    srv.daemon_threads = True
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv


def env_for(port):
    b = f"http://127.0.0.1:{port}"
    return {"YA2GD_YANDEX_API": b + "/ya/v1/disk", "YA2GD_YANDEX_WEB": b + "/web",
            "YA2GD_STORAGE_TIMEOUT": "1", "YA2GD_DEAD_HOST_TTL": "4", "YA2GD_GOOGLE_TOKEN": b + "/g/token",
            "YA2GD_YANDEX_INTERVAL": "0.01", "YA2GD_YA_COOLDOWN": "1", "YA2GD_GROW_EVERY": "2",
            "YA2GD_GOOGLE_AUTH": b + "/g/auth", "YA2GD_GOOGLE_API": b + "/g/drive/v3",
            "YA2GD_GOOGLE_UPLOAD": b + "/g/upload/drive/v3"}


if __name__ == "__main__":
    srv = start(int(sys.argv[1]))
    print("mock on", srv.server_address, flush=True)
    threading.Event().wait()
