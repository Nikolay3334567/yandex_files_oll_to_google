"""Локальный веб-интерфейс (только 127.0.0.1) и JSON API."""
import html
import json
import os
import shutil
import sys
import threading
import time
import traceback
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from . import __version__, winsys
from .http_util import ApiError

STATIC = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
# Лицензии и README: в exe они вложены в папку legal, в исходниках лежат в корне репозитория
LEGAL_DIR = os.path.join(sys._MEIPASS, "legal") if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS") \
    else os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LEGAL = {"license": "LICENSE", "license-ru": "LICENSE.ru.txt", "third-party": "THIRD_PARTY_LICENSES.txt",
         "readme": "README.txt"}


class Handler(BaseHTTPRequestHandler):
    engine = None
    server_version = "ya2gd"

    def log_message(self, fmt, *args):
        pass

    # ---------------------------------------------------------------- helpers
    def _send(self, code, body, ctype="application/json; charset=utf-8", headers=None):
        if isinstance(body, (dict, list)):
            body = json.dumps(body, ensure_ascii=False).encode("utf-8")
        elif isinstance(body, str):
            body = body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def _redirect(self, url):
        self.send_response(302)
        self.send_header("Location", url)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _body(self):
        n = int(self.headers.get("Content-Length") or 0)
        if not n:
            return {}
        return json.loads(self.rfile.read(n).decode("utf-8"))

    def _base(self):
        return f"http://127.0.0.1:{self.server.server_address[1]}"

    # ---------------------------------------------------------------- GET
    def do_GET(self):
        u = urllib.parse.urlparse(self.path)
        qs = {k: v[0] for k, v in urllib.parse.parse_qs(u.query).items()}
        e = self.engine
        try:
            if u.path in ("/", "/index.html"):
                with open(os.path.join(STATIC, "index.html"), "rb") as f:
                    return self._send(200, f.read(), "text/html; charset=utf-8")
            if u.path == "/api/status":
                st = e.status()
                st["version"] = __version__
                st["data_dir"] = e.dir
                return self._send(200, st)
            if u.path == "/api/drives":
                return self._send(200, winsys.list_drives())
            if u.path == "/api/settings":
                return self._send(200, e.settings.public())
            if u.path == "/api/log":
                return self._send(200, e.log.since(int(qs.get("after", 0))))
            if u.path == "/api/files":
                total, rows = e.db.list_files(qs.get("status") or None, qs.get("q") or None,
                                              int(qs.get("offset", 0)), min(500, int(qs.get("limit", 100))))
                return self._send(200, {"total": total, "items": rows})
            if u.path == "/api/report.csv":
                only = qs.get("status") or None
                name = f"ya2gd-{only}.csv" if only else "ya2gd-report.csv"
                return self._send(200, e.report_csv(only), "text/csv; charset=utf-8",
                                  {"Content-Disposition": f'attachment; filename="{name}"'})
            if u.path.startswith("/legal/") and u.path[7:] in LEGAL:
                try:
                    with open(os.path.join(LEGAL_DIR, LEGAL[u.path[7:]]), encoding="utf-8-sig") as f:
                        return self._send(200, f.read(), "text/plain; charset=utf-8")
                except OSError:
                    return self._send(404, "Файл не найден. Тексты лицензий есть в репозитории: "
                                           "https://github.com/Nikolay3334567/yandex_files_oll_to_google",
                                      "text/plain; charset=utf-8")
            if u.path == "/oauth/google/start":
                auth = e.google_auth()
                if not auth.client_id:
                    return self._send(400, "Сначала сохраните Client ID и Client secret", "text/plain; charset=utf-8")
                return self._redirect(auth.auth_url(self._base() + "/oauth/google/callback", e.google_state))
            if u.path == "/oauth/google/callback":
                return self._google_callback(qs)
            return self._send(404, {"error": "not found"})
        except Exception as ex:  # noqa
            return self._error(ex)

    def _google_callback(self, qs):
        e = self.engine
        if qs.get("error"):
            return self._send(400, _page(f"Google вернул ошибку: {qs['error']}"), "text/html; charset=utf-8")
        if qs.get("state") != e.google_state:
            return self._send(400, _page("Неверный state — начните вход заново из программы"),
                              "text/html; charset=utf-8")
        auth = e.google_auth()
        rt = auth.exchange_code(qs.get("code", ""), self._base() + "/oauth/google/callback")
        e.settings.update(google_refresh_token=rt)
        try:
            info = e.test_google()
            e.settings.update(google_email=info.get("email") or "")
            msg = f"Google подключён: {info.get('email')}. Запись проверена. Окно можно закрыть."
        except Exception as ex:  # noqa
            msg = f"Токен получен, но проверка не прошла: {ex}"
        e.log(msg)
        return self._send(200, _page(msg, back=True), "text/html; charset=utf-8")

    # ---------------------------------------------------------------- POST
    def do_POST(self):
        # Защита от запросов с чужих сайтов: браузер не даст им выставить этот заголовок без CORS.
        if self.headers.get("X-Ya2gd") != "1":
            return self._send(403, {"error": "forbidden"})
        e = self.engine
        p = urllib.parse.urlparse(self.path).path
        try:
            b = self._body()
            if p == "/api/settings":
                clean = {k: v for k, v in b.items() if not (k.endswith("_set"))}
                # маскированные секреты не перезаписываем
                for k in ("yandex_token", "yandex_cookies", "google_client_secret", "google_refresh_token"):
                    if k in clean and (not clean[k] or "…" in str(clean[k])):
                        clean.pop(k)
                if "google_client_id" in clean and clean["google_client_id"] != e.settings["google_client_id"]:
                    clean.setdefault("google_refresh_token", "")
                e.settings.update(**clean)
                e.sync_sections()
                if "buffer_dir" in clean and e.transferring():
                    e.apply_buffer()            # буфер — сразу, без «Пауза → Старт»
                if "win_autorun" in clean:
                    winsys.set_autorun(e.settings["win_autorun"])
                return self._send(200, e.settings.public())
            if p == "/api/pick_folder":
                return self._send(200, {"path": winsys.pick_folder(b.get("initial") or "")})
            if p == "/api/buffer/check":
                return self._send(200, _check_buffer(b.get("path") or ""))
            if p == "/api/actions/previews":
                e.rescue_previews()
                return self._send(200, {"ok": True})
            if p == "/api/quit":
                threading.Thread(target=_quit, args=(e,), daemon=True).start()
                return self._send(200, {"ok": True})
            if p == "/api/yandex/test":
                return self._send(200, e.test_yandex())
            if p == "/api/yandex/web_test":
                return self._send(200, e.test_yandex_web())
            if p == "/api/google/test":
                return self._send(200, e.test_google())
            if p == "/api/google/logout":
                e.settings.update(google_refresh_token="", google_email="")
                return self._send(200, {"ok": True})
            if p == "/api/scan/start":
                ok = e.start_scan(bool(b.get("fresh")))
                return self._send(200, {"ok": ok})
            if p == "/api/scan/stop":
                e.stop_scanning()
                return self._send(200, {"ok": True})
            if p == "/api/transfer/start":
                return self._send(200, {"ok": e.start_transfer()})
            if p == "/api/transfer/pause":
                e.pause_transfer()
                return self._send(200, {"ok": True})
            if p == "/api/actions/retry_errors":
                return self._send(200, {"ok": True, "n": e.retry_errors()})
            if p == "/api/actions/requeue_all":
                return self._send(200, {"ok": True, "n": e.requeue_all()})
            if p == "/api/actions/verify_drive":
                e.verify_drive()
                return self._send(200, {"ok": True})
            if p == "/api/actions/reset_db":
                e.reset_db()
                return self._send(200, {"ok": True})
            if p == "/api/probe/sample":
                e.speed_sample()
                return self._send(200, {"ok": True})
            if p == "/api/probe/mark":
                e.mark_all(bool(b.get("fresh")))
                return self._send(200, {"ok": True})
            if p == "/api/probe/defer":
                return self._send(200, {"ok": True, "n": e.defer_marked()})
            if p == "/api/task/stop":
                e.stop_task()
                return self._send(200, {"ok": True})
            if p == "/api/files/action":
                e.file_action(int(b["id"]), b["action"])
                return self._send(200, {"ok": True})
            return self._send(404, {"error": "not found"})
        except Exception as ex:  # noqa
            return self._error(ex)

    def _error(self, ex):
        if not isinstance(ex, ApiError):
            self.engine.log(traceback.format_exc(), "debug")
        return self._send(400 if isinstance(ex, ApiError) else 500, {"error": str(ex)})


def _page(msg, back=False):
    link = '<p><a href="/">← Вернуться в программу</a></p>' if back else ""
    return (f'<!doctype html><meta charset="utf-8"><title>ya2gd</title>'
            f'<body style="font-family:sans-serif;padding:40px;background:#f4f5f7">'
            f'<h3>{html.escape(str(msg))}</h3>{link}</body>')


def _check_buffer(path):
    """Проверка папки буфера: можно ли писать, сколько места."""
    path = (path or "").strip().strip('"')
    if not path:
        return {"ok": True, "msg": "Буфер на диске выключен: маленькие файлы ждут в памяти, большие идут напрямую"}
    if not os.path.isabs(path):
        raise ApiError("Укажите полный путь, например D:\\YandexBuffer")
    try:
        os.makedirs(path, exist_ok=True)
        probe = os.path.join(path, ".ya2gd-probe")
        with open(probe, "wb") as f:
            f.write(b"x" * 4096)
        os.remove(probe)
        du = shutil.disk_usage(path)
    except OSError as ex:
        raise ApiError(f"В папку «{path}» нельзя писать: {ex}")
    return {"ok": True, "free": du.free, "total": du.total,
            "msg": f"Папка доступна, свободно {du.free / 2**30:.1f} ГБ из {du.total / 2**30:.1f} ГБ"}


def _quit(engine):
    engine.log("Выход по команде из окна программы: останавливаю перенос…")
    engine.stop_scanning()
    engine.stop_task()
    engine.pause_transfer("выход из программы", by_user=False)
    t0 = time.time()
    while engine.transferring() and time.time() - t0 < 60:
        time.sleep(0.5)
    engine.log("Программа закрыта. Прогресс сохранён — при следующем запуске всё продолжится.")
    winsys.allow_sleep()
    time.sleep(0.5)
    os._exit(0)


class _Server(ThreadingHTTPServer):
    # На Windows SO_REUSEADDR позволяет второй программе занять тот же порт — запрещаем
    allow_reuse_address = not winsys.WIN
    daemon_threads = True


def serve(engine, port):
    Handler.engine = engine
    return _Server(("127.0.0.1", port), Handler)
