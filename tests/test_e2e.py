"""Сквозная проверка на имитации API Яндекса (REST и веб-версии) и Google Drive.

Сценарии:
1. Без буфера на диске: обрывы, 503, программа убита посреди большого файла → докачка;
   сверка после удаления на Drive; повторный полный прогон без дублей; 429 от Яндекса.
2. Раздел «Фото» через веб-версию по кукам: устаревшие куки, «молчащий» сервер хранения,
   превью вместо недоступного оригинала (и его удаление, когда оригинал перенесён),
   потерянные ответы Google без дублей, сверка MD5 и отчёт.
3. Буфер на диске + REST API отвечает 410 на «Файлы» (как с октября 2026): список и файлы
   через веб-версию; программа убита посреди скачивания в буфер → докачка с места обрыва;
   файл больше буфера — напрямую; Яндекс отвечает 504; буфер пустеет в конце.

Запуск: python tests/test_e2e.py
"""
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "tests"))
from mock_apis import env_for  # noqa: E402

PHOTOS, FILES, PREVIEWS = "Фото", "Файлы с Яндекс Диска", "Превью — оригинал пока недоступен"


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def http(method, url, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json", "X-Ya2gd": "1"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, json.loads(r.read() or b"null")
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"null")


def wait_for(fn, timeout=120, what=""):
    t = time.time()
    while time.time() - t < timeout:
        v = fn()
        if v:
            return v
        time.sleep(0.3)
    raise AssertionError(f"timeout: {what}")


class App:
    def __init__(self, port, ddir, env):
        self.port, self.ddir, self.env = port, ddir, env
        self.p = None

    def start(self):
        self.p = subprocess.Popen([sys.executable, "-m", "ya2gd", "--port", str(self.port), "--no-browser"],
                                  cwd=ROOT, env=self.env, stdout=open(os.path.join(self.ddir, "out.txt"), "a"),
                                  stderr=subprocess.STDOUT)
        wait_for(lambda: self.ok(), 30, "app start")

    def ok(self):
        try:
            return http("GET", self.u("/api/status"))[0] == 200
        except OSError:
            return False

    def u(self, p):
        return f"http://127.0.0.1:{self.port}{p}"

    def get(self, p):
        return http("GET", self.u(p))[1]

    def post(self, p, b=None):
        return http("POST", self.u(p), b or {})

    def state(self):
        return self.get("/api/status")["transfer"]["state"]

    def log_has(self, text):
        return any(text in x["msg"] for x in self.get("/api/log"))

    def logfile_has(self, text):
        """Полный журнал на диске (переживает перезапуски программы)."""
        with open(os.path.join(self.ddir, "ya2gd.log"), encoding="utf-8") as f:
            return text in f.read()

    def kill(self):
        self.p.kill()             # как выключение света: без какого-либо завершения
        self.p.wait()

    def stop(self):
        self.p.terminate()
        self.p.wait()


class Env:
    """Имитация API + программа во временной папке."""

    def __init__(self, name, **extra_env):
        self.mport, self.aport = free_port(), free_port()
        self.ddir = tempfile.mkdtemp(prefix=f"ya2gd-{name}-")
        self.mock = subprocess.Popen([sys.executable, os.path.join(ROOT, "tests", "mock_apis.py"), str(self.mport)],
                                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.M = f"http://127.0.0.1:{self.mport}"
        wait_for(lambda: self._mock_ok(), 15, "mock start")
        env = dict(os.environ, YA2GD_DATA=self.ddir, PYTHONUNBUFFERED="1", **env_for(self.mport), **extra_env)
        for k in ("HTTPS_PROXY", "HTTP_PROXY", "https_proxy", "http_proxy", "ALL_PROXY", "all_proxy"):
            env.pop(k, None)
        env["NO_PROXY"] = "127.0.0.1,localhost"
        self.app = App(self.aport, self.ddir, env)

    def _mock_ok(self):
        try:
            return http("GET", self.M + "/ctl/stats")[0] == 200
        except OSError:
            return False

    def faults(self, **kw):
        http("POST", self.M + "/ctl/faults", kw)

    def stats(self):
        return http("GET", self.M + "/ctl/stats")[1]

    def drive(self):
        return http("GET", self.M + "/ctl/drive")[1]

    def close(self, failed):
        if failed:
            print("---- журнал программы ----")
            try:
                print(open(os.path.join(self.ddir, "out.txt"), encoding="utf-8", errors="replace").read()[-8000:])
            except OSError:
                pass
        try:
            self.app.stop()
        except Exception:  # noqa
            pass
        self.mock.terminate()
        shutil.rmtree(self.ddir, ignore_errors=True)


def run(name, fn, **extra_env):
    e = Env(name, **extra_env)
    failed = True
    try:
        fn(e)
        failed = False
    finally:
        e.close(failed)


GOOD = {"yandex_token": "good", "google_client_id": "cid", "google_client_secret": "sec",
        "google_refresh_token": "rt-good", "chunk_mb": 1}
COOKIES = json.dumps([{"name": "Session_id", "value": "good-session", "domain": ".yandex.ru"},
                      {"name": "sessionid2", "value": "x"}, {"name": "yandexuid", "value": "1"}])


# ------------------------------------------------------------------------------------------- сценарий 1
def scenario_direct(e):
    app, M = e.app, e.M
    app.start()
    app.post("/api/settings", {"yandex_token": "bad"})
    code, r = app.post("/api/yandex/test")
    assert code == 400 and "токен" in r["error"], r
    print("OK неверный токен Яндекса:", r["error"])

    app.post("/api/settings", dict(GOOD, yandex_streams_max=3, google_streams=2))
    code, r = app.post("/api/yandex/test")
    assert code == 200 and r["user"] == "Тестер" and "ссылки на скачивание выдаются" in r["files_api"], r
    print("OK проверка Яндекса:", r["files_api"])
    code, r = app.post("/api/google/test")
    assert code == 200 and r["email"] == "me@example.com", r
    print("OK проверка Google:", r["email"])

    e.faults(ya_drop=0.15, g_500=0.1, slow=0.01, ya_429=2)
    assert app.post("/api/scan/start", {"fresh": True})[0] == 200
    assert app.post("/api/transfer/start")[0] == 200

    def partial():
        items = app.get("/api/files?q=VID_0003")["items"]
        return items and 0 < items[0]["bytes_done"] < items[0]["size"] and items[0]
    it = wait_for(partial, 120, "partial upload")
    app.kill()
    print(f"OK процесс убит посреди файла, передано в Google {it['bytes_done']} из {it['size']}")

    app.start()
    assert app.log_has("вернулись в очередь")
    e.faults(slow=0)
    app.post("/api/scan/start", {"fresh": False})
    assert app.post("/api/transfer/start")[0] == 200
    time.sleep(1)
    app.post("/api/transfer/pause")
    wait_for(lambda: app.state() in ("paused", "done"), 60, "pause")
    print("OK пауза:", app.state())
    assert app.post("/api/transfer/start")[0] == 200
    wait_for(lambda: app.state() == "done", 300, "transfer done")
    assert app.log_has("Докачка «VID_0003.mp4»"), "не было докачки"
    print("OK после перезапуска большой файл докачан с места, подтверждённого Google")
    assert app.logfile_has("Яндекс просит притормозить") and app.logfile_has("Скачивание с Яндекса — пауза"), \
        "на 429 не притормозили"
    print("OK на «слишком часто» (429) Яндекс получил паузу, загрузка в Google не останавливалась")
    check(app, M)

    drive = e.drive()
    victim = next(fid for fid, f in drive.items() if f["name"] == "IMG_0001.jpg")
    http("POST", M + "/ctl/trash", {"id": victim})
    app.post("/api/actions/verify_drive")
    wait_for(lambda: app.get("/api/status")["task"]["state"] == "done", 60, "verify")
    msg = app.get("/api/status")["task"]["msg"]
    total = len(http("GET", M + "/ctl/ya")[1])
    assert "нет на Drive — 1" in msg and f"совпало по размеру и MD5 — {total - 1}" in msg, msg
    app.post("/api/transfer/start")
    wait_for(lambda: app.state() == "done", 120, "transfer 2")
    check(app, M)
    print("OK удалённый на Drive файл найден сверкой и залит снова:", msg)

    links = e.stats()["ya_download_links"]
    app.post("/api/actions/requeue_all")
    app.post("/api/transfer/start")
    wait_for(lambda: app.state() == "done", 120, "transfer 3")
    check(app, M)
    notes = app.get("/api/files?status=done&limit=500")["items"]
    assert all(n["note"] == "уже был на Drive" for n in notes), notes[:3]
    assert e.stats()["ya_download_links"] == links, "повторный прогон снова качал с Яндекса"
    print("OK повторный полный прогон: дублей нет, с Яндекса ничего не качалось повторно")


# ------------------------------------------------------------------------------------------- сценарий 2
def scenario_web(e):
    app, M = e.app, e.M
    app.start()
    dead = "disk:/Фотокамера/IMG_0002.jpg"
    e.faults(pu_hidden=True, dead=[dead], g_lost=0.3, g_500=0.05)
    app.post("/api/settings", dict(GOOD, yandex_streams_max=4))
    code, r = app.post("/api/yandex/test")
    assert code == 200 and not r["photounlim_rest"] and r["photounlim_size"] > 0, r
    print("OK REST API раздел «Фото» не отдаёт; объём виден:", r["photounlim_size"])
    att = app.get("/api/status")["attention"]
    assert any("куки" in a["msg"] for a in att), att
    print("OK в блоке «Требует внимания»: нужны куки для раздела «Фото»")

    app.post("/api/settings", {"yandex_cookies": "Session_id=old; sessionid2=x; yandexuid=1"})
    code, r = app.post("/api/yandex/web_test")
    assert code == 400 and "Куки устарели" in r["error"], r
    print("OK устаревшие куки — понятная ошибка:", r["error"][:60])
    app.post("/api/settings", {"yandex_cookies": COOKIES})
    code, r = app.post("/api/yandex/web_test")
    assert code == 200 and "ссылка на оригинал выдаётся" in r["msg"], r
    print("OK куки (экспорт Cookie-Editor) работают:", r["msg"])

    app.post("/api/scan/start", {"fresh": True})
    wait_for(lambda: app.get("/api/status")["scan"]["state"] == "done", 60, "scan")
    app.post("/api/transfer/start")

    def previewed():
        it = app.get("/api/files?q=IMG_0002")["items"][0]
        return it["status"] == "pending" and it["preview_id"] and it
    it = wait_for(previewed, 90, "preview for dead file")
    assert "ждёт сервер" in (it["last_error"] or "") or it["defers"] >= 1, it
    st = app.get("/api/status")
    assert "localhost" in st["dead_hosts"], st["dead_hosts"]
    drive = e.drive()
    prev = drive[it["preview_id"]]
    assert prev["name"] == "IMG_0002.jpg (превью XXXL).jpg" and not prev["trashed"], prev
    print("OK «молчащий» сервер распознан, файл отложен без траты попыток, сохранено превью:", prev["name"])

    e.faults(dead=[])
    wait_for(lambda: app.state() == "done", 180, "web transfer")
    it = app.get("/api/files?q=IMG_0002")["items"][0]
    assert it["status"] == "done" and it["attempts"] == 0 and not it["preview_id"], it
    assert it_preview_gone(e, prev["name"]), "превью не удалено после переноса оригинала"
    print("OK сервер ожил — оригинал перенесён сам, временное превью удалено")
    check(app, M)
    calls = e.stats()
    assert calls["mpfs_url"] >= 3 and calls["small_uploads"] > 0, calls
    print(f"OK раздел «Фото» перенесён через веб-версию; потерянные ответы Google не дали дублей "
          f"(загрузок одним запросом: {calls['small_uploads']})")

    st = app.get("/api/status")["stats"]
    assert st["verified"] == st["by_status"]["done"]["count"], st
    app.post("/api/actions/verify_drive")
    wait_for(lambda: app.get("/api/status")["task"]["state"] == "done", 60, "verify")
    msg = app.get("/api/status")["task"]["msg"]
    assert "не совпало — 0" in msg and "нет на Drive — 0" in msg, msg
    with urllib.request.urlopen(app.u("/api/report.csv")) as resp:
        csv_text = resp.read().decode("utf-8-sig")
    assert csv_text.count("\n") == st["by_status"]["done"]["count"] + 1, csv_text[:300]
    print("OK сверка с Drive по размеру и MD5:", msg)


def it_preview_gone(e, name):
    return not any(f["name"] == name for f in e.drive().values())


# ------------------------------------------------------------------------------------------- сценарий 3
def scenario_buffer(e):
    app, M = e.app, e.M
    buf = os.path.join(e.ddir, "D-drive", "Буфер")
    app.start()
    e.faults(rest_410=True, pu_hidden=True, slow=0.02, e504=["disk:/Много/Папка0/file0.bin"])
    code, r = app.post("/api/buffer/check", {"path": buf})
    assert code == 200 and "свободно" in r["msg"], r
    # единица буфера в тесте — 1 МБ: буфер 3 «ГБ» = 3 МБ, видео 5.5 МБ в него не влезает
    app.post("/api/settings", dict(GOOD, yandex_cookies=COOKIES, buffer_dir=buf, buffer_gb=3,
                                   buffer_min_free_gb=0, yandex_streams_max=2, google_streams=2,
                                   order="files_first"))
    code, r = app.post("/api/yandex/test")
    assert code == 200 and "410" in r["files_api"] and "куки" in r["files_api"], r
    print("OK API отвечает 410 на «Файлы» — программа объясняет, что возьмёт их через веб-версию")

    app.post("/api/scan/start", {"fresh": True})
    wait_for(lambda: app.get("/api/status")["scan"]["state"] == "done", 60, "scan")
    total = app.get("/api/status")["stats"]
    n = sum(v["count"] for v in total["by_status"].values())
    assert n == len(http("GET", M + "/ctl/ya")[1]), total
    assert app.log_has("беру его через веб-версию Диска")
    print(f"OK список «Файлов» получен через веб-версию: {n} файлов")

    app.post("/api/transfer/start")

    def partial_part():
        for f in os.listdir(os.path.join(buf, "ya2gd-buffer")) if os.path.isdir(os.path.join(buf, "ya2gd-buffer")) else []:
            p = os.path.join(buf, "ya2gd-buffer", f)
            if f.endswith(".part") and 300_000 < os.path.getsize(p):
                return p
    part = wait_for(partial_part, 120, "partial buffer download")
    size_before = os.path.getsize(part)
    app.kill()
    print(f"OK процесс убит посреди скачивания в буфер ({size_before} байт в {os.path.basename(part)})")

    e.faults(slow=0.002)
    time.sleep(1)                         # оборванные потоки убитой программы у имитации закрываются
    http("POST", M + "/ctl/faults", {"reset_parallel": True})
    ranges = e.stats()["ranges"]
    app.start()
    app.post("/api/transfer/start")
    wait_for(lambda: app.log_has("Докачка в буфер"), 60, "buffer resume")
    assert e.stats()["ranges"] > ranges, "докачка без Range-запроса"
    print("OK после перезапуска файл докачивается в буфер с места обрыва (Range), MD5 сверяется до Google")

    def waits_504():
        it = app.get("/api/files?q=file0.bin")["items"][0]
        return it["status"] == "pending" and it["defers"] >= 1 and it
    wait_for(waits_504, 60, "504 deferral")
    print("OK на 504 от Яндекса файл отложен без траты попыток")
    e.faults(e504=[])
    http("POST", app.u("/api/actions/retry_errors"), {})
    wait_for(lambda: app.state() == "done", 240, "buffer transfer")
    check(app, M)
    leftovers = os.listdir(os.path.join(buf, "ya2gd-buffer"))
    assert leftovers == [], leftovers
    st = app.get("/api/status")
    assert st["buffer"]["enabled"] and st["buffer"]["written"] == 0, st["buffer"]
    assert e.stats()["max_parallel_dl"] <= 2, e.stats()
    assert app.get("/api/files?q=VID_0003")["items"][0]["status"] == "done"
    print("OK всё перенесено через буфер, видео больше буфера — напрямую; буфер в конце пуст; "
          f"к Яндексу не больше {e.stats()['max_parallel_dl']} потоков одновременно")


def check(app, M):
    """Каждый файл Яндекса лежит на Drive ровно один раз, в правильной папке, с правильным MD5."""
    drive = http("GET", M + "/ctl/drive")[1]
    ya = http("GET", M + "/ctl/ya")[1]

    def full(fid):
        parts = []
        while fid in drive:
            f = drive[fid]
            parts.append(f["name"])
            fid = f["parents"][0]
        return "/".join(reversed(parts))
    live = {}
    for fid, f in drive.items():
        if f["folder"] or f["trashed"]:
            continue
        p = full(fid)
        if p.startswith(PREVIEWS + "/"):
            continue
        assert p not in live, f"дубль: {p}"
        live[p] = f["md5"]
    expected = {}
    for path, h in ya.items():
        if path.startswith("disk:/"):
            expected[f"{FILES}/" + path[6:]] = h
        else:
            name = path.split("/")[-1]     # дата берётся из имени файла: «2019-05-04 10-00-00.JPG»
            expected[f"{PHOTOS}/{name[:4]}/{name[:7]}/{name}"] = h
    assert live == expected, (sorted(set(live) ^ set(expected)),
                              [k for k in live if k in expected and live[k] != expected[k]])
    st = app.get("/api/status")["stats"]["by_status"]
    assert set(st) <= {"done"}, st


if __name__ == "__main__":
    only = [a for a in sys.argv[1:] if not a.startswith("-")]
    scenarios = [("direct", scenario_direct, {}), ("web", scenario_web, {"YA2GD_PREVIEW_AFTER": "1"}),
                 ("buffer", scenario_buffer, {"YA2GD_BUFFER_UNIT": str(2 ** 20)})]
    for name, fn, extra in scenarios:
        if only and name not in only:
            continue
        print(f"\n=== Сценарий «{name}» ===")
        run(name, fn, **extra)
    print("\nВСЕ ПРОВЕРКИ ПРОЙДЕНЫ")
