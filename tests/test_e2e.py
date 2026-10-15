"""Сквозная проверка на имитации API: сбои сети, убийство процесса посреди файла, докачка,
сверка после удаления на Drive, повторный запуск без дублей.

Запуск: python tests/test_e2e.py
"""
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "tests"))
from mock_apis import env_for  # noqa: E402


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
        wait_for(lambda: self.ok(), 20, "app start")

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

    def kill(self):
        self.p.send_signal(signal.SIGKILL)
        self.p.wait()

    def stop(self):
        self.p.terminate()
        self.p.wait()


def main():
    mport, aport = free_port(), free_port()
    ddir = tempfile.mkdtemp(prefix="ya2gd-test-")
    mock = subprocess.Popen([sys.executable, os.path.join(ROOT, "tests", "mock_apis.py"), str(mport)],
                            stdout=subprocess.DEVNULL)
    M = f"http://127.0.0.1:{mport}"
    time.sleep(1)
    env = dict(os.environ, YA2GD_DATA=ddir, PYTHONUNBUFFERED="1", **env_for(mport))
    env.pop("HTTPS_PROXY", None), env.pop("HTTP_PROXY", None), env.pop("https_proxy", None), env.pop("http_proxy", None)
    env["NO_PROXY"] = "127.0.0.1,localhost"
    app = App(aport, ddir, env)
    try:
        app.start()
        # --- неверный токен даёт понятную ошибку
        app.post("/api/settings", {"yandex_token": "bad"})
        code, r = app.post("/api/yandex/test")
        assert code == 400 and "токен" in r["error"], r
        print("OK неверный токен Яндекса:", r["error"])

        app.post("/api/settings", {"yandex_token": "good", "google_client_id": "cid",
                                   "google_client_secret": "sec", "google_refresh_token": "rt-good",
                                   "chunk_mb": 1, "workers": 3})
        code, r = app.post("/api/yandex/test")
        assert code == 200 and r["user"] == "Тестер", r
        print("OK проверка Яндекса:", r)
        code, r = app.post("/api/google/test")
        assert code == 200 and r["email"] == "me@example.com", r
        print("OK проверка Google:", r)

        # --- сбои: обрывы скачивания, 503 при загрузке, медленная сеть
        http("POST", M + "/ctl/faults", {"ya_drop": 0.15, "g_500": 0.1, "slow": 0.01})
        assert app.post("/api/scan/start", {"fresh": True})[0] == 200
        assert app.post("/api/transfer/start")[0] == 200

        # ждём, когда большой файл будет загружен частично, и убиваем процесс
        def partial():
            items = app.get("/api/files?q=VID_0003")["items"]
            return items and 0 < items[0]["bytes_done"] < items[0]["size"] and items[0]
        it = wait_for(partial, 120, "partial upload")
        app.kill()
        print(f"OK процесс убит посреди файла, передано {it['bytes_done']} из {it['size']}")

        app.start()
        st = app.get("/api/status")
        log = app.get("/api/log")
        assert any("вернулись в очередь" in x["msg"] for x in log), log
        http("POST", M + "/ctl/faults", {"slow": 0})
        app.post("/api/scan/start", {"fresh": False})   # продолжить скан, если не успел
        assert app.post("/api/transfer/start")[0] == 200
        time.sleep(1)
        app.post("/api/transfer/pause")
        wait_for(lambda: app.get("/api/status")["transfer"]["state"] in ("paused", "done"), 60, "pause")
        print("OK пауза:", app.get("/api/status")["transfer"]["state"])
        assert app.post("/api/transfer/start")[0] == 200
        wait_for(lambda: app.get("/api/status")["transfer"]["state"] == "done", 300, "transfer done")
        log = app.get("/api/log")
        assert any("Докачка «VID_0003.mp4»" in x["msg"] for x in log), "не было докачки"
        print("OK после перезапуска большой файл докачан с места обрыва")
        check(app, M)

        # --- удаляем файл на Drive (в корзину), сверка, повторная загрузка
        drive = http("GET", M + "/ctl/drive")[1]
        victim = next(fid for fid, f in drive.items() if f["name"] == "IMG_0001.jpg")
        http("POST", M + "/ctl/trash", {"id": victim})
        app.post("/api/actions/verify_drive")
        wait_for(lambda: app.get("/api/status")["task"]["state"] == "done", 60, "verify")
        msg = app.get("/api/status")["task"]["msg"]
        assert "возвращено в очередь 1" in msg, msg
        app.post("/api/transfer/start")
        wait_for(lambda: app.get("/api/status")["transfer"]["state"] == "done", 120, "transfer 2")
        check(app, M)
        print("OK удалённый на Drive файл найден сверкой и залит снова:", msg)

        # --- «начать заново»: всё уже есть на Drive → ничего не дублируется
        app.post("/api/actions/requeue_all")
        app.post("/api/transfer/start")
        wait_for(lambda: app.get("/api/status")["transfer"]["state"] == "done", 120, "transfer 3")
        check(app, M)
        notes = app.get("/api/files?status=done&limit=500")["items"]
        assert all(n["note"] == "уже был на Drive" for n in notes), notes[:3]
        print("OK повторный полный прогон: дублей нет, всё распознано как уже загруженное")
        local_folder_test(app, M, ddir)
        print("\nВСЕ ПРОВЕРКИ ПРОЙДЕНЫ")
    except Exception:
        print("---- журнал программы ----")
        print(open(os.path.join(ddir, "out.txt")).read()[-6000:])
        raise
    finally:
        try:
            app.stop()
        except Exception:  # noqa
            pass
        mock.terminate()
        shutil.rmtree(ddir, ignore_errors=True)


def local_folder_test(app, M, ddir):
    """Раздел «Фото» не отдаётся API → берём файлы из папки, куда их скачала программа Яндекса."""
    http("POST", M + "/ctl/faults", {"pu_hidden": True, "ya_drop": 0, "g_500": 0.1})
    code, r = app.post("/api/yandex/test")
    assert code == 200 and r["photounlim_method"] is None and "НЕ отдаёт" in r["photounlim"], r
    print("OK раздел «Фото» скрыт от API — программа честно об этом говорит:", r["photounlim"][:70])

    src = os.path.join(ddir, "Яндекс копии", "Фотокамера")
    os.makedirs(os.path.join(src, "sub"))
    files = {"2020-07-15 12-00-00.JPG": os.urandom(300_000), "sub/2021-01-02 08-30-00.MP4": os.urandom(2_500_000),
             "IMG_без_даты.jpg": os.urandom(1000), "качается.jpg.tmp": b"x"}
    old = time.time() - 3600
    for rel, data in files.items():
        fp = os.path.join(src, rel)
        with open(fp, "wb") as f:
            f.write(data)
        os.utime(fp, (old, old))
    young = os.path.join(src, "2022-02-02 02-02-02.jpg")   # ещё «скачивается»
    with open(young, "wb") as f:
        f.write(b"young")
    app.post("/api/settings", {"local_dirs": os.path.dirname(src) + "\n", "local_delete_after": True,
                               "local_watch": False})
    code, r = app.post("/api/local/check")
    assert r["dirs"][0]["ok"] and r["dirs"][0]["files"] == 5, r
    app.post("/api/scan/local")
    wait_for(lambda: not app.get("/api/status")["scan"]["running"], 30, "local scan")
    app.post("/api/transfer/start")
    wait_for(lambda: app.get("/api/status")["transfer"]["state"] == "done", 120, "local transfer")
    drive = http("GET", M + "/ctl/drive")[1]

    def full(fid):
        parts = []
        while fid in drive:
            parts.append(drive[fid]["name"])
            fid = drive[fid]["parents"][0]
        return "/".join(reversed(parts))
    import hashlib
    got = {full(fid): f["md5"] for fid, f in drive.items() if not f["folder"] and not f["trashed"]}
    base = "Yandex Disk/Фото (безлимит)/"
    exp = {base + "2020/2020-07/2020-07-15 12-00-00.JPG": hashlib.md5(files["2020-07-15 12-00-00.JPG"]).hexdigest(),
           base + "2021/2021-01/2021-01-02 08-30-00.MP4": hashlib.md5(files["sub/2021-01-02 08-30-00.MP4"]).hexdigest(),
           base + "Без даты/IMG_без_даты.jpg": hashlib.md5(files["IMG_без_даты.jpg"]).hexdigest()}
    for k, v in exp.items():
        assert got.get(k) == v, (k, sorted(x for x in got if "безлимит" in x))
    assert not any("tmp" in k or "2022-02-02" in k for k in got)
    left = sorted(os.path.relpath(os.path.join(d, f), src) for d, _, fs in os.walk(src) for f in fs)
    assert left == ["2022-02-02 02-02-02.jpg", "качается.jpg.tmp"], left
    print("OK папка на компьютере: файлы разложены по году/месяцу, MD5 совпал, локальные копии удалены; "
          "недокачанные и .tmp не тронуты")


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
        assert p not in live, f"дубль: {p}: " + json.dumps(
            {k: v for k, v in drive.items() if v["name"] == f["name"]}, ensure_ascii=False)
        live[p] = f["md5"]
    expected = {}
    for path, h in ya.items():
        if path.startswith("disk:/"):
            expected["Yandex Disk/" + path[6:]] = h
        else:
            name = path.split("/")[-1]     # дата берётся из имени файла: «2019-05-04 10-00-00.JPG»
            expected[f"Yandex Disk/Фото (безлимит)/{name[:4]}/{name[:7]}/{name}"] = h
    assert live == expected, (sorted(set(live) ^ set(expected)),
                              [k for k in live if k in expected and live[k] != expected[k]])
    st = app.get("/api/status")["stats"]["by_status"]
    assert set(st) <= {"done"}, st


if __name__ == "__main__":
    main()
