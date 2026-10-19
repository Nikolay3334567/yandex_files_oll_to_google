"""Быстрые проверки отдельных частей без сети. Запуск: python tests/test_unit.py"""
import json
import os
import shutil
import sys
import tempfile
import threading
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")     # консоль Windows в CI — cp1252
except AttributeError:
    pass
sys.path.insert(0, ROOT)

from ya2gd import throttle  # noqa: E402
from ya2gd.core import Settings  # noqa: E402
from ya2gd.db import DB  # noqa: E402
from ya2gd.engine import MemQueue, drive_dir_for, is_media, name_date  # noqa: E402
from ya2gd.yaweb import from_mpfs, parse_cookies, to_mpfs  # noqa: E402


def test_gate():
    throttle.GROW_EVERY = 0.05
    g = throttle.AdaptiveGate(6, start=3)
    stop = threading.Event()
    assert g.state()["limit"] == 3
    for _ in range(3):
        assert g.acquire(stop)
    g.tick(has_backlog=True)            # рано: с последнего изменения прошло мало
    assert g.state()["limit"] == 3
    time.sleep(0.06)
    g.tick(has_backlog=False)           # работы нет — не растём
    assert g.state()["limit"] == 3
    g.tick(has_backlog=True)
    assert g.state()["limit"] == 4, g.state()
    g.throttled()
    assert g.state()["limit"] == 2
    for _ in range(3):
        g.error()
    assert g.state()["limit"] == 1
    for _ in range(3):
        g.release()
    # при лимите 1 второй поток ждёт, пока первый не освободит место
    assert g.acquire(stop)
    got = []
    t = threading.Thread(target=lambda: got.append(g.acquire(stop)))
    t.start()
    time.sleep(0.2)
    assert not got
    g.release()
    t.join(3)
    assert got == [True]
    g.configure(5, adaptive=False)
    assert g.state()["limit"] == 5
    print("OK автоподбор потоков к Яндексу: растёт по одному, при 429 — вдвое меньше, при сбоях — минус один")


def test_settings_migration():
    d = tempfile.mkdtemp()
    try:
        p = os.path.join(d, "settings.json")
        with open(p, "w", encoding="utf-8") as f:
            json.dump({"drive_root": "Yandex Disk", "photounlim_folder": "Фото (безлимит)",
                       "photounlim_split": "year", "workers": 8, "yandex_token": "t"}, f)
        s = Settings(p)
        assert s.migrated
        assert s["files_root"] == "Yandex Disk" and s["photos_root"] == "Yandex Disk/Фото (безлимит)"
        assert s["photos_split"] == "year" and s["yandex_streams_max"] == 8 and s["yandex_token"] == "t"
        assert s["photos_from_disk"] == "none"     # «Фотокамера» у старых установок остаётся в «Файлах»
        s.update(photos_root=" Архив \\ Фото/ ", buffer_gb="25", yandex_streams_max=99)
        assert s["photos_root"] == "Архив/Фото" and s["buffer_gb"] == 25 and s["yandex_streams_max"] == 16
        s2 = Settings(p)
        assert not s2.migrated and s2["photos_root"] == "Архив/Фото"
        print("OK настройки прежней версии переносятся, папки на Drive остаются прежними")
    finally:
        shutil.rmtree(d, ignore_errors=True)


def test_layout():
    s = {"photos_root": "Фото", "files_root": "Файлы с Яндекс Диска", "photos_split": "month",
         "photos_from_disk": "camera"}
    row = {"path": "photounlim:/2019-05-04 10-00-00.JPG", "source": "photounlim", "name": "2019-05-04 10-00-00.JPG"}
    assert drive_dir_for(row, s) == "Фото/2019/2019-05"
    row = {"path": "photounlim:/x.jpg", "source": "photounlim", "name": "x.jpg", "taken": "2021-03-01T10:00:00+00:00"}
    assert drive_dir_for(row, s) == "Фото/2021/2021-03"
    row = {"path": "photounlim:/x.jpg", "source": "photounlim", "name": "x.jpg"}
    assert drive_dir_for(row, s) == "Фото"
    assert drive_dir_for(row, dict(s, photos_split="none")) == "Фото"
    row = {"path": "disk:/Документы/2020/отчёт.pdf", "source": "disk", "name": "отчёт.pdf"}
    assert drive_dir_for(row, s) == "Файлы с Яндекс Диска/Документы/2020"
    row = {"path": "disk:/корень.txt", "source": "disk", "name": "корень.txt"}
    assert drive_dir_for(row, s) == "Файлы с Яндекс Диска"
    # автозагрузка без безлимита (папка «Фотокамера») — тоже в «Фото» по дате; документы оттуда — в «Файлы»
    cam = {"path": "disk:/Фотокамера/2020-01-02 10-00-00.MOV", "source": "disk", "name": "2020-01-02 10-00-00.MOV"}
    assert drive_dir_for(cam, s) == "Фото/2020/2020-01"
    cam_sub = {"path": "disk:/Фотокамера/Screenshots/a.png", "source": "disk", "name": "a.png",
               "created": "2021-07-01T00:00:00+00:00"}
    assert drive_dir_for(cam_sub, s) == "Фото/2021/2021-07/Screenshots"
    doc = {"path": "disk:/Фотокамера/notes.txt", "source": "disk", "name": "notes.txt", "mime": "text/plain"}
    assert drive_dir_for(doc, s) == "Файлы с Яндекс Диска/Фотокамера"
    other = {"path": "disk:/Маша камера/IMG_20190504_1.jpg", "source": "disk", "name": "IMG_20190504_1.jpg"}
    assert drive_dir_for(other, s) == "Файлы с Яндекс Диска/Маша камера"
    assert drive_dir_for(other, dict(s, photos_from_disk="all")) == "Фото/2019/2019-05"
    assert drive_dir_for(cam, dict(s, photos_from_disk="none")) == "Файлы с Яндекс Диска/Фотокамера"
    assert name_date("IMG_20190504_101010.jpg") == "2019-05-04"
    assert is_media({"name": "VID.MOV"}) and not is_media({"name": "a.pdf", "mime": "application/pdf"})
    assert to_mpfs("disk:/A/b.txt") == "/disk/A/b.txt" and from_mpfs("/disk/A/b.txt") == "disk:/A/b.txt"
    assert to_mpfs("photounlim:/a.jpg") == "/photounlim/a.jpg" and from_mpfs("/photounlim/a.jpg") == "photounlim:/a.jpg"
    assert parse_cookies("Cookie: a=1; b=2") == {"a": "1", "b": "2"}
    print("OK раскладка: «Фото/год/год-месяц» и «Файлы с Яндекс Диска/<те же папки>»")


def test_routing_sql_matches_python():
    import sqlite3
    from ya2gd.core import DEFAULTS
    from ya2gd.engine import goes_to_photos, photos_sql
    rows = [("photounlim", "photounlim:/a.jpg", "a.jpg", None, None),
            ("disk", "disk:/Фотокамера/IMG_1.JPG", "IMG_1.JPG", None, "application/octet-stream"),
            ("disk", "disk:/Фотокамера/x.pdf", "x.pdf", None, "application/pdf"),
            ("disk", "disk:/Видео проекты/clip.MOV", "clip.MOV", "video", "video/quicktime"),
            ("disk", "disk:/Документы/a.docx", "a.docx", "document", None),
            ("disk", "disk:/Camera Uploads/b.heic", "b.heic", None, None),
            ("disk", "disk:/camera uploads/c.jpg", "c.jpg", None, None),
            ("disk", "disk:/Фотокамера.jpg", "Фотокамера.jpg", "image", None)]
    c = sqlite3.connect(":memory:")
    c.execute("CREATE TABLE files(source, path, name, media_type, mime)")
    c.executemany("INSERT INTO files VALUES(?,?,?,?,?)", rows)
    for mode in ("camera", "all", "none"):
        in_sql = [bool(x[0]) for x in c.execute(f"SELECT {photos_sql(mode)} FROM files")]
        in_py = [goes_to_photos(dict(zip(("source", "path", "name", "media_type", "mime"), r)),
                                dict(DEFAULTS, photos_from_disk=mode)) for r in rows]
        assert in_sql == in_py, (mode, in_sql, in_py)
    c.close()
    print("OK подсчёт прогресса по папкам совпадает с тем, куда реально кладутся файлы")


def test_disk_warnings():
    from ya2gd.engine import Engine
    d = tempfile.mkdtemp()
    try:
        e = Engine(d)
        gb = 2 ** 30
        e.db.set("disk.info", json.dumps({"total": 10 * gb, "used": 12 * gb}))
        w = e.disk_warnings()
        assert w and w[0][0] == "err" and "44 дня" in w[0][1], w
        e.db.set("disk.info", json.dumps({"total": 10 * gb, "used": 9.8 * gb, "will_be_overdrawn": True,
                                          "unlim_end": "2026-11-01T00:00:00+00:00"}))
        w = e.disk_warnings()
        assert [lv for lv, _ in w] == ["warn", "err"] and "2026-11-01" in w[1][1], w
        e.db.set("disk.info", json.dumps({"total": 10 * gb, "used": 1 * gb}))
        assert e.disk_warnings() == []
        e.db.close()
        print("OK предупреждения: Диск переполнен (44/90 дней), почти полон, конец бесплатного безлимита")
    finally:
        shutil.rmtree(d, ignore_errors=True)


def test_memqueue():
    q = MemQueue(100)
    stop = threading.Event()
    assert q.put("a", 60, stop) and q.put("b", 40, stop)
    t = threading.Thread(target=lambda: q.put("c", 50, stop))
    t.start()
    time.sleep(0.2)
    assert len(q) == 2                   # «c» ждёт, пока освободится место
    assert q.get() == "a"
    t.join(2)
    assert len(q) == 2 and q.get() == "b" and q.get() == "c" and q.get() is None
    assert q.put("big", 500, stop)       # больше лимита, но очередь пуста — берём
    print("OK очередь в памяти ограничена по объёму")


def test_db_claims():
    d = tempfile.mkdtemp()
    try:
        db = DB(os.path.join(d, "s.sqlite3"))
        items = [{"path": f"disk:/f{i}", "source": "disk", "name": f"f{i}", "size": sz, "md5": None}
                 for i, sz in enumerate([500, 10, 3000])]
        items.append({"path": "photounlim:/p.jpg", "source": "photounlim", "name": "p.jpg", "size": 50})
        db.upsert_files(items, 1)
        r = db.claim_download(order="photos_first")
        assert r["path"] == "photounlim:/p.jpg"
        r = db.claim_download(order="small_first")
        assert r["size"] == 10
        # буфер: свободно 600, размер буфера 1000 → 3000 > буфера идёт напрямую, 500 влезает
        r = db.claim_download(order="as_listed", fits=(600, 1000))
        assert r["size"] == 500
        r = db.claim_download(order="as_listed", fits=(0, 1000))
        assert r["size"] == 3000
        assert db.claim_download() is None
        assert db.reset_active() == 4 and db.count_status("pending") == 4
        db.close()                       # Windows не даёт удалить открытый файл базы
        print("OK очередь: сначала «Фото», маленькие/по порядку, учёт места в буфере")
    finally:
        shutil.rmtree(d, ignore_errors=True)


if __name__ == "__main__":
    test_gate()
    test_settings_migration()
    test_layout()
    test_routing_sql_matches_python()
    test_disk_warnings()
    test_memqueue()
    test_db_claims()
    print("\nМОДУЛЬНЫЕ ПРОВЕРКИ ПРОЙДЕНЫ")
