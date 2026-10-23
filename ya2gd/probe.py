"""Пробный замер скорости, разметка файлов и расчёт времени переноса.

Пробный замер (1–2 минуты). Берётся серия файлов разных типов и размеров: фото, видео, документы,
архивы; до 5 МБ, 5–100 МБ, больше 100 МБ. У каждого проверяется, выдаёт ли Яндекс ссылку, как быстро
отвечает, отдаёт ли данные и с какой скоростью (до 16 МБ с файла). Потом ~12 секунд идёт скачивание
во все потоки — сколько вообще дают Яндекс и ваш интернет на приём, — и ~12 секунд загрузка тестовых
данных в Google. Загрузка не завершается, поэтому на Drive ничего не появляется.

Разметка. У каждого ещё не перенесённого файла запрашиваются ссылка и первые 256 КБ, и файл помечается:
отдаётся / не отдаётся / нет на Яндексе / отдаёт «по капле». Ничего не копируется. Ссылки программа
просит не чаще ~7 в секунду (бережёт Яндекс), поэтому 23 000 файлов размечаются примерно за час.
Прерванная разметка продолжается с неразмеченных файлов.

Расчёт времени — по замеру и разметке:
- Яндекс: на каждый файл — задержка (ссылка + первый ответ) и размер / скорость одного потока;
  недоступный файл стоит потоку столько, сколько длилась неудачная попытка. Потоки работают
  параллельно, но не быстрее общей скорости скачивания и не чаще, чем Яндекс выдаёт ссылки.
- Google: объём / скорость загрузки во все потоки + задержка на каждый файл.
- Перенос идёт конвейером: общее время задаёт самая медленная сторона.
- Google принимает не больше ~750 ГБ в сутки: каждые следующие 750 ГБ — ещё сутки.
"""
import collections
import math
import os
import random
import statistics
import threading
import time

from .fetch import Overloaded, StorageDown
from .http_util import ApiError, AuthError, FatalPause, NETWORK_ERRORS, RateLimited, WebAuthError

MB = 2 ** 20
GB = 2 ** 30
SAMPLE_FILES = int(os.environ.get("YA2GD_SAMPLE_FILES", "24"))
SAMPLE_READ = 16 * MB            # пробный замер: сколько читать из файла
MARK_READ = 256 * 1024           # разметка: только начало файла
SAMPLE_BUDGET = 20.0             # не дольше стольких секунд на чтение одного файла
MARK_BUDGET = 15.0
TEST_SECONDS = float(os.environ.get("YA2GD_SPEEDTEST_SECONDS", "12"))   # замер «во все потоки»
SLOW_BPS = 32 * 1024             # медленнее — «по капле»
GOOGLE_DAY = 750 * GB            # Google принимает не больше ~750 ГБ в сутки на аккаунт
TEST_UPLOAD_SIZE = GB            # объявленный размер тестовой загрузки: она никогда не дописывается до конца
# Разметка не должна пометить всё «не отдаётся», если на время лёг весь Яндекс (как «предохранитель» переноса)
OUTAGE_STREAK = int(os.environ.get("YA2GD_MARK_STREAK", "20"))
OUTAGE_QUIET = float(os.environ.get("YA2GD_MARK_QUIET", "120"))
OUTAGE_PAUSE = float(os.environ.get("YA2GD_MARK_PAUSE", "120"))
FAIL_DEFAULT = 10.0              # сколько стоит потоку недоступный файл, если замер этого не видел

KINDS = {"photo": "Фото", "video": "Видео", "doc": "Документы", "archive": "Архивы", "other": "Прочее"}
BUCKETS = ((5 * MB, "до 5 МБ"), (100 * MB, "5–100 МБ"), (None, "больше 100 МБ"))
VIDEO_EXT = {"mp4", "mov", "3gp", "avi", "mkv", "m4v", "mts", "m2ts", "wmv", "mpg", "mpeg", "webm", "flv", "vob"}
IMAGE_EXT = {"jpg", "jpeg", "png", "heic", "heif", "webp", "gif", "bmp", "tif", "tiff", "dng", "cr2", "cr3",
             "nef", "arw", "orf", "rw2", "raf", "raw", "avif"}
DOC_EXT = {"pdf", "doc", "docx", "xls", "xlsx", "ppt", "pptx", "odt", "ods", "odp", "rtf", "txt", "csv", "djvu",
           "fb2", "epub", "md", "pages", "numbers", "key"}
ARCH_EXT = {"zip", "rar", "7z", "tar", "gz", "tgz", "bz2", "xz", "iso", "img", "dmg", "cab"}
LABELS = ("ok", "slow", "dead", "missing")
ABORT = (AuthError, WebAuthError, FatalPause)


def kind_of(row):
    """Тип файла: по расширению, а если его нет — по тому, что сообщил Яндекс."""
    name = (row.get("name") or "").lower()
    ext = name.rsplit(".", 1)[1] if "." in name else ""
    for kind, exts in (("video", VIDEO_EXT), ("photo", IMAGE_EXT), ("doc", DOC_EXT), ("archive", ARCH_EXT)):
        if ext in exts:
            return kind
    mt, mime = (row.get("media_type") or "").lower(), (row.get("mime") or "").lower()
    if mt == "video" or mime.startswith("video/"):
        return "video"
    if mt == "image" or mime.startswith("image/"):
        return "photo"
    if mt in ("document", "spreadsheet", "text", "book") or mime.startswith("text/") or mime.endswith("/pdf"):
        return "doc"
    if mt in ("compressed", "diskimage", "backup"):
        return "archive"
    return "other"


def bucket_of(size):
    for i, (limit, _) in enumerate(BUCKETS):
        if limit is None or size < limit:
            return i


def choose_sample(rows, n, rnd=random):
    """Серия для пробного замера: из каждой группы «тип × размер» по файлу, по кругу, пока не наберётся n.
    Группы с большим объёмом — первыми: они сильнее всего влияют на общее время."""
    groups = collections.defaultdict(list)
    for r in rows:
        if r["size"] > 0:
            groups[(kind_of(r), bucket_of(r["size"]))].append(r)
    order = sorted(groups, key=lambda g: -sum(x["size"] for x in groups[g]))
    for g in order:
        rnd.shuffle(groups[g])
    out = []
    while len(out) < n and any(groups[g] for g in order):
        for g in order:
            if groups[g] and len(out) < n:
                out.append(groups[g].pop())
    return out


def _median(xs):
    xs = [x for x in xs if x is not None]
    return statistics.median(xs) if xs else None


def summarize(results, speed_from=MARK_READ):
    """Сводка проб по типам и в целом. Медианы — чтобы один выброс не портил оценку.
    speed_from: скорость потока считается только по чтениям не меньше стольких байт (None — не считать:
    256 КБ разметки — слишком мало, чтобы соединение успело разогнаться)."""
    def stat(rs):
        ok = [r for r in rs if r["result"] in ("ok", "slow")]
        speeds = [r["bytes"] / r["read_s"] for r in ok
                  if speed_from is not None and r["bytes"] >= speed_from and r["read_s"]]
        return {"n": len(rs), **{lb: sum(r["result"] == lb for r in rs) for lb in LABELS},
                "overhead_s": _median([r["link_s"] + r["ttfb_s"] for r in ok]),
                "speed_bps": _median(speeds),
                "fail_s": _median([r["fail_s"] for r in rs if r["result"] in ("dead", "missing")])}
    by = collections.defaultdict(list)
    for r in results:
        by[r["kind"]].append(r)
    return {"kinds": {k: stat(v) for k, v in by.items()}, "all": stat(results)}


def merge_measures(sample, full):
    """Для расчёта: доступность, задержки и цену недоступного файла — из последнего замера; скорость одного
    потока — из пробного (у разметки чтения слишком короткие); общие скорости — из последнего, где они есть."""
    ms = [m for m in (sample, full) if m]
    if not ms:
        return None
    latest = max(ms, key=lambda m: m["at"])
    out = dict(latest)
    out["kinds"] = {k: dict(v) for k, v in (latest.get("kinds") or {}).items()}
    out["all"] = dict(latest.get("all") or {})
    if sample and latest is not sample:
        for k, v in (sample.get("kinds") or {}).items():
            if v.get("speed_bps"):
                out["kinds"].setdefault(k, {})["speed_bps"] = v["speed_bps"]
        out["all"]["speed_bps"] = (sample.get("all") or {}).get("speed_bps")
    for key in ("ya_bps", "g_bps", "g_rtt_s"):
        if out.get(key) is None:
            other = [m.get(key) for m in sorted(ms, key=lambda m: -m["at"]) if m.get(key) is not None]
            out[key] = other[0] if other else None
    return out


def estimate(rows, meas, ya_streams, g_streams, link_iv):
    """Сколько займёт перенос rows (незавершённые файлы: size, section, status, probe, kind, source).

    link_iv: {раздел: секунд между запросами ссылок} — ограничитель частоты, которым программа бережёт Яндекс.
    Файл с меткой разметки «отдаётся» считается доступным, «не отдаётся» — недоступным; отложенный — тоже
    недоступным (сам он не повторяется); остальные — с долей доступных из разметки или замера."""
    if not meas:
        return None
    kinds, allk = meas.get("kinds") or {}, meas.get("all") or {}
    K, M = max(1, ya_streams), max(1, g_streams)
    ya_bps, g_bps = meas.get("ya_bps"), meas.get("g_bps")
    g_over = 2 * meas["g_rtt_s"] if meas.get("g_rtt_s") is not None else 0.5

    def field(kind, name):
        v = (kinds.get(kind) or {}).get(name)
        return v if v is not None else allk.get(name)

    # доля доступных: по меткам разметки этого типа, если их достаточно; иначе — по замеру
    seen = collections.Counter()
    for r in rows:
        if r.get("probe") in LABELS and r["status"] != "buffered":
            seen[(r["kind"], r["probe"] in ("ok", "slow"))] += 1

    def ratio(ok, bad, n_min):
        return ok / (ok + bad) if ok + bad >= n_min else None

    prior = {}
    for k in KINDS:
        st = kinds.get(k) or {}
        p = ratio(seen[(k, True)], seen[(k, False)], 10)
        if p is None:
            p = ratio(st.get("ok", 0) + st.get("slow", 0), st.get("dead", 0) + st.get("missing", 0), 5)
        if p is None:
            p = ratio(allk.get("ok", 0) + allk.get("slow", 0), allk.get("dead", 0) + allk.get("missing", 0), 1)
        prior[k] = 1.0 if p is None else p

    fail = allk.get("fail_s") or FAIL_DEFAULT

    def part(subset):
        th = ya_b = g_b = g_n = lost_n = lost_b = dead_th = 0.0
        links = collections.Counter()
        mark_th, mark_links, unlabeled = 0.0, collections.Counter(), 0
        missing_speed = False
        for r in subset:
            size, k, st, pr = r["size"], r["kind"], r["status"], r.get("probe")
            if st == "buffered":
                p, yandex = 1.0, False          # уже скачан в буфер — осталась только загрузка в Google
            elif pr in ("ok", "slow"):
                p, yandex = 1.0, True           # в том числе отложенный, который разметка нашла доступным
            elif pr in ("dead", "missing"):
                p, yandex = 0.0, st != "deferred"
            elif st == "deferred":
                p, yandex = 0.0, False          # Яндекс не отдал, а сам такой файл не повторяется
            else:
                p, yandex = prior.get(k, 1.0), True
            g_b += p * size
            g_n += p
            lost_n += 1 - p
            lost_b += (1 - p) * size
            if st == "buffered":
                continue
            v = field(k, "speed_bps") or (ya_bps / K if ya_bps else None)
            o = field(k, "overhead_s") or 0.5
            if not pr:
                unlabeled += 1
                mark_th += p * (o + min(size, MARK_READ) / (v or 2 * MB)) + (1 - p) * fail
                mark_links[r["source"]] += 1
            if not yandex:
                continue
            if not v:
                missing_speed = True
                v = 2 * MB
            th += p * (o + size / v) + (1 - p) * fail
            dead_th += (1 - p) * fail
            ya_b += p * size
            links[r["source"]] += 1
        floor = max((n * link_iv.get(src, 0.15) for src, n in links.items()), default=0.0)
        t_files = max(th / K, floor)
        t_speed = ya_b / ya_bps if ya_bps else 0.0
        t_ya = max(t_files, t_speed)
        t_g = (g_b / g_bps + g_n * g_over / M) if g_bps else None
        t = max(t_ya, t_g or 0.0)
        t_limit = 0.0
        if g_b > GOOGLE_DAY:
            days = math.floor(g_b / GOOGLE_DAY)
            rest = g_b - days * GOOGLE_DAY
            t_limit = days * 86400 + (rest / g_bps if g_bps else 0.0)
            t = max(t, t_limit)
        parts = {"ya_speed": t_speed, "ya_files": t_files, "google": t_g or 0.0, "limit": t_limit}
        mark_floor = max((n * link_iv.get(src, 0.15) for src, n in mark_links.items()), default=0.0)
        return {"total_s": None if missing_speed and not ya_bps else t,
                "ya_s": t_ya, "ya_files_s": t_files, "ya_speed_s": t_speed, "google_s": t_g, "limit_s": t_limit,
                "dead_s": dead_th / K,                  # сколько из этого уйдёт на попытки недоступных файлов
                "bottleneck": max(parts, key=parts.get) if t else None,
                "files": len(subset), "bytes": sum(r["size"] for r in subset),
                "avail_files": round(g_n), "avail_bytes": g_b, "lost_files": round(lost_n), "lost_bytes": lost_b,
                "unlabeled": unlabeled, "mark_s": max(mark_th / K, mark_floor) if unlabeled else 0.0}

    out = part(rows)
    out["photos_s"] = part([r for r in rows if r["section"]])["total_s"]
    out["files_s"] = part([r for r in rows if not r["section"]])["total_s"]
    out["google_measured"] = g_bps is not None
    out["prior"] = prior
    return out


class Prober:
    """Пробы файлов в несколько потоков с теми же правилами бережности, что и у переноса:
    ограничитель частоты ссылок, пауза на «слишком часто» (429) и «перегружен» (503), проверка интернета,
    «предохранитель» на случай, если на время лёг весь Яндекс."""

    def __init__(self, engine, stop):
        self.e = engine
        self.stop = stop
        self.lock = threading.Lock()
        self.pause_until = 0.0
        self.offline = False
        self.streak = []
        self.excused = set()
        self.last_ok = time.time()
        self.busy = collections.Counter()
        self.results = {}
        self.counts = collections.Counter()
        self.fatal = None
        self.total = 0
        self.t0 = time.time()

    # ------------------------------------------------------------------ один файл
    def probe(self, row, read_bytes, budget):
        """Ссылка → первый ответ → до read_bytes байт. dict результата; None — «не сейчас» (пауза Яндекса,
        нет интернета): файл вернётся в очередь; {"busy": True} — Яндекс ответил «перегружен»."""
        from .engine import LinkDown, is_network
        e = self.e
        res = {"id": row["id"], "path": row["path"], "kind": kind_of(row), "size": row["size"],
               "section": row.get("section", 0), "source": row.get("source"), "bytes": 0, "link_s": None,
               "ttfb_s": None, "read_s": None, "fail_s": None, "note": "", "result": None, "outage": False}
        t0 = time.monotonic()

        def failed(result, note, outage=False):
            res.update(result=result, note=str(note)[:300], fail_s=time.monotonic() - t0, outage=outage)
            return res

        def offline():
            self.offline = True
            return None

        want = min(row["size"], read_bytes)
        try:
            url, sess, web = e._link(row, self.stop)
            t1 = time.monotonic()
            r, _ = e._open_link(row, url, sess, web, 0, length=want or None)
        except ABORT:
            raise
        except RateLimited as ex:
            e._yandex_limited(ex)
            return None
        except Overloaded:
            return {"busy": True}
        except LinkDown as ex:
            return offline() if not e._online() else failed("dead", ex, outage=True)
        except StorageDown as ex:
            if ex.network and not e._online():
                return offline()
            return failed("dead", ex, outage=ex.network and not ex.host)
        except ApiError as ex:
            if ex.status == 404:
                return failed("missing", "нет на Яндексе (удалён после получения списка?)")
            if is_network(ex) and not e._online():
                return offline()
            return failed("dead", ex)
        except NETWORK_ERRORS as ex:
            return offline() if not e._online() else failed("dead", f"сеть: {type(ex).__name__}", outage=True)
        t2 = time.monotonic()
        got, note = 0, ""
        try:
            while got < want and time.monotonic() - t2 < budget and not self.stop.is_set():
                block = r.raw.read(min(256 * 1024, want - got))
                if not block:
                    break
                got += len(block)
        except NETWORK_ERRORS as ex:
            note = f"обрыв после {got // 1024} КБ ({type(ex).__name__})"
        finally:
            r.close()
        t3 = time.monotonic()
        res.update(link_s=t1 - t0, ttfb_s=t2 - t1, bytes=got, read_s=t3 - t2)
        if want and not got:
            return failed("dead", note or "Яндекс ответил, но не прислал ни байта")
        bps = got / max(t3 - t2, 1e-3)
        if t3 - t2 >= 5 and bps < SLOW_BPS:
            res.update(result="slow", note=f"отдаёт по капле: {bps / 1024:.1f} КБ/с")
        else:
            res.update(result="ok", note=note)
        return res

    # ------------------------------------------------------------------ много файлов
    def _wait_ready(self):
        """Ждать, пока можно спрашивать Яндекс: не пауза, есть интернет. False — остановлено."""
        e = self.e
        while not self.stop.is_set() and self.fatal is None:
            now = time.time()
            if self.offline:
                if e._online():
                    self.offline = False
                    e.log("Интернет снова есть — продолжаю")
                    continue
                self.stop.wait(15)
                continue
            wait = max(self.pause_until, e.ya_cooldown_until) - now
            if wait > 0:
                self.stop.wait(min(wait, 3))
                continue
            return True
        return False

    def _outage(self, row, res):
        """«Предохранитель» разметки: подряд много отказов «первого» сервера или выдачи ссылок и давно ни одного
        удачного файла — похоже, лежит весь Яндекс. Метки этих файлов снимаются (один раз на файл), пауза."""
        now = time.time()
        with self.lock:
            if res["result"] in ("ok", "slow"):
                self.last_ok = now
                self.streak.clear()
                return []
            if not res.get("outage"):
                return []
            self.streak.append(row)
            if len(self.streak) < OUTAGE_STREAK or now - self.last_ok <= OUTAGE_QUIET:
                return []
            back = [r for r in self.streak if r["id"] not in self.excused]
            self.excused.update(r["id"] for r in back)
            self.streak.clear()
            self.pause_until = now + OUTAGE_PAUSE
            for r in back:
                old = self.results.pop(r["id"], None)
                if old:
                    self.counts[old["result"]] -= 1
        for r in back:
            self.e.db.set_probe(r["id"], None)
        self.e.log(f"Подряд {OUTAGE_STREAK} файлов не ответили, а за {OUTAGE_QUIET:.0f} с не ответил ни один — похоже, "
                   f"Яндекс сейчас не отдаёт файлы вообще. Разметка на паузе {OUTAGE_PAUSE:.0f} с; "
                   f"{len(back)} файлов проверю ещё раз", "warn")
        return back

    def run(self, rows, read_bytes, budget, streams, label=True):
        """Пробы rows в streams потоков. Результаты — self.results (id → dict); метки — сразу в базу."""
        q = collections.deque(rows)
        self.total = len(rows)
        self.t0 = time.time()

        def worker():
            while self._wait_ready():
                with self.lock:
                    if not q:
                        return
                    row = q.popleft()
                try:
                    res = self.probe(row, read_bytes, budget)
                except ABORT as ex:
                    self.fatal = ex
                    return
                except Exception as ex:  # noqa — одна странная ошибка не должна останавливать всю разметку
                    if self.stop.is_set():
                        return
                    self.e.log(f"Разметка «{row['path']}»: непредвиденная ошибка {ex!r}", "error")
                    continue
                if self.stop.is_set():
                    return          # проверку прервала кнопка «Остановить» — её результат ничего не значит
                if res is None:
                    with self.lock:
                        q.append(row)
                    continue
                if res.get("busy"):
                    self.busy[row["id"]] += 1
                    if self.busy[row["id"]] < 3:
                        with self.lock:
                            q.append(row)
                            self.pause_until = max(self.pause_until, time.time() + 30)
                        continue
                    res = dict(self.probe_stub(row), result="dead", note="Яндекс перегружен (503) — три раза подряд")
                with self.lock:
                    self.results[row["id"]] = res
                    self.counts[res["result"]] += 1
                if label:
                    self.e.db.set_probe(row["id"], res["result"],
                                        int(((res["link_s"] or 0) + (res["ttfb_s"] or 0)) * 1000) or None,
                                        res["bytes"] / res["read_s"] if res["read_s"] else None, res["note"])
                back = self._outage(row, res)
                if back:
                    with self.lock:
                        q.extend(back)
                self._progress()

        threads = [threading.Thread(target=worker, daemon=True, name=f"probe{i}") for i in range(max(1, streams))]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        if self.fatal is not None:
            raise self.fatal
        return self.results

    @staticmethod
    def probe_stub(row):
        return {"id": row["id"], "path": row["path"], "kind": kind_of(row), "size": row["size"],
                "section": row.get("section", 0), "source": row.get("source"), "bytes": 0, "link_s": None,
                "ttfb_s": None, "read_s": None, "fail_s": None, "note": "", "outage": False}

    def _progress(self):
        c, done = self.counts, len(self.results)
        el = time.time() - self.t0
        left = (self.total - done) * el / done if done else None
        msg = f"проверено {_n(done)} из {_n(self.total)}: отдаются {_n(c['ok'])}, не отдаются {_n(c['dead'])}"
        if c["slow"]:
            msg += f", «по капле» {c['slow']}"
        if c["missing"]:
            msg += f", нет на Яндексе {c['missing']}"
        if left is not None and done < self.total:
            msg += f" · осталось ≈ {_fmt_dur(left)}"
        self.e.task.update(msg=msg, progress=[done, self.total])

    # ------------------------------------------------------------------ скорость во все потоки
    def ya_throughput(self, rows, streams, seconds):
        """Скачивание доступных файлов во все потоки ~seconds секунд: сколько вместе дают Яндекс и интернет
        на приём. Данные никуда не сохраняются. (байт/с | None, пояснение)"""
        e = self.e
        cand = sorted(rows, key=lambda r: -r["size"])[:max(streams, 4)]
        if not cand or sum(r["size"] for r in cand) < 4 * MB * min(streams, len(cand)):
            return None, "нет достаточно крупных доступных файлов, чтобы замерить скачивание во все потоки"
        total, errors, lock = [0], [], threading.Lock()
        t0 = time.monotonic()
        deadline = t0 + seconds

        def worker(i):
            j, fails = i, 0
            while not self.stop.is_set() and time.monotonic() < deadline and fails < 3:
                row = cand[j % len(cand)]
                j += 1
                try:
                    url, sess, web = e._link(row, self.stop)
                    r, _ = e._open_link(row, url, sess, web, 0, length=min(row["size"], 256 * MB))
                except (ApiError, *NETWORK_ERRORS) as ex:
                    fails += 1
                    errors.append(str(ex))
                    continue
                try:
                    while time.monotonic() < deadline and not self.stop.is_set():
                        b = r.raw.read(256 * 1024)
                        if not b:
                            break
                        with lock:
                            total[0] += len(b)
                except NETWORK_ERRORS as ex:
                    fails += 1
                    errors.append(type(ex).__name__)
                finally:
                    r.close()

        threads = [threading.Thread(target=worker, args=(i,), daemon=True) for i in range(max(1, streams))]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        el = time.monotonic() - t0
        if not total[0]:
            return None, "Яндекс не отдал данные при замере во все потоки" + (f": {errors[0]}" if errors else "")
        return total[0] / max(el, 0.5), None

    def google_throughput(self, streams, seconds):
        """Загрузка тестовых данных в Google во все потоки ~seconds секунд. Загрузка не завершается — файл на Drive
        не появляется. (байт/с | None, задержка запроса, с | None, пояснение)"""
        e = self.e
        if not e.settings["google_refresh_token"]:
            return None, None, "Google не подключён — скорость загрузки не измерена"
        drive = e.drive(self.stop)
        rtts = []
        try:
            for _ in range(3):
                t = time.monotonic()
                drive.about()
                rtts.append(time.monotonic() - t)
        except (ApiError, *NETWORK_ERRORS) as ex:
            return None, None, f"Google не ответил: {ex}"
        rtt = statistics.median(rtts)
        data = os.urandom(MB)
        total, errors, ends, lock = [0], [], [], threading.Lock()
        t0 = time.monotonic()
        deadline = t0 + seconds

        def worker():
            uri = None
            try:
                uri = drive.start_upload("ya2gd-замер-скорости.tmp", "root", TEST_UPLOAD_SIZE,
                                         "application/octet-stream", {"ya2gd_speedtest": "1"})
                off = 0
                while time.monotonic() < deadline and not self.stop.is_set() and off + len(data) < TEST_UPLOAD_SIZE:
                    st, val = drive.put_chunk(uri, data, off, TEST_UPLOAD_SIZE)
                    if st != "partial":
                        errors.append(f"Google ответил неожиданно ({st})")
                        break
                    with lock:
                        total[0] += max(0, val - off)
                    off = val
            except (ApiError, *NETWORK_ERRORS) as ex:
                errors.append(str(ex))
            finally:
                ends.append(time.monotonic())
                if uri:
                    drive.cancel_upload(uri)

        threads = [threading.Thread(target=worker, daemon=True) for _ in range(max(1, streams))]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        if not total[0]:
            return None, rtt, errors[0] if errors else "Google не принял тестовые данные"
        return total[0] / max(max(ends) - t0, 0.5), rtt, None

    # ------------------------------------------------------------------ режимы
    def _candidates(self, statuses, unlabeled=False):
        ex = self.e._excluded_sources()
        sql = (f"SELECT id, path, source, name, size, mime, media_type, section, status FROM files "
               f"WHERE status IN ({','.join('?' * len(statuses))})")
        args = list(statuses)
        if ex:
            sql += f" AND source NOT IN ({','.join('?' * len(ex))})"
            args += list(ex)
        if unlabeled:
            sql += " AND probe IS NULL"
        return [dict(r) for r in self.e.db.q(sql + " ORDER BY section DESC, id", args)]

    def _measure(self, mode, results, speed_from, ya, g, started):
        s = summarize(list(results.values()), speed_from)
        st = self.e.settings
        return {"mode": mode, "at": time.time(), "took_s": time.time() - started,
                "ya_streams": st["yandex_streams_max"], "g_streams": st["google_streams"],
                "ya_bps": ya[0], "ya_note": ya[1], "g_bps": g[0], "g_rtt_s": g[1], "g_note": g[2],
                "kinds": s["kinds"], "all": s["all"], "partial": self.stop.is_set(),
                "files": [{k: r.get(k) for k in ("path", "kind", "size", "result", "link_s", "ttfb_s", "bytes",
                                                 "read_s", "fail_s", "note")}
                          for r in sorted(results.values(), key=lambda r: (r["kind"], r["size"]))]
                if mode == "sample" else []}

    def _speed_tests(self, ok_rows):
        st = self.e.settings
        if self.stop.is_set():
            return (None, "замер остановлен"), (None, None, "замер остановлен")
        self.e.task.update(msg="скачиваю с Яндекса во все потоки (данные никуда не сохраняются)…")
        ya = self.ya_throughput(ok_rows, st["yandex_streams_max"], TEST_SECONDS)
        if self.stop.is_set():
            return ya, (None, None, "замер остановлен")
        self.e.task.update(msg="загружаю тестовые данные в Google (файл на Drive не появится)…")
        g = self.google_throughput(st["google_streams"], TEST_SECONDS)
        return ya, g

    def sample(self):
        e = self.e
        started = time.time()
        rows = self._candidates(("pending", "error")) or self._candidates(("deferred",))
        pick = choose_sample(rows, SAMPLE_FILES)
        if not pick:
            raise ApiError("Нечего замерять: список файлов не получен или всё уже перенесено")
        kinds = collections.Counter(kind_of(r) for r in pick)
        e.log("Пробный замер: " + ", ".join(f"{KINDS[k].lower()} — {n}" for k, n in kinds.most_common()))
        self.run(pick, SAMPLE_READ, SAMPLE_BUDGET, e.settings["yandex_streams_max"])
        ok_rows = [r for r in pick if self.results.get(r["id"], {}).get("result") in ("ok", "slow")]
        ya, g = self._speed_tests(ok_rows)
        meas = self._measure("sample", self.results, MARK_READ, ya, g, started)
        e.save_measure("sample", meas)
        c = self.counts
        msg = f"из {len(self.results)} файлов отдаются {c['ok'] + c['slow']}, не отдаются {c['dead'] + c['missing']}"
        if ya[0]:
            msg += f"; Яндекс {_fmt_speed(ya[0])} во все потоки"
        if g[0]:
            msg += f"; Google {_fmt_speed(g[0])}"
        est = e.probe_info(force=True).get("estimate") or {}
        if est.get("total_s"):
            msg += f"; перенос всего оставшегося ≈ {_fmt_dur(est['total_s'])}"
        return msg

    def mark_all(self, fresh=False):
        e = self.e
        started = time.time()
        if fresh:
            e.db.x("UPDATE files SET probe=NULL, probe_at=NULL, probe_ms=NULL, probe_bps=NULL, probe_note=NULL "
                   "WHERE status NOT IN ('done','skipped')")
        rows = self._candidates(("pending", "error", "deferred"), unlabeled=True)
        if not rows:
            return "все файлы уже размечены (проверить заново — «Разметить заново»)"
        e.log(f"Разметка: проверяю {_n(len(rows))} файлов — ссылка и первые 256 КБ каждого, ничего не копируя")
        self.run(rows, MARK_READ, MARK_BUDGET, e.settings["yandex_streams_max"])
        by_id = {r["id"]: r for r in rows}
        ok_rows = [by_id[i] for i, r in self.results.items() if r["result"] in ("ok", "slow")]
        ya, g = self._speed_tests(ok_rows) if not self.stop.is_set() else \
            ((None, "разметка остановлена"), (None, None, "разметка остановлена"))
        if self.results:
            e.save_measure("full", self._measure("full", self.results, None, ya, g, started))
        c = self.counts
        msg = (f"{'остановлено; ' if self.stop.is_set() else ''}проверено {_n(len(self.results))}: "
               f"отдаются {_n(c['ok'])}, не отдаются {_n(c['dead'])}")
        if c["slow"]:
            msg += f", «по капле» {c['slow']}"
        if c["missing"]:
            msg += f", нет на Яндексе {c['missing']}"
        msg += f" (за {_fmt_dur(time.time() - started)})"
        return msg


def _n(x):
    """23 000 — с пробелом между разрядами, как принято по-русски."""
    return f"{x:,}".replace(",", " ")


def _fmt_dur(sec):
    sec = int(sec)
    d, h, m = sec // 86400, sec % 86400 // 3600, sec % 3600 // 60
    if d:
        return f"{d} д {h} ч"
    if h:
        return f"{h} ч {m} мин"
    return f"{max(m, 1)} мин" if sec >= 60 else f"{sec} с"


def _fmt_speed(bps):
    return f"{bps / MB:.1f} МБ/с ({bps * 8 / 1e6:.0f} Мбит/с)"
