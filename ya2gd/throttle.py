"""Бережная нагрузка на Яндекс: число одновременных скачиваний подбирается само.

Правило «прибавь по одному, при жалобе — уполовинь» (как TCP):
- раз в GROW_EVERY секунд без единой жалобы, если все потоки заняты и работа есть — +1 поток;
- Яндекс сказал «слишком часто» (429, капча) — число потоков делится пополам;
- три ответа «перегружен» (503) за минуту — минус один поток.
Недоступный файл (сервер хранения не отвечает, 504, обрыв) жалобой НЕ считается: это беда одного файла,
а не перегрузка Яндекса. Иначе тысячи мёртвых файлов держат программу на одном потоке.
"""
import collections
import os
import threading
import time

GROW_EVERY = float(os.environ.get("YA2GD_GROW_EVERY", "30"))
START = int(os.environ.get("YA2GD_GATE_START", "4"))
OVERLOAD_WINDOW = 60.0
OVERLOADS_TO_SHRINK = 3


class AdaptiveGate:
    def __init__(self, maximum, start=START, adaptive=True, log=None):
        self.max = max(1, int(maximum))
        self.adaptive = adaptive
        self.start = start
        self.limit = self.max if not adaptive else max(1, min(start, self.max))
        self.active = 0
        self.cond = threading.Condition()
        self.overloads = collections.deque()
        self.last_change = time.time()
        self.log = log or (lambda *a, **k: None)
        self.reason = "старт"

    # ------------------------------------------------------------ слоты
    def acquire(self, stop):
        with self.cond:
            while self.active >= self.limit:
                if stop.is_set():
                    return False
                self.cond.wait(1.0)
            if stop.is_set():
                return False
            self.active += 1
            return True

    def release(self):
        with self.cond:
            self.active = max(0, self.active - 1)
            self.cond.notify_all()

    # ------------------------------------------------------------ сигналы
    def configure(self, maximum, adaptive):
        """При каждом «Старте»: потолок из настроек, начинаем снова со START (не с 1 после прошлых жалоб)."""
        with self.cond:
            self.max = max(1, int(maximum))
            self.adaptive = adaptive
            self.limit = max(1, min(self.start, self.max)) if adaptive else self.max
            self.overloads.clear()
            self.last_change = time.time()
            self.reason = "старт"
            self.cond.notify_all()

    def throttled(self):
        with self.cond:
            old = self.limit
            self.limit = max(1, self.limit // 2)
            self.last_change = time.time()
            self.overloads.clear()
            self.reason = "Яндекс попросил притормозить (429)"
        if self.limit != old:
            self.log(f"Яндекс просит притормозить: потоков к Яндексу {old} → {self.limit}", "warn")

    def overload(self):
        """Яндекс ответил «перегружен» (503)."""
        now = time.time()
        with self.cond:
            self.overloads.append(now)
            while self.overloads and self.overloads[0] < now - OVERLOAD_WINDOW:
                self.overloads.popleft()
            if len(self.overloads) < OVERLOADS_TO_SHRINK or self.limit <= 1:
                return
            old = self.limit
            self.limit -= 1
            self.overloads.clear()
            self.last_change = now
            self.reason = "Яндекс отвечает «перегружен» (503)"
        self.log(f"Яндекс перегружен: потоков к Яндексу {old} → {self.limit}", "warn")

    def tick(self, has_backlog):
        """Вызывается раз в несколько секунд: решает, можно ли добавить поток."""
        now = time.time()
        with self.cond:
            if not self.adaptive or self.limit >= self.max or not has_backlog:
                return
            if now - self.last_change < GROW_EVERY or self.active < self.limit:
                return
            if self.overloads and self.overloads[-1] > now - GROW_EVERY:
                return
            old = self.limit
            self.limit += 1
            self.last_change = now
            self.reason = "Яндекс отвечает стабильно"
            self.cond.notify_all()
        self.log(f"Яндекс отвечает стабильно: потоков к Яндексу {old} → {self.limit}")

    def state(self):
        with self.cond:
            return {"limit": self.limit, "max": self.max, "active": self.active,
                    "adaptive": self.adaptive, "reason": self.reason}
