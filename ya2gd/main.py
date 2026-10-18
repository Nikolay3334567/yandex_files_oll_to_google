"""Точка входа: поднимает интерфейс на 127.0.0.1 и открывает окно программы."""
import argparse
import os
import threading
import time

from . import __version__, winsys
from .core import data_dir
from .engine import Engine
from .web import serve

BANNER = """
  Яндекс Диск → Google Drive  v{v}
  ------------------------------------------------------------
  Это окно — сама программа. НЕ закрывайте его, пока идёт перенос.
  Окно с интерфейсом можно закрывать и открывать снова: {url}
  Если окно закрыли или выключили свет — запустите программу снова,
  перенос продолжится с того же места.
"""


def main():
    ap = argparse.ArgumentParser(description="Перенос Яндекс Диска на Google Drive")
    ap.add_argument("--port", type=int, default=None)
    ap.add_argument("--no-browser", action="store_true")
    args = ap.parse_args()

    winsys.prepare_console()
    ddir = data_dir()
    lock = winsys.InstanceLock(ddir)
    port_file = os.path.join(ddir, "instance.port")
    if not lock.acquire():
        try:
            port = int(open(port_file).read().strip())
        except (OSError, ValueError):
            port = args.port or 8765
        url = f"http://127.0.0.1:{port}/"
        print(f"Программа уже запущена. Открываю её окно: {url}")
        if not args.no_browser:
            winsys.open_ui(url)
        time.sleep(2)
        return

    engine = Engine(ddir)
    port = args.port or engine.settings["port"]
    try:
        httpd = serve(engine, port)
    except OSError:
        httpd = serve(engine, 0)            # порт занят чужой программой — берём любой свободный
        engine.log(f"Порт {port} занят другой программой, интерфейс на порту {httpd.server_address[1]}", "warn")
    port = httpd.server_address[1]
    with open(port_file, "w") as f:
        f.write(str(port))
    url = f"http://127.0.0.1:{port}/"
    print(BANNER.format(v=__version__, url=url), flush=True)
    engine.log(f"Интерфейс: {url}. Данные и журнал: {ddir}")
    winsys.set_title(f"Яндекс → Google — НЕ закрывайте это окно ({url})")

    if engine.settings["autostart"]:
        if engine.db.get("scan.state") in ("interrupted", "stopped") and engine.settings["yandex_token"]:
            engine.log("Автозапуск: продолжаю прерванное получение списка")
            engine.start_scan()
        if engine.db.get("transfer.wanted") == "1":
            engine.log("Автозапуск: продолжаю перенос")
            try:
                engine.start_transfer()
            except Exception as e:  # noqa
                engine.log(f"Автозапуск не удался: {e}", "error")

    if not args.no_browser:
        def show():
            if winsys.open_ui(url, engine.settings["app_window"]):
                time.sleep(3)
                winsys.minimize_console()
        threading.Timer(1.0, show).start()
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        engine.log("Остановка по Ctrl+C")
        engine.stop_scanning()
        engine.pause_transfer("закрытие программы", by_user=False)
        winsys.allow_sleep()


if __name__ == "__main__":
    main()
