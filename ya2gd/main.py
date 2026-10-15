"""Точка входа: поднимает веб-интерфейс на 127.0.0.1 и открывает браузер."""
import argparse
import threading
import webbrowser

from .engine import Engine
from .web import serve


def main():
    ap = argparse.ArgumentParser(description="Перенос Яндекс Диска на Google Drive")
    ap.add_argument("--port", type=int, default=None)
    ap.add_argument("--no-browser", action="store_true")
    args = ap.parse_args()

    engine = Engine()
    port = args.port or engine.settings["port"]
    try:
        httpd = serve(engine, port)
    except OSError:
        # Порт занят — скорее всего, программа уже запущена. Просто откроем её окно.
        print(f"Порт {port} занят — похоже, программа уже запущена. Открываю http://127.0.0.1:{port}/")
        if not args.no_browser:
            webbrowser.open(f"http://127.0.0.1:{port}/")
        return
    url = f"http://127.0.0.1:{port}/"
    engine.log(f"Интерфейс: {url} (не закрывайте это окно, пока идёт перенос)")

    if engine.settings["autostart"]:
        if engine.db.get("scan.state") in ("interrupted", "stopped") and engine.settings["yandex_token"]:
            engine.log("Автозапуск: продолжаю прерванное сканирование")
            engine.start_scan()
        if engine.db.get("transfer.wanted") == "1":
            engine.log("Автозапуск: продолжаю перенос")
            try:
                engine.start_transfer()
            except Exception as e:  # noqa
                engine.log(f"Автозапуск не удался: {e}", "error")

    if not args.no_browser:
        threading.Timer(1.0, lambda: webbrowser.open(url)).start()
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        engine.log("Остановка по Ctrl+C")
        engine.stop_scanning()
        engine.pause_transfer("закрытие программы", by_user=False)


if __name__ == "__main__":
    main()
