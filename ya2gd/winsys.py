"""Всё, что зависит от Windows: окно программы, сон компьютера, консоль, диски, автозапуск.

На других системах функции ничего не ломают — просто делают меньше.
"""
import os
import shutil
import subprocess
import sys
import threading
import webbrowser

WIN = sys.platform == "win32"
if WIN:
    import ctypes
    from ctypes import wintypes

ES_CONTINUOUS = 0x80000000
ES_SYSTEM_REQUIRED = 0x00000001
DRIVE_TYPES = {2: "съёмный", 3: "локальный", 4: "сетевой", 5: "CD/DVD", 6: "RAM-диск"}
APP_NAME = "YandexToGoogle"


def keep_awake():
    """Не давать Windows уснуть, пока идёт перенос (сбрасывает таймер сна; экран может гаснуть)."""
    if WIN:
        try:
            ctypes.windll.kernel32.SetThreadExecutionState(ES_CONTINUOUS | ES_SYSTEM_REQUIRED)
        except Exception:  # noqa
            pass


def allow_sleep():
    if WIN:
        try:
            ctypes.windll.kernel32.SetThreadExecutionState(ES_CONTINUOUS)
        except Exception:  # noqa
            pass


_last_title = None


def set_title(text):
    """Прогресс в заголовке окна консоли — его видно на панели задач."""
    global _last_title
    if not WIN or text == _last_title:
        return
    _last_title = text
    try:
        ctypes.windll.kernel32.SetConsoleTitleW(text)
    except Exception:  # noqa
        pass


def prepare_console():
    """Отключает «быстрое выделение»: иначе щелчок мышью в чёрном окне замораживает программу."""
    if not WIN:
        return
    try:
        k = ctypes.windll.kernel32
        h = k.GetStdHandle(-10)                 # STD_INPUT_HANDLE
        mode = wintypes.DWORD()
        if k.GetConsoleMode(h, ctypes.byref(mode)):
            k.SetConsoleMode(h, (mode.value & ~0x0040) | 0x0080)   # -QUICK_EDIT, +EXTENDED_FLAGS
        k.SetConsoleOutputCP(65001)
    except Exception:  # noqa
        pass
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:  # noqa
        pass


def minimize_console():
    if WIN:
        try:
            hwnd = ctypes.windll.kernel32.GetConsoleWindow()
            if hwnd:
                ctypes.windll.user32.ShowWindow(hwnd, 6)       # SW_MINIMIZE
        except Exception:  # noqa
            pass


def _browser_exe():
    cands = []
    for env in ("PROGRAMFILES(X86)", "PROGRAMFILES", "LOCALAPPDATA"):
        base = os.environ.get(env)
        if base:
            cands += [os.path.join(base, "Microsoft", "Edge", "Application", "msedge.exe"),
                      os.path.join(base, "Google", "Chrome", "Application", "chrome.exe"),
                      os.path.join(base, "Yandex", "YandexBrowser", "Application", "browser.exe")]
    for c in cands:
        if os.path.exists(c):
            return c
    return None


def open_ui(url, app_window=True):
    """Открывает интерфейс отдельным окном без адресной строки (Edge/Chrome --app); иначе — в браузере."""
    if WIN and app_window:
        exe = _browser_exe()
        if exe:
            try:
                subprocess.Popen([exe, f"--app={url}", "--window-size=1280,900"],
                                 creationflags=getattr(subprocess, "DETACHED_PROCESS", 0))
                return True
            except OSError:
                pass
    try:
        return webbrowser.open(url)
    except Exception:  # noqa
        return False


def list_drives():
    """Диски компьютера со свободным местом — чтобы выбрать, где держать буфер."""
    out = []
    if WIN:
        try:
            mask = ctypes.windll.kernel32.GetLogicalDrives()
        except Exception:  # noqa
            mask = 0
        for i in range(26):
            if not mask & (1 << i):
                continue
            root = f"{chr(65 + i)}:\\"
            try:
                kind = ctypes.windll.kernel32.GetDriveTypeW(root)
                if kind not in (2, 3, 6):
                    continue
                du = shutil.disk_usage(root)
            except OSError:
                continue
            label = ctypes.create_unicode_buffer(261)
            try:
                ctypes.windll.kernel32.GetVolumeInformationW(root, label, 261, None, None, None, None, 0)
            except Exception:  # noqa
                pass
            out.append({"root": root, "label": label.value, "type": DRIVE_TYPES.get(kind, ""),
                        "total": du.total, "free": du.free,
                        "system": os.environ.get("SYSTEMDRIVE", "C:").upper() == root[:2].upper()})
    else:
        seen = set()
        for root in ("/", os.path.expanduser("~")):
            try:
                du = shutil.disk_usage(root)
            except OSError:
                continue
            if (du.total, du.free) in seen:
                continue
            seen.add((du.total, du.free))
            out.append({"root": root, "label": "", "type": "локальный", "total": du.total, "free": du.free,
                        "system": root == "/"})
    return out


def pick_folder(initial=""):
    """Стандартный диалог Windows «Выбор папки». Возвращает путь или '' (отмена)."""
    result = {"path": ""}

    def run():
        try:
            import tkinter
            from tkinter import filedialog
            root = tkinter.Tk()
            root.withdraw()
            root.attributes("-topmost", True)
            result["path"] = filedialog.askdirectory(parent=root, initialdir=initial or None,
                                                     title="Папка для буфера (лучше на быстром диске с местом)",
                                                     mustexist=False) or ""
            root.destroy()
        except Exception as e:  # noqa
            result["error"] = str(e)
    t = threading.Thread(target=run, daemon=True)
    t.start()
    t.join(600)
    if result.get("error") and WIN:
        ps = ("Add-Type -AssemblyName System.Windows.Forms;"
              "$f=New-Object System.Windows.Forms.FolderBrowserDialog;"
              "$f.Description='Папка для буфера';"
              "if($f.ShowDialog() -eq 'OK'){[Console]::OutputEncoding=[Text.Encoding]::UTF8;$f.SelectedPath}")
        try:
            out = subprocess.run(["powershell", "-NoProfile", "-STA", "-Command", ps], capture_output=True,
                                 timeout=600, creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
            return out.stdout.decode("utf-8", "replace").strip()
        except Exception:  # noqa
            return ""
    if result.get("error"):
        raise OSError(f"Диалог выбора папки недоступен: {result['error']}. Впишите путь вручную.")
    return os.path.normpath(result["path"]) if result["path"] else ""


def _startup_file():
    appdata = os.environ.get("APPDATA")
    if not appdata:
        return None
    return os.path.join(appdata, "Microsoft", "Windows", "Start Menu", "Programs", "Startup", f"{APP_NAME}.cmd")


def set_autorun(enabled):
    """Запуск программы при входе в Windows (после перезагрузки или отключения света)."""
    path = _startup_file()
    if not WIN or not path:
        return False
    if not enabled:
        try:
            os.remove(path)
        except OSError:
            pass
        return False
    if getattr(sys, "frozen", False):
        cmd = f'start "" /min "{sys.executable}"'
        cwd = os.path.dirname(sys.executable)
    else:
        cwd = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        cmd = f'start "" /min "{sys.executable}" -m ya2gd'
    # Первые две строки — ASCII; после «chcp 65001» cmd.exe читает остальное как UTF-8 (пути с кириллицей)
    with open(path, "w", encoding="utf-8", newline="\r\n") as f:
        f.write(f'@echo off\nchcp 65001 >nul\ncd /d "{cwd}"\n{cmd}\n')
    return True


def autorun_enabled():
    path = _startup_file()
    return bool(WIN and path and os.path.exists(path))


class InstanceLock:
    """Не даёт запустить две копии программы на одну папку data (иначе будут дубли на Drive)."""

    def __init__(self, ddir):
        self.path = os.path.join(ddir, ".lock")
        self.f = None

    def acquire(self):
        self.f = open(self.path, "a+")
        try:
            if WIN:
                import msvcrt
                self.f.seek(0)
                msvcrt.locking(self.f.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except OSError:
            self.f.close()
            self.f = None
            return False
