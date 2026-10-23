"""Собирает THIRD_PARTY_LICENSES.txt — лицензии всех сторонних компонентов, которые входят в YandexToGoogle.exe.

Тексты берутся из установленных пакетов (то есть ровно тех версий, из которых собирается exe), а лицензия
Python — из его дистрибутива. Сборка (build_exe.bat и GitHub Actions) запускает скрипт перед PyInstaller
и вкладывает файл в exe.

Запуск: python tools/third_party_licenses.py [путь к файлу]  (по умолчанию — THIRD_PARTY_LICENSES.txt в корне)
"""
import importlib.metadata as md
import os
import platform
import sys
import sysconfig

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Что входит в exe, кроме самой программы. Лицензия и адрес — на случай, если пакет не установлен
# (например, PyInstaller при запуске программы из исходников).
BUNDLED = [
    ("requests", "HTTP-запросы к Яндексу и Google", "Apache-2.0", "https://github.com/psf/requests"),
    ("urllib3", "HTTP-соединения (используется requests)", "MIT", "https://github.com/urllib3/urllib3"),
    ("certifi", "корневые сертификаты для HTTPS (используется requests)", "MPL-2.0",
     "https://github.com/certifi/python-certifi"),
    ("charset-normalizer", "определение кодировки текста (используется requests)", "MIT",
     "https://github.com/jawah/charset_normalizer"),
    ("idna", "международные доменные имена (используется requests)", "BSD-3-Clause", "https://github.com/kjd/idna"),
    ("pyinstaller", "сборка exe; в exe входит только загрузчик PyInstaller", "GPL-2.0-or-later с исключением "
     "для загрузчика (bootloader exception): exe можно распространять на любых условиях",
     "https://github.com/pyinstaller/pyinstaller"),
]
LICENSE_WORDS = ("LICENSE", "LICENCE", "COPYING", "NOTICE")
RULE = "=" * 78


def _license_of(dist, fallback):
    meta = dist.metadata
    lic = meta.get("License-Expression") or ""
    if not lic:
        short = (meta.get("License") or "").strip()
        lic = short if short and "\n" not in short and len(short) < 80 else ""
    if not lic:
        lic = "; ".join(c.split("::")[-1].strip() for c in meta.get_all("Classifier") or []
                        if c.startswith("License ::"))
    return lic or fallback


def _license_texts(dist):
    out = []
    for f in dist.files or []:
        name = os.path.basename(str(f)).upper()
        if any(w in name for w in LICENSE_WORDS):
            try:
                out.append((str(f), f.read_text(encoding="utf-8")))
            except (OSError, UnicodeDecodeError):
                pass
    return out


def _python_license():
    """LICENSE.txt дистрибутива Python. В сборке для Windows там же лицензии вложенных библиотек
    (OpenSSL, SQLite, Tcl/Tk, libffi, zlib, bzip2, xz, expat и др.)."""
    ver = f"{sys.version_info.major}.{sys.version_info.minor}"
    for p in (os.path.join(sys.base_prefix, "LICENSE.txt"), os.path.join(sys.base_prefix, "LICENSE"),
              os.path.join(sysconfig.get_paths()["stdlib"], "LICENSE.txt"), f"/usr/lib/python{ver}/LICENSE.txt"):
        if os.path.isfile(p):
            with open(p, encoding="utf-8", errors="replace") as f:
                return f.read()
    return None


def build():
    head = [
        "СТОРОННИЕ КОМПОНЕНТЫ В YandexToGoogle.exe",
        "",
        "Сама программа распространяется по лицензии MIT (файлы LICENSE и LICENSE.ru.txt).",
        "В exe вместе с ней упакованы компоненты других авторов. Каждый из них распространяется",
        "на условиях своей лицензии; их тексты приведены ниже без изменений.",
        "Исходный код всех компонентов открыт и доступен по указанным адресам; компоненты в exe не изменены.",
        "",
        f"Список собран из версий, установленных при сборке (Python {platform.python_version()}, "
        f"{platform.system() or 'ОС не определена'}).",
        "",
    ]
    summary, bodies = [], []
    ver = platform.python_version()
    summary.append(f"  Python {ver} — интерпретатор и стандартная библиотека — PSF-2.0 — https://www.python.org/")
    for name, what, lic, url in BUNDLED:
        try:
            d = md.distribution(name)
        except md.PackageNotFoundError:
            summary.append(f"  {name} — {what} — {lic} — {url}")
            bodies.append((f"{name}", [(None, f"Пакет не был установлен при составлении списка. Лицензия: {lic}.\n"
                                              f"Полный текст: {url}\n")]))
            continue
        shown = lic if name == "pyinstaller" else _license_of(d, lic)   # у PyInstaller важно исключение
        summary.append(f"  {name} {d.version} — {what} — {shown} — {url}")
        texts = _license_texts(d) or [(None, f"Текст лицензии в пакете не найден. Лицензия: {lic}. См. {url}\n")]
        bodies.append((f"{name} {d.version}", texts))
    py = _python_license()
    out = head + ["КРАТКО", ""] + summary + [""]
    out += [RULE, f"Python {ver}", "(в сборке для Windows — вместе с лицензиями вложенных в Python библиотек)", RULE, ""]
    out.append(py if py else "Текст лицензии Python не найден. См. https://docs.python.org/3/license.html\n")
    if py and platform.system() != "Windows":
        out.append("\nВ сборке для Windows в Python входят также OpenSSL, SQLite, Tcl/Tk, libffi, zlib, bzip2, xz,\n"
                   "expat и другие библиотеки; их лицензии — в файле LICENSE.txt дистрибутива Python для Windows\n"
                   "и на https://docs.python.org/3/license.html. В exe, собранный на Windows, этот текст вложен целиком.\n")
    for title, texts in bodies:
        out += ["", RULE, title, RULE]
        for path, text in texts:
            if path:
                out += [f"--- {path} ---"]
            out += [text.rstrip(), ""]
    return "\n".join(out).rstrip() + "\n"


if __name__ == "__main__":
    dst = sys.argv[1] if len(sys.argv) > 1 else os.path.join(ROOT, "THIRD_PARTY_LICENSES.txt")
    if os.path.dirname(dst):
        os.makedirs(os.path.dirname(dst), exist_ok=True)
    with open(dst, "w", encoding="utf-8", newline="\r\n") as f:
        f.write(build())
    print(f"Записано: {dst}")
