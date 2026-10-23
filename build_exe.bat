@echo off
chcp 65001 >nul
cd /d "%~dp0"
rem Сборка YandexToGoogle.exe. Подходит любой Python 3.10 и новее, включая 3.14.
rem Готовый exe можно и не собирать: он лежит на GitHub в Releases (тег latest).
set ERR=1
where py >nul 2>nul && (set PY=py -3) || (set PY=python)
%PY% --version >nul 2>nul || (
  echo Python не найден. Установите Python с https://www.python.org/downloads/ ^(галочка "Add python.exe to PATH"^).
  goto :end
)
%PY% -c "import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)" || (
  echo Нужен Python 3.10 или новее.
  goto :end
)
rem Окружение от другой (удалённой) версии Python пересоздаём, чтобы не мешало
if exist ".venv\Scripts\python.exe" (
  ".venv\Scripts\python.exe" -c "import sys" >nul 2>nul || rmdir /s /q .venv
)
if not exist ".venv\Scripts\python.exe" (
  echo Создаю окружение...
  %PY% -m venv .venv || goto :end
)
echo Ставлю зависимости и PyInstaller ^(последнюю версию, подходящую к вашему Python^)...
".venv\Scripts\python.exe" -m pip install -q --disable-pip-version-check --upgrade pip
".venv\Scripts\python.exe" -m pip install -q --disable-pip-version-check --upgrade -r requirements.txt "pyinstaller>=6.11" || goto :end
echo Собираю лицензии сторонних компонентов (кладутся в exe)...
".venv\Scripts\python.exe" tools\third_party_licenses.py legal-build\THIRD_PARTY_LICENSES.txt || goto :end
".venv\Scripts\python.exe" -m PyInstaller --noconfirm --onefile --console --name YandexToGoogle --add-data "ya2gd\static;ya2gd\static" --add-data "LICENSE;legal" --add-data "LICENSE.ru.txt;legal" --add-data "README.txt;legal" --add-data "legal-build\THIRD_PARTY_LICENSES.txt;legal" --collect-submodules ya2gd --hidden-import tkinter launcher.py || goto :end
set ERR=0
echo.
echo Готово: dist\YandexToGoogle.exe  ^(рядом с ним появится папка data с настройками и прогрессом^)
:end
if not "%ERR%"=="0" echo. & echo Сборка не удалась — сообщение об ошибке выше.
if /i not "%~1"=="--no-pause" pause
exit /b %ERR%
