@echo off
chcp 65001 >nul
cd /d "%~dp0"
where py >nul 2>nul && (set PY=py -3) || (set PY=python)
if not exist ".venv\Scripts\python.exe" %PY% -m venv .venv
".venv\Scripts\python.exe" -m pip install -q --disable-pip-version-check -r requirements.txt pyinstaller==6.11.1 || (pause & exit /b 1)
".venv\Scripts\pyinstaller.exe" --noconfirm --onefile --console --name YandexToGoogle --add-data "ya2gd\static;ya2gd\static" --collect-submodules ya2gd --hidden-import tkinter launcher.py || (pause & exit /b 1)
echo.
echo Готово: dist\YandexToGoogle.exe  (рядом с ним появится папка data с настройками и прогрессом)
pause
