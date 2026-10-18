@echo off
chcp 65001 >nul
cd /d "%~dp0"
title Yandex Disk to Google Drive

where py >nul 2>nul && (set PY=py -3) || (set PY=python)
%PY% --version >nul 2>nul || (
  echo Python не найден. Установите Python 3.10+ с https://www.python.org/downloads/ ^(галочка "Add python.exe to PATH"^).
  pause
  exit /b 1
)

if not exist ".venv\Scripts\python.exe" (
  echo Первый запуск: создаю окружение...
  %PY% -m venv .venv || (pause & exit /b 1)
)
".venv\Scripts\python.exe" -m pip install -q --disable-pip-version-check -r requirements.txt || (pause & exit /b 1)

echo Запуск. Интерфейс откроется отдельным окном. НЕ закрывайте ЭТО окно, пока идёт перенос.
".venv\Scripts\python.exe" -m ya2gd %*
pause
