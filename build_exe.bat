@echo off
cd /d %~dp0
call .venv\Scripts\activate
python -m pip install --upgrade pip
pip install pyinstaller
pyinstaller --noconfirm ^
  --name ForgeBuild ^
  --windowed ^
  --collect-all PySide6 ^
  --add-data "buildtool/data;buildtool/data" ^
  buildtool/app.py
echo.
echo Build terminado. Ejecutable en: dist\ForgeBuild\ForgeBuild.exe
