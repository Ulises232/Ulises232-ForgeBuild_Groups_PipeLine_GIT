@echo off
setlocal
cd /d %~dp0

if not exist .venv (
  echo Creando entorno virtual...
  python -m venv .venv
)

call .venv\Scripts\activate

echo Actualizando pip...
python -m pip install --upgrade pip

echo Instalando dependencias del proyecto...
pip install --no-warn-script-location -r requirements.txt

echo Instalando PyInstaller...
pip install --no-warn-script-location pyinstaller

echo Construyendo ejecutable...
pyinstaller --noconfirm ^
  --name ForgeBuild ^
  --windowed ^
  --collect-all PySide6 ^
  --add-data "buildtool/data;buildtool/data" ^
  buildtool/app.py

if errorlevel 1 (
  echo.
  echo Hubo un error al generar el ejecutable.
  exit /b 1
)

echo.
echo Build terminado. Ejecutable en: dist\ForgeBuild\ForgeBuild.exe
