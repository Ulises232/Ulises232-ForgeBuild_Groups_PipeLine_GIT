@echo off
cd /d %~dp0

REM Crear entorno virtual si no existe
if not exist .venv (
  echo Creando entorno virtual...
  python -m venv .venv
)

REM Activar entorno virtual
call .venv\Scripts\activate

REM Actualizar pip
echo Actualizando pip...
python -m pip install --upgrade pip

REM Crear carpeta wheels si no existe
if not exist wheels (
  echo Creando carpeta wheels...
  mkdir wheels
)

REM Paso 1: Descargar paquetes en wheels/
echo Descargando dependencias a carpeta local (wheels/)...
pip download -r requirements.txt -d wheels

REM Paso 2: Instalar desde wheels/
echo Instalando dependencias desde wheels/...
pip install --no-index --find-links=wheels -r requirements.txt

REM Paso 3: Ejecutar tu aplicación
echo Iniciando aplicación...
set "QT_LOGGING_RULES=*.debug=false;*.info=false;*.warning=true;*.critical=true"
:: Extra opcional: oculta backend QPA si molesta demasiado
:: set "QT_LOGGING_RULES=%QT_LOGGING_RULES%;qt.qpa.*=false"
echo QT_LOGGING_RULES=%QT_LOGGING_RULES%
set PYTHONUNBUFFERED=1
set HERR_REPO=C:\ruta\base\del\monorepo
set GIT_TRACE_FILE=C:\tmp\git_trace.log


python -m buildtool.app
