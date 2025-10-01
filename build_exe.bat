@echo off
setlocal enabledelayedexpansion
cd /d %~dp0

rem =============================
rem  Manejo de argumentos
rem =============================
set "SKIP_DEPS=0"
set "FORCE_DEPS=0"
set "DEPLOY_PATH="

:parse_args
if "%~1"=="" goto after_parse

if /I "%~1"=="--skip-deps" (
  set "SKIP_DEPS=1"
) else if /I "%~1"=="--force-deps" (
  set "FORCE_DEPS=1"
  set "SKIP_DEPS=0"
) else if /I "%~1"=="--deploy" (
  if "%~2"=="" (
    echo Debe especificar una ruta despues de --deploy
    exit /b 1
  )
  set "DEPLOY_PATH=%~f2"
  shift
) else (
  echo Opcion desconocida: %~1
  echo Opciones disponibles: --skip-deps --force-deps --deploy ^<ruta^>
  exit /b 1
)
shift
goto parse_args

:after_parse

set "VENV_DIR=.venv"
if not exist "%VENV_DIR%" (
  echo Creando entorno virtual...
  python -m venv "%VENV_DIR%"
  if errorlevel 1 (
    echo No fue posible crear el entorno virtual.
    exit /b 1
  )
)

call "%VENV_DIR%\Scripts\activate"

set "REQ_CACHE=%VENV_DIR%\.requirements.cached.txt"

if "%FORCE_DEPS%"=="1" (
  set "SKIP_DEPS=0"
)

if "%SKIP_DEPS%"=="1" (
  where pyinstaller >nul 2>&1
  if errorlevel 1 (
    echo PyInstaller no esta instalado en el entorno. Se instalaran las dependencias.
    set "SKIP_DEPS=0"
  )
)

if "%SKIP_DEPS%"=="1" (
  if exist "%REQ_CACHE%" (
    fc /b requirements.txt "%REQ_CACHE%" >nul 2>&1
    if errorlevel 1 (
      echo Cambios detectados en requirements.txt. Se reinstalaran dependencias.
      set "SKIP_DEPS=0"
    ) else (
      echo Requisitos sin cambios. Reutilizando dependencias existentes.
    )
  ) else (
    set "SKIP_DEPS=0"
  )
)

if not "%SKIP_DEPS%"=="1" (
  echo Actualizando pip...
  python -m pip install --upgrade pip
  if errorlevel 1 goto deps_error

  echo Instalando dependencias del proyecto...
  pip install --no-warn-script-location -r requirements.txt
  if errorlevel 1 goto deps_error

  echo Instalando PyInstaller...
  pip install --no-warn-script-location pyinstaller
  if errorlevel 1 goto deps_error

  copy /y requirements.txt "%REQ_CACHE%" >nul
) else (
  echo Dependencias existentes listas.
)

echo Preparando icono...
python -m buildtool.icon_factory
if errorlevel 1 (
  echo No se pudo generar el icono requerido para el ejecutable.
  exit /b 1
)

echo Construyendo ejecutable...
pyinstaller --noconfirm ^
  --name ForgeBuild ^
  --windowed ^
  --icon "assets\\forgebuild.ico" ^
  --collect-all PySide6 ^
  --add-data "VERSION;." ^
  --add-data "buildtool\\ui\\icons;buildtool\\ui\\icons" ^
  --add-data "buildtool\\ui\\theme.qss;buildtool\\ui" ^
  --add-data "buildtool\\ui\\theme_light.qss;buildtool\\ui" ^
  --add-data "buildtool/data;buildtool/data" ^
  buildtool/app.py

if errorlevel 1 (
  echo.
  echo Hubo un error al generar el ejecutable.
  exit /b 1
)

echo.
echo Build terminado. Ejecutable en: dist\ForgeBuild\ForgeBuild.exe

if not "%DEPLOY_PATH%"=="" (
  if not exist "%DEPLOY_PATH%" (
    echo La ruta de despliegue "%DEPLOY_PATH%" no existe.
    exit /b 1
  )

  echo Copiando ejecutable a "%DEPLOY_PATH%"...
  copy /y "dist\ForgeBuild\ForgeBuild.exe" "%DEPLOY_PATH%" >nul
  if errorlevel 1 (
    echo No se pudo copiar el ejecutable a la ruta indicada.
    exit /b 1
  )
  echo Copia completada.

  echo Copiando Version a "%DEPLOY_PATH%/_internal"...
  copy /y "VERSION" "%DEPLOY_PATH%/_internal" >nul
  if errorlevel 1 (
    echo No se pudo copiar el ejecutable a la ruta indicada.
    exit /b 1
  )
  echo Copia completada.
)

exit /b 0

:deps_error
echo.
echo Hubo un error instalando o actualizando las dependencias.
exit /b 1
