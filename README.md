# ForgeBuild (Grupos) – Manual de configuración y uso

## 1. Panorama general
ForgeBuild es una herramienta de escritorio (PySide6/Qt) pensada para automatizar tareas diarias en los monorepos de los equipos:

- **Compilación controlada de proyectos y módulos Maven** por grupo y perfil, con reglas para módulos opcionales, ejecuciones únicas y patrones de artefactos.
- **Copia y publicación de artefactos** hacia carpetas de QA, producción, hotfix y otros destinos definidos por perfil.
- **Operaciones coordinadas de Git** (cambio de ramas, creación, push, eliminación y ramas de versión) sobre todos los repositorios vinculados a un proyecto.
- **Asistente visual de configuración** para administrar grupos, proyectos, módulos, perfiles, rutas de NAS y variables de entorno.
- **Importación masiva de tarjetas** desde archivos CSV/Excel mediante una plantilla descargable directamente en la aplicación ([ver detalles](docs/cards_import.md)).

El objetivo es centralizar la configuración en un YAML único por usuario y ofrecer una interfaz uniforme que evite scripts manuales dispersos.

## 2. Requisitos previos
### 2.1 Software base
- Windows 10/11 de 64 bits (la aplicación funciona en otros sistemas con Python 3.10+, pero los scripts incluidos usan `.bat`).
- Python 3.10 o superior con `pip` habilitado.
- Git 2.30+ disponible en el `PATH`.
- Apache Maven 3.8+ y JDK 11 o superior configurados en el `PATH` (la app invoca `mvn`/`mvn.cmd`).

### 2.2 Dependencias Python
El proyecto depende de:

- `PySide6` 6.8.1
- `PyYAML` 6.0.2
- `rich` 13.7.1
- `pydantic` 1.10.14
- `python-dotenv` 1.0.1 (carga de variables desde `.env`)
- `pymssql` 2.3+ (cliente nativo para SQL Server 2019)
- `openpyxl` 3.1.5 (lectura y escritura de plantillas para importar tarjetas)

Se instalan automáticamente con los scripts descritos más adelante.

### 2.3 Recursos opcionales
- Acceso a los repositorios y NAS corporativos definidos en tu `config.yaml`.
- Permisos para escribir en `%APPDATA%\ForgeBuild` (Windows) o `~/.forgebuild` (Linux/macOS).

## 3. Preparación del entorno
### 3.1 Clonado del repositorio
```bash
cd C:\ruta\donde\trabajaras
git clone https://ruta/al/repositorio.git
cd Ulises232-ForgeBuild_Groups_PipeLine_GIT
```

### 3.2 Crear y activar entorno virtual
En Windows:
```bat
python -m venv .venv
.venv\Scripts\activate
```

En Linux/macOS:
```bash
python -m venv .venv
source .venv/bin/activate
```

### 3.3 Instalar dependencias y ejecutar en modo desarrollo
La forma recomendada en Windows es usar `run_dev.bat`, que descarga las dependencias a una carpeta local (`wheels/`) y luego levanta la app:
```bat
run_dev.bat
```
El script también establece variables útiles como `QT_LOGGING_RULES`, `PYTHONUNBUFFERED`, `HERR_REPO` (ruta base del monorepo) y `GIT_TRACE_FILE`. Ajusta estas variables directamente en el `.bat` si necesitas otros valores.

Si prefieres hacerlo manualmente (o estás en Linux/macOS):
```bash
pip install --upgrade pip
pip install -r requirements.txt
export QT_LOGGING_RULES='*.debug=false;*.info=false;*.warning=true;*.critical=true'
python -m buildtool.app
```

#### Configuración del historial en SQL Server 2019

- Copia `.env.example` como `.env` y define `BRANCH_HISTORY_DB_URL` con la cadena de conexión del servidor.
- Opcionalmente fija `BRANCH_HISTORY_BACKEND=sqlserver` cuando quieras forzar el modo en línea.
- Sigue los pasos detallados en `docs/sqlserver_migration.md` para migrar el contenido existente desde SQLite y validar los privilegios del usuario en la base de datos destino.

### 3.4 Generar ejecutable con PyInstaller
Para crear un ejecutable independiente (Windows), ejecuta:
```bat
build_exe.bat
```
El script ahora detecta si las dependencias ya fueron instaladas previamente dentro de `.venv` y reutiliza el entorno cuando `requirements.txt` no cambió. Esto evita reinstalaciones completas de `pip`/`PyInstaller` en cada corrida y reduce el tiempo de build.

Opcionalmente puedes utilizar los nuevos parámetros:

```bat
build_exe.bat --skip-deps            & rem omite la verificación/instalación de dependencias
build_exe.bat --force-deps           & rem reinstala dependencias aunque no haya cambios
build_exe.bat --deploy \\servidor\ruta\compartida
```

- `--skip-deps` es útil cuando ya verificaste manualmente el entorno y solo quieres regenerar el `.exe`.
- `--force-deps` fuerza la reinstalación si sospechas de un entorno corrupto.
- `--deploy` copia automáticamente `dist\ForgeBuild\ForgeBuild.exe` a la ruta indicada (por ejemplo, una carpeta compartida o NAS), eliminando el paso manual de copiar el archivo.

> Nota: si detecta cambios en `requirements.txt` o ausencia de PyInstaller dentro del entorno virtual, el script reinstalará dependencias aunque se haya especificado `--skip-deps`.

### 3.5 Versionado y registro de cambios
- La versión oficial de la aplicación se declara en el archivo de texto `VERSION` (en la raíz del repositorio). Ese mismo número se muestra en la ventana principal y puede ser reutilizado por otros procesos (CI, empaquetado, etc.).
- El historial detallado de funcionalidades, mejoras y correcciones se mantiene en `CHANGELOG.md`, siguiendo el estilo de [Keep a Changelog](https://keepachangelog.com/es-ES/1.1.0/).
- Al liberar una nueva iteración, actualiza `VERSION` y añade una sección correspondiente en el changelog con la fecha y los cambios relevantes.

## 4. Configuración
### 4.1 Ubicación y ciclo de vida del archivo
- Al iniciar por primera vez, la aplicación copia `buildtool/data/config.yaml` a `%APPDATA%\ForgeBuild\config.yaml` (Windows) o `~/.forgebuild/config.yaml` (otros sistemas).
- Cada arranque lee ese archivo, aplica las variables declaradas en `environment` al proceso y a los subprocesos Maven/Git.
- Los grupos, proyectos y targets declarados se migran automáticamente a la base SQLite `config.sqlite3` dentro de la misma carpeta (`%APPDATA%\ForgeBuild` o `~/.forgebuild`) y a partir de entonces se consultan únicamente desde ahí.
- Los cambios guardados desde el asistente se escriben en el archivo YAML (sin secciones de grupos) y sincronizan el contenido de la base de datos en un solo paso.

### 4.2 Parámetros globales principales
- `paths.workspaces`: mapa `alias → ruta` usado para localizar repositorios cuando un proyecto define `workspace` o `repo`.
- `paths.output_base`: carpeta base donde se copian los artefactos de cada perfil (`<output_base>/<proyecto>/<perfil>`).
- `paths.nas_dir`: ruta base de un NAS para respaldos/historial (usado por las funciones de NAS).
- `environment`: diccionario de variables de entorno a exportar (se sobreescriben valores previos al lanzar la app).
- `artifact_patterns`: patrones glob para localizar artefactos en `target/` cuando no hay reglas específicas.
- `default_execution_mode`: `integrated` (log en la ventana principal) o `separate_windows` (abre consola aparte en Windows).

### 4.3 Grupos, proyectos y perfiles
Cada entrada en `groups` representa un cliente o línea de negocio con su propia configuración:

- `key`: identificador del grupo (ej. `PDF`, `ELLIS`).
- `repos`: mapa `alias → ruta absoluta` al workspace correspondiente.
- `output_base`: carpeta base para los artefactos de ese grupo (si no se define, se usa la global).
- `profiles`: lista de entornos disponibles (Desarrollo, QA, Producción, etc.).
- `projects`: proyectos Maven dentro del grupo. Cada proyecto puede sobreescribir `profiles`, `execution_mode`, `workspace` o `repo`.
- `deploy_targets`: destinos de despliegue para copiar artefactos según perfil.

### 4.4 Módulos Maven
Dentro de cada `project.modules` defines las reglas de compilación y copia por módulo:

- `name`: nombre descriptivo (se muestra en la UI).
- `path`: ruta relativa dentro del repositorio.
- `goals`: secuencia Maven (ej. `clean package`, `clean install`).
- `optional`: si es verdadero, solo se ejecuta cuando eliges “Compilar TODOS”.
- `profile_override`: fuerza un perfil Maven específico (difiere del perfil seleccionado en UI).
- `only_if_profile_equals`: limita la ejecución al perfil indicado.
- `no_profile`: ejecuta Maven sin `-P`.
- `run_once`: el módulo se compila solo una vez por sesión y sus artefactos se reutilizan para otros perfiles.
- `serial_across_profiles`: evita ejecutar ese módulo en paralelo entre perfiles (usa locks).
- `version_files`: lista de archivos relativos a modificar cuando se cambia una versión (administrados desde el wizard).
- **Salida de artefactos** (solo una opción a la vez):
  - `copy_to_profile_war`: copia los `.war` a `<output>/<perfil>/war/`.
  - `copy_to_profile_ui`: copia los `.jar` de UI a `<output>/<perfil>/ui-ellis/`.
  - `copy_to_subfolder`: copia `.jar/.war` a una subcarpeta personalizada.
  - `copy_to_root`: cuando está activo, la copia va directo a `<output>/<perfil>/`.
- **Selección avanzada**:
  - `select_pattern` y `rename_jar_to`: permiten copiar un solo archivo que cumpla el patrón y renombrarlo al destino.

### 4.5 Targets de deploy
Cada entrada en `deploy_targets` dentro de un grupo define adónde copiar los artefactos:

- `name`: identificador que verás en la UI.
- `project_key`: proyecto al que aplica.
- `profiles`: lista de perfiles permitidos para ese destino.
- `path_template`: ruta absoluta; si contiene `{version}`, se reemplaza al desplegar, si no, se crea una subcarpeta con la versión.
- `hotfix_path_template`: ruta alternativa cuando marcas la casilla **Hotfix** en la interfaz.

### 4.6 Variables de entorno
Usa `environment` para centralizar configuraciones como rutas de Maven/Java, proxies o flags internos. Al actualizar el YAML desde el asistente o manualmente, la app aplica los valores y elimina variables que ya no existan en el archivo.

## 5. Uso de la aplicación
### 5.1 Elementos comunes de la ventana principal
- **Botón “Config/Wizard”**: abre el asistente de grupos en modalidad modal. Desde ahí puedes crear/editar grupos, proyectos, módulos, perfiles y targets. Al cerrar con “Guardar”, la app recarga la configuración sin reiniciar.
- **Pestañas principales**: `Pipeline` y `Repos (Git)`.
- **Bitácora**: cada vista incluye un panel de texto donde se imprimen logs de Maven, Git o copias de archivos.

### 5.2 Pipeline → Build
1. Selecciona un **Grupo**. Si el grupo solo tiene un proyecto, el combo “Proyecto” se oculta automáticamente. Puedes escribir en los combos para filtrar rápidamente la lista de grupos/proyectos.
2. Marca los **Perfiles** a compilar (multiselección). El cuadro admite búsqueda incremental, de modo que basta con teclear parte del nombre para filtrar los perfiles disponibles.
3. Define los **Módulos** a incluir. Todos vienen seleccionados por defecto; el buscador te permite localizar módulos específicos en listas largas.
4. Usa el combo **Presets** para aplicar configuraciones guardadas (grupo, proyecto, perfiles y módulos) o para crear una nueva con “Guardar preset…”. El botón “Administrar…” abre un diálogo para renombrar o eliminar presets existentes.
5. Al ejecutar, la app agenda los perfiles en serie y distribuye los módulos en paralelo respetando `run_once` y `serial_across_profiles`.
6. Los logs muestran los comandos Maven ejecutados, advertencias (ruta faltante, patrón no encontrado) y resultados de copia.

Tips:
- Si un módulo opcional es necesario, asegúrate de usar “Compilar TODOS”.
- Revisa que `mvn` esté en el `PATH`; de lo contrario verás errores inmediatos.
- La gestión de presets es compartida con la vista de Deploy; cualquier cambio queda disponible para ambos flujos.

### 5.3 Pipeline → Deploy
1. Selecciona **Grupo** y, si corresponde, el **Proyecto** (ambos combos admiten búsqueda por texto).
2. Marca los **Perfiles** destino; el cuadro de selección incluye filtro incremental.
3. Escribe la **Versión** (formato sugerido: `yyyy-mm-dd_nnn`).
4. Marca **Hotfix** si deseas usar `hotfix_path_template`.
5. Guarda combinaciones frecuentes (grupo/proyecto/perfiles/version/hotfix) con el combo **Presets** y aplícalas con un clic.
6. Usa “Copiar seleccionados” o “Copiar TODOS”. Cada perfil utiliza el `deploy_target` configurado; la bitácora indica la ruta final y archivos copiados.

Si algún perfil no tiene destino configurado, la UI mostrará un mensaje y omitirá ese perfil.

### 5.4 Repos (Git)
La pestaña de Git reúne operaciones globales para todos los repos del proyecto activo:

- **Selector de proyecto**: filtra los repositorios vinculados y carga la rama actual.
- **Resumen por repositorio**: árbol con estado, rama activa y diferencias detectadas por `discover_status_fast`.
- **Historial de ramas**: lista de ramas registradas localmente, quién las creó y su disponibilidad local/origin.
- **Acciones principales**:
  - *Switch (global)*: cambia todos los repos del proyecto a la rama indicada y actualiza el historial.
  - *Crear (local, global)* y *Push (global)*: crea una rama en todos los repos y la publica en `origin`.
  - *Eliminar local (global)*: borra la rama localmente (requiere marcar “Confirmar”).
  - *Crear ramas de versión*: genera ramas `release` y, opcionalmente, una rama `*_QA` auxiliar.
  - *Refrescar vista*: vuelve a consultar estado de archivos y ramas.
  - *Reconciliar con Git (solo local)*: reescanea ramas locales/origin sin tocar el servidor.
  - *Recuperar NAS / Publicar NAS*: sincroniza el índice de ramas con la carpeta NAS definida.

Todas las acciones corren en hilos de fondo con protección de errores; la bitácora informa el avance y la UI muestra diálogos de éxito/fallo.

#### 5.4.1 Historial unificado en SQLite
- A partir de la versión 1.2.0 el índice de ramas y el activity log dejan de escribirse en `branches_index.json` / `activity_log.jsonl`.
- La aplicación crea un archivo `branches_history.sqlite3` tanto en la carpeta local (`%APPDATA%\ForgeBuild` o `~/.local/share/forgebuild`) como en la NAS definida en la configuración.
- Al abrir una versión nueva se migra automáticamente el contenido existente de los JSON a la base de datos sin perder información.
- Las vistas **Repos**, **NAS → Ramas** y **NAS → Activity Log** leen directamente desde SQLite, conservando filtros, búsquedas y edición manual.
- Para respaldar o sincronizar manualmente basta con copiar el archivo `.sqlite3`; el flujo de “Publicar/Recuperar NAS” ya trabaja sobre esa base.

### 5.5 Asistente de grupos (Config/Wizard)
El asistente está dividido en pestañas para **Grupos**, **Proyectos**, **Módulos** y **Targets**. Algunas características clave:

- Puedes clonar un grupo existente para acelerar la creación de nuevos entornos.
- Cada módulo permite configurar rutas, goals, flags y archivos de versión desde una interfaz amigable.
- El asistente valida claves duplicadas, rutas vacías y listas de perfiles antes de guardar.
- Al guardar, se ejecuta `save_config` y la ventana principal recarga datos inmediatamente.

### 5.6 Historial de pipelines
La pestaña **Historial** centraliza los builds y deploys ejecutados desde la aplicación:

- Filtra por tipo de pipeline, estado, grupo, proyecto y rango de fechas.
- Consulta los detalles principales (perfiles, módulos, usuario, versión y mensaje final) en una tabla ordenada cronológicamente.
- Selecciona una fila para revisar el log completo almacenado en la base SQLite interna.
- Exporta los resultados a CSV o limpia el historial cuando ya no sea necesario conservarlo.

## 6. Flujo sugerido de trabajo diario
1. **Abrir la app** y confirmar que el grupo/proyecto correctos estén seleccionados.
2. **Actualiza ramas** desde la pestaña Git (Switch o Pull manual externo) y valida el estado con “Refrescar vista”.
3. **Compila** los perfiles requeridos desde `Pipeline → Build`.
4. **Revisa artefactos** en la carpeta de salida configurada.
5. **Despliega** hacia QA/Producción usando `Pipeline → Deploy`.
6. **Publica ramas o versión** usando los botones de Git si es necesario.
7. **Guarda ajustes** de grupos/módulos cuando cambien rutas o nuevos proyectos.

## 7. Solución de problemas rápida
- **No arranca PySide6**: verifica que las dependencias estén instaladas en el entorno virtual (`pip show PySide6`).
- **`mvn` no encontrado**: agrega la carpeta de Maven al `PATH` o instala Maven.
- **Errores de acceso al NAS**: confirma conectividad y permisos sobre `paths.nas_dir`.
- **Logs vacíos o app congelada**: revisa el archivo `forgebuild_crash.log` en tu carpeta de usuario para más detalles.
- **Configuración perdida**: elimina `%APPDATA%\ForgeBuild\config.yaml` para regenerar uno por defecto y luego vuelve a configurarlo con el wizard.

---
Para dudas adicionales o propuestas de mejora, documenta el escenario y comparte el `config.yaml` relevante (omite credenciales sensibles) con el equipo de herramientas internas.
