# Changelog

Todas las versiones notables de ForgeBuild (Grupos) se documentarán en este archivo.

El formato sigue, en líneas generales, las recomendaciones de [Keep a Changelog](https://keepachangelog.com/es-ES/1.1.0/).


## [1.13.4] - 2025-10-26

### Corregido
- La creación de ramas desde tarjetas ahora usa la rama base del sprint en lugar de mostrar la rama QA como origen.
- Si la rama base no existe localmente, se sincroniza automáticamente desde `origin` antes de crear la rama de la tarjeta.

## [1.13.3] - 2025-10-25

### Corregido
- Las rutas personalizadas por usuario para módulos ahora se respetan en el descubrimiento de repos, las operaciones Git y la visualización de proyectos, combinándolas automáticamente con la carpeta base del grupo correspondiente.

## [1.13.2] - 2025-10-24

### Cambiado
- Las ramas generadas desde tarjetas se basan en la rama de versión del sprint en lugar de la rama `_QA`, evitando incluir cambios adelantados durante el desarrollo.

## [1.13.1] - 2025-10-24

### Documentación
- Se añadió `docs/sqlserver_schema.sql` con el DDL completo de las tablas e índices del backend SQL Server para Branch History.
- Se complementó el script con la definición de todas las tablas `config_*`, sus relaciones, índices y triggers para la capa compartida de configuración.

## [1.13.0] - 2025-10-23

### Añadido
- Persistencia de configuración sobre SQL Server a través de `SqlConfigStore`, incluyendo tablas `config_*` para grupos, proyectos, módulos y despliegues.
- Tablas de overrides por usuario para rutas de repositorios, módulos y despliegues junto con métodos `set_*_user_paths` en el almacén compartido.
- Asistente de grupos con pestañas separadas para definición global y "Rutas por usuario", permitiendo capturar overrides por cuenta directamente desde la UI.

### Cambiado
- `load_config` y `save_config` ahora construyen el almacén desde el repositorio compartido y permiten inyectar un repositorio alterno para pruebas.
- `ConfigStore.list_groups` admite usuario activo para combinar definiciones globales con overrides y las tareas de build/deploy resuelven rutas usando valores personalizados.
- Las vistas de Build y Deploy recargan la configuración antes de ejecutar pipelines y registran en la bitácora las rutas resueltas para el usuario activo.
- Se descartó la migración/creación de tablas `sprints`, `cards` y `card_assignments` dentro del almacén de configuración al no ser necesarias en la persistencia compartida.

### Corregido
- Se reordenó la creación de `catalog_companies` antes de `card_company_links` en el esquema de Branch History para evitar fallos de llaves foráneas al inicializar una base limpia.
- El repositorio de Branch History expone un context manager `connection()` para reutilizar el pool SQL Server desde el almacén de configuración sin errores de atributo.
- `SqlConfigStore` fuerza cursores sin `as_dict` en SQL Server y evita errores `ColumnsWithoutNamesError` al contar registros durante el arranque.

### Interno
- Pruebas de configuración actualizadas con un `FakeBranchHistoryRepo` basado en SQLite para validar la nueva capa SQL.
- Casos adicionales que verifican la composición global+usuario y la API de overrides dentro de `test_config_store`.

## [1.12.0] - 2025-10-22

### Añadido
- Selección múltiple de tarjetas en la pestaña de planeación para ejecutar acciones sobre varias al mismo tiempo.
- Botón de asignación masiva que permite definir o limpiar en un paso los responsables de desarrollo y QA de todas las tarjetas seleccionadas.

## [1.11.1] - 2025-10-21

### Añadido
- Columna `cards.branch_created_flag` para marcar automáticamente las tarjetas cuya rama se generó desde la aplicación al crearla.

### Cambiado
- El botón **Crear rama** en la planeación también se habilita para la persona asignada como desarrolladora de la tarjeta, sin requerir rol de líder.

## [1.11.0] - 2025-10-20

### Añadido
- Captura y edición de un script SQL por tarjeta, permitiendo escribirlo directamente o cargarlo desde un archivo dentro del formulario de tarjetas.
- Indicadores en la planeación y en el navegador de tarjetas para identificar cuáles tarjetas cuentan con script asociado.
- Exportación consolidada de los scripts de un sprint en un único archivo SQL con encabezados por tarjeta.
- Tabla `card_scripts` y migración correspondiente para preservar la relación tarjeta-script durante traslados de datos.

### Cambiado
- Formularios de sprint y tarjeta con panel desplazable para asegurar que todos los controles permanezcan visibles en pantallas con menor altura.

### Corregido
- El formulario de tarjetas vuelve a incluir la sección de script SQL, evitando el fallo al abrir una tarjeta tras habilitar la nueva funcionalidad.

## [1.10.1] - 2025-10-01

### Corregido
- Evita que la eliminación de sprints falle cuando la columna `card_sprint_links.sprint_id` no admite valores nulos eliminando los vínculos como alternativa.
- Los formularios de sprint y tarjeta se abren ahora con doble clic, se cierran tras guardar o eliminar y dejan de reaparecer al refrescar la vista.
- Editar tarjetas desde la pestaña **Tarjetas** respeta la pestaña activa, evita saltos a Planeación y muestra avisos de éxito al completar cada acción.
- Crear tarjetas desde la pestaña **Tarjetas** mantiene la vista activa sin cambiar automáticamente a la pestaña de Planeación.
- Al eliminar una tarjeta, el formulario asociado se oculta y cierra de inmediato (incluso desde la pestaña **Tarjetas**) para evitar que permanezca abierto tras la confirmación.
- Editar tarjetas sin sprint vuelve a permitir guardar cambios sin exigir la selección de un sprint.

## [1.10.0] - 2025-02-20

### Añadido
- Catálogo de tipos de incidencia con soporte para icono dentro del panel de configuración de catálogos.
- Selección de tipo de incidencia en el formulario de tarjetas y representación con icono tanto en la planeación como en el navegador de tarjetas.
- Compatibilidad del importador masivo y la plantilla CSV/Excel con la nueva columna de tipo de incidencia, junto con la documentación actualizada.
- Vista previa del icono al editar tipos de incidencia, mostrando cómo se verán las tarjetas y permitiendo limpiar o reemplazar la imagen rápidamente.

### Cambiado
- Las listas de tarjetas aplican un fondo semitransparente basado en el estado de cada tarjeta, conservando colores suaves y legibles.
- El catálogo de incidencias elimina la captura manual de color para simplificar la administración conservando únicamente el icono.
- Los colores de estado de tarjeta ahora distinguen backlog, tarjetas en sprint, pruebas unitarias, QA y terminadas con gamas suaves compatibles con modo claro/oscuro.

### Corregido
- El tipo de incidencia seleccionado se guarda correctamente en las tarjetas creadas o editadas, así como en las importadas por plantilla.

## [1.9.0] - 2025-02-19

### Añadido
- Importación masiva de tarjetas desde archivos CSV o Excel con validaciones de catálogo y resumen de resultados en la interfaz.
- Botones para importar tarjetas y descargar la plantilla directamente desde la pestaña **Tarjetas**, disponibles para usuarios con rol de líder.
- Documentación dedicada (`docs/cards_import.md`) y plantilla base (`docs/templates/cards_template.csv`) para preparar la información a importar.

### Cambiado
- Se añadió la dependencia `openpyxl` para soportar la lectura y generación de plantillas en formato Excel.

### Corregido
- La importación desde CSV admite también archivos guardados con codificaciones Latin-1/CP1252 para evitar errores de lectura en ambientes Windows.

## [1.8.1] - 2025-02-18

### Cambiado
- El catálogo de empresas ahora permite ajustar manualmente el consecutivo del próximo sprint desde la interfaz de edición.

## [1.8.0] - 2025-02-17

### Añadido
- Ventana de edición de sprints con listado reutilizable de tarjetas pendientes por empresa, permitiendo asignar varias al sprint en un solo paso.
- Panel de administración con pestañas para usuarios y catálogos accesible a roles de administrador y líder.
- Catálogo inicial de empresas con captura de nombre, grupo, autor y fechas de actualización reutilizable al planear sprints.
- Pestaña dedicada para explorar tarjetas con filtros por grupo, empresa, sprint y estado desde la vista de planeación.

### Cambiado
- La relación entre sprints y grupos se almacena en la tabla `sprint_groups`, migrando los datos existentes y manteniendo la compatibilidad con SQL Server.
- Los formularios de sprints y tarjetas ahora se muestran en diálogos reutilizables definidos en `editor_forms.py`, facilitando su uso desde otras vistas.
- Los sprints almacenan y muestran la empresa asociada tanto en la tabla como en el formulario de edición.
- El formulario de planeación de sprints incorpora selección de grupo/empresa sincronizada y asignación guiada al mover tarjetas.
- La pestaña de tarjetas ahora permite crear tarjetas independientes de un sprint, precargando grupo y empresa según los filtros activos y aplazando la asignación del sprint hasta la planeación.

### Corregido
- La llave foránea `fk_card_sprint_sprint` ahora se crea sin acciones en cascada para evitar que SQL Server rechace el esquema por rutas múltiples al inicializar la base.
- Manejo seguro de diálogos de sprint/tarjeta eliminando referencias a widgets destruidos para evitar fallos de Qt al actualizar permisos tras cerrar formularios.
- Se ajustaron las importaciones internas de `editor_forms` para utilizar los iconos compartidos y evitar errores de módulo faltante al iniciar la aplicación.
- Restituida la construcción de la pestaña de planeación para que vuelva a crear el árbol de sprints y los controles asociados, evitando el fallo por el método `_build_planning_tab` faltante al abrir la ventana.
- Evitado el fallo al cargar la vista de planeación inicial verificando la existencia de los combos antes de poblarlos durante el refresco automático.
- Al eliminar un sprint ahora se liberan sus tarjetas asociadas y permanecen en el catálogo, dejando el campo `sprint_id` en blanco en lugar de eliminarlas.
- Guardar una tarjeta desde el planificador valida que el formulario siga activo y limita el combo de sprints a la empresa correspondiente, evitando errores y asignaciones cruzadas.
- La relación `card_sprint_links` deja de propagar eliminaciones en cascada hacia `sprints`, evitando la ruta múltiple que impedía inicializar el esquema en SQL Server.
- Se restauró la acción "Crear rama" en la vista de planeación para que vuelva a enlazar el botón con su lógica y valide permisos/estado antes de ejecutar.
- Guardar una tarjeta sin sprint ahora conserva `cards.sprint_id` en `NULL` y pospone la relación histórica, eliminando la violación de la llave foránea `fk_cards_sprint`.

### Añadido
- Ventana de edición de sprints con listado reutilizable de tarjetas pendientes por empresa, permitiendo asignar varias al sprint en un solo paso.

### Cambiado
- La relación entre sprints y grupos se almacena en la tabla `sprint_groups`, migrando los datos existentes y manteniendo la compatibilidad con SQL Server.
- Los formularios de sprints y tarjetas ahora se muestran en diálogos reutilizables definidos en `editor_forms.py`, facilitando su uso desde otras vistas.

### Corregido
- Manejo seguro de diálogos de sprint/tarjeta eliminando referencias a widgets destruidos para evitar fallos de Qt al actualizar permisos tras cerrar formularios.
- Se ajustaron las importaciones internas de `editor_forms` para utilizar los iconos compartidos y evitar errores de módulo faltante al iniciar la aplicación.
- Restituida la construcción de la pestaña de planeación para que vuelva a crear el árbol de sprints y los controles asociados, evitando el fallo por el método `_build_planning_tab` faltante al abrir la ventana.
- Evitado el fallo al cargar la vista de planeación inicial verificando la existencia de los combos antes de poblarlos durante el refresco automático.
- Al eliminar un sprint ahora se liberan sus tarjetas asociadas y permanecen en el catálogo, dejando el campo `sprint_id` en blanco en lugar de eliminarlas.
- Guardar una tarjeta desde el planificador valida que el formulario siga activo y limita el combo de sprints a la empresa correspondiente, evitando errores y asignaciones cruzadas.

## [1.8.0] - 2025-02-17
### Añadido
- Panel de administración con pestañas para usuarios y catálogos accesible a roles de administrador y líder.
- Catálogo inicial de empresas con captura de nombre, grupo, autor y fechas de actualización reutilizable al planear sprints.
- Pestaña dedicada para explorar tarjetas con filtros por grupo, empresa, sprint y estado desde la vista de planeación.

### Cambiado
- Los sprints almacenan y muestran la empresa asociada tanto en la tabla como en el formulario de edición.
- El formulario de planeación de sprints incorpora selección de grupo/empresa sincronizada y asignación guiada al mover tarjetas.
- La pestaña de tarjetas ahora permite crear tarjetas independientes de un sprint, precargando grupo y empresa según los filtros activos y aplazando la asignación del sprint hasta la planeación.

### Corregido
- La relación `card_sprint_links` deja de propagar eliminaciones en cascada hacia `sprints`, evitando la ruta múltiple que impedía inicializar el esquema en SQL Server.
- Se restauró la acción "Crear rama" en la vista de planeación para que vuelva a enlazar el botón con su lógica y valide permisos/estado antes de ejecutar.
- Guardar una tarjeta sin sprint ahora conserva `cards.sprint_id` en `NULL` y pospone la relación histórica, eliminando la violación de la llave foránea `fk_cards_sprint`.

## [1.7.0] - 2025-02-16
### Añadido
- Tabla `branch_local_users` en SQL Server para registrar la presencia local de cada rama por usuario y exponerla mediante `load_local_states` y nuevas pruebas automatizadas.
- Autenticación de usuarios con contraseñas seguras PBKDF2, campos de hash/salt y banderas de restablecimiento obligatorio desde `branch_store.authenticate_user`.
- Módulo de administración de usuarios (`UserAdminView`) visible sólo para el rol `admin`, con altas, edición de roles, desactivación y solicitud de restablecimiento de contraseña.
- El diálogo de inicio de sesión recuerda las credenciales en AppData para precargarlas automáticamente al abrir la aplicación.

### Cambiado
- `BranchHistoryView` y la pestaña Git muestran un único historial respaldado por SQL Server, eliminando los flujos de sincronización NAS y adoptando el registro de actividad renombrado.
- `BranchRecord` ahora calcula la disponibilidad local por usuario activo, propagando la información al backend al guardar o sincronizar el índice.
- Solo los líderes pueden eliminar ramas desde el historial y la acción borra inmediatamente el registro en SQL Server.
- Las ramas creadas únicamente en local permanecen visibles solo para su autor hasta que existen en origin.
- El diálogo de inicio de sesión exige contraseña, gestiona restablecimientos forzados y oculta controles de roles/altas directas.
- Las contraseñas ahora requieren al menos 7 caracteres, una letra mayúscula y un número, sin obligar minúsculas ni caracteres especiales.
- `list_users` excluye usuarios deshabilitados por defecto y las bajas marcan el estado inactivo en lugar de eliminar filas.
- Se agregó un botón de "ojo" para mostrar u ocultar la contraseña en el inicio de sesión y durante el restablecimiento.

### Corregido
- La inicialización del esquema en SQL Server ajusta `branch_local_users.branch_key` a NVARCHAR(255) antes de crear la llave foránea hacia `branches.[key]`, evitando el error 1753 en instalaciones existentes.
- Cobertura automatizada para la autenticación, restablecimiento y asignación de roles al crear o deshabilitar usuarios.
- La migración de `users.require_password_reset` maneja instalaciones previas añadiendo la columna en pasos separados y forzando el valor 0, eliminando el error "Invalid column name" al iniciar.

## [1.6.0] - 2025-02-15
### Añadido
- Capa de persistencia extensible para el historial de ramas con soporte a SQL Server 2019 mediante `BranchHistoryRepo`.
- Pool reutilizable de conexiones TDS (`pymssql`) y carga de credenciales desde `.env` para entornos centralizados.
- Script `scripts/migrate_branch_history.py` para migrar datos desde SQLite y documentación `docs/sqlserver_migration.md` con el procedimiento completo.

### Cambiado
- `branch_store` opera exclusivamente contra SQL Server, eliminando migraciones locales, archivos SQLite y flujos offline/NAS.
- `BranchHistoryDB` retira el backend SQLite y exige una cadena `BRANCH_HISTORY_DB_URL` para inicializar la persistencia.
- `README` y `.env` documentan la nueva configuración del backend y dependencias requeridas.

### Corregido
- Las operaciones genéricas de inserción/actualización citan los identificadores en SQL Server, evitando errores de sintaxis con
  columnas reservadas como `key` al inicializar roles y usuarios.
- La lectura de roles en SQL Server deja de aliasar la columna reservada `key`, previniendo fallos al abrir el diálogo de inicio de sesión.
- La inserción/actualización genérica devuelve las claves alfanuméricas sin forzarlas a enteros, evitando el fallo `invalid literal for int()` al crear roles predeterminados en SQL Server.
- La consulta del historial de actividades cita el alias `user`, eliminando el error `Incorrect syntax near the keyword 'user'` al cargar la vista NAS en SQL Server.

## [1.5.1] - 2025-10-06
### Añadido
- Pestaña de planeación unificada para altas/ediciones de sprints y tarjetas en la
  misma vista, con finalización de sprints, borrado y bloqueo automático de nuevas
  tarjetas cuando un sprint está cerrado.
- Controles de tarjetas con ID de ticket, autoría y creación directa de ramas que
  muestran su presencia local/origin y registran al creador desde el historial NAS.
- Los sprints almacenan también la rama QA asociada para dirigir la creación de
  tarjetas y validar los flujos de revisión.
- Campos de evidencia para registrar los enlaces de pruebas unitarias y QA en cada
  tarjeta, editables únicamente por el rol correspondiente o el líder del equipo.
- Los combos de asignación filtran desarrolladores y QA según su rol e incorporan
  toggles que permiten marcar o desmarcar las pruebas unitarias/QA respetando los
  permisos del usuario y actualizando el historial de pipelines.

### Corregido
- La inicialización de `branch_history_db.sqlite3` omite la creación temprana del índice de sprints
  y valida que existan las columnas antes de construir índices, evitando el error
  `no such column: branch_key` al abrir el diálogo de autenticación en instalaciones antiguas.
- La utilería de widgets importa `QCompleter` desde `QtWidgets`, restaurando el arranque en entornos
  con PySide6 6.8 donde la clase ya no está disponible en `QtGui`.
- La migración de `config.sqlite3` añade las columnas `branch_key`/`metadata` a `sprints`
  antes de recrear índices, evitando fallos al cargar la ventana principal con bases antiguas.
- El historial de pipelines migra primero las columnas nuevas y crea los índices al final,
  evitando el error `no such column: card_id` en bases existentes sin los campos recientes.
- La inicialización de `branch_history_db.sqlite3` reconstruye las tablas `sprints` y `cards`
  cuando faltan columnas recientes, preservando los datos heredados y rellenando los valores
  por defecto esperados por la nueva UI de planeación.
- La vista de sprints permite escoger la rama base a partir del grupo y maneja de forma segura
  el alta cuando la rama ya no existe, evitando violaciones de llave foránea al crear nuevos sprints.
- La creación de ramas desde tarjetas parte ahora de la rama QA configurada en el sprint,
  validando su selección antes de operar y evitando ramas huérfanas desde `HEAD`.

### Cambiado
- La vista de sprints permite editar sprints solo para líderes y tarjetas para cualquier usuario,
  añadiendo controles de permisos y edición directa desde la misma pestaña.
- Las tarjetas nuevas y existentes normalizan el nombre de la rama anteponiendo la rama QA y el ticket
  (por ejemplo `v2.68_QA_EA-102`), guiando al usuario en los diálogos y validándolo en la capa de
  persistencia.
- Las acciones de marcar pruebas unitarias/QA y crear la rama de la tarjeta respetan al responsable
  asignado (o al líder), bloqueando el botón tras generar la rama hasta que se elimina del historial
  local/origin.
- El divisor de la vista de planeación mantiene más ancho el panel izquierdo (3/3 con tamaño inicial
  860/520) y amplía el máximo del panel de detalle, facilitando la lectura de columnas sin que la ficha
  se imponga por su tamaño mínimo.
- Las validaciones de merge en la vista Git permiten integrar tarjetas a la rama QA con solo pruebas
  unitarias aprobadas, bloquean merges directos contra la rama madre y exigen que todas las tarjetas del
  sprint tengan QA y unitarias antes de liberar la rama QA hacia la versión.

## [1.5.0] - 2025-10-05
### Añadido
- Módulo de planeación de sprints con tarjetas enlazadas a ramas, asignación de responsables y validación de checks antes del merge.
- Diálogo de autenticación con gestión de usuarios y roles persistidos en NAS/SQLite para reutilizar la identidad en todas las vistas.
- Nuevos campos en el historial de pipelines para rastrear tarjetas, aprobaciones y responsables, incluyendo filtros específicos en la UI.

### Cambiado
- El flujo de merge en la vista Git valida las aprobaciones registradas en las tarjetas y documenta el resultado en la historia.
- La vista de historial de pipelines muestra información de QA/pruebas y permite filtrar por estado o tarjeta asociada.

### Corregido
- La migración del historial crea los índices después de validar `branch_key`,
  evitando errores de inicio cuando existen bases antiguas sin la columna.
- La inicialización de la base `branch_history_db.sqlite3` migra `activity_log` heredado añadiendo `branch_key` antes de crear los
  índices, evitando fallos al abrir el diálogo de autenticación en instalaciones existentes.

## [1.4.6] - 2025-10-04
### Cambiado
- El helper `SignalBlocker` vive en `buildtool/ui/widgets.py` para reutilizarlo entre vistas y
  evitar definiciones duplicadas en los historiales.
- Los historiales de ramas y actividad NAS comparten helpers de filtros y búsqueda para
  eliminar duplicaciones y mantener una experiencia consistente.
- Los combos de grupo y proyecto en las vistas de build y deploy usan `setup_quick_filter`
  compartido para tener filtrado consistente sin duplicar código.

## [1.4.5] - 2025-10-04
### Corregido
- Se degrada automáticamente el `journal_mode` a `DELETE` cuando SQLite no permite `WAL`, evitando
  bloqueos al compartir la base de historial entre sesiones concurrentes.


## [1.4.4] - 2025-10-03
### Cambiado
- Las vistas de historial local y NAS se consolidaron en un único widget configurable, eliminando
  duplicaciones y facilitando que futuras mejoras afecten a ambos orígenes de datos.

## [1.4.3] - 2025-10-02
### Corregido
- Al cambiar a ramas creadas por otro usuario, la vista de historial conserva al autor
  original y registra al editor únicamente en el historial de actividad.
- Las vistas de ramas local y NAS muestran al creador antes que al último editor en sus
  listados para mantener claro el propietario original.

## [1.4.2] - 2025-10-01
### Cambiado
- Funciones compartidas en `core.config_queries` centralizan la lectura de grupos, proyectos, módulos, perfiles y targets,
  reutilizadas por las vistas de pipeline y Git para evitar consultas duplicadas y mantener la UI consistente.

## [1.4.1] - 2025-09-30
### Corregido
- La persistencia del asistente de grupos ahora actualiza registros existentes en `config.sqlite3` en lugar de reinserciones masivas, preservando los identificadores y evitando el crecimiento artificial de claves.

## [1.4.0] - 2025-09-29
### Añadido
- Migración automática de grupos, proyectos y targets desde `config.yaml` hacia la base `config.sqlite3`, con pruebas unitarias que validan la persistencia.

### Cambiado
- Las vistas de Build, Deploy, Git e historial consultan únicamente los proyectos declarados dentro de grupos; se eliminaron los combos y flujos que dependían del modo global legado.
- `save_config` sincroniza los grupos en SQLite y genera un YAML sin secciones de grupos, evitando duplicidades.
- La base `config.sqlite3` ahora normaliza grupos, proyectos, módulos, perfiles y targets en tablas dedicadas para facilitar futuras extensiones.

### Eliminado
- Compatibilidad con las listas globales `projects`, `profiles` y `deploy_targets` fuera de `groups`.
- Tabla auxiliar `project_profiles`; ahora los perfiles de proyecto viven dentro del `config_json` del propio proyecto.

## [1.3.4] - 2025-09-28
### Corregido
- La interfaz Git y la vista de ramas NAS ahora muestran advertencias claras y continúan en modo offline cuando la NAS no está disponible, evitando cierres inesperados.
- Las operaciones que escriben en la NAS propagan errores descriptivos para informar a la UI y al historial de actividad.

## [1.3.3] - 2025-09-27
### Corregido
- Los merges desde ramas remotas ahora realizan `fetch` previo y evitan ejecutar `merge` si la preparación del checkout falla.
- La construcción de perfiles limpia una sola vez cada carpeta de destino y retira artefactos obsoletos cuando un módulo deja de copiar archivos.

### Interno
- Pruebas unitarias que cubren el flujo abortado tras un `fetch` fallido y la limpieza diferida de destinos en los perfiles.

## [1.3.2] - 2025-09-26
### Corregido
- Conversión de los filtros de fechas del historial a UTC para respetar ejecuciones nocturnas al consultar o exportar.

## [1.3.1] - 2025-09-25
### Añadido
- Selector de hilos máximos en la vista de Build que persiste la preferencia y se aplica al crear `PipelineWorker`.
- Inclusión de proyectos definidos en grupos dentro del filtro del historial, sincronizando las listas al cambiar de grupo.

### Corregido
- Las vistas de historial y Git etiquetan sus métodos conectados como `@Slot` para evitar advertencias dinámicas y mejorar la respuesta al cambiar de pestaña.
- Local/NAS Branches y los combos reutilizables eliminan los registros dinámicos de slots, suprimiendo nuevas advertencias al navegar.
- Las vistas de Build/Deploy y el `MultiSelectComboBox` registran todos sus manejadores como slots nativos, eliminando retrasos y mensajes al alternar pestañas.
- `GitView` deja de reinstalar `ErrorGuard` en cada inicialización, suprimiendo los mensajes repetidos y reduciendo el retraso al cambiar de pestaña en el asistente.

## [1.3.0] - 2025-09-24
### Añadido
- Pestaña “Historial local” en la vista Git con un panel de búsqueda, filtrado y edición del índice SQLite local.
- Widget `LocalBranchesView` que reutiliza la experiencia de NAS para inspeccionar o capturar ramas manualmente.

### Cambiado
- Los registros manuales de ramas locales sólo generan actividad en el historial local para evitar ruido al sincronizar con la NAS.

## [1.2.0] - 2025-09-23
### Añadido
- Cuadro de búsqueda opcional en `MultiSelectComboBox` y filtrado rápido en los combos de grupo/proyecto.
- Presets reutilizables para pipelines de Build y Deploy, con diálogo dedicado para renombrarlos o eliminarlos.
- Historial persistente de pipelines en SQLite junto con una pestaña de UI para consultar, filtrar, exportar o limpiar registros.
- Persistencia de historial de ramas y bitácora de actividad en SQLite (`branches_history.sqlite3`) tanto local como en la NAS.
- Migración automática desde los archivos JSON existentes al nuevo esquema cuando se abre la versión.
- Pruebas unitarias que validan la lectura de actividad directamente desde la base de datos.

### Cambiado
- Las vistas de Build/Deploy almacenan cada ejecución (inicio, fin, estado y mensajes) en el nuevo historial y comparten la gestión de presets entre ambas.
- Documentación ampliada describiendo filtros interactivos, uso de presets y la nueva pestaña de historial.
- Las sincronizaciones “Recuperar/Publicar NAS” operan sobre la base SQLite y deduplican entradas según la rama registrada.
- Las vistas de Repos y NAS consumen la nueva capa SQL manteniendo filtros y edición manual.
- La documentación describe cómo localizar el archivo `.sqlite3` y cómo respaldarlo o copiarlo a la NAS.

### Corregido
- La migración del historial Git retira los archivos JSON legados tras importarlos para evitar reprocesarlos en cada inicio.

## [1.1.1] - 2025-09-20
### Cambiado
- `run_maven` permite que las ejecuciones en ventana separada concluyan naturalmente y respeta cancelaciones explícitas sin imponer timeouts artificiales.
- El asistente de módulos ahora expone los campos de `profile_override` y `only_if_profile_equals`, conservando sus valores al editar y guardar.

### Corregido
- El deploy copia recursivamente la estructura de carpetas de cada subdirectorio del perfil, manteniendo la jerarquía completa en el destino.

## [1.1.0] - 2025-09-19
### Añadido
- Hoja de estilos global (`buildtool/ui/theme.qss`) con tipografía, colores y estados modernos aplicados desde `MainWindow`.
- Conjunto de iconos SVG compartidos y utilidades para cargarlos en pestañas, encabezados y botones de acción.
- Worker reutilizable en `buildtool/core/workers.py` para ejecutar builds y despliegues con señales de progreso y finalización.
- Módulo `buildtool/ui/multi_select.py` con las clases `Logger` y `MultiSelectComboBox` documentadas para su reutilización.

### Cambiado
- Vistas `MainWindow`, `PipelineView` y `GitView` reorganizadas con `QSplitter`/`QScrollArea`, encabezados iconográficos y botones de acción convertidos a `QToolButton`.
- Lógica de las vistas de build y deploy actualizada para ejecutar tareas en segundo plano mediante el nuevo worker y gestionar el estado de la UI.
- Combos multi-selección refactorizados para compartir el mismo diseño con flecha desplegable e inicialización consistente.

### Corregido
- Limpieza y seguimiento de hilos de fondo a través del `TRACKER`, asegurando la liberación con `deleteLater()` y `wait()` al cerrar la aplicación.

### Interno
- Cacheo de iconos y utilidades de UI centralizadas en `buildtool/ui/widgets.py` para mantener consistencia visual en todo el proyecto.

## [1.0.0] - 2025-09-17
### Añadido
- Sistema completo de historial de ramas con sincronización hacia/desde NAS y registro de actividades.
- Vistas dedicadas para administración de ramas, publicaciones y bitácoras dentro de la pestaña Git.
- Funciones para detectar, crear, publicar y eliminar ramas en múltiples repositorios vinculados a un proyecto.
- Mejoras al asistente de configuración de grupos para importar/exportar NAS, autosalvar y preservar módulos, proyectos y targets existentes.
- Opciones del pipeline para copiar artefactos a múltiples destinos, incluyendo perfiles y carpetas personalizadas.
- Documentación oficial de instalación, configuración y uso general del sistema.

### Cambiado
- Scripts de construcción (`build_exe.bat`) optimizados para reutilizar dependencias y acelerar la regeneración del ejecutable.
- Detección y recorrido de repositorios Git alineados con las rutas declaradas en la configuración.
- Descubrimiento rápido de ramas y estado de repos, mejorando la carga inicial de la interfaz.

### Corregido
- Manejo robusto del guardián de errores y de los hilos de trabajo para evitar cierres inesperados.
- Compatibilidad del índice NAS con versiones previas del formato de almacenamiento.
- Actualización automática de archivos de versión al crear ramas de release.

### Interno
- Limpieza de logs de depuración y mejoras en los mensajes producidos por las tareas Git.
- Empaquetado consistente del paquete `buildtool` para PyInstaller y ejecuciones como script.

[1.3.0]: https://github.com/Ulises232/Ulises232-ForgeBuild_Groups_PipeLine_GIT
[1.2.0]: https://github.com/Ulises232/Ulises232-ForgeBuild_Groups_PipeLine_GIT
[1.1.1]: https://github.com/Ulises232/Ulises232-ForgeBuild_Groups_PipeLine_GIT
[1.1.0]: https://github.com/Ulises232/Ulises232-ForgeBuild_Groups_PipeLine_GIT
[1.0.0]: https://github.com/Ulises232/Ulises232-ForgeBuild_Groups_PipeLine_GIT
