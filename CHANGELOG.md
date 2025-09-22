# Changelog

Todas las versiones notables de ForgeBuild (Grupos) se documentarán en este archivo.

El formato sigue, en líneas generales, las recomendaciones de [Keep a Changelog](https://keepachangelog.com/es-ES/1.1.0/).

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
