# Changelog

Todas las versiones notables de ForgeBuild (Grupos) se documentarán en este archivo.

El formato sigue, en líneas generales, las recomendaciones de [Keep a Changelog](https://keepachangelog.com/es-ES/1.1.0/).

## [1.2.1] - 2025-09-22
### Corregido
- Se eliminó un marcador residual en `PipelineHistoryView` que provocaba un `SyntaxError` al iniciar la aplicación.

## [1.2.0] - 2025-09-21
### Añadido
- Cuadro de búsqueda opcional en `MultiSelectComboBox` y filtrado rápido en los combos de grupo/proyecto.
- Presets reutilizables para pipelines de Build y Deploy, con diálogo dedicado para renombrarlos o eliminarlos.
- Historial persistente de pipelines en SQLite junto con una pestaña de UI para consultar, filtrar, exportar o limpiar registros.

### Cambiado
- Las vistas de Build/Deploy almacenan cada ejecución (inicio, fin, estado y mensajes) en el nuevo historial y comparten la gestión de presets entre ambas.
- Documentación ampliada describiendo filtros interactivos, uso de presets y la nueva pestaña de historial.

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

[1.2.0]: https://github.com/Ulises232/Ulises232-ForgeBuild_Groups_PipeLine_GIT
[1.1.1]: https://github.com/Ulises232/Ulises232-ForgeBuild_Groups_PipeLine_GIT
[1.1.0]: https://github.com/Ulises232/Ulises232-ForgeBuild_Groups_PipeLine_GIT
[1.0.0]: https://github.com/Ulises232/Ulises232-ForgeBuild_Groups_PipeLine_GIT
