# Importación masiva de tarjetas

La vista **Tarjetas** dentro de la planeación de sprints permite cargar varias tarjetas a la vez desde archivos CSV o Excel. Además, desde la misma pantalla puedes descargar una plantilla vacía para preparar la información con los campos correctos.

## Campos esperados

Los archivos deben incluir los siguientes encabezados (en cualquier orden). El nombre puede escribirse con o sin acentos; la aplicación los normaliza automáticamente.

| Columna | Obligatorio | Descripción |
| --- | --- | --- |
| `Grupo` | Sí | Clave del grupo al que pertenece la tarjeta. Debe coincidir con el grupo registrado en el catálogo de empresas. |
| `Empresa` | Sí | Nombre de la empresa asociada. Debe existir previamente en el catálogo. |
| `Ticket` | Sí | Identificador del ticket o folio. Se utiliza para actualizar tarjetas existentes con el mismo grupo y ticket. |
| `Título` | Sí | Descripción corta de la tarjeta. |
| `Desarrollador (opcional)` | No | Usuario responsable del desarrollo. Si el valor no coincide con un usuario registrado se conserva tal cual para que pueda asignarse más adelante. |
| `QA (opcional)` | No | Usuario responsable de QA. |

Las filas completamente vacías se ignoran durante la importación.

## Formatos compatibles

- **CSV** (`.csv`): se detecta automáticamente el delimitador, por lo que funciona con comas, punto y coma o tabuladores.
- **Excel** (`.xlsx` o `.xlsm`): se toma la primera hoja del archivo.

Si intentas cargar un formato distinto la aplicación mostrará un mensaje indicando los tipos soportados.

## Descarga de la plantilla

1. Abre la pestaña **Tarjetas** dentro de *Planeación de Sprints*.
2. Haz clic en **Descargar plantilla**.
3. Selecciona la carpeta de destino y el tipo de archivo (CSV o Excel).

La plantilla únicamente contiene la fila de encabezados para que captures los datos requeridos.

## Proceso de importación

1. Prepara el archivo con la información de las tarjetas siguiendo los encabezados anteriores.
2. Desde la pestaña **Tarjetas**, presiona **Importar tarjetas**.
3. Selecciona el archivo CSV/Excel.
4. Revisa el resumen mostrado al finalizar:
   - Número de tarjetas creadas.
   - Número de tarjetas actualizadas (se usa el mismo ticket y grupo para identificar coincidencias).
   - Filas omitidas o con errores, incluyendo la fila y la causa.

Solo los usuarios con rol de líder pueden importar o crear tarjetas masivamente. Cualquier usuario puede descargar la plantilla.

## Consideraciones adicionales

- Las tarjetas nuevas se crean sin sprint asignado y con estado **Pendiente**.
- Si una fila hace referencia a una empresa inexistente se reporta en el resumen y no se crea la tarjeta correspondiente.
- Los responsables de Desarrollo y QA se guardan tal como aparecen en el archivo, incluso si todavía no existen en el catálogo de usuarios.
- Los archivos CSV guardados en codificaciones comunes (UTF-8, Latin-1/CP1252) se leen automáticamente. Si el archivo utiliza otra codificación, ábrelo en tu editor y vuelve a guardarlo como UTF-8.

