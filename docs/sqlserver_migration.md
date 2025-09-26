# Migración del historial de ramas a SQL Server 2019

Este documento describe los pasos para desplegar el nuevo backend en SQL Server 2019 y migrar los datos existentes desde SQLite.

## Preparación

1. **Instalar dependencias** en el entorno donde se ejecuta ForgeBuild:
   ```bash
   pip install -r requirements.txt
   ```
2. **Configurar las credenciales** del servidor en un archivo `.env` (puede copiarse desde `.env.example`):
   ```env
   BRANCH_HISTORY_DB_URL=mssql://usuario:contraseña@servidor:1433/forgebuild
   BRANCH_HISTORY_BACKEND=sqlserver  # opcional cuando se define la URL
   ```
3. Crear en SQL Server una base de datos vacía (por ejemplo, `forgebuild`) con permisos para el usuario configurado.

## Despliegue

Al iniciar la aplicación ForgeBuild con la variable `BRANCH_HISTORY_DB_URL` definida, la capa de persistencia se conectará automáticamente al servidor SQL Server y creará las tablas necesarias (`branches`, `activity_log`, `sprints`, `cards`, `users`, `roles`, `user_roles`). No es necesario mantener archivos SQLite locales ni compartir información vía NAS.

## Migración de datos existentes

Si se necesita traspasar la información histórica del archivo `branches_history.sqlite3`, utilice el script proporcionado:

```bash
python scripts/migrate_branch_history.py --sqlite /ruta/al/branches_history.sqlite3 --url "mssql://usuario:contraseña@servidor:1433/forgebuild"
```

El script ejecutará los siguientes pasos:

- Copia de todas las ramas y su historial de actividad.
- Migración de sprints, tarjetas, usuarios, roles y asignaciones.
- Replicación del log de actividad.

El argumento `--pool-size` permite ajustar el tamaño del pool de conexiones en el destino (valor por defecto: 5).

> **Nota:** El script no elimina ni modifica la información existente en el servidor SQL Server. Si las tablas contienen datos previos, los registros serán actualizados o insertados según corresponda.

## Operación continua

- Con el backend SQL Server activo, todas las operaciones se realizan en línea. Las funciones relacionadas con NAS y modo offline quedan deshabilitadas automáticamente.
- Para regresar temporalmente a SQLite (por ejemplo en entornos de desarrollo aislados) elimine la variable `BRANCH_HISTORY_DB_URL` o establezca `BRANCH_HISTORY_BACKEND=sqlite`.

## Solución de problemas

- Verifique que el puerto 1433 esté accesible desde el host de ForgeBuild.
- Asegúrese de que el usuario de SQL Server tenga permisos `CREATE TABLE`, `CREATE INDEX`, `INSERT`, `UPDATE`, `DELETE` y `SELECT` sobre la base de datos configurada.
- Revise los logs de la aplicación si aparecen errores de conexión o autenticación; el módulo `buildtool.core.branch_history_db` registrará cualquier incidencia durante la inicialización del esquema.
