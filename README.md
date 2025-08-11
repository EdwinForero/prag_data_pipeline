# Pragma Data Pipeline

Este repositorio contiene un pipeline incremental para carga de archivos CSV en micro batches, almacenamiento en PostgreSQL y actualización de estadísticas con SQLModel y Polars.

---
## Requisitos previos

- Python 3.10+
- PostgreSQL instalado y corriendo en tu máquina

---
## Configuración del entorno
### 1. Archivo .env

Debes crear el archivo <i>.env</i> (o <i>.env.local</i> si el entorno de ejecución es Windows) en la raíz del proyecto con los siguientes datos:

```
DATABASE_USER=pragma_user
DATABASE_PASSWORD=pragma_pass
DATABASE_HOST=localhost
DATABASE_PORT=5432
DATABASE_NAME=pragma_db
```

Estos valores permiten conectar con la base de datos definida en el pipeline.

### 2. Crear la base de datos y el rol en PostgreSQL

Antes de ejecutar el pipeline, asegúrate de que la base de datos y el usuario existen en PostgreSQL. Puedes usar el script <i>scripts/create_db.sql</i>:

- Conéctate a PostgreSQL como superusuario.

- Ejecuta el script:
  ```
  psql -U postgres -h localhost -p 5432
  \i /ruta/al/script/create_db.sql
  ```

Esto creará el rol <i>pragma_user</i> y la base de datos <i>pragma_db</i> con los permisos correctos.

### 3. Instalación de dependencias

Instala los paquetes necesarios usando el archivo requirements.txt:

```
pip install -r requirements.txt
```

Si usas un entorno virtual (<i>venv</i>), actívalo antes de instalar.

---
## Uso del pipeline

#### 1. Datos de entrada:

* Agrega los archivos <i>.csv</i> que deseas procesar en la carpeta <i>data</i> de tu proyecto.


#### 2. Configura el main:

* Asegúrate que los nombres de los archivos a procesar están definidos correctamente en el script principal (por ejemplo, <i>main_test.py</i> o el archivo que uses como entry point).


  Ejemplo de definición:

  ```
  files = [
    "2012-1.csv",
    "2012-2.csv",
    "2012-3.csv",
    "2012-4.csv",
    "2012-5.csv"
  ]
  ```

Los archivos deben existir dentro de la carpeta data.

### 3. Corre el pipeline:

  ```
  python main_test.py
  ```

---
## Notas adicionales

El pipeline inicializará la conexión, creará las tablas necesarias (si no existen) y procesará los archivos que hayas definido en la lista del main.

Puedes ajustar el modo de actualización de estadísticas (<i>row</i> o <i>file</i>) directamente en el main.

Para limpiar datos o reiniciar la tabla antes de una nueva ejecución puedes usar el método <i>truncate_table()</i> que está dispuesto en los utilitarios del proyecto.
