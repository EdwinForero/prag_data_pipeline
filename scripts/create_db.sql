-- (1) Código para crear rol, crear DATABASE y esquemas para el cliente deseado en PostgreSQL (CREATE)

/*
    Conectarse a PostgreSQL por consola
    psql -h <host> -p <puerto> -U <usuario>
        Ej: psql -h localhost -p 5432 -U postgres

    Ejecutar código
    \i script_path
        Ej: \i C:/Users/edwforer/Music/pragma_data_pipeline/scripts/create_db.sql
*/

-- Crear el rol 'pragma_user' si no existe
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pragma_user') THEN
        CREATE ROLE pragma_user LOGIN PASSWORD 'pragma2025*' SUPERUSER;
    END IF;

    ALTER ROLE pragma_user SET TIME ZONE 'America/Bogota';
END $$;

-- Crear database
CREATE DATABASE "pragma_db" WITH OWNER = 'pragma_user' ENCODING = 'UTF8' CONNECTION LIMIT = -1;
ALTER DATABASE "pragma_db" SET TIME ZONE 'America/Bogota';