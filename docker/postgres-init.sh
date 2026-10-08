#!/bin/sh
set -eu
# Миграции владеют только БД проекта; рабочая роль не является суперпользователем.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
    -v app_password="$APP_DB_PASSWORD" <<'SQL'
CREATE ROLE payments LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD :'app_password';
ALTER DATABASE payments OWNER TO payments;
SQL
