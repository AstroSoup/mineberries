#!/bin/sh
set -eu

: "${APP_DB_NAME:?APP_DB_NAME is required}"
: "${APP_DB_USER:?APP_DB_USER is required}"
: "${APP_DB_PASSWORD:?APP_DB_PASSWORD is required}"
: "${BOT_DB_NAME:?BOT_DB_NAME is required}"
: "${BOT_DB_USER:?BOT_DB_USER is required}"
: "${BOT_DB_PASSWORD:?BOT_DB_PASSWORD is required}"

psql \
    --set=ON_ERROR_STOP=1 \
    --username "$POSTGRES_USER" \
    --dbname "$POSTGRES_DB" \
    --set=app_db="$APP_DB_NAME" \
    --set=app_user="$APP_DB_USER" \
    --set=app_password="$APP_DB_PASSWORD" \
    --set=bot_db="$BOT_DB_NAME" \
    --set=bot_user="$BOT_DB_USER" \
    --set=bot_password="$BOT_DB_PASSWORD" <<'SQL'
CREATE ROLE :"app_user" LOGIN PASSWORD :'app_password';
CREATE DATABASE :"app_db" OWNER :"app_user";
REVOKE ALL ON DATABASE :"app_db" FROM PUBLIC;
GRANT ALL PRIVILEGES ON DATABASE :"app_db" TO :"app_user";

CREATE ROLE :"bot_user" LOGIN PASSWORD :'bot_password';
CREATE DATABASE :"bot_db" OWNER :"bot_user";
REVOKE ALL ON DATABASE :"bot_db" FROM PUBLIC;
GRANT ALL PRIVILEGES ON DATABASE :"bot_db" TO :"bot_user";
SQL
