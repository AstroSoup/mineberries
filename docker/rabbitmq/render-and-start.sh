#!/bin/sh
set -eu

: "${RABBITMQ_VHOST:?RABBITMQ_VHOST is required}"
: "${RABBITMQ_ADMIN_USER:?RABBITMQ_ADMIN_USER is required}"
: "${RABBITMQ_ADMIN_PASSWORD:?RABBITMQ_ADMIN_PASSWORD is required}"
: "${RABBITMQ_APP_USER:?RABBITMQ_APP_USER is required}"
: "${RABBITMQ_APP_PASSWORD:?RABBITMQ_APP_PASSWORD is required}"
: "${RABBITMQ_BOT_USER:?RABBITMQ_BOT_USER is required}"
: "${RABBITMQ_BOT_PASSWORD:?RABBITMQ_BOT_PASSWORD is required}"

definitions=/tmp/mineberries-definitions.json

umask 077
perl /opt/mineberries/render-definitions.pl \
    < /opt/mineberries/definitions.template.json \
    > "$definitions"
chown rabbitmq:rabbitmq "$definitions"

exec /usr/local/bin/docker-entrypoint.sh "$@"
