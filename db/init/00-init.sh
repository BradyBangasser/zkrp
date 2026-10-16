#!/bin/sh
# Runs once, on first start of an empty Postgres volume.
#
# Two databases with separate roles, so a compromised relay can't touch the
# nullifier set and the auth role can't read it either (see auth.sql).
set -eu

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" \
     -v auth_pw="$AUTH_DB_PASSWORD" -v relay_pw="$RELAY_DB_PASSWORD" <<'SQL'
CREATE ROLE auth  LOGIN PASSWORD :'auth_pw';
CREATE ROLE relay LOGIN PASSWORD :'relay_pw';
CREATE DATABASE auth;
CREATE DATABASE registry;
REVOKE ALL ON DATABASE auth     FROM PUBLIC;
REVOKE ALL ON DATABASE registry FROM PUBLIC;
GRANT CONNECT ON DATABASE auth     TO auth;
GRANT CONNECT ON DATABASE registry TO relay;
SQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname auth     -f /schema/auth.sql
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname registry -f /schema/registry.sql
