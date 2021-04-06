#!/usr/bin/env bash

TRY_LOOP="20"

: "${AIRFLOW_HOME:="/usr/local/airflow"}"

export AIRFLOW_HOME

# 포트 감지 함수
wait_for_port() {
  local name="$1" host="$2" port="$3"
  local j=0
  while ! nc -z "$host" "$port" >/dev/null 2>&1 < /dev/null; do
    j=$((j+1))
    if [ $j -ge $TRY_LOOP ]; then
      echo >&2 "$(date) - $host:$port still not reachable, giving up"
      exit 1
    fi
    echo "$(date) - waiting for $name... $j/$TRY_LOOP"
    sleep 5
  done
}


if [ -z "$AIRFLOW__CORE__SQL_ALCHEMY_CONN" ]; then
  # 기본 설정
  : "${POSTGRES_HOST:="postgres"}"
  : "${POSTGRES_PORT:="5432"}"
  : "${POSTGRES_USER:="airflow"}"
  : "${POSTGRES_PASSWORD:="airflow"}"
  : "${POSTGRES_DB:="airflow"}"
  : "${POSTGRES_EXTRAS:-""}"

  AIRFLOW__CORE__SQL_ALCHEMY_CONN="postgresql+psycopg2://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}${POSTGRES_EXTRAS}"
  export AIRFLOW__CORE__SQL_ALCHEMY_CONN

  
  if [ "$AIRFLOW__CORE__EXECUTOR" = "CeleryExecutor" ]; then
    AIRFLOW__CELERY__RESULT_BACKEND="db+postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}${POSTGRES_EXTRAS}"
    export AIRFLOW__CELERY__RESULT_BACKEND
  fi
else
  if [[ "$AIRFLOW__CORE__EXECUTOR" == "CeleryExecutor" && -z "$AIRFLOW__CELERY__RESULT_BACKEND" ]]; then
    >&2 printf '%s\n' "FATAL: if you set AIRFLOW__CORE__SQL_ALCHEMY_CONN manually with CeleryExecutor you must also set AIRFLOW__CELERY__RESULT_BACKEND"
    exit 1
  fi

  # Derive useful variables from the AIRFLOW__ variables provided explicitly by the user
  POSTGRES_ENDPOINT=$(echo -n "$AIRFLOW__CORE__SQL_ALCHEMY_CONN" | cut -d '/' -f3 | sed -e 's,.*@,,')
  POSTGRES_HOST=$(echo -n "$POSTGRES_ENDPOINT" | cut -d ':' -f1)
  POSTGRES_PORT=$(echo -n "$POSTGRES_ENDPOINT" | cut -d ':' -f2)
fi

wait_for_port "Postgres" "$POSTGRES_HOST" "$POSTGRES_PORT"

if [ -z "$AIRFLOW__CELERY__BROKER_URL" ]; then
  # 기본 설정
  : "${REDIS_PROTO:="redis://"}"
  : "${REDIS_HOST:="redis"}"
  : "${REDIS_PORT:="6379"}"
  : "${REDIS_PASSWORD:=""}"
  : "${REDIS_DBNUM:="1"}"

  # 패스워드 설정시
  if [ -n "$REDIS_PASSWORD" ]; then
    REDIS_PREFIX=":${REDIS_PASSWORD}@"
  else
    REDIS_PREFIX=
  fi

  AIRFLOW__CELERY__BROKER_URL="${REDIS_PROTO}${REDIS_PREFIX}${REDIS_HOST}:${REDIS_PORT}/${REDIS_DBNUM}"
  export AIRFLOW__CELERY__BROKER_URL
else
  REDIS_ENDPOINT=$(echo -n "$AIRFLOW__CELERY__BROKER_URL" | cut -d '/' -f3 | sed -e 's,.*@,,')
  REDIS_HOST=$(echo -n "$POSTGRES_ENDPOINT" | cut -d ':' -f1)
  REDIS_PORT=$(echo -n "$POSTGRES_ENDPOINT" | cut -d ':' -f2)
fi
wait_for_port "Redis" "$REDIS_HOST" "$REDIS_PORT"

case "$1" in
  webserver)
    airflow db init
    airflow users create \
    --username admin \
    --firstname Peter \
    --lastname Parker \
    --role Admin \
    --email ehd5538@gmail.com \
    --password admin

    exec airflow webserver
    ;;
  scheduler)
    sleep 20
    exec airflow "$@"
    ;;
  worker)
    sleep 20
    exec airflow celery "$@"
    ;;
  *)
    exec "$@"
    ;;
esac
