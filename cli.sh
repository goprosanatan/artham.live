#!/bin/bash

# make script executable: chmod +x scripts/01_day_start.sh

# Load environment variables from .env file
if [ -f .env ]; then
	. .env
    echo ".env file loaded."
fi

# restart Postgres and Redis services for a fresh start
echo "Restarting Postgres and Redis services..."
docker-compose restart postgres redis
echo "Postgres and Redis services restarted."

# check if postgres is accepting connections
echo "Checking Postgres connection..."
until docker-compose exec -T postgres pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"; do
  echo "Postgres is not ready yet. Waiting..."
  sleep 5
done
echo "Postgres is ready to accept connections."

# check if redis is accepting connections
# COMMAND REQUIRED = brew install redis
echo "Checking Redis connection..."
until redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" ping | grep PONG; do
  echo "Redis is not ready yet. Waiting..."
  sleep 5
done
echo "Redis is ready to accept connections."

docker exec -i artham_00_postgres \
     psql -U postgres -d artham < ./postgres-init/004_create_bar_aggregates.sql
