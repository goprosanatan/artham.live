#!/bin/bash

# make script executable: chmod +x scripts/01_day_start.sh

# Load environment variables from .env file
if [ -f .env ]; then
	. .env
    echo ".env file loaded."
fi

# restart Postgres and Redis services for a fresh start
echo "Restarting Postgres and Redis services..."
docker compose restart postgres redis
echo "Postgres and Redis services restarted."

# check if postgres is accepting connections
echo "Checking Postgres connection..."
until docker compose exec -T postgres pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"; do
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

# Set the website URL variable
KITE_URL="https://kite.zerodha.com/connect/login?api_key=$KITE_API_KEY&v=3"

# Print the website URL
echo "Website URL is: $KITE_URL"
# Open the website URL in the default browser (cross-platform)
if [[ "$OSTYPE" == "darwin"* ]]; then
  # macOS
  open "$KITE_URL"
elif [[ "$OSTYPE" == "linux"* ]]; then
  # Linux (Ubuntu, etc.)
  xdg-open "$KITE_URL"
else
  echo "Please open the following URL manually: $KITE_URL"
fi

# Prompt user for request token
read -p "Enter request token: " REQUEST_TOKEN

# connect to redis and store the request token
redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" SET kite:request_token "$REQUEST_TOKEN"
echo "Request token stored in Redis."

# Run the day start python code
python3 01_day_start.py
echo "Python script executed."

echo "Restarting python services..."
# Restart the python services
docker compose restart tick_ingestor tick_store bar_builder bar_store feature_equity feature_option order_broker_adapter order_command_service order_risk_manager order_state_manager order_execution_engine replay_engine replay_bar_builder 
echo "Services restarted."



