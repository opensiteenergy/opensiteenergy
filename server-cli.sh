#!/bin/bash

if [ -f ".env" ]; then
    . ./.env
fi

. venv/bin/activate

./local-tileserver.sh

uvicorn opensiteenergy:app --host 0.0.0.0 --port 8000 --log-level info

COMMAND_NAME="tileserver-gl"

if command -v $COMMAND_NAME >/dev/null 2>&1; then
    echo "Running $COMMAND_NAME locally..."
    
    # Kill existing local instance first

    echo "Killing existing tileserver-gl..."

    pkill -f "$COMMAND_NAME"
    sleep 1

else
    echo "$COMMAND_NAME not found locally. Falling back to Docker..."

    # Kill existing Docker instance first

    echo "Killing existing tileserver-gl..."

    docker kill opensiteenergy-tileserver
fi
