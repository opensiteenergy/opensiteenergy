#! /bin/bash -l

. ./.env

COMMAND_NAME="tileserver-gl"

if command -v $COMMAND_NAME >/dev/null 2>&1; then
    echo "Running $COMMAND_NAME locally..."
    
    # Kill existing local instance first

    echo "Killing existing tileserver-gl..."

    pkill -f "$COMMAND_NAME"
    sleep 1

    echo "Running tileserver-gl..."

    export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES

    if [ -n "${BUILD_FOLDER+1}" ]; then
        $COMMAND_NAME -p 8080 --public_url ${TILESERVER_URL} --config "$BUILD_FOLDER"tileserver-live/config.json > tileserver.log 2>&1 &
    else
        $COMMAND_NAME -p 8080 --public_url ${TILESERVER_URL} --config build/tileserver-live/config.json > tileserver.log 2>&1 &
    fi

else
    echo "$COMMAND_NAME not found locally. Falling back to Docker..."

    # Kill existing Docker instance first

    echo "Killing existing tileserver-gl..."

    docker kill opensiteenergy-tileserver

    echo "Running tileserver-gl..."

    if [ -n "${BUILD_FOLDER+1}" ]; then
        docker run --name opensiteenergy-tileserver -d --rm -v "$BUILD_FOLDER"tileserver-live/:/data -p 8080:8080 --public_url ${TILESERVER_URL} maptiler/tileserver-gl --config config.json
    else
        docker run --name opensiteenergy-tileserver -d --rm -v $(pwd)/build/tileserver-live/:/data -p 8080:8080 --public_url ${TILESERVER_URL} maptiler/tileserver-gl --config config.json
    fi
fi