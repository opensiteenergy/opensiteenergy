#! /bin/bash -l

if ! [ -f ".env" ]; then
    cp .env-template .env
fi

docker compose up -d
docker exec -ti opensiteenergy-build /usr/src/opensiteenergy/server-cli.sh "$@"
#docker exec -ti opensiteenergy-build /bin/bash
docker compose down

