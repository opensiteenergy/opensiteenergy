#! /bin/bash -l

# Start tileserver-gl

. ./.env

./local-tileserver.sh

# Run simple webserver

echo -e ""
echo -e "\033[1;35m===========================================================\033[0m"
echo -e "\033[1;35m****** OPEN SITE ENERGY - WEB + TILE SERVER RUNNING *******\033[0m"
echo -e "\033[1;35m===========================================================\033[0m"
echo -e ""
echo -e "Open web browser and enter:"
echo -e ""
echo -e "\033[1;36mhttp://localhost:8000/\033[0m"
echo -e ""
echo -e ""

if [ -n "${BUILD_FOLDER+1}" ]; then
    cd "$BUILD_FOLDER"output
else
    cd build/output
fi

python3 -m http.server 
cd ../../

# Stop tileserver-gl

echo "Closing tileserver-gl..."

docker kill opensiteenergy-tileserver
pkill -f tileserver-gl

