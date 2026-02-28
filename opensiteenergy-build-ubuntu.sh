#!/bin/bash

sudo timedatectl set-timezone Europe/London

# General function for checking whether services are running

function is_in_activation {
   activation=$(/sbin/service "$1" status | grep "active (running)" )
   if [ -z "$activation" ]; then
      true;
   else
      echo "Running"
      false;
   fi

   return $?;
}

function port_listening {
    if nc -z 127.0.0.1 "$1" >/dev/null ; then
        true;
    else
        false;
    fi

    return $?;
}

# Check whether installation has already been completed before

if [ -f "/usr/src/opensiteenergy/INSTALLCOMPLETE" ]; then
   exit 0
fi


# Query user to set up server login credentials early on
# Ideally these values are set through Terraform apply

if [ -f "/tmp/.env" ]; then
    . /tmp/.env
fi

if [ -z "${ADMIN_USERNAME}" ] || [ -z "${ADMIN_PASSWORD}" ]; then
   echo "Enter username for logging into server:"
   read ADMIN_USERNAME
   echo "Enter password for logging into server:"
   stty -echo
   read ADMIN_PASSWORD
   stty echo
fi

# Set up large swap space

sudo fallocate -l 16G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab


# Set up general directories for Open Site Energy application

mkdir /usr/src
mkdir /usr/src/opensiteenergy

echo '' >> /usr/src/opensiteenergy/opensiteenergy.log
echo '========= STARTING SOFTWARE INSTALLATION =========' >> /usr/src/opensiteenergy/opensiteenergy.log


# Run lengthy apt-get update

echo '' >> /usr/src/opensiteenergy/opensiteenergy.log
echo '********* STAGE 1: Running initial apt update **********' >> /usr/src/opensiteenergy/opensiteenergy.log

sudo apt update -y | tee -a /usr/src/opensiteenergy/opensiteenergy.log

echo '********* STAGE 1: Finished running initial apt update **********' >> /usr/src/opensiteenergy/opensiteenergy.log


# Quickly install nginx so user has something to see that updates them with progress
# During install, secure all access with simple HTTP authentication

echo '' >> /usr/src/opensiteenergy/opensiteenergy.log
echo '********* STAGE 2: Installing nginx **********' >> /usr/src/opensiteenergy/opensiteenergy.log

mkdir /var/www
mkdir /var/www/html
echo '<!doctype html><html><head><meta http-equiv="refresh" content="2"></head><body><pre>Beginning installation of Open Site Energy...</pre></body></html>' | sudo tee /var/www/html/index.nginx-debian.html
sudo apt install nginx certbot python3-certbot-nginx -y
echo "${ADMIN_USERNAME}:$(openssl passwd -6 "${ADMIN_PASSWORD}")" | sudo tee /etc/nginx/.htpasswd > /dev/null
sudo chown www-data:www-data /etc/nginx/.htpasswd
sudo chmod 600 /etc/nginx/.htpasswd
sudo tee /etc/nginx/sites-available/default <<EOF
server {
    listen 80 default_server;
    listen [::]:80 default_server;

    root /var/www/html;
    index index.html index.htm index.nginx-debian.html;

    server_name _;

    auth_basic "Restricted Area";
    auth_basic_user_file /etc/nginx/.htpasswd;

    location / {
        try_files \$uri \$uri/ =404;
    }
}
EOF

sudo nginx -t && sudo systemctl reload nginx

echo '********* STAGE 2: Finished installing nginx **********' >> /usr/src/opensiteenergy/opensiteenergy.log


# Install git

echo '' >> /usr/src/opensiteenergy/opensiteenergy.log
echo '********* STAGE 3: Installing git **********' >> /usr/src/opensiteenergy/opensiteenergy.log

echo '<!doctype html><html><head><meta http-equiv="refresh" content="2"></head><body><pre>Installing git...</pre></body></html>' | sudo tee /var/www/html/index.nginx-debian.html
sudo apt install git -y | tee -a /usr/src/opensiteenergy/opensiteenergy.log

echo '********* STAGE 3: Finished installing git **********' >> /usr/src/opensiteenergy/opensiteenergy.log


# Install Open Site Energy so log file in right place

echo '' >> /usr/src/opensiteenergy/opensiteenergy.log
echo '********* STAGE 4: Installing Open Site Energy source code **********' >> /usr/src/opensiteenergy/opensiteenergy.log

echo '<!doctype html><html><head><meta http-equiv="refresh" content="2"></head><body><pre>Cloning Open Site Energy GitHub repo...</pre></body></html>' | sudo tee /var/www/html/index.nginx-debian.html
sudo rm -R /usr/src/opensiteenergy
cd /usr/src
git clone https://github.com/SH801/opensiteenergy.git opensiteenergy

echo '********* STAGE 4: Finished installing Open Site Energy source code **********' >> /usr/src/opensiteenergy/opensiteenergy.log


echo '********* STAGE 5: Installing nodejs and frontail **********' >> /usr/src/opensiteenergy/opensiteenergy.log

echo '<!doctype html><html><head><meta http-equiv="refresh" content="2"></head><body><pre>Installing frontail to show install logs dynamically...</pre></body></html>' | sudo tee /var/www/html/index.nginx-debian.html

sudo apt update -y | tee -a /usr/src/opensiteenergy/opensiteenergy.log
sudo apt install curl -y | tee -a /usr/src/opensiteenergy/log.txt
curl -fsSL https://deb.nodesource.com/setup_22.x | sudo -E bash -
sudo apt install netcat-traditional nodejs -y | tee -a /usr/src/opensiteenergy/log.txt
sudo npm i frontail -g 2>&1 | tee -a /usr/src/opensiteenergy/opensiteenergy.log

echo "[Unit]
Description=frontail.service
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/usr/src/opensiteenergy
ExecStart=frontail /usr/src/opensiteenergy/opensiteenergy.log --lines 32000 --ui-hide-topbar --url-path /logs
Restart=on-failure

[Install]
WantedBy=multi-user.target

" | sudo tee /etc/systemd/system/frontail.service >/dev/null

sudo systemctl enable frontail.service
sudo systemctl restart frontail.service

sudo cp /usr/src/opensiteenergy/nginx/001-opensiteenergy-live.conf /etc/nginx/sites-available/.
sudo cp /usr/src/opensiteenergy/nginx/002-opensiteenergy-install.conf /etc/nginx/sites-available/.

while is_in_activation frontail ; do true; done

echo '********* frontail service running **********' >> /usr/src/opensiteenergy/opensiteenergy.log

while ! port_listening 9001 ; do true; done

echo '********* frontail service listening on port 9001 **********' >> /usr/src/opensiteenergy/opensiteenergy.log

echo '<!doctype html>
<html>
<head>
    <title>Installing Open Site Energy...</title>
    <meta http-equiv="refresh" content="15; url=/">
</head>
<body>
    <pre>Installation in progress...</pre>
    <iframe src="/logs" style="width:100%; height:90vh; border:none;"></iframe>
</body>
</html>' | sudo tee /var/www/html/index.html

sudo ln -s /etc/nginx/sites-available/002-opensiteenergy-install.conf /etc/nginx/sites-enabled/
sudo rm -f /etc/nginx/sites-enabled/default
sudo /usr/sbin/nginx -s reload

echo '********* STAGE 5: Finished installing nodejs, npm and frontail **********' >> /usr/src/opensiteenergy/opensiteenergy.log


# Install general tools and required libraries

echo '' >> /usr/src/opensiteenergy/opensiteenergy.log
echo '********* STAGE 6: Installing general tools and required libraries **********' >> /usr/src/opensiteenergy/opensiteenergy.log

sudo apt install virtualenv pip libgdal-dev -y | tee -a /usr/src/opensiteenergy/opensiteenergy.log
virtualenv -p /usr/bin/python3 /usr/src/opensiteenergy/venv | tee -a /usr/src/opensiteenergy/opensiteenergy.log
source /usr/src/opensiteenergy/venv/bin/activate
python3 -m pip install -U pip | tee -a /usr/src/opensiteenergy/opensiteenergy.log
python3 -m pip install -U setuptools wheel twine check-wheel-contents | tee -a /usr/src/opensiteenergy/opensiteenergy.log
cd opensiteenergy
pip install gdal==`gdal-config --version` | tee -a /usr/src/opensiteenergy/opensiteenergy.log
pip install -r requirements.txt | tee -a /usr/src/opensiteenergy/opensiteenergy.log
cd ..
cp /usr/src/opensiteenergy/.env-template /usr/src/opensiteenergy/.env
sudo echo "
ADMIN_USERNAME=${ADMIN_USERNAME}
ADMIN_PASSWORD=${ADMIN_PASSWORD}
" >> /usr/src/opensiteenergy/.env
sudo chown -R www-data:www-data /usr/src/opensiteenergy
sudo sed -i "s/.*TILESERVER_URL.*/TILESERVER_URL\=\/tiles/" /usr/src/opensiteenergy/.env

echo "[Unit]
Description=opensiteenergy-servicesmanager.service
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/usr/src/opensiteenergy
ExecStart=/usr/src/opensiteenergy/opensiteenergy-servicesmanager.sh
Restart=on-failure

[Install]
WantedBy=multi-user.target

" | sudo tee /etc/systemd/system/opensiteenergy-servicesmanager.service >/dev/null

sudo systemctl enable opensiteenergy-servicesmanager.service
sudo systemctl start opensiteenergy-servicesmanager.service

sudo NEEDRESTART_MODE=a apt install gnupg software-properties-common cmake make g++ dpkg build-essential autoconf pkg-config -y | tee -a /usr/src/opensiteenergy/opensiteenergy.log
sudo NEEDRESTART_MODE=a apt install libbz2-dev libpq-dev libboost-all-dev libgeos-dev libtiff-dev libspatialite-dev -y | tee -a /usr/src/opensiteenergy/opensiteenergy.log
sudo NEEDRESTART_MODE=a apt install libsqlite3-dev libcurl4-gnutls-dev liblua5.4-dev rapidjson-dev libshp-dev libgdal-dev gdal-bin -y | tee -a /usr/src/opensiteenergy/opensiteenergy.log
sudo NEEDRESTART_MODE=a apt install zip unzip lua5.4 shapelib ca-certificates curl nano wget pip proj-bin spatialite-bin sqlite3 -y | tee -a /usr/src/opensiteenergy/opensiteenergy.log
sudo NEEDRESTART_MODE=a apt install xvfb libglfw3-dev libuv1-dev libjpeg-turbo8 libcairo2-dev -y | tee -a /usr/src/opensiteenergy/opensiteenergy.log
sudo NEEDRESTART_MODE=a apt install libpango1.0-dev libjpeg-dev libgif-dev librsvg2-dev gir1.2-rsvg-2.0 librsvg2-2 librsvg2-common -y | tee -a /usr/src/opensiteenergy/opensiteenergy.log
sudo NEEDRESTART_MODE=a apt install libcurl4-openssl-dev libpixman-1-dev libpixman-1-0 ccache cmake ninja-build pkg-config xvfb -y | tee -a /usr/src/opensiteenergy/opensiteenergy.log
sudo NEEDRESTART_MODE=a apt install libc++-dev libc++abi-dev libpng-dev -y | tee -a /usr/src/opensiteenergy/opensiteenergy.log
sudo NEEDRESTART_MODE=a apt install libgl1-mesa-dev libgl1-mesa-dri libjpeg-dev -y | tee -a /usr/src/opensiteenergy/opensiteenergy.log
sudo NEEDRESTART_MODE=a apt install qgis qgis-plugin-grass -y | tee -a /usr/src/opensiteenergy/opensiteenergy.log
sudo NEEDRESTART_MODE=a apt install screen -y | tee -a /usr/src/opensiteenergy/opensiteenergy.log

sudo apt update -y | tee -a /usr/src/opensiteenergy/opensiteenergy.log

echo '********* STAGE 6: Finished installing general tools and required libraries **********' >> /usr/src/opensiteenergy/opensiteenergy.log


# Install tileserver-gl

echo '' >> /usr/src/opensiteenergy/opensiteenergy.log
echo '********* STAGE 7: Installing tileserver-gl as system daemon **********' >> /usr/src/opensiteenergy/opensiteenergy.log

# Install icu4-70
wget https://github.com/unicode-org/icu/releases/download/release-70-rc/icu4c-70rc-src.tgz | tee -a /usr/src/opensiteenergy/opensiteenergy.log
tar -xvf icu4c-70rc-src.tgz
sudo rm icu4c-70rc-src.tgz
cd icu/source
./configure --prefix=/opt | tee -a /usr/src/opensiteenergy/opensiteenergy.log
make -j | tee -a /usr/src/opensiteenergy/opensiteenergy.log
sudo make install | tee -a /usr/src/opensiteenergy/opensiteenergy.log

# Install tileserver-gl@5.4.0
# NOTE: 5.5.0 doesn't work
sudo env "PATH=$PATH" "LD_LIBRARY_PATH=/opt/lib" "PKG_CONFIG_PATH=/opt/lib/pkgconfig" npm install -g tileserver-gl@5.4.0 --unsafe-perm 2>&1 | tee -a /usr/src/opensiteenergy/opensiteenergy.log

echo "
[Unit]
Description=TileServer GL
After=network.target

[Service]
Type=simple
User=www-data
WorkingDirectory=/usr/src/opensiteenergy
ExecStart=/usr/src/opensiteenergy/server-tileserver.sh
Restart=on-failure
Environment=PORT=8080
Environment=NODE_ENV=production

[Install]
WantedBy=multi-user.target

StandardOutput=file:/var/log/tileserver-output.log
StandardError=file:/var/log/tileserver-error.log
"  | sudo tee /etc/systemd/system/tileserver.service >/dev/null

sudo /usr/bin/systemctl enable tileserver.service

echo '********* STAGE 7: Finished installing tileserver-gl as system daemon **********' >> /usr/src/opensiteenergy/opensiteenergy.log


# Install tilemaker

echo '' >> /usr/src/opensiteenergy/opensiteenergy.log
echo '********* STAGE 8: Installing tilemaker **********' >> /usr/src/opensiteenergy/opensiteenergy.log

cd /usr/src/opensiteenergy
git clone https://github.com/systemed/tilemaker.git | tee -a /usr/src/opensiteenergy/opensiteenergy.log
cd tilemaker
make -j | tee -a /usr/src/opensiteenergy/opensiteenergy.log
sudo make install | tee -a /usr/src/opensiteenergy/opensiteenergy.log

echo '********* STAGE 8: Finished installing tilemaker **********' >> /usr/src/opensiteenergy/opensiteenergy.log


# Install tippecanoe

echo '' >> /usr/src/opensiteenergy/opensiteenergy.log
echo '********* STAGE 9: Installing tippecanoe **********' >> /usr/src/opensiteenergy/opensiteenergy.log

cd /usr/src/opensiteenergy
git clone https://github.com/felt/tippecanoe.git | tee -a /usr/src/opensiteenergy/opensiteenergy.log
cd tippecanoe
make -j | tee -a /usr/src/opensiteenergy/opensiteenergy.log
sudo make install | tee -a /usr/src/opensiteenergy/opensiteenergy.log

echo '********* STAGE 9: Finished installing tippecanoe **********' >> /usr/src/opensiteenergy/opensiteenergy.log


# Install postgis

echo '' >> /usr/src/opensiteenergy/opensiteenergy.log
echo '********* STAGE 10: Installing PostGIS **********' >> /usr/src/opensiteenergy/opensiteenergy.log

sudo apt update -y | tee -a /usr/src/opensiteenergy/opensiteenergy.log
sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
sudo curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo gpg --dearmor -o /etc/apt/trusted.gpg.d/postgresql.gpg
sudo apt update -y | tee -a /usr/src/opensiteenergy/opensiteenergy.log
sudo NEEDRESTART_MODE=a apt install postgresql-postgis -y | tee -a /usr/src/opensiteenergy/opensiteenergy.log

echo '********* STAGE 10: Finished installing PostGIS  **********' >> /usr/src/opensiteenergy/opensiteenergy.log


# Install Open Site Energy application

echo '' >> /usr/src/opensiteenergy/opensiteenergy.log
echo '********* STAGE 12: Installing Open Site Energy **********' >> /usr/src/opensiteenergy/opensiteenergy.log
cd /usr/src/opensiteenergy
pip3 install gdal==`gdal-config --version` | tee -a /usr/src/opensiteenergy/opensiteenergy.log
pip3 install -r requirements.txt | tee -a /usr/src/opensiteenergy/opensiteenergy.log
pip3 install git+https://github.com/hotosm/osm-export-tool-python --no-deps | tee -a /usr/src/opensiteenergy/opensiteenergy.log
sudo service postgresql restart | tee -a /usr/src/opensiteenergy/opensiteenergy.log
sudo -u postgres psql -c "CREATE ROLE opensite WITH LOGIN PASSWORD 'password';" | tee -a /usr/src/opensiteenergy/opensiteenergy.log
sudo -u postgres createdb -O opensite opensite | tee -a /usr/src/opensiteenergy/opensiteenergy.log
sudo -u postgres psql -d opensite -c 'CREATE EXTENSION postgis;' | tee -a /usr/src/opensiteenergy/opensiteenergy.log
sudo -u postgres psql -d opensite -c 'CREATE EXTENSION postgis_raster;' | tee -a /usr/src/opensiteenergy/opensiteenergy.log
sudo -u postgres psql -d opensite -c 'GRANT ALL PRIVILEGES ON DATABASE opensite TO opensite;' | tee -a /usr/src/opensiteenergy/opensiteenergy.log

echo "[Unit]
Description=opensiteenergy.service
After=network.target postgresql.service

[Service]
CPUWeight=1000
Type=simple
User=www-data
WorkingDirectory=/usr/src/opensiteenergy
Environment="PATH=/usr/src/opensiteenergy/venv/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
ExecStart=/usr/src/opensiteenergy/venv/bin/uvicorn opensiteenergy:app --host 0.0.0.0 --port 8000 --log-level info
KillMode=mixed
TimeoutStopSec=30s
Restart=always

[Install]
WantedBy=multi-user.target

" | sudo tee /etc/systemd/system/opensiteenergy.service >/dev/null

sudo systemctl enable opensiteenergy.service
sudo systemctl start opensiteenergy.service

if [ -f "/tmp/.env" ]; then
    rm /tmp/.env
fi

while is_in_activation opensiteenergy ; do true; done

echo '********* opensiteenergy service running **********' >> /usr/src/opensiteenergy/opensiteenergy.log

while ! port_listening 8000 ; do true; done

echo '********* opensiteenergy service listening on port 8000 **********' >> /usr/src/opensiteenergy/opensiteenergy.log

sudo systemctl stop frontail.service
sudo systemctl disable frontail.service
sudo ln -s /etc/nginx/sites-available/001-opensiteenergy-live.conf /etc/nginx/sites-enabled/
sudo rm -f /etc/nginx/sites-enabled/002-opensiteenergy-install.conf
sudo /usr/sbin/nginx -s reload

echo 'FINISHED' >> /usr/src/opensiteenergy/INSTALLCOMPLETE

echo '********* STAGE 12: Finished installing Open Site Energy **********' >> /usr/src/opensiteenergy/opensiteenergy.log

echo '' >> /usr/src/opensiteenergy/opensiteenergy.log
echo '===================================================' >> /usr/src/opensiteenergy/opensiteenergy.log
echo '========= STARTUP INSTALLATION COMPLETE ===========' >> /usr/src/opensiteenergy/opensiteenergy.log
echo '===================================================' >> /usr/src/opensiteenergy/opensiteenergy.log
echo '' >> /usr/src/opensiteenergy/opensiteenergy.log



