# Installation

## Docker install

Download and install **Docker Desktop** from https://www.docker.com/. 

Then go to Docker **Settings** -> **Resources** and set **Memory Limit** to `12 GB` (minimum system requirement).

You will also need to increase Docker's 'Swap Memory':

- Locate Docker config file using instructions at https://docs.docker.com/desktop/settings-and-maintenance/settings/
- Open Docker config file and set `SwapMiB` to `10000`.
- Save Docker config file and restart Docker for new `SwapMiB` setting to take effect.

Before running a Docker install, you should customize Docker settings, eg. login details or location of `build` folder, by copying `.env-template` to `.env` and editing `.env`:

```
git clone https://github.com/opensiteenergy/opensiteenergy.git
cd opensiteenergy 
cp .env-template .env
nano .env
```

To install and run a Docker version of Open Site Energy server, enter:

```
./server-docker.sh
```

Once the server application is running, load `http://localhost:8000` in web browser:

![Open Site Energy - Login](/images/opensiteenergy-admin-login.png)

Enter default username: `admin`, password: `password` or your custom username/password if you have modified `.env`.

To run Open Site Energy command line tool (non-server version) with Docker, enter instead:

```
./build-docker.sh [configuration]
```


## Native install

The following software needs to be installed to run Open Site Energy natively, ie. without Docker:

- [GDAL](https://gdal.org): For transferring data in and out of PostGIS and general GIS operations.

- Open Site Energy Python libraries: Some of these libraries require `GDAL`.

- [PostGIS](https://postgis.net/): For storing and processing GIS data.

- [QGIS](https://qgis.org/): For generating QGIS files.

- [tilemaker](https://github.com/systemed/tilemaker): For generating mbtiles version of OpenStreetMap for use as background map within MapLibre-GL.

- [tippecanoe](https://github.com/felt/tippecanoe): For generating optimized mbtiles versions of data layers for MapLibre-GL.

- [tileserver-gl](https://tileserver.readthedocs.io/en/latest/installation.html): For serving mbtiles files that can be viewed in a web browser. 

Instructions for installing all the required software is provided below for both Linux Ubuntu and Mac OS platforms. 

For Windows platforms, install [`Multipass`](https://canonical.com/multipass/install) and create an `Ubuntu 24.04` instance by entering:

```
multipass launch noble --name opensiteenergy --cpus 4 --memory 12G --disk 250G
multipass shell opensiteenergy 
```

### 1. Initial setup (includes install of GDAL and Open Site Energy Python libraries)

Install core libraries (including GDAL) required for subsequent installations:

```
# Ubuntu
sudo apt install gnupg software-properties-common cmake make g++ dpkg build-essential autoconf pkg-config -y
sudo apt install libbz2-dev libpq-dev libboost-all-dev libgeos-dev libtiff-dev libspatialite-dev -y
sudo apt install libsqlite3-dev libcurl4-gnutls-dev liblua5.4-dev rapidjson-dev libshp-dev -y
sudo apt install zip unzip lua5.4 shapelib ca-certificates curl nano wget pip virtualenv proj-bin spatialite-bin sqlite3 -y
sudo apt install xvfb libglfw3-dev libuv1-dev libjpeg-turbo8 libcairo2-dev -y
sudo apt install libpango1.0-dev libjpeg-dev libgif-dev librsvg2-dev gir1.2-rsvg-2.0 librsvg2-2 librsvg2-common -y
sudo apt install libcurl4-openssl-dev libpixman-1-dev libpixman-1-0 ccache cmake ninja-build pkg-config -y
sudo apt install libc++-dev libc++abi-dev libpng-dev -y
sudo apt install libgl1-mesa-dev libgl1-mesa-dri libjpeg-dev -y
sudo apt install libgdal-dev gdal-bin python3-gdal -y
sudo apt install chromium-browser chromium-chromedriver -y

# Mac
brew install cmake make geos git libpq libtiff libspatialite lua rapidjson shapelib sqlite curl proj virtualenv gdal chromium chromedriver
xattr -cr /Applications/Chromium.app
xattr -d com.apple.quarantine $(which chromedriver)
```

*Note: `chromium` and `chromedriver` are needed to automate browser interaction when downloading some datasets.*

Clone the Open Site Energy repository and copy `.env-template` to `.env`:

```
git clone https://github.com/opensiteenergy/opensiteenergy.git
cd opensiteenergy 
cp .env-template .env
```

You will need to edit the `.env` file during the installation process to incorporate custom settings, eg. your `PostGIS` user's password.

Create a Python virtual environment and install the Open Site Energy Python libraries:

```
$(which python3) -m venv venv && source venv/bin/activate
python3 -m pip install -U pip
python3 -m pip install -U setuptools wheel twine check-wheel-contents
python3 -m pip install gdal==`gdal-config --version`
python3 -m pip install -r requirements.txt
python3 -m pip install git+https://github.com/hotosm/osm-export-tool-python --no-deps
python3 -m pip install git+https://github.com/opensiteenergy/openlibrary.git
```

### 2. PostGIS installation

Install PostGIS by entering:

```
# Ubuntu
sudo apt update -y
sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
sudo curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo gpg --dearmor -o /etc/apt/trusted.gpg.d/postgresql.gpg
sudo apt update -y
sudo apt install postgresql-postgis -y

# Mac
brew update
brew install postgresql
brew install postgis
brew services start postgresql
```

Create the Open Site Energy database and user by entering:
```
sudo -u postgres createuser -P opensiteenergy
sudo -u postgres createdb -O opensiteenergy opensiteenergy
sudo -u postgres psql -d opensiteenergy -c 'CREATE EXTENSION postgis;'
sudo -u postgres psql -d opensiteenergy -c 'CREATE EXTENSION postgis_raster;'
sudo -u postgres psql -d opensiteenergy -c 'GRANT ALL PRIVILEGES ON DATABASE opensiteenergy TO opensiteenergy;'
```

When prompted for a password, enter a secure password but ensure you change the `POSTGRES_PASSWORD` variable in `.env` to use this value.

The Open Site Energy toolkit requires access to PostGIS using a standard `md5` password. You will therefore need to edit PostgreSQL's `pg_hba.conf` file to allow `md5` password access:

```
sudo nano /etc/postgresql/[REPLACE WITH POSTGRES VERSION]/main/pg_hba.conf
```

Scroll to bottom of `pg_hba.conf` and edit row containing `local all all ` so it's set to `md5`:

```
...
# DO NOT DISABLE!
# If you change this first entry you will need to make sure that the
# database superuser can access the database using some other method.
# Noninteractive access to all databases is required during automatic
# maintenance (custom daily cronjobs, replication, and similar tasks).
#
# Database administrative login by Unix domain socket
local   all             postgres                                trust

# TYPE  DATABASE        USER            ADDRESS                 METHOD

# "local" is for Unix domain socket connections only
local   all             all                                     md5 <-- *** ENSURE md5
# IPv4 local connections:
host    all             all             127.0.0.1/32            md5
# IPv6 local connections:
host    all             all             ::1/128                 md5
# Allow replication connections from localhost, by a user with the
# replication privilege.
local   replication     all                                     peer
host    replication     all             127.0.0.1/32            md5
host    replication     all             ::1/128                 md5
```

Save changes and restart PostgreSQL:

```
# Ubuntu
sudo service postgresql restart

# Mac
brew services restart postgresql
```

### 3. QGIS installation

Install `QGIS` by entering:

```
# Ubuntu
sudo apt install qgis qgis-plugin-grass -y

# Mac
brew install qgis
```

To check `QGIS` is installed correct, enter:

```
QGIS_BIN=$(command -v qgis_process || echo "/Applications/QGIS.app/Contents/MacOS/bin/qgis_process") && "$QGIS_BIN" --version
```

Edit `.env` file and set `QGIS_PREFIX_PATH` and `QGIS_PYTHON_PATH` environment variables for QGIS:

```
QGIS_PREFIX_PATH=[ABSOLUTE PATH TO FOLDER CONTAINING QGIS]
QGIS_PYTHON_PATH=[ABSOLUTE PATH TO QGIS VERSION OF PYTHON3]
```

Typical values for these variables are:

```
# Ubuntu
QGIS_PREFIX_PATH=/usr/
QGIS_PYTHON_PATH=/usr/bin/python3

# Mac
QGIS_PREFIX_PATH=/Applications/QGIS.app/Contents/MacOS/
QGIS_PYTHON_PATH=/Applications/QGIS.app/Contents/MacOS/bin/python3
```

To ensure you have the correct `QGIS_PYTHON_PATH` value, enter it into a command line. For example:

```
# Ubuntu
/usr/bin/python3

# Mac
/Applications/QGIS.app/Contents/MacOS/bin/python3
```

This should load `QGIS`'s version of Python:

```
Python 3.9.5 (default, Sep 10 2021, 16:18:19) 
[Clang 12.0.5 (clang-1205.0.22.11)] on darwin
Type "help", "copyright", "credits" or "license" for more information.
>>> 
```

To attempt to load the QGIS Python module, then enter:

```
from qgis.core import (QgsProject)
```

If you are running the correct `QGIS` version of `Python`, this should return without generating any errors - if it does, `QGIS_PYTHON_PATH` is correct. 

If you see `ModuleNotFoundError` message, the `QGIS_PYTHON_PATH` is incorrect:

```
ModuleNotFoundError: No module named 'qgis' <-- **** ERROR IF INCORRECT QGIS_PYTHON_PATH
```

If you see a "Cannot find proj.db" error message, set `QGIS_PROJ_DATA` environment variable in `.env` to folder of your `PROJ` library containing `proj.db`:

```
QGIS_PROJ_DATA=/path/to/proj/
```

### 4. `tilemaker` installation

Install `tilemaker` by entering:

```
git clone https://github.com/systemed/tilemaker.git
cd tilemaker
make
sudo make install
cd ..
```

Check `tilemaker` has installed correctly by typing:

```
tilemaker --help
```

### 5. `tippecanoe` installation

Install `tippecanoe` with:

```
git clone https://github.com/felt/tippecanoe
cd tippecanoe
make -j
sudo make install
cd ..
```

Check `tippecanoe` has installed correctly by typing:

```
tippecanoe --help
```

### 6. `tileserver-gl` installation

Install `Node.js version 22` by entering:

```
# Ubuntu
sudo apt install curl -y
curl -fsSL https://deb.nodesource.com/setup_22.x | sudo -E bash -
sudo apt install nodejs -y

# Mac
brew update
brew install node@22
brew link --overwrite node@22
```

With `Node.js v22` installed, follow the [tileserver-gl Installation Instructions](https://tileserver.readthedocs.io/en/latest/installation.html) to install a native version of `tileserver-gl`.

### 7. Next steps

With all the required software natively installed, you can now run Open Site Energy:

```
# Server version
./server-cli.sh

# Command line version
./build-cli.sh [configuration]
```

Consult the general [`README`](README.md) documentation for instructions on how to use Open Site Energy.

## Cloud computing install

As an alternative to running Open Site Energy locally, you can create a dedicated cloud computing server (AWS and GCP currently supported) that installs the required software and manages the build process. *Note: creating a cloud computing server will incur charges.*

Install `Terraform` using the instructions at:

- https://developer.hashicorp.com/terraform/install

With `Terraform` installed, install the necessary cloud provider client software and set up login credentials for the cloud provider. Open Site Energy currently supports `AWS` and `Google Cloud` and cloud-specific instructions for both are provided below:

### AWS - Initial setup

1. Install [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html)
2. Login to your AWS account via a web browser and set up an `ACCESS_KEY` and `SECRET_ACCESS_KEY`. Store these variables somewhere safe.
3. Open a command prompt and export the previous `ACCESS_KEY` and `SECRET_ACCESS_KEY` values as environment variables:

```
export AWS_ACCESS_KEY_ID=<YOUR_ACCESS_KEY_ID>
export AWS_SECRET_ACCESS_KEY=<YOUR_SECRET_ACCESS_KEY>
```

4. Change to Open Site Energy's `terraform/aws` directory and initialize `Terraform` for this folder:

```
cd terraform/aws/
terraform init
```

### Google Cloud - Initial setup

1. Install [gcloud CLI](https://cloud.google.com/sdk/docs/install)
2. Enable Google Cloud authentication by entering:

```
gcloud auth application-default login
```
3. Login to your Google Cloud account and create a new project. Copy the `Project ID` for this project.

4. Change to Open Site Energy's `terraform/gcp` directory and initialize `Terraform` for this folder:

```
cd terraform/gcp
terraform init
```

### AWS and GCP build

While still in a cloud provider folder (`terraform/aws` or `terraform/gcp`), run the `Terraform` build process:

```
terraform apply
```

If building a `Google Cloud` instance, you will be prompted for a `Project ID` - use the value for your project that you copied during the initial setup, 

You will then be prompted for a new `username` and `password` - these should be the `username` and `password` you want to use to login to your new Open Site Energy server, ie. **NOT your cloud computing `username`/`password`** - do not enter these under any circumstances unless logging into your cloud computing account through a web browser.

`Terraform` will then display its intended `build plan` for your cloud server. If this all looks correct, enter `yes` when prompted and a new cloud server will then be created.

**Note: Creating a new cloud server will incur charges. Once you enter `yes`, you will be charged by your respective cloud computing provider.**

Once the cloud server is running, locate the `external IP Address` for the server and enter it into a web browser *without* the `https://` prefix:

```
http://[serveripaddress]
```

After several minutes, a login to your new Open Site Energy server will appear. This will display detailed logs showing the server creation process. 

Once the server has finished installing, you should be presented with the administration login screen:

![Open Site Energy - Login](/images/opensiteenergy-admin-login.png)




