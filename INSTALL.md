# Installation

## Software Required

- [PostGIS](https://postgis.net/): For storing and processing GIS data.

- `Python3.9`: For compatibility with [osm-export-tool](https://github.com/hotosm/osm-export-tool-python).

- [GDAL](https://gdal.org): For transferring data in and out of PostGIS.

- [QGIS](https://qgis.org/): For generating QGIS files.

- [tilemaker](https://github.com/systemed/tilemaker): For generating mbtiles version of OpenStreetMap for use as background map within MapLibre-GL.

- [tippecanoe](https://github.com/felt/tippecanoe): For generating optimized mbtiles versions of data layers for MapLibre-GL.

- [Docker](https://www.docker.com/): For previewing final data in TileServer-GL - *desirable but not mandatory*.

## Docker, local (non-Docker) and cloud-computing installs

The Open Wind Energy toolkit has three flavours of build process:

- **1. Docker-based**: The entire build is run within Docker instances. This is the recommended option as it helps avoid installation issues.

- **2. Local (non-Docker) based**: All of the build runs without Docker. This may be desirable for performance reasons or when there is a need to quickly modify the codebase and / or resolve technical issues. There may, however, be a need to run Docker after the build has completed in order to view results through a Dockerized tileserver. 

- **3. Cloud computing server**: Creates a dedicated cloud computing server (AWS and GCP currently supported) that installs the required software, runs the build process and serves the final layers through a web interface. The server instance also provides a user-friendly web interface for running customized builds. *Note: creating a cloud computing server will incur charges.*

## Software Platforms

Installation instructions for Windows (11+ only), Mac OS and Ubuntu are provided below. For other platforms, we recommend you install Docker and run the **Docker-based install** (see [2. All platforms - Docker-based install](#2-all-platforms---docker-based-install---build---view). 

If you have specific success or issues running the Open Wind Energy toolkit on other platforms, please drop us an email at support@openwind.energy

Next steps:

- For Windows, go to [1a. Windows - Docker-based install](#1a-windows---docker-based-install) or [1b. Windows - Local (non-Docker) based install](#1b-windows---local-non-docker-based-install)

- For Mac, go to [1c. Mac - All installs](#1c-mac---all-installs)

- For Ubuntu, go to [1d. Ubuntu - All installs](#1d-ubuntu---all-installs)

- For a cloud computing server install, go to [1e. Cloud computing server](#1e-cloud-computing-server)

## 1a. Windows - Docker-based install

Download and install Docker Desktop application from https://www.docker.com/

### Next steps

- Go to [2. All platforms - Docker-based install](#2-all-platforms---docker-based-install---build---view)

## 1b. Windows - Local (non-Docker) based install

The recommended way to run Open Wind Energy on Windows in non-Docker mode is to install the [Windows Subsystem for Linux](https://learn.microsoft.com/en-us/windows/wsl/about) and use Ubuntu installation instructions.  

Follow the instructions below to install `Windows Subsystem for Linux (WSL)`. In the event of problems installing `WSL`, consult [Microsoft WSL Troubleshooting](https://learn.microsoft.com/en-us/windows/wsl/troubleshooting).

### Installing Windows Subsystem for Linux
Go to Windows `Start` and search for `Turn Windows features on or off`. Click the tickbox next to `Windows Subsystem for Linux`. Restart your computer when prompted.

Once computer has restarted, go to Windows `Start`, search for `Windows PowerShell` and select `Run as Administrator`. You should see:
```
Windows PowerShell
Copyright (C) Microsoft Corporation. All rights reserved.

Install the latest PowerShell for new features and improvements! https://aka.ms/PSWindows

PS C:\Users\test>
```

Enter the following:

```
wsl.exe --set-default-version 1
```

The system will then attempt to install `WSL`. 

Once this process has completed successfully, you should see:

```
The operation completed successfully.
The requested operation is successful. Changes will not be effective until the system is rebooted.
The operation completed successfully.
```

Reboot your computer. 

After reboot, open PowerShell as administrator and type:

```
wsl.exe --install Ubuntu-22.04
```

This should install Ubuntu-22.04 and ask you to set up an Ubuntu username and password. It will then launch into an Ubuntu command prompt. 

### Initial setup once WSL installed

Once you are in Ubuntu, install PostGIS 13 - PostGIS 13 is required with `WSL` due to issues with PostGIS 14+ on `WSL`.

```
sudo apt-get update
sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
sudo apt-get update -y
sudo apt install postgresql-13-postgis -y
```

### Next steps:

- Go to section [3b. Ubuntu - Local (non-Docker) install](#3b-ubuntu---local-non-docker-install) 

## 1c. Mac - All installs

Install [QEMU](https://www.qemu.org/download/):

```
brew update
brew install qemu
```

Install Docker using [these official instructions](https://docs.docker.com/desktop/setup/install/mac-install/) or with:
```
brew install docker
```

### Next steps:

- To create a **Docker-based install**, go to section [2. All Platforms - Docker-based install -> Build -> View](#2-all-platforms---docker-based-install---build---view).

- To create a **Local (non-Docker) install**, go to section [3a. Mac - Local (non-Docker) install](#3a-mac---local-non-docker-install).


## 1d. Ubuntu - All installs

Install [QEMU](https://www.qemu.org/download/):

```
sudo apt-get update
sudo apt install qemu-user-static binfmt-support -y
```

Install Docker using [these official instructions](https://docs.docker.com/engine/install/ubuntu/) or with:
```
sudo apt-get update
sudo apt-get install ca-certificates curl
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "${UBUNTU_CODENAME:-$VERSION_CODENAME}") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
sudo apt install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin -y
```

### Next steps:

- To create a **Docker-based install**, go to section [2. All platforms - Docker-based install -> Build -> View](#2-all-platforms---docker-based-install---build---view).

- To create a **Local (non-Docker) install**, go to section [3b. Ubuntu - Local (non-Docker) install](#3b-ubuntu---local-non-docker-install).

## 1e. Cloud computing server

Install `Terraform` using the instructions at:

- https://developer.hashicorp.com/terraform/install

After installing `Terraform`, install the necessary cloud provider client software and set up login credentials for the cloud provider. Open Wind Energy currently supports `AWS` and `Google Cloud` and cloud-specific instructions for both are provided below:

### AWS - Initial setup

1. Install [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html)
2. Login to your AWS account via a web browser and set up an `ACCESS_KEY` and `SECRET_ACCESS_KEY`. Store these variables somewhere safe.
3. Open a command prompt and export the previous `ACCESS_KEY` and `SECRET_ACCESS_KEY` values as environment variables:

```
export AWS_ACCESS_KEY_ID=<YOUR_ACCESS_KEY_ID>
export AWS_SECRET_ACCESS_KEY=<YOUR_SECRET_ACCESS_KEY>
```

4. Change to Open Wind Energy's `terraform/aws` directory and initialize `Terraform` for this folder:

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

4. Change to Open Wind Energy's `terraform/gcp` directory and initialize `Terraform` for this folder:

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

You will then be prompted for a new `username` and `password` - these should be the `username` and `password` you want to use to login to your new Open Wind Energy server, ie. **NOT your cloud computing `username`/`password`** - do not enter these under any circumstances unless logging into your cloud computing account through a web browser.

`Terraform` will then display its intended `build plan` for your cloud server. If this all looks correct, enter `yes` when prompted and a new cloud server will then be created.

**Note: Creating a new cloud server will incur charges. Once you enter `yes`, you will be charged by your respective gloud computing provider.**

Once the cloud server is running, locate the `external IP Address` for the server and enter it into a web browser *without* the `https://` prefix:

```
http://[serveripaddress]
```

After several minutes, a login to your new Open Wind Energy server will appear. This will display detailed logs showing the build process. Note that a typical build will take upwards of 6 hours to complete, even on a fast server.

The Open Wind Energy Server also offers the following functionality:

- Once a build has completed, you can modify build configurations and run a new build.
- Download exported files.
- Link to the latest map showing layers from latest build. 

### Next steps:

- Your installation is complete! Go to `http://[serveripaddress]/admin` to view logs, modify build configurations and download exported files.


## 2. All platforms - Docker-based install -> Build -> View

Ensure Docker has a minimum of **12GB** memory allocated to it by going to `Settings -> Resources` and setting `Memory limit` to at least **12GB**. Also set `Swap` size to at least **2GB**.

Clone the Open Wind Energy repository:
```
git clone https://github.com/open-wind/openwindenergy.git
cd openwindenergy
```

Run the Open Wind Energy build stage (Docker-based mode) by typing:

```
./build-docker.sh
```

This will use a default value (124.2 metres) for the turbine height to tip and a default value ( metres) for the turbine blade radius. 

Alternatively, specify the turbine height to tip by adding it to the prompt:

```
./build-docker.sh [HEIGHT TO TIP]
```

For example:

```
./build-docker.sh 99.5
./build-docker.sh 129
./build-docker.sh 149.9
``` 

Specify the turbine blade radius by adding it after the turbine height to tip:

```
./build-docker.sh [HEIGHT TO TIP] [BLADE RADIUS]
```

For example:

```
./build-docker.sh 99.5 30.5
./build-docker.sh 120 40
./build-docker.sh 149.9 45
``` 

Once the build has completed (10-20 hours), view the final Open Wind Energy constraint layers by running:

```
./run-docker.sh
```

Or alternatively open the QGIS file located at `build-docker/windconstraints--latest.qgs`.

### Memory-related problems with Docker

You may experience problems with Docker due to insufficient memory. For example, the build process may fail with the following error:

```
psycopg2.OperationalError: SSL SYSCALL error: EOF detected
```

If you see this error, change Docker's `Resources` settings so it has at least **12Gb** memory and **2Gb** swap size allocated to it:

- Open `Docker Desktop`.
- Select `Settings...` then `Resources`.
- Change Docker's `Memory limit` to >= `12Gb` and `Swap` to >= `2Gb`.

If you are still experiencing memory issues building OpenWind Energy on Docker, increase the swap size to > `4Gb` by editing the Docker config file directly:

- Find the location of your platform's Docker config file by referring to https://docs.docker.com/desktop/settings-and-maintenance/settings/
- Edit the config file and set the `SwapMiB` setting to at least `10000`, ie. 10Gb.
- Fully quit and restart Docker so the new `SwapMiB` setting takes effect.
- Rerun `./build-docker.sh`


### Next steps:

- Your installation is complete!


## 3a. Mac - Local (non-Docker) install

Install `PostGIS`, `Python3.9`, `GDAL`, `tippecanoe`, `QGIS`, `OpenJDK` and general software and libraries required to compile `tilemaker`:

```
brew install postgis python@3.9 gdal tippecanoe cmake make geos rapidjson gqis java git \
libpq libtiff libspatialite lua shapelib sqlite curl proj node npm virtualenv
```
Install `tilemaker` with:

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

### Next steps:

- Go to section [4. All Platforms - Local (non-Docker) install](#4-all-platforms---local-non-docker-install).


## 3b. Ubuntu - Local (non-Docker) install

Install `Python3.9`, `GDAL`, `QGIS`, `OpenJDK` and general software and libraries required to compile `tilemaker` and `tippecanoe`:

```
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt-get update
sudo apt install  gnupg software-properties-common cmake make g++ dpkg ca-certificates \
                  libbz2-dev libpq-dev libboost-all-dev libgeos-dev libtiff-dev libspatialite-dev \
                  liblua5.4-dev rapidjson-dev libshp-dev libgdal-dev shapelib \
                  spatialite-bin sqlite3 lua5.4 gdal-bin virtualenv \
                  zip unzip curl nano wget pip git nodejs npm proj-bin \
                  qgis qgis-plugin-grass default-jdk \
                  python3.9 python3.9-dev python3.9-venv python3-gdal -y
```

If you're **not** running Ubuntu on Windows using `WSL`, also install `PostGIS`:

```
sudo apt-get update
sudo apt install postgresql-postgis -y
```

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

Install `tilemaker` with:

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

### Next steps:

- Go to section [4. All platforms - Local (non-Docker) install](#4-all-platforms---local-non-docker-install).

# 4. All platforms - Local (non-Docker) install

### Install Node Version Manager (`nvm`) and `togeojson`:
```
curl --silent -o- https://raw.githubusercontent.com/creationix/nvm/v0.31.2/install.sh | bash
source ~/.bashrc

nvm install v10.19.0
nvm use v10.19.0
npm install -g @mapbox/togeojson
```
Check `togeosjon` has installed correctly by typing:
```
togeojson
```

### Clone Open Wind Energy project repository:

```
git clone https://github.com/open-wind/openwindenergy.git
cd openwindenergy
```

Copy environment file template `.env-template` to `.env` and activate a Python 3.9 virtual environment:

```
cp .env-template .env

which python3.9
[PATH TO PYTHON 3.9]

virtualenv -p [PATH TO PYTHON 3.9 - FROM ABOVE] venv

source venv/bin/activate
```

### Install correct version of `GDAL`
Install correct version of `GDAL` Python module so it exactly matches installed (non-Python) version of `GDAL`:
```
pip3 install gdal==`gdal-config --version`
```

If you experience `AttributeError: install_layout` at this stage, enter:
```
pip install -U setuptools
```
...and rerun:

```
pip3 install gdal==`gdal-config --version`
```

### Install Python modules required for Open Wind Energy
```
pip3 install -r requirements.txt
pip3 install git+https://github.com/hotosm/osm-export-tool-python --no-deps
```

### Set QGIS environment variables
Edit `.env` file and set `QGIS_PREFIX_PATH` and `QGIS_PYTHON_PATH` environment variables for QGIS:

```
QGIS_PREFIX_PATH=[ABSOLUTE PATH TO FOLDER CONTAINING QGIS]
QGIS_PYTHON_PATH=[ABSOLUTE PATH TO QGIS VERSION OF PYTHON3]
```

Typical values for these variables are:

```
[Ubuntu]

QGIS_PREFIX_PATH=/usr/
QGIS_PYTHON_PATH=/usr/bin/python3


[Mac]

QGIS_PREFIX_PATH=/Applications/QGIS.app/Contents/MacOS/
QGIS_PYTHON_PATH=/Applications/QGIS.app/Contents/MacOS/bin/python3
```

To ensure you have the correct `QGIS_PYTHON_PATH` value, enter it into a command line. For example:

```
/usr/bin/python3

[or]

/Applications/QGIS.app/Contents/MacOS/bin/python3
```

This should load QGIS's version of Python:

```
Python 3.9.5 (default, Sep 10 2021, 16:18:19) 
[Clang 12.0.5 (clang-1205.0.22.11)] on darwin
Type "help", "copyright", "credits" or "license" for more information.
>>> 
```

Enter the following to attempt to load the QGIS Python module:

```
from qgis.core import (QgsProject)
```

If you are running the correct QGIS version of Python, this should return without generating any errors and `QGIS_PYTHON_PATH` is correct.

However, if you see `ModuleNotFoundError` message (see below), the `QGIS_PYTHON_PATH` is incorrect.

```
ModuleNotFoundError: No module named 'qgis' <-- **** ERROR IF INCORRECT QGIS_PYTHON_PATH
```

If you see a "Cannot find proj.db" error message, you will need to set the `QGIS_PROJ_DATA` environment variable in `.env` to the folder of your `PROJ` library containing `proj.db`:

```
QGIS_PROJ_DATA=/path/to/proj/
```

To find potential values for `QGIS_PROJ_DATA`, type:

```
sudo find / -name proj.db
```

Once you have set the value of `QGIS_PROJ_DATA` in `.env`, reload environment variables in `.env` and attempt to run QGIS Python again:

```
source ./.env


/usr/bin/python3

[or] 

/Applications/QGIS.app/Contents/MacOS/bin/python3

[or]

/your/path/to/QGIS/python


[When Python loads, enter:]

from qgis.core import (QgsProject)
```

This should load the `qgis.core` Python module without generating errors. If so, press `CTRL-D` to quit QGIS Python and continue with the installation process.

### Set up PostGIS
Enter the commands below to set up a new PostGIS database for Open Wind Energy:

```
sudo service postgresql restart
sudo -u postgres createuser -P openwind
sudo -u postgres createdb -O openwind openwind
sudo -u postgres psql -d openwind -c 'CREATE EXTENSION postgis;'
sudo -u postgres psql -d openwind -c 'CREATE EXTENSION postgis_raster;'
sudo -u postgres psql -d openwind -c 'GRANT ALL PRIVILEGES ON DATABASE openwind TO openwind;'
```

When prompted for a database user password, enter `password` - or enter a different password and edit the `POSTGRES_PASSWORD` variable in the `.env` file accordingly.

The Open Wind Energy toolkit also needs to access PostGIS using a standard `md5` password. Therefore edit PostgreSQL's `pg_hba.conf` file to allow `md5` password access:

```
sudo nano /etc/postgresql/[REPLACE WITH POSTGRES VERSION]/main/pg_hba.conf
```

Scroll to the bottom of `pg_hba.conf` and edit the row containing `local all all ` so it's set to `md5`:

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
sudo service postgresql restart
```

### Install openmaptiles fonts
Install folder of openmaptiles fonts:

```
git clone https://github.com/openmaptiles/fonts
cd fonts
npm install
node ./generate.js
cd ..
```

Note: you may experience problems building openmaptiles fonts. If so, you can skip this stage but will need to add the `--skipfonts` argument to all subsequent build commands:

```
./build-cli.sh 149.9 --skipfonts
```

This will instruct the build process to skip attempting an install of openmaptiles fonts and will use a CDN (pre-built) version of fonts instead.

After attempting to install openmaptiles fonts, ensure you are in the main `openwindenergy/` directory:

```
cd ..

[or] 

cd /path/to/openwindenergy/
```


### Next steps:

- Go to [All platforms - Local (non-Docker) install](#5-all-platforms---local-non-docker-install---build---view)

## 5. All Platforms - Local (non-Docker) install -> Build -> View

To run the Open Wind Energy build stage in **Local (non-Docker)** mode, type:

```
./build-cli.sh
```

This will use a default value (124.2 metres) for the turbine height to tip. 

Alternatively, explicitly specify the turbine height to tip by adding it to the prompt:

```
./build-cli.sh [HEIGHT TO TIP]
```

For example:

```
./build-cli.sh 99.5
./build-cli.sh 129
./build-cli.sh 149.9
``` 

Once the build has completed (10-20 hours), view the final Open Wind Energy constraint layers by running:

```
./run-cli.sh
```

This runs a Docker-containerized version of the TileServer-GL tileserver to serve up `mbtiles` layers to a simple web map. 

Alternatively, you can view the final layers without using Docker by opening the QGIS file located at `build-cli/windconstraints--latest.qgs`.
