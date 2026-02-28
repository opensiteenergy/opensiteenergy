
# Use Ubuntu 22.04 as problems building osm-export-tool with newer versions of Ubuntu

FROM ubuntu:24.04


# Ensure noninteractive setup during container creation

ENV DEBIAN_FRONTEND=noninteractive 


# Install general tools and required libraries

RUN apt-get update && apt-get install -y --no-install-recommends \
    gnupg software-properties-common cmake make g++ dpkg python3.12-venv \
    libbz2-dev libpq-dev libboost-all-dev libgeos-dev libtiff-dev libspatialite-dev \
    libsqlite3-dev libcurl4-gnutls-dev liblua5.4-dev rapidjson-dev libshp-dev libgdal-dev gdal-bin \
    zip unzip lua5.4 shapelib ca-certificates curl nano wget pip git proj-bin spatialite-bin sqlite3 \
    qgis qgis-plugin-grass docker.io \
    && rm -rf /var/lib/apt/lists/*
RUN apt update; exit 0


# Install chromium browser so selenium can work

RUN apt-get update && apt-get install -y \
    chromium-browser \
    chromium-chromedriver


# Install tilemaker

WORKDIR /usr/src/opensiteenergy
RUN git clone https://github.com/systemed/tilemaker.git
WORKDIR /usr/src/opensiteenergy/tilemaker
RUN make
RUN make install


# Install tippecanoe

WORKDIR /usr/src/opensiteenergy
RUN git clone https://github.com/felt/tippecanoe.git
WORKDIR /usr/src/opensiteenergy/tippecanoe
RUN make -j
RUN make install


# Create Python virtual environment and install Python libraries

WORKDIR /usr/src/opensiteenergy
COPY requirements.txt .
RUN /usr/bin/python3 -m venv /usr/src/opensiteenergy/venv
ENV PATH="/usr/src/opensiteenergy/venv/bin:$PATH"
RUN pip3 install gdal==`gdal-config --version`
RUN pip3 install -r requirements.txt
RUN pip3 install git+https://github.com/hotosm/osm-export-tool-python --no-deps


# Set working directory and copy essential files into it

WORKDIR /usr/src/opensiteenergy
COPY opensite opensite
COPY tileserver tileserver
COPY web web
COPY build-cli.sh .
COPY server-cli.sh .
RUN chmod +x build-cli.sh
RUN chmod +x server-cli.sh
COPY opensiteenergy.py .
COPY build-qgis.py .
COPY clipping-master-EPSG-25830.gpkg .
COPY osm-boundaries.yml .
COPY defaults.yml .

CMD ["/bin/bash"]
