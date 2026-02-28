#!/usr/bin/env bash
#!/bin/bash

set -e -o pipefail -u

cd "$(dirname "$0")"

rm -rf tmp-coastline tmp-landcover

mkdir -p tmp-coastline
pushd tmp-coastline

if ! [ -f "water-polygons-split-4326.zip" ]; then
  curl --proto '=https' --tlsv1.3 -sSfO https://osmdata.openstreetmap.de/download/water-polygons-split-4326.zip
fi

unzip -o -j water-polygons-split-4326.zip

popd

mkdir -p tmp-landcover
pushd tmp-landcover

if ! [ -f "ne_10m_antarctic_ice_shelves_polys.zip" ]; then
  curl --proto '=https' --tlsv1.3 -sSfO https://naciscdn.org/naturalearth/10m/physical/ne_10m_antarctic_ice_shelves_polys.zip
fi

if ! [ -f "ne_10m_urban_areas.zip" ]; then
  curl --proto '=https' --tlsv1.3 -sSfO https://naciscdn.org/naturalearth/10m/cultural/ne_10m_urban_areas.zip
fi

if ! [ -f "ne_10m_glaciated_areas.zip" ]; then
  curl --proto '=https' --tlsv1.3 -sSfO https://naciscdn.org/naturalearth/10m/physical/ne_10m_glaciated_areas.zip
fi

mkdir -p ne_10m_antarctic_ice_shelves_polys
unzip -o ne_10m_antarctic_ice_shelves_polys.zip -d ne_10m_antarctic_ice_shelves_polys

mkdir -p ne_10m_urban_areas
unzip -o ne_10m_urban_areas.zip -d ne_10m_urban_areas

mkdir -p ne_10m_glaciated_areas
unzip -o ne_10m_glaciated_areas.zip -d ne_10m_glaciated_areas

popd

mv tmp-coastline coastline
mv tmp-landcover landcover