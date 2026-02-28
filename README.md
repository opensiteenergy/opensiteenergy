# Open Site Energy

The Open Site Energy toolkit builds and displays renewable energy site constraints, eg. for wind turbines or solar farms, in a fully automated way, avoiding the need to manually download and process multiple GIS datasets by hand. 

It is designed to save time and lower the barrier to entry for wind turbine and solar farm site identification for the following users:

- Community energy groups
- Net Zero and fuel poverty organisations
- Local authorities
- Electricity companies

The toolkit provides a streamlined process for generating different parameter-specific constraint layers, eg. turbine-height-specific constraint layers, and may also be of benefit to:

- Wind farm developers
- GIS analysts and data scientists

The toolkit outputs data in a number of industry-standard GIS formats:

- `GeoJSON`
- `ESRI Shapefile`
- `GeoPackage`
- Mapbox Vector Tiles (`mbtiles`)
- QGIS file

The toolkit also provides local versions of several popular GIS viewing clients for viewing the final renewable energy site constraints:

- [MapLibre-GL](https://github.com/maplibre/maplibre-gl-js)
- [TileServer-GL](https://github.com/maptiler/tileserver-gl)

For an overview of how the toolkit works, see ['How it works'](#how-it-works), below.

The toolkit build system can be accessed in two ways:

- **Server interface**: A web-based administration interface provides a simple way to create and run custom site contraint configurations. Once a build has been started, a dynamic **process monitor** shows the progress of individual tasks in real time.
- **Command line interface**: The command line version of Open Site Energy runs site constraint builds without user interaction; the process finishes once the final build has finished.

## Quickstart - Server interface

Install [Docker](https://docker.com) then run:

```
git clone [TOCOME]
cd opensiteenergy
./server-docker.sh
```

Once the Docker setup has finished, connect to the server application by loading `http://localhost:8000` from a web browser (username: `admin`, password: `password`). This will display the Open Site Energy administration interface:

![Open Site Energy - Site configurations](/images/opensiteenergy-admin-configurations.png)

Select one of the existing build configurations from `solar` or `wind` and click **Load** to view the configuration's components. You can then view the datasets for that configuration, including dataset-specific settings such as buffers:

![Open Site Energy - Wind configuration](/images/opensiteenergy-admin-wind.png)

You can make changes to any existing configuration - such as adding or removing datasets or changing buffer values - and then click **Save** to save the configuration to your server. Note: all referenced datasets are sourced from [Open Site Energy's CKAN open data portal](`https://data.opensite.energy`).

To build any configuration - whether an existing configuration like `solar` or `wind` or a new configuration you have created - select **Run build** from the left-hand menu. Then select the configuration from the **Server configuration** dropdown and click **Add**:

![Open Site Energy Site - Setup build](/images/opensiteenergy-admin-prebuild.png)

Click **Start generating constraint layers** to start running the build. This will load the **Process monitor** which shows the current progress of the build:

![Open Site Energy Site - Process monitor](/images/opensiteenergy-admin-process-monitor.png)

You can view detailed process logs at any time by clicking the terminal icon, top-right: <img src="images/opensiteenergy-icon-terminal.png" width="25">
  
![Open Site Energy Site - View logs](/images/opensiteenergy-admin-view-console.png)

A typical build will take several hours to complete, depending on the selected configuration and the specification of the computer running the software (see [Minimum platform requirements](#minimum-platform-requirements), below). 

Once the build has completed, click **Live website** on the left-hand menu to view the final constraints map:

![Open Site Energy Site - View build result](/images/opensiteenergy-admin-map.png)

The map created using the Open Site Energy existing `solar` and `wind` configurations can be viewed at [https://map.opensite.energy](https://map.opensite.energy)

To download specific GIS files, select **Generated files** on the left-hand menu:

![Open Site Energy Site - Generated files](/images/opensiteenergy-admin-files.png)

## Quickstart - Command line interface

Install [Docker](https://docker.com) then run:

```
git clone [TOCOME]
cd opensiteenergy

./build-docker.sh wind

OR

./build-docker.sh solar
```

Once the Docker setup has finished, a complete build of wind or solar site constraints will then run.

When the build has completed, the exported GIS files will be output to `build/output/layers` while tileserver-gl-specific files will be output to `build/tileserver-live`. The HTML file for displaying `tileserver-gl` layers will be created at `build/output/index.html`.

You can view the results of a build by entering:

```
./viewbuild.sh
```

This runs a temporary tileserver to serve up `mbtiles` files and a temporary webserver to serve `build/output/index.html`.

## Installation 

See [INSTALL.md](INSTALL.md). 

If you install a local (ie. non-Docker) version of Open Site Energy, replace `-docker` with `-cli` for identical functionality with improved performance:

- Use `./build-cli.sh` instead of `./build-docker.sh`
- Use `./server-cli.sh` instead of `./server-docker.sh`

## Configuration options

The **Server interface** provides standard user interface controls for customizing aspects of a build. 

For the **Command line interface**, you must specify which configuration(s) to build as part of the command line input:

```
./build-cli.sh [configuration_1] [configuration_2] ...

OR

./build-docker.sh [configuration_1] [configuration_2] ...
```

For example:

```
./build-cli.sh wind custom-solar.yml https://yourdomain.com/confs/battery.yml
```

A `[configuration]` can be specified in one of three ways:

### 1. Name of existing configuration
These are taken from [Open Site Energy's open data portal configuration section](https://data.opensite.energy/group/custom). Current configurations available on Open Site Energy's open data portal are `solar` and `wind`. 

To run both `solar` and `wind`, for example, enter: 

```
./build-cli.sh solar wind
```
### 2. Filename of local YML configuration file

For example:

```
./build-cli.sh custom-wind.yml
```

### 3. URL of internet YML configuration file

For example: 

```
./build-cli.sh https://yourdomain.org/custom-wind.yml
```

When using a local or internet-hosted YML configuration, consult the sample configurations on [Open Site Energy's open data portal](https://data.opensite.energy/group/custom) for the correct Open Site Energy YML format. 

Note: the current version of Open Site Energy only works with datasets referenced on [Open Site Energy's open data portal](https://data.opensite.energy/). To use the dataset referenced at `https://data.opensite.energy/dataset/civilian-airports--uk` for example, the YML file would be:

```
title: 
  "Test constraints"

type: 
  test

code: 
  test

structure:
  aviation-and-exclusion-areas:
    - civilian-airports--uk
```

### Additional command line options

**--server** 

Run in server mode - this is equivalent to running `./server-cli.sh` or `./server-docker.sh`.

**--purgedb**

Deletes all Open Site Energy tables stored in PostGIS. This may be necessary if PostGIS tables become corrupted. 

**--purgeall**

Deletes all downloads, output files and Open Site Energy tables stored in PostGIS. This effectively restarts Open Site Energy from a completely clean state.

**--clip=[clip_area_1;clip_area_2;...]**

Name of area to clip data to, e.g., `Surrey`. For multiple clipping areas, separate with semicolons, eg. `--clip="East Sussex;Devon"`. For example:

```
./build-cli.sh wind --clip="East Sussex;Devon"
```

**--overwrite**

Reexports all output files, overwriting files already created.

**--graphonly**

Generates build graph at `graph.html` but without running build. This may be useful if you want to check the structure of a complex or time-consuming build before setting it running.

**--snapgrid=[snap_grid_value_in_metres]**

Snaps all imported datasets to grid of size *snap_grid_value_in_metres*. For example:

```
./build-cli.sh wind --snapgrid=0.5
```

The default value of `snapgrid` is 0.1 metres, ie. 10cm (see `defaults.yml`), to reduce processing time and size of database tables. 

**--outputformats=[format_1,format_2,...]**

Set required output format(s) for all files that will be exported. Possible values are `gpkg`, `shp`, `geojson`, `mbtiles`, `web`, and `qgis`. For multiple formats, separate values with commas, eg `--outputformats=gpkg,shp`. 

When `--outputformats` is omitted, the build will export all possible output file formats. 

### Additional configuration values

In addition to the command line options supplied to `./build-cli.sh` and `./build-docker.sh`, the YML configuration files provide further options:

**height-to-tip**: Height to tip in metres of intended wind turbine (if generating wind site constraints).

**blade-radius**: Blade radius in metres of intended wind turbine (if generating wind site constraints).

**osm**: URL of Open Street Map `PBF` data file.

Consult the sample configurations at [Open Site Energy's open data portal](https://data.opensite.energy/group/custom) for examples of all available YML parameters. 


## Minimum platform requirements

To run the Open Site Energy build process on both `solar` and `wind`, you will need a computer with the following minimum configuration:

- 12Gb memory
- 250Gb free hard disk space

## Typical timings

During local (non-Docker) testing on a Mac Arm M3, the `wind` constraints build took approximately 2.5 hours to complete while the `solar` constraints build took upwards of 10 hours to complete. The longer time for `solar` is due to the large number of high-vertex datasets involved - this involves larger downloads and more intensive GIS processing.


## How it works

### 1. Download datasets library from open data portal

The Open Site Energy toolkit starts by downloading the latest datasets library from the Open Site Energy data portal at https://data.opensite.energy. This data portal provides a trusted source for datasets and is used to store crucial metadata (including download links) for the datasets referenced in an Open Site Energy YML configuration file.

### 2. Create processing graph

Once the data portal metadata has been incorporated into the Open Site Energy YML configurations, a comprehensive processing graph is then created describing every processing step that should be be carried out. This processing graph will be visible on the **Process monitor** page when running as a server or on the `graph.html` status output when running from the command line. 

Processing steps will include actions like `download`, `preprocess`, `run` (to run an external library) or `amalgamate` (to amalgamate multiple datasets into one). Dependencies are mapped within this processing graph to ensure no processing node is attempted until all of its dependencies have completed successfully.

### 3. Run processing graph using parallel processing

Where possible - and without violating dependency requirements - multiple processing nodes are processed in parallel to save time. The toolkit might for example run a large data download from Open Street Map while simultaneously downloading, buffering and exporting a smaller dataset from another datasource.

The processing nodes are split into two categories:

- **Pre-output nodes**: This covers all nodes handling downloading, buffering, grid-splicing and amalgamation of datasets. Pre-output nodes will appear on the left-hand side of the **Process monitor** processing graph.
- **Output-specific nodes**: This covers all nodes specifically concerned with generating an output, whether the output is a `GPKG` or `GeoJSON` file or a `web` output that requires both conventional web files (`html`, `json`) and `tileserver-gl` files. Output-specific nodes will appear on the right-hand side of the **Process monitor** processing graph. 

### 4. Repeat until all nodes have completed (or failed)

Whether running as a server or from the command line, a processing queue periodically checks the status of all nodes and starts processing any unprocessed nodes whose dependencies have completed. The process continues until: (i) the build completes without error, or (ii) one or more errors 'blocks' the processing queue, preventing further nodes from being processed. 

In the event of an error, it should be possible to return to the 'Run build' page and attempt to start the build again. As all downloads and database tables are cached (assuming `purgeall` is not activated), the build process should quickly reach the same point where the error previously occurred. If the previous error was transient, eg. a dataset website temporarily going offline, the build should then complete without error. 

If you experience systematic errors, however, please drop us an email at info@opensite.energy


## Tileserver files

Open Site Energy relies heavily on the `mbtiles` (MapBox Tiles) data format to display complex granular data accurately and quickly in popular web browsers.

Once your build has completed successfully, there are a number of ways to publish the `mbtiles` layers that are produced:

- Install Open Site Energy as server application on Ubuntu linux server - see `opensiteenergy-bootstrap-ubuntu.sh` and `opensiteenergy-build-ubuntu.sh` for scripts that run on Ubuntu 24.04. 
- Run your own [TileServer-GL](https://github.com/maptiler/tileserver-gl) instance to serve up `mbtiles` files (see https://github.com/maptiler/tileserver-gl). This can be either as a system-compiled application or as a Docker instance.
- Create an account with [MapBox](https://www.mapbox.com/) and upload `mbtiles` files to this account so MapBox can serve them.


## Environment variables
The toolkit uses environment variables from `.env` and automatically copies `.env-template` (containing default values) to `.env` if `.env` has not been created. 

If you need to modify the environment variables in `.env` script - for example to use a different PostGIS database or to resolve installation issues - the **mandatory** environment variables in `.env` are described below:

- `POSTGRES_DB`: PostGIS database to use.

- `POSTGRES_USER`: Username of user who will access `POSTGRES_DB`. The user needs full access permissions to `POSTGRES_DB`. 

- `POSTGRES_PASSWORD`: Password of user who will access `POSTGRES_DB`.

- `QGIS_PREFIX_PATH`: Filesystem prefix to QGIS (see [Using PyQGIS in standalone scripts](https://docs.qgis.org/3.40/en/docs/pyqgis_developer_cookbook/intro.html#using-pyqgis-in-standalone-scripts)).

- `QGIS_PYTHON_PATH`: Absolute path to specific version of Python3 that QGIS uses, eg. `/usr/bin/python3`.

- `QGIS_PROJ_DATA`: Absolute path to `PROJ` library directory, eg. `/usr/share/proj`.

- `BUILD_FOLDER`: Absolute path to build folder where datasets will be downloaded and output files created. This will replace the default `build/` folder.

- `TILESERVER_URL`: URL of [TileServer GL](https://github.com/maptiler/tileserver-gl) instance where you will host your mbtiles, eg. `https://tiles.opensite.energy`. This variable is used when creating both the web and tileserver files as these files need to know the absolute location of the tileserver endpoint.

- `ADMIN_USERNAME`: Username of main user to administration interface.

- `ADMIN_PASSWORD`: Password of main user to administration interface.


## Contact

info@opensite.energy

https://opensite.energy

## Thanks

We are grateful to the following individuals/organisations for their invaluable feedback and suggestions in shaping Open Site Energy and its predecessor *Open Wind Energy*:

- Kayla Ente, [Brighton & Hove Energy Services Co-operative](https://bhesco.co.uk)
- Ben Cannell, [Sharenergy](https://sharenergy.coop)
- [Clive Howard](https://www.linkedin.com/in/clivehoward/)
- John Taylor, [Community Energy England](https://communityenergyengland.org/)
- Mike Childs, Magnus Gallie, Toby Bridgeman, [Friends of the Earth](https://friendsoftheearth.uk/)
- Catriona Cockburn, [Energise South Downs](https://esd.energy/)
- Ben Sharpe, [Energise South Coast](https://www.energisesussexcoast.co.uk/)
- Francis Cram, [MapStand](https://mapstand.com/)

## Copyright

Open Site Energy Toolkit
Copyright (c) Open Site Energy, 2026
Released under MIT License

Developed by Stefan Haselwimmer
