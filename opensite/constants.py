import os
import logging
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

class OpenSiteConstants:
    """Arbitrary application constants that don't change per environment."""

    # Version of opensite application
    OPENSITEENERGY_VERSION      = "1.0"

    # Text used when creating tables and files
    OPENSITEENERGY_SHORTNAME    = 'opensiteenergy'
    
    # Directory script is run from
    WORKING_FOLDER              = str(Path(__file__).absolute().parent) + '/'

    # Redirect ogr2ogr warnings to log file
    os.environ['CPL_LOG']       = WORKING_FOLDER + 'log-ogr2ogr.log'

    # Default logging level for entire application
    LOGGING_LEVEL               = logging.DEBUG

    # Logging file
    LOGGING_FILE                = f"{OPENSITEENERGY_SHORTNAME}.log"
    
    # Docker tileserver name
    DOCKER_TILESERVER_NAME      = f"{OPENSITEENERGY_SHORTNAME}-tileserver"

    # How many seconds to update console
    DOWNLOAD_INTERVAL_TIME      = 10

    # Default CRS for spatial operations
    # Use EPSG:25830 for maximum precision across United Kingdom
    CRS_DEFAULT                 = 'EPSG:25830'

    # GeoJSON default CRS
    CRS_GEOJSON                 = 'EPSG:4326'

    # CRS of all exported GIS files
    CRS_OUTPUT                  = 'EPSG:4326'

    # Format text used by CKAN to indicate osm-export-tool YML file
    OSM_YML_FORMAT              = "osm-export-tool YML"

    # Format text used by CKAN to indicate Open Site Energy YML file
    SITES_YML_FORMAT            = "Open Site Energy YML"

    # Format text used by CKAN to indicate Open Library YML file
    OPENLIBRARY_YML_FORMAT      = "Open Library YML"

    # 'User-Agent' to use when downloading datasets via WFS
    WFS_USER_AGENT              = "openwindenergy/*"

    # Location of Certbot log file
    CERTBOT_LOG                 = 'log-certbot.txt'

    # Location of Domain name file
    DOMAIN_FILE                 = 'DOMAIN'

    # CKAN formats we can accept
    CKAN_FORMATS                = \
                                [
                                    'GPKG', 
                                    'ArcGIS GeoServices REST API', 
                                    'GeoJSON', 
                                    'WFS', 
                                    'KML',
                                    'SHP',
                                    OSM_YML_FORMAT, 
                                    OPENLIBRARY_YML_FORMAT,
                                    SITES_YML_FORMAT, 
                                ]

    # CKAN formats we can download using default downloader
    CKAN_DEFAULT_DOWNLOADER     = \
                                [
                                    'OSM',
                                    'GPKG',
                                    'GeoJSON',
                                    OSM_YML_FORMAT, 
                                    SITES_YML_FORMAT, 
                                ]

    # File extensions we should expect from downloading these different CKAN formats
    CKAN_FILE_EXTENSIONS        = \
                                {
                                    'GPKG' : 'gpkg', 
                                    'ArcGIS GeoServices REST API': 'geojson', 
                                    'GeoJSON': 'geojson', 
                                    'WFS': 'gpkg', 
                                    'KML': 'geojson',
                                    'SHP': 'shp',
                                    OSM_YML_FORMAT: 'yml', 
                                    OPENLIBRARY_YML_FORMAT: 'yml',
                                    SITES_YML_FORMAT: 'yml', 
                                }

    # Priority of downloads
    DOWNLOADS_PRIORITY          = \
                                [
                                    'OSM',
                                    SITES_YML_FORMAT,
                                    OSM_YML_FORMAT,
                                ]
    
    # Formats to always download - typically small and may be subject to regular change
    ALWAYS_DOWNLOAD             = \
                                [
                                    SITES_YML_FORMAT,
                                    OSM_YML_FORMAT,
                                ]

    # OSM-related formats - so they all go in same folder
    OSM_RELATED_FORMATS         = \
                                [
                                    'OSM',
                                    OSM_YML_FORMAT,
                                ]

    # Location of clipping master file
    CLIPPING_MASTER             = 'clipping-master-' + CRS_DEFAULT.replace(':', '-') + '.gpkg'

    # Root build directory
    BUILD_ROOT                  = Path(os.getenv("BUILD_FOLDER", "build"))
    
    # Sub-directories
    OSM_SUBFOLDER               = "osm"
    OPENLIBRARY_SUBFOLDER       = "openlibrary"
    CONFIGS_FOLDER              = BUILD_ROOT / 'configs'
    BUILD_CONFIG                = CONFIGS_FOLDER / f"{OPENSITEENERGY_SHORTNAME}.json"
    DOWNLOAD_FOLDER             = BUILD_ROOT / "downloads"
    OSM_DOWNLOAD_FOLDER         = DOWNLOAD_FOLDER / OSM_SUBFOLDER
    OPENLIBRARY_DOWNLOAD_FOLDER = DOWNLOAD_FOLDER / OPENLIBRARY_SUBFOLDER
    CACHE_FOLDER                = BUILD_ROOT / "cache"
    LOG_FOLDER                  = BUILD_ROOT / "logs"
    OUTPUT_FOLDER               = BUILD_ROOT / "output"
    OUTPUT_LAYERS_FOLDER        = OUTPUT_FOLDER / "layers"
    OUTPUT_BASEMAP_FOLDER       = OUTPUT_FOLDER / "basemap"
    INSTALL_FOLDER              = BUILD_ROOT / "install"
    PROCESSING_WEB_FOLDER       = Path("web")
    
    # Location 
    # ------------------------------------------------------------
    # Tilemaker-related properties
    # ------------------------------------------------------------

    TILESERVER_FOLDER_SRC       = Path("tileserver")
    TILESERVER_OUTPUT_FOLDER    = BUILD_ROOT / "tileserver-staging"
    TILESERVER_LIVE_FOLDER      = BUILD_ROOT / "tileserver-live"
    TILESERVER_LIVE_CONFIG_FILE = TILESERVER_LIVE_FOLDER / 'config.json'
    TILESERVER_DEPRECATED_FOLDER= BUILD_ROOT / "tileserver-deprecated"
    BASEMAP_FOLDER_SRC          = TILESERVER_FOLDER_SRC / "basemap"
    BASEMAP_FOLDER_DEST         = INSTALL_FOLDER / "tileserver-basemap"
    TILESERVER_SPRITES_SRC      = TILESERVER_FOLDER_SRC / "sprites"
    TILESERVER_SPRITES_DEST     = TILESERVER_OUTPUT_FOLDER / "sprites"
    TILESERVER_CONFIG_FILE      = TILESERVER_OUTPUT_FOLDER / 'config.json'
    TILESERVER_DATA_FOLDER      = TILESERVER_OUTPUT_FOLDER / "data"
    TILESERVER_STYLES_FOLDER    = TILESERVER_OUTPUT_FOLDER / "styles"
    TILESERVER_MAIN_STYLE_FILE  = TILESERVER_STYLES_FOLDER / 'opensiteenergy.json'
    TILESERVER_FONTS_FOLDER     = TILESERVER_OUTPUT_FOLDER / "fonts"
    TILESERVER_FONTS_GITHUB     = "https://github.com/opensiteenergy/openmaptiles-fonts.git"
    TILESERVER_URL              = os.getenv("TILESERVER_URL", "http://localhost:8080")

    # Location of shell script that downloads coastline and landcover data for whole earth
    SHELL_COASTLINE_LANDCOVER   = 'get-coastline-landcover.sh'

    # United Kingdom padded bounding box
    TILEMAKER_BBOX_UK           = "-49.262695,38.548165,39.990234,64.848937"

    # Entire world bounding box
    TILEMAKER_BBOX_WORLD        = "-180,-85,180,85"

    # Tilemaker build configuration files
    TILEMAKER_COASTLINE_CONFIG  = BASEMAP_FOLDER_DEST / 'config-coastline.json'
    TILEMAKER_COASTLINE_PROCESS = BASEMAP_FOLDER_DEST / 'process-coastline.lua'
    TILEMAKER_OMT_CONFIG        = BASEMAP_FOLDER_DEST / 'config-openmaptiles.json'
    TILEMAKER_OMT_PROCESS       = BASEMAP_FOLDER_DEST / 'process-openmaptiles.lua'

    # ------------------------------------------------------------
    # Processing state files - used by server implementation
    # ------------------------------------------------------------

    # State file to indicate processing is happening
    PROCESSING_STATE_FILE       = 'PROCESSING'

    # State file showing command line submitted
    PROCESSING_CMD_FILE         = 'PROCESSINGCMD'

    # State file to indicate processing has started
    # Contains start time during processing
    PROCESSING_START_FILE       = 'PROCESSINGSTART'

    # State file to indicate processing has completed
    # Contains start and end times after processing has completed
    PROCESSING_COMPLETE_FILE    = 'PROCESSINGCOMPLETE'


    # Acceptable CLI properties
    TREE_BRANCH_PROPERTIES      = \
                                {
                                    'functions':    [
                                                        'height-to-tip', 
                                                        'blade-radius'
                                                    ],
                                    'default':      [
                                                        'title', 
                                                        'type', 
                                                        'clipping-path', 
                                                        'osm',
                                                        'ckan',
                                                    ]
                                }

    # Processing grid is used to cut up core datasets into grid squares
    # to reduce memory load on ST_Union. All final layers will have ST_Union
    # so it's okay to cut up early datasets before this
    GRID_PROCESSING_SPACING     = 100 * 1000 # Size of grid squares in metres, ie. 100km

    # Output grid is used to cut up final output into grid squares 
    # in order to improve quality and performance of rendering 
    GRID_OUTPUT_SPACING_KM      = 100 # Size of grid squares in kilometres
    GRID_OUTPUT_SPACING         = GRID_OUTPUT_SPACING_KM * 1000 

    # Basename of OSM boundaries files
    # If [basename].gpkg file doesn't exist, processing nodes will be added to create it
    OSM_BOUNDARIES              = 'osm-boundaries'

    # Location of OSM boundaries osm-export-tool YML file
    OSM_BOUNDARIES_YML          = OSM_BOUNDARIES + '.yml'

    # Database tables
    DATABASE_GENERAL_PREFIX     = f"opensite_"
    DATABASE_BASE               = f"_{DATABASE_GENERAL_PREFIX}" 
    OPENSITE_REGISTRY           = DATABASE_BASE + 'registry'
    OPENSITE_BRANCH             = DATABASE_BASE + 'branch'
    OPENSITE_OUTPUTS            = DATABASE_BASE + 'outputs'
    OPENSITE_CLIPPINGMASTER     = DATABASE_BASE + 'clipping_master'
    OPENSITE_CLIPPINGTEMP       = DATABASE_BASE + 'clipping_temp'
    OPENSITE_GRIDPROCESSING     = DATABASE_BASE + 'grid_processing'
    OPENSITE_GRIDBUFFEDGES      = OPENSITE_GRIDPROCESSING + '_buffered_edges'
    OPENSITE_GRIDOUTPUT         = DATABASE_BASE + f"grid_output_{GRID_OUTPUT_SPACING_KM}"
    OPENSITE_OSMBOUNDARIES      = DATABASE_BASE + OSM_BOUNDARIES.replace('-', '_')

    # Lookup to convert internal areas to OSM names
    OSM_NAME_CONVERT            = \
                                {
                                    'england': 'England',
                                    'wales': 'Cymru / Wales',
                                    'Wales': 'Cymru / Wales',
                                    'scotland': 'Alba / Scotland',
                                    'Scotland': 'Alba / Scotland',
                                    'northern-ireland': 'Northern Ireland / Tuaisceart Éireann',
                                    'Northern Ireland': 'Northern Ireland / Tuaisceart Éireann'
                                }
    
    # All folders that need to be created at run time
    ALL_FOLDERS                 = \
                                [
                                    BUILD_ROOT,
                                    DOWNLOAD_FOLDER,
                                    OSM_DOWNLOAD_FOLDER,
                                    OPENLIBRARY_DOWNLOAD_FOLDER,
                                    CACHE_FOLDER,
                                    LOG_FOLDER,
                                    OUTPUT_FOLDER,
                                    OUTPUT_LAYERS_FOLDER,
                                    INSTALL_FOLDER,
                                    TILESERVER_OUTPUT_FOLDER,
                                    TILESERVER_DATA_FOLDER,
                                    TILESERVER_STYLES_FOLDER,
                                ]
