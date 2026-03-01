import os
import subprocess
import json
import logging
import pyogrio
import sqlite3
import tempfile
from pathlib import Path
from pyproj import CRS
from psycopg2 import sql, Error
from opensite.processing.base import ProcessBase
from opensite.constants import OpenSiteConstants
from opensite.logging.opensite import OpenSiteLogger
from opensite.postgis.opensite import OpenSitePostGIS

class OpenSiteImporter(ProcessBase):
    def __init__(self, node, log_level=logging.INFO, shared_lock=None, shared_metadata=None):
        super().__init__(node, log_level=log_level, shared_lock=shared_lock, shared_metadata=shared_metadata)
        self.log = OpenSiteLogger("OpenSiteImporter", log_level, shared_lock)
        self.base_path = OpenSiteConstants.DOWNLOAD_FOLDER
        self.postgis = OpenSitePostGIS(log_level)

    def get_projection(self, file_path, name):
        """
        Gets CRS of file
        Due to problems with CRS on some data sources, we have to adopt ad-hoc approach
        """

        if file_path.endswith('.gpkg'): 
            return self.get_gpkg_projection(file_path)

        if file_path.endswith('.shp'):
            meta = pyogrio.read_info(file_path)
            crs = CRS.from_user_input(meta['crs'])
            epsg_code = crs.to_epsg(min_confidence=0)
            if epsg_code: return f"EPSG:{epsg_code}"
            return None

        if file_path.endswith('.geojson'): 
            
            # Check GeoJSON for crs
            # If missing and in Northern Ireland, use EPSG:29903
            # If missing and not in Northern Ireland, use EPSG:27700

            orig_srs = OpenSiteConstants.CRS_GEOJSON
            json_data = json.load(open(file_path))

            if 'crs' in json_data:
                orig_srs = json_data['crs']['properties']['name'].replace('urn:ogc:def:crs:', '').replace('::', ':').replace('OGC:1.3:CRS84', 'EPSG:4326')
            else:

                # DataMapWales' GeoJSON use EPSG:27700 even though default SRS for GeoJSON is EPSG:4326
                if name.endswith('--wales'): orig_srs = 'EPSG:27700'

                # Improvement Service GeoJSON uses EPSG:27700
                if name == 'local-nature-reserves--scotland': orig_srs = 'EPSG:27700'

                # Northern Ireland could be in correct GeoJSON without explicit crs (so EPSG:4326) or could be incorrect non-EPSG:4326 meters with non GB datum
                if name.endswith('--northern-ireland'): orig_srs = 'EPSG:29903'
                # ... so provide exceptions
                if name == 'world-heritage-sites--northern-ireland': orig_srs = 'EPSG:4326'

            return orig_srs

        return None

    def get_gpkg_projection(self, gpkg_path):
        """
        Gets projection of GPKG
        """

        if not Path(gpkg_path).exists(): return None

        with sqlite3.connect(gpkg_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("select a.srs_id from gpkg_contents as a;")
            result = cursor.fetchall()
            if len(result) == 0:
                self.log.error(f"{gpkg_path} has no layers - deleting")
                os.remove(gpkg_path)
                return None
            else:
                firstrow = result[0]
                return 'EPSG:' + str(dict(firstrow)['srs_id'])
  
    def sanitize_geojson_inplace(self, file_path, s_epsg):
        """
        Cleans the GeoJSON, overwrites the original, and logs via self.log.
        Returns: True if features were removed, False if no changes were made.
        """
        if not os.path.exists(file_path):
            self.log.error(f"File {file_path} not found.")
            return False

        # Load data
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
        except Exception as e:
            self.log.error(f"Failed to parse JSON for {file_path}: {e}")
            return False

        original_count = len(data.get('features', []))
        INF_THRESHOLD = 1e300
        clean_features = []

        # Recursive check for nested coordinates
        def is_coord_valid(c):
            if isinstance(c, (int, float)):
                return abs(c) < INF_THRESHOLD
            return all(is_coord_valid(sub) for sub in c)

        # Filter
        for feature in data.get('features', []):
            geom = feature.get('geometry')
            if geom and 'coordinates' in geom:
                if is_coord_valid(geom['coordinates']):
                    clean_features.append(feature)

        new_count = len(clean_features)
        has_changed = new_count < original_count

        # Action
        if not has_changed:
            self.log.info(f"No invalid features found in {file_path}. No changes made.")
            return False

        if new_count == 0 and original_count > 0:
            self.log.error(f"Sanitization would remove ALL features from {file_path}. Aborting.")
            return False

        # Atomic Write-back
        try:
            # Create temp file in same directory
            fd, temp_path = tempfile.mkstemp(dir=os.path.dirname(file_path))
            with os.fdopen(fd, 'w') as tmp:
                data['features'] = clean_features
                json.dump(data, tmp)
            
            os.replace(temp_path, file_path)
            self.log.info(f"Sanitized {file_path}: Removed {original_count - new_count} features.")
            return True
            
        except Exception as e:
            if 'temp_path' in locals() and os.path.exists(temp_path):
                os.remove(temp_path)
            self.log.error(f"Failed to write sanitized file {file_path}: {e}")
            return False

    def run(self):
        """
        Imports spatial files into PostGIS, resolving variables if needed
        """

        if self.postgis.table_exists(self.node.output):
            self.log.info(f"[{self.node.output}] table already exists, skipping import")
            self.node.status = 'processed'
            return True

        input_file = str(Path(self.base_path) / self.node.input)

        if not input_file or not os.path.exists(input_file):
            self.log.error(f"Import failed: File not found for input '{input_file}'")
            self.node.status = 'failed'
            return False

        # Connection and Validation
        pg_conn = self.postgis.get_ogr_connection_string()
        input_projection = self.get_projection(input_file, self.node.name)

        # Base ogr2ogr Command
        cmd = [
            "ogr2ogr",
            "-f", "PostgreSQL",
            pg_conn,
            input_file,
            "-makevalid",
            "-overwrite",
            "-lco", "GEOMETRY_NAME=geom",
            "-lco", "PRECISION=NO",
            "-nln", self.node.output,
            "-nlt", "PROMOTE_TO_MULTI",
            "-s_srs", input_projection, 
            "-t_srs", OpenSiteConstants.CRS_DEFAULT, 
            "--config", "PG_USE_COPY", "YES"
        ]

        sql_where_clause = None

        # Historic England Conservation Areas includes 'no data' polygons so remove as too restrictive
        if self.node.name == 'conservation-areas--england': sql_where_clause = "Name NOT LIKE 'No data%'"
        else:
            if 'filter' in self.node.custom_properties:
                sql_where_clause = ''
                filter = self.node.custom_properties['filter']
                total = len(filter['values'])
                for i, item in enumerate(filter['values']):
                    sql_where_clause += f"{filter['field']}='{item}'"
                    if i != total - 1:
                        sql_where_clause += " OR "

        if sql_where_clause is not None:
            for extraitem in ["-dialect", "sqlite", "-where", sql_where_clause]:
                cmd.append(extraitem)

        for extraconfig in ["--config", "OGR_PG_ENABLE_METADATA", "NO"]: cmd.append(extraconfig)

        # Format-Specific Logic
        if self.node.format == OpenSiteConstants.OSM_YML_FORMAT:

            # Layer name within GPKG is defined in osm-export-tool YML file topmost variable
            yaml_path = str(Path(self.base_path) / self.node.custom_properties['yml'])
            osm_export_tool_layer_name = self.get_top_variable(yaml_path)
            self.log.debug(f"Resolved osm-export-tool variable using {os.path.basename(yaml_path)} to layer name: {osm_export_tool_layer_name}")

            # In ogr2ogr, the layer name follows the input file
            cmd.insert(5, osm_export_tool_layer_name)
            self.log.info(f"Importing OSM layer '{osm_export_tool_layer_name}' to '{self.node.output} from {os.path.basename(input_file)}")

        else:
            # For GeoJSON or other single-layer files, just set the table name
            self.log.info(f"Importing file {os.path.basename(input_file)} to table '{self.node.output}'")

        try:
            # Execute shell command
            subprocess.run(cmd, capture_output=True, text=True, check=True)

            postgis = OpenSitePostGIS()

            # If CKAN dataset has extra attribute 'preprocess' = 'closed_lines_to_polygons' then 
            # custom_properties['preprocess'] == 'closed_lines_to_polygons' and perform extra processing
            # to convert closed lines to polygons. This is typically required for some solar farms
            if 'preprocess' in self.node.custom_properties:
                if self.node.custom_properties['preprocess'] == 'closed_lines_to_polygons':
                    self.log.info(f"[{self.node.name}] Dataset has custom attribute 'preprocess' = 'closed_lines_to_polygons' so converting closed lines to polygons")
                    dbparams = {'table': sql.Identifier(self.node.output)}
                    postgis.execute_query(sql.SQL("""
                    UPDATE {table} SET geom = ST_CollectionExtract(ST_MakeValid(ST_BuildArea(geom)), 3)
                    WHERE ST_GeometryType(geom) LIKE '%LineString%' AND ST_IsClosed(geom)""").format(**dbparams))
                    self.log.info(f"[{self.node.name}] Converting closed lines to polygons: COMPLETED")

            postgis.add_table_comment(self.node.output, self.node.name)

            # We don't track internal tables in registry so if it's one, return True
            if self.node.output.startswith(OpenSiteConstants.DATABASE_BASE): return True

            # Success Gate: Only update registry now
            if self.postgis.set_table_completed(self.node.output):
                self.log.info(f"Import and registry update complete for {os.path.basename(input_file)} into table {self.node.output}")
                return True
            else:
                # This catches the bug where the node was never registered initially
                self.log.error(f"Import succeeded but registry record for {self.node.output} was not found.")
                return False
            
        except subprocess.CalledProcessError as e:

            # If errors with ogr2ogr, there may be invalid geometries in file 
            # eg. points outside CRS, so attempt to remove elements and retry
            if input_file.endswith('.geojson'):
                if self.sanitize_geojson_inplace(input_file, input_projection):
                    self.log.info(f"sanitize_geojson_inplace removed problem geometries so rerunning ogr2ogr on {os.path.basename(input_file)}")
                    return self.run()

            self.log.error(f"PostGIS Import Error: {os.path.basename(input_file)} {' '.join(cmd)} {e.stderr}")

            return False