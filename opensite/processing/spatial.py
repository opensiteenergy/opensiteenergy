import os
import subprocess
import hashlib
import json
import logging
import time
import datetime
from pathlib import Path
from psycopg2 import sql, Error
from opensite.constants import OpenSiteConstants
from opensite.processing.base import ProcessBase
from opensite.logging.opensite import OpenSiteLogger
from opensite.postgis.opensite import OpenSitePostGIS
from opensite.model.graph.opensite import OpenSiteGraph

PROCESSINGGRID_SQUARE_IDS = None

class OpenSiteSpatial(ProcessBase):

    PROCESSING_INTERVAL_TIME = 5

    def __init__(self, node, log_level=logging.INFO, shared_lock=None, shared_metadata=None):
        super().__init__(node, log_level=log_level, shared_lock=shared_lock, shared_metadata=shared_metadata)
        self.log = OpenSiteLogger("OpenSiteSpatial", log_level, shared_lock)
        self.base_path = OpenSiteConstants.DOWNLOAD_FOLDER
        self.postgis = OpenSitePostGIS(log_level)
        
    def get_crs_default(self):
        """
        Get default CRS as number - for use in PostGIS
        """

        return OpenSiteConstants.CRS_DEFAULT.replace('EPSG:', '')
    
    def get_crs_output(self):
        """
        Get output CRS as number - for use in PostGIS
        """

        return OpenSiteConstants.CRS_OUTPUT.replace('EPSG:', '')

    def import_clipping_master(self):
        """
        Imports clipping master if not already imported
        """

        clipping_master_file = OpenSiteConstants.CLIPPING_MASTER
        clipping_temp_table = OpenSiteConstants.OPENSITE_CLIPPINGTEMP
        clipping_master_table = OpenSiteConstants.OPENSITE_CLIPPINGMASTER
        dbparams = {
            "crs": sql.Literal(self.get_crs_default()),
            'clipping_temp': sql.Identifier(clipping_temp_table),
            'clipping_master': sql.Identifier(clipping_master_table),
            "clipping_master_index": sql.Identifier(f"{clipping_master_table}_idx"),
        }
        query_create_clipping_master = sql.SQL("CREATE TABLE {clipping_master} (geom GEOMETRY(MultiPolygon, {crs}))").format(**dbparams)
        query_union_to_clipping_master = sql.SQL("INSERT INTO {clipping_master} SELECT ST_Union(geom) FROM {clipping_temp}").format(**dbparams)
        query_clipping_master_create_index = sql.SQL("CREATE INDEX {clipping_master_index} ON {clipping_master} USING GIST (geom)").format(**dbparams)

        if self.postgis.table_exists(clipping_master_table): return True

        self.log.info("[import_clipping_master] Importing clipping file")

        try:

            self.postgis.drop_table(clipping_temp_table)
            if not self.postgis.import_spatial_data(clipping_master_file, clipping_temp_table):
                self.log.error("[import_clipping_master] Unable to import clipping_master file to clipping_temp table")
                return False

            self.log.info("[import_clipping_master] Unioning clipping file and dropping temp table")

            self.postgis.execute_query(query_create_clipping_master)
            self.postgis.execute_query(query_union_to_clipping_master)
            self.postgis.execute_query(query_clipping_master_create_index)
            self.postgis.drop_table(clipping_temp_table)

            self.log.info("[import_clipping_master] Clipping file processing completed")

            return True
        except Error as e:
            self.log.error(f"[import_clipping_master] PostGIS error: {e}")
            return False
        except Exception as e:
            self.log.error(f"[import_clipping_master] Unexpected error: {e}")
            return False

    def create_processing_grid(self):
        """
        Creates processing grid
        Due to issues with calling this within parallel processor setting
        this should be called during main application initialization
        """

        global PROCESSINGGRID_SQUARE_IDS

        if not self.import_clipping_master():
            self.log.error(f"Problem importing clipping master")
            return False

        if self.postgis.table_exists(OpenSiteConstants.OPENSITE_GRIDPROCESSING):
            self.log.info("Processing grid already exists")
            self.get_processing_grid_square_ids()
            return True

        self.log.info(f"[create_processing_grid] Creating grid overlay with grid size {OpenSiteConstants.GRID_PROCESSING_SPACING} to reduce memory load during ST_Union")

        dbparams = {
            "crs": sql.Literal(self.get_crs_default()),
            "grid": sql.Identifier(OpenSiteConstants.OPENSITE_GRIDPROCESSING),
            "grid_index": sql.Identifier(f"{OpenSiteConstants.OPENSITE_GRIDPROCESSING}_idx"),
            "grid_spacing": sql.Literal(OpenSiteConstants.GRID_PROCESSING_SPACING),
            "clipping_master": sql.Identifier(OpenSiteConstants.OPENSITE_CLIPPINGMASTER)
        }

        query_grid_create = sql.SQL("""
        CREATE TABLE {grid} AS 
        SELECT 
            (ST_SquareGrid({grid_spacing}, ST_SetSRID(extent_geom, {crs}))).geom::geometry(Polygon, {crs}) as geom
        FROM (
            SELECT ST_Extent(geom)::geometry as extent_geom 
            FROM {clipping_master}
        ) AS sub;
        """).format(**dbparams)
        query_grid_alter = sql.SQL("ALTER TABLE {grid} ADD COLUMN id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY").format(**dbparams)
        query_grid_create_index = sql.SQL("CREATE INDEX {grid_index} ON {grid} USING GIST (geom)").format(**dbparams)
        query_grid_delete_squares = sql.SQL("DELETE FROM {grid} g WHERE NOT EXISTS (SELECT 1 FROM {clipping_master} c WHERE ST_Intersects(g.geom, c.geom))").format(**dbparams)

        try:
            self.postgis.execute_query(query_grid_create)
            self.postgis.execute_query(query_grid_alter)
            self.postgis.execute_query(query_grid_delete_squares)
            self.postgis.execute_query(query_grid_create_index)
            self.get_processing_grid_square_ids()

            self.log.info(f"[create_processing_grid] Finished creating grid overlay with grid size {OpenSiteConstants.GRID_PROCESSING_SPACING}")

            return True
        except Error as e:
            self.log.error(f"[create_processing_grid] PostGIS Error during grid creation: {e}")
            return False
        except Exception as e:
            self.log.error(f"[create_processing_grid] Unexpected error: {e}")
            return False

    def create_processing_grid_buffered_edges(self):
        """
        Creates buffered edges from processing grid
        """

        if not self.postgis.table_exists(OpenSiteConstants.OPENSITE_GRIDPROCESSING):
            self.create_processing_grid()

        if self.postgis.table_exists(OpenSiteConstants.OPENSITE_GRIDBUFFEDGES):
            self.log.info("Buffered grid already exists")
            return True

        self.log.info(f"[create_processing_grid_buffered_edges] Creating buffered edges from processing grid")

        dbparams = {
            "crs": sql.Literal(self.get_crs_default()),
            "grid": sql.Identifier(OpenSiteConstants.OPENSITE_GRIDPROCESSING),
            "buffered_edges": sql.Identifier(OpenSiteConstants.OPENSITE_GRIDBUFFEDGES),
            "buffered_edges_index": sql.Identifier(f"{OpenSiteConstants.OPENSITE_GRIDBUFFEDGES}_idx"),
        }

        query_buffered_edges_create = sql.SQL("""
        CREATE TABLE {buffered_edges} AS SELECT ST_Buffer(ST_Boundary(geom), 0.01)::geometry(Polygon, {crs}) as geom FROM {grid};
        CREATE INDEX {buffered_edges_index} ON {buffered_edges} USING GIST (geom);
        """).format(**dbparams)

        try:
            self.postgis.execute_query(query_buffered_edges_create)

            self.log.info(f"[create_processing_grid_buffered_edges] Finished creating buffered edges")

            return True
        except Error as e:
            self.log.error(f"[create_processing_grid_buffered_edges] PostGIS Error during buffered edge creation: {e}")
            return False
        except Exception as e:
            self.log.error(f"[create_processing_grid_buffered_edges] Unexpected error: {e}")
            return False


    def create_output_grid(self):
        """
        Creates output grid to be used when creating mbtiles to improve performance and visual quality of mbtiles
        """

        if self.postgis.table_exists(OpenSiteConstants.OPENSITE_GRIDOUTPUT):
            self.log.info("Output grid already exists")
            return True

        self.log.info(f"[create_output_grid] Creating output grid with grid size {OpenSiteConstants.GRID_OUTPUT_SPACING} to improve performance and visual quality of mbtiles")

        dbparams = {
            "crs": sql.Literal(int(self.get_crs_default())),
            "grid": sql.Identifier(OpenSiteConstants.OPENSITE_GRIDOUTPUT),
            "grid_index": sql.Identifier(f"{OpenSiteConstants.OPENSITE_GRIDOUTPUT}_idx"),
            "grid_spacing": sql.Literal(OpenSiteConstants.GRID_OUTPUT_SPACING),
            "clipping_master": sql.Identifier(OpenSiteConstants.OPENSITE_CLIPPINGMASTER)
        }

        query_grid_create = sql.SQL("""
        CREATE TABLE {grid} AS 
        SELECT 
            row_number() OVER () AS id, 
            sub.geom
        FROM (
            SELECT 
                ST_Transform(
                    (ST_SquareGrid({grid_spacing}, ST_Transform(geom, 3857))).geom, 
                    {crs}
                ) AS geom 
            FROM {clipping_master}
        ) sub;
        ALTER TABLE {grid} ADD PRIMARY KEY (id);
        CREATE INDEX ON {grid} USING GIST (geom);
        """).format(**dbparams)
        query_grid_create_index = sql.SQL("CREATE INDEX {grid_index} ON {grid} USING GIST (geom)").format(**dbparams)
        query_grid_delete_squares = sql.SQL("DELETE FROM {grid} g WHERE NOT EXISTS (SELECT 1 FROM {clipping_master} c WHERE ST_Intersects(g.geom, c.geom))").format(**dbparams)

        try:
            self.postgis.execute_query(query_grid_create)
            self.postgis.execute_query(query_grid_create_index)
            self.postgis.execute_query(query_grid_delete_squares)

            self.log.info(f"[create_output_grid] Finished creating output grid with grid size {OpenSiteConstants.GRID_OUTPUT_SPACING}")

            return True
        except Error as e:
            self.log.error(f"[create_output_grid] PostGIS Error during grid creation: {e}")
            return False
        except Exception as e:
            self.log.error(f"[create_output_grid] Unexpected error: {e}")
            return False

    def get_processing_grid_square_ids(self):
        """
        Gets ids of all squares in processing grid
        """

        global PROCESSINGGRID_SQUARE_IDS

        if not PROCESSINGGRID_SQUARE_IDS:
            if not self.postgis.table_exists(OpenSiteConstants.OPENSITE_GRIDPROCESSING):
                self.log.error("Processing grid does not exist, unable to retrieve grid square ids")
                return None
            
            results = self.postgis.fetch_all(sql.SQL("SELECT id FROM {grid}").format(grid=sql.Identifier(OpenSiteConstants.OPENSITE_GRIDPROCESSING)))
            PROCESSINGGRID_SQUARE_IDS = [row['id'] for row in results]

        return PROCESSINGGRID_SQUARE_IDS

    def buffer(self):
        """
        Adds buffer to spatial dataset 
        Buffering is always added before dataset is split into grid squares
        """
            
        if self.postgis.table_exists(self.node.output):
            self.log.info(f"[buffer] [{self.node.output}] already exists, skipping buffer for {self.node.name}")
            self.node.status = 'processed'
            return True

        if 'buffer' not in self.node.custom_properties:
            self.log.error(f"[buffer] {self.node.name} is missing 'buffer' field, buffering failed")
            self.node.status = 'failed'
            return False
         
        buffer = self.node.custom_properties['buffer']
        input_table = self.node.input
        output_table = self.node.output

        self.log.info(f"[buffer] [{self.node.name}] Adding {buffer}m buffer to {input_table} to make {output_table}")

        dbparams = {
            "input": sql.Identifier(input_table),
            "output": sql.Identifier(output_table),
            "output_index": sql.Identifier(f"{output_table}_idx"),
            "buffer": sql.Literal(buffer),
        }

        query_buffer_create = sql.SQL("CREATE TABLE {output} AS SELECT ST_Buffer(geom, {buffer}) geom FROM {input}").format(**dbparams)
        query_buffer_create_index = sql.SQL("CREATE INDEX {output_index} ON {output} USING GIST (geom)").format(**dbparams)

        # Make special exception for hedgerow as hedgerow polygons represent boundaries that should be buffered as lines
        buffer_polygons_as_lines = False
        if 'hedgerows--' in self.node.name: buffer_polygons_as_lines = True

        if buffer_polygons_as_lines:
            query_buffer_create = sql.SQL("""
            CREATE TABLE {output} AS 
            (
                (SELECT ST_Buffer(geom, {buffer}) geom 
                FROM {input} 
                WHERE ST_Dimension(geom) = 1)
                UNION ALL
                (SELECT ST_Buffer(ST_Boundary(geom), {buffer}) geom 
                FROM {input} 
                WHERE ST_Dimension(geom) = 2)
            )
            """).format(**dbparams)

        try:
            self.postgis.execute_query(query_buffer_create)
            self.postgis.execute_query(query_buffer_create_index)
            self.postgis.add_table_comment(self.node.output, self.node.name)

            # Success Gate: Only update registry now
            if self.postgis.set_table_completed(self.node.output):
                self.log.info(f"[buffer] [{self.node.name}] Finished adding {buffer}m buffer to {input_table} to make {output_table}")
                return True
            else:
                # This catches the bug where the node was never registered initially
                self.log.error(f"[buffer] Buffer added but registry record for {self.node.output} was not found.")
                return False

        except Error as e:
            self.log.error(f"[buffer] [{self.node.name}] PostGIS error during buffer creation: {e}")
            return False
        except Exception as e:
            self.log.error(f"[buffer] [{self.node.name}] Unexpected error: {e}")
            return False

    def invert(self):
        """
        Subtracts dataset from clipping path
        """
            
        if self.postgis.table_exists(self.node.output):
            self.log.info(f"[invert] [{self.node.output}] already exists, skipping invert for {self.node.name}")
            self.node.status = 'processed'
            return True

        input_table = self.node.input
        output_table = self.node.output

        self.log.info(f"[invert] Inverting {input_table} to make {output_table}")

        dbparams = {
            "crs": sql.Literal(self.get_crs_default()),
            "clip": sql.Identifier(OpenSiteConstants.OPENSITE_CLIPPINGMASTER),
            "grid": sql.Identifier(OpenSiteConstants.OPENSITE_GRIDPROCESSING),
            "input": sql.Identifier(input_table),
            "output": sql.Identifier(output_table),
            "output_index": sql.Identifier(f"{output_table}_idx"),
        }

        query_invert_create = sql.SQL("""
CREATE TABLE {output} AS
SELECT 
    m.id,
    (ST_Dump(
        ST_Difference(
            ST_Intersection(m.geom, clip.geom), 
            COALESCE(sub.geom_to_subtract, ST_GeomFromText('POLYGON EMPTY', {crs}))
        )
    )).geom AS geom
FROM 
    {grid} m
INNER JOIN 
    {clip} clip ON ST_Intersects(m.geom, clip.geom)
LEFT JOIN LATERAL (
    SELECT ST_Union(i.geom) AS geom_to_subtract
    FROM {input} i
    WHERE ST_Intersects(i.geom, m.geom)
) sub ON TRUE;
        """).format(**dbparams)
        query_invert_create_index = sql.SQL("CREATE INDEX {output_index} ON {output} USING GIST (geom)").format(**dbparams)

        try:
            self.postgis.execute_query(query_invert_create)
            self.postgis.execute_query(query_invert_create_index)
            self.postgis.add_table_comment(self.node.output, self.node.name)

            # Success Gate: Only update registry now
            if self.postgis.set_table_completed(self.node.output):
                self.log.info(f"[invert] [{self.node.name}] Finished inverting {input_table} to make {output_table}")
                return True
            else:
                # This catches the bug where the node was never registered initially
                self.log.error(f"[invert] Invert completed but registry record for {self.node.output} was not found.")
                return False

        except Error as e:
            self.log.error(f"[invert] [{self.node.name}] PostGIS error during inversion: {e}")
            return False
        except Exception as e:
            self.log.error(f"[invert] [{self.node.name}] Unexpected error: {e}")
            return False

    def distance(self):
        """
        Adds minimum distance exclusion to spatial dataset, ie. if anything is further than 'distance', it's selected 
        This requires OPENSITE_CLIPPINGMASTER to provide the bounding area for the exclusion
        """
            
        if self.postgis.table_exists(self.node.output):
            self.log.info(f"[distance] [{self.node.output}] already exists, skipping distance for {self.node.name}")
            self.node.status = 'processed'
            return True

        if 'distance' not in self.node.custom_properties:
            self.log.error(f"[distance] {self.node.name} is missing 'distance' field, distance exclusion failed")
            self.node.status = 'failed'
            return False
         
        distance = self.node.custom_properties['distance']
        input_table = self.node.input
        output_table = self.node.output

        self.log.info(f"[buffer] [{self.node.name}] Adding {distance}m distance exclusion to {input_table} to make {output_table}")

        dbparams = {
            "input": sql.Identifier(input_table),
            "clipping_master": sql.Identifier(OpenSiteConstants.OPENSITE_CLIPPINGMASTER),
            "output": sql.Identifier(output_table),
            "output_index": sql.Identifier(f"{output_table}_idx"),
            "distance": sql.Literal(distance),
        }

        query_distance_create = sql.SQL("""
        CREATE TABLE {output} AS 
            WITH exclusion AS (
                SELECT ST_Union(ST_Buffer(geom, {distance})) as geom 
                FROM {input}
            )
            SELECT 
                ROW_NUMBER() OVER () as id,
                sub.geom::geometry(MultiPolygon) as geom
            FROM (
                SELECT 
                    ST_Multi(ST_Difference(cm.geom, ex.geom)) as geom
                FROM {clipping_master} cm
                CROSS JOIN exclusion ex
            ) sub
            WHERE NOT ST_IsEmpty(sub.geom)
        """).format(**dbparams)
        query_distance_create_index = sql.SQL("CREATE INDEX {output_index} ON {output} USING GIST (geom)").format(**dbparams)

        try:
            self.postgis.execute_query(query_distance_create)
            self.postgis.execute_query(query_distance_create_index)
            self.postgis.add_table_comment(self.node.output, self.node.name)

            # Success Gate: Only update registry now
            if self.postgis.set_table_completed(self.node.output):
                self.log.info(f"[distance] [{self.node.name}] Finished adding {distance}m distance exclusion to {input_table} to make {output_table}")
                return True
            else:
                # This catches the bug where the node was never registered initially
                self.log.error(f"[distance] Distance exclusion added but registry record for {self.node.output} was not found.")
                return False

        except Error as e:
            self.log.error(f"[distance] [{self.node.name}] PostGIS error during distance exclusion creation: {e}")
            return False
        except Exception as e:
            self.log.error(f"[distance] [{self.node.name}] Unexpected error: {e}")
            return False

    def preprocess(self):
        """
        Preprocess node - dump to produce single geometry type then crop and split into grid squares
        """

        if self.postgis.table_exists(self.node.output):
            self.log.info(f"[preprocess] [{self.node.output}] already exists, skipping preprocess for {self.node.name}")
            self.node.status = 'processed'
            return True
    
        if not self.postgis.table_exists(OpenSiteConstants.OPENSITE_GRIDPROCESSING):
            self.log.info("[preprocess] Processing grid does not exist, creating it...")
            if not self.create_processing_grid():
                self.log.error(f"Failed to create processing grid, unable to preprocess {self.node.name}")
                self.node.status = 'failed'
                return False
            
        grid_table = OpenSiteConstants.OPENSITE_GRIDPROCESSING
        clip_table = OpenSiteConstants.OPENSITE_CLIPPINGMASTER
        gridsquare_ids = self.get_processing_grid_square_ids()
        scratch_table_1 = f"tmp_1_{self.node.output}_{self.node.urn}"
        scratch_table_2 = f"tmp_2_{self.node.output}_{self.node.urn}"
        snapgrid = None
        if 'snapgrid' in self.node.custom_properties:
            snapgrid = self.node.custom_properties['snapgrid']

        dbparams = {
            "crs": sql.Literal(int(self.get_crs_default())),
            "snapgrid": sql.Literal(snapgrid),
            "grid": sql.Identifier(grid_table),
            "clip": sql.Identifier(clip_table),
            "input": sql.Identifier(self.node.input),
            "scratch1": sql.Identifier(scratch_table_1),
            "scratch2": sql.Identifier(scratch_table_2),
            "output": sql.Identifier(self.node.output),
            "scratch1_index": sql.Identifier(f"{scratch_table_1}_idx"),
            "scratch2_index": sql.Identifier(f"{scratch_table_2}_idx"),
            "output_index": sql.Identifier(f"{self.node.output}_idx"),
            "output_id_index": sql.Identifier(f"{self.node.output}_id_idx"),
        }

        # Drop scratch tables
        self.postgis.drop_table(scratch_table_1)
        self.postgis.drop_table(scratch_table_2)

        # Explode geometries with ST_Dump to remove MultiPolygon,
        # MultiSurface, etc and homogenize processing
        # Ideally all dumped tables should contain polygons only (either source or buffered source is (Multi)Polygon)
        # so filter on ST_Polygon

        if snapgrid:
            query_scratch_table_1_dump_makevalid = sql.SQL("""
            CREATE TABLE {scratch1} AS 
                SELECT  ST_MakeValid(dumped.geom) geom 
                FROM    (SELECT (ST_Dump(ST_SnapToGrid(geom, {snapgrid}))).geom geom FROM {input}) dumped 
                WHERE   ST_geometrytype(dumped.geom) = 'ST_Polygon'
                """).format(**dbparams)
        else:
            query_scratch_table_1_dump_makevalid = sql.SQL("""
            CREATE TABLE {scratch1} AS 
                SELECT  ST_MakeValid(dumped.geom) geom 
                FROM    (SELECT (ST_Dump(geom)).geom geom FROM {input}) dumped 
                WHERE   ST_geometrytype(dumped.geom) = 'ST_Polygon'
            """).format(**dbparams)

        query_scratch_table_2_table_create = sql.SQL("""
        CREATE TABLE {scratch2} (
            gid SERIAL PRIMARY KEY,
            id INTEGER,
            geom GEOMETRY(Polygon, {crs}))
        """).format(**dbparams)
        # Note: we use ST_CollectionExtract(..., 3) to only select ST_Polygons from ST_Intersection
        # as with ST_SnapToGrid, we are more likely to have line segments generated by ST_Intersection
        query_scratch_table_2_table_insert = """
        INSERT INTO {scratch2} (id, geom)
            SELECT 
                grid.id, 
                (ST_Dump(
                    ST_CollectionExtract(
                        ST_Intersection(grid.geom, ST_UnaryUnion(ST_Collect(data.geom))), 
                        3
                    )
                )).geom::geometry(Polygon, {crs})
            FROM {grid} grid
            JOIN {scratch1} data ON ST_Intersects(grid.geom, data.geom)
            WHERE grid.id = {gridsquare_id}
            GROUP BY grid.id, grid.geom;"""
        query_output_create = sql.SQL("""
        CREATE TABLE {output} AS
        SELECT 
            data.id, (ST_Dump(data.geom)).geom::geometry(Polygon, {crs}) as geom
        FROM {scratch2} data
        JOIN {clip} clipper ON ST_Contains(clipper.geom, data.geom)

        UNION ALL

        SELECT 
            data.id, (ST_Dump(ST_CollectionExtract(ST_Intersection(data.geom, clipper.geom), 3))).geom::geometry(Polygon, {crs})
        FROM {scratch2} data
        JOIN {clip} clipper ON ST_Intersects(data.geom, clipper.geom) 
        AND NOT ST_Contains(clipper.geom, data.geom);
        """).format(**dbparams)
        query_scratch_table_1_index = sql.SQL("CREATE INDEX {scratch1_index} ON {scratch1} USING GIST (geom)").format(**dbparams)
        query_scratch_table_2_index = sql.SQL("CREATE INDEX {scratch2_index} ON {scratch2} USING GIST (geom)").format(**dbparams)
        query_output_index          = sql.SQL("CREATE INDEX {output_index} ON {output} USING GIST (geom)").format(**dbparams)
        query_output_id_index       = sql.SQL("CREATE INDEX {output_id_index} ON {output} (id)").format(**dbparams)

        try:
            self.log.info(f"[preprocess] [{self.node.name}] Select only polygons, dump and make valid")

            self.postgis.execute_query(query_scratch_table_1_dump_makevalid)
            self.postgis.execute_query(query_scratch_table_1_index)

            self.log.info(f"[preprocess] [{self.node.name}] Cutting data into grid squares and running ST_Union on each square")

            self.postgis.execute_query(query_scratch_table_2_table_create)

            gridsquares_index, gridsquares_count = 0, len(gridsquare_ids)
            last_log_time = time.time()

            for gridsquare_id in gridsquare_ids:
                gridsquares_index += 1

                # Progress reporting - log every PROCESSING_INTERVAL_TIME seconds to avoid flooding terminal
                current_time = time.time()
                if  (gridsquares_index == 1) or \
                    (gridsquares_index == gridsquares_count) or \
                    (current_time - last_log_time > self.PROCESSING_INTERVAL_TIME):
                    self.log.info(f"[preprocess] [{self.node.name}] Processing grid square {gridsquares_index}/{gridsquares_count}")
                    last_log_time = time.time()

                dbparams['gridsquare_id'] = sql.Literal(gridsquare_id)
                
                self.postgis.execute_query(sql.SQL(query_scratch_table_2_table_insert).format(**dbparams))

            self.postgis.execute_query(query_scratch_table_2_index)

            self.log.info(f"[preprocess] [{self.node.name}] Creating final output")

            self.postgis.execute_query(query_output_create)
            self.postgis.execute_query(query_output_index)
            self.postgis.execute_query(query_output_id_index)
            self.postgis.add_table_comment(self.node.output, self.node.name)
            self.postgis.drop_table(scratch_table_1)
            self.postgis.drop_table(scratch_table_2)

            # Success Gate: Only update registry now
            if self.postgis.set_table_completed(self.node.output):
                self.log.info(f"[preprocess] [{self.node.name}] COMPLETED")
                return True
            else:
                # This catches the bug where the node was never registered initially
                self.log.error(f"[preprocess] Preprocess completed but registry record for {self.node.output} was not found.")
                return False

            return True
        except Error as e:
            self.log.error(f"[preprocess] [{self.node.name}] PostGIS error during preprocess: {e}")
            return False
        except Exception as e:
            self.log.error(f"[preprocess] [{self.node.name}] Unexpected error: {e}")
            return False

    def amalgamate(self):
        """
        Amalgamates datasets into one
        Note: amalgamate is universally applied to all geographical subcomponents even if there's only one subcomponent
        """

        if self.postgis.table_exists(self.node.output):
            self.log.info(f"[amalgamate] [{self.node.output}] already exists, skipping amalgamate")
            self.node.status = 'processed'
            return True

        if not self.postgis.table_exists(OpenSiteConstants.OPENSITE_GRIDPROCESSING):
            self.log.info("[amalgamate] Processing grid does not exist, creating it...")
            if not self.create_processing_grid():
                self.log.error(f"[amalgamate] Failed to create processing grid, unable to amalgamate {self.node.name}")
                self.node.status = 'failed'
                return False

        inputs = self.node.input
        grid_table = OpenSiteConstants.OPENSITE_GRIDPROCESSING
        gridsquare_ids = self.get_processing_grid_square_ids()
        scratch_table_1 = f"tmp_1_{self.node.output}_{self.node.urn}"

        dbparams = {
            "crs":              sql.Literal(self.get_crs_default()),
            "grid":             sql.Identifier(grid_table),
            "scratch1":         sql.Identifier(scratch_table_1),
            "output":           sql.Identifier(self.node.output),
            "scratch1_index":   sql.Identifier(f"{scratch_table_1}_idx"),
            "output_index":     sql.Identifier(f"{self.node.output}_idx"),
            "output_id_index":  sql.Identifier(f"{self.node.output}_id_idx"),
        }

        # Drop scratch tables
        self.postgis.drop_table(scratch_table_1)

        try:
            self.log.info(f"[amalgamate] [{self.node.name}] Starting amalgamation and dissolving")

            # Create output table regardless of number of children
            self.postgis.execute_query(sql.SQL("CREATE UNLOGGED TABLE {output} (id int, geom geometry(Geometry, {crs}))").format(**dbparams))
            self.postgis.add_table_comment(self.node.output, self.node.name)

            if len(inputs) == 1:

                dbparams['input'] = sql.Identifier(inputs[0])
                self.log.info(f"[{self.node.name}] Single child so directly copying from {inputs[0]} to {self.node.output}")
                self.postgis.execute_query(sql.SQL("INSERT INTO {output} SELECT * FROM {input}").format(**dbparams))

            else:

                # Create empty tables first using UNLOGGED for speed
                self.postgis.execute_query(sql.SQL("CREATE UNLOGGED TABLE {scratch1} (id int, geom geometry(Geometry, {crs}))").format(**dbparams))
        
                # Pour each input table in one by one
                input_index = 0
                for input in inputs:
                    input_index += 1
                    dbparams['input'] = sql.Identifier(input)
                    self.log.info(f"[amalgamate] [{self.node.name}] Amalgamating child table {input_index}/{len(inputs)}")
                    query_add_table = sql.SQL("INSERT INTO {scratch1} (id, geom) SELECT id, (ST_Dump(geom)).geom FROM {input}").format(**dbparams)
                    self.postgis.execute_query(query_add_table)

                self.postgis.execute_query(sql.SQL("CREATE INDEX ON {scratch1} USING GIST (geom)").format(**dbparams))

                gridsquare_index = 0
                for gridsquare_id in gridsquare_ids:
                    gridsquare_index += 1

                    self.log.info(f"[amalgamate] [{self.node.name}] Using ST_Union to generate amalgamated grid square {gridsquare_index}/{len(gridsquare_ids)}")

                    dbparams['gridsquare_id'] = sql.Literal(gridsquare_id)
                    
                    query_union_by_gridsquare = sql.SQL("""
                        INSERT INTO {output} (id, geom)
                            SELECT grid.id, (ST_Dump(ST_Union(ST_Intersection(grid.geom, dataset.geom)))).geom FROM {grid} grid
                            INNER JOIN {scratch1} dataset ON ST_Intersects(grid.geom, dataset.geom)
                            WHERE grid.id = {gridsquare_id} AND ST_GeometryType(dataset.geom) = 'ST_Polygon' 
                            GROUP BY grid.id
                    """).format(**dbparams)

                    # # Updated Query Logic
                    # query_union_by_gridsquare = sql.SQL("""
                    #     INSERT INTO {output} (id, geom)
                    #     WITH grid_square AS (
                    #         SELECT id, geom FROM {grid} WHERE id = {gridsquare_id}
                    #     ),
                    #     intersecting_parcels AS (
                    #         SELECT d.geom 
                    #         FROM {scratch1} d
                    #         JOIN grid_square g ON ST_Intersects(d.geom, g.geom)
                    #         WHERE ST_GeometryType(d.geom) = 'ST_Polygon'
                    #     ),
                    #     overlap_check AS (
                    #         SELECT EXISTS (
                    #             SELECT 1 
                    #             FROM intersecting_parcels p1, intersecting_parcels p2
                    #             WHERE p1.ctid < p2.ctid  -- Use ctid to avoid comparing a row to itself
                    #             AND ST_Intersects(p1.geom, p2.geom)
                    #             LIMIT 1
                    #         ) as needs_union
                    #     )
                    #     SELECT 
                    #         (SELECT id FROM grid_square),
                    #         CASE 
                    #             WHEN (SELECT needs_union FROM overlap_check) THEN
                    #                 (ST_Dump(ST_Union(ST_Intersection(g.geom, p.geom)))).geom
                    #             ELSE
                    #                 ST_Intersection(g.geom, p.geom)
                    #         END
                    #     FROM grid_square g
                    #     CROSS JOIN intersecting_parcels p
                    #     CROSS JOIN overlap_check
                    #     GROUP BY g.id, p.geom, needs_union;
                    # """).format(**dbparams)
                    self.postgis.execute_query(query_union_by_gridsquare)

            self.postgis.execute_query(sql.SQL("CREATE INDEX ON {output} USING GIST (geom)").format(**dbparams))
            self.postgis.execute_query(sql.SQL("CREATE INDEX {output_id_index} ON {output} (id)").format(**dbparams))
            self.postgis.execute_query(sql.SQL("DELETE FROM {output} WHERE ST_GeometryType(geom) NOT IN ('ST_Polygon')").format(**dbparams))

            self.postgis.drop_table(scratch_table_1)
            self.postgis.add_table_comment(self.node.output, self.node.name)

            # Success Gate: Only update registry now
            # Register new table manually as output uses variable ()
            self.postgis.register_node(self.node)
            if self.postgis.set_table_completed(self.node.output):
                self.log.info(f"[amalgamate] [{self.node.name}] COMPLETED")
                return True
            else:
                # This catches the bug where the node was never registered initially
                self.log.error(f"[amalgamate] Amalgamate completed but registry record for {self.node.output} was not found.")
                return False

        except Error as e:
            self.log.error(f"[amalgamate] [{self.node.name}] PostGIS error during amalgamation: {e}")
            return False
        except Exception as e:
            self.log.error(f"[amalgamate] [{self.node.name}] Unexpected error: {e}")
            return False

    def generatehash(self, content):
        """
        Generates (semi-)unique database table name using hash from content
        """

        content_hash = hashlib.md5(content.encode()).hexdigest()

        return f"{OpenSiteConstants.DATABASE_GENERAL_PREFIX}{content_hash}"

    def parse_output_node_name(self, name):
        """
        Parses node name for output-focused nodes
        Nodes after amalgamate (postprocess, clip) are output-focused nodes and have slightly different names:
        [branch_name]--[normal_dataset_name]
        """

        name_elements = name.split('--')
        return {'name': '--'.join(name_elements[1:]), 'branch': name_elements[0]}

    def postprocess(self):
        """
        Postprocess node - join all grid squares together
        We assume each postprocess node has exactly one child, 
        ie. if postprocessing is needed on multiple children, insert amalgamate as single child 
        """

        name_elements = self.parse_output_node_name(self.node.name)
        self.node.name = name_elements['name']

        # Generate scratch table names
        def scratch(idx): return f"tmp_{idx}_{self.node.output}_{self.node.urn}"
        
        table_seams = scratch(0) # Just the polygons touching edges
        table_islands = scratch(1) # Polygons safely away from edges
        table_welded = scratch(2) # The result of the union
        
        dbparams = {
            "crs": sql.Literal(self.get_crs_default()),
            "input": sql.Identifier(self.node.input),
            "output": sql.Identifier(self.node.output),
            "buffered_edges": sql.Identifier(OpenSiteConstants.OPENSITE_GRIDBUFFEDGES),
            "table_seams": sql.Identifier(table_seams),
            "table_islands": sql.Identifier(table_islands),
            "table_welded": sql.Identifier(table_welded),
        }

        if self.postgis.table_exists(self.node.output):
            self.log.info(f"[postprocess] [{self.node.output}] already exists, skipping postprocess")
            return True

        try:

            all_scratch_tables = [table_seams, table_islands, table_welded]

            def cleanup():
                for t in all_scratch_tables:
                    self.postgis.drop_table(t)

            cleanup()
            self.postgis.drop_table(self.node.output)

            # --- STEP 1: Isolate Seam Candidates ---
            self.log.info(f"[postprocess] [{self.node.name}] Step 1: Extracting seam candidates...")
            start = datetime.datetime.now()
            self.postgis.execute_query(sql.SQL("""
            CREATE TABLE {table_seams} AS
            SELECT a.geom AS geom FROM {input} a WHERE EXISTS (SELECT 1 FROM {buffered_edges} b WHERE ST_Intersects(a.geom, b.geom))""").format(**dbparams))
            self.log.info(f"[postprocess] [{self.node.name}] Step 1: COMPLETED in {datetime.datetime.now() - start}")

            # --- STEP 2: Isolate Islands ---
            self.log.info(f"[postprocess] [{self.node.name}] Step 2: Isolating islands...")
            start = datetime.datetime.now()
            self.postgis.execute_query(sql.SQL("""
            CREATE TABLE {table_islands} AS
            SELECT a.geom AS geom FROM {input} a WHERE NOT EXISTS (SELECT 1 FROM {buffered_edges} b WHERE ST_Intersects(a.geom, b.geom))""").format(**dbparams))
            self.log.info(f"[postprocess] [{self.node.name}] Step 2: COMPLETED in {datetime.datetime.now() - start}")

            # --- STEP 3: Weld seams ---
            self.log.info(f"[postprocess] [{self.node.name}] Step 3: Unioning / welding seam geometries...")
            start = datetime.datetime.now()
            strategy = "CONVENTIONAL"

            # EXECUTION: Conventional Path (Fast)
            if strategy == "CONVENTIONAL":
                self.log.info(f"[postprocess] [{self.node.name}] Strategy: {strategy}")
                try:
                    self.postgis.execute_query(sql.SQL("CREATE TABLE {table_welded} AS SELECT ST_Union(geom) AS geom FROM {table_seams}").format(**dbparams))
                except Exception as e:
                    self.log.warning(f"[postprocess] [{self.node.name}] Conventional weld failed - geometry too complex for PostGIS so copying over gridded data to target table")
                    strategy = "KEEPGRIDDED"
                    self.postgis.execute_query(sql.SQL("DROP TABLE IF EXISTS {table_welded}").format(**dbparams))

            # EXECUTION: Copy table_seams to table_welded unchanged
            # We tried but PostGIS unable to handle ST_Union on a dataset (possibly too many vertices)
            if strategy == "KEEPGRIDDED":
                self.log.info(f"[postprocess] [{self.node.name}] Strategy: {strategy}")
                try:
                    self.postgis.execute_query(sql.SQL("CREATE TABLE {table_welded} AS SELECT geom FROM {table_seams}").format(**dbparams))
                except Exception as e:
                    self.log.warning(f"[postprocess] [{self.node.name}] Unable to copy over gridded data to target table")
                    cleanup()
                    return False

            self.log.info(f"[postprocess] [{self.node.name}] Step 3: COMPLETED in {datetime.datetime.now() - start}")

            # --- STEP 4: Final assembly ---
            self.log.info(f"[postprocess] [{self.node.name}] Step 4: Finalizing output...")
            self.postgis.execute_query(sql.SQL("""
            CREATE TABLE {output} AS
            SELECT geom FROM {table_welded}
            UNION ALL
            SELECT geom FROM {table_islands};
            CREATE INDEX ON {output} USING GIST (geom);
            """).format(**dbparams))

            cleanup()
            self.log.info(f"[postprocess] [{self.node.name}] Success")

            self.postgis.add_table_comment(self.node.output, self.node.name)
            self.postgis.register_node(self.node, None, name_elements['branch'])
            
            if self.postgis.set_table_completed(self.node.output):
                self.log.info(f"[postprocess] [{self.node.name}] COMPLETED")
                return True
            else:
                self.log.error(f"[postprocess] [{self.node.name}] Postprocess completed but registry record for {self.node.output} was not found.")
                return False

        except Exception as e:
            self.log.error(f"[postprocess] [{self.node.name}] Error during postprocess: {e}")
            return False

    def clip(self):
        """
        Clips dataset to clipping path
        We assume all clipping areas are an alphabetically-ordered lowercase list, eg. ['east sussex', 'surrey']
        """

        # Convert output-focused name to normal name for registry listing
        name_elements = self.parse_output_node_name(self.node.name)
        self.node.name = name_elements['name']
        clip_text = ';'.join(self.node.custom_properties['clip'])

        self.log.info(f"[clip] Running clip mask '{clip_text}' on {self.node.name} table {input}")

        if self.postgis.table_exists(self.node.output):
            self.postgis.drop_table(self.node.output)

        cliptemp = f"tmp_1_{self.node.output}_{self.node.urn}"

        areas, initial_areas = [], self.node.custom_properties['clip']

        # Convert all areas and check each exists in boundaries database
        for area in initial_areas:

            if area in OpenSiteConstants.OSM_NAME_CONVERT: area = OpenSiteConstants.OSM_NAME_CONVERT[area]

            # Run get_areas_bounds as check to see if area exists in boundaries table
            if not self.postgis.get_areas_bounds([area]):
                self.log.error(f"[clip] Unable to find clipping area '{area}' in boundaries database, unable to proceed")
                return False

            areas.append(area)

        dbparams = {
            "crs": sql.Literal(int(self.get_crs_default())),
            "areas": sql.Literal(areas),
            "input": sql.Identifier(self.node.input),
            "clip": sql.Identifier(OpenSiteConstants.OPENSITE_OSMBOUNDARIES),
            "cliptemp": sql.Identifier(cliptemp),
            "output": sql.Identifier(self.node.output),
            "cliptemp_index": sql.Identifier(f"{cliptemp}_idx"),
            "output_index": sql.Identifier(f"{self.node.output}_idx"),
        }

        query_cliptemp_st_union = sql.SQL("""
        CREATE TABLE {cliptemp} AS 
            SELECT (ST_Dump(ST_Union(ST_MakeValid(geom)))).geom::geometry(Polygon, {crs}) as geom 
            FROM {clip} 
            WHERE LOWER(name) = ANY({areas}) 
            OR LOWER(council_name) = ANY({areas})"""
        ).format(**dbparams)
        query_cliptemp_create_index = sql.SQL("CREATE INDEX {cliptemp_index} ON {cliptemp} USING GIST (geom)").format(**dbparams)
        query_fast_clip = sql.SQL("""
        CREATE TABLE {output} AS 
        SELECT 
            CASE 
                WHEN ST_Within(d.geom, c.geom) THEN d.geom 
                ELSE ST_Multi(ST_CollectionExtract(ST_Intersection(d.geom, c.geom), 3)) 
            END::geometry(MultiPolygon, {crs}) as geom
        FROM {input} d
        JOIN {cliptemp} c ON ST_Intersects(d.geom, c.geom)
        WHERE NOT ST_IsEmpty(ST_Intersection(d.geom, c.geom))""").format(**dbparams)

        try:
            self.postgis.execute_query(query_cliptemp_st_union)
            self.postgis.execute_query(query_cliptemp_create_index)
            self.postgis.execute_query(query_fast_clip)
            self.postgis.drop_table(cliptemp)
            self.postgis.add_table_comment(self.node.output, self.node.name)

            # Register new table manually as output uses variable ()
            self.postgis.register_node(self.node, None, name_elements['branch'])
            if self.postgis.set_table_completed(self.node.output):
                self.log.info(f"[clip] [{self.node.name}] COMPLETED")
                return True
            else:
                # This catches the bug where the node was never registered initially
                self.log.error(f"[clip] Postprocess completed but registry record for {self.node.output} was not found.")
                return False

        except Error as e:
            self.log.error(f"[clip] PostGIS Error: {e}")
            return False
        except Exception as e:
            self.log.error(f"[clip] Unexpected error: {e}")
            return False
            