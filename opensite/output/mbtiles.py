import json
import logging
import os
import subprocess
from pathlib import Path
from psycopg2 import sql, Error
from opensite.output.base import OutputBase
from opensite.constants import OpenSiteConstants
from opensite.logging.opensite import OpenSiteLogger
from opensite.postgis.opensite import OpenSitePostGIS

class OpenSiteOutputMbtiles(OutputBase):
    def __init__(self, node, log_level=logging.INFO, overwrite=False, shared_lock=None, shared_metadata=None):
        super().__init__(node, log_level=log_level, overwrite=overwrite, shared_lock=shared_lock, shared_metadata=shared_metadata)
        self.log = OpenSiteLogger("OpenSiteOutputMbtiles", log_level, shared_lock)
        self.base_path = OpenSiteConstants.OUTPUT_LAYERS_FOLDER
        self.postgis = OpenSitePostGIS(log_level)

    def run(self):
        """
        Runs Mbtiles output
        Creates grid clipped version of file to improve rendering and performance when used as mbtiles
        """

        tmp_output = f"tmp-{self.node.output.replace('.mbtiles', '.geojson')}" 
        tmp_output_path = Path(self.base_path) / tmp_output
        final_temp_path = Path(self.base_path) / f"tmp-{self.node.output}"
        final_output_path = Path(self.base_path) / self.node.output
        grid_table = OpenSiteConstants.OPENSITE_GRIDOUTPUT
        scratch_table_1 = f"tmp_1_{self.node.input}_{self.node.urn}"
        refined_grid = f"customgrid_{self.node.input}_{self.node.urn}"

        dbparams = {
            "crs": sql.Literal(self.get_crs_default()),
            "grid": sql.Identifier(grid_table),
            "input": sql.Identifier(self.node.input),
            "scratch1": sql.Identifier(scratch_table_1),
            "scratch1_index": sql.Identifier(f"{scratch_table_1}_idx"),
            "refined_grid": sql.Identifier(refined_grid),
            "refined_grid_index": sql.Identifier(f"{refined_grid}_idx"),
        }

        # Drop scratch table and custom grid
        self.postgis.drop_table(scratch_table_1)
        self.postgis.drop_table(refined_grid)

        if tmp_output_path.exists(): tmp_output_path.unlink()

        query_refined_grid = sql.SQL("""
        SET work_mem = '1GB';
        SET temp_buffers = '2GB';
        SET max_parallel_workers_per_gather = 4;

        DO $$
        DECLARE
            cutoff INT := 600000;
            max_depth INT := 3;
            current_depth INT := 0;
            cells_remaining INT;
        BEGIN
            -- 1. Initialize workspace with 'finalized' flag
            DROP TABLE IF EXISTS private_grid_workspace;
            CREATE TEMP TABLE private_grid_workspace AS 
            SELECT geom, id as coarse_id, 0 as depth, FALSE as finalized 
            FROM {grid};

            CREATE INDEX idx_workspace_gist ON private_grid_workspace USING GIST (geom);

            FOR current_depth IN 0..(max_depth - 1) LOOP
                -- Check if there's anything left to split
                SELECT count(*) INTO cells_remaining FROM private_grid_workspace WHERE NOT finalized;
                EXIT WHEN cells_remaining = 0;

                DROP TABLE IF EXISTS next_gen_step;
                CREATE TEMP TABLE next_gen_step (geom geometry, coarse_id int, depth int, finalized boolean);

                -- 2. Single-pass processing using LATERAL JOIN
                INSERT INTO next_gen_step (geom, coarse_id, depth, finalized)
                SELECT 
                    CASE 
                        WHEN (summary.total_pts <= cutoff OR g.depth >= max_depth) THEN g.geom 
                        ELSE split.geom 
                    END,
                    g.coarse_id,
                    CASE WHEN (summary.total_pts <= cutoff OR g.depth >= max_depth) THEN g.depth ELSE g.depth + 1 END,
                    CASE WHEN (summary.total_pts <= cutoff OR g.depth >= max_depth) THEN TRUE ELSE FALSE END
                FROM private_grid_workspace g
                -- Calculate density ONCE for non-finalized cells
                LEFT JOIN LATERAL (
                    SELECT COALESCE(SUM(ST_NPoints(layer.geom)), 0) as total_pts
                    FROM {input} layer
                    WHERE g.finalized = FALSE AND ST_Intersects(layer.geom, g.geom)
                ) summary ON TRUE
                -- Generate split geometries ONLY for heavy cells
                LEFT JOIN LATERAL (
                    SELECT (ST_Dump(ST_Collect(ARRAY[
                        ST_MakePolygon(ST_MakeLine(ARRAY[p1, p12, pm, p41, p1])),
                        ST_MakePolygon(ST_MakeLine(ARRAY[p12, p2, p23, pm, p12])),
                        ST_MakePolygon(ST_MakeLine(ARRAY[pm, p23, p3, p34, pm])),
                        ST_MakePolygon(ST_MakeLine(ARRAY[p41, pm, p34, p4, p41]))
                    ]))).geom as geom
                    FROM (
                        SELECT 
                            ST_PointN(ST_ExteriorRing(g.geom), 1) as p1,
                            ST_PointN(ST_ExteriorRing(g.geom), 2) as p2,
                            ST_PointN(ST_ExteriorRing(g.geom), 3) as p3,
                            ST_PointN(ST_ExteriorRing(g.geom), 4) as p4,
                            ST_LineInterpolatePoint(ST_MakeLine(ST_PointN(ST_ExteriorRing(g.geom), 1), ST_PointN(ST_ExteriorRing(g.geom), 2)), 0.5) as p12,
                            ST_LineInterpolatePoint(ST_MakeLine(ST_PointN(ST_ExteriorRing(g.geom), 2), ST_PointN(ST_ExteriorRing(g.geom), 3)), 0.5) as p23,
                            ST_LineInterpolatePoint(ST_MakeLine(ST_PointN(ST_ExteriorRing(g.geom), 3), ST_PointN(ST_ExteriorRing(g.geom), 4)), 0.5) as p34,
                            ST_LineInterpolatePoint(ST_MakeLine(ST_PointN(ST_ExteriorRing(g.geom), 4), ST_PointN(ST_ExteriorRing(g.geom), 1)), 0.5) as p41,
                            ST_Centroid(g.geom) as pm
                    ) points
                ) split ON (summary.total_pts > cutoff AND NOT g.finalized)
                WHERE NOT g.finalized;

                -- 3. Carry over already finalized cells
                INSERT INTO next_gen_step 
                SELECT geom, coarse_id, depth, finalized 
                FROM private_grid_workspace 
                WHERE finalized = TRUE;

                -- 4. Re-index is the most important part for the next loop's ST_Intersects
                TRUNCATE private_grid_workspace;
                INSERT INTO private_grid_workspace SELECT * FROM next_gen_step;
                DROP INDEX IF EXISTS idx_workspace_gist;
                CREATE INDEX idx_workspace_gist ON private_grid_workspace USING GIST (geom);
                
                DROP TABLE next_gen_step;
            END LOOP;

            -- 5. Final Output
            DROP TABLE IF EXISTS {refined_grid};
            CREATE TABLE {refined_grid} AS 
            SELECT row_number() OVER () as id, coarse_id, geom 
            FROM private_grid_workspace;

            CREATE INDEX ON {refined_grid} USING GIST (geom);
            
        END $$;
        """).format(**dbparams)
        query_refined_grid_index = sql.SQL("CREATE INDEX {refined_grid_index} ON {refined_grid} USING GIST (geom)").format(**dbparams)
        
        query_scratch_table_1_gridify = sql.SQL("""
        CREATE TABLE {scratch1} AS 
        SELECT 
            (ST_Dump(ST_Union(ST_Intersection(layer.geom, grid.geom)))).geom AS geom 
        FROM {input} layer
        JOIN {refined_grid} grid ON ST_Intersects(layer.geom, grid.geom)
        GROUP BY grid.id;
        """).format(**dbparams)
        query_scratch_table_1_index = sql.SQL("CREATE INDEX {scratch1_index} ON {scratch1} USING GIST (geom)").format(**dbparams)
        
        try:
            self.log.info(f"[OpenSiteOutputMbtiles] [{self.node.name}] Cutting up output into grid squares")

            dataset_name = self.node.output.replace('.mbtiles', '')
            # Create data-size-dependent grid so Tippecanoe/Maplibre don't 'blotch' up features
            self.postgis.execute_query(query_refined_grid)
            self.postgis.execute_query(query_refined_grid_index)
            self.postgis.execute_query(query_scratch_table_1_gridify)
            self.postgis.execute_query(query_scratch_table_1_index)
            self.postgis.export_spatial_data(scratch_table_1, dataset_name, str(tmp_output_path))
            self.postgis.drop_table(scratch_table_1)
            self.postgis.drop_table(refined_grid)

            # Check for no features as GeoJSON with no features causes problem for tippecanoe
            # If no features, add dummy point so Tippecanoe creates mbtiles

            tmp_output_path_str = str(tmp_output_path)
            if os.path.getsize(tmp_output_path_str) < 1000:
                with open(tmp_output_path_str, "r") as json_file: geojson_content = json.load(json_file)
                if ('features' not in geojson_content) or (len(geojson_content['features']) == 0):
                    geojson_content['features'] = \
                    [
                        {
                            "type":"Feature", 
                            "properties": {}, 
                            "geometry": 
                            {
                                "type": "Point", 
                                "coordinates": [0,0]
                            }
                        }
                    ]
                    with open(tmp_output_path_str, "w") as json_file: json.dump(geojson_content, json_file)

            # Run Tippecanoe
            cmd = [
                "tippecanoe", 
                "-Z4", "-z15", 
                "-B8",
                "-X", 
                "--generate-ids", 
                "--force", 
                "-n", dataset_name, 
                "-l", dataset_name, 
                tmp_output_path_str, 
                "-o", str(final_temp_path) 
            ]

            self.log.info(f"[OpenSiteOutputMbtiles] [{self.node.name}] Running tippecanoe on {tmp_output_path_str} to create {final_temp_path.name}")

            try:
                subprocess.run(cmd, capture_output=True, text=True, check=True)
            except subprocess.CalledProcessError as e:
                # If initial tippecanoe fails (rare), modify and retry
                idx = cmd.index("-X")
                cmd.insert(idx + 1, "--drop-smallest-as-needed")
                cmd.insert(idx + 1, "--coalesce-smallest-as-needed")
                subprocess.run(cmd, capture_output=True, text=True, check=True)

            if tmp_output_path.exists(): os.remove(str(tmp_output_path))

            self.log.info(f"Created temp file {final_temp_path.name} successfully, copying to {final_output_path.name}")
            os.replace(str(final_temp_path), str(final_output_path))
            
            self.log.info(f"[OpenSiteOutputMbtiles] [{self.node.name}] COMPLETED")

            return True

        except subprocess.CalledProcessError as e:
            self.log.error(f"[OpenSiteOutputMbtiles] [{self.node.name}] Tippecanoe error {cmd} {e.stderr}")
            return False
        except Error as e:
            self.log.error(f"[OpenSiteOutputMbtiles] [{self.node.name}] PostGIS error during gridify: {e}")
            return False
        except Exception as e:
            self.log.error(f"[OpenSiteOutputMbtiles] [{self.node.name}] Unexpected error: {e}")
            return False

        return False