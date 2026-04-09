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

class OpenSiteAnalyse(ProcessBase):

    PROCESSING_INTERVAL_TIME = 5

    def __init__(self, node, log_level=logging.INFO, shared_lock=None, shared_metadata=None):
        super().__init__(node, log_level=log_level, shared_lock=shared_lock, shared_metadata=shared_metadata)
        self.log = OpenSiteLogger("OpenSiteAnalyse", log_level, shared_lock)
        self.base_path = OpenSiteConstants.ANALYSE_FOLDER
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

    def run(self):
        """
        Runs analysis
        Go through each dataset in 'analyse' array and compute minimum distance from each dataset in 'datasets' array
        """

        distance_query = sql.SQL("""
        WITH NearestDistances AS (
            SELECT 
                a.fid AS analyse_fid,
                nn.fid AS comparison_fid,
                ST_Distance(a.geom, nn.geom) as minimum_distance
            FROM {analyse} a
            CROSS JOIN LATERAL (
                SELECT fid, geom 
                FROM {comparison} b 
                ORDER BY a.geom <-> b.geom 
                LIMIT 1
            ) AS nn
        ),
        RankedDistances AS (
            SELECT 
                analyse_fid,
                comparison_fid,
                minimum_distance,
                PERCENT_RANK() OVER (ORDER BY minimum_distance) as dist_percentile
            FROM NearestDistances
        ),
        ThresholdValue AS (SELECT MAX(minimum_distance) as minimum_distance FROM RankedDistances WHERE dist_percentile <= {percentile}) 
        SELECT
        r.analyse_fid,
        r.comparison_fid,
        r.minimum_distance
        FROM RankedDistances r
        JOIN ThresholdValue t ON r.minimum_distance = t.minimum_distance
        LIMIT 1;
        """)

        percentile = 0
        if 'percentile' in self.node.custom_properties:
            percentile = self.node.custom_properties['percentile']

        output_data = {'title': self.node.custom_properties['title'], 'percentile': percentile, 'readable': {}, 'raw': []}
        output_file = str(Path(self.base_path) / self.node.output)

        for analyse_dataset in self.node.custom_properties['analyse']:
            output_data['readable'][analyse_dataset['title']] = {}
            for comparison_dataset_dict in self.node.custom_properties['datasets']:

                dbparams = {
                    "crs": sql.Literal(int(self.get_crs_default())),
                    "percentile": sql.Literal(percentile),
                    "analyse": sql.Identifier(analyse_dataset['output']),
                    "comparison": sql.Identifier(comparison_dataset_dict['output']),
                }

                try:
                    self.log.info(f"Computing minimum distance between '{analyse_dataset['title']}' and '{comparison_dataset_dict['title']}'")
                    results = self.postgis.fetch_all(distance_query.format(**dbparams))
                except Error as e:
                    self.log.error(f"[analyse] PostGIS Error: {e}")
                    return False
                except Exception as e:
                    self.log.error(f"[analyse] Unexpected error: {e}")
                    return False
            
                output_data['readable'][analyse_dataset['title']][comparison_dataset_dict['title']] = results[0]['minimum_distance']
                output_data['raw'].append({\
                    'analyse': analyse_dataset['output'], 
                    'analyse_fid': results[0]['analyse_fid'], 
                    'comparison': comparison_dataset_dict['output'], 
                    'comparison_fid': results[0]['comparison_fid'], 
                    'minimum_distance': results[0]['minimum_distance']\
                })

        json.dump(output_data, open(output_file, "w"), indent=4)

        return True



        # query_cliptemp_st_union = sql.SQL("""
        # CREATE TABLE {cliptemp} AS 
        #     SELECT (ST_Dump(ST_Union(ST_MakeValid(geom)))).geom::geometry(Polygon, {crs}) as geom 
        #     FROM {clip} 
        #     WHERE LOWER(name) = ANY({areas}) 
        #     OR LOWER(council_name) = ANY({areas})"""
        # ).format(**dbparams)
        # query_cliptemp_create_index = sql.SQL("CREATE INDEX {cliptemp_index} ON {cliptemp} USING GIST (geom)").format(**dbparams)
        # query_fast_clip = sql.SQL("""
        # CREATE TABLE {output} AS 
        # SELECT 
        #     CASE 
        #         WHEN ST_Within(d.geom, c.geom) THEN d.geom 
        #         ELSE ST_Multi(ST_CollectionExtract(ST_Intersection(d.geom, c.geom), 3)) 
        #     END::geometry(MultiPolygon, {crs}) as geom
        # FROM {input} d
        # JOIN {cliptemp} c ON ST_Intersects(d.geom, c.geom)
        # WHERE NOT ST_IsEmpty(ST_Intersection(d.geom, c.geom))""").format(**dbparams)

            