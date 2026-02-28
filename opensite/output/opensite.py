import logging
import os
from pathlib import Path
from opensite.output.base import OutputBase
from opensite.constants import OpenSiteConstants
from opensite.logging.opensite import OpenSiteLogger
from opensite.postgis.opensite import OpenSitePostGIS
from opensite.output.geojson import OpenSiteOutputGeoJSON
from opensite.output.gpkg import OpenSiteOutputGPKG
from opensite.output.mbtiles import OpenSiteOutputMbtiles
from opensite.output.shp import OpenSiteOutputSHP
from opensite.output.json import OpenSiteOutputJSON
from opensite.output.qgis import OpenSiteOutputQGIS
from opensite.output.web import OpenSiteOutputWeb

class OpenSiteOutput(OutputBase):
    def __init__(self, node, log_level=logging.INFO, overwrite=False, shared_lock=None, shared_metadata=None):
        super().__init__(node, log_level=log_level, overwrite=overwrite, shared_lock=shared_lock, shared_metadata=shared_metadata)
        self.log = OpenSiteLogger("OpenSiteOutput", log_level, shared_lock)
        self.base_path = OpenSiteConstants.OUTPUT_LAYERS_FOLDER
    
    def run(self):
        """
        Runs output for every specific output type
        """

        # For some formats, we ignore registry and always export
        ignore_output_registry_formats = ['json', 'qgis', 'web']

        outputObject = None

        if self.node.format not in ignore_output_registry_formats:

            input = self.node.input
            full_path = Path(self.base_path / self.node.output).resolve()

            # For file conversions, input will be file not database
            if not input.startswith(OpenSiteConstants.DATABASE_GENERAL_PREFIX):
                input = str(Path(self.base_path / self.node.input).resolve())

            postgis = OpenSitePostGIS(self.log_level)
            # If not overwriting, file exists and there is 'last exported' entry for input table and output path, then do nothing 
            if postgis.check_export_exists(input, str(full_path)):
                if not self.overwrite:
                    if full_path.exists():
                        self.log.info(f"{self.node.output} already exists and was exported from {self.node.input} so skipping export")
                        return True

        if self.node.format == 'geojson':
            outputObject = OpenSiteOutputGeoJSON(self.node, self.log_level, self.overwrite, self.shared_lock, self.shared_metadata)

        if self.node.format == "gpkg":
            outputObject = OpenSiteOutputGPKG(self.node, self.log_level, self.overwrite, self.shared_lock, self.shared_metadata)

        if self.node.format == 'mbtiles':
            outputObject = OpenSiteOutputMbtiles(self.node, self.log_level, self.overwrite, self.shared_lock, self.shared_metadata)

        if self.node.format == 'shp':
            outputObject = OpenSiteOutputSHP(self.node, self.log_level, self.overwrite, self.shared_lock, self.shared_metadata)

        if self.node.format == 'json':
            outputObject = OpenSiteOutputJSON(self.node, self.log_level, self.overwrite, self.shared_lock, self.shared_metadata)

        if self.node.format == 'qgis':
            outputObject = OpenSiteOutputQGIS(self.node, self.log_level, self.overwrite, self.shared_lock, self.shared_metadata)

        if self.node.format == 'web':
            outputObject = OpenSiteOutputWeb(self.node, self.log_level, self.overwrite, self.shared_lock, self.shared_metadata)

        if outputObject:

            run_status = outputObject.run()

            if self.node.format not in ignore_output_registry_formats:
                # If export was successful, add to export log table
                if run_status:
                    postgis.update_export_log(str(input), str(full_path))

            return run_status

        return False
    