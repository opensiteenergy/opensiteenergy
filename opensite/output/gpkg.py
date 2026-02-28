import logging
import os
from pathlib import Path
from opensite.output.base import OutputBase
from opensite.constants import OpenSiteConstants
from opensite.logging.opensite import OpenSiteLogger
from opensite.postgis.opensite import OpenSitePostGIS
from opensite.download.base import DownloadBase

class OpenSiteOutputGPKG(OutputBase):
    def __init__(self, node, log_level=logging.INFO, overwrite=False, shared_lock=None, shared_metadata=None):
        super().__init__(node, log_level=log_level, overwrite=overwrite, shared_lock=shared_lock, shared_metadata=shared_metadata)
        self.log = OpenSiteLogger("OpenSiteOutputGPKG", log_level, shared_lock)
        self.base_path = OpenSiteConstants.OUTPUT_LAYERS_FOLDER
    
    def run(self):
        """
        Runs GPKG output
        """

        source_table = self.node.input
        fallback_table = self.node.custom_properties.get('fallback')
        temp_output = 'tmp-' + self.node.output
        temp_output_path = Path(self.base_path) / temp_output
        final_output = self.node.output
        final_output_path = Path(self.base_path) / final_output

        self.log.info(f"Exporting final layer {self.node.name} to {final_output}")

        postgis = OpenSitePostGIS(self.log_level)

        for table in [source_table, fallback_table]:
            if temp_output_path.exists(): temp_output_path.unlink()
            self.log.info(f"Exporting table {table}")
            if postgis.export_spatial_data(table, self.get_layer_from_file_path(final_output), temp_output_path):
                downloadbase = DownloadBase(self.log_level, self.shared_lock)
                if downloadbase.check_gpkg_valid(temp_output_path):
                    self.log.info(f"Exported temp file {temp_output} successfully, copying to {final_output}")
                    os.replace(temp_output_path, final_output_path)
                    return True

        self.log.error(f"Failed to export temp file {temp_output}")
        return False

