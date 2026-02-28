import logging
import os
from pathlib import Path
from opensite.output.base import OutputBase
from opensite.constants import OpenSiteConstants
from opensite.logging.opensite import OpenSiteLogger

class OpenSiteOutputGeoJSON(OutputBase):
    def __init__(self, node, log_level=logging.INFO, overwrite=False, shared_lock=None, shared_metadata=None):
        super().__init__(node, log_level=log_level, overwrite=overwrite, shared_lock=shared_lock, shared_metadata=shared_metadata)
        self.log = OpenSiteLogger("OpenSiteOutputGeoJSON", log_level, shared_lock)
        self.base_path = OpenSiteConstants.OUTPUT_LAYERS_FOLDER
    
    def run(self):
        """
        Runs GeoJSON output
        """

        return self.convert_node_input_to_output_files(self.node)