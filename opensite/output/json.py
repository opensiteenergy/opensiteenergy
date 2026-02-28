import json
import logging
import os
import shutil
from pathlib import Path
from opensite.output.base import OutputBase
from opensite.constants import OpenSiteConstants
from opensite.logging.opensite import OpenSiteLogger
from opensite.postgis.opensite import OpenSitePostGIS

class OpenSiteOutputJSON(OutputBase):
    def __init__(self, node, log_level=logging.INFO, overwrite=False, shared_lock=None, shared_metadata=None):
        super().__init__(node, log_level=log_level, overwrite=overwrite, shared_lock=shared_lock, shared_metadata=shared_metadata)
        self.log = OpenSiteLogger("OpenSiteOutputJSON", log_level, shared_lock)
        self.base_path = OpenSiteConstants.OUTPUT_FOLDER
        self.postgis = OpenSitePostGIS(self.log_level)
    
    def run(self):
        """
        Runs JSON output
        """

        js_filepath = Path(self.base_path) / self.node.output
        
        try:

            self.log.info("Outputting JSON file")

            branches_input      = self.node.custom_properties['structure']
            branches_output     = []
            for branch in branches_input:
                branch['bounds'] = None
                if 'clip' in branch:
                    branch_bounds_dict = self.postgis.get_areas_bounds(branch['clip'], OpenSiteConstants.CRS_DEFAULT, OpenSiteConstants.CRS_OUTPUT)
                    branch['bounds'] = [branch_bounds_dict['left'], branch_bounds_dict['bottom'], branch_bounds_dict['right'], branch_bounds_dict['top']]
                branches_output.append(branch)

            with open(js_filepath, 'w', encoding='utf-8') as f:
                f.write(json.dumps(branches_output, indent=4))
            self.log.info(f"[OpenSiteOutputJSON] Data exported to {self.node.output}")

            return True
        
        except Exception as e:
            self.log.error(f"[OpenSiteOutputJSON] Export failed: {e}")
            return False
