import hashlib
import logging
import os
import yaml
from pathlib import Path
from opensite.processing.base import ProcessBase
from opensite.constants import OpenSiteConstants
from opensite.logging.opensite import OpenSiteLogger

class OpenSiteConcatenator(ProcessBase):
    def __init__(self, node, log_level=logging.INFO, shared_lock=None, shared_metadata=None):
        super().__init__(node, log_level=log_level, shared_lock=shared_lock, shared_metadata=shared_metadata)
        self.log = OpenSiteLogger("OpenSiteConcatenator", log_level)
        self.base_path = OpenSiteConstants.OSM_DOWNLOAD_FOLDER

    def run(self) -> bool:
        self.log.info(f"Concatenating OSM YAML files for {self.node.name}")
        
        try:
            # Collect input paths
            input_paths = [(self.base_path / os.path.basename(p)).resolve() for p in self.node.input]

            merged_data = {}
            for p in input_paths:
                if p.exists():
                    with open(p, 'r', encoding='utf-8') as f:
                        data = yaml.safe_load(f)
                        if data: 
                            merged_data.update(data)
                else:
                    self.log.error(f"Source YAML not found: {p}")
                    return False
            
            # Generate concatenation of all yml files
            yaml_content = yaml.dump(merged_data, default_flow_style=False)
            final_path = str(Path(self.base_path) / self.node.output)

            with open(final_path, 'w', encoding='utf-8') as f:
                f.write(yaml_content)
            
            self.log.info(f"Successfully generated and registered: {self.node.output}")
            return True

        except Exception as e:
            self.log.error(f"YAML Concatenation failed for {self.node.name}: {e}")
            return False