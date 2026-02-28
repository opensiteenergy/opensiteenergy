import logging
from pathlib import Path
from opensite.constants import OpenSiteConstants
from opensite.install.base import InstallBase
from opensite.logging.opensite import OpenSiteLogger
from opensite.install.tileserver import OpenSiteTileserver

class OpenSiteInstaller(InstallBase):

    def __init__(self, node, log_level=logging.INFO, shared_lock=None, shared_metadata=None):
        super().__init__(node, log_level=log_level, shared_lock=shared_lock, shared_metadata=shared_metadata)
        self.log = OpenSiteLogger("OpenSiteInstaller", log_level, shared_lock)
        self.base_path = OpenSiteConstants.DOWNLOAD_FOLDER
        self.output_path = OpenSiteConstants.OUTPUT_FOLDER

    def run(self):
        """
        Runs install for every specific install type
        """

        outputObject = None

        if self.node.format == 'tileserver':
            outputObject = OpenSiteTileserver(self.node, self.log_level, self.shared_lock, self.shared_metadata)

        if outputObject: return outputObject.run()

        return False
