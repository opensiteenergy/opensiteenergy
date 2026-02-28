import yaml
import os
import logging
import sys
from .base import BaseCLI
from opensite.logging.opensite import OpenSiteLogger

class OpenSiteCLI(BaseCLI):
    def __init__(self, config_path: str = "defaults.yml", log_level=logging.INFO):
        super().__init__(description="OpenSiteEnergy Project Processor", log_level=log_level)
        self.log = OpenSiteLogger("OpenSiteCLI", log_level)
        self.config_path = config_path
        self.defaults = {}
        self.overrides = {}
        self.sites = []
        self.server = None
        self.preview = False
        self.outputformats = []
        self.clip = None
        self.purgedb = False
        self.purgeall = False
        self.overwrite = False
        self.graphonly = False
        self.snapgrid = None
        # Load and filter immediately
        self._load_and_filter_defaults()
        self._incoporate_cli_switched()

    def add_standard_args(self):
        """Standard arguments used across the application."""

        # Override base function
        super().add_standard_args()
        self.parser.add_argument("sites", nargs="*", help="Site(s) to generate")
        self.parser.add_argument('--server', type=int, nargs='?', const=8000, help="Runs headless app. Default port 8000, or specify with --server=[port]")
        self.parser.add_argument('--preview', action='store_true', help='Loads interactive processing graph view')
        self.parser.add_argument('--purgedb', action='store_true', help="Drop all opensite tables and exit")
        self.parser.add_argument('--purgeall', action='store_true', help="Delete all download files, drop all opensite tables and exit")
        self.parser.add_argument('--clip', type=str, help="Name of area to clip data to, e.g., 'Surrey'. For multiple clipping areas, separate with semicolon, eg. --clip=\"East Sussex;Devon\"")
        self.parser.add_argument('--overwrite', action='store_true', help="Reexports all output files, overwriting files already created")
        self.parser.add_argument('--graphonly', action='store_true', help="Generate build graph but don't run build")
        self.parser.add_argument('--snapgrid', type=float, help="Snaps all imported datasets to grid of size [snapgrid] metres")

    def get_command_line(self):
        """
        Regenerate full command line from list of arguments
        """

        output_args = []
        for arg in sys.argv:
            if ' ' in arg: arg = "'" + str(arg) + "'"
            output_args.append(arg)

        commandline = ' '.join(output_args)
        commandline = commandline.replace('opensiteenergy.py', './build.sh')
        
        return commandline

    def _load_and_filter_defaults(self):
        """Loads the file and keeps only int, float, and str variables."""
        if not os.path.exists(self.config_path):
            return

        self.log.debug(f"Loading defaults from {self.config_path}")

        with open(self.config_path, 'r') as f:
            full_data = yaml.safe_load(f) or {}
            
        # Filter for 'simple' types only
        for key, value in full_data.items():
            if isinstance(value, (int, float, str)) and not isinstance(value, bool):
                self.log.debug(f"Adding default value from {self.config_path}: {key}={value}")
                self.defaults[key] = value

        # outputformats is special case
        self.defaults['outputformats'] = full_data['outputformats']

    def inject_dynamic_args(self):
        """Adds flags for the filtered simple variables."""
        for key, value in self.defaults.items():
            help = f"Override {key} (Default: {value})"
            if key == 'snapgrid': continue
            if key == 'outputformats': 
                help =  f"Set output format(s) from "\
                        f"'gpkg', 'shp', 'geojson', "\
                        f"'mbtiles', 'web', 'qgis'. "\
                        f"For multiple formats, separate values with commas (Default: {','.join(value)})"
            self.parser.add_argument(
                f"--{key}",
                type=type(value),
                default=None,
                help=help
            )

    def get_current_value(self, value):
        """Gets current value of a property"""

        if value in self.overrides: 
            if self.overrides[value] is not None: 
                return self.overrides[value]
        
        if value in self.defaults: 
            if self.defaults[value] is not None:
                return self.defaults[value]

        self.log.error(f"'{value}' does not exist in defaults or overrides")
        return None

    def get_server(self):
        """Gets server port number - None if not running as headless server"""
        return self.server

    def get_defaults(self):
        """Gets current defaults"""
        return self.defaults

    def get_overrides(self):
        """Gets current overrides"""
        return self.overrides
    
    def get_sites(self):
        """Gets list of sites from CLI"""
        return self.sites

    def get_outputformats(self):
        """Gets list of outputformats from CLI"""
        return self.outputformats

    def get_clip(self):
        """Gets clip value from CLI"""
        return self.clip

    def get_preview(self):
        """Gets status of --preview CLI switch"""
        return self.preview
    
    def get_overwrite(self):
        """Gets status of --overwrite CLI switch"""
        return self.overwrite

    def get_graphonly(self):
        """Gets status of --buildonly CLI switch"""
        return self.graphonly

    def get_snapgrid(self):
        """Gets value of snapgrid"""
        return self.snapgrid

    def _incoporate_cli_switched(self):
        """Standard execution flow."""
        self.add_standard_args()
        self.inject_dynamic_args()
        self.parse()

        # Port number value for server
        if self.args.server:
            self.server = self.args.server

        # Boolean for purgeall
        self.purgeall = self.args.purgeall

        # Boolean for purgedb
        self.purgedb = self.args.purgedb

        # Boolean for preview
        self.preview = self.args.preview
        
        # Boolean for overwrite
        self.overwrite = self.args.overwrite

        # Boolean for graphonly
        self.graphonly = self.args.graphonly

        # Set sites to the list of sites provided in CLI
        self.sites = self.args.sites
        if not self.args.sites:
            # If no sites provided, use default list
            self.sites = ['wind', 'solar']

        # Set list of required output formats
        if self.args.outputformats is None:
            self.outputformats = self.defaults['outputformats']
        else:
            self.outputformats = (''.join(self.args.outputformats)).split(',')
            # If 'qgis' or 'shp' or 'geojson', ensure 'gpkg' in output formats as required for all of them
            if  ('qgis' in self.outputformats) or \
                ('shp' in self.outputformats) or \
                ('geojson' in self.outputformats):
                if 'gpkg' not in self.outputformats: self.outputformats.append('gpkg')
            # If 'web', ensure 'mbtiles' in output formats as required for it
            if 'web' in self.outputformats:
                if 'mbtiles' not in self.outputformats: self.outputformats.append('mbtiles')

        # Set clip to clip value provided in CLI
        if self.args.clip:
            clip_items = sorted(self.args.clip.split(";"))
            clip_items = [clip_item.lower() for clip_item in clip_items]
            self.clip = clip_items

        # Set snap grid to value provided in CLI or if not provided, default value if it exists
        if self.args.snapgrid:
            self.snapgrid = self.args.snapgrid
        else:
            if self.defaults['snapgrid']:
                self.snapgrid = self.defaults['snapgrid']

        # Capture the final state of the simple variables
        overrides = {}
        for key in self.defaults.keys():
            safe_key = key.replace("-", "_")
            if hasattr(self.args, safe_key):
                overrides[key] = getattr(self.args, safe_key)
        self.overrides = overrides
        
        self.log.debug(f"Command line sites: {self.sites}")
        self.log.debug(f"Defaults: {self.defaults}")
        self.log.debug(f"Overrides: {self.overrides}")
        
