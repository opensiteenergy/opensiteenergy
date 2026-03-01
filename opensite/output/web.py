import json
import logging
import os
import shutil
from pathlib import Path
from opensite.output.base import OutputBase
from opensite.constants import OpenSiteConstants
from opensite.logging.opensite import OpenSiteLogger
from opensite.postgis.opensite import OpenSitePostGIS

class OpenSiteOutputWeb(OutputBase):
    def __init__(self, node, log_level=logging.INFO, overwrite=False, shared_lock=None, shared_metadata=None):
        super().__init__(node, log_level=log_level, overwrite=overwrite, shared_lock=shared_lock, shared_metadata=shared_metadata)
        self.log = OpenSiteLogger("OpenSiteOutputWeb", log_level, shared_lock)
        self.base_path = OpenSiteConstants.OUTPUT_FOLDER
        self.postgis = OpenSitePostGIS(self.log_level)
    
    def flatten(self, items):
        """Produces flat list from hierarchy of objects with property 'children'"""

        return [item for i in items for item in [i] + self.flatten(i.get('children', []))]

    def output_tileserver_files(self, basemap_mbtiles=None):
        """
        Outputs mtbiles styles files and copies mbtiles to tileserver data folder
        """

        # ===============================================
        # Setup variables needed to create mbtiles styles
        # ===============================================

        # Dicts hold all data and styles indexed by dataset's unique code - dataset['dataset']
        data, styles = {}, {}

        # Get clipping bounds for clipping master
        clipping_bounds_dict = self.postgis.get_table_bounds(OpenSiteConstants.OPENSITE_CLIPPINGMASTER, OpenSiteConstants.CRS_DEFAULT, OpenSiteConstants.CRS_OUTPUT)
        clipping_bounds = [clipping_bounds_dict['left'], clipping_bounds_dict['bottom'], clipping_bounds_dict['right'], clipping_bounds_dict['top'] + 0.1]

        # Add default styles separate from individual dataset styles
        styles["opensiteenergy"] = \
        {
            "style":    "opensiteenergy.json",
            "tilejson": \
            {
                "type":     "overlay",
                "bounds":   clipping_bounds
            }
        }
        styles["openmaptiles"] = \
        {
            "style":    "openmaptiles.json",
            "tilejson": \
            {
                "type":     "overlay",
                "bounds":   clipping_bounds
            }
        }
        data["openmaptiles"] = \
        {
            "mbtiles": basemap_mbtiles
        }

        # Append style nodes before the layer with specific id
        insert_at_node_id = "place_village"

        try:

            # Modify 'openmaptiles.json' and generate mbtiles stylesheets for each dataset

            fonts_url = OpenSiteConstants.TILESERVER_URL + '/fonts/{fontstack}/{range}.pbf'
            openmaptiles_style_file_src = str(OpenSiteConstants.TILESERVER_FOLDER_SRC / 'openmaptiles.json')
            openmaptiles_style_file_dst = str(OpenSiteConstants.TILESERVER_STYLES_FOLDER / 'openmaptiles.json')
            openmaptiles_style_json = json.load(open(openmaptiles_style_file_src, 'r', encoding='utf-8'))
            openmaptiles_style_json['sources']['openmaptiles']['url'] = OpenSiteConstants.TILESERVER_URL + "/data/openmaptiles.json"
            openmaptiles_style_json['glyphs'] = fonts_url
            json.dump(openmaptiles_style_json, open(openmaptiles_style_file_dst, 'w', encoding='utf-8'), indent=4)

            first_branch_ckan = self.node.custom_properties['structure'][0]['ckan']
            attribution = f"Source data copyright of multiple organisations. For all data sources, see <a href=\"{first_branch_ckan}\" target=\"_blank\">{first_branch_ckan.replace('https://', '')}</a>"
            opensite_style_json = openmaptiles_style_json
            opensite_style_json['name'] = 'Open Site Energy'
            opensite_style_json['id'] = 'opensiteenergy'
            opensite_style_json['center'] = OpenSiteConstants.TILESERVER_DEFAULT_CENTRE
            opensite_style_json['zoom'] = OpenSiteConstants.TILESERVER_DEFAULT_ZOOM
            opensite_style_json['sources']['attribution']['attribution'] += " " + attribution

            for branch in self.node.custom_properties['structure']:

                firstdataset = True

                # Define attribution
                attribution = f"Source data copyright of multiple organisations. For all data sources, see <a href=\"{branch['ckan']}\" target=\"_blank\">{branch['ckan'].replace('https://', '')}</a>"

                # Iterate through all datasets inside each branch by flattening the hierarchy

                branch_datasets = self.flatten(branch['datasets'])

                for dataset in branch_datasets:

                    self.log.info(f"Copying {dataset['dataset']}.mbtiles to {str(OpenSiteConstants.TILESERVER_DATA_FOLDER)}")

                    mbtiles_basename    = f"{dataset['dataset']}.mbtiles"
                    mbtiles_src         = str(OpenSiteConstants.OUTPUT_LAYERS_FOLDER / mbtiles_basename)
                    mbtiles_dest        = str(OpenSiteConstants.TILESERVER_DATA_FOLDER / mbtiles_basename)

                    shutil.copy(mbtiles_src, mbtiles_dest)

                    self.log.info(f"Creating tileserver-gl style file for: {dataset['dataset']}")

                    styles[dataset['dataset']] = \
                    {
                        "style":    f"{dataset['dataset']}.json",
                        "tilejson": \
                        {
                            "type":     "overlay",
                            "bounds":   clipping_bounds
                        }
                    }
                    data[dataset['dataset']] = \
                    {
                        "mbtiles":  f"{dataset['dataset']}.mbtiles"
                    }

                    style_opacity   = 0.8 if dataset['level'] == 1 else 0.5
                    style_file      = str(OpenSiteConstants.TILESERVER_STYLES_FOLDER / f"{dataset['dataset']}.json")
                    style_json      = \
                    {
                        "version":  8,
                        "id":       dataset['dataset'],
                        "center":   OpenSiteConstants.TILESERVER_DEFAULT_CENTRE,
                        "zoom":     OpenSiteConstants.TILESERVER_DEFAULT_ZOOM,
                        "name":     dataset['title'],
                        "sources": \
                        {
                            dataset['dataset']: \
                            {
                                "type":         "vector",
                                "buffer":       512,
                                "url":          OpenSiteConstants.TILESERVER_URL + f"/data/{dataset['dataset']}.json",
                                "attribution":  attribution
                            }
                        },
                        "glyphs": fonts_url,
                        "layers": \
                        [
                            {
                                "id":           dataset['dataset'],
                                "source":       dataset['dataset'],
                                "source-layer": dataset['dataset'],
                                "type":         "fill",
                                "paint":        \
                                {
                                    "fill-opacity":         style_opacity,
                                    "fill-color":           dataset['color'],
                                    "fill-outline-color":   "rgba(0,0,0,0)",
                                    "fill-antialias":       True
                                }
                            }
                        ]
                    }

                    opensite_layer = style_json['layers'][0]
                    # Temporary workaround as setting 'fill-outline-color'='#FFFFFF00' on individual style breaks WMTS
                    # opensite_layer['paint']['fill-outline-color'] = "#FFFFFF00"
                    opensite_layer['paint']['fill-outline-color'] = "rgba(0,0,0,0)"
                    opensite_layer['paint']['fill-antialias'] = True
                    opensite_layer['layout'] = {'visibility': 'visible'}
                    insert_at_index = next(i for i, layer in enumerate(opensite_style_json['layers']) if layer.get('id') == insert_at_node_id)
                    opensite_style_json['layers'].insert(insert_at_index, opensite_layer)
                    # opensite_style_json['layers'].append(opensite_layer)
                    opensite_style_json['sources'][dataset['dataset']] = style_json['sources'][dataset['dataset']]

                    json.dump(style_json, open(style_file, 'w', encoding='utf-8'), indent=4)

                    firstdataset = False

            json.dump(opensite_style_json, open(str(OpenSiteConstants.TILESERVER_MAIN_STYLE_FILE), 'w', encoding='utf-8'), indent=4)

            # Creating final tileserver-gl config file

            config_json = \
            {
                "options": \
                {
                    "paths": \
                    {
                        "root":     "",
                        "fonts":    "fonts",
                        "sprites":  "sprites",
                        "styles":   "styles",
                        "mbtiles":  "data"
                    }
                },
                "styles":   styles,
                "data":     data
            }

            json.dump(config_json, open(str(OpenSiteConstants.TILESERVER_CONFIG_FILE), 'w', encoding='utf-8'), indent=4)

            return True

        except (Exception) as e:
            self.log.error(f"General error when generating openmaptiles.json: {e}")
            return False

    def clear_folder(self, folder_path, exceptions=[]):
        """
        Clears folder avoiding exceptions
        """

        for entry in os.scandir(folder_path):
            if entry.is_file() or entry.is_symlink():
                filename = os.path.basename(entry.path)
                if filename not in exceptions:
                    os.remove(entry.path)

    def run(self):
        """
        Runs Web output
        """
        
        try:

            self.log.info("Checking required folders exist")

            tileserver_folders = [
                OpenSiteConstants.TILESERVER_OUTPUT_FOLDER,
                OpenSiteConstants.TILESERVER_DATA_FOLDER,
                OpenSiteConstants.TILESERVER_STYLES_FOLDER,
            ]
            for tileserver_folder in tileserver_folders:
                if not tileserver_folder.exists(): tileserver_folder.mkdir(parents=True, exist_ok=True)

            self.log.info("Outputting main web page")

            # Copy main web index page to output folder
            shutil.copy('tileserver/index.html', str(Path(OpenSiteConstants.OUTPUT_FOLDER) / self.node.output))

            self.log.info("Outputting tileserver mbtiles stylesheets")

            if ('structure' not in self.node.custom_properties) or (len(self.node.custom_properties['structure']) == 0):
                self.log.error("No branches set, unable to generate web tileserver configuration files")
                return False
            
            # All branches share same 'osm-default' path used to 
            # generate basemap so okay to use first branch to get path
            osm_basemap_mbtiles_file = os.path.basename(self.node.custom_properties['structure'][0]['osm-default']).replace('.osm.pbf', '.mbtiles')
            osm_basemap_tmp_mbtiles_file = 'tmp-' + osm_basemap_mbtiles_file
            
            self.log.info("Deleting non-basemap mbtiles from tileserver data folder")
            self.clear_folder(str(OpenSiteConstants.TILESERVER_DATA_FOLDER), exceptions=[osm_basemap_mbtiles_file, osm_basemap_tmp_mbtiles_file])

            self.log.info("Deleting style files from tileserver styles folder")
            self.clear_folder(str(OpenSiteConstants.TILESERVER_STYLES_FOLDER))

            self.log.info("Generating tileserver files")
            self.output_tileserver_files(osm_basemap_mbtiles_file)

            return True
        
        except Exception as e:
            self.log.error(f"[OpenSiteOutputWeb] Export failed: {e}")
            return False
