import copy
import os
import hashlib
import json
import re
import shutil
import yaml
import logging
import uuid
import webbrowser
from pathlib import Path
from typing import Dict, Any, List, Optional
from .base import Graph
from ..node import Node
from pyvis.network import Network
from opensite.constants import OpenSiteConstants
from opensite.postgis.opensite import OpenSitePostGIS
from opensite.logging.opensite import OpenSiteLogger
from opensite.ckan.opensite import OpenSiteCKAN

class OpenSiteGraph(Graph):

    TABLENAME_PREFIX        = OpenSiteConstants.DATABASE_GENERAL_PREFIX
    TABLENAME_BASE          = OpenSiteConstants.DATABASE_BASE
    TREE_BRANCH_PROPERTIES  = OpenSiteConstants.TREE_BRANCH_PROPERTIES

    def __init__(self, overrides=None, outputformats=None, clip=None, snapgrid=None, log_level=logging.INFO):
        super().__init__(overrides)

        self.log = OpenSiteLogger("OpenSiteGraph", log_level)
        self.db = OpenSitePostGIS()
        self.db.sync_registry()
        self.outputformats = outputformats
        self.yaml_unique_id_field = 'code'
        
        # clip is special case as will be absent in defaults.yml 
        # so will not be automically added to self._overrides
        if clip: self._overrides['clip'] = clip

        self.snapgrid = snapgrid

        self.log.info("Graph initialized and ready.")
        
    def is_database_output(self, output):
        """
        Checks whether output is database output
        """

        if output:
            if output.startswith(self.TABLENAME_PREFIX): return True
            if output.startswith(self.TABLENAME_BASE): return True
        return False
    
    def register_to_database(self):
        """Syncs the graph structure to PostGIS"""
        self.log.debug("Starting database synchronization...")

        def _recurse_and_register(node, branch):
            # Use debug for high-volume mapping logs (White)
            self.log.debug(f"Mapping node: {node.name} -> {node.output}")
            if self.is_database_output(node.output):
                self.db.register_node(node, branch)
            
            for child in node.children:
                _recurse_and_register(child, branch)

        for branch in self.root.children:
            yml_hash = branch.custom_properties.get('hash')
            if yml_hash:
                self.log.debug(f"Syncing branch: {branch.name} [{yml_hash[:8]}]")
                
                try:
                    self.db.register_branch(branch.name, yml_hash, branch.custom_properties)
                    _recurse_and_register(branch, branch)
                except Exception as e:
                    # Use error for failures (Red)
                    self.log.error(f"Failed to sync branch {branch.name}: {e}")

        self.log.info("Database synchronization complete.")

    def get_action_groups(self):
        """
        Groups actions based on execution profile. 
        'import' and 'amalgamate' are CPU/DB intensive PostGIS operations.
        """
        return {
            "io_bound": [
                'install',
                'download', 
                'unzip', 
                'concatenate'
            ],
            "cpu_bound": [
                'run', 
                'import',
                'invert',
                'preprocess',
                'buffer',
                'distance', 
                'amalgamate',
                'postprocess',
                'clip',
                'output',
                'web',
                'qgis',
            ]
        }

    def get_terminal_status(self):
        """
        Get list of statuses that indicate they're terminal
        """

        return ['processed', 'failed']

    def get_distinct_actions(self):
        """
        Returns every unique action currently present in the actual graph nodes.
        Useful for debugging and ensuring the processor handles everything.
        """
        all_nodes = self.find_nodes_by_props({})
        return sorted(list(set(node.get('action') for node in all_nodes if node.get('action'))))

    def get_string_buffer_distance(self, buffer):
        """
        Generates buffer string, removing extraneous decimal zeros
        """

        buffer = str(buffer)
        if buffer.endswith('.0'): buffer = buffer[:-2]
        return buffer

    def get_suffix_buffer(self, buffer):
        """
        Generates buffer suffix
        """

        buffer = self.get_string_buffer_distance(buffer)
        buffer = buffer.replace('.', '-')
        return f"--buffer-{buffer}"

    def get_suffix_distance(self, distance):
        """
        Generates distance suffix
        """

        distance = self.get_string_buffer_distance(distance)
        distance = distance.replace('.', '-')
        return f"--distance-{distance}"

    def get_suffix_clip(self, clip):
        """
        Generates clip suffix
        """

        clip = '--'.join(clip)
        sanitized_clip = re.sub(r'[^a-zA-Z0-9-]+', '-', clip)
        return f"--clip--{sanitized_clip}"

    def generate_graph_preview(self, filename="graph.html", load=False):
        """
        Generates an interactive HTML preview.
        - Skips root node to show branches as separate networks.
        - Increases node distance for better spacing.
        """
        self.log.debug(f"Generating interactive graph preview: {filename}")
        
        net = Network(
            height="100vh", 
            width="100%", 
            bgcolor="#ffffff", 
            font_color="black", 
            directed=True,
            notebook=False 
        )
        
        # We increase node_distance and spring_length to force them apart
        net.force_atlas_2based(
            gravity=-50,        # More negative = more repulsion between nodes
            central_gravity=0.01, 
            spring_length=150,   # Minimum distance for an edge
            spring_strength=0.08,
            damping=0.4,
            overlap=0
        )

        options = {
            "nodes": {
                "shape": "dot",
                "size": 300,
                "font": {
                    "multi": True,
                    "size": 18,
                    "face": "Tahoma"
                },
                "color": {
                    "highlight": {"background": "inherit", "border": "inherit"},
                    "hover": {"background": "#FFFFFF"}
                },
                "widthConstraint": {"maximum": 1000}
            },
            "edges": {
                "smooth": False,
                "width": 2,
                "color": {
                    "color": "#D3D3D3",
                    "highlight": "#848484",
                    "hover": "#848484"
                },
                "arrows": {
                    "to": {
                        "enabled": True,
                        "scaleFactor": 20 
                    }
                }
            },
            "layout": {
                "hierarchical": {
                    "enabled": True,
                    "direction": "UD",
                    "sortMethod": "directed",
                    "levelSeparation": 3000,
                    "nodeSpacing": 900,
                    "treeSpacing": 800
                }
            },
            "physics": {
                "enabled": False
            },
            "interaction": {
                "dragNodes": True,
                "hover": True,
                "tooltipDelay": 100
            }
        }

        net.set_options(json.dumps(options))

        def add_to_vis(node):
            if node.status == 'processed':
                color = "#0b7a39"
            elif node.status == 'unprocessed':
                color = "#FEE245"
            elif node.status == 'processing':
                color = "#CBF974"
            elif node.status == 'failed':
                color = "#ec0a0a"

            node_json = node.to_json()
            del node_json['children']
            properties_json = json.dumps(node_json, indent=2)
            label = node.title if node.title else node.name
            truncated_label = self.truncate_label(label, max_length=45)
            net.add_node(
                node.urn, 
                label=truncated_label, 
                fulllabel=label,
                color=color,
                title=f"[URN:{node.urn}] {node.title}",
                properties=properties_json
            )

            if hasattr(node, 'children'):
                for child in node.children:
                    add_to_vis(child)
                    net.add_edge(node.urn, child.urn)

        # SKIP ROOT: Iterate through the root's children directly
        # This makes each top-level branch its own independent network
        if hasattr(self.root, 'children'):
            for top_level_branch in self.root.children:
                add_to_vis(top_level_branch)

        white_panel_html = """
            <div id="graph-title" style="
                position: fixed; top: 10px; left: 50%; 
                transform: translateX(-50%);
                background: rgba(255, 255, 255, 0.8);
                padding: 10px 20px; border-radius: 5px;
                border: 1px solid #ccc; font-family: sans-serif;
                z-index: 1000; font-size: 20px; font-weight: bold;">
                Open Site Energy: Processing graph
            </div>

            <div id="property-panel" style="
                position: fixed; top: 10px; right: 10px; 
                width: 500px; height: 90%; 
                background: rgba(255, 255, 255, 0.9); 
                border: 1px solid #ccc; padding: 15px; 
                overflow-y: auto; z-index: 1000;
                box-shadow: -2px 0 5px rgba(0,0,0,0.1);
                display: none; font-family: sans-serif;">
                <h3>Node Properties</h3>
                <pre id="property-content"></pre>
            </div>

            <script type="text/javascript">
                network.on("click", function (params) {
                    if (params.nodes.length > 0) {
                        var nodeId = params.nodes[0];
                        var nodeData = nodes.get(nodeId);
                        
                        document.getElementById('property-panel').style.display = 'block';
                        document.getElementById('property-content').innerHTML = 
                            "<b>Name:</b> " + nodeData.fulllabel + "\\n" +
                            "<b>Properties:</b>\\n" + nodeData.properties;
                    } else {
                        document.getElementById('property-panel').style.display = 'none';
                    }
                });
            </script>
            """

        try:
            net.write_html(filename)
            with open(filename, "a") as f:
                f.write(white_panel_html)
            self.log.debug(f"Successfully generated {filename}")

            if load:
                # Trigger loading of file
                file_path = os.path.abspath(filename)
                webbrowser.open(f"file://{file_path}")

        except Exception as e:
            self.log.error(f"Failed to generate graph preview: {e}")

    def get_math_context(self, branch: Node) -> Dict[str, float]:
        """
        Builds a math context specific to the properties 
        stored on this branch's root node.
        """
        all_props = branch.custom_properties
        function_keys = self.TREE_BRANCH_PROPERTIES.get('functions', [])
        
        return {
            k: v for k, v in all_props.items() 
            if k in function_keys and isinstance(v, (int, float, str))
        }

    def add_yaml(self, filepath: str):
        """Loads a YAML file and triggers the branch-specific enrichment logic."""
        # 1. Use the base class to load the raw file structure into a branch
        super().add_yaml(filepath)
        
        if not self.root.children: return False
            
        # 2. Get the branch we just created (the last child of the root)
        current_branch = self.root.children[-1]
        
        # 3. Trigger the unified enrichment logic
        # This now handles property mapping, math, and surgical pruning
        self.enrich_branch(current_branch)

        self.register_to_database()

        return True

    def resolve_branch_math(self, branch: Node):
        # Get context dynamically via our new function
        context = self.get_math_context()
        
        def walk(node: Node):
            for k, v in node.custom_properties.items():
                if isinstance(v, str):
                    # base.py's resolve_math handles the calculation
                    node.custom_properties[k] = self.resolve_math(v, context)
            
            for child in node.children:
                walk(child)

        walk(branch)

    def enrich_branch(self, branch: Node):
        """
        Merges file data with global defaults and prunes
        """
        
        self.log.debug(f"Running enrich_branch")

        all_registry_keys = [k for sub in self.TREE_BRANCH_PROPERTIES.values() for k in sub]

        for key in all_registry_keys:
            # 1. Try to find the node in the current YAML branch
            prop_node = self.find_child(branch, key)
            
            # 2. Determine value: Local Node > Global Default
            val = None
            if prop_node:
                val = prop_node.custom_properties.get('value')
            else:
                val = self._defaults.get(key)

            # 3. Apply to Branch Node
            if val is not None:
                if key == 'title':
                    branch.title = val
                else:
                    branch.custom_properties[key] = val

        # 3. Get math context FROM the branch properties we just set
        context = self.get_math_context(branch)
        
        # 4. Locate structure/style/buffer roots
        struct_root = self.find_child(branch, "structure")
        style_root = self.find_child(branch, "style")
        buffer_root = self.find_child(branch, "buffers")
        distance_root = self.find_child(branch, "distances")

        if not struct_root:
            # Cleanup if no structure (deletes osm, tip-height nodes etc.)
            for child in list(branch.children):
                self.delete_node(child)
            return

        # 5. Enrichment Loop (Math & Style)
        for category_node in struct_root.children:
            category_node.node_type = 'group'
            category_node.action = 'amalgamate'

            # Apply Style
            if style_root:
                style_match = self.find_child(style_root, category_node.name)
                if style_match:
                    category_node.style = {
                        c.name: c.custom_properties.get('value') 
                        for c in style_match.children
                    }

            # Determine parents, apply buffers and resolve math
            for dataset_node in category_node.children:
                dataset_node.node_type = "source"
                if '--' in dataset_node.name:
                    dataset_node.custom_properties['parent'] = dataset_node.name.split("--")[0]

                if buffer_root:
                    buf_node = self.find_child(buffer_root, dataset_node.name)
                    if buf_node:
                        val = buf_node.custom_properties.get('value')
                        dataset_node.database_action = "buffer"
                        # Math resolution uses the branch-specific context
                        dataset_node.custom_properties['buffer'] = self.resolve_math(val, context)

                if distance_root:
                    distance_node = self.find_child(distance_root, dataset_node.name)
                    if distance_node:
                        val = distance_node.custom_properties.get('value')
                        dataset_node.database_action = "distance"
                        # Math resolution uses the branch-specific context
                        dataset_node.custom_properties['distance'] = self.resolve_math(val, context)

        # 6. Sibling Cleanup
        # Deletes all original YAML nodes (tip-height, title, style, etc.)
        extraneous_nodes = self.get_siblings(struct_root)
        for node in extraneous_nodes:
            self.delete_node(node)

        # 7. Final Promotion
        valid_data_nodes = list(struct_root.children)
        for node in valid_data_nodes:
            node.parent = branch
            branch.children.append(node)

        self.delete_node(struct_root)

    def choose_priority_resource(self, resources, priority_ordered_formats):
        """
        Choose the single best dataset from a list based on FORMATS priority.
        """
        if not resources:
            return None
        
        # We want to find the dataset whose resource format has the lowest index in self.FORMATS
        best_resource = resources[0] # Default to first if no priority match found
        best_index = len(priority_ordered_formats)

        for resource in resources:
            format = resource.get('format')
            if format in priority_ordered_formats:
                current_index = priority_ordered_formats.index(format)
                if current_index < best_index:
                    best_index = current_index
                    best_resource = resource
                    
                    # Optimization: If we found the #1 priority (GPKG), we can stop looking
                    if best_index == 0:
                        return best_resource

        return best_resource
        
    def update_metadata(self, ckan: OpenSiteCKAN):
        """
        Syncs titles and URLs for both Groups and Datasets across the entire graph.
        """
        self.log.info("Synchronizing node titles with CKAN metadata...")

        model = ckan.query()

        # Build a unified lookup map for both Groups and Datasets
        ckan_lookup = {}

        for group_name, data in model.items():
            # Add the group itself to the lookup (if it's not the 'default' catch-all)
            # This allows folders in your graph to get their Titles from CKAN groups
            if group_name != 'default':
                ckan_lookup[group_name] = {
                    'title': data.get('group_title', group_name).strip(),
                }

            # Add priority resource within each dataset
            for dataset in data.get('datasets', []):
                priority_resource = self.choose_priority_resource(dataset.get('resources', []), ckan.FORMATS)
                package_name = dataset.get('package_name', '')
                if package_name:
                    ckan_lookup[package_name] = {
                        'title': dataset.get('title').strip(), 
                        'input': priority_resource.get('url').strip(),
                        'format': priority_resource.get('format').strip(),
                        'extras': dataset.get('extras')
                    }
                
        # Recursive walker (unchanged logic, now with better data)
        def walk_and_update(node):
            matches = 0
            if node.name in ckan_lookup:
                meta = ckan_lookup[node.name]
                node.title = meta['title']
                if 'input' in meta: node.input = meta['input']
                if 'format' in meta: node.format = meta['format'] 
                # Add any filters from ckan extras
                if ('extras' in meta) and (len(meta['extras']) > 0): 
                    for item in meta['extras']:
                        if item['key'].startswith('FILTER:'):
                            node.custom_properties['filter'] = {'field': item['key'][len('FILTER:'):], 'values': item['value'].split(';')}
                        if item['key'] in ['preprocess']:
                            node.custom_properties['preprocess'] = item['value']

                matches += 1
            
            if hasattr(node, 'children'):
                for child in node.children:
                    matches += walk_and_update(child)
            return matches

        # Execute
        total_matches = walk_and_update(self.root)
        self.log.info(f"Metadata sync complete. Updated {total_matches} total nodes.")

    def capture_core_structure(self):
        """
        Creates a deep copy of the current root hierarchy and stores it 
        in self.corestructure to preserve the 'unexploded' state.
        """
        self.log.info("Capturing snapshot of the core graph structure.")
        
        # deepcopy replicates the entire tree (nodes and their children lists)
        # so that modifications to the main graph won't affect the snapshot.
        self.corestructure = copy.deepcopy(self.root)

    def explode(self):
        """
        Builds processing graph
        """

        # Take the snapshot first
        self.capture_core_structure()

        # Add single amalgamation just below very top to avoid destroying branch-specific data
        self.add_overallamalgamation()

        # # Groups datasets with same initial slug together, 'national-parks--england', 'national-parks--scotland', etc
        self.add_parents()

        # Generate download nodes
        self.add_downloads()

        # Generate unzipping nodes
        self.add_unzips()

        # Generate osm-export-tool nodes
        self.add_osmexporttool()

        # Generate OpenLibrary nodes
        self.add_openlibrary()

        # Generate buffer and distances nodes
        self.add_buffers_distances()

        # Generate top-level invert nodes
        self.add_inversions()

        # Generate preprocessed nodes
        # During preprocessing, we dump and select single geometry type then slice data into grid squares to reduce memory use 
        self.add_preprocess()

        # Calculate final amalgamation children outputs
        self.compute_amalgamation_outputs()

        # # Generate output nodes
        self.add_outputs()

        # Generate OSM boundaries file if necessary
        self.add_osmboundaries()

        # Generate installer nodes
        self.add_installers()

        # Add global urns across nodes that share same output
        self.add_global_urns()

        # Add 'Import - ' prefix to all import nodes now nodes derived from it have been created
        self.add_informative_prefixes()

        # Update database registry with new nodes
        self.register_to_database()

    def add_overallamalgamation(self):
        """
        Inserts an 'all-layers' amalgamation node immediately below each 
        top-level branch node, moving existing children down the chain.
        """

        # Identify top-level branches (children of self.root)
        # We use a copy of the list to safely modify tree structure
        top_branches = [
            node for node in self.root.children 
            if node.node_type == "branch"
        ]

        for branch in top_branches:
            if any(child.name == 'all-layers' for child in branch.children):
                continue

            self.log.info(f"Inserting 'all-layers' overall amalgamation for branch: {branch.name}")

            am_node = self.create_node(
                name='all-layers',
                title='All layers',
                action='amalgamate',
                custom_properties={'branch': branch.custom_properties.get('branch', branch.name)}
            )

            # Move all current children of branch to new amalgamation node
            original_children = list(branch.children)
            am_node.children = original_children
            
            for child in original_children:
                child.parent = am_node

            branch.children = [am_node]
            am_node.parent = branch

        self.log.info("Overall amalgamation node insertion complete")
        
    def add_parents(self):
        """
        Groups sibling nodes, derives the group title from children, 
        and sets action to 'amalgamate'.
        """
        self.log.info("Organizing graph hierarchy and setting 'amalgamate' actions...")
        
        def process_node(current_node):
            if hasattr(current_node, 'children') and current_node.children:
                for child in list(current_node.children):
                    process_node(child)

            if not hasattr(current_node, 'children') or not current_node.children:
                return

            group_map = {}
            for child in current_node.children:
                props = getattr(child, 'custom_properties', {}) or {}
                parent_val = props.get('parent')
                
                if parent_val:
                    if parent_val not in group_map:
                        group_map[parent_val] = []
                    group_map[parent_val].append(child)

            for group_name, siblings in group_map.items():
                child_urns = [s.urn for s in siblings]

                # 1. Title Logic: Inherit from first child
                ref_child = siblings[0]
                original_title = getattr(ref_child, 'title', ref_child.name)
                original_branch = ref_child.custom_properties['branch']

                if original_title and ' - ' in original_title:
                    parts = original_title.split(' - ')
                    group_title = ' - '.join(parts[:-1])
                else:
                    group_title = group_name.replace('-', ' ').title()
                
                # 2. Create the node with numeric URN
                new_group = self.create_group_node(
                    parent_urn=current_node.urn,
                    child_urns=child_urns,
                    group_name=group_name,
                    group_title=group_title
                )

                # 3. Apply metadata
                if new_group:
                    new_group.node_type = 'group'
                    new_group.action = 'amalgamate'
                    new_group.custom_properties['branch'] = original_branch
                    self.log.debug(f"Created group '{group_title}' (URN: {new_group.urn}) with action 'amalgamate'")

        process_node(self.root)

    def get_osm_path(self, osm_file):
        """
        Gets path to osm download file relative to downloads folder
        """
        
        full_path = OpenSiteConstants.OSM_DOWNLOAD_FOLDER / osm_file
        relative_path = full_path.relative_to(OpenSiteConstants.DOWNLOAD_FOLDER)
        return str(relative_path)

    def add_downloads(self):
        """
        Identifies terminal nodes with remote inputs and inserts a 'download' node as a child
        """

        self.log.info("Adding download nodes for remote datasources...")
                
        terminals = self.get_terminal_nodes()

        for node in terminals:
            input_url = getattr(node, 'input', '')
            if isinstance(input_url, str) and input_url.startswith('http'):
                
                node.action = "import"
                node_format = getattr(node, 'format', 'Unknown')
                node_branch = node.custom_properties['branch']
                extension = OpenSiteConstants.CKAN_FILE_EXTENSIONS.get(node_format, 'ERROR')

                download_node = self.create_node(
                    name=f"{node.name}", 
                    title=f"Download - {node.title}", 
                )

                # Configure common Download node properties
                download_node.node_type = 'download'
                download_node.input = input_url
                download_node.format = node.format
                download_node.action = 'download'
                download_node.custom_properties['branch'] = node_branch

                # Determine local output path
                if download_node.format in OpenSiteConstants.OSM_RELATED_FORMATS:
                    osm_file = f"{node.name}.{extension}"
                    download_node.output = self.get_osm_path(osm_file)
                else:
                    download_node.output = f"{node.name}.{extension}"

                # 4. Re-wire the Parent
                node.input = download_node.output
                if not hasattr(node, 'children'):
                    node.children = []
                node.children.append(download_node)

    def add_unzips(self):
        """
        Searches for download nodes with .zip URLs and inserts 
        an 'unzip' step into the pipeline.
        """
        self.log.info("Checking for zip archives to extract...")
        
        # 1. We look for terminal nodes (which should be our 'download' nodes now)
        terminals = self.get_terminal_nodes()

        for node in terminals:
            input_url = getattr(node, 'input', '')
            node_branch = node.custom_properties['branch']

            # 2. Check if the basename of the URL ends in .zip
            if isinstance(input_url, str) and input_url.lower().split('?')[0].endswith('.zip'):
                
                # 3. Clone the node to create the 'Download' child
                
                zip_child = self.create_node(
                    name=f"{node.name}-file",
                    title=node.title, # Keep original title for the actual download
                )
                
                # 4. Define the Zip Basename
                # If node.output is 'residential.yml', zip_output is 'residential.yml.zip'
                zip_output = f"{node.output}.zip"

                # 5. Configure the Child (The Downloader)
                zip_child.node_type = 'download'
                zip_child.action = 'download'
                zip_child.input = node.input   # Child takes the remote URL
                zip_child.output = zip_output  # Child saves the .zip file
                zip_child.format = node.format
                zip_child.custom_properties = {'branch': node_branch}

                # 6. Configure the Parent (The Unzipper)
                node.node_type = 'process'
                node.action = 'unzip'
                node.title = f"Unzip - {node.title}"
                node.input = zip_output        # Parent takes the .zip from child
                # node.output stays as the unzipped filename (e.g., .yml or .gpkg)
                
                # 7. Re-parenting
                if not hasattr(node, 'children'):
                    node.children = []
                node.children.append(zip_child)
                
                self.log.debug(f"Inserted unzip step for {zip_output} (URN: {node.urn})")

    def add_osmexporttool(self):
        """
        Builds the OSM stack: Runner is the parent, with Downloader 
        and Concatenator as children below it.
        """
        self.log.info("Splicing OSM stack: Adding Downloader as sibling to Concatenator...")

        # 1. Query for the base YML download nodes
        yml_node_dicts = self.find_nodes_by_props({
            'format': OpenSiteConstants.OSM_YML_FORMAT, 
            'node_type': 'download'
        })
        
        if not yml_node_dicts:
            return

        # 2. Group by lineage-baked 'osm' URL
        groups = {}
        for d in yml_node_dicts:
            node = self.find_node_by_urn(d['urn'])
            osm_url = self.get_property_from_lineage(node.urn, 'osm')
            if not osm_url:
                continue
            if osm_url not in groups:
                groups[osm_url] = []
            groups[osm_url].append(node)

        # 3. Process each unique OSM source group
        for osm_url, group_nodes in groups.items():
            group_outputs = sorted(list(set(n.output for n in group_nodes if n.input)))
            osm_url_basename = os.path.basename(osm_url)
            hash_payload = osm_url + json.dumps(group_outputs, sort_keys=True)
            yml_group_hash = hashlib.md5(hash_payload.encode()).hexdigest()[0:16]
            concat_output = f"{OpenSiteConstants.DATABASE_GENERAL_PREFIX.replace('_', '-')}{yml_group_hash}.yml"
            run_output = f"{OpenSiteConstants.DATABASE_GENERAL_PREFIX.replace('_', '-')}{yml_group_hash}.gpkg"

            for node in group_nodes:
                node_branch = node.custom_properties['branch']

                # --- LAYER 1: Concatenator ---
                concat_node = self.create_node(
                    name=f"osm-concatenator--{osm_url}",
                    title=f"Concatenate OSM Configs - {osm_url_basename}",
                )
                concat_node.action = 'concatenate'
                concat_node.node_type = 'osm-concatenator'
                concat_node.input = group_outputs 

                # --- LAYER 2: Downloader ---
                down_node = self.create_node(
                    name=f"osm-downloader--{osm_url}",
                    title=f"Download OSM Source - {osm_url_basename}",
                    format='OSM',
                )
                down_node.action = 'download'
                down_node.node_type = 'osm-downloader'
                down_node.input = osm_url
                
                # --- LAYER 3: Runner ---
                run_node = self.create_node(
                    name=f"osm-runner--{osm_url}",
                    title=f"Run osm-export-tool to create datasets - {osm_url_basename}",
                )
                run_node.action = 'run'
                run_node.node_type = 'osm-runner'
                run_node.input = concat_output

                # Initialize properties
                for n in [concat_node, down_node, run_node]:
                    n.output = None
                    if not hasattr(n, 'custom_properties') or n.custom_properties is None:
                        n.custom_properties = {}
                    n.custom_properties['osm'] = osm_url
                    n.custom_properties['branch'] = node_branch

                # Ensure we set path to osm datafile download
                down_node.output = self.get_osm_path(osm_url_basename)

                # Ensure we set concat_node output to variable
                concat_node.output = concat_output

                # Ensure we set run node output to variable
                run_node.output = run_output
                
                # 4. Splicing logic
                # Insert concat above download
                self.insert_parent(node, concat_node)
                # Insert runner above concat
                self.insert_parent(concat_node, run_node)
                
                # Manual Sibling Attachment: 
                # Since Runner is now parent of Concat, we just add Downloader to Runner's children.
                if not hasattr(run_node, 'children') or run_node.children is None:
                    run_node.children = []
                
                # Avoid duplicates if multiple nodes in a group share a runner
                if down_node not in run_node.children:
                    run_node.children.append(down_node)

                # 5. Set original parent of the runner to 'import'
                runner_parent = self.find_parent(run_node.urn)
                if runner_parent:
                    runner_parent.action = 'import'
                    # Change location to /osm as import is non-OSM-specific
                    runner_parent.input = f"{OpenSiteConstants.OSM_SUBFOLDER}/{run_output}"
                    if not hasattr(runner_parent, 'custom_properties') or runner_parent.custom_properties is None:
                        runner_parent.custom_properties = {}
                    runner_parent.custom_properties['osm'] = osm_url
                    runner_parent.custom_properties['yml'] = node.output

        self.log.debug("OSM Tree complete: Runner is now parent to both Downloader and Concatenator.")

    def add_openlibrary(self):
        """
        Builds Open Library nodes by changing type of 'download' to 'run' and modifying paths
        """
        self.log.info("Setting up Open Library nodes")

        # Query for the base Open Library YML nodes
        yml_node_dicts = self.find_nodes_by_props({
            'format': OpenSiteConstants.OPENLIBRARY_YML_FORMAT, 
            'node_type': 'download'
        })
        
        if not yml_node_dicts:
            return

        # Group by URL
        groups = {}
        for d in yml_node_dicts:
            node = self.find_node_by_urn(d['urn'])
            if node.input not in groups:
                groups[node.input] = []
            groups[node.input].append(node)

        # 3. Process each unique url group
        for osm_url, group_nodes in groups.items():
            for node in group_nodes:
                title_elements = node.title.split(' - ')
                title_elements[0] = 'Run Open Library'
                node.title = ' - '.join(title_elements)
                node.action = 'run'
                node.node_type = 'openlibrary-runner'
                node.output = f"{str(Path(node.output).stem)}.gpkg"

                # Change input of runner's parent
                runner_parent = self.find_parent(node.urn)
                if runner_parent:
                    runner_parent.input = f"{OpenSiteConstants.OPENLIBRARY_SUBFOLDER}/{node.output}"
                    
        self.log.debug("Setting up Open Library nodes complete")

    def add_buffers_distances(self):
        """
        Inserts a buffer or distance node above any node with a 'buffer' or 'distance' custom property.
        The buffer or distance node consumes the child node's output.
        """

        self.log.info("Applying buffer and distance layers...")

        # Identify nodes that need buffering
        # We collect them in a list first to avoid iterator issues during graph mutation
        target_nodes = [
            self.find_node_by_urn(d['urn']) 
            for d in self.find_nodes_by_props({}) 
            if (d.get('custom_properties', {}).get('buffer') is not None) or (d.get('custom_properties', {}).get('distance') is not None)
        ]

        for node in target_nodes:

            # Don't do anything if not buffer or distance node
            if ('buffer' not in node.custom_properties) and ('distance' not in node.custom_properties): continue

            # Only one of 'buffer' or 'distance' can exist for any dataset
            if 'buffer' in node.custom_properties:
                buffer = node.custom_properties['buffer']
                buffer_str = self.get_string_buffer_distance(buffer)
            
                # Define the new name and properties
                new_name = f"{node.name}{self.get_suffix_buffer(buffer)}"
                
                buffer_distance_node = self.create_node(
                    name=new_name,
                    title=f"{node.title} - Buffer {buffer_str}m",
                )

                # 3. Configure the buffer node
                buffer_distance_node.action = 'buffer'

            if 'distance' in node.custom_properties:
                distance = node.custom_properties['distance']
                distance_str = self.get_string_buffer_distance(distance)
            
                # Define the new name and properties
                new_name = f"{node.name}{self.get_suffix_distance(distance)}"
                
                buffer_distance_node = self.create_node(
                    name=new_name,
                    title=f"{node.title} - Distance Exclusion {distance_str}m",
                )

                # 3. Configure the distance node
                buffer_distance_node.action = 'distance'

            buffer_distance_node.node_type = 'process'
            
            # Clone properties so we keep lineage (like 'osm' URL)
            buffer_distance_node.custom_properties = node.custom_properties.copy()
            
            # Recalculate output based on the new name
            buffer_distance_node.output = self.get_output(buffer_distance_node)

            # SET INPUT: The buffer node processes the output of the original node
            buffer_distance_node.input = node.output

            # 4. Splice the buffer node in as the parent
            self.insert_parent(node, buffer_distance_node)

            # 5. Clean up: Unset the buffer property on the original child
            # so the graph reflects that the buffer has been handled by the parent
            if 'buffer' in node.custom_properties: node.custom_properties.pop('buffer', None)
            if 'distance' in node.custom_properties: node.custom_properties.pop('distance', None)

        self.log.debug(f"Successfully wrapped {len(target_nodes)} nodes with buffer or distance processes.")

    def add_inversions(self):
        """
        Inserts invert node above final amalgamated node of each branch
        """

        self.log.info("Adding invert nodes...")

        # Identify top-level branch nodes
        target_nodes = [
            self.find_node_by_urn(d['urn']) 
            for d in self.find_nodes_by_props({"node_type": "branch"}) 
        ]

        for node in target_nodes:
                        
            # We assume branch node has only one child 
            child_node = node.children[0]
            inverted_node = self.create_node(
                name=f"all-layers--invert",
                title=f"Constraint-free sites",
                action='invert',
                input=child_node.output,
                node_type='source',
                custom_properties=child_node.custom_properties,
            )

            # Splice the invert node in as the parent
            self.insert_parent(child_node, inverted_node)

        self.log.debug(f"Successfully inserted invert node(s) to top of {len(target_nodes)} branch(es)")

    def add_preprocess(self):
        """
        Injects 'preprocess' node as parent to imports or buffers
        Preprocess nodes take newly imported data, dump it out to destroy multipolygons 
        (and create single clean geometry layer) and splits data into grid squares to maximize parallelism
        """

        import_dicts = self.find_nodes_by_props({"action": "import"})
        
        for d in import_dicts:
            import_node = self.find_node_by_urn(d['urn'])
            if not import_node: continue

            # Identify target to "wrap" - check immediate parent to see if buffer/distance has already 'claimed' import
            parent = self.find_parent(import_node.urn)
            if parent and ((getattr(parent, 'action', None) == 'buffer') or (getattr(parent, 'action', None) == 'distance')): target_node = parent
            else: target_node = import_node

            target_branch = target_node.custom_properties['branch']
            preprocess_node = self.create_node(
                name=f"{target_node.name}--preprocess",
                title=f"{target_node.title} - Preprocess",
                input=target_node.output,
                action='preprocess',
                status='unprocessed'
            )

            preprocess_node.output=self.get_output(preprocess_node)
            preprocess_node.custom_properties['branch'] = target_branch

            if self.snapgrid: preprocess_node.custom_properties['snapgrid'] = self.snapgrid

            self.insert_parent(target_node, preprocess_node)

        self.log.info("Preprocess nodes injected with status 'unprocessed'.")

    def add_outputs(self):
        """
        Synthesizes output branches as independent siblings to data branches.
        """

        # 1. Prepare format lists
        formats = self.outputformats.copy()
        if 'gpkg' in formats:
            formats.remove('gpkg')
            formats.insert(0, 'gpkg')

        global_format_keys = {'web', 'qgis'}
        local_formats = [f for f in formats if f not in global_format_keys]
        global_formats = [f for f in formats if f in global_format_keys]

        # Identify existing main branches (children of root)
        # We take a snapshot of children to avoid modifying the list while iterating
        current_branches = list(self.root.children)
        
        global_branch_custom_properties = {"structure": self.get_structure(current_branches)}

        for branch_node in current_branches:

            branch_code = branch_node.name

            # Skip if it's not a branch or if it's already an output branch
            if branch_node.node_type != "branch" or branch_node.name.endswith("--outputs"):
                continue

            original_branch_name = branch_node.custom_properties.get('branch', branch_node.name)
            output_branch_name = f"{branch_code}--outputs"
            branch_node_custom_properties = {'branch': output_branch_name, 'branch_code': branch_code, 'branch_type': 'outputs'}

            # Create a completely separate branch root node
            output_branch_root = self.create_node(
                name=output_branch_name,
                title=branch_node.title + ' - Outputs',
                node_type="branch",
                custom_properties=branch_node_custom_properties
            )
            # Attach directly to Graph Root - making it a 'next' sibling
            self.root.children.append(output_branch_root)
            output_branch_root.parent = self.root

            # The Collector: Joins all individual pipelines within this branch
            collector_node = self.create_node(
                name=f"{branch_node.name}--all-layers",
                title=branch_node.title + ' - All layers',
                action=None,
                custom_properties=branch_node_custom_properties
            )

            # Find invert node in original branch
            invert_matches = self.find_nodes_by_props(
                {'branch': original_branch_name, 'action': 'invert'}, 
                current_node=branch_node
            )

            # Find amalgamate nodes in original branch
            am_matches = self.find_nodes_by_props(
                {'branch': original_branch_name, 'action': 'amalgamate'}, 
                current_node=branch_node
            )

            am_matches = invert_matches + am_matches

            for match in am_matches:
                am_node = self.find_node_by_urn(match['urn'])
                                
                # Clone for the output branch
                # NOTE: We do NOT set am_node as a child. This keeps the branches visually disconnected.
                cloned_am = self.create_node(
                    name=f"{branch_code}--{am_node.name}",
                    title=f"{branch_code} - {am_node.title}",
                    action=am_node.action,
                    input=am_node.input,
                    output=am_node.output,
                    custom_properties=branch_node_custom_properties
                )

                node_hash = hashlib.md5(f"{am_node.output}--postprocess".encode()).hexdigest()
                postprocess_output = f"{OpenSiteConstants.DATABASE_GENERAL_PREFIX}{node_hash}"

                # 3. Postprocess
                postprocess_name = f"{cloned_am.name}----postprocess"
                postprocess_node = self.create_node(
                    name=postprocess_name,
                    title=cloned_am.title + ' - Postprocess',
                    action='postprocess',
                    input=am_node.output,
                    output=postprocess_output,
                    custom_properties=branch_node_custom_properties
                )
                postprocess_node.children.append(cloned_am)
                cloned_am.parent = postprocess_node
                current_logic_name = postprocess_name
                current_chain_head = postprocess_node
                outputs_input = postprocess_output
                output_custom_properties = branch_node_custom_properties.copy()
                # Set fallback grid-sliced table in case output from postprocess fails
                output_custom_properties['fallback'] = am_node.output

                # Clip
                if 'clip' in branch_node.custom_properties['yml']:
                    node_hash = hashlib.md5(f"{postprocess_output}--clip".encode()).hexdigest()
                    clip_output = f"{OpenSiteConstants.DATABASE_GENERAL_PREFIX}{node_hash}"
                    clip_name = f"{postprocess_name}{self.get_suffix_clip(branch_node.custom_properties['yml']['clip'])}"
                    clip_title = ";".join(branch_node.custom_properties['yml']['clip'])
                    clip_node = self.create_node(
                        name=clip_name,
                        title=f"{postprocess_node.title} - Clip - {clip_title}",
                        action='clip',
                        input=postprocess_output,
                        output=clip_output,
                        custom_properties={
                            'branch': output_branch_name, 
                            'branch_code': branch_code, 
                            'branch_type': 'outputs', 
                            'clip': branch_node.custom_properties['yml']['clip']
                        }
                    )
                    clip_node.children.append(postprocess_node)
                    postprocess_node.parent = clip_node
                    current_logic_name = clip_name
                    current_chain_head = clip_node
                    outputs_input = clip_output

                # 5. Local Formats (gpkg, geojson, etc.)
                clean_filename_base = current_logic_name.replace("----postprocess", "")
                for fmt in local_formats:
                    fmt_node = self.create_node(
                        name=f"{current_logic_name}--output-{fmt}",
                        title=f"{cloned_am.title} - Output to {fmt}",
                        format=fmt,
                        action='output',
                        input=outputs_input,
                        custom_properties=output_custom_properties
                    )
                    fmt_node.children.append(current_chain_head)
                    current_chain_head.parent = fmt_node
                    
                    fmt_node.output = f"{clean_filename_base}.{fmt}"

                    # We use 'gpkg' files as base to convert specific file formats using ogr2ogr
                    # so set input of all non-gpkg output nodes to output of 'gpkg' output node 
                    if fmt in ['geojson', 'shp']: fmt_node.input = f"{clean_filename_base}.gpkg"

                    current_chain_head = fmt_node

                # Link the end of this pipeline to the branch collector
                collector_node.children.append(current_chain_head)
                current_chain_head.parent = collector_node

            # Global Formats (web/qgis)
            # These wrap around the collector, effectively becoming the top of the branch
            branch_top = collector_node

            # If qgis or web, add json output as both require json data file
            if bool(set(global_formats) & set(['qgis', 'web'])):
                gnode = self.create_node(
                    name=f"all-branches--json",
                    title=f"All branches - JSON",
                    format='json',
                    action='output',
                    output=f"{OpenSiteConstants.OPENSITEENERGY_SHORTNAME}-data.json",
                    custom_properties=global_branch_custom_properties,
                )
                gnode.children.append(branch_top)
                branch_top.parent = gnode
                branch_top = gnode

            # Add rest of output formats above last branch_top
            for gfmt in global_formats:
                gnode = self.create_node(
                    name=f"all-branches--{gfmt}",
                    title=f"All branches - {gfmt.capitalize()}",
                    format=gfmt,
                    action='output',
                    custom_properties=global_branch_custom_properties
                )
                gnode.children.append(branch_top)
                branch_top.parent = gnode
                
                # Global filename logic
                gnode.output = f"{OpenSiteConstants.OPENSITEENERGY_SHORTNAME}-data.{gfmt}"
                if gfmt == 'qgis':  gnode.output = f"{OpenSiteConstants.OPENSITEENERGY_SHORTNAME}.qgs"
                if gfmt == 'web':   gnode.output = "index.html"
                branch_top = gnode

            # Final Step: Attach the highest node of the chain to the Branch Root
            output_branch_root.children.append(branch_top)
            branch_top.parent = output_branch_root

        self.log.info("Output branches successfully isolated as parallel sibling structures.")

    def get_structure(self, branches):
        """
        Gets core structure of branch - for use in web or QGIS output
        """

        structure = []
        defaultcolor = 'darkgrey'
        constraintfreecolor = 'darkblue'

        bounds = self.db.get_table_bounds(OpenSiteConstants.OPENSITE_CLIPPINGMASTER, OpenSiteConstants.CRS_DEFAULT, OpenSiteConstants.CRS_OUTPUT)
        maplibre_bounds = \
        [
            [
                bounds['left'], bounds['bottom']
            ],
            [
                bounds['right'], bounds['top']
            ]
        ]
        center_lng      = (bounds['left'] + bounds['right']) / 2
        center_lat      = (bounds['bottom'] + bounds['top']) / 2
        maplibre_centre = [center_lng, center_lat]

        for branch in branches:

            branch_code = branch.name

            title_all_layers = f"{branch_code} - All constraint layers"
            title_all_layers_invert = f"Constraint-free sites"
            if 'title' in branch.custom_properties['yml']:
                title_all_layers = branch.custom_properties['yml']['title']

            properties = \
            {
                'height-to-tip': branch.custom_properties['height-to-tip'],
                'blade-radius': branch.custom_properties['blade-radius'],
            }

            branchstructure = {
                'code': branch_code,
                'title': title_all_layers,
                'properties': properties,
                'osm-default': branch.custom_properties['yml']['osm'],
                'ckan': branch.custom_properties['yml']['ckan'],
                'bounds': bounds,
                'maplibre_bounds': maplibre_bounds,
                'maplibre_centre': maplibre_centre,
                'tileserver': f"{OpenSiteConstants.TILESERVER_URL}/styles/{OpenSiteConstants.OPENSITEENERGY_SHORTNAME}/style.json",
            }

            clip_suffix_dataset = ''
            if 'clip' in branch.custom_properties['yml']:
                clip = branch.custom_properties['yml']['clip']
                clip_suffix_dataset = self.get_suffix_clip(clip)
                clip_suffix_text = f" clipped to '{';'.join(clip)}'"
                title += clip_suffix_text
                title_all_layers_invert += clip_suffix_text
                branchstructure['clip'] = clip
            
            # Add 'all-layers--invert' and 'all-layers' first
            # If clipping has been applied, append clip_suffix_dataset to dataset name
            datasets = [
                {
                    'title': title_all_layers_invert,
                    'color': constraintfreecolor,
                    'dataset': f"{branch_code}--all-layers--invert{clip_suffix_dataset}",
                    'level': 1,
                    'children': [],
                    'defaultactive': False,
                },
                {
                    'title': title_all_layers,
                    'color': defaultcolor,
                    'dataset': f"{branch_code}--all-layers{clip_suffix_dataset}",
                    'level': 1,
                    'children': [],
                    'defaultactive': False,
                }
            ]

            # Every branch should have 'inversion' node that comes after main 
            # aggregation and whose child is main parent of all datasets
            inversion_child = branch.children[0]
            main_child = inversion_child.children[0]

            for child in main_child.children:

                if child.action != 'amalgamate': continue

                if (not getattr(child, 'style', None)) or ('color' not in child.style):
                    self.log.error(f"Colour missing for dataset {child.name}")
                    color = defaultcolor
                else:
                    color = child.style['color']

                # If clipping has been applied, append clip_suffix_dataset to dataset name
                dataset = {
                    'title': child.title,
                    'color': color,
                    'dataset': f"{branch_code}--{child.name}{clip_suffix_dataset}",
                    'level': 1,
                    'defaultactive': True,
                }

                children = []
                for subchild in child.children:

                    if subchild.action != 'amalgamate': continue

                    children.append({
                        'title': subchild.title,
                        'color': color,
                        'dataset': f"{branch_code}--{subchild.name}{clip_suffix_dataset}",
                        'level': 2,
                        'defaultactive': False,
                    })

                dataset['children'] = children
                datasets.append(dataset)

            branchstructure['datasets'] = datasets
            structure.append(branchstructure)

        return structure

    def add_osmboundaries(self):
        """
        Add nodes to create osm_boundaries file if necessary
        """

        self.log.info("Adding OSM boundaries nodes")

        osm_boundaries_file = Path(OpenSiteConstants.OSM_DOWNLOAD_FOLDER) / f"{OpenSiteConstants.OSM_BOUNDARIES}.gpkg"

        build_osm_boundaries_yml_path = str(Path(OpenSiteConstants.OSM_DOWNLOAD_FOLDER) / OpenSiteConstants.OSM_BOUNDARIES_YML)
        shutil.copy(OpenSiteConstants.OSM_BOUNDARIES_YML, build_osm_boundaries_yml_path)

        osm_downloaders = self.find_nodes_by_props({'node_type': 'osm-downloader'})
        osm_default = self._defaults['osm']

        # If no clipping required on current graph, add to general output branch on all branches
        # If clipping required on current graph, add before actual clipping
        current_branches = list(self.root.children)
        no_clipping_required = True
        for branch_node in current_branches:
            if 'yml' not in branch_node.custom_properties: continue
            if 'clip' in branch_node.custom_properties['yml']: 
                no_clipping_required = False

        node_urns_to_amend = []
        if no_clipping_required:

            current_branches = list(self.root.children)

            for branch_node in current_branches:
                if  ('branch_type' not in branch_node.custom_properties) or \
                    (branch_node.custom_properties['branch_type'] != 'outputs'): 
                    continue
                node_urns_to_amend.append(branch_node.urn)

        else:

            node_urns_to_amend = [node['urn'] for node in self.find_nodes_by_props({'action': 'clip'})]

        for node_urn_to_amend in node_urns_to_amend:
            node = self.find_node_by_urn(node_urn_to_amend)
            
            osm_downloader = self.create_node(
                name=f"osm-downloader--{osm_default}",
                title="Download OSM for clipping boundaries",
                node_type="osm-downloader",
                format="OSM",
                input=osm_default,
                action="download",
                output=f"{OpenSiteConstants.OSM_SUBFOLDER}/{os.path.basename(osm_default)}",
                custom_properties={"osm": osm_default}
            )

            osm_runner = self.create_node(
                name=f"osm-runner--{osm_default}",
                title="Run osm-export-tool to create clipping boundaries",
                node_type="osm-runner",
                input=OpenSiteConstants.OSM_BOUNDARIES_YML,
                action="run",
                output=OpenSiteConstants.OSM_BOUNDARIES_YML.replace('.yml', '.gpkg'),
                custom_properties={"osm": osm_default},
                children=[osm_downloader]
            )

            osm_importer = self.create_node(
                name=OpenSiteConstants.OSM_BOUNDARIES,
                title="Import OSM clipping boundaries",
                node_type="source",
                input=f"{OpenSiteConstants.OSM_SUBFOLDER}/{OpenSiteConstants.OSM_BOUNDARIES}.gpkg",
                action="import",
                output=OpenSiteConstants.OPENSITE_OSMBOUNDARIES,
                custom_properties={"osm": osm_default},
                children=[osm_runner]
            )

            node.children.append(osm_importer)

    def add_installers(self):
        """
        Adds installer nodes
        """

        self.log.info("Adding installer nodes")

        osm_downloaders = self.find_nodes_by_props({'node_type': 'osm-downloader'})
        osm_default = self._defaults['osm']
        current_branches = list(self.root.children)

        node_urns_to_amend = []
        for branch_node in current_branches:
            if  ('branch_type' not in branch_node.custom_properties) or \
                (branch_node.custom_properties['branch_type'] != 'outputs'): 
                continue
            node_urns_to_amend.append(branch_node.urn)

        for node_urn_to_amend in node_urns_to_amend:
            node = self.find_node_by_urn(node_urn_to_amend)
            
            osm_downloader_download_first = self.create_node(
                name=f"osm-downloader--{osm_default}",
                title="Download OSM - prerequisite for tileserver basemap generation",
                format="OSM",
                input=osm_default,
                action="download",
                output=f"{OpenSiteConstants.OSM_SUBFOLDER}/{os.path.basename(osm_default)}",
                custom_properties={"osm": osm_default}
            )

            tileserver_installer = self.create_node(
                name=f"tileserver-installer",
                title="Download/install tileserver-related files and generate tileserver basemap",
                format="tileserver",
                input=osm_default,
                action="install",
                output="install-tileserver-basemap",
                custom_properties={"osm": osm_default},
                children=[osm_downloader_download_first]
            )

            node.children.append(tileserver_installer)

    def compute_amalgamation_outputs(self, node=None):
        """
        Computes outputs of all amalgamation nodes recursively by 
        generating hash of the amalgamation's child output fields 
        - but only once each child's output field is non-null
        """

        if node is None: node = self.root

        for child in node.children:
            self.compute_amalgamation_outputs(child)

        # Process only if it's an 'amalgamate' or 'invert' node and hasn't been solved yet
        if node.action in ['amalgamate', 'invert'] and node.output is None:
            
            # Check if all children now have their outputs
            if all(child.output is not None for child in node.children):
                
                # Create alphabetically ordered list of child outputs
                child_outputs = sorted([child.output for child in node.children])
                
                # Update input
                if node.action == 'invert': node.input = child_outputs[0]
                else:                       node.input = child_outputs

                # Create a deterministic hash
                # Using sort_keys=True is a safety measure for JSON stability
                hash_payload = json.dumps(child_outputs, sort_keys=True)
                node_hash = hashlib.md5(hash_payload.encode()).hexdigest()
                
                # Set the output field
                node.output = f"{OpenSiteConstants.DATABASE_GENERAL_PREFIX}{node_hash}"

    def add_global_urns(self, node=None):
        """
        Traverses the tree, identifies nodes with shared outputs, 
        and assigns a consistent global_urn to them.
        """

        output_map = {}

        # Pass 1: Collect all nodes grouped by their output value
        def collect_outputs(node):
            if node.output:
                if node.output not in output_map:
                    output_map[node.output] = []
                output_map[node.output].append(node)
            
            for child in node.children:
                collect_outputs(child)

        collect_outputs(self.root)

        # Pass 2: Assign URNs where outputs are shared
        for out_val, shared_nodes in output_map.items():
            # If any two nodes (or more) share the same output
            if len(shared_nodes) > 1:
                # Generate one URN for this specific output value
                # Using a namespace or deterministic UUID based on the output string 
                # ensures that if the output stays the same, the URN stays the same.
                global_urn = f"{uuid.uuid5(uuid.NAMESPACE_DNS, out_val)}"
                
                for node in shared_nodes:
                    node.global_urn = global_urn

    def add_informative_prefixes(self):
        """
        Add 'Import -' and 'Amalgamate - ' prefixes to all relevant nodes to aid with legibility
        Note: We do this at very end to prevent the prefix being added to derived nodes like buffer, etc
        """

        self.log.info("Adding import prefix to all import nodes")

        import_node_dicts = self.find_nodes_by_props({
            'action': 'import'
        })
                
        for node_dict in import_node_dicts: 
            node = self.find_node_by_urn(node_dict['urn'])
            node.title = f"Import - {node.title}"

        self.log.info("Adding amalgamate prefix to all amalgamate nodes")

        amalgamate_node_dicts = self.find_nodes_by_props({
            'action': 'amalgamate'
        })
                
        for node_dict in amalgamate_node_dicts: 
            node = self.find_node_by_urn(node_dict['urn'])
            node.title = f"Amalgamate - {node.title}"
