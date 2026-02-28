import hashlib
import json
import logging
import os
import requests
import yaml
from typing import Optional, Dict, Any, List
from opensite.model.node import Node
from opensite.logging.opensite import LoggingBase

class Graph:

    TABLENAME_PREFIX        = ''
    DEFAULT_YML             = 'defaults.yml'
    TREE_BRANCH_PROPERTIES  = {}
    OUTPUT_FIELDS           = [
                                "urn", 
                                "global_urn",
                                "name", 
                                "title", 
                                "node_type", 
                                "format", 
                                "input", 
                                "action", 
                                "output", 
                                "style", 
                                "custom_properties", 
                                "status", 
                                "dependencies", 
                                "log"
                            ]
    
    def __init__(self, overrides: dict = None, log_level=logging.INFO):
        """
        Initialize class
        """

        self.log = LoggingBase("Graph", log_level)
        self._nodes_by_urn: Dict[int, Any] = {}
        self._urn_counter = 1
        self._overrides = overrides or {}
        self._defaults = {}
        self.load_defaults(self.DEFAULT_YML)
        self.root = self.create_node("root", node_type="root")
        self.yaml_unique_id_field = 'id'
        self.yaml_unique_ids = []

    def load_defaults(self, filepath: str):
        """
        Loads a YAML file directly into the defaults dictionary
        """

        if not os.path.exists(filepath):
            self.log.warning(f"Warning: Defaults file not found at {filepath}")
            return

        with open(filepath, 'r') as f:
            try:
                raw_data = yaml.safe_load(f)
                if not raw_data:
                    return

                # We only want to store keys defined in our registry
                all_keys = [k for sub in self.TREE_BRANCH_PROPERTIES.values() for k in sub]
                
                for key in all_keys:
                    if key in raw_data:
                        # Store the value directly (no nodes involved)
                        self._defaults[key] = raw_data[key]
                        
            except yaml.YAMLError as e:
                self.log.error(f"Error parsing defaults.yml: {e}")

    def create_node(self, name: str, **kwargs) -> Node:
        """
        Creates new node in graph
        """

        urn = self._urn_counter
        self._urn_counter += 1
        node = Node(urn=urn, name=name, **kwargs)
        self._nodes_by_urn[urn] = node
        return node

    def insert_parent(self, child_node, new_parent):
        """
        Slices a new parent node into the tree directly above the child_node.
        The child_node's current parent will now point to new_parent, 
        and new_parent will point to child_node.
        """

        old_parent = self.find_parent(child_node.urn)
        
        if old_parent:
            if hasattr(old_parent, 'children') and child_node in old_parent.children:
                idx = old_parent.children.index(child_node)
                old_parent.children[idx] = new_parent
        else:
            if self.root and self.root.urn == child_node.urn:
                self.root = new_parent

        if not hasattr(new_parent, 'children'):
            new_parent.children = []
            
        if child_node not in new_parent.children:
            new_parent.children.append(child_node)
            
        return new_parent

    def delete_node(self, node: Node):
        """
        Permanently removes a node and all its descendants from the graph 
        and the URN registry.
        """
        if not node:
            return

        # Recursive helper to unregister URNs
        def unregister_recursive(n: Node):
            if n.urn in self._nodes_by_urn:
                del self._nodes_by_urn[n.urn]
            for child in n.children:
                unregister_recursive(child)

        # Wipe the URNs for this node and all its children
        unregister_recursive(node)

        # Sever the connection from the parent
        if node.parent:
            try:
                node.parent.children.remove(node)
            except ValueError:
                # Node wasn't in parent's list, already detached
                pass
        
        # 4. Clear the node's own references to be safe
        node.parent = None
        node.children = []

    def find_node(self, name: str, start_node: Optional[Node] = None) -> Optional[Node]:
        """Recursive search for a node by name."""
        current = start_node or self.root
        if current.name == name:
            return current
        for child in current.children:
            result = self.find_node(name, child)
            if result:
                return result
        return None

    def find_node_by_urn(self, urn, current_node=None):
        """
        Recursively searches the graph for a node with a matching URN.
        """
        if current_node is None:
            current_node = self.root

        if current_node.urn == urn:
            return current_node

        if hasattr(current_node, 'children') and current_node.children:
            for child in current_node.children:
                found = self.find_node_by_urn(urn, child)
                if found:
                    return found
        return None

    def find_nodes_by_props(self, search_dict={}, current_node=None, matches=None):
        """
        Recursively finds nodes that match all key-value pairs in search_dict.
        Returns a list of dictionaries (using _node_to_dict).
        """
        if matches is None:
            matches = []
        if current_node is None:
            current_node = self.root

        # Check if this node matches all criteria
        is_match = True
        for key, value in search_dict.items():
            # Check top-level attribute
            actual_val = getattr(current_node, key, None)
            
            # Fallback to custom_properties
            if actual_val is None and hasattr(current_node, 'custom_properties'):
                props = current_node.custom_properties or {}
                actual_val = props.get(key)

            if actual_val != value:
                is_match = False
                break

        if is_match:
            # Convert the Node object to a dict immediately using your existing helper
            matches.append(self._node_to_dict(current_node))

        # Recurse through children
        if hasattr(current_node, 'children') and current_node.children:
            for child in current_node.children:
                self.find_nodes_by_props(search_dict, child, matches)

        return matches

    def find_parent(self, target_urn, current_node=None):
        """
        Recursively traverses the tree to find the parent of the node with target_urn.
        """
        if current_node is None:
            current_node = self.root
        
        if not hasattr(current_node, 'children') or not current_node.children:
            return None

        for child in current_node.children:
            # Check if this child is the one we are looking for
            if child.urn == target_urn:
                return current_node
            
            # Otherwise, search deeper
            found = self.find_parent(target_urn, child)
            if found:
                return found
                
        return None

    def find_child(self, parent: Node, name: str) -> Optional[Node]:
        """Non-recursive search for a direct child."""
        for child in parent.children:
            if child.name == name:
                return child
        return None

    def get_siblings(self, node: Node) -> List[Node]:
        """Returns a list of all nodes at the same level as the given node."""
        if not node or not node.parent:
            return []
        # Return all children of the parent except the node itself
        return [child for child in node.parent.children if child != node]

    def get_property_from_lineage(self, node_urn, prop_name):
        """
        Walks up the tree from the given node to the root, returning the 
        first instance of prop_name found in custom_properties.
        """
        current_node = self.find_node_by_urn(node_urn)
        while current_node:
            props = getattr(current_node, 'custom_properties', {}) or {}
            if prop_name in props:
                return props[prop_name]
            
            # Move to the parent
            current_node = self.find_parent(current_node.urn)
            
        return None

    def prune_node(self, node: Node):
        """Removes node from its parent."""
        if node.parent:
            node.parent.children.remove(node)

    def _detach_node_from_parent(self, target_urn, current_node=None):
        """
        Locates the parent of target_urn and removes the target from its children list.
        """
        if current_node is None:
            current_node = self.root

        if hasattr(current_node, 'children') and current_node.children:
            for i, child in enumerate(current_node.children):
                if child.urn == target_urn:
                    return current_node.children.pop(i)
                
                detached = self._detach_node_from_parent(target_urn, child)
                if detached:
                    return detached
        return None

    def get_new_global_urn(self) -> int:
        """
        Increments and returns a unique global URN for the tree instance.
        Used to link logically identical nodes across different branches.
        """
        if not hasattr(self, '_global_urn_counter'):
            self._global_urn_counter = 0
            
        self._global_urn_counter += 1
        return self._global_urn_counter

    def sync_global_field(self, g_urn, field: str, value: str):
        """
        Sets field=value for all cloned nodes with same global_urn
        """
        # Sync all clones sharing the same global_urn
        if g_urn:
            clones = self.find_nodes_by_props({'global_urn': g_urn})
            for c_dict in clones:
                c_node = self.find_node_by_urn(c_dict['urn'])
                setattr(c_node, field, value)

    def get_terminal_nodes(self, current_node=None, terminal_list=None):
        """
        Recursively finds all nodes that have no children.
        """
        if terminal_list is None:
            terminal_list = []
        if current_node is None:
            current_node = self.root

        # If it has no children attribute or the list is empty, it's terminal
        if not hasattr(current_node, 'children') or not current_node.children:
            terminal_list.append(current_node)
        else:
            for child in current_node.children:
                self.get_terminal_nodes(child, terminal_list)
        
        return terminal_list

    def create_group_node(self, parent_urn, child_urns, group_name, group_title, global_urn=None):
        """
        Creates a new hierarchy level:
        1. Finds the parent node.
        2. Creates a new group node (OpenSiteNode).
        3. Sets global_urn if provided.
        4. Detaches children from their old parents and moves them to the new group.
        """
        # 1. Find the destination parent
        parent_node = self.find_node_by_urn(parent_urn)
        if not parent_node:
            self.log.error(f"Cannot create group: Parent URN {parent_urn} not found.")
            return None

        new_group = self.create_node(name=group_name, title=group_title)

        # 3. Handle global_urn
        if global_urn:
            new_group.global_urn = global_urn
            self.log.debug(f"Assigned global_urn {global_urn} to {new_group.urn}")

        # 4. Attach new group to parent
        if not hasattr(parent_node, 'children'):
            parent_node.children = []
        parent_node.children.append(new_group)

        # 5. Re-parent children
        for c_urn in child_urns:
            # First, find and remove the node from its current location
            child_node = self._detach_node_from_parent(c_urn)
            
            if child_node:
                if not hasattr(new_group, 'children'):
                    new_group.children = []
                new_group.children.append(child_node)
                self.log.debug(f"Moved node {c_urn} to new group {new_group.urn}")
            else:
                self.log.warning(f"Could not find child {c_urn} to move into group {group_name}")

        return new_group

    def truncate_label(self, text, max_length=20):
        """Truncates text and adds ellipsis if it exceeds max_length."""
        if text and len(text) > max_length:
            return text[:max_length-3] + "..."
        return text

    def to_json(self) -> Dict[str, Any]:
        """
        Returns the graph as a JSON-compatible dictionary object.
        """
        return self._node_to_dict(self.root)

    def to_list(self, node: Optional[Node] = None, depth: int = 0) -> List[Dict[str, Any]]:
        """
        Flattens the graph into a list of dictionaries, 
        adding 'depth' as the first field.
        """
        current = node or self.root
        if not current:
            return []

        # Create the dict starting with depth
        node_dict = {"depth": depth}
        
        # Pull all attributes from the node
        for field in self.OUTPUT_FIELDS:
            node_dict[field] = getattr(current, field, None)

        # 2. Start the list with the current node
        flat_list = [node_dict]

        # 3. Recursively add children, incrementing depth
        for child in current.children:
            flat_list.extend(self.to_list(child, depth + 1))

        return flat_list
    
    def _node_to_dict(self, node: Node) -> Dict[str, Any]:
        """Recursive helper to build the full JSON object."""
        if not node:
            return {}

        # Use a dictionary comprehension to dump the data
        data = {field: getattr(node, field, None) for field in self.OUTPUT_FIELDS}

        # Ensure custom_properties is at least an empty dict if getattr returns None
        if "custom_properties" in data:
            if data["custom_properties"] is None:
                data["custom_properties"] = {}

        # Recursively add children
        data["children"] = [self._node_to_dict(child) for child in node.children]
        
        return data

    def load_yaml(self, filepath: str):
        """Clears existing branches (below root) and loads fresh."""
        self.root.children = []
        # Reset the URN lookup to just the root
        self._nodes_by_urn = {self.root.urn: self.root}
        self.add_yaml(filepath)

    def set_terminal_nodes_output(self, node, branch):
        """
        Recursively sets the output field for all terminal nodes
        """

        if len(node.children) == 0: 
            node.output = self.get_output(node)
            self.log.debug(f"Setting output of {node.name} to {node.output}")

        for child in node.children:
            self.set_terminal_nodes_output(child, branch)

    def get_branch_name(self, yml_data, filepath):
        """Calculates branch name from yml data"""

        branch_name = os.path.basename(filepath)
        if 'type' in yml_data: branch_name = yml_data['type']
        if 'code' in yml_data: branch_name = yml_data['code']

        return branch_name

    def check_unique_id(self, data):
        """
        Checks unique id field of yaml to avoid potential data conflict issues 
        """

        if self.yaml_unique_id_field in data:
            unique_id_value = data[self.yaml_unique_id_field]
            if unique_id_value in self.yaml_unique_ids:
                self.log.error(f"One input YAML file has conflicting id field '{self.yaml_unique_id_field}' = '{unique_id_value}' with another input YAML.")
                self.log.error(f"Please change value of '{self.yaml_unique_id_field}' in one or more YAML files to resolve issue.")
                self.log.error("******* ABORTING *******")
                exit()
            else:
                self.yaml_unique_ids.append(unique_id_value)
                return True

    def add_yaml(self, path_or_url: str):
        """Adds a YAML file/url as a new sibling branch under the root."""
        
        if path_or_url.startswith(('http://', 'https://')):
            try:
                self.log.info(f"Streaming remote YAML: {path_or_url}")
                response = requests.get(path_or_url, timeout=10)
                response.raise_for_status()
                data = yaml.safe_load(response.text)
                
            except requests.exceptions.RequestException as e:
                self.log.error(f"Network error fetching YAML: {e}")
                raise
        else:
            if not os.path.exists(path_or_url):
                raise FileNotFoundError(f"Local YAML not found: {path_or_url}")

            with open(path_or_url, 'r') as f:
                data = yaml.safe_load(f)

        if not data: return False

        self.check_unique_id(data)

        processed_data = data.copy()

        # Apply defaults to fill in any missing values
        for key in self._defaults:
            if key not in processed_data:
                processed_data[key] = self._defaults[key]

        # Apply overrides to override any values
        for key in self._overrides:
            if self._overrides[key]:
                processed_data[key] = self._overrides[key]

        # Generate the unique hash for this specific state
        # We use sort_keys to ensure consistent hashing regardless of dictionary order
        state_string = json.dumps(processed_data, sort_keys=True).encode('utf-8')
        state_hash = hashlib.md5(state_string).hexdigest()

        # Create a branch container for this file
        branch_name = self.get_branch_name(processed_data, path_or_url)
        branch_node = self.create_node(branch_name, node_type="branch")
        branch_node.parent = self.root
        branch_node.custom_properties['branch'] = branch_name
        branch_node.custom_properties['yml'] = processed_data
        branch_node.custom_properties['hash'] = state_hash
        self.root.children.append(branch_node)

        # Build raw structure into this branch
        self.build_from_dict(processed_data, branch_node)

        self.set_terminal_nodes_output(branch_node, branch_node)

        return True

    def add_yamls(self, yaml_paths: list):
        """
        Batch processes a list of file paths.
        """
        self.log.info(f"Batch processing {len(yaml_paths)} YAML files...")
        
        results = []
        for path in yaml_paths:
            # We call the existing single-file logic
            success = self.add_yaml(path)
            if success:
                results.append(path)
        
        self.log.info(f"Successfully added {len(results)}/{len(yaml_paths)} files to graph.")

        return results

    def build_from_dict(self, data: Any, parent_node: Node):
        """Standard recursive dictionary-to-node mapper."""
        if isinstance(data, dict):
            for key, value in data.items():
                new_node = self.create_node(name=str(key))
                new_node.custom_properties['branch'] = parent_node.custom_properties['branch']
                new_node.parent = parent_node
                parent_node.children.append(new_node)
                if isinstance(value, (dict, list)):
                    self.build_from_dict(value, new_node)
                else:
                    new_node.custom_properties['value'] = value
        elif isinstance(data, list):
            for item in data:
                if isinstance(item, (dict, list)):
                    self.build_from_dict(item, parent_node)
                else:
                    child = self.create_node(name=str(item))
                    child.custom_properties['branch'] = parent_node.custom_properties['branch']
                    child.parent = parent_node
                    parent_node.children.append(child)

    def get_output(self, node) -> str:
        """
        Generates a PostGIS-safe table name.
        Uses the node for the name identity and the branch for the data state hash.
        Format: opensite_[short-node-hash]_[full-yml-hash]
        """
        # Generate shortened Name Hash (8 chars) from specific node name
        node_name_clean = str(node.name).strip().lower()
        node_hash = hashlib.md5(node_name_clean.encode()).hexdigest()

        # Prefixing with self.TABLENAME_PREFIX ensures it starts with a letter
        table_name = f"{self.TABLENAME_PREFIX}{node_hash}"

        return table_name

    def round_float(self, val):
        """
        Rounds to 1 decimal place and removes .0 if it exists.
        Example: 245.04 -> 245, 245.06 -> 245.1
        """
        # Round to 1 decimal place
        rounded = round(float(val), 1)
        
        # Check if the float is an integer value (ends in .0)
        if rounded.is_integer():
            return float(int(rounded))
        
        return rounded

    def resolve_math(self, expression: Any, context: Dict[str, Any]) -> Any:
        """
        Performs the actual mathematical calculation.
        Example: "1.1 * tip-height" with context {'tip-height': 100} -> 110.0
        """
        # 1. Only process strings. If it's already a number, return it.
        if not isinstance(expression, str):
            return expression

        # 2. Check if any variable names from our context exist in the string.
        # We sort by length descending so 'blade-radius-max' doesn't 
        # get partially replaced by 'blade-radius'.
        sorted_keys = sorted(context.keys(), key=len, reverse=True)
        
        has_variable = False
        templated_expr = expression
        
        for key in sorted_keys:
            if key in templated_expr:
                # Replace the variable name with its actual number
                templated_expr = templated_expr.replace(key, str(context[key]))
                has_variable = True

        # 3. If we found variables, calculate the result.
        if has_variable:
            try:
                # We strip dangerous built-ins to keep eval safe.
                # This performs the actual "math" (multiplication, addition, etc.)
                return self.round_float(eval(templated_expr, {"__builtins__": None}, {}))
            except Exception as e:
                # If the math is garbage (e.g. "1.1 * /path/to/osm"), return raw string.
                return expression
        
        return expression
    
    def update_metadata(self, model: dict):
        """Placeholder for updating graph nodes with external metadata."""
        raise NotImplementedError("Subclasses must implement update_metadata")