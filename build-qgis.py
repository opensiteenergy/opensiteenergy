import sys
import os
import json
import xml.etree.ElementTree as ET
from pathlib import Path
from os.path import isfile, basename
from qgis.core import (\
    QgsProject, 
    QgsVectorLayer, 
    QgsRasterLayer, 
    QgsRectangle, 
    QgsReferencedRectangle, 
    QgsApplication, 
    QgsCoordinateReferenceSystem,
    QgsCoordinateTransform,
)
from qgis.gui import *
from PyQt5.QtGui import *
from PyQt5.QtCore import QFileInfo

BUILD_FOLDER = Path('build')
if 'BUILD_FOLDER' in os.environ: BUILD_FOLDER = Path(os.environ['BUILD_FOLDER'])

QGIS_PREFIX_PATH = '/usr/'
if 'QGIS_PREFIX_PATH' in os.environ: QGIS_PREFIX_PATH = os.environ['QGIS_PREFIX_PATH']

QGIS_OUTPUT_FILE = BUILD_FOLDER / "output" / "opensiteenergy.qgs"
if len(sys.argv) > 1: QGIS_OUTPUT_FILE = Path(sys.argv[1])


# We can only set these environment variables now as setting them
# in original .env file caused problems with ogr2ogr in main script

if 'QGIS_PROJ_DATA' in os.environ: os.environ['PROJ_DATA'] = os.environ['QGIS_PROJ_DATA']
if 'QGIS_PROJ_LIB' in os.environ: os.environ['PROJ_LIB'] = os.environ['QGIS_PROJ_LIB']


os.environ['QT_QPA_PLATFORM'] = 'offscreen'
os.environ['XDG_RUNTIME_DIR'] = '/tmp/runtime-root'

QGIS_PARENT_OPACITY             = 0.7
QGIS_CHILD_OPACITY              = 0.4

# From https://github.com/ubernostrum/webcolors/blob/trunk/src/webcolors/_definitions.py
_CSS3_NAMES_TO_HEX = {
    "aliceblue": "#f0f8ff",
    "antiquewhite": "#faebd7",
    "aqua": "#00ffff",
    "aquamarine": "#7fffd4",
    "azure": "#f0ffff",
    "beige": "#f5f5dc",
    "bisque": "#ffe4c4",
    "black": "#000000",
    "blanchedalmond": "#ffebcd",
    "blue": "#0000ff",
    "blueviolet": "#8a2be2",
    "brown": "#a52a2a",
    "burlywood": "#deb887",
    "cadetblue": "#5f9ea0",
    "chartreuse": "#7fff00",
    "chocolate": "#d2691e",
    "coral": "#ff7f50",
    "cornflowerblue": "#6495ed",
    "cornsilk": "#fff8dc",
    "crimson": "#dc143c",
    "cyan": "#00ffff",
    "darkblue": "#00008b",
    "darkcyan": "#008b8b",
    "darkgoldenrod": "#b8860b",
    "darkgray": "#a9a9a9",
    "darkgrey": "#a9a9a9",
    "darkgreen": "#006400",
    "darkkhaki": "#bdb76b",
    "darkmagenta": "#8b008b",
    "darkolivegreen": "#556b2f",
    "darkorange": "#ff8c00",
    "darkorchid": "#9932cc",
    "darkred": "#8b0000",
    "darksalmon": "#e9967a",
    "darkseagreen": "#8fbc8f",
    "darkslateblue": "#483d8b",
    "darkslategray": "#2f4f4f",
    "darkslategrey": "#2f4f4f",
    "darkturquoise": "#00ced1",
    "darkviolet": "#9400d3",
    "deeppink": "#ff1493",
    "deepskyblue": "#00bfff",
    "dimgray": "#696969",
    "dimgrey": "#696969",
    "dodgerblue": "#1e90ff",
    "firebrick": "#b22222",
    "floralwhite": "#fffaf0",
    "forestgreen": "#228b22",
    "fuchsia": "#ff00ff",
    "gainsboro": "#dcdcdc",
    "ghostwhite": "#f8f8ff",
    "gold": "#ffd700",
    "goldenrod": "#daa520",
    "gray": "#808080",
    "grey": "#808080",
    "green": "#008000",
    "greenyellow": "#adff2f",
    "honeydew": "#f0fff0",
    "hotpink": "#ff69b4",
    "indianred": "#cd5c5c",
    "indigo": "#4b0082",
    "ivory": "#fffff0",
    "khaki": "#f0e68c",
    "lavender": "#e6e6fa",
    "lavenderblush": "#fff0f5",
    "lawngreen": "#7cfc00",
    "lemonchiffon": "#fffacd",
    "lightblue": "#add8e6",
    "lightcoral": "#f08080",
    "lightcyan": "#e0ffff",
    "lightgoldenrodyellow": "#fafad2",
    "lightgray": "#d3d3d3",
    "lightgrey": "#d3d3d3",
    "lightgreen": "#90ee90",
    "lightpink": "#ffb6c1",
    "lightsalmon": "#ffa07a",
    "lightseagreen": "#20b2aa",
    "lightskyblue": "#87cefa",
    "lightslategray": "#778899",
    "lightslategrey": "#778899",
    "lightsteelblue": "#b0c4de",
    "lightyellow": "#ffffe0",
    "lime": "#00ff00",
    "limegreen": "#32cd32",
    "linen": "#faf0e6",
    "magenta": "#ff00ff",
    "maroon": "#800000",
    "mediumaquamarine": "#66cdaa",
    "mediumblue": "#0000cd",
    "mediumorchid": "#ba55d3",
    "mediumpurple": "#9370db",
    "mediumseagreen": "#3cb371",
    "mediumslateblue": "#7b68ee",
    "mediumspringgreen": "#00fa9a",
    "mediumturquoise": "#48d1cc",
    "mediumvioletred": "#c71585",
    "midnightblue": "#191970",
    "mintcream": "#f5fffa",
    "mistyrose": "#ffe4e1",
    "moccasin": "#ffe4b5",
    "navajowhite": "#ffdead",
    "navy": "#000080",
    "oldlace": "#fdf5e6",
    "olive": "#808000",
    "olivedrab": "#6b8e23",
    "orange": "#ffa500",
    "orangered": "#ff4500",
    "orchid": "#da70d6",
    "palegoldenrod": "#eee8aa",
    "palegreen": "#98fb98",
    "paleturquoise": "#afeeee",
    "palevioletred": "#db7093",
    "papayawhip": "#ffefd5",
    "peachpuff": "#ffdab9",
    "peru": "#cd853f",
    "pink": "#ffc0cb",
    "plum": "#dda0dd",
    "powderblue": "#b0e0e6",
    "purple": "#800080",
    "red": "#ff0000",
    "rosybrown": "#bc8f8f",
    "royalblue": "#4169e1",
    "saddlebrown": "#8b4513",
    "salmon": "#fa8072",
    "sandybrown": "#f4a460",
    "seagreen": "#2e8b57",
    "seashell": "#fff5ee",
    "sienna": "#a0522d",
    "silver": "#c0c0c0",
    "skyblue": "#87ceeb",
    "slateblue": "#6a5acd",
    "slategray": "#708090",
    "slategrey": "#708090",
    "snow": "#fffafa",
    "springgreen": "#00ff7f",
    "steelblue": "#4682b4",
    "tan": "#d2b48c",
    "teal": "#008080",
    "thistle": "#d8bfd8",
    "tomato": "#ff6347",
    "turquoise": "#40e0d0",
    "violet": "#ee82ee",
    "wheat": "#f5deb3",
    "white": "#ffffff",
    "whitesmoke": "#f5f5f5",
    "yellow": "#ffff00",
    "yellowgreen": "#9acd32",
}

def getJSON(json_path):
    """
    Gets contents of JSON file
    """

    with open(json_path, "r") as json_file: return json.load(json_file)

def hex_to_rgb(value):
    """
    Converts hex value to RGB
    From https://stackoverflow.com/questions/29643352/converting-hex-to-rgb-value-in-python
    """

    value = value.lstrip('#')
    lv = len(value)
    return tuple(int(value[i:i + lv // 3], 16) for i in range(0, lv, lv // 3))

def convertCSSColor2RGB(color):
    """
    Converts CSS color to RGB
    """

    global _CSS3_NAMES_TO_HEX

    color = color.strip()
    if '#' not in color: 
        if color in _CSS3_NAMES_TO_HEX: color = _CSS3_NAMES_TO_HEX[color]
        else: return None

    color = color[1:]
    return hex_to_rgb(color)

def force_qgis_startup_extent(project_path, extent_list, epsg_in=4326):
    tree = ET.parse(project_path)
    root = tree.getroot()

    # 1. Get/Transform the Rect (same as before)
    crs_node = root.find(".//destinationsrs/spatialrefsys/authid")
    proj_epsg = crs_node.text if crs_node is not None else f"EPSG:{epsg_in}"
    source_crs = QgsCoordinateReferenceSystem.fromEpsgId(epsg_in)
    dest_crs = QgsCoordinateReferenceSystem(proj_epsg)
    rect = QgsRectangle(extent_list[0], extent_list[1], extent_list[2], extent_list[3])
    
    if source_crs != dest_crs:
        xform = QgsCoordinateTransform(source_crs, dest_crs, QgsProject.instance())
        rect = xform.transformBoundingBox(rect)

    # 2. Update the Properties Block (The "Secret" location)
    properties = root.find("properties")
    if properties is None:
        properties = ET.SubElement(root, "properties")
    
    # We need to find or create the 'Gui' and 'CanvasExtentFull' sections
    gui = properties.find("Gui")
    if gui is None:
        gui = ET.SubElement(properties, "Gui")
    
    canvas_ext = gui.find("CanvasExtentFull")
    if canvas_ext is None:
        canvas_ext = ET.SubElement(gui, "CanvasExtentFull")
    
    # This string format is specific: xmin, ymin, xmax, ymax
    ext_str = f"{rect.xMinimum()},{rect.yMinimum()},{rect.xMaximum()},{rect.yMaximum()}"
    canvas_ext.text = ext_str

    # 3. Repeat for the standard MapCanvas (for when QGIS is already open)
    mapcanvas = root.find("mapcanvas")
    if mapcanvas is None:
        mapcanvas = ET.SubElement(root, "mapcanvas")
    
    extent_node = mapcanvas.find("extent")
    if extent_node is None:
        extent_node = ET.SubElement(mapcanvas, "extent")
    
    for child in list(extent_node): extent_node.remove(child)
    ET.SubElement(extent_node, "xmin").text = str(rect.xMinimum())
    ET.SubElement(extent_node, "ymin").text = str(rect.yMinimum())
    ET.SubElement(extent_node, "xmax").text = str(rect.xMaximum())
    ET.SubElement(extent_node, "ymax").text = str(rect.yMaximum())

    tree.write(project_path)

def createQGISFile():
    """
    Creates QGIS file using structure of datasets in 'datasets-style.json'
    """

    global BUILD_FOLDER, QGIS_PREFIX_PATH, QGIS_OUTPUT_FILE

    data = getJSON(str(BUILD_FOLDER / "output" / "opensiteenergy-data.json"))

    # Folder with all gpkg layers

    layers_folder = Path(BUILD_FOLDER).resolve() /  "output" / "layers"

    # Delete existing file if it exists

    if QGIS_OUTPUT_FILE.exists(): QGIS_OUTPUT_FILE.unlink()

    # Initialize QGIS and start project

    QgsApplication.setPrefixPath(QGIS_PREFIX_PATH, True)

    QGISAPP = QgsApplication([], True)
    QGISAPP.initQgis()
    project = QgsProject.instance()
    project.setCrs(QgsCoordinateReferenceSystem.fromEpsgId(4326))

    # Set crs of project

    project.setCrs(QgsCoordinateReferenceSystem.fromEpsgId(3857))

    # Add layers and groups to QGIS project 

    root        = QgsProject.instance().layerTreeRoot()
    firstlayer  = None

    for branch in data:

        # Add branch

        qgis_branch = root.addGroup(branch['title'])

        # Iterate through every section in branch

        for section in branch['datasets']:

            qgis_section    = qgis_branch.addGroup(section['title'])
            dataset         = section['dataset']
            color           = convertCSSColor2RGB(section['color'])
            if color is None: 
                color = convertCSSColor2RGB('grey')

            layer = QgsVectorLayer(str(layers_folder / f"{section['dataset']}.gpkg"), section['title'])
            layer.setName(section['title'])
            layer.renderer().symbol().setOpacity(QGIS_PARENT_OPACITY)
            layer.renderer().symbol().setColor(QColor.fromRgb(color[0], color[1], color[2]))
            layer.renderer().symbol().symbolLayer(0).setStrokeWidth(0)
            layer.renderer().symbol().symbolLayer(0).setStrokeColor(QColor.fromRgb(color[0], color[1], color[2]))
            project.addMapLayer(layer, False)
            qgis_section.addLayer(layer)
            qgis_section.setExpanded(False)

            if not firstlayer: firstlayer = layer

            if dataset == branch['datasets'][0]['dataset']:

                qgis_section.setItemVisibilityChecked(False)

                # Set default full extent of project to extent of first (aggregate) layer

                # Use the layer's native CRS to avoid projection errors
                ref_rect = QgsReferencedRectangle(layer.extent(), layer.crs())
                project.viewSettings().setPresetFullExtent(ref_rect)
                project.viewSettings().setDefaultViewExtent(ref_rect)

                # project.viewSettings().setPresetFullExtent(QgsReferencedRectangle(layer.extent(), QgsCoordinateReferenceSystem.fromEpsgId(4326)))
                # project.viewSettings().setDefaultViewExtent(QgsReferencedRectangle(layer.extent(), QgsCoordinateReferenceSystem.fromEpsgId(4326)))

            # Iterate through every child in section

            for child in section['children']:
                layer = QgsVectorLayer(str(layers_folder / f"{child['dataset']}.gpkg"), child['title'])
                layer.setName(child['title'])
                layer.renderer().symbol().setOpacity(QGIS_CHILD_OPACITY)
                layer.renderer().symbol().setColor(QColor.fromRgb(color[0], color[1], color[2]))
                layer.renderer().symbol().symbolLayer(0).setStrokeWidth(0)
                layer.renderer().symbol().symbolLayer(0).setStrokeColor(QColor.fromRgb(color[0], color[1], color[2]))
                project.addMapLayer(layer, False)
                qgis_section.addLayer(layer)

                # Make layer invisible

                node = root.findLayer(layer.id())
                node.setItemVisibilityChecked(False)

    # Finally, add OSM as background layer

    qgis_group = root.addGroup('Background')

    # Add layer to group

    tms = 'type=xyz&url=https://tile.openstreetmap.org/%7Bz%7D/%7Bx%7D/%7By%7D.png&zmax=19&zmin=0&crs=EPSG3857'
    layer = QgsRasterLayer(tms,' OpenStreetMap', 'wms')
    project.addMapLayer(layer, False)
    qgis_group.addLayer(layer)

    # Save project and quit

    project.write(str(QGIS_OUTPUT_FILE))
    QGISAPP.exitQgis()

    force_qgis_startup_extent(str(QGIS_OUTPUT_FILE), [-0.15, 51.48, -0.10, 51.52]) # London Bounds

    print("QGIS file created at:", QGIS_OUTPUT_FILE)

createQGISFile()