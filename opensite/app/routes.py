import asyncio
import httpx
import json
import os
import socket
import time
import threading
import uuid
import yaml
import zipfile
from datetime import timedelta
from io import BytesIO
from typing import List, Dict, Any, Optional
from pathlib import Path
from psycopg2 import sql
from pydantic import BaseModel
from fastapi import APIRouter, Request, BackgroundTasks, Query, Form, Response, HTTPException
from fastapi.responses import RedirectResponse, FileResponse, PlainTextResponse, HTMLResponse, JSONResponse
from starlette.status import HTTP_303_SEE_OTHER
from dotenv import load_dotenv
from opensite.constants import OpenSiteConstants
from opensite.postgis.opensite import OpenSitePostGIS

# Create the router instance
OpenSiteRouter = APIRouter()

# **********************************************************
# **************** Core website functions ******************
# **********************************************************

def is_logged_in(request: Request) -> bool:
    """
    Checks the session for the 'logged_in' key.
    Returns False if not found.
    """
    return request.session.get("logged_in", False)

def get_qgis_path():
    """Gets path of main QGIS file"""
    return OpenSiteConstants.OUTPUT_LAYERS_FOLDER.parent / f"{OpenSiteConstants.OPENSITEENERGY_SHORTNAME}.qgs"

@OpenSiteRouter.get("/", response_class=HTMLResponse)
async def home(request: Request):
    """
    Website home page
    Return app index page which may redirect to login page if not logged in 
    If build is running, return staging page
    """

    orchestrator = request.app.state.orchestrator
    templates = request.app.state.templates

    all_required_live_files_exist = True
    live_required_files = \
    [
        OpenSiteConstants.OUTPUT_FOLDER / 'index.html', 
        OpenSiteConstants.OUTPUT_FOLDER / f"{OpenSiteConstants.OPENSITEENERGY_SHORTNAME}-data.json",
        OpenSiteConstants.TILESERVER_LIVE_CONFIG_FILE, 
    ]
    for live_required_file in live_required_files:
        if not live_required_file.is_file(): all_required_live_files_exist = False

    if all_required_live_files_exist:
        return templates.TemplateResponse(
            "index.html", 
            {"request": request}
        )
    else:
        return templates.TemplateResponse(
            "index_staging.html", 
            {"request": request}
        )

@OpenSiteRouter.get(f"/{OpenSiteConstants.OPENSITEENERGY_SHORTNAME}-data.json")
async def get_site_data():
    file_path = Path(OpenSiteConstants.OUTPUT_FOLDER) / f"{OpenSiteConstants.OPENSITEENERGY_SHORTNAME}-data.json"
    
    if file_path.exists():
        return FileResponse(path=file_path)
    
    return {"error": f"File not found at {file_path}"}

@OpenSiteRouter.get("/admin", response_class=HTMLResponse)
def admin(request: Request):
    """
    Admin home page
    """

    if not request.session.get('logged_in', False): 
        return RedirectResponse(url="/login", status_code=303)

    return RedirectResponse(url="/configurations", status_code=303)

@OpenSiteRouter.get("/login", response_class=HTMLResponse)
async def login(request: Request, error: str = Query(None)):
    """
    Show login page
    """
    # Reset login state on visiting login page
    request.session['logged_in'] = False
    
    return request.app.state.templates.TemplateResponse(
        "login.html", 
        {"request": request, "error": error or ""}
    )

@OpenSiteRouter.get("/logout")
async def logout(request: Request):
    """
    Logs user out
    """
    request.session['logged_in'] = False
    return RedirectResponse(url="/login", status_code=303)

@OpenSiteRouter.post("/processlogin")
async def process_login(
    request: Request, 
    username: str = Form(""), 
    password: str = Form("")
):
    """
    Process login credentials
    """
    request.session['logged_in'] = False

    load_dotenv()

    admin_user = os.getenv('ADMIN_USERNAME')
    admin_pass = os.getenv('ADMIN_PASSWORD')

    if not admin_user or not admin_pass:
        return HTMLResponse(content="Server credentials missing in file", status_code=500)

    # Security: Anti-brute force delay
    time.sleep(5)

    if (username.strip() != admin_user) or (password.strip() != admin_pass):
        return RedirectResponse(url="/login?error=Login%20failed", status_code=303)

    request.session['logged_in'] = True
    return RedirectResponse(url="/configurations", status_code=303)

@OpenSiteRouter.get("/status")
async def status(request: Request):

    log = request.app.state.log
    log.info("Status endpoint accessed")

    # Access the state variables we attached in the main class
    processing_start = request.app.state.processing_start
    
    uptime_seconds = int(time.time() - processing_start)
    uptime_str = str(timedelta(seconds=uptime_seconds))
    
    return {
        "status": "running",
        "uptime": uptime_str,
        "start_time": time.ctime(processing_start),
        "active_workers": threading.active_count()
    }


# **********************************************************
# *************** Configurations functions *****************
# **********************************************************

@OpenSiteRouter.get("/configurations", response_class=HTMLResponse)
async def configurations(request: Request):
    """
    Renders configurations page
    """
    if not request.session.get('logged_in', False):
        return RedirectResponse(url="/login", status_code=303)

    return request.app.state.templates.TemplateResponse(
        "configurations.html", 
        {"request": request}
    )

@OpenSiteRouter.get("/ckan")
async def proxy(request: Request, url: str = Query(..., description="The CKAN target URL")):
    # 1. FastAPI automatically handles the 'if not url' check via Query(...)
    # and returns a 422 if it's missing.
    
    log = request.app.state.log

    try:
        async with httpx.AsyncClient() as client:
            headers = {'User-Agent': 'Mozilla/5.0'}
            response = await client.get(url, headers=headers, timeout=15.0)
            response.raise_for_status()
            return Response(
                content=response.content,
                status_code=response.status_code,
                media_type=response.headers.get('Content-Type')
            )

    except httpx.HTTPStatusError as e:
        log.error(f"CKAN PROXY ERROR: {e}")
        raise HTTPException(status_code=502, detail=f"Proxy failed: {str(e)}")
    except Exception as e:
        log.error(f"INTERNAL PROXY ERROR: {e}")
        raise HTTPException(status_code=500, detail=f"Internal Error: {str(e)}")

@OpenSiteRouter.get('/list')
async def config_list(request: Request):
    """
    Gets all available local config YMLs
    """
    if not request.session.get('logged_in', False):
        return []

    OpenSiteConstants.CONFIGS_FOLDER.mkdir(parents=True, exist_ok=True)
    configs = []
    log = request.app.state.log

    for config_path in OpenSiteConstants.CONFIGS_FOLDER.iterdir():
        if config_path.is_file():
            # Standard check for your file naming convention
            if not config_path.name.startswith('local-opensiteenergy-') or not config_path.name.endswith('.yml'):
                continue

            try:
                with open(config_path, 'r', encoding='utf-8') as f:
                    yaml_data = yaml.safe_load(f)
                    if yaml_data and 'title' in yaml_data:
                        configs.append({
                            'id': config_path.name, 
                            'title': yaml_data['title']
                        })
            except yaml.YAMLError as e:
                log.error(f"Error parsing {config_path.name}: {e}")

    return configs

@OpenSiteRouter.post('/save')
async def save(
    request: Request, 
    urn: str = Form(None), 
    content: str = Form('title: "Untitled configuration file"')
):
    """
    Save new or existing config YML via Form data
    """
    if not request.session.get('logged_in', False):
        return []

    OpenSiteConstants.CONFIGS_FOLDER.mkdir(parents=True, exist_ok=True)

    # Generate a URN if it doesn't exist
    if not urn:
        urn = f"local-opensiteenergy-{uuid.uuid4()}.yml"

    if urn.startswith('local-opensiteenergy-') and urn.endswith('.yml'):
        config_path = OpenSiteConstants.CONFIGS_FOLDER / urn
        with open(config_path, 'w', encoding='utf-8') as file:
            file.write(content)

    # Call the list function logic (re-using the logic above)
    configs_all = await config_list(request)
    return {'urn': urn, 'configs': configs_all}

@OpenSiteRouter.get('/get')
async def get_config(request: Request, urn: str = Query(...)):
    """
    Get existing config YML content
    """
    if not request.session.get('logged_in', False):
        return ""

    config_content = ''
    if urn.startswith('local-opensiteenergy-') and urn.endswith('.yml'):
        config_path = OpenSiteConstants.CONFIGS_FOLDER / urn
        if config_path.is_file():
            with open(config_path, 'r', encoding='utf-8') as file:
                config_content = file.read()

    return PlainTextResponse(content=config_content)

@OpenSiteRouter.get('/delete')
async def delete_config(request: Request, urn: str = Query(...)):
    """
    Delete existing config YML
    """
    if not request.session.get('logged_in', False):
        return []

    if urn:
        config_path = OpenSiteConstants.CONFIGS_FOLDER / urn
        # Security check: Ensure we stay inside the config folder
        if config_path.is_file() and ".." not in urn:
            os.remove(config_path)
            request.app.state.log.info(f"Deleted configuration: {urn}")

    return await config_list(request)

# **********************************************************
# ******************** Build functions *********************
# **********************************************************

COUNTRIES_LIST = \
[
    OpenSiteConstants.OSM_NAME_CONVERT['england'],
    OpenSiteConstants.OSM_NAME_CONVERT['scotland'],
    OpenSiteConstants.OSM_NAME_CONVERT['wales'],
    OpenSiteConstants.OSM_NAME_CONVERT['northern-ireland'],

]

class ConfigItem(BaseModel):
    type: str
    value: str
    name: str

class BuildConfiguration(BaseModel):
    configurations: List[ConfigItem]
    clip: List[str] = ['United Kingdom']
    last_updated: Optional[str] = None

def get_clipping_areas(request: Request):
    """
    Gets all available clipping areas from PostGIS _opensite_clipping_master table
    """

    log = request.app.state.log
    postgis = OpenSitePostGIS()
    if not postgis.table_exists(OpenSiteConstants.OPENSITE_OSMBOUNDARIES):
        log.warning(f"Table {OpenSiteConstants.OPENSITE_OSMBOUNDARIES} missing")
        return COUNTRIES_LIST

    clipping_query = sql.SQL("""
    SELECT DISTINCT all_names.name FROM 
    (
        SELECT DISTINCT name name FROM {boundaries} WHERE name <> '' AND admin_level <> '4' UNION 
        SELECT DISTINCT council_name name FROM {boundaries} WHERE council_name <> '' AND admin_level <> '4' 
    ) all_names ORDER BY all_names.name""").format(boundaries=sql.Identifier(OpenSiteConstants.OPENSITE_OSMBOUNDARIES))  
    clippingareas = postgis.fetch_all(clipping_query)
    clippingareas = [clippingarea['name'] for clippingarea in clippingareas]
    clippingareas = COUNTRIES_LIST + clippingareas

    return clippingareas

@OpenSiteRouter.get("/build", response_class=HTMLResponse)
async def build(request: Request):
    """
    Renders build page
    """
    if not request.session.get('logged_in', False):
        return RedirectResponse(url="/login", status_code=303)

    orchestrator = request.app.state.orchestrator

    if orchestrator.build_running:
        return RedirectResponse(url="/processmonitor", status_code=303)
    else:
        clipping_areas = get_clipping_areas(request)

        return request.app.state.templates.TemplateResponse(
            "build.html", 
            {
                "request": request,
                "clippingareas": clipping_areas
            }
        )

@OpenSiteRouter.get("/getbuild")
async def get_build(request: Request):
    """
    Reads the build configuration from the JSON file.
    """
    config_path = Path(OpenSiteConstants.BUILD_CONFIG)
    
    if not config_path.exists():
        # Return a default empty structure if the file doesn't exist yet
        return {"configurations": []}

    try:
        with open(config_path, "r") as f:
            data = json.load(f)
        return data
    except Exception as e:
        request.app.state.log.error(f"Failed to read build config: {e}")
        return {"configurations": []}

@OpenSiteRouter.post("/savebuild")
async def save_build(build: BuildConfiguration, request: Request):
    """
    Saves the build configuration to the JSON file.
    """
    # Security Check (Standard for your other routes)
    if not request.session.get('logged_in', False):
        raise HTTPException(status_code=401, detail="Unauthorized")

    config_path = Path(OpenSiteConstants.BUILD_CONFIG)
    
    try:
        # Ensure the directory exists
        config_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Convert Pydantic model to dict and save
        with open(config_path, "w") as f:
            json.dump(build.model_dump(), f, indent=4)
            
        request.app.state.log.info(f"Build config saved successfully to {config_path}")
        return {"status": "success"}
    except Exception as e:
        request.app.state.log.error(f"Failed to save build config: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@OpenSiteRouter.post("/buildstart")
async def route_build_start(request: Request):
    """Endpoint to trigger starting of build"""
    
    if not request.session.get('logged_in', False):
        return RedirectResponse(url="/login", status_code=303)

    request.app.state.log.info("FastAPI Endpoint: Starting build")

    orchestrator = request.app.state.orchestrator
    
    try:
        data = await request.json()
    except:
        data = None

    sites = []
    for configuration in data['configurations']:
        if configuration['type'] in ['server', 'url']:  sites.append(configuration['value'])
        if configuration['type'] == 'local':            sites.append(str(Path(OpenSiteConstants.CONFIGS_FOLDER) / configuration['value']))

    data['sites'] = sites
    del data['configurations']

    success = orchestrator.build_start(data)
    if success:
        return {"status": "success"}
    
    return {"status": "inprogress"}

@OpenSiteRouter.get("/processmonitor")
async def processmonitor(request: Request):

    if not request.session.get('logged_in', False):
        return RedirectResponse(url="/login", status_code=303)

    templates = request.app.state.templates
    return templates.TemplateResponse(
        "processmonitor.html", 
        {"request": request}
    )

@OpenSiteRouter.get("/nodes")
async def route_build_nodes(request: Request, last_index: int = 0):
    """Retrieves latest node data"""

    if not request.session.get('logged_in', False):
        return RedirectResponse(url="/login", status_code=303)

    orchestrator = request.app.state.orchestrator
    return orchestrator.build_nodes(last_index)

@OpenSiteRouter.get("/buildstop")
async def route_build_stop(request: Request):
    """Endpoint to trigger stopping of build"""

    if not request.session.get('logged_in', False):
        return RedirectResponse(url="/login", status_code=303)

    request.app.state.log.info("FastAPI Endpoint: Stopping build")

    orchestrator = request.app.state.orchestrator
    orchestrator.build_stop()

    return RedirectResponse(url="/build", status_code=HTTP_303_SEE_OTHER)


# **********************************************************
# *************** Download files functions *****************
# **********************************************************

SUFFIX_TO_NAME = {
    'geojson': 'GeoJSON',
    'shp': 'Shapefile',
    'mbtiles': 'MBTiles',
    'gpkg': 'GPKG',
    'qgis': 'QGIS',
    'all': 'all'
}

zip_progress = {}

def zip_worker(request: Request, session_id: str, zip_suffix: str, extension_filter: list = None, qgis_mode: bool = False):
    """
    Background worker that zips files and updates the progress global dict.
    """
    log = request.app.state.log
    folder = OpenSiteConstants.OUTPUT_LAYERS_FOLDER
    
    # 1. Identify files to process
    files_to_process = []
    if qgis_mode:
        qgis_file = get_qgis_path()
        if qgis_file.is_file():
            files_to_process.append((qgis_file, qgis_file.name))
        for f in folder.iterdir():
            if f.is_file() and f.suffix == '.gpkg':
                files_to_process.append((f, f"output/{f.name}"))
    else:
        for f in folder.iterdir():
            if f.is_file():
                if extension_filter and f.suffix.lstrip('.') not in extension_filter:
                    continue
                files_to_process.append((f, f.name))

    # 2. Update Progress Metadata
    zip_progress[session_id] = {"current": 0, "total": len(files_to_process), "status": "processing", "file_type": SUFFIX_TO_NAME[zip_suffix]}
        
    # 3. Create the Physical Zip in /tmp
    temp_path = f"/tmp/opensite_{session_id}.zip"
    try:
        with zipfile.ZipFile(temp_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            for i, (file_path, arcname) in enumerate(files_to_process):
                log.info(f"Adding {file_path.name} to zip for session {session_id}")
                zf.write(file_path, arcname=arcname)
                
                # Update progress count
                zip_progress[session_id]["current"] = i + 1
        
        zip_progress[session_id]["status"] = "complete"
    except Exception as e:
        log.error(f"Zip failed for {session_id}: {e}")
        zip_progress[session_id]["status"] = "failed"

@OpenSiteRouter.get("/files", response_class=HTMLResponse)
async def files_page(request: Request):
    """
    Renders downloadable files page
    """
    if not request.session.get('logged_in', False):
        return RedirectResponse(url="/login", status_code=303)

    files_list = []
    
    if OpenSiteConstants.OUTPUT_LAYERS_FOLDER.is_dir():
        # Using a simple list comprehension with pathlib
        files_list = [
            {'name': f.name, 'url': f'/outputfiles/{f.name}'} 
            for f in OpenSiteConstants.OUTPUT_LAYERS_FOLDER.iterdir() if f.is_file()
        ]

    qgis_file = get_qgis_path()
    qgis_exists = qgis_file.is_file()

    return request.app.state.templates.TemplateResponse(
        "files.html", 
        {"request": request, "files": files_list, "qgis": qgis_exists}
    )

@OpenSiteRouter.get("/download/progress")
def get_progress(request: Request):
    """Endpoint for JS to poll progress"""

    if not request.session.get('logged_in', False):
            return JSONResponse({"status": "unauthorized"}, status_code=401)

    session_id = request.session.get("download_id")
    return zip_progress.get(session_id, {"status": "idle"})

@OpenSiteRouter.get("/download/get-file")
def get_file(request: Request):
    """Final endpoint to download the result"""

    if not request.session.get('logged_in', False):
        return JSONResponse({"status": "unauthorized"}, status_code=401)

    session_id = request.session.get("download_id")
    temp_path = f"/tmp/opensite_{session_id}.zip"
    
    if os.path.exists(temp_path):
        return FileResponse(
            temp_path, 
            filename=f"{OpenSiteConstants.OPENSITEENERGY_SHORTNAME}-export.zip"
        )
    return JSONResponse({"error": "File not found"}, status_code=404)

@OpenSiteRouter.get("/downloadall")
def download_all(request: Request, background_tasks: BackgroundTasks):
    if not request.session.get('logged_in', False):
        return RedirectResponse(url="/login", status_code=303)
    session_id = str(uuid.uuid4())
    request.session["download_id"] = session_id
    background_tasks.add_task(zip_worker, request, session_id, 'all', None)
    return {"status": "started"}

@OpenSiteRouter.get("/downloadgpkg")
def download_gpkg(request: Request, background_tasks: BackgroundTasks):
    if not request.session.get('logged_in', False):
        return RedirectResponse(url="/login", status_code=303)
    session_id = str(uuid.uuid4())
    request.session["download_id"] = session_id
    background_tasks.add_task(zip_worker, request, session_id, 'gpkg', ['gpkg'])
    return {"status": "started"}

@OpenSiteRouter.get("/downloadgeojson")
def download_geojson(request: Request, background_tasks: BackgroundTasks):
    if not request.session.get('logged_in', False):
        return RedirectResponse(url="/login", status_code=303)
    session_id = str(uuid.uuid4())
    request.session["download_id"] = session_id
    background_tasks.add_task(zip_worker, request, session_id, 'geojson', ['geojson'])
    return {"status": "started"}

@OpenSiteRouter.get("/downloadshp")
def download_shp(request: Request, background_tasks: BackgroundTasks):
    if not request.session.get('logged_in', False):
        return RedirectResponse(url="/login", status_code=303)
    session_id = str(uuid.uuid4())
    request.session["download_id"] = session_id
    # Shapefiles require multiple extensions to be functional
    background_tasks.add_task(zip_worker, request, session_id, 'shp', ['shp', 'prj', 'shx', 'dbf'])
    return {"status": "started"}

@OpenSiteRouter.get("/downloadmbtiles")
def download_mbtiles(request: Request, background_tasks: BackgroundTasks):
    if not request.session.get('logged_in', False):
        return RedirectResponse(url="/login", status_code=303)
    session_id = str(uuid.uuid4())
    request.session["download_id"] = session_id
    background_tasks.add_task(zip_worker, request, session_id, 'mbtiles', ['mbtiles'])
    return {"status": "started"}

@OpenSiteRouter.get("/downloadqgis")
def download_qgis(request: Request, background_tasks: BackgroundTasks):
    if not request.session.get('logged_in', False):
        return RedirectResponse(url="/login", status_code=303)
    
    qgis_file = get_qgis_path()
    if not qgis_file.is_file():
         return request.app.state.templates.TemplateResponse(
            "error.html", 
            {"request": request, "message": "No QGIS file has been created yet."},
            status_code=404
        )

    session_id = str(uuid.uuid4())
    request.session["download_id"] = session_id
    
    background_tasks.add_task(zip_worker, request, session_id, 'qgis', qgis_mode=True)
    return {"status": "started"}

# **********************************************************
# ***************** Set domain functions *******************
# **********************************************************

@OpenSiteRouter.get("/setdomain", response_class=HTMLResponse)
async def set_domain(request: Request):
    """
    Show set domain name page
    """
    if not request.session.get('logged_in', False):
        return RedirectResponse(url="/login", status_code=303)

    return request.app.state.templates.TemplateResponse(
        "setdomain.html", 
        {"request": request, "error": None}
    )

@OpenSiteRouter.post("/processdomain")
async def process_domain(request: Request, domain: str = Form("")):
    """
    Process submitted domain name
    """
    if not request.session.get('logged_in', False):
        return RedirectResponse(url="/login", status_code=303)

    domain = domain.strip()

    # 1. Check Domain IP
    try:
        # socket.gethostbyname is blocking, but usually fast. 
        # For a true async setup, you'd use a resolver, but this works for now.
        domain_ip = socket.gethostbyname(domain).strip()
    except:
        domain_ip = None

    # 2. Check Visible IP (Async way)
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get('https://ipinfo.io/ip', timeout=5.0)
            visible_ip = resp.text.strip()
        except Exception:
            visible_ip = "0.0.0.0"

    # 3. Validation
    if domain_ip != visible_ip:
        return request.app.state.templates.TemplateResponse(
            "setdomain.html", 
            {"request": request, "error": domain}
        )

    # 4. Save Domain File
    # Ensure the directory exists or path is correct for your environment
    try:
        with open(OpenSiteConstants.DOMAIN_FILE, 'w') as file:
            file.write(f"DOMAIN={domain}")
    except Exception as e:
        request.app.state.log.error(f"Failed to write DOMAIN file: {e}")

    # 5. Redirect to the non-secure IP to monitor progress
    redirect_url = f"http://{visible_ip}/redirectdomain?id={uuid.uuid4()}&domain={domain}"
    return RedirectResponse(url=redirect_url, status_code=303)

@OpenSiteRouter.get("/redirectdomain", response_class=HTMLResponse)
async def redirect_domain(
    request: Request, 
    domain: str = Query(""), 
    id: str = Query(None)
):
    """
    Creates redirect page that shows result of Certbot
    """
    if not request.session.get('logged_in', False):
        return RedirectResponse(url="/login", status_code=303)

    # 1. Non-blocking sleep to allow servicesmanager to clear logs
    await asyncio.sleep(4)

    certbot_result, certbot_success = '', False
    if os.path.isfile(OpenSiteConstants.CERTBOT_LOG):
        with open(OpenSiteConstants.CERTBOT_LOG, "r", encoding='utf-8') as text_file:
            certbot_result = text_file.read().strip()

    if 'Successfully deployed certificate' in certbot_result:
        certbot_success = True

    # 2. Get Visible IP again
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get('https://ipinfo.io/ip', timeout=5.0)
            visible_ip = resp.text.strip()
        except:
            visible_ip = "0.0.0.0"

    # 3. Logic for the next redirect
    base_redirect = f"http://{visible_ip}/redirectdomain?id={uuid.uuid4()}"
    
    if domain:
        if certbot_success:
            redirect_url = f"https://{domain}/admin"
        else:
            redirect_url = f"{base_redirect}&domain={domain}"
    else:
        redirect_url = base_redirect

    return request.app.state.templates.TemplateResponse(
        "redirectdomain.html", 
        {
            "request": request, 
            "domain": domain, 
            "certbot_success": certbot_success, 
            "certbot_result": certbot_result, 
            "redirect_url": redirect_url
        }
    )
