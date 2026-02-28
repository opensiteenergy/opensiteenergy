import os
import requests
import time
from dotenv import load_dotenv

# Initialize Environment
load_dotenv()

CKAN_URL = os.getenv("CKAN_URL").rstrip('/')
WEBUI_URL = os.getenv("WEBUI_URL").rstrip('/')
API_KEY = os.getenv("OPEN_WEBUI_API_KEY")
KNOWLEDGE_ID = os.getenv("KNOWLEDGE_ID")

def fetch_ckan_metadata():
    print(f"📡 Scraping CKAN: {CKAN_URL}...")
    api_url = f"{CKAN_URL}/api/3/action/package_search?rows=1000"
    
    try:
        r = requests.get(api_url, timeout=30)
        r.raise_for_status()
        datasets = r.json().get('result', {}).get('results', [])
        
        md_output = "# CKAN Data Catalog\n\n"
        for ds in datasets:
            title = ds.get('title', ds.get('name'))
            url = f"{CKAN_URL}/dataset/{ds['name']}"
            notes = ds.get('notes', 'No description available.')
            org = ds.get('organization', {}).get('title', 'N/A')
            
            md_output += f"## Dataset: {title}\n"
            md_output += f"**Source Link:** {url}\n"
            md_output += f"**Organization:** {org}\n"
            md_output += f"### Description\n{notes}\n"
            md_output += "\n---\n\n"
            
        return md_output
    except Exception as e:
        print(f"❌ CKAN Error: {e}")
        return None

def upload_and_index(content):
    headers = {"Authorization": f"Bearer {API_KEY}", "Accept": "application/json"}
    
    # 1. Upload File
    print("📤 Uploading metadata to Open WebUI...")
    files = {'file': ('ckan_catalog.md', content, 'text/markdown')}
    # We set process=true so it gets embedded immediately
    r_upload = requests.post(f"{WEBUI_URL}/api/v1/files/", headers=headers, files=files)
    r_upload.raise_for_status()
    file_id = r_upload.json().get('id')
    print(f"✅ File Uploaded (ID: {file_id})")

    # 2. Wait for Processing (RAG requires content to be extracted)
    print("⏳ Waiting for vector embedding to complete...")
    for _ in range(10): # Timeout after ~20 seconds
        status_res = requests.get(f"{WEBUI_URL}/api/v1/files/{file_id}/process/status", headers=headers)
        status = status_res.json().get('status')
        if status == 'completed':
            break
        time.sleep(2)

    # 3. Add to Knowledge Collection
    print(f"🔗 Linking to Knowledge Base: {KNOWLEDGE_ID}...")
    # Open WebUI endpoint: POST /api/v1/knowledge/{id}/file/add
    payload = {"file_id": file_id}
    r_kb = requests.post(f"{WEBUI_URL}/api/v1/knowledge/{KNOWLEDGE_ID}/file/add", 
                         headers=headers, json=payload)
    
    if r_kb.status_code == 200:
        print("🚀 Success! Your AI now knows about your CKAN layers.")
    else:
        print(f"❌ KB Error: {r_kb.text}")

if __name__ == "__main__":
    if not all([API_KEY, KNOWLEDGE_ID]):
        print("❌ Error: Missing API_KEY or KNOWLEDGE_ID in .env file.")
    else:
        metadata_md = fetch_ckan_metadata()
        if metadata_md:
            upload_and_index(metadata_md)