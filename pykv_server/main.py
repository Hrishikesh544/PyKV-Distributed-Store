from fastapi import FastAPI, Request, Form, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field, ValidationError
from collections import OrderedDict

import os
import asyncio
import threading
import time
import requests
import json  # ✅ Added JSON import for SSE serialization

app = FastAPI()

# -----------------------------
# CONFIG (Replication)
# -----------------------------
IS_PRIMARY = True  
REPLICA_URL = "http://localhost:8001"

# -----------------------------
# Static files and templates
# -----------------------------
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# -----------------------------
# Persistence (Append Only Log)
# -----------------------------
LOG_DIR = "data"
LOG_FILE = f"{LOG_DIR}/log.txt"

os.makedirs(LOG_DIR, exist_ok=True)

def write_log(command: str):
    with open(LOG_FILE, "a") as f:
        f.write(command + "\n")

# -----------------------------
# LRU Cache
# -----------------------------
class LRUCache:
    def __init__(self, capacity: int = 10):
        self.capacity = capacity
        self.cache = OrderedDict()

    def get(self, key: str):
        if key not in self.cache:
            return None
        self.cache.move_to_end(key)
        return self.cache[key]

    def put(self, key: str, value: str):
        if key in self.cache:
            self.cache.move_to_end(key)
        self.cache[key] = value
        if len(self.cache) > self.capacity:
            removed_key, _ = self.cache.popitem(last=False)
            print(f"LRU Removed: {removed_key}")

    def delete(self, key: str):
        if key in self.cache:
            del self.cache[key]

    def all_items(self):
        return dict(self.cache)

lru_store = LRUCache(capacity=10)

# -----------------------------
# Crash Recovery
# -----------------------------
def recover_from_log():
    if not os.path.exists(LOG_FILE):
        return
    with open(LOG_FILE, "r") as f:
        for line in f:
            parts = line.strip().split(" ", 2)
            if not parts: continue
            cmd = parts[0]
            if cmd == "SET" and len(parts) == 3:
                lru_store.put(parts[1], parts[2])
            elif cmd == "DEL" and len(parts) == 2:
                lru_store.delete(parts[1])

# -----------------------------
# Log Compaction
# -----------------------------
def compact_log():
    while True:
        time.sleep(60)
        temp_file = f"{LOG_DIR}/log_compacted.txt"
        with open(temp_file, "w") as f:
            for key, value in lru_store.all_items().items():
                f.write(f"SET {key} {value}\n")
        os.replace(temp_file, LOG_FILE)
        print("Log Compacted")

recover_from_log()
threading.Thread(target=compact_log, daemon=True).start()

# -----------------------------
# Pydantic Model
# -----------------------------
class Item(BaseModel):
    key: str = Field(..., min_length=1, max_length=50)
    value: str = Field(..., min_length=1, max_length=200)

# -----------------------------
# Replication Helper
# -----------------------------
def replicate_to_secondary(method: str, key: str, value: str = None):
    if not IS_PRIMARY:
        return
    try:
        if method == "SET":
            requests.post(f"{REPLICA_URL}/replica/store", json={"key": key, "value": value}, timeout=1)
        elif method == "DEL":
            requests.delete(f"{REPLICA_URL}/replica/store/{key}", timeout=1)
    except requests.exceptions.RequestException:
        print("⚠️ Replica unavailable")

# -----------------------------
# LIVE LOG STREAMING (SSE)
# -----------------------------
@app.get("/stream-log")
async def stream_log():
    """Streams the log file content to the frontend live."""
    async def log_generator():
        last_size = 0
        while True:
            if os.path.exists(LOG_FILE):
                current_size = os.path.getsize(LOG_FILE)
                if current_size != last_size:
                    with open(LOG_FILE, "r") as f:
                        content = f.read()
                        # ✅ FIXED: Serialize content to safely pass newlines over SSE
                        yield f"data: {json.dumps(content)}\n\n"
                    last_size = current_size
            await asyncio.sleep(1) 

    return StreamingResponse(log_generator(), media_type="text/event-stream")

# -----------------------------
# Routes
# -----------------------------
USERNAME = "admin"
PASSWORD = "1234"

@app.get("/", response_class=HTMLResponse)
async def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})

@app.post("/login")
async def login(request: Request, username: str = Form(...), password: str = Form(...)):
    if username == USERNAME and password == PASSWORD:
        return RedirectResponse("/dashboard", status_code=303)
    return templates.TemplateResponse("login.html", {"request": request, "error": "Invalid credentials"})

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    return templates.TemplateResponse(
        "dashboard.html",
        {"request": request, "store": lru_store.all_items(), "is_primary": IS_PRIMARY}
    )

@app.post("/add")
async def add_item(request: Request, key: str = Form(...), value: str = Form(...)):
    try:
        item = Item(key=key, value=value)
        write_log(f"SET {item.key} {item.value}")
        lru_store.put(item.key, item.value)
        replicate_to_secondary("SET", item.key, item.value)
        message, msg_type = "Key stored successfully", "success"
    except ValidationError:
        message, msg_type = "Invalid data", "error"

    return templates.TemplateResponse(
        "dashboard.html",
        {"request": request, "store": lru_store.all_items(), "message": message, "msg_type": msg_type, "is_primary": IS_PRIMARY}
    )

@app.post("/delete")
async def delete_item(request: Request, key: str = Form(...)):
    if key not in lru_store.cache:
        return templates.TemplateResponse(
            "dashboard.html",
            {"request": request, "store": lru_store.all_items(), "message": "Key not found", "msg_type": "error", "is_primary": IS_PRIMARY}
        )
    write_log(f"DEL {key}")
    lru_store.delete(key)
    replicate_to_secondary("DEL", key)
    return templates.TemplateResponse(
        "dashboard.html",
        {"request": request, "store": lru_store.all_items(), "message": "Key deleted successfully", "msg_type": "success", "is_primary": IS_PRIMARY}
    )

@app.post("/search")
async def search_item(request: Request, key: str = Form(...)):
    value = lru_store.get(key)
    message, msg_type = (f"Found! Key: {key} | Value: {value}", "success") if value else (f"Key '{key}' not found.", "error")
    return templates.TemplateResponse(
        "dashboard.html",
        {"request": request, "store": lru_store.all_items(), "message": message, "msg_type": msg_type, "is_primary": IS_PRIMARY}
    )

# -----------------------------
# API & Replica Endpoints
# -----------------------------
@app.post("/store")
async def store_item(item: Item):
    write_log(f"SET {item.key} {item.value}")
    lru_store.put(item.key, item.value)
    replicate_to_secondary("SET", item.key, item.value)
    return {"status": "stored"}

@app.get("/store/{key}")
async def get_item(key: str):
    value = lru_store.get(key)
    if value is None: raise HTTPException(status_code=404, detail="Key not found")
    return {"key": key, "value": value}

@app.delete("/store/{key}")
async def delete_item_api(key: str):
    write_log(f"DEL {key}")
    lru_store.delete(key)
    replicate_to_secondary("DEL", key)
    return {"status": "deleted"}

@app.post("/replica/store")
async def replica_store(item: Item):
    lru_store.put(item.key, item.value)
    return {"status": "replicated"}

@app.delete("/replica/store/{key}")
async def replica_delete(key: str):
    lru_store.delete(key)
    return {"status": "replicated"}
