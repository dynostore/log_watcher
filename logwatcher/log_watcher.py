import heapq
import threading
import concurrent.futures
from datetime import datetime
import atexit
import os
import sys
import time
from watchdog.observers.polling import PollingObserver as Observer
from watchdog.events import FileSystemEventHandler
import requests
from requests.adapters import HTTPAdapter
import urllib3
import logging

http_session = requests.Session()
adapter = HTTPAdapter(pool_connections=100, pool_maxsize=100)
http_session.mount('http://', adapter)
http_session.mount('https://', adapter)

from dotenv import load_dotenv
from urllib.parse import urlparse
import ast
import json

load_dotenv()


LOG_OUTPUT_FILE = os.path.abspath(os.getenv("LOG_WATCHER_LOG_FILE", "log_watcher.log"))
os.makedirs(os.path.dirname(LOG_OUTPUT_FILE), exist_ok=True)

# Configure logging
log_level = logging.WARNING if "--batch" in sys.argv else logging.INFO
logging.basicConfig(
    level=log_level,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(LOG_OUTPUT_FILE)
    ]
)
logger = logging.getLogger(__name__)



class TeeStream:
    def __init__(self, stream, log_file):
        self.stream = stream
        self.log_file = log_file
        self.encoding = getattr(stream, "encoding", "utf-8")
        self.errors = getattr(stream, "errors", "replace")

    def write(self, data):
        self.stream.write(data)
        self.log_file.write(data)

    def flush(self):
        self.stream.flush()
        self.log_file.flush()

    def isatty(self):
        return self.stream.isatty()


_original_stdout = sys.stdout
_original_stderr = sys.stderr
_log_output = open(LOG_OUTPUT_FILE, "a", encoding="utf-8", buffering=1)
sys.stdout = TeeStream(_original_stdout, _log_output)
sys.stderr = TeeStream(_original_stderr, _log_output)


def close_log_output():
    sys.stdout.flush()
    sys.stderr.flush()
    sys.stdout = _original_stdout
    sys.stderr = _original_stderr
    _log_output.close()


atexit.register(close_log_output)


API_BASE_URL = os.getenv("API_BASE_URL")

log_files_str = os.getenv("LOG_FILES")
LOG_FILES = ast.literal_eval(log_files_str)
logger.info(f"LOG_FILES: {LOG_FILES}")

# Track file offsets so we only read new data
# Load saved state if available
STATE_FILE = ".logwatcher_state.json"
saved_state = {"offsets": {}, "client_counter": 0}
if os.path.exists(STATE_FILE):
    try:
        with open(STATE_FILE, "r") as f:
            saved_state = json.load(f)
    except:
        pass

file_offsets = saved_state.get("offsets", {})
client_counter = saved_state.get("client_counter", 0)

# Min-heap for chronological ordering
log_heap = []

# Sequence number for stable heap sort
global_seq = 0

def parse_timestamp(line):
    if line[0].isdigit():
        try:
            ts_str = line.split(",")[0]
            return datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%S.%f%z")
        except Exception:
            logger.warning(f"exception parsing timestamp: {line}")
            pass

# Initialize offsets for already existing files
for f in LOG_FILES:
    if os.path.exists(f):
        offset = file_offsets.get(f, 0)
        # Read existing lines
        with open(f, "r") as file:
            file.seek(offset)
            for line in file:
                ts = parse_timestamp(line)
                if ts:
                    global_seq += 1
                    heapq.heappush(log_heap, (ts, global_seq, f, line.strip()))
        # Move offset to the end
        file_offsets[f] = os.path.getsize(f)

def create_client(client_id: str):
    payload = {
        "_key": "client_" + client_id
    }
    r = http_session.post(f"{API_BASE_URL}/vertices/dynostoreclients", json=payload)
    if r.status_code == 200:
        logger.info(f"Client {client_id} created.")
    else:
        logger.warning(f"Failed to create Client {client_id}: {r.text}")

def create_data_object(object_id: str, size: int, encrypted: bool, compressed: bool, threshold: int):
    payload = {
        "size": size,
        "_key": "object_" + object_id,
        "encrypted": encrypted,
        "compressed": compressed,
        "reconstruction_threshold": threshold
    }
    r = http_session.post(f"{API_BASE_URL}/vertices/dataobjects", json=payload)
    if r.status_code == 200:
        logger.info(f"DataObject {object_id} created.")
    else:
        logger.warning(f"Failed to create DataObject {object_id}: {r.text}")


state_lock = threading.Lock()
_batched_dc_utilization = {}
_batched_pn_utilization = {}

def flush_utilization_batches():
    for dc_id, mem in _batched_dc_utilization.items():
        payload = {"_key": dc_id, "used_memory": mem}
        r = http_session.put(f"{API_BASE_URL}/vertices/datacontainers/", json=payload)
        if r.status_code != 200:
            logger.warning(f"Failed to update DataContainer {dc_id} utilization: {r.text}")
    _batched_dc_utilization.clear()

    for pn_id, payload in _batched_pn_utilization.items():
        r = http_session.put(f"{API_BASE_URL}/vertices/physicalnodes/", json=payload)
        if r.status_code != 200:
            logger.warning(f"Failed to update PhysicalNode {pn_id} utilization: {r.text}")
    _batched_pn_utilization.clear()

def modify_data_container_utilization(datacontainer_id: str, utilization: float):
    with state_lock:
        _batched_dc_utilization[datacontainer_id] = (float(utilization) / (1024**3))

_pn_cache = {}
def get_pn_from_dc(datacontainer_id: str) -> (str | None):
    if datacontainer_id in _pn_cache: return _pn_cache[datacontainer_id]

    r = http_session.get(f"{API_BASE_URL}/datacontainers/{datacontainer_id}/physicalnode")
    if r.status_code == 200:
        data = r.json()
        _pn_cache[datacontainer_id] = data
        return data
    else:
        logger.warning(f"Failed to get PhysicalNode from DataContainer {datacontainer_id}: {r.text}")
        return None

def modify_physical_node_storage_utilization(physical_node, utilization: float):
    with state_lock:
        pn_id = physical_node["_key"]
        if pn_id not in _batched_pn_utilization:
            _batched_pn_utilization[pn_id] = physical_node.copy()
        _batched_pn_utilization[pn_id]["used_storage"] = (float(utilization) / (1024**3))

def increase_physical_node_memory_utilization(physical_node, utilization: float):
    with state_lock:
        pn_id = physical_node["_key"]
        if pn_id not in _batched_pn_utilization:
            _batched_pn_utilization[pn_id] = physical_node.copy()
        _batched_pn_utilization[pn_id]["used_memory"] = float(_batched_pn_utilization[pn_id].get("used_memory", 0)) + (float(utilization) / (1024**3))

def increase_physical_node_storage_utilization(physical_node, utilization: float):
    with state_lock:
        pn_id = physical_node["_key"]
        if pn_id not in _batched_pn_utilization:
            _batched_pn_utilization[pn_id] = physical_node.copy()
        _batched_pn_utilization[pn_id]["used_storage"] = float(_batched_pn_utilization[pn_id].get("used_storage", 0)) + (float(utilization) / (1024**3))

def create_metadata(object_id: str, size: int = 0):
    payload = {
        "size": size,
        "_key": "metadata_" + object_id
    }
    r = http_session.post(f"{API_BASE_URL}/vertices/metadata", json=payload)
    if r.status_code == 200:
        logger.info(f"Metadata {object_id} created.")
    else:
        logger.warning(f"Failed to create Metadata {object_id}: {r.text}")

def create_object_chunk(chunk_id: str, size: int, encrypted: bool):
    payload = {
        "size": size,
        "_key": "chunk_" + chunk_id,
        "encrypted": encrypted
    }
    r = http_session.post(f"{API_BASE_URL}/vertices/objectchunks", json=payload)
    if r.status_code == 200:
        logger.info(f"ObjectChunk {chunk_id} created.")
    else:
        logger.warning(f"Failed to create ObjectChunk {chunk_id}: {r.text}")

def connect_chunk_to_object(chunk_id: str, object_id: str):
    edge = {
        "_from": f"object_chunks/chunk_{chunk_id}",
        "_to": f"data_objects/object_{object_id}"
    }
    r = http_session.post(f"{API_BASE_URL}/edges/parts", json=edge)
    if r.status_code == 200:
        logger.info(f"Connected Chunk {chunk_id} to Object {object_id}.")
    else:
        logger.warning(f"Failed to connect Chunk {chunk_id} to Object {object_id}: {r.text}")

def connect_metadata_to_object(metadata_id: str, object_id: str):
    edge = {
        "_from": f"metadata/metadata_{metadata_id}",
        "_to": f"data_objects/object_{object_id}"
    }
    r = http_session.post(f"{API_BASE_URL}/edges/references", json=edge)
    if r.status_code == 200:
        logger.info(f"Connected Metadata {metadata_id} to Object {object_id}.")
    else:
        logger.warning(f"Failed to connect Metadata {metadata_id} to Object {object_id}: {r.text}")

def connect_metadata_to_chunk(metadata_id: str, chunk_id: str):
    edge = {
        "_from": f"metadata/metadata_{metadata_id}",
        "_to": f"object_chunks/chunk_{chunk_id}"
    }
    r = http_session.post(f"{API_BASE_URL}/edges/references", json=edge)
    if r.status_code == 200:
        logger.info(f"Connected Metadata {metadata_id} to Chunk {chunk_id}.")
    else:
        logger.warning(f"Failed to connect Metadata {metadata_id} to Chunk {chunk_id}: {r.text}")

def connect_chunk_to_datacontainer(chunk_id: str, datacontainer_id: str, up_overhead: str = None):
    edge = {
        "_from": f"object_chunks/chunk_{chunk_id}",
        "_to": f"data_containers/{datacontainer_id}",
        "up_overhead": up_overhead,
    }
    r = http_session.post(f"{API_BASE_URL}/edges/stored", json=edge)
    if r.status_code == 200:
        logger.info(f"Connected Chunk {chunk_id} to DataContainer {datacontainer_id}.")
    else:
        logger.warning(f"Failed to connect Chunk {chunk_id} to DataContainer {datacontainer_id}: {r.text}")

def connect_client_to_metadata(client_id: str, metadata_id: str, timestamp: int):
    edge = {
        "_from": f"dynostore_clients/{client_id}",
        "_to": f"metadata/metadata_{metadata_id}",
        "timestamp": timestamp
    }
    r = http_session.post(f"{API_BASE_URL}/edges/lookup", json=edge)
    if r.status_code == 200:
        logger.info(f"Connected Client {client_id} to Metadata {metadata_id}.")
    else:
        logger.warning(f"Failed to connect Client {client_id} to Metadata {metadata_id}: {r.text}")

def connect_client_to_chunk(client_id: str, chunk_id: str, timestamp: int, operation: str):
    edge = {
        "_from": f"dynostore_clients/{client_id}",
        "_to": f"object_chunks/chunk_{chunk_id}",
        "timestamp": timestamp,
        "operation": operation
    }
    r = http_session.post(f"{API_BASE_URL}/edges/readwrite", json=edge)
    if r.status_code == 200:
        logger.info(f"Connected Client {client_id} to Chunk {chunk_id}.")
    else:
        logger.warning(f"Failed to connect Client {client_id} to Chunk {chunk_id}: {r.text}")

_dc_cache = {}
def get_dc_id_from_uri(uri: str) -> str:
    if uri in _dc_cache: return _dc_cache[uri]
    #get data container from uri suing get request on apis
    r = http_session.get(f"{API_BASE_URL}/vertices/datacontainers?uri={uri}")
    #print(f"Requesting DataContainer ID from URI {uri}: {r.status_code}")
    if r.status_code == 200:
        data = r.json()
        _dc_cache[uri] = data["_key"]
        return data["_key"]
    else:
        logger.warning(f"Failed to get DataContainer ID from URI {uri}: {r.text}")
        return ""

uploading_objects = {}
object_to_client = {}
replicating_objects = {}

def process_log_line(line: str):
    global client_counter
    parts = line.split(',')
    
    ## ToDo: handle malformed log lines more robustly this is just a turn around to avoid crashes during development
    #print("Processing log line:", line)
    #print("size of parts:", len(parts))
    #print((not "dynostore" in line) and len(parts) < 8, (not "dynostore" in line), len(parts) < 8)
    
    if len(parts) < 8:
        logger.warning(f"Malformed log line: {line}")
        return
    
    if not "dynostore" in line:
        #print("Log line is not from dynostore:", line)
        return

    timestamp = datetime.fromisoformat(parts[0].strip())
    log_level = parts[1].strip()
    source = parts[2].strip()
    entity_id = parts[3].strip()
    action = parts[4].strip()
    object_id = parts[5].strip()
    progress = parts[6].strip()
    status = parts[7].strip()
    details = {}
    try:
        for p in parts[8].split(';'):
            if '=' in p:
                key, value = p.split('=')
                details[key] = value
    except Exception:
        logger.warning(f"Exception parsing details: {parts}")
        return
    #print("uploading_objects:", uploading_objects)
    match source:
        case "dynostore.controllers.data":
            match action:
                case "UPLOAD_METADATA":
                    logger.info("Upload metadata action detected.")
                    create_metadata(object_id)
                    #TODO: create lookup edge from details
                    pass
                case "UPLOAD_DATA":
                    logger.info("Upload data action detected.")
                    match status:
                        case "STREAM_OK":
                            logger.info("Server upload status detected.")
                            uploading_objects[object_id] = {
                                "size": details.get("bytes", 0),
                                "_key": object_id
                            }
                            pass
                        case "RUN":
                            """ logger.info("Server upload status detected.")
                            uploading_objects[object_id] = {
                                "size": details.get("bytes", 0),
                                "_key": object_id
                            } """
                            pass
                        case "SUCCESS":
                            #TODO: update object upload time
                            logger.info("Upload data success detected.")
                            pass
                        case _:
                            logger.warning(f"Unknown status for UPLOAD_DATA: {status}")
                    #create_data_object(object_id, size=1024, encrypted=False, compressed=False, threshold=3)
                    pass
                case "EC":
                    logger.info("Erasure coding action detected.")
                    pass
                case "EC_SPLIT":
                    logger.info("Erasure coding split action detected.")
                    with state_lock:
                        client_counter += 1
                        cc = client_counter
                    object_to_client[object_id] = cc
                    create_client(client_id=str(cc))
                    if object_id not in uploading_objects:
                        uploading_objects[object_id] = {"size": 0}
                    uploading_objects[object_id]["reconstruction_threshold"] = details.get("k", 0)
                    create_data_object(
                        object_id,
                        size=uploading_objects[object_id]["size"],
                        encrypted=False,
                        compressed=False,
                        threshold=uploading_objects[object_id]["reconstruction_threshold"]
                    )
                    connect_metadata_to_object(
                        metadata_id=object_id,
                        object_id=object_id
                    )
                    uploading_objects[object_id]["n"] = details.get("n", 0)
                    if "chunks" in uploading_objects[object_id] and len(uploading_objects[object_id]["chunks"]) == uploading_objects[object_id]["n"]:
                        logger.info(f"All chunks for object {object_id} have been processed.")
                        uploading_objects.pop(object_id)
                    pass
                case "EC_PUSH":
                    logger.info("Erasure coding push action detected.")
                    if object_id not in uploading_objects:
                        uploading_objects[object_id] = {"size": 0}
                    if "chunks" not in uploading_objects[object_id]:
                        uploading_objects[object_id]["chunks"] = {}
                    chunk_id = details.get("frag") + "_1"
                    url = details.get("url", "")
                    parsed = urlparse(url)
                    uri = parsed.netloc
                    data_container_id = get_dc_id_from_uri(uri)
                    uploading_objects[object_id]["chunks"][chunk_id] = {
                        "size": details.get("bytes", 0),
                        "upload_time": details.get("time_ms", 0),
                        "dc": data_container_id
                    }
                    create_object_chunk(
                        chunk_id=object_id + "_" + chunk_id,
                        size=uploading_objects[object_id]["chunks"][chunk_id]["size"],
                        encrypted=False
                    )
                    connect_chunk_to_object(
                        chunk_id=object_id + "_" + chunk_id,
                        object_id=object_id
                    )
                    connect_chunk_to_datacontainer(
                        chunk_id=object_id + "_" + chunk_id,
                        datacontainer_id=data_container_id,
                        up_overhead=uploading_objects[object_id]["chunks"][chunk_id].get("upload_time", None)
                    )
                    connect_metadata_to_chunk(
                        metadata_id=object_id,
                        chunk_id=object_id + "_" + chunk_id
                    )
                    connect_client_to_chunk(
                        client_id=f"client_{object_to_client.get(object_id, client_counter)}",
                        chunk_id=object_id + "_" + chunk_id,
                        timestamp=timestamp.timestamp(),
                        operation="WRITE"
                    )
                    logger.info(f"Created and connected chunk {chunk_id} for object {object_id} to datacontainer {data_container_id}")
                    #TODO: update read/write operation edges between client and  object chunks
                    #print(line)
                    if len(uploading_objects[object_id]["chunks"]) == uploading_objects[object_id].get("n", -1):
                        logger.info(f"All chunks for object {object_id} have been processed.")
                        uploading_objects.pop(object_id)
                    pass
                case "PULL":
                    logger.info("Pull data action detected.")
                    if status == "INIT":
                        with state_lock:
                            client_counter += 1
                            cc = client_counter
                        object_to_client[object_id] = cc
                        create_client(client_id=str(cc))
                    pass
                case "PULL_METADATA":
                    connect_client_to_metadata(client_id=f"client_{object_to_client.get(object_id, client_counter)}", metadata_id=object_id, timestamp=timestamp.timestamp())
                    logger.info("Pull metadata action detected.")
                    pass
                case "DOWNLOAD_CHUNK":
                    match status:
                        case "RUN":
                            pass
                        case "SUCCESS":
                            chunk_number = details.get("chunk_id", None)
                            if chunk_number is not None:
                                connect_client_to_chunk(client_id=f"client_{object_to_client.get(object_id, client_counter)}", chunk_id=f"{object_id}_{chunk_number}", timestamp=timestamp.timestamp(), operation="READ")
                            pass
                    logger.info("Download chunk action detected.")
                    pass
                case "PULL_CHUNKS":
                    logger.info("Needed chunks pull complete detected.")
                    pass
                case "RECONSTRUCT":
                    logger.info("Reconstruct data action detected.")
                    pass
                case "CACHE_WRITE":
                    logger.info("Cache write action detected.")
                    pass
                case "REPLICATE":
                    replicating_objects[object_id] = {
                        "n": details.get("n", 0),
                        "replicated_chunks": []
                    }
                    logger.info("Data replication action detected.")
                    pass
                case "COPY_CHUNK":
                    logger.info("Copy chunk action detected.")
                    match status:
                        case "RUN":
                            pass
                        case "SUCCESS":
                            chunk_id = details.get("frag") + "_2"
                            to = details.get("to", "")
                            data_container_id = get_dc_id_from_uri(to)
                            replicating_objects[object_id]["replicated_chunks"].append(chunk_id)
                            connect_chunk_to_datacontainer(
                                chunk_id=object_id + "_" + chunk_id,
                                datacontainer_id=data_container_id
                            )
                            
                            if len(replicating_objects[object_id]["replicated_chunks"]) == replicating_objects[object_id]["n"]:
                                logger.info(f"All replicated chunks for object {object_id} have been processed.")
                                replicating_objects.pop(object_id)
                            pass
                case _:
                    logger.warning(f"Unknown action for data controller: {action}")
            pass
        case "dynostore.storage" | "storage":
            match action:
                case "WRITE":
                    logger.info("Write operation detected.")
                    data_container_id = get_dc_id_from_uri(entity_id)
                    physical_node = get_pn_from_dc(data_container_id)
                    utilization = details.get("utilization", 0)
                    #TODO: update communication edge overheads
                    overhead = details.get("total_time_ms", 0)
                    if physical_node:
                        modify_physical_node_storage_utilization(physical_node, utilization)
                    pass
                case "READ":
                    logger.info("Read operation detected.")
                    pass
                case _:
                    logger.warning(f"Unknown action for storage: {action}")
            logger.info("Storage log detected.")
            pass
        case "dynostore.caching" | "caching":
            match action:
                case "GET":
                    logger.info("Cache read action detected.")
                    pass
                case "PUT":
                    logger.info("Cache write action detected.")
                    data_container_id = get_dc_id_from_uri(entity_id)
                    data_container_utilization = details.get("utilization", 0)
                    modify_data_container_utilization(data_container_id, data_container_utilization)
                    physical_node = get_pn_from_dc(data_container_id)
                    if physical_node:
                        increase_physical_node_memory_utilization(physical_node, data_container_utilization)
                    pass
                case _:
                    logger.warning(f"Unknown action for caching: {action}")
            logger.info("Caching log detected.")
            pass
#        case "app": #app logs can be ignored
#            pass
#        case "local": #local logs can be ignored
#            logger.info("Local log detected.")
            pass
        case _:
            logger.warning(f"Unknown source: {source}")

class LogHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.src_path in LOG_FILES:
            file_offsets[event.src_path] = 0  # start tracking new file
    
    def on_modified(self, event):
        if event.src_path in LOG_FILES and os.path.exists(event.src_path):
            with open(event.src_path, "r") as f:
                f.seek(file_offsets[event.src_path])  # jump to last read position
                new_lines = f.readlines()
                file_offsets[event.src_path] = f.tell()  # update position
                for line in new_lines:
                    ts = parse_timestamp(line)
                    if ts:
                        global global_seq
                        global_seq += 1
                        heapq.heappush(log_heap, (ts, global_seq, event.src_path, line.strip()))

def main():
    global client_counter
    if "--batch" in sys.argv:
        logger.info("Running in batch mode: processing existing logs and exiting...")
        
        grouped_logs = {}
        no_obj_logs = []
        while log_heap:
            popped = heapq.heappop(log_heap)
            line = popped[-1]
            parts = line.split(',')
            if len(parts) >= 8 and "dynostore" in line:
                object_id = parts[5].strip()
                if object_id:
                    if object_id not in grouped_logs:
                        grouped_logs[object_id] = []
                    grouped_logs[object_id].append(line)
                else:
                    no_obj_logs.append(line)
            else:
                no_obj_logs.append(line)

        for line in no_obj_logs:
            process_log_line(line)
            
        def process_group(lines):
            for line in lines:
                process_log_line(line)
                
        logger.info(f"Processing {len(grouped_logs)} objects in parallel...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor:
            executor.map(process_group, grouped_logs.values())

        logger.info("Batch mode complete. Flushing utilization updates...")
        flush_utilization_batches()
        logger.info("Saving state...")
        with open(STATE_FILE, "w") as f:
            json.dump({"offsets": file_offsets, "client_counter": client_counter}, f)
        return

    observer = Observer()
    handler = LogHandler()

    # Schedule one observer per file directory
    paths_to_watch = set()
    for file in LOG_FILES:
        path = file.rsplit("/", 1)[0]
        paths_to_watch.add(path)
        
    for path in paths_to_watch:
        observer.schedule(handler, path, recursive=False)

    observer.start()
    try:
        while True:
            # Process logs in chronological order
            while log_heap:
                popped = heapq.heappop(log_heap)
                line = popped[-1]
                #print(line)  # For demonstration, print the log line
                process_log_line(line)
            flush_utilization_batches()
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

    #print("uploading_objects:", uploading_objects[list(uploading_objects.keys())[0]])

if __name__ == "__main__":
    main()
