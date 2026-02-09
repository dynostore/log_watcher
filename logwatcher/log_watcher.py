import time
import heapq
from datetime import datetime
import os
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import requests
from dotenv import load_dotenv
from urllib.parse import urlparse
import ast

load_dotenv()

API_BASE_URL = os.getenv("API_BASE_URL")

log_files_str = os.getenv("LOG_FILES")
LOG_FILES = ast.literal_eval(log_files_str)
print("LOG_FILES:", LOG_FILES)

# Track file offsets so we only read new data
#file_offsets = {f: 0 for f in LOG_FILES}
file_offsets = {}
for f in LOG_FILES:
    if os.path.exists(f):
        file_offsets[f] = os.path.getsize(f)

# Min-heap for chronological ordering
log_heap = []
#counter for client ids
global client_counter
client_counter = 0

def parse_timestamp(line):
    if line[0].isdigit():
        try:
            ts_str = line.split(",")[0]
            return datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%S.%f%z")
        except Exception:
            print("exception parsing timestamp:", line)
            pass

# Initialize offsets for already existing files
for f in LOG_FILES:
    if os.path.exists(f):
        file_offsets[f] = 0
        # Read existing lines
        with open(f, "r") as file:
            for line in file:
                ts = parse_timestamp(line)
                if ts:
                    heapq.heappush(log_heap, (ts, f, line.strip()))
        # Move offset to the end
        file_offsets[f] = os.path.getsize(f)

def create_client(client_id: str):
    payload = {
        "_key": "client_" + client_id
    }
    r = requests.post(f"{API_BASE_URL}/vertices/dynostoreclients", json=payload)
    if r.status_code == 200:
        print(f"Client {client_id} created.")
    else:
        print(f"Failed to create Client {client_id}: {r.text}")

def create_data_object(object_id: str, size: int, encrypted: bool, compressed: bool, threshold: int):
    payload = {
        "size": size,
        "_key": "object_" + object_id,
        "encrypted": encrypted,
        "compressed": compressed,
        "reconstruction_threshold": threshold
    }
    r = requests.post(f"{API_BASE_URL}/vertices/dataobjects", json=payload)
    if r.status_code == 200:
        print(f"DataObject {object_id} created.")
    else:
        print(f"Failed to create DataObject {object_id}: {r.text}")

def modify_data_container_utilization(datacontainer_id: str, utilization: float):
    payload = {
        "_key": datacontainer_id,
        "used_memory": (float(utilization) / (1024**3)) # Convert bytes to GB
    }
    r = requests.put(f"{API_BASE_URL}/vertices/datacontainers/", json=payload)
    if r.status_code == 200:
        print(f"DataContainer {datacontainer_id} utilization updated.")
    else:
        print(f"Failed to update DataContainer {datacontainer_id} utilization: {r.text}")

def get_pn_from_dc(datacontainer_id: str) -> (str | None):

    r = requests.get(f"{API_BASE_URL}/datacontainers/{datacontainer_id}/physicalnode")
    if r.status_code == 200:
        data = r.json()
        return data
    else:
        print(f"Failed to get PhysicalNode from DataContainer {datacontainer_id}: {r.text}")
        return None

def modify_physical_node_storage_utilization(physical_node, utilization: float):
    payload = physical_node
    payload["used_storage"] = (float(utilization) / (1024**3))  # Convert bytes to GB
    r = requests.put(f"{API_BASE_URL}/vertices/physicalnodes/", json=payload)
    if r.status_code == 200:
        print(f"PhysicalNode {physical_node['_key']} utilization updated.")
    else:
        print(f"Failed to update PhysicalNode {physical_node['_key']} utilization: {r.text}")

def increase_physical_node_memory_utilization(physical_node, utilization: float):
    payload = physical_node
    payload["used_memory"] = float(payload["used_memory"]) + (float(utilization) / (1024**3))  # Convert bytes to GB
    r = requests.put(f"{API_BASE_URL}/vertices/physicalnodes/", json=payload)
    if r.status_code == 200:
        print(f"PhysicalNode {physical_node['_key']} utilization updated.")
    else:
        print(f"Failed to update PhysicalNode {physical_node['_key']} utilization: {r.text}")

def increase_physical_node_storage_utilization(physical_node, utilization: float):
    payload = physical_node
    payload["used_storage"] += (utilization / (1024**3))  # Convert bytes to GB
    r = requests.put(f"{API_BASE_URL}/vertices/physicalnodes/", json=payload)
    if r.status_code == 200:
        print(f"PhysicalNode {physical_node['_key']} utilization updated.")
    else:
        print(f"Failed to update PhysicalNode {physical_node['_key']} utilization: {r.text}")

def create_metadata(object_id: str, size: int = 0):
    payload = {
        "size": size,
        "_key": "metadata_" + object_id
    }
    r = requests.post(f"{API_BASE_URL}/vertices/metadata", json=payload)
    if r.status_code == 200:
        print(f"Metadata {object_id} created.")
    else:
        print(f"Failed to create Metadata {object_id}: {r.text}")

def create_object_chunk(chunk_id: str, size: int, encrypted: bool):
    payload = {
        "size": size,
        "_key": "chunk_" + chunk_id,
        "encrypted": encrypted
    }
    r = requests.post(f"{API_BASE_URL}/vertices/objectchunks", json=payload)
    if r.status_code == 200:
        print(f"ObjectChunk {chunk_id} created.")
    else:
        print(f"Failed to create ObjectChunk {chunk_id}: {r.text}")

def connect_chunk_to_object(chunk_id: str, object_id: str):
    edge = {
        "_from": f"object_chunks/chunk_{chunk_id}",
        "_to": f"data_objects/object_{object_id}"
    }
    r = requests.post(f"{API_BASE_URL}/edges/parts", json=edge)
    if r.status_code == 200:
        print(f"Connected Chunk {chunk_id} to Object {object_id}.")
    else:
        print(f"Failed to connect Chunk {chunk_id} to Object {object_id}: {r.text}")

def connect_metadata_to_object(metadata_id: str, object_id: str):
    edge = {
        "_from": f"metadata/metadata_{metadata_id}",
        "_to": f"data_objects/object_{object_id}"
    }
    r = requests.post(f"{API_BASE_URL}/edges/references", json=edge)
    if r.status_code == 200:
        print(f"Connected Metadata {metadata_id} to Object {object_id}.")
    else:
        print(f"Failed to connect Metadata {metadata_id} to Object {object_id}: {r.text}")

def connect_metadata_to_chunk(metadata_id: str, chunk_id: str):
    edge = {
        "_from": f"metadata/metadata_{metadata_id}",
        "_to": f"object_chunks/chunk_{chunk_id}"
    }
    r = requests.post(f"{API_BASE_URL}/edges/references", json=edge)
    if r.status_code == 200:
        print(f"Connected Metadata {metadata_id} to Chunk {chunk_id}.")
    else:
        print(f"Failed to connect Metadata {metadata_id} to Chunk {chunk_id}: {r.text}")

def connect_chunk_to_datacontainer(chunk_id: str, datacontainer_id: str, up_overhead: str = None):
    edge = {
        "_from": f"object_chunks/chunk_{chunk_id}",
        "_to": f"data_containers/{datacontainer_id}",
        "up_overhead": up_overhead,
    }
    r = requests.post(f"{API_BASE_URL}/edges/stored", json=edge)
    if r.status_code == 200:
        print(f"Connected Chunk {chunk_id} to DataContainer {datacontainer_id}.")
    else:
        print(f"Failed to connect Chunk {chunk_id} to DataContainer {datacontainer_id}: {r.text}")

def connect_client_to_metadata(client_id: str, metadata_id: str):
    edge = {
        "_from": f"dynostore_clients/{client_id}",
        "_to": f"metadata/metadata_{metadata_id}"
    }
    r = requests.post(f"{API_BASE_URL}/edges/lookup", json=edge)
    if r.status_code == 200:
        print(f"Connected Client {client_id} to Metadata {metadata_id}.")
    else:
        print(f"Failed to connect Client {client_id} to Metadata {metadata_id}: {r.text}")

def connect_client_to_chunk(client_id: str, chunk_id: str):
    edge = {
        "_from": f"dynostore_clients/{client_id}",
        "_to": f"object_chunks/chunk_{chunk_id}"
    }
    r = requests.post(f"{API_BASE_URL}/edges/readwrite", json=edge)
    if r.status_code == 200:
        print(f"Connected Client {client_id} to Chunk {chunk_id}.")
    else:
        print(f"Failed to connect Client {client_id} to Chunk {chunk_id}: {r.text}")

def get_dc_id_from_uri(uri: str) -> str:
    #get data container from uri suing get request on apis
    r = requests.get(f"{API_BASE_URL}/vertices/datacontainers?uri={uri}")
    if r.status_code == 200:
        data = r.json()
        return data["_key"]
    else:
        print(f"Failed to get DataContainer ID from URI {uri}: {r.text}")
        return ""

uploading_objects = {}
replicating_objects = {}

def process_log_line(line: str):
    global client_counter
    parts = line.split(',')
    
    if len(parts) < 8:
        print("Malformed log line:", line)
        return
    
    timestamp = parts[0].strip()
    log_level = parts[1].strip()
    source = parts[2].strip()
    entity_id = parts[3].strip()
    action = parts[4].strip()
    object_id = parts[5].strip()
    progress = parts[6].strip()
    status = parts[7].strip()
    details = {}
    for p in parts[8].split(';'):
        if '=' in p:
            key, value = p.split('=')
            details[key] = value
    #print("uploading_objects:", uploading_objects)
    match source:
        case "dynostore.controllers.data":
            match action:
                case "UPLOAD_METADATA":
                    print("Upload metadata action detected.")
                    create_metadata(object_id)
                    #TODO: create lookup edge from details
                    pass
                case "UPLOAD_DATA":
                    print("Upload data action detected.")
                    match status:
                        case "STREAM_OK":
                            print("Server upload status detected.")
                            uploading_objects[object_id] = {
                                "size": details.get("bytes", 0),
                                "_key": object_id
                            }
                            pass
                        case "RUN":
                            """ print("Server upload status detected.")
                            uploading_objects[object_id] = {
                                "size": details.get("bytes", 0),
                                "_key": object_id
                            } """
                            pass
                        case "SUCCESS":
                            #TODO: update object upload time
                            print("Upload data success detected.")
                            pass
                        case _:
                            print("Unknown status for UPLOAD_DATA:", status)
                    #create_data_object(object_id, size=1024, encrypted=False, compressed=False, threshold=3)
                    pass
                case "EC":
                    print("Erasure coding action detected.")
                    pass
                case "EC_SPLIT":
                    print("Erasure coding split action detected.")
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

                    pass
                case "EC_PUSH":
                    print("Erasure coding push action detected.")
                    if "chunks" not in uploading_objects[object_id]:
                        uploading_objects[object_id]["chunks"] = {}
                    chunk_id = details.get("frag") + "_1"
                    url = details.get("url", "")
                    parsed = urlparse(url)
                    uri = parsed.hostname
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
                    print(f"Created and connected chunk {chunk_id} for object {object_id} to datacontainer {data_container_id}")
                    #TODO: update read/write operation edges between client and  object chunks
                    if len(uploading_objects[object_id]["chunks"]) == uploading_objects[object_id]["n"]:
                        print(f"All chunks for object {object_id} have been processed.")
                        uploading_objects.pop(object_id)
                    pass
                case "PULL":
                    print("Pull data action detected.")
                    if status == "INIT":
                        client_counter += 1
                        create_client(client_id=str(client_counter))
                    pass
                case "PULL_METADATA":
                    connect_client_to_metadata(client_id=f"client_{client_counter}", metadata_id=object_id)
                    print("Pull metadata action detected.")
                    pass
                case "DOWNLOAD_CHUNK":
                    match status:
                        case "RUN":
                            pass
                        case "SUCCESS":
                            chunk_number = details.get("chunk_id", None)
                            if chunk_number is not None:
                                connect_client_to_chunk(client_id=f"client_{client_counter}", chunk_id=f"{object_id}_{chunk_number}")
                            pass
                    print("Download chunk action detected.")
                    pass
                case "PULL_CHUNKS":
                    print("Needed chunks pull complete detected.")
                    pass
                case "RECONSTRUCT":
                    print("Reconstruct data action detected.")
                    pass
                case "CACHE_WRITE":
                    print("Cache write action detected.")
                    pass
                case "REPLICATE":
                    replicating_objects[object_id] = {
                        "n": details.get("n", 0),
                        "replicated_chunks": []
                    }
                    print("Data replication action detected.")
                    pass
                case "COPY_CHUNK":
                    print("Copy chunk action detected.")
                    match status:
                        case "RUN":
                            pass
                        case "SUCCESS":
                            chunk_id = details.get("frag") + "_2"
                            to = details.get("to", "")
                            data_container_id = "dc-" + to.split("_")[1]
                            replicating_objects[object_id]["replicated_chunks"].append(chunk_id)
                            connect_chunk_to_datacontainer(
                                chunk_id=object_id + "_" + chunk_id,
                                datacontainer_id=data_container_id
                            )
                            if len(replicating_objects[object_id]["replicated_chunks"]) == replicating_objects[object_id]["n"]:
                                print(f"All replicated chunks for object {object_id} have been processed.")
                                replicating_objects.pop(object_id)
                            pass
                case _:
                    print("Unknown action for data controller:", action)
            pass
        case "storage":
            match action:
                case "WRITE":
                    print("Write operation detected.")
                    data_container_id = "dc-" + str(int(entity_id.split("_")[1])-1)
                    physical_node = get_pn_from_dc(data_container_id)
                    utilization = details.get("utilization", 0)
                    #TODO: update communication edge overheads
                    overhead = details.get("total_time_ms", 0)
                    if physical_node:
                        modify_physical_node_storage_utilization(physical_node, utilization)
                    pass
                case "READ":
                    print("Read operation detected.")
                    pass
                case _:
                    print("Unknown action for storage:", action)
            print("Storage log detected.")
            pass
        case "caching":
            match action:
                case "GET":
                    print("Cache read action detected.")
                    pass
                case "PUT":
                    print("Cache write action detected.")
                    data_container_id = "dc-" + str(int(entity_id.split("_")[1])-1)
                    data_container_utilization = details.get("utilization", 0)
                    modify_data_container_utilization(data_container_id, data_container_utilization)
                    physical_node = get_pn_from_dc(data_container_id)
                    if physical_node:
                        increase_physical_node_memory_utilization(physical_node, data_container_utilization)
                    pass
                case _:
                    print("Unknown action for caching:", action)
            print("Caching log detected.")
            pass
#        case "app": #app logs can be ignored
#            pass
#        case "local": #local logs can be ignored
#            print("Local log detected.")
            pass
        case _:
            print("Unknown source:", source)

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
                        heapq.heappush(log_heap, (ts, event.src_path, line.strip()))

def main():
    observer = Observer()
    handler = LogHandler()

    # Schedule one observer per file directory
    for file in LOG_FILES:
        path = file.rsplit("/", 1)[0]
        observer.schedule(handler, path, recursive=False)

    observer.start()
    try:
        while True:
            # Process logs in chronological order
            while log_heap:
                ts, source, line = heapq.heappop(log_heap)
                #print(line)  # For demonstration, print the log line
                process_log_line(line)
            #time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

    #print("uploading_objects:", uploading_objects[list(uploading_objects.keys())[0]])

if __name__ == "__main__":
    main()
 