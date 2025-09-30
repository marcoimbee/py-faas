import logging
import socket
import dill
import json
import base64
import time
from typing import Callable

from .util import *


MAX_DATA = 4096

# --- PyFaaS configuration
PYFAAS_CONFIGURED = False
CONFIG_FILE_PATH = None
DEFAULT_CONFIG_FILE_PATH = "test/client_config.toml"
PYFAAS_CONFIG = None

logging.basicConfig(
    format='[PYFAAS, %(levelname)s]    %(message)s',
    level=logging.WARNING,   # default until config is loaded
    force=True
)

def pyfaas_config(file_path: str = None) -> None:
    global CONFIG_FILE_PATH, PYFAAS_CONFIG, PYFAAS_CONFIGURED
    if not file_path:
        logging.warning(f"Unspecified PyFaaS configuration file path, defaulting to {DEFAULT_CONFIG_FILE_PATH}")
        CONFIG_FILE_PATH = DEFAULT_CONFIG_FILE_PATH
    else:
        CONFIG_FILE_PATH = file_path

    try:
        PYFAAS_CONFIG = read_config_toml(CONFIG_FILE_PATH)
    except Exception as e:
        raise Exception(e)

    setup_logging(PYFAAS_CONFIG['misc']['log_level'])

    PYFAAS_CONFIGURED = True
    logging.info(f"PyFaaS has been configured using {CONFIG_FILE_PATH}")


# Function name is not necessary, as it can be extracted from the code via function.__name__
# override: if True, if the worker already has registered a function with the same name, 
#           will override the previous one and register this new one instead with this name
def pyfaas_register(func_code: Callable, override: bool = True) -> int:
    if not PYFAAS_CONFIGURED:
        logging.warning("PyFaaS was not previously configured by calling pyfaas_config()")
        pyfaas_config()
    
    cmd = "register"

    worker_ip_port_tuple = (PYFAAS_CONFIG['network']['worker_ip_addr'], PYFAAS_CONFIG['network']['worker_port'])

    # Serializing func
    encoding_start = time.time()
    serialized_func = dill.dumps(func_code)
    serialized_func_base64 = base64.b64encode(serialized_func).decode("utf-8")
    logging.debug(f"Base64-encoded function: {serialized_func_base64}")
    encoding_end = time.time()
    logging.debug(f"Function encoding took {encoding_end - encoding_start} s")

    json_payload = {                 # To be sent to server
        "cmd": cmd,
        "serialized_func_base64": serialized_func_base64,
        "override": override
    }

    json_payload_bytes = json.dumps(json_payload).encode()

    # Send to worker through socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect(worker_ip_port_tuple)
        s.sendall(json_payload_bytes)

        worker_resp_bytes = s.recv(MAX_DATA)
        worker_resp_json = json.loads(worker_resp_bytes.decode())

    func_name = func_code.__name__
    status = worker_resp_json.get("status")
    action = worker_resp_json.get("action")
    message = worker_resp_json.get("message")
    if status == "ok":
        if action == "registered":
            logging.info(f"Successfully registered '{func_name}'")
        elif action == "overridden":
            logging.info(f"Successfully overridden '{func_name}'")
        elif action == "no_action":
            logging.info(f"No action was performed")
        return 1
    else:
        logging.warning(f"Error while registering a function: {message}")
        return -1


def pyfaas_unregister(func_name: str) -> int:
    if not PYFAAS_CONFIGURED:
        logging.warning("PyFaaS was not previously configured by calling pyfaas_config()")
        pyfaas_config()

    cmd = "unregister"

    worker_ip_port_tuple = (PYFAAS_CONFIG['network']['worker_ip_addr'], PYFAAS_CONFIG['network']['worker_port'])

    json_payload = {                 # To be sent to server
        "cmd": cmd,
        "func_name": func_name
    }

    json_payload_bytes = json.dumps(json_payload).encode()

    # Send to worker through socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect(worker_ip_port_tuple)
        s.sendall(json_payload_bytes)

        worker_resp_bytes = s.recv(MAX_DATA)
        worker_resp_json = json.loads(worker_resp_bytes.decode())

    status = worker_resp_json.get("status")
    action = worker_resp_json.get("action")
    message = worker_resp_json.get("message")
    if status == "ok":
        if action == "unregistered":
            logging.info(f"Successfully unregistered {func_name}()")
            return 1
    elif status == "err":
        logging.warning(f"Error while unregistering a function: {message}")
        return -1

def pyfaas_get_stats(func_name: str = None) -> int | dict:
    if not PYFAAS_CONFIGURED:
        logging.warning("PyFaaS was not previously configured by calling pyfaas_config()")
        pyfaas_config()
    
    cmd = "get_stats"

    json_payload = {
        "cmd": cmd,
        "func_name": func_name
    }

    worker_ip_port_tuple = (PYFAAS_CONFIG['network']['worker_ip_addr'], PYFAAS_CONFIG['network']['worker_port'])
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect(worker_ip_port_tuple)
        s.sendall(json.dumps(json_payload).encode())

        worker_resp_bytes = s.recv(MAX_DATA)
        worker_resp_json = json.loads(worker_resp_bytes.decode())

    status = worker_resp_json.get("status")
    json_stats = worker_resp_json.get("result")
    message = worker_resp_json.get("message")

    if status == "ok":
        if func_name != None:
            logging.info(f"Retrieved stats for '{func_name}'")
        else:
            logging.info(f"Retrieved general stats")
        logging.debug(f"Stats: {json_stats}")
        return json_stats
    else:
        if func_name != None:
            logging.error(f"Error while retrieving stats for '{func_name}': {message}")
        else:
            logging.error(f"Error while retrieving general stats: {message}")
        return -1


def pyfaas_kill_worker() -> None:
    if not PYFAAS_CONFIGURED:
        logging.warning("PyFaaS was not previously configured by calling pyfaas_config()")
        pyfaas_config()

    cmd = "kill"
    logging.debug(f"Called pyfaas_kill_worker")

    json_payload = {
        "cmd": cmd
    }
    
    worker_ip_port_tuple = (PYFAAS_CONFIG['network']['worker_ip_addr'], PYFAAS_CONFIG['network']['worker_port'])
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect(worker_ip_port_tuple)
        s.sendall(json.dumps(json_payload).encode())
    logging.info("Worker killed by client")

def pyfaas_list() -> int | list[str]:
    if not PYFAAS_CONFIGURED:
        logging.warning("PyFaaS was not previously configured by calling pyfaas_config()")
        pyfaas_config()

    cmd = "list"

    worker_ip_port_tuple = (PYFAAS_CONFIG['network']['worker_ip_addr'], PYFAAS_CONFIG['network']['worker_port'])

    json_payload = {                 # To be sent to server
        "cmd": cmd
    }

    json_payload_bytes = json.dumps(json_payload).encode()

    # Send to worker through socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect(worker_ip_port_tuple)
        s.sendall(json_payload_bytes)

        worker_resp_bytes = s.recv(MAX_DATA)
        worker_resp_json = json.loads(worker_resp_bytes.decode())

    status = worker_resp_json.get("status")
    func_list = worker_resp_json.get("result")
    message = worker_resp_json.get("message")

    if status == "ok":
        logging.info(f"Retrieved {len(func_list)} functions")
        return func_list
    else:
        logging.warning(f"Error while listing functions on the worker: {message}")
        return -1

def pyfaas_exec(func_name: str, func_arglist: list[object], func_kwargslist: list[object]) -> object:
    if not PYFAAS_CONFIGURED:
        logging.warning("PyFaaS was not previously configured by calling pyfaas_config()")
        pyfaas_config()

    cmd = "exec"
    
    logging.debug(f"Called faas_exec. Args: {func_name, func_arglist, func_kwargslist}")

    worker_ip_port_tuple = (PYFAAS_CONFIG['network']['worker_ip_addr'], PYFAAS_CONFIG['network']['worker_port'])

    json_payload = {                 # To be sent to server
        "cmd": cmd,
        "func_name": func_name,
        "args": func_arglist,
        "kwargs": func_kwargslist,
        "additional_data": None
    }

    json_payload_bytes = json.dumps(json_payload).encode()

    # Send to worker through socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect(worker_ip_port_tuple)
        s.sendall(json_payload_bytes)

        worker_resp_bytes = s.recv(MAX_DATA)
        worker_resp_json = json.loads(worker_resp_bytes.decode())

    status = worker_resp_json.get("status")
    action = worker_resp_json.get("action")
    result_type = worker_resp_json.get("result_type")
    result = worker_resp_json.get("result")
    message = worker_resp_json.get("message")

    if status == "ok":
        if action == "executed":
            logging.info(f"Executed '{func_name}'")
            if result_type == "pickle_base64":
                result_bytes = base64.b64decode(result)
                result = dill.loads(result_bytes)
            return result      # it's the JSON result that was included in the worker msg, or the deserialized Base64 result
    else:
        logging.warning(f"Error while executing '{func_name}' on the worker: {message}")
        return -1


def pyfaas_ping() -> None:
    if not PYFAAS_CONFIGURED:
        logging.warning("PyFaaS was not previously configured by calling pyfaas_config()")
        pyfaas_config()
    cmd = "PING"
    json_payload = {
        "cmd": cmd
    }
    
    worker_ip_port_tuple = (PYFAAS_CONFIG['network']['worker_ip_addr'], PYFAAS_CONFIG['network']['worker_port'])
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect(worker_ip_port_tuple)
        s.sendall(json.dumps(json_payload).encode())
        worker_response = s.recv(MAX_DATA)
        worker_response = worker_response.decode()
        logging.info(f"Worker says '{worker_response}'")
