import logging
import socket
import dill
import json
import base64
import time

from .util import *


MAX_DATA = 65536

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

def pyfaas_config(file_path=None):
    global CONFIG_FILE_PATH, PYFAAS_CONFIG, PYFAAS_CONFIGURED
    if not file_path:
        logging.warning(f"Unspecified PyFaaS configuration file path, defaulting to {DEFAULT_CONFIG_FILE_PATH}")
        CONFIG_FILE_PATH = DEFAULT_CONFIG_FILE_PATH
    else:
        CONFIG_FILE_PATH = file_path

    PYFAAS_CONFIG = read_config_toml(CONFIG_FILE_PATH)
    setup_logging(PYFAAS_CONFIG['misc']['log_level'])

    PYFAAS_CONFIGURED = True
    logging.info(f"PyFaaS has been configured using {CONFIG_FILE_PATH}")


def pyfaas_get_stats():
    if not PYFAAS_CONFIGURED:
        logging.warning("PyFaaS was not previously configured by calling pyfaas_config()")
        pyfaas_config()
    
    cmd = "get_stats"
    pass

def pyfaas_kill_worker():
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


def pyfaas_exec(func_code, func_arglist, func_kwargslist, dependencies):
    if not PYFAAS_CONFIGURED:
        logging.warning("PyFaaS was not previously configured by calling pyfaas_config()")
        pyfaas_config()

    cmd = "exec"
    
    logging.debug(f"Called faas_exec. Args: {func_code, func_arglist, func_kwargslist, dependencies}")

    worker_ip_port_tuple = (PYFAAS_CONFIG['network']['worker_ip_addr'], PYFAAS_CONFIG['network']['worker_port'])

    # Serialize simple func
    encoding_start = time.time()
    serialized_func = dill.dumps(func_code)
    serialized_func_base64 = base64.b64encode(serialized_func).decode("utf-8")
    logging.debug(f"Base64-encoded function: {serialized_func_base64}")
    encoding_end = time.time()
    logging.debug(f"Function encoding took {encoding_end - encoding_start} s")

    json_payload = {                 # To be sent to server
        "cmd": cmd,
        "serialized_func_base64": serialized_func_base64,
        "args": func_arglist,
        "kwargs": func_kwargslist
    }

    # Send to worker through socket
    res = None
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect(worker_ip_port_tuple)
        s.sendall(json.dumps(json_payload).encode())

        res_bytes = s.recv(MAX_DATA)
        res = dill.loads(res_bytes)

    return res





def pyfaas_ping():    
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
