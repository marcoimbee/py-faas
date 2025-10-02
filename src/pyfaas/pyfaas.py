import logging
import socket
import dill
import json
import base64
import time
from typing import Callable

from .util import *


# --- PyFaaS configuration
_PYFAAS_CONFIGURED: bool = False
_CONFIG_FILE_PATH: str | None = None
_DEFAULT_CONFIG_FILE_PATH: str = 'test/client_config.toml'
_PYFAAS_CONFIG: dict | None = None

# --- PyFaaS networking
_CLIENT_SOCKET: socket.socket | None = None

logging.basicConfig(
    format='[PYFAAS, %(levelname)s]    %(message)s',
    level=logging.WARNING,   # default until config is loaded
    force=True
)

def pyfaas_config(file_path: str = None) -> None:
    global _CONFIG_FILE_PATH, _PYFAAS_CONFIG, _PYFAAS_CONFIGURED, _CLIENT_SOCKET
    if not file_path:
        logging.warning(f'Unspecified PyFaaS configuration file path, defaulting to {_DEFAULT_CONFIG_FILE_PATH}')
        _CONFIG_FILE_PATH = _DEFAULT_CONFIG_FILE_PATH
    else:
        _CONFIG_FILE_PATH = file_path

    try:
        _PYFAAS_CONFIG = read_config_toml(_CONFIG_FILE_PATH)
    except Exception as e:
        raise Exception(e)

    setup_logging(_PYFAAS_CONFIG['misc']['log_level'])

    if _CLIENT_SOCKET is None:
        worker_ip_port_tuple = (
            _PYFAAS_CONFIG['network']['worker_ip_addr'],
            _PYFAAS_CONFIG['network']['worker_port']
        )
        _CLIENT_SOCKET = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        _CLIENT_SOCKET.settimeout(10)      # 10 s timeout -> socket closes
        _CLIENT_SOCKET.connect(worker_ip_port_tuple)
        logging.info(f'Persistent socket connected to {worker_ip_port_tuple}')

    _PYFAAS_CONFIGURED = True
    logging.info(f'PyFaaS has been configured using {_CONFIG_FILE_PATH}')


# Function name is not necessary, as it can be extracted from the code via function.__name__
# override: if True, if the worker already has registered a function with the same name, 
#           will override the previous one and register this new one instead with this name
def pyfaas_register(func_code: Callable, override: bool = True) -> int:
    if not _PYFAAS_CONFIGURED:
        logging.warning('PyFaaS was not previously configured by calling pyfaas_config()')
        pyfaas_config()

    global _CLIENT_SOCKET
    
    cmd = 'register'

    # Serializing func
    encoding_start = time.time()
    serialized_func = dill.dumps(func_code)
    serialized_func_base64 = base64.b64encode(serialized_func).decode('utf-8')
    logging.debug(f'Base64-encoded function: {serialized_func_base64}')
    encoding_end = time.time()
    logging.debug(f'Function encoding took {encoding_end - encoding_start} s')

    json_payload = {                 # To be sent to server
        'cmd': cmd,
        'serialized_func_base64': serialized_func_base64,
        'override': override
    }

    # Send to worker through socket
    _send_msg(_CLIENT_SOCKET, json_payload)

    # Get worker payload
    worker_resp_json = _recv_msg(_CLIENT_SOCKET)

    func_name = func_code.__name__
    status = worker_resp_json.get('status')
    action = worker_resp_json.get('action')
    message = worker_resp_json.get('message')
    if status == 'ok':
        if action == 'registered':
            logging.info(f"Successfully registered '{func_name}'")
        elif action == 'overridden':
            logging.info(f"Successfully overridden '{func_name}'")
        elif action == 'no_action':
            logging.info(f'No action was performed')
        return 1
    else:
        logging.warning(f'Error while registering a function: {message}')
        return -1


def pyfaas_unregister(func_name: str) -> int:
    if not _PYFAAS_CONFIGURED:
        logging.warning('PyFaaS was not previously configured by calling pyfaas_config()')
        pyfaas_config()

    global _CLIENT_SOCKET

    cmd = 'unregister'

    json_payload = {                 # To be sent to server
        'cmd': cmd,
        'func_name': func_name
    }

    # Send to worker through socket
    _send_msg(_CLIENT_SOCKET, json_payload)

    # Get worker payload
    worker_resp_json = _recv_msg(_CLIENT_SOCKET)

    status = worker_resp_json.get('status')
    action = worker_resp_json.get('action')
    message = worker_resp_json.get('message')
    if status == 'ok':
        if action == 'unregistered':
            logging.info(f'Successfully unregistered {func_name}()')
            return 1
    elif status == 'err':
        logging.warning(f'Error while unregistering a function: {message}')
        return -1

def pyfaas_get_stats(func_name: str = None) -> int | dict:
    if not _PYFAAS_CONFIGURED:
        logging.warning('PyFaaS was not previously configured by calling pyfaas_config()')
        pyfaas_config()

    global _CLIENT_SOCKET
    
    cmd = 'get_stats'

    json_payload = {
        'cmd': cmd,
        'func_name': func_name
    }

    # Send to worker through socket
    _send_msg(_CLIENT_SOCKET, json_payload)

    # Get worker payload
    worker_resp_json = _recv_msg(_CLIENT_SOCKET)

    status = worker_resp_json.get('status')
    json_stats = worker_resp_json.get('result')
    message = worker_resp_json.get('message')

    if status == 'ok':
        if func_name != None:
            logging.info(f"Retrieved stats for '{func_name}'")
        else:
            logging.info(f'Retrieved general stats')
        logging.debug(f'Stats: {json_stats}')
        return json_stats
    else:
        if func_name != None:
            logging.error(f"Error while retrieving stats for '{func_name}': {message}")
        else:
            logging.error(f'Error while retrieving general stats: {message}')
        return -1


def pyfaas_kill_worker() -> None:
    if not _PYFAAS_CONFIGURED:
        logging.warning('PyFaaS was not previously configured by calling pyfaas_config()')
        pyfaas_config()

    global _CLIENT_SOCKET

    cmd = 'kill'

    json_payload = {
        'cmd': cmd
    }
    
    # Send to worker through socket
    _send_msg(_CLIENT_SOCKET, json_payload)

    logging.info('Worker killed by client')

def pyfaas_list() -> int | list[str]:
    if not _PYFAAS_CONFIGURED:
        logging.warning('PyFaaS was not previously configured by calling pyfaas_config()')
        pyfaas_config()

    global _CLIENT_SOCKET

    cmd = 'list'

    json_payload = {                 # To be sent to server
        'cmd': cmd
    }

    # Send to worker through socket
    _send_msg(_CLIENT_SOCKET, json_payload)

    # Get worker payload
    worker_resp_json = _recv_msg(_CLIENT_SOCKET)

    status = worker_resp_json.get('status')
    func_list = worker_resp_json.get('result')
    message = worker_resp_json.get('message')

    if status == 'ok':
        logging.info(f'Retrieved {len(func_list)} functions')
        return func_list
    else:
        logging.warning(f'Error while listing functions on the worker: {message}')
        return -1

def pyfaas_exec(func_name: str, func_arglist: list[object], func_kwargslist: dict[str, object]) -> object:
    if not _PYFAAS_CONFIGURED:
        logging.warning('PyFaaS was not previously configured by calling pyfaas_config()')
        pyfaas_config()

    global _CLIENT_SOCKET

    cmd = 'exec'
    
    logging.debug(f'Called faas_exec. Args: {func_name, func_arglist, func_kwargslist}')

    json_payload = {                 # To be sent to server
        'cmd': cmd,
        'func_name': func_name,
        'args': func_arglist,
        'kwargs': func_kwargslist,
        'additional_data': None
    }
    # Send to worker through socket
    _send_msg(_CLIENT_SOCKET, json_payload)

    # Get worker payload
    worker_resp_json = _recv_msg(_CLIENT_SOCKET)

    status = worker_resp_json.get('status')
    action = worker_resp_json.get('action')
    result_type = worker_resp_json.get('result_type')
    result = worker_resp_json.get('result')
    message = worker_resp_json.get('message')

    if status == 'ok':
        if action == 'executed':
            logging.info(f"Executed '{func_name}'")
            if result_type == 'pickle_base64':
                result_bytes = base64.b64decode(result)
                result = dill.loads(result_bytes)
            return result      # it's the JSON result that was included in the worker msg, or the deserialized Base64 result
    else:
        logging.warning(f"Error while executing '{func_name}' on the worker: {message}")
        return -1

def pyfaas_get_worker_info() -> int | dict:
    if not _PYFAAS_CONFIGURED:
        logging.warning('PyFaaS was not previously configured by calling pyfaas_config()')
        pyfaas_config()

    global _CLIENT_SOCKET

    cmd = 'get_worker_info'
    json_payload = {
        'cmd': cmd
    }
    
    # Send to worker through socket
    _send_msg(_CLIENT_SOCKET, json_payload)
    
    # Get worker payload
    worker_resp_json = _recv_msg(_CLIENT_SOCKET)

    status = worker_resp_json.get('status')
    result = worker_resp_json.get('result')
    message = worker_resp_json.get('message')

    if status == 'ok':
        return result
    else:
        logging.warning(f'Error while retrieving worker info: {message}')
        return -1

def pyfaas_ping() -> None:
    if not _PYFAAS_CONFIGURED:
        logging.warning('PyFaaS was not previously configured by calling pyfaas_config()')
        pyfaas_config()

    global _CLIENT_SOCKET

    cmd = 'PING'
    json_payload = {
        'cmd': cmd
    }
    
    # Send to worker through socket
    _send_msg(_CLIENT_SOCKET, json_payload)
    
    # Get worker payload
    worker_resp_json = _recv_msg(_CLIENT_SOCKET)

    status = worker_resp_json.get('status')
    result = worker_resp_json.get('result')
    message = worker_resp_json.get('message')

    if status == 'ok':
        logging.info(f"Worker says: '{result}'")
    else:
        logging.warning(f'Error while executing  on the worker: {message}')


def _send_msg(socket: socket.socket, msg: dict) -> None:
    data = json.dumps(msg).encode()
    data_length = len(data).to_bytes(4, 'big')      # Big endian 4 bytes header with msg length
    socket.sendall(data_length + data)      # Sending both data length and data. Worker knows when to stop reading

def _recv_msg(socket: socket.socket) -> dict:
    data_length_bytes = socket.recv(4)      # Receive header first
    if not data_length_bytes:
        return None         # Connection closed
    data_length = int.from_bytes(data_length_bytes, 'big')

    data = b''
    while len(data) < data_length:
        pkt = socket.recv(data_length - len(data))
        if not pkt:
            return None         # Connection closed in the middle of the msg
        data += pkt
    
    return json.loads(data.decode())
