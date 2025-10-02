import util
import socket
import logging
import dill
import time
import json
import base64
import datetime
import platform
import os
import inspect

from file_logger import FileLogger

logging.basicConfig(
    format='[WORKER, %(levelname)s]    %(message)s',
    level=logging.ERROR,   # default until config is loaded
    force=True
)

_TOML_CONFIG_FILE = 'worker/worker_config.toml'
_WORKER_DATA_DUMP_FILE = 'worker/worker_dump.bin'

class PyfaasWorker:
    def __init__(self, config):
        self._host = config['network']['worker_ip_addr']
        self._port = config['network']['worker_port']
        self._config = config

        logging.debug(self._config['misc']['greeting_msg'])

        self._file_logger = FileLogger(
            self._config['logging']['log_directory'],
            self._config['logging']['log_filename'],
            self._host,
            self._port
        )

        if self._config['behavior']['shutdown_persistence'] and os.path.exists(_WORKER_DATA_DUMP_FILE):
            try:
                worker_state = self._load_worker_state()
                self._functions = worker_state['functions']
                self._stats = worker_state['stats']
                self._request_count = worker_state['request_count']
                logging.debug(f'Functions: {self._functions}')
                logging.debug(f'Stats: {self._stats}')
                logging.debug(f'Request count: {self._request_count}')
            except Exception as e:
                logging.error(f'Shutdown persistence enabled but unable to load worker state: {e}')
                logging.info('Worker will resort to classical initialization. Structures will be empty')
                self._functions = {}
                self._stats = {}
                self._request_count = 0
        else:
            self._functions = {}
            self._stats = {}
            self._request_count = 0

        self._start_time = datetime.datetime.now()
        self._last_client_connection_ts = None

    def run(self) -> None:
        worker_ip_port_tuple = (self._host, self._port)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(worker_ip_port_tuple)
            s.listen()
            s.settimeout(1.0)     # To be able to catch ctrl+c
            logging.info(f'Worker reachable at: {worker_ip_port_tuple[0]}:{worker_ip_port_tuple[1]}')
            
            running = True
            while running:
                try:
                    conn, client_addr = s.accept()
                    logging.debug(f'Worker connected by {client_addr}')
                    self._file_logger.log('INFO', f'Client connected: {client_addr}')
                    self._last_client_connection_ts = datetime.datetime.now()
                except socket.timeout:
                    continue
                except Exception as e:
                    logging.erorr('Unexpected')
                    break

                with conn:
                    while running:
                        json_payload = self._recv_msg(conn)
                        if json_payload == 'EOF':
                            logging.info(f'Client at {client_addr} closed the connection')
                            self._file_logger.log('INFO', f'Client disconnected: {client_addr}')
                            break       # Stop processing client if the conneciton gets closed
                        elif json_payload is None:      # Client crash
                            logging.warning(f'Client at {client_addr} closed the connection unexpectedly')
                            self._file_logger.log('WARNING', f'Client disconnected unexpectedly: {client_addr}')
                            break
                        
                        # Get client command. Command args are parsed in each case arm
                        cmd = json_payload['cmd']
                        self._request_count += 1

                        match cmd:
                            case 'register':
                                self._execute_register_cmd(conn, json_payload)

                            case 'unregister':
                                self._execute_unregister_cmd(conn, json_payload)

                            case 'exec':
                                self._execute_exec_cmd(conn, json_payload, client_addr)

                            case 'list':
                                self._execute_list_cmd(conn)

                            case 'get_stats':
                                self._execute_get_stats_cmd(conn, json_payload)
                                    
                            case 'get_worker_info':
                                self._execute_get_worker_info_cmd(conn)

                            case 'kill':
                                self._execute_kill_cmd()
                                running = False
                                break

                            case 'PING':
                                self._execute_ping_cmd(conn, cmd)

                            case _:
                                self._file_logger.log('WARNING', f"Unknown command: '{cmd}'")
                                logging.warning(f"Client specified unknown command '{cmd}'")
            # Fallback
            self.cleanup()
            logging.info('Goodbye')

    def _execute_kill_cmd(self):
        logging.info(f'Worker killed by client at {datetime.datetime.now()}')
        self._file_logger.log('INFO', f'Worker killed by client')
        self.cleanup()

    def _execute_ping_cmd(self, conn, cmd):
        logging.info(f"Client says: '{cmd}'")
        client_json_response = self._build_JSON_response('ok', None, 'json', 'PONG', None)
        self._send_msg(conn, client_json_response)

    def _execute_get_worker_info_cmd(self, conn):
        try:
            info_summary = {}

            # Worker identity
            info_summary['identity'] = {}
            info_summary['identity']['ip_address'] = self._host
            info_summary['identity']['port'] = self._port
            info_summary['identity']['start_time'] = self._start_time.isoformat()
            str_uptime = str(datetime.datetime.now() - self._start_time)
            info_summary['identity']['uptime'] = str_uptime

            # System info
            info_summary['system'] = {}
            info_summary['system']['python_version'] = platform.python_version()
            info_summary['system']['OS'] = platform.system()
            info_summary['system']['CPU'] = platform.processor()
            info_summary['system']['cores'] = os.cpu_count() or 1   # Fallback to 1 if unable to determine

            # Worker configuration info
            info_summary['config'] = {}
            info_summary['config']['enabled_statistics'] = self._config['statistics']['enabled']
            info_summary['config']['log_level'] = self._config['misc']['log_level']

            # Function info
            info_summary['functions'] = {}
            info_summary['functions'] = self._functions

            # Network info
            info_summary['network'] = {}
            info_summary['network']['request_count'] = self._request_count
            info_summary['network']['last_client_connection_timestamp'] = str(self._last_client_connection_ts)

            client_json_response = self._build_JSON_response('ok', None, 'json', info_summary, None)
            self._send_msg(conn, client_json_response)
        except Exception as e:
            client_json_response = self._build_JSON_response('err', None, 'json', None, f'{e}')
            self._send_msg(conn, client_json_response)

    def _execute_get_stats_cmd(self, conn, json_payload):
        try:
            func_name = json_payload['func_name']
            if func_name != None:
                if func_name not in self._stats:
                    raise Exception(f"No function named '{func_name}' is registered right now")
                else:
                    stats_for_client = self._stats[func_name]   # Send only stats for the specified function
            else:
                stats_for_client = self._stats   # No func name was specified, send all stats

            client_json_response = self._build_JSON_response('ok', None, 'json', stats_for_client, None)
            self._send_msg(conn, client_json_response)
        except Exception as e:
            client_json_response = self._build_JSON_response('err', None, 'json', None, f'{e}')
            self._send_msg(conn, client_json_response)

    def _execute_list_cmd(self, conn):
        try:
            func_list = [f for f, _ in self._functions.items()]
            logging.info(f'List: retrieved {len(func_list)} functions')

            client_json_response = self._build_JSON_response('ok', None, 'json', func_list, None)

            self._send_msg(conn, client_json_response)
        except Exception as e:
            client_json_response = self._build_JSON_response('err', None, 'json', None, f'{type(e).__name__}: {e}')
            self._send_msg(conn, client_json_response)

    def _execute_exec_cmd(self, conn, json_payload, client_addr):
        func_name = json_payload['func_name']
        func_args = json_payload.get('args', [])            # Default empty list
        func_kwargs = json_payload.get('kwargs', {})        # Default empty dict

        if func_name not in self._functions:
            logging.info(f"No function named '{func_name}' is registered right now")
            client_json_response = self._build_JSON_response('err', 'no_func', None, None, f"No function named '{func_name}' is registered at the worker right now")
            self._send_msg(conn, client_json_response)
        else:
            try:
                logging.info(f'Executing the following call: {func_name}({func_args}, {func_kwargs})')

                client_function = self._functions[func_name]

                start_time = time.time()
                func_res = client_function(*func_args, **func_kwargs)
                end_time = time.time()

                exec_time = end_time - start_time
                if self._config['statistics']['enabled']:
                    self._record_stats(func_name, exec_time)
                else:
                    logging.info('Statistics have not been enabled')

                logging.info(f"Executed '{func_name}' for {client_addr[0]}:{client_addr[1]} in {exec_time} s")
                logging.debug(f'{func_name} data: \n \t{self._stats[func_name]}')
                logging.debug(f'Function result: {func_res}')

                encoded_func_res, func_res_type = self._encode_func_result(func_res)          # JSON or base64
                client_json_response = self._build_JSON_response('ok', 'executed', func_res_type, encoded_func_res, None)

                self._file_logger.log('INFO', f'Executed {func_name}({func_args}, {func_kwargs}) in {exec_time}')
                self._send_msg(conn, client_json_response)
            except Exception as e:
                client_json_response = self._build_JSON_response('err', None, 'json', None, f'{type(e).__name__}: {e}')
                self._send_msg(conn, client_json_response)

    def _execute_unregister_cmd(self, conn, json_payload):
        func_name = json_payload['func_name']
        client_json_response = None
        if func_name in self._functions:
            logging.info(f"Unregistering '{func_name}'...")
            self._file_logger.log('INFO', f'Function unregistration: {func_name}')
            del self._functions[func_name]
            if self._config['statistics']['enabled']:
                del self._stats[func_name]
            client_json_response = self._build_JSON_response('ok', 'unregistered', None, None, None)
        else:
            logging.info(f"No function named '{func_name}' is registered right now")
            client_json_response = self._build_JSON_response('err', 'no_func', None, None, f"No function named '{func_name}' is registered at the worker right now")

        logging.debug('Currently registered functions:')
        logging.debug(f'\t {self._functions}')

        self._send_msg(conn, client_json_response)

    def _execute_register_cmd(self, conn, json_payload):
        serialized_func_base64 = json_payload['serialized_func_base64']
        serialized_func_bytes = base64.b64decode(serialized_func_base64)
        client_function = dill.loads(serialized_func_bytes)
        func_name = client_function.__name__

        func_params = inspect.signature(client_function)
        logging.debug(f'Params: {func_params}')

        client_json_response = None
        if func_name not in self._functions:
            self._functions[func_name] = client_function
            logging.info(f'Function {func_name} successfully registered')
            self._file_logger.log('INFO', f"Function registration: '{func_name}'")
            client_json_response = self._build_JSON_response('ok', 'registered', None, None, None)
        else:
            override = json_payload['override']
            if override:
                logging.warning(f"A function named '{func_name}' is already registered")
                logging.warning('Overriding...')
                self._functions[func_name] = client_function
                client_json_response = self._build_JSON_response('ok', 'overridden', None, None, None)
            else:
                logging.warning(f"A function named '{func_name}' is already registered")
                logging.warning(f"Function '{func_name}' will not be overridden")
                client_json_response = self._build_JSON_response('ok', 'no_action', None, None, None)
        
        logging.debug('Currently registered functions:')
        logging.debug(f'\t {self._functions}')

        self._send_msg(conn, client_json_response)

    def _dump_worker_state(self) -> None:
        dump = {
            'functions': self._functions,
            'stats': self._stats,
            'request_count': self._request_count
        }
        try:
            with open(_WORKER_DATA_DUMP_FILE, 'wb') as f:
                dill.dump(dump, f)
        except Exception as e:
            raise Exception(e)

    def _load_worker_state(self) -> dict | None:
        try:
            with open(_WORKER_DATA_DUMP_FILE, 'rb') as f:
                return dill.load(f)
        except Exception as e:
            raise Exception(e)

    def _record_stats(self, func_name: str, exec_time: float) -> None:
        if func_name not in self._stats:
            self._stats[func_name] = {}
            self._stats[func_name]['#calls'] = 1
            self._stats[func_name]['avg_exec_time'] = exec_time
            self._stats[func_name]['tot_exec_time'] = exec_time
        else:
            self._stats[func_name]['#calls'] += 1
            self._stats[func_name]['tot_exec_time'] += exec_time
            avg_exec_time = self._stats[func_name]['tot_exec_time'] / self._stats[func_name]['#calls']
            self._stats[func_name]['avg_exec_time'] = avg_exec_time
            

    def _build_JSON_response(self, status: str, action: str, result_type: str, result: object, message: str) -> bytes:
        return {
            'status': status,
            'action': action,
            'result_type': result_type,
            'result': result,
            'message': message
        } 

    def _encode_func_result(self, func_result: object) -> tuple[str, str]:
        try:
            json.dumps(func_result)      # Test JSON-serializability, return plain result if successful
            return func_result, 'json'
        except (TypeError, OverflowError):      # result is not JSON-serializable, let caller know
            func_result_bytes = dill.dumps(func_result)
            func_result_base64 = base64.b64encode(func_result_bytes).decode()
            return func_result_base64, 'pickle_base64'
            
    def _send_msg(self, socket: socket.socket, msg: dict) -> None:
        data = json.dumps(msg).encode()
        data_length = len(data).to_bytes(4, 'big')      # Big endian 4 bytes header with msg length
        socket.sendall(data_length + data)      # Sending both data length and data. Client knows when to stop reading

    def _recv_msg(self, socket: socket.socket) -> dict:
        data_length_bytes = socket.recv(4)      # Receive header first
        if not data_length_bytes:
            return 'EOF'         # Connection closed normally (differentiating between this and crashes/disconnections)
        data_length = int.from_bytes(data_length_bytes, 'big')
        data = b''
        while len(data) < data_length:
            pkt = socket.recv(data_length - len(data))
            if not pkt:
                return None         # Connection closed in the middle of the msg
            data += pkt
        return json.loads(data.decode())
    
    def cleanup(self):
        # Dump worker state to file only if enabled and if there has been at least a call (there is something to save)
        if self._config['behavior']['shutdown_persistence'] and self._request_count != 0:
            try:
                self._dump_worker_state()
                self._file_logger.log('INFO', 'Dumped worker state')
            except Exception as e:
                logging.error(f'Unable to dump worker state: {e}')
                self._file_logger.log('ERROR', f'Unable to dump worker state: {e}')
        

def main():
    try:
        config = util.read_config_toml(_TOML_CONFIG_FILE)
    except Exception as e:
        logging.error(e)
        exit(0)

    util.setup_logging(config['logging']['log_level'])

    worker = PyfaasWorker(config)
    try:
        worker.run()
    except KeyboardInterrupt:
        logging.info('Ctrl+C pressed, exiting...')
        worker.cleanup()
        logging.info('Goodbye')


if __name__ == '__main__':
    main()
