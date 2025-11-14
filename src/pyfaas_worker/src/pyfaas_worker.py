import logging
import dill
import time
import json
import datetime
import os
import uuid
import threading
import zmq
import sys
import queue
import base64

from .util import general
from .util.file_logger import FileLogger
from .worker_caching.func_cache import WorkerFunctionExecutionCache
from .exceptions import *
from .worker_operations import WorkerOperations


_TOML_CONFIG_FILE = 'pyfaas_worker/worker_config.toml'

class PyfaasWorker:
    def __init__(self, config: dict):
        self._logger = logging.getLogger('pyfaas.worker')

        self._id = f'worker-{uuid.uuid4()}'
        self._logger.info(f"Worker '{self._id}' up")

        # Dependency injection: get operartions from dedicated class and make the 
        # class able to access PyfaasWorker attributes with self
        self._operations = WorkerOperations(self)

        self._running = False

        self._config = config

        # TODO: test
        # self._exec_limits = {}
        # self._exec_limits['cpu_time_s'] = resource.RLIM_INFINITY if self._config['behavior']['exec_limits']['cpu_time_limit'] == 0 else self._config['behavior']['exec_limits']['cpu_time_limit']
        # self._exec_limits['address_space_mb'] = resource.RLIM_INFINITY if self._config['behavior']['exec_limits']['address_space_limit_mb'] == 0 else self._config['behavior']['exec_limits']['address_space_limit_mb']
        # self._logger.debug(f'Execution limits: {self._exec_limits['cpu_time_s']} s (CPU time), {self._exec_limits['address_space_mb']} MB (address space)')

        self._director_host = self._config['network']['director_ip_addr']
        self._director_port = self._config['network']['director_port']
        self._hearbeat_interval_ms = self._config['network']['heartbeat_interval_ms']

        # Multiple tyhreads could access self._functions, self._stats, self._function_exec_cache
        self._lock = threading.RLock()

        # Heartbeat thread
        self._heartbeat_thread = None
        self._threading_stop_event = threading.Event()

        # --- ZeroMQ vars ---
        self._zmq_context = zmq.Context()
        self._zmq_socket = self._zmq_context.socket(zmq.DEALER)
        self._zmq_socket.setsockopt_string(zmq.IDENTITY, self._id)

        self._outgoing_tx_queue = queue.Queue()         # Queue of messages to send to the Director
        self._io_thread = threading.Thread(             # Dedicated ZMQ I/O thread (started in run())
            target=self._socket_loop,
            daemon=True
        )

        # Queue for incoming synchronization messages from the Director
        self._incoming_sync_update_queue = queue.Queue()

        # Queue to store incoming messages containing missing functions code from the Director
        self._incoming_sync_function_code_queue = queue.Queue()

        self._logger.debug(self._config['misc']['greeting_msg'])

        self._file_logger = FileLogger(
            self._config['logging']['log_directory'],
            self._config['logging']['log_filename'],
            self._id
        )

        self._function_exec_cache = WorkerFunctionExecutionCache(
            self._config['behavior']['caching']['policy'],
            self._config['behavior']['caching']['max_size']
        )
        if self._config['behavior']['caching']['max_size'] != 0:
            self._logger.info('Caching is enabled')
        else:
            self._logger.info('Caching is disabled. Cache adding is a no-op')

        if self._config['behavior']['shutdown_persistence'] is True:
            self._dump_file = self._config['behavior']['dump_file']

        if self._config['behavior']['shutdown_persistence'] and os.path.exists(self._dump_file):
            try:
                worker_state = self._load_worker_state()
                with self._lock:
                    self._functions = worker_state['functions']
                    self._stats = worker_state['stats']
                self._request_count = worker_state['request_count']
                self._logger.debug(f'Functions: {self._functions}')
                self._logger.debug(f'Stats: {self._stats}')
                self._logger.debug(f'Request count: {self._request_count}')
            except Exception as e:
                self._logger.error(f'Shutdown persistence enabled but unable to load worker state: {e}')
                self._logger.info('Worker will resort to classical initialization. Structures will be empty')
                with self._lock:
                    self._functions = {}
                    self._stats = {}
                self._request_count = 0
        else:
            with self._lock:
                self._functions = {}
                self._stats = {}
            self._request_count = 0

        # Content of self._functions entries:
        # func_id: {
        #       name: name of the function
        #       code: code of the function
        #       registering_client: client ID of the client that registered the function
        # }

        self._start_time = datetime.datetime.now()
        self._last_client_connection_ts = None

    def _register_to_director(self) -> None:
        director_connection_str = f'tcp://{self._director_host}:{self._director_port}'
        self._zmq_socket.connect(director_connection_str)
        
        registration_msg = [b'', json.dumps({'operation': 'worker_registration'}).encode()]   # Worker ID automatically included by ZeroMQ (see call to setsockopt in __int__)
        self._zmq_socket.send_multipart(registration_msg)

        # Polling for director ACK: wait for up to 10s
        poller = zmq.Poller()
        poller.register(self._zmq_socket, zmq.POLLIN)
        sockets = dict(poller.poll(timeout=10000))

        if self._zmq_socket in sockets:
            ack_msg_parts = self._zmq_socket.recv_multipart()   # Receiving [empty][JSON_payload]
            ack_msg = json.loads(ack_msg_parts[-1].decode())
            if ack_msg.get('ACK') == 'OK':
                self._logger.info(f'Connected and registered to director at {self._director_host}:{self._director_port}')
        else:
            self._logger.error(f'No ACK received from Director at {self._director_host}:{self._director_port} within time limits (10s)')
            self._kill_worker(cause='director_unreachable')

    def run(self) -> None:
        self._running = True

        # Try to connect to the director specified in TOML config file
        self._register_to_director()

        # Start thread to send periodic heartbeats to the director
        self._heartbeat_thread = threading.Thread(
            target=self._send_heartbeat,
            daemon=True
        )
        self._heartbeat_thread.start()

        # Starting dedicated ZMQ I/O thread -> executes _socket_loop()
        self._io_thread.start()

        # Keeping main thread alive
        try:
            while self._running:
                time.sleep(1)
        except KeyboardInterrupt:
            self._logger.info('Ctrl+C pressed, exiting...')
            self._logger.info('Goodbye')
            self._cleanup()

    # ZMQ socket loop (single thread)
    def _socket_loop(self) -> None:
        # Setting up polling to catch Ctrl+C
        poller = zmq.Poller()
        poller.register(self._zmq_socket, zmq.POLLIN)

        while self._running:
            sockets = dict(poller.poll(timeout=100))

            # --- Incoming messages handler ---
            if self._zmq_socket in sockets:
                director_msg_parts = self._zmq_socket.recv_multipart()      # Receiving [empty][JSON_payload]
                json_payload = json.loads(director_msg_parts[-1].decode())

                self._logger.debug(f"Received '{json_payload}' from director")
                self._request_count += 1
                
                # Starting incoming request handler thread
                command = json_payload.get('operation')
                threading.Thread(
                    target=self._handle_incoming_request,
                    args=(command, json_payload),
                    daemon=True
                ).start()

            # --- Outgoing messages handler ---
            while True:
                try:
                    outgoing_msg = self._outgoing_tx_queue.get_nowait()
                    self._zmq_socket.send_multipart(outgoing_msg)
                except queue.Empty:
                    break           # Send until empty queue
        
    def _handle_incoming_request(self, command: str, json_payload: dict) -> None:
        match command:
            case 'register':
                self._operations.execute_register_cmd(json_payload)

            case 'unregister':
                self._operations.execute_unregister_cmd(json_payload)

            case 'exec':
                self._operations.execute_exec_cmd(json_payload)

            case 'list':
                self._operations.execute_list_cmd(json_payload)

            case 'get_stats':
                self._operations.execute_get_stats_cmd(json_payload)

            case 'get_worker_info':
                self._operations.execute_get_worker_info_cmd(json_payload)

            case 'get_cache_dump':
                self._operations.execute_get_cache_dump_cmd(json_payload)

            case 'chain_exec':
                self._operations.execute_chain_exec_cmd(json_payload)

            case 'PING':
                self._operations.execute_ping_cmd(json_payload)

            # Synchronize with other registered Workers
            case 'sync_state_request':
                threading.Thread(
                    target=self._synchronize_state,
                    daemon=True
                ).start()

            # The Director is asking the Worker for the code of a specific function that he has availabe, for synchronization purposes
            case 'sync_function_code_request':
                self._forward_function_code(json_payload)

            # The Director is sending the Worker the code of one of the functions he was missing
            case 'sync_missing_function_code':
                self._incoming_sync_function_code_queue.put(json_payload)   # This unblocks the synchronization thread

            case _:
                self._file_logger.log('WARNING', f"Unknown command: '{command}'")
                self._logger.warning(f"Client specified unknown command '{command}'")

    def _forward_function_code(self, json_payload: dict) -> None:
        '''
        TODO: 
        '''
        requested_func_id = json_payload.get('func_id')
        requested_func_code = self._functions[requested_func_id]['code']
        serialized_func = dill.dumps(requested_func_code)
        serialized_func_base64 = base64.b64encode(serialized_func).decode('utf-8')
        director_json_response = {
            'operation': 'sync_state_response',
            'action': 'function_code_response',
            'func_id': requested_func_id,
            'serialized_func_base64': serialized_func_base64
        }
        response = [b'', json.dumps(director_json_response).encode()]
        self._outgoing_tx_queue.put(response)

    def _synchronize_state(self):
        # Send to Director the function IDs of the functions registered on this Worker
        synch_json_response = {
            'operation': 'sync_state_response',
            'action': 'current_functions_state',
            'functions': list(self._functions.keys())   # Send just the IDs, code will be received later on, if needed
        }
        response = [b'', json.dumps(synch_json_response).encode()]
        self.worker._outgoing_tx_queue.put(response)
        
        # Wait for the missing functions' code and update
        # First message of this kind contains the number of messages
        # containing functions' code to expect from the Director
        missing_functions_total_msg = self._incoming_sync_function_code_queue.get()      # Blocks waiting for a message
        missing_functions_total = missing_functions_total_msg.get('missing_functions_total')
        self._logger.debug(f'Sync: waiting for the code of {missing_functions_total} function(s)')
        
        if missing_functions_total != 0:        # Receiving the messages with the codes
            missing_function_code_msg = self._incoming_sync_function_code_queue.get()      # Blocks waiting for a message

            func_id = missing_function_code_msg['func_id']
            serialized_func_base64 = missing_function_code_msg['serialized_func_base64']

            serialized_func_bytes = base64.b64decode(serialized_func_base64)
            final_function = dill.loads(serialized_func_bytes)
            func_name = final_function.__name__

            with self._lock:
                self._functions[func_id] = {}
                self._functions[func_id]['name'] = func_name
                self._functions[func_id]['code'] = final_function
                self._functions[func_id]['registering_client'] = None    # TODO: what do we do here??????
            self._logger.debug(f"Sync: added function '{func_id}' to the set of available functions")    

        self._logger.debug('Sync: finished synchronization procedure')

    def _dump_worker_state(self) -> None:
        dump = {
            'functions': self._functions,
            'stats': self._stats,
            'request_count': self._request_count
        }
        try:
            with open(self._dump_file, 'wb') as f:
                dill.dump(dump, f)
        except Exception as e:
            raise Exception(e)

    def _load_worker_state(self) -> dict | None:
        try:
            with open(self._dump_file, 'rb') as f:
                return dill.load(f)
        except Exception as e:
            raise Exception(e)

    def _cleanup(self) -> None:
        # Stop heartbeat thread
        try:
            self._threading_stop_event.set()        # Signaling heartbeat thread to stop
            if self._heartbeat_thread and self._heartbeat_thread.is_alive():
                self._heartbeat_thread.join(timeout=2)      # Waiting for it to exit cleanly
            self._logger.info('Successfully stopped heartbeat thread')
        except Exception as e:
            self._logger.error(f'Unable to stop heartbeat thread: {e}')

        # Dump worker state to file only if enabled and if there has been at least a call (there is something to save)
        if self._config['behavior']['shutdown_persistence'] and self._request_count != 0:
            try:
                self._dump_worker_state()
                self._file_logger.log('INFO', 'Dumped worker state')
            except Exception as e:
                self._logger.error(f'Unable to dump worker state: {e}')
                self._file_logger.log('ERROR', f'Unable to dump worker state: {e}')

    def _kill_worker(self, cause: str):
        if cause == 'director_unreachable':
            self._logger.info(f'Worker killed at {datetime.datetime.now()}: director unreachable')
        try:
            self._zmq_socket.close(linger=0)
            self._zmq_context.term()
        except Exception as e:
            self._logger.warning(f"Error during socket cleanup: {e}")
        self._file_logger.log('INFO', f'Worker killed. Cause: {cause}')
        self._cleanup()
        sys.exit(1)         # Exiting immediately with error code

    def _send_heartbeat(self) -> None:
        heartbeat_msg = [b'', json.dumps({'operation': 'heartbeat'}).encode()]       # Worker ID automatically included by ZeroMQ (see call to setsockopt in __int__)
        while not self._threading_stop_event.is_set():
            time.sleep(self._hearbeat_interval_ms / 1000)
            self._zmq_socket.send_multipart(heartbeat_msg)


def main():
    try:
        config = general.read_config_toml(_TOML_CONFIG_FILE)
    except Exception as e:
        logging.error(e)
        exit(0)

    general.setup_logging(config['logging']['log_level'])

    worker = PyfaasWorker(config)
    worker.run()


if __name__ == '__main__':
    main()
