import logging
import json
import datetime
import threading
import time
import zmq
import random
import hashlib
import dill
import uuid
import base64
import queue

from .util import general
from .util.file_logger import FileLogger
from .exceptions import *


_TOML_CONFIG_FILE = 'pyfaas_director/director_config.toml'

class PyfaasDirector:
    def __init__(self, config: dict):
        self._logger = logging.getLogger('pyfaas.director')

        self._host = config['network']['director_ip_addr']
        self._port = config['network']['director_port']
        self._config = config

        self._logger.debug(self._config['misc']['greeting_msg'])

        self._file_logger = FileLogger(
            self._config['logging']['log_directory'],
            self._config['logging']['log_filename'],
            self._host,
            self._port
        )

        # ZeroMQ vars
        self._zmq_context = zmq.Context()
        self._zmq_socket = self._zmq_context.socket(zmq.ROUTER)

        # --- Workers management ---
        self._lock = threading.Lock()
        self._workers = {}
        self._request_count = 0

        # Heartbeat monitor thread
        self._heartbeat_thread = None
        self._threading_stop_event = threading.Event()

        # Every how many ms we check if workers are alive
        self._heartbeat_check_interval_ms = self._config['workers']['heartbeat_check_interval_ms']
        # Every how many ms a worker should send its heartbeat msg
        self._expected_heartbeat_interval_ms = self._config['workers']['expected_heartbeat_interval_ms']

        # Every how many ms the Director starts the synchronization procedure
        self._synchronization_interval_ms = self._config['workers']['synchronization_interval_ms']

        # Workers selection
        self._round_robin_index = 0
        self._worker_selection_strategy = self._config['workers']['worker_selection_strategy']

        self._start_time = datetime.datetime.now()
        self._last_worker_connection_ts = None

        # Keep track of clients that are currently waiting for a response from a worker
        self._currently_connected_clients = []
        
        self._worker_synchronizer_thread = None   # Thread to synchronize worker state (functions list)
        self._workers_are_synchronized = False

        # Functions map (which worker holds which function)
        #   - Key: sha256(func_name, func_code), func_code is the base64 representation
        #   - Value: a set() of worker_ids
        #       - single worker_id if just registered or a list of worker_id's for synchronization 
        #       - if the function is on all Workers -> len(set) = len(self._workers)
        self._functions_workers_map = {}

        # Gathers incoming synchronization messages from the connected Workers, upon Director request
        self._incoming_synchronization_msg_queue = queue.Queue()
        self._incoming_synchronization_func_code_msg_queue = queue.Queue()

        # Used for messages that the Director needs to send to a client
        # that are triggered once every registered Worker has sent its response
        # e.g.: unregister operation: the unregister req is sent to every worker
        #       every worker, upon unregistering the function, sends back
        #       a message to the Director that would be routed by him to the requesting client
        #       Need instead to route a single message, not every Worker's response to the request 
        self._pending_multiple_responses = {}

    def run(self) -> None:
        # Setting up ZeroMQ stuff
        tcp_connection_str = f'tcp://{self._host}:{self._port}'
        self._zmq_socket.bind(tcp_connection_str)
        self._logger.info(f'Listening on {tcp_connection_str}')
        
        # Setting up polling to catch Ctrl+C
        poller = zmq.Poller()
        poller.register(self._zmq_socket, zmq.POLLIN)
    
        # Starting thread to check for registered workers heartbeats
        # Removes the workers which have not sent a heartbeat in the last
        # self._heartbeat_ineterval_ms interval
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeats_watcher,
            args=(),
            daemon=True
        )
        self._heartbeat_thread.start()

        # Starting workers synchronization thread
        self._worker_synchronizer_thread = threading.Thread(
            target=self._synchronize_workers,
            args=(),
            daemon=True
        )
        self._worker_synchronizer_thread.start()
        
        # Main loop
        while True:
            try:
                sockets = dict(poller.poll(1000))           # 1s timeout
                if self._zmq_socket in sockets:
                    # Receive a ZeroMQ multipart msg from a client 
                    # (either a pyfaas client or a pyfaas worker, which is also a client at this stage)
                    # Msg: [identity][empty][JSON_payload]
                    msg_parts = self._zmq_socket.recv_multipart()

                    if len(msg_parts) < 3:
                        self._logger.warning(f'Malformed message received: {msg_parts}')
                        continue
                    
                    # Parsing
                    source_id, _, payload = msg_parts
                    source_id = source_id.decode()                  # Requester identity
                    json_payload = json.loads(payload.decode())     # msg JSON body
                    
                    # Dispatching
                    if source_id.startswith('worker-'):
                        # self._logger.debug(f'Handling worker request (source = {source_id})')
                        self._handle_worker_request(source_id, json_payload)
                    elif source_id.startswith('client-'):
                        # self._logger.debug(f'Handling client request (source = {source_id})')
                        self._handle_client_request(source_id, json_payload)
                    else:
                        self._logger.warning(f'Unknown message source: {source_id}')
                        continue
            except KeyboardInterrupt:
                self._logger.info('Ctrl+C pressed, exiting...')
                self._logger.info('Goodbye')
                self._cleanup()
                break

    # Handle a request from a client identified by client_id
    # The request is an operation that the client is asking to be executed on a worker
    # The director must proxy such a request to one of the registered workers
    def _handle_client_request(self, client_id: str, json_payload: dict) -> None:
        operation = json_payload.get('operation')
        self._logger.debug(f'Operation "{operation}" requested by client "{client_id}"')

        # Record that client is waiting for a response
        with self._lock:
            self._currently_connected_clients.append(client_id)

        # Proxy msg to the selected worker
        try:
            # Function registration, handle data structures for synchronization
            match operation:
                case 'register':
                    func_code_base64 = json_payload.get('serialized_func_base64')
                    func_code_bytes = base64.b64decode(func_code_base64)
                    func_code = dill.loads(func_code_bytes)
                    func_name = func_code.__name__
                    func_id = self._compute_function_id(func_name, func_code_base64)

                    # Appending the computed ID to the json payload to send to the worker
                    json_payload['func_id'] = func_id
                    # TODO: fix with sets
                    selected_worker_id = list(self._workers.keys())[0]              # Choose first worker to save the function
                    self._functions_workers_map[func_id] = [selected_worker_id]     # Until synchronized, the function can be found only on that Worker
                    with self._lock:
                        self._workers_are_synchronized = False
                    
                    self._logger.debug(f'Workers-Functions state: {self._functions_workers_map}')

                case 'unregister':
                    request_id = uuid.uuid4()

                    func_id = json_payload['func_id']       # Needed to know to which Worker(s) (one/more) to send the unregistration request
                    if self._functions_workers_map[func_id] != 'ANY':
                        selected_worker_ids = self._functions_workers_map[func_id]   # Get single or multiple worker ID, but not all of them
                    else:
                        selected_worker_ids = list(self._workers.values())
                    
                    if not selected_worker_ids:         # No Worker available
                        raise DirectorNoAvailableWorkersError
                    
                    self._pending_multiple_responses[request_id] = {
                        'client_id': client_id,
                        'remaining': len(selected_worker_ids)
                    }

                    # Needed by the Director once the worker(s) will respond to such a request
                    json_payload['request_id'] = request_id
                
                    # Send unregister message to every Worker holding the function
                    self._logger.debug(f"Sending 'unregister' request to {len(selected_worker_ids)} worker(s)")
                    for worker_id in selected_worker_ids:
                        msg = [worker_id.encode(), b'', json.dumps(json_payload).encode()]
                        self._zmq_socket.send_multipart(msg)
                        self._logger.debug(f"Request from client '{client_id}' formwarded to worker '{worker_id}'")

                    # Update function-worker mapping data structure
                    del self._functions_workers_map[func_id]

                    return      # End here, message(s) has already been forwarded
                
                case 'get_worker_ids':
                    active_worker_ids = self._workers.keys()
                    self._logger.debug(f'Currently active workers: {active_worker_ids}')
                    get_worker_ids_response = {
                        'status': 'ok',
                        'result': active_worker_ids,
                    }

                    # Director self-responds to requester client without contacting any worker
                    msg = [client_id.encode(), b'', json.dumps(get_worker_ids_response).encode()]
                    self._zmq_socket.send_multipart(msg)
                    with self._lock:
                        self._currently_connected_clients.remove(client_id)
                    return
                
                case 'get_worker_info' | 'get_cache_dump':
                    requested_worker_id = json_payload.get('worker_id')
                    if requested_worker_id not in self._workers:
                        err_msg = f"No currently registered Worker is identified by ID '{requested_worker_id}'"
                        self._logger.debug(err_msg)
                        err_response = {
                            'status': 'err',
                            'message': err_msg
                        }
                        msg = [client_id.encode(), b'', json.dumps(err_response).encode()]
                        self._zmq_socket.send_multipart(msg)
                        with self._lock:
                            self._currently_connected_clients.remove(client_id)
                        return
                    else:
                        selected_worker_id = requested_worker_id
                
                case 'exec':
                    requested_func_id = json_payload.get('func_id')      # The ID (hash) of the function the user has requested the execution 
                    self._logger.debug(f'Client {client_id} requested execution of function identified by {requested_func_id}')
                    
                    selected_worker_id = self._select_worker(requested_func_id)
                    self._logger.debug(f'Chosen worker {selected_worker_id} for {requested_func_id} execution')
                
                case _:         # Any other case: any connected worker can handle the request
                    selected_worker_id = self._select_worker()

        except DirectorNoAvailableWorkersError as e:
            self._logger.warning('No available workers to handle client request right now')
            err_response = {
                'status': 'err',
                'message': e
            }
            msg = [client_id.encode(), b'', json.dumps(err_response).encode()]
            self._zmq_socket.send_multipart(msg)
            with self._lock:
                self._currently_connected_clients.remove(client_id)
            return

        msg = [selected_worker_id.encode(), b'', json.dumps(json_payload).encode()]
        self._zmq_socket.send_multipart(msg)
        self._logger.debug(f"Request from client '{client_id}' formwarded to worker '{selected_worker_id}'")

    # TODO: refactor this, 
    # TODO: fix docstring
    def _select_worker(self, func_id: str = None) -> str:
        '''
        Chooses a Worker ID from the pool of connected ones based on some policy.

        Args:
            func_id (str): In case of an 'exec' command request, the ID of the function that needs to be executed.

        Returns:
            str: the Worker ID that has been chosen.

        Raises:
            DirectorNoAvailableWorkersError: Raised if no Workers are registered to the Director. 
        '''
        if not self._workers:
            raise DirectorNoAvailableWorkersError('No workers are available')
        
        # User requested a function execution operation (passed the target function's hash)
        if func_id is not None:
            # Check if the function can be found only in a single worker (this means workers have not
            # been synchronized yet, if multiple)
            if len(self._functions_workers_map[func_id]) != len(self._workers):
                if len(self._functions_workers_map[func_id]) == 1:
                    return self._functions_workers_map[func_id]
                else:       # If here, during synchronization one/more Workers failed to synchronize, choose one
                    match self._worker_selection_strategy:
                        case 'Round-Robin':
                            worker_ids = self._functions_workers_map[func_id]           # It's a list
                            worker_id = worker_ids[self._round_robin_index % len(worker_ids)]
                            self._round_robin_index += 1
                            return worker_id
                        case 'Random':
                            worker_id, _ = random.choice(self._functions_workers_map[func_id])
                            return worker_id            

        # Multiple Workers and possibly synchronized, choose worker
        match self._worker_selection_strategy:
            case 'Round-Robin':
                worker_ids = list(self._workers.keys())
                worker_id = worker_ids[self._round_robin_index % len(worker_ids)]
                self._round_robin_index += 1
                return worker_id
            case 'Random':
                worker_id, _ = random.choice(list(self._workers.items()))
                return worker_id

    def _handle_worker_request(self, worker_id: str, json_payload: dict) -> None:
        operation = json_payload.get('director_operation')

        if operation is None:
            self._logger.warning(f"Worker {worker_id} sent malformed JSON: {json_payload}")
            return

        match operation:
            case 'worker_registration':
                self._logger.debug(f"Handling registration request for worker '{worker_id}'")
                # 1) Worker sends register msg
                # 2) Director receives register msg, does its things
                # 3) Director sends ACK msg
                # 4) Worker receives ACK msg

                # Init dict entry for the new worker
                with self._lock:
                    self._workers[worker_id] = {
                        'registered_at': datetime.datetime.now(),
                        'last_heartbeat': datetime.datetime.now()
                    }
                
                # Send back ACK msg to worker that wants to register
                ack_msg = [worker_id.encode(), b'', json.dumps({'ACK': 'OK'}).encode()]
                self._zmq_socket.send_multipart(ack_msg)
                self._logger.info(f"Worker '{worker_id}' registered and stored")
                self._logger.debug(f'Current status of self._workers: {self._workers}')

                self._last_worker_connection_ts = datetime.datetime.now()
            
            case 'forward_to_client':
                original_client_operation = json_payload.get('original_client_operation')

                if original_client_operation == 'unregister':
                    # Need to collect every response to the 'unregister' command from the workers and
                    # forward to the client only one of them (otherwise it would receive multiple and break everything)
                    request_id = json_payload['message_id']
                    pending_responses = self._pending_multiple_responses[request_id]
                    if pending_responses is None:
                        return      # Already handled
                    
                    pending_responses['remaining'] -= 1
                    if pending_responses['remaining'] != 0:
                        # This means there are sill Workers that need to send their response to the unregister command
                        return
                    else:
                        del self._pending_multiple_responses[request_id]        # Can continue with sending the single message to the client

                # The worker contacts the director to make it proxy the message to the client specified in the message
                # The message contains the response for the client request
                destination_client_id = json_payload['destination_client']
                self._logger.debug(f"Received message to be forwarded to client '{destination_client_id}' from '{worker_id}': {json_payload}")
                
                # Proxy message back to the client, stripped of unnecessary fields
                response = json_payload.copy()
                response.pop('destination_client', None)        # Delete key if present
                msg = [destination_client_id.encode(), b'', json.dumps(response).encode()]
                self._zmq_socket.send_multipart(msg)
                self._logger.debug(f'Routed to {destination_client_id}')

                # Remove client from list of clients that are waiting for a response
                with self._lock:
                    self._currently_connected_clients.remove(destination_client_id)

            # Worker is responding to a 'sync_state_request' message from the Director
            # This incoming message can either be a response containing:
            #   - 'action': 'current_functions_state' -> the Worker is letting the Director know the functions he currently has available
            #   - 'action': 'function_code_request'   -> the Worker is requesting the Director for the code of the functions he misses
            case 'sync_state_response':
                action = json_payload.get('action')

                # Worker is providing its currently registered functions
                if action == 'current_functions_state':
                    # Pushes in the synchronization msg queue the received Worker response
                    # The queue is watched by the synchronization manager thread
                    # Note: queue.Queue() is thread-safe (no need for lock)
                    self._incoming_synchronization_msg_queue.put([worker_id, json_payload])
                    self._logger.debug(f"Received current state response from Worker '{worker_id}'")
                    # Execution passes to the synchronizer thread from here
                
                # Worker is providing the code of a function previously requested to him by the Director
                elif action == 'function_code_response':
                    self._incoming_synchronization_func_code_msg_queue.put(json_payload)
                    self._logger.debug(f"Received function code response from Worker '{worker_id}'")

            case 'heartbeat':
                # self._logger.debug(f"received heartbeat message from '{worker_id}'")
                with self._lock:
                    if worker_id in self._workers:
                        self._workers[worker_id]['last_heartbeat'] = datetime.datetime.now()

            case _:
                self._logger.info(f"Unknown action specified by '{worker_id}': '{operation}'")

    def _synchronize_workers(self) -> None:
        '''
        Synchronizes the state of the currently connected PyFaaS Workers to make sure each connected Worker has the same set of registered functions. 

        This function runs in a dedicated thread started in run().
        '''
        while True:
            # Try to synchronize Workers every self._synchronization_interval_ms milliseconds
            time.sleep(self._synchronization_interval_ms / 1000)
            if len(self._workers) <= 1:     # No workers to synchronize or just 1 registered Worker
                continue
            if len(self._currently_connected_clients) != 0:     # Wait until no clients are being served
                continue
            if self._workers_are_synchronized:      # Workers are synchronized, no need to run all of this
                continue

            all_functions = set()   # Set of ALL registered functions in the system

            # No clients are waiting, can try to synchronize Workers

            # Send message to every connected Worker asking for its set of registered functions
            for worker_id in self._workers.keys():
                request_state_msg = [worker_id.encode(), b'', json.dumps({'operation': 'sync_state_request'}).encode()]
                self._zmq_socket.send_multipart(request_state_msg)

            # Wait for all the Workers' responses: watch dedicated queue
            functions_per_worker = {}       # Map to keep the functions received from each Worker in the next loop
            for _ in range(len(self._workers)):
                worker_id, json_payload = self._incoming_synchronization_msg_queue.get()    # Blocks until a message arrives
                worker_functions = json_payload['functions']    # Get the currently available functions IDs at the Worker which responded
                functions_per_worker[worker_id] = set(worker_functions)
                
                # Union of the received function IDs with all the previously received (no duplicates)
                all_functions |= functions_per_worker[worker_id]

            # Compute missing functions for each worker
            missing_functions_per_worker = {
                worker_id: all_functions - funcs
                for worker_id, funcs in functions_per_worker.items()
            }

            # Compute the set of functions whose code needs to be requested to Workers, 
            # so that is can be shared to the other Workers (aggregated)
            function_code_to_be_requested = set(missing_functions_per_worker.keys())

            # Ask the Workers that have available the functions missing on other 
            # Workers to provide the code for such functions
            for func_id in function_code_to_be_requested:
                target_worker = self._select_worker(func_id)        # Get Worker to contact to get such function code
                json_payload = {
                    'operation': 'sync_function_code_request',
                    'func_id': func_id
                }
                msg = [target_worker.encode(), b'', json.dumps(json_payload).encode()]
                self._zmq_socket.send_multipart(msg)
                self._logger.debug(f"Asked Worker '{target_worker}' for function code of function '{func_id}'")
            
            # Wait for as many messages as the number of single functions that need to be shared
            # Send such function to all the Workers that are missing it
            workers_per_missing_function = {}  # Keeps, for each missing function somewhere, the list of Workers that miss it
            for worker_id, missing_functions in missing_functions_per_worker.items():
                for func_id in missing_functions:
                    if func_id not in workers_per_missing_function:
                        workers_per_missing_function[func_id] = []
                    workers_per_missing_function[func_id].append(worker_id)

            # Messaging the Workers to communicate how many messages containing functions code they'll be expecting
            for worker_id, missing_functions in missing_functions_per_worker.items():
                json_payload = {
                    'operation': 'sync_missing_function_code',
                    'missing_functions_total': missing_functions_per_worker[worker_id]
                }
                msg = [worker_id.encode(), b'', json.dumps(json_payload).encode()]
                self._zmq_socket.send_multipart(msg)

            # Finally sending the actual functions' code
            for _ in function_code_to_be_requested:
                json_payload = self._incoming_synchronization_func_code_msg_queue.get()    # Blocks until a message arrives
                func_id = json_payload['func_id']           # Get function ID from the newly arrived function code message
                
                # Send the code to the Workers that miss such function
                json_payload['operation'] = 'sync_missing_function_code'
                for worker_id in workers_per_missing_function[func_id]:
                    msg = [worker_id.encode(), b'', json.dumps(json_payload).encode()]
                    self._zmq_socket.send_multipart(msg)
                    self._logger.debug(f"Sent to Worker '{worker_id}' the code for function '{func_id}'")

            with self._lock:
                self._workers_are_synchronized = True

    def _compute_function_id(self, func_name: str, func_code: str) -> str:
        return hashlib.sha256(f"{func_name}:{func_code}".encode()).hexdigest()

    def _heartbeats_watcher(self) -> None:
        self._logger.info('Started worker unregistration check thread...')
        while not self._threading_stop_event.is_set():
            time.sleep(self._heartbeat_check_interval_ms / 1000)
            to_be_unregistered = []

            with self._lock:
                now = datetime.datetime.now()
                for worker_id, worker_info in list(self._workers.items()):
                    elapsed = (now - worker_info['last_heartbeat']).total_seconds() * 1000
                    since_registration = (now - worker_info['registered_at']).total_seconds() * 1000
                    
                    if since_registration < 2 * self._expected_heartbeat_interval_ms:    # Grace period: don't unregister within first 2 * expect_heartbeat_ms
                        continue
                    
                    if elapsed > 2 * self._expected_heartbeat_interval_ms:      # Double time tolerance (considered dead after 2 missed heartbeats)
                        to_be_unregistered.append(worker_id)

            for worker_id in to_be_unregistered:
                try:
                    # TODO: handle this on worker??? What if it disconnected???
                    unregister_msg = [worker_id.encode(), b'', json.dumps({'Action': 'Unregister'}).encode()]       # This is wrong
                    self._zmq_socket.send_multipart(unregister_msg)
                    self._logger.info(f"Notified worker unregistration to '{worker_id}'")
                except Exception as e:
                    self._logger.warning(f"Failed to notify worker '{worker_id}': {e}")
                finally:
                    with self._lock:
                        if worker_id in self._workers:
                            self._logger.info(f"Worker '{worker_id}' unregistered")
                            del self._workers[worker_id]

    def _cleanup(self) -> None:
        try:
            self._logger.info('Cleaning up Director resources...')
            self._threading_stop_event.set()        # Signaling heartbeat thread to stop
            if self._heartbeat_thread and self._heartbeat_thread.is_alive():
                self._heartbeat_thread.join(timeout=2)           # Waiting for it to exit cleanly
                self._logger.info('Successfully stopped worker heartbeat monitor thread')
            self._zmq_socket.close(linger=0)
            self._zmq_context.term()
            self._logger.info('Successfully closed ZeroMQ context and socket')
        except Exception as e:
            self._logger.warning(f'Error during cleanup: {e}')


def main():
    try:
        config = general.read_config_toml(_TOML_CONFIG_FILE)
    except Exception as e:
        logging.error(e)
        exit(0)

    general.setup_logging(config['logging']['log_level'])

    director = PyfaasDirector(config)
    director.run()


if __name__ == '__main__':
    main()
