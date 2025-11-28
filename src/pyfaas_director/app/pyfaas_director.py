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
import argparse

from pathlib import Path
from pyfaas_director.app.util import general
from pyfaas_director.app.util.file_logger import FileLogger
from pyfaas_director.app.exceptions import *


_DEFAULT_TOML_CONFIG_FILE = 'pyfaas_director/director_config.toml'

class PyfaasDirector:
    def __init__(self, config: dict):
        '''
        Initializes the PyFaaS Director with the provided configuration.

        The constructor prepares all internal state required for handling Workers,
        Clients, and function-distribution logic. No network binding or thread
        startup occurs here; those are performed in `run()`.

        Args:
            config (dict): A dictionary containing Director configuration, including:
                - network settings (`director_ip_addr`, `director_port`)
                - logging settings
                - worker heartbeat/synchronization parameters
                - worker selection strategy

        Initialization performed:
            - Creates the Director logger and file logger.
            - Loads network configuration and stores the full config.
            - Initializes the ZeroMQ ROUTER socket and ZMQ context (not yet bound).
            - Sets up thread-safety primitives, queues, and shared state
            used for Worker/Client management.
            - Prepares heartbeat monitoring and Worker-synchronization structures.
            - Initializes the function-to-Worker mapping used for dispatching.
            - Initializes containers used for coordinating multi-Worker operations
            (e.g., unregister requests requiring multiple responses).

        Side effects:
            - Logs an initial greeting message from the configuration.
        '''
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
        '''
        Starts the main event loop of the PyFaaS Director.

        This method:
            - Binds the Director's ZeroMQ ROUTER socket to the configured host/port.
            - Starts the background heartbeat-monitoring thread that removes Workers
            which fail to send a heartbeat within `self._heartbeat_interval_ms`.
            - Starts the background Worker-synchronization thread that ensures all
            connected Workers share the same set of function definitions.
            - Enters the main polling loop, where incoming multipart ZeroMQ messages
            are received and dispatched to the appropriate handler based on the
            sender identity.

        Message format:
            [identity][empty][JSON_payload]
            - `identity` identifies either a Worker (`worker-*`) or a Client (`client-*`).
            - `JSON_payload` contains the operation to perform.

        Behavior:
            - The loop uses a 1-second poll timeout to periodically check for
            interrupts while remaining responsive to network traffic.
            - Worker messages are forwarded to `_handle_worker_request()`.
            - Client messages are forwarded to `_handle_client_request()`.

        Termination:
            - Pressing Ctrl+C raises `KeyboardInterrupt`, which is caught here to
            trigger a clean shutdown. The Director logs the shutdown event, frees
            resources via `_cleanup()`, and exits the loop gracefully.
        '''
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
                    # (either a pyfaas client or a pyfaas Worker, which is also a client at this stage)
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
                        self._handle_worker_request(source_id, json_payload)
                    elif source_id.startswith('client-'):
                        self._handle_client_request(source_id, json_payload)
                    else:
                        self._logger.warning(f'Unknown message source: {source_id}')
                        continue
            except KeyboardInterrupt:
                self._logger.info('Ctrl+C pressed, exiting...')
                self._logger.info('Goodbye')
                self._cleanup()
                break

    def _handle_client_request(self, client_id: str, json_payload: dict) -> None:
        '''
        Handles a request sent by a connected Client and forwards it to the appropriate Worker
        or returns a self-generated response when no Worker interaction is required.

        This method:
            - Marks the client as waiting for a response.
            - Inspects the requested operation and applies operation-specific routing logic.
            - Selects a Worker (or Workers) as needed based on the operation and internal state.
            - Forwards the request to the Worker(s), updating Director bookkeeping structures
            (e.g., function-to-worker maps, pending multi-response tracking).
            - In some cases (`get_worker_ids`, invalid worker info requests), responds directly
            to the client without contacting any Worker.

        Supported operations:
            - **register**: Registers a new function on a selected Worker, computes the function ID,
            updates synchronization state, and forwards the request to the chosen Worker.
            - **unregister**: Sends unregister requests to all Workers holding the function and sets
            up a multi-response tracker to aggregate Workers' replies before responding to the client.
            - **get_worker_ids**: Returns the list of active Worker IDs directly to the client.
            - **get_worker_info**, **get_cache_dump**: Validates the target Worker; either errors back
            to the client or forwards the request to the specified Worker.
            - **exec**: Selects a Worker that holds the requested function and forwards the execution
            request.
            - Any other operation: Forwarded to a Worker selected by the default strategy.

        Args:
            client_id (str): The ZeroMQ identity of the requesting client.
            json_payload (dict): The JSON-decoded request body sent by the client.

        Raises:
            DirectorNoAvailableWorkersError:
                Raised if a Worker must be contacted to serve the request but none are available.
        '''
        operation = json_payload.get('operation')

        # Record that client is waiting for a response
        with self._lock:
            self._currently_connected_clients.append(client_id)

        # Proxy msg to the selected worker
        try:
            self._logger.info(f"Client '{client_id}' requested operation: '{operation}'")

            # Function registration, handle data structures for synchronization
            match operation:
                case 'register':
                    func_code_base64 = json_payload.get('serialized_func_base64')
                    func_code_bytes = base64.b64decode(func_code_base64)
                    func_code = dill.loads(func_code_bytes)
                    func_name = func_code.__name__
                    func_id = self._compute_function_id(func_name, func_code_base64)

                    json_payload['func_id'] = func_id       # Appending the computed ID to the json payload to send to the worker

                    selected_worker_id = self._select_worker()      # Choose Worker on which the function will be first saved
                    self._functions_workers_map[func_id] = [selected_worker_id]     # Until synchronized, the function can be found only on that Worker
                    
                    with self._lock:
                        self._workers_are_synchronized = False
                    
                    self._logger.debug(f'Workers-Functions state: {self._functions_workers_map}')

                case 'unregister':
                    request_id = uuid.uuid4()

                    func_id = json_payload['func_id']       # Needed to know to which Worker(s) (one/more) to send the unregistration request
                    if self._functions_workers_map[func_id] != 'ANY':
                        selected_worker_ids = self._functions_workers_map[func_id]   # Get Worker ID, but not all of them
                    else:
                        selected_worker_ids = list(self._workers.keys())  # Get ALL Worker IDs, since the function is available everywhere
                    
                    if not selected_worker_ids:         # No Worker available
                        raise DirectorNoAvailableWorkersError
                    
                    # Expecting #responses = #contacted workers
                    self._pending_multiple_responses[request_id] = {
                        'client_id': client_id,
                        'remaining': len(selected_worker_ids),
                        'additional_needed_data': {
                            'func_id': func_id     # Embed the func_id, needed later if the client was allowed to request such an unregistration
                        }
                    }

                    # Needed by the Director once the worker(s) will respond to such a request
                    json_payload['request_id'] = str(request_id)
                
                    # Send unregister message to every Worker holding the function
                    self._logger.debug(f"Sending 'unregister' request to {len(selected_worker_ids)} worker(s)")
                    for worker_id in selected_worker_ids:
                        msg = [worker_id.encode(), b'', json.dumps(json_payload).encode()]
                        self._zmq_socket.send_multipart(msg)
                        self._logger.debug(f"Request from client '{client_id}' forwarded to worker '{worker_id}'")

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
                    self._logger.debug(f"Function is identified by '{requested_func_id}'")
                    
                    selected_worker_id = self._select_worker(requested_func_id)
                    self._logger.debug(f"Chosen worker '{selected_worker_id}' for '{requested_func_id}' execution")
                
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
        self._logger.debug(f"Request from client '{client_id}' forwarded to worker '{selected_worker_id}'")

    def _select_worker(self, func_id: str = None) -> str:
        '''
        Selects a worker ID from the pool of connected Workers according to the
        configured selection strategy.

        If `func_id` is provided, the selection is constrained to Workers that
        advertise support for the given function. When multiple Workers support
        the function, the chosen strategy determines which one is returned. 
        If only a single Worker supports the function, that Worker is returned directly.

        When `func_id` is not provided, the strategy is applied to the entire
        set of connected Workers.

        Args:
            func_id (str, optional): The ID of the function for which a
                Worker holding its code should be selected. 
                If omitted, a generic Worker is selected.

        Returns:
            str: The ID of the selected Worker.

        Raises:
            DirectorNoAvailableWorkersError: Raised if no Workers are currently registered.
        '''    
        if not self._workers:
            raise DirectorNoAvailableWorkersError('No workers are available')
        
        # User requested a function execution operation (passed the target function's hash)
        if func_id is not None:
            # Check if the function can be found only in a single worker (this means workers have not
            # been synchronized yet, if multiple)
            if self._functions_workers_map[func_id] != 'ANY':
                if len(self._functions_workers_map[func_id]) == 1:
                    return self._functions_workers_map[func_id][0]
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
        '''
        Handles an incoming request from a Worker and performs the appropriate
        Director-side action based on the Worker-provided operation.

        Supported operations include:

        - **worker_registration**:  
        Registers a new Worker, initializes its metadata, and sends back an ACK
        response. Updates the Director's internal Worker registry.

        - **forward_to_client**:  
        The Worker provides a response meant for a specific client.  
        The Director proxies the message to the target client, optionally handling
        multi-response operations (e.g., unregister synchronization) by tracking
        and aggregating responses from multiple Workers.

        - **sync_state_response**:  
        Handles synchronization messages from Workers during the Director–Worker
        state synchronization procedure. These may include reports of currently
        available functions or responses containing missing function code.

        - **heartbeat**:  
        Updates the timestamp of the last heartbeat received from the Worker.

        Any unknown operation results in a logged informational message.

        Args:
            worker_id (str): The unique ID of the Worker sending the request.
            json_payload (dict): The parsed JSON message received from the Worker.
                Must contain an 'operation' field indicating the request type.
        '''
        operation = json_payload.get('operation')

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
                    request_id = uuid.UUID(json_payload['message_id'])
                    if request_id not in self._pending_multiple_responses:
                        return      # Already handled

                    self._pending_multiple_responses[request_id]['remaining'] -= 1
                    self._logger.debug('Received an unregister response...')
                    if self._pending_multiple_responses[request_id]['remaining'] != 0:
                        # This means there are sill Workers that need to send their response to the unregister command
                        return
                    else:
                        # If here, all the Workers have responded to the unregistration request

                        # Inspecting one of the Workers responses, to check if the 
                        # client that requested the unregistering of the function was 
                        # actually allowed to unregister it
                        if json_payload['status'] != 'err':
                            # Update function-worker mapping data structure, deleting the entry
                            func_id = self._pending_multiple_responses[request_id]['additional_needed_data']['func_id']
                            del self._functions_workers_map[func_id]

                        # Can finally delete the pending messages entry
                        del self._pending_multiple_responses[request_id]

                # The worker contacts the director to make it proxy the message to the client specified in the message
                # The message contains the response for the client request
                destination_client_id = json_payload['destination_client']
                self._logger.debug(f"Received message to be forwarded to client '{destination_client_id}' from '{worker_id}': {json_payload}")
                
                # Proxy message back to the client, stripped of unnecessary fields
                response = json_payload.copy()
                response.pop('destination_client', None)        # Delete key if present
                msg = [destination_client_id.encode(), b'', json.dumps(response).encode()]
                self._zmq_socket.send_multipart(msg)
                self._logger.debug(f"Response routed back to client '{destination_client_id}'")

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
                    # Execution passes to the synchronizer thread from here
                
                # Worker is providing the code of a function previously requested to him by the Director
                elif action == 'function_code_response':
                    self._incoming_synchronization_func_code_msg_queue.put(json_payload)

            case 'heartbeat':
                with self._lock:
                    if worker_id in self._workers:
                        self._workers[worker_id]['last_heartbeat'] = datetime.datetime.now()

            case _:
                self._logger.info(f"Unknown action specified by '{worker_id}': '{operation}'")

    def _synchronize_workers(self) -> None:
        '''
        Periodically synchronizes the set of registered functions across all
        connected PyFaaS Workers.

        This method runs in a dedicated background thread started by `run()` and
        loops indefinitely. Every `self._synchronization_interval_ms` milliseconds,
        it performs a synchronization cycle if:
            - more than one Worker is connected,
            - no clients are currently being served, and
            - the Workers are not already synchronized.

        Synchronization procedure:
            1. Each Worker is queried for its currently registered function IDs.
            2. The function sets from all Workers are collected and merged.
            3. For each Worker, the function IDs it is missing are computed.
            4. The Workers that have the missing functions are contacted to obtain
            the corresponding function code.
            5. The missing function code is redistributed to the Workers that do
            not have it.
            6. The internal `self._functions_workers_map` is updated to reflect
            global availability and a flag is set marking all Workers as
            synchronized.

        This method blocks on internal queues while waiting for synchronization
        messages and function code responses. It holds `self._lock` briefly when
        updating shared state.

        The loop terminates if `self._threading_stop_event` is set, allowing
        graceful shutdown of the synchronization thread.

        Side effects:
            - Sends ZeroMQ multipart messages to Workers.
            - Updates `self._functions_workers_map`, `self._workers_are_synchronized`,
            and logs extensive synchronization details.
        '''
        while not self._threading_stop_event.is_set():
            # Try to synchronize Workers every self._synchronization_interval_ms milliseconds
            time.sleep(self._synchronization_interval_ms / 1000)
            if (
                len(self._workers) <= 1 or                       # No workers to synchronize or just 1 registered Worker
                len(self._currently_connected_clients) != 0 or   # Wait until no clients are being served
                self._workers_are_synchronized                   # Workers are synchronized, no need to run all of this
            ):    
                self._logger.debug('Nothing to synchronize...')
                self._logger.debug(f'Current Workers state: {self._functions_workers_map}')
                continue
            
            self._logger.info('Started new Worker synchronization run...')

            all_functions = set()   # Set of ALL registered functions in the system

            # No clients are waiting, can try to synchronize Workers

            # Send message to every connected Worker asking for its set of registered functions
            for worker_id in self._workers.keys():
                request_state_msg = [worker_id.encode(), b'', json.dumps({'operation': 'sync_state_request'}).encode()]
                self._zmq_socket.send_multipart(request_state_msg)

            # Wait for all the Workers' responses: watch dedicated queue
            functions_per_worker = {}       # Map to keep the functions received from each Worker in the next loop
            for _ in range(len(self._workers)):
                # Blocks here until a 'current_functions_state' message arrives
                worker_id, json_payload = self._incoming_synchronization_msg_queue.get()
                worker_functions = json_payload['functions']    # Get the currently available functions IDs at the Worker which responded
                functions_per_worker[worker_id] = set(worker_functions)
                
                # Union of the received function IDs with all the previously received (no duplicates)
                all_functions |= functions_per_worker[worker_id]

            # Compute missing functions for each worker
            # Elements of this map: {worker_id1: set of missing functions, worker_id2: set of missing functions, ...} 
            missing_functions_per_worker = {
                worker_id: all_functions - funcs
                for worker_id, funcs in functions_per_worker.items()
            }

            # Compute the ensemble set of function IDs whose code needs to be requested to Workers that have it
            # The code will then be shared with the Workers that don't have it
            # function_code_to_be_Requested = set(func_id1, func_id2, ...)
            function_code_to_be_requested = set()
            for _, missing_func_set in missing_functions_per_worker.items():
                function_code_to_be_requested.update(missing_func_set)

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
                    'missing_functions_total': len(missing_functions_per_worker[worker_id])
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

            with self._lock:
                # Updating the Functions-Workers map: for each function, 
                # need to say that it is available at each Worker
                for func_id in self._functions_workers_map.keys():
                    self._functions_workers_map[func_id] = 'ANY'

                self._workers_are_synchronized = True
                self._logger.info(f'Workers synchronization run completed successfully')
                self._logger.debug(f'Current Workers state: {self._functions_workers_map}')

    def _compute_function_id(self, func_name: str, func_code: str) -> str:
        '''
        Computes a unique identifier for a function based on its name and code.

        The ID is computed as the SHA256 hash of the string "{func_name}:{func_code}", 
        ensuring that each unique combination of function name and code 
        has a reproducible, unique ID.

        Args:
            func_name (str): The name of the function.
            func_code (str): The serialized function code (e.g., base64 string).

        Returns:
            str: A SHA256 hash representing the unique function ID.
        '''
        return hashlib.sha256(f"{func_name}:{func_code}".encode()).hexdigest()

    def _heartbeats_watcher(self) -> None:
        '''
        Monitors the heartbeat of all connected Workers and unregisters Workers
        that fail to send timely heartbeats.

        This method runs in a dedicated background thread and periodically checks
        the last heartbeat timestamp of each Worker. A Worker is considered dead
        if it misses two consecutive expected heartbeat intervals and has been
        registered longer than a short grace period (2 × expected heartbeat interval).

        For Workers deemed dead:
            - A notification message is sent to the Worker (if still reachable).
            - The Worker is removed from the Director's internal registry.

        The loop terminates if `self._threading_stop_event` is set, allowing
        graceful shutdown of the monitoring thread.

        Side effects:
            - Sends ZeroMQ messages to Workers marked for unregistration.
            - Updates `self._workers` dictionary by removing dead Workers.
            - Logs unregistration events and warnings.
        '''
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
        '''
        Cleans up all resources used by the Director before shutdown.

        This method performs a graceful termination of background threads and
        network resources:

            - Signals the Worker heartbeat monitor and Worker synchronization threads
            to stop via `self._threading_stop_event`.
            - Waits briefly for the threads to exit cleanly.
            - Closes the ZeroMQ ROUTER socket and terminates the ZeroMQ context.
            - Logs the status of each cleanup operation.

        Raises:
            DirectorCleanupError: Raised if any error occurs during the cleanup process,
            such as failure to stop threads or close network resources.

        Side effects:
            - Stops background threads.
            - Closes ZeroMQ sockets and context.
            - Logs cleanup progress and warnings.
        '''
        try:
            self._logger.info('Cleaning up Director resources...')
            self._threading_stop_event.set()        # Signaling heartbeat and sync thread to stop
            if self._heartbeat_thread and self._heartbeat_thread.is_alive():
                self._heartbeat_thread.join(timeout=2)           # Waiting for it to exit cleanly
                self._logger.info('Successfully stopped Worker heartbeat monitor thread')
            if self._worker_synchronizer_thread and self._worker_synchronizer_thread.is_alive():
                self._worker_synchronizer_thread.join(timeout=2) # Waiting for it to exit cleanly
                self._logger.info('Successfully stopped Worker synchronization thread')
            self._zmq_socket.close(linger=0)
            self._zmq_context.term()
            self._logger.info('Successfully closed ZeroMQ context and socket')
        except DirectorCleanupError as e:
            self._logger.warning(f'Error during cleanup: {e}')


def setup_parser() -> argparse.ArgumentParser:
    '''
    Creates and configures an ArgumentParser for the Director process.

    This parser handles command-line arguments that configure the Director
    at startup. It supports specifying a custom configuration file.

    Returns:
        argparse.ArgumentParser: A fully configured ArgumentParser instance
        ready to parse command-line arguments.
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config_file', default=None, help="The Director's configuration file path")
    return parser


def main():
    '''
    Entry point for the PyFaaS Director application.

    This function performs the following steps:
        1. Parses command-line arguments to optionally specify a configuration file.
        2. Validates the configuration file path and falls back to the default
           configuration file if necessary.
        3. Loads the Director configuration from a TOML file.
        4. Sets up logging according to the loaded configuration.
        5. Initializes a PyFaaS Director instance.
        6. Starts the Director's main loop, which handles Worker and Client requests.
    '''
    parser = setup_parser()
    args = parser.parse_args()

    config_path = args.config_file
    if config_path:
        config_path = Path(config_path)
        if not config_path.exists():
            print(f"Config file '{config_path}' not found. Falling back to '{_DEFAULT_TOML_CONFIG_FILE}'.")
            config_path = Path(_DEFAULT_TOML_CONFIG_FILE)
    else:
        print(f"Using default config file '{_DEFAULT_TOML_CONFIG_FILE}'.")
        config_path = Path(_DEFAULT_TOML_CONFIG_FILE)

    director_config_file = config_path

    try:
        config = general.read_config_toml(director_config_file)
    except Exception as e:
        logging.error(e)
        exit(0)

    general.setup_logging(config['logging']['log_level'])

    director = PyfaasDirector(config)
    director.run()


if __name__ == '__main__':
    main()
