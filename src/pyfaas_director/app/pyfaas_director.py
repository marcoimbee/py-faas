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

from collections import defaultdict
from pathlib import Path
from pyfaas_director.app.util import general
from pyfaas_director.app.util.file_logger import FileLogger
from pyfaas_director.app.exceptions import DirectorCleanupError, DirectorNoAvailableWorkersError, DirectorWorkflowValidationError


_DEFAULT_TOML_CONFIG_FILE = 'pyfaas_director/director_config.toml'

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

        # Functions map: which Worker holds which function, with additional metadata
        #   - Key: sha256(func_name, func_code), where func_code is the base64 representation
        #   - Value: a map containing the following data:
        #       - 'registering_client': ID of the client that registered the function
        #       - 'func_name': function name
        #       - 'available_on': a list of Worker IDs, containing 1/more Worker IDs on which the function is available
        #           - single worker_id if just registered or a list of worker_id's for synchronization 
        #           - if the function is on all Workers -> 'ANY'
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

        # To keep track of Workers' responses to 'list' commands, will store what the Workers
        # send back to be able to aggregate the results and provide a complete list of functions
        # registered by the requesting user
        self._worker_list_cmd_responses = []

        # To keep track of Workers' responses to 'get_stats' commands, will store what the Workers
        # send back to be able to aggregate the results and provide a complete stats snapshot
        self._worker_get_stats_cmd_responses = []

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
        operation = json_payload.get('operation')

        # Record that client is waiting for a response
        with self._lock:
            self._currently_connected_clients.append(client_id)

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

                    self._functions_workers_map[func_id] = {
                        'func_name': func_name,
                        'registering_client': client_id,
                        'available_on': [selected_worker_id]     # Until synchronized, the function can be found only on that Worker
                    }

                    self._forward_client_req_single_worker(client_id, selected_worker_id, json_payload)

                    with self._lock:
                        self._workers_are_synchronized = False
                    
                    self._logger.debug(f'Workers-Functions state: {self._functions_workers_map}')

                case 'unregister':
                    request_id = uuid.uuid4()

                    func_id = json_payload['func_id']       # Needed to know to which Worker(s) (one/more) to send the unregistration request
                    if self._functions_workers_map[func_id]['available_on'] != 'ANY':
                        selected_worker_ids = self._functions_workers_map[func_id]['available_on']   # Get Worker ID, but not all of them
                    else:
                        selected_worker_ids = list(self._workers.keys())  # Get ALL Worker IDs, since the function is available everywhere
                    
                    if not selected_worker_ids:         # No Worker is available
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
                    self._forward_client_req_multiple_workers(client_id, selected_worker_ids, json_payload)

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
                        self._forward_client_req_single_worker(client_id, selected_worker_id, json_payload)
                
                case 'exec':
                    requested_func_id = json_payload.get('func_id')      # The ID (hash) of the function the user has requested the execution 
                    self._logger.debug(f"Function is identified by '{requested_func_id}'")
                    
                    selected_worker_id = self._select_worker(requested_func_id)
                    self._logger.debug(f"Chosen worker '{selected_worker_id}' for '{requested_func_id}' execution")

                    self._forward_client_req_single_worker(client_id, selected_worker_id, json_payload)

                # TODO: !!!
                case 'chain_exec':
                    request_id = uuid.uuid4()

                    json_workflow = json_payload['json_workflow']
                    # workflow_id = json_workflow.get('id')
                    workflow_function_set = json_workflow.get('functions')

                    # Checking if:
                    #   - The referenced functions have been registered at all (do functions with such name exist?)
                    #   - The requesting client has registered every function he references in the workflow.
                    #     This means checking if the client has registered functions with the NAMES he specified in the workflow.
                    function_names = [func_name for func_name, _ in workflow_function_set.items()]
                    client_func_names = [
                        entry['func_name']
                        for entry in self._functions_workers_map.values()
                        if entry['registering_client'] == client_id
                    ]

                    missing_funcs = set(function_names) - set(client_func_names)
                    if missing_funcs:       # There is one/more function that has been referenced by the client but was never registered by him
                        self._logger.warning(f"Client '{client_id}' requested a chained execution of functions named '{function_names}', but functions '{missing_funcs}' have not been registered")
                        err_response = {
                            'status': 'err',
                            'message': f"unknown function(s) '{missing_funcs}' referenced"
                        }
                        msg = [client_id.encode(), b'', json.dumps(err_response).encode()]
                        self._zmq_socket.send_multipart(msg)
                        with self._lock:
                            self._currently_connected_clients.remove(client_id)
                        return          # Can finish here

                    # Each Worker will validate the referenced function he is called to execute.
                    # All Workers are synchronized OR single Worker: 
                    #       workflow can be executed on a single Worker, chosen depending on the policy
                    # Unsynchronized Workers: 
                    #       functions might be scattered across > 1 Workers
                    #       Following the ordering of the functions in the workflow,
                    #       execute each one of them. 
                    #       This can happen on different Workers
                    if self._workers_are_synchronized or len(self._workers) == 1:   # All Workers are synchronized OR single Worker
                        selected_worker_id = self._select_worker()      # Any Worker can be contacted to execute the whole workflow

                        json_payload['request_id'] = str(request_id)

                        # Expecting #responses = 1
                        self._pending_multiple_responses[request_id] = {
                            'client_id': client_id,
                            'remaining': 1,
                            'additional_needed_data': {}
                        }

                        self._forward_client_req_single_worker(client_id, selected_worker_id, json_payload)
                    else:   # Unsynchronized Workers
                        # Here it is a  bit more of a mess
                        # We could do like this:
                        # we create a dedicated queue for the chained execution requests.
                        # We pre-fill the queue with the messages that need to be sent to the Workers, 
                        # minus the field containing the serialized result from the previous function execution.
                        # We send the first message, we wait for the response of the first Worker. If OK, embed result
                        # ini the following msg in the queue, and send to another Worker. If not OK at any point, return the error
                        # to the user.
                        # When the queue is empty, the workflow functions have finished, and the result is delivered back to the user.
                        
                        pass

                case 'list':
                    request_id = uuid.uuid4()
                    if self._workers_are_synchronized or len(self._workers) == 1:       # All Workers are synchronized OR single Worker
                        selected_worker_id = self._select_worker()    # Any Worker can be contacted to get the func list

                        # Needed by the Director once the worker(s) will respond to such a request
                        json_payload['request_id'] = str(request_id)

                        # Expecting #responses = 1
                        self._pending_multiple_responses[request_id] = {
                            'client_id': client_id,
                            'remaining': 1,
                            'additional_needed_data': {}
                        }

                        self._forward_client_req_single_worker(client_id, selected_worker_id, json_payload)
                    else:   # Unsynchronized Workers
                        selected_worker_ids = self._workers.keys()    # ALL Workers have to be contacted and their result is later aggregated

                        # Expecting #responses = #contacted workers
                        self._pending_multiple_responses[request_id] = {
                            'client_id': client_id,
                            'remaining': len(selected_worker_ids),
                            'additional_needed_data': {}
                        }

                        # Needed by the Director once the workers will respond to such a request
                        json_payload['request_id'] = str(request_id)
                    
                        # Send list message to every Worker holding the function
                        self._logger.debug(f"Sending 'list' request to {len(selected_worker_ids)} worker(s)")
                        self._forward_client_req_multiple_workers(client_id, selected_worker_ids, json_payload)

                        return      # End here, message(s) has already been forwarded

                case 'get_stats':
                    request_id = uuid.uuid4()

                    if len(self._workers) == 1:       # Single Worker
                        selected_worker_id = self._select_worker()    # The Worker can be contacted to get the stats
                        
                        # Needed by the Director once the worker(s) will respond to such a request
                        json_payload['request_id'] = str(request_id)

                        # Expecting #responses = 1
                        self._pending_multiple_responses[request_id] = {
                            'client_id': client_id,
                            'remaining': 1,
                            'additional_needed_data': {}
                        }

                        self._forward_client_req_single_worker(client_id, selected_worker_id, json_payload)
                    else:   # Multiple Workers (syndhronized or unsynchronized, everybody needs to be contacted)
                        selected_worker_ids = self._workers.keys()    # ALL Workers have to be contacted and their result is later aggregated

                        # Expecting #responses = #contacted workers
                        self._pending_multiple_responses[request_id] = {
                            'client_id': client_id,
                            'remaining': len(selected_worker_ids),
                            'additional_needed_data': {}
                        }

                        # Needed by the Director once the workers will respond to such a request
                        json_payload['request_id'] = str(request_id)
                    
                        # Send get_stats message to every Worker
                        self._logger.debug(f"Sending 'get_stats' request to {len(selected_worker_ids)} worker(s)")
                        self._forward_client_req_multiple_workers(client_id, selected_worker_ids, json_payload)

                        return      # End here, message(s) has already been forwarded

                case _:         # Any other case: any connected worker can handle the request
                    selected_worker_id = self._select_worker()

        except DirectorNoAvailableWorkersError as e:            # Raised by _select_worker() if no Workers are available/registered
            self._logger.warning('No available Workers to handle client request right now')
            err_response = {
                'status': 'err',
                'message': e
            }
            msg = [client_id.encode(), b'', json.dumps(err_response).encode()]
            self._zmq_socket.send_multipart(msg)
            with self._lock:
                self._currently_connected_clients.remove(client_id)

    def _forward_client_req_single_worker(self, client_id: str, worker_id: str, json_payload: dict) -> None:
        msg = [worker_id.encode(), b'', json.dumps(json_payload).encode()]
        self._zmq_socket.send_multipart(msg)
        self._logger.debug(f"Request from client '{client_id}' forwarded to Worker '{worker_id}'")

    def _forward_client_req_multiple_workers(self, client_id: str, worker_ids: list, json_payload: dict) -> None:
        for worker_id in worker_ids:
            msg = [worker_id.encode(), b'', json.dumps(json_payload).encode()]
            self._zmq_socket.send_multipart(msg)
            self._logger.debug(f"Request from client '{client_id}' forwarded to Worker '{worker_id}'")

    def _select_worker(self, func_id: str = None) -> str:
        if not self._workers:
            raise DirectorNoAvailableWorkersError('No Workers are available')
        
        # User requested a function execution operation (passed the target function's hash)
        if func_id is not None:
            # Check if the function can be found only in a single worker (this means workers have not
            # been synchronized yet, if multiple)
            if self._functions_workers_map[func_id]['available_on'] != 'ANY':
                if len(self._functions_workers_map[func_id]['available_on']) == 1:
                    return self._functions_workers_map[func_id]['available_on'][0]      # Get the single Worker on which the function is available on
                else:       # If here, during synchronization one/more Workers failed to synchronize, choose one
                    match self._worker_selection_strategy:
                        case 'Round-Robin':
                            worker_ids = self._functions_workers_map[func_id]['available_on']           # It's a list
                            worker_id = worker_ids[self._round_robin_index % len(worker_ids)]
                            self._round_robin_index += 1
                            return worker_id
                        case 'Random':
                            worker_id = random.choice(self._functions_workers_map[func_id]['available_on'])
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

                match original_client_operation:
                    case 'unregister':
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

                    case 'list':       # LIST operation: needs aggregated results, if # of contacted Workers is > 1
                        request_id = uuid.UUID(json_payload['message_id'])
                        if request_id not in self._pending_multiple_responses:
                            return      # Already handled

                        self._pending_multiple_responses[request_id]['remaining'] -= 1
                        self._logger.debug('Received a list response...')

                        # Saving the received message to later aggregate
                        with self._lock:
                            self._worker_list_cmd_responses.append(json_payload)

                        if self._pending_multiple_responses[request_id]['remaining'] != 0:
                            # This means there are sill Workers that need to send their response to the list command
                            return
                        else:
                            # If here, all the Workers (can also be just 1) have responded to the list request
                            # Can now aggregate the response(s)
                            # If any response produced an error, forward the first error response
                            # Otherwise, build a custom JSON payload with the list of all the received function IDs and names
                            print(self._worker_list_cmd_responses)
                            destination_client_id = next(
                                r['destination_client'] for r in self._worker_list_cmd_responses
                                if uuid.UUID(r['message_id']) == request_id
                            )
                            json_payload = {
                                'message_id': str(uuid.uuid4()),
                                'destination_client': destination_client_id,
                                'operation': 'list',
                                'action': None,
                                'result_type': 'json'
                            }
                            worker_side_error = any(resp_msg['status'] == 'err'
                                                    for resp_msg in self._worker_list_cmd_responses)
                            if worker_side_error:    # Embed in JSON response info abt the first encountered error
                                first_error = next(resp for resp in self._worker_list_cmd_responses
                                                if uuid.UUID(resp['message_id']) == request_id and resp['status'] == 'err')
                                json_payload['status'] = 'err'
                                json_payload['result'] = None
                                json_payload['message'] = first_error['message']
                            else:      # Embed in JSON response an aggregated map of function data
                                all_results = [resp_msg['result'] for resp_msg in self._worker_list_cmd_responses if uuid.UUID(resp_msg['message_id']) == request_id]
                                aggregated_func_data = {
                                    func_id: func_name      # func_id: func_name
                                    for result in all_results
                                    for func_id, func_name in result.items() 
                                }
                                json_payload['result'] = aggregated_func_data
                                json_payload['status'] = 'ok'
                                json_payload['message'] = None

                            with self._lock:    # Removing all the processed messages
                                self._worker_list_cmd_responses = [msg for msg in self._worker_list_cmd_responses if uuid.UUID(msg['message_id']) != request_id]

                    case 'get_stats':      # GET_STATS operation: needs aggregated results, if # of contacted Workers is > 1
                        request_id = uuid.UUID(json_payload['message_id'])
                        if request_id not in self._pending_multiple_responses:
                            return      # Already handled
                        
                        self._pending_multiple_responses[request_id]['remaining'] -= 1
                        self._logger.debug('Received a get_stats response...')

                        # Saving the received message to later aggregate
                        with self._lock:
                            self._worker_get_stats_cmd_responses.append(json_payload)

                        if self._pending_multiple_responses[request_id]['remaining'] != 0:
                            # This means there are sill Workers that need to send their response to the get_stats command
                            return
                        else:
                            # If here, all the Workers (can also be just 1) have responded to the get_stats request
                            # Can now aggregate the response(s)
                            # If any response produced an error, forward the first error response
                            # Otherwise, build a custom JSON payload with the stats of all the received functions
                            destination_client_id = next(
                                r['destination_client'] for r in self._worker_get_stats_cmd_responses
                                if uuid.UUID(r['message_id']) == request_id
                            )
                            json_payload = {
                                'message_id': str(uuid.uuid4()),
                                'destination_client': destination_client_id,
                                'operation': 'get_stats',
                                'action': None,
                                'result_type': 'json'
                            }
                            worker_side_error = any(resp_msg['status'] == 'err'
                                                    for resp_msg in self._worker_get_stats_cmd_responses)
                            if worker_side_error:    # Embed in JSON response info abt the first encountered error
                                first_error = next(resp for resp in self._worker_get_stats_cmd_responses
                                                if uuid.UUID(resp['message_id']) == request_id and resp['status'] == 'err')
                                json_payload['status'] = 'err'
                                json_payload['result'] = None
                                json_payload['message'] = first_error['message']
                            else:      # Embed in JSON response an aggregated map of function stats
                                all_results = [resp_msg['result'] for resp_msg in self._worker_get_stats_cmd_responses if uuid.UUID(resp_msg['message_id']) == request_id]
                                aggregated_stats = self._aggregate_stats_per_id(all_results)    # Statistics aggregation
                                json_payload['result'] = aggregated_stats
                                json_payload['status'] = 'ok'
                                json_payload['message'] = None
                            
                            with self._lock:    # Removing all the processed messages
                                self._worker_get_stats_cmd_responses = [msg for msg in self._worker_get_stats_cmd_responses if uuid.UUID(msg['message_id']) != request_id]

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
                    self._functions_workers_map[func_id]['available_on'] = 'ANY'

                self._workers_are_synchronized = True
                self._logger.info('Workers synchronization run completed successfully')
                self._logger.debug(f'Current Workers state: {self._functions_workers_map}')

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

    def _aggregate_stats_per_id(self, list_of_maps: list) -> dict:
        acc = defaultdict(lambda: {'calls_sum': 0, 'tot_exec_sum': 0.0, 'avg_sum': 0.0, 'avg_count': 0})    # Accumulator per func_id: store sums and counts needed to compute the final fields

        for entry in list_of_maps:      # Building the accumulator
            for func_id, stats in entry.items():
                if stats == {}:
                    continue
                calls = int(stats.get('#calls', 0))     # Safe extraction with defaults
                tot_exec = float(stats.get('tot_exec_time', 0.0))
                avg_exec = float(stats.get('avg_exec_time'))

                acc[func_id]['calls_sum'] += calls
                acc[func_id]['tot_exec_sum'] += tot_exec

                if avg_exec is not None:
                    acc[func_id]['avg_sum'] += float(avg_exec)
                    acc[func_id]['avg_count'] += 1

        # Building the final result
        aggregated_stats = {}
        for func_id, v in acc.items():
            avg_value = (v['avg_sum'] / v['avg_count']) if v['avg_count'] > 0 else 0.0
            aggregated_stats[func_id] = {
                '#calls': v['calls_sum'],
                'avg_exec_time': avg_value,
                'tot_exec_time': v['tot_exec_sum']
            }
        
        return aggregated_stats


def setup_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config_file', default=None, help="The Director's configuration file path")
    return parser


def main():
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
