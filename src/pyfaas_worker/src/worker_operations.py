import json
import datetime
import platform
import os
import base64
import dill
import inspect
import time
import multiprocessing
import signal
import sys
import uuid

from .exceptions import *
from .util.worker_side_workflow_validation import *


class WorkerOperations:
    def __init__(self, worker):
        self.worker = worker        # Worker instance that created this operations obj
        
    def execute_register_cmd(self, json_payload: dict) -> None:
        requester_client = json_payload['requester']

        serialized_func_base64 = json_payload['serialized_func_base64']
        serialized_func_bytes = base64.b64decode(serialized_func_base64)
        client_function = dill.loads(serialized_func_bytes)
        func_name = client_function.__name__
        func_id = json_payload['func_id']           # Computed and sent by the Director

        client_json_response = None
        response = None

        func_signature = inspect.signature(client_function)

        # Checking that client has specified type annotations for parameters
        for name, param in func_signature.parameters.items():
            if param.annotation is inspect._empty:
                self.worker._logger.debug(f"Unspecified type annotation for parameter '{name}' of function '{func_name}'")
                client_json_response = self._build_JSON_response(
                    message_id=uuid.uuid4(),
                    dest_client=requester_client, 
                    director_operation='forward_to_client', 
                    original_client_operation='register',
                    status='err', 
                    action=None, 
                    result_type=None, 
                    result=None, 
                    message=f"Unspecified type annotation for parameter '{name}' of function '{func_name}'"
                )
                response = [b'', json.dumps(client_json_response).encode()]
                self.worker._outgoing_tx_queue.put(response)
                return
        
        # Checking if the client has specified type annotation for the return type
        if func_signature.return_annotation is inspect._empty:
            self.worker._logger.debug(f"Unspecified return annotation of function '{func_name}'")
            client_json_response = self._build_JSON_response(
                message_id=uuid.uuid4(),
                dest_client=requester_client, 
                director_operation='forward_to_client', 
                original_client_operation='register',
                status='err', 
                action=None, 
                result_type=None, 
                result=None, 
                message=f"Unspecified return annotation of function '{func_name}'"
            )
            response = [b'', json.dumps(client_json_response).encode()]
            self.worker._outgoing_tx_queue.put(response)
            return

        # Function has been validated and is register-able at this point
        if func_id not in self.worker._functions:
            with self.worker._lock:
                self.worker._functions[func_id] = {}
                self.worker._functions[func_id]['name'] = func_name
                self.worker._functions[func_id]['code'] = client_function
                self.worker._functions[func_id]['registering_client'] = requester_client
            self.worker._logger.info(f'Function {func_name} successfully registered')
            self.worker._file_logger.log('INFO', f"Function registration: '{func_name}'")
            client_json_response = self._build_JSON_response(
                message_id=uuid.uuid4(),
                dest_client=requester_client, 
                director_operation='forward_to_client', 
                original_client_operation='register',
                status='ok', 
                action='registered', 
                result_type='text', 
                result=func_id,           # Propagate to client
                message=None
            )
        else:
            self.worker._logger.warning(f"A function named '{func_name}' is already registered")
            client_json_response = self._build_JSON_response(
                message_id=uuid.uuid4(),
                dest_client=requester_client, 
                director_operation='forward_to_client', 
                original_client_operation='register',
                status='ok', 
                action='no_action', 
                result_type='text', 
                result=func_id, 
                message=None
            )

        response = [b'', json.dumps(client_json_response).encode()]
        self.worker._outgoing_tx_queue.put(response)

    def execute_get_cache_dump_cmd(self, json_payload: dict) -> None:
        requester_client = json_payload['requester']           # Extracting ID of the client that requested the operation
        with self.worker._lock:
            cache_dump = self.worker._function_exec_cache.get_cache_dump()
        client_json_response = self._build_JSON_response(
            message_id=uuid.uuid4(),
            dest_client=requester_client, 
            director_operation='forward_to_client', 
            original_client_operation='register',
            status='ok', 
            action=None, 
            result_type='json', 
            result=cache_dump, 
            message=None
        )
        response = [b'', json.dumps(client_json_response).encode()]
        self.worker._outgoing_tx_queue.put(response)

    def execute_ping_cmd(self, json_payload: dict) -> None:
        self.worker._logger.info(f"Client says: 'PING'")
        requester_client = json_payload['requester']
        client_json_response = self._build_JSON_response(
            message_id=uuid.uuid4(),
            dest_client=requester_client, 
            director_operation='forward_to_client', 
            original_client_operation='ping',
            status='ok', 
            action=None, 
            result_type='json', 
            result='PONG', 
            message=None
        )
        response = [b'', json.dumps(client_json_response).encode()]
        self.worker._outgoing_tx_queue.put(response)

    def execute_get_worker_info_cmd(self, json_payload: dict) -> None:
        requester_client = json_payload['requester']
        try:
            info_summary = {}

            # Worker identity
            info_summary['identity'] = {}
            info_summary['identity']['id'] = self.worker._id
            info_summary['identity']['start_time'] = self.worker._start_time.isoformat()
            str_uptime = str(datetime.datetime.now() - self.worker._start_time)
            info_summary['identity']['uptime'] = str_uptime

            # System info
            info_summary['system'] = {}
            info_summary['system']['python_version'] = platform.python_version()
            info_summary['system']['OS'] = platform.system()
            info_summary['system']['CPU'] = platform.processor()
            info_summary['system']['cores'] = os.cpu_count() or 1   # Fallback to 1 if unable to determine

            # Worker configuration info
            info_summary['config'] = {}
            info_summary['config']['enabled_statistics'] = self.worker._config['statistics']['enabled']
            info_summary['config']['log_level'] = self.worker._config['misc']['log_level']

            # TODO: add execution limits in info

            # Function info
            info_summary['functions'] = {}
            info_summary['functions'] = self.worker._functions

            # Network info
            info_summary['network'] = {}
            info_summary['network']['request_count'] = self.worker._request_count
            info_summary['network']['last_client_connection_timestamp'] = str(self.worker._last_client_connection_ts)

            client_json_response = self._build_JSON_response(
                message_id=uuid.uuid4(),
                dest_client=requester_client, 
                director_operation='forward_to_client', 
                original_client_operation='get_worker_info',
                status='ok', 
                action=None, 
                result_type='json', 
                result=info_summary, 
                message=None
            )
            response = [b'', json.dumps(client_json_response).encode()]
            self.worker._outgoing_tx_queue.put(response)
        except Exception as e:
            client_json_response = self._build_JSON_response(
                message_id=uuid.uuid4(),
                dest_client=requester_client, 
                director_operation='forward_to_client', 
                original_client_operation='get_worker_info',
                status='err', 
                action=None, 
                result_type='json', 
                result=None, 
                message=f'{e}'
            )
            response = [b'', json.dumps(client_json_response).encode()]
            self.worker._outgoing_tx_queue.put(response)

    def execute_get_stats_cmd(self, json_payload: dict) -> None:
        requester_client = json_payload['requester']
        try:
            func_name = json_payload['func_name']
            if func_name is not None:
                if func_name not in self.worker._stats:
                    raise Exception(f"No function named '{func_name}' is registered right now")
                else:
                    with self.worker._lock:
                        stats_for_client = self.worker._stats[func_name]   # Send only stats for the specified function
            else:
                with self.worker._lock:
                    stats_for_client = self.worker._stats   # No func name was specified, send all stats

            client_json_response = self._build_JSON_response(
                message_id=uuid.uuid4(),
                dest_client=requester_client, 
                director_operation='forward_to_client', 
                original_client_operation='get_stats',
                status='ok', 
                action=None, 
                result_type='json', 
                result=stats_for_client, 
                message=None
            )
            response = [b'', json.dumps(client_json_response).encode()]
            self.worker._outgoing_tx_queue.put(response)
        except Exception as e:
            client_json_response = self._build_JSON_response(
                message_id=uuid.uuid4(),
                dest_client=requester_client, 
                director_operation='forward_to_client', 
                original_client_operation='get_stats', 
                status='err', 
                action=None, 
                result_type='json', 
                result=None, 
                message=f'{e}'
            )
            response = [b'', json.dumps(client_json_response).encode()]
            self.worker._outgoing_tx_queue.put(response)

    def execute_list_cmd(self, json_payload: dict) -> None:
        requester_client = json_payload['requester']
        try:
            func_list = {
                func_id: data
                for func_id, data in self.worker._functions.items()
                if data['registering_client'] == requester_client
            }
            self.worker._logger.info(f'List: retrieved {len(func_list)} functions')

            client_json_response = self._build_JSON_response(
                message_id=uuid.uuid4(),
                dest_client=requester_client, 
                director_operation='forward_to_client', 
                original_client_operation='list',
                status='ok', 
                action=None, 
                result_type='json', 
                result=func_list, 
                message=None
            )
            response = [b'', json.dumps(client_json_response).encode()]
            self.worker._outgoing_tx_queue.put(response)
        except Exception as e:
            client_json_response = self._build_JSON_response(
                message_id=uuid.uuid4(),
                dest_client=requester_client, 
                director_operation='forward_to_client', 
                original_client_operation='list', 
                status='err', 
                action=None, 
                result_type='json', 
                result=None, 
                message=f'{type(e).__name__}: {e}'
            )
            response = [b'', json.dumps(client_json_response).encode()]
            self.worker._outgoing_tx_queue.put(response)

    def execute_exec_cmd(self, json_payload: dict) -> None:
        requester_client = json_payload['requester']

        func_id = json_payload['func_id']       # Used as KEY to access self._functions
        func_positional_args = json_payload.get('positional_args', [])        # Default empty list
        func_default_args = json_payload.get('default_args', {})              # Default empty dict
        save_in_cache = json_payload.get('save_in_cache', False)  # Default to False if something weird has been specified client-side

        if func_id not in self.worker._functions:
            self.worker._logger.info(f"No function with ID '{func_id}' is registered right now")
            client_json_response = self._build_JSON_response(
                message_id=uuid.uuid4(),
                dest_client=requester_client, 
                director_operation='forward_to_client', 
                original_client_operation='exec',
                status='err', 
                action='no_func', 
                result_type=None, 
                result=None, 
                message=f"No function with ID '{func_id}' is registered at the Worker right now"
            )
            response = [b'', json.dumps(client_json_response).encode()]
            self.worker._outgoing_tx_queue.put(response)
        else:
            try:
                func_res = self._execute_function(
                    func_id=func_id,
                    func_positional_args=func_positional_args,
                    func_default_args=func_default_args,
                    save_in_cache=save_in_cache
                )

                encoded_func_res, func_res_type = self._encode_func_result(func_res)          # JSON or base64
                client_json_response = self._build_JSON_response(
                    message_id=uuid.uuid4(),
                    dest_client=requester_client, 
                    director_operation='forward_to_client', 
                    original_client_operation='exec',
                    status='ok', 
                    action='executed', 
                    result_type=func_res_type, 
                    result=encoded_func_res, 
                    message=None
                )

                response = [b'', json.dumps(client_json_response).encode()]
                self.worker._outgoing_tx_queue.put(response)
            except Exception as e:
                client_json_response = self._build_JSON_response(
                    message_id=uuid.uuid4(),
                    dest_client=requester_client, 
                    director_operation='forward_to_client', 
                    original_client_operation='exec',
                    status='err', 
                    action=None, 
                    result_type='json', 
                    result=None, 
                    message=f'{type(e).__name__}: {e}'
                )
                response = [b'', json.dumps(client_json_response).encode()]
                self.worker._outgoing_tx_queue.put(response)

    def execute_chain_exec_cmd(self, json_payload: dict) -> None:
        requester_client = json_payload['requester']
        
        workflow = json_payload['json_workflow']

        workflow_id = workflow.get('id')
        workflow_function_set = workflow.get('functions')
        
        # Check if all the listed functions are registered
        function_names = [func_name for func_name, _ in workflow_function_set.items()]
        all_funcs_registered, missing_func_name = self._check_function_set_registration(function_names)
        if not all_funcs_registered:
            self.worker._logger.error(f"No function named '{missing_func_name}' specified in the workflow is registered right now")
            client_json_response = self._build_JSON_response(
                message_id=uuid.uuid4(),
                dest_client=requester_client, 
                director_operation='forward_to_client', 
                original_client_operation='chain_exec',
                status='err', 
                action=None, 
                result_type=None, 
                result=None, 
                message=f"No function named '{missing_func_name}' specified in the workflow is registered at the worker right now"
            )
            response = [b'', json.dumps(client_json_response).encode()]
            self.worker._outgoing_tx_queue.put(response)
            return

        self.worker._logger.info('All functions are registered')

        # Worker-side function validation
        try:
            for func_name in function_names:        # Validating each function
                func_code = self.worker._functions[func_name]   # Here function is registered for sure
                func_positional_args = workflow_function_set[func_name]['positional_args']
                func_default_args = workflow_function_set[func_name]['default_args']
                
                # Passed arguments validation
                validate_function_args(
                    func_code, 
                    func_positional_args, 
                    func_default_args
                )
                
                # Referenced arguments validation
                next_func_in_chain = workflow_function_set[func_name]['next']    # Get function that receives input from this function
                if next_func_in_chain != '':        # If not the final function in chain
                    next_func_in_chain_code = self._functions[next_func_in_chain]
                    next_func_in_chain_positional_args = workflow_function_set[next_func_in_chain]['positional_args']
                    next_func_in_chain_default_args = workflow_function_set[next_func_in_chain]['default_args']
                    validate_return_type_references(        # Provide function i and (i+1) in chain data
                        func_code, 
                        next_func_in_chain_code,
                        next_func_in_chain_positional_args,
                        next_func_in_chain_default_args
                    )
        except WorkerWorkflowValidationError as e:
            self.worker._logger.error(f"Error while validating workflow: {e}")
            client_json_response = self._build_JSON_response(
                message_id=uuid.uuid4(),
                dest_client=requester_client, 
                director_operation='forward_to_client', 
                original_client_operation='chain_exec',
                status='err', 
                action=None, 
                result_type=None, 
                result=None, 
                message=f"Error while validating workflow '{workflow_id}': {e}"
            )
            response = [b'', json.dumps(client_json_response).encode()]
            self.worker._outgoing_tx_queue.put(response)
            return

        # Functions have been validated, are OK, and can be chained
        try:
            # Executing entry function
            entry_func_name = workflow.get('entry_function')
            entry_func_positional_args = workflow_function_set[entry_func_name]['positional_args']
            entry_func_default_args = workflow_function_set[entry_func_name]['default_args']
            save_in_cache = workflow_function_set[entry_func_name]['cache_result']

            entry_func_res = self._execute_function(
                func_name=entry_func_name,
                func_positional_args=entry_func_positional_args,
                func_default_args=entry_func_default_args,
                save_in_cache=save_in_cache
            )

            next_func = workflow_function_set[entry_func_name]['next']
            
            # Executing the following functions in the workflow
            prev_func_name = entry_func_name
            prev_func_result = entry_func_res
            while True:
                func_name = next_func
                save_in_cache = workflow_function_set[func_name]['cache_result']
                
                # Replacing references with results of the previous function
                func_positional_args = workflow_function_set[func_name]['positional_args']
                func_default_args = workflow_function_set[func_name]['default_args']
                for i, arg in enumerate(func_positional_args):
                    if arg == f'${prev_func_name}.output':
                        func_positional_args[i] = prev_func_result
                for def_arg_name, def_arg_value in func_default_args.items():
                    if def_arg_value == f'${prev_func_name}.output':
                        func_default_args[def_arg_name] = prev_func_result
                
                func_res = self._execute_function(
                    func_name=func_name,
                    func_positional_args=func_positional_args,
                    func_default_args=func_default_args,
                    save_in_cache=save_in_cache
                )

                # Updating cycle vars
                next_func = workflow_function_set[func_name]['next']
                if next_func == '':
                    # End of workflow, executed last function, can break out of while
                    break
                prev_func_name = func_name
                prev_func_result = func_res

            encoded_func_res, func_res_type = self._encode_func_result(func_res)          # JSON or base64

            client_json_response = self._build_JSON_response(
                message_id=uuid.uuid4(),
                dest_client=requester_client, 
                director_operation='forward_to_client', 
                original_client_operation='chain_exec',
                status='ok', 
                action='chain_executed', 
                result_type=func_res_type, 
                result=encoded_func_res, 
                message=None
            )
            response = [b'', json.dumps(client_json_response).encode()]
            self.worker._outgoing_tx_queue.put(response)

        except WorkerChainedExecutionError as e:
            self.worker._logger.error(f"Error while executing workflow '{workflow_id}': {e}")
            client_json_response = self._build_JSON_response(
                message_id=uuid.uuid4(),
                dest_client=requester_client, 
                director_operation='forward_to_client', 
                original_client_operation='chain_exec',
                status='err', 
                action=None, 
                result_type=None, 
                result=None, 
                message=f"Error while executing workflow '{workflow_id}': {e}"
            )
            response = [b'', json.dumps(client_json_response).encode()]
            self.worker._outgoing_tx_queue.put(response)

    def execute_unregister_cmd(self, json_payload: dict) -> None:
        requester_client = json_payload['requester']
        request_id = json_payload['request_id']

        func_id = json_payload['func_id']
        client_json_response = None

        if func_id in self.worker._functions:       # The function exists
            # Only the client that registered the function is able to unregister it
            if self.worker._functions[func_id]['registering_client'] == requester_client:
                func_name = self.worker._functions[func_id]['name']
                self.worker._logger.info(f"Unregistering '{func_name}'...")
                self.worker._file_logger.log('INFO', f'Function unregistration: {func_name}')
                with self.worker._lock:
                    del self.worker._functions[func_id]
                if self.worker._config['statistics']['enabled']:
                    del self.worker._stats[func_name]
                client_json_response = self._build_JSON_response(
                    message_id=request_id,         # Sending back the same ID for the director to handle multiple Workers'responses
                    dest_client=requester_client, 
                    director_operation='forward_to_client', 
                    original_client_operation='unregister',
                    status='ok', 
                    action='unregistered', 
                    result_type=None, 
                    result=None, 
                    message=None
                )
            else:       # The unregistration has been requested by a client that did not register the function, this is not allowed
                self.worker._logger.info(f"Client '{requester_client}' is not allowed to unregister the function")
                client_json_response = self._build_JSON_response(
                    message_id=request_id,         # Sending back the same ID for the director to handle multiple Workers'responses
                    dest_client=requester_client, 
                    director_operation='forward_to_client', 
                    original_client_operation='unregister',
                    status='err', 
                    action='forbidden', 
                    result_type=None, 
                    result=None, 
                    message='Only the client that registered a function is able to unregister it'
                )
        else:           # The function does not exist
            self.worker._logger.info(f"No function with ID '{func_id}' is registered right now")
            client_json_response = self._build_JSON_response(
                message_id=request_id,          # Sending back the same ID for the director to handle multiple Workers'responses
                dest_client=requester_client, 
                director_operation='forward_to_client', 
                original_client_operation='unregister',
                status='err', 
                action='no_func', 
                result_type=None, 
                result=None, 
                message=f"No function with ID '{func_id}' is registered at the worker right now"
            )

        response = [b'', json.dumps(client_json_response).encode()]
        self.worker._outgoing_tx_queue.put(response)

    def _execute_function(self, func_id: str, func_positional_args: list, func_default_args: dict, save_in_cache: bool) -> None:
        func_name = self.worker._functions[func_id]['name']
        self.worker._logger.info(f'Executing the following call: {func_name}({func_positional_args}, {func_default_args})')
        try:
            requested_function = self.worker._functions[func_id]['code']

            # Checking for cached result
            with self.worker._lock:
                func_res_already_in_cache = self.worker._function_exec_cache.check_cached(
                    func_id,
                    func_positional_args,
                    func_default_args
                )
            if func_res_already_in_cache:
                # Result is in cache: get it
                try:
                    with self.worker._lock:
                        func_res = self.worker._function_exec_cache.get_cached_result(
                            func_id,
                            func_positional_args,
                            func_default_args
                        )
                    self._logger.info(f"Got cached result: '{func_res}' for '{func_name}'")
                    self._file_logger.log('INFO', 'Cache hit')
                except WorkerFunctionCacheError as e:
                    self._logger.error(f'Exception while fetching result from cache: {e}')
                    self._file_logger.log('ERROR', f'Cache error: {e}')
                    raise Exception(e)
            else:
                # Result is NOT in cache
                # --- FUNCTION EXECUTION ON WORKER ---
                start_time = time.time()
                # TODO: sandboxing in subprocess + limit resources and time
                func_res = requested_function(*func_positional_args, **func_default_args)
                end_time = time.time()
                # ------------------------------------

                exec_time = end_time - start_time
                if self.worker._config['statistics']['enabled']:       
                    self._record_stats(func_id, exec_time)   # If the result is in cache, stats are not recorded for the call
                else:
                    self.worker._logger.info('Statistics have not been enabled')

                # Add to cache if the user wants to
                if save_in_cache:
                    try:
                        with self.worker._lock:
                            self.worker._function_exec_cache.add(func_id, func_positional_args, func_default_args, func_res)
                        self.worker._logger.info(f"Result for latest '{func_name}' call has been saved to worker cache")
                        self.worker._file_logger.log('INFO', f"Cache update: result for latest '{func_name}' call saved to cache")
                    except WorkerFunctionCacheError as e:
                        # This should never happen because we already checked that the
                        # result is NOT in cache. 
                        # However, we check for it 
                        self.worker._logger.error(f'Exception while adding result to cache: {e}')
                        raise Exception(e)

                self.worker._logger.info(f"Executed '{func_name}' in {exec_time} s. Result: '{func_res}'")
                self.worker._file_logger.log('INFO', f'Executed {func_name}({func_positional_args}, {func_default_args}) in {exec_time}')

                return func_res
        except Exception as e:
            self.worker._logger.error(f"Error while executing function '{func_name}': {e}")
            raise WorkerFunctionExecutionError(e)

    def _record_stats(self, func_name: str, exec_time: float) -> None:
        if func_name not in self.worker._stats:
            with self.worker._lock:
                self.worker._stats[func_name] = {}
                self.worker._stats[func_name]['#calls'] = 1
                self.worker._stats[func_name]['avg_exec_time'] = exec_time
                self.worker._stats[func_name]['tot_exec_time'] = exec_time
        else:
            with self.worker._lock:
                self.worker._stats[func_name]['#calls'] += 1
                self.worker._stats[func_name]['tot_exec_time'] += exec_time
                avg_exec_time = self.worker._stats[func_name]['tot_exec_time'] / self.worker._stats[func_name]['#calls']
                self.worker._stats[func_name]['avg_exec_time'] = avg_exec_time

    def _build_JSON_response(
            self, 
            message_id: str, 
            dest_client: str, 
            director_operation: str, 
            client_operation: str, 
            status: str, 
            action: str, 
            result_type: str, 
            result: object, 
            message: str
        ) -> bytes:
        return {
            'message_id': message_id,
            'destination_client': dest_client,               # Client that requested the execution of the operation
            'director_operation': director_operation,        # What the director should do at the reception of this msg
            'original_client_operation': client_operation,   # The operation that was originally requested by the client, for which this message is a response
            'status': status,                                # Outcome of the operation
            'action': action,                                # What has been done (depends on the operation)
            'result_type': result_type,                      # Type of the result (mostly JSON)
            'result': result,                                # Operation result (if any)
            'message': message                               # A non-mandatory message (used in exception handling)
        }

    def _encode_func_result(self, func_result: object) -> tuple[str, str]:
        try:
            json.dumps(func_result)      # Test JSON-serializability, return plain result if successful
            return func_result, 'json'
        except (TypeError, OverflowError):      # result is not JSON-serializable, let caller know
            func_result_bytes = dill.dumps(func_result)
            func_result_base64 = base64.b64encode(func_result_bytes).decode()
            return func_result_base64, 'pickle_base64'

    def _check_function_set_registration(self, function_set: list[str]) -> tuple[bool, str | None]:
        for func in function_set:
            if func not in self.worker._functions:
                return False, func
        return True, None

    # TODO:
    # # Runs inside the child process
    # def _sandbox_function_execution(self):
    #     try:
    #         # Applying limits
            
    #         # CPU time limit
    #         resource.setrlimit(resource.RLIMIT_CPU, (self.worker._exec_limits['cpu_time_s'], self._exec_limits['cpu_time_s']))
            
    #         # Address space limit (memory)
    #         mem_limit_bytes = self._exec_limits['address_space_mb'] * 1024 * 1024
    #         resource.setrlimit(resource.RLIMIT_AS, (mem_limit_bytes, mem_limit_bytes))

    #         # TODO:
