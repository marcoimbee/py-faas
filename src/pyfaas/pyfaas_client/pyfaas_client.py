import zmq
import uuid
import logging
import time
import dill
import base64
import json

from typing import Callable


class PyfaasClient:
    def __init__(self, director_ip_addr: str, director_port: int, receive_timeout_s: int):
        self._logger = logging.getLogger('pyfaas.client')

        self._client_id = f'client-{uuid.uuid4()}'
        self._director_ip_addr = director_ip_addr
        self._director_port = director_port

        self._receive_timeout_ms = receive_timeout_s * 1000

        # ZeroMQ
        self._zmq_context = zmq.Context()
        self._zmq_socket = self._zmq_context.socket(zmq.DEALER)
        self._zmq_socket.setsockopt(zmq.IDENTITY, self._client_id.encode())
        self._zmq_socket.setsockopt(zmq.RCVTIMEO, self._receive_timeout_ms)
        self._zmq_socket.setsockopt(zmq.LINGER, 0)

        director_connection_string = f'tcp://{self._director_ip_addr}:{self._director_port}'
        self._logger.info(f'Connecting to PyFaaS Director at {director_connection_string}...')
        self._zmq_socket.connect(director_connection_string)

    def _send_request(self, operation: str, extra_payload: dict = None) -> dict:
        payload = {
            'requester': self._client_id,
            'operation': operation
        }

        if extra_payload:
            payload.update(extra_payload)

        msg = [b'', json.dumps(payload).encode()]
        self._zmq_socket.send_multipart(msg)

        try:
            _, response = self._zmq_socket.recv_multipart()
        except zmq.Again:               
            # TODO: here it retries automatically after 
            # timeout without backoff or safety -> may double send, potential double execution on worker 
            self._recreate_socket()
            self._logger.warning(f"Timeout on '{operation}', retrying once...")
            self._zmq_socket.send_multipart(msg)
            _, response = self._zmq_socket.recv_multipart()
        
        return json.loads(response.decode())
    
    def _recreate_socket(self) -> None:
        try:
            self._logger.warning('Recreating ZeroMQ socket due to timeout or connection issue...')

            if self._zmq_socket and not self._zmq_socket.closed:
                self._zmq_socket.close(linger=0)        # Closing existing socket immediately
            
            # Creating a new DEALER socket
            self._zmq_socket = self._zmq_context.socket(zmq.DEALER)
            self._zmq_socket.setsockopt(zmq.IDENTITY, self._client_id.encode())
            self._zmq_socket.setsockopt(zmq.RCVTIMEO, self._receive_timeout_ms)
            self._zmq_socket.setsockopt(zmq.LINGER, 0)

            connection_str = f"tcp://{self._director_ip_addr}:{self._director_port}"
            self._zmq_socket.connect(connection_str)

            self._logger.info(f'Successfully recreated DEALER socket and reconnected to Director at {connection_str}')
        except Exception as e:
            self._logger.error(f'Failed to recreate ZeroMQ socket: {e}')
            raise

    def pyfaas_register(self, func_code: Callable) -> dict:
        # Function serialization
        encoding_start = time.time()
        serialized_func = dill.dumps(func_code)
        serialized_func_base64 = base64.b64encode(serialized_func).decode('utf-8')
        self._logger.debug(f'Base64-encoded function: {serialized_func_base64}')
        encoding_end = time.time()
        self._logger.debug(f'Function encoding took {encoding_end - encoding_start} s')

        extra_payload = {    # To be sent to director, will be forwarded by it to an active worker
            'serialized_func_base64': serialized_func_base64,
        }

        return self._send_request('register', extra_payload)

    def pyfaas_unregister(self, func_id: str) -> dict:
        extra_payload = {    # To be sent to director, will be forwarded by it to an active worker
            'func_id': func_id
        }
        return self._send_request('unregister', extra_payload)
    
    def pyfaas_get_stats(self, func_name: str = None) -> dict:
        extra_payload = {
            'func_name': func_name
        }
        return self._send_request('get_stats', extra_payload)

    def pyfaas_list(self) -> dict:
        return self._send_request('list')

    def pyfaas_exec(self, func_id: str, func_positional_args_list: list[object], func_default_args_list: dict[str, object] = None, save_in_cache: bool = False) -> dict:
        # self._logger.debug(f'Called pyfaas_exec. Args: {func_id, func_positional_args_list, func_default_args_list}, save_in_cache={save_in_cache}')
        extra_payload = {
            'func_id': func_id,
            'positional_args': func_positional_args_list,
            'default_args': func_default_args_list,
            'save_in_cache': save_in_cache,
            'additional_data': None
        }

        return self._send_request('exec', extra_payload)

    def pyfaas_get_worker_info(self, worker_id: str) -> dict:
        extra_payload = {
            'worker_id': worker_id
        }
        return self._send_request('get_worker_info', extra_payload)
    
    def pyfaas_get_cache_dump(self, worker_id: str) -> dict:
        extra_payload = {
            'worker_id': worker_id
        }
        return self._send_request('get_cache_dump', extra_payload)

    def pyfaas_chain_exec(self, json_workflow: dict[str, dict[str, object]]) -> dict:
        extra_payload = {
            'json_workflow': json_workflow
        }
        return self._send_request('chain_exec', extra_payload)
    
    def pyfaas_get_worker_ids(self) -> dict:
        return self._send_request('get_worker_ids')
    
    def pyfaas_ping(self) -> dict:
        return self._send_request('PING')

    def zmq_close(self) -> None:
        try:
            self._zmq_socket.close()
            self._zmq_context.term()
            self._logger.info('Closed PyFaaS ZeroMQ context and socket')
        except Exception as e:
            self._logger.warning(f'Error during PyFaaS client cleanup: {e}')
