import base64
import json
from unittest.mock import MagicMock

import dill
import pytest
import zmq

from pyfaas.pyfaas_client.pyfaas_client import PyfaasClient


def make_client():
    client = PyfaasClient.__new__(PyfaasClient)  # bypass __init__, avoid a real socket connect
    client._logger = MagicMock()
    client._client_id = 'client-test'
    client._director_ip_addr = '127.0.0.1'
    client._director_port = 40000
    client._receive_timeout_ms = 1000
    client._max_request_retries = 3
    client._zmq_context = MagicMock()
    client._zmq_socket = MagicMock()
    return client


def queue_response(socket, payload: dict):
    socket.recv_multipart.return_value = [b'', json.dumps(payload).encode()]


def test_send_request_success_returns_parsed_json():
    client = make_client()
    queue_response(client._zmq_socket, {'status': 'ok', 'result': 42})

    result = client._send_request('get_stats')

    assert result == {'status': 'ok', 'result': 42}
    sent_msg = client._zmq_socket.send_multipart.call_args[0][0]
    sent_payload = json.loads(sent_msg[1].decode())
    assert sent_payload == {'requester': 'client-test', 'operation': 'get_stats'}


def test_send_request_merges_extra_payload():
    client = make_client()
    queue_response(client._zmq_socket, {'status': 'ok'})

    client._send_request('exec', {'func_id': 'abc'})

    sent_msg = client._zmq_socket.send_multipart.call_args[0][0]
    sent_payload = json.loads(sent_msg[1].decode())
    assert sent_payload['func_id'] == 'abc'
    assert sent_payload['operation'] == 'exec'


def test_send_request_gives_up_after_max_retries(monkeypatch):
    client = make_client()
    client._recreate_socket = MagicMock()  # isolate retry/backoff logic from socket-recreation
    client._zmq_socket.recv_multipart.side_effect = zmq.Again()
    monkeypatch.setattr('pyfaas.pyfaas_client.pyfaas_client.time.sleep', lambda s: None)

    with pytest.raises(zmq.Again):
        client._send_request('PING')

    assert client._zmq_socket.send_multipart.call_count == 3
    assert client._recreate_socket.call_count == 3


def test_send_request_should_not_sleep_after_final_failed_attempt(monkeypatch):
    # After the 3rd (last) failed attempt, _send_request falls straight through to
    # `raise zmq.Again` -- no further attempt will ever use that sleep, so it's a
    # pure wasted delay (up to ~0.6s across the exponential backoff).
    client = make_client()
    client._recreate_socket = MagicMock()
    client._zmq_socket.recv_multipart.side_effect = zmq.Again()
    sleep_calls = []
    monkeypatch.setattr('pyfaas.pyfaas_client.pyfaas_client.time.sleep', lambda s: sleep_calls.append(s))

    with pytest.raises(zmq.Again):
        client._send_request('PING')

    if len(sleep_calls) == 3:
        pytest.xfail('_send_request sleeps after every failed attempt, including the 3rd and final one, '
                      'which is guaranteed to be followed only by `raise zmq.Again`')


def test_send_request_recovers_after_transient_timeout(monkeypatch):
    client = make_client()
    client._recreate_socket = MagicMock()  # isolate retry/backoff logic from socket-recreation
    monkeypatch.setattr('pyfaas.pyfaas_client.pyfaas_client.time.sleep', lambda s: None)

    call_count = {'n': 0}

    def recv_side_effect(*args, **kwargs):
        call_count['n'] += 1
        if call_count['n'] == 1:
            raise zmq.Again()
        return [b'', json.dumps({'status': 'ok'}).encode()]

    client._zmq_socket.recv_multipart.side_effect = recv_side_effect

    result = client._send_request('PING')

    assert result == {'status': 'ok'}
    assert call_count['n'] == 2


def test_recreate_socket_closes_old_and_builds_new_one():
    client = make_client()
    old_socket = client._zmq_socket
    old_socket.closed = False

    client._recreate_socket()

    old_socket.close.assert_called_once_with(linger=0)
    assert client._zmq_socket is client._zmq_context.socket.return_value
    client._zmq_socket.connect.assert_called_once_with('tcp://127.0.0.1:40000')


def test_pyfaas_register_encodes_function_and_sends_it():
    client = make_client()
    queue_response(client._zmq_socket, {'status': 'ok', 'action': 'registered', 'result': 'abc123'})

    def add(a: int, b: int) -> int:
        return a + b

    result = client.pyfaas_register(add)

    assert result == {'status': 'ok', 'action': 'registered', 'result': 'abc123'}
    sent_msg = client._zmq_socket.send_multipart.call_args[0][0]
    sent_payload = json.loads(sent_msg[1].decode())
    decoded_func = dill.loads(base64.b64decode(sent_payload['serialized_func_base64']))
    assert decoded_func(2, 3) == 5


def test_init_sets_socket_options_and_connects(monkeypatch):
    fake_context = MagicMock()
    fake_socket = MagicMock()
    fake_context.socket.return_value = fake_socket
    monkeypatch.setattr('pyfaas.pyfaas_client.pyfaas_client.zmq.Context', lambda: fake_context)

    client = PyfaasClient('127.0.0.1', 40000, receive_timeout_s=5)

    assert client._zmq_socket is fake_socket
    fake_socket.setsockopt.assert_any_call(zmq.IDENTITY, client._client_id.encode())
    fake_socket.setsockopt.assert_any_call(zmq.RCVTIMEO, 5000)
    fake_socket.setsockopt.assert_any_call(zmq.LINGER, 0)
    fake_socket.connect.assert_called_once_with('tcp://127.0.0.1:40000')


def test_pyfaas_unregister_payload_shape():
    client = make_client()
    queue_response(client._zmq_socket, {'status': 'ok'})

    client.pyfaas_unregister('fid-1')

    sent_payload = json.loads(client._zmq_socket.send_multipart.call_args[0][0][1].decode())
    assert sent_payload['operation'] == 'unregister'
    assert sent_payload['func_id'] == 'fid-1'


def test_pyfaas_get_stats_payload_shape():
    client = make_client()
    queue_response(client._zmq_socket, {'status': 'ok'})

    client.pyfaas_get_stats()

    sent_payload = json.loads(client._zmq_socket.send_multipart.call_args[0][0][1].decode())
    assert sent_payload['operation'] == 'get_stats'


def test_pyfaas_list_payload_shape():
    client = make_client()
    queue_response(client._zmq_socket, {'status': 'ok'})

    client.pyfaas_list()

    sent_payload = json.loads(client._zmq_socket.send_multipart.call_args[0][0][1].decode())
    assert sent_payload['operation'] == 'list'


def test_pyfaas_get_worker_info_payload_shape():
    client = make_client()
    queue_response(client._zmq_socket, {'status': 'ok'})

    client.pyfaas_get_worker_info('worker-1')

    sent_payload = json.loads(client._zmq_socket.send_multipart.call_args[0][0][1].decode())
    assert sent_payload['operation'] == 'get_worker_info'
    assert sent_payload['worker_id'] == 'worker-1'


def test_pyfaas_get_cache_dump_payload_shape():
    client = make_client()
    queue_response(client._zmq_socket, {'status': 'ok'})

    client.pyfaas_get_cache_dump('worker-1')

    sent_payload = json.loads(client._zmq_socket.send_multipart.call_args[0][0][1].decode())
    assert sent_payload['operation'] == 'get_cache_dump'
    assert sent_payload['worker_id'] == 'worker-1'


def test_pyfaas_chain_exec_payload_shape():
    client = make_client()
    queue_response(client._zmq_socket, {'status': 'ok'})
    workflow = {'id': 'wf1'}

    client.pyfaas_chain_exec(workflow)

    sent_payload = json.loads(client._zmq_socket.send_multipart.call_args[0][0][1].decode())
    assert sent_payload['operation'] == 'chain_exec'
    assert sent_payload['json_workflow'] == workflow


def test_pyfaas_get_worker_ids_payload_shape():
    client = make_client()
    queue_response(client._zmq_socket, {'status': 'ok'})

    client.pyfaas_get_worker_ids()

    sent_payload = json.loads(client._zmq_socket.send_multipart.call_args[0][0][1].decode())
    assert sent_payload['operation'] == 'get_worker_ids'


def test_pyfaas_ping_payload_shape():
    client = make_client()
    queue_response(client._zmq_socket, {'status': 'ok'})

    client.pyfaas_ping()

    sent_payload = json.loads(client._zmq_socket.send_multipart.call_args[0][0][1].decode())
    assert sent_payload['operation'] == 'PING'


def test_recreate_socket_skips_closing_already_closed_socket():
    client = make_client()
    old_socket = client._zmq_socket
    old_socket.closed = True

    client._recreate_socket()

    old_socket.close.assert_not_called()


def test_recreate_socket_reraises_on_failure():
    client = make_client()
    client._zmq_context.socket.side_effect = Exception('boom')

    with pytest.raises(Exception, match='boom'):
        client._recreate_socket()


def test_pyfaas_exec_payload_shape():
    client = make_client()
    queue_response(client._zmq_socket, {'status': 'ok'})

    client.pyfaas_exec('func-id', [1, 2], {'c': 3}, save_in_cache=True)

    sent_payload = json.loads(client._zmq_socket.send_multipart.call_args[0][0][1].decode())
    assert sent_payload['func_id'] == 'func-id'
    assert sent_payload['positional_args'] == [1, 2]
    assert sent_payload['default_args'] == {'c': 3}
    assert sent_payload['save_in_cache'] is True


def test_zmq_close_closes_socket_and_context():
    client = make_client()
    client.zmq_close()
    client._zmq_socket.close.assert_called_once()
    client._zmq_context.term.assert_called_once()


def test_zmq_close_swallows_errors():
    client = make_client()
    client._zmq_socket.close.side_effect = Exception('boom')
    client.zmq_close()  # must not raise
