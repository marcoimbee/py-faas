import base64
import json

import dill
import pytest

import pyfaas.pyfaas as pyfaas
from pyfaas.exceptions import (
    PyFaaSCacheDumpingError,
    PyFaaSChainedExecutionError,
    PyFaaSFunctionExecutionError,
    PyFaaSFunctionListingError,
    PyFaaSFunctionUnregistrationError,
    PyFaaSParameterMismatchError,
    PyFaaSPingingError,
    PyFaaSStatisticsRetrievalError,
    PyFaaSTimeoutError,
    PyFaaSWorkerIDsRetrievalError,
    PyFaaSWorkerInfoError,
    PyFaaSWorkflowLoadingError,
    PyFaaSWorkflowValidationError,
)


class FakeClient:
    def __init__(self):
        self.responses = {}
        self.calls = []
        self.raise_again_for = set()

    def set_response(self, method, response):
        self.responses[method] = response

    def __getattr__(self, name):
        def call(*args, **kwargs):
            self.calls.append((name, args, kwargs))
            if name in self.raise_again_for:
                import zmq
                raise zmq.Again()
            return self.responses[name]
        return call


@pytest.fixture
def fake_client(monkeypatch):
    client = FakeClient()
    monkeypatch.setattr(pyfaas._CLIENT_MANAGER, 'configured', True)
    monkeypatch.setattr(pyfaas._CLIENT_MANAGER, 'client', client)
    return client


def test_unconfigured_calls_raise_runtime_error(monkeypatch):
    monkeypatch.setattr(pyfaas._CLIENT_MANAGER, 'configured', False)
    with pytest.raises(RuntimeError):
        pyfaas.pyfaas_list()
    with pytest.raises(RuntimeError):
        pyfaas.pyfaas_exec('id', [])


def test_register_success_returns_func_id(fake_client):
    fake_client.set_response('pyfaas_register', {'status': 'ok', 'action': 'registered', 'result': 'fid1'})

    def add(a: int, b: int) -> int:
        return a + b

    assert pyfaas.pyfaas_register(add) == 'fid1'


def test_register_error_raises(fake_client):
    from pyfaas.exceptions import PyFaaSFunctionRegistrationError
    fake_client.set_response('pyfaas_register', {'status': 'err', 'message': 'bad function'})

    def add(a: int, b: int) -> int:
        return a + b

    with pytest.raises(PyFaaSFunctionRegistrationError):
        pyfaas.pyfaas_register(add)


def test_register_timeout_raises(fake_client):
    fake_client.raise_again_for.add('pyfaas_register')

    def add(a: int, b: int) -> int:
        return a + b

    with pytest.raises(PyFaaSTimeoutError):
        pyfaas.pyfaas_register(add)


def test_exec_rejects_non_list_positional_args(fake_client):
    with pytest.raises(PyFaaSParameterMismatchError):
        pyfaas.pyfaas_exec('fid', 'not-a-list')


def test_exec_returns_plain_json_result(fake_client):
    fake_client.set_response('pyfaas_exec', {
        'status': 'ok', 'action': 'executed', 'result_type': 'json', 'result': 42,
    })
    assert pyfaas.pyfaas_exec('fid', [1, 2]) == 42


def test_exec_decodes_pickle_base64_result(fake_client):
    payload = {1, 2, 3}  # not JSON-serializable, must go through dill
    encoded = base64.b64encode(dill.dumps(payload)).decode()
    fake_client.set_response('pyfaas_exec', {
        'status': 'ok', 'action': 'executed', 'result_type': 'pickle_base64', 'result': encoded,
    })
    assert pyfaas.pyfaas_exec('fid', []) == payload


def test_exec_error_raises(fake_client):
    fake_client.set_response('pyfaas_exec', {'status': 'err', 'message': 'boom'})
    with pytest.raises(PyFaaSFunctionExecutionError):
        pyfaas.pyfaas_exec('fid', [])


def test_unregister_success(fake_client):
    fake_client.set_response('pyfaas_unregister', {'status': 'ok', 'action': 'unregistered'})
    assert pyfaas.pyfaas_unregister('fid') == 1


def test_unregister_error_raises(fake_client):
    fake_client.set_response('pyfaas_unregister', {'status': 'err', 'message': 'forbidden'})
    with pytest.raises(PyFaaSFunctionUnregistrationError):
        pyfaas.pyfaas_unregister('fid')


def test_list_success(fake_client):
    fake_client.set_response('pyfaas_list', {'status': 'ok', 'result': {'fid1': 'add'}})
    assert pyfaas.pyfaas_list() == {'fid1': 'add'}


def test_list_error_raises(fake_client):
    fake_client.set_response('pyfaas_list', {'status': 'err', 'message': 'boom'})
    with pytest.raises(PyFaaSFunctionListingError):
        pyfaas.pyfaas_list()


def test_get_stats_success(fake_client):
    fake_client.set_response('pyfaas_get_stats', {'status': 'ok', 'result': {'fid1': {'#calls': 1}}})
    assert pyfaas.pyfaas_get_stats() == {'fid1': {'#calls': 1}}


def test_get_stats_error_raises(fake_client):
    fake_client.set_response('pyfaas_get_stats', {'status': 'err', 'message': 'boom'})
    with pytest.raises(PyFaaSStatisticsRetrievalError):
        pyfaas.pyfaas_get_stats()


def test_get_worker_info_requires_worker_id(fake_client):
    with pytest.raises(PyFaaSWorkerInfoError):
        pyfaas.pyfaas_get_worker_info(None)


def test_get_worker_info_success(fake_client):
    fake_client.set_response('pyfaas_get_worker_info', {'status': 'ok', 'result': {'id': 'worker-1'}})
    assert pyfaas.pyfaas_get_worker_info('worker-1') == {'id': 'worker-1'}


def test_get_worker_info_error_raises(fake_client):
    fake_client.set_response('pyfaas_get_worker_info', {'status': 'err', 'message': 'unknown worker'})
    with pytest.raises(PyFaaSWorkerInfoError):
        pyfaas.pyfaas_get_worker_info('worker-1')


def test_get_cache_dump_requires_worker_id(fake_client):
    with pytest.raises(PyFaaSCacheDumpingError):
        pyfaas.pyfaas_get_cache_dump(None)


def test_get_cache_dump_success(fake_client):
    fake_client.set_response('pyfaas_get_cache_dump', {'status': 'ok', 'result': {'cache': {}}})
    assert pyfaas.pyfaas_get_cache_dump('worker-1') == {'cache': {}}


def test_get_cache_dump_error_raises(fake_client):
    fake_client.set_response('pyfaas_get_cache_dump', {'status': 'err', 'message': 'unknown worker'})
    with pytest.raises(PyFaaSCacheDumpingError):
        pyfaas.pyfaas_get_cache_dump('worker-1')


def test_load_workflow_requires_path(fake_client):
    with pytest.raises(PyFaaSWorkflowLoadingError):
        pyfaas.pyfaas_load_workflow(None)


def test_load_workflow_missing_file_raises(fake_client):
    with pytest.raises(PyFaaSWorkflowLoadingError):
        pyfaas.pyfaas_load_workflow('/does/not/exist.json')


def test_load_workflow_reads_json(fake_client, tmp_path):
    workflow_file = tmp_path / 'wf.json'
    workflow_file.write_text(json.dumps({'id': 'wf1'}))
    assert pyfaas.pyfaas_load_workflow(str(workflow_file)) == {'id': 'wf1'}


def _valid_workflow():
    return {
        'id': 'wf1',
        'entry_function': 'add',
        'functions': {
            'add': {
                'positional_args': [1, 2],
                'default_args': {},
                'next': '',
                'cache_result': False,
            },
        },
    }


def test_chain_exec_requires_workflow(fake_client):
    with pytest.raises(PyFaaSChainedExecutionError):
        pyfaas.pyfaas_chain_exec(None)


def test_chain_exec_rejects_structurally_invalid_workflow_without_contacting_client(fake_client):
    with pytest.raises(PyFaaSWorkflowValidationError):
        pyfaas.pyfaas_chain_exec({'id': 'wf1'})  # missing entry_function/functions
    assert fake_client.calls == []


def test_chain_exec_success(fake_client):
    fake_client.set_response('pyfaas_chain_exec', {'status': 'ok', 'result': 3})
    assert pyfaas.pyfaas_chain_exec(_valid_workflow()) == 3


def test_chain_exec_error_raises(fake_client):
    fake_client.set_response('pyfaas_chain_exec', {'status': 'err', 'message': 'boom'})
    with pytest.raises(PyFaaSChainedExecutionError):
        pyfaas.pyfaas_chain_exec(_valid_workflow())


def test_ping_success(fake_client):
    fake_client.set_response('pyfaas_ping', {'status': 'ok', 'result': 'PONG'})
    pyfaas.pyfaas_ping()  # must not raise


def test_ping_error_raises(fake_client):
    fake_client.set_response('pyfaas_ping', {'status': 'err', 'message': 'boom'})
    with pytest.raises(PyFaaSPingingError):
        pyfaas.pyfaas_ping()


def test_get_worker_ids_success(fake_client):
    fake_client.set_response('pyfaas_get_worker_ids', {'status': 'ok', 'result': ['worker-1', 'worker-2']})
    assert pyfaas.pyfaas_get_worker_ids() == ['worker-1', 'worker-2']


def test_get_worker_ids_error_raises(fake_client):
    fake_client.set_response('pyfaas_get_worker_ids', {'status': 'err', 'message': 'boom'})
    with pytest.raises(PyFaaSWorkerIDsRetrievalError):
        pyfaas.pyfaas_get_worker_ids()
