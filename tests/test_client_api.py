import base64
import json
from unittest.mock import MagicMock

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


@pytest.mark.parametrize('call', [
    lambda: pyfaas.pyfaas_register(lambda: None),
    lambda: pyfaas.pyfaas_unregister('fid'),
    lambda: pyfaas.pyfaas_get_stats(),
    lambda: pyfaas.pyfaas_get_worker_info('worker-1'),
    lambda: pyfaas.pyfaas_get_cache_dump('worker-1'),
    lambda: pyfaas.pyfaas_load_workflow('wf.json'),
    lambda: pyfaas.pyfaas_chain_exec({'id': 'wf1'}),
    lambda: pyfaas.pyfaas_ping(),
    lambda: pyfaas.pyfaas_get_worker_ids(),
])
def test_all_operations_raise_runtime_error_when_unconfigured(monkeypatch, call):
    monkeypatch.setattr(pyfaas._CLIENT_MANAGER, 'configured', False)
    with pytest.raises(RuntimeError):
        call()


def test_register_requires_func_code(fake_client):
    from pyfaas.exceptions import PyFaaSFunctionRegistrationError
    with pytest.raises(PyFaaSFunctionRegistrationError):
        pyfaas.pyfaas_register(None)


def test_unregister_requires_func_id(fake_client):
    with pytest.raises(PyFaaSFunctionUnregistrationError):
        pyfaas.pyfaas_unregister(None)


def test_unregister_ok_status_with_unexpected_action_returns_none(fake_client):
    # Only 'unregistered' is handled on status == 'ok'; any other action value falls
    # through with no return/raise, so the caller silently gets None back.
    fake_client.set_response('pyfaas_unregister', {'status': 'ok', 'action': 'something_else'})
    assert pyfaas.pyfaas_unregister('fid') is None


def test_exec_normalizes_none_default_args_to_empty_dict(fake_client):
    fake_client.set_response('pyfaas_exec', {
        'status': 'ok', 'action': 'executed', 'result_type': 'json', 'result': 3,
    })
    pyfaas.pyfaas_exec('fid', [1, 2], None)
    _, call_args, _ = fake_client.calls[-1]
    assert call_args[2] == {}


def test_exec_ok_status_with_unexpected_action_returns_none(fake_client):
    fake_client.set_response('pyfaas_exec', {'status': 'ok', 'action': 'something_else'})
    assert pyfaas.pyfaas_exec('fid', []) is None


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


def test_load_workflow_malformed_json_raises(fake_client, tmp_path):
    workflow_file = tmp_path / 'wf.json'
    workflow_file.write_text('{not valid json')
    with pytest.raises(PyFaaSWorkflowLoadingError):
        pyfaas.pyfaas_load_workflow(str(workflow_file))


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


def test_chain_exec_rejects_empty_dict_workflow(fake_client):
    with pytest.raises(PyFaaSChainedExecutionError):
        pyfaas.pyfaas_chain_exec({})


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


# --- pyfaas_config / pyfaas_close ---

@pytest.fixture
def reset_client_manager(monkeypatch):
    # pyfaas_config()/pyfaas_close() mutate the module-level singleton directly
    # (rather than being swappable like the client, which `fake_client` replaces),
    # so each test gets a pristine one and the real one is restored afterwards.
    fresh = pyfaas._ClientManager()
    monkeypatch.setattr(pyfaas, '_CLIENT_MANAGER', fresh)
    return fresh


def test_config_with_missing_file_path_uses_default_and_configures(reset_client_manager, monkeypatch, tmp_path):
    config_file = tmp_path / 'config.toml'
    config_file.write_text(
        '[network]\ndirector_ip_addr = "127.0.0.1"\ndirector_port = 40000\nreceive_timeout_s = 5\n'
        '[misc]\nlog_level = "info"\n'
    )
    monkeypatch.setattr(pyfaas, '_DEFAULT_CONFIG_FILE_PATH', str(config_file))
    fake_pyfaas_client = MagicMock()
    monkeypatch.setattr(pyfaas.pyfaas_client, 'PyfaasClient', fake_pyfaas_client)

    pyfaas.pyfaas_config()

    assert reset_client_manager.configured is True
    assert reset_client_manager.client is fake_pyfaas_client.return_value


def test_config_called_twice_reuses_existing_client(reset_client_manager, monkeypatch, tmp_path):
    config_file = tmp_path / 'config.toml'
    config_file.write_text(
        '[network]\ndirector_ip_addr = "127.0.0.1"\ndirector_port = 40000\nreceive_timeout_s = 5\n'
        '[misc]\nlog_level = "info"\n'
    )
    fake_pyfaas_client = MagicMock()
    monkeypatch.setattr(pyfaas.pyfaas_client, 'PyfaasClient', fake_pyfaas_client)

    pyfaas.pyfaas_config(str(config_file))
    first_client = reset_client_manager.client
    pyfaas.pyfaas_config(str(config_file))

    assert reset_client_manager.client is first_client
    fake_pyfaas_client.assert_called_once()


def test_config_wraps_toml_errors(reset_client_manager, tmp_path):
    from pyfaas.exceptions import PyFaaSConfigError
    bad_config = tmp_path / 'bad.toml'
    bad_config.write_text('not valid toml [[[')
    with pytest.raises(PyFaaSConfigError):
        pyfaas.pyfaas_config(str(bad_config))


def test_close_shuts_down_client_and_resets_state(reset_client_manager):
    fake_client = MagicMock()
    reset_client_manager.client = fake_client
    reset_client_manager.configured = True

    pyfaas.pyfaas_close()

    fake_client.zmq_close.assert_called_once()
    assert reset_client_manager.client is None
    assert reset_client_manager.configured is False


def test_close_with_no_client_is_a_no_op(reset_client_manager):
    pyfaas.pyfaas_close()  # must not raise
