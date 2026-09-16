from unittest.mock import MagicMock

import pytest

from pyfaas_worker.app.pyfaas_worker import PyfaasWorker


@pytest.fixture
def worker(worker_config):
    w = PyfaasWorker(worker_config)
    w._zmq_socket = MagicMock()  # never actually bind/send over a real socket
    return w


# --- known-bug regression: a mid-sync unregister can hang the Director's synchronization thread forever ---

def test_forward_function_code_for_unknown_func_id_should_not_raise(worker):
    # The Director's synchronization thread blocks on a queue.get() waiting for
    # this response. If the requesting client unregisters the function between
    # the Director's request and this call, self._functions[requested_func_id]
    # raises an uncaught KeyError here, no response is ever queued, and the
    # Director's synchronization thread hangs permanently (requires a restart).
    try:
        worker._forward_function_code_and_registering_client({'func_id': 'never-registered'})
    except KeyError:
        pytest.xfail('_forward_function_code_and_registering_client raises KeyError for an unregistered '
                      'func_id instead of sending some response, which hangs the Director\'s '
                      'synchronization thread forever')

    assert worker._outgoing_tx_queue.qsize() == 1
