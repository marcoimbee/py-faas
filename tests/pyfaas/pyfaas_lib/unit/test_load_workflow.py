import pytest
import json
from pyfaas.pyfaas import pyfaas_load_workflow, _CLIENT_MANAGER
from pyfaas.exceptions import PyFaaSWorkflowLoadingError


def setup_module():
    # Configure client for tests
    _CLIENT_MANAGER.configured = True

def teardown_module():
    # Reset client configuration after tests
    _CLIENT_MANAGER.configured = False

def test_load_workflow_success(tmp_path):
    # Create a temporary workflow JSON file
    workflow = {
        "task1": {"func": "foo", "args": []},
        "task2": {"func": "bar", "args": [1]}
    }
    workflow_file = tmp_path / "workflow.json"
    workflow_file.write_text(json.dumps(workflow))

    result = pyfaas_load_workflow(str(workflow_file))
    assert result == workflow

def test_load_workflow_not_configured():
    _CLIENT_MANAGER.configured = False
    with pytest.raises(RuntimeError):
        pyfaas_load_workflow("dummy.json")
    _CLIENT_MANAGER.configured = True

def test_load_workflow_missing_path():
    with pytest.raises(PyFaaSWorkflowLoadingError) as exc:
        pyfaas_load_workflow(None)
    assert "Missing required argument" in str(exc.value)

def test_load_workflow_non_existent_file():
    with pytest.raises(PyFaaSWorkflowLoadingError) as exc:
        pyfaas_load_workflow("non_existent_file.json")
    assert "Error while loading the workflow" in str(exc.value)

def test_load_workflow_invalid_json(tmp_path):
    # Create a file with invalid JSON
    invalid_file = tmp_path / "invalid.json"
    invalid_file.write_text("{invalid_json: }")

    with pytest.raises(PyFaaSWorkflowLoadingError) as exc:
        pyfaas_load_workflow(str(invalid_file))
    assert "Error while loading the workflow" in str(exc.value)
