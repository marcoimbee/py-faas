import tomllib
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent


def _load_pyproject():
    with open(_REPO_ROOT / 'pyproject.toml', 'rb') as f:
        return tomllib.load(f)


def test_pyzmq_is_a_declared_dependency():
    # zmq is imported directly by pyfaas_client.py, pyfaas_director.py and
    # pyfaas_worker.py; a clean `pip install` without this listed fails at
    # import time with ModuleNotFoundError.
    pyproject = _load_pyproject()
    assert 'pyzmq' in pyproject['project']['dependencies']


def test_requires_python_supports_match_statements_and_pep_701_fstrings():
    # match statements (director/worker code) need 3.10+; nested-quote f-strings
    # in the read_config_toml implementations need 3.12+ (PEP 701).
    pyproject = _load_pyproject()
    assert pyproject['project']['requires-python'] in ('>=3.12', '>=3.13')
