import logging
import dill
import json
import base64
import atexit
import zmq

from typing import Callable
from pyfaas.pyfaas_client import pyfaas_client
from pyfaas.util.general import *
from pyfaas.util.client_side_workflow_validation import *
from pyfaas.exceptions import *


# --- PyFaaS Client ---
class _ClientManager:
    def __init__(self):
        self.client = None
        self.configured = False
        self.config = None

_CLIENT_MANAGER = _ClientManager()               # Initialized by a call to pyfaas_config()

# --- PyFaaS configuration
_CONFIG_FILE_PATH: str | None = None
_DEFAULT_CONFIG_FILE_PATH: str = 'test/client_config.toml'

# --- Logging ---
logger = logging.getLogger('pyfaas')
logger.setLevel(logging.INFO)

def pyfaas_config(file_path: str = None) -> None:
    '''
    Configures PyFaaS for client usage.

    Args:
        file_path (str): The path to the configuration file to be used to configure PyFaaS.

    Raises:
        PyFaaSConfigError: Raised if the parsing of the TOML configuration file does not go as expected / there are errors in the file.
    '''
    global _CLIENT_MANAGER, _CONFIG_FILE_PATH
    
    if not _CLIENT_MANAGER.configured:
        _CLIENT_MANAGER.configured = True

        if not file_path:
            logger.warning(f'Unspecified PyFaaS configuration file path, defaulting to {_DEFAULT_CONFIG_FILE_PATH}')
            _CONFIG_FILE_PATH = _DEFAULT_CONFIG_FILE_PATH
        else:
            _CONFIG_FILE_PATH = file_path

        try:
            _CLIENT_MANAGER.config = read_config_toml(_CONFIG_FILE_PATH)
        except Exception as e:
            raise PyFaaSConfigError(e)

        setup_logging(_CLIENT_MANAGER.config['misc']['log_level'])

        # Client instantiation
        _CLIENT_MANAGER.client = pyfaas_client.PyfaasClient(
            _CLIENT_MANAGER.config['network']['director_ip_addr'],
            _CLIENT_MANAGER.config['network']['director_port'],
            _CLIENT_MANAGER.config['network']['receive_timeout_s']
        )
        
        logger.info(f'PyFaaS has been configured using {_CONFIG_FILE_PATH}')
    else:
        logger.info(f'PyFaaS has already been configured. Reusing existing PyFaaS client')

def pyfaas_close():
    '''
    Gracefully shuts down the ZeroMQ session opened in _global_pyfaas_client
    '''
    global _CLIENT_MANAGER
    if _CLIENT_MANAGER and _CLIENT_MANAGER.client:
        _CLIENT_MANAGER.client.zmq_close()
        _CLIENT_MANAGER.client = None
        _CLIENT_MANAGER.configured = False
        logger.info('PyFaaS client session closed')

def pyfaas_register(func_code: Callable) -> str:
    '''
    Registers the function identified by the ID passed as a parameter to the PyFaaS cluster.

    The returned ID (which is computed as SHA256(func_name, func_code)) can be then 
    used to invoke such a registered function on the PyFaaS cluster.
    The function must have specified all the type annotations (for parameters and return type).

    Args:
        func_code (Callable): The function to be registered.

    Returns:
        str: The ID of the provided function if registered successfully. If the function is already registered, its ID is returned.
    
    Raises:
        PyFaaSTimeoutError: Raised if a timeout is reached while waiting from the Director's response.
        RuntimeError: Raised if PyFaaS has not been configured with a call to pyfaas_config().
        PyFaaSFunctionRegistrationError: Raised if one/more type annotations are missing from the function definition.
    '''
    if not _CLIENT_MANAGER.configured:
        raise RuntimeError('Unable to execute PyFaaS operations: PyFaaS has not been configured with a call to pyfaas_config()')
    
    if not func_code:
        raise PyFaaSFunctionRegistrationError("Missing required argument 'func_code'")

    # Calling actual pyfaas_register() function from global object
    try:
        director_resp_json = _CLIENT_MANAGER.client.pyfaas_register(func_code)
    except zmq.Again:
        raise PyFaaSTimeoutError('Timeout while waiting for Director\'s response during a call to pyfaas_register()')

    func_name = func_code.__name__      # Function name is not necessary, as it can be extracted from the code via function.__name__
    status = director_resp_json.get('status')
    action = director_resp_json.get('action')
    function_id = director_resp_json.get('result')
    message = director_resp_json.get('message')
    if status == 'ok':
        if action == 'registered':
            logger.info(f"Successfully registered '{func_name}', id = '{function_id}'")
        elif action == 'no_action':
            logger.info(f"No action was performed, '{func_name}' is already registered")

        return function_id
    else:
        logger.warning(f'Error while registering a function: {message}')
        raise PyFaaSFunctionRegistrationError(message)

def pyfaas_unregister(func_id: str) -> int:
    '''
    Unregisters the function identified by the ID passed as a parameter from the PyFaaS cluster.

    Only the client that has previously registered a function is able to request its unregistration.

    Args:
        func_id (str): The ID of  the function to be unregistered.

    Returns:
        int: 1 if the unregistration is successful.
    
    Raises:
        RuntimeError: Raised if PyFaaS has not been configured with a call to pyfaas_config().
        PyFaaSTimeoutError: Raised if a timeout is reached while waiting from the Director's response.
        PyFaaSFunctionUnregistrationError: #TODO:
    '''
    if not _CLIENT_MANAGER.configured:
        raise RuntimeError('Unable to execute PyFaaS operations: PyFaaS has not been configured with a call to pyfaas_config()')
    
    if not func_id:
        raise PyFaaSFunctionUnregistrationError("Missing required argument 'func_id'")

    # Calling actual pyfaas_unregister() function from global object
    try:
        director_resp_json = _CLIENT_MANAGER.client.pyfaas_unregister(func_id)
    except zmq.Again:
        raise PyFaaSTimeoutError('Timeout while waiting for Director\'s response during a call to pyfaas_unregister()')

    status = director_resp_json.get('status')
    action = director_resp_json.get('action')
    message = director_resp_json.get('message')
    if status == 'ok':
        if action == 'unregistered':
            logger.info(f"Successfully unregistered '{func_id}'")
            return 1
    elif status == 'err':
        logger.warning(f'Error while unregistering a function: {message}')
        raise PyFaaSFunctionUnregistrationError(message)

# TODO:
def pyfaas_get_stats(func_name: str = None) -> dict:
    if not _CLIENT_MANAGER.configured:
        raise RuntimeError('Unable to execute PyFaaS operations: PyFaaS has not been configured with a call to pyfaas_config()')
    
    # Calling actual pyfaas_get_stats() function from global object
    try:
        director_resp_json = _CLIENT_MANAGER.client.pyfaas_get_stats(func_name)
    except zmq.Again:
        raise PyFaaSTimeoutError('Timeout while waiting for Director\'s response during a call to pyfaas_get_stats()')

    status = director_resp_json.get('status')
    json_stats = director_resp_json.get('result')
    message = director_resp_json.get('message')

    if status == 'ok':
        if func_name is not None:
            logger.info(f"Retrieved stats for '{func_name}'")
        else:
            logger.info(f'Retrieved general stats')
        logger.debug(f'Stats: {json_stats}')
        return json_stats
    else:
        if func_name is not None:
            logger.error(f"Error while retrieving stats for '{func_name}': {message}")
        else:
            logger.error(f'Error while retrieving general stats: {message}')
        raise PyFaaSStatisticsRetrievalError(message)

def pyfaas_list() -> dict:
    '''
    Provides a list of the functions registered by the client.

    Returns:
        dict: A dict representing the functions' data of the functions that have been registered by the client.
    
    Raises:
        RuntimeError: Raised if PyFaaS has not been configured with a call to pyfaas_config().
        PyFaaSTimeoutError: Raised if a timeout is reached while waiting from the Director's response.
        PyFaaSFunctionListingError: Raised if anything in the listing procedure goes unexpectedly.
    '''
    if not _CLIENT_MANAGER.configured:
        raise RuntimeError('Unable to execute PyFaaS operations: PyFaaS has not been configured with a call to pyfaas_config()')
    
    # Calling actual pyfaas_list() function from global object
    try:
        director_resp_json = _CLIENT_MANAGER.client.pyfaas_list()
    except zmq.Again:
        raise PyFaaSTimeoutError('Timeout while waiting for Director\'s response during a call to pyfaas_list()')

    status = director_resp_json.get('status')
    func_list = director_resp_json.get('result')
    message = director_resp_json.get('message')

    if status == 'ok':
        logger.info(f'Retrieved {len(func_list)} functions')
        return func_list
    else:
        logger.warning(f'Error while listing functions on the worker: {message}')
        raise PyFaaSFunctionListingError(message)

# TODO: is it possible not to pass positional args?
def pyfaas_exec(func_id: str, func_positional_args_list: list[object], func_default_args_list: dict[str, object] = None, save_in_cache: bool = False) -> object:
    '''
    Remotely executes the function identified by 'dunc_id' in a Worker of the PyFaaS cluster and returns the result.

    Args:
        func_id (str): The ID of the function to be executed. The ID is returned at registration time by a call to pyfaas_register().
        func_positional_args_list (list[object]): The list of the positional arguments accepted by the specified function.
        func_default_args_list (dict[str, object]): The list of default arguments accepted by the specified function.
        save_in_cache (bool): Whether to save or not the result of the function's execution the executing Worker's cache.

    Returns:
        object: The return value of the remotely executed function.
    
    Raises:
        PyFaaSParameterMismatchError: Raised if the provided arguments type are not compliant with the function's signature.
        PyFaaSTimeoutError: Raised if a timeout is reached while waiting from the Director's response. 
        PyFaaSDeserializationError: Raised if any error occures while deserializing the remotely executed function's result.
        PyFaaSFunctionExecutionError: Raised if the specified function is not registered at any Worker or if an exception is raised during the function's execution.
    '''
    if not _CLIENT_MANAGER.configured:
        raise RuntimeError('Unable to execute PyFaaS operations: PyFaaS has not been configured with a call to pyfaas_config()')
    
    if type(func_positional_args_list) != list:
        logger.error(f"Parameters mismatch: func_arglist must be of type 'list[object]', while {type(func_positional_args_list)} was provided")
        raise PyFaaSParameterMismatchError(f"Parameters mismatch: func_arglist must be of type 'list[object]', while {type(func_positional_args_list)} was provided")

    if func_default_args_list is None:
        func_default_args_list = {}

    # Calling actual pyfaas_exec() function from global object
    try:
        director_resp_json = _CLIENT_MANAGER.client.pyfaas_exec(func_id, func_positional_args_list, func_default_args_list, save_in_cache)
    except zmq.Again:
        raise PyFaaSTimeoutError('Timeout while waiting for Director\'s response during a call to pyfaas_exec()')

    status = director_resp_json.get('status')
    action = director_resp_json.get('action')
    result_type = director_resp_json.get('result_type')
    result = director_resp_json.get('result')
    message = director_resp_json.get('message')

    if status == 'ok':
        if action == 'executed':
            logger.info(f"Executed '{func_id}'")
            if result_type == 'pickle_base64':
                try:
                    result_bytes = base64.b64decode(result)
                    result = dill.loads(result_bytes)
                except Exception as e:
                    raise PyFaaSDeserializationError(f'Failed to deserialize worker result: {e}')
            return result      # it's the JSON result that was included in the worker msg, or the deserialized Base64 result
    else:
        logger.error(f"Error while executing '{func_id}' on the worker: {message}")
        raise PyFaaSFunctionExecutionError(message)

def pyfaas_get_worker_info(worker_id: str) -> dict:
    '''
    Obtains a dict containing information about the specified Worker.

    Args:
        worker_id (str): the Worker ID to retrieve information about.

    Returns:
        dict: A dict containing information about the specified Worker.

    Raises:
        RuntimeError: Raised if PyFaaS has not been configured with a call to pyfaas_config().
        PyFaaSTimeoutError: Raised if a timeout is reached while waiting from the Director's response.
        PyFaaSWorkerInfoError: Raised if the specified Worker ID refers to an unknown Worker, for which the information cannot get retrieved, or if left unspecified.
    '''
    if not _CLIENT_MANAGER.configured:
        raise RuntimeError('Unable to execute PyFaaS operations: PyFaaS has not been configured with a call to pyfaas_config()')
    
    if not worker_id:
        raise PyFaaSWorkerInfoError("Missing required argument 'worker_id'")

    # Calling actual pyfaas_get_worker_info() function from global object
    try:
        director_resp_json = _CLIENT_MANAGER.client.pyfaas_get_worker_info(worker_id)
    except zmq.Again:
        raise PyFaaSTimeoutError('Timeout while waiting for Director\'s response during a call to pyfaas_get_worker_info()')

    status = director_resp_json.get('status')
    result = director_resp_json.get('result')
    message = director_resp_json.get('message')

    if status == 'ok':
        return result
    else:
        logger.warning(f'Error while retrieving worker info: {message}')
        raise PyFaaSWorkerInfoError(message)

def pyfaas_get_cache_dump(worker_id: str) -> dict:
    '''
    Obtains a dict containing the cache dump of the specified Worker.

    Args:
        worker_id (str): the Worker ID of which the cache dump must be retrieved.

    Returns:
        dict: A dict containing the cache dump of the specified Worker.

    Raises:
        RuntimeError: Raised if PyFaaS has not been configured with a call to pyfaas_config().
        PyFaaSTimeoutError: Raised if a timeout is reached while waiting from the Director's response.
        PyFaaSCacheDumpingError: Raised if the specified Worker ID refers to an unknown Worker, for which the information cannot get retrieved, or if left unspecified.
    '''
    if not _CLIENT_MANAGER.configured:
        raise RuntimeError('Unable to execute PyFaaS operations: PyFaaS has not been configured with a call to pyfaas_config()')
    
    if not worker_id:
        raise PyFaaSCacheDumpingError("Missing required argument 'worker_id'")

    # Calling actual pyfaas_get_cache_dump() function from global object
    try:
        director_resp_json = _CLIENT_MANAGER.client.pyfaas_get_cache_dump(worker_id)
    except zmq.Again:
        raise PyFaaSTimeoutError('Timeout while waiting for Director\'s response during a call to pyfaas_get_cache_dump()')

    status = director_resp_json.get('status')
    result = director_resp_json.get('result')

    if status == 'ok':
        return result
    else:
        message = director_resp_json.get('message')
        logger.error(f'Error while retrieving cache dump: {message}')
        raise PyFaaSCacheDumpingError(message)

def pyfaas_load_workflow(workflow_file_path: str) -> dict[str, dict[str, object]]:
    '''
    Loads a PyFaaS workflow specified in workflow_file_path in a dictionary.

    Args:
        workflow_file_path (str): The file from which to load the workflow.

    Returns:
        dict[str, dict[str, object]]: The dictionary containing the loaded workflow.
    
    Raises:
        RuntimeError: Raised if PyFaaS has not been configured with a call to pyfaas_config().
        PyFaaSWorkflowLoadingError: Raised if the specified file path is an invalid/non-existent path, or if left unspecified.
    '''
    if not _CLIENT_MANAGER.configured:
        raise RuntimeError('Unable to execute PyFaaS operations: PyFaaS has not been configured with a call to pyfaas_config()')
    
    if not workflow_file_path:
        raise PyFaaSWorkflowLoadingError("Missing required argument 'workflow_file_path'")
    
    try:
        with open(workflow_file_path) as content:
            json_workflow = json.load(content)
            return json_workflow
    except Exception as e:
        raise PyFaaSWorkflowLoadingError(f'Error while loading the workflow: {e}')

# TODO: problematic if functions are scattered across multiple workers. Trivial if all workers are synchronized.
def pyfaas_chain_exec(json_workflow: dict[str, dict[str, object]]):
    if not _CLIENT_MANAGER.configured:
        raise RuntimeError('Unable to execute PyFaaS operations: PyFaaS has not been configured with a call to pyfaas_config()')

    if not json_workflow:
        raise PyFaaSChainedExecutionError("Missing required argument 'json_workflow'")

    try:
        logger.debug('Validating workflow...')
        validate_json_workflow_structure(json_workflow)
    except PyFaaSWorkflowValidationError as e:
        logger.error(f'Error while validating the workflow: {e}')
        raise PyFaaSWorkflowValidationError(e)
    
    # If here, workflow is STRUCTURALLY valid, and can be passed to the worker
    logger.info(f'Provided workwlow is structurally valid')
    
    # Calling actual pyfaas_chain_exec() function from global object
    try:
        director_resp_json = _CLIENT_MANAGER.client.pyfaas_chain_exec(json_workflow)
    except zmq.Again:
        raise PyFaaSTimeoutError('Timeout while waiting for Director\'s response during a call to pyfaas_chain_exec()')

    workflow_id = json_workflow.get('id')
    status = director_resp_json.get('status')
    result = director_resp_json.get('result')
    message = director_resp_json.get('message')

    if status == 'ok':
        logger.info(f"Chain execution completed. Yielded: '{result}'")
        return result
    else:
        logger.error(f"Error while chain-executing workflow '{workflow_id}': {message}")
        raise PyFaaSChainedExecutionError(message)

def pyfaas_ping() -> None:
    '''
    Tests the PyFaaS cluster connectivity by sending a 'PING' message to an active Worker, and expecting a 'PONG' resonse message.
    
    Raises:
        RuntimeError: Raised if PyFaaS has not been configured with a call to pyfaas_config().
        PyFaaSTimeoutError: Raised if a timeout is reached while waiting from the Director's response.
        PyFaaSPingingError: Raised if anything goes wrong during the PING operation of the Worker.
    '''
    if not _CLIENT_MANAGER.configured:
        raise RuntimeError('Unable to execute PyFaaS operations: PyFaaS has not been configured with a call to pyfaas_config()')

    # Calling actual pyfaas_ping() function from global object
    try:
        director_resp_json = _CLIENT_MANAGER.client.pyfaas_ping()
    except zmq.Again:
        raise PyFaaSTimeoutError('Timeout while waiting for Director\'s response during a call to pyfaas_ping()')

    status = director_resp_json.get('status')
    result = director_resp_json.get('result')
    message = director_resp_json.get('message')

    if status == 'ok':
        logger.info(f"Worker says: '{result}'")
    else:
        logger.warning(f'Error while PING-ing the worker: {message}')
        raise PyFaaSPingingError(message)

def pyfaas_get_worker_ids() -> list[str]:
    '''
    Obtains a list of IDs of the currenly active workers.

    Returns:
        list[str]: A list of strings representing the IDs of the active workers.

    Raises:
        RuntimeError: Raised if PyFaaS has not been configured with a call to pyfaas_config().
        PyFaaSTimeoutError: Raised if a timeout is reached while waiting from the Director's response.
        PyFaaSWorkerIDsRetrievalError: Raised if anything goes wrong during the retrieval of the Worker IDs.
    '''
    if not _CLIENT_MANAGER.configured:
        raise RuntimeError('Unable to execute PyFaaS operations: PyFaaS has not been configured with a call to pyfaas_config()')

    # Calling actual pyfaas_get_worker_ids() function from global object
    try:
        director_resp_json = _CLIENT_MANAGER.client.pyfaas_get_worker_ids()
    except zmq.Again:
        raise PyFaaSTimeoutError('Timeout while waiting for Director\'s response during a call to pyfaas_get_worker_ids()')

    status = director_resp_json.get('status')
    result = director_resp_json.get('result')
    message = director_resp_json.get('message')

    if status == 'ok':
        logger.debug(f'Currently connected workers: {result}')
        return result
    else:
        logger.warning(f'Error while retrieving currently connected workers IDs: {message}')
        raise PyFaaSWorkerIDsRetrievalError(message)

# --- CLEANUP ---
atexit.register(pyfaas_close)
