import tomli
import logging
import socket

from pyfaas_worker.app.exceptions import WorkerConfigError


def read_config_toml(path: str) -> dict:
    with open(path, mode='rb') as fp:
        config = tomli.load(fp)
    
    # Checking director IP addr validity
    try:
        socket.inet_aton(config['network']['director_ip_addr'])
    except socket.error:
        raise WorkerConfigError(f"Config error: invalid field value for 'director_ip_addr': {config['network']['director_ip_addr']} is not a valid IP address")

    # Checking director port validity
    if config['network']['director_port'] is None or config['network']['director_port'] <= 1024 or config['network']['director_port'] >= 65535:
        raise WorkerConfigError(f"Config error: invalid field value for 'director_port': {config['network']['director_port']}")
    
    # Checking caching options validity
    available_policies = ['LRU']
    if config['behavior']['caching']['policy'] not in available_policies:
        raise WorkerConfigError(f"Config error: unknown caching policy '{config['behavior']['caching']['policy']}'")
    if config['behavior']['caching']['max_size'] < 0:
        raise WorkerConfigError(f"Config error: invalid cache max size {config['behavior']['caching']['max_size']}")

    # Checking heartbeat interval
    if config['network']['heartbeat_interval_ms'] is None or config['network']['heartbeat_interval_ms'] <= 0:
        raise WorkerConfigError(f"Config error: invalid field value for 'heartbeat_interval_ms'. A positive integer is needed, {config['network']['heartbeat_interval_ms']} was provided")

    # Checking shutdown persistence fields
    if config['behavior']['shutdown_persistence'] is True and config['behavior']['dump_file'] is None:
        raise WorkerConfigError(f"Config error: field 'shutdown_persistence' set to true but no field 'dump_file' was specified")
    
    # # TODO: test
    # # Checking execution limits fields
    # if config['behavior']['exec_limits']['cpu_time_limit'] is not None and config['behavior']['exec_limits']['cpu_time_limit'] <= 0:
    #     raise WorkerConfigError(f"Config error: invalid value filed value for 'cpu_time_limit'. A positive integer is needed, {config['behavior']['exec_limits']['cpu_time_limit']} was provided")
    # if config['behavior']['exec_limits']['cpu_time_limit'] is None:
    #     config['behavior']['exec_limits']['cpu_time_limit'] = 0             # Will be set to max in Worker
    # if config['behavior']['exec_limits']['address_space_limit_mb'] is not None and config['behavior']['exec_limits']['address_space_limit_mb'] <= 0:
    #     raise WorkerConfigError(f"Config error: invalid value filed value for 'address_space_limit_mb'. A positive integer is needed, {config['behavior']['exec_limits']['address_space_limit_mb']} was provided")
    # if config['behavior']['exec_limits']['address_space_limit_mb'] is None:
    #     config['behavior']['exec_limits']['address_space_limit_mb'] = 0     # Will be set to max in Worker

    return config

def setup_logging(log_level: str) -> None:
    match log_level:
        case 'info':
            log_level = logging.INFO
        case 'debug':
            log_level = logging.DEBUG
        case 'warning':
            log_level = logging.WARNING
        case 'critical':
            log_level = logging.CRITICAL
        case 'fatal':
            log_level = logging.FATAL
        case 'error':
            log_level = logging.ERROR
        case _:
            log_level = logging.INFO
    logging.basicConfig(format='[WORKER, %(levelname)s]\t %(message)s', level=log_level, force=True)
