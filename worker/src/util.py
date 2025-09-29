import tomli
import logging


def read_config_toml(path: str) -> dict:
    with open(path, mode="rb") as fp:
        return tomli.load(fp)
    
def setup_logging(log_level: str) -> None:
    match log_level:
        case "info":
            log_level = logging.INFO
        case "debug":
            log_level = logging.DEBUG
        case "warning":
            log_level = logging.WARNING
        case "critical":
            log_level = logging.CRITICAL
        case "fatal":
            log_level = logging.FATAL
        case _:
            log_level = logging.INFO
    logging.basicConfig(format='[WORKER, %(levelname)s]\t %(message)s', level=log_level)
