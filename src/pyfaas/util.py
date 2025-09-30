import logging
import tomli


def read_config_toml(path) -> dict:
    with open(path, mode="rb") as fp:
        return tomli.load(fp)

def setup_logging(log_level):
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
        case "error":
            log_level = logging.ERROR
        case "":
            log_level = None
        case _:
            log_level = logging.INFO
    logging.basicConfig(format='[PYFAAS, %(levelname)s]\t %(message)s', level=log_level, force=True)
