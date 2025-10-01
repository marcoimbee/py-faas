import os
import datetime


class FileLogger():
    def __init__(self, log_directory, log_file_path, worker_ip_addr, worker_port):
        self._log_directory = log_directory
        self._log_file_path = os.path.join(log_directory, log_file_path)
        self._worker_ip_addr = worker_ip_addr
        self._worker_port = worker_port
        
        os.makedirs(self._log_directory, exist_ok=True)

    def log(self, log_level: str, msg: str) -> None | str:
        timestamp = str(datetime.datetime.now())
        log_line = f"[{timestamp}, {self._worker_ip_addr}:{self._worker_port}]\t{log_level}\t{msg}\n"
        try:
            with open(self._log_file_path, "a") as log_file:
                log_file.write(log_line)
        except Exception as e:
            raise Exception(e)
        