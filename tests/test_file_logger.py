import pytest

from pyfaas_director.app.util.file_logger import FileLogger as DirectorFileLogger
from pyfaas_worker.app.util.file_logger import FileLogger as WorkerFileLogger


def test_director_file_logger_creates_log_directory(tmp_path):
    log_dir = tmp_path / 'nested' / 'logs'
    DirectorFileLogger(str(log_dir), 'log.txt', '127.0.0.1', 40000)
    assert log_dir.is_dir()


def test_director_file_logger_log_appends_line(tmp_path):
    logger = DirectorFileLogger(str(tmp_path), 'log.txt', '127.0.0.1', 40000)

    logger.log('INFO', 'hello world')
    logger.log('ERROR', 'something broke')

    lines = (tmp_path / 'log.txt').read_text().splitlines()
    assert len(lines) == 2
    assert 'INFO' in lines[0] and 'hello world' in lines[0] and '127.0.0.1:40000' in lines[0]
    assert 'ERROR' in lines[1] and 'something broke' in lines[1]


def test_director_file_logger_log_wraps_write_failures(tmp_path):
    logger = DirectorFileLogger(str(tmp_path), 'log.txt', '127.0.0.1', 40000)
    logger._log_file_path = str(tmp_path)  # a directory, not a file -- open() for append will fail

    with pytest.raises(Exception):
        logger.log('INFO', 'this will fail to write')


def test_worker_file_logger_creates_log_directory(tmp_path):
    log_dir = tmp_path / 'nested' / 'logs'
    WorkerFileLogger(str(log_dir), 'log.txt', 'worker-1')
    assert log_dir.is_dir()


def test_worker_file_logger_log_appends_line(tmp_path):
    logger = WorkerFileLogger(str(tmp_path), 'log.txt', 'worker-1')

    logger.log('INFO', 'hello world')
    logger.log('ERROR', 'something broke')

    lines = (tmp_path / 'log.txt').read_text().splitlines()
    assert len(lines) == 2
    assert 'INFO' in lines[0] and 'hello world' in lines[0] and 'worker-1' in lines[0]
    assert 'ERROR' in lines[1] and 'something broke' in lines[1]


def test_worker_file_logger_log_wraps_write_failures(tmp_path):
    logger = WorkerFileLogger(str(tmp_path), 'log.txt', 'worker-1')
    logger._log_file_path = str(tmp_path)  # a directory, not a file -- open() for append will fail

    with pytest.raises(Exception):
        logger.log('INFO', 'this will fail to write')
