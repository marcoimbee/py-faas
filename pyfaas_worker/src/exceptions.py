class WorkerError(Exception):
    pass

class WorkerWorkflowValidationError(WorkerError):
    pass

class WorkerChainedExecutionError(WorkerError):
    pass

class WorkerFunctionCacheError(WorkerError):
    pass

class WorkerFunctionExecutionError(WorkerError):
    pass
