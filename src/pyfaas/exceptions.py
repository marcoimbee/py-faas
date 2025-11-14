class PyFaaSError(Exception):
    pass

class PyFaaSConfigError(PyFaaSError):
    pass

class PyFaaSNetworkError(PyFaaSError):
    pass

class PyFaaSFunctionRegistrationError(PyFaaSError):
    pass

class PyFaaSFunctionUnregistrationError(PyFaaSError):
    pass

class PyFaaSFunctionListingError(PyFaaSError):
    pass

class PyFaaSFunctionExecutionError(PyFaaSError):
    pass

class PyFaaSWorkerInfoError(PyFaaSError):
    pass

class PyFaaSCacheDumpingError(PyFaaSError):
    pass

class PyFaaSStatisticsRetrievalError(PyFaaSError):
    pass

class PyFaaSFunctionSetRegistrationCheckError(PyFaaSError):
    pass

class PyFaaSWorkflowValidationError(PyFaaSError):
    pass

class PyFaaSTimeoutError(PyFaaSError):
    pass

class PyFaaSDeserializationError(PyFaaSError):
    pass

class PyFaaSParameterMismatchError(PyFaaSError):
    pass

class PyFaaSWorkflowLoadingError(PyFaaSError):
    pass

class PyFaaSPingingError(PyFaaSError):
    pass

class PyFaaSWorkerIDsRetrievalError(PyFaaSError):
    pass

class PyFaaSChainedExecutionError(PyFaaSError):
    pass
