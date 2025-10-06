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
