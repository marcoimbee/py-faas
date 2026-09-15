class DirectorError(Exception):
    pass

class DirectorConfigError(DirectorError):
    pass

class DirectorNoAvailableWorkersError(DirectorError):
    pass

class DirectorCleanupError(DirectorError):
    pass

class DirectorWorkflowValidationError(DirectorError):
    pass
