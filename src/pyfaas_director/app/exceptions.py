class DirectorError(Exception):
    pass

class DirectorConfigError(DirectorError):
    pass

class DirectorNoAvailableWorkersError(DirectorError):
    pass
