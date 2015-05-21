

class CondorpyBaseException(Exception):
    pass

class NoExecutable(CondorpyBaseException):
    pass

class CircularDependency(CondorpyBaseException):
    pass

class HTCondorError(CondorpyBaseException):
    pass

class RemoteError(CondorpyBaseException):
    pass