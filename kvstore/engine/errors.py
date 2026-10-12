class EngineError(Exception):
    pass


class NotFoundError(EngineError):
    def __init__(self, msg: str = "engine: key not found"):
        super().__init__(msg)


class EmptyKeyError(EngineError):
    def __init__(self, msg: str = "engine: empty key"):
        super().__init__(msg)


class ClosedError(EngineError):
    def __init__(self, msg: str = "engine: closed"):
        super().__init__(msg)


class CorruptError(EngineError):
    def __init__(self, msg: str = "engine: corrupt data"):
        super().__init__(msg)


class AbortedError(EngineError):
    def __init__(self, msg: str = "engine: compaction aborted"):
        super().__init__(msg)
