"""Exceptions, split by what they mean for a run.

Everything derived from MiniEtlError aborts the run (the setup is wrong, so no
row can be trusted). RowError is the opposite: it rejects one row and the run
carries on. Keeping the two families apart is what makes "reject a bad row
instead of aborting" a property of the types, not of scattered try/except.
"""


class MiniEtlError(Exception):
    """Base class for problems that abort a run."""


class ConfigError(MiniEtlError):
    """The configuration is malformed or refers to columns that do not exist."""


class SourceError(MiniEtlError):
    """A CSV input cannot be read as a table (missing, empty, undecodable...)."""


class LoadError(MiniEtlError):
    """The SQLite target cannot be prepared or written."""


class RowError(Exception):
    """One row cannot pass a step. The row is rejected; the run continues.

    ``stage`` names where it happened (e.g. "step 2 (cast)"); the pipeline fills
    it in when the step did not.
    """

    def __init__(self, reason: str, stage: str = "") -> None:
        super().__init__(reason)
        self.reason = reason
        self.stage = stage
