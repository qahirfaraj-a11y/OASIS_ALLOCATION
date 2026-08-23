# The base module owns no business models — only the app menu, the settings
# shell, and the connection settings every feature module shares.
from . import models  # noqa: F401
