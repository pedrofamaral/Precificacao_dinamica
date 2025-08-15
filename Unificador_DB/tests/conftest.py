import os
import sys
from pathlib import Path
import contextlib
import copy
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

@contextlib.contextmanager
def temp_config(module):
    cfg_backup = copy.deepcopy(module.CONFIG)
    try:
        yield module.CONFIG
    finally:
        module.CONFIG.clear()
        module.CONFIG.update(cfg_backup)

@pytest.fixture
def cfg():
    # Uso: with cfg(um) as CONFIG: ...
    return temp_config
