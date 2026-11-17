import sys
from pathlib import Path
from types import ModuleType

# Stub homeassistant so const.py imports without the full HA package.
ha = ModuleType("homeassistant")
ha_const = ModuleType("homeassistant.const")
ha_const.CONF_EMAIL = "email"
ha_const.CONF_PASSWORD = "password"
sys.modules.setdefault("homeassistant", ha)
sys.modules.setdefault("homeassistant.const", ha_const)

ROOT = Path(__file__).resolve().parents[1]
PACKAGE_DIR = ROOT / "custom_components" / "molekule"
sys.path.insert(0, str(PACKAGE_DIR))

# Lets api.py's relative imports resolve without running __init__.py, which needs HA.
molekule = ModuleType("molekule")
molekule.__path__ = [str(PACKAGE_DIR)]
sys.modules.setdefault("molekule", molekule)
