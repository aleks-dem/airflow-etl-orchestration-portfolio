from pathlib import Path

from airflow.models import Variable
from yaml import safe_load


def _resolve_config_path() -> Path:
    base_dir = Variable.get("etl_directory", default_var=str(Path(__file__).resolve().parents[1]))
    return Path(base_dir) / "utils" / "config.yaml"


def get_config(key1, key2=None):
    config_path = _resolve_config_path()
    with open(config_path, "r", encoding="utf-8") as config_file:
        config = safe_load(config_file) or {}
    if key1 not in config:
        raise KeyError(f"Config section '{key1}' not found in {config_path}")
    if key2 is not None:
        if key2 not in config[key1]:
            raise KeyError(f"Config key '{key1}.{key2}' not found in {config_path}")
        return config[key1][key2]
    return config[key1]


