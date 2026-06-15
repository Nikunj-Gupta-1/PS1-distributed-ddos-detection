import logging
import logging.config
import yaml
from pathlib import Path


def setup_logging(cfg_path: str = None):
    """
    Configure logging from YAML file (fallback to basicConfig).
    """
    if cfg_path and Path(cfg_path).exists():
        with open(cfg_path, "r") as fp:
            config = yaml.safe_load(fp)
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        )

