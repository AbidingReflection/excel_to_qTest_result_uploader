from pprint import pformat

from config_env_initializer.config_loader import ConfigLoader

def load_config(config_path):
    loader = ConfigLoader(config_path)
    CONFIG = loader.config
    logger = CONFIG.get('logger')
    logger.info("Loaded CONFIG:\n%s", pformat(CONFIG, indent=4, width=120))
    return CONFIG
