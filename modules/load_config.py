from pprint import pformat

from config_env_initializer.config_loader import ConfigLoader

def load_config(config_path):
    """Load configuration from the given path and log it."""
    loader = ConfigLoader(config_path)
    CONFIG = loader.config

    # Log the loaded configuration in a pretty-printed format
    logger = CONFIG.get('logger')
    logger.info("Loaded CONFIG:\n%s", pformat(CONFIG, indent=4, width=120))

    return CONFIG