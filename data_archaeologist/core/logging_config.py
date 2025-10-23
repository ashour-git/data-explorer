"""
Professional logging configuration for Data Archaeologist framework
"""

import logging
import logging.config
from pathlib import Path

# Professional logging configuration
LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S'
        },
        'detailed': {
            'format': '%(asctime)s [%(levelname)s] %(name)s:%(lineno)d: %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S'
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'level': 'INFO',
            'formatter': 'standard',
            'stream': 'ext://sys.stdout'
        },
        'file': {
            'class': 'logging.handlers.RotatingFileHandler',
            'level': 'DEBUG',
            'formatter': 'detailed',
            'filename': 'data_archaeologist.log',
            'maxBytes': 10485760,  # 10MB
            'backupCount': 5
        },
        'error_file': {
            'class': 'logging.handlers.RotatingFileHandler',
            'level': 'ERROR',
            'formatter': 'detailed',
            'filename': 'data_archaeologist_errors.log',
            'maxBytes': 10485760,  # 10MB
            'backupCount': 5
        }
    },
    'loggers': {
        'data_archaeologist': {
            'level': 'DEBUG',
            'handlers': ['console', 'file', 'error_file'],
            'propagate': False
        }
    },
    'root': {
        'level': 'INFO',
        'handlers': ['console', 'file']
    }
}

def setup_professional_logging(config_dict: dict = None) -> None:
    """
    Setup professional logging configuration for the framework.
    
    Args:
        config_dict: Optional custom logging configuration
    """
    if config_dict is None:
        config_dict = LOGGING_CONFIG
    
    # Create logs directory if it doesn't exist
    logs_dir = Path('logs')
    logs_dir.mkdir(exist_ok=True)
    
    # Update file paths to use logs directory
    for handler_name, handler_config in config_dict['handlers'].items():
        if 'filename' in handler_config:
            handler_config['filename'] = str(logs_dir / handler_config['filename'])
    
    logging.config.dictConfig(config_dict)
    
    # Log startup message
    logger = logging.getLogger('data_archaeologist')
    logger.info("Data Archaeologist framework logging initialized")
