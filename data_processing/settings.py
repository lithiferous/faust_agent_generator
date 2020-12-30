import os
SIMPLE_SETTINGS = {
    'OVERRIDE_BY_ENV': True,
    'CONFIGURE_LOGGING': True,
    'REQUIRED_SETTINGS': ('KAFKA_BOOTSTRAP_SERVER', ),
}

# The following variables can be ovirriden from ENV

if os.environ.get("DEV"):
    APP_ID = "cluster-1"
else:
    APP_ID = "test"
KAFKA_BOOTSTRAP_SERVER = "kafka://kafka:9092"
SCHEMA_REGISTRY_URL = "http://schema-registry:8081"

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'default': {
            'format': '%(asctime)s %(levelname)s %(name)s %(message)s',
        },
    },
    'handlers': {
        'console': {
            'level': 'INFO',
            'class': 'logging.StreamHandler',
            'formatter': 'default',
        },
    },
    'loggers': {
        'processing_module': {
            'handlers': ['console'],
            'level': 'INFO',
        },
    },
}
