import logging
import os
from datetime import timedelta
from typing import Optional

from cachelib.file import FileSystemCache
from celery.schedules import crontab

logger = logging.getLogger()


def get_env_variable(var_name: str, default: Optional[str] = None) -> str:
    """Get the environment variable or raise exception."""
    try:
        return os.environ[var_name]
    except KeyError:
        if default is not None:
            return default
        else:
            error_msg = "The environment variable {} was missing, abort...".format(
                var_name
            )
            raise EnvironmentError(error_msg)


# DB CONFIG


DATABASE_DIALECT = get_env_variable("DATABASE_DIALECT")
DATABASE_USER = get_env_variable("DATABASE_USER")
DATABASE_PASSWORD = get_env_variable("DATABASE_PASSWORD")
DATABASE_HOST = get_env_variable("DATABASE_HOST")
DATABASE_PORT = get_env_variable("DATABASE_PORT")
DATABASE_DB = get_env_variable("DATABASE_DB")

# MAPBOX CONFIG


MAPBOX_API_KEY = get_env_variable("MAPBOX_API_KEY", "")

# ALERTS CONFIG

ALERT_REPORTS_NOTIFICATION_DRY_RUN = False

WEBDRIVER_TYPE = get_env_variable("WEBDRIVER_TYPE", "chrome")
WEBDRIVER_BASEURL = get_env_variable("WEBDRIVER_BASEURL", "http://superset:8088")

# UI CONFIG

APP_NAME = get_env_variable("APP_NAME", "")

# The SQLAlchemy connection string.
SQLALCHEMY_DATABASE_URI = "%s://%s:%s@%s:%s/%s" % (
    DATABASE_DIALECT,
    DATABASE_USER,
    DATABASE_PASSWORD,
    DATABASE_HOST,
    DATABASE_PORT,
    DATABASE_DB,
)

REDIS_HOST = get_env_variable("REDIS_HOST")
REDIS_PORT = get_env_variable("REDIS_PORT")
REDIS_CELERY_DB = get_env_variable("REDIS_CELERY_DB", "0")
REDIS_RESULTS_DB = get_env_variable("REDIS_RESULTS_DB", "1")

RESULTS_BACKEND = FileSystemCache("/app/superset_home/sqllab")

CACHE_CONFIG = {
    "CACHE_TYPE": "redis",
    "CACHE_DEFAULT_TIMEOUT": 86400,
    "CACHE_KEY_PREFIX": "superset_",
    "CACHE_REDIS_HOST": REDIS_HOST,
    "CACHE_REDIS_PORT": REDIS_PORT,
    "CACHE_REDIS_DB": REDIS_RESULTS_DB,
}
DATA_CACHE_CONFIG = CACHE_CONFIG

DEFAULT_FEATURE_FLAGS = {
    'DASHBOARD_CROSS_FILTERS': get_env_variable("DASHBOARD_CROSS_FILTERS", "False"),
    'DASHBOARD_NATIVE_FILTERS': get_env_variable("DASHBOARD_NATIVE_FILTERS", "False"),
    "ENABLE_JAVASCRIPT_CONTROLS": get_env_variable("ENABLE_JAVASCRIPT_CONTROLS", "False"),
    "ENABLE_TEMPLATE_PROCESSING": get_env_variable("ENABLE_TEMPLATE_PROCESSING", "False"),
    "ENABLE_SCHEDULED_EMAIL_REPORTS": get_env_variable("ENABLE_SCHEDULED_EMAIL_REPORTS", "False")
}

FEATURE_FLAGS = {
    "ALERT_REPORTS": get_env_variable("ALERT_REPORTS", "True"),
    "GENERIC_CHART_AXES": get_env_variable("GENERIC_CHART_AXES", "False"),
    "EMBEDDED_SUPERSET": get_env_variable("EMBEDDED_SUPERSET", "False"),
    "VERSIONED_EXPORT": get_env_variable("VERSIONED_EXPORT", "False"),
}


class CeleryConfig(object):
    broker_url = f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_CELERY_DB}"
    imports = ("superset.sql_lab", 'superset.tasks')
    result_backend = f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_RESULTS_DB}"
    worker_prefetch_multiplier = 10
    task_acks_late = True
    task_annotations = {
        "sql_lab.get_sql_results": {"rate_limit": "100/s"},
        "email_reports.send": {
            "rate_limit": "1/s",
            "time_limit": int(timedelta(seconds=120).total_seconds()),
            "soft_time_limit": int(timedelta(seconds=150).total_seconds()),
            "ignore_result": True,
        },
    }
    beat_schedule = {
        'cache-warmup-hourly': {
            'task': 'cache-warmup',
            'schedule': crontab(minute=0, hour='*'),  # hourly
            'kwargs': {
                'strategy_name': 'top_n_dashboards',
                'top_n': 5,
                'since': '7 days ago',
            },
        },
        'email_reports.schedule_hourly': {
            'task': 'email_reports.schedule_hourly',
            'schedule': crontab(minute=1, hour='*'),
        },
        'reports.scheduler': {
            'task': 'reports.scheduler',
            'schedule': crontab(minute='*', hour='*'),
        },
        'reports.prune_log': {
            'task': 'reports.prune_log',
            'schedule': crontab(minute=0, hour=0),
        },
    }


CELERY_TASK_PROTOCOL = 1
CELERY_CONFIG = CeleryConfig
SCREENSHOT_LOCATE_WAIT = 100
SCREENSHOT_LOAD_WAIT = 600

# The base URL for the email report hyperlinks.
WEBDRIVER_OPTION_ARGS = [
    "--force-device-scale-factor=2.0",
    "--high-dpi-support=2.0",
    "--headless",
    "--disable-gpu",
    "--disable-dev-shm-usage",
    "--no-sandbox",
    "--disable-setuid-sandbox",
    "--disable-extensions",
]
WEBDRIVER_BASEURL_USER_FRIENDLY = WEBDRIVER_BASEURL

SQLLAB_CTAS_NO_LIMIT = True

#
# Optionally import superset_config_docker.py (which will have been included on
# the PYTHONPATH) in order to allow for local settings to be overridden
#
try:
    import superset_config_docker
    from superset_config_docker import *  # noqa

    logger.info(
        f"Loaded your Docker configuration at " f"[{superset_config_docker.__file__}]"
    )
except ImportError:
    logger.info("Using default Docker config...")
