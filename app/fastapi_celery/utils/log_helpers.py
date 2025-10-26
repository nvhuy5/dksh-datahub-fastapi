import os
import dataclasses
import logging
import logging.config
from typing import Any, Dict
from prometheus_client import Enum
from pydantic import BaseModel
from models.tracking_models import LogType, ServiceLog
import config_loader


# =========================
# Global constants
# =========================
LOG_COLORS = {
    "DEBUG": "cyan",
    "INFO": "green",
    "WARNING": "yellow",
    "ERROR": "red",
    "CRITICAL": "bold_red",
}
EXCLUDED_FIELDS = [
    "log.original",
    "process",
    "log.origin",
]

# Determine environment and log level
ENV = config_loader.get_config_value("environment", "env") or "dev"
LOG_LEVEL = os.getenv("LOG_LEVEL", "DEBUG" if ENV == "dev" else "INFO")


# =========================
# Logging Configuration
# =========================
def logging_config(logger_name: str) -> None:
    """
    Configure a logger with ECS console handler.
    """
    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "ecs": {
                    "()": "ecs_logging.StdlibFormatter",
                    "exclude_fields": EXCLUDED_FIELDS,
                },
            },
            "handlers": {
                "console": {
                    "level": "INFO",
                    "class": "logging.StreamHandler",
                    "formatter": "ecs",
                }
            },
            "loggers": {
                f"{logger_name}": {
                    "level": LOG_LEVEL,
                    "handlers": ["console"],
                    "propagate": False,
                },
            },
            "root": {
                "level": LOG_LEVEL,
                "handlers": ["console"],
            },
        }
    )


# =========================
# Custom Logger Adapter
# =========================
class ValidatingLoggerAdapter(logging.LoggerAdapter):
    """
    Enhanced logger adapter:
    - Validates and normalizes 'service' and 'log_type' fields.
    - Automatically converts Pydantic models, Enums, and custom objects in `extra`.
    """

    def validate_log_fields(self, extra: Dict[str, Any]) -> Dict[str, Any]:
        if "service" in extra:
            service = extra["service"]
            if not isinstance(service, ServiceLog):
                if service in ServiceLog._value2member_map_:
                    service = ServiceLog(service)
                else:
                    raise ValueError(f"Invalid service log value: {service}")
            extra["service"] = str(service)

        if "log_type" in extra:
            log_type = extra["log_type"]
            if not isinstance(log_type, LogType):
                if log_type in LogType._value2member_map_:
                    log_type = LogType(log_type)
                else:
                    raise ValueError(f"Invalid log type value: {log_type}")
            extra["log_type"] = str(log_type)

        return extra

    def _normalize_extra(self, extra: dict) -> dict:
        """Convert complex objects in extra to safe, serializable forms."""
        normalized = {}
        for key, value in extra.items():
            try:
                # --- Pydantic (v2/v1) ---
                if hasattr(value, "model_dump"):  # Pydantic v2
                    normalized[key] = value.model_dump()
                elif hasattr(value, "dict"):  # Pydantic v1
                    normalized[key] = value.dict()

                # --- Dataclass ---
                elif dataclasses.is_dataclass(value):
                    normalized[key] = dataclasses.asdict(value)

                # --- Enum ---
                elif isinstance(value, Enum):
                    normalized[key] = str(value)

                # --- Common JSON-friendly types ---
                elif isinstance(value, (dict, list, tuple, str, int, float, bool, type(None))):
                    normalized[key] = value

                # --- Fallback: try string conversion ---
                else:
                    normalized[key] = str(value)

            except Exception:
                normalized[key] = f"<Unserializable: {type(value).__name__}>"
        return normalized

    def process(self, msg, kwargs):
        extra = kwargs.get("extra") or {}
        if not isinstance(extra, dict):
            extra = {}

        try:
            # validate ECS fields first
            extra = self.validate_log_fields(extra)
            # then normalize data fields
            extra = self.normalize_extra(extra)
            # always include environment info
            extra.setdefault("environment", ENV)
        except Exception as e:
            logging.getLogger(__name__).debug(f"Log field validation failed: {e}")

        kwargs["extra"] = extra
        return msg, kwargs


# =========================
# Helper function
# =========================
def get_logger(name: str) -> ValidatingLoggerAdapter:
    """
    Returns a ready-to-use logger with automatic validation and normalization.

    Example:
        logger = log_helpers.get_logger("Celery Task Execution")
        logger.info("Task started", extra={"service": ServiceLog.TASK_EXECUTION})
    """
    logging_config(name)
    base_logger = logging.getLogger(name)
    return ValidatingLoggerAdapter(base_logger, {})
