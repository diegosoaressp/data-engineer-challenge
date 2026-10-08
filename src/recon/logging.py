import json
import logging
import sys
from datetime import UTC, datetime

from recon import config

_CONTEXT_FIELDS = ("run_id", "reference_date")


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "environment": config.ENVIRONMENT,
        }
        for field in _CONTEXT_FIELDS:
            payload[field] = getattr(record, field, None)
        extra = getattr(record, "extra_fields", None)
        if extra:
            payload.update(extra)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


class RunLogger(logging.LoggerAdapter):
    """Injects run_id and reference_date into every record."""

    def process(self, msg, kwargs):
        extra = kwargs.pop("extra", None)
        kwargs["extra"] = {
            "run_id": self.extra["run_id"],
            "reference_date": self.extra["reference_date"],
            "extra_fields": extra or {},
        }
        return msg, kwargs


def get_logger(run_id: str, reference_date: str | None, stream=None) -> RunLogger:
    logger = logging.getLogger("recon")
    logger.setLevel(config.LOG_LEVEL.upper())
    logger.propagate = False
    logger.handlers.clear()
    handler = logging.StreamHandler(stream or sys.stderr)
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
    return RunLogger(logger, {"run_id": run_id, "reference_date": reference_date})
