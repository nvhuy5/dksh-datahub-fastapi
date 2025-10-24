#!/bin/sh

LOG_DIR="/tmp/fastapi_uvicorn_logs"
LOG_DATE=$(date +%F)
LOG_PATH="$LOG_DIR/${ENVIRONMENT}_fastapi_uvicorn_${LOG_DATE}.log"

# mkdir -p "$LOG_DIR"

# Create a dynamic ECS-compliant log config
cat > /app/uvicorn_log_config.yaml <<EOF
version: 1
disable_existing_loggers: False
formatters:
  ecs:
    class: ecs_logging.StdlibFormatter
handlers:
  console:
    class: logging.StreamHandler
    formatter: ecs

loggers:
  uvicorn.error:
    level: ERROR
    handlers: [console]
    propagate: False
EOF

echo "Starting FastAPI with ECS-formatted logs!"

# Start Uvicorn
/usr/local/bin/wait-for-it "$REDIS_HOST:$REDIS_PORT" -- \
  uvicorn main:app \
    --host 0.0.0.0 \
    --port "${APP_PORT:-8000}" \
    --log-config /app/uvicorn_log_config.yaml \
    --reload
