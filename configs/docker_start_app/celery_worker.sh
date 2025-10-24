#!/bin/sh

/usr/local/bin/wait-for-it "$REDIS_HOST:$REDIS_PORT" -- \
    celery -A celery_worker worker -E --loglevel=info \
    --concurrency=$CELERY_CONCURRENCY \
    --pool=$CELERY_POOL \
    --max-tasks-per-child=$CELERY_MAX_TASKS_PER_CHILD \
    --max-memory-per-child=$CELERY_MAX_MEMORY_PER_CHILD
