# Third-Party Imports
from celery import Celery
import config_loader

celery_app = Celery("File Processor")

# Celery configuration
redis_url = (
    f"redis://"
    f"{config_loader.get_env_variable('REDIS_HOST')}"
    f":{config_loader.get_env_variable('REDIS_PORT')}/0"
)

celery_app.conf.update(
    broker_url=redis_url,
    result_backend=redis_url,
    broker_connection_retry_on_startup=True,
    task_track_started=True,
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    task_serializer="json",
    worker_prefetch_multiplier=3,
    task_acks_late=True,
    result_expires=3600,
    task_soft_time_limit=300,
    task_time_limit=600,
    task_default_retry_delay=5,
    task_max_retries=3,
    task_reject_on_worker_lost=True,
    worker_hijack_root_logger=False,
    task_retry_backoff=True,
    task_retry_backoff_max=30,
    task_retry_jitter=True,
)

celery_app.autodiscover_tasks(["celery_worker"])
