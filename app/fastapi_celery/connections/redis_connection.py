import redis
from redis.exceptions import RedisError
import config_loader
from utils import log_helpers
from models.tracking_models import ServiceLog, LogType

# === Set up logging ===
logger = log_helpers.get_logger("Redis Connection")


class RedisConnector:
    """Redis connector for storing and retrieving workflow data."""

    def __init__(self):
        self.redis_client = redis.Redis(
            host=config_loader.get_env_variable("REDIS_HOST", "localhost"),
            port=config_loader.get_env_variable("REDIS_PORT", 6379),
            password=None,
            db=0,
            decode_responses=True,
        )

    def store_step_status(
        self,
        task_id: str,
        step_name: str,
        status: str,
        step_id: str | None = None,
        ttl: int = 3600,
    ) -> bool:
        """Store step status (and optional step_id) for a task."""
        status_key = f"task:{task_id}:step_statuses"
        ids_key = f"task:{task_id}:step_ids"
        try:
            self.redis_client.hset(status_key, step_name, status)
            if step_id:
                self.redis_client.hset(ids_key, step_name, step_id)
            self.redis_client.expire(status_key, ttl)
            self.redis_client.expire(ids_key, ttl)
            logger.info(
                "[Redis] Stored step status successfully",
                extra={
                    "service": ServiceLog.REDIS_SERVICE,
                    "log_type": LogType.ACCESS,
                    "task_id": task_id,
                    "step_name": step_name,
                    "status": status,
                    "step_id": step_id,
                    "ttl": ttl,
                },
            )
            return True
        except RedisError as e:
            logger.error(
                "[Redis] Failed to store step status",
                exc_info=True,
                extra={
                    "service": ServiceLog.REDIS_SERVICE,
                    "log_type": LogType.ERROR,
                    "task_id": task_id,
                    "step_name": step_name,
                    "status": status,
                    "step_id": step_id,
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                },
            )
            return False

    def get_all_step_status(self, task_id: str) -> dict[str, str]:
        """Get all step statuses for a task."""
        key = f"task:{task_id}:step_statuses"
        try:
            data = self.redis_client.hgetall(key)
            logger.info(
                "[Redis] Retrieved all step statuses successfully",
                extra={
                    "service": ServiceLog.REDIS_SERVICE,
                    "log_type": LogType.ACCESS,
                    "task_id": task_id,
                    "redis_key": key,
                    "step_count": len(data),
                },
            )
            return data
        except RedisError as e:
            logger.error(
                "[Redis] Failed to fetch all step statuses",
                exc_info=True,
                extra={
                    "service": ServiceLog.REDIS_SERVICE,
                    "log_type": LogType.ERROR,
                    "task_id": task_id,
                    "redis_key": key,
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                },
            )
            return {}

    def get_step_ids(self, task_id: str) -> dict[str, str]:
        """Get all step IDs for a task."""
        key = f"task:{task_id}:step_ids"
        try:
            data = self.redis_client.hgetall(key)
            logger.info(
                "[Redis] Retrieved all step IDs successfully",
                extra={
                    "service": ServiceLog.REDIS_SERVICE,
                    "log_type": LogType.ACCESS,
                    "task_id": task_id,
                    "redis_key": key,
                    "step_count": len(data),
                },
            )
            return data
        except RedisError as e:
            logger.error(
                "[Redis] Failed to fetch step IDs",
                exc_info=True,
                extra={
                    "service": ServiceLog.REDIS_SERVICE,
                    "log_type": LogType.ERROR,
                    "task_id": task_id,
                    "redis_key": key,
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                },
            )
            return {}

    def store_workflow_id(
        self, task_id: str, workflow_id: str, status: str, ttl: int = 3600
    ) -> bool:
        """Store workflow_id and status with TTL."""
        key = f"task:{task_id}:workflow_id"
        try:
            self.redis_client.hset(key, workflow_id, status)
            self.redis_client.expire(key, ttl)
            logger.info(
                "[Redis] Stored workflow_id in Redis successfully",
                extra={
                    "service": ServiceLog.REDIS_SERVICE,
                    "log_type": LogType.ACCESS,
                    "task_id": task_id,
                    "workflow_id": workflow_id,
                    "status": status,
                    "redis_key": key,
                    "ttl_seconds": ttl,
                },
            )
            return True
        except RedisError as e:
            logger.error(
                "[Redis] Failed to store workflow_id in Redis",
                exc_info=True,
                extra={
                    "service": ServiceLog.REDIS_SERVICE,
                    "log_type": LogType.ERROR,
                    "task_id": task_id,
                    "workflow_id": workflow_id,
                    "status": status,
                    "redis_key": key,
                    "ttl_seconds": ttl,
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                },
            )
            return False

    def get_workflow_id(self, task_id: str) -> dict[str, str] | None:
        """Get workflow_id and status for a task."""
        key = f"task:{task_id}:workflow_id"
        try:
            data = self.redis_client.hgetall(key)
            if not data:
                logger.info(
                    "[Redis] No workflow_id found",
                    extra={
                        "service": ServiceLog.REDIS_SERVICE,
                        "log_type": LogType.ACCESS,
                        "task_id": task_id,
                        "redis_key": key,
                    },
                )
                return None
            workflow_id, status = next(iter(data.items()))
            logger.info(
                "[Redis] Retrieved workflow_id successfully",
                extra={
                    "service": ServiceLog.REDIS_SERVICE,
                    "log_type": LogType.ACCESS,
                    "task_id": task_id,
                    "workflow_id": workflow_id,
                    "status": status,
                },
            )
            return {"workflow_id": workflow_id, "status": status}
        except RedisError as e:
            logger.error(
                "[Redis] Failed to fetch workflow_id",
                exc_info=True,
                extra={
                    "service": ServiceLog.REDIS_SERVICE,
                    "log_type": LogType.ERROR,
                    "task_id": task_id,
                    "redis_key": key,
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                },
            )
            return None

    def store_jwt_token(self, token: str, ttl: int) -> bool:
        """Store JWT token with TTL."""
        try:
            self.redis_client.set("jwt_token", token, ex=ttl)
            logger.info("[Redis] Stored JWT token successfully")
            return True
        except RedisError as e:
            logger.error(f"[Redis] Failed to store JWT token: {e}")
            return False

    def get_jwt_token(self) -> str | None:
        """Retrieve JWT token from Redis."""
        try:
            token = self.redis_client.get("jwt_token")
            if token:
                logger.info("[Redis] Retrieved JWT token successfully")
            return token
        except RedisError as e:
            logger.error(f"[Redis] Failed to retrieve JWT token: {e}")
            return None
