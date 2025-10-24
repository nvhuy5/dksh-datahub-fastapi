# Standard Library Imports
import logging
import traceback
from typing import Optional, Dict

# Third-Party Imports
import redis
from redis.exceptions import RedisError

import config_loader
from utils import log_helpers
from models.tracking_models import ServiceLog, LogType


# === Set up logging ===
logger = log_helpers.get_logger("Redis Connection")


# === Store per-task workflow step statuses in Redis === #
class RedisConnector:
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
        step_id: Optional[str] = None,
        ttl: int = 3600,
    ) -> bool:
        """
        Store a step's status and optional Id in Redis.

        Args:
            task_id: Task identifier.
            step_name: Workflow step name.
            status: Step execution status.
            step_id: Optional step identifier.
            ttl: Expiration time (seconds), defaults to 3600.

        Returns:
            True if stored successfully, False otherwise.
        """
        step_status_key = f"task:{task_id}:step_statuses"
        step_ids_key = f"task:{task_id}:step_ids"

        try:
            self.redis_client.hset(step_status_key, step_name, status)
            if step_id:
                self.redis_client.hset(step_ids_key, step_name, step_id)

            self.redis_client.expire(step_status_key, ttl)
            self.redis_client.expire(step_ids_key, ttl)

            logger.info(
                "[Redis] Stored step status successfully",
                extra={
                    "service": ServiceLog.DATABASE,
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
                    "service": ServiceLog.DATABASE,
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

    def get_all_step_status(self, task_id: str) -> Dict[str, str]:
        """
        Get all step statuses for a task from Redis.

        Args:
            task_id: Task identifier.

        Returns:
            A dict mapping step names to their status, or empty dict if failed.
        """
        step_status_key = f"task:{task_id}:step_status"

        try:
            data = self.redis_client.hgetall(step_status_key)
            logger.info(
                "[Redis] Retrieved all step statuses successfully",
                extra={
                    "service": ServiceLog.DATABASE,
                    "log_type": LogType.ACCESS,
                    "task_id": task_id,
                    "redis_key": step_status_key,
                    "step_count": len(data),
                },
            )
            return data

        except RedisError as e:
            logger.error(
                "[Redis] Failed to fetch all step statuses",
                exc_info=True,
                extra={
                    "service": ServiceLog.DATABASE,
                    "log_type": LogType.ERROR,
                    "task_id": task_id,
                    "redis_key": step_status_key,
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                },
            )
            return {}

    def get_step_ids(self, task_id: str) -> Dict[str, str]:
        """
        Get all step Ids for a task from Redis.

        Args:
            task_id: Task identifier.

        Returns:
            A dict mapping step names to their IDs, or empty dict if failed.
        """
        step_ids_key = f"task:{task_id}:step_ids"

        try:
            data = self.redis_client.hgetall(step_ids_key)
            logger.info(
                "[Redis] Retrieved all step IDs successfully",
                extra={
                    "service": ServiceLog.DATABASE,
                    "log_type": LogType.ACCESS,
                    "task_id": task_id,
                    "redis_key": step_ids_key,
                    "step_count": len(data),
                },
            )
            return data

        except RedisError as e:
            logger.error(
                "[Redis] Failed to fetch step IDs",
                exc_info=True,
                extra={
                    "service": ServiceLog.DATABASE,
                    "log_type": LogType.ERROR,
                    "task_id": task_id,
                    "redis_key": step_ids_key,
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                },
            )
            return {}

    # === Store and retrieve workflow Id associated with a task === #
    def store_workflow_id(
        self, task_id: str, workflow_id: str, status: str, ttl: int = 3600
    ) -> bool:
        """
        Store workflow Id and status in Redis with a TTL.

        Args:
            task_id: Task identifier.
            workflow_id: Workflow identifier.
            status: Current workflow status.
            ttl: Expiration time in seconds. Default is 3600.

        Returns:
            True if stored successfully, otherwise False.
        """
        key = f"task:{task_id}:workflow_id"
        try:
            self.redis_client.hset(key, workflow_id, status)
            self.redis_client.expire(key, ttl)
            logger.info(
                "[Redis] Stored workflow Id in Redis successfully",
                extra={
                    "service": ServiceLog.DATABASE,
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
                "[Redis] Failed to store workflow Id in Redis",
                exc_info=True,
                extra={
                    "service": ServiceLog.DATABASE,
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

    def get_workflow_id(self, task_id: str) -> Optional[Dict[str, str]]:
        """
        Get workflow Id and status from Redis by task Id.

        Args:
            task_id: Task identifier.

        Returns:
            A dict with workflow_id and status, or None if not found or failed.
        """

        key = f"task:{task_id}:workflow_id"
        try:
            hash_data = self.redis_client.hgetall(key)
            if not hash_data:
                logger.info(
                    "[Redis] No workflow Id found",
                    extra={
                        "service": ServiceLog.DATABASE,
                        "log_type": LogType.ACCESS,
                        "task_id": task_id,
                        "redis_key": key,
                    },
                )
                return None

            workflow_id, status = next(iter(hash_data.items()))
            logger.info(
                "[Redis] Retrieved workflow Id successfully",
                extra={
                    "service": ServiceLog.DATABASE,
                    "log_type": LogType.ACCESS,
                    "task_id": task_id,
                    "workflow_id": workflow_id,
                    "status": status,
                    "redis_key": key,
                },
            )
            return {"workflow_id": workflow_id, "status": status}

        except RedisError as e:
            logger.error(
                "[Redis] Failed to fetch workflow Id",
                exc_info=True,
                extra={
                    "service": ServiceLog.DATABASE,
                    "log_type": LogType.ERROR,
                    "task_id": task_id,
                    "redis_key": key,
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                },
            )
            return None

    def store_jwt_token(self, token: str, ttl: int) -> bool:
        """
        Store a JWT token in Redis with a specified time-to-live (TTL).

        Args:
            token: The JWT token to store.
            ttl: Time-to-live in seconds for the token in Redis.

        Returns:
            bool: True if the token was stored successfully, False otherwise.
        """
        try:
            key = "jwt_token"
            self.redis_client.set(key, token, ex=ttl)
            logger.info("Updated JWT token to Redis")
            return True
        except RedisError as e:
            logger.error(f"Redis error while updating JWT token: {e}")
            return False

    def get_jwt_token(self) -> Optional[str]:
        """
        Retrieve a JWT token from Redis.

        Returns:
            Optional[str]: The JWT token if found, None otherwise.
        """
        try:
            key = "jwt_token"
            token = self.redis_client.get(key)
            if token:
                logger.info("Found the JWT token from Redis")
                return token
            return None
        except RedisError as e:
            logger.error(f"Redis error while retrieving JWT token: {e}")
            return None
