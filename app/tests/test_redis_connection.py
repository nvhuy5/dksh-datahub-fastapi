# tests/test_redis_connection.py
import pytest
from unittest.mock import patch, MagicMock
from redis.exceptions import RedisError
from fastapi_celery.connections.redis_connection import RedisConnector

# === RedisConnector Tests ===

TASK_ID = "task123"
STEP_NAME = "stepA"
STATUS = "completed"
STEP_ID = "step-id-001"
WORKFLOW_ID = "workflow-xyz"

# -------------------------
# store_step_status
# -------------------------
@patch("fastapi_celery.connections.redis_connection.redis.Redis")
def test_store_step_status_success(mock_redis_class):
    mock_redis = mock_redis_class.return_value
    mock_redis.hset.return_value = True
    mock_redis.expire.return_value = True

    redis_conn = RedisConnector()
    result = redis_conn.store_step_status(TASK_ID, STEP_NAME, STATUS, STEP_ID)
    assert result is True
    assert mock_redis.hset.call_count == 2
    assert mock_redis.expire.call_count == 2


@patch("fastapi_celery.connections.redis_connection.redis.Redis")
def test_store_step_status_no_step_id(mock_redis_class):
    mock_redis = mock_redis_class.return_value
    redis_conn = RedisConnector()
    result = redis_conn.store_step_status(TASK_ID, STEP_NAME, STATUS)
    assert result is True
    mock_redis.hset.assert_called_once()  # only step_status
    mock_redis.expire.assert_called()


@patch("fastapi_celery.connections.redis_connection.redis.Redis")
def test_store_step_status_failure(mock_redis_class):
    mock_redis = mock_redis_class.return_value
    mock_redis.hset.side_effect = RedisError("Connection error")

    redis_conn = RedisConnector()
    result = redis_conn.store_step_status(TASK_ID, STEP_NAME, STATUS)
    assert result is False


# -------------------------
# get_all_step_status
# -------------------------
@patch("fastapi_celery.connections.redis_connection.redis.Redis")
def test_get_all_step_status_success(mock_redis_class):
    mock_redis = mock_redis_class.return_value
    mock_redis.hgetall.return_value = {STEP_NAME: STATUS}

    redis_conn = RedisConnector()
    result = redis_conn.get_all_step_status(TASK_ID)
    assert result == {STEP_NAME: STATUS}


@patch("fastapi_celery.connections.redis_connection.redis.Redis")
def test_get_all_step_status_failure(mock_redis_class):
    mock_redis = mock_redis_class.return_value
    mock_redis.hgetall.side_effect = RedisError("Connection error")

    redis_conn = RedisConnector()
    result = redis_conn.get_all_step_status(TASK_ID)
    assert result == {}


# -------------------------
# get_step_ids
# -------------------------
@patch("fastapi_celery.connections.redis_connection.redis.Redis")
def test_get_step_ids_success(mock_redis_class):
    mock_redis = mock_redis_class.return_value
    mock_redis.hgetall.return_value = {STEP_NAME: STEP_ID}

    redis_conn = RedisConnector()
    result = redis_conn.get_step_ids(TASK_ID)
    assert result == {STEP_NAME: STEP_ID}


@patch("fastapi_celery.connections.redis_connection.redis.Redis")
def test_get_step_ids_failure(mock_redis_class):
    mock_redis = mock_redis_class.return_value
    mock_redis.hgetall.side_effect = RedisError("Connection error")

    redis_conn = RedisConnector()
    result = redis_conn.get_step_ids(TASK_ID)
    assert result == {}


# -------------------------
# store_workflow_id
# -------------------------
@patch("fastapi_celery.connections.redis_connection.redis.Redis")
def test_store_workflow_id_success(mock_redis_class):
    mock_redis = mock_redis_class.return_value
    mock_redis.hset.return_value = True
    mock_redis.expire.return_value = True

    redis_conn = RedisConnector()
    result = redis_conn.store_workflow_id(TASK_ID, WORKFLOW_ID, STATUS)
    assert result is True
    mock_redis.hset.assert_called_once()
    mock_redis.expire.assert_called_once()


@patch("fastapi_celery.connections.redis_connection.redis.Redis")
def test_store_workflow_id_failure(mock_redis_class):
    mock_redis = mock_redis_class.return_value
    mock_redis.hset.side_effect = RedisError("Connection error")

    redis_conn = RedisConnector()
    result = redis_conn.store_workflow_id(TASK_ID, WORKFLOW_ID, STATUS)
    assert result is False


# -------------------------
# get_workflow_id
# -------------------------
@patch("fastapi_celery.connections.redis_connection.redis.Redis")
def test_get_workflow_id_success(mock_redis_class):
    mock_redis = mock_redis_class.return_value
    mock_redis.hgetall.return_value = {WORKFLOW_ID: STATUS}

    redis_conn = RedisConnector()
    result = redis_conn.get_workflow_id(TASK_ID)
    assert result == {"workflow_id": WORKFLOW_ID, "status": STATUS}


@patch("fastapi_celery.connections.redis_connection.redis.Redis")
def test_get_workflow_id_failure(mock_redis_class):
    mock_redis = mock_redis_class.return_value
    mock_redis.hgetall.side_effect = RedisError("Connection error")

    redis_conn = RedisConnector()
    result = redis_conn.get_workflow_id(TASK_ID)
    assert result is None


# -------------------------
# JWT token store & get
# -------------------------
@patch("fastapi_celery.connections.redis_connection.redis.Redis")
def test_store_and_get_jwt_token(mock_redis_class):
    mock_redis = mock_redis_class.return_value
    mock_redis.set.return_value = True
    mock_redis.get.return_value = "jwt-token"

    redis_conn = RedisConnector()
    store_result = redis_conn.store_jwt_token("jwt-token", 3600)
    get_result = redis_conn.get_jwt_token()

    assert store_result is True
    assert get_result == "jwt-token"


@patch("fastapi_celery.connections.redis_connection.redis.Redis")
def test_store_jwt_token_failure(mock_redis_class):
    mock_redis = mock_redis_class.return_value
    mock_redis.set.side_effect = RedisError("Connection error")

    redis_conn = RedisConnector()
    result = redis_conn.store_jwt_token("jwt-token", 3600)
    assert result is False


@patch("fastapi_celery.connections.redis_connection.redis.Redis")
def test_get_jwt_token_failure(mock_redis_class):
    mock_redis = mock_redis_class.return_value
    mock_redis.get.side_effect = RedisError("Connection error")

    redis_conn = RedisConnector()
    result = redis_conn.get_jwt_token()
    assert result is None
