import pytest
import asyncio
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock
from fastapi import FastAPI
from fastapi_celery.routers.api_file_processor import router
from fastapi_celery.models.class_models import StatusEnum
from fastapi_celery.models.tracking_models import LogType, ServiceLog

# Create a FastAPI app instance for testing
app = FastAPI()
app.include_router(router)
client = TestClient(app)


# ------------------ /file/process Tests ------------------

@patch("fastapi_celery.routers.api_file_processor.celery_task.task_execute.apply_async")
@patch("celery.app.task.Task.apply_async")
@patch("kombu.connection.Connection")
def test_process_file(mock_connection, mock_apply_async, mock_apply_async_task):
    mock_apply_async.return_value = None
    mock_apply_async_task.return_value = None
    mock_connection.return_value = MagicMock()

    payload = {"file_path": "/some/path/to/file.csv", "project": "test_project", "source": "SFTP"}
    response = client.post("/file/process", json=payload)

    assert response.status_code == 200
    res_json = response.json()
    assert "file_path" in res_json and res_json["file_path"] == payload["file_path"]
    assert "celery_id" in res_json
    assert mock_apply_async.called
    args, kwargs = mock_apply_async.call_args
    task_data = kwargs["kwargs"]["data"]
    assert task_data["file_path"] == payload["file_path"]


@patch("fastapi_celery.routers.api_file_processor.celery_task.task_execute.apply_async")
@patch("celery.app.task.Task.apply_async")
@patch("kombu.connection.Connection")
def test_process_file_failure(mock_connection, mock_apply_async, mock_apply_async_task):
    # When Celery apply_async throws an error
    mock_apply_async.side_effect = Exception("Task submission failed")
    mock_apply_async_task.return_value = None
    mock_connection.return_value = MagicMock()

    payload = {"file_path": "/some/path/to/file.csv", "project": "test_project", "source": "SFTP"}
    response = client.post("/file/process", json=payload)

    # The root router will pay 500 if Celery raises
    assert response.status_code == 500
    res_json = response.json()
    assert "Task submission failed" in res_json.get("detail", "") or "Internal Server Error" in res_json.get("detail", "")


# ------------------ /tasks/stop Tests ------------------

@pytest.mark.asyncio
@patch("fastapi_celery.routers.api_file_processor.DISABLE_STOP_TASK_ENDPOINT", False)
@patch("fastapi_celery.routers.api_file_processor.celery_app.control.revoke")
@patch("fastapi_celery.routers.api_file_processor.BEConnector")
@patch("fastapi_celery.routers.api_file_processor.RedisConnector.get_step_ids")
@patch("fastapi_celery.routers.api_file_processor.RedisConnector.get_all_step_status")
@patch("fastapi_celery.routers.api_file_processor.RedisConnector.get_workflow_id")
async def test_stop_task_success(mock_get_workflow_id, mock_get_step_statuses, mock_get_step_ids, mock_BEConnector, mock_revoke):
    mock_get_workflow_id.return_value = {"workflow_id": "wf_1", "status": StatusEnum.PROCESSING}
    mock_get_step_ids.return_value = {"step1": "step_1"}
    mock_get_step_statuses.return_value = {"step1": "InProgress"}
    fut = asyncio.Future()
    fut.set_result(MagicMock(status_code=200))
    mock_BEConnector.return_value.post = MagicMock(return_value=fut)

    payload = {"task_id": "task_123", "reason": "Manual stop"}
    response = client.post("/tasks/stop", json=payload)

    assert response.status_code == 200
    res_json = response.json()
    assert res_json["status"] == "Task stopped successfully"
    mock_revoke.assert_called_once_with("task_123", terminate=True, signal="SIGKILL")


@pytest.mark.asyncio
@patch("fastapi_celery.routers.api_file_processor.DISABLE_STOP_TASK_ENDPOINT", False)
@patch("fastapi_celery.routers.api_file_processor.celery_app.control.revoke")
@patch("fastapi_celery.routers.api_file_processor.BEConnector")
@patch("fastapi_celery.routers.api_file_processor.RedisConnector.get_step_ids")
@patch("fastapi_celery.routers.api_file_processor.RedisConnector.get_all_step_status")
@patch("fastapi_celery.routers.api_file_processor.RedisConnector.get_workflow_id")
async def test_stop_task_failure(mock_get_workflow_id, mock_get_step_statuses, mock_get_step_ids, mock_BEConnector, mock_revoke):
    mock_get_workflow_id.return_value = None
    payload = {"task_id": "task_123", "reason": "Manual stop"}
    response = client.post("/tasks/stop", json=payload)

    assert response.status_code == 404
    res_json = response.json()
    assert res_json["error"] == "Workflow ID not found for task"
    mock_revoke.assert_not_called()


@patch("fastapi_celery.routers.api_file_processor.DISABLE_STOP_TASK_ENDPOINT", False)
@patch("fastapi_celery.routers.api_file_processor.celery_app.control.revoke")
@patch("fastapi_celery.routers.api_file_processor.BEConnector")
@patch("fastapi_celery.routers.api_file_processor.RedisConnector.get_step_ids")
@patch("fastapi_celery.routers.api_file_processor.RedisConnector.get_all_step_status")
@patch("fastapi_celery.routers.api_file_processor.RedisConnector.get_workflow_id")
@patch("fastapi_celery.routers.api_file_processor.logger")
def test_stop_task_exception_handling(mock_logger, mock_get_workflow_id, mock_get_step_statuses, mock_get_step_ids, mock_BEConnector, mock_revoke):
    mock_get_workflow_id.return_value = {"workflow_id": "wf_1", "status": StatusEnum.PROCESSING}
    mock_get_step_ids.return_value = {"step1": "step_1"}
    mock_get_step_statuses.return_value = {"step1": "InProgress"}
    mock_BEConnector.return_value.post.side_effect = Exception("Simulated BEConnector Exception")

    payload = {"task_id": "task_123", "reason": "Manual stop"}
    response = client.post("/tasks/stop", json=payload)

    assert response.status_code == 500
    res_json = response.json()
    assert "Simulated BEConnector Exception" in res_json["error"]
    mock_logger.error.assert_called()


@pytest.mark.asyncio
@patch("fastapi_celery.routers.api_file_processor.DISABLE_STOP_TASK_ENDPOINT", False)
@patch("fastapi_celery.routers.api_file_processor.celery_app.control.revoke")
@patch("fastapi_celery.routers.api_file_processor.BEConnector")
@patch("fastapi_celery.routers.api_file_processor.RedisConnector.get_step_ids")
@patch("fastapi_celery.routers.api_file_processor.RedisConnector.get_all_step_status")
@patch("fastapi_celery.routers.api_file_processor.RedisConnector.get_workflow_id")
async def test_stop_task_inprogress_flow(mock_get_workflow_id, mock_get_step_statuses, mock_get_step_ids, mock_BEConnector, mock_revoke):
    mock_get_workflow_id.return_value = {"workflow_id": "wf_1", "status": StatusEnum.PROCESSING}
    mock_get_step_ids.return_value = {"step1": "step_1"}
    mock_get_step_statuses.return_value = {"step1": "InProgress"}

    fut = asyncio.Future()
    fut.set_result(MagicMock(status_code=200))
    mock_BEConnector.return_value.post = MagicMock(return_value=fut)

    payload = {"task_id": "task_456", "reason": "Test InProgress"}
    response = client.post("/tasks/stop", json=payload)

    assert response.status_code == 200
    res_json = response.json()
    assert res_json["status"] == "Task stopped successfully"
    mock_revoke.assert_called_once_with("task_456", terminate=True, signal="SIGKILL")
