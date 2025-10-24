import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from celery.exceptions import Retry, MaxRetriesExceededError
from fastapi_celery.celery_worker import celery_task
from fastapi_celery.models.class_models import StepOutput, StatusEnum, DocumentType
from pydantic import BaseModel
import json


# === task_execute success ===
@patch("fastapi_celery.celery_worker.celery_task.handle_task", new_callable=AsyncMock)
def test_task_execute_success(mock_handle_task):
    fake_data = {"file_path": "dummy.xlsx", "project": "proj", "source": "src"}
    fake_tracking_model = MagicMock()
    fake_tracking_model.request_id = "req_1"

    with patch("fastapi_celery.celery_worker.celery_task.TrackingModel.from_data_request",
               return_value=fake_tracking_model):
        mock_handle_task.return_value = None
        result = celery_task.task_execute.run(fake_data)
        assert result == "Task completed"
        mock_handle_task.assert_awaited_once()


@patch("fastapi_celery.celery_worker.celery_task.handle_task", new_callable=AsyncMock)
def test_task_execute_failure(mock_handle_task):
    fake_data = {"file_path": "dummy.xlsx", "project": "proj", "source": "src"}
    fake_tracking_model = MagicMock()
    fake_tracking_model.request_id = "req_2"
    mock_handle_task.side_effect = Exception("Boom!")



# === inject_metadata tests ===
class DummyOutput(BaseModel):
    value: int


@pytest.mark.asyncio
async def test_inject_metadata_base_model():
    from fastapi_celery.celery_worker.celery_task import inject_metadata_into_step_output, ContextData

    step_result = StepOutput(output=DummyOutput(value=42),
                             step_status=StatusEnum.SUCCESS,
                             step_failure_message=[])
    context_data = ContextData(request_id="req_3")
    context_data.step_detail = ["dummy"]
    context_data.workflow_detail = "wf"
    inject_metadata_into_step_output(step_result, context_data, DocumentType.MASTER_DATA)
    assert hasattr(step_result.output, "step_detail")


@pytest.mark.asyncio
async def test_inject_metadata_none_output():
    from fastapi_celery.celery_worker.celery_task import inject_metadata_into_step_output, ContextData

    step_result = StepOutput(output=None, step_status=StatusEnum.SUCCESS, step_failure_message=[])
    context_data = ContextData(request_id="req_4")
    context_data.step_detail = ["dummy"]
    context_data.workflow_detail = "wf"
    inject_metadata_into_step_output(step_result, context_data, DocumentType.MASTER_DATA)
    assert step_result.output is None


@pytest.mark.asyncio
async def test_inject_metadata_dict_missing_output():
    """Test dict output without json_data.output"""
    from fastapi_celery.celery_worker import celery_task
    step_result = StepOutput(
        output={"json_data": {}},
        step_status=StatusEnum.SUCCESS,
        step_failure_message=[]
    )
    context_data = celery_task.ContextData(request_id="req_5")
    context_data.step_detail = ["dummy"]
    context_data.workflow_detail = "wf"

    with patch.object(celery_task.logger, "warning") as mock_warn:
        celery_task.inject_metadata_into_step_output(
            step_result, context_data, DocumentType.MASTER_DATA
        )

    # Lấy tất cả log gọi warning
    logged_msgs = [str(args[0]) for args, _ in mock_warn.call_args_list]
    assert any("Dict output missing" in msg for msg in logged_msgs), \
        f"Không thấy log 'Dict output missing' trong {logged_msgs}"


@pytest.mark.asyncio
async def test_inject_metadata_unsupported_type():
    """Test unsupported output type"""
    from fastapi_celery.celery_worker import celery_task
    step_result = StepOutput(
        output=["invalid"],
        step_status=StatusEnum.SUCCESS,
        step_failure_message=[]
    )
    context_data = celery_task.ContextData(request_id="req_6")
    context_data.step_detail = ["dummy"]
    context_data.workflow_detail = "wf"

    with patch.object(celery_task.logger, "warning") as mock_warn:
        celery_task.inject_metadata_into_step_output(
            step_result, context_data, DocumentType.ORDER
        )

    logged_msgs = [str(args[0]) for args, _ in mock_warn.call_args_list]
    assert any("Unsupported type" in msg for msg in logged_msgs), \
        f"Không thấy log 'Unsupported type' trong {logged_msgs}"


@pytest.mark.asyncio
async def test_inject_metadata_dict_with_output(monkeypatch):
    from fastapi_celery.celery_worker.celery_task import inject_metadata_into_step_output, ContextData
    class DummyParsed(BaseModel):
        x: int = 1
        def model_copy(self, update=None): return self
    monkeypatch.setattr(celery_task.template_helper, "parse_data", lambda **_: DummyParsed())

    step_result = StepOutput(output={"json_data": {"output": {"x": 1}}},
                             step_status=StatusEnum.SUCCESS,
                             step_failure_message=[])
    context_data = ContextData(request_id="req_7")
    context_data.step_detail = ["dummy"]
    context_data.workflow_detail = "wf"
    inject_metadata_into_step_output(step_result, context_data, DocumentType.ORDER)
    assert isinstance(step_result.output, DummyParsed)


# === handle_task success ===
@pytest.mark.asyncio
@patch("fastapi_celery.celery_worker.celery_task.execute_step", new_callable=AsyncMock)
@patch("fastapi_celery.celery_worker.celery_task.get_workflow_filter", new_callable=AsyncMock)
@patch("fastapi_celery.celery_worker.celery_task.call_workflow_session_start", new_callable=AsyncMock)
@patch("fastapi_celery.celery_worker.celery_task.call_workflow_session_finish", new_callable=AsyncMock)
@patch("fastapi_celery.celery_worker.celery_task.call_workflow_step_start", new_callable=AsyncMock)
@patch("fastapi_celery.celery_worker.celery_task.call_workflow_step_finish", new_callable=AsyncMock)
@patch("fastapi_celery.celery_worker.celery_task.ProcessorBase")
@patch("fastapi_celery.celery_worker.celery_task.RedisConnector")
async def test_handle_task_success(mock_redis, mock_processor, mock_finish, mock_start,
                                   mock_sess_finish, mock_sess_start, mock_getwf, mock_exec):
    fake_tracking = MagicMock()
    fake_tracking.request_id = "req_8"
    fake_tracking.project_name = "proj"
    fake_tracking.source_name = "src"
    fake_tracking.rerun_attempt = None

    fake_proc = MagicMock()
    fake_proc.document_type = DocumentType.ORDER.value
    fake_proc.run.return_value = None
    fake_proc.file_record = {"file_name": "f.xlsx", "file_extension": ".xlsx"}
    mock_processor.return_value = fake_proc

    step = MagicMock(stepName="STEP1", workflowStepId="s1", stepOrder=0)
    mock_getwf.return_value.workflowSteps = [step]

    class FakeOutput:
        def model_copy(self, update=None): return self
    mock_exec.return_value = StepOutput(output=FakeOutput(),
                                        step_status=StatusEnum.SUCCESS,
                                        step_failure_message=[])

    mock_sess_start.return_value = MagicMock()
    mock_sess_finish.return_value = MagicMock()
    mock_start.return_value = MagicMock()
    mock_finish.return_value = MagicMock()

    from fastapi_celery.celery_worker.celery_task import ContextData
    ctx_instance = ContextData(request_id=fake_tracking.request_id)
    ctx_instance.s3_key_prefix = "pref"
    celery_task.ContextData = MagicMock(return_value=ctx_instance)
    ContextData.is_done = property(lambda self: False)
    result = await celery_task.handle_task(fake_tracking)
    assert hasattr(result, "request_id")
    mock_exec.assert_awaited_once()
    mock_redis.assert_called_once()


# === helper function tests ===
@pytest.mark.asyncio
@patch("fastapi_celery.celery_worker.celery_task.BEConnector")
async def test_get_workflow_filter_success(mock_conn):
    from fastapi_celery.celery_worker.celery_task import get_workflow_filter, ContextData
    ctx = ContextData(request_id="req_9")
    file_proc = MagicMock()
    file_proc.file_record = {"file_path_parent": "p", "file_name": "f", "file_extension": ".x"}
    tracking = MagicMock(project_name="proj", source_name="src", request_id="req_9")
    mock_inst = mock_conn.return_value
    # Add workflowSteps để tránh ValidationError
    mock_inst.post = AsyncMock(return_value={"id": "wf1", "name": "wf",
                                             "sapMasterData": True,
                                             "workflowSteps": []})
    result = await get_workflow_filter(ctx, file_proc, tracking)
    assert result.id == "wf1"


@pytest.mark.asyncio
@patch("fastapi_celery.celery_worker.celery_task.BEConnector")
async def test_call_workflow_session_start_success(mock_conn):
    from fastapi_celery.celery_worker.celery_task import call_workflow_session_start, ContextData
    ctx = ContextData(request_id="req_10")
    tracking = MagicMock(workflow_id="wf", request_id="req_10", file_path="path")
    mock_inst = mock_conn.return_value
    # Add "status" để tránh ValidationError
    mock_inst.post = AsyncMock(return_value={"id": "sess1", "status": "OK"})
    res = await call_workflow_session_start(ctx, tracking)
    assert res.id == "sess1"


@pytest.mark.asyncio
@patch("fastapi_celery.celery_worker.celery_task.BEConnector")
async def test_call_workflow_session_finish_success(mock_conn):
    from fastapi_celery.celery_worker.celery_task import call_workflow_session_finish, ContextData
    ctx = ContextData(request_id="req_11")

    ctx.workflow_detail.metadata_api.session_start_api.response.id = "sess_1"
    tracking = MagicMock(request_id="req_11")

    mock_inst = mock_conn.return_value
    mock_inst.post = AsyncMock(return_value={"ok": True})

    fake_file_proc = MagicMock()
    fake_step_names = ["STEP1", "STEP2"]

    res = await call_workflow_session_finish(ctx, tracking, fake_file_proc, fake_step_names)
    assert res == {"ok": True}



@pytest.mark.asyncio
@patch("fastapi_celery.celery_worker.celery_task.BEConnector")
async def test_call_workflow_step_finish_with_xml(mock_conn):
    from fastapi_celery.celery_worker.celery_task import call_workflow_step_finish, ContextData
    ctx = ContextData(request_id="req_12")
    ctx.s3_key_prefix = "prefix"
    step = MagicMock(stepOrder=0, stepName="STEPXML")
    step_detail = MagicMock()
    step_detail.metadata_api.Step_start_api.response.workflowHistoryId = "wh1"
    step_detail.config_api = [{"response": {"processorArgumentDtos": [
        {"processorArgumentName": "P1", "value": "A"}]}}]
    ctx.step_detail = [step_detail]
    mock_inst = mock_conn.return_value
    mock_inst.post = AsyncMock(return_value={"ok": True})
    step_result = StepOutput(step_status=StatusEnum.SUCCESS, step_failure_message=[], output=None)
    res = await call_workflow_step_finish(ctx, step, step_result)
    assert res == {"ok": True}


@pytest.mark.asyncio
@patch("fastapi_celery.celery_worker.celery_task.BEConnector")
@patch("fastapi_celery.celery_worker.celery_task.build_data_output")
async def test_call_workflow_step_finish_with_build_data_output(mock_build_data_output, mock_conn):
    from fastapi_celery.celery_worker.celery_task import call_workflow_step_finish, ContextData
    # --- Mock context ---
    ctx = ContextData(request_id="req_123")
    ctx.s3_key_prefix = "prefix"
 
    step = MagicMock(stepOrder=0, stepName="TEMPLATE_FILE_PARSE")
    step_detail = MagicMock()
    step_detail.metadata_api.Step_start_api.response.workflowHistoryId = "wh123"
    step_detail.config_api = []
    ctx.step_detail = [step_detail]
 
    # --- Mock BEConnector ---
    mock_inst = mock_conn.return_value
    mock_inst.post = AsyncMock(return_value={"ok": True})
 
    # --- Mock build_data_output ---
    mock_build_data_output.return_value = {
        "totalRecords": 100,
        "storageLocation": "DKSH/VN/XLSX",
        "fileLogLink": "https://s3.abc//DKSK_batch_202510/log.json"
    }
 
    # --- Prepare input StepOutput ---
    step_result = StepOutput(
        step_status=StatusEnum.SUCCESS,
        step_failure_message=[],
        output=None
    )
 
    # --- Call the target function ---
    res = await call_workflow_step_finish(ctx, step, step_result)
 
    # --- Assertions ---
    mock_build_data_output.assert_called_once_with(ctx, step, step_result)
    mock_inst.post.assert_awaited_once()
 
    # Check that final result came from BEConnector.post
    assert res == {"ok": True}
 
    # Check data sent to BEConnector
    body_sent = mock_conn.call_args[1].get("body_data", {})
    expected_output = mock_build_data_output.return_value
 
    assert body_sent["dataOutput"] == json.dumps(expected_output)
 
    # Check context updated
    assert ctx.step_detail[0].data_output == expected_output