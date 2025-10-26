from unittest.mock import AsyncMock, MagicMock, patch
import pytest
from pydantic import BaseModel
from fastapi_celery.celery_worker import step_handler
from fastapi_celery.models.class_models import StepOutput
from fastapi_celery.celery_worker.step_handler import (
    build_s3_key_prefix,
    execute_step,
    extract,
    extract_to_wrapper,
    get_context_api,
    get_model_dump_if_possible,
    raise_if_failed,
    get_value,
    resolve_args,
)
from fastapi_celery.celery_worker.step_handler import StepDefinition, StatusEnum


# === get_model_dump_if_possible ===
def test_get_model_dump_if_possible_with_base_model():
    class DummyOutput(BaseModel):
        value: int

    class DummyStepOutput(BaseModel):
        output: DummyOutput

    obj = DummyStepOutput(output=DummyOutput(value=42))
    result = get_model_dump_if_possible(obj)
    assert result == {"value": 42}


def test_get_model_dump_if_possible_non_model():
    obj = {"output": "abc"}
    result = get_model_dump_if_possible(obj)
    assert result == obj


# === raise_if_failed ===
def test_raise_if_failed_success():
    class DummyResult(BaseModel):
        step_status: StatusEnum
        step_failure_message: list[str] | None = None

    result = DummyResult(step_status=StatusEnum.SUCCESS)
    raise_if_failed(result, "dummy_step")  # should not raise


def test_raise_if_failed_failed():
    class DummyResult(BaseModel):
        step_status: StatusEnum
        step_failure_message: list[str] | None = None

    result = DummyResult(step_status=StatusEnum.FAILED, step_failure_message=["error"])
    with pytest.raises(RuntimeError) as exc:
        raise_if_failed(result, "dummy_step")
    assert "dummy_step" in str(exc.value)
    assert "error" in str(exc.value)


# === get_value ===
def test_get_value_base_model_and_dict():
    class DummyCtx(BaseModel):
        foo: str = "bar"

    ctx_model = DummyCtx()
    ctx_dict = {"foo": "bar"}

    assert get_value(ctx_model, "foo") == "bar"
    assert get_value(ctx_model, "missing") is None
    assert get_value(ctx_dict, "foo") == "bar"
    assert get_value(ctx_dict, "missing") is None


# === resolve_args ===
def test_resolve_args_with_args_and_kwargs():
    context = {"a": 1, "b": 2}
    step_def = StepDefinition(
        function_name="dummy_function",
        args=["a", "b"],
        kwargs={"x": "a", "y": 2},
    )
    args, kwargs = resolve_args(step_def, context, "dummy_step")
    assert args == [1, 2]
    assert kwargs == {"x": 1, "y": 2}
    assert context["input_data"] == [1, 2]


# === extract / extract_to_wrapper ===
def test_extract_and_wrapper():
    ctx = {}
    result = {"foo": "bar"}
    extract(ctx, result, "ctx_key", "foo")
    assert ctx["ctx_key"] == "bar"

    def fail_func(c, r, k, rk):
        raise ValueError("boom")

    wrapped = extract_to_wrapper(fail_func)
    ctx2 = {}
    wrapped(ctx2, {}, "key", "rk")
    assert ctx2["key"] is None


# === build_s3_key_prefix ===
def test_build_s3_key_prefix(monkeypatch):
    processor = MagicMock()
    processor.file_record = {"file_name": "file.xlsx"}
    processor.tracking_model = MagicMock()
    context_data = MagicMock()
    step = MagicMock()
    step.stepName = "STEP1"
    step.stepOrder = 1
    step_config = MagicMock()
    step_config.target_store_data = "target"
    step_config.data_input = None
    filter_api = MagicMock()
    filter_api.response.isMasterDataWorkflow = True
    context_data.workflow_detail.filter_api = filter_api

    prefix = build_s3_key_prefix(processor, context_data, step, step_config)
    assert prefix.startswith("target/file")


def test_build_s3_key_prefix_non_master_data():
    processor = MagicMock()
    processor.file_record = {"file_name": "file.xlsx"}
    processor.tracking_model = MagicMock()
    context_data = MagicMock()
    step = MagicMock()
    step.stepName = "STEP2"
    step.stepOrder = 1
    step_config = MagicMock()
    step_config.target_store_data = "target"
    step_config.data_input = None

    filter_api = MagicMock()
    filter_api.response.isMasterDataWorkflow = False
    filter_api.response.folderName = "folder1"
    filter_api.response.customerFolderName = "customerA"
    context_data.workflow_detail.filter_api = filter_api

    prefix = build_s3_key_prefix(processor, context_data, step, step_config)
    assert "folder1/customerA" in prefix


# === get_context_api ===
def test_get_context_api_returns_list_and_other_steps():
    result = get_context_api("FILE_PARSE", {})
    assert isinstance(result, list)
    for step_name in [
        "VALIDATE_HEADER",
        "VALIDATE_DATA",
        "MASTER_DATA_LOAD",
        "TEMPLATE_DATA_MAPPING",
    ]:
        calls = get_context_api(step_name, {})
        assert isinstance(calls, list)


# === execute_step ===
@pytest.mark.asyncio
@patch("fastapi_celery.celery_worker.step_handler.get_context_api")
@patch("fastapi_celery.celery_worker.step_handler.BEConnector")
async def test_execute_step_basic(mock_be_connector, mock_get_context_api):
    from fastapi_celery.celery_worker import step_handler

    file_processor = MagicMock()
    file_processor.file_record = {"file_name": "dummy.xlsx", "file_extension": ".xlsx"}
    file_processor.workflow_step_ids = {}
    file_processor.tracking_model = MagicMock()

    step = MagicMock()
    step.stepName = "STEP1"
    step.workflowStepId = "step_1"
    step.stepOrder = 0

    context_data = MagicMock()
    context_data.request_id = "req_1"
    context_data.step_detail = []
    context_data.workflow_detail.filter_api.response.isMasterDataWorkflow = True

    step_def = MagicMock()
    step_def.require_data_output = True
    step_def.function_name = "func"
    step_def.data_output = None
    step_def.extract_to = {}
    step_def.data_input = None

    step_handler.PROCESS_DEFINITIONS["STEP1"] = step_def
    file_processor.func = AsyncMock(
        return_value=StepOutput(step_status=StatusEnum.SUCCESS, step_failure_message=[], data=None)
    )

    mock_get_context_api.return_value = None
    result = await step_handler.execute_step(file_processor, context_data, step)
    assert result.step_status == StatusEnum.SUCCESS
    del step_handler.PROCESS_DEFINITIONS["STEP1"]


@pytest.mark.asyncio
async def test_execute_step_not_defined():
    from fastapi_celery.celery_worker import step_handler

    file_processor = MagicMock()
    file_processor.workflow_step_ids = {}
    context_data = MagicMock()
    step = MagicMock()
    step.stepName = "UNKNOWN_STEP"
    step.workflowStepId = "step_x"

    result = await step_handler.execute_step(file_processor, context_data, step)
    assert result.step_status == step_handler.StatusEnum.NOT_DEFINED
    assert "not yet defined" in result.step_failure_message[0]


@pytest.mark.asyncio
async def test_execute_step_skip_if_s3_exists():
    from fastapi_celery.celery_worker import step_handler
    from fastapi_celery.models.class_models import StepOutput, StatusEnum

    file_processor = MagicMock()
    file_processor.file_record = {"file_name": "dummy.xlsx", "file_extension": ".xlsx"}
    file_processor.tracking_model = MagicMock()
    file_processor.workflow_step_ids = {"STEP1": "step_1"}
    file_processor.check_step_result_exists_in_s3 = MagicMock(
        return_value=StepOutput(step_status="1", step_failure_message=[], data=None)
    )

    step = MagicMock()
    step.stepName = "STEP1"
    step.workflowStepId = "step_1"
    step.stepOrder = 0

    context_data = MagicMock()
    context_data.request_id = "req_1"
    context_data.step_detail = []

    step_def = MagicMock()
    step_def.require_data_output = True
    step_def.function_name = "func"
    step_def.data_output = None
    step_def.extract_to = {}
    step_def.target_store_data = "target"
    step_def.data_input = None

    step_handler.PROCESS_DEFINITIONS["STEP1"] = step_def
    result = await step_handler.execute_step(file_processor, context_data, step)
    assert result is None or isinstance(result, StepOutput)
    del step_handler.PROCESS_DEFINITIONS["STEP1"]


@pytest.mark.asyncio
async def test_execute_step_with_extract_to():
    from fastapi_celery.celery_worker import step_handler
    from fastapi_celery.models.class_models import StepOutput, StatusEnum

    file_processor = MagicMock()
    file_processor.file_record = {"file_name": "dummy.xlsx", "file_extension": ".xlsx"}
    file_processor.tracking_model = MagicMock()
    file_processor.workflow_step_ids = {}

    step = MagicMock()
    step.stepName = "STEP_EXTRACT"
    step.workflowStepId = "step_ex"
    step.stepOrder = 0

    context_data = MagicMock()
    context_data.request_id = "req_3"
    context_data.step_detail = []

    step_def = MagicMock()
    step_def.require_data_output = True
    step_def.function_name = "func"
    step_def.data_output = None
    step_def.extract_to = {"some_field": "value"}
    step_def.target_store_data = "target"
    step_def.data_input = None

    step_handler.PROCESS_DEFINITIONS["STEP_EXTRACT"] = step_def

    async def fake_func(*args, **kwargs):
        class DummyOutput:
            value = 123
        return StepOutput(step_status=StatusEnum.SUCCESS, step_failure_message=[], data=DummyOutput())

    file_processor.func = fake_func
    result = await step_handler.execute_step(file_processor, context_data, step)
    assert result.step_status == StatusEnum.SUCCESS
    del step_handler.PROCESS_DEFINITIONS["STEP_EXTRACT"]


# === raise_if_failed ===
def test_raise_if_failed_non_model():
    """Test case when result is not BaseModel — should do nothing."""
    try:
        raise_if_failed({"not": "model"}, "STEP")
    except Exception:
        pytest.fail("Should not raise error for non-BaseModel")


# === extract_to_wrapper when context is BaseModel ===
def test_extract_to_wrapper_with_basemodel_context():
    class DummyCtx(BaseModel):
        foo: str | None = "bar"

    ctx = DummyCtx()

    def bad_func(c, r, k, rk):
        raise ValueError("oops")

    wrapped = extract_to_wrapper(bad_func)
    wrapped(ctx, {}, "foo", "rk")
    assert ctx.foo is None  # must be set to None if exception happens


@pytest.mark.asyncio
async def test_execute_step_missing_method(monkeypatch):
    from fastapi_celery.celery_worker import step_handler

    file_processor = MagicMock()
    file_processor.file_record = {"file_name": "abc.xlsx", "file_extension": ".xlsx"}
    file_processor.workflow_step_ids = {}
    file_processor.tracking_model = MagicMock()
    file_processor.tracking_model.rerun_attempt = None

    context_data = MagicMock()
    context_data.request_id = "r1"
    context_data.step_detail = []
    context_data.workflow_detail.filter_api.response.isMasterDataWorkflow = True

    step = MagicMock()
    step.stepName = "STEP_ERR"
    step.workflowStepId = "w1"
    step.stepOrder = 0

    step_def = MagicMock(spec=[])  # 👈 Không auto-gen thuộc tính
    step_def.function_name = "non_existing_function"
    step_def.require_data_output = False
    step_def.extract_to = {}
    step_def.target_store_data = "target"
    step_def.data_input = None  # 👈 Quan trọng: string hoặc None, không mock

    step_handler.PROCESS_DEFINITIONS["STEP_ERR"] = step_def

    with pytest.raises(AttributeError):
        await step_handler.execute_step(file_processor, context_data, step)

    del step_handler.PROCESS_DEFINITIONS["STEP_ERR"]



# === execute_step with generic Exception ===
@pytest.mark.asyncio
async def test_execute_step_generic_exception(monkeypatch):
    file_processor = MagicMock()
    file_processor.file_record = {"file_name": "abc.xlsx", "file_extension": ".xlsx"}
    file_processor.workflow_step_ids = {}
    file_processor.tracking_model = MagicMock()
    file_processor.tracking_model.rerun_attempt = None

    context_data = MagicMock()
    context_data.request_id = "r2"
    context_data.step_detail = []
    context_data.workflow_detail.filter_api.response.isMasterDataWorkflow = True

    step = MagicMock()
    step.stepName = "STEP_EXC"
    step.workflowStepId = "w2"
    step.stepOrder = 0

    async def bad_func(*args, **kwargs):
        raise RuntimeError("bad stuff")

    step_def = MagicMock()
    step_def.function_name = "func"
    step_def.require_data_output = True
    step_def.target_store_data = "target"
    step_def.extract_to = {}
    step_def.data_input = "dummy"  # ✅ thêm dòng này
    file_processor.func = bad_func

    step_handler.PROCESS_DEFINITIONS["STEP_EXC"] = step_def

    with pytest.raises(RuntimeError):
        await step_handler.execute_step(file_processor, context_data, step)

    del step_handler.PROCESS_DEFINITIONS["STEP_EXC"]


# === build_s3_key_prefix missing folder/customer ===
def test_build_s3_key_prefix_missing_folder_customer(caplog, monkeypatch):
    import logging
    from fastapi_celery.celery_worker import step_handler

    # ⚙️ Patch logger adapter để log ra stderr bình thường
    monkeypatch.setattr(step_handler, "logger", logging.getLogger("test_logger"))
    caplog.set_level(logging.ERROR)

    processor = MagicMock()
    processor.file_record = {"file_name": "abc.xlsx"}
    processor.tracking_model = MagicMock()
    processor.tracking_model.request_id = "req_1"

    context_data = MagicMock()
    context_data.workflow_detail.filter_api.response.isMasterDataWorkflow = False
    context_data.workflow_detail.filter_api.response.folderName = None
    context_data.workflow_detail.filter_api.response.customerFolderName = None

    step = MagicMock()
    step.stepName = "STEP_MISS"
    step.stepOrder = 1

    step_conf = MagicMock()
    step_conf.target_store_data = "target"

    prefix = step_handler.build_s3_key_prefix(processor, context_data, step, step_conf)

    assert "target" in prefix
    assert "STEP_MISS" in prefix
    # ✅ Giờ logger.error sẽ xuất hiện
    assert "Missing 'folderName'" in caplog.text
