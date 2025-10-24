import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from fastapi_celery.processors.processor_registry import ProcessorRegistry, ProcessorTemplate

@pytest.mark.asyncio
@patch("fastapi_celery.processors.processor_registry.BEConnector")
@patch.object(ProcessorTemplate.PDF_001_TEMPLATE, "create_instance")
async def test_valid_processor_mapping(mock_create_instance, mock_connector):
    file_processor = MagicMock()
    file_processor.workflowStepId = "step123"
    file_processor.file_path = "/tmp/sample.pdf"

    mock_response = {
    "data": [{
        "templateFileParse": {"code": "PDF_001_TEMPLATE"}
    }]
    }

    mock_connector.return_value.get = AsyncMock(return_value=mock_response)
    mock_create_instance.return_value = "processor-instance"

    result = await ProcessorRegistry.get_processor_for_file(file_processor)

    assert result == "processor-instance"
    mock_create_instance.assert_called_once_with(tracking_model=file_processor.tracking_model)


@pytest.mark.asyncio
async def test_missing_workflow_step_id():
    file_processor = MagicMock()
    file_processor.document_type = None
    file_processor.workflow_step_ids = {}

    with pytest.raises(RuntimeError, match="Missing workflowStepId"):
        await ProcessorRegistry.get_processor_for_file(file_processor)


@pytest.mark.asyncio
@patch("fastapi_celery.processors.processor_registry.BEConnector")
async def test_invalid_be_response_structure(mock_connector):
    file_processor = MagicMock()
    file_processor.workflowStepId = "step123"

    mock_connector.return_value.get = AsyncMock(return_value={"data": [{}]})

    with pytest.raises(RuntimeError, match="missing template code"):
        await ProcessorRegistry.get_processor_for_file(file_processor)


@pytest.mark.asyncio
@patch("fastapi_celery.processors.processor_registry.BEConnector")
async def test_unknown_template_code(mock_connector):
    file_processor = MagicMock()
    file_processor.workflowStepId = "step123"

    mock_connector.return_value.get = AsyncMock(return_value={
        "data": [{
            "templateFileParse": {"code": "UNKNOWN_TEMPLATE"}
        }]
    })

    with pytest.raises(RuntimeError, match="Unknown template code"):
        await ProcessorRegistry.get_processor_for_file(file_processor)


@pytest.mark.asyncio
@patch("fastapi_celery.processors.processor_registry.BEConnector")
@patch.object(ProcessorTemplate.XML_001_TEMPLATE, "create_instance")
async def test_response_as_list_format(mock_create_instance, mock_connector):
    file_processor = MagicMock()
    file_processor.workflowStepId = "step456"
    file_processor.file_path = "/tmp/sample.xml"

    mock_connector.return_value.get = AsyncMock(return_value=[
        {"templateFileParse": {"code": "XML_001_TEMPLATE"}}
    ])
    mock_create_instance.return_value = "xml-instance"

    result = await ProcessorRegistry.get_processor_for_file(file_processor)

    assert result == "xml-instance"
    mock_create_instance.assert_called_once_with(tracking_model=file_processor.tracking_model)



@pytest.mark.asyncio
@patch("fastapi_celery.processors.processor_registry.BEConnector")
async def test_connector_get_raises_exception(mock_connector):
    file_processor = MagicMock()
    file_processor.workflowStepId = "step789"

    mock_connector.return_value.get = AsyncMock(side_effect=Exception("BE failure"))

    with pytest.raises(Exception, match="BE failure"):
        await ProcessorRegistry.get_processor_for_file(file_processor)
