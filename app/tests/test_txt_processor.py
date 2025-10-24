import io
import builtins
import uuid
import pytest
from unittest.mock import MagicMock, patch, mock_open
from pathlib import Path

from fastapi_celery.processors.file_processors.txt_processor import TXTProcessor, PO_MAPPING_KEY
from fastapi_celery.models.tracking_models import TrackingModel
from models.class_models import SourceType, StatusEnum, DocumentType, PODataParsed


@pytest.fixture
def dummy_tracking_model(tmp_path):
    return TrackingModel(
        request_id=str(uuid.uuid4()),
        file_path=str(tmp_path / "dummy.txt"),
    )


@pytest.fixture
def mock_file_processor_local(monkeypatch, dummy_tracking_model):
    """Mock FileExtensionProcessor for local source"""
    mock_instance = MagicMock()
    mock_instance.source = SourceType.LOCAL
    mock_instance.file_path = str(dummy_tracking_model.file_path)
    mock_instance._get_file_capacity.return_value = "1.23 KB"
    mock_instance._get_document_type.return_value = DocumentType.ORDER

    with patch("fastapi_celery.processors.file_processors.txt_processor.ext_extraction.FileExtensionProcessor",
               return_value=mock_instance):
        yield mock_instance


@pytest.fixture
def mock_file_processor_s3(monkeypatch, dummy_tracking_model):
    """Mock FileExtensionProcessor for S3 source"""
    mock_instance = MagicMock()
    mock_instance.source = SourceType.S3
    mock_instance.object_buffer = io.BytesIO("採購單-PO123\n料品代號\t品名\t數量\nA001\tABC\t10".encode("utf-8"))
    mock_instance._get_file_capacity.return_value = "2.34 KB"
    mock_instance._get_document_type.return_value = DocumentType.ORDER

    with patch("fastapi_celery.processors.file_processors.txt_processor.ext_extraction.FileExtensionProcessor",
               return_value=mock_instance):
        yield mock_instance


def test_extract_text_local(mock_file_processor_local, dummy_tracking_model):
    """Test extracting text from local file"""
    txt_content = "採購單-PO123\n料品代號\t品名\t數量\nA001\tABC\t10"
    m_open = mock_open(read_data=txt_content)

    with patch.object(builtins, "open", m_open):
        processor = TXTProcessor(dummy_tracking_model, source=SourceType.LOCAL)
        result = processor.extract_text()

    assert "PO123" in result
    assert mock_file_processor_local._get_file_capacity.called
    assert mock_file_processor_local._get_document_type.called


def test_extract_text_s3(mock_file_processor_s3, dummy_tracking_model):
    """Test extracting text from S3 buffer"""
    processor = TXTProcessor(dummy_tracking_model, source=SourceType.S3)
    result = processor.extract_text()
    assert "PO123" in result
    assert "ABC" in result


def test_parse_file_to_json_basic(mock_file_processor_s3, dummy_tracking_model):
    """Test normal parse from S3 source"""
    processor = TXTProcessor(dummy_tracking_model, source=SourceType.S3)
    parsed = processor.parse_file_to_json()
    print("\n[DEBUG] type(parsed):", type(parsed))
    print("[DEBUG] parsed value:", parsed)

    if isinstance(parsed, dict):
        parsed = PODataParsed(**parsed)
    assert isinstance(parsed, PODataParsed)
    assert parsed.po_number == "PO123"
    assert parsed.items[PO_MAPPING_KEY] == "PO123"
    assert parsed.items["products"][0]["料品代號"] == "A001"
    assert parsed.step_status == StatusEnum.SUCCESS
    assert parsed.capacity == "2.34 KB"


def test_parse_file_with_key_values(monkeypatch, mock_file_processor_s3, dummy_tracking_model):
    """Test parsing with various key-value formats"""
    text_data = """採購單-PO999
單號：T001
日期：2024-10-10\t公司：TestCorp
料品代號\t名稱\t數量
B001\tXYZ\t5
--- Footer line ---
"""
    mock_file_processor_s3.object_buffer = io.BytesIO(text_data.encode("utf-8"))

    processor = TXTProcessor(dummy_tracking_model, source=SourceType.S3)
    parsed = processor.parse_file_to_json()

    assert parsed.po_number == "PO999"
    assert "單號" in parsed.items
    assert parsed.items["公司"] == "TestCorp"
    assert parsed.items["products"][0]["名稱"] == "XYZ"


def test_parse_file_with_incomplete_product(mock_file_processor_s3, dummy_tracking_model):
    """Ensure product lines with missing values are padded"""
    text_data = "採購單-PO777\n料品代號\t品名\t數量\nC001\tSample\n"
    mock_file_processor_s3.object_buffer = io.BytesIO(text_data.encode("utf-8"))

    processor = TXTProcessor(dummy_tracking_model, source=SourceType.S3)
    parsed = processor.parse_file_to_json()

    assert parsed.items["products"][0]["數量"] == ""


def test_parse_file_skip_lines(mock_file_processor_s3, dummy_tracking_model):
    """Ensure lines with '---' or empty are ignored"""
    text_data = """---
採購單-PO555
料品代號\t品名\t數量
D001\tTest\t1
---
"""
    mock_file_processor_s3.object_buffer = io.BytesIO(text_data.encode("utf-8"))
    processor = TXTProcessor(dummy_tracking_model, source=SourceType.S3)
    parsed = processor.parse_file_to_json()
    assert parsed.po_number == "PO555"
    assert len(parsed.items["products"]) == 1


def test_parse_file_no_products(mock_file_processor_s3, dummy_tracking_model):
    """File with no table section"""
    text_data = "採購單-PO321\n單號：T002"
    mock_file_processor_s3.object_buffer = io.BytesIO(text_data.encode("utf-8"))
    processor = TXTProcessor(dummy_tracking_model, source=SourceType.S3)
    parsed = processor.parse_file_to_json()
    assert "products" not in parsed.items


def test_logger_called(monkeypatch, mock_file_processor_s3, dummy_tracking_model):
    """Ensure logging happens"""
    mock_logger = MagicMock()
    monkeypatch.setattr("fastapi_celery.processors.file_processors.txt_processor.logger", mock_logger)

    processor = TXTProcessor(dummy_tracking_model, source=SourceType.S3)
    processor.parse_file_to_json()

    mock_logger.info.assert_any_call("File has been proceeded successfully!")
    mock_logger.info.assert_any_call(
        f"Start processing for file: {dummy_tracking_model.file_path}"
    )
