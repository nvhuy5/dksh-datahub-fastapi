# tests/test_txt_master_processor.py
import pytest
from pathlib import Path
from fastapi_celery.processors.master_processors.txt_master_processor import TxtMasterProcessor
from fastapi_celery.models.class_models import SourceType, StatusEnum, DocumentType
from models.class_models import MasterDataParsed

# ====== Fake TrackingModel ======
class FakeTrackingModel:
    def __init__(self, file_path):
        self.file_path = Path(file_path)

# ====== Fake FileExtensionProcessor ======
class FakeFileProcessor:
    def __init__(self, tracking_model, source):
        self.tracking_model = tracking_model
        self.source = source
        self.file_path = tracking_model.file_path
        self.object_buffer = None

    def _get_document_type(self):
        return DocumentType.MASTER_DATA

    def _get_file_capacity(self):
        return "10KB"

class FakeFileProcessorForException:
    def __init__(self, tracking_model, source):
        self.file_path = tracking_model.file_path
        self.source = source
        self._document_type = DocumentType.MASTER_DATA
        self._capacity = "unknown"

    def _get_document_type(self):
        return self._document_type

    def _get_file_capacity(self):
        return self._capacity

# ====== Fixtures ======
@pytest.fixture
def tracking_model():
    return FakeTrackingModel("tests/samples/SAP_Master_data.txt")

@pytest.fixture
def fake_file_processor(monkeypatch):
    monkeypatch.setattr(
        "fastapi_celery.processors.master_processors.txt_master_processor.ext_extraction.FileExtensionProcessor",
        FakeFileProcessor
    )

# ====== Tests ======
def test_parse_file_to_json_success(tracking_model, fake_file_processor):
    processor = TxtMasterProcessor(tracking_model, SourceType.LOCAL)
    result = processor.parse_file_to_json()
    assert isinstance(result, MasterDataParsed)
    assert result.step_status == StatusEnum.SUCCESS
    assert result.document_type == DocumentType.MASTER_DATA
    assert result.capacity == "10KB"
    assert result.headers != {}
    assert result.items != {}

def test_parse_file_to_json_multiple_tables(tracking_model, fake_file_processor):
    processor = TxtMasterProcessor(tracking_model, SourceType.LOCAL)
    text = """# Table: Products
ID | Name | Price
1 | A | 10
2 | B | 20

# Table: Sales
SaleID | ProductID | Qty
100 | 1 | 5
101 | 2 | 3
"""
    processor._read_file_content = lambda x: text
    result = processor.parse_file_to_json()
    assert "Products" in result.headers
    assert "Sales" in result.headers
    assert len(result.items["Products"]) == 2
    assert len(result.items["Sales"]) == 2

def test_parse_file_to_json_exception(monkeypatch, tracking_model):
    def raise_exception(tracking_model, source):
        _ = FakeFileProcessorForException(tracking_model, source)
        raise ValueError("Forced Error")

    monkeypatch.setattr(
        "fastapi_celery.processors.master_processors.txt_master_processor.ext_extraction.FileExtensionProcessor",
        raise_exception
    )
    processor = TxtMasterProcessor(tracking_model, SourceType.LOCAL)
    result = processor.parse_file_to_json()
    assert result.step_status == StatusEnum.FAILED
    assert result.document_type == DocumentType.MASTER_DATA
    assert result.capacity == "unknown"
    assert any("Forced Error" in m for m in result.messages)

def test_parse_text_blocks_various_cases(tracking_model, fake_file_processor):
    processor = TxtMasterProcessor(tracking_model, SourceType.LOCAL)
    text = """# Table: Test
Col1 | Col2
1 | A
2 | B
"""
    headers, items = processor._parse_text_blocks(text)
    assert headers == {"Test": ["Col1", "Col2"]}
    assert items == {"Test": [{"Col1": "1", "Col2": "A"}, {"Col1": "2", "Col2": "B"}]}

def test_parse_text_blocks_invalid_rows(tracking_model, fake_file_processor):
    processor = TxtMasterProcessor(tracking_model, SourceType.LOCAL)
    text = """# Table: Test
Col1 | Col2
1
2 | B
"""
    headers, items = processor._parse_text_blocks(text)
    assert headers == {"Test": ["Col1", "Col2"]}
    assert items == {"Test": [{"Col1": "2", "Col2": "B"}]}

def test_read_file_content_local(tracking_model, fake_file_processor):
    processor = TxtMasterProcessor(tracking_model, SourceType.LOCAL)
    fake_fp = FakeFileProcessor(tracking_model, SourceType.LOCAL)
    content = processor._read_file_content(fake_fp)
    assert isinstance(content, str)
    assert len(content) > 0
