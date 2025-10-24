import pytest
from pydantic import BaseModel

from fastapi_celery.processors.helpers.template_helper import parse_data
from fastapi_celery.models.class_models import DocumentType
from models.class_models import MasterDataParsed, PODataParsed


# === Mock data models ===

@pytest.fixture
def po_data_dict():
    return {
        "original_file_path": "path/to/file.txt",
        "document_type": "order",
        "po_number": "PO123",
        "items": {"a": 1},
        "metadata": None,
        "step_status": "1",
        "messages": None,
        "capacity": "3 KB",
    }


@pytest.fixture
def master_data_dict():
    return {
        "original_file_path": "path/to/master.txt",
        "document_type": "master_data",
        "headers": ["col1", "col2"],
        "items": {"data": "ok"},
        "metadata": None,
        "step_status": "1",
        "messages": None,
        "capacity": "5 KB",
    }


# === Tests ===

def test_parse_data_with_order_document(po_data_dict):
    """Should parse PODataParsed when document_type=ORDER"""
    result = parse_data(DocumentType.ORDER, po_data_dict)
    assert isinstance(result, PODataParsed)
    assert result.document_type == DocumentType.ORDER.value
    assert result.po_number == "PO123"


def test_parse_data_with_master_data_document(master_data_dict):
    """Should parse MasterDataParsed when document_type=MASTER_DATA"""
    result = parse_data(DocumentType.MASTER_DATA, master_data_dict)
    assert isinstance(result, MasterDataParsed)
    assert result.document_type == DocumentType.MASTER_DATA.value


def test_parse_data_with_custom_type(po_data_dict):
    """Should use provided custom_type instead of default mapping"""

    class DummyModel(BaseModel):
        x: int
        y: str

    dummy_data = {"x": 1, "y": "test"}

    result = parse_data(DocumentType.ORDER, dummy_data, custom_type=DummyModel)
    assert isinstance(result, DummyModel)
    assert result.x == 1
    assert result.y == "test"


def test_parse_data_with_pydantic_input(po_data_dict):
    """Should accept already-initialized BaseModel (auto-dump and rebuild)"""
    po_instance = PODataParsed(**po_data_dict)
    result = parse_data(DocumentType.ORDER, po_instance)
    assert isinstance(result, PODataParsed)
    assert result.po_number == "PO123"


def test_parse_data_with_none_input():
    """Should raise ValueError when data=None"""
    with pytest.raises(ValueError, match="Input data is None"):
        parse_data(DocumentType.ORDER, None)


def test_parse_data_with_unknown_document_type(po_data_dict):
    """Should raise ValueError for unsupported document type"""
    with pytest.raises(ValueError, match="Unknown document type"):
        parse_data("INVALID_TYPE", po_data_dict)
