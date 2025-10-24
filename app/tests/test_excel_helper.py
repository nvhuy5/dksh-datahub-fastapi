import os
import pytest
import pandas as pd
from fastapi_celery.processors.helpers.excel_helper import ExcelHelper
from fastapi_celery.models.class_models import SourceType

# ==============================
# Fixtures
# ==============================

@pytest.fixture
def tracking_model_xlsx():
    """TrackingModel cho file .xlsx"""
    class TM:
        def __init__(self):
            self.file_path = os.path.join(
                os.path.dirname(__file__), "samples", "0808fake_xlsx.xlsx"
            )
            self.project_name = "TEST_PROJECT"
            self.bucket_name = "mock-bucket"
            self.request_id = "REQ-001"
    return TM()

@pytest.fixture
def tracking_model_xls():
    """TrackingModel cho file .xls"""
    class TM:
        def __init__(self):
            self.file_path = os.path.join(
                os.path.dirname(__file__), "samples", "0808三友WX.xls"
            )
            self.project_name = "TEST_PROJECT"
            self.bucket_name = "mock-bucket"
            self.request_id = "REQ-002"
    return TM()


# ==============================
# Tests read_rows
# ==============================

def test_read_rows_xlsx(tracking_model_xlsx):
    """Đọc .xlsx với engine openpyxl"""
    helper = ExcelHelper(tracking_model_xlsx, source=SourceType.LOCAL)
    assert isinstance(helper.rows, list)
    assert len(helper.rows) > 0
    assert helper.document_type is not None
    assert helper.capacity is not None

def test_read_rows_xls(tracking_model_xls):
    """Đọc .xls với engine xlrd"""
    helper = ExcelHelper(tracking_model_xls, source=SourceType.LOCAL)
    assert isinstance(helper.rows, list)
    assert len(helper.rows) > 0
    assert helper.document_type is not None
    assert helper.capacity is not None


# ==============================
# Tests extract_metadata
# ==============================

def test_has_inner_metadata(tracking_model_xlsx):
    helper = ExcelHelper(tracking_model_xlsx, SourceType.LOCAL)
    helper.separator = "："
    assert helper._has_inner_metadata("Title(Version：1.0)")
    assert not helper._has_inner_metadata("Title 1.0")

def test_extract_inner_metadata(tracking_model_xlsx):
    helper = ExcelHelper(tracking_model_xlsx, SourceType.LOCAL)
    helper.separator = "："
    metadata = {}
    helper._extract_inner_metadata("Header(Version：2.1)", metadata)
    assert metadata["header"] == "Header(Version：2.1)"
    assert metadata["Version"] == "2.1"

def test_is_url(tracking_model_xlsx):
    helper = ExcelHelper(tracking_model_xlsx, SourceType.LOCAL)
    helper.separator = "："
    assert helper._is_url("https://example.com")
    assert not helper._is_url("not_a_url")

def test_extract_standard_metadata(tracking_model_xlsx):
    helper = ExcelHelper(tracking_model_xlsx, SourceType.LOCAL)
    helper.separator = "："
    metadata = {}

    helper._extract_standard_metadata("Key：Value", 0, ["Key：Value"], metadata)
    assert metadata["Key"] == "Value"

    metadata2 = {}
    helper._extract_standard_metadata("Key：", 0, ["Key：", "NextVal"], metadata2)
    assert metadata2["Key"] == "NextVal"

    metadata3 = {}
    helper._extract_standard_metadata("NoSepCell", 0, ["NoSepCell"], metadata3)
    assert metadata3 == {}


def test_extract_metadata_simple(tracking_model_xlsx):
    helper = ExcelHelper(tracking_model_xlsx, SourceType.LOCAL)
    helper.separator = "："
    row = ["Header(Ver：1.0)", "Owner：Alice", "Empty："]
    result = helper.extract_metadata(row)
    assert "Owner" in result
    assert any(k in result for k in ("Ver", "Version"))

def test_extract_metadata_complex(tracking_model_xlsx):
    helper = ExcelHelper(tracking_model_xlsx, SourceType.LOCAL)
    helper.separator = "："
    row = [
        "Header(Ver：1.0)", 
        "Owner：Alice", 
        "DocLink：https://example.com", 
        "Empty：", 
        "Note：Check"
    ]
    metadata = helper.extract_metadata(row)
    # check inner metadata
    assert metadata["header"] == "Header(Ver：1.0)"
    assert metadata["Ver"] == "1.0"
    # check standard key-value
    assert metadata["Owner"] == "Alice"
    assert metadata["DocLink"] == "https://example.com"
    assert metadata["Note"] == "Check"

def test_extract_metadata_empty(tracking_model_xlsx):
    helper = ExcelHelper(tracking_model_xlsx, SourceType.LOCAL)
    result = helper.extract_metadata(["", "   "])
    assert result == {}


# ==============================
# Misc tests
# ==============================

def test_logger(tracking_model_xlsx):
    helper = ExcelHelper(tracking_model_xlsx, SourceType.LOCAL)
    assert helper.rows is not None

def test_parse_file_to_json(tracking_model_xlsx):
    helper = ExcelHelper(tracking_model_xlsx, SourceType.LOCAL)
    assert helper.parse_file_to_json() is None
