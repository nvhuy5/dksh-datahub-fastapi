import io
import csv
import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path

from fastapi_celery.processors.file_processors.csv_processor import CSVProcessor, METADATA_SEPARATOR
from fastapi_celery.models.tracking_models import TrackingModel
from models.class_models import DocumentType, PODataParsed, SourceType, StatusEnum


@pytest.fixture
def mock_tracking_model():
    return TrackingModel(
        request_id="123",
        file_path="dummy/path/file.csv",
        project_name="DKSH_TW",
        document_type="order"
    )


@pytest.fixture
def mock_file_processor(monkeypatch):
    """Mock ext_extraction.FileExtensionProcessor used inside CSVProcessor."""
    mock_instance = MagicMock()
    mock_instance._extract_file_extension.return_value = None
    mock_instance._get_document_type.return_value = DocumentType.ORDER
    mock_instance._get_file_capacity.return_value = "small"
    mock_instance.source = "s3"
    mock_instance.object_buffer = io.BytesIO(b"col1,col2\nval1,val2\n")

    mock_class = MagicMock(return_value=mock_instance)
    monkeypatch.setattr(
        "fastapi_celery.processors.file_processors.csv_processor.ext_extraction.FileExtensionProcessor",
        mock_class,
    )
    return mock_class


@patch("fastapi_celery.processors.file_processors.csv_processor.chardet.detect", return_value={"encoding": "utf-8"})
def test_load_csv_rows(mock_detect, mock_tracking_model, mock_file_processor):
    processor = CSVProcessor(mock_tracking_model)
    rows = processor.rows
    assert rows == [["col1", "col2"], ["val1", "val2"]]
    mock_file_processor.assert_called_once()


def test_extract_metadata_found(mock_tracking_model, mock_file_processor):
    processor = CSVProcessor(mock_tracking_model)
    row = [f"Key{METADATA_SEPARATOR}Value"]
    result = processor.extract_metadata(row)
    assert result == {"Key": "Value"}


def test_extract_metadata_not_found(mock_tracking_model, mock_file_processor):
    processor = CSVProcessor(mock_tracking_model)
    row = ["no meta here"]
    result = processor.extract_metadata(row)
    assert result == {}


@pytest.mark.parametrize("row,expected", [
    (["A", "B", "C"], True),
    (["1", "2", "3"], False),
])
def test_is_likely_header(row, expected, mock_tracking_model, mock_file_processor):
    processor = CSVProcessor(mock_tracking_model)
    assert processor.is_likely_header(row) == expected


def test_parse_metadata_rows(mock_tracking_model, mock_file_processor):
    processor = CSVProcessor(mock_tracking_model)
    processor.rows = [
        [f"A{METADATA_SEPARATOR}1"],
        [f"B{METADATA_SEPARATOR}2"],
        ["data"]
    ]
    metadata, index = processor._parse_metadata_rows(0)
    assert metadata == {"A": "1", "B": "2"}
    assert index == 2


def test_identify_header_found(mock_tracking_model, mock_file_processor):
    processor = CSVProcessor(mock_tracking_model)
    processor.rows = [["Header1", "Header2"], ["data1", "data2"]]
    header, next_idx = processor._identify_header(0)
    assert header == ["Header1", "Header2"]
    assert next_idx == 1


def test_identify_header_generate(mock_tracking_model, mock_file_processor):
    processor = CSVProcessor(mock_tracking_model)
    processor.rows = [["1", "2"], ["a", "b"]]
    header, next_idx = processor._identify_header(0)
    assert header == ["col_1", "col_2"]
    assert next_idx == 1


def test_collect_data_block(mock_tracking_model, mock_file_processor):
    processor = CSVProcessor(mock_tracking_model)
    processor.rows = [
        ["val1", "val2"],
        ["val3", "val4"],
        ["meta：1"]
    ]
    header = ["col1", "col2"]
    items, next_idx = processor._collect_data_block(0, header)
    assert items == [{"col1": "val1", "col2": "val2"}, {"col1": "val3", "col2": "val4"}]
    assert next_idx == 2


def test_parse_file_to_json_full_flow(mock_tracking_model, mock_file_processor):
    processor = CSVProcessor(mock_tracking_model)
    processor.rows = [
        [f"meta{METADATA_SEPARATOR}value"],
        ["header1", "header2"],
        ["v1", "v2"],
        ["v3", "v4"]
    ]

    result = processor.parse_file_to_json()
    print(type(result))
    print(result)
    assert isinstance(result, (PODataParsed, dict))

    if isinstance(result, PODataParsed):
        result = result.model_dump()

    assert result["document_type"] in ["order", "master_data"]
    assert len(result["items"]) == 2
    assert result["items"][0] == {"header1": "v1", "header2": "v2"}
    assert result["metadata"] == {"meta": "value"}
