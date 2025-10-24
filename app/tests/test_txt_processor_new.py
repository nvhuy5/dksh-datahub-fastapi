import pytest
from unittest.mock import MagicMock, patch
from fastapi_celery.models.class_models import DocumentType, StatusEnum
from fastapi_celery.processors.file_processors.txt_processor_new import (
    Txt001Template,
    Txt002Template,
    Txt003Template,
    Txt004Template,
)
from fastapi_celery.models.class_models import SourceType, PODataParsed
from fastapi_celery.models.tracking_models import TrackingModel


@pytest.fixture
def dummy_tracking_model(tmp_path):
    return TrackingModel(request_id="req-001", file_path=str(tmp_path / "dummy.txt"))


# === Txt001Template ===
def test_txt001_parse_space_separated_lines(dummy_tracking_model):
    tmpl = Txt001Template(dummy_tracking_model, source=SourceType.S3)
    lines = ["A  B  C", "D  E  F"]
    result = tmpl.parse_space_separated_lines(lines)
    assert result == [
        {"col_1": "A", "col_2": "B", "col_3": "C"},
        {"col_1": "D", "col_2": "E", "col_3": "F"},
    ]


@patch("fastapi_celery.processors.file_processors.txt_processor_new.TxtHelper.parse_file_to_json")
def test_txt001_parse_file_to_json_calls_super(mock_super, dummy_tracking_model):
    tmpl = Txt001Template(dummy_tracking_model, source=SourceType.S3)
    tmpl.parse_file_to_json()
    mock_super.assert_called_once()
    assert callable(mock_super.call_args[0][0])


# === Txt002Template ===
def test_txt002_parse_tab_separated_lines(dummy_tracking_model):
    tmpl = Txt002Template(dummy_tracking_model, source=SourceType.S3)
    lines = ["A\tB\tC", "X\tY\tZ"]
    result = tmpl.parse_tab_separated_lines(lines)
    assert result == [
        {"col_1": "A", "col_2": "B", "col_3": "C"},
        {"col_1": "X", "col_2": "Y", "col_3": "Z"},
    ]


@patch("fastapi_celery.processors.file_processors.txt_processor_new.TxtHelper.parse_file_to_json")
def test_txt002_parse_file_to_json_calls_super(mock_super, dummy_tracking_model):
    tmpl = Txt002Template(dummy_tracking_model, source=SourceType.S3)
    tmpl.parse_file_to_json()
    mock_super.assert_called_once()


# === Txt003Template ===
def test_txt003_parse_space_separated_lines(dummy_tracking_model):
    tmpl = Txt003Template(dummy_tracking_model, source=SourceType.S3)
    lines = ["A B C", "1 2 3"]
    result = tmpl.parse_space_separated_lines(lines)
    assert result == [
        {"col_1": "A", "col_2": "B", "col_3": "C"},
        {"col_1": "1", "col_2": "2", "col_3": "3"},
    ]


@patch("fastapi_celery.processors.file_processors.txt_processor_new.TxtHelper.parse_file_to_json")
def test_txt003_parse_file_to_json_calls_super(mock_super, dummy_tracking_model):
    tmpl = Txt003Template(dummy_tracking_model, source=SourceType.S3)
    tmpl.parse_file_to_json()
    mock_super.assert_called_once()


# === Txt004Template ===
def test_txt004_parse_tabular_data_with_headers(dummy_tracking_model):
    tmpl = Txt004Template(dummy_tracking_model, source=SourceType.S3)
    lines = [
        "2024.07.11  Dynamic List Display",
        "HeaderText\tBatch\tQty",
        "value1\tbatch1\t10",
        "value2\tbatch2",
    ]
    result = tmpl.parse_tabular_data_with_headers(lines)
    assert result == [
        {"HeaderText": "value1", "Batch": "batch1", "Qty": "10"},
        {"HeaderText": "value2", "Batch": "batch2", "Qty": ""},
    ]


@patch("fastapi_celery.processors.file_processors.txt_processor_new.TxtHelper.parse_file_to_json")
def test_txt004_parse_file_to_json_calls_super(mock_super, dummy_tracking_model):
    tmpl = Txt004Template(dummy_tracking_model, source=SourceType.S3)
    tmpl.parse_file_to_json()
    mock_super.assert_called_once()


# === Integration sanity check ===
def test_integration_all_templates_call_super(monkeypatch, dummy_tracking_model):
    """Ensure all subclasses can call parse_file_to_json without errors"""
    dummy_return = PODataParsed(
        original_file_path=dummy_tracking_model.file_path,
        document_type=DocumentType.ORDER,
        po_number="PO123",
        items={"a": 1},
        metadata=None,
        step_status=StatusEnum.SUCCESS,
        messages=None,
        capacity="1 KB",
    )

    called = {}

    def dummy_super(self, func):
        called[self.__class__.__name__] = True
        return dummy_return

    monkeypatch.setattr("processors.file_processors.txt_processor_new.TxtHelper.parse_file_to_json", dummy_super)

    from fastapi_celery.processors.file_processors.txt_processor_new import (
        Txt001Template,
        Txt002Template,
        Txt003Template,
        Txt004Template,
    )

    # Instantiate all templates and ensure parse_file_to_json executes
    templates = [
        Txt001Template(dummy_tracking_model),
        Txt002Template(dummy_tracking_model),
        Txt003Template(dummy_tracking_model),
        Txt004Template(dummy_tracking_model),
    ]

    for template in templates:
        result = template.parse_file_to_json()
        assert result == dummy_return
        assert template.__class__.__name__ in called