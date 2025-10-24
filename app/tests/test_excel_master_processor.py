import pytest
from pathlib import Path
from models.class_models import MasterDataParsed, StatusEnum, SourceType
from fastapi_celery.processors.master_processors.excel_master_processor import ExcelMasterProcessor


# === Fixtures ===

@pytest.fixture
def fake_tracking_model():
    class FakeTrackingModel:
        def __init__(self, file_path: str):
            self.file_path = file_path
            self.project_name = "TEST_PROJECT"
            self.bucket_name = "mock-bucket"
            self.request_id = "REQ-001"

    return FakeTrackingModel


@pytest.fixture
def processor(fake_tracking_model):
    model = fake_tracking_model("tests/samples/0808fake_xlsx.xlsx")
    return ExcelMasterProcessor(tracking_model=model, source=SourceType.LOCAL)


def _ensure_parsed_object(result):
    if isinstance(result, MasterDataParsed):
        return result
    elif isinstance(result, dict):
        return MasterDataParsed(**result)
    else:
        raise AssertionError(f"Unexpected return type: {type(result)}")
    

# === Tests ===

def test_parse_file_to_json_success(processor):
    processor.rows = [
        ["Customer：DKSH"],
        ["Code", "Name", "Age"],
        ["001", "John", "30"],
        ["002", "Anna", "25"],
    ]

    result = _ensure_parsed_object(processor.parse_file_to_json())

    assert isinstance(result, MasterDataParsed)
    assert result.step_status == StatusEnum.SUCCESS
    assert result.headers == ["Code", "Name", "Age"]
    assert len(result.items) == 2
    assert result.items[0]["Code"] == "001"
    assert result.items[1]["Name"] == "Anna"


def test_parse_file_to_json_metadata_only(processor):
    processor.rows = [
        ["DocType：Master Data"],
        ["Version：1.0"],
    ]

    result = _ensure_parsed_object(processor.parse_file_to_json())
    assert result.step_status == StatusEnum.SUCCESS
    assert result.headers == []
    assert result.items == []


def test_parse_file_to_json_exception(processor, monkeypatch):
    processor.rows = [["Bad", "Row"]]

    # Hàm giả thay cho extract_metadata
    def raise_error(*args, **kwargs):
        raise Exception("mock error")

    # Monkeypatch method extract_metadata
    monkeypatch.setattr(processor, "extract_metadata", raise_error)

    result = _ensure_parsed_object(processor.parse_file_to_json())
    assert result.step_status == StatusEnum.FAILED
    assert isinstance(result.messages, list)
    assert any("mock error" in msg for msg in result.messages)


def test_extract_table_block_with_metadata(processor):
    processor.rows = [
        ["Code", "Name"],
        ["001", "John"],
        ["002", "Anna"],
        ["Version：1.0"]
    ]

    headers = ["Code", "Name"]
    table_block, next_index, metadata = processor._extract_table_block(1, headers)

    assert len(table_block) == 2
    assert next_index > 1
    assert metadata == {"Version": "1.0"}


def test_clean_row_strip(processor):
    row = ["  Code ", " Name  ", " Age "]
    cleaned = processor._clean_row(row)
    assert cleaned == ["Code", "Name", "Age"]

@pytest.mark.parametrize("sample_file", [
    "0808fake_xlsx.xlsx",
    "0808三友WX.xls",
])
def test_real_excel_files_exist(sample_file):
    path = Path("tests/samples") / sample_file
    assert path.exists(), f"Sample file missing: {path}"
