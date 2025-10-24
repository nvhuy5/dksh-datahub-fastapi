import pytest
import importlib
from unittest.mock import patch, MagicMock
from pathlib import Path


class DummyTrackingModel:
    """Dummy class mô phỏng TrackingModel."""
    def __init__(self, file_path="/tmp/testfile.pdf"):
        self.file_path = file_path

    def model_dump(self):
        return {"file_path": self.file_path}


class TestFileProcessor:
    @patch("fastapi_celery.processors.workflow_processors.extract_metadata.ext_extraction.FileExtensionProcessor")
    def test_extract_metadata_success(self, mock_ext_processor: MagicMock) -> None:
        """Test successful metadata extraction."""
        def side_effect(key):
            values = {
                "request_id": "test-request-id",
                "workflow_id": "test-workflow",
                "document_number": "DOC123",
            }
            return values.get(key, None)

        mock_instance = MagicMock()
        mock_instance.file_path_parent = "/tmp"
        mock_instance.file_name = "testfile.pdf"
        mock_instance.file_extension = ".pdf"
        mock_instance.document_type = "order"
        mock_instance.target_bucket_name = "bucket"
        mock_ext_processor.return_value = mock_instance

        from fastapi_celery.processors import processor_base as file_processor_reload
        importlib.reload(file_processor_reload)

        processor = file_processor_reload.ProcessorBase("/tmp/testfile.pdf")
        processor.tracking_model = DummyTrackingModel("/tmp/testfile.pdf")

        result = processor.extract_metadata()
        # FIX: check enum name instead of value
        assert result.step_status.name == "SUCCESS"

    @patch(
        "fastapi_celery.processors.workflow_processors.extract_metadata.ext_extraction.FileExtensionProcessor",
        side_effect=FileNotFoundError("not found"),
    )
    def test_extract_metadata_file_not_found(self, mock_ext_processor: MagicMock) -> None:
        from fastapi_celery.processors import processor_base as file_processor_reload
        importlib.reload(file_processor_reload)

        processor = file_processor_reload.ProcessorBase("/tmp/testfile.pdf")
        processor.tracking_model = DummyTrackingModel("/tmp/testfile.pdf")

        result = processor.extract_metadata()
        assert result.step_status.name == "FAILED"

    @patch(
        "fastapi_celery.processors.workflow_processors.extract_metadata.ext_extraction.FileExtensionProcessor",
        side_effect=ValueError("bad value"),
    )
    def test_extract_metadata_value_error(self, mock_ext_processor: MagicMock) -> None:
        from fastapi_celery.processors import processor_base as file_processor_reload
        importlib.reload(file_processor_reload)

        processor = file_processor_reload.ProcessorBase("/tmp/testfile.pdf")
        processor.tracking_model = DummyTrackingModel("/tmp/testfile.pdf")

        result = processor.extract_metadata()
        assert result.step_status.name == "FAILED"

    @patch(
        "fastapi_celery.processors.workflow_processors.extract_metadata.ext_extraction.FileExtensionProcessor",
        side_effect=Exception("unknown"),
    )
    def test_extract_metadata_exception(self, mock_ext_processor: MagicMock) -> None:
        from fastapi_celery.processors import processor_base as file_processor_reload
        importlib.reload(file_processor_reload)

        processor = file_processor_reload.ProcessorBase("/tmp/testfile.pdf")
        processor.tracking_model = DummyTrackingModel("/tmp/testfile.pdf")

        result = processor.extract_metadata()
        assert result.step_status.name == "FAILED"


from fastapi_celery.utils import ext_extraction


def test_valid_extension(monkeypatch, base_path: Path = Path("app/tests/samples")) -> None:
    """Patch lại để chạy đúng với file_path string."""
    class DummyTracking:
        def __init__(self, file_path):
            self.file_path = file_path
            self.project_name = "DKSH_TW" 

    monkeypatch.setattr("os.path.isfile", lambda path: True)

    from fastapi_celery.utils import ext_extraction
    monkeypatch.setattr(ext_extraction, "TrackingModel", DummyTracking)

    file_path = base_path / "0C-RLBH75-K0.pdf"
    file_processor = ext_extraction.FileExtensionProcessor(
        DummyTracking(str(file_path)), source="local"
    )

    assert file_processor.file_extension == ".pdf"
    assert file_processor.file_name.endswith(".pdf")

