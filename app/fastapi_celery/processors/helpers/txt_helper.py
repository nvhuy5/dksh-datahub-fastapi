from pathlib import Path
import logging

from models.tracking_models import TrackingModel
from models.class_models import SourceType, PODataParsed, StatusEnum
from utils import log_helpers, ext_extraction


class TxtHelper:
    """
    Base class for TXT processors. Handles file extraction and common operations.
    """

    def __init__(
        self,
        tracking_model: TrackingModel,
        source: SourceType = SourceType.S3,
        encoding: str = "utf-8",
    ):
        """
        Initialize the TXT processor with file path, source type, and encoding.
        """
        self.tracking_model = tracking_model
        self.source = source
        self.encoding = encoding
        self.capacity = None
        self.document_type = None

    def extract_text(self) -> str:
        """
        Extract and return the text content of the file using the specified encoding.
        """
        file_object = ext_extraction.FileExtensionProcessor(tracking_model=self.tracking_model, source=self.source)

        self.capacity = file_object._get_file_capacity()
        self.document_type = file_object._get_document_type()

        if file_object.source == "local":
            with open(file_object.file_path, "r", encoding=self.encoding) as f:
                return f.read()
        else:
            file_object.object_buffer.seek(0)
            return file_object.object_buffer.read().decode(self.encoding)

    def parse_file_to_json(self, parse_func) -> PODataParsed:
        """
        Extract text, parse lines with given function, and return structured output.
        """
        text = self.extract_text()
        lines = text.splitlines()
        items = parse_func(lines)

        return PODataParsed(
            original_file_path=self.tracking_model.file_path,
            document_type=self.document_type,
            po_number=str(len(items)),
            items=items,
            metadata=None,
            step_status = StatusEnum.SUCCESS,
            messages=None,
            capacity=self.capacity,
        )
