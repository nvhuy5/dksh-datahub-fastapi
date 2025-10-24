import traceback
from pathlib import Path
from models.tracking_models import TrackingModel
from models.class_models import DocumentType, SourceType, MasterDataParsed, StatusEnum
from utils import ext_extraction


class TxtMasterProcessor:
    """Processor for handling master data files.

    Initializes with a file path and source type, parses the file into JSON format,
    and uploads the result to S3.
    """

    def __init__(self, tracking_model: TrackingModel, source: SourceType = SourceType.S3):
        """Initialize the master data processor with a file path and source type.

        Args:
            file_path (Path): The path to the master data file.
            source (SourceType, optional): The source type, defaults to SourceType.S3.
        """
        self.tracking_model = tracking_model
        self.file_object = None
        self.source = source

    # Text to json
    def parse_file_to_json(self) -> MasterDataParsed:
        """Parse the master data file into a JSON-compatible dictionary.

        Reads the file content from local or S3, splits it into blocks,
        extracts headers and items, and uploads the result to S3.

        Returns:
            Dict[str, Any]: A dictionary containing the original file path,
                headers (Dict[str, List[str]]), items (Dict[str, List[Dict[str, str]]]),
                and capacity (str).
        """
        try:
            file_object = ext_extraction.FileExtensionProcessor(tracking_model=self.tracking_model, source=self.source)
            document_type = file_object._get_document_type()
            capacity = file_object._get_file_capacity()
            original_file_path = self.tracking_model.file_path

            text = self._read_file_content(file_object)
            headers, items = self._parse_text_blocks(text)

            return MasterDataParsed(
                original_file_path=original_file_path,
                headers=headers,
                document_type=document_type,
                items=items,
                step_status=StatusEnum.SUCCESS,
                messages=None,
                capacity=capacity,
            )

        except Exception as e:
            print(f"Error while parsing file to JSON: {e}")
            return MasterDataParsed(
                original_file_path=self.tracking_model.file_path,
                headers=[],
                document_type=DocumentType.MASTER_DATA,
                items=[],
                step_status=StatusEnum.FAILED,
                messages=[traceback.format_exc()],
                capacity="unknown",
            )

    def _read_file_content(self, file_object) -> str:
        if file_object.source == "local":
            with open(file_object.file_path, "r", encoding="utf-8") as f:
                return f.read()
        else:
            file_object.object_buffer.seek(0)
            return file_object.object_buffer.read().decode("utf-8")

    def _parse_text_blocks(self, text: str) -> tuple[dict, dict]:
        headers = {}
        items = {}

        blocks = text.strip().split("# Table: ")
        for block in blocks:
            if not block.strip():
                continue

            lines = block.strip().splitlines()
            if len(lines) < 2:
                continue  # Invalid block

            table_name = lines[0].strip()
            table_headers = [col.strip() for col in lines[1].split("|")]
            headers[table_name] = table_headers

            table_items = []
            for row in lines[2:]:
                values = [v.strip() for v in row.split("|")]
                # Only include rows that match header length
                if len(values) == len(table_headers):
                    item = dict(zip(table_headers, values))
                    table_items.append(item)

            items[table_name] = table_items

        return headers, items
