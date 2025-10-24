from models.tracking_models import TrackingModel
from utils import log_helpers
from models.class_models import SourceType, PODataParsed, StatusEnum
from processors.helpers import excel_helper

PO_MAPPING_KEY = ""


# === Set up logging ===
logger = log_helpers.get_logger("Excel Processor")


class ExcelProcessor(excel_helper.ExcelHelper):
    """Processor for handling Excel file operations.

    Initializes with a file path and source type, reads rows from the file,
    and provides methods to parse the content into JSON format.
    """

    def __init__(self, tracking_model: TrackingModel, source: SourceType = SourceType.S3):
        """Initialize the Excel processor with a file path and source type.

        Args:
            file (Path): The path to the Excel file.
            source (SourceType, optional): The source type, defaults to SourceType.S3.
        """
        super().__init__(tracking_model=tracking_model, source=source)
        self.po_number = None

    def parse_file_to_json(self) -> PODataParsed:  # NOSONAR
        """Parse the Excel file content into a JSON-compatible dictionary.

        Extracts metadata and table data from rows, handling key-value pairs
        and table structures separated by METADATA_SEPARATOR.

        Returns:
            PODataParsed: PODataParsed object
        """
        metadata = {}
        items = []
        i = 0

        while i < len(self.rows):
            row = [str(cell).strip() for cell in self.rows[i]]
            key_value_pairs = self.extract_metadata(row)

            if key_value_pairs:
                metadata.update(key_value_pairs)
                i += 1
                continue

            # Start checking for table data
            header_row = row
            table_block = []
            j = i + 1

            while j < len(self.rows):
                current_row = [str(cell).strip() for cell in self.rows[j]]
                kv_pairs = self.extract_metadata(current_row)

                if kv_pairs:
                    metadata.update(kv_pairs)
                    break

                if len(current_row) == len(header_row):
                    table_block.append(current_row)
                    j += 1
                else:
                    break

            if table_block:
                headers = header_row
                for row_data in table_block:
                    items.append(dict(zip(headers, row_data)))
                i = j
            else:
                i += 1
        return PODataParsed(
            original_file_path=self.tracking_model.file_path,
            document_type=getattr(self, "document_type", None),
            po_number=self.po_number,
            items=items,
            metadata=metadata,
            step_status = StatusEnum.SUCCESS,
            messages=None,
            capacity=getattr(self, "capacity", None),
        )
