import io
import csv
import chardet
from typing import List, Optional, Tuple
from models.class_models import PODataParsed, StatusEnum
import config_loader

METADATA_SEPARATOR = config_loader.get_env_variable("METADATA_SEPARATOR", "：")



class CSVProcessor:
    """Processor for handling CSV PO template."""

    def __init__(self, file_record: dict):
        """Initialize with CSV file path and source type.

        Args:
            file_path (Path): The path to the CSV file.
            source (SourceType, optional): The source type, defaults to SourceType.S3.
        """
        self.file_record = file_record
        self.po_number = None
        self.rows = self.load_csv_rows()

    def load_csv_rows(self) -> List[List[str]]:
        """Load rows from a CSV file.

        Returns:
            list: A list of non-empty rows from the CSV file.
        """
        self.document_type = self.file_record.document_type
        self.file_size = self.file_record.file_size

        if self.file_record.source_type == "local":
            with open(self.file_record.file_path, "rb") as csv_file:
                content = csv_file.read()
        else:
            self.file_record.object_buffer.seek(0)
            content = self.file_record.object_buffer.read()

        detected = chardet.detect(content)
        encoding = detected["encoding"] or "utf-8"
        decoded_content = io.TextIOWrapper(
            io.BytesIO(content), encoding=encoding, errors="replace"
        )

        reader = csv.reader(decoded_content)
        return [row for row in reader if any(cell.strip() for cell in row)]

    def extract_metadata(self, row: List[str]) -> dict:
        """Extract key-value pairs from a row if it contains metadata.

        Args:
            row (list[str]): A list of cells in the row.

        Returns:
            dict: A dictionary of key-value pairs extracted from the row.
        """
        for cell in row:
            if METADATA_SEPARATOR in cell:
                key, val = cell.split(METADATA_SEPARATOR, 1)
                return {key.strip(): val.strip()}
        return {}

    def is_likely_header(self, row: List[str]) -> bool:
        """Heuristic: return True if most fields are non-numeric and non-empty.

        Args:
            row (List[str]): A list of cells in the row.

        Returns:
            bool: True if at least 70% of fields are non-numeric and non-empty.
        """
        non_numeric_count = sum(
            1 for cell in row if cell and not cell.replace(".", "", 1).isdigit()
        )
        return non_numeric_count >= len(row) * 0.7

    def parse_file_to_json(self) -> PODataParsed:
        """Parse the CSV content into MasterDataParsed."""
        metadata, i = self._parse_metadata_rows(0)
        items: List[dict] = []
        header_row: Optional[List[str]] = None

        while i < len(self.rows):
            if not header_row:
                header_row, i = self._identify_header(i)
                continue

            row = [cell.strip() for cell in self.rows[i]]
            if len(row) != len(header_row):
                i += 1
                continue

            block, i = self._collect_data_block(i, header_row)
            items.extend(block)

        return PODataParsed(
            original_file_path=self.file_record.file_path,
            document_type=self.document_type,
            po_number=self.po_number,
            items=items,
            step_status = StatusEnum.SUCCESS,
            messages=None,
            metadata=metadata,
            capacity=self.file_size,
        )

    def _parse_metadata_rows(self, start_index: int) -> Tuple[dict, int]:
        metadata = {}
        i = start_index
        while i < len(self.rows):
            row = [cell.strip() for cell in self.rows[i]]
            key_value = self.extract_metadata(row)
            if key_value:
                metadata.update(key_value)
                i += 1
            else:
                break
        return metadata, i

    def _identify_header(self, i: int) -> Tuple[Optional[List[str]], int]:
        while i < len(self.rows):
            row = [cell.strip() for cell in self.rows[i]]
            if self.is_likely_header(row):
                return row, i + 1
            elif len(row) > 0:
                header = [f"col_{idx + 1}" for idx in range(len(row))]
                return header, i + 1
            i += 1
        return None, i

    def _collect_data_block(
        self, start: int, header: List[str]
    ) -> Tuple[List[dict], int]:
        items = []
        j = start
        while j < len(self.rows):
            row = [cell.strip() for cell in self.rows[j]]
            if self.extract_metadata(row):
                break
            if len(row) == len(header):
                items.append(dict(zip(header, row)))
                j += 1
            else:
                break
        return items, j
