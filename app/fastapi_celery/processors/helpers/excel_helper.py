import re
from io import BytesIO
import pandas as pd
from pathlib import Path
from typing import List, Dict
from models.tracking_models import TrackingModel
from utils import ext_extraction
from models.class_models import SourceType, PODataParsed, MasterDataParsed
import config_loader

METADATA_SEPARATOR = config_loader.get_env_variable("METADATA_SEPARATOR", "：")
PO_MAPPING_KEY = ""


class ExcelHelper:
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
        self.tracking_model = tracking_model
        self.source = source
        self.rows = self.read_rows()
        self.separator = METADATA_SEPARATOR
        self.po_number = None

    def read_rows(self) -> List[List[str]]:
        """Read rows from the Excel file based on its source.

        Extracts the file extension, loads the file from local or S3,
        and returns a list of non-empty rows from all sheets.

        Returns:
            List[List[str]]: A list of rows, where each row is a list of strings.
        """
        file_object = ext_extraction.FileExtensionProcessor(
            tracking_model=self.tracking_model, source=self.source
        )
        file_object._extract_file_extension()
        self.document_type = file_object._get_document_type()
        self.capacity = file_object._get_file_capacity()
        ext = file_object.file_extension

        # Load file from local or S3
        if file_object.source == "local":
            file_input = file_object.file_path  # this is Path
        else:
            file_object.object_buffer.seek(0)
            file_input = BytesIO(file_object.object_buffer.read())  # this is BytesIO

        # Choose engine based on extension
        if ext == ".xls":
            df_dict = pd.read_excel(
                file_input, sheet_name=None, header=None, dtype=str, engine="xlrd"
            )
        else:
            df_dict = pd.read_excel(
                file_input, sheet_name=None, header=None, dtype=str, engine="openpyxl"
            )

        # Extract all rows from all sheets
        all_rows = []
        for sheet_name, sheet_df in df_dict.items():
            sheet_df.fillna("", inplace=True)
            all_rows.extend(sheet_df.astype(str).values.tolist())

        return [row for row in all_rows if any(cell.strip() for cell in row)]

    def extract_metadata(self, row: List[str]) -> Dict[str, str]:
        """Extract metadata from a row based on the METADATA_SEPARATOR.

        Handles key-value pairs within cells, including special cases with parentheses,
        and returns a dictionary of extracted metadata.

        Args:
            row (List[str]): A list of cells representing a row.

        Returns:
            Dict[str, str]: A dictionary of metadata key-value pairs.
        """
        metadata = {}
        cells = [cell.strip() for cell in row if cell.strip()]
        if not cells:
            return metadata

        handled_cells = set()

        for cell in cells:
            if self._has_inner_metadata(cell):
                self._extract_inner_metadata(cell, metadata)
                handled_cells.add(cell)

        for idx, cell in enumerate(cells):
            if cell in handled_cells or self._is_url(cell):
                continue
            self._extract_standard_metadata(cell, idx, cells, metadata)

        return metadata

    def _has_inner_metadata(self, cell: str) -> bool:
        return (
            re.search(rf"\(([^()]*?{re.escape(self.separator)}[^()]*)\)", cell)
            is not None
        )

    def _extract_inner_metadata(self, cell: str, metadata: Dict[str, str]) -> None:
        match = re.search(rf"(.*)\(([^()]*?{re.escape(self.separator)}[^()]*)\)", cell)
        if match:
            metadata["header"] = cell
            inner = match.group(2)
            key, value = map(str.strip, inner.split(self.separator, 1))
            if key:
                metadata[key] = value or None

    def _is_url(self, value: str) -> bool:
        if not isinstance(value, str):
            return False
        return value.lower().startswith("http")

    def _extract_standard_metadata(
        self, cell: str, idx: int, cells: List[str], metadata: Dict[str, str]
    ) -> None:
        if self.separator not in cell:
            return
        key, value = map(str.strip, cell.split(self.separator, 1))
        if not value and idx + 1 < len(cells):
            value = cells[idx + 1].strip()
        if key:
            metadata[key] = value or None

    def parse_file_to_json(self) -> PODataParsed | MasterDataParsed:
        # This method is intentionally left blank.
        # Subclasses or future implementations should override this to provide validation logic.
        pass
