import re
import pymupdf as fitz
import pdfplumber
from typing import List, Dict, Any, Tuple, Optional
from models.tracking_models import TrackingModel
from utils import ext_extraction
from utils import log_helpers
from models.class_models import SourceType, PODataParsed, StatusEnum
from processors.helpers.pdf_helper import build_success_response, build_failed_response


# === Set up logging ===
logger = log_helpers.get_logger("PDF Processor")

LINE_ENDS_WITH_COLON_PATTERN = re.compile(r".+[：:]\s*$")
KV_PATTERN = re.compile(r"([^：:\s]+)[：:]\s*")


class Pdf001Template:
    """
    PDF Processor to extract file name 0C-RLBH75-K0.pdf
    """

    def __init__(self, file_record: dict):
        self.file_record = file_record
        self.po_number = None

    def extract_metadata_from_lines(self, text_lines: List[str]) -> Dict[str, Any]:  # pragma: no cover  # NOSONAR
        metadata = {}
        prev_key = None

        for line in text_lines:
            line = line.strip()
            if not line:
                continue

            segments = re.split(r'\t+|\s{2,}', line)
            found_kv = False

            for seg in segments:
                match = re.match(r"(.+?)[：:]\s*(.*)", seg)
                if match:
                    key = match.group(1).strip()
                    value = match.group(2).strip()
                    metadata[key] = value
                    found_kv = True

            if not found_kv:
                if prev_key:
                    metadata[prev_key] = line
                    prev_key = None
                elif LINE_ENDS_WITH_COLON_PATTERN.match(line):
                    prev_key = re.split(r"[：:]", line)[0].strip()
                    metadata[prev_key] = ""

        return metadata

    def extract_tables(self, text_lines: List[str]) -> List[Dict[str, Any]]:
        """
        Extract table rows from text lines based on the provided structure.

        Args:
            text_lines (List[str]): All text lines in the PDF.

        Returns:
            List[Dict[str, Any]]: Table rows.
        """
        # Collect headers
        headers = []
        collecting_header = False
        for line in text_lines:
            if line.startswith("幣別"):
                collecting_header = True
            elif collecting_header and line.startswith("-----"):
                break
            elif collecting_header and line:
                headers.append(line.strip())
        # Collect items
        items = []
        collecting_item = False
        for line in text_lines:
            if line.startswith("-----"):
                collecting_item = True
            elif collecting_item and line == "- / -":
                break
            elif collecting_item and line:
                items.append(line.strip())
        row_dict = dict(zip(headers, items))
        return [row_dict]

    def parse_file_to_json(self) -> PODataParsed:
        """
        Process PDF into metadata and items using dynamic parsing.

        Returns:
            PODataParsed: Extracted data object.
        """
        try:
            # file_object = ext_extraction.FileExtensionProcessor(
            #     tracking_model=self.tracking_model,
            #     source=self.source
            # )
            # capacity = file_object._get_file_capacity()
            # document_type = file_object._get_document_type()

            if self.file_record.source_type == "local":
                doc = fitz.open(self.file_record.file_path)
            else:
                self.file_record.object_buffer.seek(0)
                doc = fitz.open(
                    stream=self.file_record.object_buffer.read(),
                    filetype="pdf"
                )

            logger.info(f"Start processing for file: {self.file_record.file_path}")

            # Extract text from all pages
            full_text_lines = []
            for page in doc:
                text = page.get_text()
                lines = [line.strip() for line in text.split("\n") if line.strip()]
                full_text_lines.extend(lines)

            doc.close()

            # Extract metadata and items
            metadata = self.extract_metadata_from_lines(full_text_lines)
            items = self.extract_tables(full_text_lines)

            self.po_number = metadata.get("訂購編號")

            return build_success_response(
                self.file_record.file_path, self.file_record.document_type, self.po_number, items, metadata, self.file_record.file_size
            )

        except Exception as e:
            return build_failed_response(
                self.file_record.file_path,
                getattr(self, "document_type", None),
                getattr(self, "capacity", None),
                e,
            )


class Pdf002Template:
    """
    PDF Processor to extract file name 0819啄木鳥A.pdf, 20240628120641957.pdf, 20240814141011543.pdf
    """

    def __init__(self, file_record: dict):
        self.file_record = file_record
        self.po_number = None

    def extract_metadata_from_lines(self, text_lines: List[str]) -> Dict[str, Any]:  # pragma: no cover  # NOSONAR
        metadata = {}
        prev_key = None

        time_pattern = re.compile(r"^(上午|下午)\d{2}:\d{2}:\d{2}$")

        collecting_notes = False
        note_key = ""
        note_lines = []

        for line in text_lines:
            line = line.strip()
            if not line:
                continue

            if not collecting_notes and (line.startswith("※") or line.startswith("二")):
                collecting_notes = True
                note_key = line
                note_lines = []
                continue

            if collecting_notes:
                if re.match(r"^\d+[.．]", line) or (note_lines and not KV_PATTERN.match(line)):
                    note_lines.append(line)
                    continue
                else:
                    metadata[note_key] = "\n".join(note_lines)
                    collecting_notes = False

            if time_pattern.match(line):
                if prev_key:
                    metadata[prev_key] = line
                    prev_key = None
                else:
                    continue

            kv_matches = list(KV_PATTERN.finditer(line))
            if kv_matches:
                for idx, match in enumerate(kv_matches):
                    key = match.group(1).strip()
                    start = match.end()
                    end = (
                        kv_matches[idx + 1].start()
                        if idx + 1 < len(kv_matches)
                        else len(line)
                    )
                    value = line[start:end].strip()
                    metadata[key] = value
                prev_key = None
            elif prev_key:
                metadata[prev_key] = line
                prev_key = None
            elif LINE_ENDS_WITH_COLON_PATTERN.match(line):
                prev_key = re.split(r"[：:]", line)[0].strip()
                metadata[prev_key] = ""

        if collecting_notes:
            metadata[note_key] = "\n".join(note_lines)

        return metadata

    def extract_tables(self, pdf_source) -> List[Dict[str, Any]]:
        """
        Use pdfplumber to extract tables from the PDF.

        Returns:
            List[Dict[str, Any]]: Table rows.
        """
        all_rows = []

        try:
            with pdfplumber.open(pdf_source) as pdf:
                for page in pdf.pages:
                    tables = page.extract_tables()

                    for table in tables:
                        header = [str(col).strip() for col in table[0]]
                        for row in table[1:]:
                            if not any(row):
                                continue
                            record = {
                                header[i]: str(cell).strip() if i < len(row) else ""
                                for i, cell in enumerate(row)
                            }
                            all_rows.append(record)

            return all_rows

        except Exception as e:
            logger.error(f"Failed to extract tables with pdfplumber: {e}")
            return []

    def parse_file_to_json(self) -> PODataParsed:
        """
        Process PDF into metadata and items using dynamic parsing.

        Returns:
            PODataParsed: Extracted data object.
        """
        try:
            # file_object = ext_extraction.FileExtensionProcessor(
            #     tracking_model=self.tracking_model, source=self.source
            # )
            # capacity = file_object._get_file_capacity()
            # document_type = file_object._get_document_type()

            full_text_lines = []

            if self.file_record.source_type == "local":
                pdf_source = self.file_record.file_path
            else:
                self.file_record.object_buffer.seek(0)
                pdf_source = self.file_record.object_buffer

            with pdfplumber.open(pdf_source) as pdf:
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        lines = [
                            line.strip() for line in text.split("\n") if line.strip()
                        ]
                        full_text_lines.extend(lines)

            # Extract metadata and items
            metadata = self.extract_metadata_from_lines(full_text_lines)
            items = self.extract_tables(pdf_source)

            self.po_number = metadata.get("訂單編號") or metadata.get("採購單號")

            return build_success_response(
                self.file_record.file_path, self.file_record.document_type, self.po_number, items, metadata, self.file_record.file_size
            )

        except Exception as e:
            return build_failed_response(
                self.file_record.file_path,
                getattr(self, "document_type", None),
                getattr(self, "capacity", None),
                e,
            )


class Pdf004Template:
    """
    PDF Processor to extract file name 20240722102127096.pdf
    """

    def __init__(self, file_record: dict):
        self.file_record = file_record
        self.po_number = None

    def extract_metadata_from_lines(self, text_lines: List[str]) -> Dict[str, Any]:  # pragma: no cover  # NOSONAR
        metadata = {}
        prev_key = None

        for line in text_lines:
            line = line.strip()
            if not line:
                continue

            kv_matches = list(KV_PATTERN.finditer(line))
            if kv_matches:
                for idx, match in enumerate(kv_matches):
                    key = match.group(1).strip()
                    start = match.end()
                    end = kv_matches[idx + 1].start() if idx + 1 < len(kv_matches) else len(line)
                    value = line[start:end].strip()
                    metadata[key] = value
                prev_key = None
            elif prev_key:
                metadata[prev_key] = line
                prev_key = None
            elif LINE_ENDS_WITH_COLON_PATTERN.match(line):
                prev_key = re.split(r"[：:]", line)[0].strip()
                metadata[prev_key] = ""

        return metadata

    def parse_item_lines(self, all_lines: List[str]) -> List[Tuple[str, str]]:
        """
        Identify and group product lines and optional additional lines.

        Returns:
            List[Tuple[str, str]]: List of (main_line, additional_spec) tuples.
        """
        items = []
        product_code_pattern = re.compile(r"^S\d{7}[A-Z]?$")
        additional_spec_pattern = re.compile(r".*?\d入/盒")

        i = 0
        while i < len(all_lines):
            line = all_lines[i]
            tokens = line.split()
            if tokens and product_code_pattern.match(tokens[0]):
                main_line = line
                additional_spec = ""
                if i + 1 < len(all_lines) and additional_spec_pattern.match(all_lines[i + 1]):
                    additional_spec = all_lines[i + 1].strip()
                    i += 2
                else:
                    i += 1
                items.append((main_line, additional_spec))
            else:
                logger.warning(f"Skipping non-table line: {line}")
                i += 1
        return items

    def build_table_from_items(self, items: List[Tuple[str, str]]) -> List[Dict[str, Any]]:
        """
        Convert parsed item lines into structured table rows.

        Args:
            items (List[Tuple[str, str]]): Parsed product lines.

        Returns:
            List[Dict[str, Any]]: Table rows.
        """
        results = []
        product_code_pattern = re.compile(r"^S\d{7}[A-Z]?$")

        for main_line, additional_spec in items:
            tokens = re.split(r"\s+", main_line)
            if len(tokens) >= 8 and product_code_pattern.match(tokens[0]):
                full_product_name = tokens[1] + (" " + additional_spec if additional_spec else "")
                results.append({
                    "產品編號": tokens[0],
                    "品名規格": full_product_name,
                    "數量": tokens[2],
                    "單位": tokens[3],
                    "單價": tokens[4].replace(",", ""),
                    "金額": tokens[5].replace(",", ""),
                    "預進貨日": tokens[6],
                    "未轉數量": tokens[7],
                })
            else:
                logger.warning(f"Invalid tokens: {tokens}")
        return results

    def extract_tables(self, all_lines: List[str]) -> List[Dict[str, Any]]:
        """
        Extract clean table with 8 columns from PDF using custom logic.

        Args:
            all_lines (List[str]): All text lines extracted from PDF.

        Returns:
            List[Dict[str, Any]]: Table rows.
        """
        try:
            items = self.parse_item_lines(all_lines)
            return self.build_table_from_items(items)
        except Exception as e:
            logger.error(f"Failed to extract tables: {e}")
            return []

    def parse_file_to_json(self) -> PODataParsed:
        """
        Process PDF into metadata and items using dynamic parsing.

        Returns:
            PODataParsed: Extracted data object.
        """
        try:
            # file_object = ext_extraction.FileExtensionProcessor(
            #     tracking_model=self.tracking_model,
            #     source=self.source
            # )
            # capacity = file_object._get_file_capacity()
            # document_type = file_object._get_document_type()

            full_text_lines = []

            if self.file_record.source_type == "local":
                pdf_source = self.file_record.file_path
            else:
                self.file_record.object_buffer.seek(0)
                pdf_source = self.file_record.object_buffer

            with pdfplumber.open(pdf_source) as pdf:
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        lines = [line.strip() for line in text.split("\n") if line.strip()]
                        full_text_lines.extend(lines)

            # Extract metadata and items
            metadata = self.extract_metadata_from_lines(full_text_lines)
            items = self.extract_tables(full_text_lines)

            self.po_number = metadata.get("採購單號")

            return build_success_response(
                self.file_record.file_path, self.file_record.document_type, self.po_number, items, metadata, self.file_record.file_size
            )

        except Exception as e:
            return build_failed_response(
                self.file_record.file_path,
                getattr(self, "document_type", None),
                getattr(self, "capacity", None),
                e,
            )


class Pdf006Template:
    """
    PDF Processor for file A202405220043.pdf
    """

    def __init__(self, file_record: dict):
        self.file_record = file_record
        self.po_number = None

    def _parse_kv_line(self, line: str, metadata: dict, current_key_holder: dict):
        kv_pattern = re.compile(r"^([^：:]{2,20})[：:]([^\n]{0,100})$")
        match = kv_pattern.match(line)
        if match:
            key = match.group(1).strip()
            value = match.group(2).strip()
            metadata[key] = value
            current_key_holder["current"] = key
            return True
        return False

    def _handle_non_kv_continuation(self, line: str, metadata: dict, current_key_holder: dict):
        if current_key_holder.get("current"):
            metadata[current_key_holder["current"]] += "\n" + line
            return True
        return False

    def _extract_special_fields(self, line: str, metadata: dict):
        long_number_pattern = re.compile(r"^\d{8}台灣大昌華嘉股份有限公司.*")
        if long_number_pattern.match(line):
            metadata["廠商名稱"] = line

        if "交貨地點" in line:
            parts = line.split("交貨地點")
            if len(parts) == 2:
                metadata["交貨地點"] = parts[1].replace("：", "").strip()
            else:
                metadata["交貨地點"] = parts[0].replace("：", "").strip()

    def _handle_note_line(self, line: str, note_key: str, note_lines: List[str], metadata: dict) -> tuple[str, List[str]]:
        """Handle lines that belong to notes starting with '※'."""
        if line.startswith("※"):
            if note_key:
                metadata[note_key] = "\n".join(note_lines)
            return line, []
        note_lines.append(line)
        return note_key, note_lines

    def extract_metadata_from_lines(self, text_lines: List[str]) -> Dict[str, Any]: # pragma: no cover  # NOSONAR
        """
        Extract metadata from text lines in the PDF.
        Handles key-value pairs, PO numbers, vendor name, delivery location, notes, and order dates.
        """
        metadata: dict[str, str] = {}
        current_key_holder: dict = {"current": None}
        note_key: Optional[str] = None
        note_lines: List[str] = []

        for raw in text_lines:
            line = raw.strip()
            if not line:
                continue

            if line.startswith("※"):
                note_key, note_lines = self._handle_note_line(line, note_key, note_lines, metadata)
                continue

            if note_key:
                note_key, note_lines = self._handle_note_line(line, note_key, note_lines, metadata)
                continue

            if self._parse_kv_line(line, metadata, current_key_holder):
                continue

            if self._handle_non_kv_continuation(line, metadata, current_key_holder):
                continue

            self._extract_special_fields(line, metadata)

        if note_key and note_lines:
            metadata[note_key] = "\n".join(note_lines)

        return metadata

    def _parse_item_block(self, lines: List[str], i: int) -> tuple[dict, int]:
        """
        Try to parse item starting at index i. Returns (item_dict, next_index).
        """
        line1 = lines[i]
        line2 = lines[i + 1] if i + 1 < len(lines) else ""
        item: dict[str, Any] = {}

        if not re.match(r"^\d{6}\s+\w+", line1):
            return {}, i + 1 

        parts1 = line1.split()
        try:
            item["料號"] = parts1[0]
            item["產品代碼"] = parts1[1]
            item["數量"] = parts1[-3]
            item["單位"] = parts1[-2]
            item["交貨日期"] = parts1[-1]
            item["品名／規格／製造廠／型號"] = " ".join(parts1[2:-3])
        except Exception as e:
            logger.warning(f"Malformed line1 at {i}: {line1} ({e})")
            return {}, i + 1

        parts2 = line2.split()
        if parts2:
            item["型號"] = parts2[0]
            if len(parts2) > 1:
                item["訂購單號"] = parts2[1]
            if len(parts2) > 2:
                more_desc = " ".join(parts2[2:])
                item["品名／規格／製造廠／型號"] += " " + more_desc

        next_i = i + 2
        if i + 2 < len(lines) and re.match(r"^\*U\d{8}-\d{4}\*$", lines[i + 2]):
            next_i = i + 3

        return item, next_i

    def extract_tables(self, lines: List[str]) -> List[Dict[str, Any]]:
        """
        Extract table items from the PDF. Each item spans 2 or 3 lines.
        """
        items: List[Dict[str, Any]] = []
        try:
            i = 0
            while i < len(lines) - 1:
                item, next_i = self._parse_item_block(lines, i)
                if item:
                    items.append(item)
                i = next_i
            return items
        except Exception as e:
            logger.error(f"Failed to extract tables: {e}")
            return []

    def parse_file_to_json(self) -> PODataParsed:
        """
        Process PDF into metadata and items using dynamic parsing.
        """
        try:
            # file_object = ext_extraction.FileExtensionProcessor(
            #     tracking_model=self.tracking_model,
            #     source=self.source
            # )
            # capacity = file_object._get_file_capacity()
            # document_type = file_object._get_document_type()

            full_text_lines: List[str] = []

            if self.file_record.source_type == "local":
                pdf_source = self.file_record.file_path
            else:
                self.file_record.object_buffer.seek(0)
                pdf_source = self.file_record.object_buffer

            with pdfplumber.open(pdf_source) as pdf:
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        lines = [line.strip() for line in text.split("\n") if line.strip()]
                        full_text_lines.extend(lines)

            metadata = self.extract_metadata_from_lines(full_text_lines)
            items = self.extract_tables(full_text_lines)

            logger.info("File has been processed successfully.")

            return build_success_response(
                self.file_record.file_path, self.file_record.document_type, self.po_number, items, metadata, self.file_record.file_size
            )

        except Exception as e:
            return build_failed_response(
                self.file_record.file_path,
                getattr(self, "document_type", None),
                getattr(self, "capacity", None),
                e,
            )


class Pdf007Template:
    """
    PDF Processor to extract file name O20240620TPB026.PDF
    """

    def __init__(self, file_record: dict):
        self.file_record = file_record
        self.po_number = None

    def _extract_key_value_pairs(self, line: str) -> Dict[str, Any]:
        """
        Extract key-value pairs from a single line using the KV pattern.

        Args:
            line (str): A single line of text.

        Returns:
            Dict[str, Any]: Extracted key-value pairs.
        """
        metadata = {}
        matches = list(KV_PATTERN.finditer(line.strip()))
        if matches:
            for idx, match in enumerate(matches):
                key = match.group(1).strip()
                start = match.end()
                end = matches[idx + 1].start() if idx + 1 < len(matches) else len(line)
                value = line[start:end].strip()
                if key and value:
                    metadata[key] = value
        return metadata

    def _save_note(self, notes: Dict[str, str], key: str, lines: List[str]) -> tuple[str, List[str]]:
        """
        Save the current note and reset the state.

        Args:
            notes (Dict[str, str]): The notes dictionary to update.
            key (str): The current note key.
            lines (List[str]): The current note lines to save.

        Returns:
            tuple[str, List[str]]: Reset values for key and lines.
        """
        if key and lines:
            notes[key] = "\n".join(lines)
        return "", []

    def _collect_notes(self, lines: List[str]) -> Dict[str, str]:
        """
        Collect notes starting with "●" and ending at "列印日期".

        Args:
            lines (List[str]): List of text lines.

        Returns:
            Dict[str, str]: Dictionary with note keys and their concatenated values.
        """
        notes = {}
        current_note_key = ""
        current_note_lines = []

        for line in lines:
            line = line.strip()
            if not line:
                continue

            if line.startswith("列印日期"):
                current_note_key, current_note_lines = self._save_note(notes, current_note_key, current_note_lines)
                break

            if line.startswith("●"):
                current_note_key, current_note_lines = self._save_note(notes, current_note_key, current_note_lines)
                current_note_key = line
                continue

            if current_note_key:
                current_note_lines.append(line)

        if current_note_key:
            current_note_key, current_note_lines = self._save_note(notes, current_note_key, current_note_lines)

        return notes

    def extract_metadata_from_lines(self, text_lines: List[str]) -> Dict[str, Any]:
        """
        Extract metadata from text lines, handling both key-value pairs and notes.

        Args:
            text_lines (List[str]): All text lines in the PDF.

        Returns:
            Dict[str, Any]: Combined metadata from key-value pairs and notes.
        """
        metadata = {}

        for line in text_lines:
            if not line.strip() or line.startswith("●") or line.startswith("列印日期"):
                continue
            metadata.update(self._extract_key_value_pairs(line))

        notes = self._collect_notes(text_lines)
        metadata.update(notes)

        return metadata

    def extract_tables(self, pdf_source) -> List[Dict[str, Any]]:
        """
        Use pdfplumber to extract tables from the PDF.

        Args:
            pdf_source: Path or buffer of the PDF file.

        Returns:
            List[Dict[str, Any]]: Table rows.
        """
        all_rows = []

        try:
            with pdfplumber.open(pdf_source) as pdf:
                for page in pdf.pages:
                    tables = page.extract_tables()
                    for table in tables:
                        header = [str(col).strip() for col in table[0]]
                        for row in table[1:]:
                            if not any(row):
                                continue
                            record = {
                                header[i]: str(cell).strip() if i < len(row) else ""
                                for i, cell in enumerate(row)
                            }
                            all_rows.append(record)
            return all_rows

        except Exception as e:
            logger.error(f"Failed to extract tables with pdfplumber: {e}")
            return []

    def parse_file_to_json(self) -> PODataParsed:
        """
        Process PDF into metadata and items using dynamic parsing.

        Returns:
            PODataParsed: Extracted data object.
        """
        try:
            # file_object = ext_extraction.FileExtensionProcessor(
            #     tracking_model=self.tracking_model, source=self.source
            # )
            # capacity = file_object._get_file_capacity()
            # document_type = file_object._get_document_type()

            full_text_lines = []

            if self.file_record.source_type == "local":
                pdf_source = self.file_record.file_path
            else:
                self.file_record.object_buffer.seek(0)
                pdf_source = self.file_record.object_buffer

            with pdfplumber.open(pdf_source) as pdf:
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        lines = [line.strip() for line in text.split("\n") if line.strip()]
                        full_text_lines.extend(lines)

            metadata = self.extract_metadata_from_lines(full_text_lines)
            items = self.extract_tables(pdf_source)

            self.po_number = items[0].get("請購明細單號")

            return build_success_response(
                self.file_record.file_path, self.file_record.document_type, self.po_number, items, metadata, self.file_record.file_size
            )

        except Exception as e:
            return build_failed_response(
                self.file_record.file_path,
                getattr(self, "document_type", None),
                getattr(self, "capacity", None),
                e,
            )


class Pdf008Template:
    """
    PDF Processor to extract file name RSV_1921_M24081500290_DC3.pdf
    """

    def __init__(self, file_record: dict):
        self.file_record = file_record
        self.po_number = None

    def extract_metadata_from_lines(self, text_lines: List[str]) -> Dict[str, Any]:  # pragma: no cover  # NOSONAR
        metadata = {}

        for line in text_lines:
            line = line.strip()
            if not line:
                continue

            kv_matches = list(KV_PATTERN.finditer(line))
            if kv_matches:
                for idx, match in enumerate(kv_matches):
                    key = match.group(1).strip()
                    start = match.end()
                    end = (
                        kv_matches[idx + 1].start()
                        if idx + 1 < len(kv_matches)
                        else len(line)
                    )
                    value = line[start:end].strip()
                    metadata[key] = value

        if "預約退貨時段" in metadata:
            hour = next((k for k in metadata if k.isdigit()), None)
            end_min = metadata.get(f"00~{hour}")
            if hour and end_min:
                metadata["預約退貨時段"] = f"{int(hour):02d}:00~{int(hour):02d}:{end_min.zfill(2)}"
                metadata.pop(hour, None)
                metadata.pop(f"00~{hour}", None)

        return metadata

    def extract_tables(self, text_lines: List[str]) -> List[Dict[str, Any]]:
        all_rows = []
        for i in range(len(text_lines)):
            if re.match(r"^\d", text_lines[i]):
                row = text_lines[i].split()
                if len(row) >= 4:
                    record = {
                        "退貨單號": row[1],
                        "退貨日期": row[2],
                        "箱數": row[3],
                        "棧板數": row[0]
                    }
                    all_rows.append(record)
                else:
                    logger.warning(f"Skipping invalid table row: {text_lines[i]}")
        return all_rows

    def parse_file_to_json(self) -> PODataParsed:
        """
        Process PDF into metadata and items using dynamic parsing.

        Returns:
            PODataParsed: Extracted data object.
        """
        try:
            # file_object = ext_extraction.FileExtensionProcessor(
            #     tracking_model=self.tracking_model, source=self.source
            # )
            # capacity = file_object._get_file_capacity()
            # document_type = file_object._get_document_type()

            full_text_lines = []

            if self.file_record.source_type == "local":
                pdf_source = self.file_record.file_path
            else:
                self.file_record.object_buffer.seek(0)
                pdf_source = self.file_record.object_buffer

            with pdfplumber.open(pdf_source) as pdf:
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        lines = [
                            line.strip() for line in text.split("\n") if line.strip()
                        ]
                        full_text_lines.extend(lines)

            # Extract metadata and items
            metadata = self.extract_metadata_from_lines(full_text_lines)
            items = self.extract_tables(full_text_lines)

            self.po_number = items[0].get("退貨單號")

            return build_success_response(
                self.file_record.file_path, self.file_record.document_type, self.po_number, items, metadata, self.file_record.file_size
            )

        except Exception as e:
            return build_failed_response(
                self.file_record.file_path,
                getattr(self, "document_type", None),
                getattr(self, "capacity", None),
                e,
            )
