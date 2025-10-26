import re
import xml.etree.ElementTree as ET
from models.tracking_models import TrackingModel
from models.class_models import SourceType, PODataParsed, StatusEnum
from utils import log_helpers, ext_extraction


# === Set up logging ===
logger = log_helpers.get_logger("XML Processor")


class XMLProcessor:
    """
    Processor for handling XML file operations dynamically.

    Initializes with a file path and source type, and provides methods to extract
    text and parse it into JSON format.
    """

    def __init__(self, file_record: dict):
        """Initialize the XML processor with a file path and source type.

        Args:
            file_path (Path): The path to the XML file.
            source (SourceType, optional): The source type, defaults to SourceType.S3.
        """
        self.file_record = file_record


    def extract_text(self) -> str:
        """Extract and return the text content of the XML file.

        Reads the file content from local or S3 source, handling encoding appropriately.

        Returns:
            str: The extracted text content of the file.
        """
        # file_object = ext_extraction.FileExtensionProcessor(
        #     tracking_model=self.tracking_model, source=self.source
        # )
        # self.capacity = file_object._get_file_capacity()
        # self.document_type = file_object._get_document_type()
        if self.file_record.source_type == "local":
            with open(self.file_record.file_path, "r", encoding="utf-8") as f:
                text = f.read()
        else:
            # S3: read from in-memory buffer
            self.file_record.object_buffer.seek(0)
            text = self.file_record.object_buffer.read().decode("utf-8")

        return text

    def parse_element(self, element) -> dict | str:
        """Parse an XML element into a JSON-compatible dictionary.

        Args:
            element: The XML element to parse.

        Returns:
            dict or str: Parsed data as a dictionary or text content if no children.
        """
        result = {}

        for child in element:
            child_tag = child.tag.split("}")[-1]
            child_data = self.parse_element(child)

            if child_tag in result:
                if not isinstance(result[child_tag], list):
                    result[child_tag] = [result[child_tag]]
                result[child_tag].append(child_data)
            else:
                result[child_tag] = child_data

        if element.text and element.text.strip():
            result = element.text.strip()

        return result

    def find_po_in_xml(self, element) -> str:
        """Recursively search for 'PO' in XML element text or attributes.
        Args:
            element: The XML element to search.

        Returns:
            str: The PO number if found, otherwise an empty string.
        """
        po_number = ""

        # Check text content
        if element.text and element.text.strip():
            po_match = re.search(r"PO\d+", element.text.strip())
            if po_match:
                po_number = po_match.group(0)
                return po_number

        # Check attributes
        for attr_value in element.attrib.values():
            po_match = re.search(r"PO\d+", attr_value)
            if po_match:
                po_number = po_match.group(0)
                return po_number

        for child in element:
            result = self.find_po_in_xml(child)
            if result:
                return result

        return po_number

    def parse_file_to_json(self) -> PODataParsed:
        """Parse the XML file content into a JSON-compatible dictionary dynamically.

        Extracts key-value pairs and data from XML elements. The PO number is extracted
        from anywhere containing 'PO' in the XML.

        Returns:
            PODataParsed: PODataParsed object with parsed data.
        """
        json_data = {}

        logger.info(f"Start processing for file: {self.file_record.file_path}")

        xml_content = self.extract_text()
        root = ET.fromstring(xml_content)
        po_number = self.find_po_in_xml(root)
        json_data.update(self.parse_element(root))

        logger.info("File has been processed successfully!")

        return PODataParsed(
            original_file_path=self.file_record.file_path,
            document_type=self.file_record.document_type,
            po_number=po_number,
            items=json_data,
            metadata=None,
            step_status = StatusEnum.SUCCESS,
            messages=None,
            capacity=self.file_record.file_size,
        )
