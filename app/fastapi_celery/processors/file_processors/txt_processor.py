from models.tracking_models import TrackingModel
from models.class_models import SourceType, PODataParsed, StatusEnum
from utils import log_helpers, ext_extraction


# === Set up logging ===
logger = log_helpers.get_logger("TXT Processor")

PO_MAPPING_KEY = "採購單"


class TXTProcessor:
    """
    PO file: PO202404007116.txt
    Processor for handling TXT file operations.

    Initializes with a file path and source type, and provides methods to extract
    text and parse it into JSON format.
    """

    def __init__(self, file_record: dict):
        """Initialize the TXT processor with a file path and source type.

        Args:
            file (Path): The path to the TXT file.
            source (SourceType, optional): The source type, defaults to SourceType.S3.
        """
        self.file_record = file_record

    def extract_text(self) -> str:  # pragma: no cover  # NOSONAR
        """Extract and return the text content of the file.

        Reads the file content from local or S3 source, handling encoding appropriately.

        Returns:
            str: The extracted text content of the file.
        """
        """
        Extracts and returns the text content of the file.
        Works for both local and S3 sources.
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

    def parse_file_to_json(self) -> PODataParsed:  # NOSONAR
        """Parse the TXT file content into a JSON-compatible dictionary.

        Extracts key-value pairs and product data from text lines,
        handling PO-specific formats and tab-separated values.

        Returns:
            PODataParsed: PODataParsed object
        """
        text = self.extract_text()
        lines = text.split("\n")
        json_data = {}
        products = []
        column = None
        logger.info(f"Start processing for file: {self.file_record.file_path}")

        for line in lines:
            line = line.strip()

            if not line or line.startswith("---"):
                continue

            if "PO" in line:
                key, value = line.split("-", 1)
                json_data[key.strip()] = value.strip()
                continue

            count = line.count("：")

            if count >= 2 and "\t" in line:
                parts = line.split("\t")
                for part in parts:
                    if "：" in part:
                        key, value = part.split("：", 1)
                        json_data[key.strip()] = value.strip()
            elif count == 1 and "\t" not in line:
                key, value = line.split("：", 1)
                json_data[key.strip()] = value.strip()
            elif line.startswith("料品代號"):
                column = [h.strip() for h in line.split("\t") if h.strip()]
            elif column and "\t" in line:
                values = [v.strip() for v in line.split("\t")]
                while len(values) < len(column):
                    values.append("")
                product = dict(zip(column, values))
                products.append(product)

        if products:
            json_data["products"] = products
        logger.info("File has been proceeded successfully!")

        return PODataParsed(
            original_file_path=self.file_record.file_path,
            document_type=self.file_record.document_type,
            po_number=json_data[PO_MAPPING_KEY],
            items=json_data,
            metadata=None,
            step_status= StatusEnum.SUCCESS,
            messages= None,
            capacity=self.file_record.file_size,
        )
