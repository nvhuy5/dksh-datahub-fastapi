import os
import json
from pathlib import Path, PurePosixPath
from utils.bucket_helper import get_bucket_name
from utils import log_helpers, read_n_write_s3
from connections import aws_connection
from models.class_models import SourceType, DocumentType
from models.tracking_models import TrackingModel
import config_loader


# === Set up logging ===
logger = log_helpers.get_logger("Extension Detection")

types_string = config_loader.get_config_value("support_types", "types")
types_list = json.loads(types_string)


class FileExtensionProcessor:
    """
    Processor responsible for extracting file metadata and loading files
    from local or S3 storage sources.
    """

    def __init__(self, tracking_model: TrackingModel, source_type: SourceType = SourceType.S3):
        """
        Initialize the FileExtensionProcessor with context information.

        Args:
            tracking_model: Tracking model containing file path and metadata.
            source_type: The file source type (LOCAL, S3, etc.).

        Raises:
            ValueError: If file_path is not a valid string or Path.
        """
        if not isinstance(tracking_model.file_path, (str, Path)):
            raise ValueError("file_path must be a string or Path.")

        self.tracking_model = tracking_model
        self.file_path: str = tracking_model.file_path
        self.file_path_parent: str | None = None
        self.source_type: SourceType = source_type
        self.object_buffer: bytes | None = None
        self.file_size: str | None = None
        self.file_name: str | None = None
        self.file_name_wo_ext: str | None = None
        self.file_extension: str | None = None
        self.client: object | None = None
        self.document_type: DocumentType | None = None
        self.raw_bucket_name: str | None = None
        self.target_bucket_name: str | None = None

        self._prepare_object()

    def _prepare_object(self) -> None:
        """Prepare file-related attributes by sequentially loading and analyzing the file."""

        self._get_document_type()
        self._get_bucket_name()

        if self.source_type == SourceType.LOCAL:
            self._load_local_file()
        else:
            self._load_s3_file()

        self._get_file_extension()
        self._get_file_capacity()

    def _load_local_file(self) -> None:
        """
        Load a file from the local filesystem.

        Raises:
            FileNotFoundError: If the specified local file does not exist.
        """
        try:

            if not os.path.isfile(self.file_path):
                raise FileNotFoundError(f"Failed to find local file '{self.file_path}'.")
            self.file_name = Path(self.file_path).name
            self.file_path_parent = str(Path(self.file_path).parent) + "/"

        except Exception as e:
            raise FileNotFoundError(
                f"Failed to load local file '{self.file_path}'. Original error: {e}"
            ) from e

    def _load_s3_file(self) -> None:
        """
        Load file content from an S3 bucket into memory.

        Raises:
            FileNotFoundError: If the S3 object does not exist or cannot be loaded.
        """
        try:
            s3_connector = aws_connection.S3Connector(bucket_name=self.raw_bucket_name)
            self.client = s3_connector.client

            buffer = read_n_write_s3.get_object(
                client=self.client,
                bucket_name=self.raw_bucket_name,
                object_name=self.file_path,
            )

            if not buffer:
                raise FileNotFoundError(f"Failed to find S3 object '{self.file_path}' in bucket '{self.raw_bucket_name}'.")

            self.object_buffer = buffer
            self.file_name = Path(self.file_path).name
            self.file_path_parent = str(Path(self.file_path).parent) + "/"

        except Exception as e:
            raise FileNotFoundError(
                f"Failed to load file '{self.file_path}' from S3. Original error: {e}"
            ) from e

    def _get_file_extension(self) -> None:
        """
        Extract and validate the file extension.

        Raises:
            ValueError: If the file has no extension.
            TypeError: If the extension is not supported.
        """
        try:
            suffix = Path(self.file_path).suffix.lower()

            if not suffix:
                raise ValueError(f"File '{self.file_path}' has no extension.")

            if suffix not in types_list:
                raise TypeError(
                    f"Unsupported file extension '{suffix}'. Allowed types: {types_string}"
                )

            self.file_extension = suffix
            self.file_name_wo_ext = Path(self.file_path).stem

        except Exception as e:
            raise ValueError(
                f"Failed to get file extension for '{self.file_path}'. Original error: {e}"
            ) from e

    def _get_file_capacity(self) -> None:
        """
        Determine and format the file size in KB or MB.

        Raises:
            FileNotFoundError: If the file cannot be accessed.
        """
        try:
            if self.source_type == SourceType.LOCAL:
                if not os.path.exists(self.file_path):
                    raise FileNotFoundError(f"Failed to find local file '{self.file_path}'.")
                size_bytes = os.path.getsize(self.file_path)
            else:
                head = self.client.head_object(Bucket=self.raw_bucket_name, Key=self.file_path)
                size_bytes = head.get("ContentLength", 0)

            self.file_size = self._format_size(size_bytes)
        except Exception as e:
            raise FileNotFoundError(
                f"Failed to determine file size for '{self.file_path}'. Original error: {e}"
            ) from e

    def _get_document_type(self) -> None:
        """
        Determine the document type (MASTER_DATA or ORDER) based on file path.

        Raises:
            ValueError: If file path is invalid or cannot be parsed.
        """
        try:
            # Normalize path components based on the file source
            parts = (
                Path(os.path.normpath(self.file_path)).parts
                if self.source_type == SourceType.LOCAL
                else PurePosixPath(self.file_path).parts
            )

            if not parts:
                raise ValueError(
                    f"Invalid file path: '{self.file_path}'. No path components found."
                )
            # Determine document type by folder naming
            document_type = (
                DocumentType.MASTER_DATA
                if any(part.lower() == DocumentType.MASTER_DATA.value for part in parts)
                else DocumentType.ORDER
            )
            self.document_type = document_type

        except Exception as e:
            raise ValueError(
                f"Failed to determine document type for '{self.file_path}'. Original error: {e}"
            ) from e

    def _get_bucket_name(self) -> None:
        """
        Retrieve the raw and target S3 bucket names based on document type and project context.

        Raises:
            ValueError: If bucket name resolution fails.
        """
        try:
            self.raw_bucket_name = get_bucket_name(
                self.document_type,
                "raw_bucket",
                self.tracking_model.project_name,
                self.tracking_model.sap_masterdata,
            )

            self.target_bucket_name = get_bucket_name(
                self.document_type,
                "target_bucket",
                self.tracking_model.project_name,
                self.tracking_model.sap_masterdata,
            )
        except Exception as e:
            raise ValueError(
                f"Failed to retrieve bucket names for document type '{self.document_type}'. Original error: {e}"
            ) from e

    @staticmethod
    def _format_size(size_bytes: int) -> str:
        """Convert byte size to a readable KB or MB string."""
        if size_bytes / (1024**2) >= 1:
            return f"{size_bytes / (1024**2):.2f} MB"
        return f"{size_bytes / 1024:.2f} KB"
    