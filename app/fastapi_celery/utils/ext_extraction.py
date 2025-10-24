import os
import json
import traceback
from pathlib import Path, PurePosixPath
from typing import Optional
from utils import log_helpers, read_n_write_s3
from connections import aws_connection
from models.class_models import SourceType, DocumentType
from models.tracking_models import ServiceLog, LogType, TrackingModel
import config_loader


# === Set up logging ===
logger = log_helpers.get_logger("Extension Detection")

types_string = config_loader.get_config_value("support_types", "types")
types_list = json.loads(types_string)


class FileExtensionProcessor:
    """
    Processes file extensions and determines document types from file paths.

    Loads files from local or S3 sources, validates extensions, and identifies document types.

    Attributes:
        file_path (str): File path being processed.
        file_path_parent (str | None): Parent directory of the file.
        source (SourceType): File source (LOCAL or S3).
        object_buffer (bytes | None): File content buffer (S3 only).
        file_extension (str | None): File extension.
        file_name (str | None): File name.
        client (boto3.client | None): AWS S3 client.
        document_type (DocumentType): Determined document type.
    """

    def __init__(self, tracking_model: TrackingModel, source: SourceType = SourceType.S3):
        """
        Initialize with file path and source type.

        Args:
            file_path (str): Path to the file.
            source (SourceType, optional): File source (LOCAL or S3). Defaults to S3.

        Raises:
            ValueError: If file_path is not a string or Path.
        """
        if not isinstance(tracking_model.file_path, (str, Path)):
            raise ValueError("file_path must be a string or Path.")

        self.tracking_model = tracking_model
        self.file_path = str(tracking_model.file_path)
        self.file_path_parent = None
        self.source = source
        self.object_buffer = None
        self.file_extension = None
        self.file_name = None
        self.client = None
        self.document_type = self._get_document_type()
        self.target_bucket_name = self._get_target_bucket()

        self._prepare_object()

    def _prepare_object(self) -> None:
        """
        Load file from source and extract extension.

        Raises:
            FileNotFoundError: If file doesn't exist.
            ValueError: If file has no or unsupported extension.
        """
        logger.info(
            f"Loading file from {self.source}...",
            extra={
                "service": ServiceLog.METADATA_EXTRACTION,
                "log_type": LogType.TASK,
                "data": self.tracking_model,
            },
        )
        if self.source == SourceType.LOCAL:
            self._load_local_file()
        else:
            self._load_s3_file()

        self._extract_file_extension()

    def _load_local_file(self) -> None:
        """
        Load local file and set name and parent directory.

        Raises:
            FileNotFoundError: If local file doesn't exist.
        """
        if not os.path.isfile(self.file_path):
            raise FileNotFoundError(f"Local file '{self.file_path}' does not exist.")
        self.file_name = Path(self.file_path).name
        self.file_path_parent = str(Path(self.file_path).parent) + "/"

    def _load_s3_file(self) -> None:  # pragma: no cover  # NOSONAR
        """
        Load S3 file and set name, parent directory, and buffer.

        Raises:
            FileNotFoundError: If S3 object cannot be loaded.
        """
        try:
            raw_bucket = self._get_raw_bucket()
            s3_connector = aws_connection.S3Connector(bucket_name=raw_bucket)
            self.client = s3_connector.client
            self._get_file_capacity()

            buffer = read_n_write_s3.get_object(
                client=self.client,
                bucket_name=raw_bucket,
                object_name=self.file_path,
            )

            if not buffer:  # pragma: no cover  # NOSONAR
                logger.error(
                    f"S3 object '{self.file_path}' not found - {traceback.format_exc()}",
                    extra={
                        "service": ServiceLog.METADATA_EXTRACTION,
                        "log_type": LogType.ERROR,
                        "data": self.tracking_model,
                    },
                )
                raise FileNotFoundError(f"S3 object '{self.file_path}' not found.")

            self.object_buffer = buffer
            self.file_name = Path(self.file_path).name
            self.file_path_parent = str(Path(self.file_path).parent) + "/"

        except Exception as e:
            logger.error(
                f"Error accessing file '{self.file_path}': {traceback.format_exc()}",
                extra={
                    "service": ServiceLog.METADATA_EXTRACTION,
                    "log_type": LogType.ERROR,
                    "data": self.tracking_model,
                },
            )
            raise FileNotFoundError(
                f"File '{self.file_path}' could not be loaded from S3. Error: {str(e)}"
            )

    def _extract_file_extension(self) -> None:  # pragma: no cover  # NOSONAR
        """
        Extract and validate file extension.

        Raises:
            ValueError: If file has no extension.
            TypeError: If extension is unsupported.
        """
        suffix = Path(self.file_path).suffix.lower()
        if not suffix:
            logger.error(
                "The file does not have an extension.",
                extra={
                    "service": ServiceLog.METADATA_EXTRACTION,
                    "log_type": LogType.ERROR,
                    "data": self.tracking_model,
                },
            )
            raise ValueError(f"File '{self.file_path}' has no extension.")

        if suffix not in types_list:
            logger.error(
                f"Unsupported file type: {suffix}",
                extra={
                    "service": ServiceLog.METADATA_EXTRACTION,
                    "log_type": LogType.ERROR,
                    "data": self.tracking_model,
                },
            )
            raise TypeError(
                f"Unsupported file extension '{suffix}'. Allowed types: {types_string}"
            )

        self.file_extension = suffix
        logger.info(
            f"File extension detected: {self.file_extension}",
            extra={
                "service": ServiceLog.METADATA_EXTRACTION,
                "log_type": LogType.TASK,
                "data": self.tracking_model,
            },
        )

    # Function to calculate the size of a file
    def _get_file_capacity(self) -> str:  # pragma: no cover  # NOSONAR
        """
        Calculate file size in KB or MB.

        Returns:
            str: File size (e.g., "1.23 MB" or "456.78 KB").

        Raises:
            FileNotFoundError: If file cannot be accessed.
        """
        raw_bucket = self._get_raw_bucket()
        if self.source == SourceType.LOCAL:
            size_bytes = os.path.getsize(self.file_path)
        else:
            # Use head_object for file size info (standard in boto3)
            head = self.client.head_object(Bucket=raw_bucket, Key=self.file_path)
            size_bytes = head.get("ContentLength", 0)

        if size_bytes / (1024**2) >= 1:
            size_mb = size_bytes / (1024**2)
            logger.info(
                f"Size of the input file {self.file_path} in MB: {size_mb:.2f}",
                extra={
                    "service": ServiceLog.METADATA_EXTRACTION,
                    "log_type": LogType.TASK,
                    "data": self.tracking_model,
                },
            )
            return f"{size_mb:.2f} MB"
        else:
            size_kb = size_bytes / 1024
            logger.info(
                f"Size of the input file {self.file_path} in KB: {size_kb:.2f}",
                extra={
                    "service": ServiceLog.METADATA_EXTRACTION,
                    "log_type": LogType.TASK,
                    "data": self.tracking_model,
                },
            )
            return f"{size_kb:.2f} KB"

    # Extracts the root directory from a file path to determine the document type.
    def _get_document_type(self) -> DocumentType:
        """
        Extracts the root directory from a file path to determine the document type.
        :return: DocumentType indicating the document type based on the root directory.
        :raises ValueError: If the file path is invalid or cannot be parsed correctly.
        """
        try:
            if self.source == SourceType.LOCAL:
                parts = Path(os.path.normpath(self.file_path)).parts
            else:
                parts = PurePosixPath(self.file_path).parts

            if not parts:
                logger.error(
                    f"Invalid file path: '{self.file_path}'. No path components found.",
                    extra={
                        "service": ServiceLog.METADATA_EXTRACTION,
                        "log_type": LogType.ERROR,
                        "data": self.tracking_model,
                    },
                )
                raise ValueError(
                    f"Invalid file path: '{self.file_path}'. No path components found."
                )

            logger.info(
                f"The file_path's exposed: {parts}",
                extra={
                    "service": ServiceLog.METADATA_EXTRACTION,
                    "log_type": LogType.TASK,
                    "data": self.tracking_model,
                },
            )
            document_type = (
                DocumentType.MASTER_DATA
                if any(part.lower() == "master_data" for part in parts)
                else DocumentType.ORDER
            )

            return document_type

        except Exception as e:
            logger.error(
                f"Error determining document type from path '{self.file_path}': {traceback.format_exc()}",
                extra={
                    "service": ServiceLog.METADATA_EXTRACTION,
                    "log_type": LogType.ERROR,
                    "data": self.tracking_model,
                },
            )
            raise ValueError(
                f"Error determining document type from path '{self.file_path}': {str(e)}"
            )

    def _get_target_bucket(self) -> Optional[str]:
        # Determine bucket name based on document type
        bucket_name = None

        if self.document_type == DocumentType.ORDER:
            if self.tracking_model.project_name and self.tracking_model.project_name.upper() == "DKSH_TW":
                bucket_name = config_loader.get_config_value(
                    "s3_buckets", "datahub_s3_process_data_tw"
                )
            elif self.tracking_model.project_name and self.tracking_model.project_name.upper() == "DKSH_VN":
                bucket_name = config_loader.get_config_value(
                    "s3_buckets", "datahub_s3_process_data_vn"
                )

        elif self.document_type == DocumentType.MASTER_DATA:

            if self.tracking_model.sap_masterdata:
                bucket_name = config_loader.get_config_value(
                    "s3_buckets", "sap_masterdata_bucket"
                )
            elif self.tracking_model.project_name and self.tracking_model.project_name.upper() == "DKSH_TW":
                bucket_name = config_loader.get_config_value(
                    "s3_buckets", "tw_masterdata_bucket"
                )
            elif self.tracking_model.project_name and self.tracking_model.project_name.upper() == "DKSH_VN":
                bucket_name = config_loader.get_config_value(
                    "s3_buckets", "vn_masterdata_bucket"
                )
        else:
            logger.warning(
                f"Unknown document type: {self.document_type}",
                extra={
                    "service": ServiceLog.FILE_STORAGE,
                    "log_type": LogType.WARNING,
                    "data": self.tracking_model,
                },
            )
            return None

        return bucket_name


    def _get_raw_bucket(self) -> Optional[str]:
        """
        Get raw_bucket base on project.
        """
        bucket_name = None
        if self.tracking_model.project_name and self.tracking_model.project_name.upper() == "DKSH_TW":
            bucket_name = config_loader.get_config_value(
                "s3_buckets", "datahub_s3_raw_data_tw"
            )
        elif self.tracking_model.project_name and self.tracking_model.project_name.upper() == "DKSH_VN":
            bucket_name = config_loader.get_config_value(
                "s3_buckets", "datahub_s3_raw_data_vn"
            )
        else:
            logger.warning(
                f"Unknown project_name: {self.tracking_model.project_name}",
                extra={
                    "service": ServiceLog.FILE_STORAGE,
                    "log_type": LogType.WARNING,
                    "data": self.tracking_model,
                },
            )
        return bucket_name
