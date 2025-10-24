from typing import Optional
from pydantic import BaseModel
from enum import Enum

from models.class_models import FilePathRequest


class TrackingModel(BaseModel):
    """
    Model representing traceability context data shared across the project.

    Attributes:
        request_id (str): Unique ID to trace the request.
        document_number (Optional[str]): Optional document number for additional context.
    """

    request_id: str
    file_path: Optional[str] = None
    project_name: Optional[str] = None
    source_name: Optional[str] = None
    workflow_id: Optional[str] = None
    workflow_name: Optional[str] = None
    document_number: Optional[str] = None
    document_type: Optional[str] = None
    sap_masterdata: Optional[bool] = None
    rerun_attempt: Optional[int] = None

    @classmethod
    def from_data_request(cls, data: FilePathRequest) -> "TrackingModel":
        return cls(
            request_id=data.celery_id,
            file_path=data.file_path,
            project_name=data.project,
            source_name=data.source,
            rerun_attempt=data.rerun_attempt,
        )


class ServiceLog(str, Enum):
    """
    Enum representing the type of service or component emitting logs.
    Used to categorize logs based on their origin within the system.
    """

    API_GATEWAY = "api-gateway"
    DATABASE = "database"
    FILE_PROCESSOR = "file-processor"
    TASK_EXECUTION = "task-execution"
    NOTIFICATION = "notification-service"

    METADATA_EXTRACTION = "metadata-extraction"
    METADATA_VALIDATION = "metadata-validation"
    DOCUMENT_PARSER = "document-parser"
    VALIDATION = "input-validation"
    MAPPING = "mapping"
    DATA_TRANSFORM = "data-transform"
    FILE_STORAGE = "file-storage"

    def __str__(self):
        return self.value


class LogType(str, Enum):
    """
    Enum representing the type of log being recorded.
    Helps distinguish between different log purposes.

    Attributes:
        "access": Logs for normal operations, requests, etc.
        "error": Logs for exceptions, failures, or unexpected behavior
    """

    ACCESS = "access"
    TASK = "task"
    ERROR = "error"
    WARNING = "warning"

    def __str__(self):
        return self.value
