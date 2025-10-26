from pydantic import BaseModel
from enum import Enum
from models.class_models import FilePathRequest


class TrackingModel(BaseModel):
    """
    Represents traceability and context data shared across the project.
    """

    request_id: str
    file_path: str | None = None
    project_name: str | None = None
    source_name: str | None = None
    workflow_id: str | None = None
    workflow_name: str | None = None
    document_number: str | None = None
    document_type: str | None = None
    sap_masterdata: bool | None = None
    rerun_attempt: int | None = None

    @classmethod
    def from_data_request(cls, data: FilePathRequest) -> "TrackingModel":
        """Create a TrackingModel instance from a FilePathRequest object."""
        return cls(
            request_id=data.celery_id,
            file_path=data.file_path,
            project_name=data.project,
            source_name=data.source,
            rerun_attempt=data.rerun_attempt,
        )


class ServiceLog(str, Enum):
    """
    Identifies the service or component that emits a log.
    Used for log source categorization.
    """

    API_GATEWAY = "api-gateway"
    CALL_BE_API = "call-BE-api"
    REDIS_SERVICE = "redis-service"
    FILE_PROCESSOR = "file-processor"
    TASK_EXECUTION = "task-execution"
    STEP_EXECUTION = "step-execution"
    NOTIFICATION = "notification-service"

    FILE_EXTRACTION = "file-extraction"
    METADATA_EXTRACTION = "metadata-extraction"
    METADATA_VALIDATION = "metadata-validation"
    DOCUMENT_PARSER = "document-parser"
    DATA_VALIDATION = "data-validation"
    DATA_MAPPING = "data-mapping"
    DATA_TRANSFORM = "data-transform"
    FILE_STORAGE = "file-storage"

    def __str__(self):
        return self.value


class LogType(str, Enum):
    """
    Defines the type or purpose of a log entry.
    """

    ACCESS = "access"
    TASK = "task"
    ERROR = "error"
    WARNING = "warning"

    def __str__(self):
        return self.value
