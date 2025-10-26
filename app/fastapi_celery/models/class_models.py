import json
from pathlib import Path
from typing import Any
from pydantic import BaseModel, ConfigDict, Field, model_validator
from enum import Enum
import config_loader
from urllib.parse import urljoin


# === Source Type Enum ===
class SourceType(str, Enum):
    """Type of data source."""

    LOCAL = "local"
    S3 = "s3"


class Environment(str, Enum):
    """Supported environments."""

    PROD = "prod"
    PREPROD = "preprod"
    UAT = "uat"
    QA = "qa"
    DEV = "dev"

    def __repr__(self):
        return self.value


class DocumentType(str, Enum):
    """Type of document being processed."""

    MASTER_DATA = "master_data"
    ORDER = "order"


class StatusEnum(str, Enum):
    """Status of a workflow step."""

    SUCCESS = "1"
    FAILED = "2"
    SKIPPED = "3"
    PROCESSING = "4"
    NOT_DEFINED = "5"


class ApiUrl(str, Enum):
    """API endpoint paths used in workflow processing."""

    WORKFLOW_FILTER = "/api/workflow/filter"
    WORKFLOW_SESSION_START = "/api/workflow/session/start"
    WORKFLOW_SESSION_FINISH = "/api/workflow/session/finish"
    WORKFLOW_STEP_START = "/api/workflow/step/start"
    WORKFLOW_STEP_FINISH = "/api/workflow/step/finish"
    WORKFLOW_TOKEN_GENERATION = "/api/workflow/token"
    MASTERDATA_HEADER_VALIDATION = "/api/master-data/header"
    MASTERDATA_COLUMN_VALIDATION = "/api/master-data/column/validation-criteria"
    WORKFLOW_TEMPLATE_PARSE = "/api/template/template-parse"
    TEMPLATE_FORMAT_VALIDATION = "/api/template/format-validation"
    MASTER_DATA_LOAD_DATA = "/api/data-sync-record/sync-data"
    DATA_MAPPING = "/api/data-mapping"
    WORKFLOW_STEP = "/api/workflow/step"

    def full_url(self) -> str:
        env = Environment(config_loader.get_env_variable("ENVIRONMENT", "prod").lower())
        if env == Environment.DEV:
            base_url = config_loader.get_env_variable("BASE_API_URL", "")
        else:
            host = config_loader.get_env_variable("BACKEND_HOST", "")
            port = config_loader.get_env_variable("BACKEND_PORT", "")
            base_url = f"{host}:{port}"
        return urljoin(base_url + "/", self.value.lstrip("/"))

    def __str__(self):
        return self.full_url()


class StopTaskRequest(BaseModel):
    """Request payload to stop a task."""

    task_id: str
    reason: str | None = None


class FilePathRequest(BaseModel):
    """Payload for /file/process API."""

    file_path: str
    project: str
    source: str
    celery_id: str | None = None
    rerun_attempt: int | None = None


class WorkflowStep(BaseModel):
    """Represents a step in the workflow."""

    workflowStepId: str
    stepName: str
    stepOrder: int
    stepConfiguration: list[dict] = Field(default_factory=list)


class WorkflowModel(BaseModel):
    """Represents a workflow structure."""

    id: str
    name: str | None = None
    status: str | None = None
    isMasterDataWorkflow: bool | None = None
    sapMasterData: bool | None = None
    customerId: str | None = None
    folderName: str | None = None
    flowId: str | None = None
    customerFolderName: str | None = None
    workflowSteps: list[WorkflowStep]


class WorkflowSession(BaseModel):
    """Represents a workflow session."""

    id: str
    status: str


class StartStep(BaseModel):
    """Request model for starting a workflow step."""

    workflowHistoryId: str
    status: str


class PathEncoder(json.JSONEncoder):  # pragma: no cover  # NOSONAR
    """JSON encoder for Path objects."""

    def default(self, obj):
        if isinstance(obj, Path):
            return obj.as_posix()
        return super().default(obj)


class StepDefinition(BaseModel):  # pragma: no cover  # NOSONAR
    """Defines a single step function configuration."""

    function_name: str
    data_input: str | None = None
    data_output: str | None = None
    require_data_api: bool = False
    require_data_output: bool = False
    target_store_data: str | None = None
    args: list[str] = Field(default_factory=list)
    kwargs: dict[str, Any] = Field(default_factory=dict)
    extract_to: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_constraints(cls, values):
        if values.require_data_output and not values.data_output:
            raise ValueError(
                "Invalid config: 'data_output' is required when 'require_data_output' is True."
            )
        return values


class StepOutput(BaseModel):
    """Output data returned from a workflow step."""

    data: Any | None = None
    sub_data: dict[str, Any] = Field(default_factory=dict)
    step_status: StatusEnum | None = None
    step_failure_message: list[str] | None = None


class MasterDataParsed(BaseModel):
    """Represents processed master data output."""

    original_file_path: Path
    headers: list[str] | dict[str, Any]
    document_type: DocumentType
    items: list[dict[str, Any]] | dict[str, Any]
    step_status: StatusEnum | None
    messages: list[str] | None = None
    capacity: str
    step_detail: list[dict[str, Any]] | None = None
    workflow_detail: dict[str, Any] | None = None
    json_output: str | None = None

    def __repr__(self) -> str:
        return self.model_dump_json(indent=2, exclude_none=True)


class GenericStepResult(BaseModel):
    """Simple model for generic step results."""

    step_status: str
    message: str | None = None


class PODataParsed(BaseModel):
    """Represents parsed Purchase Order (PO) document data."""

    model_config = ConfigDict(extra="allow")

    original_file_path: Path
    document_type: DocumentType
    po_number: str | None
    items: list[dict[str, Any]] | dict[str, Any]
    metadata: dict[str, str] | None
    step_status: StatusEnum | None
    messages: list[str] | None = None
    capacity: str
    step_detail: list[dict[str, Any]] | None = None
    workflow_detail: dict[str, Any] | None = None
    json_output: str | None = None

    def __repr__(self) -> str:
        return self.model_dump_json(indent=2, exclude_none=True)


class ApiConfig(BaseModel):
    """Configuration for an API call."""

    url: str | None = None
    method: str | None = None
    request: dict[str, Any] | None = None
    response: dict[str, Any] | None = None


class SessionConfig(BaseModel):
    """Workflow session API configurations."""

    session_start_api: ApiConfig = Field(default_factory=ApiConfig)
    session_finish_api: ApiConfig = Field(default_factory=ApiConfig)


class WorkflowDetailConfig(BaseModel):
    """Workflow-level API configurations."""

    filter_api: ApiConfig = Field(default_factory=ApiConfig)
    metadata_api: SessionConfig | None = Field(default_factory=SessionConfig)


class StepDetailConfig(BaseModel):
    """Step-level API configurations."""

    Step_start_api: ApiConfig = Field(default_factory=ApiConfig)
    Step_finish_api: ApiConfig = Field(default_factory=ApiConfig)


class StepDetail(BaseModel):
    """Detail information for an individual step."""

    step: dict[str, Any] | None = None
    config_api: Any | None = None
    metadata_api: StepDetailConfig | None = Field(default_factory=StepDetailConfig)
    data_output: dict[str, Any] | None = None


class ContextData(BaseModel):
    """Holds runtime context information across workflow execution."""

    model_config = ConfigDict(extra="allow")

    request_id: str
    # s3_key_prefix: str | None = None
    step_detail: list[StepDetail] | None = None
    workflow_detail: WorkflowDetailConfig | None = None
    processing_steps: dict = Field(default_factory=dict)
