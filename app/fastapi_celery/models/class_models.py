import json
from pathlib import Path
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field, model_validator
from enum import Enum
import config_loader
from urllib.parse import urljoin


# === Source Type Enum ===
class SourceType(str, Enum):
    """Enum representing the source type of data.

    Attributes:
        LOCAL (str): Indicates a local file source.
        S3 (str): Indicates an S3 bucket source.
    """

    LOCAL = "local"
    S3 = "s3"


class Environment(str, Enum):
    """Present the all supported environments"""

    PROD = "prod"
    PREPROD = "preprod"
    UAT = "uat"
    QA = "qa"
    DEV = "dev"

    def __repr__(self):
        return self.value


class DocumentType(str, Enum):
    """Enum representing the type of document.

    Attributes:
        MASTER_DATA (str): Indicates a master data document.
        ORDER (str): Indicates an order document.
    """

    MASTER_DATA = "master_data"
    ORDER = "order"


class StatusEnum(str, Enum):
    """Enum representing the status of a workflow step.

    Attributes:
        SUCCESS (str): Indicates the step completed successfully.
        FAILED (str): Indicates the step failed.
        SKIPPED (str): Indicates the step was skipped.
        PROCESSING (str): Indicates the step is currently processing.
    """

    SUCCESS = "1"
    FAILED = "2"
    SKIPPED = "3"
    PROCESSING = "4"
    NOT_DEFINED = "5"


class ApiUrl(str, Enum):
    """Enum representing API endpoint URLs.

    Attributes:
        workflow_filter (str): URL for filtering workflows.
        workflow_session_start (str): URL for starting a workflow session.
        workflow_session_finish (str): URL for finishing a workflow session.
        workflow_step_start (str): URL for starting a workflow step.
        workflow_step_finish (str): URL for finishing a workflow step.
    """

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
    """Pydantic model for stopping a task request.

    Attributes:
        task_id (str): The ID of the task to stop.
        reason (str | None): The reason for stopping the task, optional.
    """

    task_id: str
    reason: str | None = None


class FilePathRequest(BaseModel):
    """
    Pydantic model for the /file/process request payload.

    Attributes:
        file_path (str): Required. The absolute or relative path to the file to be processed.
        celery_id (Optional[str]): Optional. A specific task identifier to use for reruns.
            If not provided, a new UUID will be generated automatically.
        rerun_attempt (Optional[int]): Optional. Indicates which rerun attempt this is (e.g., 1, 2, 3...).
            Used to distinguish and version rerun outputs in S3.
    """

    file_path: str
    project: str
    source: str
    celery_id: Optional[str] = None
    rerun_attempt: Optional[int] = None


class WorkflowStep(BaseModel):
    """Pydantic model representing a workflow step.

    Attributes:
        workflowStepId (str): The unique ID of the workflow step.
        stepName (str): The name of the step.
        stepOrder (int): The order of the step in the workflow.
        stepConfiguration (List[dict]): Configuration details for the step, default empty list.
    """

    workflowStepId: str
    stepName: str
    stepOrder: int
    stepConfiguration: List[dict] = []


class WorkflowModel(BaseModel):
    """Pydantic model representing a workflow.

    Attributes:
        id (str): The unique ID of the workflow.
        name (str): The name of the workflow.
        workflowSteps (List[WorkflowStep]): List of steps in the workflow.
    """

    id: str
    name: str | None = None
    status: str | None = None
    isMasterDataWorkflow: Optional[bool] = None
    sapMasterData: Optional[bool] = None
    customerId: str | None = None
    folderName: str | None = None
    flowId: str | None = None
    customerFolderName: str | None = None
    workflowSteps: List[WorkflowStep]


class WorkflowSession(BaseModel):
    """Pydantic model representing a workflow session.

    Attributes:
        id (str): The unique ID of the workflow session.
        status (str): The status of the workflow session.
    """

    id: str
    status: str


class StartStep(BaseModel):
    """Pydantic model for starting a workflow step.

    Attributes:
        workflowHistoryId (str): The ID of the workflow history.
        status (str): The initial status of the step.
    """

    workflowHistoryId: str
    status: str


class PathEncoder(json.JSONEncoder):  # pragma: no cover  # NOSONAR
    """Custom JSON encoder for serializing Path objects.

    Extends json.JSONEncoder to handle Path objects by converting them to POSIX strings.
    """

    def default(self, obj):
        if isinstance(obj, Path):
            return obj.as_posix()
        return super().default(obj)


class StepDefinition(BaseModel):  # pragma: no cover  # NOSONAR
    """
    Pydantic model representing a step definition in a workflow.

    Attributes:
        function_name (str): The name of the function to execute.
        data_input (Optional[str]): The input data key, optional.
        data_output (Optional[str]): The output data key, optional.
        require_data_output (bool): Indicates whether an output key is required.
        target_store_data (Optional[str]): Optional target name for storing processed data.
        extract_to (Dict[str, str]): Mapping of keys to extract data to, defaults to an empty dict.
        args (Optional[List[str]]): List of positional argument names, optional.
        kwargs (Optional[Dict[str, Any]]): Dictionary of keyword arguments, optional.

    Raises:
        ValueError: If `require_data_output` is True but `data_output` is not provided.
    """

    function_name: str
    data_input: Optional[str] = None
    data_output: Optional[str] = None
    require_data_output: bool = False
    target_store_data: Optional[str] = None
    extract_to: Dict[str, str] = {}
    args: Optional[List[str]] = None
    kwargs: Optional[Dict[str, Any]] = None

    @model_validator(mode="after")
    def validate_constraints(cls, values):
        if values.require_data_output and not values.data_output:
            raise ValueError(
                "Invalid configuration: 'data_output' must be provided when 'require_data_output' is True."
            )
        return values


class StepOutput(BaseModel):
    output: Any
    step_status: Optional[StatusEnum]
    step_failure_message: Optional[List[str]]


class MasterDataParsed(BaseModel):
    """Represents the output structure for processed master data.

    Attributes:
        original_file_path (Path): Path to the original input file.
        headers (List[str]): List of column headers extracted from the file.
        document_type (DocumentType): Type of the document being processed.
        items (List[Dict[str, Any]]): Parsed data items from the document, represented as a list of dictionaries.
        step_status (Optional[StatusEnum]): Current processing status of the step (e.g., success, failure).
        message (Optional[str]): Additional information or error message from processing.
        capacity (str): Indicates the capacity or data volume context of the document.
    """

    original_file_path: Path
    headers: List[str] | Dict[str, Any]
    document_type: DocumentType
    items: List[Dict[str, Any]] | Dict[str, Any]
    step_status: Optional[StatusEnum]
    messages: Optional[List[str]] = None
    capacity: str
    step_detail: Optional[List[Dict[str, Any]]] = None
    workflow_detail: Optional[Dict[str, Any]] = None
    json_output: Optional[str] = None

    def __repr__(self) -> str:
        return self.model_dump_json(indent=2, exclude_none=True)


class GenericStepResult(BaseModel):
    step_status: str
    message: Optional[str] = None


class PODataParsed(BaseModel):
    """
    Represents the parsed content of a purchase order (PO) document.

    Attributes:
        original_file_path (Path): The path to the original PO file.
        document_type (DocumentType): The type of document, expected to be 'order' for PO files.
        po_number (str): The unique purchase order number extracted from the document.
        items (List[Dict[str, Any]]): The list of item entries parsed from the PO, each represented as a dictionary.
        metadata (Optional[Dict[str, str]]): Optional metadata extracted from the document.
        capacity (str): Indicates the data volume or context of the PO (e.g., 'full', 'partial').
    """
    
    model_config = {
        "extra": "allow"
    }

    original_file_path: Path
    document_type: DocumentType
    po_number: Optional[str]
    items: List[Dict[str, Any]] | Dict[str, Any]
    metadata: Optional[Dict[str, str]]
    step_status: Optional[StatusEnum]
    messages: Optional[List[str]] = None
    capacity: str
    step_detail: Optional[List[Dict[str, Any]]] = None
    workflow_detail: Optional[Dict[str, Any]] = None
    json_output: Optional[str] = None

    def __repr__(self) -> str:
        return self.model_dump_json(indent=2, exclude_none=True)


class ApiConfig(BaseModel):
    url: str | None = None
    method: str | None = None
    request: Dict[str, Any] | None = None
    response: Dict[str, Any] | None = None


class SessionConfig(BaseModel):
    session_start_api: ApiConfig = Field(default_factory=ApiConfig)
    session_finish_api: ApiConfig = Field(default_factory=ApiConfig)


class WorkflowDetailConfig(BaseModel):
    filter_api: ApiConfig = Field(default_factory=ApiConfig)
    metadata_api: SessionConfig | None = Field(default_factory=SessionConfig)


class StepDetailConfig(BaseModel):
    Step_start_api: ApiConfig = Field(default_factory=ApiConfig)
    Step_finish_api: ApiConfig = Field(default_factory=ApiConfig)


class StepDetail(BaseModel):
    step: Dict[str, Any] | None = None
    config_api: Any | None = None
    metadata_api: StepDetailConfig | None = Field(default_factory=StepDetailConfig)
    data_output: Dict[str, Any] | None = None
    

class ContextData(BaseModel):
    request_id: str
    data_input: Any | None = None
    step_detail: list[StepDetail] | None = None
    workflow_detail: WorkflowDetailConfig | None = None
    
    class Config:
        extra = "allow"
