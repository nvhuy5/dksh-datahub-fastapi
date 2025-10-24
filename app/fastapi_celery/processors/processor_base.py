# Standard Library Imports
from datetime import datetime, timezone
import logging
from models.tracking_models import TrackingModel
from processors.processor_nodes import WORKFLOW_PROCESSORS
from utils import log_helpers
import importlib
import inspect
import types


# === Set up logging ===
logger = log_helpers.get_logger("ProcessorBase")


class ProcessorBase:
    """
    Base class for workflow processors.

    This class dynamically loads and binds processing functions (called "processes")
    defined in the `processors.workflow_processors` package.

    Each process name in `WORKFLOW_PROCESSORS` corresponds to a Python module
    within the `workflow_processors/` directory, for example:
    - `extract_metadata`  →  `workflow_processors/extract_metadata.py`
    - `template_mapping`  →  `workflow_processors/template_mapping.py`
    - `parse_file_to_json` → `workflow_processors/parse_file_to_json.py`
    """

    def __init__(self, tracking_model: TrackingModel):
        """
        Initialize a ProcessorBase instance.

        Args:
            tracking_model (TrackingModel): The model used for tracking and logging
                processing status and metadata throughout the workflow.
        """
        self.file_record = {}
        self.source_type = None
        self.document_type = None
        self.target_bucket_name = None
        self.tracking_model = tracking_model
        self.current_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        self._register_workflow_processors()

    def run(self):
        self.extract_metadata()

    def _register_workflow_processors(self) -> None:
        """
        Dynamically register workflow processors as instance methods.

        This method imports each module listed in `WORKFLOW_PROCESSORS` from the
        `processors.workflow_processors` package. All functions found in these
        modules are bound to the current instance as methods.

        Missing modules are logged as warnings.
        Successfully registered functions are logged at debug level.
        """
        base_modules = {
            "workflow_processors": "processors.workflow_processors"
        }

        # Register from WORKFLOW_PROCESSORS (from workflow_processors)
        for module_name in WORKFLOW_PROCESSORS:
            try:
                module_path = f"{base_modules['workflow_processors']}.{module_name}"
                module = importlib.import_module(module_path)
                for name, func in inspect.getmembers(module):
                    if inspect.isfunction(func) or inspect.iscoroutinefunction(func):
                        bound_method = types.MethodType(func, self)
                        setattr(self, name, bound_method)
                        logger.debug(f"Registered processor: {name} from {module_name}")
            except ModuleNotFoundError:
                logger.warning(f"Module {module_name} not found in workflow_processors.")
