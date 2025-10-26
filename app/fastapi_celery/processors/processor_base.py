import types
import importlib
import inspect
from models.tracking_models import TrackingModel
from processors.processor_nodes import WORKFLOW_PROCESSORS
from utils import log_helpers


# === Set up logging ===
logger = log_helpers.get_logger("ProcessorBase")


class ProcessorBase:
    """
    Base class for workflow processors.

    Dynamically loads and binds all processing functions defined in
    `processors.workflow_processors`.
    """

    def __init__(self, tracking_model: TrackingModel):
        """
        Initialize the processor with a tracking model.

        Args:
            tracking_model (TrackingModel): Model for tracking and logging workflow status.
        """
        self.tracking_model = tracking_model
        self.file_record = {}
        self._register_workflow_processors()

    def run(self):
        """Execute the default entry process."""
        self.extract_metadata()

    def _register_workflow_processors(self) -> None:
        """
        Dynamically register workflow processor functions as instance methods.

        Imports each module listed in `WORKFLOW_PROCESSORS` from
        `processors.workflow_processors` and binds their functions to the instance.
        Logs a warning if a module is missing.
        """
        base_modules = {"workflow_processors": "processors.workflow_processors"}

        for module_name in WORKFLOW_PROCESSORS:
            try:
                module_path = f"{base_modules['workflow_processors']}.{module_name}"
                module = importlib.import_module(module_path)

                for name, func in inspect.getmembers(module):
                    if inspect.isfunction(func) or inspect.iscoroutinefunction(func):
                        setattr(self, name, types.MethodType(func, self))
                        logger.debug(f"Registered processor: {name} from {module_name}")

            except ModuleNotFoundError:
                logger.warning(f"Module not found: {module_name}")
