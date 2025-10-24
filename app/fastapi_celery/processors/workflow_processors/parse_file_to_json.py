import logging
import traceback
from utils import log_helpers
from models.class_models import StatusEnum, StepOutput
from models.tracking_models import ServiceLog, LogType
from processors.processor_registry import ProcessorRegistry
from processors.processor_base import logger


async def parse_file_to_json(self, *args, **kwargs) -> StepOutput:  # pragma: no cover  # NOSONAR
    """Parses a file to JSON based on its document type and extension.

    Uses the appropriate processor from `POFileProcessorRegistry` or
    `MasterdataProcessorRegistry` based on `self.document_type` and file extension.
    Logs errors for unsupported types, extensions, or parsing failures.

    Returns:
        StepOutput: Parsed JSON data if successful, None otherwise.
    """
    try:

        # Handle document type processor for Master Data and PO Data
        processor_instance = await ProcessorRegistry.get_processor_for_file(self)
        json_data = processor_instance.parse_file_to_json()

        return StepOutput(
            output=json_data,
            step_status=StatusEnum.SUCCESS,
            step_failure_message=None,
        )

    except Exception as e:  # pragma: no cover  # NOSONAR
        logger.error(
            f"[parse_file_to_json] Failed to parse file: {e}!\n",
            extra={
                "service": ServiceLog.DOCUMENT_PARSER,
                "log_type": LogType.ERROR,
                "data": self.tracking_model,
            },
            exc_info=True,
        )

        return StepOutput(
            output=None,
            step_status=StatusEnum.FAILED,
            step_failure_message=[traceback.format_exc()],
        )
