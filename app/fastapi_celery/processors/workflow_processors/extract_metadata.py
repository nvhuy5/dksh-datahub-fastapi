import time
import logging
import traceback
from datetime import datetime, timezone
from utils import log_helpers, ext_extraction
from models.tracking_models import ServiceLog, LogType
from models.class_models import StatusEnum, StepOutput
from processors.processor_base import logger


def extract_metadata(self) -> StepOutput:
    """
    Extract file metadata using FileExtensionProcessor.

    Populates instance attributes (document_type, target_bucket_name, file_record)
    and logs results or errors.

    Returns:
        StepOutput: SUCCESS with True output if extraction succeeds,
        otherwise FAILED with error details.
    """
    try:

        file_processor = ext_extraction.FileExtensionProcessor(self.tracking_model)

        # Assign key metadata fields for tracking and downstream use
        self.document_type = file_processor.document_type
        self.target_bucket_name = file_processor.target_bucket_name
        self.file_record = {
            "file_path": self.tracking_model.file_path,
            "file_path_parent": file_processor.file_path_parent,
            "file_name": file_processor.file_name,
            "file_extension": file_processor.file_extension,
            "proceed_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        }

        logger.info(
            f"Metadata extracted for file: {self.tracking_model.file_path}",
            extra={
                "service": ServiceLog.METADATA_EXTRACTION,
                "log_type": LogType.TASK,
                "data": self.tracking_model.model_dump(),
            },
        )
        return StepOutput(
            output=True,
            step_status=StatusEnum.SUCCESS,
            step_failure_message=None,
        )
    
    except (Exception) as e:
        error_type = type(e).__name__
        logger.error(
            f"[extract_metadata] {error_type}: {e}\n",
            extra={
                "service": ServiceLog.METADATA_EXTRACTION,
                "log_type": LogType.ERROR,
                "data": self.tracking_model.model_dump(),
            },
            exc_info=True,
        )

        return StepOutput(
            output=False,
            step_status=StatusEnum.FAILED,
            step_failure_message=[traceback.format_exc() or "Unknown error occurred."],
        )
