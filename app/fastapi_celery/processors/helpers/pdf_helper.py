import traceback
from utils import log_helpers
from models.class_models import PODataParsed, StatusEnum


# === Set up logging ===
logger = log_helpers.get_logger("PDF Helper")

def build_success_response(file_path, document_type, po_number, items, metadata, capacity):
    return PODataParsed(
        original_file_path=file_path,
        document_type=document_type,
        po_number=po_number,
        items=items,
        metadata=metadata,
        step_status=StatusEnum.SUCCESS,
        messages=None,
        capacity=capacity,
    )


def build_failed_response(file_path, document_type=None, capacity=None, exc: Exception = None):
    logger.error(f"Error while parse file JSON from PDF: {exc}")
    return PODataParsed(
        original_file_path=file_path,
        document_type=document_type if document_type in ("master_data", "order") else "order",
        po_number=None,
        items=[],
        metadata={},
        step_status=StatusEnum.FAILED,
        messages=[traceback.format_exc()],
        capacity=capacity or "",
    )
