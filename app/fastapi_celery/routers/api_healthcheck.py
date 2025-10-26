import traceback
from typing import Dict, Any
from fastapi import APIRouter, status
from fastapi.responses import JSONResponse
from utils import log_helpers
from models.tracking_models import ServiceLog, LogType

# === Set up logging ===
logger = log_helpers.get_logger("Health-check Router")

router = APIRouter()


def _internal_health_check() -> Dict[str, str]:
    """
    Update the internal healthcheck aiming to replace by error simulation when running test case

    Logs a success message and returns a status dictionary. Intended to be updated
    with error simulation for testing purposes.

    Returns:
        Dict[str, str]: A dictionary with a 'status' key set to 'ok'.
    """
    logger.info(
        "Health check passed successfully.",
        extra={
            "service": ServiceLog.API_GATEWAY,
            "log_type": LogType.ACCESS,
        },
    )
    return {"status": "ok"}


@router.get("/api_health")
async def api_health() -> Dict[str, Any]:
    """Health check endpoint for the service.

    Calls the internal health check function and returns the result. If an error occurs,
    logs the error and returns a JSON response with error details.

    Returns:
        Dict[str, Any]: A dictionary with 'status' key on success,
            or a JSONResponse with status code 503 and error details on failure.
    """
    try:
        return _internal_health_check()
    except Exception as e:
        # Log the error details
        logger.error(
            f"Health check failed: {e} - {traceback.format_exc()}",
            extra={
                "service": ServiceLog.API_GATEWAY,
                "log_type": LogType.ERROR,
            },
        )
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"status": "error", "details": f"{e} - {traceback.format_exc()}"},
        )
