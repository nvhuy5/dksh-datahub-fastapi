import json
import os
import traceback
import asyncio
from urllib.parse import urlparse
from pydantic import BaseModel
from utils.bucket_helper import get_s3_key_prefix
from connections.be_connection import BEConnector
from processors.processor_nodes import PROCESS_DEFINITIONS
from processors.processor_base import ProcessorBase
from processors.helpers import template_helper
from models.class_models import (
    ApiUrl,
    ContextData,
    StepDetail,
    WorkflowStep,
    StepDefinition,
    StatusEnum,
    StepOutput,
)
from models.tracking_models import ServiceLog, LogType, TrackingModel
from typing import Dict, Any, Callable, List, Optional, Union
from utils import log_helpers
from datetime import datetime, timezone


# === Set up logging ===
logger = log_helpers.get_logger("Step Handler")


# Suppress Cognitive Complexity warning due to step-specific business logic  # NOSONAR
async def execute_step(
    file_processor: ProcessorBase, context_data: ContextData, step: WorkflowStep
) -> StepOutput:  # NOSONAR

    step_name = step.stepName
    sub_step_name = get_sub_step_name(step_name)
    logger.info(f"[{context_data.request_id}] Starting execute step: [{sub_step_name}]")

    step_config = PROCESS_DEFINITIONS.get(sub_step_name)
    if not step_config:
        logger.error(
            f"[{context_data.request_id}] The step [{step_name}] is not yet defined"
        )

        from models.class_models import StepOutput, StatusEnum

        # === Return a standardized StepOutput with NOT_DEFINED status ===
        return StepOutput(
            step_status=StatusEnum.NOT_DEFINED,
            step_failure_message=[f"The step [{step_name}] is not yet defined"],
            data=None,
        )

    # s3_key_prefix = get_s3_key_prefix(
    #     context_data.request_id, file_processor.file_record, step, step_config,
    #     file_processor.tracking_model.rerun_attempt, file_processor.tracking_model.sap_masterdata
    # )
    # if step_config.require_data_output:
    #     context_data.s3_key_prefix = s3_key_prefix

    result_ctx_api = get_context_api(step_name)
    ctxs = result_ctx_api["ctxs"]
    required_keys = result_ctx_api["required_keys"]
    required_keys = fill_required_keys_for_request(
        required=required_keys,
        file_record=file_processor.file_record,
        step=step,
        processing_steps=context_data.processing_steps,
    )

    ctx_api_records = []
    response = None
    if not result_ctx_api:
        logger.warning(
            f"[{context_data.request_id}] There is no API context for this step: {step_name}"
        )
    else:
        for ctx in ctxs:
            url = ctx["url"](required_keys) if callable(ctx["url"]) else ctx["url"]
            method = ctx["method"]
            params = (
                ctx["params"](required_keys)
                if callable(ctx["params"])
                else ctx["params"]
            )
            body = ctx["body"](required_keys) if callable(ctx["body"]) else ctx["body"]

            connector = BEConnector(api_url=url, body_data=body, params=params)
            response = await (connector.get() if method == "get" else connector.post())

            if "extract" in ctx:
                ctx["extract"](response, required_keys)

            parsed_url = urlparse(url)
            short_url = parsed_url.path

            ctx_api_records.append(
                {
                    "url": short_url,
                    "method": method.upper(),
                    "request": {"params": params, "body": body},
                    "response": response,
                }
            )

    context_data.step_detail[step.stepOrder].config_api = ctx_api_records

    try:
        method_name = step_config.function_name
        method = getattr(file_processor, method_name, None)

        if method is None or not callable(method):
            raise AttributeError(
                f"Function '{method_name}' not found in FileProcessor."
            )

        args = []
        kwargs = fill_required_keys_from_response(response, step_config.kwargs)

        data_input = None
        if hasattr(step_config, "data_input") and getattr(
            step_config, "data_input", None
        ):
            data_input = context_data.processing_steps[step_config.data_input]

        result = (
            await method(data_input, response, *args, **kwargs)
            if asyncio.iscoroutinefunction(method)
            else method(data_input, response, *args, **kwargs)
        )

        key_name = step_config.data_output
        if key_name:
            context_data.processing_steps[key_name] = result

        return result

    except Exception as e:
        full_tb = traceback.format_exception(type(e), e, e.__traceback__)
        logger.exception(
            f"Exception during step '{step_name}': {e}",
            extra={
                "service": ServiceLog.DATA_TRANSFORM,
                "log_type": LogType.ERROR,
                "data": file_processor.tracking_model,
                "traceback": full_tb,
            },
        )
        raise


# def build_s3_key_prefix(
#     file_processor: ProcessorBase,
#     context_data: ContextData,
#     step: WorkflowStep,
#     step_config: StepDefinition,
# ) -> str:
#     """
#     Build S3 prefix for both Processor workflow and Master Data workflow.

#     Processor workflow: {materialized_step_data_loc}/{folderName}/{customerFolderName}/{yyyyMMdd}/{celery_id}/{step_order}_{step_name}
#     Master data workflow: {materialized_step_data_loc}/{fileName}/{yyyyMMdd}/{celery_id}/{step_order}_{step_name}
#     """
#     filter_api = context_data.workflow_detail.filter_api
#     is_master_data = getattr(filter_api.response, "isMasterDataWorkflow", False)
#     date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
#     step_order = f"{int(step.stepOrder):02}"

#     if is_master_data:
#         file_name = file_processor.file_record.get("file_name")
#         prefix_part, _ = os.path.splitext(file_name or "")
#         logger.info("prefix_part: %s", prefix_part)
#     else:
#         folder = filter_api.response.folderName
#         customer = filter_api.response.customerFolderName
#         if not folder or not customer:
#             logger.error(
#                 "Missing 'folderName' or 'customerFolderName' in filter_api response. "
#                 f"filter_api={filter_api}"
#             )
#         prefix_part = f"{folder}/{customer}"

#     return (
#         f"{step_config.target_store_data}/"
#         f"{prefix_part}/{date_str}/"
#         f"{file_processor.tracking_model.request_id}/"
#         f"{step_order}_{step.stepName}"
#     )


def get_context_api(step_name: str) -> dict[str, Any] | None:

    step_name_upper = step_name.upper()
    step_map = {
        "FILE_PARSE": [
            {
                "url": ApiUrl.WORKFLOW_TEMPLATE_PARSE.full_url(),
                "method": "get",
                "required_context": ["workflowStepId"],
                "params": lambda ctx: {"workflowStepId": ctx["workflowStepId"]},
                "body": None,
            }
        ],
        "VALIDATE_HEADER": [
            {
                "url": ApiUrl.MASTERDATA_HEADER_VALIDATION.full_url(),
                "method": "get",
                "required_context": ["file_name"],
                "params": lambda ctx: {"fileName": ctx["file_name"]},
                "body": None,
            }
        ],
        "VALIDATE_DATA": [
            {
                "url": ApiUrl.MASTERDATA_COLUMN_VALIDATION.full_url(),
                "method": "get",
                "required_context": ["file_name"],
                "params": lambda ctx: {"fileName": ctx["file_name"]},
                "body": None,
            }
        ],
        "MASTER_DATA_LOAD": [
            {
                "url": ApiUrl.MASTER_DATA_LOAD_DATA.full_url(),
                "method": "post",
                "required_context": ["file_name_wo_ext", "items"],
                "params": None,
                "body": lambda ctx: {
                    "fileName": ctx["file_name_wo_ext"],
                    "data": ctx["items"],
                },
            }
        ],
        "TEMPLATE_FORMAT_VALIDATION": [
            {
                "url": ApiUrl.WORKFLOW_TEMPLATE_PARSE.full_url(),
                "method": "get",
                "required_context": ["workflowStepId"],
                "params": lambda ctx: {"workflowStepId": ctx["workflowStepId"]},
                "body": None,
                "extract": lambda resp, ctx: ctx.update(
                    {"templateFileParseId": resp[0]["templateFileParse"]["id"]}
                ),
            },
            {
                "url": lambda ctx: f"{ApiUrl.TEMPLATE_FORMAT_VALIDATION.full_url()}/{ctx['templateFileParseId']}",
                "method": "get",
                "required_context": ["templateFileParseId"],
                "params": lambda _: {},
                "body": None,
            },
        ],
        "TEMPLATE_DATA_MAPPING": [
            {
                "url": ApiUrl.WORKFLOW_TEMPLATE_PARSE.full_url(),
                "method": "get",
                "required_context": ["workflowStepId"],
                "params": lambda ctx: {"workflowStepId": ctx["workflowStepId"]},
                "body": None,
                "extract": lambda resp, ctx: ctx.update(
                    {"templateFileParseId": resp[0]["templateFileParse"]["id"]}
                ),
            },
            {
                "url": lambda ctx: f"{ApiUrl.DATA_MAPPING.full_url()}?templateFileParseId={ctx['templateFileParseId']}",
                "method": "get",
                "required_context": ["templateFileParseId"],
                "params": lambda ctx: {
                    "templateFileParseId": ctx["templateFileParseId"]
                },
                "body": None,
            },
        ],
        "METADATA_EXTRACT": [
            {
                "url": lambda ctx: f"{ApiUrl.WORKFLOW_STEP.full_url()}/{ctx['workflowStepId']}",
                "method": "get",
                "required_context": ["workflowStepId"],
                "params": lambda _: {},
                "body": None,
            }
        ],
        "XSL_TRANSLATION": [
            {
                "url": lambda ctx: f"{ApiUrl.WORKFLOW_STEP.full_url()}/{ctx['workflowStepId']}",
                "method": "get",
                "required_context": ["workflowStepId"],
                "params": lambda _: {},
                "body": None,
            }
        ],
        "SUBMIT": [
            {
                "url": lambda ctx: f"{ApiUrl.WORKFLOW_STEP.full_url()}/{ctx['workflowStepId']}",
                "method": "get",
                "required_context": ["workflowStepId"],
                "params": lambda _: {},
                "body": None,
            }
        ],
        "SEND_TO": [
            {
                "url": lambda ctx: f"{ApiUrl.WORKFLOW_STEP.full_url()}/{ctx['workflowStepId']}",
                "method": "get",
                "required_context": ["workflowStepId"],
                "params": lambda _: {},
                "body": None,
            }
        ],
    }

    for key, ctxs in step_map.items():
        if key in step_name_upper:
            required_keys = {
                key: None for ctx in ctxs for key in ctx.get("required_context", [])
            }
            return {
                "ctxs": ctxs,
                "required_keys": required_keys,
            }

    return None


def get_sub_step_name(step_name: str) -> Optional[str]:
    """
    DYNAMIC MAPPING: step_name → PROCESS_DEFINITIONS key

    Workflow API → Definition:
    - "CUSTOMER_3_DKSH_TW_SUBMIT" → "SUBMIT"
    - "CUSTOMER_3_DKSH_TW_XSL_TRANSLATION" → "XSL_TRANSLATION"
    - "TEMPLATE_FILE_PARSE" → "TEMPLATE_FILE_PARSE"
    """
    list_step = list(PROCESS_DEFINITIONS.keys())

    if step_name in list_step:
        return step_name

    parts = step_name.rsplit("_", maxsplit=1)
    if len(parts) == 2:
        suffix = parts[1]
        if suffix in list_step:
            logger.info(f"[get_sub_step_name] '{step_name}' → '{suffix}' (suffix)")
            return suffix

    for step_key in list_step:
        if step_name.endswith(step_key):
            logger.info(f"[get_sub_step_name] '{step_name}' → '{step_key}' (endswith)")
            return step_key

    logger.warning(f"[get_sub_step_name] No dynamic match for '{step_name}'")
    return None


def fill_required_keys_for_request(
    required: Dict[str, Any],
    file_record: Dict[str, Any],
    step: BaseModel,
    processing_steps: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Fill required fields with values from file_record, step, and processing_steps.
    Priority: file_record > step > processing_steps.
    """
    # Convert Pydantic model to dict
    step_data = step.model_dump() if hasattr(step, "model_dump") else step.model_dump()

    for key in required.keys():
        if key in file_record:
            required[key] = file_record[key]
        elif key in step_data:
            required[key] = step_data[key]
        else:
            for _, step_info in processing_steps.items():
                if key in step_info:
                    required[key] = step_info[key]
                    break
    return required


def fill_required_keys_from_response(
    response: Union[Dict[str, Any], List[Dict[str, Any]]], required: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Fill values from response into required

    Args:
        response: API response (dict or list of dicts).
        required: Dict with keys to fill (values may be None).

    Returns:
        Updated dict with matched keys filled from response.
    """
    if isinstance(response, dict):
        # Directly map keys from dict response
        for key, value in response.items():
            if key in required:
                required[key] = value
    elif isinstance(response, list):
        # Iterate all dicts in list, overwrite if key repeats
        for item in response:
            if isinstance(item, dict):
                for key, value in item.items():
                    if key in required:
                        required[key] = value

    return required
