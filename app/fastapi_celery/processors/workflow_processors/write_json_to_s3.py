from typing import Optional, Any, List
from utils.bucket_helper import get_s3_key_prefix
from models.class_models import (
    StatusEnum,
    StepDefinition,
    StepOutput,
    WorkflowStep,
)
from models.tracking_models import ServiceLog, LogType
from utils import log_helpers, read_n_write_s3
import traceback
from processors.helpers import template_helper
from processors.processor_base import logger


def write_json_to_s3(self, data_input, response_api, *args, **kwargs) -> StepOutput:
    """
    Writes JSON data to an S3 bucket based on document type.
    """

    try:

        result = read_n_write_s3.write_json_to_s3(
            json_data=data_input,
            bucket_name=self.file_record["target_bucket_name"],
            s3_key_prefix=args[0],
        )

        return StepOutput(
            data=result,
            sub_data={},
            step_status=StatusEnum.SUCCESS,
            step_failure_message=None,
        )

    except Exception as e:
        full_tb = traceback.format_exception(type(e), e, e.__traceback__)
        logger.error(
            f"Exception occurred while executing step 'write_json_to_s3'",
            extra={
                "service": ServiceLog.FILE_STORAGE,
                "log_type": LogType.ERROR,
                "data": self.tracking_model,
                "traceback": full_tb,
            },
            exc_info=True,
        )

        return StepOutput(
            data=None,
            step_status=StatusEnum.FAILED,
            step_failure_message=traceback.format_exc(),
        )


def get_step_result_from_s3(self, step: WorkflowStep, step_config: StepDefinition):

    rerun_attempt = (
        self.tracking_model.rerun_attempt - 1
        if self.tracking_model.rerun_attempt is not None and self.tracking_model.rerun_attempt > 1
        else None
    )

    s3_key_prefix = get_s3_key_prefix(
        request_id=self.tracking_model.request_id,
        file_record=self.file_record,
        step=step,
        step_config=step_config,
        rerun_attempt=rerun_attempt,
        is_master_data=self.tracking_model.sap_masterdata,
        target_folder=None,
        is_full_prefix=False,
        version_folder=None
    )

    keys = read_n_write_s3.list_objects_with_prefix(bucket_name=self.file_record.target_bucket_name, prefix=s3_key_prefix)
    key = read_n_write_s3.select_latest_rerun(keys=keys, base_filename=self.file_record.file_name_wo_ext)
    data = read_n_write_s3.read_json_from_s3(bucket_name=self.file_record.target_bucket_name, object_name=key)

    if data is None:
        logger.info(
            f"[check_step_result_exists_in_s3] No file found for '{step.stepName}' "
            f"attempt {self.tracking_model.rerun_attempt}. Will rerun.",
            extra={
                "service": ServiceLog.FILE_STORAGE,
                "data": self.tracking_model,
            },
        )
        return None
    
    step_status = data.get("step_status")
    data.update(json_output=key)
    if step_status == "1":
        logger.info(
            f"[check_step_result_exists_in_s3] Step '{step.stepName}' "
            f"attempt {rerun_attempt} has step_status=1. Skipping.",
            extra={
                "service": ServiceLog.FILE_STORAGE,
                "data": self.tracking_model,
            },
        )
    else:
        logger.info(
            f"[check_step_result_exists_in_s3] Step '{step.stepName}' "
            f"attempt {rerun_attempt} has step_status={step_status}. Will rerun.",
            extra={
                "service": ServiceLog.FILE_STORAGE,
                "data": self.tracking_model,
            },
        )
    return template_helper.parse_data(document_type=self.file_record.document_type, data=data)
