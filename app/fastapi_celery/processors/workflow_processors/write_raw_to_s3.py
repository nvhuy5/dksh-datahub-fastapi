import re
from utils.bucket_helper import get_s3_key_prefix
from processors.processor_base import logger
from models.class_models import DocumentType, StepOutput, StatusEnum
from models.tracking_models import ServiceLog, LogType
from utils import read_n_write_s3


def write_raw_to_s3(self) -> StepOutput:
 
    try:
        logger.info(
            "Preparing to write raw master data to s3",
            extra={
                "service": ServiceLog.FILE_STORAGE,
                "log_type": LogType.TASK,
                "data": self.tracking_model,
            },
        )

        s3_key_prefix = get_s3_key_prefix(
            request_id=self.tracking_model.request_id,
            file_record=self.file_record,
            step=None,
            step_config=None,
            rerun_attempt=self.tracking_model.rerun_attempt,
            is_master_data=self.tracking_model.sap_masterdata,
            target_folder="master_data",
            is_full_prefix=True,
            version_folder=None
        )

        result = read_n_write_s3.copy_object_between_buckets(
            source_bucket=self.file_record.raw_bucket_name,
            source_key=self.file_record.file_path,
            dest_bucket=self.file_record.target_bucket_name,
            dest_key=s3_key_prefix,
        )
           
        version_prefix = f"versioning/{self.file_record.file_name_wo_ext}/"
        existing_keys = read_n_write_s3.list_objects_with_prefix(
            bucket_name= self.file_record.target_bucket_name,
            prefix= version_prefix
        )
        version_number = 1
        if existing_keys:  # pragma: no cover  # NOSONAR
            numbers = []
            for key in existing_keys:
                match = re.search(rf"versioning/{re.escape(self.file_record.file_name_wo_ext)}/(\d{{3}})/", key)
                if match:
                    numbers.append(int(match.group(1)))
            if numbers:
                version_number = max(numbers) + 1
 
        version_folder = f"{version_number:03d}"
        version_key = f"{version_prefix}{version_folder}/{self.file_record.file_name}"
 
        read_n_write_s3.copy_object_between_buckets(
            source_bucket=self.file_record.raw_bucket_name,
            source_key=self.file_record.file_path,
            dest_bucket=self.file_record.target_bucket_name,
            dest_key=version_key,
        )
 
        logger.info(
            "write_raw_to_s3 completed.",
            extra={
                "service": ServiceLog.FILE_STORAGE,
                "log_type": LogType.TASK,
                "data": self.tracking_model,
            },
        )
 
        return StepOutput(
            data=result,
            step_status=StatusEnum.SUCCESS,
            step_failure_message=None,
        )
 
    except Exception as e:
        logger.error(
            f"Exception in write_raw_to_s3: {e}",
            extra={
                "service": ServiceLog.FILE_STORAGE,
                "log_type": LogType.ERROR,
                "data": self.tracking_model,
            },
            exc_info=True,
            )
 
        return StepOutput(
            data=None,
            step_status=StatusEnum.FAILED,
            step_failure_message=[f"Exception in write_raw_to_s3: {e}"],
        )
