import json
import re
from typing import List, Dict, Any
import pandas as pd
from connections.be_connection import BEConnector
from models.class_models import (
    ApiUrl,
    StatusEnum,
    StepOutput,
    PODataParsed,
)
from models.tracking_models import ServiceLog, LogType, TrackingModel
from processors.processor_base import logger 

class TemplateValidation:
    """
    Perform template data validation for PODataParsed against schema from API.
    """
    def __init__(self, po_json: PODataParsed, tracking_model: TrackingModel):
        self.po_json = po_json
        self.tracking_model = tracking_model
        self.items = po_json.items if isinstance(po_json.items, list) else [po_json.items]

    def _check_required(self, val: Any, required: bool, allow_empty: bool, col_key: str, idx: int) -> str | None:
        if required and not allow_empty and (val is None or str(val).strip() == ""):
            return f"Row {idx}: Field '{col_key}' is required but empty"
        return None
 
    def _check_max_length(self, val: Any, max_length: int | None, col_key: str, idx: int) -> str | None:
        if max_length and len(str(val)) > int(max_length):
            return f"Row {idx}: Field '{col_key}' exceeds maxLength {max_length}"
        return None
 
    def _check_regex(self, val: Any, regex: str | None, col_key: str, idx: int) -> str | None:
        if regex and not re.fullmatch(regex, str(val)):
            return f"Row {idx}: Field '{col_key}'='{val}' does not match regex {regex}"
        return None
 
    def _check_dtype(self, val: Any, dtype: str | None, col_key: str, idx: int) -> str | None:
        if dtype == "Number" and not re.fullmatch(r"-?\d+(\.\d+)?", str(val)):
            return f"Row {idx}: Field '{col_key}'='{val}' is not a valid number"
        if dtype == "Date":
            try:
                pd.to_datetime(val, errors="raise")
            except Exception:
                return f"Row {idx}: Field '{col_key}'='{val}' is not a valid date"
        return None
 
    def _validate_cell(self, val: Any, col_def: Dict[str, Any], col_key: str, idx: int) -> tuple[List[str], bool]:
        metadata = json.loads(col_def.get("metadata", "{}"))
        errors = []
        is_error = False

        # Required check
        err = self._check_required(val, metadata.get("required", False), metadata.get("allowEmpty", True), col_key, idx)
        if err:
            is_error = True
            errors.append(err)
            return errors, is_error 

        if val is None or str(val).strip() == "":
            return errors, is_error 

        # Max length check
        if (err := self._check_max_length(val, metadata.get("maxLength"), col_key, idx)):
            is_error = True
            errors.append(err)

        # Regex check
        if (err := self._check_regex(val, metadata.get("regex"), col_key, idx)):
            is_error = True
            errors.append(err)

        # Data type check
        if (err := self._check_dtype(val, col_def.get("dataType"), col_key, idx)):
            is_error = True
            errors.append(err)

        return errors, is_error 
 
    def data_validation(self, schema_columns: List[Dict[str, Any]]) -> tuple[PODataParsed, dict]:
        """
        Validate PO items against schema columns definition.
        """
        errors = []
        total_records = len(self.items)
        error_records = 0

        df_columns = list(self.items[0].keys()) if self.items else []

        for col_def in schema_columns:
            order = col_def.get("order")
            if not order or not isinstance(order, int):
                continue  

            col_index = order - 1
            if col_index >= len(df_columns):
                continue  

            col_key = df_columns[col_index]  

            for idx, row in enumerate(self.items, start=2):
                val = row.get(col_key)
                list_error, is_error = self._validate_cell(val, col_def, col_key, idx)
                if is_error:
                    error_records = error_records + 1
                errors.extend(list_error)

        if errors:
            logger.error(
                f"Template format validation failed with {len(errors)} error(s)",
                extra=self._log_extra(log_type=LogType.ERROR),
            )
            return self.po_json.model_copy(
                update={"step_status": StatusEnum.FAILED, "messages": errors}
            ), {} 

        logger.info(
            f"{__name__} successfully executed!",
            extra=self._log_extra(log_type=LogType.TASK),
        )

        valid_records = total_records - error_records

        data_output = {
            "totalRecords": total_records,
            "validRecords": valid_records,
            "errorRecords": error_records,
            "fileLogLink": ""
        }

        return self.po_json.model_copy( 
            update={"step_status": StatusEnum.SUCCESS, "messages": None}
        ), data_output
 
    def _log_extra(self, log_type: LogType) -> dict:
        return {
            "service": ServiceLog.METADATA_VALIDATION,
            "log_type": log_type,
            "data": self.tracking_model,
        }
 
def template_format_validation(self, data_input, response_api, *args, **kwargs) -> StepOutput:

    schema_columns = []
    if isinstance(response_api, dict):
        if "data" in response_api and isinstance(response_api["data"], dict):
            schema_columns = response_api["data"].get("columns", [])
        elif "columns" in response_api:
            schema_columns = response_api.get("columns", [])
 
    if not schema_columns:
        logger.error(
            "Schema columns not found in API response",
            extra={
                "service": ServiceLog.DATA_VALIDATION,
                "log_type": LogType.ERROR,
                "data": self.tracking_model,
            },
        )
        failed_output = data_input.data.model_copy(
            update={"step_status": StatusEnum.FAILED, "messages": ["Schema columns not found in API response"]}
        )
        return StepOutput(
            data=failed_output,
            sub_data={},
            step_status=StatusEnum.FAILED,
            step_failure_message=failed_output.messages,
        )
 
    # Step 3: run validation
    po_validation = TemplateValidation(po_json=data_input.data, tracking_model=self.tracking_model)
    validation_result, data_output = po_validation.data_validation(schema_columns=schema_columns)
 
    # Step 4: Store data_output in validation_result for later use
    # Add validation_stats as a new attribute if PODataParsed supports it
    # Or use model_copy if it has a suitable field
    # validation_result_with_stats = validation_result.model_copy(
    #     update={"validation_stats": data_output} 
    # )
 
    # Step 5: wrap into StepOutput
    return StepOutput(
        data=validation_result,
        sub_data={"data_output": data_output},
        step_status=(
            StatusEnum.SUCCESS
            if validation_result.step_status == StatusEnum.SUCCESS
            else StatusEnum.FAILED
        ),
        step_failure_message=(
            None if validation_result.step_status == StatusEnum.SUCCESS else validation_result.messages
        ),
    )
 