import logging
from typing import List, Dict, Any
from utils import log_helpers
from datetime import datetime
import pandas as pd
from connections.be_connection import BEConnector
from models.class_models import (
    MasterDataParsed,
    ApiUrl,
    DocumentType,
    StatusEnum,
    StepOutput,
)
from models.tracking_models import ServiceLog, LogType, TrackingModel
from processors.processor_base import logger


class MasterValidation:
    def __init__(self, masterdata_json: MasterDataParsed, tracking_model: TrackingModel):
        self.masterdata_json = masterdata_json
        self.tracking_model = tracking_model
        self.masterdata_headers = self.masterdata_json.headers
        self.masterdata = pd.DataFrame(self.masterdata_json.items)

    def header_validation(
        self, header_reference: List[Dict[str, Any]]
    ) -> MasterDataParsed:
        """
        Validates the headers of the master data against a reference schema.

        Args:
            header_reference (List[Dict[str, Any]]): A list of schema definitions where each dictionary includes:
                - "name" (str): The expected name of the header field.
                - "posidx" (int): The expected position of the header field in the data.

        Returns:
            MasterDataParsed: An updated master data
        """
        # Sort schema by posidx to compare with header order
        sorted_schema = sorted(header_reference, key=lambda x: x["posidx"])
        error_messages = []

        if len(sorted_schema) != len(self.masterdata_headers):
            logger.error(
                f"Length mismatch: schema has {len(sorted_schema)} fields, headers have {len(self.masterdata_headers)}",
                extra={
                    "service": ServiceLog.METADATA_VALIDATION,
                    "log_type": LogType.ERROR,
                    "data": self.tracking_model,
                },
            )
            message = (
                f"Length mismatch: schema has {len(sorted_schema)} fields, "
                f"headers have {len(self.masterdata_headers)}"
            )
            error_messages.append(message)

        for idx, (schema_item, header) in enumerate(
            zip(sorted_schema, self.masterdata_headers)
        ):
            if schema_item["name"] != header:
                logger.error(
                    f"Mismatch at position {idx}: expected '{schema_item['name']}', got '{header}'",
                    extra={
                        "service": ServiceLog.METADATA_VALIDATION,
                        "log_type": LogType.ERROR,
                        "data": self.tracking_model,
                    },
                )
                message = f"Mismatch at position {idx}: expected '{schema_item['name']}', got '{header}'"
                error_messages.append(message)
        if not error_messages:
            logger.info(
                f"{__name__} successfully executed!",
                extra={
                    "service": ServiceLog.METADATA_VALIDATION,
                    "log_type": LogType.TASK,
                    "data": self.tracking_model,
                },
            )
            updated = self.masterdata_json.model_copy(
                update={
                    "step_status": StatusEnum.SUCCESS,
                    "messages": None,
                }
            )
        else:
            updated = self.masterdata_json.model_copy(
                update={
                    "step_status": StatusEnum.FAILED,
                    "messages": error_messages,
                }
            )
        return updated

    def data_validation(self, data_reference: List[Dict[str, Any]]) -> MasterDataParsed:
        """
        Validates a list of data records against a reference schema using pandas.

        Stops validation and returns immediately upon encountering the first error.

        Parameters:
            data_reference (List[Dict[str, Any]]): List of field schema definitions, each with:
                - name (str): Field name.
                - datatype (str): One of "int", "bigint", "float", "timestamp", "string".
                - nullable (bool, optional): Whether the field can be empty. Defaults to True.
                - maxlength (int, optional): Max length allowed for string fields.

        Returns:
            MasterDataParsed: An updated master data
        """
        for field in data_reference:
            name = field["name"]
            nullable = field.get("nullable", True)
            datatype = field["datatype"]
            maxlength = field.get("maxlength")

            if name not in self.masterdata.columns:
                return self._fail(f"Missing column '{name}' in data.")

            col = self.masterdata[name]
            if not nullable and self._has_null(col):
                idx = self._first_null_index(col)
                return self._fail(
                    f"Row {idx}: Field '{name}' is required but missing/empty."
                )

            if self._has_invalid_type(col, datatype, maxlength):
                idx = self._first_invalid_index(col, datatype, maxlength)
                val = col.iloc[idx]
                return self._fail(
                    f"Row {idx}: Field '{name}' has invalid value '{val}' for type '{datatype}'."
                )

        logger.info(
            f"{__name__} successfully executed!",
            extra=self._log_extra(log_type=LogType.TASK),
        )
        return self.masterdata_json.model_copy(
            update={"step_status": StatusEnum.SUCCESS, "messages": None}
        )

    # === Helpers ===
    def _fail(self, message: str) -> MasterDataParsed:
        logger.error(message, extra=self._log_extra(log_type=LogType.ERROR))
        messages = self.masterdata_json.messages or []
        messages.append(message)
        return self.masterdata_json.model_copy(
            update={"step_status": StatusEnum.FAILED, "messages": messages}
        )

    def _log_extra(self, log_type: LogType) -> dict:
        return {
            "service": ServiceLog.METADATA_VALIDATION,
            "log_type": log_type,
            "data": self.tracking_model,
        }

    def _has_null(self, col: pd.Series) -> bool:
        return (col.isnull() | col.astype(str).str.strip().eq("")).any()

    def _first_null_index(self, col: pd.Series) -> int:
        null_mask = col.isnull() | col.astype(str).str.strip().eq("")
        return null_mask.idxmax()

    def _has_invalid_type(
        self, col: pd.Series, dtype: str, maxlength: int | None
    ) -> bool:
        return self._build_type_mask(col, dtype, maxlength).any()

    def _first_invalid_index(
        self, col: pd.Series, dtype: str, maxlength: int | None
    ) -> int:
        return self._build_type_mask(col, dtype, maxlength).idxmax()

    def _build_type_mask(
        self, col: pd.Series, dtype: str, maxlength: int | None
    ) -> pd.Series:
        col_str = col.astype(str)
        if dtype in ("int", "bigint"):
            return ~col_str.str.fullmatch(r"-?\d+")
        elif dtype == "float":
            return ~col_str.str.fullmatch(r"-?\d+(\.\d+)?")
        elif dtype == "timestamp":
            return ~col_str.apply(lambda x: self._is_valid_date(x, "%Y%m%d"))
        elif dtype == "string" and maxlength is not None:
            return col_str.str.len() > maxlength
        return pd.Series([False] * len(col))

    def _is_valid_date(self, value: str, fmt) -> bool:
        try:
            datetime.strptime(value, fmt)
            return True
        except Exception:
            return False


async def masterdata_header_validation(self, input_data: StepOutput) -> StepOutput:
    """
    Validates the header section of the input data against a provided reference schema.

    Args:
        reference (dict): A dictionary representing the expected header schema,
                          typically defining required fields and formats.

    Returns:
        bool: True if header validation passes, False otherwise.
    """
    valid_headers = await BEConnector(
        ApiUrl.MASTERDATA_HEADER_VALIDATION.full_url(),
        params={"fileName": self.file_record["file_name"]},
    ).get()
    master_data = MasterValidation(masterdata_json=input_data.output, tracking_model=self.tracking_model)
    header_validation_result = master_data.header_validation(
        header_reference=valid_headers
    )

    return StepOutput(
        output=header_validation_result,
        step_status=(
            StatusEnum.SUCCESS
            if header_validation_result.step_status == StatusEnum.SUCCESS
            else StatusEnum.FAILED
        ),
        step_failure_message=(
            None
            if header_validation_result.step_status == StatusEnum.SUCCESS
            else header_validation_result.messages
        ),
    )


async def masterdata_data_validation(self, input_data: StepOutput) -> StepOutput:
    """
    Validates the data rows in the input data against a provided reference schema.

    Args:
        reference (dict): A dictionary describing the validation schema for each field,
                          including keys like 'name', 'datatype', 'nullable', and 'maxlength'.

    Returns:
        bool: True if all data rows pass validation, False otherwise.
    """
    valid_data = await BEConnector(
        ApiUrl.MASTERDATA_COLUMN_VALIDATION.full_url(),
        params={"fileName": self.file_record["file_name"]},
    ).get()
    master_data = MasterValidation(masterdata_json=input_data.output, tracking_model=self.tracking_model)
    data_validation_result = master_data.data_validation(data_reference=valid_data)

    return StepOutput(
        output=data_validation_result,
        step_status=(
            StatusEnum.SUCCESS
            if data_validation_result.step_status == StatusEnum.SUCCESS
            else StatusEnum.FAILED
        ),
        step_failure_message=(
            None
            if data_validation_result.step_status == StatusEnum.SUCCESS
            else data_validation_result.messages
        ),
    )
