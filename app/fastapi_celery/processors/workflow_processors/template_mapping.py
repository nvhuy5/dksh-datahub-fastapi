import traceback
import pandas as pd
import json
from models.class_models import StatusEnum, StepOutput, ApiUrl, DocumentType
from connections.be_connection import BEConnector
from models.tracking_models import ServiceLog, LogType
from processors.processor_base import logger


FAILED_PARSE_API_MSG = "Failed to call template-parse API"


async def template_data_mapping(self, input_data: StepOutput) -> StepOutput:
    """
    Perform data mapping based on template mapping configuration.
    This step renames and reorders dataframe columns according to the mapping API response.
    """

    try:

        # Step 1: Call template-parse API to get template ID
        template_parse_resp = await BEConnector(
            ApiUrl.WORKFLOW_TEMPLATE_PARSE.full_url(),
            params={"workflowStepId": self.workflow_step_ids.get("TEMPLATE_FILE_PARSE")},
        ).get()

        if not template_parse_resp or not isinstance(template_parse_resp, list):
            raise RuntimeError(FAILED_PARSE_API_MSG)

        template_id = template_parse_resp[0]["templateFileParse"]["id"]

        # Step 2: Call data-mapping API to get mapping configuration
        mapping_resp = await BEConnector(
            ApiUrl.DATA_MAPPING.full_url(),
            params={"templateFileParseId": template_id},
        ).get()

        if not mapping_resp or "templateMappingHeaders" not in mapping_resp:
            raise RuntimeError(f"Mapping API did not return a valid response: {mapping_resp}")

        headers_sorted = sorted(
            mapping_resp["templateMappingHeaders"], key=lambda x: x["order"]
        )

        # Step 3: Convert items to DataFrame
        df = pd.DataFrame(input_data.output.items)

        # Step 4: Validate expected headers from API vs actual DataFrame columns
        expected_headers = [
            m["fromHeader"]
            for m in headers_sorted
            if m.get("fromHeader") not in (None, "Unmapping")
        ]
        missing_headers = [h for h in expected_headers if h not in df.columns]

        if missing_headers:
            error_msg = (
                f"Mapping failed: expected headers not found in input data: {missing_headers}. "
                f"Found columns: {list(df.columns)}"
            )
            logger.error(error_msg)
            return StepOutput(
                output=input_data.output.model_copy(
                    update={
                        "step_status": StatusEnum.FAILED,
                        "messages": [error_msg],
                        "metadata": {"mapping_result": json.dumps({"error": error_msg})},
                    }
                ),
                step_status=StatusEnum.FAILED,
                step_failure_message=[error_msg],
            )

        # Step 5: Build mapping dictionary: only map when fromHeader is valid and different from header
        mapping_dict = {}

        for m in headers_sorted:
            from_col = m.get("fromHeader")
            to_col = m.get("header")
            if from_col and from_col != "Unmapping" and from_col in df.columns:
                mapping_dict[from_col] = to_col

        df = df.rename(columns=mapping_dict)

        for idx, m in enumerate(headers_sorted):
            from_col = m.get("fromHeader")
            to_col = m.get("header")

            if not from_col or from_col == "Unmapping":
                df[to_col] = None

        # Step 6: Apply rename (only if mapping_dict is not empty)
        if not mapping_dict:
            logger.warning(
                "[template_data_mapping] No headers matched — skipping rename and keeping original columns."
            )
        else:
            # Apply rename if we found mappings
            df = df.rename(columns=mapping_dict)

            # Reorder to match header order from API
            ordered_headers = [m["header"] for m in headers_sorted if m.get("header") in df.columns]
            df = df[ordered_headers]
        # Step 7: Return success output
        updated_output = input_data.output.model_copy(
            update={"items": df.to_dict(orient="records")}
        )

        data_output = {
            "totalHeaders": len(headers_sorted),
            "mappedHeaders": len(mapping_dict),
            "unmappedHeaders": len(headers_sorted) - len(mapping_dict),
            "fileLogLink": ""
        }

        updated_output = updated_output.model_copy(
            update={"data_mapping_output": data_output}
        )


        return StepOutput(
            output=updated_output,
            step_status=StatusEnum.SUCCESS,
            step_failure_message=None,
        )

    except Exception as e:  # pragma: no cover
        logger.error(
            f"[template_data_mapping] Failed to map data: {e}!\n",
            extra={
                "service": ServiceLog.MAPPING,
                "log_type": LogType.ERROR,
                "data": self.tracking_model,
            },
            exc_info=True,
        )

        failed_output = input_data.output.model_copy(
            update={"step_status": StatusEnum.FAILED, "messages": [str(e)]}
        )

        return StepOutput(
            output=failed_output,
            step_status=StatusEnum.FAILED,
            step_failure_message=[traceback.format_exc()],
        )
