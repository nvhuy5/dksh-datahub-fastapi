import logging
from typing import Dict, Any, Union
from pathlib import Path
from utils import log_helpers
from processors.helpers.xml_helper import build_processor_setting_xml

# === Set up logging ===
logger = log_helpers.get_logger("Workflow output builder")

def build_data_output(context_data: Any, step: Any, step_result: Any) -> Union[Dict[str, Any], str]:
    """
    Build data_output object for the finish step payload.
    If the step is TEMPLATE_FILE_PARSE, returns a detailed dict.
    Otherwise, returns the S3 prefix as a simple string.
    """
    special_keywords = ["SUBMIT", "XSL_TRANSLATION", "METADATA_EXTRACT", "SEND_TO", "RENAME"]

    if step.stepName == "TEMPLATE_FILE_PARSE" and step_result.output:
        original_path = step_result.output.original_file_path
        return {
            "totalRecords": len(step_result.output.items),
            "storageLocation": str(original_path),
            "fileLogLink": f"{context_data.s3_key_prefix}/{original_path.stem}.json"
        }
    
    if step.stepName == "TEMPLATE_FORMAT_VALIDATION":
        output = step_result.output
        
        # Check if validation_stats exists in the output
        if hasattr(output, 'validation_stats') and isinstance(output.validation_stats, dict):
            data_output = output.validation_stats
            original_path = output.original_file_path
            return {
                "totalRecords": data_output.get("totalRecords", len(output.items)),
                "validRecords": data_output.get("validRecords", 0),
                "errorRecords": data_output.get("errorRecords", 0),
                "fileLogLink": f"{context_data.s3_key_prefix}/{original_path.stem}.json"
            }
            
    if step.stepName == "TEMPLATE_DATA_MAPPING":
        output = step_result.output
    
        if hasattr(output, "data_mapping_output") and isinstance(output.data_mapping_output, dict):
            data_output = output.data_mapping_output
            original_path = output.original_file_path
            return {
                "totalHeaders": data_output.get("totalHeaders", 0),
                "mappedHeaders": data_output.get("mappedHeaders", 0),
                "unmappedHeaders": data_output.get("unmappedHeaders", 0),
                "fileLogLink": f"{context_data.s3_key_prefix}/{original_path.stem}.json"
            }
    
    if any(key in step.stepName.upper() for key in special_keywords):
        step_detail = context_data.step_detail[step.stepOrder]
        config_api_records = getattr(step_detail, "config_api", [])

        processor_args = []
        for record in config_api_records:
            response = record.get("response", {})
            if "processorArgumentDtos" in response:
                raw_args = response["processorArgumentDtos"] or []
                processor_args = [
                    {
                        "name": arg["processorArgumentName"],
                        "value": arg["value"]
                    }
                    for arg in raw_args
                ]
                break

        xml_data = build_processor_setting_xml(processor_args)
    
        return {
                "processorArgs": processor_args or [],
                "processorConfigXml": xml_data or "<PROCESSORSETTINGXML></PROCESSORSETTINGXML>",
                "fileLogLink": f"{context_data.s3_key_prefix}/"
            }

