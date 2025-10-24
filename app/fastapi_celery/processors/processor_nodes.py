from models.class_models import StepDefinition


WORKFLOW_PROCESSORS = [
    "extract_metadata",
    "template_mapping",
    "parse_file_to_json",
    "publish_data",
    "template_validation",
    "write_json_to_s3",
    # === Masterdata only ===
    "master_validation",
    "write_raw_to_s3",
    # === Rule mapping only ===
    "rule_mapping_submit",
    "rule_mapping_xsl_translation",
    "rule_mapping_metadata_extract",
    "rule_mapping_send_to"
]

PROCESS_DEFINITIONS = {
    "TEMPLATE_FILE_PARSE": StepDefinition(
        function_name="parse_file_to_json",
        data_input=None,
        data_output="parsed_data",
        extract_to={
            "document_number": "po_number",  # PO_NUMBER
            "document_type": "document_type",  # master_data or order
        },
        # Write data to S3 after the step
        require_data_output=True,
        target_store_data="workflow-node-materialized",

    ),
    "TEMPLATE_DATA_MAPPING": StepDefinition(
        function_name="template_data_mapping",
        data_input="parsed_data",
        data_output="mapped_data",
        require_data_output=True,
        target_store_data="workflow-node-materialized",
    ),
    "TEMPLATE_FORMAT_VALIDATION": StepDefinition(
        function_name="template_format_validation",
        data_input="parsed_data",
        data_output="data_validation",
        require_data_output=True,
        target_store_data="workflow-node-materialized",
    ),
    "write_json_to_s3": StepDefinition(
        function_name="write_json_to_s3",
        data_input="parsed_data",
        data_output="s3_result",
        require_data_output=True,
        target_store_data="workflow-node-materialized",
    ),
    "publish_data": StepDefinition(
        function_name="publish_data",
        data_output="publish_data_result",
        require_data_output=True,
        target_store_data="workflow-node-materialized",
    ),
    # === Masterdata only ===
    "MASTER_DATA_FILE_PARSER": StepDefinition(
        function_name="parse_file_to_json",
        data_input=None,
        data_output="master_data_parsed",
        require_data_output=True,
        target_store_data="workflow-node-materialized",
    ),
    "MASTER_DATA_VALIDATE_HEADER": StepDefinition(
        function_name="masterdata_header_validation",
        data_input="master_data_parsed",
        data_output="masterdata_header_validation",
        require_data_output=True,
        target_store_data="workflow-node-materialized",
    ),
    "MASTER_DATA_VALIDATE_DATA": StepDefinition(
        function_name="masterdata_data_validation",
        data_input="master_data_parsed",
        data_output="masterdata_data_validation",
        require_data_output=True,
        target_store_data="workflow-node-materialized",
    ),
    "MASTER_DATA_LOAD_DATA": StepDefinition(
        function_name="write_json_to_s3",
        data_input="masterdata_data_validation",
        data_output="s3_result",
        # Custom flag to customize the object path to S3
        # The value will be handled in the function via **kwargs arg
        kwargs={"customized_object_name": True},
        require_data_output=True,
        target_store_data="workflow-node-materialized",
    ),
    # === Rule mapping only ===
    "SUBMIT" : StepDefinition(
        function_name = "submit",
        data_input="mapped_data",
        data_output="submit_data",
        require_data_output=True,
        target_store_data="workflow-node-materialized",
    ),
    "XSL_TRANSLATION" : StepDefinition(
        function_name = "xsl_translation",
        data_input="mapped_data",
        data_output="xsl_translation_data",
        require_data_output=True,
        target_store_data="workflow-node-materialized",
    ),
    "METADATA_EXTRACT" : StepDefinition(
        function_name = "metadata_extract",
        data_input="mapped_data",
        data_output="metadata_extract",
        require_data_output=True,
        target_store_data="workflow-node-materialized",
    ),
    "SEND_TO" : StepDefinition(
        function_name = "send_to",
        data_input="mapped_data",
        data_output="send_to",
        require_data_output=True,
        target_store_data="workflow-node-materialized",
    ),
}

