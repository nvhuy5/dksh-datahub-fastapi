from typing import Optional, Type
from processors.processor_template import ProcessorTemplate
from connections.be_connection import BEConnector
from models.class_models import ApiUrl, DocumentType
from utils import log_helpers

# === Set up logging ===
logger = log_helpers.get_logger("ProcessorRegistry")


class ProcessorRegistry:
    """
    Central registry to fetch processor mapping from BE based on workflowStepId.
    """

    # Static mapping of BE template codes to TemplateProcessor enums
    code_to_processor = {
        # PO specific templates
        "TXT_001_TEMPLATE": ProcessorTemplate.TXT_001_TEMPLATE,
        "TXT_002_TEMPLATE": ProcessorTemplate.TXT_002_TEMPLATE,
        "TXT_003_TEMPLATE": ProcessorTemplate.TXT_003_TEMPLATE,
        "TXT_004_TEMPLATE": ProcessorTemplate.TXT_004_TEMPLATE,
        "XLS_001_TEMPLATE": ProcessorTemplate.XLS_001_TEMPLATE,
        "XLS_002_TEMPLATE": ProcessorTemplate.XLS_002_TEMPLATE,
        "XLSX_001_TEMPLATE": ProcessorTemplate.XLSX_001_TEMPLATE,
        "XLSX_002_TEMPLATE": ProcessorTemplate.XLSX_002_TEMPLATE,
        "XML_001_TEMPLATE": ProcessorTemplate.XML_001_TEMPLATE,
        "CSV_001_TEMPLATE": ProcessorTemplate.CSV_001_TEMPLATE,
        "CSV_002_TEMPLATE": ProcessorTemplate.CSV_002_TEMPLATE,
        "CSV_003_TEMPLATE": ProcessorTemplate.CSV_003_TEMPLATE,
        "CSV_004_TEMPLATE": ProcessorTemplate.CSV_004_TEMPLATE,
        "PDF_001_TEMPLATE" : ProcessorTemplate.PDF_001_TEMPLATE,
        "PDF_002_TEMPLATE" : ProcessorTemplate.PDF_002_TEMPLATE,
        "PDF_003_TEMPLATE" : ProcessorTemplate.PDF_003_TEMPLATE,
        "PDF_004_TEMPLATE" : ProcessorTemplate.PDF_004_TEMPLATE,
        "PDF_005_TEMPLATE" : ProcessorTemplate.PDF_005_TEMPLATE,
        "PDF_006_TEMPLATE" : ProcessorTemplate.PDF_006_TEMPLATE,
        "PDF_007_TEMPLATE" : ProcessorTemplate.PDF_007_TEMPLATE,
        "PDF_008_TEMPLATE" : ProcessorTemplate.PDF_008_TEMPLATE,
        # Masterdata specific templates
        "TXT_MASTERDATA_TEMPLATE": ProcessorTemplate.TXT_MASTERADATA_TEMPLATE,
        "EXCEL_MASTERDATA_TEMPLATE": ProcessorTemplate.EXCEL_MASTERADATA_TEMPLATE,
    }

    @classmethod
    async def get_processor_for_file(cls, file_processor) -> ProcessorTemplate:
        """
        Calls BE to get processor mapping for this file, returns processor instance.

        Args:
            file_processor: The FileProcessor object.

        Returns:
            TemplateProcessor instance.
        """

        try:
            if file_processor.document_type == DocumentType.ORDER:
                step_id = file_processor.workflow_step_ids.get("TEMPLATE_FILE_PARSE")
            else: 
                step_id = file_processor.workflow_step_ids.get("MASTER_DATA_FILE_PARSER")
            
            if not step_id:
                logger.error(f"[{file_processor.tracking_model.request_id}] Missing current_step_id in context")
                raise RuntimeError("Missing workflowStepId in context")

            # === Call BE ===
            connector = BEConnector(ApiUrl.WORKFLOW_TEMPLATE_PARSE.full_url(), params={"workflowStepId": step_id})
            response = await connector.get()

            try:
                # If the response is a dictionary with a "data" key as expected
                if isinstance(response, dict):
                    template_info = response.get("data", [{}])[0].get(
                        "templateFileParse", {}
                    )
                    template_code = template_info.get("code")

                # If the response is a list (in case BE returns a raw list)
                elif isinstance(response, list):
                    template_info = response[0].get("templateFileParse", {})
                    template_code = template_info.get("code")
                else:
                    raise RuntimeError("Invalid response format from BE.")

                if not template_code:
                    raise RuntimeError("Invalid BE response structure: missing template code")

            except (KeyError, IndexError, TypeError):
                raise RuntimeError(
                    "Invalid BE response structure: missing template code."
                )

            processor_enum = cls._map_code_to_processor(template_code)
            if not processor_enum:
                logger.error(
                    f"[{file_processor.tracking_model.request_id}] Unknown template code from BE: {template_code}",
                    extra={
                        "service": "ProcessorRegistry",
                        "log_type": "ERROR",
                        "data": file_processor.tracking_model,
                    },
                )
                raise RuntimeError(f"Unknown template code: {template_code}")

            logger.info(
                f"[{file_processor.tracking_model.request_id}] Resolved processor: {processor_enum.name}",
                extra={
                    "service": "ProcessorRegistry",
                    "log_type": "INFO",
                    "data": file_processor.tracking_model,
                },
            )

            return processor_enum.create_instance(tracking_model=file_processor.tracking_model)

        except Exception as e:
            logger.error(
                f"[{file_processor.tracking_model.request_id}] Error in get_processor_for_file: {str(e)}",
                extra={
                    "service": "ProcessorRegistry",
                    "log_type": "ERROR",
                    "data": file_processor.tracking_model,
                },
            )
            raise

    @classmethod
    def _map_code_to_processor(cls, code: str) -> Optional[Type[ProcessorTemplate]]:
        return cls.code_to_processor.get(code)
