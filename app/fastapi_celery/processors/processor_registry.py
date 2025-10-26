from typing import Optional, Type
from processors.processor_template import ProcessorTemplate
from utils import log_helpers

# === Set up logging ===
logger = log_helpers.get_logger("ProcessorRegistry")


class ProcessorRegistry:
    """
    Registry for resolving processor templates by backend template code.
    """

    code_to_processor = {
        # PO templates
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
        # Master templates
        "TXT_MASTERDATA_TEMPLATE": ProcessorTemplate.TXT_MASTERADATA_TEMPLATE,
        "EXCEL_MASTERDATA_TEMPLATE": ProcessorTemplate.EXCEL_MASTERADATA_TEMPLATE,
    }

    @classmethod
    def get_processor_for_file(cls, template_code: str) -> type[ProcessorTemplate] | None:
        processor_enum = cls._map_code_to_processor(template_code)
        return processor_enum

    @classmethod
    def _map_code_to_processor(cls, code: str) -> Optional[Type[ProcessorTemplate]]:
        return cls.code_to_processor.get(code)
