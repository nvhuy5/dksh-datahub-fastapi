# Third-Party Imports
from enum import Enum
from typing import Type
from dataclasses import dataclass

# Local Application Imports
from models.tracking_models import TrackingModel
from processors import file_processors, master_processors


@dataclass
class ProcessorMeta:
    """
    Metadata for a file processor.

    Stores configuration details for a specific file processor, including its class,
    description, and input/output types.

    Attributes:
        cls (Type): The processor class to handle the file.
        description (str): Description of the processor and its purpose.
        input_type (str): Expected input file type (e.g., 'pdf', 'txt').
        output_type (str): Output data type (e.g., 'dataframe').
    """

    cls: Type
    description: str
    input_type: str
    output_type: str


class ProcessorTemplate(Enum):
    """Registry of file processor templates.

    Maps template names to `ProcessorMeta` instances for processing specific file
    types (e.g., PO or master data). Provides methods to create processor instances
    and access metadata.
    """

    """
    Template Registry
    """

    def create_instance(self, tracking_model: TrackingModel) -> object:
        """Creates an instance of the processor class for the given file path.

        Args:
            file_path (str): Path to the file to be processed.

        Returns:
            object: Instance of the processor class.
        """
        return self.value.cls(tracking_model)

    @property
    def description(self) -> str:
        """Gets the description of the processor.

        Returns:
            str: Description of the processor from its metadata.
        """
        return self.value.description

    def __repr__(self) -> str:
        """Returns a string representation of the processor.

        Returns:
            str: String in the format 'name (input_type → output_type)'.
        """
        return f"{self.name} ({self.input_type} → {self.output_type})"

    # ======================================================== #
    # === Registry the template to specific file processor === #
    PDF_001_TEMPLATE = ProcessorMeta(
        cls=file_processors.pdf_processor.Pdf001Template,
        description="PDF layout processor for PO template - 0C-RLBH75-K0.pdf",
        input_type="pdf",
        output_type="dataframe",
    )
 
    PDF_002_TEMPLATE = ProcessorMeta(
        cls=file_processors.pdf_processor.Pdf002Template,
        description="PDF layout processor for PO template - 0819啄木鳥A.pdf",
        input_type="pdf",
        output_type="dataframe",
    )
 
    PDF_003_TEMPLATE = ProcessorMeta(
        cls=file_processors.pdf_processor.Pdf002Template,
        description="PDF layout processor for PO template - 20240628120641957.pdf",
        input_type="pdf",
        output_type="dataframe",
    )
 
    PDF_004_TEMPLATE = ProcessorMeta(
        cls=file_processors.pdf_processor.Pdf004Template,
        description="PDF layout processor for PO template - 20240722102127096.pdf",
        input_type="pdf",
        output_type="dataframe",
    )

    PDF_005_TEMPLATE = ProcessorMeta(
        cls=file_processors.pdf_processor.Pdf002Template,
        description="PDF layout processor for PO template - 20240814141011543.pdf",
        input_type="pdf",
        output_type="dataframe",
    )

    PDF_006_TEMPLATE = ProcessorMeta(
        cls=file_processors.pdf_processor.Pdf006Template,
        description="PDF layout processor for PO template - A202405220043.pdf",
        input_type="pdf",
        output_type="dataframe",
    )

    PDF_007_TEMPLATE = ProcessorMeta(
        cls=file_processors.pdf_processor.Pdf007Template,
        description="PDF layout processor for PO template - O20240620TPB026.PDF",
        input_type="pdf",
        output_type="dataframe",
    )

    PDF_008_TEMPLATE = ProcessorMeta(
        cls=file_processors.pdf_processor.Pdf008Template,
        description="PDF layout processor for PO template - RSV_1921_M24081500290_DC3.pdf",
        input_type="pdf",
        output_type="dataframe",
    )

    TXT_001_TEMPLATE = ProcessorMeta(
        cls=file_processors.txt_processor_new.Txt001Template,
        description="TXT layout processor for PO template - 0809-1.TXT",
        input_type="txt",
        output_type="dataframe",
    )

    TXT_002_TEMPLATE = ProcessorMeta(
        cls=file_processors.txt_processor_new.Txt002Template,
        description="TXT layout processor for PO template - 20240726-131542-w25out20240726全聯.TXT",
        input_type="txt",
        output_type="dataframe",
    )

    TXT_003_TEMPLATE = ProcessorMeta(
        cls=file_processors.txt_processor_new.Txt003Template,
        description="TXT layout processor for PO template - DELV082001.TXT",
        input_type="txt",
        output_type="dataframe",
    )

    TXT_004_TEMPLATE = ProcessorMeta(
        cls=file_processors.txt_processor_new.Txt004Template,
        description="TXT layout processor for PO template - 20240711-143536-w25in20240711.TXT",
        input_type="txt",
        output_type="dataframe",
    )

    XLS_001_TEMPLATE = ProcessorMeta(
        cls=file_processors.excel_processor.ExcelProcessor,
        description="Excel layout processor for PO template - 開元進大昌華嘉20240822 的複本.xls",
        input_type="xls",
        output_type="dataframe",
    )

    XLS_002_TEMPLATE = ProcessorMeta(
        cls=file_processors.excel_processor.ExcelProcessor,
        description="Excel layout processor for PO template - 20240819生豆 SAP(0D97) 的複本.xls",
        input_type="xls",
        output_type="dataframe",
    )

    XLSX_001_TEMPLATE = ProcessorMeta(
        cls=file_processors.excel_processor.ExcelProcessor,
        description="Excel layout processor for PO template - 20240619_121506-camacafé門市訂單明細表-J0143大昌華嘉(蛋糕加帕尼尼)20240621.xlsx",
        input_type="xlsx",
        output_type="dataframe",
    )

    XLSX_002_TEMPLATE = ProcessorMeta(
        cls=file_processors.excel_processor.ExcelProcessor,
        description="Excel layout processor for PO template - 2024082328-NIVEA+FMCG.xlsx",
        input_type="xlsx",
        output_type="dataframe",
    )

    XML_001_TEMPLATE = ProcessorMeta(
        cls=file_processors.xml_processor.XMLProcessor,
        description="Xml layout processor for all PO template",
        input_type="xml",
        output_type="dataframe",
    )

    CSV_001_TEMPLATE = ProcessorMeta(
        cls=file_processors.csv_processor.CSVProcessor,
        description="Csv layout processor for PO template - Purchase_KFC_20240730_053929--貨櫃.csv",
        input_type="csv",
        output_type="dataframe",
    )

    CSV_002_TEMPLATE = ProcessorMeta(
        cls=file_processors.csv_processor.CSVProcessor,
        description="Csv layout processor for PO template - Purchase_PH_20240731_015930-退.CSV",
        input_type="csv",
        output_type="dataframe",
    )

    CSV_003_TEMPLATE = ProcessorMeta(
        cls=file_processors.csv_processor.CSVProcessor,
        description="Csv layout processor for PO template - DN800018251920240708123641.csv",
        input_type="csv",
        output_type="dataframe",
    )

    CSV_004_TEMPLATE = ProcessorMeta(
        cls=file_processors.csv_processor.CSVProcessor,
        description="Csv layout processor for PO template - Transfer_KFC_20240708_141225.csv",
        input_type="csv",
        output_type="dataframe",
    )

    # ================================================================== #
    # === Registry the template to specific processor for masterdata === #
    TXT_MASTERADATA_TEMPLATE = ProcessorMeta(
        cls=master_processors.txt_master_processor.TxtMasterProcessor,
        description="TXT layout processor for metadata template",
        input_type="txt",
        output_type="dataframe",
    )
    EXCEL_MASTERADATA_TEMPLATE = ProcessorMeta(
        cls=master_processors.excel_master_processor.ExcelMasterProcessor,
        description="Excel layout processor for metadata template",
        input_type="xls or xlsx",
        output_type="dataframe",
    )
