import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path
from io import BytesIO
import pandas as pd
import unittest
from xml.etree.ElementTree import Element
from fastapi_celery.processors.file_processors.xml_processor import (
    XMLProcessor,
)
from fastapi_celery.models.class_models import SourceType, PODataParsed, StatusEnum
from fastapi_celery.processors.master_processors.excel_master_processor import (
    ExcelMasterProcessor,
)
from fastapi_celery.processors.helpers.excel_helper import (
    ExcelHelper,
    METADATA_SEPARATOR,
)
from fastapi_celery.models.tracking_models import TrackingModel

# Sample data
@pytest.fixture
def sample_dataframe():
    return pd.DataFrame(
        [
            ["Invoice No：12345", "Date：2023-07-12"],
            ["Customer(Name：Acme Corp)", ""],
            ["https://example.com", ""],
            ["Empty:", ""],
        ]
    )


@pytest.fixture
def expected_metadata():
    return {"Invoice No": "12345", "Date": "2023-07-12"}


@pytest.fixture
def processor_local():
    tm = TrackingModel.model_construct(file_path="dummy.xlsx")
    with patch.object(ExcelHelper, "read_rows", return_value=[]):
        yield ExcelHelper(tracking_model=tm, source=SourceType.LOCAL)


# Test extract_metadata for standard key:value
def test_extract_metadata_basic(processor_local):
    row = ["Key1：Value1", "Key2：Value2"]
    metadata = processor_local.extract_metadata(row)
    assert metadata == {"Key1": "Value1", "Key2": "Value2"}


# Test extract_metadata with inner metadata like Name(A：B)
def test_extract_metadata_with_inner(processor_local):
    row = ["Customer(Name：Acme Corp)"]
    metadata = processor_local.extract_metadata(row)
    assert metadata["header"] == "Customer(Name：Acme Corp)"
    assert metadata["Name"] == "Acme Corp"


# Test extract_metadata with URL and skip
def test_extract_metadata_ignores_url(processor_local):
    row = ["https://example.com"]
    metadata = processor_local.extract_metadata(row)
    assert metadata == {}


# Test extract_metadata with separator but empty value
def test_extract_metadata_empty_value(processor_local):
    row = ["PO Number：", "123456"]
    metadata = processor_local.extract_metadata(row)
    assert metadata["PO Number"] == "123456"


# Test read_rows with mocked S3 / local logic
@patch(
    "fastapi_celery.processors.helpers.excel_helper"
    ".ext_extraction.FileExtensionProcessor"
)
@patch("pandas.read_excel")
def test_read_rows_local(mock_read_excel, mock_ext_processor, sample_dataframe):
    # Setup the mock file extension processor
    mock_instance = MagicMock()
    mock_instance.file_extension = ".xlsx"
    mock_instance.file_path = Path("dummy.xlsx")
    mock_instance.source = "local"
    mock_instance._extract_file_extension.return_value = None
    mock_instance._get_document_type.return_value = "document"
    mock_instance._get_file_capacity.return_value = "small"

    mock_ext_processor.return_value = mock_instance
    mock_read_excel.return_value = {"Sheet1": sample_dataframe}

    tm = TrackingModel.model_construct(file_path="dummy.xlsx")
    processor = ExcelHelper(tracking_model=tm, source=SourceType.LOCAL)
    rows = processor.read_rows()

    assert len(rows) == 4
    assert isinstance(rows[0], list)
    assert any("Invoice No" in cell for cell in rows[0])


@pytest.fixture
def mocked_rows():
    return [
        ["Document ID：D123", "Company：ACME"],
        ["Product", "Quantity", "Price"],
        ["Widget A", "10", "100"],
        ["Widget B", "5", "50"],
        ["Note：Special discount applied"],
    ]


@pytest.fixture
def expected_output():
    return {
        "items": [
            {"Product": "Widget A", "Quantity": "10", "Price": "100"},
            {"Product": "Widget B", "Quantity": "5", "Price": "50"},
        ],
    }


@patch.object(ExcelMasterProcessor, "read_rows")
def test_parse_file_to_json_success(mock_read_rows, mocked_rows, expected_output):
    mock_read_rows.return_value = mocked_rows
    tm = TrackingModel.model_construct(file_path="dummy.xlsx")
    processor = ExcelMasterProcessor(tracking_model=tm, source=SourceType.LOCAL)

    processor.document_type = "master_data"
    processor.capacity = "small"

    result = processor.parse_file_to_json()

    assert result.step_status == StatusEnum.SUCCESS
    assert result.headers == ["Product", "Quantity", "Price"]
    assert result.items == expected_output["items"]


@patch.object(ExcelMasterProcessor, "read_rows", return_value=[["Header1：Value1"]])
def test_parse_file_to_json_error_handling(mock_read_rows):
    tm = TrackingModel.model_construct(file_path="dummy.xlsx")
    processor = ExcelMasterProcessor(tracking_model=tm, source=SourceType.LOCAL)

    processor.document_type = "master_data"
    processor.capacity = "small"

    with patch.object(
        processor, "_clean_row", side_effect=Exception("Simulated failure")
    ):
        result = processor.parse_file_to_json()

    assert result.step_status == StatusEnum.FAILED
    assert result.items == []

    print("Returned messages:", result.messages)
    if result.messages is not None:
        assert any("Simulated failure" in msg for msg in result.messages)


class TestXMLProcessor(unittest.TestCase):

    def setUp(self):
        self.tracking_model = TrackingModel.model_construct(file_path="dummy.xml")
        self.xml_content = """
        <Invoice>
            <Header>
                <Number>PO123456</Number>
                <Date>2025-07-10</Date>
            </Header>
            <Items>
                <Item>
                    <Name>Widget A</Name>
                    <Quantity>10</Quantity>
                </Item>
            </Items>
        </Invoice>
        """
        self.expected_parsed_dict = {
            "Header": {"Number": "PO123456", "Date": "2025-07-10"},
            "Items": {"Item": {"Name": "Widget A", "Quantity": "10"}},
        }

    @patch(
        "fastapi_celery.processors.file_processors.xml_processor"
        ".ext_extraction.FileExtensionProcessor"
    )
    def test_extract_text_local_file(self, mock_processor):
        mock_file = MagicMock()
        mock_file.source = "local"
        mock_file.file_path = Path("dummy.xml")
        mock_file._get_file_capacity.return_value = "small"
        mock_file._get_document_type.return_value = "invoice"

        mock_processor.return_value = mock_file

        # Simulate reading local file
        with patch(
            "builtins.open", unittest.mock.mock_open(read_data=self.xml_content)
        ):
            processor = XMLProcessor(tracking_model=self.tracking_model, source=SourceType.LOCAL)
            text = processor.extract_text()
            self.assertIn("<Invoice>", text)

    def test_parse_element_and_find_po(self):
        root = Element("Root")
        header = Element("Header")
        number = Element("Number")
        number.text = "PO999888"
        header.append(number)
        root.append(header)

        processor = XMLProcessor(tracking_model=self.tracking_model)
        parsed = processor.parse_element(root)
        self.assertEqual(parsed, {"Header": {"Number": "PO999888"}})

        po = processor.find_po_in_xml(root)
        self.assertEqual(po, "PO999888")

    @patch(
        "fastapi_celery.processors.file_processors.xml_processor"
        ".ext_extraction.FileExtensionProcessor"
    )
    def test_parse_file_to_json(self, mock_processor):
        mock_file = MagicMock()
        mock_file.source = "local"
        mock_file.file_path = Path("dummy.xml")
        mock_file._get_file_capacity.return_value = "medium"
        mock_file._get_document_type.return_value = (
            "order"
        )
        mock_processor.return_value = mock_file

        with patch(
            "fastapi_celery.processors.file_processors.xml_processor.PODataParsed"
        ) as MockPODataParsed:
            # Create a dummy return object (you can assert it was called with correct data later if needed)
            dummy_parsed = MagicMock()
            MockPODataParsed.return_value = dummy_parsed

            # Mock reading from a local file
            with patch(
                "builtins.open", unittest.mock.mock_open(read_data=self.xml_content)
            ):
                processor = XMLProcessor(tracking_model=self.tracking_model, source=SourceType.LOCAL)
                result = processor.parse_file_to_json()

                # Check that PODataParsed was constructed correctly
                MockPODataParsed.assert_called_once()
                called_args = MockPODataParsed.call_args.kwargs

                self.assertEqual(called_args["po_number"], "PO123456")
                self.assertEqual(
                    called_args["document_type"], "order"
                )  # Should match mock
                self.assertEqual(called_args["items"]["Header"]["Number"], "PO123456")
                self.assertEqual(result, dummy_parsed)  # Final result is mocked object
