import unittest
from unittest.mock import MagicMock, patch
from botocore.exceptions import ClientError

from fastapi_celery.models.class_models import StepOutput, StatusEnum, DocumentType
from fastapi_celery.processors.workflow_processors import write_json_to_s3


class DummySelf:
    def __init__(self, document_type=DocumentType.ORDER):
        self.document_type = document_type
        self.file_record = {"file_name": "test.json", "file_extension": ".json"}
        self.target_bucket_name = "test-bucket"
        self.tracking_model = {"id": 1}
        self.current_time = "20250101T000000"
        self.current_output_path = None


class TestWriteJsonToS3Fixed(unittest.TestCase):

    @patch("utils.read_n_write_s3.write_json_to_s3")
    def test_write_json_to_s3_success(self, mock_write):
        dummy = DummySelf()
        mock_write.return_value = "s3://test/path.json"

        result = write_json_to_s3.write_json_to_s3(dummy, input_data={"a": 1})

        self.assertEqual(result.output, "s3://test/path.json")
        self.assertEqual(result.step_status, StatusEnum.SUCCESS)
        self.assertIsNone(result.step_failure_message)

    def test_write_json_to_s3_unknown_document_type(self):
        dummy = DummySelf(document_type="UNKNOWN")
        result = write_json_to_s3.write_json_to_s3(dummy, input_data={"a": 1})
        self.assertEqual(result.step_status, StatusEnum.FAILED)
        self.assertIn("Unknown document type", result.step_failure_message[0])

    @patch("utils.read_n_write_s3.write_json_to_s3")
    def test_write_json_to_s3_exception(self, mock_write):
        dummy = DummySelf()
        mock_write.side_effect = Exception("S3 write failed")
        result = write_json_to_s3.write_json_to_s3(dummy, input_data={"a": 1})
        self.assertEqual(result.step_status, StatusEnum.FAILED)
        self.assertIsNone(result.output)
        self.assertIn("S3 write failed", result.step_failure_message[0])

    def test_write_json_to_s3_no_input_data(self):
        dummy = DummySelf()
        result = write_json_to_s3.write_json_to_s3(dummy, input_data=None)
        self.assertIsNone(result.output)
        self.assertIsNone(result.step_status)
        self.assertIn("No input data provided", result.step_failure_message[0])

    @patch("utils.read_n_write_s3.write_json_to_s3")
    def test_write_json_to_s3_master_data_customized_object_name(self, mock_write):
        dummy = DummySelf(document_type=DocumentType.MASTER_DATA)
        mock_write.return_value = "s3://custom/path.json"
        result = write_json_to_s3.write_json_to_s3(
            dummy,
            input_data={"key": "value"},
            customized_object_name="custom.json",
        )
        self.assertEqual(result.output, "s3://custom/path.json")
        self.assertEqual(dummy.file_record["s3_key_prefix"], "process_data/test/")

    def test_list_all_attempt_objects_some_found(self):
        client = MagicMock()
        client.head_object.side_effect = [
            None,
            ClientError({"Error": {"Code": "404"}}, "head_object"),
        ]
        result = write_json_to_s3.list_all_attempt_objects(
            client, "bucket", "path/", "base", rerun_attempt=2
        )
        self.assertIn("path/base_rerun_1.json", result)
        self.assertNotIn("path/base.json", result)

    def test_list_all_attempt_objects_clienterror_other(self):
        client = MagicMock()
        client.head_object.side_effect = ClientError({"Error": {"Code": "500"}}, "head_object")
        with self.assertRaises(ClientError):
            write_json_to_s3.list_all_attempt_objects(
                client, "bucket", "path/", "base", rerun_attempt=1
            )

    @patch("fastapi_celery.processors.workflow_processors.write_json_to_s3.check_step_result_exists_in_s3")
    def test_check_step_result_exists_in_s3_exception(self, mock_check):
        dummy = DummySelf()
        mock_check.return_value = "parsed_data"

        result = write_json_to_s3.check_step_result_exists_in_s3(
            dummy, task_id="t2", step_name="step", s3_key_prefix="prefix"
        )
        self.assertEqual(result, "parsed_data")

    @patch("fastapi_celery.processors.workflow_processors.write_json_to_s3.check_step_result_exists_in_s3")
    def test_check_step_result_exists_in_s3_file_found(self, mock_check):
        dummy = DummySelf()
        mock_check.return_value = "parsed_data"

        result = write_json_to_s3.check_step_result_exists_in_s3(
            dummy, task_id="t1", step_name="step", s3_key_prefix="prefix"
        )
        mock_check.assert_called_once()
        self.assertEqual(result, "parsed_data")

    @patch("utils.read_n_write_s3.read_json_from_s3")
    def test_check_step_result_exists_in_s3_file_not_found(self, mock_read):
        dummy = DummySelf()
        mock_read.return_value = None

        result = write_json_to_s3.check_step_result_exists_in_s3(
            dummy, task_id="t3", step_name="step", s3_key_prefix="prefix"
        )

        self.assertIsNone(result)
        self.assertEqual(dummy.current_output_path, "prefix/test.json")


if __name__ == "__main__":
    unittest.main()
