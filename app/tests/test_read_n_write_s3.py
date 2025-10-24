import io
import json
import unittest
from unittest.mock import MagicMock, patch
from botocore.exceptions import ClientError, BotoCoreError

from fastapi_celery.utils import read_n_write_s3 as s3_utils


class TestReadWriteS3(unittest.TestCase):

    def setUp(self):
        self.bucket_name = "test-bucket"
        self.object_name = "test.json"
        self.client = MagicMock()
        s3_utils._s3_connectors.clear()

    # === put_object ===
    def test_put_object_with_buffer_success(self):
        buf = io.BytesIO(b"data")
        result = s3_utils.put_object(self.client, self.bucket_name, self.object_name, buf)
        self.client.upload_fileobj.assert_called_once()
        self.assertEqual(result["status"], "Success")

    def test_put_object_with_filepath_success(self):
        result = s3_utils.put_object(self.client, self.bucket_name, self.object_name, "dummy.txt")
        self.client.upload_file.assert_called_once()
        self.assertEqual(result["status"], "Success")

    def test_put_object_with_invalid_type(self):
        result = s3_utils.put_object(self.client, self.bucket_name, self.object_name, 123)
        self.assertEqual(result["status"], "Failed")
        self.assertIn("must be either", result["error"])

    def test_put_object_with_client_error(self):
        self.client.upload_fileobj.side_effect = ClientError(
            {"Error": {"Code": "500", "Message": "Failed"}}, "upload"
        )
        buf = io.BytesIO(b"data")
        result = s3_utils.put_object(self.client, self.bucket_name, self.object_name, buf)
        self.assertEqual(result["status"], "Failed")

    # === get_object ===
    def test_get_object_success(self):
        body = io.BytesIO(b"content")
        self.client.get_object.return_value = {"Body": io.BytesIO(b"content")}
        buf = s3_utils.get_object(self.client, self.bucket_name, self.object_name)
        self.assertIsInstance(buf, io.BytesIO)
        self.assertEqual(buf.read(), b"content")

    def test_get_object_fail(self):
        self.client.get_object.side_effect = ClientError({"Error": {}}, "get_object")
        buf = s3_utils.get_object(self.client, self.bucket_name, self.object_name)
        self.assertIsNone(buf)

    # === copy_object_between_buckets ===
    @patch("fastapi_celery.utils.read_n_write_s3.aws_connection.S3Connector")
    def test_copy_object_between_buckets_success(self, mock_connector):
        mock_client = MagicMock()
        mock_connector.return_value.client = mock_client
        result = s3_utils.copy_object_between_buckets("src-bucket", "src-key", "dest-bucket", "dest-key")
        self.assertEqual(result["status"], "Success")
        mock_client.copy_object.assert_called_once()

    @patch("fastapi_celery.utils.read_n_write_s3.aws_connection.S3Connector")
    def test_copy_object_between_buckets_fail(self, mock_connector):
        mock_client = MagicMock()
        mock_client.copy_object.side_effect = ClientError({"Error": {}}, "copy_object")
        mock_connector.return_value.client = mock_client
        result = s3_utils.copy_object_between_buckets("src", "key", "dest", "key2")
        self.assertEqual(result["status"], "Failed")

    # === object_exists ===
    def test_object_exists_true(self):
        self.client.head_object.return_value = {"ContentLength": 100}
        exists, meta = s3_utils.object_exists(self.client, self.bucket_name, self.object_name)
        self.assertTrue(exists)
        self.assertEqual(meta["ContentLength"], 100)

    def test_object_exists_not_found(self):
        error = ClientError({"Error": {"Code": "404"}}, "head_object")
        self.client.head_object.side_effect = error
        exists, meta = s3_utils.object_exists(self.client, self.bucket_name, self.object_name)
        self.assertFalse(exists)
        self.assertIsNone(meta)

    # === any_json_in_s3_prefix ===
    @patch("fastapi_celery.utils.read_n_write_s3.aws_connection.S3Connector")
    def test_any_json_in_s3_prefix_found(self, mock_connector):
        paginator = MagicMock()
        paginator.paginate.return_value = [{"Contents": [{"Key": "data/file.json"}]}]
        mock_connector.return_value.client.get_paginator.return_value = paginator

        result = s3_utils.any_json_in_s3_prefix("bucket", "prefix")
        self.assertTrue(result)

    @patch("fastapi_celery.utils.read_n_write_s3.aws_connection.S3Connector")
    def test_any_json_in_s3_prefix_not_found(self, mock_connector):
        paginator = MagicMock()
        paginator.paginate.return_value = [{"Contents": [{"Key": "file.txt"}]}]
        mock_connector.return_value.client.get_paginator.return_value = paginator

        result = s3_utils.any_json_in_s3_prefix("bucket", "prefix")
        self.assertFalse(result)

    # === write_json_to_s3 ===
    @patch("fastapi_celery.utils.read_n_write_s3.put_object", return_value={"status": "Success"})
    @patch("fastapi_celery.utils.read_n_write_s3.aws_connection.S3Connector")
    def test_write_json_to_s3_success(self, mock_connector, mock_put):
        mock_connector.return_value.client = self.client
        mock_connector.return_value.bucket_name = self.bucket_name

        file_record = {"file_name": "test.json", "current_time": "20251017"}
        data = {"foo": "bar"}

        result = s3_utils.write_json_to_s3(data, file_record, self.bucket_name)
        self.assertEqual(result["status"], "Success")
        mock_put.assert_called_once()

    @patch("fastapi_celery.utils.read_n_write_s3.aws_connection.S3Connector")
    def test_write_json_to_s3_fail_on_upload(self, mock_connector):
        mock_connector.return_value.client = self.client
        mock_connector.return_value.bucket_name = self.bucket_name
        file_record = {"file_name": "x.json", "current_time": "1"}

        with patch("fastapi_celery.utils.read_n_write_s3.put_object", return_value={"status": "Failed", "error": "oops"}):
            result = s3_utils.write_json_to_s3({"a": 1}, file_record, self.bucket_name)
            self.assertEqual(result["status"], "Failed")

    # === read_json_from_s3 ===
    @patch("fastapi_celery.utils.read_n_write_s3.aws_connection.S3Connector")
    @patch("fastapi_celery.utils.read_n_write_s3.get_object")
    def test_read_json_from_s3_success(self, mock_get_object, mock_connector):
        buffer = io.BytesIO(json.dumps({"a": 1}).encode())
        mock_get_object.return_value = buffer
        mock_connector.return_value.client = self.client
        mock_connector.return_value.bucket_name = self.bucket_name

        result = s3_utils.read_json_from_s3(self.bucket_name, self.object_name)
        self.assertEqual(result, {"a": 1})

    @patch("fastapi_celery.utils.read_n_write_s3.aws_connection.S3Connector")
    @patch("fastapi_celery.utils.read_n_write_s3.get_object", return_value=None)
    def test_read_json_from_s3_none(self, mock_get_object, mock_connector):
        mock_connector.return_value.client = self.client
        mock_connector.return_value.bucket_name = self.bucket_name
        result = s3_utils.read_json_from_s3(self.bucket_name, self.object_name)
        self.assertIsNone(result)

    # === list_objects_with_prefix ===
    @patch("fastapi_celery.utils.read_n_write_s3.aws_connection.S3Connector")
    def test_list_objects_with_prefix_success(self, mock_connector):
        paginator = MagicMock()
        paginator.paginate.return_value = [{"Contents": [{"Key": "a.json"}, {"Key": "b.json"}]}]
        mock_connector.return_value.client.get_paginator.return_value = paginator

        result = s3_utils.list_objects_with_prefix("bucket", "prefix")
        self.assertEqual(result, ["a.json", "b.json"])

    @patch("fastapi_celery.utils.read_n_write_s3.aws_connection.S3Connector", side_effect=Exception("init fail"))
    def test_list_objects_with_prefix_fail(self, mock_connector):
        result = s3_utils.list_objects_with_prefix("bucket", "prefix")
        self.assertEqual(result, [])