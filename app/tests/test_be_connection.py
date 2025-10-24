# tests/test_be_connection.py
import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from fastapi_celery.connections.be_connection import BEConnector

# === Test BEConnector ===

@pytest.mark.asyncio
async def test_post_success():
    """Test BEConnector.post returns expected data."""
    mock_response_data = {"data": {"key": "value"}}

    # Mock httpx.AsyncClient.request
    mock_response = MagicMock()
    mock_response.json = MagicMock(return_value=mock_response_data)
    mock_response.raise_for_status = MagicMock()

    with patch("httpx.AsyncClient.request", return_value=mock_response):
        connector = BEConnector(api_url="https://fakeapi.com", body_data={"x": 1})
        result = await connector.post()
        assert result == {"key": "value"}
        mock_response.raise_for_status.assert_called_once()
        mock_response.json.assert_called_once()


@pytest.mark.asyncio
async def test_get_success():
    """Test BEConnector.get returns expected data."""
    mock_response_data = {"data": {"foo": "bar"}}
    mock_response = MagicMock()
    mock_response.json = MagicMock(return_value=mock_response_data)
    mock_response.raise_for_status = MagicMock()

    with patch("httpx.AsyncClient.request", return_value=mock_response):
        connector = BEConnector(api_url="https://fakeapi.com")
        result = await connector.get()
        assert result == {"foo": "bar"}
        mock_response.raise_for_status.assert_called_once()


@pytest.mark.asyncio
async def test_put_success():
    """Test BEConnector.put returns expected data."""
    mock_response_data = {"data": {"updated": True}}
    mock_response = MagicMock()
    mock_response.json = MagicMock(return_value=mock_response_data)
    mock_response.raise_for_status = MagicMock()

    with patch("httpx.AsyncClient.request", return_value=mock_response):
        connector = BEConnector(api_url="https://fakeapi.com")
        result = await connector.put()
        assert result == {"updated": True}
        mock_response.raise_for_status.assert_called_once()


def test_get_field_existing_and_missing():
    """Test BEConnector.get_field returns correct value."""
    connector = BEConnector(api_url="https://fakeapi.com")
    connector.metadata = {"foo": "bar"}
    # existing key
    assert connector.get_field("foo") == "bar"
    # missing key
    assert connector.get_field("missing") is None


def test_repr_contains_metadata_keys():
    """Test BEConnector.__repr__ returns string with metadata keys."""
    connector = BEConnector(api_url="https://fakeapi.com")
    connector.metadata = {"step1": "done", "step2": "pending"}
    repr_str = repr(connector)
    assert "step1" in repr_str and "step2" in repr_str
    assert repr_str.startswith("<POTemplateMetadata keys=")
