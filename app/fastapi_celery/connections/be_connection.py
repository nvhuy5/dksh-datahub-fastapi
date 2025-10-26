import httpx
from typing import Any
from models.tracking_models import ServiceLog, LogType
from utils import log_helpers
import config_loader

# === Set up logging ===
logger = log_helpers.get_logger("Backend API Connection")

API_KEY = config_loader.get_env_variable("JWT_SECRET_KEY")
JWT_TOKEN_KEY = "jwt_token"
jwt_request = {"type": "AUTHENTICATE_DATA_WORKFLOW_CODE"}


class BEConnector:
    """
    Backend API Connector for making asynchronous HTTP requests using httpx.

    This class provides helper methods (POST, GET, PUT) to interact with
    backend APIs, including automatic logging and error handling.
    """

    def __init__(
        self,
        api_url: str,
        body_data: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
    ):
        """
        Initialize the connector with an API URL and optional body data.

        Args:
            api_url (str): The URL of the API endpoint.
            body_data (dict[str, Any] | None): Data to send in the request body. Defaults to {}.
            params (dict[str, Any] | None): Query parameters for the request. Defaults to {}.
        """
        self.api_url = api_url
        self.body_data = body_data or {}
        self.params = params or {}
        self.metadata: dict[str, Any] = {}

    async def post(self) -> dict[str, Any] | None:
        """
        Send a POST request to the API endpoint.

        Returns:
            dict[str, Any] | None: Response data under the 'data' key, or None if request fails.
        """
        return await self._request("POST")

    async def get(self) -> dict[str, Any] | None:
        """
        Send a GET request to the API endpoint.

        Returns:
            dict[str, Any] | None: Response data under the 'data' key, or None if request fails.
        """
        return await self._request("GET")

    async def put(self) -> dict[str, Any] | None:
        """
        Send a PUT request to the API endpoint.

        Returns:
            dict[str, Any] | None: Response data under the 'data' key, or None if request fails.
        """
        return await self._request("PUT")

    async def _request(self, method: str) -> dict[str, Any] | None:
        """
        Send an HTTP request to the API endpoint using the specified method.

        Args:
            method (str): HTTP method ('POST', 'GET', or 'PUT').

        Returns:
            dict[str, Any] | None: Parsed response data or None on failure.
        """
        async with httpx.AsyncClient() as client:
            try:
                headers = {"X-Token": API_KEY}
                response = await client.request(
                    method,
                    self.api_url,
                    headers=headers,
                    json=self.body_data,
                    params=self.params,
                )
                response.raise_for_status()
                response_data = response.json()
                return response_data.get("data", {})
            except httpx.HTTPStatusError as e:
                logger.error(
                    "API request failed with HTTPStatusError",
                    exc_info=True,
                    extra={
                        "service": ServiceLog.CALL_BE_API,
                        "log_type": LogType.ERROR,
                        "url": self.api_url,
                        "method": method,
                        "params": self.params,
                        "body": self.body_data,
                        "status_code": e.response.status_code if e.response else None,
                        "response_text": e.response.text if e.response else None,
                        "error_type": type(e).__name__,
                        "error_message": str(e),
                    },
                )
            except Exception as e:
                logger.error(
                    "API request raised unexpected exception",
                    exc_info=True,
                    extra={
                        "service": ServiceLog.CALL_BE_API,
                        "log_type": LogType.ERROR,
                        "url": self.api_url,
                        "method": method,
                        "params": self.params,
                        "body": self.body_data,
                        "error_type": type(e).__name__,
                        "error_message": str(e) or "No message",
                    },
                )
        return None

    def get_field(self, key: str) -> Any | None:
        """
        Retrieve a specific field from the metadata dictionary.

        Args:
            key (str): The metadata key to look up.

        Returns:
            Any | None: The value associated with the key if present, else None.
        """
        return self.metadata.get(key)

    def __repr__(self) -> str:
        """
        Return a string representation of the connector.

        Returns:
            str: String representation with metadata keys.
        """
        return f"<POTemplateMetadata keys={list(self.metadata.keys())}>"
