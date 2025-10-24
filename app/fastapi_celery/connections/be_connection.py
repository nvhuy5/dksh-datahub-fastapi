import httpx
from typing import Optional, Dict, Any
from models.tracking_models import ServiceLog, LogType
from utils import log_helpers
import config_loader


# === Set up logging ===
logger = log_helpers.get_logger("Backend API Connection")

API_KEY = config_loader.get_env_variable("JWT_SECRET_KEY")
JWT_TOKEN_KEY = "jwt_token"
jwt_request = {"type": "AUTHENTICATE_DATA_WORKFLOW_CODE"}


class BEConnector:
    """Backend API Connector for making HTTP requests.

    Initializes an HTTP client with a URL and optional body data, and provides
    methods for sending POST, GET, and PUT requests. Logs errors during requests.
    """

    def __init__(
        self,
        api_url: str,
        body_data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ):
        """Initialize the connector with an API URL and optional body data.

        Args:
            api_url (str): The URL of the API endpoint.
            body_data (Optional[Dict[str, Any]], optional): Data to send in the request body. Defaults to None.

        """
        self.api_url = api_url
        self.body_data = body_data or {}
        self.params = params or {}
        self.metadata = {}

    async def post(self) -> Optional[Dict[str, Any]]:
        """Send a POST request to the API endpoint.

        Returns:
            Optional[Dict[str, Any]]: Response data under the 'data' key, or None if request fails.
        """
        return await self._request("POST")

    async def get(self) -> Optional[Dict[str, Any]]:
        """Send a GET request to the API endpoint.

        Returns:
            Optional[Dict[str, Any]]: Response data under the 'data' key, or None if request fails.
        """
        return await self._request("GET")

    async def put(self) -> Optional[Dict[str, Any]]:
        """Send a PUT request to the API endpoint.

        Returns:
            Optional[Dict[str, Any]]: Response data under the 'data' key, or None if request fails.
        """
        return await self._request("PUT")

    async def _request(self, method: str) -> Optional[Dict[str, Any]]:
        """Send an HTTP request to the API endpoint using the specified method.

        Args:
            method (str): HTTP method to use ('POST', 'GET', or 'PUT').

        Returns:
            Optional[Dict[str, Any]]: Response data under the 'data' key, or None if request fails.
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
                        "service": ServiceLog.DATABASE,
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
                        "service": ServiceLog.DATABASE,
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

    def get_field(self, key: str) -> Optional[Any]:
        """
        Get a specific field from the metadata dictionary.

        Args:
            key (str): The key of the metadata field to retrieve.

        Returns:
            Optional[Any]: The value associated with the key if present, else None.
        """
        return self.metadata.get(key)

    def __repr__(self) -> str:
        """Return a string representation of the connector.

        Returns:
            str: String representation with metadata keys.
        """
        return f"<POTemplateMetadata keys={list(self.metadata.keys())}>"
