import pytest
from httpx import AsyncClient
from asgi_lifespan import LifespanManager
from fastapi.testclient import TestClient
from fastapi_celery.main import app
from fastapi import FastAPI, Request
from fastapi_celery.utils.middlewares.request_id import RequestIDMiddleware
import uuid

client = TestClient(app)


def test_healthcheck_endpoint() -> None:
    """Test the healthcheck endpoint of the FastAPI application.

    Sends a GET request to the /fastapi/healthz endpoint and verifies
    that it returns a 200 status code with the expected response.

    Returns:
        None
    """
    response = client.get("/fastapi/api_health")
    assert response.status_code == 200
    # Adjust to actual response
    assert response.json() == {"status": "ok"}


@pytest.mark.asyncio
async def test_lifespan_startup_event() -> None:
    """Test the lifespan startup event of the FastAPI application.

    Verifies that the startup event is triggered and the healthcheck
    endpoint works as expected during the application lifespan.

    Returns:
        None
    """
    with TestClient(app) as client:
        # Verify startup logic executed
        assert app.state.startup_triggered is True

        # Check healthcheck endpoint
        response = client.get("/fastapi/api_health")
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}


@pytest.fixture
def app_with_request_id_middleware():
    app = FastAPI()
    app.add_middleware(RequestIDMiddleware)

    @app.get("/test")
    async def test_endpoint(request: Request):
        return {"request_id": request.state.request_id}

    return app


def test_request_id_middleware(app_with_request_id_middleware):
    client = TestClient(app_with_request_id_middleware)

    response = client.get("/test")
    # Check that X-Request-ID header exists
    assert response.status_code == 200
    assert "X-Request-ID" in response.headers

    # Check the request_id in header matches the one in the JSON response
    request_id_header = response.headers["X-Request-ID"]
    request_id_body = response.json().get("request_id")
    assert request_id_header == request_id_body

    # Validate the request_id is a valid UUID string
    uuid_obj = uuid.UUID(request_id_header)
    assert str(uuid_obj) == request_id_header
