from fastapi.testclient import TestClient
from fastapi import FastAPI
from fastapi_celery.routers.api_healthcheck import router as healthcheck_router

# Create a test FastAPI app and include the healthcheck router
app = FastAPI()
app.include_router(healthcheck_router)

client = TestClient(app)


def test_api_health() -> None:
    """Test successful health check."""
    response = client.get("/api_health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_api_health_error_handling(monkeypatch) -> None:
    """Test error handling when internal health check raises Exception."""

    # Mock internal health check to raise Exception
    def mock_health_check_error():
        raise Exception("Simulated error")

    monkeypatch.setattr(
        "fastapi_celery.routers.api_healthcheck._internal_health_check",
        mock_health_check_error
    )

    response = client.get("/api_health")

    assert response.status_code == 503
    assert response.json()["status"] == "error"
    assert "Simulated error" in response.json()["details"]
