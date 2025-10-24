from fastapi import FastAPI
from contextlib import asynccontextmanager
from typing import AsyncIterator

from routers import api_file_processor, api_healthcheck
import config_loader
from utils.middlewares.request_id import RequestIDMiddleware
from fastapi.middleware.cors import CORSMiddleware
from fastapi import Request, status
from fastapi.responses import JSONResponse

# Define the lifespan event handler


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """
    Lifespan event handler for FastAPI.

    This asynchronous context manager runs setup logic before the application starts
    and teardown logic after it shuts down.

    Args:
        app (FastAPI): The FastAPI application instance.

    Yields:
        None: Allows the application to run between startup and shutdown.
    """
    app.state.startup_triggered = True
    # # Startup logic
    # Application runs here, during this time the app is alive
    yield
    # Shutdown logic can go here if needed (after `yield`)


app = FastAPI(lifespan=lifespan, root_path="/fastapi")
app.add_middleware(RequestIDMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Restrict in production!
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"detail": str(exc)},
    )


# Include routers
app.include_router(api_healthcheck.router)
app.include_router(api_file_processor.router)

# Run the app with uvicorn (only when this script is executed directly)
if __name__ == "__main__":  # pragma: no cover
    import uvicorn
    uvicorn.run(
        app, host="0.0.0.0", port=int(config_loader.get_env_variable("APP_PORT", 8000))
    )
