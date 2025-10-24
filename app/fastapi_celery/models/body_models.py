from typing import Dict, Any, Callable, Optional
from dataclasses import dataclass

@dataclass # NOSONAR
class WorkflowFilterBody:
    filePath: str # NOSONAR
    fileName: str # NOSONAR
    fileExtension: str # NOSONAR
    project: str # NOSONAR
    source: str # NOSONAR

@dataclass # NOSONAR
class WorkflowSessionStartBody:
    workflowId: str # NOSONAR
    celeryId: str # NOSONAR
    filePath: str # NOSONAR

@dataclass # NOSONAR
class WorkflowStepStartBody:
    sessionId: str # NOSONAR
    stepId: str # NOSONAR
    dataInput: str # NOSONAR

