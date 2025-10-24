from pydantic import BaseModel
from typing import Optional
from models.class_models import (
    DocumentType,
    MasterDataParsed,
    PODataParsed,
)


def parse_data(
    document_type: DocumentType,
    data: dict,
    custom_type: Optional[type[BaseModel]] = None,
) -> MasterDataParsed | PODataParsed:
    "Dumps a dictionary data to the model of Master Data or Order based on its document_type"
    if data is None:
        raise ValueError("Input data is None — cannot continue task execution.")
    if isinstance(data, BaseModel):
        data = data.model_dump()

    if custom_type:
        return custom_type(**data)
    elif document_type == DocumentType.ORDER:
        return PODataParsed(**data)
    elif document_type == DocumentType.MASTER_DATA:
        return MasterDataParsed(**data)
    else:
        raise ValueError(f"Unknown document type: {document_type}")
