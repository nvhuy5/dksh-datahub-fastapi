from datetime import datetime, timezone
from utils import ext_extraction


def extract_metadata(self) -> None:
    try:
        file_processor = ext_extraction.FileExtensionProcessor(self.tracking_model)
        self.file_record = {
            "file_path": file_processor.file_path,
            "file_path_parent": file_processor.file_path_parent,
            "source_type": file_processor.source_type,
            "object_buffer": file_processor.object_buffer,
            "file_size": file_processor.file_size,
            "file_name": file_processor.file_name,
            "file_name_wo_ext": file_processor.file_name_wo_ext,
            "file_extension": file_processor.file_extension,
            "document_type": file_processor.document_type,
            "raw_bucket_name": file_processor.raw_bucket_name,
            "target_bucket_name": file_processor.target_bucket_name,
            "proceed_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        }

    except Exception:
        raise
