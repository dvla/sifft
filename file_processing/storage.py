from typing import Any

import fsspec


def detect_protocol(path: str) -> str:
    if path.startswith(("s3://", "az://", "gs://", "hdfs://")):
        return path.split("://")[0]
    return "file"


def get_filesystem(path: str, storage_options: dict[str, Any] | None = None):
    protocol = detect_protocol(path)
    return fsspec.filesystem(protocol, **(storage_options or {}))
