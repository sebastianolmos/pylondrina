from __future__ import annotations

from pylondrina.issues.core import IssueSpec


def _info(code: str, message: str, *, details_keys=(), defaults=None) -> IssueSpec:
    return IssueSpec(
        code=code,
        level="info",
        message_template=message,
        details_keys=tuple(details_keys),
        defaults=defaults or {},
        fatal=False,
        exception=None,
    )


def _warn(code: str, message: str, *, details_keys=(), defaults=None) -> IssueSpec:
    return IssueSpec(
        code=code,
        level="warning",
        message_template=message,
        details_keys=tuple(details_keys),
        defaults=defaults or {},
        fatal=False,
        exception=None,
    )


def _err(
    code: str,
    message: str,
    *,
    details_keys=(),
    defaults=None,
    exception: str = "export",
    fatal: bool = True,
) -> IssueSpec:
    return IssueSpec(
        code=code,
        level="error",
        message_template=message,
        details_keys=tuple(details_keys),
        defaults=defaults or {},
        fatal=fatal,
        exception=exception,
    )


WRITE_TRIPS_ISSUES: dict[str, IssueSpec] = {
    # ------------------------------------------------------------------
    # CORE / INPUT
    # ------------------------------------------------------------------
    "WRT.CORE.INVALID_TRIPDATASET": _err(
        "WRT.CORE.INVALID_TRIPDATASET",
        "El objeto recibido no es interpretable como TripDataset para write_trips.",
        details_keys=("received_type", "reason", "action"),
        defaults={"action": "abort"},
        exception="export",
    ),
    "WRT.CORE.INVALID_DATA_SURFACE": _err(
        "WRT.CORE.INVALID_DATA_SURFACE",
        "TripDataset.data no está disponible o no es tabular; no es posible persistir trips.",
        details_keys=("data_type", "reason", "action"),
        defaults={"action": "abort"},
        exception="export",
    ),
    "WRT.CORE.EMPTY_DATAFRAME": _info(
        "WRT.CORE.EMPTY_DATAFRAME",
        "El TripDataset contiene 0 filas; se persistirá un artefacto formal vacío.",
        details_keys=("n_rows", "path", "note"),
        defaults={"note": "empty_dataset_written"},
    ),

    # ------------------------------------------------------------------
    # OPTIONS / PRECONDITIONS
    # ------------------------------------------------------------------
    "WRT.OPTIONS.INVALID_MODE": _err(
        "WRT.OPTIONS.INVALID_MODE",
        "El modo de escritura {mode!r} no es válido para write_trips.",
        details_keys=("mode", "expected", "action"),
        defaults={
            "expected": ["error_if_exists", "overwrite"],
            "action": "abort",
        },
        exception="export",
    ),
    "WRT.OPTIONS.UNSUPPORTED_STORAGE_FORMAT": _err(
        "WRT.OPTIONS.UNSUPPORTED_STORAGE_FORMAT",
        "El formato de persistencia {storage_format!r} no está soportado en v1.1 para write_trips.",
        details_keys=("storage_format", "supported_formats", "action"),
        defaults={"supported_formats": ["parquet"], "action": "abort"},
        exception="export",
    ),
    "WRT.OPTIONS.UNSUPPORTED_PARQUET_COMPRESSION": _err(
        "WRT.OPTIONS.UNSUPPORTED_PARQUET_COMPRESSION",
        "La compresión Parquet {compression!r} no está soportada para write_trips.",
        details_keys=("compression", "supported_compressions", "action"),
        defaults={
            "supported_compressions": ["snappy", "gzip", "zstd", "brotli", "none", None],
            "action": "abort",
        },
        exception="export",
    ),
    "WRT.VALIDATION.REQUIRED_NOT_VALIDATED": _err(
        "WRT.VALIDATION.REQUIRED_NOT_VALIDATED",
        "write_trips requiere un dataset validado, pero metadata['is_validated']={validated_flag!r}.",
        details_keys=("require_validated", "validated_flag", "path", "action"),
        defaults={"action": "abort"},
        exception="validation",
    ),

    # ------------------------------------------------------------------
    # PATH / DESTINATION
    # ------------------------------------------------------------------
    "WRT.PATH.INVALID_DESTINATION": _err(
        "WRT.PATH.INVALID_DESTINATION",
        "El path de destino {path!r} no es válido o no se puede resolver para write_trips.",
        details_keys=("path", "resolved_path", "reason", "action"),
        defaults={"action": "abort"},
        exception="export",
    ),
    "WRT.DEST.ALREADY_EXISTS": _err(
        "WRT.DEST.ALREADY_EXISTS",
        "El destino formal ya existe en {resolved_path!r} y mode='error_if_exists' no permite sobrescribirlo.",
        details_keys=("path", "resolved_path", "mode", "files_present_sample", "files_present_total", "action"),
        defaults={"action": "abort"},
        exception="export",
    ),

    # ------------------------------------------------------------------
    # METADATA / IDENTITY / SERIALIZATION
    # ------------------------------------------------------------------
    "WRT.METADATA.DATASET_ID_CREATED": _info(
        "WRT.METADATA.DATASET_ID_CREATED",
        "Se generó dataset_id para poder persistir el artefacto de trips: {dataset_id!r}.",
        details_keys=("dataset_id", "generator", "stored_in"),
        defaults={"stored_in": "metadata.dataset_id"},
    ),
    "WRT.METADATA.DATASET_ID_REGENERATED": _warn(
        "WRT.METADATA.DATASET_ID_REGENERATED",
        "El dataset_id existente era inválido o no interpretable; se regeneró como {dataset_id!r}.",
        details_keys=("dataset_id", "reason", "previous_value", "stored_in", "action"),
        defaults={"stored_in": "metadata.dataset_id", "action": "regenerated"},
    ),
    "WRT.JSON.NOT_SERIALIZABLE": _err(
        "WRT.JSON.NOT_SERIALIZABLE",
        "No es posible serializar {label!r} a JSON-safe para construir trips.metadata.json.",
        details_keys=("label", "reason", "offending_type", "example_repr", "action"),
        defaults={"action": "abort"},
        exception="export",
    ),

    # ------------------------------------------------------------------
    # STAGING / IO
    # ------------------------------------------------------------------
    "WRT.IO.STAGING_CREATE_FAILED": _err(
        "WRT.IO.STAGING_CREATE_FAILED",
        "No fue posible crear el staging temporal para materializar el artefacto de trips.",
        details_keys=("path", "resolved_path", "reason", "exception_type", "exception_message", "action"),
        defaults={"action": "abort"},
        exception="export",
    ),
    "WRT.PARQUET.WRITE_FAILED": _err(
        "WRT.PARQUET.WRITE_FAILED",
        "Falló la escritura de trips.parquet durante write_trips.",
        details_keys=("path", "resolved_path", "storage_format", "compression", "n_rows", "exception_type", "exception_message", "action"),
        defaults={"storage_format": "parquet", "action": "abort"},
        exception="export",
    ),
    "WRT.JSON.WRITE_FAILED": _err(
        "WRT.JSON.WRITE_FAILED",
        "Falló la escritura de trips.metadata.json durante write_trips.",
        details_keys=("path", "resolved_path", "dataset_id", "artifact_id", "exception_type", "exception_message", "action"),
        defaults={"action": "abort"},
        exception="export",
    ),
    "WRT.IO.STAGING_INCOMPLETE": _err(
        "WRT.IO.STAGING_INCOMPLETE",
        "La materialización en staging quedó incompleta; no están presentes todos los artefactos requeridos.",
        details_keys=("path", "resolved_path", "files_expected", "files_present_sample", "files_present_total", "files_written", "reason", "action"),
        defaults={"action": "abort"},
        exception="export",
    ),
    "WRT.IO.COMMIT_FAILED": _err(
        "WRT.IO.COMMIT_FAILED",
        "Falló el commit final del artefacto de trips desde staging hacia el destino definitivo.",
        details_keys=("path", "resolved_path", "mode", "files_written", "reason", "exception_type", "exception_message", "action"),
        defaults={"action": "abort"},
        exception="export",
    ),
    "WRT.IO.CLEANUP_FAILED": _warn(
        "WRT.IO.CLEANUP_FAILED",
        "La escritura finalizó, pero falló el cleanup best-effort del staging temporal.",
        details_keys=("path", "resolved_path", "files_written", "reason", "exception_type", "exception_message", "action"),
        defaults={"action": "staging_leftover"},
    ),
}
