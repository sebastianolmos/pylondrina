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


READ_TRIPS_ISSUES: dict[str, IssueSpec] = {
    # ------------------------------------------------------------------
    # PATH / LAYOUT
    # ------------------------------------------------------------------
    "READ.PATH.INVALID_ROOT": _err(
        "READ.PATH.INVALID_ROOT",
        "El path {path!r} no es válido o no se puede resolver como artefacto de trips.",
        details_keys=("path", "resolved_path", "reason", "action"),
        defaults={"action": "abort"},
        exception="export",
    ),
    "READ.LAYOUT.MISSING_DATA_FILE": _err(
        "READ.LAYOUT.MISSING_DATA_FILE",
        "El artefacto formal de trips no contiene el archivo de datos requerido: {expected_file!r}.",
        details_keys=("path", "resolved_path", "expected_file", "files_present_sample", "files_present_total", "action"),
        defaults={"expected_file": "trips.parquet", "action": "abort"},
        exception="export",
    ),
    "READ.LAYOUT.MISSING_SIDECAR": _err(
        "READ.LAYOUT.MISSING_SIDECAR",
        "El artefacto formal de trips no contiene el sidecar obligatorio {expected_file!r}.",
        details_keys=("path", "resolved_path", "expected_file", "files_present_sample", "files_present_total", "action"),
        defaults={"expected_file": "trips.metadata.json", "action": "abort"},
        exception="export",
    ),
    "READ.LAYOUT.LEGACY_SIDECAR_DETECTED": _err(
        "READ.LAYOUT.LEGACY_SIDECAR_DETECTED",
        "Se detectó un sidecar legacy {legacy_file!r}, pero falta el sidecar formal {expected_file!r}; el artefacto no es válido para read_trips v1.1.",
        details_keys=("path", "resolved_path", "legacy_file", "expected_file", "action"),
        defaults={"legacy_file": "metadata.json", "expected_file": "trips.metadata.json", "action": "abort"},
        exception="export",
    ),

    # ------------------------------------------------------------------
    # SIDECAR / STORAGE
    # ------------------------------------------------------------------
    "READ.JSON.LOAD_FAILED": _err(
        "READ.JSON.LOAD_FAILED",
        "No fue posible leer o parsear trips.metadata.json.",
        details_keys=("path", "resolved_path", "exception_type", "exception_message", "action"),
        defaults={"action": "abort"},
        exception="export",
    ),
    "READ.SIDECAR.INVALID_TOP_LEVEL": _err(
        "READ.SIDECAR.INVALID_TOP_LEVEL",
        "El sidecar trips.metadata.json no tiene la estructura top-level esperada para un artefacto formal de trips.",
        details_keys=("path", "resolved_path", "missing_keys", "invalid_keys", "reason", "action"),
        defaults={"action": "abort"},
        exception="export",
    ),
    "READ.STORAGE.UNSUPPORTED_FORMAT": _err(
        "READ.STORAGE.UNSUPPORTED_FORMAT",
        "El formato de almacenamiento {storage_format!r} indicado en el sidecar no está soportado por read_trips v1.1.",
        details_keys=("storage_format", "supported_formats", "action"),
        defaults={"supported_formats": ["parquet"], "action": "abort"},
        exception="export",
    ),

    # ------------------------------------------------------------------
    # SCHEMA / RECONSTRUCTION
    # ------------------------------------------------------------------
    "READ.SCHEMA.METADATA_INVALID_IGNORED": _warn(
        "READ.SCHEMA.METADATA_INVALID_IGNORED",
        "El bloque schema del sidecar no es interpretable y será ignorado porque options.schema provee un esquema usable.",
        details_keys=("schema_source", "reason", "action"),
        defaults={"schema_source": "options", "action": "ignored_metadata_schema"},
    ),
    "READ.SCHEMA.UNAVAILABLE": _err(
        "READ.SCHEMA.UNAVAILABLE",
        "No fue posible resolver un TripSchema usable para read_trips ni desde options.schema ni desde el sidecar.",
        details_keys=("schema_source", "reason", "action"),
        defaults={"action": "abort"},
        exception="export",
    ),
    "READ.SCHEMA.MISMATCH": _warn(
        "READ.SCHEMA.MISMATCH",
        "El schema provisto por options no coincide con el snapshot persistido en el sidecar; se usará options.schema según precedencia.",
        details_keys=("schema_source", "schema_mismatch", "version_options", "version_metadata", "required_diff", "fields_diff_sample", "fields_diff_total", "action"),
        defaults={"schema_source": "options", "schema_mismatch": True, "action": "use_options_schema"},
    ),
    "READ.SCHEMA_EFFECTIVE.DEFAULTED": _warn(
        "READ.SCHEMA_EFFECTIVE.DEFAULTED",
        "schema_effective no está disponible o no es interpretable; se reconstruirá un estado efectivo vacío/default.",
        details_keys=("reason", "strict", "action"),
        defaults={"action": "default_empty_schema_effective"},
    ),

    # ------------------------------------------------------------------
    # DATA TABLE
    # ------------------------------------------------------------------
    "READ.PARQUET.LOAD_FAILED": _err(
        "READ.PARQUET.LOAD_FAILED",
        "No fue posible leer trips.parquet para reconstruir el TripDataset.",
        details_keys=("path", "resolved_path", "storage_format", "exception_type", "exception_message", "action"),
        defaults={"storage_format": "parquet", "action": "abort"},
        exception="export",
    ),
    "READ.CORE.EMPTY_DATAFRAME": _info(
        "READ.CORE.EMPTY_DATAFRAME",
        "Se reconstruyó un TripDataset vacío desde el artefacto persistido.",
        details_keys=("n_rows", "n_columns", "path", "note"),
        defaults={"note": "empty_dataset_loaded"},
    ),

    # ------------------------------------------------------------------
    # METADATA / IDENTITY / POST-READ STATE
    # ------------------------------------------------------------------
    "READ.METADATA.DATASET_ID_REGENERATED": _warn(
        "READ.METADATA.DATASET_ID_REGENERATED",
        "El dataset_id persistido faltaba o era inválido; se regeneró un dataset_id efectivo para el dataset cargado.",
        details_keys=("dataset_id", "dataset_id_status", "previous_value", "reason", "action"),
        defaults={"dataset_id_status": "regenerated", "action": "regenerated"},
    ),
    "READ.METADATA.ARTIFACT_ID_SET_NONE": _warn(
        "READ.METADATA.ARTIFACT_ID_SET_NONE",
        "El artifact_id persistido faltaba o era inválido; se dejará artifact_id=None en el dataset cargado.",
        details_keys=("artifact_id", "artifact_id_status", "previous_value", "reason", "action"),
        defaults={"artifact_id_status": "missing_or_invalid", "action": "set_none"},
    ),
    "READ.METADATA.VALIDATED_FORCED_FALSE": _info(
        "READ.METADATA.VALIDATED_FORCED_FALSE",
        "Se forzó metadata['is_validated']=False tras la lectura formal del artefacto de trips.",
        details_keys=("previous_value", "new_value", "action"),
        defaults={"new_value": False, "action": "force_unvalidated"},
    ),
}
