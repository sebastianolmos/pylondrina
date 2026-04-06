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


READ_FLOWS_ISSUES: dict[str, IssueSpec] = {
    # ------------------------------------------------------------------
    # PATH / LAYOUT
    # ------------------------------------------------------------------
    "READ_FLOWS.PATH.INVALID_ROOT": _err(
        "READ_FLOWS.PATH.INVALID_ROOT",
        "La ruta {path!r} no existe, no es un directorio válido o no puede usarse como bundle de lectura formal.",
        details_keys=("path", "strict", "files_expected", "reason", "action"),
        defaults={"action": "abort"},
        exception="export",
    ),
    "READ_FLOWS.LAYOUT.MISSING_DATA_FILE": _err(
        "READ_FLOWS.LAYOUT.MISSING_DATA_FILE",
        "El bundle de flows no contiene el archivo obligatorio flows.parquet.",
        details_keys=("path", "strict", "files_expected", "reason", "action"),
        defaults={"action": "abort"},
        exception="export",
    ),
    "READ_FLOWS.LAYOUT.MISSING_SIDECAR": _err(
        "READ_FLOWS.LAYOUT.MISSING_SIDECAR",
        "El bundle de flows no contiene flows.metadata.json; la lectura formal no es recuperable sin sidecar.",
        details_keys=("path", "strict", "files_expected", "reason", "action"),
        defaults={"action": "abort"},
        exception="export",
    ),

    # ------------------------------------------------------------------
    # SIDECAR / STORAGE
    # ------------------------------------------------------------------
    "READ_FLOWS.IO.SIDECAR_READ_FAILED": _err(
        "READ_FLOWS.IO.SIDECAR_READ_FAILED",
        "No fue posible leer o parsear flows.metadata.json.",
        details_keys=("path", "strict", "reason", "action"),
        defaults={"action": "abort"},
        exception="export",
    ),
    "READ_FLOWS.SIDECAR.INVALID_TOP_LEVEL": _err(
        "READ_FLOWS.SIDECAR.INVALID_TOP_LEVEL",
        "El sidecar de flows no cumple la estructura top-level esperada para persistencia formal v1.1.",
        details_keys=("path", "strict", "reason", "action"),
        defaults={"action": "abort"},
        exception="export",
    ),
    "READ_FLOWS.STORAGE.UNSUPPORTED_FORMAT": _err(
        "READ_FLOWS.STORAGE.UNSUPPORTED_FORMAT",
        "El storage.format {storage_format!r} declarado en el sidecar no es soportado por read_flows en v1.1.",
        details_keys=("path", "strict", "storage_format", "reason", "action"),
        defaults={"action": "abort"},
        exception="export",
    ),

    # ------------------------------------------------------------------
    # RECOVERY OF SNAPSHOTS / IDS
    # ------------------------------------------------------------------
    "READ_FLOWS.METADATA.DATASET_ID_REGENERATED": _warn(
        "READ_FLOWS.METADATA.DATASET_ID_REGENERATED",
        "dataset_id faltaba o era inválido en el sidecar; se regeneró bajo strict=False.",
        details_keys=(
            "path",
            "strict",
            "reason",
            "recovered",
            "recovery_action",
            "dataset_id_status",
        ),
        defaults={
            "recovered": True,
            "recovery_action": "regenerate_dataset_id",
            "dataset_id_status": "regenerated",
        },
    ),
    "READ_FLOWS.METADATA.ARTIFACT_ID_SET_NONE": _warn(
        "READ_FLOWS.METADATA.ARTIFACT_ID_SET_NONE",
        "artifact_id faltaba o era inválido en el sidecar; se degradó a None bajo strict=False.",
        details_keys=(
            "path",
            "strict",
            "reason",
            "recovered",
            "recovery_action",
            "artifact_id_status",
        ),
        defaults={
            "recovered": True,
            "recovery_action": "set_artifact_id_none",
            "artifact_id_status": "set_none",
        },
    ),
    "READ_FLOWS.SIDECAR.AGGREGATION_SPEC_DEFAULTED": _warn(
        "READ_FLOWS.SIDECAR.AGGREGATION_SPEC_DEFAULTED",
        "aggregation_spec faltaba o era inválido en el sidecar; se usó {} bajo strict=False.",
        details_keys=("path", "strict", "reason", "recovered", "recovery_action"),
        defaults={"recovered": True, "recovery_action": "default_empty_aggregation_spec"},
    ),
    "READ_FLOWS.SIDECAR.PROVENANCE_DEFAULTED": _info(
        "READ_FLOWS.SIDECAR.PROVENANCE_DEFAULTED",
        "provenance faltaba o era inválido en el sidecar; se usó {} para reconstruir el FlowDataset.",
        details_keys=("path", "strict", "reason", "recovered", "recovery_action"),
        defaults={"recovered": True, "recovery_action": "default_empty_provenance"},
    ),
    "READ_FLOWS.SIDECAR.METADATA_DEFAULTED": _warn(
        "READ_FLOWS.SIDECAR.METADATA_DEFAULTED",
        "metadata faltaba o no era interpretable en el sidecar; se usó {} bajo strict=False.",
        details_keys=("path", "strict", "reason", "recovered", "recovery_action"),
        defaults={"recovered": True, "recovery_action": "default_empty_metadata"},
    ),

    # ------------------------------------------------------------------
    # TABLE READING
    # ------------------------------------------------------------------
    "READ_FLOWS.IO.FLOWS_READ_FAILED": _err(
        "READ_FLOWS.IO.FLOWS_READ_FAILED",
        "No fue posible leer flows.parquet desde el bundle persistido.",
        details_keys=("path", "strict", "files_read", "reason", "action"),
        defaults={"action": "abort"},
        exception="export",
    ),
    "READ_FLOWS.FLOW_TO_TRIPS.REQUESTED_BUT_MISSING": _warn(
        "READ_FLOWS.FLOW_TO_TRIPS.REQUESTED_BUT_MISSING",
        "Se solicitó cargar flow_to_trips, pero el archivo no existe; la lectura continuará sin auxiliar bajo strict=False.",
        details_keys=(
            "path",
            "strict",
            "read_flow_to_trips",
            "files_expected",
            "files_read",
            "reason",
            "recovered",
            "recovery_action",
        ),
        defaults={
            "recovered": True,
            "recovery_action": "omit_missing_flow_to_trips",
        },
    ),
    "READ_FLOWS.IO.FLOW_TO_TRIPS_READ_FAILED": _err(
        "READ_FLOWS.IO.FLOW_TO_TRIPS_READ_FAILED",
        "No fue posible leer flow_to_trips.parquet desde el bundle persistido.",
        details_keys=("path", "strict", "read_flow_to_trips", "files_read", "reason", "action"),
        defaults={"action": "abort"},
        exception="export",
    ),

    # ------------------------------------------------------------------
    # POST-READ METADATA / EVENT
    # ------------------------------------------------------------------
    "READ_FLOWS.METADATA.VALIDATED_FORCED_FALSE": _info(
        "READ_FLOWS.METADATA.VALIDATED_FORCED_FALSE",
        "El estado metadata['is_validated'] fue forzado a False al finalizar read_flows.",
        details_keys=("path", "strict", "reason", "recovered", "recovery_action"),
        defaults={"recovered": True, "recovery_action": "force_is_validated_false"},
    ),
    "READ_FLOWS.EVENT.APPEND_FAILED": _warn(
        "READ_FLOWS.EVENT.APPEND_FAILED",
        "No fue posible anexar el evento read_flows en metadata['events']; el FlowDataset se devolvió igualmente.",
        details_keys=("path", "strict", "keep_metadata", "reason", "recovered", "recovery_action"),
        defaults={
            "recovered": True,
            "recovery_action": "return_dataset_without_event_append",
        },
    ),
}
