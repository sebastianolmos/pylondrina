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


WRITE_FLOWS_ISSUES: dict[str, IssueSpec] = {
    # ------------------------------------------------------------------
    # INPUT / OPTIONS
    # ------------------------------------------------------------------
    "WRITE_FLOWS.INPUT.INVALID_DATASET": _err(
        "WRITE_FLOWS.INPUT.INVALID_DATASET",
        "La entrada entregada a write_flows no es interpretable como un FlowDataset válido.",
        details_keys=("path", "reason", "action"),
        defaults={"action": "abort"},
        exception="export",
    ),
    "WRITE_FLOWS.INPUT.MISSING_FLOWS_TABLE": _err(
        "WRITE_FLOWS.INPUT.MISSING_FLOWS_TABLE",
        "El FlowDataset no contiene una tabla principal 'flows' utilizable para persistencia formal.",
        details_keys=("path", "reason", "action"),
        defaults={"action": "abort"},
        exception="export",
    ),
    "WRITE_FLOWS.OPTIONS.INVALID_MODE": _err(
        "WRITE_FLOWS.OPTIONS.INVALID_MODE",
        "El modo de escritura {mode!r} no es soportado por write_flows.",
        details_keys=("path", "mode", "reason", "action"),
        defaults={"action": "abort"},
        exception="export",
    ),
    "WRITE_FLOWS.OPTIONS.UNSUPPORTED_STORAGE_FORMAT": _err(
        "WRITE_FLOWS.OPTIONS.UNSUPPORTED_STORAGE_FORMAT",
        "El storage_format {storage_format!r} no es soportado por write_flows en v1.1.",
        details_keys=("path", "storage_format", "reason", "action"),
        defaults={"action": "abort"},
        exception="export",
    ),

    # ------------------------------------------------------------------
    # PATH / LAYOUT
    # ------------------------------------------------------------------
    "WRITE_FLOWS.PATH.INVALID_TARGET": _err(
        "WRITE_FLOWS.PATH.INVALID_TARGET",
        "La ruta destino {path!r} no es operable para escribir el bundle de flows.",
        details_keys=("path", "mode", "normalize_artifact_dir", "reason", "action"),
        defaults={"action": "abort"},
        exception="export",
    ),
    "WRITE_FLOWS.LAYOUT.BUNDLE_EXISTS": _err(
        "WRITE_FLOWS.LAYOUT.BUNDLE_EXISTS",
        "El bundle destino ya existe y mode='error_if_exists'; write_flows abortado.",
        details_keys=("path", "mode", "artifact", "reason", "action"),
        defaults={"reason": "bundle_already_exists", "action": "abort"},
        exception="export",
    ),
    "WRITE_FLOWS.LAYOUT.BUNDLE_OVERWRITTEN": _info(
        "WRITE_FLOWS.LAYOUT.BUNDLE_OVERWRITTEN",
        "El bundle destino ya existía y será sobrescrito porque mode='overwrite'.",
        details_keys=("path", "mode", "artifact", "recovered", "recovery_action"),
        defaults={"recovered": True, "recovery_action": "overwrite_existing_bundle"},
    ),

    # ------------------------------------------------------------------
    # SNAPSHOT / METADATA
    # ------------------------------------------------------------------
    "WRITE_FLOWS.METADATA.DATASET_ID_CREATED": _info(
        "WRITE_FLOWS.METADATA.DATASET_ID_CREATED",
        "Se generó dataset_id para el FlowDataset antes de persistirlo: {dataset_id!r}.",
        details_keys=("path", "dataset_id", "recovered", "recovery_action"),
        defaults={"recovered": True, "recovery_action": "create_dataset_id"},
    ),
    "WRITE_FLOWS.METADATA.DATASET_ID_REGENERATED": _warn(
        "WRITE_FLOWS.METADATA.DATASET_ID_REGENERATED",
        "El dataset_id existente era inválido o no interpretable; se regeneró antes de persistir el bundle.",
        details_keys=("path", "dataset_id", "reason", "recovered", "recovery_action"),
        defaults={"recovered": True, "recovery_action": "regenerate_dataset_id"},
    ),
    "WRITE_FLOWS.SNAPSHOT.AGGREGATION_SPEC_INVALID": _err(
        "WRITE_FLOWS.SNAPSHOT.AGGREGATION_SPEC_INVALID",
        "aggregation_spec no es utilizable o no es serializable; no se puede construir el sidecar formal de flows.",
        details_keys=("path", "reason", "action"),
        defaults={"action": "abort"},
        exception="export",
    ),
    "WRITE_FLOWS.SNAPSHOT.NOT_JSON_SERIALIZABLE": _err(
        "WRITE_FLOWS.SNAPSHOT.NOT_JSON_SERIALIZABLE",
        "Uno de los bloques a persistir en el sidecar no es serializable a JSON.",
        details_keys=("path", "artifact", "reason", "action"),
        defaults={"action": "abort"},
        exception="export",
    ),
    "WRITE_FLOWS.SNAPSHOT.SIDECAR_INCONSISTENT": _err(
        "WRITE_FLOWS.SNAPSHOT.SIDECAR_INCONSISTENT",
        "El sidecar construido para write_flows es inconsistente con el layout o con los snapshots persistidos.",
        details_keys=("path", "artifact", "reason", "action"),
        defaults={"action": "abort"},
        exception="export",
    ),

    # ------------------------------------------------------------------
    # OPTIONAL AUXILIARY
    # ------------------------------------------------------------------
    "WRITE_FLOWS.FLOW_TO_TRIPS.REQUESTED_BUT_MISSING": _warn(
        "WRITE_FLOWS.FLOW_TO_TRIPS.REQUESTED_BUT_MISSING",
        "Se solicitó persistir flow_to_trips, pero el FlowDataset no contiene esa tabla; el bundle se escribirá sin auxiliar.",
        details_keys=(
            "path",
            "write_flow_to_trips",
            "artifact",
            "reason",
            "recovered",
            "recovery_action",
        ),
        defaults={
            "recovered": True,
            "recovery_action": "omit_missing_flow_to_trips",
        },
    ),

    # ------------------------------------------------------------------
    # IO / STAGING / COMMIT
    # ------------------------------------------------------------------
    "WRITE_FLOWS.IO.STAGING_CREATE_FAILED": _err(
        "WRITE_FLOWS.IO.STAGING_CREATE_FAILED",
        "No fue posible crear el staging_dir para write_flows.",
        details_keys=("path", "artifact", "reason", "action"),
        defaults={"action": "abort"},
        exception="export",
    ),
    "WRITE_FLOWS.IO.FLOWS_WRITE_FAILED": _err(
        "WRITE_FLOWS.IO.FLOWS_WRITE_FAILED",
        "Falló la escritura de la tabla principal de flows ({artifact}) durante write_flows.",
        details_keys=(
            "path",
            "artifact",
            "storage_format",
            "parquet_compression",
            "feather_compression",
            "reason",
            "action",
        ),
        defaults={"action": "abort"},
        exception="export",
    ),
    "WRITE_FLOWS.IO.FLOW_TO_TRIPS_WRITE_FAILED": _err(
        "WRITE_FLOWS.IO.FLOW_TO_TRIPS_WRITE_FAILED",
        "Falló la escritura de la tabla auxiliar flow_to_trips ({artifact}) durante write_flows.",
        details_keys=(
            "path",
            "artifact",
            "storage_format",
            "parquet_compression",
            "feather_compression",
            "reason",
            "action",
        ),
        defaults={"action": "abort"},
        exception="export",
    ),
    "WRITE_FLOWS.IO.SIDECAR_WRITE_FAILED": _err(
        "WRITE_FLOWS.IO.SIDECAR_WRITE_FAILED",
        "Falló la escritura de flows.metadata.json durante write_flows.",
        details_keys=("path", "artifact", "reason", "action"),
        defaults={"action": "abort"},
        exception="export",
    ),
    "WRITE_FLOWS.IO.STAGING_INCOMPLETE": _err(
        "WRITE_FLOWS.IO.STAGING_INCOMPLETE",
        "El staging del bundle quedó incompleto; write_flows no puede hacer commit final.",
        details_keys=("path", "artifact", "reason", "action"),
        defaults={"action": "abort"},
        exception="export",
    ),
    "WRITE_FLOWS.IO.COMMIT_FAILED": _err(
        "WRITE_FLOWS.IO.COMMIT_FAILED",
        "Falló la promoción del staging al destino final del bundle .golondrina.",
        details_keys=("path", "artifact", "mode", "reason", "action"),
        defaults={"action": "abort"},
        exception="export",
    ),
    "WRITE_FLOWS.IO.CLEANUP_FAILED": _warn(
        "WRITE_FLOWS.IO.CLEANUP_FAILED",
        "Falló el cleanup best-effort posterior a un error de write_flows; pueden quedar artefactos temporales residuales.",
        details_keys=("path", "artifact", "reason", "recovered", "recovery_action"),
        defaults={"recovered": False, "recovery_action": "manual_cleanup_may_be_required"},
    ),
}
