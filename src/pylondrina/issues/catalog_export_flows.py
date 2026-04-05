from __future__ import annotations

from pylondrina.issues.core import IssueSpec


"""
Catálogo de issues para OP-09: export_flows.

Notas normativas v1.1
---------------------
- OP-09 usa `OperationReport` genérico y `FlowExportResult`; los errores fatales
  se modelan como `ExportError`.
- El preflight de export debe trabajar sobre el contrato interno vigente de flows:
  `origin_h3_index`, `destination_h3_index`, `flow_value`.
- El layout externo flowmap se construye recién después del preflight:
  `origin`, `dest`, `count`.
- `extra_flow_fields` solo salen si el usuario los pide explícitamente.
- Si la exportación se materializó y luego falla el append del evento en memoria,
  no se revierte el directorio exportado; se agrega warning al report.
"""


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


_COMMON_EXPORT_DETAILS = (
    "output_root",
    "export_dir",
    "format",
    "mode",
    "folder_name_input",
    "folder_name_effective",
    "artifact",
    "reason",
    "exception_type",
    "recovered",
    "recovery_action",
)

_COMMON_PREFLIGHT_DETAILS = (
    "format",
    "reason",
    "action",
    "missing_fields",
    "invalid_fields",
    "reserved_fields",
    "n_rows",
    "field",
)

EXPORT_FLOWS_ISSUES: dict[str, IssueSpec] = {
    # ------------------------------------------------------------------
    # CORE / INPUT
    # ------------------------------------------------------------------
    "EXPORT_FLOWS.CORE.INVALID_FLOWDATASET": _err(
        "EXPORT_FLOWS.CORE.INVALID_FLOWDATASET",
        "El objeto recibido no es interpretable como FlowDataset para export_flows.",
        details_keys=("received_type", "expected_type", "action"),
        defaults={"expected_type": "FlowDataset", "action": "abort"},
    ),
    "EXPORT_FLOWS.CORE.INVALID_DATA_SURFACE": _err(
        "EXPORT_FLOWS.CORE.INVALID_DATA_SURFACE",
        "FlowDataset.flows no está disponible o no es tabular; no es posible exportar flujos.",
        details_keys=("attribute", "data_type", "reason", "action"),
        defaults={"attribute": "flows", "action": "abort"},
    ),

    # ------------------------------------------------------------------
    # OPTIONS / REQUEST
    # ------------------------------------------------------------------
    "EXPORT_FLOWS.OPTIONS.UNSUPPORTED_FORMAT": _err(
        "EXPORT_FLOWS.OPTIONS.UNSUPPORTED_FORMAT",
        "El formato de exportación {format!r} no está soportado en v1.1 para export_flows.",
        details_keys=("format", "supported_formats", "reason", "action"),
        defaults={"supported_formats": ["flowmap_blue"], "reason": "unsupported_format", "action": "abort"},
    ),
    "EXPORT_FLOWS.OPTIONS.UNSUPPORTED_WRITE_MODE": _err(
        "EXPORT_FLOWS.OPTIONS.UNSUPPORTED_WRITE_MODE",
        "La política de escritura {mode!r} no está soportada para export_flows.",
        details_keys=("mode", "supported_modes", "reason", "action"),
        defaults={"supported_modes": ["error_if_exists", "overwrite"], "reason": "unsupported_write_mode", "action": "abort"},
    ),
    "EXPORT_FLOWS.PATH.INVALID_OUTPUT_ROOT": _err(
        "EXPORT_FLOWS.PATH.INVALID_OUTPUT_ROOT",
        "El output_root {output_root!r} no es válido o no es operable para export_flows.",
        details_keys=("output_root", "resolved_path", "reason", "action"),
        defaults={"reason": "invalid_output_root", "action": "abort"},
    ),
    "EXPORT_FLOWS.OPTIONS.INVALID_FOLDER_NAME": _warn(
        "EXPORT_FLOWS.OPTIONS.INVALID_FOLDER_NAME",
        "folder_name={folder_name_input!r} no es operable; se generará un nombre alternativo.",
        details_keys=_COMMON_EXPORT_DETAILS,
        defaults={
            "artifact": "export_dir",
            "reason": "invalid_folder_name",
            "recovered": True,
            "recovery_action": "generated_folder_name",
        },
    ),
    "EXPORT_FLOWS.OPTIONS.FOLDER_NAME_GENERATED": _info(
        "EXPORT_FLOWS.OPTIONS.FOLDER_NAME_GENERATED",
        "Se generó folder_name efectivo para el export: {folder_name_effective!r}.",
        details_keys=("output_root", "folder_name_effective", "generator"),
    ),

    # ------------------------------------------------------------------
    # LAYOUT / COLISIONES
    # ------------------------------------------------------------------
    "EXPORT_FLOWS.LAYOUT.EXPORT_DIR_EXISTS_ABORT": _err(
        "EXPORT_FLOWS.LAYOUT.EXPORT_DIR_EXISTS_ABORT",
        "El directorio de exportación {export_dir!r} ya existe y mode='error_if_exists' impide continuar.",
        details_keys=_COMMON_EXPORT_DETAILS,
        defaults={"artifact": "export_dir", "reason": "export_dir_exists", "action": "abort"},
    ),
    "EXPORT_FLOWS.LAYOUT.EXPORT_DIR_EXISTS_OVERWRITE": _warn(
        "EXPORT_FLOWS.LAYOUT.EXPORT_DIR_EXISTS_OVERWRITE",
        "El directorio de exportación {export_dir!r} ya existe; se sobrescribirán los artefactos estándar.",
        details_keys=_COMMON_EXPORT_DETAILS,
        defaults={
            "artifact": "export_dir",
            "reason": "export_dir_exists",
            "recovered": True,
            "recovery_action": "overwrite_existing",
        },
    ),

    # ------------------------------------------------------------------
    # PREFLIGHT DE DATOS
    # ------------------------------------------------------------------
    "EXPORT_FLOWS.DATA.REQUIRED_FIELDS_MISSING": _err(
        "EXPORT_FLOWS.DATA.REQUIRED_FIELDS_MISSING",
        "Faltan campos mínimos en FlowDataset.flows para exportar en {format!r}: {missing_fields}.",
        details_keys=_COMMON_PREFLIGHT_DETAILS,
        defaults={"reason": "required_fields_missing", "action": "abort"},
    ),
    "EXPORT_FLOWS.DATA.NULL_ORIGIN_DESTINATION": _err(
        "EXPORT_FLOWS.DATA.NULL_ORIGIN_DESTINATION",
        "Existen filas con origin_h3_index o destination_h3_index nulos; el export no es interpretable.",
        details_keys=(
            "format",
            "reason",
            "action",
            "n_rows",
            "n_violations",
            "row_indices_sample",
            "field",
        ),
        defaults={"reason": "null_origin_destination", "action": "abort"},
    ),
    "EXPORT_FLOWS.DATA.INVALID_FLOW_VALUE": _err(
        "EXPORT_FLOWS.DATA.INVALID_FLOW_VALUE",
        "flow_value contiene valores nulos o no numéricos; no puede mapearse a count en el layout externo.",
        details_keys=(
            "format",
            "reason",
            "action",
            "field",
            "n_rows",
            "n_violations",
            "row_indices_sample",
            "values_sample",
        ),
        defaults={"field": "flow_value", "reason": "invalid_flow_value", "action": "abort"},
    ),
    "EXPORT_FLOWS.DATA.EMPTY_FLOWS": _warn(
        "EXPORT_FLOWS.DATA.EMPTY_FLOWS",
        "FlowDataset.flows contiene 0 filas; se exportarán artefactos vacíos con headers.",
        details_keys=("format", "n_rows", "reason"),
        defaults={"reason": "empty_flows"},
    ),

    # ------------------------------------------------------------------
    # EXTRAS DE EXPORT
    # ------------------------------------------------------------------
    "EXPORT_FLOWS.EXTRA.INVALID_FIELDS": _err(
        "EXPORT_FLOWS.EXTRA.INVALID_FIELDS",
        "extra_flow_fields contiene campos inexistentes en FlowDataset.flows: {invalid_fields}.",
        details_keys=("format", "invalid_fields", "reason", "action"),
        defaults={"reason": "invalid_extra_fields", "action": "abort"},
    ),
    "EXPORT_FLOWS.EXTRA.RESERVED_FIELDS": _err(
        "EXPORT_FLOWS.EXTRA.RESERVED_FIELDS",
        "extra_flow_fields contiene columnas reservadas del layout externo: {reserved_fields}.",
        details_keys=("format", "reserved_fields", "reason", "action"),
        defaults={"reason": "reserved_extra_fields", "action": "abort"},
    ),
    "EXPORT_FLOWS.EXTRA.NON_SERIALIZABLE_FIELDS": _err(
        "EXPORT_FLOWS.EXTRA.NON_SERIALIZABLE_FIELDS",
        "extra_flow_fields contiene columnas no serializables de forma segura a CSV: {invalid_fields}.",
        details_keys=("format", "invalid_fields", "reason", "action"),
        defaults={"reason": "non_serializable_extra_fields", "action": "abort"},
    ),

    # ------------------------------------------------------------------
    # TRANSFORMACIÓN H3 -> LOCATIONS
    # ------------------------------------------------------------------
    "EXPORT_FLOWS.H3.CENTROID_CONVERSION_FAILED": _err(
        "EXPORT_FLOWS.H3.CENTROID_CONVERSION_FAILED",
        "No fue posible convertir uno o más índices H3 a centroides para construir locations.csv.",
        details_keys=(
            "artifact",
            "reason",
            "action",
            "n_violations",
            "values_sample",
            "exception_type",
        ),
        defaults={"artifact": "locations.csv", "reason": "centroid_conversion_failed", "action": "abort"},
    ),

    # ------------------------------------------------------------------
    # SERIALIZACIÓN / SIDECAR
    # ------------------------------------------------------------------
    "EXPORT_FLOWS.SERIALIZATION.METADATA_NOT_SERIALIZABLE": _err(
        "EXPORT_FLOWS.SERIALIZATION.METADATA_NOT_SERIALIZABLE",
        "No fue posible serializar metadata.json; existe contenido no serializable en el sidecar de exportación.",
        details_keys=(
            "artifact",
            "reason",
            "non_serializable_key",
            "exception_type",
            "action",
        ),
        defaults={"artifact": "metadata.json", "reason": "metadata_not_serializable", "action": "abort"},
    ),

    # ------------------------------------------------------------------
    # IO / MATERIALIZACIÓN
    # ------------------------------------------------------------------
    "EXPORT_FLOWS.IO.CREATE_DIR_FAILED": _err(
        "EXPORT_FLOWS.IO.CREATE_DIR_FAILED",
        "No fue posible crear o preparar el directorio de exportación {export_dir!r}.",
        details_keys=_COMMON_EXPORT_DETAILS,
        defaults={"artifact": "export_dir", "reason": "create_dir_failed", "action": "abort"},
    ),
    "EXPORT_FLOWS.IO.WRITE_FLOWS_FAILED": _err(
        "EXPORT_FLOWS.IO.WRITE_FLOWS_FAILED",
        "Falló la escritura de {artifact!r} en {export_dir!r}.",
        details_keys=_COMMON_EXPORT_DETAILS,
        defaults={"artifact": "flows.csv", "reason": "write_failed", "action": "abort"},
    ),
    "EXPORT_FLOWS.IO.WRITE_LOCATIONS_FAILED": _err(
        "EXPORT_FLOWS.IO.WRITE_LOCATIONS_FAILED",
        "Falló la escritura de {artifact!r} en {export_dir!r}.",
        details_keys=_COMMON_EXPORT_DETAILS,
        defaults={"artifact": "locations.csv", "reason": "write_failed", "action": "abort"},
    ),
    "EXPORT_FLOWS.IO.WRITE_METADATA_FAILED": _err(
        "EXPORT_FLOWS.IO.WRITE_METADATA_FAILED",
        "Falló la escritura de {artifact!r} en {export_dir!r}.",
        details_keys=_COMMON_EXPORT_DETAILS,
        defaults={"artifact": "metadata.json", "reason": "write_failed", "action": "abort"},
    ),
    "EXPORT_FLOWS.IO.PERMISSION_DENIED": _err(
        "EXPORT_FLOWS.IO.PERMISSION_DENIED",
        "Permiso denegado al escribir en {export_dir!r}.",
        details_keys=("output_root", "export_dir", "artifact", "reason", "exception_type", "action"),
        defaults={"reason": "permission_denied", "action": "abort"},
    ),
    "EXPORT_FLOWS.IO.PARTIAL_WRITE_CLEANUP_FAILED": _warn(
        "EXPORT_FLOWS.IO.PARTIAL_WRITE_CLEANUP_FAILED",
        "La exportación falló y no fue posible limpiar completamente los artefactos parciales en {export_dir!r}.",
        details_keys=("export_dir", "reason", "exception_type", "recovered", "recovery_action"),
        defaults={"reason": "cleanup_failed", "recovered": False, "recovery_action": "manual_cleanup_required"},
    ),

    # ------------------------------------------------------------------
    # EVENTO POST-EXPORT
    # ------------------------------------------------------------------
    "EXPORT_FLOWS.EVENT.APPEND_FAILED": _warn(
        "EXPORT_FLOWS.EVENT.APPEND_FAILED",
        "No fue posible registrar el evento de exportación en FlowDataset.metadata['events']; los artefactos ya materializados no se revertirán.",
        details_keys=("reason", "exception_type", "recovered", "recovery_action"),
        defaults={"reason": "event_append_failed", "recovered": True, "recovery_action": "keep_materialized_export"},
    ),
}