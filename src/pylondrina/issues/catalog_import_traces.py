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
    exception: str = "import",
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


_IMPORT_SHAPE_DETAILS = (
    "rows_in",
    "rows_out",
    "columns_in",
    "columns_out",
)

IMPORT_TRACES_ISSUES: dict[str, IssueSpec] = {
    # ------------------------------------------------------------------
    # INPUT / OPTIONS
    # ------------------------------------------------------------------
    "IMP.INPUT.INVALID_DATAFRAME": _err(
        "IMP.INPUT.INVALID_DATAFRAME",
        "El objeto entregado a import_traces_from_dataframe no es un DataFrame utilizable.",
        details_keys=("received_type", "expected_type", "action"),
        defaults={"expected_type": "pd.DataFrame", "action": "abort"},
        exception="import",
        fatal=True,
    ),
    "IMP.INPUT.DUPLICATE_COLUMNS": _err(
        "IMP.INPUT.DUPLICATE_COLUMNS",
        "El DataFrame de entrada contiene columnas duplicadas; el import de trazas sería ambiguo.",
        details_keys=("duplicate_columns", "columns_in", "action"),
        defaults={"action": "abort"},
        exception="import",
        fatal=True,
    ),
    "IMP.INPUT.EMPTY_DATAFRAME": _warn(
        "IMP.INPUT.EMPTY_DATAFRAME",
        "El DataFrame de entrada no contiene filas; se importará un TraceDataset vacío.",
        details_keys=("rows_in", "columns_in", "note"),
        defaults={"note": "empty_input"},
    ),
    "IMP.OPTIONS.INVALID_SELECTED_FIELDS_SPEC": _err(
        "IMP.OPTIONS.INVALID_SELECTED_FIELDS_SPEC",
        "selected_fields debe ser None o una secuencia serializable de nombres de campo; se recibió {received_type!r}.",
        details_keys=("received_type", "selected_fields", "action"),
        defaults={"action": "abort"},
        exception="import",
        fatal=True,
    ),
    "IMP.OPTIONS.EMPTY_SELECTED_FIELDS": _info(
        "IMP.OPTIONS.EMPTY_SELECTED_FIELDS",
        "selected_fields está vacío; se conservará únicamente el núcleo canónico obligatorio de traces.",
        details_keys=("selected_fields", "effective_selected_fields", "fallback"),
        defaults={"fallback": "canonical_core_only"},
    ),
    "IMP.OPTIONS.SELECTED_FIELDS_UNKNOWN": _warn(
        "IMP.OPTIONS.SELECTED_FIELDS_UNKNOWN",
        "selected_fields contiene nombres que no existen tras el mapeo efectivo; se omitirán (n={n_unknown}).",
        details_keys=(
            "selected_fields",
            "unknown_fields",
            "n_unknown",
            "available_columns_sample",
            "available_columns_total",
            "action",
        ),
        defaults={"action": "omit_unknown_selected_fields"},
    ),
    "IMP.OPTIONS.EXTRA_FIELDS_DROPPED": _info(
        "IMP.OPTIONS.EXTRA_FIELDS_DROPPED",
        "Se descartaron columnas extra porque la política efectiva de selección/conservación no las admite (n={n_dropped}).",
        details_keys=(
            "keep_extra_fields",
            "selected_fields",
            "n_dropped",
            "dropped_columns_sample",
            "dropped_columns_total",
            "action",
        ),
        defaults={"action": "drop_extra_fields"},
    ),
    "IMP.OPTIONS.INVALID_SOURCE_TIMEZONE": _err(
        "IMP.OPTIONS.INVALID_SOURCE_TIMEZONE",
        "La source_timezone entregada en ImportTraceOptions no es válida o no se puede interpretar: {source_timezone!r}.",
        details_keys=("source_timezone", "expected", "action"),
        defaults={"expected": "IANA timezone string", "action": "abort"},
        exception="import",
        fatal=True,
    ),

    # ------------------------------------------------------------------
    # SCHEMA
    # ------------------------------------------------------------------
    "SCH.TRACE_SCHEMA.MISSING_SCHEMA": _err(
        "SCH.TRACE_SCHEMA.MISSING_SCHEMA",
        "No se entregó un TraceSchema utilizable; no es posible ejecutar import_traces_from_dataframe.",
        details_keys=("schema_present", "action"),
        defaults={"schema_present": False, "action": "abort"},
        exception="schema",
        fatal=True,
    ),
    "SCH.TRACE_SCHEMA.INVALID_VERSION": _err(
        "SCH.TRACE_SCHEMA.INVALID_VERSION",
        "La versión del TraceSchema no es válida o no se puede interpretar: {schema_version!r}.",
        details_keys=("schema_version", "expected", "action"),
        defaults={"expected": "non-empty string", "action": "abort"},
        exception="schema",
        fatal=True,
    ),
    "SCH.TRACE_SCHEMA.CATEGORICAL_NOT_ALLOWED": _err(
        "SCH.TRACE_SCHEMA.CATEGORICAL_NOT_ALLOWED",
        "El dtype 'categorical' no está permitido en TraceSchema v1.1 (campo {field!r}).",
        details_keys=("field", "dtype", "allowed_dtypes", "action"),
        defaults={
            "dtype": "categorical",
            "allowed_dtypes": ("string", "int", "float", "datetime", "bool"),
            "action": "abort",
        },
        exception="schema",
        fatal=True,
    ),
    "SCH.FIELD_SPEC.UNKNOWN_DTYPE": _err(
        "SCH.FIELD_SPEC.UNKNOWN_DTYPE",
        "El campo {field!r} declara un dtype no soportado por import_traces: {dtype!r}.",
        details_keys=("field", "dtype", "allowed_dtypes", "action"),
        defaults={
            "allowed_dtypes": ("string", "int", "float", "datetime", "bool"),
            "action": "abort",
        },
        exception="schema",
        fatal=True,
    ),
    "SCH.FIELD_SPEC.UNKNOWN_REQUIRED_FIELD": _err(
        "SCH.FIELD_SPEC.UNKNOWN_REQUIRED_FIELD",
        "TraceSchema.required contiene campos que no pertenecen al catálogo definido por el schema: {unknown_required}.",
        details_keys=("unknown_required", "required", "schema_fields_sample", "schema_fields_total", "action"),
        defaults={"action": "abort"},
        exception="schema",
        fatal=True,
    ),
    "SCH.CONSTRAINTS.UNKNOWN_RULE": _err(
        "SCH.CONSTRAINTS.UNKNOWN_RULE",
        "El campo {field!r} declara una constraint no soportada por traces v1.1: {constraint!r}.",
        details_keys=("field", "constraint", "allowed_constraints", "action"),
        defaults={
            "allowed_constraints": ("nullable", "range", "datetime", "pattern", "length", "unique"),
            "action": "abort",
        },
        exception="schema",
        fatal=True,
    ),
    "SCH.CONSTRAINTS.NOT_ALLOWED_FOR_DTYPE": _err(
        "SCH.CONSTRAINTS.NOT_ALLOWED_FOR_DTYPE",
        "La constraint {constraint!r} no está permitida para el dtype {dtype!r} del campo {field!r}.",
        details_keys=("field", "dtype", "constraint", "allowed_constraints", "action"),
        defaults={"action": "abort"},
        exception="schema",
        fatal=True,
    ),
    "SCH.TRACE_SCHEMA.INVALID_CRS": _warn(
        "SCH.TRACE_SCHEMA.INVALID_CRS",
        "El CRS declarado en el TraceSchema no es reconocible: {crs!r}; se conservará solo como metadata declarativa.",
        details_keys=("crs", "action"),
        defaults={"action": "keep_declared_crs_only"},
    ),
    "SCH.TRACE_SCHEMA.INVALID_TIMEZONE": _warn(
        "SCH.TRACE_SCHEMA.INVALID_TIMEZONE",
        "La timezone declarada en el TraceSchema no es reconocible: {timezone!r}; se continuará con la precedencia temporal restante.",
        details_keys=("timezone", "action"),
        defaults={"action": "fallback_to_other_time_sources"},
    ),

    # ------------------------------------------------------------------
    # FIELD CORRESPONDENCE / COLUMN RESOLUTION
    # ------------------------------------------------------------------
    "MAP.FIELDS.INVALID_SPEC": _err(
        "MAP.FIELDS.INVALID_SPEC",
        "field_correspondence no tiene una estructura válida para import_traces.",
        details_keys=("received_type", "reason", "action"),
        defaults={"action": "abort"},
        exception="import",
        fatal=True,
    ),
    "MAP.FIELDS.UNKNOWN_CANONICAL_FIELD": _err(
        "MAP.FIELDS.UNKNOWN_CANONICAL_FIELD",
        "field_correspondence referencia un campo canónico no soportado por traces: {field!r}.",
        details_keys=("field", "source_field", "allowed_fields_sample", "allowed_fields_total", "action"),
        defaults={"action": "abort"},
        exception="import",
        fatal=True,
    ),
    "MAP.FIELDS.COLLISION_DUPLICATE_TARGET": _err(
        "MAP.FIELDS.COLLISION_DUPLICATE_TARGET",
        "field_correspondence intenta mapear múltiples columnas fuente hacia el mismo campo canónico {field!r}.",
        details_keys=("field", "source_fields", "action"),
        defaults={"action": "abort"},
        exception="import",
        fatal=True,
    ),
    "MAP.FIELDS.SOURCE_COLUMN_NOT_FOUND": _warn(
        "MAP.FIELDS.SOURCE_COLUMN_NOT_FOUND",
        "La columna fuente {source_field!r} declarada para el campo canónico {field!r} no existe en el DataFrame de entrada.",
        details_keys=("field", "source_field", "available_columns_sample", "available_columns_total", "action"),
        defaults={"action": "skip_mapping_entry"},
    ),

    # ------------------------------------------------------------------
    # CANONICAL CORE / TEMPORAL INTERPRETATION
    # ------------------------------------------------------------------
    "IMP.CORE.POINT_ID_GENERATED": _info(
        "IMP.CORE.POINT_ID_GENERATED",
        "No se encontró point_id alcanzable; se generó automáticamente una columna técnica secuencial.",
        details_keys=("field", "insert_position", "rows_out", "action"),
        defaults={"field": "point_id", "insert_position": 0, "action": "generate_point_id"},
    ),
    "IMP.CORE.MINIMUM_FIELDS_UNREACHABLE": _err(
        "IMP.CORE.MINIMUM_FIELDS_UNREACHABLE",
        "No fue posible materializar el núcleo canónico mínimo de traces: {missing_fields}.",
        details_keys=("missing_fields", "available_columns_sample", "available_columns_total", "action"),
        defaults={"action": "abort"},
        exception="import",
        fatal=True,
    ),
    "IMP.TIME.TIMEZONE_UNRESOLVED": _warn(
        "IMP.TIME.TIMEZONE_UNRESOLVED",
        "No fue posible desambiguar completamente la zona horaria de los timestamps; se conservará la limitación en metadata.",
        details_keys=("precedence_tried", "time_field", "action"),
        defaults={
            "time_field": "time_utc",
            "action": "record_temporal_limitation",
        },
    ),
    "IMP.TIME.NORMALIZATION_FAILED": _err(
        "IMP.TIME.NORMALIZATION_FAILED",
        "No fue posible normalizar o interpretar {field!r} de forma utilizable para construir time_utc.",
        details_keys=(
            "field",
            "reason",
            "n_invalid",
            "invalid_values_sample",
            "source_timezone",
            "schema_timezone",
            "action",
        ),
        defaults={"field": "time_utc", "action": "abort"},
        exception="import",
        fatal=True,
    ),

    # ------------------------------------------------------------------
    # PROVENANCE / METADATA / FINALIZATION
    # ------------------------------------------------------------------
    "IMP.PROVENANCE.INVALID_STRUCTURE": _warn(
        "IMP.PROVENANCE.INVALID_STRUCTURE",
        "El bloque provenance no es usable o no es JSON-safe; se omitirá del TraceDataset resultante.",
        details_keys=("received_type", "reason", "exception_type", "exception_message", "action"),
        defaults={"action": "drop_invalid_provenance"},
    ),
    "IMP.META.NOT_JSON_SERIALIZABLE": _err(
        "IMP.META.NOT_JSON_SERIALIZABLE",
        "Se detectaron valores no serializables en metadata/evento del import de trazas; se aplicará degradación segura si es posible.",
        details_keys=("reason", "problematic_keys", "exception_type", "exception_message", "action"),
        defaults={"action": "degrade_or_drop_non_json_safe_fields"},
        exception="import",
        fatal=False,
    ),
}