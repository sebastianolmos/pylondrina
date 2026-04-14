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


IMPORT_ISSUES: dict[str, IssueSpec] = {
    # ------------------------------------------------------------------
    # INPUT / OPTIONS
    # ------------------------------------------------------------------
    "IMP.INPUT.EMPTY_DATAFRAME": _warn(
        "IMP.INPUT.EMPTY_DATAFRAME",
        "El DataFrame de entrada no contiene filas; se importará un dataset vacío.",
        details_keys=("rows_in", "columns_in", "note"),
        defaults={"note": "empty_input"},
    ),
    "IMP.OPTIONS.INVALID_SELECTED_FIELD": _err(
        "IMP.OPTIONS.INVALID_SELECTED_FIELD",
        "selected_fields contiene campos inválidos/no definidos en el schema: {invalid_fields}.",
        details_keys=("selected_fields", "invalid_fields", "reason", "action"),
        defaults={"reason": "not_in_schema", "action": "abort"},
        exception="import",
    ),
    "IMP.OPTIONS.EMPTY_SELECTED_FIELD": _info(
        "IMP.OPTIONS.EMPTY_SELECTED_FIELD",
        "selected_fields está vacío; se conservarán únicamente los campos obligatorios del schema.",
        details_keys=("selected_fields", "effective_selected_fields", "fallback"),
        defaults={"fallback": "required_only"},
    ),
    "IMP.OPTIONS.EXTRA_FIELDS_DROPPED": _info(
        "IMP.OPTIONS.EXTRA_FIELDS_DROPPED",
        "Se descartaron columnas extra de la fuente porque keep_extra_fields=False (n={n_dropped}).",
        details_keys=("keep_extra_fields", "n_dropped", "dropped_columns_sample", "dropped_columns_total", "action"),
        defaults={"action": "dropped_extras"},
    ),

    # ------------------------------------------------------------------
    # SCHEMA
    # ------------------------------------------------------------------
    "SCH.TRIP_SCHEMA.INVALID_VERSION": _err(
        "SCH.TRIP_SCHEMA.INVALID_VERSION",
        "La versión del TripSchema no es válida o no se puede interpretar: {schema_version!r}.",
        details_keys=("schema_version", "expected", "schema_name"),
        defaults={"expected": "non-empty string"},
        exception="schema",
    ),
    "SCH.TRIP_SCHEMA.EMPTY_FIELDS": _err(
        "SCH.TRIP_SCHEMA.EMPTY_FIELDS",
        "El TripSchema no define campos (fields está vacío); no es posible importar con un esquema sin catálogo de campos.",
        details_keys=("schema_version", "fields_size", "note"),
        defaults={"note": "no_fields_defined"},
        exception="schema",
    ),
    "SCH.TRIP_SCHEMA.EMPTY_REQUIRED": _err(
        "SCH.TRIP_SCHEMA.EMPTY_REQUIRED",
        "El TripSchema no define campos obligatorios (required está vacío); no es posible asegurar el contrato mínimo de Golondrina.",
        details_keys=("schema_version", "required_size"),
        exception="schema",
    ),
    "SCH.FIELD_SPEC.UNKNOWN_DTYPE": _warn(
        "SCH.FIELD_SPEC.UNKNOWN_DTYPE",
        "El tipo lógico declarado para el campo {field!r} no es reconocido ({dtype!r}); se usará fallback conservador a texto.",
        details_keys=("field", "dtype", "fallback_dtype", "action"),
        defaults={"fallback_dtype": "string", "action": "fallback_dtype"},
    ),
    "SCH.DOMAIN.MISSING_FOR_CATEGORICAL": _warn(
        "SCH.DOMAIN.MISSING_FOR_CATEGORICAL",
        "El campo categórico {field!r} no tiene DomainSpec asociado; se degradará a texto para permitir el import.",
        details_keys=("field", "dtype", "fallback_dtype", "action"),
        defaults={"dtype": "categorical", "fallback_dtype": "string", "action": "fallback_dtype"},
    ),
    "SCH.DOMAIN.EMPTY_VALUES": _info(
        "SCH.DOMAIN.EMPTY_VALUES",
        "El DomainSpec del campo {field!r} tiene values vacío; se tratará como dominio bootstrap/extensible según política.",
        details_keys=("field", "values_size", "extendable", "note"),
        defaults={"values_size": 0, "note": "candidate_bootstrap"},
    ),
    "SCH.DOMAIN.NON_STRING_VALUES": _warn(
        "SCH.DOMAIN.NON_STRING_VALUES",
        "El DomainSpec del campo {field!r} contiene valores no string; revise el dominio base.",
        details_keys=("field", "domain_values_sample", "domain_values_total"),
    ),
    "SCH.CONSTRAINTS.INVALID_FORMAT": _err(
        "SCH.CONSTRAINTS.INVALID_FORMAT",
        "El conjunto de constraints del campo {field!r} contiene una regla con formato inválido; el esquema no es confiable para import.",
        details_keys=("field", "rule_raw", "expected", "reason"),
        defaults={"expected": "dict-like constraint spec", "reason": "not_parseable"},
        exception="schema",
    ),
    "SCH.CONSTRAINTS.UNKNOWN_RULE": _err(
        "SCH.CONSTRAINTS.UNKNOWN_RULE",
        "El campo {field!r} incluye una constraint no soportada ({rule!r}); el esquema no puede ejecutarse de forma consistente.",
        details_keys=("field", "rule", "supported_rules", "action"),
        defaults={"action": "abort"},
        exception="schema",
    ),
    "SCH.CONSTRAINTS.INCOMPATIBLE_WITH_DTYPE": _warn(
        "SCH.CONSTRAINTS.INCOMPATIBLE_WITH_DTYPE",
        "El campo {field!r} declara una constraint potencialmente incompatible con su dtype efectivo ({dtype!r}); se conserva solo como advertencia.",
        details_keys=("field", "dtype", "rule"),
    ),

    # ------------------------------------------------------------------
    # FIELD CORRESPONDENCE
    # ------------------------------------------------------------------
    "MAP.FIELDS.UNKNOWN_CANONICAL_FIELD": _err(
        "MAP.FIELDS.UNKNOWN_CANONICAL_FIELD",
        "field_correspondence contiene un campo canónico desconocido para el TripSchema: {field!r}.",
        details_keys=("field", "schema_fields_sample", "schema_fields_total", "action"),
        defaults={"action": "abort"},
        exception="import",
    ),
    "MAP.FIELDS.MISSING_SOURCE_COLUMN": _err(
        "MAP.FIELDS.MISSING_SOURCE_COLUMN",
        "field_correspondence referencia una columna fuente inexistente para {field!r}: {source_field!r}.",
        details_keys=("field", "source_field", "source_columns_sample", "source_columns_total", "action"),
        defaults={"action": "abort"},
        exception="import",
    ),
    "MAP.FIELDS.COLLISION_DUPLICATE_TARGET": _err(
        "MAP.FIELDS.COLLISION_DUPLICATE_TARGET",
        "La correspondencia de campos produce colisión: múltiples canónicos apuntan a {source_column!r}.",
        details_keys=("source_column", "canonical_fields", "field_correspondence", "action"),
        defaults={"action": "abort"},
        exception="import",
    ),
    "MAP.FIELDS.CANONICAL_ALREADY_PRESENT_CONFLICT": _err(
        "MAP.FIELDS.CANONICAL_ALREADY_PRESENT_CONFLICT",
        "La correspondencia de campos entra en conflicto porque el canónico {field!r} ya existe en el DataFrame y además se intenta renombrar {source_field!r} hacia él.",
        details_keys=("field", "source_field", "action"),
        defaults={"action": "abort"},
        exception="import",
    ),
    "IMP.INPUT.OPTIONAL_FIELD_NOT_FOUND": _info(
        "IMP.INPUT.OPTIONAL_FIELD_NOT_FOUND",
        "El campo opcional {field!r} no está presente en la fuente y no se encontró correspondencia; se omitirá.",
        details_keys=("field", "source_columns", "field_correspondence_used", "action"),
        defaults={"action": "omit_optional"},
    ),
    "IMP.INPUT.MISSING_REQUIRED_FIELD": _err(
        "IMP.INPUT.MISSING_REQUIRED_FIELD",
        "Faltan campos obligatorios para importar según el TripSchema: {missing_required}.",
        details_keys=("missing_required", "required", "source_columns", "field_correspondence_keys", "field_correspondence_values_sample"),
        exception="import",
    ),

    # ------------------------------------------------------------------
    # TEMPORAL
    # ------------------------------------------------------------------
    "IMP.TEMPORAL.TIER_LIMITED": _warn(
        "IMP.TEMPORAL.TIER_LIMITED",
        "El dataset fue importado con capacidad temporal limitada ({temporal_tier}); algunas operaciones posteriores quedarán restringidas.",
        details_keys=("temporal_tier", "fields_present", "note"),
    ),
    "IMP.DATETIME.INVALID_SOURCE_TIMEZONE": _warn(
        "IMP.DATETIME.INVALID_SOURCE_TIMEZONE",
        "source_timezone={source_timezone!r} no es válida; se ignorará y los datetimes naive no se convertirán automáticamente.",
        details_keys=("source_timezone", "reason", "action"),
        defaults={"reason": "invalid_timezone", "action": "ignore_timezone"},
    ),
    "IMP.DATETIME.NAIVE_WITHOUT_SOURCE_TZ": _warn(
        "IMP.DATETIME.NAIVE_WITHOUT_SOURCE_TZ",
        "El campo datetime {field!r} contiene valores naive y no se declaró source_timezone; se conservará sin conversión a UTC.",
        details_keys=("field", "action"),
        defaults={"action": "naive_unconverted"},
    ),
    "IMP.DATETIME.NUMERIC_NOT_PARSED": _warn(
        "IMP.DATETIME.NUMERIC_NOT_PARSED",
        "El campo datetime {field!r} contiene valores numéricos y v1.1 no parsea epochs automáticamente; se marcarán como NaT.",
        details_keys=("field", "action"),
        defaults={"action": "set_nat"},
    ),

    # ------------------------------------------------------------------
    # VALUE CORRESPONDENCE / DOMAINS
    # ------------------------------------------------------------------
    "MAP.VALUES.UNKNOWN_CANONICAL_FIELD": _err(
        "MAP.VALUES.UNKNOWN_CANONICAL_FIELD",
        "value_correspondence referencia un campo canónico inexistente en el TripSchema: {field!r}.",
        details_keys=("field", "schema_fields_sample", "schema_fields_total", "action"),
        defaults={"action": "abort"},
        exception="import",
    ),
    "MAP.VALUES.NON_CATEGORICAL_FIELD": _warn(
        "MAP.VALUES.NON_CATEGORICAL_FIELD",
        "Se proporcionó value_correspondence para {field!r}, pero el campo no es categórico; se ignorará.",
        details_keys=("field", "field_dtype", "reason", "action"),
        defaults={"reason": "value_mapping_on_non_categorical", "action": "ignored_mapping"},
    ),
    "MAP.VALUES.UNKNOWN_CANONICAL_VALUE": _err(
        "MAP.VALUES.UNKNOWN_CANONICAL_VALUE",
        "value_correspondence para el campo {field!r} mapea hacia un valor canónico no definido en el dominio: {canonical_value!r}.",
        details_keys=("field", "canonical_value", "domain_values_sample", "domain_values_total"),
        exception="import",
    ),
    "DOM.POLICY.FIELD_NOT_EXTENDABLE": _warn(
        "DOM.POLICY.FIELD_NOT_EXTENDABLE",
        "El campo {field!r} no admite extensión de dominio (extendable=False); los valores fuera de dominio se mapearán a unknown.",
        details_keys=("field", "strict_domains", "domain_extendable", "action"),
        defaults={"action": "map_to_unknown"},
    ),
    "DOM.POLICY.MAPPING_REQUIRES_EXTENSION_BLOCKED": _err(
        "DOM.POLICY.MAPPING_REQUIRES_EXTENSION_BLOCKED",
        "La correspondencia de valores para {field!r} requiere extender el dominio, pero la política vigente lo prohíbe.",
        details_keys=("field", "strict_domains", "domain_extendable", "reason", "unmapped_examples", "unmapped_count", "action"),
        defaults={"reason": "extension_required_but_disallowed", "action": "abort"},
        exception="import",
    ),
    "DOM.STRICT.OUT_OF_DOMAIN_ABORT": _err(
        "DOM.STRICT.OUT_OF_DOMAIN_ABORT",
        "Se detectaron valores fuera de dominio en {field!r} con strict_domains=True; import abortado.",
        details_keys=("field", "unknown_count", "total_count", "unknown_rate", "unknown_examples", "policy", "action"),
        defaults={"action": "abort"},
        exception="import",
    ),
    "DOM.EXTENSION.APPLIED": _info(
        "DOM.EXTENSION.APPLIED",
        "Se extendió el dominio de {field!r} con {n_added} valores nuevos.",
        details_keys=("field", "n_added", "added_values_sample", "added_values_total", "policy", "action"),
        defaults={"action": "extended_domain"},
    ),

    # ------------------------------------------------------------------
    # TYPE COERCION / COORD / H3
    # ------------------------------------------------------------------
    "IMP.TYPE.COERCE_FAILED_REQUIRED": _err(
        "IMP.TYPE.COERCE_FAILED_REQUIRED",
        "No fue posible convertir el campo requerido {field!r} a un formato mínimo utilizable; import abortado.",
        details_keys=("field", "dtype_expected", "parse_fail_count", "rows_in", "fail_rate", "examples_sample", "action"),
        defaults={"action": "abort"},
        exception="import",
    ),
    "IMP.TYPE.COERCE_PARTIAL": _warn(
        "IMP.TYPE.COERCE_PARTIAL",
        "La conversión mínima del campo {field!r} falló en algunas filas ({fail_rate:.1%}); se marcarán como nulos para validación posterior.",
        details_keys=("field", "dtype_expected", "parse_fail_count", "total_count", "fail_rate", "fallback", "action"),
        defaults={"fallback": "set_null", "action": "continue"},
    ),
    "IMP.H3.INVALID_RESOLUTION": _err(
        "IMP.H3.INVALID_RESOLUTION",
        "La resolución H3 solicitada ({h3_resolution!r}) es inválida para import.",
        details_keys=("h3_resolution", "expected", "action"),
        defaults={"expected": "integer in [0, 15]", "action": "abort"},
        exception="import",
    ),
    "IMP.H3.REQUIRED_FIELDS_UNAVAILABLE": _err(
        "IMP.H3.REQUIRED_FIELDS_UNAVAILABLE",
        "No es posible materializar los índices H3 requeridos por el schema porque faltan campos fuente utilizables.",
        details_keys=("missing_pairs", "required_h3_fields", "action"),
        defaults={"action": "abort"},
        exception="import",
    ),
    "IMP.H3.PARTIAL_DERIVATION": _warn(
        "IMP.H3.PARTIAL_DERIVATION",
        "La derivación de índices H3 dejó filas sin valor en algunas observaciones; se conservarán como NA.",
        details_keys=("derived_fields", "null_count", "rows_in"),
    ),

    # ------------------------------------------------------------------
    # IDS
    # ------------------------------------------------------------------
    "IMP.ID.MOVEMENT_ID_DUPLICATE": _err(
        "IMP.ID.MOVEMENT_ID_DUPLICATE",
        "La columna movement_id contiene valores duplicados; no es posible construir un TripDataset con unicidad de fila garantizada.",
        details_keys=("duplicate_count", "duplicate_examples", "action"),
        defaults={"action": "abort"},
        exception="import",
    ),
    "IMP.ID.MOVEMENT_ID_CREATED": _info(
        "IMP.ID.MOVEMENT_ID_CREATED",
        "Se generó movement_id porque no venía presente en la fuente.",
        details_keys=("field", "action"),
        defaults={"field": "movement_id", "action": "generated"},
    ),
    "IMP.ID.TRIP_ID_CREATED": _info(
        "IMP.ID.TRIP_ID_CREATED",
        "Se generó trip_id a partir de movement_id porque single_stage=True y no venía en la fuente.",
        details_keys=("field", "action"),
        defaults={"field": "trip_id", "action": "generated_from_movement_id"},
    ),
    "IMP.ID.MOVEMENT_SEQ_CREATED": _info(
        "IMP.ID.MOVEMENT_SEQ_CREATED",
        "Se generó movement_seq=0 porque single_stage=True y no venía en la fuente.",
        details_keys=("field", "action"),
        defaults={"field": "movement_seq", "action": "generated_zero"},
    ),

    # ------------------------------------------------------------------
    # PROVENANCE / METADATA
    # ------------------------------------------------------------------
    "PRV.INPUT.NOT_JSON_SERIALIZABLE": _err(
        "PRV.INPUT.NOT_JSON_SERIALIZABLE",
        "El objeto provenance no es serializable a JSON; no se puede registrar procedencia de forma persistente.",
        details_keys=("type", "reason", "example_repr", "action", "suggestion"),
        defaults={"action": "abort"},
        exception="import",
    ),
    "IMP.METADATA.DATASET_ID_CREATED": _info(
        "IMP.METADATA.DATASET_ID_CREATED",
        "Se generó dataset_id para el dataset importado: {dataset_id!r}.",
        details_keys=("dataset_id", "generator", "stored_in"),
    ),

    # ------------------------------------------------------------------
    # Fields: SOFT CAP / HARD CAP
    # ------------------------------------------------------------------
    "IMP.COLUMNS.WIDE_TABLE": _warn(
        "IMP.COLUMNS.WIDE_TABLE",
        "El TripDataset resultante tiene {n_columns} columnas, superando el soft cap {soft_cap}; se permite continuar, pero la tabla es ancha y puede generar fricción operativa.",
        details_keys=("n_columns", "soft_cap", "hard_cap", "extra_fields_kept_sample", "extra_fields_kept_total", "action"),
        defaults={"action": "allow_with_warning"},
    ),

    "IMP.COLUMNS.HARD_CAP_EXCEEDED": _err(
        "IMP.COLUMNS.HARD_CAP_EXCEEDED",
        "El TripDataset resultante tiene {n_columns} columnas, superando el hard cap {hard_cap}; se rechaza la importación.",
        details_keys=("n_columns", "soft_cap", "hard_cap", "extra_fields_kept_sample", "extra_fields_kept_total", "action"),
        defaults={"action": "abort"},
        exception="import",
        fatal=True,
    ),
}