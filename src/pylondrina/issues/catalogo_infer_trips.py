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
    exception: str = "inference",
    fatal: bool = False,
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


def _fatal_infer(code: str, message: str, *, details_keys=(), defaults=None) -> IssueSpec:
    return _err(
        code,
        message,
        details_keys=details_keys,
        defaults=defaults,
        exception="inference",
        fatal=True,
    )


def _fatal_schema(code: str, message: str, *, details_keys=(), defaults=None) -> IssueSpec:
    return _err(
        code,
        message,
        details_keys=details_keys,
        defaults=defaults,
        exception="schema",
        fatal=True,
    )


_REQUEST_DETAILS = (
    "infer_mode",
    "strict",
    "strict_domains",
    "drop_invalid",
    "require_validated_traces",
    "h3_resolution",
    "max_time_delta_s",
    "min_time_delta_s",
    "min_distance_m",
    "cluster_radius_m",
    "cluster_max_time_gap_s",
)

_INPUT_COUNTS = (
    "n_points_in",
    "n_users_in",
    "n_candidates_in",
    "n_candidates_dropped",
    "n_trips_out",
    "n_clusters_out",
)

_AVAILABLE_FIELDS = (
    "available_fields_sample",
    "available_fields_total",
)

_CANDIDATE_DROP_DETAILS = (
    *_REQUEST_DETAILS,
    *_INPUT_COUNTS,
    "reason",
    "rule",
    "threshold",
    "row_indices_sample",
    "pairs_sample",
    "action",
)

_CLUSTER_DETAILS = (
    *_REQUEST_DETAILS,
    "n_points_in",
    "n_users_in",
    "n_clusters_out",
    "cluster_radius_m",
    "cluster_max_time_gap_s",
    "clusters_sample",
    "action",
)

_PROPAGATION_DETAILS = (
    *_REQUEST_DETAILS,
    "field",
    "propagate_trace_fields",
    "target_columns",
    "created_columns_sample",
    "created_columns_total",
    "reason",
    "action",
)

_H3_DETAILS = (
    *_REQUEST_DETAILS,
    *_INPUT_COUNTS,
    "row_indices_sample",
    "sample_rows",
    "reason",
    "action",
)

_DOMAIN_DETAILS = (
    "field",
    "field_dtype",
    "strict_domains",
    "domain_extendable",
    "domain_values_sample",
    "domain_values_total",
    "canonical_value",
    "unknown_count",
    "total_count",
    "unknown_rate",
    "unknown_examples",
    "unmapped_examples",
    "unmapped_count",
    "n_added",
    "added_values_sample",
    "added_values_total",
    "n_values_changed",
    "n_unique_observed",
    "n_rows_non_null",
    "observed_values_sample",
    "observed_values_total",
    "alpha",
    "k_max",
    "cardinality_limit",
    "policy",
    "reason",
    "fallback_dtype",
    "action",
)

_PROVENANCE_DETAILS = (
    *_REQUEST_DETAILS,
    "field",
    "reason",
    "expected",
    "action",
)


INFER_TRIPS_ISSUES: dict[str, IssueSpec] = {
    # ------------------------------------------------------------------
    # INPUT / OPTIONS / PRECONDITIONS
    # ------------------------------------------------------------------
    "INF.INPUT.INVALID_TRACES_OBJECT": _fatal_infer(
        "INF.INPUT.INVALID_TRACES_OBJECT",
        "El objeto entregado a infer_trips_from_traces no es un TraceDataset utilizable.",
        details_keys=("received_type", "expected_type", "action"),
        defaults={"expected_type": "TraceDataset", "action": "abort"},
    ),
    "INF.INPUT.MISSING_DATAFRAME": _fatal_infer(
        "INF.INPUT.MISSING_DATAFRAME",
        "El TraceDataset no expone una tabla usable en traces.data para ejecutar la inferencia.",
        details_keys=("attribute", "reason", "action"),
        defaults={"attribute": "data", "action": "abort"},
    ),
    "INF.INPUT.EMPTY_DATAFRAME": _warn(
        "INF.INPUT.EMPTY_DATAFRAME",
        "TraceDataset.data no contiene filas; se devolverá un TripDataset vacío.",
        details_keys=(*_REQUEST_DETAILS, "n_points_in", "action"),
        defaults={"action": "return_empty_output"},
    ),
    "INF.INPUT.MISSING_MIN_TRACE_FIELDS": _fatal_infer(
        "INF.INPUT.MISSING_MIN_TRACE_FIELDS",
        "El TraceDataset no contiene el mínimo canónico requerido para inferir viajes; faltan: {missing_fields}.",
        details_keys=("missing_fields", "required_fields", *_AVAILABLE_FIELDS, "action"),
        defaults={
            "required_fields": ("point_id", "user_id", "time_utc", "latitude", "longitude"),
            "action": "abort",
        },
    ),
    "INF.PRECONDITION.TRACES_NOT_VALIDATED": _fatal_infer(
        "INF.PRECONDITION.TRACES_NOT_VALIDATED",
        "No se puede inferir viajes porque require_validated_traces=True y traces.metadata['is_validated']={flag_value!r}.",
        details_keys=(*_REQUEST_DETAILS, "flag_field", "flag_value", "expected", "action"),
        defaults={
            "flag_field": "is_validated",
            "expected": True,
            "action": "abort",
        },
    ),
    "INF.PRECONDITION.VALIDATION_BYPASS_USED": _warn(
        "INF.PRECONDITION.VALIDATION_BYPASS_USED",
        "Se ejecutará la inferencia sin exigir trazas validadas (validation bypass activo).",
        details_keys=(*_REQUEST_DETAILS, "flag_field", "flag_value", "action"),
        defaults={"flag_field": "is_validated", "action": "continue_unvalidated"},
    ),
    "INF.OPTIONS.UNKNOWN_INFER_MODE": _fatal_infer(
        "INF.OPTIONS.UNKNOWN_INFER_MODE",
        "infer_mode={value!r} no es válido; modos soportados: {supported_modes}.",
        details_keys=("option", "value", "supported_modes", "action"),
        defaults={
            "option": "infer_mode",
            "supported_modes": ("consecutive_points", "consecutive_clusters"),
            "action": "abort",
        },
    ),
    "INF.OPTIONS.INVALID_H3_RESOLUTION": _fatal_infer(
        "INF.OPTIONS.INVALID_H3_RESOLUTION",
        "h3_resolution={value!r} no es válida; se requiere una resolución H3 interpretable en el rango permitido.",
        details_keys=("option", "value", "expected", "action"),
        defaults={"option": "h3_resolution", "expected": "int H3 resolution", "action": "abort"},
    ),
    "INF.OPTIONS.INVALID_MAX_TIME_DELTA": _fatal_infer(
        "INF.OPTIONS.INVALID_MAX_TIME_DELTA",
        "max_time_delta_s={value!r} es inválido; se requiere un número positivo o None.",
        details_keys=("option", "value", "expected", "action"),
        defaults={"option": "max_time_delta_s", "expected": "positive number or None", "action": "abort"},
    ),
    "INF.OPTIONS.INVALID_MIN_TIME_DELTA": _fatal_infer(
        "INF.OPTIONS.INVALID_MIN_TIME_DELTA",
        "min_time_delta_s={value!r} es inválido; se requiere un número no negativo o None.",
        details_keys=("option", "value", "expected", "action"),
        defaults={"option": "min_time_delta_s", "expected": "non-negative number or None", "action": "abort"},
    ),
    "INF.OPTIONS.INVALID_MIN_DISTANCE": _fatal_infer(
        "INF.OPTIONS.INVALID_MIN_DISTANCE",
        "min_distance_m={value!r} es inválido; se requiere un número no negativo o None.",
        details_keys=("option", "value", "expected", "action"),
        defaults={"option": "min_distance_m", "expected": "non-negative number or None", "action": "abort"},
    ),
    "INF.OPTIONS.INCONSISTENT_TIME_THRESHOLDS": _fatal_infer(
        "INF.OPTIONS.INCONSISTENT_TIME_THRESHOLDS",
        "La configuración temporal es inconsistente: min_time_delta_s={min_value!r} supera max_time_delta_s={max_value!r}.",
        details_keys=("min_value", "max_value", "reason", "action"),
        defaults={"reason": "min_gt_max", "action": "abort"},
    ),
    "INF.OPTIONS.INVALID_CLUSTER_RADIUS": _fatal_infer(
        "INF.OPTIONS.INVALID_CLUSTER_RADIUS",
        "cluster_radius_m={value!r} es inválido para infer_mode='consecutive_clusters'; se requiere un número positivo.",
        details_keys=("option", "value", "expected", "action"),
        defaults={"option": "cluster_radius_m", "expected": "positive number", "action": "abort"},
    ),
    "INF.OPTIONS.INVALID_CLUSTER_MAX_TIME_GAP": _fatal_infer(
        "INF.OPTIONS.INVALID_CLUSTER_MAX_TIME_GAP",
        "cluster_max_time_gap_s={value!r} es inválido para infer_mode='consecutive_clusters'; se requiere un número positivo.",
        details_keys=("option", "value", "expected", "action"),
        defaults={"option": "cluster_max_time_gap_s", "expected": "positive number", "action": "abort"},
    ),
    "INF.OPTIONS.INVALID_PROPAGATE_TRACE_FIELDS": _fatal_infer(
        "INF.OPTIONS.INVALID_PROPAGATE_TRACE_FIELDS",
        "propagate_trace_fields tiene una estructura inválida o no interpretable.",
        details_keys=("option", "value", "expected", "reason", "action"),
        defaults={
            "option": "propagate_trace_fields",
            "expected": "Mapping[str, Literal['origin','destination','both']]",
            "action": "abort",
        },
    ),

    # ------------------------------------------------------------------
    # OUTPUT SCHEMA / CONTRACT
    # ------------------------------------------------------------------
    "SCH.TRIP_SCHEMA.INVALID_VERSION": _fatal_schema(
        "SCH.TRIP_SCHEMA.INVALID_VERSION",
        "La versión del TripSchema no es válida o no se puede interpretar: {schema_version!r}.",
        details_keys=("schema_version", "expected", "schema_name"),
        defaults={"expected": "non-empty string"},
    ),
    "SCH.TRIP_SCHEMA.EMPTY_FIELDS": _fatal_schema(
        "SCH.TRIP_SCHEMA.EMPTY_FIELDS",
        "El TripSchema no define campos (fields está vacío); no es posible materializar el output de infer_trips.",
        details_keys=("schema_version", "fields_size", "action"),
        defaults={"action": "abort"},
    ),
    "SCH.TRIP_SCHEMA.MISSING_MIN_OUTPUT_FIELDS": _fatal_schema(
        "SCH.TRIP_SCHEMA.MISSING_MIN_OUTPUT_FIELDS",
        "El TripSchema no cubre el núcleo mínimo requerido del output de trips; faltan: {missing_fields}.",
        details_keys=("missing_fields", "required_fields", "schema_fields_sample", "schema_fields_total", "action"),
        defaults={
            "required_fields": (
                "movement_id",
                "user_id",
                "origin_longitude",
                "origin_latitude",
                "destination_longitude",
                "destination_latitude",
                "origin_time_utc",
                "destination_time_utc",
                "origin_h3_index",
                "destination_h3_index",
                "trip_id",
                "movement_seq",
            ),
            "action": "abort",
        },
    ),

    # ------------------------------------------------------------------
    # CANDIDATE BUILDING / CLUSTERS
    # ------------------------------------------------------------------
    "INF.CANDIDATES.POINTS_MODE_APPLIED": _info(
        "INF.CANDIDATES.POINTS_MODE_APPLIED",
        "Se construyeron {n_candidates_in} candidatos OD a partir de pares consecutivos por usuario.",
        details_keys=(*_REQUEST_DETAILS, "n_points_in", "n_users_in", "n_candidates_in", "action"),
        defaults={"action": "built_point_candidates"},
    ),
    "INF.CLUSTERS.MODE_APPLIED": _info(
        "INF.CLUSTERS.MODE_APPLIED",
        "Se construyeron {n_clusters_out} clusters secuenciales y {n_candidates_in} candidatos OD entre clusters consecutivos.",
        details_keys=_CLUSTER_DETAILS + ("n_candidates_in",),
        defaults={"action": "built_cluster_candidates"},
    ),
    "INF.CANDIDATES.NO_CANDIDATES_BUILT": _warn(
        "INF.CANDIDATES.NO_CANDIDATES_BUILT",
        "No se construyeron candidatos OD a partir del input entregado.",
        details_keys=(*_REQUEST_DETAILS, "n_points_in", "n_users_in", "reason", "action"),
        defaults={"action": "return_empty_output"},
    ),
    "INF.CANDIDATES.DROPPED_MAX_TIME_DELTA": _info(
        "INF.CANDIDATES.DROPPED_MAX_TIME_DELTA",
        "Se descartaron {n_candidates_dropped} candidatos por exceder max_time_delta_s={max_time_delta_s!r}.",
        details_keys=_CANDIDATE_DROP_DETAILS,
        defaults={"rule": "max_time_delta_s", "action": "dropped_candidates"},
    ),
    "INF.CANDIDATES.DROPPED_MIN_TIME_DELTA": _info(
        "INF.CANDIDATES.DROPPED_MIN_TIME_DELTA",
        "Se descartaron {n_candidates_dropped} candidatos por quedar bajo min_time_delta_s={min_time_delta_s!r}.",
        details_keys=_CANDIDATE_DROP_DETAILS,
        defaults={"rule": "min_time_delta_s", "action": "dropped_candidates"},
    ),
    "INF.CANDIDATES.DROPPED_MIN_DISTANCE": _info(
        "INF.CANDIDATES.DROPPED_MIN_DISTANCE",
        "Se descartaron {n_candidates_dropped} candidatos por quedar bajo min_distance_m={min_distance_m!r}.",
        details_keys=_CANDIDATE_DROP_DETAILS,
        defaults={"rule": "min_distance_m", "action": "dropped_candidates"},
    ),
    "INF.CANDIDATES.DROPPED_SAME_PLACE": _info(
        "INF.CANDIDATES.DROPPED_SAME_PLACE",
        "Se descartaron {n_candidates_dropped} candidatos por regla same_place basada en location_ref.",
        details_keys=_CANDIDATE_DROP_DETAILS,
        defaults={"rule": "same_place", "action": "dropped_candidates"},
    ),
    "INF.CANDIDATES.INVALID_DROPPED": _info(
        "INF.CANDIDATES.INVALID_DROPPED",
        "Se descartaron {n_candidates_dropped} candidatos inválidos porque drop_invalid=True.",
        details_keys=_CANDIDATE_DROP_DETAILS,
        defaults={"reason": "invalid_candidates", "action": "dropped_candidates"},
    ),
    "INF.CANDIDATES.INVALID_RETAINED": _warn(
        "INF.CANDIDATES.INVALID_RETAINED",
        "Se conservaron candidatos con problemas operacionales porque drop_invalid=False; el output puede requerir revisión.",
        details_keys=_CANDIDATE_DROP_DETAILS,
        defaults={"reason": "invalid_candidates_retained", "action": "retain_with_evidence"},
    ),
    "INF.CANDIDATES.NO_MATERIALIZABLE_CANDIDATES": _err(
        "INF.CANDIDATES.NO_MATERIALIZABLE_CANDIDATES",
        "Tras aplicar thresholds y política operacional no queda ningún candidato materializable como trip.",
        details_keys=(*_REQUEST_DETAILS, *_INPUT_COUNTS, "reason", "action"),
        defaults={"action": "return_empty_or_raise"},
        exception="inference",
        fatal=False,
    ),

    # ------------------------------------------------------------------
    # PROPAGATION / OUTPUT SHAPE / MATERIALIZATION
    # ------------------------------------------------------------------
    "INF.PROPAGATION.UNKNOWN_TRACE_FIELD": _fatal_infer(
        "INF.PROPAGATION.UNKNOWN_TRACE_FIELD",
        "propagate_trace_fields hace referencia al campo {field!r}, pero este no existe en traces.data.",
        details_keys=("field", *_AVAILABLE_FIELDS, "action"),
        defaults={"action": "abort"},
    ),
    "INF.PROPAGATION.RESERVED_TARGET_CONFLICT": _fatal_infer(
        "INF.PROPAGATION.RESERVED_TARGET_CONFLICT",
        "La propagación solicitada para {field!r} produciría una colisión con columnas reservadas o ya materializadas del output: {target_columns!r}.",
        details_keys=_PROPAGATION_DETAILS,
        defaults={"action": "abort"},
    ),
    "INF.PROPAGATION.APPLIED": _info(
        "INF.PROPAGATION.APPLIED",
        "Se propagaron campos extra desde traces hacia el output de trips (nuevas columnas={created_columns_total}).",
        details_keys=_PROPAGATION_DETAILS,
        defaults={"action": "propagated_fields"},
    ),
    "INF.OUTPUT.SOFT_WIDTH_EXCEEDED": _warn(
        "INF.OUTPUT.SOFT_WIDTH_EXCEEDED",
        "El output inferido alcanzó {n_columns} columnas, superando el soft cap {soft_cap}; revise la propagación de extras.",
        details_keys=("n_columns", "soft_cap", "hard_cap", "created_columns_sample", "created_columns_total", "action"),
        defaults={"action": "review_output_width"},
    ),
    "INF.OUTPUT.HARD_WIDTH_EXCEEDED": _fatal_infer(
        "INF.OUTPUT.HARD_WIDTH_EXCEEDED",
        "El output inferido alcanzó {n_columns} columnas, superando el hard cap {hard_cap}; se aborta la inferencia.",
        details_keys=("n_columns", "soft_cap", "hard_cap", "created_columns_sample", "created_columns_total", "action"),
        defaults={"action": "abort"},
    ),
    "INF.OUTPUT.MISSING_REQUIRED_COLUMNS": _fatal_infer(
        "INF.OUTPUT.MISSING_REQUIRED_COLUMNS",
        "No fue posible materializar el núcleo canónico mínimo del output de trips; faltan columnas requeridas: {missing_fields}.",
        details_keys=("missing_fields", "required_fields", "output_fields_sample", "output_fields_total", "action"),
        defaults={"action": "abort"},
    ),
    "INF.WARN.ZERO_TRIPS": _warn(
        "INF.WARN.ZERO_TRIPS",
        "La inferencia terminó sin viajes materializados; el resultado contiene 0 filas.",
        details_keys=(*_REQUEST_DETAILS, *_INPUT_COUNTS, "dropped_by_reason", "action"),
        defaults={"action": "return_empty_output"},
    ),

    # ------------------------------------------------------------------
    # H3
    # ------------------------------------------------------------------
    "INF.H3.DERIVED": _info(
        "INF.H3.DERIVED",
        "Se derivaron origin_h3_index y destination_h3_index con h3_resolution={h3_resolution!r}.",
        details_keys=(*_REQUEST_DETAILS, "n_trips_out", "action"),
        defaults={"action": "derived_h3"},
    ),
    "INF.H3.DERIVATION_FAILED": _err(
        "INF.H3.DERIVATION_FAILED",
        "No fue posible derivar H3 para parte del output inferido; revise coordenadas OD y configuración espacial.",
        details_keys=_H3_DETAILS,
        defaults={"action": "drop_or_raise"},
        exception="inference",
        fatal=False,
    ),

    # ------------------------------------------------------------------
    # VALUE CORRESPONDENCE / DOMAINS
    # ------------------------------------------------------------------
    "MAP.VALUES.APPLIED": _info(
        "MAP.VALUES.APPLIED",
        "Se aplicó value_correspondence sobre {field!r}; se modificaron {n_values_changed} valores en el output.",
        details_keys=("field", "n_values_changed", "policy", "action"),
        defaults={"action": "normalized_values"},
    ),
    "MAP.VALUES.UNKNOWN_CANONICAL_FIELD": _fatal_infer(
        "MAP.VALUES.UNKNOWN_CANONICAL_FIELD",
        "value_correspondence referencia un campo canónico inexistente en el TripSchema: {field!r}.",
        details_keys=("field", "schema_fields_sample", "schema_fields_total", "action"),
        defaults={"action": "abort"},
    ),
    "MAP.VALUES.FIELD_NOT_MATERIALIZED": _warn(
        "MAP.VALUES.FIELD_NOT_MATERIALIZED",
        "Se proporcionó value_correspondence para {field!r}, pero el campo no fue materializado en el output; se ignorará.",
        details_keys=("field", "output_fields_sample", "output_fields_total", "reason", "action"),
        defaults={"reason": "field_not_materialized", "action": "ignored_mapping"},
    ),
    "MAP.VALUES.NON_CATEGORICAL_FIELD": _warn(
        "MAP.VALUES.NON_CATEGORICAL_FIELD",
        "Se proporcionó value_correspondence para {field!r}, pero el campo no es categórico; se ignorará.",
        details_keys=("field", "field_dtype", "reason", "action"),
        defaults={"reason": "value_mapping_on_non_categorical", "action": "ignored_mapping"},
    ),
    "MAP.VALUES.UNKNOWN_CANONICAL_VALUE": _fatal_infer(
        "MAP.VALUES.UNKNOWN_CANONICAL_VALUE",
        "value_correspondence para el campo {field!r} mapea hacia un valor canónico no definido en el dominio: {canonical_value!r}.",
        details_keys=("field", "canonical_value", "domain_values_sample", "domain_values_total", "action"),
        defaults={"action": "abort"},
    ),
    "DOM.POLICY.FIELD_NOT_EXTENDABLE": _warn(
        "DOM.POLICY.FIELD_NOT_EXTENDABLE",
        "El campo {field!r} no admite extensión de dominio (extendable=False); los valores fuera de dominio no se extenderán automáticamente.",
        details_keys=("field", "strict_domains", "domain_extendable", "action"),
        defaults={"action": "keep_or_map_without_extension"},
    ),
    "DOM.POLICY.MAPPING_REQUIRES_EXTENSION_BLOCKED": _err(
        "DOM.POLICY.MAPPING_REQUIRES_EXTENSION_BLOCKED",
        "La normalización de valores para {field!r} requiere extender el dominio, pero la política vigente lo prohíbe.",
        details_keys=("field", "strict_domains", "domain_extendable", "reason", "unmapped_examples", "unmapped_count", "action"),
        defaults={"reason": "extension_required_but_disallowed", "action": "raise_or_keep_evidence"},
        exception="inference",
        fatal=False,
    ),
    "DOM.STRICT.OUT_OF_DOMAIN_ABORT": _err(
        "DOM.STRICT.OUT_OF_DOMAIN_ABORT",
        "Se detectaron valores fuera de dominio en {field!r} con strict_domains=True; la inferencia no puede cerrar ese campo de salida consistentemente.",
        details_keys=("field", "unknown_count", "total_count", "unknown_rate", "unknown_examples", "policy", "action"),
        defaults={"policy": "strict_domains", "action": "abort"},
        exception="inference",
        fatal=False,
    ),
    "DOM.EXTENSION.APPLIED": _info(
        "DOM.EXTENSION.APPLIED",
        "Se extendió el dominio de {field!r} con {n_added} valores nuevos durante el cierre del output.",
        details_keys=("field", "n_added", "added_values_sample", "added_values_total", "policy", "action"),
        defaults={"action": "extended_domain"},
    ),
    "DOM.INFERENCE.APPLIED": _info(
        "DOM.INFERENCE.APPLIED",
        "Se infirió el dominio efectivo de {field!r} a partir de los valores observados ({n_unique_observed} únicos sobre {n_rows_non_null} filas no nulas); el campo se mantiene como categórico.",
        details_keys=(
            "field",
            "n_rows_non_null",
            "n_unique_observed",
            "alpha",
            "k_max",
            "cardinality_limit",
            "observed_values_sample",
            "observed_values_total",
            "action",
        ),
        defaults={"action": "inferred_categorical_domain"},
    ),
    "DOM.INFERENCE.DEGRADED_TO_STRING": _warn(
        "DOM.INFERENCE.DEGRADED_TO_STRING",
        "El campo {field!r} fue declarado categórico con DomainSpec.values vacío, pero su cardinalidad observada ({n_unique_observed} únicos sobre {n_rows_non_null} filas no nulas) supera el límite {cardinality_limit}; se degradará a texto.",
        details_keys=(
            "field",
            "n_rows_non_null",
            "n_unique_observed",
            "alpha",
            "k_max",
            "cardinality_limit",
            "observed_values_sample",
            "observed_values_total",
            "fallback_dtype",
            "reason",
            "action",
        ),
        defaults={
            "fallback_dtype": "string",
            "reason": "high_cardinality_for_categorical_inference",
            "action": "fallback_dtype",
        },
    ),

    # ------------------------------------------------------------------
    # PROVENANCE / CLOSURE / SUMMARY
    # ------------------------------------------------------------------
    "PROV.INPUT.INVALID_USER_PROVENANCE": _fatal_infer(
        "PROV.INPUT.INVALID_USER_PROVENANCE",
        "El bloque provenance entregado a infer_trips_from_traces no es válido o no es serializable.",
        details_keys=_PROVENANCE_DETAILS,
        defaults={"field": "provenance", "expected": "JSON-safe dict or degradable mapping", "action": "abort"},
    ),
    "PROV.DERIVED_FROM_BUILD_FAILED": _warn(
        "PROV.DERIVED_FROM_BUILD_FAILED",
        "No fue posible construir provenance['derived_from'] completo; se continuará con trazabilidad parcial.",
        details_keys=_PROVENANCE_DETAILS,
        defaults={"field": "derived_from", "action": "continue_with_partial_provenance"},
    ),
    "INF.OK.SUMMARY": _info(
        "INF.OK.SUMMARY",
        "Se infirieron {n_trips_out} viajes a partir de {n_points_in} puntos usando infer_mode={infer_mode!r}.",
        details_keys=(*_REQUEST_DETAILS, *_INPUT_COUNTS, "dropped_by_reason", "action"),
        defaults={"action": "return_trip_dataset"},
    ),
}
