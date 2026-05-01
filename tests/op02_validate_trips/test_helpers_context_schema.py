import json

import pandas as pd

from pylondrina.schema import DomainSpec, FieldSpec, TripSchema, TripSchemaEffective
from pylondrina.validation import (
    ValidationOptions,
    _build_effective_domains_by_field,
    _build_effective_nullable_by_field,
    _build_temporal_context,
    _constraint_params_are_valid,
    _describe_received_params,
    _extract_domain_values,
    _options_to_event_parameters,
)


# ---------------------------------------------------------------------
# Helpers locales de test
# ---------------------------------------------------------------------


def assert_json_safe(obj, label: str = "object") -> None:
    try:
        json.dumps(obj, default=str)
    except Exception as exc:
        raise AssertionError(f"{label} no es JSON-safe: {exc}") from exc


def make_field(
    name: str,
    dtype: str,
    *,
    required: bool = False,
    constraints: dict | None = None,
    domain: DomainSpec | None = None,
) -> FieldSpec:
    return FieldSpec(
        name=name,
        dtype=dtype,
        required=required,
        constraints=constraints,
        domain=domain,
    )


def make_trip_schema(fields: list[FieldSpec], *, version: str = "1.1") -> TripSchema:
    return TripSchema(
        version=version,
        fields={field.name: field for field in fields},
        required=[field.name for field in fields if field.required],
        semantic_rules=None,
    )


# ---------------------------------------------------------------------
# Tests de _build_effective_nullable_by_field
# ---------------------------------------------------------------------


def test_build_effective_nullable_by_field_uses_required_when_nullable_constraint_is_absent():
    """Verifica que required=True implique no-nullable y required=False implique nullable cuando no hay constraint nullable."""
    schema = make_trip_schema(
        [
            make_field("movement_id", "string", required=True),
            make_field("mode", "categorical", required=False),
        ]
    )

    nullable_by_field = _build_effective_nullable_by_field(schema)

    assert nullable_by_field["movement_id"] is False
    assert nullable_by_field["mode"] is True


def test_build_effective_nullable_by_field_nullable_constraint_overrides_required_default():
    """Verifica que constraints['nullable'] tenga precedencia sobre la regla derivada desde required."""
    schema = make_trip_schema(
        [
            make_field("purpose", "categorical", required=False, constraints={"nullable": False}),
            make_field("comment", "string", required=True, constraints={"nullable": False}),
        ]
    )

    nullable_by_field = _build_effective_nullable_by_field(schema)

    assert nullable_by_field["purpose"] is False
    assert nullable_by_field["comment"] is False


def test_build_effective_nullable_by_field_matches_notebook_combined_case():
    """Verifica el caso combinado del notebook: required, optional y nullable explícito en un mismo schema."""
    schema = make_trip_schema(
        [
            make_field("movement_id", "string", required=True),
            make_field("mode", "categorical", required=False),
            make_field("purpose", "categorical", required=False, constraints={"nullable": False}),
            make_field("comment", "string", required=True, constraints={"nullable": False}),
        ]
    )

    nullable_by_field = _build_effective_nullable_by_field(schema)

    assert nullable_by_field == {
        "movement_id": False,
        "mode": True,
        "purpose": False,
        "comment": False,
    }


# ---------------------------------------------------------------------
# Tests de _constraint_params_are_valid y _describe_received_params
# ---------------------------------------------------------------------


def test_constraint_params_are_valid_accepts_well_formed_payloads():
    """Verifica que constraints conocidas acepten payloads bien formados según su contrato vigente."""
    assert _constraint_params_are_valid("nullable", True) is True
    assert _constraint_params_are_valid("pattern", r"^[A-Z]+$") is True
    assert _constraint_params_are_valid("range", {"min": 0, "max": 10}) is True
    assert _constraint_params_are_valid("length", {"min": 2, "max": 8}) is True
    assert _constraint_params_are_valid("datetime", {"allow_naive": False, "min": "2026-01-01"}) is True
    assert _constraint_params_are_valid("h3", {"require_valid": True, "resolution": 8}) is True
    assert _constraint_params_are_valid("unique", True) is True


def test_constraint_params_are_valid_rejects_malformed_payloads():
    """Verifica que constraints conocidas rechacen payloads mal formados para que luego puedan omitirse con warning."""
    assert _constraint_params_are_valid("nullable", {"value": True}) is False
    assert _constraint_params_are_valid("range", {"minimum": 0}) is False
    assert _constraint_params_are_valid("length", {"min": "a"}) is False
    assert _constraint_params_are_valid("datetime", {"allow_naive": "no"}) is False
    assert _constraint_params_are_valid("h3", {"resolution": "8"}) is False
    assert _constraint_params_are_valid("unique", "yes") is False


def test_constraint_params_are_valid_rejects_unknown_constraint_name():
    """Verifica que un nombre de constraint no reconocido no sea considerado payload válido."""
    assert _constraint_params_are_valid("unknown_constraint", True) is False


def test_describe_received_params_reports_mapping_keys_or_value_type():
    """Verifica que la descripción de parámetros recibidos entregue keys para mappings y tipo para escalares."""
    assert _describe_received_params({"min": 0, "max": 10}) == ["min", "max"]
    assert _describe_received_params(True) == "bool"
    assert _describe_received_params("abc") == "str"


# ---------------------------------------------------------------------
# Tests de _extract_domain_values
# ---------------------------------------------------------------------


def test_extract_domain_values_from_simple_sequence():
    """Verifica que un dominio simple como lista/tupla/set se extraiga como set de strings."""
    assert _extract_domain_values(["walk", "bus", "metro"]) == {"walk", "bus", "metro"}
    assert _extract_domain_values(("walk", "bus")) == {"walk", "bus"}
    assert _extract_domain_values({"walk", "bus"}) == {"walk", "bus"}


def test_extract_domain_values_from_rich_domain_dict():
    """Verifica que un dominio rico se extraiga desde la clave values sin depender de otros metadatos del dominio."""
    rich_domain = {
        "values": ["walk", "bus", "metro", "unknown"],
        "extendable": True,
        "extended": False,
        "added_values": [],
    }

    assert _extract_domain_values(rich_domain) == {"walk", "bus", "metro", "unknown"}


def test_extract_domain_values_returns_none_when_domain_is_missing_or_not_interpretable():
    """Verifica que dominios ausentes o sin clave values no produzcan un dominio efectivo inventado."""
    assert _extract_domain_values({"extendable": True}) is None
    assert _extract_domain_values(None) is None
    assert _extract_domain_values("walk,bus,metro") is None


# ---------------------------------------------------------------------
# Tests de _build_effective_domains_by_field
# ---------------------------------------------------------------------


def test_build_effective_domains_by_field_uses_schema_effective_before_metadata_and_schema():
    """Verifica que schema_effective tenga precedencia sobre metadata y schema base para dominios efectivos."""
    schema = make_trip_schema(
        [
            make_field(
                "mode",
                "categorical",
                domain=DomainSpec(values=["walk", "bus", "metro"]),
            ),
            make_field(
                "purpose",
                "categorical",
                domain=DomainSpec(values=["work", "study"]),
            ),
            make_field("user_id", "string", required=True),
        ]
    )

    schema_effective = TripSchemaEffective(
        domains_effective={
            "mode": {
                "values": ["walk", "bus", "metro", "bike"],
                "extendable": True,
                "extended": True,
                "added_values": ["bike"],
            }
        }
    )

    metadata = {
        "domains_effective": {
            "mode": {"values": ["walk", "bus"]},
            "purpose": {"values": ["work", "study", "health"]},
        }
    }

    effective_domains = _build_effective_domains_by_field(
        schema=schema,
        schema_effective=schema_effective,
        metadata=metadata,
    )

    assert effective_domains["mode"] == {"walk", "bus", "metro", "bike"}
    assert effective_domains["purpose"] == {"work", "study", "health"}
    assert "user_id" not in effective_domains


def test_build_effective_domains_by_field_uses_schema_base_when_no_effective_domain_exists():
    """Verifica que el dominio base del schema se use como fallback cuando no hay schema_effective ni metadata."""
    schema = make_trip_schema(
        [
            make_field(
                "mode",
                "categorical",
                domain=DomainSpec(values=["walk", "bus", "metro"]),
            ),
            make_field("user_id", "string", required=True),
        ]
    )

    effective_domains = _build_effective_domains_by_field(
        schema=schema,
        schema_effective=TripSchemaEffective(),
        metadata={},
    )

    assert effective_domains["mode"] == {"walk", "bus", "metro"}
    assert "user_id" not in effective_domains


def test_build_effective_domains_by_field_returns_none_for_categorical_without_domain_info():
    """Verifica que un campo categórico sin dominio interpretable quede explícitamente sin dominio efectivo."""
    schema = make_trip_schema(
        [
            make_field("mode", "categorical", domain=None),
            make_field("user_id", "string", required=True),
        ]
    )

    effective_domains = _build_effective_domains_by_field(
        schema=schema,
        schema_effective=TripSchemaEffective(),
        metadata={},
    )

    assert effective_domains["mode"] is None
    assert "user_id" not in effective_domains


# ---------------------------------------------------------------------
# Tests de _build_temporal_context
# ---------------------------------------------------------------------


def test_build_temporal_context_uses_schema_effective_before_metadata_and_column_inference():
    """Verifica que schema_effective.temporal tenga precedencia sobre metadata temporal e inferencia por columnas."""
    df = pd.DataFrame(
        {
            "origin_time_utc": ["origin_t"],
            "destination_time_utc": ["destination_t"],
        }
    )

    context = _build_temporal_context(
        df=df,
        metadata={"temporal": {"tier": "tier_2"}},
        schema_effective=TripSchemaEffective(temporal={"tier": "tier_1"}),
    )

    assert context["tier"] == "tier_1"
    assert context["fields_present"] == ["origin_time_utc", "destination_time_utc"]


def test_build_temporal_context_uses_metadata_when_schema_effective_has_no_temporal_tier():
    """Verifica que metadata['temporal']['tier'] se use cuando schema_effective no declara tier temporal."""
    df = pd.DataFrame(
        {
            "origin_time_utc": ["origin_t"],
            "destination_time_utc": ["destination_t"],
        }
    )

    context = _build_temporal_context(
        df=df,
        metadata={"temporal": {"tier": "tier_2"}},
        schema_effective=TripSchemaEffective(),
    )

    assert context["tier"] == "tier_2"
    assert context["fields_present"] == ["origin_time_utc", "destination_time_utc"]


def test_build_temporal_context_infers_tier_1_from_utc_columns():
    """Verifica que el contexto temporal infiera tier_1 cuando existen origin_time_utc y destination_time_utc."""
    df = pd.DataFrame(
        {
            "origin_time_utc": ["origin_t"],
            "destination_time_utc": ["destination_t"],
        }
    )

    context = _build_temporal_context(
        df=df,
        metadata={},
        schema_effective=TripSchemaEffective(),
    )

    assert context["tier"] == "tier_1"
    assert context["fields_present"] == ["origin_time_utc", "destination_time_utc"]


def test_build_temporal_context_infers_tier_2_from_hhmm_columns():
    """Verifica que el contexto temporal infiera tier_2 cuando solo existen columnas HH:MM locales."""
    df = pd.DataFrame(
        {
            "origin_time_local_hhmm": ["origin_hhmm"],
            "destination_time_local_hhmm": ["destination_hhmm"],
        }
    )

    context = _build_temporal_context(
        df=df,
        metadata={},
        schema_effective=TripSchemaEffective(),
    )

    assert context["tier"] == "tier_2"
    assert context["fields_present"] == []


def test_build_temporal_context_infers_tier_3_when_no_od_temporal_columns_exist():
    """Verifica que el contexto temporal infiera tier_3 cuando no hay columnas temporales OD reconocidas."""
    df = pd.DataFrame({"user_id": ["u1"]})

    context = _build_temporal_context(
        df=df,
        metadata={},
        schema_effective=TripSchemaEffective(),
    )

    assert context["tier"] == "tier_3"
    assert context["fields_present"] == []


def test_build_temporal_context_fields_present_only_reports_utc_od_fields():
    """Verifica que fields_present solo reporte origin_time_utc y destination_time_utc, no columnas HH:MM."""
    df = pd.DataFrame(
        {
            "origin_time_utc": ["origin_t"],
            "destination_time_utc": ["destination_t"],
            "origin_time_local_hhmm": ["origin_hhmm"],
            "destination_time_local_hhmm": ["destination_hhmm"],
        }
    )

    context = _build_temporal_context(
        df=df,
        metadata={},
        schema_effective=TripSchemaEffective(),
    )

    assert context["tier"] == "tier_1"
    assert context["fields_present"] == ["origin_time_utc", "destination_time_utc"]


# ---------------------------------------------------------------------
# Tests de _options_to_event_parameters
# ---------------------------------------------------------------------


def test_options_to_event_parameters_serializes_duplicates_subset_as_list():
    """Verifica que duplicates_subset se convierta de tuple a list para quedar JSON-safe en el evento."""
    options = ValidationOptions(
        strict=True,
        validate_duplicates=True,
        duplicates_subset=("user_id", "origin_time_utc"),
        allow_partial_od_spatial=True,
    )

    parameters = _options_to_event_parameters(options)

    assert parameters["strict"] is True
    assert parameters["validate_duplicates"] is True
    assert parameters["duplicates_subset"] == ["user_id", "origin_time_utc"]
    assert parameters["allow_partial_od_spatial"] is True
    assert_json_safe(parameters, "event_parameters")


def test_options_to_event_parameters_preserves_none_duplicates_subset():
    """Verifica que duplicates_subset=None se preserve como None cuando no hay check de duplicados configurado."""
    options = ValidationOptions(
        validate_duplicates=False,
        duplicates_subset=None,
    )

    parameters = _options_to_event_parameters(options)

    assert parameters["validate_duplicates"] is False
    assert parameters["duplicates_subset"] is None
    assert_json_safe(parameters, "event_parameters_without_duplicates")


def test_options_to_event_parameters_contains_stable_validation_option_keys():
    """Verifica que los parámetros serializados incluyan las opciones públicas estables de ValidationOptions."""
    options = ValidationOptions()

    parameters = _options_to_event_parameters(options)

    assert set(parameters.keys()) == {
        "strict",
        "max_issues",
        "sample_rows_per_issue",
        "validate_required_fields",
        "validate_types_and_formats",
        "validate_constraints",
        "validate_domains",
        "domains_sample_frac",
        "domains_min_in_domain_ratio",
        "validate_temporal_consistency",
        "validate_duplicates",
        "duplicates_subset",
        "allow_partial_od_spatial",
    }
    assert_json_safe(parameters, "default_event_parameters")