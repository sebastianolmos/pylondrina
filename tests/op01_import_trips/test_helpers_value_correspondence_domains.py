import json

import pandas as pd
import pytest

from pylondrina.errors import ImportError as PylondrinaImportError
from pylondrina.importing import (
    ImportOptions,
    _apply_value_correspondence,
    _standardize_categorical_values,
)
from pylondrina.schema import DomainSpec, FieldSpec, TripSchema, TripSchemaEffective


# ---------------------------------------------------------------------
# Helpers locales de test
# ---------------------------------------------------------------------


def assert_json_safe(obj, label: str = "object") -> None:
    try:
        json.dumps(obj, default=str)
    except Exception as exc:
        raise AssertionError(f"{label} no es JSON-safe: {exc}") from exc


def issue_codes(issues) -> list[str]:
    return [issue.code for issue in issues]


def assert_issue_present(issues, code: str) -> None:
    codes = issue_codes(issues)
    assert code in codes, f"No se encontró issue {code!r}. Issues encontrados: {codes!r}"


def assert_issue_absent(issues, code: str) -> None:
    codes = issue_codes(issues)
    assert code not in codes, f"No se esperaba issue {code!r}. Issues encontrados: {codes!r}"


def get_issue(issues, code: str):
    for issue in issues:
        if issue.code == code:
            return issue
    raise AssertionError(f"No se encontró issue {code!r}. Issues encontrados: {issue_codes(issues)!r}")


def assert_list_with_na(actual, expected) -> None:
    assert len(actual) == len(expected)

    for actual_value, expected_value in zip(actual, expected):
        if pd.isna(expected_value):
            assert pd.isna(actual_value)
        else:
            assert actual_value == expected_value


# ---------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------


@pytest.fixture
def schema_g5() -> TripSchema:
    return TripSchema(
        version="0.1.0",
        fields={
            "mode": FieldSpec(
                name="mode",
                dtype="categorical",
                required=False,
                domain=DomainSpec(
                    values=["car", "bus", "unknown"],
                    extendable=False,
                    aliases=None,
                ),
            ),
            "purpose": FieldSpec(
                name="purpose",
                dtype="categorical",
                required=False,
                domain=DomainSpec(
                    values=["work", "study"],
                    extendable=True,
                    aliases=None,
                ),
            ),
            "bootstrap_cat": FieldSpec(
                name="bootstrap_cat",
                dtype="categorical",
                required=False,
                domain=DomainSpec(
                    values=[],
                    extendable=True,
                    aliases=None,
                ),
            ),
            "source_label": FieldSpec(
                name="source_label",
                dtype="string",
                required=False,
                domain=None,
            ),
        },
        required=[],
        semantic_rules=None,
    )


@pytest.fixture
def schema_effective_g5() -> TripSchemaEffective:
    return TripSchemaEffective(
        dtype_effective={
            "mode": "categorical",
            "purpose": "categorical",
            "bootstrap_cat": "categorical",
            "source_label": "string",
        },
        overrides={},
        domains_effective={},
        temporal={},
        fields_effective=[],
    )


@pytest.fixture
def df_g5() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "mode": ["auto", "bus", "taxi", pd.NA],
            "purpose": ["trabajo", "study", "funeral", pd.NA],
            "bootstrap_cat": ["alpha", "beta", pd.NA, "alpha"],
            "source_label": ["X", "Y", "Z", "W"],
        }
    )


@pytest.fixture
def target_schema_fields_all() -> set[str]:
    return {"mode", "purpose", "bootstrap_cat", "source_label"}


def make_schema_effective_copy(schema_effective: TripSchemaEffective) -> TripSchemaEffective:
    return TripSchemaEffective(
        dtype_effective=dict(schema_effective.dtype_effective),
        overrides={},
        domains_effective={},
        temporal={},
        fields_effective=[],
    )


# ---------------------------------------------------------------------
# Tests de _apply_value_correspondence
# ---------------------------------------------------------------------


def test_apply_value_correspondence_applies_only_used_pairs():
    """Verifica que solo se registren y apliquen pares de correspondencia observados en la serie."""
    s_base = pd.Series(["auto", "bus", "auto", pd.NA, "taxi"], dtype="string")

    s_out, used_pairs = _apply_value_correspondence(
        s_base,
        {"auto": "car", "bike": "bicycle"},
    )

    assert_list_with_na(s_out.tolist(), ["car", "bus", "car", pd.NA, "taxi"])
    assert used_pairs == {"auto": "car"}


def test_apply_value_correspondence_no_used_pairs_returns_original_series():
    """Verifica que un mapping sin valores observados no modifique la serie ni registre pares usados."""
    s_base = pd.Series(["auto", "bus", "auto", pd.NA, "taxi"], dtype="string")

    s_out, used_pairs = _apply_value_correspondence(
        s_base,
        {"bike": "bicycle", "train": "rail"},
    )

    assert s_out.equals(s_base)
    assert used_pairs == {}


def test_apply_value_correspondence_none_returns_original_series():
    """Verifica que value_correspondence=None sea passthrough y no registre mappings usados."""
    s_base = pd.Series(["auto", "bus", "auto", pd.NA, "taxi"], dtype="string")

    s_out, used_pairs = _apply_value_correspondence(s_base, None)

    assert s_out.equals(s_base)
    assert used_pairs == {}


# ---------------------------------------------------------------------
# Tests de _standardize_categorical_values
# ---------------------------------------------------------------------


def test_standardize_categorical_values_main_mapping_extension_and_degradation(
    schema_g5,
    schema_effective_g5,
    df_g5,
    target_schema_fields_all,
):
    """Verifica el caso principal: mappings usados, unknown para dominio cerrado, extensión de dominio y degradación de dominio vacío."""
    value_corr_main = {
        "mode": {
            "auto": "car",
        },
        "purpose": {
            "trabajo": "work",
        },
        "source_label": {
            "X": "foo",
        },
    }

    schema_effective = make_schema_effective_copy(schema_effective_g5)

    work, domains_eff, vc_applied, domains_extended, n_map, issues = (
        _standardize_categorical_values(
            df_g5.copy(deep=True),
            schema=schema_g5,
            schema_effective=schema_effective,
            value_correspondence=value_corr_main,
            options=ImportOptions(strict=False, strict_domains=False),
            target_schema_fields=target_schema_fields_all,
        )
    )

    assert_list_with_na(work["mode"].tolist(), ["car", "bus", "unknown", pd.NA])
    assert_list_with_na(work["purpose"].tolist(), ["work", "study", "funeral", pd.NA])
    assert_list_with_na(work["bootstrap_cat"].tolist(), ["alpha", "beta", pd.NA, "alpha"])
    assert work["source_label"].tolist() == ["X", "Y", "Z", "W"]

    assert vc_applied == {
        "mode": {"auto": "car"},
        "purpose": {"trabajo": "work"},
    }
    assert n_map == 2

    assert "mode" in domains_eff
    assert domains_eff["mode"]["extended"] is False
    assert domains_eff["mode"]["unknown_value"] == "unknown"
    assert domains_eff["mode"]["unknown_values"] == ["taxi"]
    assert domains_eff["mode"]["added_values"] == []
    assert domains_eff["mode"]["value_correspondence_applied"] == {"auto": "car"}

    assert "purpose" in domains_eff
    assert domains_eff["purpose"]["extended"] is True
    assert domains_eff["purpose"]["added_values"] == ["funeral"]
    assert domains_eff["purpose"]["value_correspondence_applied"] == {"trabajo": "work"}
    assert "funeral" in domains_eff["purpose"]["values"]

    # DomainSpec(values=[]) ya no se extiende automáticamente con pocos datos.
    # En la política vigente se degrada a string y no queda en domains_effective.
    assert "bootstrap_cat" not in domains_eff
    assert schema_effective.dtype_effective["bootstrap_cat"] == "string"
    assert "bootstrap_cat" in schema_effective.overrides
    assert (
        "categorical_inference_degraded_to_string_high_cardinality"
        in schema_effective.overrides["bootstrap_cat"]["reasons"]
    )

    assert set(domains_extended) == {"purpose"}

    assert_issue_present(issues, "DOM.POLICY.FIELD_NOT_EXTENDABLE")
    assert_issue_present(issues, "DOM.EXTENSION.APPLIED")
    assert_issue_present(issues, "MAP.VALUES.NON_CATEGORICAL_FIELD")
    assert_issue_present(issues, "DOM.INFERENCE.DEGRADED_TO_STRING")

    assert_json_safe(domains_eff, "domains_effective")


def test_standardize_categorical_values_unknown_canonical_field_raises(
    schema_g5,
    schema_effective_g5,
    df_g5,
    target_schema_fields_all,
):
    """Verifica que value_correspondence hacia un campo inexistente en el schema aborte la estandarización."""
    with pytest.raises(PylondrinaImportError) as exc_info:
        _standardize_categorical_values(
            df_g5.copy(deep=True),
            schema=schema_g5,
            schema_effective=make_schema_effective_copy(schema_effective_g5),
            value_correspondence={"fake_field": {"x": "y"}},
            options=ImportOptions(strict=False, strict_domains=False),
            target_schema_fields=target_schema_fields_all,
        )

    assert_issue_present(exc_info.value.issues, "MAP.VALUES.UNKNOWN_CANONICAL_FIELD")

    issue = exc_info.value.issue
    assert issue.code == "MAP.VALUES.UNKNOWN_CANONICAL_FIELD"
    assert issue.field == "fake_field"
    assert issue.details["action"] == "abort"


@pytest.mark.parametrize("strict", [False, True])
def test_standardize_categorical_values_unknown_canonical_value_always_raises(
    strict,
    schema_g5,
    schema_effective_g5,
    df_g5,
    target_schema_fields_all,
):
    """Verifica que mapear hacia un valor canónico fuera de un dominio cerrado aborte aunque strict=False."""
    value_corr_bad_canonical = {
        "mode": {
            "auto": "plane",
        }
    }

    with pytest.raises(PylondrinaImportError) as exc_info:
        _standardize_categorical_values(
            df_g5.copy(deep=True),
            schema=schema_g5,
            schema_effective=make_schema_effective_copy(schema_effective_g5),
            value_correspondence=value_corr_bad_canonical,
            options=ImportOptions(strict=strict, strict_domains=False),
            target_schema_fields=target_schema_fields_all,
        )

    assert_issue_present(exc_info.value.issues, "MAP.VALUES.UNKNOWN_CANONICAL_VALUE")

    issue = exc_info.value.issue
    assert issue.code == "MAP.VALUES.UNKNOWN_CANONICAL_VALUE"
    assert issue.field == "mode"
    assert issue.details["canonical_value"] == "plane"


@pytest.fixture
def schema_strict_domains() -> TripSchema:
    return TripSchema(
        version="0.1.0",
        fields={
            "purpose": FieldSpec(
                name="purpose",
                dtype="categorical",
                required=False,
                domain=DomainSpec(
                    values=["work", "study"],
                    extendable=True,
                    aliases=None,
                ),
            ),
        },
        required=[],
        semantic_rules=None,
    )


@pytest.fixture
def df_strict_domains() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "purpose": ["work", "funeral", pd.NA],
        }
    )


@pytest.mark.parametrize("strict", [False, True])
def test_standardize_categorical_values_strict_domains_out_of_domain_always_raises(
    strict,
    schema_strict_domains,
    df_strict_domains,
):
    """Verifica que strict_domains=True aborte frente a valores fuera de dominio incluso cuando strict=False."""
    with pytest.raises(PylondrinaImportError) as exc_info:
        _standardize_categorical_values(
            df_strict_domains.copy(deep=True),
            schema=schema_strict_domains,
            schema_effective=TripSchemaEffective(
                dtype_effective={"purpose": "categorical"},
                overrides={},
                domains_effective={},
                temporal={},
                fields_effective=[],
            ),
            value_correspondence=None,
            options=ImportOptions(strict=strict, strict_domains=True),
            target_schema_fields={"purpose"},
        )

    assert_issue_present(exc_info.value.issues, "DOM.STRICT.OUT_OF_DOMAIN_ABORT")

    issue = exc_info.value.issue
    assert issue.code == "DOM.STRICT.OUT_OF_DOMAIN_ABORT"
    assert issue.field == "purpose"
    assert issue.details["policy"] == "strict_domains"
    assert issue.details["action"] == "abort"


@pytest.mark.parametrize("strict", [False, True])
def test_standardize_categorical_values_mapping_requires_extension_blocked_always_raises(
    strict,
    schema_strict_domains,
):
    """Verifica que un mapping que requiere extender dominio aborte cuando strict_domains=True."""
    df = pd.DataFrame({"purpose": ["trabajo", "work"]})

    with pytest.raises(PylondrinaImportError) as exc_info:
        _standardize_categorical_values(
            df.copy(deep=True),
            schema=schema_strict_domains,
            schema_effective=TripSchemaEffective(
                dtype_effective={"purpose": "categorical"},
                overrides={},
                domains_effective={},
                temporal={},
                fields_effective=[],
            ),
            value_correspondence={"purpose": {"trabajo": "funeral"}},
            options=ImportOptions(strict=strict, strict_domains=True),
            target_schema_fields={"purpose"},
        )

    assert_issue_present(exc_info.value.issues, "DOM.POLICY.MAPPING_REQUIRES_EXTENSION_BLOCKED")

    issue = exc_info.value.issue
    assert issue.code == "DOM.POLICY.MAPPING_REQUIRES_EXTENSION_BLOCKED"
    assert issue.field == "purpose"
    assert issue.details["strict_domains"] is True
    assert issue.details["domain_extendable"] is True
    assert issue.details["unmapped_examples"] == ["funeral"]
    assert issue.details["action"] == "abort"


def test_standardize_categorical_values_target_schema_fields_limits_processed_fields(
    schema_g5,
    schema_effective_g5,
    df_g5,
):
    """Verifica que solo los categóricos incluidos en target_schema_fields sean procesados y registrados."""
    target_schema_fields_partial = {"mode"}

    value_corr_target = {
        "mode": {"auto": "car"},
        "purpose": {"trabajo": "work"},
    }

    work, domains_eff, vc_applied, domains_extended, n_map, issues = (
        _standardize_categorical_values(
            df_g5.copy(deep=True),
            schema=schema_g5,
            schema_effective=make_schema_effective_copy(schema_effective_g5),
            value_correspondence=value_corr_target,
            options=ImportOptions(strict=False, strict_domains=False),
            target_schema_fields=target_schema_fields_partial,
        )
    )

    assert_list_with_na(work["mode"].tolist(), ["car", "bus", "unknown", pd.NA])
    assert "mode" in domains_eff

    assert_list_with_na(work["purpose"].tolist(), ["trabajo", "study", "funeral", pd.NA])
    assert "purpose" not in domains_eff

    assert_list_with_na(work["bootstrap_cat"].tolist(), ["alpha", "beta", pd.NA, "alpha"])
    assert "bootstrap_cat" not in domains_eff

    assert vc_applied == {"mode": {"auto": "car"}}
    assert domains_extended == []
    assert n_map == 1

    assert_issue_present(issues, "DOM.POLICY.FIELD_NOT_EXTENDABLE")
    assert_issue_absent(issues, "DOM.EXTENSION.APPLIED")
    assert_issue_absent(issues, "DOM.INFERENCE.DEGRADED_TO_STRING")


def test_standardize_categorical_values_updates_schema_effective(
    schema_g5,
    schema_effective_g5,
    df_g5,
    target_schema_fields_all,
):
    """Verifica que domains_effective y overrides queden sincronizados en schema_effective."""
    schema_effective = make_schema_effective_copy(schema_effective_g5)

    work, domains_eff, vc_applied, domains_extended, n_map, issues = (
        _standardize_categorical_values(
            df_g5.copy(deep=True),
            schema=schema_g5,
            schema_effective=schema_effective,
            value_correspondence={
                "mode": {"auto": "car"},
                "purpose": {"trabajo": "work"},
            },
            options=ImportOptions(strict=False, strict_domains=False),
            target_schema_fields=target_schema_fields_all,
        )
    )

    assert set(schema_effective.domains_effective.keys()) == set(domains_eff.keys())
    assert "mode" in schema_effective.domains_effective
    assert "purpose" in schema_effective.domains_effective
    assert "bootstrap_cat" not in schema_effective.domains_effective

    assert "mode" in schema_effective.overrides
    assert "purpose" in schema_effective.overrides
    assert "bootstrap_cat" in schema_effective.overrides

    assert "out_of_domain_mapped_to_unknown" in schema_effective.overrides["mode"]["reasons"]
    assert "domain_extended" in schema_effective.overrides["purpose"]["reasons"]
    assert (
        "categorical_inference_degraded_to_string_high_cardinality"
        in schema_effective.overrides["bootstrap_cat"]["reasons"]
    )

    assert schema_effective.dtype_effective["bootstrap_cat"] == "string"

    assert_json_safe(schema_effective.to_dict(), "schema_effective")


def test_standardize_categorical_values_empty_domain_inference_applied():
    """Verifica que un dominio vacío se infiera cuando la cardinalidad observada cumple la regla alpha."""
    schema = TripSchema(
        version="0.1.0",
        fields={
            "bootstrap_cat": FieldSpec(
                name="bootstrap_cat",
                dtype="categorical",
                required=False,
                domain=DomainSpec(values=[], extendable=True, aliases=None),
            ),
        },
        required=[],
        semantic_rules=None,
    )

    df = pd.DataFrame({"bootstrap_cat": ["alpha", "beta"] * 20})
    schema_effective = TripSchemaEffective(
        dtype_effective={"bootstrap_cat": "categorical"},
        overrides={},
        domains_effective={},
        temporal={},
        fields_effective=[],
    )

    work, domains_eff, vc_applied, domains_extended, n_map, issues = (
        _standardize_categorical_values(
            df.copy(deep=True),
            schema=schema,
            schema_effective=schema_effective,
            value_correspondence={},
            options=ImportOptions(strict=False, strict_domains=False),
            target_schema_fields={"bootstrap_cat"},
        )
    )

    assert work["bootstrap_cat"].tolist() == ["alpha", "beta"] * 20

    assert "bootstrap_cat" in domains_eff
    bootstrap_domain = domains_eff["bootstrap_cat"]

    assert bootstrap_domain["inference_applied"] is True
    assert bootstrap_domain["extended"] is False
    assert bootstrap_domain["added_values"] == []
    assert bootstrap_domain["extended_values"] == []
    assert bootstrap_domain["unknown_values"] == []
    assert bootstrap_domain["n_unique_observed"] == 2
    assert set(bootstrap_domain["observed_values"]) == {"alpha", "beta"}
    assert set(bootstrap_domain["values"]) == {"alpha", "beta"}

    assert domains_extended == []
    assert vc_applied == {}
    assert n_map == 0

    assert "bootstrap_cat" in schema_effective.domains_effective
    assert schema_effective.dtype_effective["bootstrap_cat"] == "categorical"
    assert "bootstrap_cat" in schema_effective.overrides
    assert (
        "categorical_domain_inferred_from_observed_values"
        in schema_effective.overrides["bootstrap_cat"]["reasons"]
    )

    assert_issue_present(issues, "DOM.INFERENCE.APPLIED")
    assert_issue_absent(issues, "DOM.INFERENCE.DEGRADED_TO_STRING")

    assert_json_safe(domains_eff, "domains_effective_bootstrap_ok")
    assert_json_safe(schema_effective.to_dict(), "schema_effective_bootstrap_ok")


def test_standardize_categorical_values_empty_domain_degraded_to_string():
    """Verifica que un dominio vacío se degrade a string cuando la cardinalidad observada supera el límite de inferencia."""
    schema = TripSchema(
        version="0.1.0",
        fields={
            "bootstrap_cat": FieldSpec(
                name="bootstrap_cat",
                dtype="categorical",
                required=False,
                domain=DomainSpec(values=[], extendable=True, aliases=None),
            ),
        },
        required=[],
        semantic_rules=None,
    )

    df = pd.DataFrame({"bootstrap_cat": ["alpha", "beta", pd.NA, "alpha"]})
    schema_effective = TripSchemaEffective(
        dtype_effective={"bootstrap_cat": "categorical"},
        overrides={},
        domains_effective={},
        temporal={},
        fields_effective=[],
    )

    work, domains_eff, vc_applied, domains_extended, n_map, issues = (
        _standardize_categorical_values(
            df.copy(deep=True),
            schema=schema,
            schema_effective=schema_effective,
            value_correspondence={},
            options=ImportOptions(strict=False, strict_domains=False),
            target_schema_fields={"bootstrap_cat"},
        )
    )

    assert_list_with_na(work["bootstrap_cat"].tolist(), ["alpha", "beta", pd.NA, "alpha"])

    assert domains_eff == {}
    assert domains_extended == []
    assert vc_applied == {}
    assert n_map == 0

    assert schema_effective.dtype_effective["bootstrap_cat"] == "string"
    assert "bootstrap_cat" not in schema_effective.domains_effective
    assert "bootstrap_cat" in schema_effective.overrides
    assert (
        "categorical_inference_degraded_to_string_high_cardinality"
        in schema_effective.overrides["bootstrap_cat"]["reasons"]
    )
    assert schema_effective.overrides["bootstrap_cat"]["fallback_dtype"] == "string"
    assert schema_effective.overrides["bootstrap_cat"]["observed_values_total"] == 2

    assert_issue_present(issues, "DOM.INFERENCE.DEGRADED_TO_STRING")
    assert_issue_absent(issues, "DOM.INFERENCE.APPLIED")

    assert_json_safe(schema_effective.to_dict(), "schema_effective_bootstrap_degraded")