import pytest

from pylondrina.errors import ImportError as PylondrinaImportError
from pylondrina.errors import SchemaError
from pylondrina.issues.core import IssueSpec, emit_and_maybe_raise, emit_issue


OP01_SMOKE_ISSUES: dict[str, IssueSpec] = {
    "IMP.INPUT.EMPTY_DATAFRAME": IssueSpec(
        code="IMP.INPUT.EMPTY_DATAFRAME",
        level="warning",
        fatal=False,
        exception="import",
        message_template="El DataFrame de entrada no contiene filas; se importará un dataset vacío.",
        details_keys=("rows_in", "columns_in", "note"),
        defaults={
            "rows_in": 0,
            "note": "empty_input",
        },
    ),
    "SCH.TRIP_SCHEMA.EMPTY_FIELDS": IssueSpec(
        code="SCH.TRIP_SCHEMA.EMPTY_FIELDS",
        level="error",
        fatal=True,
        exception="schema",
        message_template=(
            "El TripSchema no define campos (fields está vacío); "
            "no es posible importar con un esquema sin catálogo de campos."
        ),
        details_keys=("schema_version", "fields_size", "note"),
        defaults={
            "fields_size": 0,
            "note": "no_fields_defined",
        },
    ),
    "SCH.CONSTRAINTS.UNKNOWN_RULE": IssueSpec(
        code="SCH.CONSTRAINTS.UNKNOWN_RULE",
        level="error",
        fatal=True,
        exception="schema",
        message_template=(
            "El campo {field!r} incluye una constraint no soportada ({rule!r}); "
            "la validación no puede ejecutarse de forma consistente."
        ),
        details_keys=("field", "rule", "supported_rules", "action"),
        defaults={
            "action": "abort",
        },
    ),
    "MAP.FIELDS.COLLISION_DUPLICATE_TARGET": IssueSpec(
        code="MAP.FIELDS.COLLISION_DUPLICATE_TARGET",
        level="error",
        fatal=True,
        exception="import",
        message_template=(
            "La correspondencia de campos produce colisión: "
            "múltiples canónicos apuntan a {source_column!r}."
        ),
        details_keys=("source_column", "canonical_fields", "field_correspondence", "action"),
        defaults={
            "action": "abort",
        },
    ),
    "DOM.POLICY.FIELD_NOT_EXTENDABLE": IssueSpec(
        code="DOM.POLICY.FIELD_NOT_EXTENDABLE",
        level="warning",
        fatal=False,
        exception="import",
        message_template=(
            "El campo {field!r} no admite extensión de dominio (extendable=False); "
            "los valores fuera de dominio no se incorporarán pese a que strict_domains=False."
        ),
        details_keys=("field", "strict_domains", "domain_extendable", "action"),
        defaults={
            "strict_domains": False,
            "domain_extendable": False,
            "action": "map_to_unknown",
        },
    ),
    "DOM.STRICT.OUT_OF_DOMAIN_ABORT": IssueSpec(
        code="DOM.STRICT.OUT_OF_DOMAIN_ABORT",
        level="error",
        fatal=True,
        exception="import",
        message_template=(
            "Se detectaron valores fuera de dominio en {field!r} "
            "con strict_domains=True; import abortado."
        ),
        details_keys=(
            "field",
            "unknown_count",
            "total_count",
            "unknown_rate",
            "unknown_examples",
            "policy",
            "action",
            "suggestion",
        ),
        defaults={
            "action": "abort",
        },
    ),
    "DOM.EXTENSION.APPLIED": IssueSpec(
        code="DOM.EXTENSION.APPLIED",
        level="info",
        fatal=False,
        exception="import",
        message_template="Se extendió el dominio de {field!r} con {n_added} valores nuevos.",
        details_keys=(
            "field",
            "n_added",
            "added_values_sample",
            "added_values_total",
            "policy",
            "action",
        ),
        defaults={
            "action": "extended_domain",
        },
    ),
    "IMP.TYPE.COERCE_PARTIAL": IssueSpec(
        code="IMP.TYPE.COERCE_PARTIAL",
        level="warning",
        fatal=False,
        exception="import",
        message_template=(
            "La conversión mínima del campo {field!r} falló en algunas filas "
            "({fail_rate:.1%}); se marcarán como nulos para validación posterior."
        ),
        details_keys=(
            "field",
            "dtype_expected",
            "parse_fail_count",
            "total_count",
            "fail_rate",
            "fallback",
            "action",
        ),
        defaults={
            "fallback": "set_null",
            "action": "continue",
        },
    ),
    "IMP.METADATA.DATASET_ID_CREATED": IssueSpec(
        code="IMP.METADATA.DATASET_ID_CREATED",
        level="info",
        fatal=False,
        exception="import",
        message_template="Se generó dataset_id para el dataset importado: {dataset_id!r}.",
        details_keys=("dataset_id", "generator", "stored_in"),
        defaults={
            "generator": "uuid4",
            "stored_in": ["TripDataset.metadata", "ImportReport.metadata"],
        },
    ),
    "IMP.INPUT.MISSING_REQUIRED_FIELD": IssueSpec(
        code="IMP.INPUT.MISSING_REQUIRED_FIELD",
        level="error",
        fatal=True,
        exception="import",
        message_template=(
            "Faltan campos obligatorios para importar según el TripSchema: "
            "{missing_required}."
        ),
        details_keys=(
            "missing_required",
            "required",
            "source_columns",
            "field_correspondence_keys",
            "field_correspondence_values_sample",
        ),
    ),
    "TST.ERROR.STRICT_ONLY": IssueSpec(
        code="TST.ERROR.STRICT_ONLY",
        level="error",
        fatal=False,
        exception="import",
        message_template="Error solo en strict: {reason}",
        details_keys=("reason",),
    ),
}


EXC_MAP_IMPORT = {
    "schema": SchemaError,
    "import": PylondrinaImportError,
}
DEFAULT_EXC = PylondrinaImportError


# ---------------------------------------------------------------------
# Grupo A: warnings/info no fatales con emit_issue
# ---------------------------------------------------------------------


def test_emit_issue_empty_dataframe_warning_does_not_abort():
    """Verifica que el issue de DataFrame vacío se emita como warning no fatal con details mínimos."""
    issues = []

    issue = emit_issue(
        issues,
        OP01_SMOKE_ISSUES,
        "IMP.INPUT.EMPTY_DATAFRAME",
        rows_in=0,
        columns_in=["a", "b"],
    )

    assert issue.code == "IMP.INPUT.EMPTY_DATAFRAME"
    assert issue.level == "warning"
    assert issue.details["rows_in"] == 0
    assert issue.details["columns_in"] == ["a", "b"]
    assert issue.details["note"] == "empty_input"
    assert issue is issues[-1]
    assert len(issues) == 1


def test_emit_issue_non_extendable_domain_warning_records_policy():
    """Verifica que un dominio no extendible quede trazado como warning con action map_to_unknown."""
    issues = []

    issue = emit_issue(
        issues,
        OP01_SMOKE_ISSUES,
        "DOM.POLICY.FIELD_NOT_EXTENDABLE",
        field="mode",
        strict_domains=False,
        domain_extendable=False,
        action="map_to_unknown",
        row_count=12,
    )

    assert issue.code == "DOM.POLICY.FIELD_NOT_EXTENDABLE"
    assert issue.level == "warning"
    assert issue.field == "mode"
    assert issue.row_count == 12
    assert issue.details["strict_domains"] is False
    assert issue.details["domain_extendable"] is False
    assert issue.details["action"] == "map_to_unknown"
    assert issue is issues[-1]


def test_emit_issue_domain_extension_is_info():
    """Verifica que la extensión de dominio se registre como issue informativo de auditoría."""
    issues = []

    issue = emit_issue(
        issues,
        OP01_SMOKE_ISSUES,
        "DOM.EXTENSION.APPLIED",
        field="mode",
        n_added=1,
    )

    assert issue.code == "DOM.EXTENSION.APPLIED"
    assert issue.level == "info"
    assert issue.field == "mode"
    assert issue.details["n_added"] == 1
    assert issue.details["action"] == "extended_domain"
    assert issue is issues[-1]


def test_emit_issue_partial_coercion_warning_formats_fail_rate():
    """Verifica que la coerción parcial registre warning y formatee correctamente el porcentaje en el mensaje."""
    issues = []
    fail_rate = 3 / 24

    issue = emit_issue(
        issues,
        OP01_SMOKE_ISSUES,
        "IMP.TYPE.COERCE_PARTIAL",
        field="origin_time_utc",
        dtype_expected="datetime",
        parse_fail_count=3,
        total_count=24,
        fail_rate=fail_rate,
        fallback="set_null",
        action="continue",
    )

    assert issue.code == "IMP.TYPE.COERCE_PARTIAL"
    assert issue.level == "warning"
    assert issue.field == "origin_time_utc"
    assert "12.5%" in issue.message
    assert issue.details["parse_fail_count"] == 3
    assert issue.details["total_count"] == 24
    assert issue.details["fail_rate"] == pytest.approx(fail_rate)
    assert issue.details["fallback"] == "set_null"
    assert issue.details["action"] == "continue"
    assert issue is issues[-1]


def test_emit_and_maybe_raise_warning_and_info_do_not_raise():
    """Verifica que warning/info no fatales se acumulen sin levantar excepción."""
    issues = []

    issue_warning = emit_and_maybe_raise(
        issues,
        OP01_SMOKE_ISSUES,
        "IMP.INPUT.EMPTY_DATAFRAME",
        strict=False,
        exception_map=EXC_MAP_IMPORT,
        default_exception=DEFAULT_EXC,
        rows_in=0,
        columns_in=[],
    )

    assert issue_warning.level == "warning"
    assert issue_warning is issues[-1]
    assert len(issues) == 1

    issue_info = emit_and_maybe_raise(
        issues,
        OP01_SMOKE_ISSUES,
        "IMP.METADATA.DATASET_ID_CREATED",
        strict=False,
        exception_map=EXC_MAP_IMPORT,
        default_exception=DEFAULT_EXC,
        dataset_id="ds_001",
    )

    assert issue_info.level == "info"
    assert issue_info is issues[-1]
    assert len(issues) == 2


# ---------------------------------------------------------------------
# Grupo B: errores fatales con emit_and_maybe_raise
# ---------------------------------------------------------------------


def test_emit_and_maybe_raise_empty_schema_fields_raises_schema_error():
    """Verifica que schema sin fields emita issue y levante SchemaError con snapshot de issues."""
    issues = []

    with pytest.raises(SchemaError) as exc_info:
        emit_and_maybe_raise(
            issues,
            OP01_SMOKE_ISSUES,
            "SCH.TRIP_SCHEMA.EMPTY_FIELDS",
            strict=False,
            exception_map=EXC_MAP_IMPORT,
            default_exception=DEFAULT_EXC,
            schema_version="1.1",
            fields_size=0,
        )

    exc = exc_info.value
    assert exc.code == "SCH.TRIP_SCHEMA.EMPTY_FIELDS"
    assert exc.issue is issues[-1]
    assert exc.issue.level == "error"
    assert exc.issue.details["schema_version"] == "1.1"
    assert exc.issue.details["fields_size"] == 0
    assert exc.issue.details["note"] == "no_fields_defined"
    assert len(exc.issues) == 1


def test_emit_and_maybe_raise_unknown_constraint_raises_schema_error():
    """Verifica que una constraint desconocida emita SchemaError con field, rule y supported_rules."""
    issues = []

    with pytest.raises(SchemaError) as exc_info:
        emit_and_maybe_raise(
            issues,
            OP01_SMOKE_ISSUES,
            "SCH.CONSTRAINTS.UNKNOWN_RULE",
            strict=False,
            exception_map=EXC_MAP_IMPORT,
            default_exception=DEFAULT_EXC,
            field="origin_latitude",
            rule="h3",
            supported_rules=["nullable", "range", "pattern"],
        )

    exc = exc_info.value
    assert exc.code == "SCH.CONSTRAINTS.UNKNOWN_RULE"
    assert exc.issue is issues[-1]
    assert "origin_latitude" in exc.issue.message
    assert "h3" in exc.issue.message
    assert exc.issue.details["field"] == "origin_latitude"
    assert exc.issue.details["rule"] == "h3"
    assert isinstance(exc.issue.details["supported_rules"], list)
    assert exc.issue.details["action"] == "abort"
    assert len(exc.issues) == 1


def test_emit_and_maybe_raise_mapping_collision_raises_import_error():
    """Verifica que una colisión de mapping se clasifique como ImportError fatal."""
    issues = []
    field_correspondence = {
        "origin_latitude": "lat_o",
        "destination_latitude": "lat_o",
    }

    with pytest.raises(PylondrinaImportError) as exc_info:
        emit_and_maybe_raise(
            issues,
            OP01_SMOKE_ISSUES,
            "MAP.FIELDS.COLLISION_DUPLICATE_TARGET",
            strict=False,
            exception_map=EXC_MAP_IMPORT,
            default_exception=DEFAULT_EXC,
            source_column="lat_o",
            canonical_fields=["origin_latitude", "destination_latitude"],
            field_correspondence=field_correspondence,
        )

    exc = exc_info.value
    assert exc.code == "MAP.FIELDS.COLLISION_DUPLICATE_TARGET"
    assert exc.issue is issues[-1]
    assert exc.issue.details["source_column"] == "lat_o"
    assert exc.issue.details["canonical_fields"] == [
        "origin_latitude",
        "destination_latitude",
    ]
    assert exc.issue.details["field_correspondence"] == field_correspondence
    assert exc.issue.details["action"] == "abort"
    assert len(exc.issues) == 1


def test_emit_and_maybe_raise_strict_domain_abort_raises_import_error():
    """Verifica que strict_domains con fuera de dominio levante ImportError y conserve policy/action."""
    issues = []

    with pytest.raises(PylondrinaImportError) as exc_info:
        emit_and_maybe_raise(
            issues,
            OP01_SMOKE_ISSUES,
            "DOM.STRICT.OUT_OF_DOMAIN_ABORT",
            strict=False,
            exception_map=EXC_MAP_IMPORT,
            default_exception=DEFAULT_EXC,
            field="mode",
            unknown_count=5,
            total_count=100,
            unknown_rate=0.05,
            unknown_examples=["taxi", "moto"],
            policy={"strict_domains": True, "domain_extendable": True},
            action="abort",
        )

    exc = exc_info.value
    assert exc.code == "DOM.STRICT.OUT_OF_DOMAIN_ABORT"
    assert exc.issue is issues[-1]
    assert exc.issue.field == "mode"
    assert exc.issue.details["action"] == "abort"
    assert exc.issue.details["policy"]["strict_domains"] is True
    assert len(exc.issues) == 1


def test_emit_and_maybe_raise_fatal_errors_always_raise():
    """Verifica smoke mínimo de errores fatales: schema vacío y missing required abortan aunque strict=False."""
    schema_issues = []

    with pytest.raises(SchemaError) as schema_exc_info:
        emit_and_maybe_raise(
            schema_issues,
            OP01_SMOKE_ISSUES,
            "SCH.TRIP_SCHEMA.EMPTY_FIELDS",
            strict=False,
            exception_map=EXC_MAP_IMPORT,
            default_exception=DEFAULT_EXC,
            schema_version="1.1",
            fields_size=0,
        )

    assert schema_exc_info.value.code == "SCH.TRIP_SCHEMA.EMPTY_FIELDS"
    assert schema_exc_info.value.issue is schema_issues[-1]
    assert len(schema_exc_info.value.issues) == 1

    import_issues = []

    with pytest.raises(PylondrinaImportError) as import_exc_info:
        emit_and_maybe_raise(
            import_issues,
            OP01_SMOKE_ISSUES,
            "IMP.INPUT.MISSING_REQUIRED_FIELD",
            strict=False,
            exception_map=EXC_MAP_IMPORT,
            default_exception=DEFAULT_EXC,
            missing_required=["user_id", "origin_latitude"],
            action="abort",
        )

    assert import_exc_info.value.code == "IMP.INPUT.MISSING_REQUIRED_FIELD"
    assert import_exc_info.value.issue is import_issues[-1]
    assert len(import_exc_info.value.issues) == 1


# ---------------------------------------------------------------------
# Grupo C: error no fatal que depende de strict
# ---------------------------------------------------------------------


def test_emit_and_maybe_raise_error_nonfatal_does_not_raise_when_strict_false():
    """Verifica que un issue level=error pero fatal=False no lance excepción con strict=False."""
    issues = []

    issue = emit_and_maybe_raise(
        issues,
        OP01_SMOKE_ISSUES,
        "TST.ERROR.STRICT_ONLY",
        strict=False,
        exception_map=EXC_MAP_IMPORT,
        default_exception=DEFAULT_EXC,
        reason="algo malo pero tolerable",
    )

    assert issue.code == "TST.ERROR.STRICT_ONLY"
    assert issue.level == "error"
    assert issue.details["reason"] == "algo malo pero tolerable"
    assert issue is issues[-1]
    assert len(issues) == 1


def test_emit_and_maybe_raise_error_nonfatal_raises_when_strict_true():
    """Verifica que un issue level=error y fatal=False lance ImportError cuando strict=True."""
    issues = []

    with pytest.raises(PylondrinaImportError) as exc_info:
        emit_and_maybe_raise(
            issues,
            OP01_SMOKE_ISSUES,
            "TST.ERROR.STRICT_ONLY",
            strict=True,
            exception_map=EXC_MAP_IMPORT,
            default_exception=DEFAULT_EXC,
            reason="algo malo pero tolerable",
        )

    exc = exc_info.value
    assert exc.code == "TST.ERROR.STRICT_ONLY"
    assert exc.details["reason"] == "algo malo pero tolerable"
    assert exc.issue is issues[-1]
    assert len(exc.issues) == 1