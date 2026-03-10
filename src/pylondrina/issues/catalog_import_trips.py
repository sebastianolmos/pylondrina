# DE PRUEBA, no definitivo

from pylondrina.issues.core import IssueSpec


IMPORT_ISSUES: dict[str, IssueSpec] = {
    "SCH.TRIP_SCHEMA.EMPTY_FIELDS": IssueSpec(
        code="SCH.TRIP_SCHEMA.EMPTY_FIELDS",
        level="error",
        message_template=(
            "El TripSchema no define campos (fields está vacío); "
            "no es posible importar con un esquema sin catálogo de campos."
        ),
        details_keys=("schema_version", "fields_size", "note"),
        defaults={"note": "no_fields_defined"},
    ),

    "SCH.FIELD_SPEC.UNSUPPORTED_CONSTRAINT": IssueSpec(
        code="SCH.FIELD_SPEC.UNSUPPORTED_CONSTRAINT",
        level="error",
        message_template=(
            "El campo {field!r} incluye una constraint no soportada ({rule!r}); "
            "la importación/validación no puede ejecutarse de forma consistente."
        ),
        details_keys=("schema_version", "field", "rule"),
    ),
}