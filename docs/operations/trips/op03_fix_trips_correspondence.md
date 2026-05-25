# OP-03 Fix trips correspondence

`fix_trips_correspondence` es la operación de corrección semántica post-import para datasets de trips. Se implementó para ajustar correspondencias de campos y valores categóricos sobre un `TripDataset` ya construido, sin repetir la importación completa y sin reemplazar la validación formal.

La operación permite corregir problemas de mapping o normalización conceptual detectados después de importar. Por ejemplo, una fuente puede haber conservado una columna no canónica que debe alinearse con un campo del schema, o puede contener valores categóricos equivalentes que conviene unificar antes de seguir el pipeline.

## Para qué sirve

Esta operación permite corregir el estado semántico de un `TripDataset` cuando el problema identificado corresponde a correspondencias de campos o valores, no a estructura física de la fuente ni a validación formal.

La operación puede aplicar dos tipos de corrección, siempre en este orden:

1. `field_corrections`: renombrado de columnas actuales hacia campos canónicos.
2. `value_corrections`: recodificación de valores observados hacia valores canónicos.

Además de modificar la tabla de salida, el sistema actualiza la trazabilidad asociada:

- `TripDataset.field_correspondence`;
- `TripDataset.value_correspondence`;
- `metadata["mappings"]`;
- `metadata["domains_effective"]`;
- `schema_effective.domains_effective`;
- `schema_effective.fields_effective`;
- `metadata["events"]`;
- `metadata["is_validated"]`, según si hubo cambios efectivos.

## Cuándo usarla

Esta operación se usa después de importar trips y antes de revalidar, cuando se detecta que el dataset ya existe dentro de Pylondrina, pero todavía requiere una corrección semántica fina.

Un flujo típico es:

```text
import_trips_from_dataframe -> validate_trips -> fix_trips_correspondence -> validate_trips
```

También puede usarse antes de `clean_trips`, `filter_trips` o `build_flows` si los campos o categorías corregidas son relevantes para esas operaciones posteriores.

No corresponde usar OP-03 para:

* volver a importar una fuente;
* certificar conformidad formal;
* ejecutar validaciones de required fields, tipos, constraints, dominios o duplicados;
* limpiar filas problemáticas;
* escribir artefactos en disco;
* redefinir el schema base.

## Qué recibe y qué retorna

La operación recibe un `TripDataset` ya construido y, opcionalmente, tres bloques de configuración:

* `field_corrections`: mapping desde nombre actual de columna hacia nombre canónico objetivo;
* `value_corrections`: mapping por campo canónico, desde valor observado hacia valor canónico;
* `correspondence_context`: metadata contextual de la corrección, por ejemplo motivo, alcance o fuente de la decisión.

También recibe `FixCorrespondenceOptions`, que controla:

* `strict`;
* `max_issues`;
* `sample_rows_per_issue`.

La operación retorna:

```python
TripDataset, OperationReport
```

El `TripDataset` retornado es un nuevo objeto. El input no se muta. El `OperationReport` resume qué se solicitó, qué se aplicó, cuántos reemplazos se realizaron, si hubo cambios efectivos y qué issues se registraron.

## Qué evidencia deja

OP-03 deja evidencia en el `OperationReport` y en el evento agregado a `metadata["events"]` del dataset de salida.

El `OperationReport` incluye:

* `ok`;
* `issues`;
* `summary`;
* `parameters`.

El `summary` se mantiene pequeño y estable. Sus claves principales son:

* `n_rows`;
* `n_field_corrections_requested`;
* `n_field_corrections_applied`;
* `n_value_corrections_fields_requested`;
* `n_value_corrections_fields_applied`;
* `n_value_replacements_applied`;
* `domains_effective_updated_fields`;
* `noop`.

Si se alcanza el límite de issues, el summary puede incluir un bloque `limits`.

El evento registrado usa:

```python
"op": "fix_trips_correspondence"
```

y contiene:

* `ts_utc`;
* `parameters`;
* `summary`;
* `issues_summary`;
* `context`.

El bloque `parameters` conserva el request efectivo serializable, incluyendo correcciones pedidas y opciones efectivas. El bloque `context` conserva el `correspondence_context` saneado.

## Consideraciones importantes

La operación no valida el dataset. Si hubo cambios semánticos reales, el resultado queda marcado como no validado:

```python
metadata["is_validated"] = False
```

Si la operación no produjo cambios efectivos, el estado previo de `metadata["is_validated"]` se preserva. OP-03 nunca marca un dataset como validado.

`field_corrections` está pensado para correcciones quirúrgicas post-import. Se usa con forma:

```python
{
    "columna_actual": "campo_canonico"
}
```

No debe usarse para sobrescribir un campo canónico que ya existe en el dataset, para intercambiar campos canónicos entre sí ni para renombrar hacia campos que no pertenecen al schema.

`value_corrections` se aplica después de resolver los nombres de campos. Se usa con forma:

```python
{
    "campo_canonico": {
        "valor_observado": "valor_canonico"
    }
}
```

Solo se aplican valores observados en los datos. Si parte del request no es aplicable, la operación puede continuar con las reglas válidas y registrar issues recuperables.

`correspondence_context` permite documentar por qué se hizo la corrección. El sistema acepta un diccionario con información contextual serializable. Si hay claves desconocidas o fragmentos no serializables, se descartan con evidencia. Si la raíz no es un diccionario, la operación aborta.

## Ejemplo mínimo

El siguiente ejemplo corrige valores categóricos equivalentes en el campo `Proposito`, sin reimportar el dataset.

```python
from pylondrina.fixing import FixCorrespondenceOptions, fix_trips_correspondence

value_corrections = {
    "Proposito": {
        "Al estudio": "Estudio",
        "Por estudio": "Estudio",
        "Al trabajo": "Trabajo",
        "Por trabajo": "Trabajo",
        "volver a casa": "A casa",
    }
}

fixed_trips, fix_report = fix_trips_correspondence(
    trips,
    value_corrections=value_corrections,
    options=FixCorrespondenceOptions(
        strict=False,
        max_issues=200,
        sample_rows_per_issue=20,
    ),
    correspondence_context={
        "reason": "unificación de propósitos equivalentes",
        "scope": "baseline de estudio",
        "source": "EOD",
    },
)

print(fix_report.summary)
print(fixed_trips.metadata["is_validated"])
```

Después de aplicar una corrección con cambios efectivos, se recomienda ejecutar nuevamente `validate_trips` para reestablecer la conformidad formal del dataset.

## Operación anterior y siguiente

Dentro de la familia de operaciones sobre trips, OP-03 aparece después de importar y validar inicialmente un dataset, cuando se detectan problemas de correspondencia o normalización semántica.

| Posición                 | Operación                                                                            |
| ------------------------ | ------------------------------------------------------------------------------------ |
| Anterior recomendada     | [OP-02 Validate trips](op02_validate_trips.md)                                       |
| Actual                   | OP-03 Fix trips correspondence                                                       |
| Siguiente recomendada    | [OP-02 Validate trips](op02_validate_trips.md)                                       |
| Alternativas posteriores | [OP-04 Clean trips](op04_clean_trips.md), [OP-05 Filter trips](op05_filter_trips.md) |

## Implementación relacionada

Esta operación forma parte de la API pública de Pylondrina v1.1. Su implementación principal puede revisarse en el código fuente del proyecto.

| Recurso            | Enlace                                                                                                                                                      |
| ------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Archivo fuente     | [`src/pylondrina/fixing.py`](https://github.com/sebastianolmos/pylondrina/blob/main/src/pylondrina/fixing.py)                                               |
| Catálogo de issues | [`src/pylondrina/issues/catalog_fix_trips_cors.py`](https://github.com/sebastianolmos/pylondrina/blob/main/src/pylondrina/issues/catalog_fix_trips_cors.py) |
| Referencia API     | [Ver referencia técnica](../../api/trips.md)                                                                                                                |
