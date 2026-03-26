# -------------------------
# file: pylondrina/schema.py
# -------------------------
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional, Sequence

from pylondrina.types import FieldName, DomainValue

import re
from pprint import pformat
import html

VALID_DTYPES = {"string", "int", "float", "datetime", "categorical", "bool"}

VALID_CONSTRAINT_KEYS = {
    "nullable", "range", "datetime", "h3", "pattern", "length", "unique"
}

# Conjunto de constraints permitidos por dtype (heurística del formato)
CONSTRAINTS_BY_DTYPE = {
    "string": {"nullable", "pattern", "length", "unique", "h3"},
    "int": {"nullable", "range", "unique"},
    "float": {"nullable", "range", "unique"},
    "bool": {"nullable", "unique"},
    "datetime": {"nullable", "datetime", "unique"},
    "categorical": {"nullable", "unique"},  # domain va aparte
}

@dataclass(frozen=True)
class DomainSpec:
    """
    Especificación de un dominio de valores para un campo categórico.

    Parameters
    ----------
    values : sequence of str
        Valores canónicos permitidos para el campo.
    extendable : bool, default=True
        Si el dominio puede extenderse a nivel de dataset mediante una extensión controlada.
    aliases : dict, optional
        Mapeo opcional de sinónimos/variantes -> valor canónico.

    Notes
    -----
    - En v1, las extensiones controladas se registran a nivel de dataset (dominios efectivos).
    - Los aliases permiten normalización sin “romper” el dominio base.
    """
    values: Sequence[DomainValue]
    extendable: bool = True
    aliases: Optional[Dict[DomainValue, DomainValue]] = None

    def to_pretty_dict(self) -> Dict[str, Any]:
        return {
            "values": list(self.values),
            "extendable": self.extendable,
            "aliases": dict(self.aliases) if self.aliases is not None else None,
        }

    def __str__(self) -> str:
        return pformat(self.to_pretty_dict(), sort_dicts=False, width=100)

    def __repr__(self) -> str:
        return f"DomainSpec(\n{pformat(self.to_pretty_dict(), sort_dicts=False, width=100)}\n)"

    def _repr_pretty_(self, p, cycle):
        if cycle:
            p.text("DomainSpec(...)")
        else:
            p.text(str(self))

    def _repr_html_(self) -> str:
        data = html.escape(pformat(self.to_pretty_dict(), sort_dicts=False, width=100))
        return f"<pre>{data}</pre>"


@dataclass(frozen=True)
class FieldSpec:
    """
    Especificación de un campo estándar del formato Golondrina.

    Parameters
    ----------
    name : str
        Nombre estándar del campo (canon del formato Golondrina).
    dtype : str
        Tipo lógico del campo. Ej.: "string", "int", "float", "datetime", "categorical".
    required : bool, default=False
        Si el campo es obligatorio para considerar un viaje válido.
    constraints : dict, optional
        Restricciones estructuradas (p. ej. no-negativo, formato datetime, etc.).
    domain : DomainSpec, optional
        Dominio de valores permitido si dtype="categorical".

    Notes
    -----
    - Las restricciones se evalúan en validación. La representación concreta (pandas dtypes, etc.)
      se define en la implementación.
    """
    name: FieldName
    dtype: str
    required: bool = False
    constraints: Optional[Dict[str, Any]] = None
    domain: Optional[DomainSpec] = None

    def to_pretty_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "dtype": self.dtype,
            "required": self.required,
            "constraints": self.constraints,
            "domain": self.domain.to_pretty_dict() if self.domain is not None else None,
        }

    def __str__(self) -> str:
        return pformat(self.to_pretty_dict(), sort_dicts=False, width=100)

    def __repr__(self) -> str:
        return f"FieldSpec(\n{pformat(self.to_pretty_dict(), sort_dicts=False, width=100)}\n)"

    def _repr_pretty_(self, p, cycle):
        if cycle:
            p.text("FieldSpec(...)")
        else:
            p.text(str(self))

    def _repr_html_(self) -> str:
        data = html.escape(pformat(self.to_pretty_dict(), sort_dicts=False, width=100))
        return f"<pre>{data}</pre>"


@dataclass
class TripSchema:
    """
    Esquema de viajes (Trip Schema) para el formato Golondrina.

    Attributes
    ----------
    version : str
        Versión del esquema (permite trazabilidad y reproducibilidad).
    fields : dict[str, FieldSpec]
        Catálogo de campos válidos del esquema (incluye obligatorios y opcionales).
    required : list[str]
        Lista de nombres estándar que deben estar presentes para conformidad.
    semantic_rules : dict, optional
        Reglas de equivalencia semántica a nivel conceptual (p. ej. mapeos recomendados).
        En v1 se modela como metadato; la aplicación ocurre en funciones de importación.

    Notes
    -----
    - Este objeto es el contrato central para validar datasets de viajes.
    - La “correspondencia” (campo estándar -> campo fuente) se registra en el dataset, no en el schema.
    """
    version: str = "0.1.0"
    fields: Dict[FieldName, FieldSpec] = field(default_factory=dict)
    required: List[FieldName] = field(default_factory=list)
    semantic_rules: Optional[Dict[str, Any]] = None

    def get_field(self, name: FieldName) -> FieldSpec:
        """
        Obtiene la especificación de un campo por nombre estándar.

        Parameters
        ----------
        name : str
            Nombre estándar del campo.

        Returns
        -------
        FieldSpec
            Especificación del campo.

        Raises
        ------
        KeyError
            Si el campo no existe en el catálogo.
        """
        if name not in self.fields:
            raise KeyError(name)
        return self.fields[name]
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convierte este TripSchema a un diccionario **JSON-safe** para persistencia en `metadata.json`.

        El diccionario retornado representa un *snapshot* estable del esquema (v1.1) y debe poder
        serializarse con `json.dumps(...)` sin transformaciones adicionales.

        Returns
        -------
        Dict[str, Any]
            Diccionario serializable (JSON-safe) con la especificación del esquema. En v1.1
            se espera, como mínimo, que incluya:
            - `fields`: lista de especificaciones de campos.
            - `domains`: definición de dominios/catálogos (si aplica).

        """
        return asdict(self)
    
    def get_field(self, name: FieldName) -> FieldSpec:
        if name not in self.fields:
            raise KeyError(name)
        return self.fields[name]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def to_pretty_dict(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "required": list(self.required),
            "semantic_rules": self.semantic_rules,
            "fields": {
                name: spec.to_pretty_dict()
                for name, spec in self.fields.items()
            },
        }

    def __str__(self) -> str:
        return pformat(self.to_pretty_dict(), sort_dicts=False, width=120)

    def __repr__(self) -> str:
        return f"TripSchema(\n{pformat(self.to_pretty_dict(), sort_dicts=False, width=120)}\n)"

    def _repr_pretty_(self, p, cycle):
        if cycle:
            p.text("TripSchema(...)")
        else:
            p.text(str(self))

    def _repr_html_(self) -> str:
        data = html.escape(pformat(self.to_pretty_dict(), sort_dicts=False, width=120))
        return f"<pre>{data}</pre>"
    
@dataclass
class TripSchemaEffective:
    # dtype realmente usado por campo durante import
    dtype_effective: Dict[str, str] = field(default_factory=dict)

    # razones/acciones aplicadas por campo (trazabilidad)
    overrides: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    # dominios efectivos observados (incluye extensiones)
    domains_effective: Dict[str, Any] = field(default_factory=dict)

    # espacio para capacidades/tiers (futuro)
    temporal: Dict[str, Any] = field(default_factory=dict)

    # campos efectivos observados (no incluye extensiones)
    fields_effective: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "dtype_effective": self.dtype_effective,
            "overrides": self.overrides,
            "domains_effective": self.domains_effective,
            "temporal": self.temporal,
            "fields_effective": self.fields_effective,
        }
    
    def to_pretty_dict(self) -> Dict[str, Any]:
        return self.to_dict()

    def __str__(self) -> str:
        return pformat(self.to_pretty_dict(), sort_dicts=False, width=120)

    def __repr__(self) -> str:
        return f"TripSchemaEffective(\n{pformat(self.to_pretty_dict(), sort_dicts=False, width=120)}\n)"

    def _repr_pretty_(self, p, cycle):
        if cycle:
            p.text("TripSchemaEffective(...)")
        else:
            p.text(str(self))

    def _repr_html_(self) -> str:
        data = html.escape(pformat(self.to_pretty_dict(), sort_dicts=False, width=120))
        return f"<pre>{data}</pre>"

def verify_trip_schema_fields(schema):
    findings = []

    for field_name, fs in schema.fields.items():
        dtype = fs.dtype

        # (1) dtype válido
        if dtype not in VALID_DTYPES:
            findings.append({
                "field": field_name,
                "level": "warning",
                "kind": "dtype_invalid",
                "detail": f"dtype='{dtype}' no está en {sorted(VALID_DTYPES)}"
            })

        # (2) categorical/domain
        if dtype == "categorical":
            if fs.domain is None:
                findings.append({
                    "field": field_name,
                    "level": "warning",
                    "kind": "categorical_no_domain",
                    "detail": "dtype='categorical' pero domain=None"
                })
            else:
                # domain declarado pero vacío -> candidato a bootstrapping en S6
                if len(fs.domain.values) == 0:
                    findings.append({
                        "field": field_name,
                        "level": "info",  # yo lo dejaría info (no es fallo; es señal)
                        "kind": "categorical_empty_domain",
                        "detail": "dtype='categorical' y DomainSpec.values vacío (bootstrapping mas adelante)"
                    })

                bad_vals = [v for v in fs.domain.values if not isinstance(v, str)]
                if bad_vals:
                    findings.append({
                        "field": field_name,
                        "level": "warning",
                        "kind": "domain_values_not_string",
                        "detail": f"DomainSpec.values contiene no-string: {bad_vals}"
                    })

        # (3) constraints keys + (4) pattern válido + (5) compatibilidad dtype-constraints
        if fs.constraints is not None:
            keys = list(fs.constraints.keys())

            # keys desconocidas
            unknown = [k for k in keys if k not in VALID_CONSTRAINT_KEYS]
            if unknown:
                findings.append({
                    "field": field_name,
                    "level": "warning",
                    "kind": "unknown_constraints",
                    "detail": f"constraints contiene llaves desconocidas: {unknown}"
                })

            # pattern compila
            if "pattern" in fs.constraints:
                pat = fs.constraints["pattern"]
                if not isinstance(pat, str):
                    findings.append({
                        "field": field_name,
                        "level": "error",
                        "kind": "pattern_not_string",
                        "detail": "constraints['pattern'] debe ser str"
                    })
                else:
                    try:
                        re.compile(pat)
                    except re.error as e:
                        findings.append({
                            "field": field_name,
                            "level": "error",
                            "kind": "pattern_invalid",
                            "detail": f"regex no compila: {e}"
                        })

            # compatibilidad dtype <-> constraints (solo si dtype es válido conocido)
            if dtype in CONSTRAINTS_BY_DTYPE:
                allowed = CONSTRAINTS_BY_DTYPE[dtype]
                incompatible = [k for k in keys if (k in VALID_CONSTRAINT_KEYS and k not in allowed)]
                if incompatible:
                    findings.append({
                        "field": field_name,
                        "level": "warning",
                        "kind": "constraint_incompatible_with_dtype",
                        "detail": f"constraints {incompatible} no aplican a dtype='{dtype}'"
                    })

    return findings

def build_schema_effective_from_findings(schema, findings):
    eff = TripSchemaEffective()

    # defaults: si no hay override, dtype_effective es el dtype del schema base
    for fname, fs in schema.fields.items():
        eff.dtype_effective[fname] = fs.dtype

    for f in findings:
        kind = f["kind"]
        field = f["field"]

        if kind in {"dtype_invalid", "categorical_no_domain"}:
            eff.dtype_effective[field] = "string"
            eff.overrides.setdefault(field, {})
            eff.overrides[field]["dtype_effective"] = "string"
            eff.overrides[field].setdefault("reasons", []).append(kind)

    return eff

@dataclass(frozen=True)
class TraceSchema:
    """
    Esquema de trazas (Trace Schema) para TraceDataset en formato Golondrina.

    En v1.1, TraceDataset utiliza nombres de campos estándar definidos por el Formato Golondrina.
    Por lo tanto, este esquema NO define mapeos desde columnas externas; su propósito es describir
    el conjunto de campos estándar y reglas/contexto de interpretación.

    Attributes
    ----------
    version : str
        Versión del esquema.
    fields : dict[FieldName, FieldSpec]
        Catálogo de campos estándar esperados y reglas (si aplica).
    required : list[FieldName]
        Lista explícita de campos requeridos. Si se deja vacía, el sistema asume como requeridos
        los campos mínimos de trazas definidos por Golondrina (p. ej., user_id, timestamp, lat, lon).
    crs : str, optional
        CRS asociado a coordenadas. Por defecto EPSG:4326.
    timezone : str, optional
        Zona horaria para interpretar timestamps si no son timezone-aware.

    Notes
    -----
    - El mapeo desde fuentes externas se realiza en `import_traces` mediante `field_correspondence`
      y/o `preprocess`.
    """
    version: str = "0.1.0"

    fields: Dict["FieldName", "FieldSpec"] = field(default_factory=dict)
    required: List["FieldName"] = field(default_factory=list)

    crs: Optional[str] = "EPSG:4326"
    timezone: Optional[str] = None

