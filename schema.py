# -------------------------
# file: pylondrina/schema.py
# -------------------------
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence

from .types import FieldName, DomainValue


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
        raise NotImplementedError
    
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
        raise NotImplementedError


@dataclass(frozen=True)
class TraceSchema:
    """
    Esquema de trazas (Trace Schema) para interpretar datos como trayectoria/traza.

    Attributes
    ----------
    version : str
        Versión del esquema.
    fields : dict[FieldName, FieldSpec]
        Catálogo de campos esperados y reglas (si aplica).
    required : list[FieldName]
        Lista de campos requeridos. Si se deja vacía, el sistema asume como requeridos los roles
        mínimos (user_id, timestamp, lat, lon) según los nombres configurados.
    user_id_field : FieldName
        Nombre del campo que identifica usuario/dispositivo.
    time_field : FieldName
        Nombre del campo temporal (timestamp).
    lon_field : FieldName
        Nombre del campo de longitud o coordenada X.
    lat_field : FieldName
        Nombre del campo de latitud o coordenada Y.
    crs : str, optional
        CRS asociado a coordenadas. Por defecto EPSG:4326 si se usa lon/lat.
    timezone : str, optional
        Zona horaria para interpretar timestamps si no son timezone-aware.

    Notes
    -----
    - En v1 se usa para validaciones básicas y consistencia (espacio-temporal) antes de inferir viajes.
    - `required` se mantiene por compatibilidad/expresividad, pero el mínimo operativo lo definen
      los campos de rol. En general, conviene no duplicar: o defines `required` explícitamente
      o confías en los roles mínimos.
    """
    version: str = "0.1.0"

    fields: Dict[FieldName, FieldSpec] = field(default_factory=dict)
    required: List[FieldName] = field(default_factory=list)

    user_id_field: FieldName = "user_id"
    time_field: FieldName = "timestamp"
    lon_field: FieldName = "lon"
    lat_field: FieldName = "lat"

    crs: Optional[str] = "EPSG:4326"
    timezone: Optional[str] = None