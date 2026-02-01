# pylondrina/io/trips.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Optional

from pathlib import Path
from typing import Optional, Union

from ..datasets import TripDataset
from ..errors import ExportError
from ..reports import OperationReport
from ..schema import TripSchema


WriteMode = Literal["error_if_exists", "overwrite"]
PathLike = Union[str, Path]

@dataclass(frozen=True)
class TripWriteOptions:
    """
    Opciones para persistir un TripDataset (viajes) en disco usando Parquet + sidecar JSON.

    Parameters
    ----------
    mode : {"error_if_exists", "overwrite"}, default="error_if_exists"
        Política si el destino ya existe.
        - "error_if_exists": falla si el directorio/artefactos ya existen.
        - "overwrite": sobrescribe los artefactos estándar del dataset.

    require_validated : bool, default=True
        Si True, exige que `trips.metadata["flags"]["validated"]` exista y sea True.
        Si False, permite persistir datasets no validados (útil para debug/testing).

    parquet_compression : str, default="snappy"
        Compresión sugerida para el Parquet (el soporte depende del engine disponible).
    """

    mode: WriteMode = "error_if_exists"
    require_validated: bool = True
    parquet_compression: str = "snappy"

def write_trips(
    trips: TripDataset,
    path: str,
    *,
    options: Optional[TripWriteOptions] = None,
) -> OperationReport:
    """
    Persiste un TripDataset en formato Golondrina a disco.

    Layout v1.1 
    ----------------------------------
    - Datos tabulares: `trips.parquet`
    - Metadata estructurada (sidecar): `trips.metadata.json`

    Parameters
    ----------
    trips : TripDataset
        Dataset de viajes en formato Golondrina.
    path : str
        Directorio destino del dataset persistido.
    options : TripWriteOptions, optional
        Opciones efectivas de escritura. Por defecto, requiere dataset validado.

    Returns
    -------
    OperationReport
        Reporte de la operación. El summary debe incluir rutas escritas y conteos básicos.

    Raises
    ------
    ValidationError
        Si `options.require_validated=True` y el dataset no aparece como validado.
    ExportError
        Si ocurre un error de I/O o el destino existe y `mode="error_if_exists"`.
    """
    raise NotImplementedError




@dataclass(frozen=True)
class ReadTripsOptions:
    """
    Opciones para cargar un TripDataset desde un directorio persistido (artefacto Golondrina).

    En v1.1 se asume un layout fijo dentro del directorio:
    - trips.parquet
    - metadata.json (obligatorio)

    El dataset se carga siempre como "no validado" (se fuerza el flag correspondiente en metadata),
    para exigir una validación posterior en el pipeline.

    Attributes
    ----------
    schema : TripSchema, optional
        Esquema a usar para reconstruir el TripDataset. Si no se entrega, se intenta reconstruir
        desde `metadata.json`. Si no es posible obtener un esquema, la lectura falla.
    strict : bool
        Si True, condiciones recuperables se elevan a error; si False, se reportan como issues
        cuando sea razonable.
    keep_metadata : bool
        Si True, agrega un evento de lectura en `TripDataset.metadata["events"]`.
    """

    schema: Optional[TripSchema] = None
    strict: bool = False
    keep_metadata: bool = True


def read_trips(
    path: PathLike,
    *,
    options: Optional[ReadTripsOptions] = None,
) -> tuple[TripDataset, OperationReport]:
    """
    Carga un TripDataset persistido desde un directorio Golondrina.

    Reglas de reconstrucción (v1.1)
    -------------------------------
    - El dataframe se carga desde `trips.parquet`.
    - El metadata/provenance se carga desde `metadata.json` (obligatorio).
    - El `TripSchema` se obtiene desde `options.schema` si se entrega; si no, se intenta reconstruir desde `metadata.json`.
    - El dataset retornado se marca como "no validado" (se fuerza el flag en metadata), para exigir re-validación posterior.
    - Se retorna un `OperationReport` con issues y summary mínimos de la lectura.

    Parameters
    ----------
    path : PathLike
        Directorio del dataset persistido.
    options : ReadTripsOptions, optional
        Opciones de lectura (esquema opcional, modo strict, registro de evento).

    Returns
    -------
    tuple[TripDataset, OperationReport]
        Dataset reconstruido y reporte de la operación.

    Raises
    ------
    ExportError
        Si `path` no existe/no es directorio, si falta `trips.parquet` o `metadata.json`,
        si ocurre un error de lectura I/O, o si no se puede reconstruir un `TripSchema`.
    """
    raise NotImplementedError
