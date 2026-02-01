# -------------------------
# file: pylondrina/io.py
# -------------------------

from dataclasses import dataclass
from typing import Literal, Optional

from .datasets import TripDataset
from .errors import ExportError, ValidationError
from .reports import OperationReport

WriteMode = Literal["error_if_exists", "overwrite"]


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

