from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Optional, Union
from pathlib import Path

from ..datasets import FlowDataset
from ..reports import OperationReport
from ..errors import ExportError


PathLike = Union[str, Path]
WriteMode = Literal["error_if_exists", "overwrite"]


@dataclass(frozen=True)
class FlowWriteOptions:
    """
    Opciones para persistir un FlowDataset (flujos) en disco como artefacto Golondrina (v1.1).

    Parameters / Attributes
    -----------------------
    mode:
        Política si el directorio destino ya existe.
    folder_name:
        Nombre de la carpeta a crear dentro de `output_root`. Si es None, el módulo
        genera un nombre a partir de metadata/provenance del dataset (y un componente único).
    write_flow_to_trips:
        Si True y `flows.flow_to_trips` no es None, también persiste la tabla
        `flow_to_trips.parquet`.
    parquet_compression:
        Compresión sugerida para Parquet.
    """
    mode: WriteMode = "error_if_exists"
    folder_name: Optional[str] = None
    write_flow_to_trips: bool = True
    parquet_compression: str = "snappy"


def write_flows(
    flows: FlowDataset,
    output_root: PathLike,
    *,
    options: Optional[FlowWriteOptions] = None,
) -> OperationReport:
    """
    Persiste un FlowDataset a disco en formato Golondrina (layout v1.1).

    En v1.1, la persistencia siempre se realiza en un directorio contenedor creado
    dentro de `output_root`. El nombre del directorio puede ser entregado por el usuario
    (`options.folder_name`) o generado por el módulo.

    Layout v1.1 (dentro del directorio creado)
    ------------------------------------------
    - `flows.parquet`
    - `flow_to_trips.parquet` (opcional)
    - `flows.metadata.json`

    Parameters
    ----------
    flows:
        Dataset de flujos a persistir.
    output_root:
        Directorio raíz donde se creará la carpeta del artefacto persistido.
    options:
        Opciones efectivas de escritura. Si None, se usan defaults.

    Returns
    -------
    OperationReport
        Reporte de la operación, incluyendo rutas escritas y conteos.

    Raises
    ------
    ExportError
        Para problemas de persistencia/IO o colisiones de destino según la política.
    """
    raise NotImplementedError

@dataclass(frozen=True)
class ReadFlowsOptions:
    """
    Opciones para cargar un FlowDataset desde un directorio persistido (artefacto Golondrina).

    En v1.1 se asume un layout fijo dentro del directorio:
    - flows.parquet
    - flows.metadata.json (obligatorio)
    - flow_to_trips.parquet (opcional)

    Attributes
    ----------
    strict : bool, default=False
        Si True, condiciones recuperables se elevan a error; si False, se reportan como issues
        cuando sea razonable.
    keep_metadata : bool, default=True
        Si True, agrega un evento de lectura en `FlowDataset.metadata["events"]`.
    read_flow_to_trips : bool, default=True
        Si True, intenta cargar `flow_to_trips.parquet` cuando exista. Si False, omite su carga
        aunque el archivo exista, dejando `flow_to_trips=None`.
    """
    strict: bool = False
    keep_metadata: bool = True
    read_flow_to_trips: bool = True


def read_flows(
    path: PathLike,
    *,
    options: Optional[ReadFlowsOptions] = None,
) -> tuple[FlowDataset, OperationReport]:
    """
    Carga un FlowDataset persistido desde un directorio Golondrina.

    Parameters
    ----------
    path : PathLike
        Directorio del artefacto de flujos persistido.
    options : ReadFlowsOptions, optional
        Opciones de lectura (modo strict, registro de evento, carga de flow_to_trips).

    Returns
    -------
    tuple[FlowDataset, OperationReport]
        Dataset reconstruido y reporte de la operación.

    Raises
    ------
    ExportError
        Si `path` no existe/no es directorio, si falta `flows.parquet` o `flows.metadata.json`,
        si ocurre un error de lectura I/O, o si el artefacto no contiene el mínimo necesario
        para construir un FlowDataset (p. ej., falta `aggregation_spec` en modo strict).
    """
    raise NotImplementedError
