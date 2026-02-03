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
