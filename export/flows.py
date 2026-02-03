from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional, Tuple, Literal, Union
from pathlib import Path

from ..datasets import FlowDataset
from ..reports import OperationReport
from ..errors import ExportError


PathLike = Union[str, Path]
ExportFormat = Literal["flowmap_blue"]
WriteMode = Literal["error_if_exists", "overwrite"]


@dataclass(frozen=True)
class FlowExportResult:
    """
    Resultado materializado de una exportación de flujos.

    Atributos
    ---------
    export_dir:
        Directorio creado para esta exportación.
    artifacts:
        Mapa de artefactos exportados (nombre lógico -> ruta).
        En v1.1 con format="flowmap_blue" se incluyen, al menos:
        - "flows": CSV de flujos
        - "nodes": CSV de nodos/locations
        - "metadata": JSON de metadatos de exportación
    """
    export_dir: str
    artifacts: Dict[str, str]


@dataclass(frozen=True)
class ExportFlowsOptions:
    """
    Opciones de la operación Export flows (API v1.1).

    En v1.1 el `format` define el layout de exportación. Por ejemplo, con
    format="flowmap_blue" la operación materializa dos tablas (flows y nodes)
    más un sidecar metadata.json.

    Atributos
    ---------
    format:
        Objetivo de exportación. En v1.1 el único valor soportado es "flowmap_blue".
    mode:
        Política si el directorio de exportación ya existe.
    folder_name:
        Nombre de la carpeta a crear dentro de `output_root`. Si es None, el módulo
        genera un nombre a partir de metadata/provenance del dataset (y, si no hay
        información suficiente, genera un nombre único).
    """
    format: ExportFormat = "flowmap_blue"
    mode: WriteMode = "error_if_exists"
    folder_name: Optional[str] = None


def export_flows(
    flows: FlowDataset,
    output_root: PathLike,
    *,
    options: Optional[ExportFlowsOptions] = None,
) -> Tuple[FlowExportResult, OperationReport]:
    """
    Exporta un FlowDataset a un formato externo orientado a visualización.

    En v1.1 se soporta `format="flowmap_blue"`, que materializa un directorio de
    exportación con:
    - flows (CSV)
    - nodes/locations (CSV)
    - metadata (JSON con trazabilidad completa)

    Parameters
    ----------
    flows:
        FlowDataset de entrada (en memoria).
    output_root:
        Directorio raíz donde se creará la carpeta de exportación.
    options:
        Opciones de exportación. Si es None, se usan defaults.

    Returns
    -------
    (FlowExportResult, OperationReport)
        Resultado con rutas materializadas + reporte de la operación.

    Raises
    ------
    ExportError
        Si falla la materialización (I/O), si el directorio ya existe y mode no lo
        permite, o si el dataset no cumple requisitos mínimos para exportar según el
        `format`.
    """
    raise NotImplementedError
