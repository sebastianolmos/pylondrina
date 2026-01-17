# -------------------------
# file: pylondrina/export/flowmap_blue.py
# -------------------------
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional, Tuple, Literal

import pandas as pd

from ..datasets import FlowDataset


ExportFormat = Literal["csv", "parquet"]


@dataclass(frozen=True)
class FlowmapBlueExportResult:
    """
    Resultado de exportación compatible con flowmap.blue.

    Attributes
    ----------
    locations_path : str
        Ruta del archivo de locations exportado.
    flows_path : str
        Ruta del archivo de flows exportado.
    metadata_path : str, optional
        Ruta a metadatos complementarios (opcional).
    """
    locations_path: str
    flows_path: str
    metadata_path: Optional[str] = None


def export_to_flowmap_blue(
    flows: FlowDataset,
    output_dir: str,
    *,
    format: ExportFormat = "csv",
    location_id_field: str = "id",
    origin_id_field: str = "origin",
    destination_id_field: str = "destination",
    count_field: str = "count",
    include_metadata: bool = True,
) -> FlowmapBlueExportResult:
    """
    Exporta un FlowDataset a archivos compatibles con flowmap.blue.

    flowmap.blue típicamente consume dos tablas:
    - locations: (id, lon, lat, ... opcional)
    - flows: (origin, destination, count, ... opcional)

    Parameters
    ----------
    flows : FlowDataset
        Flujos OD a exportar.
    output_dir : str
        Directorio destino para los archivos.
    format : {"csv","parquet"}, default="csv"
        Formato de salida.
    location_id_field : str, default="id"
        Nombre del campo id en tabla de locations.
    origin_id_field : str, default="origin"
        Nombre del campo origin en tabla flows.
    destination_id_field : str, default="destination"
        Nombre del campo destination en tabla flows.
    count_field : str, default="count"
        Nombre del campo de conteo.
    include_metadata : bool, default=True
        Si True, exporta metadatos de trazabilidad (p. ej., resoluciones H3, filtros, versión esquema).

    Returns
    -------
    FlowmapBlueExportResult
        Rutas generadas para locations/flows y metadatos (si aplica).
    """
    raise NotImplementedError
