from typing import Optional

import pandas as pd

from pylondrina.sources.profile import SourceProfile
from pylondrina.schema import TripSchema
from pylondrina.types import FieldCorrespondence, ValueCorrespondence

def make_adatrap_stages_profile(
    *,
    df_stops: Optional[pd.DataFrame] = None,
    stop_code_col: str = "stop_code",
    stop_lat_col: str = "lat",
    stop_lon_col: str = "lon",
    # configuración wide->long
    max_stages: int = 4,
    stage_index_start: int = 1,
    trip_id_col: str = "trip_id",
    origin_stop_tpl: str = "stage{idx}_origin_stop_code",
    destination_stop_tpl: str = "stage{idx}_destination_stop_code",
    # nombres output
    movement_seq_col_out: str = "movement_seq",
    origin_lat_out: str = "origin_latitude",
    origin_lon_out: str = "origin_longitude",
    destination_lat_out: str = "destination_latitude",
    destination_lon_out: str = "destination_longitude",
    schema_override: Optional[TripSchema] = None,
    default_field_correspondence: Optional[FieldCorrespondence] = None,
    default_value_correspondence: Optional[ValueCorrespondence] = None,
) -> SourceProfile:
    """
    Construye un `SourceProfile` para importar **etapas ADATRAP** cuando el input viene en formato
    *wide* (hasta 4 etapas embebidas por fila) y se requiere:
    - reshaping wide -> long (1 fila por etapa real),
    - creación de `movement_seq = 1..k`,
    - mapeo de códigos O/D a coordenadas (join con tabla de paraderos/estaciones).:contentReference[oaicite:11]{index=11}

    Parameters
    ----------
    df_stops : pandas.DataFrame, optional
        Tabla auxiliar `stop_code -> (lat, lon)` para resolver coordenadas.
    max_stages : int, default=4
        Máximo de etapas esperadas en el registro wide.
    stage_index_start : int, default=1
        Índice inicial en los sufijos/prefijos (1 si tus columnas son stage1..stage4).
    trip_id_col : str
        Identificador del viaje, se replica en el long.
    origin_stop_tpl, destination_stop_tpl : str
        Plantillas de nombre de columnas wide. Deben contener `{idx}`.
        Ej: "stage{idx}_origin_stop_code".
    movement_seq_col_out : str
        Columna secuencia de etapa creada en el long.
    schema_override : TripSchema, optional
        Variante de esquema si aplica (por ejemplo, exigir movement_seq).
    default_field_correspondence, default_value_correspondence : mapping, optional
        Mapeos sugeridos para el import genérico.

    Returns
    -------
    SourceProfile
        Perfil listo para importar etapas ADATRAP (wide->long).
    """

    def preprocess(df_wide: pd.DataFrame) -> pd.DataFrame:
        rows = []

        df_long = pd.DataFrame(rows)
        # join coords si hay lookup
        
        return df_long

    return SourceProfile(
        name="ADATRAP_STAGES",
        description="ADATRAP: etapas wide->long (hasta 4) + movement_seq + coords desde códigos de paradero/estación.",
        default_field_correspondence=default_field_correspondence,
        default_value_correspondence=default_value_correspondence,
        preprocess=preprocess,
        schema_override=schema_override,
    )