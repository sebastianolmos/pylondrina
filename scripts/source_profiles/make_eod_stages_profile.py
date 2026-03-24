from typing import Optional

import pandas as pd

from pylondrina.sources.profile import SourceProfile
from pylondrina.schema import TripSchema
from pylondrina.types import FieldCorrespondence, ValueCorrespondence

def make_eod_stages_profile(
    *,
    df_catalogo_modo: Optional[pd.DataFrame] = None,
    df_catalogo_proposito: Optional[pd.DataFrame] = None,
    # columnas típicas
    trip_id_col: str = "trip_id",
    stage_order_col: Optional[str] = None,
    origin_time_col: Optional[str] = None,
    modo_id_col: str = "modo_id",
    proposito_id_col: str = "proposito_id",
    modo_name_col: str = "modo_nombre",
    proposito_name_col: str = "proposito_nombre",
    movement_seq_col_out: str = "movement_seq",
    schema_override: Optional[TripSchema] = None,
    default_field_correspondence: Optional[FieldCorrespondence] = None,
    default_value_correspondence: Optional[ValueCorrespondence] = None,
) -> SourceProfile:
    """
    Construye un `SourceProfile` para importar **etapas EOD** (1 fila = 1 etapa/movimiento).

    Responsabilidad mínima del `preprocess` para etapas:
    - construir `movement_seq` por viaje (usando una columna de orden si existe; si no, una
      estrategia determinista como timestamps o el orden de aparición),
    - decodificar IDs categóricos (modo, propósito) mediante catálogos.:contentReference[oaicite:8]{index=8}

    Parameters
    ----------
    df_catalogo_modo, df_catalogo_proposito : pandas.DataFrame, optional
        Catálogos para decodificar IDs.
    trip_id_col : str
        Identificador para agrupar etapas por viaje.
    stage_order_col : str, optional
        Columna explícita de orden de etapa (si existe).
    origin_time_col : str, optional
        Columna temporal para ordenar si no existe `stage_order_col`.
    movement_seq_col_out : str, default="movement_seq"
        Nombre de la columna que se creará/asegurará como secuencia 1..k.
    schema_override : TripSchema, optional
        Útil si decides que “etapas” requieren un mínimo distinto (p.ej. exigir `movement_seq`).:contentReference[oaicite:9]{index=9}
    default_field_correspondence, default_value_correspondence : mapping, optional
        Mapeos sugeridos para el import genérico.

    Returns
    -------
    SourceProfile
        Perfil listo para importar etapas EOD.
    """

    def preprocess(df_etapas: pd.DataFrame) -> pd.DataFrame:
        df = df_etapas

        # (1) movement_seq por viaje
        # (2) decodificación categórica

        return df

    return SourceProfile(
        name="EOD_STAGES",
        description="EOD: tabla de etapas (movimientos) + construcción de movement_seq + decodificación de IDs.",
        default_field_correspondence=default_field_correspondence,
        default_value_correspondence=default_value_correspondence,
        preprocess=preprocess,
        schema_override=schema_override,
    )