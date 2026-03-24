from typing import Optional

import pandas as pd

from pylondrina.sources.profile import SourceProfile
from pylondrina.schema import TripSchema
from pylondrina.types import FieldCorrespondence, ValueCorrespondence

def make_adatrap_trips_profile(
    *,
    df_stops: Optional[pd.DataFrame] = None,
    stop_code_col: str = "stop_code",
    stop_lat_col: str = "lat",
    stop_lon_col: str = "lon",
    origin_stop_col: str = "origin_stop_code",
    destination_stop_col: str = "destination_stop_code",
    origin_lat_out: str = "origin_latitude",
    origin_lon_out: str = "origin_longitude",
    destination_lat_out: str = "destination_latitude",
    destination_lon_out: str = "destination_longitude",
    schema_override: Optional[TripSchema] = None,
    default_field_correspondence: Optional[FieldCorrespondence] = None,
    default_value_correspondence: Optional[ValueCorrespondence] = None,
) -> SourceProfile:
    """
    Construye un `SourceProfile` para importar **viajes ADATRAP resumidos** (1 fila = 1 viaje),
    donde origen/destino suelen venir como **códigos de paradero/estación** que deben mapearse
    a coordenadas mediante una tabla auxiliar.:contentReference[oaicite:10]{index=10}

    Parameters
    ----------
    df_stops : pandas.DataFrame, optional
        Tabla auxiliar `stop_code -> (lat, lon)` para resolver coordenadas.
    stop_code_col, stop_lat_col, stop_lon_col : str
        Nombres de columnas en `df_stops`.
    origin_stop_col, destination_stop_col : str
        Columnas en el input ADATRAP que contienen el código O/D.
    *_out : str
        Nombres de columnas de salida para coordenadas (se crean si es posible).
    schema_override : TripSchema, optional
        Variante de esquema si aplica.
    default_field_correspondence, default_value_correspondence : mapping, optional
        Mapeos sugeridos para el import genérico (p.ej., si el input no usa nombres Golondrina).

    Returns
    -------
    SourceProfile
        Perfil listo para importar viajes ADATRAP resumidos.
    """

    def preprocess(df_trips: pd.DataFrame) -> pd.DataFrame:
        df = df_trips

        # Resolver coords desde stop codes, si hay tabla auxiliar

        return df

    return SourceProfile(
        name="ADATRAP_TRIPS",
        description="ADATRAP: viajes resumidos. Mapea códigos O/D de paradero/estación a coordenadas usando tabla auxiliar.",
        default_field_correspondence=default_field_correspondence,
        default_value_correspondence=default_value_correspondence,
        preprocess=preprocess,
        schema_override=schema_override,
    )