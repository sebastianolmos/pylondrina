from __future__ import annotations

from typing import Optional

import pandas as pd

from .profile import SourceProfile
from ..schema import TripSchema
from ..types import FieldCorrespondence, ValueCorrespondence


def make_eod_trips_profile(
    *,
    df_personas: Optional[pd.DataFrame] = None,
    df_hogares: Optional[pd.DataFrame] = None,
    df_catalogo_modo: Optional[pd.DataFrame] = None,
    df_catalogo_proposito: Optional[pd.DataFrame] = None,
    # claves / columnas típicas (parametrizables)
    person_id_col: str = "person_id",
    household_id_col: str = "household_id",
    modo_id_col: str = "modo_id",
    proposito_id_col: str = "proposito_id",
    modo_name_col: str = "modo_nombre",
    proposito_name_col: str = "proposito_nombre",
    # control de salida
    attach_person_fields: bool = True,
    attach_household_fields: bool = False,
    schema_override: Optional[TripSchema] = None,
    default_field_correspondence: Optional[FieldCorrespondence] = None,
    default_value_correspondence: Optional[ValueCorrespondence] = None,
) -> SourceProfile:
    """
    Construye un `SourceProfile` para importar viajes EOD desde una **tabla de viajes resumidos**
    (1 fila = 1 viaje), aplicando *adaptación de fuente* vía `preprocess`.

    En EOD es común que modo/motivo y otros atributos vengan como IDs/códigos; por eso el
    `preprocess` típico hace merges con catálogos para producir columnas categóricas interpretables
    (p.ej. `mode`, `purpose`) antes de pasar al import genérico.:contentReference[oaicite:5]{index=5}

    Parameters
    ----------
    df_personas, df_hogares : pandas.DataFrame, optional
        Tablas auxiliares para enriquecer atributos de persona/hogar (si se desea).
        En v1 es aceptable hacerlo en `preprocess` como solución práctica (aunque en v2 podría
        formalizarse como `enrich_trips`).:contentReference[oaicite:6]{index=6}
    df_catalogo_modo, df_catalogo_proposito : pandas.DataFrame, optional
        Catálogos/lookup para decodificar IDs (modo, propósito) a valores “humanos”.
    *_col : str
        Nombres de columnas para llaves y nombres decodificados; se exponen para tolerar variantes
        de extractos EOD.
    attach_person_fields : bool, default=True
        Si True y `df_personas` está presente, hace merge para traer atributos adicionales.
    attach_household_fields : bool, default=False
        Si True y `df_hogares` está presente, hace merge para traer atributos del hogar.
    schema_override : TripSchema, optional
        Variante de esquema si esta fuente requiere reglas distintas (evitar si no es necesario).:contentReference[oaicite:7]{index=7}
    default_field_correspondence, default_value_correspondence : mapping, optional
        Mapeos sugeridos para que el import genérico pueda asumir nombres típicos de la fuente.

    Returns
    -------
    SourceProfile
        Perfil listo para usar con `import_trips_from_profile(...)` o registrable en el registry.

    Notes
    -----
    - `preprocess` aquí **no valida** Golondrina; solo adapta/decodifica/normaliza mínimamente.
    """

    def preprocess(df_viajes: pd.DataFrame) -> pd.DataFrame:
        df = df_viajes

        # (1) Enriquecimiento opcional por persona/hogar
        # (2) Decodificación de modo / propósito (IDs -> nombres)
        # (3) Normalización de columnas “puente” para facilitar import genérico
        # (no fuerza el esquema Golondrina; solo crea columnas útiles si existen)

        return df

    return SourceProfile(
        name="EOD_TRIPS",
        description="EOD: tabla viajes resumidos + decodificación de IDs mediante catálogos (y opcionalmente Personas/Hogares).",
        default_field_correspondence=default_field_correspondence,
        default_value_correspondence=default_value_correspondence,
        preprocess=preprocess,
        schema_override=schema_override,
    )


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
