from typing import Optional

import pandas as pd

from pylondrina.sources.profile import SourceProfile
from pylondrina.schema import TripSchema
from pylondrina.types import FieldCorrespondence, ValueCorrespondence


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