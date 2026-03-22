from __future__ import annotations

"""
Wrappers genéricos sobre `generate_synthetic_trip_dataframe()`.

La idea de este archivo es ofrecer una capa mínima y reusable para los casos de
entrada más comunes al testear `import_trips_from_dataframe()`, sin amarrar los
nombres a tests de integración específicos.

Diseño seguido:
- Los wrappers son deliberadamente genéricos y semánticos.
- Cada wrapper fija una configuración base razonable.
- Todos aceptan `**overrides` para refinar la configuración sin perder la idea
  principal del preset.
- La función base sigue siendo la fuente de verdad para casos más específicos.
"""

from typing import Any, Optional, Sequence

import pandas as pd

from scripts.synthetic_data.base_generator import generate_synthetic_trip_dataframe


DEFAULT_RICH_BASE_FIELDS: list[str] = [
    "mode",
    "purpose",
    "day_type",
    "time_period",
    "user_gender",
    "trip_weight",
    "origin_municipality",
    "destination_municipality",
]

DEFAULT_RICH_EXTRA_COLUMNS: list[str] = [
    "household_id",
    "source_person_id",
    "stage_count",
    "activity_destination",
    "travel_time_min",
    "fare_amount",
]

DEFAULT_EXTENDED_DOMAIN_CONFIG: dict[str, list[str]] = {
    "mode": ["canon", "submodes"],
    "purpose": ["canon", "finos"],
    "time_period": ["canon", "eod_finos"],
    "user_gender": ["canon", "es"],
}

DEFAULT_EXTENDED_SAMPLING_POLICY: dict[str, dict[str, Any]] = {
    "mode": {"canonical_ratio": 0.75, "ensure_presence": True},
    "purpose": {"canonical_ratio": 0.70, "ensure_presence": True},
    "time_period": {"canonical_ratio": 0.80},
    "user_gender": {"canonical_ratio": 0.90},
}


def _call_generator(*, filas: int, seed: Optional[int], base_config: dict[str, Any], overrides: dict[str, Any]) -> pd.DataFrame:
    """Helper interno para fusionar configuración base + overrides."""
    config = dict(base_config)
    config.update(overrides)
    return generate_synthetic_trip_dataframe(filas=filas, seed=seed, **config)


def make_happy_path_minimal(
    filas: int = 6,
    *,
    seed: Optional[int] = None,
    **overrides: Any,
) -> pd.DataFrame:
    """
    Genera el caso feliz más simple posible.

    Perfil del dataset
    ------------------
    - Solo required canónicos.
    - Tier 1 con datetime parseable y limpio.
    - Coordenadas numéricas.
    - H3 válidos ya presentes.
    - Sin campos base ni extras.
    - Sin nulos, ruido ni corrupción.

    Cuándo usarlo
    -------------
    - Primera prueba de smoke/integración.
    - Verificar retorno básico de `TripDataset` e `ImportReport`.
    - Comprobar que el flujo canónico termina correctamente.

    Overrides útiles
    ----------------
    - `field_correspondence` si quieres renombrar columnas.
    - `filas` si quieres más o menos volumen.
    """
    base_config = {
        "tier_temporal": "tier_1",
        "tier1_datetime_format": "utc_string_z",
        "coord_format": "numeric",
        "h3_mode": "provided_valid",
        "trip_structure": "independent",
    }
    return _call_generator(filas=filas, seed=seed, base_config=base_config, overrides=overrides)


def make_happy_path_rich(
    filas: int = 15,
    *,
    seed: Optional[int] = None,
    **overrides: Any,
) -> pd.DataFrame:
    """
    Genera un dataset válido y rico, parecido a una fuente real moderadamente completa.

    Perfil del dataset
    ------------------
    - Required canónicos.
    - Tier 1 con datetime parseable.
    - Coordenadas numéricas.
    - H3 válidos presentes.
    - Campos base frecuentes.
    - Varias columnas extra plausibles.
    - Sin errores deliberados.

    Cuándo usarlo
    -------------
    - Happy path más realista que el mínimo.
    - Casos donde se quiere revisar trazabilidad, materialización y variedad.
    - Notebooks de documentación rápida del import.
    """
    base_config = {
        "tier_temporal": "tier_1",
        "tier1_datetime_format": "offset_string",
        "coord_format": "numeric",
        "h3_mode": "provided_valid",
        "trip_structure": "independent",
        "base_fields": list(DEFAULT_RICH_BASE_FIELDS),
        "extra_columns": list(DEFAULT_RICH_EXTRA_COLUMNS),
    }
    return _call_generator(filas=filas, seed=seed, base_config=base_config, overrides=overrides)


def make_h3_derivable(
    filas: int = 8,
    *,
    seed: Optional[int] = None,
    **overrides: Any,
) -> pd.DataFrame:
    """
    Genera un dataset donde los H3 no vienen informados y deben poder derivarse.

    Perfil del dataset
    ------------------
    - H3 omitidos explícitamente.
    - Coordenadas OD presentes y válidas.
    - Tier 1 limpio por defecto.

    Cuándo usarlo
    -------------
    - Verificar derivación H3 desde coordenadas.
    - Casos donde la fuente no trae indexación espacial precomputada.

    Nota
    ----
    Este wrapper omite automáticamente `origin_h3_index` y `destination_h3_index`.
    """
    base_config = {
        "tier_temporal": "tier_1",
        "tier1_datetime_format": "utc_string_z",
        "coord_format": "numeric",
        "h3_mode": "omitted_derivable",
        "omit_required_fields": ["origin_h3_index", "destination_h3_index"],
        "trip_structure": "independent",
        "base_fields": ["mode", "purpose"],
    }
    return _call_generator(filas=filas, seed=seed, base_config=base_config, overrides=overrides)


def make_tier2_valid(
    filas: int = 8,
    *,
    seed: Optional[int] = None,
    **overrides: Any,
) -> pd.DataFrame:
    """
    Genera un dataset limpio en Tier 2 con campos `HH:MM` válidos.

    Perfil del dataset
    ------------------
    - Sin `origin_time_utc` ni `destination_time_utc`.
    - Con `origin_time_local_hhmm` y `destination_time_local_hhmm` válidos.
    - H3 válidos presentes.

    Cuándo usarlo
    -------------
    - Casos limpios de import en Tier 2.
    - Comprobar manejo de horarios locales sin datetime completo.
    """
    base_config = {
        "tier_temporal": "tier_2",
        "tier2_hhmm_format": "valid_hhmm",
        "coord_format": "numeric",
        "h3_mode": "provided_valid",
        "trip_structure": "independent",
        "base_fields": ["origin_time_local_hhmm", "destination_time_local_hhmm", "mode", "purpose"],
    }
    return _call_generator(filas=filas, seed=seed, base_config=base_config, overrides=overrides)


def make_tier2_mixed_invalid(
    filas: int = 10,
    *,
    seed: Optional[int] = None,
    mostly_invalid: bool = False,
    **overrides: Any,
) -> pd.DataFrame:
    """
    Genera un dataset Tier 2 con mezcla de `HH:MM` válidos e inválidos.

    Parameters
    ----------
    mostly_invalid : bool, default=False
        Si es True, usa una configuración donde predominan horarios inválidos.
        Si es False, genera una mezcla más equilibrada.

    Cuándo usarlo
    -------------
    - Casos de calidad no fatal para temporalidad Tier 2.
    - Verificar warnings/issues asociados a parseo parcial de HH:MM.
    """
    base_config = {
        "tier_temporal": "tier_2",
        "tier2_hhmm_format": "mostly_invalid_hhmm" if mostly_invalid else "mixed_hhmm",
        "coord_format": "numeric",
        "h3_mode": "provided_valid",
        "trip_structure": "independent",
        "base_fields": ["origin_time_local_hhmm", "destination_time_local_hhmm", "mode", "purpose"],
    }
    return _call_generator(filas=filas, seed=seed, base_config=base_config, overrides=overrides)


def make_multistage_valid(
    filas: int = 12,
    *,
    seed: Optional[int] = None,
    max_movements_per_trip: int = 3,
    **overrides: Any,
) -> pd.DataFrame:
    """
    Genera un dataset válido con viajes compuestos por múltiples movements.

    Parameters
    ----------
    max_movements_per_trip : int, default=3
        Máximo de filas que pueden compartir el mismo `trip_id`.

    Cuándo usarlo
    -------------
    - Casos donde interesa revisar `trip_id` + `movement_seq`.
    - Datasets más parecidos a una fuente con estructura viaje-etapa.
    - Pruebas con `single_stage=False` o datasets multietapa generales.
    """
    base_config = {
        "tier_temporal": "tier_1",
        "tier1_datetime_format": "offset_string",
        "coord_format": "numeric",
        "h3_mode": "provided_valid",
        "trip_structure": "multistage",
        "max_movements_per_trip": max_movements_per_trip,
        "base_fields": ["mode", "purpose", "trip_weight", "mode_sequence"],
        "extra_columns": ["stage_count", "travel_time_min"],
    }
    return _call_generator(filas=filas, seed=seed, base_config=base_config, overrides=overrides)


def make_extended_domains(
    filas: int = 12,
    *,
    seed: Optional[int] = None,
    include_noise: bool = False,
    **overrides: Any,
) -> pd.DataFrame:
    """
    Genera un dataset con dominios categóricos extendidos, pero todavía usable como entrada natural.

    Parameters
    ----------
    include_noise : bool, default=False
        Si es True, además de valores extendidos inyecta una pequeña proporción de
        valores fuera de dominio en algunos campos categóricos.

    Cuándo usarlo
    -------------
    - Probar dominios extendibles y `domains_effective`.
    - Revisar mezcla de valores canónicos y extendidos.
    - Preparar datasets con mayor riqueza semántica.
    """
    base_config = {
        "tier_temporal": "tier_1",
        "tier1_datetime_format": "offset_string",
        "coord_format": "numeric",
        "h3_mode": "provided_valid",
        "trip_structure": "independent",
        "base_fields": ["mode", "purpose", "time_period", "user_gender", "trip_weight"],
        "extra_value_domains": dict(DEFAULT_EXTENDED_DOMAIN_CONFIG),
        "categorical_sampling_policy": dict(DEFAULT_EXTENDED_SAMPLING_POLICY),
    }
    if include_noise:
        base_config["noise_ratio"] = {"mode": 0.03, "purpose": 0.04, "user_gender": 0.02}
    return _call_generator(filas=filas, seed=seed, base_config=base_config, overrides=overrides)


def make_missing_required(
    filas: int = 6,
    *,
    seed: Optional[int] = None,
    missing_fields: Optional[Sequence[str]] = None,
    n_random_missing_required: int = 0,
    **overrides: Any,
) -> pd.DataFrame:
    """
    Genera un dataset con required faltantes.

    Parameters
    ----------
    missing_fields : sequence of str, optional
        Required que se omitirán explícitamente.
    n_random_missing_required : int, default=0
        Required adicionales a omitir al azar.

    Cuándo usarlo
    -------------
    - Probar abortos estructurales por ausencia de required.
    - Generar entradas degradadas sin tener que quitar columnas manualmente.

    Nota
    ----
    Si no se pasan `missing_fields` ni `n_random_missing_required`, el wrapper
    omite por defecto `user_id` para que el caso sea claramente inválido.
    """
    base_config = {
        "tier_temporal": "tier_1",
        "tier1_datetime_format": "utc_string_z",
        "coord_format": "numeric",
        "h3_mode": "provided_valid",
        "trip_structure": "independent",
        "omit_required_fields": list(missing_fields) if missing_fields is not None else ["user_id"],
        "n_random_missing_required": n_random_missing_required,
    }
    return _call_generator(filas=filas, seed=seed, base_config=base_config, overrides=overrides)


def make_duplicate_movement_id(
    filas: int = 8,
    *,
    seed: Optional[int] = None,
    full_rows: bool = False,
    **overrides: Any,
) -> pd.DataFrame:
    """
    Genera un dataset con duplicación controlada ligada a `movement_id`.

    Parameters
    ----------
    full_rows : bool, default=False
        Si es True, duplica filas completas. Si es False, duplica principalmente
        el campo `movement_id`.

    Cuándo usarlo
    -------------
    - Probar abortos por unicidad de `movement_id`.
    - Preparar entradas con duplicados estructurales.
    """
    base_config = {
        "tier_temporal": "tier_1",
        "tier1_datetime_format": "utc_string_z",
        "coord_format": "numeric",
        "h3_mode": "provided_valid",
        "trip_structure": "independent",
        "duplicate_mode": "full_rows" if full_rows else "movement_id_only",
        "base_fields": ["mode", "purpose"],
    }
    return _call_generator(filas=filas, seed=seed, base_config=base_config, overrides=overrides)


__all__ = [
    "make_happy_path_minimal",
    "make_happy_path_rich",
    "make_h3_derivable",
    "make_tier2_valid",
    "make_tier2_mixed_invalid",
    "make_multistage_valid",
    "make_extended_domains",
    "make_missing_required",
    "make_duplicate_movement_id",
]
