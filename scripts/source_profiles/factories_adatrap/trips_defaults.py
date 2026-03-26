from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from pylondrina.importing import ImportOptions
from pylondrina.schema import TripSchema, FieldSpec, DomainSpec
from pylondrina.types import FieldCorrespondence, ValueCorrespondence


def load_yaml_file(path: str | Path) -> dict[str, Any]:
    path = Path(path)
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def clean_domain_values(values: list[Any]) -> list[str]:
    out = []
    seen = set()

    for v in values:
        s = str(v).strip()
        if not s:
            continue
        if s not in seen:
            seen.add(s)
            out.append(s)

    return out


def clean_domain_dict(domains_raw: dict[str, list[Any]]) -> dict[str, list[str]]:
    return {k: clean_domain_values(v) for k, v in domains_raw.items()}


def merge_domains(domains: dict[str, list[str]], *keys: str) -> list[str]:
    merged = []
    seen = set()

    for key in keys:
        for v in domains.get(key, []):
            if v not in seen:
                seen.add(v)
                merged.append(v)

    return merged


def build_adatrap_time_period_mapping(domain_values: list[str]) -> dict[str, str]:
    mapping = {}

    for v in domain_values:
        s = v.upper().strip()

        if "PRE NOCTURNO" in s:
            mapping[v] = "evening"
        elif "NOCTURNO" in s:
            mapping[v] = "night"
        elif "MANANA" in s:
            mapping[v] = "morning"
        elif "MEDIODIA" in s:
            mapping[v] = "midday"
        elif "TARDE" in s:
            mapping[v] = "afternoon"

    return mapping


BASE_GOLONDRINA_IMPORT_SCHEMA = TripSchema(
    version="1.1",
    fields={
        "movement_id": FieldSpec("movement_id", "string", required=True),
        "user_id": FieldSpec("user_id", "string", required=True),
        "origin_longitude": FieldSpec("origin_longitude", "float", required=True),
        "origin_latitude": FieldSpec("origin_latitude", "float", required=True),
        "destination_longitude": FieldSpec("destination_longitude", "float", required=True),
        "destination_latitude": FieldSpec("destination_latitude", "float", required=True),
        "origin_h3_index": FieldSpec("origin_h3_index", "string", required=True),
        "destination_h3_index": FieldSpec("destination_h3_index", "string", required=True),
        "origin_time_utc": FieldSpec("origin_time_utc", "datetime", required=False),
        "destination_time_utc": FieldSpec("destination_time_utc", "datetime", required=False),
        "trip_id": FieldSpec("trip_id", "string", required=True),
        "movement_seq": FieldSpec("movement_seq", "int", required=True),

        "mode": FieldSpec(
            "mode",
            "categorical",
            required=False,
            domain=DomainSpec(
                values=[
                    "walk", "bicycle", "scooter", "motorcycle", "car",
                    "taxi", "ride_hailing", "bus", "metro", "train", "other"
                ],
                extendable=True,
            ),
        ),
        "purpose": FieldSpec(
            "purpose",
            "categorical",
            required=False,
            domain=DomainSpec(
                values=[
                    "home", "work", "education", "shopping",
                    "errand", "health", "leisure", "transfer", "other"
                ],
                extendable=True,
            ),
        ),
        "day_type": FieldSpec(
            "day_type",
            "categorical",
            required=False,
            domain=DomainSpec(
                values=["weekday", "weekend", "holiday"],
                extendable=True,
            ),
        ),
        "time_period": FieldSpec(
            "time_period",
            "categorical",
            required=False,
            domain=DomainSpec(
                values=["night", "morning", "midday", "afternoon", "evening"],
                extendable=True,
            ),
        ),
        "user_gender": FieldSpec(
            "user_gender",
            "categorical",
            required=False,
            domain=DomainSpec(
                values=["female", "male", "other", "unknown"],
                extendable=True,
            ),
        ),
        "user_age_group": FieldSpec(
            "user_age_group",
            "categorical",
            required=False,
            domain=DomainSpec(
                values=["0-14", "15-24", "25-34", "35-44", "45-54", "55-64", "65-plus", "unknown"],
                extendable=True,
            ),
        ),
        "income_quintile": FieldSpec(
            "income_quintile",
            "categorical",
            required=False,
            domain=DomainSpec(
                values=["1", "2", "3", "4", "5", "unknown"],
                extendable=True,
            ),
        ),

        "origin_time_local_hhmm": FieldSpec("origin_time_local_hhmm", "string", required=False),
        "destination_time_local_hhmm": FieldSpec("destination_time_local_hhmm", "string", required=False),
        "origin_municipality": FieldSpec("origin_municipality", "string", required=False),
        "destination_municipality": FieldSpec("destination_municipality", "string", required=False),
        "trip_weight": FieldSpec("trip_weight", "float", required=False),
        "mode_sequence": FieldSpec("mode_sequence", "string", required=False),
    },
    required=[
        "movement_id",
        "user_id",
        "origin_longitude",
        "origin_latitude",
        "destination_longitude",
        "destination_latitude",
        "origin_h3_index",
        "destination_h3_index",
        "trip_id",
        "movement_seq",
    ],
)


def make_adatrap_trips_default_schema(adatrap_domains_yaml: str | Path) -> TripSchema:
    domains_raw = load_yaml_file(adatrap_domains_yaml)
    domains = clean_domain_dict(domains_raw)

    extra_fields = {
        "ultimaetapaconbajada": FieldSpec(
            "ultimaetapaconbajada",
            "categorical",
            required=False,
            domain=DomainSpec(values=domains["ultimaetapaconbajada"], extendable=True),
        ),
        "tipodiamediodeviaje": FieldSpec(
            "tipodiamediodeviaje",
            "categorical",
            required=False,
            domain=DomainSpec(values=domains["tipodiamediodeviaje"], extendable=True),
        ),
        "tipo_corte_etapa_viaje": FieldSpec(
            "tipo_corte_etapa_viaje",
            "categorical",
            required=False,
            domain=DomainSpec(values=domains["tipo_corte_etapa_viaje"], extendable=True),
        ),
        "periodosubida": FieldSpec(
            "periodosubida",
            "categorical",
            required=False,
            domain=DomainSpec(values=domains["periodosubida"], extendable=True),
        ),
        "periodobajada": FieldSpec(
            "periodobajada",
            "categorical",
            required=False,
            domain=DomainSpec(values=domains["periodobajada"], extendable=True),
        ),
        "periodomediodeviaje": FieldSpec(
            "periodomediodeviaje",
            "categorical",
            required=False,
            domain=DomainSpec(values=domains["periodomediodeviaje"], extendable=True),
        ),
        "mediahora": FieldSpec(
            "mediahora",
            "categorical",
            required=False,
            domain=DomainSpec(values=domains["mediahora"], extendable=True),
        ),
        "mediahoramediodeviaje": FieldSpec(
            "mediahoramediodeviaje",
            "categorical",
            required=False,
            domain=DomainSpec(values=domains["mediahoramediodeviaje"], extendable=True),
        ),
        "linea_metro_subida": FieldSpec(
            "linea_metro_subida",
            "categorical",
            required=False,
            domain=DomainSpec(
                values=merge_domains(
                    domains,
                    "linea_metro_subida_1",
                    "linea_metro_subida_2",
                    "linea_metro_subida_3",
                    "linea_metro_subida_4",
                ),
                extendable=True,
            ),
        ),
        "linea_metro_bajada": FieldSpec(
            "linea_metro_bajada",
            "categorical",
            required=False,
            domain=DomainSpec(
                values=merge_domains(
                    domains,
                    "linea_metro_bajada_1",
                    "linea_metro_bajada_2",
                    "linea_metro_bajada_3",
                    "linea_metro_bajada_4",
                ),
                extendable=True,
            ),
        ),
        "origin_stop_code": FieldSpec("origin_stop_code", "string", required=False),
        "destination_stop_code": FieldSpec("destination_stop_code", "string", required=False),
    }

    return TripSchema(
        version=BASE_GOLONDRINA_IMPORT_SCHEMA.version,
        fields={**BASE_GOLONDRINA_IMPORT_SCHEMA.fields, **extra_fields},
        required=list(BASE_GOLONDRINA_IMPORT_SCHEMA.required),
    )


ADATRAP_TRIPS_DEFAULT_OPTIONS = ImportOptions(
    keep_extra_fields=True,
    selected_fields=None,
    strict=False,
    strict_domains=False,
    single_stage=True,
    source_timezone="America/Santiago",
)

# Sin colisiones:
# NO mapear trip_id ni movement_seq a la misma columna que movement_id.
# single_stage=True deriva trip_id=movement_id y movement_seq=0.
ADATRAP_TRIPS_DEFAULT_FIELD_CORRESPONDENCE: FieldCorrespondence = {
    "user_id": "id",
    "origin_longitude": "subida_lon",
    "origin_latitude": "subida_lat",
    "destination_longitude": "bajada_lon",
    "destination_latitude": "bajada_lat",
    "origin_time_utc": "tiemposubida",
    "destination_time_utc": "tiempobajada",
    "origin_municipality": "comunasubida",
    "destination_municipality": "comunabajada",
    "origin_stop_code": "paraderosubida",
    "destination_stop_code": "paraderobajada",
    "purpose": "proposito",
    "day_type": "tipodia",
    "time_period": "periodomediodeviaje",
    "trip_weight": "factorexpansion",
    "linea_metro_subida": "linea_metro_subida",
    "linea_metro_bajada": "linea_metro_bajada",
    "ultimaetapaconbajada": "ultimaetapaconbajada",
    "tipodiamediodeviaje": "tipodiamediodeviaje",
    "tipo_corte_etapa_viaje": "tipo_corte_etapa_viaje",
    "periodosubida": "periodosubida",
    "periodobajada": "periodobajada",
    "mediahora": "mediahora",
    "mediahoramediodeviaje": "mediahoramediodeviaje",
}

def make_adatrap_trips_default_value_correspondence(
    adatrap_domains_yaml: str | Path,
) -> ValueCorrespondence:
    domains_raw = load_yaml_file(adatrap_domains_yaml)
    domains = clean_domain_dict(domains_raw)

    return {
        "purpose": {
            "HOGAR": "home",
            "TRABAJO": "work",
            "OTROS": "other",
            "MENOS1MINUTO": "other",
            "SINBAJADA": "other",
        },
        "day_type": {
            "LABORAL": "weekday",
            "SABADO": "weekend",
            "DOMINGO": "weekend",
        },
        "time_period": build_adatrap_time_period_mapping(domains["periodomediodeviaje"]),
    }

ADATRAP_TRIPS_DEFAULT_PROVENANCE_EXAMPLE = {
    "source": {
        "name": "ADATRAP",
        "profile": "ADATRAP_TRIPS",
        "entity": "trips",
        "version": "perfil_semana",
    },
    "notes": [
        "factory nivel 3 para ADATRAP trips",
        "preprocess resume viaje desde primera subida y última bajada válida",
        "usa stage_layout.yaml y domains.yaml",
    ],
}

