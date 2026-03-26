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
        "trip_weight": FieldSpec("trip_weight", "float", required=False),
        "origin_municipality": FieldSpec("origin_municipality", "string", required=False),
        "destination_municipality": FieldSpec("destination_municipality", "string", required=False),
        "origin_stop_code": FieldSpec("origin_stop_code", "string", required=False),
        "destination_stop_code": FieldSpec("destination_stop_code", "string", required=False),
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
        "movement_seq",
    ],
)


def make_adatrap_stages_default_schema(adatrap_domains_yaml: str | Path) -> TripSchema:
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
        "op_etapa": FieldSpec(
            "op_etapa",
            "categorical",
            required=False,
            domain=DomainSpec(
                values=merge_domains(
                    domains,
                    "op_1era_etapa",
                    "op_2da_etapa",
                    "op_3era_etapa",
                    "op_4ta_etapa",
                ),
                extendable=True,
            ),
        ),
        "linea_metro_subida_etapa": FieldSpec(
            "linea_metro_subida_etapa",
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
        "linea_metro_bajada_etapa": FieldSpec(
            "linea_metro_bajada_etapa",
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
    }

    stage_fields = {f: fs for f, fs in BASE_GOLONDRINA_IMPORT_SCHEMA.fields.items() if f != "trip_id"}
    stage_required = [r for r in BASE_GOLONDRINA_IMPORT_SCHEMA.required if r != "trip_id"]

    return TripSchema(
        version=BASE_GOLONDRINA_IMPORT_SCHEMA.version,
        fields={**stage_fields, **extra_fields},
        required=list(stage_required),
    )


ADATRAP_STAGES_DEFAULT_OPTIONS = ImportOptions(
    keep_extra_fields=True,
    selected_fields=None,
    strict=False,
    strict_domains=False,
    single_stage=False,
    source_timezone="America/Santiago",
)

ADATRAP_STAGES_DEFAULT_FIELD_CORRESPONDENCE: FieldCorrespondence = {
    "user_id": "id",
    "movement_seq": "numero_etapa",

    "origin_longitude": "subida_lon",
    "origin_latitude": "subida_lat",
    "destination_longitude": "bajada_lon",
    "destination_latitude": "bajada_lat",

    "origin_time_utc": "tiemposubida_etapa",
    "destination_time_utc": "tiempobajada_etapa",

    "origin_municipality": "comunasubida",
    "destination_municipality": "comunabajada",
    "origin_stop_code": "paraderosubida_etapa",
    "destination_stop_code": "paraderobajada_etapa",

    "mode": "tipotransporte_etapa",
    "purpose": "proposito",
    "day_type": "tipodia",
    "time_period": "periodomediodeviaje",

    "trip_weight": "factorexpansion",

    "ultimaetapaconbajada": "ultimaetapaconbajada",
    "tipodiamediodeviaje": "tipodiamediodeviaje",
    "tipo_corte_etapa_viaje": "tipo_corte_etapa_viaje",
    "periodosubida": "periodosubida",
    "periodobajada": "periodobajada",
    "mediahora": "mediahora",
    "mediahoramediodeviaje": "mediahoramediodeviaje",
    "op_etapa": "op_etapa",
    "linea_metro_subida_etapa": "linea_metro_subida_etapa",
    "linea_metro_bajada_etapa": "linea_metro_bajada_etapa",
}

def make_adatrap_stages_default_value_correspondence(
    adatrap_domains_yaml: str | Path,
) -> ValueCorrespondence:
    domains_raw = load_yaml_file(adatrap_domains_yaml)
    domains = clean_domain_dict(domains_raw)

    return {
        "mode": {
            "BUS": "bus",
            "METRO": "metro",
            "METROTREN": "train",
            "ZP": "other",
        },
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

ADATRAP_STAGES_DEFAULT_PROVENANCE_EXAMPLE = {
    "source": {
        "name": "ADATRAP",
        "profile": "ADATRAP_STAGES",
        "entity": "stages",
        "version": "perfil_semana",
    },
    "notes": [
        "factory nivel 3 para ADATRAP stages",
        "preprocess wide->long usando stage_layout.yaml y domains.yaml",
    ],
}

# -------------------------------------------------------------------------
# Objetos custom independientes de los defaults
# -------------------------------------------------------------------------

def make_adatrap_stages_custom_schema(adatrap_domains_yaml: str | Path) -> TripSchema:
    domains_raw = load_yaml_file(adatrap_domains_yaml)
    domains = clean_domain_dict(domains_raw)

    return TripSchema(
        version="1.1-adatrap-stages-custom",
        fields={
            "movement_id": FieldSpec("movement_id", "string", required=True),
            "user_id": FieldSpec("user_id", "string", required=True),
            "movement_seq": FieldSpec("movement_seq", "int", required=True),

            "origin_longitude": FieldSpec("origin_longitude", "float", required=True),
            "origin_latitude": FieldSpec("origin_latitude", "float", required=True),
            "destination_longitude": FieldSpec("destination_longitude", "float", required=True),
            "destination_latitude": FieldSpec("destination_latitude", "float", required=True),
            "origin_h3_index": FieldSpec("origin_h3_index", "string", required=True),
            "destination_h3_index": FieldSpec("destination_h3_index", "string", required=True),

            "origin_time_utc": FieldSpec("origin_time_utc", "datetime", required=False),
            "destination_time_utc": FieldSpec("destination_time_utc", "datetime", required=False),
            "origin_stop_code": FieldSpec("origin_stop_code", "string", required=False),
            "destination_stop_code": FieldSpec("destination_stop_code", "string", required=False),
            "origin_municipality": FieldSpec("origin_municipality", "string", required=False),
            "destination_municipality": FieldSpec("destination_municipality", "string", required=False),

            "mode": FieldSpec(
                "mode",
                "categorical",
                required=False,
                domain=DomainSpec(values=["bus", "metro", "train", "other"], extendable=True),
            ),
            "purpose": FieldSpec(
                "purpose",
                "categorical",
                required=False,
                domain=DomainSpec(values=["home", "work", "other"], extendable=True),
            ),
            "day_type": FieldSpec(
                "day_type",
                "categorical",
                required=False,
                domain=DomainSpec(values=["weekday", "weekend"], extendable=True),
            ),
            "time_period": FieldSpec(
                "time_period",
                "categorical",
                required=False,
                domain=DomainSpec(
                    values=build_adatrap_time_period_mapping(domains["periodomediodeviaje"]).values(),
                    extendable=True,
                ),
            ),
            "trip_weight": FieldSpec("trip_weight", "float", required=False),
        },
        required=[
            "movement_id",
            "user_id",
            "movement_seq",
            "origin_longitude",
            "origin_latitude",
            "destination_longitude",
            "destination_latitude",
            "origin_h3_index",
            "destination_h3_index",
        ],
    )

ADATRAP_STAGES_CUSTOM_OPTIONS = ImportOptions(
    keep_extra_fields=False,
    selected_fields=[
        "movement_id",
        "user_id",
        "movement_seq",
        "origin_longitude",
        "origin_latitude",
        "destination_longitude",
        "destination_latitude",
        "origin_h3_index",
        "destination_h3_index",
        "origin_time_utc",
        "destination_time_utc",
        "origin_stop_code",
        "destination_stop_code",
        "origin_municipality",
        "destination_municipality",
        "mode",
        "purpose",
        "day_type",
        "time_period",
        "trip_weight",
    ],
    strict=False,
    strict_domains=False,
    single_stage=False,
    source_timezone="America/Santiago",
)

ADATRAP_STAGES_CUSTOM_FIELD_CORRESPONDENCE: FieldCorrespondence = {
    "user_id": "id",
    "movement_seq": "numero_etapa",

    "origin_longitude": "subida_lon",
    "origin_latitude": "subida_lat",
    "destination_longitude": "bajada_lon",
    "destination_latitude": "bajada_lat",

    "origin_time_utc": "tiemposubida_etapa",
    "destination_time_utc": "tiempobajada_etapa",

    "origin_municipality": "comunasubida",
    "destination_municipality": "comunabajada",
    "origin_stop_code": "paraderosubida_etapa",
    "destination_stop_code": "paraderobajada_etapa",

    "mode": "tipotransporte_etapa",
    "purpose": "proposito",
    "day_type": "tipodia",
    "time_period": "periodomediodeviaje",
    "trip_weight": "factorexpansion",
}

def make_adatrap_stages_custom_value_correspondence(
    adatrap_domains_yaml: str | Path,
) -> ValueCorrespondence:
    domains_raw = load_yaml_file(adatrap_domains_yaml)
    domains = clean_domain_dict(domains_raw)

    return {
        "mode": {
            "BUS": "bus",
            "METRO": "metro",
            "METROTREN": "train",
            "ZP": "other",
        },
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

ADATRAP_STAGES_CUSTOM_PROVENANCE_EXAMPLE = {
    "source": {
        "name": "ADATRAP",
        "profile": "ADATRAP_STAGES_CUSTOM",
        "entity": "stages",
        "version": "perfil_semana",
    },
    "notes": [
        "factory nivel 3 ADATRAP stages custom",
        "schema y mappings definidos explícitamente sin depender de defaults",
    ],
}