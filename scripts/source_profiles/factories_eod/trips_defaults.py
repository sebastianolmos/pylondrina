from pylondrina.types import FieldCorrespondence, ValueCorrespondence
from pylondrina.schema import TripSchema, FieldSpec, DomainSpec
from pylondrina.importing import ImportOptions

EOD_TRIPS_DEFAULT_SCHEMA = TripSchema(
    version="1.1",
    fields={
        "movement_id": FieldSpec(
            "movement_id",
            "string",
            required=True,
            constraints={"nullable": False, "unique": True},
        ),
        "user_id": FieldSpec(
            "user_id",
            "string",
            required=True,
            constraints={"nullable": False},
        ),
        "origin_longitude": FieldSpec(
            "origin_longitude",
            "float",
            required=True,
            constraints={"nullable": False, "range": {"min": -180.0, "max": 180.0}},
        ),
        "origin_latitude": FieldSpec(
            "origin_latitude",
            "float",
            required=True,
            constraints={"nullable": False, "range": {"min": -90.0, "max": 90.0}},
        ),
        "destination_longitude": FieldSpec(
            "destination_longitude",
            "float",
            required=True,
            constraints={"nullable": False, "range": {"min": -180.0, "max": 180.0}},
        ),
        "destination_latitude": FieldSpec(
            "destination_latitude",
            "float",
            required=True,
            constraints={"nullable": False, "range": {"min": -90.0, "max": 90.0}},
        ),
        "origin_h3_index": FieldSpec(
            "origin_h3_index",
            "string",
            required=True,
            constraints={"nullable": False},
        ),
        "destination_h3_index": FieldSpec(
            "destination_h3_index",
            "string",
            required=True,
            constraints={"nullable": False},
        ),
        "trip_id": FieldSpec(
            "trip_id",
            "string",
            required=True,
            constraints={"nullable": False},
        ),
        "movement_seq": FieldSpec(
            "movement_seq",
            "int",
            required=True,
            constraints={"nullable": False},
        ),
        "origin_time_utc": FieldSpec("origin_time_utc", "datetime", required=False),
        "destination_time_utc": FieldSpec("destination_time_utc", "datetime", required=False),
        "origin_time_local_hhmm": FieldSpec("origin_time_local_hhmm", "string", required=False),
        "destination_time_local_hhmm": FieldSpec("destination_time_local_hhmm", "string", required=False),
        "origin_municipality": FieldSpec("origin_municipality", "string", required=False),
        "destination_municipality": FieldSpec("destination_municipality", "string", required=False),
        "trip_weight": FieldSpec("trip_weight", "float", required=False),
        "mode_sequence": FieldSpec("mode_sequence", "string", required=False),
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

EOD_TRIPS_DEFAULT_OPTIONS = ImportOptions(
    keep_extra_fields=True,
    selected_fields=None,
    strict=False,
    strict_domains=False,
    single_stage=True,
    source_timezone=None,
)

# Importante:
# - NO mapear trip_id ni movement_seq a la misma columna que movement_id
# - trip_id y movement_seq se derivan por single_stage=True
# Así evitamos colisiones.
EOD_TRIPS_DEFAULT_FIELD_CORRESPONDENCE: FieldCorrespondence = {
    "user_id": "Persona",
    "movement_id": "Viaje",
    "origin_longitude": "OrigenCoordLon",
    "origin_latitude": "OrigenCoordLat",
    "destination_longitude": "DestinoCoordLon",
    "destination_latitude": "DestinoCoordLat",
    "origin_time_local_hhmm": "HoraIni",
    "destination_time_local_hhmm": "HoraFin",
    "origin_municipality": "ComunaOrigen",
    "destination_municipality": "ComunaDestino",
    "mode": "ModoAgregado",
    "purpose": "Proposito",
    "day_type": "TipoDia",
    "user_gender": "Sexo",
    "trip_weight": "factor_expansion",
}

EOD_TRIPS_DEFAULT_VALUE_CORRESPONDENCE: ValueCorrespondence = {
    "mode": {
        "Auto": "car",
        "Bus TS": "bus",
        "Bus no TS": "bus",
        "Metro": "metro",
        "Taxi Colectivo": "taxi",
        "Taxi": "taxi",
        "Caminata": "walk",
        "Bicicleta": "bicycle",
        "Bus TS - Bus no TS": "bus",
        "Auto - Metro": "other",
        "Bus TS - Metro": "other",
        "Bus no TS - Metro": "other",
        "Taxi Colectivo - Metro": "other",
        "Taxi - Metro": "other",
        "Otros - Metro": "other",
        "Otros - Bus TS": "other",
        "Otros - Bus TS - Metro": "other",
        "Otros": "other",
    },
    "purpose": {
        "volver a casa": "home",
        "Volver a casa": "home",
        "Al trabajo": "work",
        "Por trabajo": "work",
        "Al estudio": "education",
        "Por estudio": "education",
        "De compras": "shopping",
        "Buscar o Dejar a alguien": "errand",
        "Buscar o dejar algo": "errand",
        "Trámites": "errand",
        "De salud": "health",
        "Comer o Tomar algo": "leisure",
        "Visitar a alguien": "leisure",
        "Recreación": "leisure",
        "Otra actividad": "other",
        #"Otro" : "other",
        #"Estudio": "education",
        #"Trabajo": "work",
    },
    "day_type": {
        "Laboral": "weekday",
        "Fin de Semana": "weekend",
    },
    "user_gender": {
        "Hombre": "male",
        "Mujer": "female",
    },
}

# Opcional: ejemplos visibles de provenance recomendada
EOD_TRIPS_DEFAULT_PROVENANCE_EXAMPLE = {
    "source": {
        "name": "EOD",
        "profile": "EOD_TRIPS",
        "entity": "trips",
        "version": "EOD_STGO",
    },
    "notes": [
        "factory nivel 3 para EOD trips",
        "preprocess recomendado con joins y decodificación de catálogos",
    ],
}