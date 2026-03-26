from __future__ import annotations

from pylondrina.importing import ImportOptions
from pylondrina.schema import DomainSpec, FieldSpec, TripSchema
from pylondrina.types import FieldCorrespondence, ValueCorrespondence


# -----------------------------------------------------------------------------
# Constantes visibles y reutilizables
# -----------------------------------------------------------------------------

DEFAULT_EOD_SOURCE_CRS = "EPSG:5361"
DEFAULT_EOD_TARGET_CRS = "EPSG:4326"

EOD_TRIP_FACTOR_COLS = [
    "FactorLaboralNormal",
    "FactorSabadoNormal",
    "FactorDomingoNormal",
    "FactorLaboralEstival",
    "FactorFindesemanaEstival",
]

EOD_STAGE_LOOKUP_TABLES = {
    "Autopista": "Autopista.csv",
    "ComunaOrigen": "Comuna.csv",
    "ComunaDestino": "Comuna.csv",
    "SectorOrigen": "Sector.csv",
    "SectorDestino": "Sector.csv",
    "Modo": "Modo.csv",
    "FormaPago": "Formapago.csv",
    "Estaciona": "Estaciona.csv",
    "RecorridoTransantiago": "RecorridoTransantiago.csv",
    "EstacionTrenIni": "EstacionTren.csv",
    "EstacionTrenFin": "EstacionTren.csv",
    "EstacionMetroIni": "EstacionMetro.csv",
    "EstacionMetroFin": "EstacionMetro.csv",
    "HorarioMetro": "HorarioMetro.csv",
    "EstacionMetroCambio": "EstacionMetroCambio.csv",
    "PropiedadBicicleta": "PropiedadBicicleta.csv",
    "UsaCiclovia": "UsaCiclovia.csv",
    "CirculacionBicicleta": "CirculacionBicicleta.csv",
    "EstacionaBicicleta": "EstacionaBicicleta.csv",
    "ModoEstacionaBicicleta": "ModoestacionaBicicleta.csv",
    "UsoHabitualBicicleta": "UsoHabitualBicicleta.csv",
}

EOD_PERSON_LOOKUP_TABLES = {
    "Sexo": "Sexo.csv",
    "Relacion": "Relacion.csv",
    "LicenciaConducir": "LicenciaConducir.csv",
    "PaseEscolar": "PaseEscolar.csv",
    "AdultoMayor": "AdultoMayor.csv",
    "Estudios": "Estudios.csv",
    "Actividad": "Actividad.csv",
    "Ocupacion": "Ocupacion.csv",
    "ActividadEmpresa": "ActividadEmpresa.csv",
    "JornadaTrabajo": "JornadaTrabajo.csv",
    "DondeEstudia": "Donde Estudia.csv",
    "MedioViajeRestricion": "MedioViajeRestriccion.csv",
    "ConoceTransantiago": "ConoceSantiago.csv",
    "NoUsaTransantiago": "NoUsaTransantiago.csv",
    "Discapacidad": "Discapacidad.csv",
    "TieneIngresos": "TieneIngresos.csv",
    "TramoIngreso": "TramoIngreso.csv",
    "TramoIngresoFinal": "TramoIngreso.csv",
    "IngresoImputado": "IngresoImputado.csv",
}

EOD_HOUSEHOLD_LOOKUP_TABLES = {
    "TipoDia": "TipoDia.csv",
    "Temporada": "Temporada.csv",
    "Propiedad": "Propiedad.csv",
}

EOD_PERSON_COLS_USEFUL_FOR_STAGES = [
    "Hogar",
    "Persona",
    "AnoNac",
    "Sexo",
    "Actividad",
    "Ocupacion",
    "LicenciaConducir",
    "PaseEscolar",
    "AdultoMayor",
    "TieneIngresos",
    "TramoIngreso",
]

EOD_HOUSEHOLD_COLS_USEFUL_FOR_STAGES = [
    "Hogar",
    "Fecha",
    "TipoDia",
    "Temporada",
    "NumVeh",
    "NumBicAdulto",
    "NumBicNino",
    "IngresoHogar",
    "Comuna",
]

EOD_TRIP_COLS_USEFUL_FOR_STAGES = [
    "Hogar",
    "Persona",
    "Viaje",
    "Etapas",
    "Proposito",
    "PropositoAgregado",
    "ActividadDestino",
    "ModoAgregado",
    "HoraIni",
    "HoraFin",
    "TiempoViaje",
    "TiempoMedio",
    "Periodo",
    "FactorLaboralNormal",
    "FactorSabadoNormal",
    "FactorDomingoNormal",
    "FactorLaboralEstival",
    "FactorFindesemanaEstival",
]

EOD_TRIP_CONTEXT_LOOKUP_MAP = {
    "trip_Proposito": "Proposito.csv",
    "trip_PropositoAgregado": "PropositoAgregado.csv",
    "trip_ActividadDestino": "ActividadDestino.csv",
    "trip_ModoAgregado": "ModoAgregado.csv",
    "trip_Periodo": "Periodo.csv",
    "trip_TiempoMedio": "TiempoMedio.csv",
}


# -----------------------------------------------------------------------------
# Schema recomendado por defecto
# -----------------------------------------------------------------------------

EOD_STAGES_DEFAULT_SCHEMA = TripSchema(
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
            constraints={"nullable": False, "range": {"min": 1}},
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
        "user_gender": FieldSpec(
            "user_gender",
            "categorical",
            required=False,
            domain=DomainSpec(
                values=["female", "male", "other", "unknown"],
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

EOD_STAGES_DEFAULT_OPTIONS = ImportOptions(
    keep_extra_fields=True,
    selected_fields=None,
    strict=False,
    strict_domains=False,
    single_stage=False,
    source_timezone=None,
)

# Sin colisiones:
# - trip_id <- Viaje
# - movement_id <- movement_id_src (se crea en preprocess)
# - movement_seq <- NumeroEtapa
EOD_STAGES_DEFAULT_FIELD_CORRESPONDENCE: FieldCorrespondence = {
    "user_id": "Persona",
    "trip_id": "Viaje",
    "movement_id": "movement_id_src",
    "movement_seq": "NumeroEtapa",
    "origin_longitude": "OrigenCoordLon",
    "origin_latitude": "OrigenCoordLat",
    "destination_longitude": "DestinoCoordLon",
    "destination_latitude": "DestinoCoordLat",
    "origin_municipality": "ComunaOrigen",
    "destination_municipality": "ComunaDestino",
    "mode": "Modo",
    "purpose": "trip_Proposito",
    "day_type": "TipoDia",
    "user_gender": "Sexo",
    "trip_weight": "factor_expansion",
}

EOD_STAGES_DEFAULT_VALUE_CORRESPONDENCE: ValueCorrespondence = {
    "mode": {
        "Auto Chofer": "car",
        "Auto Acompañante": "car",
        "Bus alimentador": "bus",
        "Bus troncal": "bus",
        "Bus institucional": "bus",
        "Bus interurbano o rural": "bus",
        "Bus urbano con pago al conductor (Metrobus y otros)": "bus",
        "Metro": "metro",
        "Taxi colectivo": "taxi",
        "Taxi o radiotaxi": "taxi",
        "Enteramente a pie": "walk",
        "Bicicleta": "bicycle",
        "Motocicleta": "motorcycle",
        "Motocicleta Acompañante": "motorcycle",
        "Tren": "train",
        "Servicio Informal": "other",
        "Furgón escolar, como pasajero": "other",
        "Furgón escolar, como chofer o acompañante": "other",
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

EOD_STAGES_DEFAULT_PROVENANCE_EXAMPLE = {
    "source": {
        "name": "EOD",
        "profile": "EOD_STAGES",
        "entity": "stages",
        "version": "EOD_STGO",
    },
    "notes": [
        "factory nivel 3 para EOD stages",
        "preprocess recomendado con joins, decodificación y XY -> WGS84",
    ],
}


