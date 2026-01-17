# -------------------------
# file: pylondrina/errors.py
# -------------------------

class PylondrinaError(Exception):
    """
    Excepción base para errores del módulo Pylondrina.

    Se recomienda capturar esta excepción cuando se desea manejar cualquier error
    proveniente del módulo de manera genérica.
    """


class SchemaError(PylondrinaError):
    """
    Error asociado a la definición, construcción o uso de un esquema (TripSchema/TraceSchema).

    Ejemplos típicos:
    - El esquema declara un campo obligatorio inexistente.
    - El esquema contiene reglas contradictorias (p. ej., restricciones incompatibles).
    - Se solicita validar un dataset con un esquema no compatible.
    """


class ImportError(PylondrinaError):
    """
    Error durante la importación/conversión de una fuente externa hacia el formato Golondrina.

    Ejemplos típicos:
    - No se puede construir el dataset por ausencia de información mínima.
    - La correspondencia de campos no permite resolver campos obligatorios.
    - Tipos de datos de entrada imposibilitan la conversión (p. ej., timestamps ilegibles).

    Incluye errores asociados a correspondencias (campos/valores) durante importación y normalización.
    """


class ValidationError(PylondrinaError):
    """
    Error de validación de conformidad respecto de un esquema Golondrina.

    Ejemplos típicos:
    - Falta un campo obligatorio.
    - Valores violan restricciones de tipo o formato.
    - Reglas temporales/espaciales mínimas no se cumplen (cuando la operación lo exige).
    """


class InferenceError(PylondrinaError):
    """
    Error al inferir viajes desde datos de trazas/trayectorias.

    Ejemplos típicos:
    - Los datos de entrada no cumplen el esquema mínimo de trazas.
    - Parámetros del algoritmo imposibilitan inferir viajes (p. ej., sin puntos suficientes).
    - El resultado no puede producir un conjunto de viajes conforme al esquema de viajes.
    """


class ExportError(PylondrinaError):
    """
    Error durante la exportación/serialización de datasets (p. ej., Parquet, GeoJSON, Flowmap).

    Ejemplos típicos:
    - Falta información necesaria para el formato de salida (p. ej., geometrías).
    - Columnas requeridas por el exportador no están presentes.
    - Fallos de escritura en el destino (p. ej., permisos o ruta inválida).
    """
