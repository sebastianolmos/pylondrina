# pylondrina
Modulo para el manejo de datos de viajes en el formato unificado Golondrina

## Estructura del paquete Pylondrina (API v1.0.2)

- **`pylondrina/`**: Módulo principal para trabajar con datos de movilidad en el formato unificado Golondrina (importación, validación, transformaciones, inferencia y exportación), manteniendo trazabilidad mediante metadatos y reportes.
    - **`__init__.py`**: Punto de entrada del paquete; expone la interfaz pública y agrupa los componentes principales para facilitar el uso.
    - **`types.py`**: Definiciones de tipos y convenciones semánticas utilizadas en toda la API para asegurar consistencia.
    - **`errors.py`**: Definición de errores del dominio y excepciones del módulo para manejo controlado de fallas.
    - **`schema.py`**: Especificación del contrato de datos (campos, tipos, dominios y reglas) para interpretar y validar viajes y trazas.
    - **`datasets.py`**: Estructuras de datos que representan colecciones de viajes, flujos y trazas, incluyendo metadatos y contexto.
    - **`reports.py`**: Estructuras de reporte para documentar resultados, advertencias, errores y resúmenes de procesos (importación, validación, inferencia, consistencia y agregaciones).
    - **`importing.py`**: Lógica de conversión desde tablas externas hacia el formato Golondrina, incluyendo estandarización y generación de trazabilidad.
    - **`validation.py`**: Reglas y procedimientos para verificar conformidad de los datos frente al contrato definido por el esquema.
- **`pylondrina/sources/`**: Submódulo para adaptar fuentes específicas al flujo de importación genérico, encapsulando particularidades de datasets reales.
    - **`__init__.py`**: Entrada del submódulo; organiza y expone utilidades relacionadas con fuentes.
    - **`profile.py`**: Configuración declarativa de una fuente, incluyendo preprocesamiento para preparar los datos antes de la importación estándar.
    - **`registry.py`**: Mecanismo para registrar y recuperar configuraciones de fuentes disponibles.
    - **`helpers.py`**: Capa de conveniencia que aplica la configuración de una fuente y la integra con el proceso de importación.
- **`pylondrina/transforms/`**: Transformaciones post-importación sobre datos ya estandarizados, para preparar análisis, consolidación, enriquecimiento y agregación.
    - **`__init__.py`**: Entrada del submódulo; agrupa transformaciones y facilita imports consistentes.
    - **`filtering.py`**: Filtrado espacial, temporal y por atributos para construir subconjuntos reproducibles de datos.
    - **`flows.py`**: Agregación de viajes a representaciones de flujo origen–destino, con opciones de segmentación y agregación temporal/espacial.
    - **`concat.py`**: Combinación e incrementalidad de conjuntos de viajes ya estandarizados, con control de compatibilidad y consistencia.
    - **`enrich.py`**: Enriquecimiento de viajes incorporando atributos externos mediante unión controlada, evitando duplicaciones y dejando evidencia del proceso.
    - **`traces.py`**: Diagnósticos y estadísticas descriptivas sobre trazas para apoyar procesos de inferencia y evaluación de calidad.
- **`pylondrina/inference/`**: Inferencia de viajes a partir de trazas, con parametrización y trazabilidad del método aplicado (v1 minimal, extensible a futuro).
    - **`__init__.py`**: Entrada del submódulo; organiza la interfaz asociada a inferencia.
    - **`trips_from_traces.py`**: Lógica para derivar viajes a partir de trazas, registrando parámetros y resultados del proceso.
- **`pylondrina/export/`**: Exportación de resultados a formatos externos para interoperabilidad y visualización (ruta práctica de v1).
    - **`__init__.py`**: Entrada del submódulo; organiza la interfaz de exportación.
    - **`flowmap_blue.py`**: Generación de archivos compatibles con una herramienta externa de visualización de flujos, incluyendo metadatos opcionales.