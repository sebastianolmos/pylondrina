from __future__ import annotations

import copy
import json
import shutil
import tempfile
import uuid
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Literal, Mapping, Optional, Sequence, Tuple, Union

import pandas as pd

from pylondrina.datasets import TripDataset
from pylondrina.errors import ExportError, ValidationError
from pylondrina.issues.catalog_read_trips import READ_TRIPS_ISSUES
from pylondrina.issues.catalog_write_trips import WRITE_TRIPS_ISSUES
from pylondrina.issues.core import emit_and_maybe_raise, emit_issue
from pylondrina.reports import Issue, OperationReport
from pylondrina.schema import DomainSpec, FieldSpec, TripSchema, TripSchemaEffective

WriteMode = Literal["error_if_exists", "overwrite"]
PathLike = Union[str, Path]
StorageFormat = Literal["parquet"]
ParquetCompression = Optional[Literal["snappy", "gzip", "zstd", "brotli", "none"]]

EXCEPTION_MAP_WRITE = {
    "validation": ValidationError,
    "export": ExportError,
}
EXCEPTION_MAP_READ = {
    "export": ExportError,
}

_REQUIRED_SIDECAR_TOP_LEVEL = {
    "dataset_type",
    "format",
    "layout_version",
    "storage",
    "files",
    "schema",
    "provenance",
    "metadata",
}

_GOLONDRINA_ARTIFACT_SUFFIX = ".golondrina"
_SUPPORTED_STORAGE_FORMATS = {"parquet"}
_SUPPORTED_PARQUET_COMPRESSIONS = {"snappy", "gzip", "zstd", "brotli", "none", None}


@dataclass(frozen=True)
class WriteTripsOptions:
    """
    Opciones efectivas para persistir un TripDataset de trips.

    Parameters
    ----------
    mode : {"error_if_exists", "overwrite"}, default="error_if_exists"
        Política cuando el directorio destino ya existe.
    require_validated : bool, default=True
        Si True, exige `metadata["is_validated"] is True` antes de escribir.
    storage_format : {"parquet"}, default="parquet"
        Backend de persistencia tabular. En v1.1 solo se soporta Parquet.
    parquet_compression : {"snappy", "gzip", "zstd", "brotli", "none", None}, default="snappy"
        Compresión efectiva usada al escribir `trips.parquet`.
    normalize_artifact_dir : bool, default=True
        Si True, normaliza el directorio root del artefacto para que termine en
        `.golondrina`. Si False, usa el path entregado tal cual.
    """

    mode: WriteMode = "error_if_exists"
    require_validated: bool = True
    storage_format: StorageFormat = "parquet"
    parquet_compression: ParquetCompression = "snappy"
    normalize_artifact_dir: bool = True


@dataclass(frozen=True)
class ReadTripsOptions:
    """
    Opciones efectivas para reconstruir un TripDataset desde persistencia formal.

    Parameters
    ----------
    schema : TripSchema, optional
        Esquema a usar al reconstruir el dataset. Si se entrega, tiene precedencia
        sobre el snapshot persistido en el sidecar.
    strict : bool, default=False
        Si True, inconsistencias recuperables del sidecar/layout se tratan como fatales.
    keep_metadata : bool, default=True
        Si True, agrega un evento `read_trips` en `metadata["events"]`.
    """

    schema: Optional[TripSchema] = None
    strict: bool = False
    keep_metadata: bool = True


@dataclass(frozen=True)
class TripsArtifactPaths:
    """Rutas resueltas del layout formal de trips."""

    root_dir: Path
    data_path: Path
    sidecar_path: Path
    legacy_sidecar_path: Path


@dataclass(frozen=True)
class WriteResolvedState:
    """Estado resuelto para materializar write_trips."""

    dataset_id: str
    artifact_id: str
    dataset_id_status: str
    metadata_for_persist: Dict[str, Any]
    sidecar_payload: Dict[str, Any]
    files_written: List[str]
    issues: List[Issue]


@dataclass(frozen=True)
class ReadSchemaState:
    """Resultado de resolver schema/schema_effective en read_trips."""

    schema: TripSchema
    schema_effective: TripSchemaEffective
    schema_source: str
    schema_mismatch: bool
    issues: List[Issue]


@dataclass(frozen=True)
class LoadedIdentityState:
    """Estado final de identidad y metadata tras cargar el sidecar."""

    metadata: Dict[str, Any]
    dataset_id: str
    dataset_id_status: str
    artifact_id: Optional[str]
    artifact_id_status: str
    issues: List[Issue]


# -----------------------------------------------------------------------------
# Funciones públicas
# -----------------------------------------------------------------------------

def write_trips(
    trips: TripDataset,
    path: PathLike,
    *,
    options: Optional[WriteTripsOptions] = None,
) -> OperationReport:
    """
    Persiste un TripDataset como artefacto formal de trips (v1.1).

    Parameters
    ----------
    trips : TripDataset
        Dataset de trips a persistir.
    path : PathLike
        Directorio destino del artefacto formal. Si
        `options.normalize_artifact_dir=True` y el nombre no termina en
        `.golondrina`, se normaliza automáticamente al sufijo canónico.
    options : WriteTripsOptions, optional
        Opciones efectivas de escritura. Si None, se usan defaults.

    Returns
    -------
    OperationReport
        Reporte estructurado de la operación.
    """
    # Se inicializa el acumulador de evidencia y se fijan las options efectivas.
    issues: List[Issue] = []
    options_eff = options or WriteTripsOptions()
    
    # Se normaliza primero el root efectivo del artefacto para que todo el pipeline
    # trabaje sobre el mismo directorio canónico.
    write_root = _normalize_trips_artifact_root_for_write(
        path,
        normalize_artifact_dir=options_eff.normalize_artifact_dir,
    )
    parameters = _options_to_write_parameters(path=write_root, options=options_eff)

    # Se valida el contrato de write antes de tocar disco.
    _validate_write_contract(
        trips,
        write_root,
        options_eff,
        issues=issues,
    )

    # Se resuelve el layout formal y el estado persistible del sidecar.
    paths = _resolve_trips_artifact_paths(write_root)
    resolved = _resolve_write_identity_and_sidecar(
        trips,
        paths,
        options_eff,
        existing_issues=issues,
    )
    issues.extend(resolved.issues)

    # Se materializa primero en staging para no dejar artefactos ambiguos a medio escribir.
    staging_dir = _create_trips_staging_dir(paths.root_dir, issues=issues)
    staging_paths = _resolve_trips_artifact_paths(staging_dir)
    try:
        _write_trips_table_to_staging(
            trips.data,
            staging_paths.data_path,
            storage_format=options_eff.storage_format,
            parquet_compression=options_eff.parquet_compression,
            schema=trips.schema,
            schema_effective=trips.schema_effective,
            issues=issues,
            destination_path=paths.root_dir,
        )
        _write_sidecar_json(
            resolved.sidecar_payload,
            staging_paths.sidecar_path,
            issues=issues,
            destination_path=paths.root_dir,
            dataset_id=resolved.dataset_id,
            artifact_id=resolved.artifact_id,
        )
        _assert_staging_complete(
            staging_paths,
            expected_files=resolved.files_written,
            issues=issues,
            destination_path=paths.root_dir,
        )

        # Se hace el commit final del artefacto solo cuando staging quedó completo.
        _commit_staged_trips_artifact(
            staging_dir,
            paths.root_dir,
            mode=options_eff.mode,
            files_written=resolved.files_written,
            issues=issues,
        )
        staging_dir = None
    finally:
        if staging_dir is not None:
            _cleanup_staging_dir(staging_dir, paths.root_dir, resolved.files_written, issues)

    # Se alinea la metadata del dataset en memoria recién después del commit exitoso.
    trips.metadata = copy.deepcopy(resolved.metadata_for_persist)

    # Se construye el summary/evidencia final de la operación usando el mismo contrato observable del sidecar.
    summary = _build_write_trips_summary(
        n_rows=int(len(trips.data)),
        path=paths.root_dir,
        artifact_id=resolved.artifact_id,
        dataset_id_status=resolved.dataset_id_status,
        dataset_id=resolved.dataset_id,
        storage_format=options_eff.storage_format,
        files_written=resolved.files_written,
    )

    # Se retorna un OperationReport pequeño y estable, usando la misma vista de parameters del evento.
    return OperationReport(
        ok=not any(issue.level == "error" for issue in issues),
        issues=issues,
        summary=summary,
        parameters=parameters,
    )



def read_trips(
    path: PathLike,
    *,
    options: Optional[ReadTripsOptions] = None,
) -> Tuple[TripDataset, OperationReport]:
    """
    Reconstruye un TripDataset desde un artefacto formal de trips (v1.1).

    Parameters
    ----------
    path : PathLike
        Directorio del artefacto formal persistido. Si el path exacto no existe
        y no termina en `.golondrina`, la operación intenta automáticamente con
        el sufijo canónico antes de fallar.
    options : ReadTripsOptions, optional
        Opciones efectivas de lectura. Si None, se usan defaults.

    Returns
    -------
    tuple[TripDataset, OperationReport]
        Dataset reconstruido y reporte estructurado de la lectura.
    """
    # Se inicializa evidencia y se fijan las options efectivas de lectura.
    issues: List[Issue] = []
    options_eff = options or ReadTripsOptions()
    
    # Se resuelve primero el root efectivo. Si el path exacto no existe,
    # se intenta con el sufijo canónico `.golondrina`.
    read_root = _resolve_trips_artifact_root_for_read(path)
    paths = _resolve_trips_artifact_paths(read_root)
    parameters = _options_to_read_parameters(path=paths.root_dir, options=options_eff)

    # Se valida que exista el layout formal mínimo antes de cargar sidecar o tabla.
    _validate_read_layout(paths.root_dir, paths, issues=issues)

    # Se carga el sidecar y se resuelve el backend de almacenamiento desde la metadata persistida.
    sidecar_payload = _load_sidecar_json(
        paths.sidecar_path,
        issues=issues,
        destination_path=paths.root_dir,
    )
    storage_format = _extract_storage_format(
        sidecar_payload,
        strict=options_eff.strict,
        issues=issues,
    )

    # Se reconstruyen schema y schema_effective respetando precedencia y política strict.
    schema_state = _resolve_read_schema_state(
        sidecar_payload,
        options_eff,
    )
    issues.extend(schema_state.issues)
    parameters["schema"] = {
        "source": schema_state.schema_source,
        "version": schema_state.schema.version,
    }

    # Se lee la tabla persistida y se materializa el dataset en memoria.
    data = _read_trips_table_from_storage(
        paths.data_path,
        storage_format=storage_format,
        issues=issues,
        destination_path=paths.root_dir,
    )
    metadata_loaded = _safe_deepcopy_dict(sidecar_payload.get("metadata"), default={})
    identity_state = _finalize_loaded_metadata_state(
        metadata_loaded,
        sidecar_payload=sidecar_payload,
        strict=options_eff.strict,
        destination_path=paths.root_dir,
    )
    issues.extend(identity_state.issues)

    field_correspondence, value_correspondence = _extract_correspondence_from_metadata(identity_state.metadata)
    dataset = TripDataset(
        data=data,
        schema=schema_state.schema,
        schema_version=schema_state.schema.version,
        provenance=_safe_deepcopy_dict(sidecar_payload.get("provenance"), default={}),
        field_correspondence=field_correspondence,
        value_correspondence=value_correspondence,
        metadata=identity_state.metadata,
        schema_effective=schema_state.schema_effective,
    )

    # Se construye el summary de lectura con la identidad efectiva y la resolución de schema usada.
    summary = _build_read_trips_summary(
        n_rows=int(len(dataset.data)),
        n_columns=int(len(dataset.data.columns)),
        path=paths.root_dir,
        storage_format=storage_format,
        schema_source=schema_state.schema_source,
        schema_mismatch=schema_state.schema_mismatch,
        dataset_id_status=identity_state.dataset_id_status,
        dataset_id=identity_state.dataset_id,
        artifact_id_status=identity_state.artifact_id_status,
        artifact_id=identity_state.artifact_id,
    )
    issues_summary = _build_issues_summary(issues)

    # Se agrega el evento solo cuando keep_metadata=True; la metadata cargada se preserva completa.
    if options_eff.keep_metadata:
        event = _build_io_event(
            op="read_trips",
            parameters=parameters,
            summary=summary,
            issues_summary=issues_summary,
        )
        dataset.metadata = _append_event(dataset.metadata, event)

    # Se retorna el dataset reconstruido junto con el OperationReport observable de la lectura.
    report = OperationReport(
        ok=not any(issue.level == "error" for issue in issues),
        issues=issues,
        summary=summary,
        parameters=parameters,
    )
    return dataset, report


# -----------------------------------------------------------------------------
# Helpers internos principales del pipeline
# -----------------------------------------------------------------------------

def _validate_write_contract(
    trips: TripDataset,
    path: PathLike,
    options_eff: WriteTripsOptions,
    *,
    issues: List[Issue],
) -> None:
    """
    Valida el contrato mínimo de write antes de tocar disco.

    Emite codes
    -----------
    - WRT.CORE.INVALID_TRIPDATASET
    - WRT.CORE.INVALID_DATA_SURFACE
    - WRT.CORE.EMPTY_DATAFRAME
    - WRT.OPTIONS.INVALID_MODE
    - WRT.OPTIONS.UNSUPPORTED_STORAGE_FORMAT
    - WRT.OPTIONS.UNSUPPORTED_PARQUET_COMPRESSION
    - WRT.VALIDATION.REQUIRED_NOT_VALIDATED
    - WRT.PATH.INVALID_DESTINATION
    - WRT.JSON.NOT_SERIALIZABLE
    """
    # Se valida que el input realmente sea un TripDataset interpretable.
    if not isinstance(trips, TripDataset):
        # Se aborta porque la operación necesita el contrato completo del dataset.
        emit_and_maybe_raise(
            issues,
            WRITE_TRIPS_ISSUES,
            "WRT.CORE.INVALID_TRIPDATASET",
            strict=False,
            exception_map=EXCEPTION_MAP_WRITE,
            default_exception=ExportError,
            received_type=type(trips).__name__,
            reason="expected_tripdataset",
        )

    # Se valida la superficie tabular viva antes de pensar en persistencia.
    if not hasattr(trips, "data") or not isinstance(trips.data, pd.DataFrame):
        # Se aborta porque no existe una tabla serializable para trips.parquet.
        emit_and_maybe_raise(
            issues,
            WRITE_TRIPS_ISSUES,
            "WRT.CORE.INVALID_DATA_SURFACE",
            strict=False,
            exception_map=EXCEPTION_MAP_WRITE,
            default_exception=ExportError,
            data_type=type(getattr(trips, "data", None)).__name__,
            reason="expected_dataframe",
        )

    # Se deja evidencia cuando el dataset es vacío, pero se permite persistencia formal.
    if len(trips.data) == 0:
        # Se registra info porque el round-trip formal también debe soportar datasets vacíos.
        emit_issue(
            issues,
            WRITE_TRIPS_ISSUES,
            "WRT.CORE.EMPTY_DATAFRAME",
            n_rows=0,
            path=str(path),
        )

    # Se valida el modo explícito de escritura para no abrir rutas ambiguas.
    if options_eff.mode not in {"error_if_exists", "overwrite"}:
        # Se aborta porque la política de colisión de destino quedó cerrada por contrato.
        emit_and_maybe_raise(
            issues,
            WRITE_TRIPS_ISSUES,
            "WRT.OPTIONS.INVALID_MODE",
            strict=False,
            exception_map=EXCEPTION_MAP_WRITE,
            default_exception=ExportError,
            mode=options_eff.mode,
        )

    # Se valida el backend tabular soportado en v1.1.
    if options_eff.storage_format not in _SUPPORTED_STORAGE_FORMATS:
        # Se aborta porque el sidecar debe ser autocontenible y consistente con el backend real.
        emit_and_maybe_raise(
            issues,
            WRITE_TRIPS_ISSUES,
            "WRT.OPTIONS.UNSUPPORTED_STORAGE_FORMAT",
            strict=False,
            exception_map=EXCEPTION_MAP_WRITE,
            default_exception=ExportError,
            storage_format=options_eff.storage_format,
        )

    # Se valida la compresión Parquet permitida por contrato.
    if options_eff.parquet_compression not in _SUPPORTED_PARQUET_COMPRESSIONS:
        # Se aborta porque no conviene dejar la elección de compresión a comportamiento implícito.
        emit_and_maybe_raise(
            issues,
            WRITE_TRIPS_ISSUES,
            "WRT.OPTIONS.UNSUPPORTED_PARQUET_COMPRESSION",
            strict=False,
            exception_map=EXCEPTION_MAP_WRITE,
            default_exception=ExportError,
            compression=options_eff.parquet_compression,
        )

    # Se valida la precondición de validación cuando el write exige dataset validado.
    if options_eff.require_validated and not _extract_validated_flag(trips.metadata):
        # Se aborta porque require_validated=True es una precondición explícita del bloque de persistencia.
        emit_and_maybe_raise(
            issues,
            WRITE_TRIPS_ISSUES,
            "WRT.VALIDATION.REQUIRED_NOT_VALIDATED",
            strict=False,
            exception_map=EXCEPTION_MAP_WRITE,
            default_exception=ValidationError,
            require_validated=True,
            validated_flag=_extract_validated_flag(trips.metadata),
            path=str(path),
        )

    # Se valida que el directorio destino sea resoluble como root formal del artefacto.
    try:
        paths = _resolve_trips_artifact_paths(path)
    except Exception as exc:
        # Se aborta porque no se puede construir un layout formal sobre una ruta ilegible.
        emit_and_maybe_raise(
            issues,
            WRITE_TRIPS_ISSUES,
            "WRT.PATH.INVALID_DESTINATION",
            strict=False,
            exception_map=EXCEPTION_MAP_WRITE,
            default_exception=ExportError,
            path=str(path),
            resolved_path=None,
            reason=str(exc),
        )

    if paths.root_dir.exists() and options_eff.mode == "error_if_exists":
        present = _list_directory_entries(paths.root_dir) if paths.root_dir.is_dir() else []
        # Se aborta temprano para no escribir staging innecesario cuando el destino ya existe.
        emit_and_maybe_raise(
            issues,
            WRITE_TRIPS_ISSUES,
            "WRT.DEST.ALREADY_EXISTS",
            strict=False,
            exception_map=EXCEPTION_MAP_WRITE,
            default_exception=ExportError,
            path=str(paths.root_dir),
            resolved_path=str(paths.root_dir),
            mode=options_eff.mode,
            files_present_sample=present[:10],
            files_present_total=len(present),
        )

    # Se valida serialización JSON-safe de los bloques que obligatoriamente van al sidecar.
    _assert_json_safe(trips.provenance, label="provenance", issues=issues)
    _assert_json_safe(_trip_schema_to_snapshot(trips.schema), label="schema", issues=issues)
    _assert_json_safe(_trip_schema_effective_to_snapshot(trips.schema_effective), label="schema_effective", issues=issues)
    _assert_json_safe(_safe_deepcopy_dict(trips.metadata, default={}), label="metadata", issues=issues)


def _resolve_write_identity_and_sidecar(
    trips: TripDataset,
    paths: TripsArtifactPaths,
    options_eff: WriteTripsOptions,
    *,
    existing_issues: Sequence[Issue],
) -> WriteResolvedState:
    """
    Resuelve identidad, metadata persistible y payload oficial del sidecar.

    Emite codes
    -----------
    - WRT.METADATA.DATASET_ID_CREATED
    - WRT.METADATA.DATASET_ID_REGENERATED
    """
    issues: List[Issue] = []

    # Se parte desde una copia de metadata para no mutar el input antes del commit exitoso.
    metadata_work = _safe_deepcopy_dict(trips.metadata, default={})

    # Se asegura dataset_id lógico antes de materializar el artefacto.
    dataset_id_raw = metadata_work.get("dataset_id")
    if dataset_id_raw is None:
        dataset_id = _new_dataset_id()
        dataset_id_status = "created"
        metadata_work["dataset_id"] = dataset_id
        # Se deja evidencia porque el write cerró identidad lógica faltante.
        emit_issue(
            issues,
            WRITE_TRIPS_ISSUES,
            "WRT.METADATA.DATASET_ID_CREATED",
            dataset_id=dataset_id,
            generator="uuid4",
        )
    elif not _is_non_empty_string(dataset_id_raw):
        dataset_id = _new_dataset_id()
        dataset_id_status = "regenerated"
        metadata_work["dataset_id"] = dataset_id
        # Se deja warning porque el dataset_id previo era inválido pero recuperable en write.
        emit_issue(
            issues,
            WRITE_TRIPS_ISSUES,
            "WRT.METADATA.DATASET_ID_REGENERATED",
            dataset_id=dataset_id,
            previous_value=_json_safe_scalar(dataset_id_raw),
            reason="invalid_or_empty",
        )
    else:
        dataset_id = str(dataset_id_raw)
        dataset_id_status = "preserved"

    # Cada materialización crea un artifact_id nuevo y lo propaga a metadata.
    artifact_id = _new_artifact_id()
    metadata_work["artifact_id"] = artifact_id

    # Se mantiene explícita la señal de validación observada; write no la recalcula.
    metadata_work["is_validated"] = _extract_validated_flag(metadata_work)

    # Se construye un metadata snapshot con el evento futuro ya incorporado para que disco y memoria queden alineados.
    files_written = ["trips.parquet", "trips.metadata.json"]
    summary_preview = _build_write_trips_summary(
        n_rows=int(len(trips.data)),
        path=paths.root_dir,
        artifact_id=artifact_id,
        dataset_id_status=dataset_id_status,
        dataset_id=dataset_id,
        storage_format=options_eff.storage_format,
        files_written=files_written,
    )
    parameters_preview = _options_to_write_parameters(path=paths.root_dir, options=options_eff)
    issues_summary_preview = _build_issues_summary(list(existing_issues) + issues)
    metadata_work = _append_event(
        metadata_work,
        _build_io_event(
            op="write_trips",
            parameters=parameters_preview,
            summary=summary_preview,
            issues_summary=issues_summary_preview,
        ),
    )

    # Se construye el sidecar oficial top-level sin mirrors redundantes de domains_effective.
    sidecar_payload = {
        "dataset_type": "trips",
        "format": "golondrina",
        "layout_version": "1.1",
        "storage": {
            "format": options_eff.storage_format,
            "options": {
                "compression": options_eff.parquet_compression,
            },
        },
        "dataset_id": dataset_id,
        "artifact_id": artifact_id,
        "files": {
            "data": "trips.parquet",
            "metadata": "trips.metadata.json",
        },
        "schema": _trip_schema_to_snapshot(trips.schema),
        "schema_effective": _trip_schema_effective_to_snapshot(trips.schema_effective),
        "provenance": _safe_deepcopy_dict(trips.provenance, default={}),
        "metadata": metadata_work,
    }

    return WriteResolvedState(
        dataset_id=dataset_id,
        artifact_id=artifact_id,
        dataset_id_status=dataset_id_status,
        metadata_for_persist=metadata_work,
        sidecar_payload=sidecar_payload,
        files_written=files_written,
        issues=issues,
    )


def _create_trips_staging_dir(final_dir: Path, *, issues: List[Issue]) -> Path:
    """
    Crea el directorio temporal hermano usado para staging de write_trips.

    Emite codes
    -----------
    - WRT.IO.STAGING_CREATE_FAILED
    """
    parent = final_dir.parent if final_dir.parent != Path("") else Path.cwd()

    # Se asegura que el parent exista antes de crear el staging temporal.
    try:
        parent.mkdir(parents=True, exist_ok=True)
        staging_dir = Path(tempfile.mkdtemp(prefix=f".{final_dir.name}.staging.", dir=str(parent)))
        return staging_dir
    except Exception as exc:
        # Se aborta porque sin staging no se puede garantizar escritura formal consistente.
        emit_and_maybe_raise(
            issues,
            WRITE_TRIPS_ISSUES,
            "WRT.IO.STAGING_CREATE_FAILED",
            strict=False,
            exception_map=EXCEPTION_MAP_WRITE,
            default_exception=ExportError,
            path=str(final_dir),
            resolved_path=str(final_dir),
            reason=str(exc),
            exception_type=type(exc).__name__,
            exception_message=str(exc),
        )
        raise AssertionError("unreachable")


def _collect_parquet_categorical_fields(
    df: pd.DataFrame,
    schema: TripSchema,
    schema_effective: Optional[TripSchemaEffective],
) -> List[str]:
    """Retorna los campos que deben persistirse como categóricos en Parquet."""
    categorical_fields: set[str] = set()

    # Se toman los categóricos declarados en el schema base.
    for field_name, field_spec in schema.fields.items():
        if getattr(field_spec, "dtype", None) == "categorical":
            categorical_fields.add(field_name)

    # Se agregan campos que el estado efectivo haya fijado explícitamente como categóricos.
    if schema_effective is not None:
        for field_name, dtype_name in (schema_effective.dtype_effective or {}).items():
            if dtype_name == "categorical":
                categorical_fields.add(field_name)

        # También se consideran los dominios efectivos como señal de categorización real del dataset.
        for field_name in (schema_effective.domains_effective or {}).keys():
            categorical_fields.add(field_name)

    return [field_name for field_name in df.columns if field_name in categorical_fields]


def _prepare_trips_df_for_parquet_write(
    df: pd.DataFrame,
    schema: TripSchema,
    schema_effective: Optional[TripSchemaEffective],
) -> pd.DataFrame:
    """Prepara una copia del dataframe para persistencia Parquet eficiente."""
    df_prepared = df.copy()
    categorical_fields = _collect_parquet_categorical_fields(df_prepared, schema, schema_effective)

    # Se convierten a pandas.Categorical solo los campos categóricos del contrato real.
    for field_name in categorical_fields:
        series = df_prepared[field_name]
        if not isinstance(series.dtype, pd.CategoricalDtype):
            df_prepared[field_name] = series.astype("category")

        # Se remueven categorías no usadas para no inflar el side effect en disco.
        if isinstance(df_prepared[field_name].dtype, pd.CategoricalDtype):
            df_prepared[field_name] = df_prepared[field_name].cat.remove_unused_categories()

    return df_prepared


def _write_trips_table_to_staging(
    df: pd.DataFrame,
    data_path: Path,
    *,
    storage_format: str,
    parquet_compression: ParquetCompression,
    schema: TripSchema,
    schema_effective: Optional[TripSchemaEffective],
    issues: List[Issue],
    destination_path: Path,
) -> None:
    """
    Escribe la tabla de trips en staging según el backend tabular efectivo.

    Emite codes
    -----------
    - WRT.PARQUET.WRITE_FAILED
    """
    # Se despacha por backend tabular, aunque v1.1 solo soporte parquet.
    try:
        if storage_format != "parquet":
            raise ValueError(f"Unsupported storage_format: {storage_format!r}")
        compression = None if parquet_compression == "none" else parquet_compression

        # Se prepara una copia con campos categóricos reales como pandas.Categorical
        # para que PyArrow los persista de forma eficiente.
        df_to_write = _prepare_trips_df_for_parquet_write(df, schema, schema_effective)
        df_to_write.to_parquet(
            data_path,
            index=False,
            compression=compression,
            engine="pyarrow",
        )
    except Exception as exc:
        # Se aborta porque sin tabla persistida el artefacto formal queda roto.
        emit_and_maybe_raise(
            issues,
            WRITE_TRIPS_ISSUES,
            "WRT.PARQUET.WRITE_FAILED",
            strict=False,
            exception_map=EXCEPTION_MAP_WRITE,
            default_exception=ExportError,
            path=str(destination_path),
            resolved_path=str(destination_path),
            storage_format=storage_format,
            compression=parquet_compression,
            n_rows=int(len(df)),
            exception_type=type(exc).__name__,
            exception_message=str(exc),
        )


def _write_sidecar_json(
    payload: Dict[str, Any],
    sidecar_path: Path,
    *,
    issues: List[Issue],
    destination_path: Path,
    dataset_id: str,
    artifact_id: str,
) -> None:
    """
    Escribe el sidecar `trips.metadata.json` con la política única de serialización.

    Emite codes
    -----------
    - WRT.JSON.WRITE_FAILED
    """
    # Se serializa el sidecar oficial completo en UTF-8 para mantener reproducibilidad del artefacto.
    try:
        sidecar_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception as exc:
        # Se aborta porque el sidecar es obligatorio para la lectura formal posterior.
        emit_and_maybe_raise(
            issues,
            WRITE_TRIPS_ISSUES,
            "WRT.JSON.WRITE_FAILED",
            strict=False,
            exception_map=EXCEPTION_MAP_WRITE,
            default_exception=ExportError,
            path=str(destination_path),
            resolved_path=str(destination_path),
            dataset_id=dataset_id,
            artifact_id=artifact_id,
            exception_type=type(exc).__name__,
            exception_message=str(exc),
        )


def _assert_staging_complete(
    staging_paths: TripsArtifactPaths,
    *,
    expected_files: Sequence[str],
    issues: List[Issue],
    destination_path: Path,
) -> None:
    """
    Verifica que staging contenga todos los artefactos requeridos antes del commit final.

    Emite codes
    -----------
    - WRT.IO.STAGING_INCOMPLETE
    """
    present = []
    if staging_paths.data_path.exists():
        present.append("trips.parquet")
    if staging_paths.sidecar_path.exists():
        present.append("trips.metadata.json")

    # Se verifica completitud mínima del layout antes de exponer el artefacto final.
    if set(present) != set(expected_files):
        # Se aborta porque el staging incompleto rompería el contrato formal del layout.
        emit_and_maybe_raise(
            issues,
            WRITE_TRIPS_ISSUES,
            "WRT.IO.STAGING_INCOMPLETE",
            strict=False,
            exception_map=EXCEPTION_MAP_WRITE,
            default_exception=ExportError,
            path=str(destination_path),
            resolved_path=str(destination_path),
            files_expected=list(expected_files),
            files_present_sample=present[:10],
            files_present_total=len(present),
            files_written=present,
            reason="missing_required_artifact_in_staging",
        )


def _commit_staged_trips_artifact(
    staging_dir: Path,
    final_dir: Path,
    *,
    mode: WriteMode,
    files_written: Sequence[str],
    issues: List[Issue],
) -> None:
    """
    Aplica la política de commit final desde staging hacia el destino formal.

    Emite codes
    -----------
    - WRT.DEST.ALREADY_EXISTS
    - WRT.IO.COMMIT_FAILED
    """
    final_exists = final_dir.exists()

    # Se respeta la política de colisión del destino antes de promover staging.
    if final_exists and mode == "error_if_exists":
        present = _list_directory_entries(final_dir)
        # Se aborta porque el contrato exige no sobrescribir en mode='error_if_exists'.
        emit_and_maybe_raise(
            issues,
            WRITE_TRIPS_ISSUES,
            "WRT.DEST.ALREADY_EXISTS",
            strict=False,
            exception_map=EXCEPTION_MAP_WRITE,
            default_exception=ExportError,
            path=str(final_dir),
            resolved_path=str(final_dir),
            mode=mode,
            files_present_sample=present[:10],
            files_present_total=len(present),
        )

    # Se reemplaza el artefacto destino solo cuando la política lo permite.
    try:
        if final_exists and mode == "overwrite":
            if final_dir.is_dir():
                shutil.rmtree(final_dir)
            else:
                final_dir.unlink()
        shutil.move(str(staging_dir), str(final_dir))
    except Exception as exc:
        # Se aborta porque el commit fallido deja el artefacto final en estado no confiable.
        emit_and_maybe_raise(
            issues,
            WRITE_TRIPS_ISSUES,
            "WRT.IO.COMMIT_FAILED",
            strict=False,
            exception_map=EXCEPTION_MAP_WRITE,
            default_exception=ExportError,
            path=str(final_dir),
            resolved_path=str(final_dir),
            mode=mode,
            files_written=list(files_written),
            reason=str(exc),
            exception_type=type(exc).__name__,
            exception_message=str(exc),
        )


def _cleanup_staging_dir(
    staging_dir: Path,
    destination_path: Path,
    files_written: Sequence[str],
    issues: List[Issue],
) -> None:
    """
    Limpia el staging residual cuando hubo fallo antes del commit final.

    Emite codes
    -----------
    - WRT.IO.CLEANUP_FAILED
    """
    if not staging_dir.exists():
        return

    # Se intenta cleanup best-effort para no dejar staging colgando tras fallos previos.
    try:
        shutil.rmtree(staging_dir)
    except Exception as exc:
        # Se deja warning porque el staging residual no invalida la evidencia ya emitida.
        emit_issue(
            issues,
            WRITE_TRIPS_ISSUES,
            "WRT.IO.CLEANUP_FAILED",
            path=str(destination_path),
            resolved_path=str(destination_path),
            files_written=list(files_written),
            reason=str(exc),
            exception_type=type(exc).__name__,
            exception_message=str(exc),
        )


def _validate_read_layout(
    root_path: Path,
    paths: TripsArtifactPaths,
    *,
    issues: List[Issue],
) -> None:
    """
    Valida que el layout formal de lectura exista y sea interpretable.

    Emite codes
    -----------
    - READ.PATH.INVALID_ROOT
    - READ.LAYOUT.MISSING_DATA_FILE
    - READ.LAYOUT.MISSING_SIDECAR
    - READ.LAYOUT.LEGACY_SIDECAR_DETECTED
    """
    # Se valida que el root exista y sea directorio antes de inspeccionar el layout.
    if not root_path.exists() or not root_path.is_dir():
        # Se aborta porque read_trips solo opera sobre artefactos formales en directorio.
        emit_and_maybe_raise(
            issues,
            READ_TRIPS_ISSUES,
            "READ.PATH.INVALID_ROOT",
            strict=False,
            exception_map=EXCEPTION_MAP_READ,
            default_exception=ExportError,
            path=str(root_path),
            resolved_path=str(root_path),
            reason="path_missing_or_not_directory",
        )

    present = _list_directory_entries(root_path)

    # Se rechaza explícitamente el sidecar viejo para no reabrir el contrato ya cerrado.
    if not paths.sidecar_path.exists() and paths.legacy_sidecar_path.exists():
        # Se aborta porque metadata.json legacy ya no es layout formal válido en v1.1.
        emit_and_maybe_raise(
            issues,
            READ_TRIPS_ISSUES,
            "READ.LAYOUT.LEGACY_SIDECAR_DETECTED",
            strict=False,
            exception_map=EXCEPTION_MAP_READ,
            default_exception=ExportError,
            path=str(root_path),
            resolved_path=str(root_path),
        )

    # Se exige el archivo de datos del layout oficial.
    if not paths.data_path.exists():
        # Se aborta porque sin trips.parquet no existe superficie tabular a reconstruir.
        emit_and_maybe_raise(
            issues,
            READ_TRIPS_ISSUES,
            "READ.LAYOUT.MISSING_DATA_FILE",
            strict=False,
            exception_map=EXCEPTION_MAP_READ,
            default_exception=ExportError,
            path=str(root_path),
            resolved_path=str(root_path),
            files_present_sample=present[:10],
            files_present_total=len(present),
        )

    # Se exige el sidecar formal como fuente de verdad del artefacto persistido.
    if not paths.sidecar_path.exists():
        # Se aborta porque read formal no admite cargas sin trips.metadata.json.
        emit_and_maybe_raise(
            issues,
            READ_TRIPS_ISSUES,
            "READ.LAYOUT.MISSING_SIDECAR",
            strict=False,
            exception_map=EXCEPTION_MAP_READ,
            default_exception=ExportError,
            path=str(root_path),
            resolved_path=str(root_path),
            files_present_sample=present[:10],
            files_present_total=len(present),
        )


def _load_sidecar_json(
    sidecar_path: Path,
    *,
    issues: List[Issue],
    destination_path: Path,
) -> Dict[str, Any]:
    """
    Carga y valida el sidecar top-level de trips.

    Emite codes
    -----------
    - READ.JSON.LOAD_FAILED
    - READ.SIDECAR.INVALID_TOP_LEVEL
    """
    # Se carga el sidecar completo porque es la fuente de verdad del artefacto formal.
    try:
        payload = json.loads(sidecar_path.read_text(encoding="utf-8"))
    except Exception as exc:
        # Se aborta porque un sidecar ilegible impide reconstrucción confiable.
        emit_and_maybe_raise(
            issues,
            READ_TRIPS_ISSUES,
            "READ.JSON.LOAD_FAILED",
            strict=False,
            exception_map=EXCEPTION_MAP_READ,
            default_exception=ExportError,
            path=str(destination_path),
            resolved_path=str(destination_path),
            exception_type=type(exc).__name__,
            exception_message=str(exc),
        )
        raise AssertionError("unreachable")

    # Se valida la estructura top-level mínima obligatoria del sidecar oficial.
    if not isinstance(payload, dict):
        emit_and_maybe_raise(
            issues,
            READ_TRIPS_ISSUES,
            "READ.SIDECAR.INVALID_TOP_LEVEL",
            strict=False,
            exception_map=EXCEPTION_MAP_READ,
            default_exception=ExportError,
            path=str(destination_path),
            resolved_path=str(destination_path),
            missing_keys=sorted(_REQUIRED_SIDECAR_TOP_LEVEL),
            invalid_keys=[],
            reason="sidecar_root_not_dict",
        )

    missing_keys = sorted(key for key in _REQUIRED_SIDECAR_TOP_LEVEL if key not in payload)
    invalid_keys = sorted(key for key in payload.keys() if key not in (_REQUIRED_SIDECAR_TOP_LEVEL | {"dataset_id", "artifact_id", "schema_effective"}))
    if missing_keys:
        emit_and_maybe_raise(
            issues,
            READ_TRIPS_ISSUES,
            "READ.SIDECAR.INVALID_TOP_LEVEL",
            strict=False,
            exception_map=EXCEPTION_MAP_READ,
            default_exception=ExportError,
            path=str(destination_path),
            resolved_path=str(destination_path),
            missing_keys=missing_keys,
            invalid_keys=invalid_keys,
            reason="missing_required_top_level_keys",
        )

    return payload


def _extract_storage_format(
    sidecar_payload: Mapping[str, Any],
    *,
    strict: bool,
    issues: List[Issue],
) -> str:
    """
    Extrae y valida el backend tabular indicado por `storage.format`.

    Emite codes
    -----------
    - READ.STORAGE.UNSUPPORTED_FORMAT
    """
    storage = sidecar_payload.get("storage")
    storage_format = None
    if isinstance(storage, dict):
        storage_format = storage.get("format")

    # Se valida el backend leído desde el sidecar para mantener el artefacto autocontenible.
    if storage_format not in _SUPPORTED_STORAGE_FORMATS:
        # Se aborta porque el sidecar pide un backend que la lectura actual no sabe interpretar.
        emit_and_maybe_raise(
            issues,
            READ_TRIPS_ISSUES,
            "READ.STORAGE.UNSUPPORTED_FORMAT",
            strict=strict,
            exception_map=EXCEPTION_MAP_READ,
            default_exception=ExportError,
            storage_format=storage_format,
        )
    return str(storage_format)


def _resolve_read_schema_state(
    sidecar_payload: Mapping[str, Any],
    options_eff: ReadTripsOptions,
) -> ReadSchemaState:
    """
    Resuelve schema y schema_effective respetando precedencia y política strict.

    Emite codes
    -----------
    - READ.SCHEMA.METADATA_INVALID_IGNORED
    - READ.SCHEMA.UNAVAILABLE
    - READ.SCHEMA.MISMATCH
    - READ.SCHEMA_EFFECTIVE.DEFAULTED
    """
    issues: List[Issue] = []
    schema_source = "metadata"
    schema_mismatch = False
    metadata_snapshot = sidecar_payload.get("schema")

    # Se intenta reconstruir schema desde metadata solo como fallback del schema explícito.
    metadata_schema: Optional[TripSchema] = None
    metadata_schema_error: Optional[str] = None
    if isinstance(metadata_snapshot, dict):
        try:
            metadata_schema = _trip_schema_from_snapshot(metadata_snapshot)
        except Exception as exc:
            metadata_schema_error = str(exc)
    elif metadata_snapshot is not None:
        metadata_schema_error = "schema_snapshot_not_dict"

    if options_eff.schema is not None:
        # Se privilegia el schema entregado por el usuario sobre el snapshot persistido.
        schema = options_eff.schema
        schema_source = "options"
        if metadata_schema is None and metadata_schema_error is not None:
            # Se deja warning porque el snapshot persistido quedó inválido, pero el usuario entregó schema usable.
            emit_issue(
                issues,
                READ_TRIPS_ISSUES,
                "READ.SCHEMA.METADATA_INVALID_IGNORED",
                schema_source=schema_source,
                reason=metadata_schema_error,
            )
        elif metadata_schema is not None:
            mismatch = _compare_schema_snapshots(schema, metadata_schema)
            if mismatch["schema_mismatch"]:
                schema_mismatch = True
                # Se deja warning porque bajo precedencia vigente se sigue usando options.schema.
                issue = emit_issue(
                    issues,
                    READ_TRIPS_ISSUES,
                    "READ.SCHEMA.MISMATCH",
                    schema_source=schema_source,
                    schema_mismatch=True,
                    version_options=schema.version,
                    version_metadata=metadata_schema.version,
                    required_diff=mismatch["required_diff"],
                    fields_diff_sample=mismatch["fields_diff_sample"],
                    fields_diff_total=mismatch["fields_diff_total"],
                )
                if options_eff.strict:
                    raise ExportError(
                        issue.message,
                        code=issue.code,
                        details=issue.details,
                        issue=issue,
                        issues=issues,
                    )
    else:
        # Se usa el snapshot persistido solo cuando el usuario no reinyecta un schema explícito.
        if metadata_schema is None:
            issue = emit_issue(
                issues,
                READ_TRIPS_ISSUES,
                "READ.SCHEMA.UNAVAILABLE",
                schema_source="metadata",
                reason=metadata_schema_error or "missing_schema_snapshot",
            )
            raise ExportError(
                issue.message,
                code=issue.code,
                details=issue.details,
                issue=issue,
                issues=issues,
            )
        schema = metadata_schema
        schema_source = "metadata"

    schema_effective_snapshot = sidecar_payload.get("schema_effective")
    try:
        schema_effective, schema_effective_issues = _trip_schema_effective_from_snapshot(
            schema_effective_snapshot,
            strict=options_eff.strict,
        )
    except Exception as exc:
        issue = emit_issue(
            issues,
            READ_TRIPS_ISSUES,
            "READ.SCHEMA_EFFECTIVE.DEFAULTED",
            reason=str(exc),
            strict=options_eff.strict,
        )
        raise ExportError(
            issue.message,
            code=issue.code,
            details=issue.details,
            issue=issue,
            issues=issues,
        )
    issues.extend(schema_effective_issues)

    return ReadSchemaState(
        schema=schema,
        schema_effective=schema_effective,
        schema_source=schema_source,
        schema_mismatch=schema_mismatch,
        issues=issues,
    )


def _read_trips_table_from_storage(
    data_path: Path,
    *,
    storage_format: str,
    issues: List[Issue],
    destination_path: Path,
) -> pd.DataFrame:
    """
    Lee la tabla de trips desde el backend indicado por `storage.format`.

    Emite codes
    -----------
    - READ.PARQUET.LOAD_FAILED
    - READ.CORE.EMPTY_DATAFRAME
    """
    # Se despacha por backend tabular usando lo que declara el sidecar, no el usuario.
    try:
        if storage_format != "parquet":
            raise ValueError(f"Unsupported storage_format: {storage_format!r}")
        df = pd.read_parquet(data_path, engine="pyarrow")
    except Exception as exc:
        # Se aborta porque sin tabla cargada no existe dataset a reconstruir.
        emit_and_maybe_raise(
            issues,
            READ_TRIPS_ISSUES,
            "READ.PARQUET.LOAD_FAILED",
            strict=False,
            exception_map=EXCEPTION_MAP_READ,
            default_exception=ExportError,
            path=str(destination_path),
            resolved_path=str(destination_path),
            storage_format=storage_format,
            exception_type=type(exc).__name__,
            exception_message=str(exc),
        )
        raise AssertionError("unreachable")

    # Se deja evidencia informativa si el artefacto formal reconstruye un dataset vacío.
    if len(df) == 0:
        emit_issue(
            issues,
            READ_TRIPS_ISSUES,
            "READ.CORE.EMPTY_DATAFRAME",
            n_rows=0,
            n_columns=int(len(df.columns)),
            path=str(destination_path),
        )
    return df


def _finalize_loaded_metadata_state(
    metadata: Dict[str, Any],
    *,
    sidecar_payload: Mapping[str, Any],
    strict: bool,
    destination_path: Path,
) -> LoadedIdentityState:
    """
    Ajusta identidad y estado post-read sobre la metadata cargada.

    Emite codes
    -----------
    - READ.SIDECAR.INVALID_TOP_LEVEL
    - READ.METADATA.DATASET_ID_REGENERATED
    - READ.METADATA.ARTIFACT_ID_SET_NONE
    - READ.METADATA.VALIDATED_FORCED_FALSE
    """
    issues: List[Issue] = []
    metadata_work = _safe_deepcopy_dict(metadata, default={})

    # Se consolida dataset_id priorizando el top-level del sidecar y permitiendo recovery en strict=False.
    dataset_id_raw = sidecar_payload.get("dataset_id", metadata_work.get("dataset_id"))
    if _is_non_empty_string(dataset_id_raw):
        dataset_id = str(dataset_id_raw)
        dataset_id_status = "loaded"
    elif strict:
        issue = emit_issue(
            issues,
            READ_TRIPS_ISSUES,
            "READ.SIDECAR.INVALID_TOP_LEVEL",
            path=str(destination_path),
            resolved_path=str(destination_path),
            missing_keys=["dataset_id"],
            invalid_keys=[],
            reason="dataset_id_missing_or_invalid_in_strict_mode",
        )
        raise ExportError(
            issue.message,
            code=issue.code,
            details=issue.details,
            issue=issue,
            issues=issues,
        )
    else:
        dataset_id = _new_dataset_id()
        dataset_id_status = "regenerated"
        metadata_work["dataset_id"] = dataset_id
        # Se deja warning porque la identidad lógica fue recuperada con generación nueva.
        emit_issue(
            issues,
            READ_TRIPS_ISSUES,
            "READ.METADATA.DATASET_ID_REGENERATED",
            dataset_id=dataset_id,
            previous_value=_json_safe_scalar(dataset_id_raw),
            reason="missing_or_invalid_in_sidecar",
        )

    # Se consolida artifact_id, pero nunca se regenera como si fuera el snapshot persistido original.
    artifact_id_raw = sidecar_payload.get("artifact_id", metadata_work.get("artifact_id"))
    if _is_non_empty_string(artifact_id_raw):
        artifact_id = str(artifact_id_raw)
        artifact_id_status = "loaded"
        metadata_work["artifact_id"] = artifact_id
    elif strict:
        issue = emit_issue(
            issues,
            READ_TRIPS_ISSUES,
            "READ.SIDECAR.INVALID_TOP_LEVEL",
            path=str(destination_path),
            resolved_path=str(destination_path),
            missing_keys=["artifact_id"],
            invalid_keys=[],
            reason="artifact_id_missing_or_invalid_in_strict_mode",
        )
        raise ExportError(
            issue.message,
            code=issue.code,
            details=issue.details,
            issue=issue,
            issues=issues,
        )
    else:
        artifact_id = None
        artifact_id_status = "missing_or_invalid"
        metadata_work["artifact_id"] = None
        # Se deja warning porque se pierde la identidad de snapshot, pero el dataset cargado sigue siendo usable.
        emit_issue(
            issues,
            READ_TRIPS_ISSUES,
            "READ.METADATA.ARTIFACT_ID_SET_NONE",
            artifact_id=None,
            previous_value=_json_safe_scalar(artifact_id_raw),
            reason="missing_or_invalid_in_sidecar",
        )

    # Se fuerza siempre el estado no validado tras la lectura formal del artefacto.
    previous_validated = metadata_work.get("is_validated")
    metadata_work["dataset_id"] = dataset_id
    metadata_work["is_validated"] = False
    emit_issue(
        issues,
        READ_TRIPS_ISSUES,
        "READ.METADATA.VALIDATED_FORCED_FALSE",
        previous_value=_json_safe_scalar(previous_validated),
    )

    return LoadedIdentityState(
        metadata=metadata_work,
        dataset_id=dataset_id,
        dataset_id_status=dataset_id_status,
        artifact_id=artifact_id,
        artifact_id_status=artifact_id_status,
        issues=issues,
    )


def _build_write_trips_summary(
    *,
    n_rows: int,
    path: Path,
    artifact_id: str,
    dataset_id_status: str,
    dataset_id: str,
    storage_format: str,
    files_written: Sequence[str],
) -> Dict[str, Any]:
    """
    Construye el summary mínimo y estable de write_trips.
    """
    # Se mantiene el summary pequeño y explícitamente orientado al contrato observable del write.
    return {
        "n_rows": int(n_rows),
        "files_written": list(files_written),
        "path": str(path),
        "dataset_id": dataset_id,
        "artifact_id": artifact_id,
        "dataset_id_status": dataset_id_status,
        "storage_format": storage_format,
    }


def _build_read_trips_summary(
    *,
    n_rows: int,
    n_columns: int,
    path: Path,
    storage_format: str,
    schema_source: str,
    schema_mismatch: bool,
    dataset_id_status: str,
    dataset_id: str,
    artifact_id_status: str,
    artifact_id: Optional[str],
) -> Dict[str, Any]:
    """
    Construye el summary mínimo y estable de read_trips.
    """
    # Se deja un summary compacto con identidad, shape y resolución contractual de schema.
    return {
        "n_rows": int(n_rows),
        "n_columns": int(n_columns),
        "path": str(path),
        "storage_format": storage_format,
        "schema_source": schema_source,
        "schema_mismatch": bool(schema_mismatch),
        "dataset_id": dataset_id,
        "dataset_id_status": dataset_id_status,
        "artifact_id": artifact_id,
        "artifact_id_status": artifact_id_status,
    }


# -----------------------------------------------------------------------------
# Helpers internos de uso general
# -----------------------------------------------------------------------------

def _has_golondrina_artifact_suffix(path: Path) -> bool:
    """Indica si el nombre del directorio ya usa el sufijo canónico `.golondrina`."""
    return path.name.endswith(_GOLONDRINA_ARTIFACT_SUFFIX)


def _append_golondrina_artifact_suffix(path: Path) -> Path:
    """Agrega el sufijo canónico `.golondrina` al nombre del directorio del artefacto."""
    if _has_golondrina_artifact_suffix(path):
        return path
    if path.name:
        return path.parent / f"{path.name}{_GOLONDRINA_ARTIFACT_SUFFIX}"
    return Path(f"{str(path)}{_GOLONDRINA_ARTIFACT_SUFFIX}")


def _normalize_trips_artifact_root_for_write(
    root_path: PathLike,
    *,
    normalize_artifact_dir: bool,
) -> Path:
    """
    Normaliza el root del artefacto en write_trips.

    Si `normalize_artifact_dir=True` y el nombre no termina en `.golondrina`,
    se agrega el sufijo canónico al directorio destino.
    """
    # Se expande primero el path del usuario para trabajar siempre con un root consistente.
    root_dir = Path(root_path).expanduser()
    if normalize_artifact_dir:
        # Se fuerza la convención canónica del bundle persistido cuando la opción lo pide.
        return _append_golondrina_artifact_suffix(root_dir)
    return root_dir

def _resolve_trips_artifact_root_for_read(root_path: PathLike) -> Path:
    """
    Resuelve el root del artefacto en read_trips con fallback amigable a `.golondrina`.

    Regla:
    1) intentar el path exacto;
    2) si no existe y no termina en `.golondrina`, intentar `path + ".golondrina"`;
    3) si no existe ninguno, devolver el path exacto para que el error lo emita read
       sobre la ruta realmente solicitada.
    """
    # Se intenta primero exactamente la ruta que el usuario pidió.
    root_dir = Path(root_path).expanduser()
    if root_dir.exists():
        return root_dir

    # Si ya venía con el sufijo canónico, no hay segundo intento alternativo.
    if _has_golondrina_artifact_suffix(root_dir):
        return root_dir

    # Solo si el path exacto no existe, se intenta el nombre canónico del bundle.
    candidate = _append_golondrina_artifact_suffix(root_dir)
    if candidate.exists():
        return candidate

    # Si no existe ninguno, se devuelve el original para que la operación falle sobre esa ruta.
    return root_dir

def _resolve_trips_artifact_paths(root_path: PathLike) -> TripsArtifactPaths:
    """Resuelve el layout formal de trips a partir del root del artefacto."""
    root_dir = Path(root_path).expanduser()
    return TripsArtifactPaths(
        root_dir=root_dir,
        data_path=root_dir / "trips.parquet",
        sidecar_path=root_dir / "trips.metadata.json",
        legacy_sidecar_path=root_dir / "metadata.json",
    )


def _assert_json_safe(value: Any, *, label: str, issues: List[Issue]) -> None:
    """Verifica que un bloque sea JSON-safe y aborta si no lo es."""
    try:
        json.dumps(value, ensure_ascii=False)
    except Exception as exc:
        # Se aborta porque el sidecar y los details del bloque de persistencia deben ser JSON-safe.
        emit_and_maybe_raise(
            issues,
            WRITE_TRIPS_ISSUES,
            "WRT.JSON.NOT_SERIALIZABLE",
            strict=False,
            exception_map=EXCEPTION_MAP_WRITE,
            default_exception=ExportError,
            label=label,
            reason=str(exc),
            offending_type=type(value).__name__,
            example_repr=repr(value)[:300],
        )


def _trip_schema_to_snapshot(schema: TripSchema) -> Dict[str, Any]:
    """Serializa un TripSchema al bloque `schema` del sidecar."""
    return schema.to_dict()


def _trip_schema_from_snapshot(schema_snapshot: Mapping[str, Any]) -> TripSchema:
    """Reconstruye TripSchema desde un snapshot serializable del sidecar."""
    if not isinstance(schema_snapshot, Mapping):
        raise TypeError("schema snapshot must be a mapping")

    fields_raw = schema_snapshot.get("fields")
    if not isinstance(fields_raw, Mapping):
        raise TypeError("schema snapshot must contain mapping 'fields'")

    fields: Dict[str, FieldSpec] = {}
    for field_name, spec_raw in fields_raw.items():
        if not isinstance(spec_raw, Mapping):
            raise TypeError(f"field spec for {field_name!r} must be a mapping")
        domain_raw = spec_raw.get("domain")
        domain = None
        if isinstance(domain_raw, Mapping):
            domain = DomainSpec(
                values=list(domain_raw.get("values", [])),
                extendable=bool(domain_raw.get("extendable", True)),
                aliases=_safe_deepcopy_dict(domain_raw.get("aliases"), default=None),
            )
        fields[str(field_name)] = FieldSpec(
            name=str(spec_raw.get("name", field_name)),
            dtype=str(spec_raw.get("dtype")),
            required=bool(spec_raw.get("required", False)),
            constraints=_safe_deepcopy_dict(spec_raw.get("constraints"), default=None),
            domain=domain,
        )

    required_raw = schema_snapshot.get("required", [])
    if required_raw is None:
        required: List[str] = []
    elif isinstance(required_raw, list):
        required = [str(value) for value in required_raw]
    else:
        raise TypeError("schema snapshot field 'required' must be a list")

    semantic_rules = _safe_deepcopy_dict(schema_snapshot.get("semantic_rules"), default=None)
    return TripSchema(
        version=str(schema_snapshot.get("version", "0.0.0")),
        fields=fields,
        required=required,
        semantic_rules=semantic_rules,
    )


def _trip_schema_effective_to_snapshot(schema_effective: TripSchemaEffective) -> Dict[str, Any]:
    """Serializa TripSchemaEffective al bloque `schema_effective` del sidecar."""
    return schema_effective.to_dict()


def _trip_schema_effective_from_snapshot(
    snapshot: Any,
    *,
    strict: bool,
) -> Tuple[TripSchemaEffective, List[Issue]]:
    """Reconstruye TripSchemaEffective o degrada a default vacío según política strict."""
    issues: List[Issue] = []
    if isinstance(snapshot, Mapping):
        return (
            TripSchemaEffective(
                dtype_effective=_safe_deepcopy_dict(snapshot.get("dtype_effective"), default={}),
                overrides=_safe_deepcopy_dict(snapshot.get("overrides"), default={}),
                domains_effective=_safe_deepcopy_dict(snapshot.get("domains_effective"), default={}),
                temporal=_safe_deepcopy_dict(snapshot.get("temporal"), default={}),
                fields_effective=_safe_string_list(snapshot.get("fields_effective")),
            ),
            issues,
        )

    if snapshot is None and not strict:
        # Se deja warning porque strict=False permite degradar a schema_effective vacío.
        emit_issue(
            issues,
            READ_TRIPS_ISSUES,
            "READ.SCHEMA_EFFECTIVE.DEFAULTED",
            reason="missing_schema_effective_snapshot",
            strict=False,
        )
        return TripSchemaEffective(), issues

    if strict:
        raise ValueError("schema_effective unavailable or invalid in strict mode")

    # Se deja warning porque el snapshot es inválido pero la lectura puede continuar con default vacío.
    emit_issue(
        issues,
        READ_TRIPS_ISSUES,
        "READ.SCHEMA_EFFECTIVE.DEFAULTED",
        reason="invalid_schema_effective_snapshot",
        strict=False,
    )
    return TripSchemaEffective(), issues


def _build_issues_summary(issues: Sequence[Issue]) -> Dict[str, Any]:
    """Resume issues por severidad y por código para eventos del bloque IO."""
    level_counts = Counter(issue.level for issue in issues)
    code_counts = Counter(issue.code for issue in issues)
    return {
        "counts": {
            "info": int(level_counts.get("info", 0)),
            "warning": int(level_counts.get("warning", 0)),
            "error": int(level_counts.get("error", 0)),
        },
        "top_codes": [
            {"code": code, "count": int(count)}
            for code, count in sorted(code_counts.items(), key=lambda item: (-item[1], item[0]))[:10]
        ],
    }


def _build_io_event(
    *,
    op: str,
    parameters: Dict[str, Any],
    summary: Dict[str, Any],
    issues_summary: Dict[str, Any],
) -> Dict[str, Any]:
    """Construye la forma mínima del evento IO (`op`, `ts_utc`, `parameters`, `summary`, `issues_summary`)."""
    return {
        "op": op,
        "ts_utc": _utc_now_iso(),
        "parameters": parameters,
        "summary": summary,
        "issues_summary": issues_summary,
    }


def _append_event(metadata: Dict[str, Any], event: Dict[str, Any]) -> Dict[str, Any]:
    """Agrega un evento append-only en `metadata["events"]` sin mutar el input original."""
    metadata_out = _safe_deepcopy_dict(metadata, default={})
    events = metadata_out.get("events")
    if not isinstance(events, list):
        events = []
    else:
        events = copy.deepcopy(events)
    events.append(event)
    metadata_out["events"] = events
    return metadata_out


def _options_to_write_parameters(*, path: PathLike, options: WriteTripsOptions) -> Dict[str, Any]:
    """Serializa WriteTripsOptions a parameters estables para report y evento."""
    return {
        "path": str(Path(path).expanduser()),
        "mode": options.mode,
        "require_validated": bool(options.require_validated),
        "storage_format": options.storage_format,
        "parquet_compression": options.parquet_compression,
        "normalize_artifact_dir": bool(options.normalize_artifact_dir),
    }


def _options_to_read_parameters(*, path: PathLike, options: ReadTripsOptions) -> Dict[str, Any]:
    """Serializa ReadTripsOptions a parameters estables para report y evento."""
    schema_summary = None
    if options.schema is not None:
        schema_summary = {
            "source": "options",
            "version": options.schema.version,
        }
    return {
        "path": str(Path(path).expanduser()),
        "strict": bool(options.strict),
        "keep_metadata": bool(options.keep_metadata),
        "schema": schema_summary,
    }


def _compare_schema_snapshots(options_schema: TripSchema, metadata_schema: TripSchema) -> Dict[str, Any]:
    """Compara snapshots mínimos de schema para detectar mismatches observables en read."""
    required_diff = sorted(set(options_schema.required) ^ set(metadata_schema.required))
    fields_diff = sorted(set(options_schema.fields.keys()) ^ set(metadata_schema.fields.keys()))
    version_diff = options_schema.version != metadata_schema.version
    schema_mismatch = bool(required_diff or fields_diff or version_diff)
    return {
        "schema_mismatch": schema_mismatch,
        "required_diff": required_diff,
        "fields_diff_sample": fields_diff[:10],
        "fields_diff_total": len(fields_diff),
    }


def _extract_correspondence_from_metadata(metadata: Mapping[str, Any]) -> Tuple[Dict[str, str], Dict[str, Dict[str, str]]]:
    """Extrae correspondencias desde `metadata["mappings"]` cuando existen."""
    mappings = metadata.get("mappings")
    if not isinstance(mappings, Mapping):
        return {}, {}
    field_corr = mappings.get("field_correspondence")
    value_corr = mappings.get("value_correspondence")
    return _safe_string_mapping(field_corr), _safe_nested_string_mapping(value_corr)


def _list_directory_entries(path: Path) -> List[str]:
    """Lista entradas del directorio de forma estable para details resumidos."""
    try:
        return sorted(entry.name for entry in path.iterdir())
    except Exception:
        return []


def _new_dataset_id() -> str:
    """Genera un dataset_id lógico nuevo y estable para el bloque IO."""
    return f"dset_{uuid.uuid4()}"


def _new_artifact_id() -> str:
    """Genera un artifact_id nuevo para una materialización específica."""
    return f"art_{uuid.uuid4()}"


def _extract_validated_flag(metadata: Any) -> bool:
    """Lee `metadata["is_validated"]` con fallback legacy interno."""
    if not isinstance(metadata, Mapping):
        return False
    if "is_validated" in metadata:
        return bool(metadata.get("is_validated", False))
    flags = metadata.get("flags", {})
    if isinstance(flags, Mapping):
        return bool(flags.get("validated", False))
    return False


def _safe_deepcopy_dict(value: Any, *, default: Any) -> Any:
    """Devuelve una copia profunda de dicts y deja `default` cuando el tipo no calza."""
    if value is None:
        return copy.deepcopy(default)
    if isinstance(value, Mapping):
        return copy.deepcopy(dict(value))
    return copy.deepcopy(default)


def _safe_string_list(value: Any) -> List[str]:
    """Normaliza una lista de strings para snapshots reconstruidos desde sidecar."""
    if not isinstance(value, list):
        return []
    return [str(item) for item in value]


def _safe_string_mapping(value: Any) -> Dict[str, str]:
    """Normaliza un mapping simple string->string proveniente de metadata persistida."""
    if not isinstance(value, Mapping):
        return {}
    return {str(k): str(v) for k, v in value.items()}


def _safe_nested_string_mapping(value: Any) -> Dict[str, Dict[str, str]]:
    """Normaliza un mapping anidado string->(string->string) desde metadata persistida."""
    if not isinstance(value, Mapping):
        return {}
    out: Dict[str, Dict[str, str]] = {}
    for outer_key, inner in value.items():
        if isinstance(inner, Mapping):
            out[str(outer_key)] = {str(k): str(v) for k, v in inner.items()}
    return out


def _is_non_empty_string(value: Any) -> bool:
    """Chequea si un valor puede interpretarse como string no vacío."""
    return isinstance(value, str) and value.strip() != ""


def _json_safe_scalar(value: Any) -> Any:
    """Normaliza un escalar a una representación simple y JSON-friendly."""
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _utc_now_iso() -> str:
    """Retorna timestamp UTC ISO-8601 compacto con sufijo Z."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
