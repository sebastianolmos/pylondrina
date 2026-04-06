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
from typing import Any, Dict, List, Literal, Mapping, Optional, Sequence, Tuple, Union

import pandas as pd

from pylondrina.datasets import FlowDataset
from pylondrina.errors import ExportError
from pylondrina.issues.catalog_read_flows import READ_FLOWS_ISSUES
from pylondrina.issues.catalog_write_flows import WRITE_FLOWS_ISSUES
from pylondrina.issues.core import emit_and_maybe_raise, emit_issue
from pylondrina.reports import Issue, OperationReport

PathLike = Union[str, Path]
WriteMode = Literal["error_if_exists", "overwrite"]
StorageFormat = Literal["parquet"]
ParquetCompression = Optional[Literal["snappy", "gzip", "zstd", "brotli", "none"]]

EXCEPTION_MAP_WRITE = {"export": ExportError}
EXCEPTION_MAP_READ = {"export": ExportError}

_GOLONDRINA_ARTIFACT_SUFFIX = ".golondrina"
_SUPPORTED_STORAGE_FORMATS = {"parquet"}
_SUPPORTED_PARQUET_COMPRESSIONS = {"snappy", "gzip", "zstd", "brotli", "none", None}
_REQUIRED_SIDECAR_TOP_LEVEL = {
    "dataset_type",
    "format",
    "layout_version",
    "storage",
    "dataset_id",
    "artifact_id",
    "files",
    "aggregation_spec",
    "provenance",
    "metadata",
    "tables",
}


@dataclass(frozen=True)
class WriteFlowsOptions:
    """
    Opciones efectivas para persistir un FlowDataset como artefacto formal.

    Parameters
    ----------
    mode : {"error_if_exists", "overwrite"}, default="error_if_exists"
        Política cuando el directorio destino ya existe.
    storage_format : {"parquet"}, default="parquet"
        Backend tabular de persistencia. En v1.1 solo se soporta Parquet.
    parquet_compression : {"snappy", "gzip", "zstd", "brotli", "none", None}, default="snappy"
        Compresión efectiva usada al escribir tablas Parquet.
    normalize_artifact_dir : bool, default=True
        Si True, normaliza el directorio root para que termine en `.golondrina`.
    write_flow_to_trips : bool, default=True
        Si True, intenta persistir `flow_to_trips.parquet` cuando exista en memoria.
    """

    mode: WriteMode = "error_if_exists"
    storage_format: StorageFormat = "parquet"
    parquet_compression: ParquetCompression = "snappy"
    normalize_artifact_dir: bool = True
    write_flow_to_trips: bool = True


@dataclass(frozen=True)
class ReadFlowsOptions:
    """
    Opciones efectivas para reconstruir un FlowDataset desde persistencia formal.

    Parameters
    ----------
    strict : bool, default=False
        Si True, inconsistencias recuperables del sidecar/layout se tratan como fatales.
    keep_metadata : bool, default=True
        Si True, agrega un evento `read_flows` en `metadata["events"]`.
    read_flow_to_trips : bool, default=True
        Si True, intenta cargar `flow_to_trips.parquet` cuando exista.
    """

    strict: bool = False
    keep_metadata: bool = True
    read_flow_to_trips: bool = True


@dataclass(frozen=True)
class FlowsArtifactPaths:
    """Rutas resueltas del layout formal de flows."""

    root_dir: Path
    data_path: Path
    sidecar_path: Path
    flow_to_trips_path: Path


@dataclass(frozen=True)
class FlowWriteSnapshot:
    """Snapshot serializable listo para persistencia formal de flows."""

    dataset_id: str
    artifact_id: str
    dataset_id_status: str
    metadata_for_persist: Dict[str, Any]
    sidecar_payload: Dict[str, Any]
    files_written: List[str]
    n_flow_to_trips: Optional[int]
    issues: List[Issue]


# -----------------------------------------------------------------------------
# Funciones públicas
# -----------------------------------------------------------------------------

def write_flows(
    flows: FlowDataset,
    path: PathLike,
    *,
    options: Optional[WriteFlowsOptions] = None,
) -> OperationReport:
    """
    Persiste un FlowDataset como artefacto formal de flows (v1.1).

    Parameters
    ----------
    flows : FlowDataset
        Dataset de flows a persistir.
    path : PathLike
        Directorio destino del artefacto formal. Si
        `options.normalize_artifact_dir=True` y el nombre no termina en
        `.golondrina`, se normaliza automáticamente al sufijo canónico.
    options : WriteFlowsOptions, optional
        Opciones efectivas de escritura. Si None, se usan defaults.

    Returns
    -------
    OperationReport
        Reporte estructurado de la operación.
    """
    # Se inicializa el acumulador de evidencia y se fijan las options efectivas.
    issues: List[Issue] = []
    options_eff = options or WriteFlowsOptions()

    # Se normaliza primero el root efectivo del artefacto para que todo el pipeline
    # trabaje sobre el mismo directorio canónico.
    write_root = _normalize_flows_artifact_root_for_write(
        path,
        normalize_artifact_dir=options_eff.normalize_artifact_dir,
    )
    parameters = _options_to_write_parameters(path=write_root, options=options_eff)

    # Se valida el contrato de write antes de tocar disco.
    _validate_write_contract(
        flows,
        write_root,
        options_eff,
        issues=issues,
    )

    # Se resuelve el layout formal y se congela el snapshot serializable del dataset.
    paths = _resolve_flows_artifact_paths(write_root)
    snapshot = _freeze_flow_write_snapshot(
        flows,
        paths,
        options_eff,
        existing_issues=issues,
    )
    issues.extend(snapshot.issues)

    # Se materializa primero en staging para no dejar bundles ambiguos a medio escribir.
    staging_dir = _create_flows_staging_dir(paths.root_dir, issues=issues)
    staging_paths = _resolve_flows_artifact_paths(staging_dir)
    try:
        _write_flows_table_to_staging(
            flows.flows,
            staging_paths.data_path,
            storage_format=options_eff.storage_format,
            parquet_compression=options_eff.parquet_compression,
            issues=issues,
            destination_path=paths.root_dir,
        )
        _write_optional_flow_to_trips_to_staging(
            flows.flow_to_trips,
            staging_paths.flow_to_trips_path,
            write_flow_to_trips=options_eff.write_flow_to_trips,
            storage_format=options_eff.storage_format,
            parquet_compression=options_eff.parquet_compression,
            issues=issues,
            destination_path=paths.root_dir,
        )
        _write_flow_sidecar_to_staging(
            snapshot.sidecar_payload,
            staging_paths.sidecar_path,
            issues=issues,
            destination_path=paths.root_dir,
        )
        _assert_flows_staging_complete(
            staging_paths,
            expected_files=snapshot.files_written,
            issues=issues,
            destination_path=paths.root_dir,
        )

        # Se hace el commit final del artefacto solo cuando staging quedó completo.
        _commit_staged_flow_bundle(
            staging_dir,
            paths.root_dir,
            mode=options_eff.mode,
            files_written=snapshot.files_written,
            issues=issues,
        )
        staging_dir = None
    finally:
        if staging_dir is not None:
            _cleanup_staging_dir(staging_dir, paths.root_dir, snapshot.files_written, issues)

    # Se alinea la metadata del dataset en memoria recién después del commit exitoso.
    flows.metadata = copy.deepcopy(snapshot.metadata_for_persist)

    # Se construye el summary/evidencia final de la operación con el mismo contrato del sidecar.
    summary = _build_write_flows_summary(
        n_flows=int(len(flows.flows)),
        n_flow_to_trips=snapshot.n_flow_to_trips,
        path=paths.root_dir,
        dataset_id=snapshot.dataset_id,
        artifact_id=snapshot.artifact_id,
        files_written=snapshot.files_written,
    )

    # Se retorna un OperationReport pequeño y estable, usando la misma vista de parameters del evento.
    return OperationReport(
        ok=not any(issue.level == "error" for issue in issues),
        issues=issues,
        summary=summary,
        parameters=parameters,
    )



def read_flows(
    path: PathLike,
    *,
    options: Optional[ReadFlowsOptions] = None,
) -> Tuple[FlowDataset, OperationReport]:
    """
    Reconstruye un FlowDataset desde un artefacto formal de flows (v1.1).

    Parameters
    ----------
    path : PathLike
        Directorio del artefacto formal persistido. Si el path exacto no existe
        y no termina en `.golondrina`, la operación intenta automáticamente con
        el sufijo canónico antes de fallar.
    options : ReadFlowsOptions, optional
        Opciones efectivas de lectura. Si None, se usan defaults.

    Returns
    -------
    tuple[FlowDataset, OperationReport]
        Dataset reconstruido y reporte estructurado de la lectura.
    """
    # Se inicializa evidencia y se fijan las options efectivas de lectura.
    issues: List[Issue] = []
    options_eff = options or ReadFlowsOptions()

    # Se resuelve primero el root efectivo. Si el path exacto no existe,
    # se intenta con el sufijo canónico `.golondrina`.
    read_root = _resolve_flows_artifact_root_for_read(path)
    paths = _resolve_flows_artifact_paths(read_root)
    parameters = _options_to_read_parameters(path=paths.root_dir, options=options_eff)

    # Se valida que exista el layout formal mínimo antes de cargar sidecar o tabla.
    _validate_read_layout(paths.root_dir, paths, strict=options_eff.strict, issues=issues)

    # Se carga el sidecar y se resuelve el backend de almacenamiento desde la metadata persistida.
    sidecar_payload = _load_flow_sidecar(
        paths.sidecar_path,
        strict=options_eff.strict,
        issues=issues,
        destination_path=paths.root_dir,
    )
    recovered = _recover_flow_read_state(
        sidecar_payload,
        strict=options_eff.strict,
        issues=issues,
        destination_path=paths.root_dir,
    )

    # Se leen las tablas persistidas que el contrato de lectura autoriza materializar.
    flows_df = _read_flows_table(
        paths.data_path,
        storage_format=recovered["storage_format"],
        issues=issues,
        destination_path=paths.root_dir,
    )
    flow_to_trips, flow_to_trips_loaded, files_read, n_flow_to_trips = _read_optional_flow_to_trips(
        paths.flow_to_trips_path,
        requested=options_eff.read_flow_to_trips,
        strict=options_eff.strict,
        storage_format=recovered["storage_format"],
        issues=issues,
        destination_path=paths.root_dir,
    )
    files_read.insert(0, "flows.parquet")
    files_read.append("flows.metadata.json")

    # Se reconstruye el dataset vivo desde los snapshots efectivos y se fuerza el estado no validado.
    metadata_out = _safe_deepcopy_dict(recovered["metadata"], default={})
    previous_validated = _extract_validated_flag(metadata_out)
    metadata_out["dataset_id"] = recovered["dataset_id"]
    metadata_out["artifact_id"] = recovered["artifact_id"]
    metadata_out["is_validated"] = False
    if previous_validated:
        # Se deja evidencia explícita porque read siempre fuerza estado no validado al final.
        emit_issue(
            issues,
            READ_FLOWS_ISSUES,
            "READ_FLOWS.METADATA.VALIDATED_FORCED_FALSE",
            path=str(paths.root_dir),
            strict=bool(options_eff.strict),
            reason="force_unvalidated_after_read",
        )

    dataset = FlowDataset(
        flows=flows_df,
        flow_to_trips=flow_to_trips,
        aggregation_spec=_safe_deepcopy_dict(recovered["aggregation_spec"], default={}),
        source_trips=None,
        metadata=metadata_out,
        provenance=_safe_deepcopy_dict(recovered["provenance"], default={}),
    )

    # Se construye el summary estable y, si corresponde, se registra el evento de lectura.
    summary = _build_read_summary(
        flows_df=flows_df,
        flow_to_trips_loaded=flow_to_trips_loaded,
        n_flow_to_trips=n_flow_to_trips,
        files_read=files_read,
        dataset_id=recovered["dataset_id"],
        artifact_id=recovered["artifact_id"],
    )
    issues_summary = _build_issues_summary(issues)
    if options_eff.keep_metadata:
        event = _build_io_event(
            op="read_flows",
            parameters=parameters,
            summary=summary,
            issues_summary=issues_summary,
        )
        try:
            dataset.metadata = _append_event(dataset.metadata, event)
        except Exception as exc:
            # Se degrada con warning porque el dataset ya fue reconstruido y la falla afecta solo el append del evento.
            emit_issue(
                issues,
                READ_FLOWS_ISSUES,
                "READ_FLOWS.EVENT.APPEND_FAILED",
                path=str(paths.root_dir),
                strict=bool(options_eff.strict),
                keep_metadata=True,
                reason=str(exc),
            )

    # Se retorna el dataset reconstruido junto con el OperationReport observable de la lectura.
    report = OperationReport(
        ok=not any(issue.level == "error" for issue in issues),
        issues=issues,
        summary=summary,
        parameters=parameters,
    )
    return dataset, report


# -----------------------------------------------------------------------------
# Helpers internos principales del pipeline de la operación
# -----------------------------------------------------------------------------

def _validate_write_contract(
    flows: FlowDataset,
    path: PathLike,
    options_eff: WriteFlowsOptions,
    *,
    issues: List[Issue],
) -> None:
    """
    Valida el contrato mínimo de write antes de tocar disco.

    Emite codes
    -----------
    - WRITE_FLOWS.INPUT.INVALID_DATASET
    - WRITE_FLOWS.INPUT.MISSING_FLOWS_TABLE
    - WRITE_FLOWS.OPTIONS.INVALID_MODE
    - WRITE_FLOWS.OPTIONS.UNSUPPORTED_STORAGE_FORMAT
    - WRITE_FLOWS.PATH.INVALID_TARGET
    """
    # Se valida que el input realmente sea un FlowDataset interpretable.
    if not isinstance(flows, FlowDataset):
        # Se aborta porque la operación necesita el contrato completo del dataset de flows.
        emit_and_maybe_raise(
            issues,
            WRITE_FLOWS_ISSUES,
            "WRITE_FLOWS.INPUT.INVALID_DATASET",
            strict=False,
            exception_map=EXCEPTION_MAP_WRITE,
            default_exception=ExportError,
            path=str(path),
            reason="expected_flowdataset",
        )

    # Se valida la superficie tabular viva antes de pensar en persistencia.
    if not hasattr(flows, "flows") or not isinstance(flows.flows, pd.DataFrame):
        # Se aborta porque no existe una tabla serializable para flows.parquet.
        emit_and_maybe_raise(
            issues,
            WRITE_FLOWS_ISSUES,
            "WRITE_FLOWS.INPUT.MISSING_FLOWS_TABLE",
            strict=False,
            exception_map=EXCEPTION_MAP_WRITE,
            default_exception=ExportError,
            path=str(path),
            reason="expected_dataframe",
        )

    # Se valida el modo explícito de escritura para no abrir rutas ambiguas.
    if options_eff.mode not in {"error_if_exists", "overwrite"}:
        # Se aborta porque la política de colisión de destino quedó cerrada por contrato.
        emit_and_maybe_raise(
            issues,
            WRITE_FLOWS_ISSUES,
            "WRITE_FLOWS.OPTIONS.INVALID_MODE",
            strict=False,
            exception_map=EXCEPTION_MAP_WRITE,
            default_exception=ExportError,
            path=str(path),
            mode=options_eff.mode,
            reason="invalid_mode",
        )

    # Se valida el backend tabular soportado en v1.1.
    if options_eff.storage_format not in _SUPPORTED_STORAGE_FORMATS:
        # Se aborta porque el sidecar debe ser autocontenible y consistente con el backend real.
        emit_and_maybe_raise(
            issues,
            WRITE_FLOWS_ISSUES,
            "WRITE_FLOWS.OPTIONS.UNSUPPORTED_STORAGE_FORMAT",
            strict=False,
            exception_map=EXCEPTION_MAP_WRITE,
            default_exception=ExportError,
            path=str(path),
            storage_format=options_eff.storage_format,
            reason="unsupported_storage_format",
        )

    # La compresión se deja pasar hasta la capa de escritura real; si el backend no la soporta,
    # la falla se captura como WRITE_FLOWS.IO.FLOWS_WRITE_FAILED con evidencia concreta del motivo.

    # Se valida que el directorio destino sea resoluble como root formal del artefacto.
    try:
        _resolve_flows_artifact_paths(path)
    except Exception as exc:
        # Se aborta porque no se puede construir un layout formal sobre una ruta ilegible.
        emit_and_maybe_raise(
            issues,
            WRITE_FLOWS_ISSUES,
            "WRITE_FLOWS.PATH.INVALID_TARGET",
            strict=False,
            exception_map=EXCEPTION_MAP_WRITE,
            default_exception=ExportError,
            path=str(path),
            mode=options_eff.mode,
            normalize_artifact_dir=bool(options_eff.normalize_artifact_dir),
            reason=str(exc),
        )


def _freeze_flow_write_snapshot(
    flows: FlowDataset,
    paths: FlowsArtifactPaths,
    options_eff: WriteFlowsOptions,
    *,
    existing_issues: Sequence[Issue],
) -> FlowWriteSnapshot:
    """
    Congela el snapshot serializable y el sidecar oficial de write_flows.

    Emite codes
    -----------
    - WRITE_FLOWS.METADATA.DATASET_ID_CREATED
    - WRITE_FLOWS.METADATA.DATASET_ID_REGENERATED
    - WRITE_FLOWS.SNAPSHOT.AGGREGATION_SPEC_INVALID
    - WRITE_FLOWS.SNAPSHOT.NOT_JSON_SERIALIZABLE
    - WRITE_FLOWS.SNAPSHOT.SIDECAR_INCONSISTENT
    - WRITE_FLOWS.FLOW_TO_TRIPS.REQUESTED_BUT_MISSING
    """
    issues: List[Issue] = []

    # Se toma una copia viva de metadata para alinear ids/eventos sin contaminar el objeto original todavía.
    metadata_base = _safe_deepcopy_dict(getattr(flows, "metadata", None), default={})
    provenance = _safe_deepcopy_dict(getattr(flows, "provenance", None), default={})
    aggregation_spec = _safe_deepcopy_dict(getattr(flows, "aggregation_spec", None), default=None)

    if not isinstance(aggregation_spec, dict):
        # Se aborta porque aggregation_spec es parte estructural del sidecar formal de flows.
        emit_and_maybe_raise(
            issues,
            WRITE_FLOWS_ISSUES,
            "WRITE_FLOWS.SNAPSHOT.AGGREGATION_SPEC_INVALID",
            strict=False,
            exception_map=EXCEPTION_MAP_WRITE,
            default_exception=ExportError,
            path=str(paths.root_dir),
            reason="aggregation_spec_not_mapping",
        )

    dataset_id, dataset_id_status = _ensure_dataset_id(metadata_base)
    if dataset_id_status == "created":
        # Se deja evidencia porque el dataset todavía no tenía identidad lógica persistible.
        emit_issue(
            issues,
            WRITE_FLOWS_ISSUES,
            "WRITE_FLOWS.METADATA.DATASET_ID_CREATED",
            path=str(paths.root_dir),
            dataset_id=dataset_id,
        )
    elif dataset_id_status == "regenerated":
        # Se deja warning porque la identidad previa existía pero no era interpretable.
        emit_issue(
            issues,
            WRITE_FLOWS_ISSUES,
            "WRITE_FLOWS.METADATA.DATASET_ID_REGENERATED",
            path=str(paths.root_dir),
            dataset_id=dataset_id,
            reason="invalid_dataset_id",
        )

    artifact_id = _new_artifact_id()
    metadata_base["dataset_id"] = dataset_id
    metadata_base["artifact_id"] = artifact_id

    files_written = ["flows.parquet", "flows.metadata.json"]
    n_flow_to_trips: Optional[int] = None
    if options_eff.write_flow_to_trips and isinstance(flows.flow_to_trips, pd.DataFrame):
        files_written.append("flow_to_trips.parquet")
        n_flow_to_trips = int(len(flows.flow_to_trips))
    elif options_eff.write_flow_to_trips:
        # Se registra warning porque el auxiliar fue pedido, pero el dataset no lo trae en memoria.
        emit_issue(
            issues,
            WRITE_FLOWS_ISSUES,
            "WRITE_FLOWS.FLOW_TO_TRIPS.REQUESTED_BUT_MISSING",
            path=str(paths.root_dir),
            write_flow_to_trips=True,
            artifact="flow_to_trips.parquet",
            reason="flow_to_trips_missing_in_memory",
        )

    tables = {
        "flows": {
            "n_rows": int(len(flows.flows)),
            "n_cols": int(len(flows.flows.columns)),
            "columns": [str(col) for col in flows.flows.columns],
        },
        "flow_to_trips": None,
    }
    if isinstance(flows.flow_to_trips, pd.DataFrame) and "flow_to_trips.parquet" in files_written:
        tables["flow_to_trips"] = {
            "n_rows": int(len(flows.flow_to_trips)),
            "n_cols": int(len(flows.flow_to_trips.columns)),
            "columns": [str(col) for col in flows.flow_to_trips.columns],
        }

    summary = {
        "n_flows": int(len(flows.flows)),
        "n_flow_to_trips": n_flow_to_trips,
        "files_written": list(files_written),
        "dataset_id": dataset_id,
        "artifact_id": artifact_id,
    }
    parameters = _options_to_write_parameters(path=paths.root_dir, options=options_eff)
    issues_summary = _build_issues_summary([*existing_issues, *issues])
    event = _build_io_event(
        op="write_flows",
        parameters=parameters,
        summary=summary,
        issues_summary=issues_summary,
    )
    metadata_for_persist = _append_event(metadata_base, event)

    sidecar_payload = {
        "dataset_type": "flows",
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
            "data": "flows.parquet",
            "metadata": "flows.metadata.json",
            "flow_to_trips": "flow_to_trips.parquet" if "flow_to_trips.parquet" in files_written else None,
        },
        "aggregation_spec": aggregation_spec,
        "provenance": provenance,
        "metadata": metadata_for_persist,
        "tables": tables,
    }

    _assert_json_safe(aggregation_spec, label="aggregation_spec", issues=issues, path=paths.root_dir, artifact="flows.metadata.json")
    _assert_json_safe(provenance, label="provenance", issues=issues, path=paths.root_dir, artifact="flows.metadata.json")
    _assert_json_safe(metadata_for_persist, label="metadata", issues=issues, path=paths.root_dir, artifact="flows.metadata.json")
    _assert_json_safe(sidecar_payload, label="sidecar_payload", issues=issues, path=paths.root_dir, artifact="flows.metadata.json")

    missing_top_level = sorted(_REQUIRED_SIDECAR_TOP_LEVEL - set(sidecar_payload.keys()))
    if missing_top_level:
        # Se aborta porque el sidecar no puede quedar estructuralmente incompleto.
        emit_and_maybe_raise(
            issues,
            WRITE_FLOWS_ISSUES,
            "WRITE_FLOWS.SNAPSHOT.SIDECAR_INCONSISTENT",
            strict=False,
            exception_map=EXCEPTION_MAP_WRITE,
            default_exception=ExportError,
            path=str(paths.root_dir),
            artifact="flows.metadata.json",
            reason=f"missing_top_level={missing_top_level}",
        )

    return FlowWriteSnapshot(
        dataset_id=dataset_id,
        artifact_id=artifact_id,
        dataset_id_status=dataset_id_status,
        metadata_for_persist=metadata_for_persist,
        sidecar_payload=sidecar_payload,
        files_written=files_written,
        n_flow_to_trips=n_flow_to_trips,
        issues=issues,
    )


def _create_flows_staging_dir(final_dir: Path, *, issues: List[Issue]) -> Path:
    """
    Crea el staging_dir temporal hermano del destino final.

    Emite codes
    -----------
    - WRITE_FLOWS.IO.STAGING_CREATE_FAILED
    """
    parent_dir = final_dir.parent if final_dir.parent != Path("") else Path(".")
    try:
        parent_dir.mkdir(parents=True, exist_ok=True)
        staging_dir = Path(
            tempfile.mkdtemp(prefix=f".{final_dir.name}.staging_", dir=str(parent_dir))
        )
        return staging_dir
    except Exception as exc:
        # Se aborta porque sin staging no hay forma segura de materializar el bundle formal.
        emit_and_maybe_raise(
            issues,
            WRITE_FLOWS_ISSUES,
            "WRITE_FLOWS.IO.STAGING_CREATE_FAILED",
            strict=False,
            exception_map=EXCEPTION_MAP_WRITE,
            default_exception=ExportError,
            path=str(final_dir),
            artifact=str(final_dir.name),
            reason=str(exc),
        )
        raise AssertionError("unreachable")


def _write_flows_table_to_staging(
    df_flows: pd.DataFrame,
    parquet_path: Path,
    *,
    storage_format: str,
    parquet_compression: ParquetCompression,
    issues: List[Issue],
    destination_path: Path,
) -> None:
    """
    Escribe `flows.parquet` en staging.

    Emite codes
    -----------
    - WRITE_FLOWS.IO.FLOWS_WRITE_FAILED
    """
    try:
        parquet_path.parent.mkdir(parents=True, exist_ok=True)
        if storage_format != "parquet":
            raise ValueError(f"unsupported storage_format: {storage_format!r}")
        df_out = _prepare_dataframe_for_parquet(df_flows)
        compression = None if parquet_compression == "none" else parquet_compression
        df_out.to_parquet(parquet_path, index=False, compression=compression)
    except Exception as exc:
        # Se aborta porque la tabla principal del bundle no quedó materializada.
        emit_and_maybe_raise(
            issues,
            WRITE_FLOWS_ISSUES,
            "WRITE_FLOWS.IO.FLOWS_WRITE_FAILED",
            strict=False,
            exception_map=EXCEPTION_MAP_WRITE,
            default_exception=ExportError,
            path=str(destination_path),
            artifact="flows.parquet",
            storage_format=storage_format,
            parquet_compression=parquet_compression,
            reason=str(exc),
        )


def _write_optional_flow_to_trips_to_staging(
    flow_to_trips: Optional[pd.DataFrame],
    parquet_path: Path,
    *,
    write_flow_to_trips: bool,
    storage_format: str,
    parquet_compression: ParquetCompression,
    issues: List[Issue],
    destination_path: Path,
) -> None:
    """
    Escribe `flow_to_trips.parquet` solo cuando el contrato lo pide y el auxiliar existe.

    Emite codes
    -----------
    - WRITE_FLOWS.IO.FLOW_TO_TRIPS_WRITE_FAILED
    """
    # Si el usuario no pidió el auxiliar o no existe en memoria, no se escribe nada.
    if not write_flow_to_trips or not isinstance(flow_to_trips, pd.DataFrame):
        return

    try:
        parquet_path.parent.mkdir(parents=True, exist_ok=True)
        if storage_format != "parquet":
            raise ValueError(f"unsupported storage_format: {storage_format!r}")
        df_out = _prepare_dataframe_for_parquet(flow_to_trips)
        compression = None if parquet_compression == "none" else parquet_compression
        df_out.to_parquet(parquet_path, index=False, compression=compression)
    except Exception as exc:
        # Se aborta porque el auxiliar fue solicitado explícitamente y su escritura falló.
        emit_and_maybe_raise(
            issues,
            WRITE_FLOWS_ISSUES,
            "WRITE_FLOWS.IO.FLOW_TO_TRIPS_WRITE_FAILED",
            strict=False,
            exception_map=EXCEPTION_MAP_WRITE,
            default_exception=ExportError,
            path=str(destination_path),
            artifact="flow_to_trips.parquet",
            storage_format=storage_format,
            parquet_compression=parquet_compression,
            reason=str(exc),
        )


def _write_flow_sidecar_to_staging(
    sidecar_payload: Dict[str, Any],
    sidecar_path: Path,
    *,
    issues: List[Issue],
    destination_path: Path,
) -> None:
    """
    Escribe `flows.metadata.json` en staging.

    Emite codes
    -----------
    - WRITE_FLOWS.IO.SIDECAR_WRITE_FAILED
    """
    try:
        sidecar_path.parent.mkdir(parents=True, exist_ok=True)
        with sidecar_path.open("w", encoding="utf-8") as fh:
            json.dump(sidecar_payload, fh, ensure_ascii=False, indent=2)
    except Exception as exc:
        # Se aborta porque sin sidecar no existe persistencia formal ni round-trip confiable.
        emit_and_maybe_raise(
            issues,
            WRITE_FLOWS_ISSUES,
            "WRITE_FLOWS.IO.SIDECAR_WRITE_FAILED",
            strict=False,
            exception_map=EXCEPTION_MAP_WRITE,
            default_exception=ExportError,
            path=str(destination_path),
            artifact="flows.metadata.json",
            reason=str(exc),
        )


def _assert_flows_staging_complete(
    paths: FlowsArtifactPaths,
    *,
    expected_files: Sequence[str],
    issues: List[Issue],
    destination_path: Path,
) -> None:
    """
    Verifica que el staging haya quedado completo antes del commit.

    Emite codes
    -----------
    - WRITE_FLOWS.IO.STAGING_INCOMPLETE
    """
    expected_paths = {
        "flows.parquet": paths.data_path,
        "flows.metadata.json": paths.sidecar_path,
        "flow_to_trips.parquet": paths.flow_to_trips_path,
    }
    missing = [name for name in expected_files if not expected_paths[name].exists()]
    if missing:
        # Se aborta porque el commit no puede promover un bundle incompleto.
        emit_and_maybe_raise(
            issues,
            WRITE_FLOWS_ISSUES,
            "WRITE_FLOWS.IO.STAGING_INCOMPLETE",
            strict=False,
            exception_map=EXCEPTION_MAP_WRITE,
            default_exception=ExportError,
            path=str(destination_path),
            artifact=", ".join(missing),
            reason=f"missing_files={missing}",
        )


def _commit_staged_flow_bundle(
    staging_dir: Path,
    final_dir: Path,
    *,
    mode: WriteMode,
    files_written: Sequence[str],
    issues: List[Issue],
) -> None:
    """
    Promueve el staging completo al destino final del bundle.

    Emite codes
    -----------
    - WRITE_FLOWS.LAYOUT.BUNDLE_EXISTS
    - WRITE_FLOWS.LAYOUT.BUNDLE_OVERWRITTEN
    - WRITE_FLOWS.IO.COMMIT_FAILED
    """
    try:
        if final_dir.exists():
            if mode == "error_if_exists":
                # Se aborta porque el contrato no permite colisiones silenciosas del bundle.
                emit_and_maybe_raise(
                    issues,
                    WRITE_FLOWS_ISSUES,
                    "WRITE_FLOWS.LAYOUT.BUNDLE_EXISTS",
                    strict=False,
                    exception_map=EXCEPTION_MAP_WRITE,
                    default_exception=ExportError,
                    path=str(final_dir),
                    mode=mode,
                    artifact=final_dir.name,
                )
            else:
                # Se deja evidencia porque overwrite es una política explícita y observable.
                emit_issue(
                    issues,
                    WRITE_FLOWS_ISSUES,
                    "WRITE_FLOWS.LAYOUT.BUNDLE_OVERWRITTEN",
                    path=str(final_dir),
                    mode=mode,
                    artifact=final_dir.name,
                )
                if final_dir.is_dir():
                    shutil.rmtree(final_dir)
                else:
                    final_dir.unlink()

        final_dir.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(staging_dir), str(final_dir))
    except Exception as exc:
        # Se aborta porque el bundle no logró materializarse en su ruta final observable.
        emit_and_maybe_raise(
            issues,
            WRITE_FLOWS_ISSUES,
            "WRITE_FLOWS.IO.COMMIT_FAILED",
            strict=False,
            exception_map=EXCEPTION_MAP_WRITE,
            default_exception=ExportError,
            path=str(final_dir),
            artifact=", ".join(files_written),
            mode=mode,
            reason=str(exc),
        )


def _validate_read_layout(
    read_root: Path,
    paths: FlowsArtifactPaths,
    *,
    strict: bool,
    issues: List[Issue],
) -> None:
    """
    Valida el layout formal mínimo antes de leer sidecar o tablas.

    Emite codes
    -----------
    - READ_FLOWS.PATH.INVALID_ROOT
    - READ_FLOWS.LAYOUT.MISSING_DATA_FILE
    - READ_FLOWS.LAYOUT.MISSING_SIDECAR
    """
    files_expected = ["flows.parquet", "flows.metadata.json"]
    if not read_root.exists() or not read_root.is_dir():
        # Se aborta porque no existe un root formal sobre el cual resolver el artefacto.
        emit_and_maybe_raise(
            issues,
            READ_FLOWS_ISSUES,
            "READ_FLOWS.PATH.INVALID_ROOT",
            strict=strict,
            exception_map=EXCEPTION_MAP_READ,
            default_exception=ExportError,
            path=str(read_root),
            files_expected=files_expected,
            reason="root_not_directory",
        )

    if not paths.data_path.exists():
        # Se aborta porque la tabla principal es obligatoria en el round-trip formal.
        emit_and_maybe_raise(
            issues,
            READ_FLOWS_ISSUES,
            "READ_FLOWS.LAYOUT.MISSING_DATA_FILE",
            strict=strict,
            exception_map=EXCEPTION_MAP_READ,
            default_exception=ExportError,
            path=str(read_root),
            files_expected=files_expected,
            reason="missing_flows_parquet",
        )

    if not paths.sidecar_path.exists():
        # Se aborta porque la lectura formal v1.1 no es recuperable sin sidecar obligatorio.
        emit_and_maybe_raise(
            issues,
            READ_FLOWS_ISSUES,
            "READ_FLOWS.LAYOUT.MISSING_SIDECAR",
            strict=strict,
            exception_map=EXCEPTION_MAP_READ,
            default_exception=ExportError,
            path=str(read_root),
            files_expected=files_expected,
            reason="missing_flows_metadata_json",
        )


def _load_flow_sidecar(
    sidecar_path: Path,
    *,
    strict: bool,
    issues: List[Issue],
    destination_path: Path,
) -> Dict[str, Any]:
    """
    Lee y parsea el sidecar formal de flows.

    Emite codes
    -----------
    - READ_FLOWS.IO.SIDECAR_READ_FAILED
    - READ_FLOWS.SIDECAR.INVALID_TOP_LEVEL
    """
    try:
        with sidecar_path.open("r", encoding="utf-8") as fh:
            payload = json.load(fh)
    except Exception as exc:
        # Se aborta porque sin sidecar parseable no existe lectura formal confiable.
        emit_and_maybe_raise(
            issues,
            READ_FLOWS_ISSUES,
            "READ_FLOWS.IO.SIDECAR_READ_FAILED",
            strict=strict,
            exception_map=EXCEPTION_MAP_READ,
            default_exception=ExportError,
            path=str(destination_path),
            reason=str(exc),
        )
        raise AssertionError("unreachable")

    if not isinstance(payload, Mapping):
        # Se aborta porque el sidecar debe ser un objeto top-level interpretable.
        emit_and_maybe_raise(
            issues,
            READ_FLOWS_ISSUES,
            "READ_FLOWS.SIDECAR.INVALID_TOP_LEVEL",
            strict=strict,
            exception_map=EXCEPTION_MAP_READ,
            default_exception=ExportError,
            path=str(destination_path),
            reason="sidecar_not_mapping",
        )

    missing_top_level = sorted(_REQUIRED_SIDECAR_TOP_LEVEL - set(payload.keys()))
    if missing_top_level:
        # Se aborta porque el sidecar no cumple la estructura top-level mínima del contrato.
        emit_and_maybe_raise(
            issues,
            READ_FLOWS_ISSUES,
            "READ_FLOWS.SIDECAR.INVALID_TOP_LEVEL",
            strict=strict,
            exception_map=EXCEPTION_MAP_READ,
            default_exception=ExportError,
            path=str(destination_path),
            reason=f"missing_top_level={missing_top_level}",
        )

    return dict(payload)


def _recover_flow_read_state(
    sidecar: Mapping[str, Any],
    *,
    strict: bool,
    issues: List[Issue],
    destination_path: Path,
) -> Dict[str, Any]:
    """
    Aplica la matriz de recuperación de read_flows antes de reconstruir el dataset.

    Emite codes
    -----------
    - READ_FLOWS.STORAGE.UNSUPPORTED_FORMAT
    - READ_FLOWS.METADATA.DATASET_ID_REGENERATED
    - READ_FLOWS.METADATA.ARTIFACT_ID_SET_NONE
    - READ_FLOWS.SIDECAR.AGGREGATION_SPEC_DEFAULTED
    - READ_FLOWS.SIDECAR.PROVENANCE_DEFAULTED
    - READ_FLOWS.SIDECAR.METADATA_DEFAULTED
    - READ_FLOWS.SIDECAR.INVALID_TOP_LEVEL
    """
    storage = sidecar.get("storage")
    storage_format = None
    if isinstance(storage, Mapping):
        storage_format = storage.get("format")

    if storage_format not in _SUPPORTED_STORAGE_FORMATS:
        # Se aborta porque el backend declarado por el artefacto no es soportado por read_flows.
        emit_and_maybe_raise(
            issues,
            READ_FLOWS_ISSUES,
            "READ_FLOWS.STORAGE.UNSUPPORTED_FORMAT",
            strict=strict,
            exception_map=EXCEPTION_MAP_READ,
            default_exception=ExportError,
            path=str(destination_path),
            storage_format=storage_format,
            reason="unsupported_storage_format",
        )

    dataset_id = sidecar.get("dataset_id")
    if _is_non_empty_string(dataset_id):
        dataset_id_eff = str(dataset_id)
    elif strict:
        # Se aborta porque strict=True exige identidad lógica interpretable en el artefacto.
        emit_and_maybe_raise(
            issues,
            READ_FLOWS_ISSUES,
            "READ_FLOWS.SIDECAR.INVALID_TOP_LEVEL",
            strict=True,
            exception_map=EXCEPTION_MAP_READ,
            default_exception=ExportError,
            path=str(destination_path),
            reason="invalid_dataset_id",
        )
        raise AssertionError("unreachable")
    else:
        dataset_id_eff = _new_dataset_id()
        # Se degrada porque strict=False permite regenerar dataset_id faltante o inválido.
        emit_issue(
            issues,
            READ_FLOWS_ISSUES,
            "READ_FLOWS.METADATA.DATASET_ID_REGENERATED",
            path=str(destination_path),
            reason="invalid_dataset_id",
        )

    artifact_id = sidecar.get("artifact_id")
    if _is_non_empty_string(artifact_id):
        artifact_id_eff: Optional[str] = str(artifact_id)
    elif strict:
        # Se aborta porque strict=True exige artifact_id interpretable para trazabilidad del bundle.
        emit_and_maybe_raise(
            issues,
            READ_FLOWS_ISSUES,
            "READ_FLOWS.SIDECAR.INVALID_TOP_LEVEL",
            strict=True,
            exception_map=EXCEPTION_MAP_READ,
            default_exception=ExportError,
            path=str(destination_path),
            reason="invalid_artifact_id",
        )
        raise AssertionError("unreachable")
    else:
        artifact_id_eff = None
        # Se degrada a None porque artifact_id ausente no debe inventarse como si fuera original.
        emit_issue(
            issues,
            READ_FLOWS_ISSUES,
            "READ_FLOWS.METADATA.ARTIFACT_ID_SET_NONE",
            path=str(destination_path),
            reason="invalid_artifact_id",
        )

    aggregation_spec = sidecar.get("aggregation_spec")
    if not isinstance(aggregation_spec, Mapping):
        if strict:
            # Se aborta porque strict=True exige aggregation_spec interpretable en el sidecar.
            emit_and_maybe_raise(
                issues,
                READ_FLOWS_ISSUES,
                "READ_FLOWS.SIDECAR.INVALID_TOP_LEVEL",
                strict=True,
                exception_map=EXCEPTION_MAP_READ,
                default_exception=ExportError,
                path=str(destination_path),
                reason="invalid_aggregation_spec",
            )
            raise AssertionError("unreachable")
        aggregation_spec = {}
        # Se degrada a {} porque strict=False permite reconstrucción mínima del dataset.
        emit_issue(
            issues,
            READ_FLOWS_ISSUES,
            "READ_FLOWS.SIDECAR.AGGREGATION_SPEC_DEFAULTED",
            path=str(destination_path),
            reason="invalid_aggregation_spec",
        )
    else:
        aggregation_spec = _safe_deepcopy_dict(aggregation_spec, default={})

    provenance = sidecar.get("provenance")
    if not isinstance(provenance, Mapping):
        provenance = {}
        # Se deja info porque la reconstrucción puede continuar sin provenance detallada.
        emit_issue(
            issues,
            READ_FLOWS_ISSUES,
            "READ_FLOWS.SIDECAR.PROVENANCE_DEFAULTED",
            path=str(destination_path),
            reason="invalid_or_missing_provenance",
        )
    else:
        provenance = _safe_deepcopy_dict(provenance, default={})

    metadata = sidecar.get("metadata")
    if not isinstance(metadata, Mapping):
        if strict:
            # Se aborta porque strict=True no permite reconstruir metadata viva desde un bloque inválido.
            emit_and_maybe_raise(
                issues,
                READ_FLOWS_ISSUES,
                "READ_FLOWS.SIDECAR.INVALID_TOP_LEVEL",
                strict=True,
                exception_map=EXCEPTION_MAP_READ,
                default_exception=ExportError,
                path=str(destination_path),
                reason="invalid_metadata",
            )
            raise AssertionError("unreachable")
        metadata = {}
        # Se degrada porque strict=False permite reconstrucción mínima de metadata.
        emit_issue(
            issues,
            READ_FLOWS_ISSUES,
            "READ_FLOWS.SIDECAR.METADATA_DEFAULTED",
            path=str(destination_path),
            reason="invalid_metadata",
        )
    else:
        metadata = _safe_deepcopy_dict(metadata, default={})

    return {
        "storage_format": str(storage_format),
        "dataset_id": dataset_id_eff,
        "artifact_id": artifact_id_eff,
        "aggregation_spec": aggregation_spec,
        "provenance": provenance,
        "metadata": metadata,
    }


def _read_flows_table(
    data_path: Path,
    *,
    storage_format: str,
    issues: List[Issue],
    destination_path: Path,
) -> pd.DataFrame:
    """
    Lee la tabla principal `flows.parquet` usando el backend resuelto desde sidecar.

    Emite codes
    -----------
    - READ_FLOWS.IO.FLOWS_READ_FAILED
    """
    try:
        if storage_format != "parquet":
            raise ValueError(f"unsupported storage_format: {storage_format!r}")
        return pd.read_parquet(data_path)
    except Exception as exc:
        # Se aborta porque la tabla principal del bundle no pudo materializarse en memoria.
        emit_and_maybe_raise(
            issues,
            READ_FLOWS_ISSUES,
            "READ_FLOWS.IO.FLOWS_READ_FAILED",
            strict=False,
            exception_map=EXCEPTION_MAP_READ,
            default_exception=ExportError,
            path=str(destination_path),
            files_read=["flows.metadata.json"],
            reason=str(exc),
        )
        raise AssertionError("unreachable")


def _read_optional_flow_to_trips(
    aux_path: Path,
    *,
    requested: bool,
    strict: bool,
    storage_format: str,
    issues: List[Issue],
    destination_path: Path,
) -> Tuple[Optional[pd.DataFrame], bool, List[str], Optional[int]]:
    """
    Resuelve la carga opcional de `flow_to_trips.parquet`.

    Emite codes
    -----------
    - READ_FLOWS.FLOW_TO_TRIPS.REQUESTED_BUT_MISSING
    - READ_FLOWS.IO.FLOW_TO_TRIPS_READ_FAILED
    """
    files_read: List[str] = []
    if not requested:
        return None, False, files_read, None

    if not aux_path.exists():
        if strict:
            # Se aborta porque strict=True no permite degradar la ausencia del auxiliar solicitado.
            emit_and_maybe_raise(
                issues,
                READ_FLOWS_ISSUES,
                "READ_FLOWS.IO.FLOW_TO_TRIPS_READ_FAILED",
                strict=True,
                exception_map=EXCEPTION_MAP_READ,
                default_exception=ExportError,
                path=str(destination_path),
                read_flow_to_trips=True,
                files_read=files_read,
                reason="missing_flow_to_trips_file",
            )
            raise AssertionError("unreachable")
        # Se degrada porque strict=False permite continuar sin el auxiliar ausente.
        emit_issue(
            issues,
            READ_FLOWS_ISSUES,
            "READ_FLOWS.FLOW_TO_TRIPS.REQUESTED_BUT_MISSING",
            path=str(destination_path),
            read_flow_to_trips=True,
            files_expected=["flow_to_trips.parquet"],
            files_read=files_read,
            reason="missing_flow_to_trips_file",
        )
        return None, False, files_read, None

    try:
        if storage_format != "parquet":
            raise ValueError(f"unsupported storage_format: {storage_format!r}")
        df = pd.read_parquet(aux_path)
        files_read.append("flow_to_trips.parquet")
        return df, True, files_read, int(len(df))
    except Exception as exc:
        # Se aborta porque el auxiliar solicitado existe pero no se pudo materializar en memoria.
        emit_and_maybe_raise(
            issues,
            READ_FLOWS_ISSUES,
            "READ_FLOWS.IO.FLOW_TO_TRIPS_READ_FAILED",
            strict=strict,
            exception_map=EXCEPTION_MAP_READ,
            default_exception=ExportError,
            path=str(destination_path),
            read_flow_to_trips=True,
            files_read=files_read,
            reason=str(exc),
        )
        raise AssertionError("unreachable")


def _build_read_summary(
    *,
    flows_df: pd.DataFrame,
    flow_to_trips_loaded: bool,
    n_flow_to_trips: Optional[int],
    files_read: Sequence[str],
    dataset_id: str,
    artifact_id: Optional[str],
) -> Dict[str, Any]:
    """Construye el summary estable de read_flows."""
    return {
        "n_flows": int(len(flows_df)),
        "n_columns": int(len(flows_df.columns)),
        "flow_to_trips_loaded": bool(flow_to_trips_loaded),
        "n_flow_to_trips": n_flow_to_trips,
        "files_read": list(files_read),
        "dataset_id": dataset_id,
        "artifact_id": artifact_id,
    }


# -----------------------------------------------------------------------------
# Helpers internos de uso general
# -----------------------------------------------------------------------------

def _normalize_flows_artifact_root_for_write(
    root_path: PathLike,
    *,
    normalize_artifact_dir: bool,
) -> Path:
    """Normaliza el root del artefacto en write_flows."""
    root_dir = Path(root_path).expanduser()
    if normalize_artifact_dir:
        return _append_golondrina_artifact_suffix(root_dir)
    return root_dir


def _resolve_flows_artifact_root_for_read(root_path: PathLike) -> Path:
    """Resuelve el root del artefacto en read_flows con fallback a `.golondrina`."""
    root_dir = Path(root_path).expanduser()
    if root_dir.exists():
        return root_dir
    if _has_golondrina_artifact_suffix(root_dir):
        return root_dir
    candidate = _append_golondrina_artifact_suffix(root_dir)
    if candidate.exists():
        return candidate
    return root_dir


def _resolve_flows_artifact_paths(root_path: PathLike) -> FlowsArtifactPaths:
    """Resuelve el layout formal de flows a partir del root del artefacto."""
    root_dir = Path(root_path).expanduser()
    return FlowsArtifactPaths(
        root_dir=root_dir,
        data_path=root_dir / "flows.parquet",
        sidecar_path=root_dir / "flows.metadata.json",
        flow_to_trips_path=root_dir / "flow_to_trips.parquet",
    )


def _build_write_flows_summary(
    *,
    n_flows: int,
    n_flow_to_trips: Optional[int],
    path: Path,
    dataset_id: str,
    artifact_id: str,
    files_written: Sequence[str],
) -> Dict[str, Any]:
    """Construye el summary estable de write_flows."""
    return {
        "n_flows": n_flows,
        "n_flow_to_trips": n_flow_to_trips,
        "files_written": list(files_written),
        "dataset_id": dataset_id,
        "artifact_id": artifact_id,
        "path": str(path),
    }


def _assert_json_safe(
    value: Any,
    *,
    label: str,
    issues: List[Issue],
    path: Path,
    artifact: str,
) -> None:
    """Verifica que un bloque sea JSON-safe y aborta si no lo es."""
    try:
        json.dumps(value, ensure_ascii=False)
    except Exception as exc:
        # Se aborta porque el sidecar y sus snapshots deben ser JSON-safe por contrato.
        emit_and_maybe_raise(
            issues,
            WRITE_FLOWS_ISSUES,
            "WRITE_FLOWS.SNAPSHOT.NOT_JSON_SERIALIZABLE",
            strict=False,
            exception_map=EXCEPTION_MAP_WRITE,
            default_exception=ExportError,
            path=str(path),
            artifact=artifact,
            reason=f"{label}: {exc}",
        )


def _cleanup_staging_dir(
    staging_dir: Path,
    final_dir: Path,
    files_written: Sequence[str],
    issues: List[Issue],
) -> None:
    """Intenta remover el staging residual de write_flows sin abortar de nuevo."""
    try:
        if staging_dir.exists():
            shutil.rmtree(staging_dir)
    except Exception as exc:
        # Se deja warning porque el fallo real ya ocurrió y solo queda cleanup best-effort.
        emit_issue(
            issues,
            WRITE_FLOWS_ISSUES,
            "WRITE_FLOWS.IO.CLEANUP_FAILED",
            path=str(final_dir),
            artifact=", ".join(files_written),
            reason=str(exc),
        )


def _options_to_write_parameters(*, path: PathLike, options: WriteFlowsOptions) -> Dict[str, Any]:
    """Serializa WriteFlowsOptions a parameters estables para report y evento."""
    return {
        "path": str(Path(path).expanduser()),
        "mode": options.mode,
        "storage_format": options.storage_format,
        "parquet_compression": options.parquet_compression,
        "normalize_artifact_dir": bool(options.normalize_artifact_dir),
        "write_flow_to_trips": bool(options.write_flow_to_trips),
    }


def _options_to_read_parameters(*, path: PathLike, options: ReadFlowsOptions) -> Dict[str, Any]:
    """Serializa ReadFlowsOptions a parameters estables para report y evento."""
    return {
        "path": str(Path(path).expanduser()),
        "strict": bool(options.strict),
        "keep_metadata": bool(options.keep_metadata),
        "read_flow_to_trips": bool(options.read_flow_to_trips),
    }


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
    """Agrega un evento append-only en `metadata['events']` sin mutar el input original."""
    metadata_out = _safe_deepcopy_dict(metadata, default={})
    events = metadata_out.get("events")
    if not isinstance(events, list):
        events = []
    else:
        events = copy.deepcopy(events)
    events.append(event)
    metadata_out["events"] = events
    return metadata_out


def _prepare_dataframe_for_parquet(df: pd.DataFrame) -> pd.DataFrame:
    """Hace una copia defensiva y deja listas/escalares simples listos para Parquet."""
    return df.copy(deep=True)


def _ensure_dataset_id(metadata: Dict[str, Any]) -> Tuple[str, str]:
    """Obtiene o crea dataset_id siguiendo la política vigente del bloque IO."""
    dataset_id = metadata.get("dataset_id")
    if _is_non_empty_string(dataset_id):
        return str(dataset_id), "preserved"
    if dataset_id is None:
        return _new_dataset_id(), "created"
    return _new_dataset_id(), "regenerated"


def _new_dataset_id() -> str:
    """Genera un dataset_id lógico nuevo y estable para el bloque IO."""
    return f"dset_{uuid.uuid4()}"


def _new_artifact_id() -> str:
    """Genera un artifact_id nuevo para una materialización específica."""
    return f"art_{uuid.uuid4()}"


def _extract_validated_flag(metadata: Any) -> bool:
    """Lee `metadata['is_validated']` con fallback legacy interno."""
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


def _is_non_empty_string(value: Any) -> bool:
    """Chequea si un valor puede interpretarse como string no vacío."""
    return isinstance(value, str) and value.strip() != ""


def _has_golondrina_artifact_suffix(path: Path) -> bool:
    """Chequea si el path ya usa el sufijo canónico del bundle."""
    return path.name.endswith(_GOLONDRINA_ARTIFACT_SUFFIX)


def _append_golondrina_artifact_suffix(path: Path) -> Path:
    """Agrega el sufijo `.golondrina` al nombre final del path si no existe."""
    if _has_golondrina_artifact_suffix(path):
        return path
    return path.with_name(f"{path.name}{_GOLONDRINA_ARTIFACT_SUFFIX}")


def _utc_now_iso() -> str:
    """Retorna timestamp UTC ISO-8601 compacto con sufijo Z."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
