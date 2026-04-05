# -------------------------
# file: pylondrina/export/flows.py
# -------------------------
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
from typing import Any, Dict, List, Optional, Sequence, Tuple, Literal, Union

import h3
import pandas as pd
from pandas.api import types as ptypes

from pylondrina.datasets import FlowDataset
from pylondrina.errors import ExportError
from pylondrina.issues.catalog_export_flows import EXPORT_FLOWS_ISSUES
from pylondrina.issues.core import emit_and_maybe_raise, emit_issue
from pylondrina.reports import Issue, OperationReport

PathLike = Union[str, Path]
ExportFormat = Literal["flowmap_blue"]
WriteMode = Literal["error_if_exists", "overwrite"]

EXCEPTION_MAP_EXPORT = {
    "export": ExportError,
}
_RESERVED_EXTRA_FLOW_FIELDS = {"origin", "dest", "count"}


@dataclass(frozen=True)
class FlowExportResult:
    """
    Resultado materializado de una exportación de flujos.

    Attributes
    ----------
    export_dir : str
        Directorio final del export materializado.
    artifacts : dict[str, str]
        Mapa nombre lógico -> path del artefacto escrito.
        En v1.1 incluye: `flows`, `locations`, `metadata`.
    """

    export_dir: str
    artifacts: Dict[str, str]


@dataclass(frozen=True)
class ExportFlowsOptions:
    """
    Opciones efectivas de exportación para `export_flows`.

    Parameters
    ----------
    format : {"flowmap_blue"}, default="flowmap_blue"
        Layout externo objetivo del export.
    mode : {"error_if_exists", "overwrite"}, default="error_if_exists"
        Política cuando el directorio de exportación ya existe.
    folder_name : str, optional
        Nombre de la carpeta a crear bajo `output_root`.
    extra_flow_fields : sequence of str, optional
        Columnas extra de `FlowDataset.flows` que se desean preservar en `flows.csv`.
    """

    format: ExportFormat = "flowmap_blue"
    mode: WriteMode = "error_if_exists"
    folder_name: Optional[str] = None
    extra_flow_fields: Optional[Sequence[str]] = None


# -----------------------------------------------------------------------------
# Función pública principal
# -----------------------------------------------------------------------------

def export_flows(
    flows: FlowDataset,
    output_root: PathLike,
    *,
    options: Optional[ExportFlowsOptions] = None,
) -> Tuple[FlowExportResult, OperationReport]:
    """
    Exporta un FlowDataset a un layout externo orientado a flowmap.blue.

    Parameters
    ----------
    flows : FlowDataset
        Dataset de flujos en memoria.
    output_root : PathLike
        Directorio raíz donde se creará el directorio del export.
    options : ExportFlowsOptions, optional
        Opciones efectivas del export. Si es None, se usan defaults.

    Returns
    -------
    tuple[FlowExportResult, OperationReport]
        Resultado materializado y reporte estructurado del export.
    """
    issues: List[Issue] = []

    # Se normaliza el request y se resuelve el destino antes del preflight de contenido.
    options_eff, export_dir, parameters = _resolve_export_request(flows, output_root, options)

    # Se dejan como evidencia las decisiones recuperables del request estructural.
    if parameters.pop("_folder_name_input_invalid", False):
        # Se deja warning porque el nombre pedido no era operable y fue saneado/generado.
        emit_issue(
            issues,
            EXPORT_FLOWS_ISSUES,
            "EXPORT_FLOWS.OPTIONS.INVALID_FOLDER_NAME",
            output_root=parameters["output_root"],
            export_dir=parameters["export_dir"],
            format=parameters["format"],
            mode=parameters["mode"],
            folder_name_input=parameters.get("_folder_name_input"),
            folder_name_effective=parameters["folder_name"],
        )
    if parameters.pop("_folder_name_generated", False):
        # Se deja info porque el export terminó usando un nombre de carpeta efectivo generado por el módulo.
        emit_issue(
            issues,
            EXPORT_FLOWS_ISSUES,
            "EXPORT_FLOWS.OPTIONS.FOLDER_NAME_GENERATED",
            output_root=parameters["output_root"],
            folder_name_effective=parameters["folder_name"],
            generator="export_flows",
        )
    if parameters.pop("_export_dir_exists_overwrite", False):
        # Se deja warning porque la política overwrite reemplazará el directorio destino existente.
        emit_issue(
            issues,
            EXPORT_FLOWS_ISSUES,
            "EXPORT_FLOWS.LAYOUT.EXPORT_DIR_EXISTS_OVERWRITE",
            output_root=parameters["output_root"],
            export_dir=parameters["export_dir"],
            format=parameters["format"],
            mode=parameters["mode"],
            folder_name_effective=parameters["folder_name"],
        )

    # Se verifica que el FlowDataset sea exportable sin recalcular ni reinterpretar la agregación.
    preflight_issues, preflight_info = _preflight_export_flows(flows, options_eff)
    issues.extend(preflight_issues)

    # Se transforma el layout interno a las tablas externas mínimas del formato flowmap.
    flows_out_df, locations_df = _build_flowmap_tables(
        flows.flows,
        preflight_info.get("extra_flow_fields"),
        preflight_info["count_source"],
    )

    # Se materializan artefactos y sidecar recién al cruzar la frontera de exportación.
    result, report, event_dict = _materialize_flowmap_export(
        flows,
        export_dir,
        flows_out_df,
        locations_df,
        parameters,
        preflight_info["count_source"],
    )

    if issues:
        report.issues = list(issues) + list(report.issues)
        report.ok = not any(issue.level == "error" for issue in report.issues)

    # Se cierra la trazabilidad en memoria sin afectar un export ya materializado.
    report = _append_export_event_or_warning(flows, event_dict, report)

    return result, report


# -----------------------------------------------------------------------------
# Helpers internos principales del pipeline de la operación
# -----------------------------------------------------------------------------

def _resolve_export_request(
    flows: FlowDataset,
    output_root: str,
    options: ExportFlowsOptions | None,
) -> tuple[ExportFlowsOptions, str, dict[str, Any]]:
    """
    Normaliza opciones y resuelve el directorio efectivo del export.

    Emite
    -----
    - EXPORT_FLOWS.CORE.INVALID_FLOWDATASET
    - EXPORT_FLOWS.CORE.INVALID_DATA_SURFACE
    - EXPORT_FLOWS.OPTIONS.UNSUPPORTED_FORMAT
    - EXPORT_FLOWS.OPTIONS.UNSUPPORTED_WRITE_MODE
    - EXPORT_FLOWS.PATH.INVALID_OUTPUT_ROOT
    - EXPORT_FLOWS.LAYOUT.EXPORT_DIR_EXISTS_ABORT
    """
    issues: List[Issue] = []

    if not isinstance(flows, FlowDataset):
        # Se aborta porque export_flows solo opera sobre FlowDataset ya construido.
        emit_and_maybe_raise(
            issues,
            EXPORT_FLOWS_ISSUES,
            "EXPORT_FLOWS.CORE.INVALID_FLOWDATASET",
            strict=False,
            exception_map=EXCEPTION_MAP_EXPORT,
            default_exception=ExportError,
            received_type=type(flows).__name__,
        )

    if not hasattr(flows, "flows") or not isinstance(flows.flows, pd.DataFrame):
        # Se aborta porque el contrato de export necesita la tabla canónica `flows`.
        emit_and_maybe_raise(
            issues,
            EXPORT_FLOWS_ISSUES,
            "EXPORT_FLOWS.CORE.INVALID_DATA_SURFACE",
            strict=False,
            exception_map=EXCEPTION_MAP_EXPORT,
            default_exception=ExportError,
            data_type=type(getattr(flows, "flows", None)).__name__,
            reason="flows_not_dataframe",
        )

    options_raw = options or ExportFlowsOptions()
    format_eff = options_raw.format or "flowmap_blue"
    mode_eff = options_raw.mode or "error_if_exists"
    extra_flow_fields = None if options_raw.extra_flow_fields is None else [str(name) for name in options_raw.extra_flow_fields]

    if format_eff != "flowmap_blue":
        # Se aborta porque v1.1 solo soporta el layout flowmap_blue.
        emit_and_maybe_raise(
            issues,
            EXPORT_FLOWS_ISSUES,
            "EXPORT_FLOWS.OPTIONS.UNSUPPORTED_FORMAT",
            strict=False,
            exception_map=EXCEPTION_MAP_EXPORT,
            default_exception=ExportError,
            format=format_eff,
        )

    if mode_eff not in {"error_if_exists", "overwrite"}:
        # Se aborta porque la política de escritura debe ser una de las dos variantes cerradas.
        emit_and_maybe_raise(
            issues,
            EXPORT_FLOWS_ISSUES,
            "EXPORT_FLOWS.OPTIONS.UNSUPPORTED_WRITE_MODE",
            strict=False,
            exception_map=EXCEPTION_MAP_EXPORT,
            default_exception=ExportError,
            mode=mode_eff,
        )

    try:
        output_root_path = Path(output_root)
    except Exception:
        # Se aborta porque no hay forma segura de resolver el directorio raíz del export.
        emit_and_maybe_raise(
            issues,
            EXPORT_FLOWS_ISSUES,
            "EXPORT_FLOWS.PATH.INVALID_OUTPUT_ROOT",
            strict=False,
            exception_map=EXCEPTION_MAP_EXPORT,
            default_exception=ExportError,
            output_root=output_root,
            resolved_path=None,
        )

    if output_root_path.exists() and not output_root_path.is_dir():
        # Se aborta porque output_root no puede apuntar a un archivo regular.
        emit_and_maybe_raise(
            issues,
            EXPORT_FLOWS_ISSUES,
            "EXPORT_FLOWS.PATH.INVALID_OUTPUT_ROOT",
            strict=False,
            exception_map=EXCEPTION_MAP_EXPORT,
            default_exception=ExportError,
            output_root=str(output_root),
            resolved_path=str(output_root_path),
        )

    folder_name_input = options_raw.folder_name
    folder_name_invalid = False
    folder_name_generated = False

    if folder_name_input is None:
        folder_name_effective = _generate_folder_name(flows)
        folder_name_generated = True
    else:
        folder_name_effective = _sanitize_folder_name(folder_name_input)
        if folder_name_effective != folder_name_input or folder_name_effective == "":
            folder_name_invalid = True
        if folder_name_effective == "":
            folder_name_effective = _generate_folder_name(flows)
            folder_name_generated = True

    export_dir = output_root_path / folder_name_effective
    if export_dir.exists() and mode_eff == "error_if_exists":
        # Se aborta porque el contrato no permite colisión silenciosa del directorio final.
        emit_and_maybe_raise(
            issues,
            EXPORT_FLOWS_ISSUES,
            "EXPORT_FLOWS.LAYOUT.EXPORT_DIR_EXISTS_ABORT",
            strict=False,
            exception_map=EXCEPTION_MAP_EXPORT,
            default_exception=ExportError,
            output_root=str(output_root_path),
            export_dir=str(export_dir),
            format=format_eff,
            mode=mode_eff,
            folder_name_effective=folder_name_effective,
        )

    options_eff = ExportFlowsOptions(
        format=format_eff,
        mode=mode_eff,
        folder_name=folder_name_effective,
        extra_flow_fields=extra_flow_fields,
    )

    parameters = {
        "output_root": str(output_root_path),
        "export_dir": str(export_dir),
        "format": options_eff.format,
        "mode": options_eff.mode,
        "folder_name": options_eff.folder_name,
        "extra_flow_fields": list(options_eff.extra_flow_fields) if options_eff.extra_flow_fields is not None else None,
        "_folder_name_input": folder_name_input,
        "_folder_name_input_invalid": folder_name_invalid,
        "_folder_name_generated": folder_name_generated,
        "_export_dir_exists_overwrite": bool(export_dir.exists() and mode_eff == "overwrite"),
    }
    return options_eff, str(export_dir), parameters


def _preflight_export_flows(
    flows: FlowDataset,
    options: ExportFlowsOptions,
) -> tuple[list[Issue], dict[str, Any]]:
    """
    Ejecuta el preflight mínimo del FlowDataset antes de materializar el export.

    Emite
    -----
    - EXPORT_FLOWS.DATA.REQUIRED_FIELDS_MISSING
    - EXPORT_FLOWS.DATA.NULL_ORIGIN_DESTINATION
    - EXPORT_FLOWS.DATA.INVALID_FLOW_VALUE
    - EXPORT_FLOWS.DATA.EMPTY_FLOWS
    - EXPORT_FLOWS.EXTRA.INVALID_FIELDS
    - EXPORT_FLOWS.EXTRA.RESERVED_FIELDS
    - EXPORT_FLOWS.EXTRA.NON_SERIALIZABLE_FIELDS
    """
    issues: List[Issue] = []
    df = flows.flows
    required_fields = ["origin_h3_index", "destination_h3_index", "flow_value"]
    missing_fields = [field for field in required_fields if field not in df.columns]
    if missing_fields:
        # Se aborta porque el layout externo no puede construirse sin el núcleo interno mínimo.
        emit_and_maybe_raise(
            issues,
            EXPORT_FLOWS_ISSUES,
            "EXPORT_FLOWS.DATA.REQUIRED_FIELDS_MISSING",
            strict=False,
            exception_map=EXCEPTION_MAP_EXPORT,
            default_exception=ExportError,
            format=options.format,
            missing_fields=missing_fields,
            n_rows=int(len(df)),
        )

    if len(df) == 0:
        # Se deja warning porque el export vacío sigue siendo un artefacto consistente y reproducible.
        emit_issue(
            issues,
            EXPORT_FLOWS_ISSUES,
            "EXPORT_FLOWS.DATA.EMPTY_FLOWS",
            format=options.format,
            n_rows=0,
        )

    origin_null_mask = df["origin_h3_index"].isna()
    dest_null_mask = df["destination_h3_index"].isna()
    null_od_mask = origin_null_mask | dest_null_mask
    if bool(null_od_mask.any()):
        # Se aborta porque origin/dest externos no pueden construirse con H3 OD nulos.
        emit_and_maybe_raise(
            issues,
            EXPORT_FLOWS_ISSUES,
            "EXPORT_FLOWS.DATA.NULL_ORIGIN_DESTINATION",
            strict=False,
            exception_map=EXCEPTION_MAP_EXPORT,
            default_exception=ExportError,
            format=options.format,
            n_rows=int(len(df)),
            n_violations=int(null_od_mask.sum()),
            row_indices_sample=_sample_indices_from_mask(null_od_mask),
            field="origin_h3_index|destination_h3_index",
        )

    flow_value_numeric = pd.to_numeric(df["flow_value"], errors="coerce")
    invalid_flow_value_mask = flow_value_numeric.isna()
    if bool(invalid_flow_value_mask.any()):
        # Se aborta porque count debe salir desde flow_value y, por tanto, debe ser numérico y no nulo.
        emit_and_maybe_raise(
            issues,
            EXPORT_FLOWS_ISSUES,
            "EXPORT_FLOWS.DATA.INVALID_FLOW_VALUE",
            strict=False,
            exception_map=EXCEPTION_MAP_EXPORT,
            default_exception=ExportError,
            format=options.format,
            field="flow_value",
            n_rows=int(len(df)),
            n_violations=int(invalid_flow_value_mask.sum()),
            row_indices_sample=_sample_indices_from_mask(invalid_flow_value_mask),
            values_sample=_sample_series_values(df["flow_value"], invalid_flow_value_mask),
        )

    extra_flow_fields = list(options.extra_flow_fields) if options.extra_flow_fields is not None else None
    if extra_flow_fields:
        invalid_fields = [name for name in extra_flow_fields if name not in df.columns]
        if invalid_fields:
            # Se aborta porque el usuario pidió preservar columnas que no existen en el FlowDataset.
            emit_and_maybe_raise(
                issues,
                EXPORT_FLOWS_ISSUES,
                "EXPORT_FLOWS.EXTRA.INVALID_FIELDS",
                strict=False,
                exception_map=EXCEPTION_MAP_EXPORT,
                default_exception=ExportError,
                format=options.format,
                invalid_fields=invalid_fields,
            )

        reserved_fields = [name for name in extra_flow_fields if name in _RESERVED_EXTRA_FLOW_FIELDS]
        if reserved_fields:
            # Se aborta porque origin/dest/count pertenecen al layout externo fijo y no a extras del usuario.
            emit_and_maybe_raise(
                issues,
                EXPORT_FLOWS_ISSUES,
                "EXPORT_FLOWS.EXTRA.RESERVED_FIELDS",
                strict=False,
                exception_map=EXCEPTION_MAP_EXPORT,
                default_exception=ExportError,
                format=options.format,
                reserved_fields=reserved_fields,
            )

        non_serializable_fields = [name for name in extra_flow_fields if not _series_is_csv_serializable(df[name])]
        if non_serializable_fields:
            # Se aborta porque las extras deben poder escribirse a CSV sin estructuras anidadas ambiguas.
            emit_and_maybe_raise(
                issues,
                EXPORT_FLOWS_ISSUES,
                "EXPORT_FLOWS.EXTRA.NON_SERIALIZABLE_FIELDS",
                strict=False,
                exception_map=EXCEPTION_MAP_EXPORT,
                default_exception=ExportError,
                format=options.format,
                invalid_fields=non_serializable_fields,
            )

    return issues, {
        "count_source": "flow_value",
        "extra_flow_fields": extra_flow_fields,
    }


def _build_flowmap_tables(
    flows_df: pd.DataFrame,
    extra_flow_fields: list[str] | None,
    count_source: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Transforma el esquema interno de flows al layout externo flowmap.

    Emite
    -----
    - EXPORT_FLOWS.H3.CENTROID_CONVERSION_FAILED
    """
    issues: List[Issue] = []

    flows_out_df = pd.DataFrame(
        {
            "origin": flows_df["origin_h3_index"].astype(str),
            "dest": flows_df["destination_h3_index"].astype(str),
            "count": pd.to_numeric(flows_df[count_source], errors="coerce"),
        }
    )
    if extra_flow_fields:
        for field_name in extra_flow_fields:
            flows_out_df[field_name] = flows_df[field_name].copy()

    unique_h3 = pd.Index(
        pd.concat(
            [
                flows_df["origin_h3_index"].dropna().astype(str),
                flows_df["destination_h3_index"].dropna().astype(str),
            ],
            ignore_index=True,
        ).unique()
    )

    records: List[Dict[str, Any]] = []
    invalid_h3_values: List[str] = []
    for h3_index in unique_h3.tolist():
        try:
            lat, lon = h3.cell_to_latlng(h3_index)
        except Exception as exc:
            invalid_h3_values.append(str(h3_index))
            last_exception = exc
            continue
        records.append({"id": h3_index, "name": h3_index, "lat": lat, "lon": lon})

    if invalid_h3_values:
        # Se aborta porque sin centroides H3 no se puede construir locations.csv de forma consistente.
        emit_and_maybe_raise(
            issues,
            EXPORT_FLOWS_ISSUES,
            "EXPORT_FLOWS.H3.CENTROID_CONVERSION_FAILED",
            strict=False,
            exception_map=EXCEPTION_MAP_EXPORT,
            default_exception=ExportError,
            n_violations=int(len(invalid_h3_values)),
            values_sample=invalid_h3_values[:10],
            exception_type=type(last_exception).__name__,
        )

    locations_df = pd.DataFrame(records, columns=["id", "name", "lat", "lon"])
    return flows_out_df, locations_df


def _materialize_flowmap_export(
    flows: FlowDataset,
    export_dir: str,
    flows_out_df: pd.DataFrame,
    locations_df: pd.DataFrame,
    parameters: dict[str, Any],
    count_source: str,
) -> tuple[FlowExportResult, OperationReport, dict[str, Any]]:
    """
    Materializa el directorio exportado, el sidecar y el resultado del export.

    Emite
    -----
    - EXPORT_FLOWS.IO.CREATE_DIR_FAILED
    - EXPORT_FLOWS.SERIALIZATION.METADATA_NOT_SERIALIZABLE
    - EXPORT_FLOWS.IO.WRITE_FLOWS_FAILED
    - EXPORT_FLOWS.IO.WRITE_LOCATIONS_FAILED
    - EXPORT_FLOWS.IO.WRITE_METADATA_FAILED
    - EXPORT_FLOWS.IO.PERMISSION_DENIED
    - EXPORT_FLOWS.IO.PARTIAL_WRITE_CLEANUP_FAILED
    """
    issues: List[Issue] = []
    export_path = Path(export_dir)
    output_root_path = export_path.parent

    try:
        output_root_path.mkdir(parents=True, exist_ok=True)
    except PermissionError as exc:
        # Se aborta porque ni siquiera fue posible preparar el directorio raíz del export.
        emit_and_maybe_raise(
            issues,
            EXPORT_FLOWS_ISSUES,
            "EXPORT_FLOWS.IO.PERMISSION_DENIED",
            strict=False,
            exception_map=EXCEPTION_MAP_EXPORT,
            default_exception=ExportError,
            output_root=str(output_root_path),
            export_dir=str(export_path),
            artifact="export_dir",
            exception_type=type(exc).__name__,
        )
    except Exception:
        # Se aborta porque el directorio raíz del export no pudo prepararse de forma operable.
        emit_and_maybe_raise(
            issues,
            EXPORT_FLOWS_ISSUES,
            "EXPORT_FLOWS.IO.CREATE_DIR_FAILED",
            strict=False,
            exception_map=EXCEPTION_MAP_EXPORT,
            default_exception=ExportError,
            output_root=str(output_root_path),
            export_dir=str(export_path),
            format=parameters["format"],
            mode=parameters["mode"],
            folder_name_effective=parameters["folder_name"],
        )

    staging_dir = Path(tempfile.mkdtemp(prefix=".tmp_flow_export_", dir=str(output_root_path)))
    files_written: List[str] = []
    artifact_id = f"flow_export_{uuid.uuid4().hex[:12]}"

    try:
        flows_path = staging_dir / "flows.csv"
        locations_path = staging_dir / "locations.csv"
        metadata_path = staging_dir / "metadata.json"

        _write_csv(flows_out_df, flows_path, issues, code="EXPORT_FLOWS.IO.WRITE_FLOWS_FAILED", destination_path=export_path)
        files_written.append("flows.csv")

        _write_csv(locations_df, locations_path, issues, code="EXPORT_FLOWS.IO.WRITE_LOCATIONS_FAILED", destination_path=export_path)
        files_written.append("locations.csv")

        sidecar_payload = _build_export_metadata_json(
            flows,
            parameters=parameters,
            summary={
                "n_flows": int(len(flows_out_df)),
                "n_locations": int(len(locations_df)),
                "files_written": ["flows.csv", "locations.csv", "metadata.json"],
            },
            count_source=count_source,
            artifact_id=artifact_id,
        )

        try:
            metadata_json_text = json.dumps(sidecar_payload, ensure_ascii=False, indent=2)
        except Exception as exc:
            non_serializable_key = _find_non_serializable_top_level_key(sidecar_payload)
            # Se aborta porque el sidecar debe quedar completamente serializable y reproducible.
            emit_and_maybe_raise(
                issues,
                EXPORT_FLOWS_ISSUES,
                "EXPORT_FLOWS.SERIALIZATION.METADATA_NOT_SERIALIZABLE",
                strict=False,
                exception_map=EXCEPTION_MAP_EXPORT,
                default_exception=ExportError,
                non_serializable_key=non_serializable_key,
                exception_type=type(exc).__name__,
            )
        _write_text(metadata_json_text, metadata_path, issues, code="EXPORT_FLOWS.IO.WRITE_METADATA_FAILED", destination_path=export_path)
        files_written.append("metadata.json")

        if export_path.exists() and parameters["mode"] == "overwrite":
            if export_path.is_dir():
                shutil.rmtree(export_path)
            else:
                export_path.unlink()
        shutil.move(str(staging_dir), str(export_path))
        staging_dir = None
    except PermissionError as exc:
        # Se aborta porque el sistema negó permiso al momento de materializar artefactos del export.
        emit_and_maybe_raise(
            issues,
            EXPORT_FLOWS_ISSUES,
            "EXPORT_FLOWS.IO.PERMISSION_DENIED",
            strict=False,
            exception_map=EXCEPTION_MAP_EXPORT,
            default_exception=ExportError,
            output_root=str(output_root_path),
            export_dir=str(export_path),
            artifact=files_written[-1] if files_written else "export_dir",
            exception_type=type(exc).__name__,
        )
    finally:
        if staging_dir is not None:
            try:
                shutil.rmtree(staging_dir)
            except Exception as exc:
                # Se deja warning porque puede quedar basura temporal si la limpieza del staging falla.
                emit_issue(
                    issues,
                    EXPORT_FLOWS_ISSUES,
                    "EXPORT_FLOWS.IO.PARTIAL_WRITE_CLEANUP_FAILED",
                    export_dir=str(export_path),
                    exception_type=type(exc).__name__,
                )

    artifacts = {
        "flows": str(export_path / "flows.csv"),
        "locations": str(export_path / "locations.csv"),
        "metadata": str(export_path / "metadata.json"),
    }
    summary = {
        "n_flows": int(len(flows_out_df)),
        "n_locations": int(len(locations_df)),
        "files_written": ["flows.csv", "locations.csv", "metadata.json"],
    }
    report = OperationReport(
        ok=not any(issue.level == "error" for issue in issues),
        issues=issues,
        summary=summary,
        parameters={
            "output_root": parameters["output_root"],
            "export_dir": parameters["export_dir"],
            "format": parameters["format"],
            "mode": parameters["mode"],
            "folder_name": parameters["folder_name"],
            "extra_flow_fields": parameters["extra_flow_fields"],
        },
    )
    event_dict = {
        "op": "export_flows",
        "ts_utc": _utc_now_iso(),
        "parameters": report.parameters,
        "summary": summary,
        "issues_summary": _build_issues_summary(issues),
    }
    return FlowExportResult(export_dir=str(export_path), artifacts=artifacts), report, event_dict


def _append_export_event_or_warning(
    flows: FlowDataset,
    event_dict: dict[str, Any],
    report: OperationReport,
) -> OperationReport:
    """
    Intenta appendear el evento `export_flows` sin revertir artefactos ya escritos.

    Emite
    -----
    - EXPORT_FLOWS.EVENT.APPEND_FAILED
    """
    try:
        if not isinstance(flows.metadata, dict):
            flows.metadata = {}
        _ensure_events_list(flows.metadata).append(event_dict)
    except Exception as exc:
        # Se deja warning porque el export ya quedó materializado y no corresponde revertirlo por bookkeeping.
        emit_issue(
            report.issues,
            EXPORT_FLOWS_ISSUES,
            "EXPORT_FLOWS.EVENT.APPEND_FAILED",
            exception_type=type(exc).__name__,
        )
        report.ok = not any(issue.level == "error" for issue in report.issues)
    return report


# -----------------------------------------------------------------------------
# Helpers internos de uso general
# -----------------------------------------------------------------------------

def _generate_folder_name(flows: FlowDataset) -> str:
    """Genera un nombre estable-en-la-práctica para la carpeta de exportación."""
    source_name = None
    provenance = getattr(flows, "provenance", None)
    if isinstance(provenance, dict):
        source_name = provenance.get("source_name")
        if source_name is None:
            derived = provenance.get("derived_from")
            if isinstance(derived, list) and derived:
                source_name = derived[0].get("source_type")
    if source_name is None:
        source_name = "flows"
    base = _sanitize_folder_name(str(source_name)) or "flows"
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return f"{base}_flows_flowmap_{ts}_{uuid.uuid4().hex[:6]}"


def _sanitize_folder_name(value: str) -> str:
    """Convierte un folder_name a una forma operable y simple."""
    if value is None:
        return ""
    cleaned = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in str(value).strip())
    while "__" in cleaned:
        cleaned = cleaned.replace("__", "_")
    return cleaned.strip("._-")


def _build_export_metadata_json(
    flows: FlowDataset,
    *,
    parameters: dict[str, Any],
    summary: dict[str, Any],
    count_source: str,
    artifact_id: str,
) -> dict[str, Any]:
    """Construye el sidecar pequeño y orientado al artefacto exportado."""
    flow_metadata = copy.deepcopy(flows.metadata) if isinstance(flows.metadata, dict) else {}
    flow_provenance = copy.deepcopy(dict(flows.provenance)) if isinstance(flows.provenance, dict) else copy.deepcopy(flows.provenance)
    sidecar = {
        "artifact_type": "flow_export",
        "format": parameters["format"],
        "layout_version": "1.1",
        "artifact_id": artifact_id,
        "created_at_utc": _utc_now_iso(),
        "files": {
            "flows": "flows.csv",
            "locations": "locations.csv",
            "metadata": "metadata.json",
        },
        "flow_dataset_ref": {
            "dataset_id": flow_metadata.get("dataset_id"),
            "aggregation_spec": _to_json_serializable_or_none(getattr(flows, "aggregation_spec", None)),
            "provenance": _to_json_serializable_or_none(flow_provenance),
            "metadata": _to_json_serializable_or_none(flow_metadata),
        },
        "export": {
            "parameters": _to_json_serializable_or_none(
                {
                    "output_root": parameters["output_root"],
                    "export_dir": parameters["export_dir"],
                    "format": parameters["format"],
                    "mode": parameters["mode"],
                    "folder_name": parameters["folder_name"],
                    "extra_flow_fields": parameters["extra_flow_fields"],
                }
            ),
            "summary": _to_json_serializable_or_none(summary),
            "count_source": count_source,
        },
    }
    return sidecar


def _write_csv(
    df: pd.DataFrame,
    path: Path,
    issues: List[Issue],
    *,
    code: str,
    destination_path: Path,
) -> None:
    """Escribe un CSV UTF-8 y eleva ExportError si falla."""
    try:
        df.to_csv(path, index=False)
    except PermissionError as exc:
        # Se aborta porque el sistema negó permiso al escribir el artefacto CSV pedido.
        emit_and_maybe_raise(
            issues,
            EXPORT_FLOWS_ISSUES,
            "EXPORT_FLOWS.IO.PERMISSION_DENIED",
            strict=False,
            exception_map=EXCEPTION_MAP_EXPORT,
            default_exception=ExportError,
            output_root=str(destination_path.parent),
            export_dir=str(destination_path),
            artifact=path.name,
            exception_type=type(exc).__name__,
        )
    except Exception:
        # Se aborta porque el artefacto CSV no pudo escribirse de forma confiable.
        emit_and_maybe_raise(
            issues,
            EXPORT_FLOWS_ISSUES,
            code,
            strict=False,
            exception_map=EXCEPTION_MAP_EXPORT,
            default_exception=ExportError,
            output_root=str(destination_path.parent),
            export_dir=str(destination_path),
            format="flowmap_blue",
            mode="materialize",
            artifact=path.name,
        )


def _write_text(
    text: str,
    path: Path,
    issues: List[Issue],
    *,
    code: str,
    destination_path: Path,
) -> None:
    """Escribe texto UTF-8 y eleva ExportError si falla."""
    try:
        path.write_text(text, encoding="utf-8")
    except PermissionError as exc:
        # Se aborta porque el sistema negó permiso al escribir el artefacto solicitado.
        emit_and_maybe_raise(
            issues,
            EXPORT_FLOWS_ISSUES,
            "EXPORT_FLOWS.IO.PERMISSION_DENIED",
            strict=False,
            exception_map=EXCEPTION_MAP_EXPORT,
            default_exception=ExportError,
            output_root=str(destination_path.parent),
            export_dir=str(destination_path),
            artifact=path.name,
            exception_type=type(exc).__name__,
        )
    except Exception:
        # Se aborta porque el artefacto textual no pudo escribirse de forma confiable.
        emit_and_maybe_raise(
            issues,
            EXPORT_FLOWS_ISSUES,
            code,
            strict=False,
            exception_map=EXCEPTION_MAP_EXPORT,
            default_exception=ExportError,
            output_root=str(destination_path.parent),
            export_dir=str(destination_path),
            format="flowmap_blue",
            mode="materialize",
            artifact=path.name,
        )


def _series_is_csv_serializable(series: pd.Series) -> bool:
    """Verifica de forma pragmática que una columna pueda serializarse a CSV sin estructuras anidadas."""
    sample = series.dropna().head(50).tolist()
    for value in sample:
        if isinstance(value, (dict, list, tuple, set)):
            return False
    return True


def _sample_indices_from_mask(mask: pd.Series, limit: int = 10) -> list[int]:
    """Devuelve una muestra acotada de índices donde la máscara es verdadera."""
    out: list[int] = []
    for idx in mask[mask].index[:limit].tolist():
        if isinstance(idx, (int,)):
            out.append(idx)
        else:
            try:
                out.append(int(idx))
            except Exception:
                pass
    return out


def _sample_series_values(series: pd.Series, mask: pd.Series, limit: int = 10) -> list[Any]:
    """Devuelve una muestra JSON-friendly de valores de una serie bajo una máscara."""
    values = series.loc[mask].head(limit).tolist()
    return [_json_safe_scalar(value) for value in values]


def _find_non_serializable_top_level_key(payload: dict[str, Any]) -> str | None:
    """Busca una key top-level que no logre serializarse limpiamente a JSON."""
    for key, value in payload.items():
        try:
            json.dumps(value)
        except Exception:
            return str(key)
    return None


def _ensure_events_list(metadata: dict[str, Any]) -> list[dict[str, Any]]:
    """Asegura que metadata['events'] exista como lista append-only."""
    if not isinstance(metadata.get("events"), list):
        metadata["events"] = []
    return metadata["events"]


def _build_issues_summary(issues: Sequence[Issue]) -> dict[str, Any]:
    """Resume issues por severidad y por code para el evento de export."""
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


def _to_json_serializable_or_none(obj: Any) -> Any:
    """Convierte dict/list anidados a una forma JSON-safe sin fallback silencioso complejo."""
    if obj is None:
        return None
    if isinstance(obj, dict):
        return {str(key): _to_json_serializable_or_none(value) for key, value in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_to_json_serializable_or_none(value) for value in obj]
    if isinstance(obj, (pd.Timestamp, datetime)):
        return obj.isoformat()
    if pd.isna(obj):
        return None
    if _json_is_serializable(obj):
        return obj
    return _json_safe_scalar(obj)


def _json_is_serializable(obj: Any) -> bool:
    """Chequea si un objeto puede serializarse directamente a JSON."""
    try:
        json.dumps(obj)
        return True
    except Exception:
        return False


def _json_safe_scalar(value: Any) -> Any:
    """Normaliza un escalar a una forma JSON-friendly y estable."""
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    return str(value)


def _utc_now_iso() -> str:
    """Retorna timestamp UTC ISO-8601 para eventos del módulo."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
