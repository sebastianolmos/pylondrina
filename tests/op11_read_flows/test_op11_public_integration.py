from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from pylondrina.errors import ExportError
from pylondrina.io.flows import (
    ReadFlowsOptions,
    WriteFlowsOptions,
    read_flows,
    write_flows,
)


# -----------------------------------------------------------------------------
# Bloque 1 - Read feliz desde bundle formal Parquet rico
# -----------------------------------------------------------------------------


def test_read_flows_happy_path_from_rich_parquet_bundle(
    tmp_path,
    rich_flowdataset_small,
    assert_df_equal_untyped,
):
    """Verifica lectura feliz Parquet con fallback `.golondrina` y reconstrucción completa."""
    case_dir = tmp_path / "case_01_read_happy_parquet"
    artifact_path = case_dir / "flows_read_happy"

    flows = copy.deepcopy(rich_flowdataset_small)

    write_report = write_flows(
        flows,
        artifact_path,
        options=WriteFlowsOptions(
            mode="error_if_exists",
            storage_format="parquet",
            normalize_artifact_dir=True,
            write_flow_to_trips=False,
        ),
    )

    assert write_report.ok is True

    loaded, read_report = read_flows(
        artifact_path,
        options=ReadFlowsOptions(
            strict=False,
            keep_metadata=True,
            read_flow_to_trips=False,
        ),
    )

    effective_root = Path(str(artifact_path) + ".golondrina")

    assert read_report.ok is True

    # Parameters efectivos
    assert read_report.parameters["path"] == str(effective_root)
    assert read_report.parameters["strict"] is False
    assert read_report.parameters["keep_metadata"] is True
    assert read_report.parameters["read_flow_to_trips"] is False

    # Summary
    assert read_report.summary["n_flows"] == len(flows.flows)
    assert read_report.summary["n_columns"] == len(flows.flows.columns)
    assert read_report.summary["flow_to_trips_loaded"] is False
    assert read_report.summary["n_flow_to_trips"] is None
    assert set(read_report.summary["files_read"]) == {
        "flows.parquet",
        "flows.metadata.json",
    }

    # Dataset reconstruido
    assert_df_equal_untyped(
        loaded.flows,
        flows.flows,
        by=["flow_id"],
    )

    assert loaded.aggregation_spec == flows.aggregation_spec
    assert loaded.provenance == flows.provenance
    assert loaded.metadata["dataset_id"] == flows.metadata["dataset_id"]
    assert loaded.metadata["artifact_id"] == flows.metadata["artifact_id"]
    assert loaded.metadata["is_validated"] is False
    assert loaded.source_trips is None

    # Evento read
    event = loaded.metadata["events"][-1]
    assert event["op"] == "read_flows"
    assert event["parameters"] == read_report.parameters
    assert event["summary"] == read_report.summary
    assert "issues_summary" in event


# -----------------------------------------------------------------------------
# Bloque 2 - Read con auxiliar Parquet solicitado y existente
# -----------------------------------------------------------------------------


def test_read_flows_loads_existing_parquet_flow_to_trips_auxiliary(
    tmp_path,
    rich_flowdataset_with_trip_links,
    assert_df_equal_untyped,
):
    """Verifica lectura Parquet con auxiliar `flow_to_trips` solicitado y existente."""
    case_dir = tmp_path / "case_02_read_with_aux_parquet"
    artifact_path = case_dir / "flows_with_aux"

    flows = copy.deepcopy(rich_flowdataset_with_trip_links)

    write_report = write_flows(
        flows,
        artifact_path,
        options=WriteFlowsOptions(
            mode="error_if_exists",
            storage_format="parquet",
            normalize_artifact_dir=False,
            write_flow_to_trips=True,
        ),
    )

    assert write_report.ok is True

    loaded, read_report = read_flows(
        artifact_path,
        options=ReadFlowsOptions(
            strict=False,
            keep_metadata=True,
            read_flow_to_trips=True,
        ),
    )

    assert read_report.ok is True
    assert read_report.summary["flow_to_trips_loaded"] is True
    assert read_report.summary["n_flow_to_trips"] == len(flows.flow_to_trips)
    assert loaded.flow_to_trips is not None

    assert_df_equal_untyped(
        loaded.flows,
        flows.flows,
        by=["flow_id"],
    )

    assert_df_equal_untyped(
        loaded.flow_to_trips,
        flows.flow_to_trips,
        by=["flow_id", "movement_id"],
    )


# -----------------------------------------------------------------------------
# Bloque 3 - Read degradado con auxiliar Parquet faltante
# -----------------------------------------------------------------------------


def test_read_flows_degrades_when_parquet_flow_to_trips_auxiliary_is_missing(
    tmp_path,
    rich_flowdataset_with_trip_links,
    artifact_aux_filename,
    issue_codes,
):
    """Verifica degradación recuperable si falta el auxiliar Parquet solicitado."""
    case_dir = tmp_path / "case_03_read_missing_aux_parquet"
    artifact_path = case_dir / "flows_missing_aux"

    flows = copy.deepcopy(rich_flowdataset_with_trip_links)

    write_report = write_flows(
        flows,
        artifact_path,
        options=WriteFlowsOptions(
            mode="error_if_exists",
            storage_format="parquet",
            normalize_artifact_dir=False,
            write_flow_to_trips=True,
        ),
    )

    assert write_report.ok is True

    aux_path = artifact_path / artifact_aux_filename("parquet")
    assert aux_path.exists()

    # Simula pérdida del auxiliar Parquet.
    aux_path.unlink()

    loaded, read_report = read_flows(
        artifact_path,
        options=ReadFlowsOptions(
            strict=False,
            keep_metadata=True,
            read_flow_to_trips=True,
        ),
    )

    codes = issue_codes(read_report)

    assert read_report.ok is True
    assert "READ_FLOWS.FLOW_TO_TRIPS.REQUESTED_BUT_MISSING" in codes
    assert loaded.flow_to_trips is None
    assert read_report.summary["flow_to_trips_loaded"] is False
    assert read_report.summary["n_flow_to_trips"] is None


# -----------------------------------------------------------------------------
# Bloque 4 - Sidecar degradado bajo strict=False
# -----------------------------------------------------------------------------


def test_read_flows_recovers_from_partially_degraded_sidecar_when_strict_false(
    tmp_path,
    rich_flowdataset_small,
    read_json,
    issue_codes,
):
    """Verifica recuperación controlada de `dataset_id`, `artifact_id` y `aggregation_spec`."""
    case_dir = tmp_path / "case_04_read_incomplete_sidecar"
    artifact_path = case_dir / "flows_incomplete_sidecar"

    flows = copy.deepcopy(rich_flowdataset_small)

    write_report = write_flows(
        flows,
        artifact_path,
        options=WriteFlowsOptions(
            mode="error_if_exists",
            storage_format="parquet",
            normalize_artifact_dir=False,
            write_flow_to_trips=False,
        ),
    )

    assert write_report.ok is True

    sidecar_path = artifact_path / "flows.metadata.json"
    sidecar = read_json(sidecar_path)

    original_dataset_id = sidecar["dataset_id"]

    # Corrompe solo partes recuperables bajo strict=False.
    sidecar["dataset_id"] = ""
    sidecar["artifact_id"] = None
    sidecar["aggregation_spec"] = None

    sidecar_path.write_text(
        json.dumps(sidecar, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    loaded, read_report = read_flows(
        artifact_path,
        options=ReadFlowsOptions(
            strict=False,
            keep_metadata=True,
            read_flow_to_trips=False,
        ),
    )

    codes = issue_codes(read_report)

    assert read_report.ok is True
    assert "READ_FLOWS.METADATA.DATASET_ID_REGENERATED" in codes
    assert "READ_FLOWS.METADATA.ARTIFACT_ID_SET_NONE" in codes
    assert "READ_FLOWS.SIDECAR.AGGREGATION_SPEC_DEFAULTED" in codes

    assert loaded.metadata["dataset_id"] != original_dataset_id
    assert loaded.metadata["dataset_id"] is not None
    assert loaded.metadata["artifact_id"] is None
    assert loaded.aggregation_spec == {}


# -----------------------------------------------------------------------------
# Bloque 5 - Layout fatal sin sidecar
# -----------------------------------------------------------------------------


def test_read_flows_raises_when_formal_sidecar_is_missing(
    tmp_path,
    rich_flowdataset_small,
):
    """Verifica fatalidad pública si el bundle carece de `flows.metadata.json`."""
    case_dir = tmp_path / "case_05_layout_fatal_missing_sidecar"
    artifact_path = case_dir / "flows_without_sidecar"
    artifact_path.mkdir(parents=True, exist_ok=True)

    rich_flowdataset_small.flows.to_parquet(
        artifact_path / "flows.parquet",
        index=False,
        compression="snappy",
        engine="pyarrow",
    )

    with pytest.raises(ExportError) as excinfo:
        read_flows(
            artifact_path,
            options=ReadFlowsOptions(
                strict=False,
                keep_metadata=True,
                read_flow_to_trips=False,
            ),
        )

    assert excinfo.value.code == "READ_FLOWS.LAYOUT.MISSING_SIDECAR"


# -----------------------------------------------------------------------------
# Bloque 6 - Round-trip Parquet rico + VALIDATED_FORCED_FALSE
# -----------------------------------------------------------------------------


def test_read_flows_roundtrip_parquet_preserves_rich_data_and_forces_unvalidated_state(
    tmp_path,
    rich_flowdataset_factory,
    assert_df_equal_untyped,
    issue_codes,
):
    """Verifica round-trip Parquet rico y política post-read `is_validated=False`."""
    case_dir = tmp_path / "case_06_roundtrip_parquet_policy"
    artifact_path = case_dir / "flows_roundtrip"

    flows = rich_flowdataset_factory(
        repeat_blocks=2,
        with_trip_links=True,
        validated=True,
        dataset_id="flow-dset-roundtrip-parquet-001",
    )

    flows_before = flows.flows.copy(deep=True)
    flow_to_trips_before = flows.flow_to_trips.copy(deep=True)
    aggregation_before = copy.deepcopy(flows.aggregation_spec)
    provenance_before = copy.deepcopy(flows.provenance)
    dataset_id_before = flows.metadata["dataset_id"]

    write_report = write_flows(
        flows,
        artifact_path,
        options=WriteFlowsOptions(
            mode="error_if_exists",
            storage_format="parquet",
            normalize_artifact_dir=False,
            write_flow_to_trips=True,
        ),
    )

    assert write_report.ok is True

    loaded, read_report = read_flows(
        artifact_path,
        options=ReadFlowsOptions(
            strict=False,
            keep_metadata=True,
            read_flow_to_trips=True,
        ),
    )

    codes = issue_codes(read_report)

    assert read_report.ok is True
    assert "READ_FLOWS.METADATA.VALIDATED_FORCED_FALSE" in codes

    assert_df_equal_untyped(
        loaded.flows,
        flows_before,
        by=["flow_id"],
    )

    assert_df_equal_untyped(
        loaded.flow_to_trips,
        flow_to_trips_before,
        by=["flow_id", "movement_id"],
    )

    assert loaded.aggregation_spec == aggregation_before
    assert loaded.provenance == provenance_before
    assert loaded.metadata["dataset_id"] == dataset_id_before
    assert loaded.metadata["artifact_id"] == flows.metadata["artifact_id"]
    assert loaded.metadata["is_validated"] is False
    assert loaded.source_trips is None


# -----------------------------------------------------------------------------
# Bloque 7 - Read feliz Feather
# -----------------------------------------------------------------------------


def test_read_flows_happy_path_from_feather_bundle(
    tmp_path,
    rich_flowdataset_small,
    assert_df_equal_untyped,
):
    """Verifica lectura feliz Feather con fallback `.golondrina` y reconstrucción completa."""
    case_dir = tmp_path / "case_07_read_happy_feather"
    artifact_path = case_dir / "flows_read_happy_feather"

    flows = copy.deepcopy(rich_flowdataset_small)

    write_report = write_flows(
        flows,
        artifact_path,
        options=WriteFlowsOptions(
            mode="error_if_exists",
            normalize_artifact_dir=True,
            write_flow_to_trips=False,
        ),
    )

    assert write_report.ok is True

    loaded, read_report = read_flows(
        artifact_path,
        options=ReadFlowsOptions(
            strict=False,
            keep_metadata=True,
            read_flow_to_trips=False,
        ),
    )

    effective_root = Path(str(artifact_path) + ".golondrina")

    assert read_report.ok is True

    # Parameters efectivos
    assert read_report.parameters["path"] == str(effective_root)
    assert read_report.parameters["strict"] is False
    assert read_report.parameters["keep_metadata"] is True
    assert read_report.parameters["read_flow_to_trips"] is False

    # Summary
    assert read_report.summary["n_flows"] == len(flows.flows)
    assert read_report.summary["n_columns"] == len(flows.flows.columns)
    assert read_report.summary["flow_to_trips_loaded"] is False
    assert read_report.summary["n_flow_to_trips"] is None
    assert set(read_report.summary["files_read"]) == {
        "flows.feather",
        "flows.metadata.json",
    }

    # Dataset reconstruido
    assert_df_equal_untyped(
        loaded.flows,
        flows.flows,
        by=["flow_id"],
    )

    assert loaded.aggregation_spec == flows.aggregation_spec
    assert loaded.provenance == flows.provenance
    assert loaded.metadata["dataset_id"] == flows.metadata["dataset_id"]
    assert loaded.metadata["artifact_id"] == flows.metadata["artifact_id"]
    assert loaded.metadata["is_validated"] is False
    assert loaded.source_trips is None

    # Evento read
    event = loaded.metadata["events"][-1]
    assert event["op"] == "read_flows"
    assert event["parameters"] == read_report.parameters
    assert event["summary"] == read_report.summary
    assert "issues_summary" in event


# -----------------------------------------------------------------------------
# Bloque 8 - Round-trip Feather con auxiliar presente
# -----------------------------------------------------------------------------


def test_read_flows_roundtrip_feather_with_auxiliary_preserves_rich_data(
    tmp_path,
    rich_flowdataset_factory,
    assert_df_equal_untyped,
    issue_codes,
):
    """Verifica round-trip Feather con auxiliar y política post-read no validada."""
    case_dir = tmp_path / "case_08_roundtrip_feather_with_aux"
    artifact_path = case_dir / "flows_roundtrip_feather"

    flows = rich_flowdataset_factory(
        repeat_blocks=2,
        with_trip_links=True,
        validated=True,
        dataset_id="flow-dset-roundtrip-feather-001",
    )

    flows_before = flows.flows.copy(deep=True)
    flow_to_trips_before = flows.flow_to_trips.copy(deep=True)
    aggregation_before = copy.deepcopy(flows.aggregation_spec)
    provenance_before = copy.deepcopy(flows.provenance)
    dataset_id_before = flows.metadata["dataset_id"]

    write_report = write_flows(
        flows,
        artifact_path,
        options=WriteFlowsOptions(
            mode="error_if_exists",
            normalize_artifact_dir=False,
            write_flow_to_trips=True,
        ),
    )

    assert write_report.ok is True
    assert (artifact_path / "flows.feather").exists()
    assert (artifact_path / "flow_to_trips.feather").exists()
    assert set(write_report.summary["files_written"]) == {
        "flows.feather",
        "flow_to_trips.feather",
        "flows.metadata.json",
    }

    loaded, read_report = read_flows(
        artifact_path,
        options=ReadFlowsOptions(
            strict=False,
            keep_metadata=True,
            read_flow_to_trips=True,
        ),
    )

    codes = issue_codes(read_report)

    assert read_report.ok is True
    assert "READ_FLOWS.METADATA.VALIDATED_FORCED_FALSE" in codes

    assert read_report.summary["flow_to_trips_loaded"] is True
    assert read_report.summary["n_flow_to_trips"] == len(flows.flow_to_trips)
    assert set(read_report.summary["files_read"]) == {
        "flows.feather",
        "flow_to_trips.feather",
        "flows.metadata.json",
    }

    assert_df_equal_untyped(
        loaded.flows,
        flows_before,
        by=["flow_id"],
    )

    assert_df_equal_untyped(
        loaded.flow_to_trips,
        flow_to_trips_before,
        by=["flow_id", "movement_id"],
    )

    assert loaded.aggregation_spec == aggregation_before
    assert loaded.provenance == provenance_before
    assert loaded.metadata["dataset_id"] == dataset_id_before
    assert loaded.metadata["artifact_id"] == flows.metadata["artifact_id"]
    assert loaded.metadata["is_validated"] is False
    assert loaded.source_trips is None


# -----------------------------------------------------------------------------
# Bloque 9 - Auxiliar Feather faltante
# -----------------------------------------------------------------------------


def test_read_flows_degrades_when_feather_flow_to_trips_auxiliary_is_missing(
    tmp_path,
    rich_flowdataset_with_trip_links,
    issue_codes,
):
    """Verifica degradación recuperable si falta el auxiliar Feather solicitado."""
    case_dir = tmp_path / "case_09_read_missing_aux_feather"
    artifact_path = case_dir / "flows_missing_aux_feather"

    flows = copy.deepcopy(rich_flowdataset_with_trip_links)

    write_report = write_flows(
        flows,
        artifact_path,
        options=WriteFlowsOptions(
            mode="error_if_exists",
            normalize_artifact_dir=False,
            write_flow_to_trips=True,
        ),
    )

    assert write_report.ok is True

    aux_path = artifact_path / "flow_to_trips.feather"
    assert aux_path.exists()

    # Simula pérdida del auxiliar Feather.
    aux_path.unlink()

    loaded, read_report = read_flows(
        artifact_path,
        options=ReadFlowsOptions(
            strict=False,
            keep_metadata=True,
            read_flow_to_trips=True,
        ),
    )

    codes = issue_codes(read_report)

    assert read_report.ok is True
    assert "READ_FLOWS.FLOW_TO_TRIPS.REQUESTED_BUT_MISSING" in codes
    assert loaded.flow_to_trips is None
    assert read_report.summary["flow_to_trips_loaded"] is False
    assert read_report.summary["n_flow_to_trips"] is None
    assert set(read_report.summary["files_read"]) == {
        "flows.feather",
        "flows.metadata.json",
    }


# -----------------------------------------------------------------------------
# Bloque 10 - Mismatch fatal storage.format / files.data
# -----------------------------------------------------------------------------


def test_read_flows_raises_when_feather_sidecar_points_to_parquet_data_filename(
    tmp_path,
    rich_flowdataset_small,
    read_json,
):
    """Verifica fatalidad por mismatch entre `storage.format='feather'` y `files.data`."""
    case_dir = tmp_path / "case_10_sidecar_mismatch_feather"
    artifact_path = case_dir / "flows_sidecar_mismatch_feather"

    flows = copy.deepcopy(rich_flowdataset_small)

    write_report = write_flows(
        flows,
        artifact_path,
        options=WriteFlowsOptions(
            mode="error_if_exists",
            normalize_artifact_dir=False,
            write_flow_to_trips=False,
        ),
    )

    assert write_report.ok is True
    assert (artifact_path / "flows.feather").exists()

    sidecar_path = artifact_path / "flows.metadata.json"
    sidecar = read_json(sidecar_path)

    # Fuerza inconsistencia entre backend declarado y nombre físico.
    sidecar["files"]["data"] = "flows.parquet"

    sidecar_path.write_text(
        json.dumps(sidecar, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    with pytest.raises(ExportError) as excinfo:
        read_flows(
            artifact_path,
            options=ReadFlowsOptions(
                strict=True,
                keep_metadata=True,
                read_flow_to_trips=False,
            ),
        )

    assert excinfo.value.code == "READ_FLOWS.LAYOUT.MISSING_DATA_FILE"


# -----------------------------------------------------------------------------
# Bloque 11 - Parquet formal sigue funcionando
# -----------------------------------------------------------------------------


def test_read_flows_keeps_formal_parquet_compatibility_with_auxiliary(
    tmp_path,
    rich_flowdataset_with_trip_links,
    assert_df_equal_untyped,
):
    """Verifica que la lectura formal Parquet siga funcionando aunque Feather sea default."""
    case_dir = tmp_path / "case_11_read_formal_parquet_still_works"
    artifact_path = case_dir / "flows_read_formal_parquet"

    flows = copy.deepcopy(rich_flowdataset_with_trip_links)

    write_report = write_flows(
        flows,
        artifact_path,
        options=WriteFlowsOptions(
            mode="error_if_exists",
            storage_format="parquet",
            parquet_compression="snappy",
            normalize_artifact_dir=False,
            write_flow_to_trips=True,
        ),
    )

    assert write_report.ok is True
    assert (artifact_path / "flows.parquet").exists()
    assert (artifact_path / "flow_to_trips.parquet").exists()

    loaded, read_report = read_flows(
        artifact_path,
        options=ReadFlowsOptions(
            strict=False,
            keep_metadata=True,
            read_flow_to_trips=True,
        ),
    )

    assert read_report.ok is True
    assert set(read_report.summary["files_read"]) == {
        "flows.parquet",
        "flow_to_trips.parquet",
        "flows.metadata.json",
    }
    assert read_report.summary["flow_to_trips_loaded"] is True
    assert read_report.summary["n_flow_to_trips"] == len(flows.flow_to_trips)

    assert_df_equal_untyped(
        loaded.flows,
        flows.flows,
        by=["flow_id"],
    )

    assert_df_equal_untyped(
        loaded.flow_to_trips,
        flows.flow_to_trips,
        by=["flow_id", "movement_id"],
    )

    assert loaded.aggregation_spec == flows.aggregation_spec
    assert loaded.provenance == flows.provenance
    assert loaded.metadata["dataset_id"] == flows.metadata["dataset_id"]
    assert loaded.metadata["artifact_id"] == flows.metadata["artifact_id"]
    assert loaded.metadata["is_validated"] is False
    assert loaded.source_trips is None

    event = loaded.metadata["events"][-1]
    assert event["op"] == "read_flows"
    assert event["parameters"] == read_report.parameters
    assert event["summary"] == read_report.summary
    assert "issues_summary" in event