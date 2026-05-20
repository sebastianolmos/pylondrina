from __future__ import annotations

import pytest

from pylondrina.errors import ExportError
from pylondrina.io.flows import (
    _build_read_summary,
    _read_flows_table,
    _read_optional_flow_to_trips,
)


# -----------------------------------------------------------------------------
# Bloque 6. Lectura de tablas físicas
# -----------------------------------------------------------------------------


@pytest.mark.parametrize("storage_format", ["parquet", "feather"])
def test_read_flows_table_loads_main_table_for_supported_backends(
    tmp_path,
    formal_flow_artifact_factory,
    minimal_flows_df,
    storage_format,
):
    """Verifica lectura física de la tabla principal en Parquet y Feather."""
    artifact = formal_flow_artifact_factory(
        tmp_path / f"artifact_{storage_format}.golondrina",
        storage_format=storage_format,
        with_aux=True,
    )

    issues = []

    flows_df = _read_flows_table(
        artifact["data_path"],
        storage_format=storage_format,
        issues=issues,
        destination_path=artifact["paths"].root_dir,
    )

    assert len(flows_df) == len(minimal_flows_df)
    assert list(flows_df.columns) == list(minimal_flows_df.columns)
    assert issues == []


def test_read_optional_flow_to_trips_returns_empty_state_when_not_requested(
    tmp_path,
):
    """Verifica que el auxiliar no solicitado no se lea ni emita issues."""
    case_dir = tmp_path / "case_aux_not_requested"
    case_dir.mkdir(parents=True, exist_ok=True)

    aux_path = case_dir / "flow_to_trips.feather"
    issues = []

    df_aux, loaded, files_read, n_rows = _read_optional_flow_to_trips(
        aux_path,
        requested=False,
        strict=False,
        storage_format="feather",
        issues=issues,
        destination_path=case_dir,
    )

    assert df_aux is None
    assert loaded is False
    assert files_read == []
    assert n_rows is None
    assert issues == []


def test_read_optional_flow_to_trips_degrades_when_requested_file_is_missing_and_strict_false(
    tmp_path,
    assert_issue_present,
):
    """Verifica degradación recuperable si el auxiliar pedido falta con `strict=False`."""
    case_dir = tmp_path / "case_aux_missing_strict_false"
    case_dir.mkdir(parents=True, exist_ok=True)

    aux_path = case_dir / "flow_to_trips.parquet"
    issues = []

    df_aux, loaded, files_read, n_rows = _read_optional_flow_to_trips(
        aux_path,
        requested=True,
        strict=False,
        storage_format="parquet",
        issues=issues,
        destination_path=case_dir,
    )

    assert df_aux is None
    assert loaded is False
    assert files_read == []
    assert n_rows is None

    assert_issue_present(
        issues,
        "READ_FLOWS.FLOW_TO_TRIPS.REQUESTED_BUT_MISSING",
    )


def test_read_optional_flow_to_trips_raises_when_requested_file_is_missing_and_strict_true(
    tmp_path,
    assert_issue_present,
):
    """Verifica fatalidad si el auxiliar pedido falta con `strict=True`."""
    case_dir = tmp_path / "case_aux_missing_strict_true"
    case_dir.mkdir(parents=True, exist_ok=True)

    aux_path = case_dir / "flow_to_trips.feather"
    issues = []

    with pytest.raises(ExportError) as excinfo:
        _read_optional_flow_to_trips(
            aux_path,
            requested=True,
            strict=True,
            storage_format="feather",
            issues=issues,
            destination_path=case_dir,
        )

    assert excinfo.value.code == "READ_FLOWS.IO.FLOW_TO_TRIPS_READ_FAILED"
    assert_issue_present(
        issues,
        "READ_FLOWS.IO.FLOW_TO_TRIPS_READ_FAILED",
    )


@pytest.mark.parametrize("storage_format", ["parquet", "feather"])
def test_read_optional_flow_to_trips_loads_existing_auxiliary_for_supported_backends(
    tmp_path,
    formal_flow_artifact_factory,
    minimal_flow_to_trips_df,
    storage_format,
):
    """Verifica lectura física del auxiliar `flow_to_trips` en Parquet y Feather."""
    artifact = formal_flow_artifact_factory(
        tmp_path / f"artifact_aux_{storage_format}.golondrina",
        storage_format=storage_format,
        with_aux=True,
    )

    issues = []

    flow_to_trips_df, loaded, files_read, n_rows = _read_optional_flow_to_trips(
        artifact["aux_path"],
        requested=True,
        strict=False,
        storage_format=storage_format,
        issues=issues,
        destination_path=artifact["paths"].root_dir,
    )

    assert flow_to_trips_df is not None
    assert loaded is True
    assert n_rows == len(minimal_flow_to_trips_df)
    assert files_read == [artifact["aux_path"].name]
    assert list(flow_to_trips_df.columns) == list(minimal_flow_to_trips_df.columns)
    assert issues == []


# -----------------------------------------------------------------------------
# Bloque 7. Summary de lectura
# -----------------------------------------------------------------------------


def test_build_read_summary_returns_stable_public_summary(
    minimal_flows_df,
    minimal_flow_to_trips_df,
    assert_json_dumpable,
):
    """Verifica el summary mínimo estable construido para `read_flows`."""
    files_read = [
        "flows.feather",
        "flow_to_trips.feather",
        "flows.metadata.json",
    ]
    dataset_id = "dset_001"
    artifact_id = "art_001"
    n_flow_to_trips = len(minimal_flow_to_trips_df)

    summary = _build_read_summary(
        flows_df=minimal_flows_df,
        flow_to_trips_loaded=True,
        n_flow_to_trips=n_flow_to_trips,
        files_read=files_read,
        dataset_id=dataset_id,
        artifact_id=artifact_id,
    )

    assert summary["n_flows"] == len(minimal_flows_df)
    assert summary["n_columns"] == len(minimal_flows_df.columns)
    assert summary["flow_to_trips_loaded"] is True
    assert summary["n_flow_to_trips"] == n_flow_to_trips
    assert summary["files_read"] == files_read
    assert summary["dataset_id"] == dataset_id
    assert summary["artifact_id"] == artifact_id

    assert_json_dumpable(summary, "read_flows_summary")