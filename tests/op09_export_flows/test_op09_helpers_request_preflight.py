from __future__ import annotations

from pathlib import Path

import pytest

from pylondrina.errors import ExportError
from pylondrina.export.flows import (
    ExportFlowsOptions,
    _preflight_export_flows,
    _resolve_export_request,
)


def test_resolve_export_request_generates_folder_and_preserves_effective_options(
    make_flowdataset_for_export,
    export_root: Path,
) -> None:
    """Verifica request feliz con folder autogenerado y parámetros efectivos coherentes."""
    flows = make_flowdataset_for_export()

    options_eff, export_dir, parameters = _resolve_export_request(
        flows,
        str(export_root),
        ExportFlowsOptions(
            format="flowmap_blue",
            mode="error_if_exists",
            folder_name=None,
            extra_flow_fields=["mode"],
        ),
    )

    assert isinstance(options_eff, ExportFlowsOptions)
    assert options_eff.format == "flowmap_blue"
    assert options_eff.mode == "error_if_exists"
    assert options_eff.extra_flow_fields == ["mode"]

    export_dir_path = Path(export_dir)

    assert export_dir_path.parent == export_root
    assert Path(parameters["export_dir"]) == export_dir_path
    assert parameters["output_root"] == str(export_root)

    assert parameters["format"] == "flowmap_blue"
    assert parameters["mode"] == "error_if_exists"
    assert parameters["folder_name"] == options_eff.folder_name
    assert parameters["folder_name"] is not None
    assert parameters["extra_flow_fields"] == ["mode"]

    assert parameters["_folder_name_input"] is None
    assert parameters["_folder_name_generated"] is True
    assert parameters["_folder_name_input_invalid"] is False


def test_resolve_export_request_raises_when_export_directory_already_exists(
    make_flowdataset_for_export,
    export_root: Path,
) -> None:
    """Verifica error fatal por colisión de carpeta con mode='error_if_exists'."""
    flows = make_flowdataset_for_export()

    existing_dir = export_root / "fixed_folder"
    existing_dir.mkdir(parents=True, exist_ok=True)

    with pytest.raises(ExportError) as excinfo:
        _resolve_export_request(
            flows,
            str(export_root),
            ExportFlowsOptions(
                format="flowmap_blue",
                mode="error_if_exists",
                folder_name="fixed_folder",
            ),
        )

    assert excinfo.value.code == "EXPORT_FLOWS.LAYOUT.EXPORT_DIR_EXISTS_ABORT"


def test_preflight_export_flows_accepts_valid_extras_and_uses_flow_value_as_count_source(
    make_flowdataset_for_export,
) -> None:
    """Verifica preflight feliz con extras válidas y count derivado desde flow_value."""
    flows = make_flowdataset_for_export(
        with_extra_fields=True,
    )

    issues, info = _preflight_export_flows(
        flows,
        ExportFlowsOptions(
            extra_flow_fields=["mode", "purpose", "window_start_utc"],
        ),
    )

    assert not any(issue.level == "error" for issue in issues)
    assert info["count_source"] == "flow_value"
    assert info["extra_flow_fields"] == [
        "mode",
        "purpose",
        "window_start_utc",
    ]


def test_preflight_export_flows_raises_when_requested_extra_field_is_not_available(
    make_flowdataset_for_export,
) -> None:
    """Verifica error fatal cuando extra_flow_fields pide una columna interna inexistente."""
    flows = make_flowdataset_for_export(
        with_extra_fields=True,
    )

    with pytest.raises(ExportError) as excinfo:
        _preflight_export_flows(
            flows,
            ExportFlowsOptions(
                extra_flow_fields=["mode", "count"],
            ),
        )

    assert excinfo.value.code == "EXPORT_FLOWS.EXTRA.INVALID_FIELDS"