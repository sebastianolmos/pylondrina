from __future__ import annotations

"""
Ejecuta una sola run del experimento de persistencia Parquet vs Feather.

Pipeline adoptado
-----------------
1. Resuelve una configuración experimental `Ci`.
2. Genera en memoria el caso base llamando exactamente una vez a
   `generate_experiment_case(...)`.
3. Mide `write_trips(...)` sobre ese `TripDataset`.
4. Mide tamaño del artefacto formal persistido (data + sidecar).
5. Mide `read_trips(...)` usando el sidecar, sin pasar schema externo.
6. Verifica fidelidad roundtrip:
   - datasets pequeños: `assert_frame_equal(...)` + fingerprint
   - datasets grandes: checks estructurales + fingerprint
7. Emite un JSON único por stdout, pensado para que `run_matrix.py` lo capture.

"""

from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
import argparse
import errno
import gc
import json
import os
import shutil
import socket
import sys
import tempfile
import threading
import time
import traceback
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

# -----------------------------------------------------------------------------
# Bootstrapping de imports locales
# -----------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).resolve().parent
IMPORT_CANDIDATES = [
    SCRIPT_DIR,
    SCRIPT_DIR.parent,
    SCRIPT_DIR / "src",
    SCRIPT_DIR.parent / "src",
]
for candidate in IMPORT_CANDIDATES:
    if candidate.exists() and str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

from pandas.testing import assert_frame_equal

from generate_case import (
    DEFAULT_SEED,
    ExperimentConfig,
    compute_expected_fingerprint,
    generate_experiment_case,
    get_predefined_config,
)
from pylondrina.io.trips import ReadTripsOptions, WriteTripsOptions, read_trips, write_trips

try:
    import psutil
except ImportError as exc:  # pragma: no cover - depende del entorno del usuario
    raise ImportError(
        "run_one.py requiere `psutil` para medir peak RAM del proceso. "
        "Instálalo en tu ambiente antes de ejecutar el experimento."
    ) from exc


# -----------------------------------------------------------------------------
# Constantes de la run
# -----------------------------------------------------------------------------

EXACT_COMPARE_MAX_ROWS = 20_000
DEFAULT_MEMORY_POLL_INTERVAL_MS = 10
DEFAULT_ARTIFACTS_ROOT = Path("data/experiments/persistence_formats/artifacts_tmp")

STATUS_PASS = "PASS"
STATUS_FAIL_FIDELITY = "FAIL_FIDELITY"
STATUS_FAIL_RUNTIME = "FAIL_RUNTIME"
STATUS_INVALIDATED = "INVALIDATED"


# -----------------------------------------------------------------------------
# Monitoreo de memoria
# -----------------------------------------------------------------------------


class PeakRSSPoller:
    """
    Monitorea RSS del proceso actual mediante polling periódico.

    Se usa para aproximar peak RAM durante write y read de forma simple y portable.
    """

    def __init__(self, *, interval_ms: int = DEFAULT_MEMORY_POLL_INTERVAL_MS) -> None:
        if interval_ms <= 0:
            raise ValueError("interval_ms debe ser > 0")
        self.interval_ms = int(interval_ms)
        self.interval_s = self.interval_ms / 1000.0
        self._process = psutil.Process(os.getpid())
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._peak_rss_bytes: int = 0
        self._samples: int = 0

    @property
    def peak_rss_bytes(self) -> int:
        return int(self._peak_rss_bytes)

    @property
    def samples(self) -> int:
        return int(self._samples)

    def start(self) -> None:
        self._peak_rss_bytes = max(self._peak_rss_bytes, self._safe_rss())
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        self._thread.join(timeout=max(1.0, self.interval_s * 10))
        self._peak_rss_bytes = max(self._peak_rss_bytes, self._safe_rss())

    def _run(self) -> None:
        while not self._stop_event.is_set():
            self._peak_rss_bytes = max(self._peak_rss_bytes, self._safe_rss())
            self._samples += 1
            self._stop_event.wait(self.interval_s)

    def _safe_rss(self) -> int:
        try:
            return int(self._process.memory_info().rss)
        except Exception:
            return 0


# -----------------------------------------------------------------------------
# Medición de fases
# -----------------------------------------------------------------------------


def measure_with_peak_rss(
    fn: Callable[[], Any],
    *,
    poll_interval_ms: int,
) -> Tuple[Any, float, int]:
    """
    Ejecuta `fn`, mide tiempo wall-clock y peak RSS del proceso actual.
    """
    gc.collect()
    poller = PeakRSSPoller(interval_ms=poll_interval_ms)
    poller.start()
    t0 = time.perf_counter_ns()
    try:
        result = fn()
    finally:
        t1 = time.perf_counter_ns()
        poller.stop()
    elapsed_s = (t1 - t0) / 1_000_000_000.0
    return result, float(elapsed_s), int(poller.peak_rss_bytes)


# -----------------------------------------------------------------------------
# Utilidades de paths / artefactos
# -----------------------------------------------------------------------------


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def build_run_id(*, config_id: str, backend: str, repetition: int, is_warmup: bool) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    phase = "warmup" if is_warmup else f"rep{repetition}"
    return f"{ts}__{config_id}__{backend}__{phase}"


def build_artifact_path(
    *,
    artifacts_root: Path,
    run_id: str,
    config_id: str,
    backend: str,
) -> Path:
    root = ensure_dir(artifacts_root)
    artifact_name = f"{run_id}__{config_id}__{backend}.golondrina"
    return root / artifact_name


def load_artifact_size_breakdown(artifact_root: Path) -> Dict[str, int]:
    """
    Obtiene tamaños del artefacto persistido leyendo el sidecar oficial.
    """
    sidecar_path = artifact_root / "trips.metadata.json"
    payload = json.loads(sidecar_path.read_text(encoding="utf-8"))
    files = payload.get("files", {})
    data_filename = files.get("data")
    metadata_filename = files.get("metadata", "trips.metadata.json")

    if not data_filename:
        raise RuntimeError("El sidecar no declara files['data']")

    data_path = artifact_root / str(data_filename)
    metadata_path = artifact_root / str(metadata_filename)

    size_data_bytes = int(data_path.stat().st_size)
    size_sidecar_bytes = int(metadata_path.stat().st_size)
    size_total_bytes = int(size_data_bytes + size_sidecar_bytes)
    return {
        "size_data_bytes": size_data_bytes,
        "size_sidecar_bytes": size_sidecar_bytes,
        "size_total_bytes": size_total_bytes,
    }


# -----------------------------------------------------------------------------
# Serialización JSON-safe
# -----------------------------------------------------------------------------


def issue_to_dict(issue: Any) -> Dict[str, Any]:
    try:
        return asdict(issue)
    except Exception:
        return {
            "level": getattr(issue, "level", None),
            "code": getattr(issue, "code", None),
            "message": getattr(issue, "message", None),
            "field": getattr(issue, "field", None),
            "source_field": getattr(issue, "source_field", None),
            "row_count": getattr(issue, "row_count", None),
            "details": getattr(issue, "details", None),
        }


def report_to_dict(report: Any) -> Dict[str, Any]:
    return {
        "ok": bool(getattr(report, "ok", False)),
        "summary": getattr(report, "summary", {}) or {},
        "parameters": getattr(report, "parameters", None),
        "issues": [issue_to_dict(issue) for issue in (getattr(report, "issues", []) or [])],
    }


# -----------------------------------------------------------------------------
# Fidelidad
# -----------------------------------------------------------------------------


def verify_fidelity(
    *,
    df_expected,
    df_observed,
    expected_fingerprint: Mapping[str, Any],
    config: ExperimentConfig,
    seed: int,
) -> Tuple[bool, str, Dict[str, Any]]:
    """
    Verifica fidelidad roundtrip entre el dataset esperado y el reconstruido.

    Regla cerrada:
    - datasets pequeños (`n_rows <= 20_000`): assert_frame_equal + fingerprint
    - datasets grandes: checks estructurales + fingerprint
    """
    observed_fingerprint = compute_expected_fingerprint(
        df_observed,
        config=config,
        seed=seed,
    )

    shape_expected = (int(df_expected.shape[0]), int(df_expected.shape[1]))
    shape_observed = (int(df_observed.shape[0]), int(df_observed.shape[1]))

    columns_expected = list(expected_fingerprint["columns_order"])
    columns_observed = list(df_observed.columns)

    dtypes_expected = dict(expected_fingerprint["dtypes_expected"])
    dtypes_observed = {column: str(df_observed[column].dtype) for column in columns_observed}

    shape_match = shape_expected == shape_observed
    columns_order_match = columns_expected == columns_observed
    dtypes_match = dtypes_expected == dtypes_observed
    dataset_fingerprint_match = (
        observed_fingerprint["dataset_fingerprint"] == expected_fingerprint["dataset_fingerprint"]
    )

    mismatched_columns = [
        column
        for column in columns_expected
        if observed_fingerprint["column_fingerprints"].get(column)
        != expected_fingerprint["column_fingerprints"].get(column)
    ]

    detail: Dict[str, Any] = {
        "shape_match": shape_match,
        "columns_order_match": columns_order_match,
        "dtypes_match": dtypes_match,
        "dataset_fingerprint_match": dataset_fingerprint_match,
        "n_mismatched_columns": int(len(mismatched_columns)),
    }
    
    if not dataset_fingerprint_match or not shape_match or not columns_order_match or not dtypes_match:
        detail["shape_expected"] = list(shape_expected)
        detail["shape_observed"] = list(shape_observed)
        detail["mismatched_columns_sample"] = mismatched_columns[:20]
        detail["observed_dataset_fingerprint"] = observed_fingerprint["dataset_fingerprint"]
        detail["expected_dataset_fingerprint"] = expected_fingerprint["dataset_fingerprint"]

    is_small_dataset = int(config.n_rows) <= EXACT_COMPARE_MAX_ROWS
    if is_small_dataset:
        method = "assert_frame_equal+fingerprint"
        try:
            assert_frame_equal(
                df_expected,
                df_observed,
                check_dtype=True,
                check_exact=True,
                check_categorical=True,
            )
            frame_equal_pass = True
            frame_equal_message = None
        except AssertionError as exc:
            frame_equal_pass = False
            frame_equal_message = str(exc)

        detail["frame_equal_pass"] = frame_equal_pass
        if frame_equal_message is not None:
            detail["frame_equal_message"] = frame_equal_message[:4000]

        passed = all(
            [
                frame_equal_pass,
                shape_match,
                columns_order_match,
                dtypes_match,
                dataset_fingerprint_match,
            ]
        )
        return bool(passed), method, detail

    method = "column_fingerprint"
    passed = all(
        [
            shape_match,
            columns_order_match,
            dtypes_match,
            dataset_fingerprint_match,
        ]
    )
    return bool(passed), method, detail


# -----------------------------------------------------------------------------
# Ejecución de una run
# -----------------------------------------------------------------------------


def run_one(args: argparse.Namespace) -> Dict[str, Any]:
    timestamp_start = utc_now_iso()
    config = get_predefined_config(args.config)
    is_warmup = bool(int(args.warmup))
    run_id = build_run_id(
        config_id=config.config_id,
        backend=args.backend,
        repetition=int(args.rep),
        is_warmup=is_warmup,
    )

    result: Dict[str, Any] = {
        "run_id": run_id,
        "timestamp_start": timestamp_start,
        "config_id": config.config_id,
        "backend": args.backend,
        "repetition": int(args.rep),
        "is_warmup": is_warmup,
        "seed": int(args.seed),
        "n_rows": int(config.n_rows),
        "n_cols": int(config.n_cols),
        "k": int(config.k),
        "memory_measurement_method": "psutil.Process().memory_info().rss polling",
        "memory_poll_interval_ms": int(args.memory_poll_interval_ms),
        "t_write_s": None,
        "t_read_s": None,
        "t_total_s": None,
        "size_data_bytes": None,
        "size_sidecar_bytes": None,
        "size_total_bytes": None,
        "peak_rss_write_MB": None,
        "peak_rss_read_MB": None,
        "peak_ram_total_MB": None,
        "fidelity_method": None,
        "fidelity_pass": None,
        "fidelity_detail": None,
        "status": None,
        "notes": [],
        "artifact_kept": bool(int(args.keep_artifact)),
        "hostname": socket.gethostname(),
    }

    artifact_path: Optional[Path] = None
    try:
        case = generate_experiment_case(
            config,
            seed=int(args.seed),
            persist_fingerprint_dir=None,
        )
        trips_expected = case.trips
        df_expected = case.trips.data
        fingerprint_expected = case.expected_fingerprint
        result["expected_dataset_fingerprint"] = fingerprint_expected["dataset_fingerprint"]

        artifact_path = build_artifact_path(
            artifacts_root=Path(args.artifacts_root),
            run_id=run_id,
            config_id=config.config_id,
            backend=args.backend,
        )
        
        if bool(int(args.keep_artifact)):
            result["artifact_path"] = str(artifact_path)    

        write_options = WriteTripsOptions(
            mode="overwrite",
            require_validated=True,
            storage_format=args.backend,
            parquet_compression=None if args.parquet_compression == "none" else args.parquet_compression,
            feather_compression=(
                None if args.feather_compression == "none" else args.feather_compression
            ),
            normalize_artifact_dir=True,
        )

        def _write_phase() -> Any:
            return write_trips(trips_expected, artifact_path, options=write_options)

        write_report, t_write_s, peak_write_rss_bytes = measure_with_peak_rss(
            _write_phase,
            poll_interval_ms=int(args.memory_poll_interval_ms),
        )
        result["t_write_s"] = round(t_write_s, 9)
        result["peak_rss_write_MB"] = bytes_to_mb(peak_write_rss_bytes)

        size_breakdown = load_artifact_size_breakdown(artifact_path)
        result.update(size_breakdown)

        read_options = ReadTripsOptions(
            schema=None,
            strict=bool(int(args.read_strict)),
            keep_metadata=True,
        )

        def _read_phase() -> Any:
            return read_trips(artifact_path, options=read_options)

        read_result, t_read_s, peak_read_rss_bytes = measure_with_peak_rss(
            _read_phase,
            poll_interval_ms=int(args.memory_poll_interval_ms),
        )
        trips_read, read_report = read_result
        result["t_read_s"] = round(t_read_s, 9)
        result["peak_rss_read_MB"] = bytes_to_mb(peak_read_rss_bytes)

        fidelity_pass, fidelity_method, fidelity_detail = verify_fidelity(
            df_expected=df_expected,
            df_observed=trips_read.data,
            expected_fingerprint=fingerprint_expected,
            config=config,
            seed=int(args.seed),
        )
        result["fidelity_method"] = fidelity_method
        result["fidelity_pass"] = bool(fidelity_pass)
        result["fidelity_detail"] = fidelity_detail

        peak_total_mb = max(
            bytes_to_mb(peak_write_rss_bytes),
            bytes_to_mb(peak_read_rss_bytes),
        )
        result["peak_ram_total_MB"] = peak_total_mb
        result["t_total_s"] = round(float(t_write_s + t_read_s), 9)

        if fidelity_pass:
            result["status"] = STATUS_PASS
        else:
            result["status"] = STATUS_FAIL_FIDELITY
            result["notes"].append("La run terminó, pero la verificación de fidelidad falló.")

        return result

    except KeyboardInterrupt:
        result["status"] = STATUS_INVALIDATED
        result["notes"].append("Run invalidada por interrupción manual (KeyboardInterrupt).")
        return result
    except OSError as exc:
        if getattr(exc, "errno", None) == errno.ENOSPC:
            result["status"] = STATUS_INVALIDATED
            result["notes"].append("Run invalidada por disco lleno (ENOSPC).")
        else:
            result["status"] = STATUS_FAIL_RUNTIME
            result["notes"].append(f"OSError durante la run: {exc}")
        result["exception_type"] = type(exc).__name__
        result["exception_message"] = str(exc)
        return result
    except Exception as exc:
        result["status"] = STATUS_FAIL_RUNTIME
        result["notes"].append(f"Excepción no controlada durante la run: {exc}")
        result["exception_type"] = type(exc).__name__
        result["exception_message"] = str(exc)
        result["traceback"] = traceback.format_exc(limit=20)
        return result
    finally:
        if artifact_path is not None and not bool(int(args.keep_artifact)):
            shutil.rmtree(artifact_path, ignore_errors=True)


# -----------------------------------------------------------------------------
# Helpers generales
# -----------------------------------------------------------------------------


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def bytes_to_mb(value: int | float | None) -> Optional[float]:
    if value is None:
        return None
    return round(float(value) / (1024.0 * 1024.0), 6)


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Ejecuta una sola run del experimento de persistencia de trips.",
    )
    parser.add_argument("--config", required=True, choices=sorted(["C1", "C2", "C3", "C4", "C5", "C6", "C7", "C8", "C9"]))
    parser.add_argument("--backend", required=True, choices=["parquet", "feather"])
    parser.add_argument("--warmup", type=int, default=0, choices=[0, 1])
    parser.add_argument("--rep", type=int, default=0)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--artifacts-root", type=Path, default=DEFAULT_ARTIFACTS_ROOT)
    parser.add_argument("--keep-artifact", type=int, default=0, choices=[0, 1])
    parser.add_argument("--memory-poll-interval-ms", type=int, default=DEFAULT_MEMORY_POLL_INTERVAL_MS)
    parser.add_argument("--read-strict", type=int, default=0, choices=[0, 1])
    parser.add_argument("--parquet-compression", default="snappy", choices=["snappy", "gzip", "zstd", "brotli", "none"])
    parser.add_argument("--feather-compression", default="lz4", choices=["lz4", "zstd", "uncompressed", "none"])
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    result = run_one(args)
    print(json.dumps(result, ensure_ascii=False, sort_keys=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
