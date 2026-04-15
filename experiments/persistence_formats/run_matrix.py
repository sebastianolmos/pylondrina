from __future__ import annotations

"""
Orquestador secuencial del experimento de persistencia Parquet vs Feather.

Responsabilidades
-----------------
- Recorrer configuraciones, backends, warm-ups y repeticiones medidas.
- Lanzar `run_one.py` como proceso hijo nuevo por run.
- Forzar que cada run se ejecute con `cwd=repo_root` para que las rutas relativas
  del experimento se resuelvan siempre respecto de la raíz del repositorio.
- Capturar el JSON por stdout emitido por `run_one.py`.
- Appendear resultados a `runs.csv`.
- Crear una sola vez `experiment_manifest.json` con el contexto del experimento.

Decisiones operativas
---------------------
- No usa multiprocessing ni paralelismo.
- No escribe `summary_by_config.csv`; eso se deja para una etapa posterior.
- Si una run falla, la registra y continúa con las siguientes.
- Al final retorna código 0 si todas las runs terminaron con status=PASS;
  en caso contrario, retorna 1.
"""

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
import argparse
import csv
import json
import platform
import subprocess
import sys
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence


SCRIPT_DIR = Path(__file__).resolve().parent
RUN_ONE_PATH = SCRIPT_DIR / "run_one.py"
DEFAULT_CONFIGS = ["C1", "C2", "C3", "C4", "C5", "C6", "C7", "C8", "C9"]
DEFAULT_BACKENDS = ["parquet", "feather"]
DEFAULT_SEED = 20260415


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def _find_repo_root(start: Path) -> Path:
    current = start.resolve()
    for candidate in [current, *current.parents]:
        if (candidate / "pyproject.toml").exists() and (candidate / "src" / "pylondrina").exists():
            return candidate
    raise RuntimeError(
        "No se pudo ubicar la raíz del repo. "
        "Se esperaba encontrar pyproject.toml y src/pylondrina."
    )


REPO_ROOT = _find_repo_root(SCRIPT_DIR)
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "data" / "experiments" / "persistence_formats"
DEFAULT_RUNS_CSV = DEFAULT_OUTPUT_ROOT / "runs.csv"
DEFAULT_MANIFEST_PATH = DEFAULT_OUTPUT_ROOT / "experiment_manifest.json"
DEFAULT_ARTIFACTS_ROOT = DEFAULT_OUTPUT_ROOT / "artifacts_tmp"


CSV_COLUMNS: List[str] = [
    # Contexto de orquestación
    "orchestrator_timestamp",
    "launch_index",
    "subprocess_returncode",
    "stdout_json_parsed",
    "command",
    "stderr",
    "orchestrator_error",
    # Salida esperada de run_one.py
    "run_id",
    "timestamp_start",
    "config_id",
    "backend",
    "repetition",
    "is_warmup",
    "seed",
    "n_rows",
    "n_cols",
    "k",
    "memory_measurement_method",
    "memory_poll_interval_ms",
    "t_write_s",
    "t_read_s",
    "t_total_s",
    "size_data_bytes",
    "size_sidecar_bytes",
    "size_total_bytes",
    "peak_rss_write_MB",
    "peak_rss_read_MB",
    "peak_ram_total_MB",
    "fidelity_method",
    "fidelity_pass",
    "fidelity_detail",
    "status",
    "notes",
    "artifact_kept",
    "artifact_path",
    "hostname",
    "expected_dataset_fingerprint",
    "manifest",
    "exception_type",
    "exception_message",
    "traceback",
]


@dataclass(frozen=True)
class LaunchSpec:
    config_id: str
    backend: str
    is_warmup: bool
    repetition: int


def version_or_none(dist_name: str) -> Optional[str]:
    try:
        if sys.version_info >= (3, 10):
            from importlib.metadata import PackageNotFoundError, version
        else:  # pragma: no cover
            from importlib_metadata import PackageNotFoundError, version  # type: ignore
        return version(dist_name)
    except Exception:
        return None


def get_machine_summary() -> Dict[str, Any]:
    try:
        import psutil
    except Exception:
        psutil = None

    total_ram_bytes = None
    logical_cores = None
    physical_cores = None
    if psutil is not None:
        try:
            total_ram_bytes = int(psutil.virtual_memory().total)
        except Exception:
            total_ram_bytes = None
        try:
            logical_cores = int(psutil.cpu_count(logical=True))
        except Exception:
            logical_cores = None
        try:
            physical_cores = int(psutil.cpu_count(logical=False)) if psutil.cpu_count(logical=False) is not None else None
        except Exception:
            physical_cores = None

    return {
        "hostname": platform.node(),
        "platform": platform.platform(),
        "system": platform.system(),
        "release": platform.release(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "python_version": platform.python_version(),
        "logical_cores": logical_cores,
        "physical_cores": physical_cores,
        "total_ram_bytes": total_ram_bytes,
        "package_versions": {
            "pandas": version_or_none("pandas"),
            "pyarrow": version_or_none("pyarrow"),
            "psutil": version_or_none("psutil"),
            "pylondrina": version_or_none("pylondrina"),
        },
    }


def build_manifest(args: argparse.Namespace, launch_plan: Sequence[LaunchSpec]) -> Dict[str, Any]:
    measured_runs = sum(1 for spec in launch_plan if not spec.is_warmup)
    warmup_runs = sum(1 for spec in launch_plan if spec.is_warmup)
    return {
        "experiment_name": "persistence_formats_v1",
        "created_at_utc": utc_now_iso(),
        "repo_root": str(REPO_ROOT),
        "cwd_used_for_runs": str(REPO_ROOT),
        "script_path": str(Path(__file__).resolve()),
        "run_one_path": str(RUN_ONE_PATH.resolve()),
        "effective_args": {
            "configs": list(args.configs),
            "backends": list(args.backends),
            "warmup_repetitions": int(args.warmup_repetitions),
            "measured_repetitions": int(args.repetitions),
            "seed": int(args.seed),
            "artifacts_root": str(args.artifacts_root),
            "keep_artifact": bool(int(args.keep_artifact)),
            "memory_poll_interval_ms": int(args.memory_poll_interval_ms),
            "read_strict": bool(int(args.read_strict)),
            "parquet_compression": args.parquet_compression,
            "feather_compression": args.feather_compression,
            "append": bool(int(args.append)),
        },
        "planned_counts": {
            "n_configs": len(args.configs),
            "n_backends": len(args.backends),
            "warmup_runs": warmup_runs,
            "measured_runs": measured_runs,
            "total_runs": len(launch_plan),
        },
        "measurement_policy": {
            "time_metric": "time.perf_counter_ns() measured inside run_one.py",
            "memory_measurement_method": "psutil.Process().memory_info().rss polling",
            "memory_poll_interval_ms": int(args.memory_poll_interval_ms),
            "artifact_size_definition": "data file + sidecar metadata json",
            "fidelity_rule_small": "assert_frame_equal+fingerprint when n_rows <= 20000",
            "fidelity_rule_large": "column_fingerprint + structural checks otherwise",
        },
        "machine": get_machine_summary(),
    }


def write_manifest_once(manifest_path: Path, payload: Mapping[str, Any], *, append: bool) -> None:
    ensure_dir(manifest_path.parent)
    if manifest_path.exists() and not append:
        raise FileExistsError(
            f"El manifest ya existe y append=0: {manifest_path}. "
            "Bórralo o usa --append 1 si quieres continuar sobre el mismo experimento."
        )
    if manifest_path.exists() and append:
        return
    manifest_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def ensure_runs_csv(runs_csv: Path, *, append: bool) -> None:
    ensure_dir(runs_csv.parent)
    if runs_csv.exists() and not append:
        raise FileExistsError(
            f"El archivo de runs ya existe y append=0: {runs_csv}. "
            "Bórralo o usa --append 1 para seguir agregando filas."
        )
    if not runs_csv.exists():
        with runs_csv.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=CSV_COLUMNS)
            writer.writeheader()


def normalize_scalar_for_csv(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, (str, int, float, bool)):
        return value
    return json.dumps(value, ensure_ascii=False, sort_keys=False)


def parse_run_one_stdout(stdout: str) -> Dict[str, Any]:
    lines = [line.strip() for line in stdout.splitlines() if line.strip()]
    if not lines:
        raise ValueError("run_one.py no emitió stdout utilizable.")
    candidate = lines[-1]
    payload = json.loads(candidate)
    if not isinstance(payload, dict):
        raise TypeError("El stdout parseado no es un JSON object.")
    return payload


def build_failure_payload_for_orchestrator(
    *,
    launch_spec: LaunchSpec,
    seed: int,
    stdout: str,
    stderr: str,
    returncode: int,
    error_message: str,
) -> Dict[str, Any]:
    return {
        "run_id": "",
        "timestamp_start": "",
        "config_id": launch_spec.config_id,
        "backend": launch_spec.backend,
        "repetition": int(launch_spec.repetition),
        "is_warmup": bool(launch_spec.is_warmup),
        "seed": int(seed),
        "n_rows": "",
        "n_cols": "",
        "k": "",
        "memory_measurement_method": "",
        "memory_poll_interval_ms": "",
        "t_write_s": "",
        "t_read_s": "",
        "t_total_s": "",
        "size_data_bytes": "",
        "size_sidecar_bytes": "",
        "size_total_bytes": "",
        "peak_rss_write_MB": "",
        "peak_rss_read_MB": "",
        "peak_ram_total_MB": "",
        "fidelity_method": "",
        "fidelity_pass": "",
        "fidelity_detail": "",
        "status": "FAIL_RUNTIME",
        "notes": json.dumps(["Fallo del orquestador al consumir la salida de run_one.py"], ensure_ascii=False),
        "artifact_kept": "",
        "artifact_path": "",
        "hostname": "",
        "expected_dataset_fingerprint": "",
        "manifest": "",
        "exception_type": "RunMatrixParseError",
        "exception_message": error_message,
        "traceback": "",
        # extras de orquestación
        "subprocess_returncode": returncode,
        "stdout_json_parsed": False,
        "stderr": stderr,
        "orchestrator_error": error_message,
    }


def append_run_row(
    runs_csv: Path,
    *,
    launch_index: int,
    command: Sequence[str],
    returncode: int,
    stdout_json_parsed: bool,
    stderr: str,
    orchestrator_error: str,
    run_payload: Mapping[str, Any],
) -> None:
    row: Dict[str, Any] = {column: "" for column in CSV_COLUMNS}
    row.update(
        {
            "orchestrator_timestamp": utc_now_iso(),
            "launch_index": int(launch_index),
            "subprocess_returncode": int(returncode),
            "stdout_json_parsed": bool(stdout_json_parsed),
            "command": " ".join(command),
            "stderr": stderr,
            "orchestrator_error": orchestrator_error,
        }
    )

    for key, value in run_payload.items():
        if key in row:
            row[key] = normalize_scalar_for_csv(value)

    with runs_csv.open("a", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_COLUMNS)
        writer.writerow(row)


def build_launch_plan(
    *,
    configs: Sequence[str],
    backends: Sequence[str],
    warmup_repetitions: int,
    repetitions: int,
) -> List[LaunchSpec]:
    if warmup_repetitions < 0:
        raise ValueError("warmup_repetitions debe ser >= 0")
    if repetitions < 0:
        raise ValueError("repetitions debe ser >= 0")

    plan: List[LaunchSpec] = []
    for config_id in configs:
        for backend in backends:
            for warm_idx in range(warmup_repetitions):
                plan.append(
                    LaunchSpec(
                        config_id=config_id,
                        backend=backend,
                        is_warmup=True,
                        repetition=warm_idx,
                    )
                )
            for rep in range(repetitions):
                plan.append(
                    LaunchSpec(
                        config_id=config_id,
                        backend=backend,
                        is_warmup=False,
                        repetition=rep,
                    )
                )
    return plan


def build_run_one_command(args: argparse.Namespace, spec: LaunchSpec) -> List[str]:
    return [
        sys.executable,
        str(RUN_ONE_PATH),
        "--config", spec.config_id,
        "--backend", spec.backend,
        "--warmup", "1" if spec.is_warmup else "0",
        "--rep", str(spec.repetition),
        "--seed", str(int(args.seed)),
        "--artifacts-root", str(Path(args.artifacts_root)),
        "--keep-artifact", str(int(args.keep_artifact)),
        "--memory-poll-interval-ms", str(int(args.memory_poll_interval_ms)),
        "--read-strict", str(int(args.read_strict)),
        "--parquet-compression", args.parquet_compression,
        "--feather-compression", args.feather_compression,
    ]


def run_matrix(args: argparse.Namespace) -> int:
    launch_plan = build_launch_plan(
        configs=args.configs,
        backends=args.backends,
        warmup_repetitions=int(args.warmup_repetitions),
        repetitions=int(args.repetitions),
    )

    write_manifest_once(
        Path(args.manifest_path),
        build_manifest(args, launch_plan),
        append=bool(int(args.append)),
    )
    ensure_runs_csv(Path(args.runs_csv), append=bool(int(args.append)))

    total = len(launch_plan)
    n_pass = 0
    n_non_pass = 0

    for launch_index, spec in enumerate(launch_plan, start=1):
        cmd = build_run_one_command(args, spec)
        label = f"[{launch_index}/{total}] config={spec.config_id} backend={spec.backend}"
        if spec.is_warmup:
            label += f" warmup={spec.repetition}"
        else:
            label += f" rep={spec.repetition}"
        print(label, flush=True)

        completed = subprocess.run(
            cmd,
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )

        stdout = completed.stdout or ""
        stderr = completed.stderr or ""
        parse_ok = False
        orchestrator_error = ""

        try:
            run_payload = parse_run_one_stdout(stdout)
            parse_ok = True
        except Exception as exc:
            orchestrator_error = f"No se pudo parsear el stdout JSON de run_one.py: {exc}"
            run_payload = build_failure_payload_for_orchestrator(
                launch_spec=spec,
                seed=int(args.seed),
                stdout=stdout,
                stderr=stderr,
                returncode=int(completed.returncode),
                error_message=orchestrator_error,
            )

        append_run_row(
            Path(args.runs_csv),
            launch_index=launch_index,
            command=cmd,
            returncode=int(completed.returncode),
            stdout_json_parsed=parse_ok,
            stderr=stderr,
            orchestrator_error=orchestrator_error,
            run_payload=run_payload,
        )

        status = str(run_payload.get("status", ""))
        if status == "PASS":
            n_pass += 1
        else:
            n_non_pass += 1
            print(
                f"  -> status={status or 'UNKNOWN'} | config={spec.config_id} backend={spec.backend} rep={spec.repetition}",
                flush=True,
            )

    summary = {
        "finished_at_utc": utc_now_iso(),
        "total_runs": total,
        "n_pass": n_pass,
        "n_non_pass": n_non_pass,
        "runs_csv": str(Path(args.runs_csv)),
        "manifest_path": str(Path(args.manifest_path)),
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2), flush=True)
    return 0 if n_non_pass == 0 else 1


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Orquesta el experimento de persistencia lanzando run_one.py secuencialmente.",
    )
    parser.add_argument("--configs", nargs="+", default=DEFAULT_CONFIGS, choices=DEFAULT_CONFIGS)
    parser.add_argument("--backends", nargs="+", default=DEFAULT_BACKENDS, choices=DEFAULT_BACKENDS)
    parser.add_argument("--warmup-repetitions", type=int, default=1)
    parser.add_argument("--repetitions", type=int, default=5)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--artifacts-root", type=Path, default=DEFAULT_ARTIFACTS_ROOT)
    parser.add_argument("--runs-csv", type=Path, default=DEFAULT_RUNS_CSV)
    parser.add_argument("--manifest-path", type=Path, default=DEFAULT_MANIFEST_PATH)
    parser.add_argument("--append", type=int, default=0, choices=[0, 1])
    parser.add_argument("--keep-artifact", type=int, default=0, choices=[0, 1])
    parser.add_argument("--memory-poll-interval-ms", type=int, default=10)
    parser.add_argument("--read-strict", type=int, default=0, choices=[0, 1])
    parser.add_argument("--parquet-compression", default="snappy", choices=["snappy", "gzip", "zstd", "brotli", "none"])
    parser.add_argument("--feather-compression", default="lz4", choices=["lz4", "zstd", "uncompressed", "none"])
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return run_matrix(args)


if __name__ == "__main__":
    raise SystemExit(main())
