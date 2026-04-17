#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


try:
    import pyarrow.feather as feather
    import pyarrow.parquet as pq
except ImportError:  # pragma: no cover
    feather = None
    pq = None
    


FLOWMAP_FLOWS_REQUIRED = {"origin", "dest", "count"}
FLOWMAP_LOCATIONS_REQUIRED = {"id", "lat", "lon"}

GOLONDRINA_FLOWS_REQUIRED = {
    "flow_id",
    "origin_h3_index",
    "destination_h3_index",
    "flow_count",
    "flow_value",
}

FLOW_TO_TRIPS_SIGNATURE = {"flow_id", "movement_id"}
GOLONDRINA_PARQUET_FORMAT = "golondrina_parquet"
GOLONDRINA_FEATHER_FORMAT = "golondrina_feather"

DEFAULT_MAX_DEPTH = 10


@dataclass(frozen=True)
class ScanConfig:
    repo_root: Path
    data_root: Path
    output_path: Path
    max_depth: int
    verbose: bool = False


def log(message: str, *, verbose: bool) -> None:
    """Imprime mensajes de apoyo solo cuando el modo verbose está activado."""
    if verbose:
        print(message)


def iso_utc_now() -> str:
    """Retorna timestamp UTC en formato ISO-8601 con sufijo Z."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def slugify(value: str) -> str:
    """Convierte texto libre en un identificador simple, estable y seguro para JSON/URLs lógicas."""
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9._-]+", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")
    return value or "dataset"


def make_dataset_id(*parts: str) -> str:
    """Construye un id estable a partir de partes de ruta o nombre."""
    cleaned = [slugify(part) for part in parts if part and part.strip()]
    return "__".join(cleaned) if cleaned else "dataset"


def normalize_columns(columns: list[str]) -> set[str]:
    """Normaliza nombres de columnas para detección por firma mínima."""
    return {col.strip().lower() for col in columns if col and col.strip()}


def to_web_path(path: Path, repo_root: Path) -> str:
    """
    Convierte una ruta del filesystem del repo a una ruta tipo web.

    Ejemplo:
    repo_root/data/flows/synthetic/demo -> /data/flows/synthetic/demo
    """
    relative = path.resolve().relative_to(repo_root.resolve())
    return "/" + relative.as_posix()


def inspect_csv_columns(path: Path) -> list[str] | None:
    """
    Inspecciona solo la cabecera de un CSV para obtener sus columnas.

    No carga filas de datos.
    """
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            sample = f.read(4096)
            f.seek(0)

            try:
                dialect = csv.Sniffer().sniff(sample)
            except csv.Error:
                dialect = csv.excel

            reader = csv.reader(f, dialect)
            header = next(reader, None)

        if not header:
            return None

        return [col.strip() for col in header]
    except Exception:
        return None


def inspect_parquet_columns(path: Path) -> list[str] | None:
    """
    Inspecciona el schema de un Parquet sin cargar la tabla completa.

    Requiere pyarrow.
    """
    if pq is None:
        raise RuntimeError(
            "pyarrow no está instalado. Instálalo para poder inspeccionar archivos parquet."
        )

    try:
        schema = pq.read_schema(path)
        return list(schema.names)
    except Exception:
        return None
    
def inspect_feather_columns(path: Path) -> list[str] | None:
    """
    Inspecciona columnas de un Feather v2 usando PyArrow.

    Nota:
    - Feather v2 está representado en disco como Arrow IPC.
    - Para este generador se usa `pyarrow.feather.read_table(...)`
      por simplicidad y compatibilidad del flujo.
    """
    if feather is None:
        raise RuntimeError(
            "pyarrow no está instalado. Instálalo para poder inspeccionar archivos feather."
        )

    try:
        table = feather.read_table(path)
        return list(table.schema.names)
    except Exception:
        return None


def build_dataset_node(
    *,
    dataset_id: str,
    label: str,
    format_name: str,
    dataset_path: Path,
    files: dict[str, str],
    repo_root: Path,
) -> dict[str, Any]:
    """Construye un nodo dataset serializable para el registry."""
    return {
        "type": "dataset",
        "id": dataset_id,
        "label": label,
        "format": format_name,
        "dataset_path": to_web_path(dataset_path, repo_root),
        "files": files,
    }


def detect_flowmap_layout_dataset(dir_path: Path, config: ScanConfig) -> dict[str, Any] | None:
    """
    Detecta un dataset válido Flowmap layout dentro de una carpeta.

    Política conservadora:
    - la carpeta debe contener exactamente 1 CSV válido de flows
    - y exactamente 1 CSV válido de locations

    Si hay ambigüedad, la carpeta se omite en vez de adivinar.
    """
    csv_files = sorted([p for p in dir_path.iterdir() if p.is_file() and p.suffix.lower() == ".csv"])

    flow_candidates: list[Path] = []
    location_candidates: list[Path] = []

    for csv_path in csv_files:
        columns = inspect_csv_columns(csv_path)
        if not columns:
            continue

        normalized = normalize_columns(columns)

        if FLOWMAP_FLOWS_REQUIRED.issubset(normalized):
            flow_candidates.append(csv_path)

        if FLOWMAP_LOCATIONS_REQUIRED.issubset(normalized):
            location_candidates.append(csv_path)

    if len(flow_candidates) != 1 or len(location_candidates) != 1:
        if flow_candidates or location_candidates:
            log(
                f"[flowmap] Carpeta omitida por ambigüedad: {dir_path} "
                f"(flows={len(flow_candidates)}, locations={len(location_candidates)})",
                verbose=config.verbose,
            )
        return None

    flows_path = flow_candidates[0]
    locations_path = location_candidates[0]

    if flows_path == locations_path:
        log(f"[flowmap] Carpeta omitida: un mismo CSV calzó como flows y locations: {dir_path}", verbose=config.verbose)
        return None

    relative_dir = dir_path.relative_to(config.data_root)
    dataset_id = make_dataset_id("flowmap", *relative_dir.parts)
    label = dir_path.name

    files: dict[str, str] = {
        "flows": flows_path.name,
        "locations": locations_path.name,
    }

    metadata_path = dir_path / "metadata.json"
    if metadata_path.is_file():
        files["metadata"] = metadata_path.name

    return build_dataset_node(
        dataset_id=dataset_id,
        label=label,
        format_name="flowmap_layout",
        dataset_path=dir_path,
        files=files,
        repo_root=config.repo_root,
    )


def detect_golondrina_flows_datasets(dir_path: Path, config: ScanConfig) -> list[dict[str, Any]]:
    """
    Detecta datasets válidos de flujos Golondrina dentro de una carpeta,
    distinguiendo explícitamente entre backend Parquet y backend Feather v2.

    Formatos emitidos en el registry:
    - golondrina_parquet
    - golondrina_feather

    Reglas:
    - Un archivo principal de flows es válido si contiene la firma mínima:
      flow_id, origin_h3_index, destination_h3_index, flow_count, flow_value
    - Un archivo auxiliar flow_to_trips se reconoce por la firma mínima:
      flow_id, movement_id
    - La metadata sidecar compartida sigue siendo flows.metadata.json
    """

    backend_specs = [
        {
            "storage_name": "parquet",
            "format_name": GOLONDRINA_PARQUET_FORMAT,
            "suffix": ".parquet",
            "flows_name_hint": "flows.parquet",
            "flow_to_trips_name_hint": "flow_to_trips.parquet",
            "inspect_columns": inspect_parquet_columns,
        },
        {
            "storage_name": "feather",
            "format_name": GOLONDRINA_FEATHER_FORMAT,
            "suffix": ".feather",
            "flows_name_hint": "flows.feather",
            "flow_to_trips_name_hint": "flow_to_trips.feather",
            "inspect_columns": inspect_feather_columns,
        },
    ]

    dataset_nodes: list[dict[str, Any]] = []

    for backend in backend_specs:
        candidate_files = sorted(
            [p for p in dir_path.iterdir() if p.is_file() and p.suffix.lower() == backend["suffix"]]
        )

        valid_flow_files: list[Path] = []
        auxiliary_flow_to_trips: list[Path] = []

        for candidate_path in candidate_files:
            columns = backend["inspect_columns"](candidate_path)
            if not columns:
                continue

            normalized = normalize_columns(columns)

            if GOLONDRINA_FLOWS_REQUIRED.issubset(normalized):
                valid_flow_files.append(candidate_path)
                continue

            if FLOW_TO_TRIPS_SIGNATURE.issubset(normalized):
                auxiliary_flow_to_trips.append(candidate_path)

        attach_shared_aux_files = len(valid_flow_files) == 1

        for flows_file in valid_flow_files:
            relative_dir = dir_path.relative_to(config.data_root)

            dataset_id = make_dataset_id(
                backend["storage_name"],
                *relative_dir.parts,
                flows_file.stem,
            )

            label = dir_path.name if flows_file.name == backend["flows_name_hint"] else flows_file.stem

            files: dict[str, str] = {
                "flows": flows_file.name,
            }

            metadata_path = dir_path / "flows.metadata.json"
            if metadata_path.is_file():
                files["metadata"] = metadata_path.name

            if attach_shared_aux_files:
                preferred_aux_path = dir_path / backend["flow_to_trips_name_hint"]
                if preferred_aux_path.is_file():
                    files["flow_to_trips"] = preferred_aux_path.name

            dataset_node = build_dataset_node(
                dataset_id=dataset_id,
                label=label,
                format_name=backend["format_name"],
                dataset_path=dir_path,
                files=files,
                repo_root=config.repo_root,
            )

            dataset_nodes.append(dataset_node)

        if auxiliary_flow_to_trips and not valid_flow_files:
            log(
                f"[{backend['storage_name']}] flow_to_trips detectado sin flows principal en: {dir_path}",
                verbose=config.verbose,
            )

    return dataset_nodes


def scan_directory(dir_path: Path, depth: int, config: ScanConfig) -> dict[str, Any] | None:
    """
    Recorre recursivamente una carpeta y construye un nodo jerárquico.

    Solo conserva:
    - subdirectorios que contengan datasets válidos o hijos útiles
    - datasets válidos detectados en el nivel actual
    """
    if depth > config.max_depth:
        return None

    child_directories: list[dict[str, Any]] = []
    datasets: list[dict[str, Any]] = []

    # Primero subdirectorios
    for subdir in sorted([p for p in dir_path.iterdir() if p.is_dir()], key=lambda p: p.name.lower()):
        child_node = scan_directory(subdir, depth + 1, config)
        if child_node is not None:
            child_directories.append(child_node)

    # Luego datasets del nivel actual
    flowmap_dataset = detect_flowmap_layout_dataset(dir_path, config)
    if flowmap_dataset is not None:
        datasets.append(flowmap_dataset)

    datasets.extend(detect_golondrina_flows_datasets(dir_path, config))
    datasets.sort(key=lambda item: item["label"].lower())

    children = child_directories + datasets

    # La raíz siempre se conserva, aunque esté vacía
    if dir_path != config.data_root and not children:
        return None

    return {
        "type": "directory",
        "name": dir_path.name,
        "path": to_web_path(dir_path, config.repo_root),
        "children": children,
    }


def write_registry_json(registry: dict[str, Any], output_path: Path) -> None:
    """Escribe el JSON final del registry con indentación legible."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(registry, f, ensure_ascii=False, indent=2)
        f.write("\n")


def parse_args() -> argparse.Namespace:
    """Parsea argumentos CLI del generador."""
    script_path = Path(__file__).resolve()
    repo_root_default = script_path.parents[1]
    data_root_default = repo_root_default / "data" / "flows"
    output_default = data_root_default / "viewer_registry.json"

    parser = argparse.ArgumentParser(
        description="Genera viewer_registry.json a partir de datasets válidos bajo data/flows/."
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=repo_root_default,
        help="Raíz del repo (default: carpeta padre de scripts/).",
    )
    parser.add_argument(
        "--data-root",
        type=Path,
        default=data_root_default,
        help="Directorio raíz visible para el viewer (default: data/flows/).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=output_default,
        help="Ruta de salida del viewer_registry.json.",
    )
    parser.add_argument(
        "--max-depth",
        type=int,
        default=DEFAULT_MAX_DEPTH,
        help="Profundidad máxima de escaneo recursivo.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Imprime mensajes de diagnóstico durante el escaneo.",
    )
    return parser.parse_args()


def main() -> int:
    """Punto de entrada del script."""
    args = parse_args()

    if pq is None or feather is None:
        print(
            "ERROR: falta la dependencia 'pyarrow'.\n"
            "Instálala en tu entorno Python para poder detectar datasets Golondrina "
            "en Parquet y Feather v2.\n"
            "Ejemplo:\n"
            "  pip install pyarrow\n"
            "o con conda:\n"
            "  conda install pyarrow",
            file=sys.stderr,
        )
        return 1

    repo_root = args.repo_root.resolve()
    data_root = args.data_root.resolve()
    output_path = args.output.resolve()

    if args.max_depth < 0:
        print("ERROR: --max-depth debe ser >= 0", file=sys.stderr)
        return 1

    if not data_root.exists() or not data_root.is_dir():
        print(f"ERROR: data_root no existe o no es directorio: {data_root}", file=sys.stderr)
        return 1

    try:
        root_node = scan_directory(
            data_root,
            depth=0,
            config=ScanConfig(
                repo_root=repo_root,
                data_root=data_root,
                output_path=output_path,
                max_depth=args.max_depth,
                verbose=args.verbose,
            ),
        )

        if root_node is None:
            root_node = {
                "type": "directory",
                "name": data_root.name,
                "path": to_web_path(data_root, repo_root),
                "children": [],
            }

        registry = {
            "version": "1.0",
            "generated_at_utc": iso_utc_now(),
            "root_label": data_root.name,
            "root_path": to_web_path(data_root, repo_root),
            "max_scan_depth": args.max_depth,
            "root": root_node,
        }

        write_registry_json(registry, output_path)

        print(f"viewer_registry.json generado en: {output_path}")
        return 0

    except Exception as exc:
        print(f"ERROR al generar viewer_registry.json: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())