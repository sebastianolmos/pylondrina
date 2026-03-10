import sys
import importlib

def ok(msg: str) -> None:
    print(f"[OK] {msg}")

def main() -> None:
    import pylondrina
    ok(f"pylondrina importado desde: {pylondrina.__file__}")
    ok(f"python: {sys.executable}")
    ok(f"pylondrina.__version__: {getattr(pylondrina, '__version__', 'N/A')}")

    # Imports estilo “bonito”
    from pylondrina.datasets import TripDataset, FlowDataset, TraceDataset
    ok("from pylondrina.datasets import TripDataset, FlowDataset, TraceDataset")

    from pylondrina.validation import ValidationOptions, validate_trips
    ok("from pylondrina.validation import ValidationOptions, validate_trips")

    from pylondrina.issues.core import IssueSpec, emit_issue
    ok("from pylondrina.issues.core import IssueSpec, emit_issue")

    from pylondrina.sources.helpers import import_trips_from_source
    ok("from pylondrina.sources.helpers import import_trips_from_source")

    from pylondrina.transforms.flows import FlowBuildOptions, build_flows
    ok("from pylondrina.transforms.flows import FlowBuildOptions, build_flows")

    from pylondrina.export.flows import export_flows
    ok("from pylondrina.export.flows import export_flows")

    print("\nTodo bien :D")

if __name__ == "__main__":
    main()