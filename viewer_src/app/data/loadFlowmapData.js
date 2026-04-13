import { FLOW_REQUIRED_COLUMNS } from "../config.js";
import { csv } from "d3-fetch";

/** Construye la URL de un archivo perteneciente a un dataset Flowmap del registry. */
export function datasetFileFromRegistry(datasetNode, fileKey) {
  if (!datasetNode?.dataset_path || !datasetNode?.files?.[fileKey]) {
    throw new Error(`No se pudo resolver el archivo '${fileKey}' del dataset seleccionado.`);
  }

  return `${datasetNode.dataset_path}/${datasetNode.files[fileKey]}`;
}

/** Carga un dataset Flowmap layout desde un nodo dataset ya resuelto por el selector. */
export async function fetchFlowmapData(datasetNode) {
  if (!datasetNode || datasetNode.format !== "flowmap_layout") {
    throw new Error("El loader actual solo soporta datasets flowmap_layout.");
  }

  const [locationRows, flowRows] = await Promise.all([
    csv(datasetFileFromRegistry(datasetNode, "locations")),
    csv(datasetFileFromRegistry(datasetNode, "flows")),
  ]);

  const locationColumns =
    locationRows.columns ?? Object.keys(locationRows[0] ?? {});
  const flowColumns = flowRows.columns ?? Object.keys(flowRows[0] ?? {});

  const extraFlowColumns = flowColumns.filter(
    (column) => !FLOW_REQUIRED_COLUMNS.includes(column)
  );

  const locations = locationRows.map((row) => ({
    ...row,
    id: row.id,
    name: row.name ?? row.id,
    lat: Number(row.lat),
    lon: Number(row.lon),
  }));

  const flows = flowRows.map((row) => ({
    ...row,
    origin: row.origin,
    dest: row.dest,
    count: Number(row.count),
  }));

  return {
    locations,
    flows,
    locationColumns,
    flowColumns,
    extraFlowColumns,
    hasSegmentedFlows: extraFlowColumns.length > 0,
  };
}
