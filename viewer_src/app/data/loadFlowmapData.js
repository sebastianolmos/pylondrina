import {
  DATASET_DIR_NAME,
  FLOW_EXPORTS_BASE_PATH,
  FLOW_REQUIRED_COLUMNS,
} from "../config.js";
import { csv } from "d3-fetch";

/** Construye la ruta final de un archivo del dataset activo. */
export function datasetFile(fileName) {
  const datasetBase = FLOW_EXPORTS_BASE_PATH
    ? `${FLOW_EXPORTS_BASE_PATH}/${DATASET_DIR_NAME}`
    : `/${DATASET_DIR_NAME}`;

  return `${datasetBase}/${fileName}`;
}

/** Carga locations.csv y flows.csv, preservando columnas extra para detectar segmentación. */
export async function fetchFlowmapData() {
  const [locationRows, flowRows] = await Promise.all([
    csv(datasetFile("locations.csv")),
    csv(datasetFile("flows.csv")),
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
