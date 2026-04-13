import { fetchFlowmapData } from "./loadFlowmapData.js";
import { fetchGolondrinaFlowData } from "./loadGolondrinaFlowData.js";

/** Resuelve el loader apropiado según el formato declarado en el nodo dataset del registry. */
export async function fetchDatasetData(datasetNode) {
  switch (datasetNode?.format) {
    case "flowmap_layout":
      return fetchFlowmapData(datasetNode);
    case "golondrina_flows":
      return fetchGolondrinaFlowData(datasetNode);
    default:
      throw new Error(`Formato de dataset no soportado: ${datasetNode?.format ?? "desconocido"}.`);
  }
}
