import { tableFromIPC } from "apache-arrow";
import { cellToLatLng, isValidCell } from "h3-js";
import initParquetWasm, { readParquet } from "parquet-wasm";
import parquetWasmUrl from "parquet-wasm/esm/parquet_wasm_bg.wasm?url";

import { GOLONDRINA_FLOW_REQUIRED_COLUMNS } from "../config.js";
import { datasetFileFromRegistry } from "./loadFlowmapData.js";

let parquetRuntimeInitPromise = null;

/** Inicializa una sola vez el runtime WebAssembly necesario para leer Parquet en el navegador. */
async function ensureParquetRuntime() {
  if (!parquetRuntimeInitPromise) {
    parquetRuntimeInitPromise = initParquetWasm(parquetWasmUrl);
  }

  await parquetRuntimeInitPromise;
}

/** Normaliza valores escalares provenientes de Apache Arrow a tipos JS más manejables. */
function normalizeArrowScalar(value) {
  if (value == null) return null;
  if (typeof value === "bigint") return Number(value);
  return value;
}

/** Convierte una Arrow Table a un arreglo de filas JS sin depender de APIs dataframe-like extra. */
function arrowTableToRows(arrowTable) {
  const fieldNames = arrowTable.schema.fields.map((field) => field.name);
  const columnValues = Object.fromEntries(
    fieldNames.map((fieldName) => [
      fieldName,
      arrowTable.getChild(fieldName)?.toArray() ?? [],
    ])
  );

  return Array.from({ length: arrowTable.numRows }, (_, rowIndex) => {
    const row = {};

    fieldNames.forEach((fieldName) => {
      row[fieldName] = normalizeArrowScalar(columnValues[fieldName][rowIndex]);
    });

    return row;
  });
}

/** Valida que el parquet contenga las columnas mínimas exigidas por el viewer para flujos Golondrina. */
function assertRequiredGolondrinaColumns(columns) {
  const missingColumns = GOLONDRINA_FLOW_REQUIRED_COLUMNS.filter(
    (column) => !columns.includes(column)
  );

  if (missingColumns.length > 0) {
    throw new Error(
      `El parquet de flujos Golondrina no contiene las columnas mínimas requeridas: ${missingColumns.join(", ")}.`
    );
  }
}

/** Construye la tabla locations del viewer usando el propio índice H3 como id estable. */
function buildLocationsFromH3Rows(rows) {
  const uniqueH3Indexes = new Set();

  rows.forEach((row) => {
    uniqueH3Indexes.add(String(row.origin_h3_index));
    uniqueH3Indexes.add(String(row.destination_h3_index));
  });

  return Array.from(uniqueH3Indexes)
    .sort()
    .map((h3Index) => {
      if (!isValidCell(h3Index)) {
        throw new Error(`Se encontró un índice H3 inválido en el parquet: ${h3Index}`);
      }

      const [lat, lon] = cellToLatLng(h3Index);

      return {
        id: h3Index,
        name: h3Index,
        lat,
        lon,
      };
    });
}

/** Construye la tabla flows del viewer a partir del contrato interno canónico de FlowDataset.flows. */
function buildViewerFlowsFromGolondrinaRows(rows) {
  return rows.map((row) => {
    const flowValue = Number(row.flow_value);
    const flowCount = Number(row.flow_count);

    if (!Number.isFinite(flowValue)) {
      throw new Error(`flow_value inválido para flow_id='${row.flow_id ?? "?"}'.`);
    }

    if (!Number.isFinite(flowCount)) {
      throw new Error(`flow_count inválido para flow_id='${row.flow_id ?? "?"}'.`);
    }

    return {
      ...row,
      origin: String(row.origin_h3_index),
      dest: String(row.destination_h3_index),
      count: flowValue,
      flow_count: flowCount,
    };
  });
}

/** Carga un parquet Golondrina y lo adapta a la estructura locations + flows que consume FlowmapLayer. */
export async function fetchGolondrinaFlowData(datasetNode) {
  if (!datasetNode || datasetNode.format !== "golondrina_flows") {
    throw new Error("El loader Golondrina solo soporta datasets golondrina_flows.");
  }

  await ensureParquetRuntime();

  const parquetUrl = datasetFileFromRegistry(datasetNode, "flows");
  const response = await fetch(parquetUrl, { cache: "no-store" });

  if (!response.ok) {
    throw new Error(
      `No se pudo cargar el parquet de flujos Golondrina (${response.status} ${response.statusText}).`
    );
  }

    const parquetBytes = new Uint8Array(await response.arrayBuffer());
    const arrowWasmTable = readParquet(parquetBytes);

    const arrowTable = tableFromIPC(arrowWasmTable.intoIPCStream());
    const flowColumns = arrowTable.schema.fields.map((field) => field.name);

    assertRequiredGolondrinaColumns(flowColumns);

    const extraFlowColumns = flowColumns.filter(
    (column) => !GOLONDRINA_FLOW_REQUIRED_COLUMNS.includes(column)
    );

    const rows = arrowTableToRows(arrowTable);
    const locations = buildLocationsFromH3Rows(rows);
    const flows = buildViewerFlowsFromGolondrinaRows(rows);

    return {
        locations,
        flows,
        locationColumns: ["id", "name", "lat", "lon"],
        flowColumns,
        extraFlowColumns,
        hasSegmentedFlows: extraFlowColumns.length > 0,
    };
}
