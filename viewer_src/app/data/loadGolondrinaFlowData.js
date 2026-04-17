import { tableFromIPC } from "apache-arrow";
import {
  CompressionType,
  setCompressionCodec,
  tableFromIPC as flechetteTableFromIPC,
} from "@uwdata/flechette";
import { cellToLatLng, isValidCell } from "h3-js";
import * as lz4 from "lz4js";
import initParquetWasm, { readParquet } from "parquet-wasm";
import parquetWasmUrl from "parquet-wasm/esm/parquet_wasm_bg.wasm?url";
import { ZstdCodec } from "zstd-codec";

import { GOLONDRINA_FLOW_REQUIRED_COLUMNS } from "../config.js";
import { datasetFileFromRegistry } from "./loadFlowmapData.js";

let parquetRuntimeInitPromise = null;
let flechetteCompressionInitPromise = null;

/** Inicializa una sola vez el runtime WebAssembly necesario para leer Parquet en el navegador. */
async function ensureParquetRuntime() {
  if (!parquetRuntimeInitPromise) {
    parquetRuntimeInitPromise = initParquetWasm(parquetWasmUrl);
  }

  await parquetRuntimeInitPromise;
}

/**
 * Registra una sola vez los codecs de compresión Arrow IPC requeridos para Feather v2.
 *
 * Se registra:
 * - LZ4_FRAME mediante `lz4js`
 * - ZSTD mediante `zstd-codec`
 */
async function ensureFlechetteCompressionCodecs() {
  if (!flechetteCompressionInitPromise) {
    flechetteCompressionInitPromise = (async () => {
      setCompressionCodec(CompressionType.LZ4_FRAME, {
        encode: (data) => lz4.compress(data),
        decode: (data) => lz4.decompress(data),
      });

      await new Promise((resolve) => {
        ZstdCodec.run((zstd) => {
          const codec = new zstd.Simple();
          setCompressionCodec(CompressionType.ZSTD, {
            encode: (data) => codec.compress(data),
            decode: (data) => codec.decompress(data),
          });
          resolve();
        });
      });
    })();
  }

  await flechetteCompressionInitPromise;
}

/** Indica si un nodo del registry representa un dataset Golondrina soportado por el viewer. */
function isGolondrinaDatasetNode(datasetNode) {
  return ["golondrina_parquet", "golondrina_feather"].includes(datasetNode?.format);
}

/** Normaliza valores escalares provenientes de tablas Arrow a tipos JS más manejables. */
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

/** Valida que el archivo Golondrina contenga las columnas mínimas exigidas por el viewer. */
function assertRequiredGolondrinaColumns(columns, fileName) {
  const missingColumns = GOLONDRINA_FLOW_REQUIRED_COLUMNS.filter(
    (column) => !columns.includes(column)
  );

  if (missingColumns.length > 0) {
    throw new Error(
      `El archivo de flujos Golondrina '${fileName}' no contiene las columnas mínimas requeridas: ${missingColumns.join(", ")}.`
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
        throw new Error(`Se encontró un índice H3 inválido en el archivo de flujos: ${h3Index}`);
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

/** Lee un dataset Golondrina almacenado físicamente en Parquet y lo convierte a Arrow Table. */
async function loadGolondrinaParquetTable(datasetNode) {
  await ensureParquetRuntime();

  const parquetUrl = datasetFileFromRegistry(datasetNode, "flows");
  const response = await fetch(parquetUrl, { cache: "no-store" });

  if (!response.ok) {
    throw new Error(
      `No se pudo cargar el archivo Parquet de flujos Golondrina (${response.status} ${response.statusText}).`
    );
  }

  const parquetBytes = new Uint8Array(await response.arrayBuffer());
  const arrowWasmTable = readParquet(parquetBytes);
  return tableFromIPC(arrowWasmTable.intoIPCStream());
}

/**
 * Lee un dataset Golondrina almacenado físicamente en Feather v2.
 *
 * Esta ruta usa Flechette porque permite registrar codecs de compresión
 * Arrow IPC (LZ4_FRAME y ZSTD) requeridos por Feather comprimido.
 */
async function loadGolondrinaFeatherTable(datasetNode) {
  await ensureFlechetteCompressionCodecs();

  const featherUrl = datasetFileFromRegistry(datasetNode, "flows");
  const response = await fetch(featherUrl, { cache: "no-store" });

  if (!response.ok) {
    throw new Error(
      `No se pudo cargar el archivo Feather de flujos Golondrina (${response.status} ${response.statusText}).`
    );
  }

  const featherBytes = new Uint8Array(await response.arrayBuffer());
  return flechetteTableFromIPC(featherBytes);
}

/** Resuelve el backend físico del dataset Golondrina y retorna una Arrow Table uniforme. */
async function loadGolondrinaArrowTable(datasetNode) {
  switch (datasetNode?.format) {
    case "golondrina_parquet":
      return loadGolondrinaParquetTable(datasetNode);
    case "golondrina_feather":
      return loadGolondrinaFeatherTable(datasetNode);
    default:
      throw new Error(`Formato Golondrina no soportado: ${datasetNode?.format ?? "desconocido"}.`);
  }
}

/** Carga un dataset Golondrina y lo adapta a la estructura locations + flows que consume FlowmapLayer. */
export async function fetchGolondrinaFlowData(datasetNode) {
  if (!isGolondrinaDatasetNode(datasetNode)) {
    throw new Error(
      "El loader Golondrina solo soporta datasets golondrina_parquet o golondrina_feather."
    );
  }

  const arrowTable = await loadGolondrinaArrowTable(datasetNode);
  const flowColumns = arrowTable.schema.fields.map((field) => field.name);

  assertRequiredGolondrinaColumns(flowColumns, datasetNode?.files?.flows ?? "flows");

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
