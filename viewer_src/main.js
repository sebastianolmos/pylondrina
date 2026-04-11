/**
 * Viewer web de Pylondrina para visualizar flujos OD en Flowmap layout.
 *
 * Responsabilidades principales:
 * - Cargar locations.csv y flows.csv.
 * - Inicializar mapa base y FlowmapLayer.
 * - Renderizar overlays/paneles de apoyo.
 * - Exponer controles visuales mediante lil-gui.
 * - Detectar datasets segmentados y advertir al usuario.
 *
 * Alcance actual:
 * - Visualización de flujos no segmentados en layout Flowmap.
 * - Modo foco por nodo.
 * - Paneles informativos y tooltips.
 *
 * Fuera de alcance actual:
 * - Lectura nativa de flows.golondrina (parquet + metadata).
 * - Selector general de datasets.
 * - Soporte completo de segmentaciones analíticas.
 */

import {Deck} from "@deck.gl/core";
import {FlowmapLayer, PickingType} from "@flowmap.gl/layers";
import {getViewStateForLocations} from "@flowmap.gl/data";
import {csv} from "d3-fetch";
import maplibregl from "maplibre-gl";
import GUI from "lil-gui";

// Estilos base disponibles para el mapa de fondo.
const MAP_STYLES = {
  dark: "https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json",
  light: "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
};

// Ruta base de datasets exportados a layout Flowmap.
// Se inyecta desde Vite para diferenciar dev y build.
const FLOW_EXPORTS_BASE_PATH = __FLOW_EXPORTS_BASE_PATH__;

// Nombre del dataset que se cargará por defecto.
const DATASET_DIR_NAME = "demo_trip_to_flow_happy_path_v1";

/** Construye la ruta final de un archivo del dataset activo. */
function datasetFile(fileName) {
  const datasetBase = FLOW_EXPORTS_BASE_PATH
    ? `${FLOW_EXPORTS_BASE_PATH}/${DATASET_DIR_NAME}`
    : `/${DATASET_DIR_NAME}`;

  return `${datasetBase}/${fileName}`;
}

// Paletas de color disponibles para FlowmapLayer.
const COLOR_SCHEMES = [
  "Blues",
  "BluGrn",
  "BluYl",
  "BrwnYl",
  "BuGn",
  "BuPu",
  "Burg",
  "BurgYl",
  "Cool",
  "DarkMint",
  "Emrld",
  "GnBu",
  "Grayish",
  "Greens",
  "Greys",
  "Inferno",
  "Magenta",
];

// Texto de ayuda asociado a cada control del panel lil-gui.
const PARAM_HELP = {
  darkMode:
    "Cambia la estética interna del flowmap y además, en este viewer, cambia entre mapa base oscuro y claro.",
  baseMapOpacity:
    "Controla cuánta presencia visual tiene el mapa base. En 0 casi desaparece; en 1 se ve completamente.",
  colorScheme:
    "Paleta de colores usada por los flujos y nodos del flowmap.",
  highlightColor:
    "Color con que se resalta el flujo o nodo bajo hover.",
  opacity:
    "Opacidad global del flowmap.",
  fadeAmount:
    "Intensidad del desvanecimiento aplicado a elementos no destacados.",
  animationEnabled:
    "Activa o desactiva la animación visual de los flujos.",
  locationsEnabled:
    "Muestra u oculta locations. En este viewer, al apagarlo también se apagan totals y labels para que el efecto sea claro.",
  locationLabelsEnabled:
    "Muestra etiquetas de texto para las locations.",
  clusteringEnabled:
    "Agrupa locations cercanas y re-agrega flujos para mejorar legibilidad.",
  clusteringAuto:
    "Si está activo, el nivel de clustering se ajusta automáticamente.",
  clusteringLevel:
    "Nivel manual de clustering. Solo aplica si clustering está activo y clusteringAuto está apagado.",
  adaptiveScalesEnabled:
    "Permite que la capa adapte escalas visuales para mantener legibilidad en distintos niveles de zoom/agregación.",
  maxTopFlowsDisplayNum:
    "Tope de flujos principales que se renderizan. Sirve para legibilidad y rendimiento.",
};

// Referencias a elementos fijos del DOM ya presentes en index.html.
const tooltipEl = document.getElementById("tooltip");
const mapEl = document.getElementById("map");
const deckCanvasEl = document.getElementById("deck-canvas");

let controlHelpTooltipEl = null;

// Estado de configuración visual editable desde el panel de controles.
const config = {
  darkMode: true,
  baseMapOpacity: 0.75,
  colorScheme: "Blues",
  highlightColor: "#ff9b29",

  opacity: 0.6,
  //fadeEnabled: true,
  //fadeOpacityEnabled: true,
  fadeAmount: 45,
  animationEnabled: false,

  locationsEnabled: true,
  locationLabelsEnabled: false,

  clusteringEnabled: false,
  clusteringAuto: true,
  clusteringLevel: 1,

  adaptiveScalesEnabled: true,
  maxTopFlowsDisplayNum: 5000,
};

// Estado global mínimo del viewer en tiempo de ejecución.
let map;
let deck;
let flowmapData;
let currentMapStyleKey = null;
let clusteringLevelController;

// Estado del modo foco por nodo.
let selectedLocation = null;
let focusModeBannerEl = null;

// Estado del bootstrap del viewer y del warning por segmentación.
let viewerInitialized = false;
let segmentedWarningAccepted = false;

// Referencias a overlays creados dinámicamente.
let datasetInfoPanelEl = null;
let datasetInfoPanelBodyEl = null;
let segmentedWarningOverlayEl = null;

// Columnas mínimas esperadas para considerar flows.csv como layout Flowmap puro.
const FLOW_REQUIRED_COLUMNS = ["origin", "dest", "count"];

// Textos del panel informativo superior izquierdo.
const INFO_PANEL_TITLE = "Visualizador de Pylondrina";
const INFO_PANEL_DESCRIPTION =
  "Visualizador de flujos no segmentados para Pylondrina. Actualmente consume datos en formato Flowmap layout y queda preparado para evolucionar hacia soporte directo del formato Golondrina.";

/** Carga locations.csv y flows.csv, preservando columnas extra para detectar segmentación. */
async function fetchData() {
  const [locationRows, flowRows] = await Promise.all([
    csv(datasetFile("locations.csv")),
    csv(datasetFile("flows.csv")),
  ]);

  const locationColumns =
    locationRows.columns ?? Object.keys(locationRows[0] ?? {});
  const flowColumns =
    flowRows.columns ?? Object.keys(flowRows[0] ?? {});

  const extraFlowColumns = flowColumns.filter(
    (col) => !FLOW_REQUIRED_COLUMNS.includes(col)
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

/** Oculta el tooltip principal del mapa. */
function hideTooltip() {
  tooltipEl.style.display = 'none';
  tooltipEl.innerHTML = '';
}

/** Muestra el tooltip principal del mapa en una posición de pantalla. */
function showTooltip(x, y, html) {
  tooltipEl.style.left = `${x}px`;
  tooltipEl.style.top = `${y}px`;
  tooltipEl.innerHTML = html;
  tooltipEl.style.display = 'block';
}

/** Genera el HTML del tooltip para hover sobre un flujo o una location. */
function getTooltipHTML(info) {
  if (!info || !info.object) return null;

  const { object } = info;

  switch (object.type) {
    case PickingType.LOCATION:
      return `
        <div class="tooltip-title">${object.name ?? object.id}</div>
        <div class="tooltip-row">Incoming trips: <span class="tooltip-value">${object.totals?.incomingCount ?? 0}</span></div>
        <div class="tooltip-row">Outgoing trips: <span class="tooltip-value">${object.totals?.outgoingCount ?? 0}</span></div>
        <div class="tooltip-row">Internal/round trips: <span class="tooltip-value">${object.totals?.internalCount ?? 0}</span></div>
      `;

    case PickingType.FLOW:
      return `
        <div class="tooltip-title">${object.origin?.id ?? '-'} → ${object.dest?.id ?? '-'}</div>
        <div class="tooltip-row">Trips: <span class="tooltip-value">${object.count ?? 0}</span></div>
      `;

    default:
      return null;
  }
}

/** Maneja el hover de FlowmapLayer y actualiza el tooltip principal. */
function handleHover(info) {
  const html = getTooltipHTML(info);

  if (!html) {
    hideTooltip();
    return;
  }

  showTooltip(info.x, info.y, html);
}

/** Crea, si no existe, el banner superior del modo foco por nodo. */
function ensureFocusModeBanner() {
  if (focusModeBannerEl) return focusModeBannerEl;

  focusModeBannerEl = document.createElement("div");
  focusModeBannerEl.id = "focus-mode-banner";
  focusModeBannerEl.style.position = "fixed";
  focusModeBannerEl.style.top = "12px";
  focusModeBannerEl.style.left = "50%";
  focusModeBannerEl.style.transform = "translateX(-50%)";
  focusModeBannerEl.style.zIndex = "45";
  focusModeBannerEl.style.display = "none";
  focusModeBannerEl.style.padding = "10px 14px";
  focusModeBannerEl.style.borderRadius = "8px";
  focusModeBannerEl.style.background = "rgba(20, 20, 20, 0.92)";
  focusModeBannerEl.style.color = "#fff";
  focusModeBannerEl.style.boxShadow = "0 6px 18px rgba(0, 0, 0, 0.35)";
  focusModeBannerEl.style.fontSize = "13px";
  focusModeBannerEl.style.lineHeight = "1.35";
  focusModeBannerEl.style.pointerEvents = "none";
  focusModeBannerEl.style.textAlign = "center";
  focusModeBannerEl.style.maxWidth = "420px";

  document.body.appendChild(focusModeBannerEl);
  return focusModeBannerEl;
}

/** Actualiza el contenido y visibilidad del banner del modo foco por nodo. */
function updateFocusModeBanner() {
  const el = ensureFocusModeBanner();

  if (!selectedLocation) {
    el.style.display = "none";
    el.innerHTML = "";
    return;
  }

  el.innerHTML = `
    Mostrando solo flujos y locations relacionados a
    <strong>${selectedLocation.name}</strong>
    <span style="opacity:0.8">(${selectedLocation.id})</span>
  `;
  el.style.display = "block";
}

/** Retorna los datos visibles del mapa, aplicando el filtro por nodo seleccionado cuando existe. */
function getFilteredFlowmapData() {
  if (!flowmapData || !selectedLocation) {
    return flowmapData;
  }

  const selectedId = selectedLocation.id;
  const filteredFlows = flowmapData.flows.filter(
    (flow) => flow.origin === selectedId || flow.dest === selectedId
  );

  const relatedLocationIds = new Set([selectedId]);
  filteredFlows.forEach((flow) => {
    relatedLocationIds.add(flow.origin);
    relatedLocationIds.add(flow.dest);
  });

  const filteredLocations = flowmapData.locations.filter((loc) =>
    relatedLocationIds.has(loc.id)
  );

  return {
    locations: filteredLocations,
    flows: filteredFlows,
  };
}

/** Activa, cambia o desactiva el modo foco al hacer click en una location. */
function handleLocationClick(info) {
  if (!info?.object || info.object.type !== PickingType.LOCATION) return;

  if (config.clusteringEnabled) {
    console.warn(
      "El modo foco por location está implementado solo para clustering desactivado."
    );
    return;
  }

  const clickedId = info.object.id;
  const clickedName = info.object.name ?? clickedId;

  if (selectedLocation?.id === clickedId) {
    selectedLocation = null;
  } else {
    selectedLocation = {
      id: clickedId,
      name: clickedName,
    };
  }

  hideTooltip();
  updateLayers();
}

/** Escapa texto para inyectarlo de forma segura en HTML. */
function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

/** Obtiene la métrica de viajes de un flujo, priorizando flow_count y usando count como fallback. */
function getTripMetric(flow) {
  const explicitFlowCount = Number(flow.flow_count);
  if (Number.isFinite(explicitFlowCount)) return explicitFlowCount;

  const count = Number(flow.count);
  return Number.isFinite(count) ? count : 0;
}

/** Formatea un valor numérico como entero redondeado y localizado para la UI. */
function formatRoundedInt(value) {
  return Math.round(value).toLocaleString("es-CL");
}

/** Calcula el resumen global del dataset cargado para el panel informativo. */
function getDatasetSummary() {
  if (!flowmapData) {
    return {
      totalTrips: 0,
      totalFlows: 0,
    };
  }

  const totalTrips = flowmapData.flows.reduce(
    (acc, flow) => acc + getTripMetric(flow),
    0
  );

  return {
    totalTrips,
    totalFlows: flowmapData.flows.length,
  };
}

/** Calcula el resumen del nodo seleccionado en modo foco. */
function getSelectedLocationSummary() {
  if (!flowmapData || !selectedLocation) return null;

  const selectedId = selectedLocation.id;
  const relatedFlows = flowmapData.flows.filter(
    (flow) => flow.origin === selectedId || flow.dest === selectedId
  );

  const outgoingFlows = relatedFlows.filter((flow) => flow.origin === selectedId);
  const incomingFlows = relatedFlows.filter((flow) => flow.dest === selectedId);

  const totalTrips = relatedFlows.reduce(
    (acc, flow) => acc + getTripMetric(flow),
    0
  );

  const outgoingTrips = outgoingFlows.reduce(
    (acc, flow) => acc + getTripMetric(flow),
    0
  );

  const incomingTrips = incomingFlows.reduce(
    (acc, flow) => acc + getTripMetric(flow),
    0
  );

  return {
    id: selectedLocation.id,
    name: selectedLocation.name ?? selectedLocation.id,
    totalTrips,
    totalFlows: relatedFlows.length,
    incomingTrips,
    outgoingTrips,
    incomingFlows: incomingFlows.length,
    outgoingFlows: outgoingFlows.length,
  };
}

/** Crea, si no existe, el panel informativo superior izquierdo del viewer. */
function ensureDatasetInfoPanel() {
  if (datasetInfoPanelEl) return datasetInfoPanelEl;

  datasetInfoPanelEl = document.createElement("div");
  datasetInfoPanelEl.id = "dataset-info-panel";

  Object.assign(datasetInfoPanelEl.style, {
    position: "fixed",
    top: "12px",
    left: "12px",
    zIndex: "36",
    width: "330px",
    borderRadius: "8px",
    overflow: "hidden",
    background: "rgba(31, 31, 31, 0.92)",
    color: "#fff",
    boxShadow: "0 6px 18px rgba(0, 0, 0, 0.35)",
    fontFamily: "Arial, sans-serif",
    fontSize: "13px",
    lineHeight: "1.35",
  });

  const headerEl = document.createElement("div");
  Object.assign(headerEl.style, {
    display: "flex",
    alignItems: "center",
    justifyContent: "space-between",
    padding: "8px 10px",
    background: "rgba(20, 20, 20, 0.92)",
    borderBottom: "1px solid rgba(255, 255, 255, 0.12)",
  });

  const titleEl = document.createElement("div");
  titleEl.textContent = INFO_PANEL_TITLE;
  Object.assign(titleEl.style, {
    fontWeight: "700",
    fontSize: "13px",
  });

  const toggleButtonEl = document.createElement("button");
  toggleButtonEl.textContent = "-";
  Object.assign(toggleButtonEl.style, {
    border: "none",
    background: "transparent",
    color: "#fff",
    cursor: "pointer",
    fontSize: "16px",
    lineHeight: "1",
    padding: "0 4px",
  });

  datasetInfoPanelBodyEl = document.createElement("div");
  Object.assign(datasetInfoPanelBodyEl.style, {
    padding: "10px 12px",
  });

  toggleButtonEl.addEventListener("click", () => {
    const collapsed = datasetInfoPanelBodyEl.style.display === "none";
    datasetInfoPanelBodyEl.style.display = collapsed ? "block" : "none";
    toggleButtonEl.textContent = collapsed ? "-" : "+";
  });

  headerEl.appendChild(titleEl);
  headerEl.appendChild(toggleButtonEl);

  datasetInfoPanelEl.appendChild(headerEl);
  datasetInfoPanelEl.appendChild(datasetInfoPanelBodyEl);

  document.body.appendChild(datasetInfoPanelEl);
  return datasetInfoPanelEl;
}

/** Actualiza el panel informativo principal según dataset, nodo seleccionado y warnings activos. */
function updateDatasetInfoPanel() {
  if (!flowmapData) return;
  ensureDatasetInfoPanel();

  const summary = getDatasetSummary();
  const selectedSummary = getSelectedLocationSummary();

  const selectedSectionHtml = selectedSummary
    ? `
      <div style="margin-top: 10px; padding-top: 10px; border-top: 1px solid rgba(255,255,255,0.12);">
        <div style="font-weight: 700; margin-bottom: 6px;">
          Nodo: ${escapeHtml(selectedSummary.name)}
          <span style="opacity: 0.8;">(${escapeHtml(selectedSummary.id)})</span>
        </div>

        <div><strong>Viajes relacionados:</strong> ${formatRoundedInt(selectedSummary.totalTrips)}</div>
        <div style="margin-top: 2px;"><strong>Flujos relacionados:</strong> ${formatRoundedInt(selectedSummary.totalFlows)}</div>

        <div style="margin-top: 8px;"><strong>Viajes de entrada:</strong> ${formatRoundedInt(selectedSummary.incomingTrips)}</div>
        <div style="margin-top: 2px;"><strong>Viajes de salida:</strong> ${formatRoundedInt(selectedSummary.outgoingTrips)}</div>
        <div style="margin-top: 2px;"><strong>Flujos de entrada:</strong> ${formatRoundedInt(selectedSummary.incomingFlows)}</div>
        <div style="margin-top: 2px;"><strong>Flujos de salida:</strong> ${formatRoundedInt(selectedSummary.outgoingFlows)}</div>
      </div>
    `
    : "";

  const segmentedWarningHtml =
    flowmapData.hasSegmentedFlows && segmentedWarningAccepted
      ? `
        <div style="margin-top: 10px; padding-top: 10px; border-top: 1px solid rgba(255,255,255,0.12); color: #ffd28c;">
          <strong>Advertencia:</strong> se está visualizando un dataset con flujos segmentados no soportados por el viewer actual. Los flujos pueden verse solapados o interpretarse de forma engañosa.
        </div>
      `
      : "";

  datasetInfoPanelBodyEl.innerHTML = `
    <div style="opacity: 0.92; margin-bottom: 10px;">
      ${escapeHtml(INFO_PANEL_DESCRIPTION)}
    </div>

    <div><strong>Viajes totales:</strong> ${formatRoundedInt(summary.totalTrips)}</div>
    <div style="margin-top: 4px;"><strong>Flujos totales:</strong> ${formatRoundedInt(summary.totalFlows)}</div>

    ${selectedSectionHtml}
    ${segmentedWarningHtml}
  `;
}

/** Crea, si no existe, la pantalla de advertencia para datasets segmentados. */
function ensureSegmentedWarningScreen() {
  if (segmentedWarningOverlayEl) return segmentedWarningOverlayEl;

  segmentedWarningOverlayEl = document.createElement("div");
  segmentedWarningOverlayEl.id = "segmented-warning-overlay";

  Object.assign(segmentedWarningOverlayEl.style, {
    position: "fixed",
    inset: "0",
    zIndex: "60",
    display: "none",
    alignItems: "center",
    justifyContent: "center",
    background: "#292929",
    padding: "24px",
    boxSizing: "border-box",
  });

  const panelEl = document.createElement("div");
  panelEl.id = "segmented-warning-panel";

  Object.assign(panelEl.style, {
    width: "min(640px, 100%)",
    padding: "22px 24px",
    borderRadius: "10px",
    background: "rgba(20, 20, 20, 0.98)",
    color: "#fff",
    boxShadow: "0 10px 24px rgba(0, 0, 0, 0.28)",
    fontFamily: "Arial, sans-serif",
    lineHeight: "1.45",
  });

  const titleEl = document.createElement("div");
  titleEl.textContent = "Dataset segmentado detectado";
  Object.assign(titleEl.style, {
    fontSize: "20px",
    fontWeight: "700",
    marginBottom: "12px",
  });

  const messageEl = document.createElement("div");
  messageEl.id = "segmented-warning-message";
  Object.assign(messageEl.style, {
    fontSize: "14px",
    marginBottom: "16px",
  });

  const buttonEl = document.createElement("button");
  buttonEl.textContent = "Continuar de todas maneras";
  Object.assign(buttonEl.style, {
    border: "none",
    borderRadius: "8px",
    padding: "10px 14px",
    cursor: "pointer",
    background: "#f0f0f0",
    color: "#222",
    fontWeight: "700",
    fontSize: "14px",
  });

  buttonEl.addEventListener("click", () => {
    segmentedWarningAccepted = true;
    segmentedWarningOverlayEl.style.display = "none";
    startViewer();
  });

  panelEl.appendChild(titleEl);
  panelEl.appendChild(messageEl);
  panelEl.appendChild(buttonEl);

  segmentedWarningOverlayEl.appendChild(panelEl);
  document.body.appendChild(segmentedWarningOverlayEl);

  return segmentedWarningOverlayEl;
}

/** Muestra la advertencia previa cuando se detectan columnas extra en flows.csv. */
function showSegmentedWarningScreen() {
  const overlayEl = ensureSegmentedWarningScreen();
  const messageEl = overlayEl.querySelector("#segmented-warning-message");

  const extraColumnsText = flowmapData.extraFlowColumns
    .map((col) => escapeHtml(col))
    .join(", ");

  messageEl.innerHTML = `
    <div style="margin-bottom: 10px;">
      Se detectaron campos extra en <code>flows.csv</code>, por lo que se asume que el dataset contiene <strong>flujos segmentados</strong>.
    </div>

    <div style="margin-bottom: 10px;">
      El visualizador actual no soporta este modo. Si continúas, los flujos segmentados se renderizarán igualmente y pueden verse <strong>solapados</strong> o no interpretarse correctamente.
    </div>

    <div style="opacity: 0.9;">
      <strong>Campos detectados:</strong> ${extraColumnsText || "sin detalle"}
    </div>
  `;

  overlayEl.style.display = "flex";
}

/** Construye la instancia de FlowmapLayer con los datos y controles visuales vigentes. */
function buildLayer() {
  const effectiveData = getFilteredFlowmapData();
  const {locations, flows} = effectiveData;

  return new FlowmapLayer({
    id: "my-flowmap-layer",
    data: {locations, flows},
    pickable: true,

    ...getLayerVisualProps(),

    getLocationId: (loc) => loc.id,
    getLocationLat: (loc) => loc.lat,
    getLocationLon: (loc) => loc.lon,
    getLocationName: (loc) => loc.name,
    getFlowOriginId: (flow) => flow.origin,
    getFlowDestId: (flow) => flow.dest,
    getFlowMagnitude: (flow) => flow.count,

    onHover: handleHover,
    onClick: (info) => {
      if (info?.object) {
        console.log("clicked", info.object.type, info.object, info);
      }

      handleLocationClick(info);
    },
  });
}

/** Re-renderiza la capa del mapa y sincroniza overlays/controles dependientes del estado actual. */
function updateLayers() {
  if (!deck || !flowmapData) return;

  updateBaseMap();
  syncClusteringControls();
  updateFocusModeBanner();
  updateDatasetInfoPanel();

  deck.setProps({
    layers: [buildLayer()],
  });
}

/** Inicializa el panel de controles visuales basado en lil-gui. */
function initGui() {
  const gui = new GUI({ title: "Flowmap controls" });

  const darkModeController = gui
    .add(config, "darkMode")
    .name("Dark mode")
    .onChange(updateLayers);
  setControllerHelp(darkModeController, PARAM_HELP.darkMode);

  const baseMapOpacityController = gui
    .add(config, "baseMapOpacity", 0, 1, 0.01)
    .name("Base map")
    .onChange(updateLayers);
  setControllerHelp(baseMapOpacityController, PARAM_HELP.baseMapOpacity);

  const colorSchemeController = gui
    .add(config, "colorScheme", COLOR_SCHEMES)
    .name("Color scheme")
    .onChange(updateLayers);
  setControllerHelp(colorSchemeController, PARAM_HELP.colorScheme);

  const highlightColorController = gui
    .addColor(config, "highlightColor")
    .name("Highlight color")
    .onChange(updateLayers);
  setControllerHelp(highlightColorController, PARAM_HELP.highlightColor);

  const opacityController = gui
    .add(config, "opacity", 0, 1, 0.01)
    .name("Opacity")
    .onChange(updateLayers);
  setControllerHelp(opacityController, PARAM_HELP.opacity);

  const fadeAmountController = gui
    .add(config, "fadeAmount", 0, 100, 1)
    .name("Fade amount")
    .onChange(updateLayers);
  setControllerHelp(fadeAmountController, PARAM_HELP.fadeAmount);

  const animationController = gui
    .add(config, "animationEnabled")
    .name("Animation")
    .onChange(updateLayers);
  setControllerHelp(animationController, PARAM_HELP.animationEnabled);

  const locationsController = gui
    .add(config, "locationsEnabled")
    .name("Show locations")
    .onChange(updateLayers);
  setControllerHelp(locationsController, PARAM_HELP.locationsEnabled);

  const locationLabelsController = gui
    .add(config, "locationLabelsEnabled")
    .name("Location labels")
    .onChange(updateLayers);
  setControllerHelp(locationLabelsController, PARAM_HELP.locationLabelsEnabled);

  const clusteringFolder = gui.addFolder("Clustering");

  const clusteringEnabledController = clusteringFolder
    .add(config, "clusteringEnabled")
    .name("Enabled")
    .onChange(updateLayers);
  setControllerHelp(clusteringEnabledController, PARAM_HELP.clusteringEnabled);

  const clusteringAutoController = clusteringFolder
    .add(config, "clusteringAuto")
    .name("Auto")
    .onChange(updateLayers);
  setControllerHelp(clusteringAutoController, PARAM_HELP.clusteringAuto);

  clusteringLevelController = clusteringFolder
    .add(config, "clusteringLevel", 1, 12, 1)
    .name("Level")
    .onChange(updateLayers);
  setControllerHelp(clusteringLevelController, PARAM_HELP.clusteringLevel);

  const adaptiveScalesController = gui
    .add(config, "adaptiveScalesEnabled")
    .name("Adaptive scales")
    .onChange(updateLayers);
  setControllerHelp(adaptiveScalesController, PARAM_HELP.adaptiveScalesEnabled);

  const maxTopFlowsController = gui
    .add(config, "maxTopFlowsDisplayNum", 100, 10000, 100)
    .name("Max top flows")
    .onChange(updateLayers);
  setControllerHelp(maxTopFlowsController, PARAM_HELP.maxTopFlowsDisplayNum);

  syncClusteringControls();
  return gui;
}

/** Elimina propiedades undefined antes de pasarlas a FlowmapLayer. */
function compactProps(obj) {
  return Object.fromEntries(
    Object.entries(obj).filter(([, value]) => value !== undefined)
  );
}

/** Crea, si no existe, el tooltip de ayuda contextual para controles del panel. */
function ensureControlHelpTooltip() {
  if (controlHelpTooltipEl) return controlHelpTooltipEl;

  controlHelpTooltipEl = document.createElement("div");
  controlHelpTooltipEl.id = "control-help-tooltip";
  controlHelpTooltipEl.style.position = "fixed";
  controlHelpTooltipEl.style.zIndex = "50";
  controlHelpTooltipEl.style.pointerEvents = "none";
  controlHelpTooltipEl.style.display = "none";
  controlHelpTooltipEl.style.maxWidth = "280px";
  controlHelpTooltipEl.style.padding = "10px 12px";
  controlHelpTooltipEl.style.borderRadius = "8px";
  controlHelpTooltipEl.style.background = "rgba(20, 20, 20, 0.92)";
  controlHelpTooltipEl.style.color = "#fff";
  controlHelpTooltipEl.style.boxShadow = "0 6px 18px rgba(0, 0, 0, 0.35)";
  controlHelpTooltipEl.style.fontSize = "13px";
  controlHelpTooltipEl.style.lineHeight = "1.35";

  document.body.appendChild(controlHelpTooltipEl);
  return controlHelpTooltipEl;
}

/** Oculta el tooltip de ayuda contextual del panel de controles. */
function hideControlHelpTooltip() {
  const el = ensureControlHelpTooltip();
  el.style.display = "none";
  el.textContent = "";
}

/** Muestra el tooltip de ayuda contextual junto al cursor. */
function showControlHelpTooltip(x, y, text) {
  const el = ensureControlHelpTooltip();
  el.textContent = text;
  el.style.display = "block";

  // Primero lo mostramos, luego medimos su tamaño real
  requestAnimationFrame(() => {
    const rect = el.getBoundingClientRect();
    const margin = 12;

    // Lo dejamos a la izquierda del cursor
    let left = x - rect.width - margin;

    // Centrado vertical respecto al cursor
    let top = y - rect.height / 2;

    // Clamp para que no se salga de la pantalla
    left = Math.max(12, left);
    top = Math.max(12, Math.min(window.innerHeight - rect.height - 12, top));

    el.style.left = `${left}px`;
    el.style.top = `${top}px`;
  });
}

/** Vincula un texto de ayuda a una fila del panel de controles. */
function setControllerHelp(controller, text) {
  const rowEl = controller?.domElement;
  if (!rowEl || !text) return;

  const nameEl = rowEl.querySelector(".name");
  if (nameEl) {
    nameEl.style.cursor = "help";
  }

  const handleMouseEnter = (event) => {
    showControlHelpTooltip(event.clientX, event.clientY, text);
  };

  const handleMouseMove = (event) => {
    showControlHelpTooltip(event.clientX, event.clientY, text);
  };

  const handleMouseLeave = () => {
    hideControlHelpTooltip();
  };

  const handleFocusIn = () => {
    const rect = rowEl.getBoundingClientRect();
    showControlHelpTooltip(rect.left, rect.top + rect.height / 2, text);
  };

  const handleFocusOut = () => {
    hideControlHelpTooltip();
  };

  rowEl.addEventListener("mouseenter", handleMouseEnter);
  rowEl.addEventListener("mousemove", handleMouseMove);
  rowEl.addEventListener("mouseleave", handleMouseLeave);
  rowEl.addEventListener("focusin", handleFocusIn);
  rowEl.addEventListener("focusout", handleFocusOut);
}

/** Habilita o deshabilita visualmente el control de nivel de clustering según el estado actual. */
function syncClusteringControls() {
  if (!clusteringLevelController) return;

  const disabled = !config.clusteringEnabled || config.clusteringAuto;
  clusteringLevelController.domElement.style.opacity = disabled ? "0.45" : "1";
  clusteringLevelController.domElement.style.pointerEvents = disabled ? "none" : "auto";
}

/** Actualiza estilo y opacidad del mapa base según la configuración visual vigente. */
function updateBaseMap(force = false) {
  const styleKey = config.darkMode ? "dark" : "light";

  if (map && currentMapStyleKey !== styleKey) {
    currentMapStyleKey = styleKey;
    map.setStyle(MAP_STYLES[styleKey]);
  }

  if (mapEl) {
    mapEl.style.opacity = String(config.baseMapOpacity);
  }

  if (deckCanvasEl) {
    deckCanvasEl.style.mixBlendMode = config.darkMode ? "screen" : "multiply";
  }
}

/** Traduce la configuración del viewer al conjunto de props que recibirá FlowmapLayer. */
function getLayerVisualProps() {
  const effectiveLocationsEnabled = config.locationsEnabled;

  const effectiveLocationLabelsEnabled =
    effectiveLocationsEnabled === false
      ? false
      : config.locationLabelsEnabled;

  return compactProps({
    darkMode: config.darkMode,
    colorScheme: config.colorScheme,
    highlightColor: config.highlightColor,
    opacity: config.opacity,
    fadeAmount: config.fadeAmount,
    animationEnabled: config.animationEnabled,

    locationsEnabled: effectiveLocationsEnabled,
    locationTotalsEnabled: effectiveLocationsEnabled ? true : false,
    locationLabelsEnabled: effectiveLocationLabelsEnabled,

    clusteringEnabled: config.clusteringEnabled,
    clusteringAuto: config.clusteringEnabled ? config.clusteringAuto : undefined,
    clusteringLevel:
      config.clusteringEnabled && !config.clusteringAuto
        ? config.clusteringLevel
        : undefined,
    adaptiveScalesEnabled: config.adaptiveScalesEnabled,
    maxTopFlowsDisplayNum: config.maxTopFlowsDisplayNum,
  });
}

/** Inicializa mapa, Deck, paneles y controles una vez que los datos ya fueron cargados. */
function startViewer() {
  if (viewerInitialized || !flowmapData) {
    if (viewerInitialized) {
      updateDatasetInfoPanel();
      updateLayers();
    }
    return;
  }

  const { locations } = flowmapData;
  const [width, height] = [globalThis.innerWidth, globalThis.innerHeight];

  const initialViewState = getViewStateForLocations(
    locations,
    (loc) => [loc.lon, loc.lat],
    [width, height],
    { pad: 0.3 }
  );

  const initialStyleKey = config.darkMode ? "dark" : "light";
  currentMapStyleKey = initialStyleKey;

  map = new maplibregl.Map({
    container: "map",
    style: MAP_STYLES[initialStyleKey],
    interactive: false,
    center: [initialViewState.longitude, initialViewState.latitude],
    zoom: initialViewState.zoom,
    bearing: initialViewState.bearing,
    pitch: initialViewState.pitch,
  });

  deck = new Deck({
    canvas: "deck-canvas",
    width: "100%",
    height: "100%",
    initialViewState,
    controller: true,
    map: true,
    onViewStateChange: ({ viewState }) => {
      hideTooltip();
      map.jumpTo({
        center: [viewState.longitude, viewState.latitude],
        zoom: viewState.zoom,
        bearing: viewState.bearing,
        pitch: viewState.pitch,
      });
    },
    layers: [],
  });

  if (mapEl) {
    mapEl.style.opacity = String(config.baseMapOpacity);
  }
  if (deckCanvasEl) {
    deckCanvasEl.style.mixBlendMode = config.darkMode ? "screen" : "multiply";
  }

  ensureDatasetInfoPanel();
  initGui();

  viewerInitialized = true;
  updateDatasetInfoPanel();
  updateLayers();
}

fetchData().then((data) => {
  flowmapData = data;

  if (flowmapData.hasSegmentedFlows) {
    showSegmentedWarningScreen();
    return;
  }

  startViewer();
});

window.addEventListener("resize", () => {
  hideTooltip();
  hideControlHelpTooltip();
});

window.addEventListener("blur", () => {
  hideTooltip();
  hideControlHelpTooltip();
});