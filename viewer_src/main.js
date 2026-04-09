import {Deck} from "@deck.gl/core";
import {FlowmapLayer, PickingType} from "@flowmap.gl/layers";
import {getViewStateForLocations} from "@flowmap.gl/data";
import {csv} from "d3-fetch";
import maplibregl from "maplibre-gl";
import GUI from "lil-gui";

const MAP_STYLES = {
  dark: "https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json",
  light: "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
};

const DATA_PATH =
  "https://gist.githubusercontent.com/ilyabo/68d3dba61d86164b940ffe60e9d36931/raw/a72938b5d51b6df9fa7bba9aa1fb7df00cd0f06a";

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

const tooltipEl = document.getElementById("tooltip");
const mapEl = document.getElementById("map");
const deckCanvasEl = document.getElementById("deck-canvas");
let controlHelpTooltipEl = null;

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

let map;
let deck;
let flowmapData;
let currentMapStyleKey = null;
let clusteringLevelController;

async function fetchData() {
  return await Promise.all([
    csv(`${DATA_PATH}/locations.csv`, (row, i) => ({
      id: row.id,
      name: row.name,
      lat: Number(row.lat),
      lon: Number(row.lon),
    })),
    csv(`${DATA_PATH}/flows.csv`, (row) => ({
      origin: row.origin,
      dest: row.dest,
      count: Number(row.count),
    })),
  ]).then(([locations, flows]) => ({locations, flows}));
}

function hideTooltip() {
  tooltipEl.style.display = 'none';
  tooltipEl.innerHTML = '';
}

function showTooltip(x, y, html) {
  tooltipEl.style.left = `${x}px`;
  tooltipEl.style.top = `${y}px`;
  tooltipEl.innerHTML = html;
  tooltipEl.style.display = 'block';
}

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

function handleHover(info) {
  const html = getTooltipHTML(info);

  if (!html) {
    hideTooltip();
    return;
  }

  showTooltip(info.x, info.y, html);
}

function buildLayer() {
  const {locations, flows} = flowmapData;

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
    },
  });
}

function updateLayers() {
  if (!deck || !flowmapData) return;

  updateBaseMap();
  syncClusteringControls();

  deck.setProps({
    layers: [buildLayer()],
  });
}

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

function compactProps(obj) {
  return Object.fromEntries(
    Object.entries(obj).filter(([, value]) => value !== undefined)
  );
}

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

function hideControlHelpTooltip() {
  const el = ensureControlHelpTooltip();
  el.style.display = "none";
  el.textContent = "";
}

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

function syncClusteringControls() {
  if (!clusteringLevelController) return;

  const disabled = !config.clusteringEnabled || config.clusteringAuto;
  clusteringLevelController.domElement.style.opacity = disabled ? "0.45" : "1";
  clusteringLevelController.domElement.style.pointerEvents = disabled ? "none" : "auto";
}

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

fetchData().then((data) => {
  flowmapData = data;

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
    container: 'map',
    style: MAP_STYLES[initialStyleKey],
    interactive: false,
    center: [initialViewState.longitude, initialViewState.latitude],
    zoom: initialViewState.zoom,
    bearing: initialViewState.bearing,
    pitch: initialViewState.pitch,
  });

  deck = new Deck({
    canvas: 'deck-canvas',
    width: '100%',
    height: '100%',
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

  initGui();
  updateLayers();
});

window.addEventListener("resize", () => {
  hideTooltip();
  hideControlHelpTooltip();
});

window.addEventListener("blur", () => {
  hideTooltip();
  hideControlHelpTooltip();
});