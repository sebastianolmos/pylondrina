import { Deck } from "@deck.gl/core";
import { getViewStateForLocations } from "@flowmap.gl/data";
import { FlowmapLayer, PickingType } from "@flowmap.gl/layers";
import maplibregl from "maplibre-gl";

import { MAP_STYLES, config } from "../config.js";
import { state } from "../state.js";
import { compactProps } from "../utils/format.js";
import { initControls } from "../ui/controls.js";
import {
  getDomRefs,
  handleMapHover,
  hideTooltip,
  ensureDatasetInfoPanel,
  updateDatasetInfoPanel,
  updateFocusModeBanner,
} from "../ui/overlays.js";

/** Retorna los datos visibles del mapa, aplicando el filtro por nodo seleccionado cuando existe. */
function getFilteredFlowmapData() {
  if (!state.flowmapData || !state.selectedLocation) {
    return state.flowmapData;
  }

  const selectedId = state.selectedLocation.id;
  const filteredFlows = state.flowmapData.flows.filter(
    (flow) => flow.origin === selectedId || flow.dest === selectedId
  );

  const relatedLocationIds = new Set([selectedId]);
  filteredFlows.forEach((flow) => {
    relatedLocationIds.add(flow.origin);
    relatedLocationIds.add(flow.dest);
  });

  const filteredLocations = state.flowmapData.locations.filter((location) =>
    relatedLocationIds.has(location.id)
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

  if (state.selectedLocation?.id === clickedId) {
    state.selectedLocation = null;
  } else {
    state.selectedLocation = {
      id: clickedId,
      name: clickedName,
    };
  }

  hideTooltip();
  updateLayers();
}

/** Traduce la configuración del viewer al conjunto de props que recibirá FlowmapLayer. */
function getLayerVisualProps() {
  const effectiveLocationsEnabled = config.locationsEnabled;
  const effectiveLocationLabelsEnabled =
    effectiveLocationsEnabled === false ? false : config.locationLabelsEnabled;

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

/** Construye la instancia de FlowmapLayer con los datos y controles visuales vigentes. */
function buildLayer() {
  const effectiveData = getFilteredFlowmapData();
  const { locations, flows } = effectiveData;

  return new FlowmapLayer({
    id: "my-flowmap-layer",
    data: { locations, flows },
    pickable: true,
    ...getLayerVisualProps(),
    getLocationId: (location) => location.id,
    getLocationLat: (location) => location.lat,
    getLocationLon: (location) => location.lon,
    getLocationName: (location) => location.name,
    getFlowOriginId: (flow) => flow.origin,
    getFlowDestId: (flow) => flow.dest,
    getFlowMagnitude: (flow) => flow.count,
    onHover: handleMapHover,
    onClick: (info) => {
      if (info?.object) {
        console.log("clicked", info.object.type, info.object, info);
      }
      handleLocationClick(info);
    },
  });
}

/** Habilita o deshabilita visualmente el control de nivel de clustering según el estado actual. */
function syncClusteringControls() {
  if (!state.clusteringLevelController) return;

  const disabled = !config.clusteringEnabled || config.clusteringAuto;
  state.clusteringLevelController.domElement.style.opacity = disabled ? "0.45" : "1";
  state.clusteringLevelController.domElement.style.pointerEvents = disabled
    ? "none"
    : "auto";
}

/** Actualiza estilo y opacidad del mapa base según la configuración visual vigente. */
function updateBaseMap() {
  const { mapEl, deckCanvasEl } = getDomRefs();
  const styleKey = config.darkMode ? "dark" : "light";

  if (state.map && state.currentMapStyleKey !== styleKey) {
    state.currentMapStyleKey = styleKey;
    state.map.setStyle(MAP_STYLES[styleKey]);
  }

  if (mapEl) {
    mapEl.style.opacity = String(config.baseMapOpacity);
  }

  if (deckCanvasEl) {
    deckCanvasEl.style.mixBlendMode = config.darkMode ? "screen" : "multiply";
  }
}

/** Calcula una vista inicial que encuadra las locations del dataset actualmente cargado. */
function getCurrentDatasetViewState() {
  const locations = state.flowmapData?.locations ?? [];
  const [width, height] = [window.innerWidth, window.innerHeight];

  return getViewStateForLocations(
    locations,
    (location) => [location.lon, location.lat],
    [width, height],
    { pad: 0.3 }
  );
}

/** Libera el mapa y el canvas de deck.gl antes de reconstruir el viewer con otro dataset. */
export function destroyViewer() {
  hideTooltip();
  state.selectedLocation = null;

  if (state.controlsGui) {
    state.controlsGui.destroy();
    state.controlsGui = null;
  }

  if (state.deck) {
    state.deck.finalize();
    state.deck = null;
  }

  if (state.map) {
    state.map.remove();
    state.map = null;
  }

  state.currentMapStyleKey = null;
  state.clusteringLevelController = null;
  state.viewerInitialized = false;
}

/** Re-renderiza la capa del mapa y sincroniza overlays/controles dependientes del estado actual. */
export function updateLayers() {
  if (!state.deck || !state.flowmapData) return;

  updateBaseMap();
  syncClusteringControls();
  updateFocusModeBanner();
  updateDatasetInfoPanel();

  state.deck.setProps({
    layers: [buildLayer()],
  });
}

/** Inicializa mapa, Deck, paneles y controles una vez que los datos ya fueron cargados. */
export function startViewer() {
  if (!state.flowmapData) return;

  if (state.viewerInitialized) {
    destroyViewer();
  }

  const { mapEl, deckCanvasEl } = getDomRefs();
  const initialViewState = getCurrentDatasetViewState();
  const initialStyleKey = config.darkMode ? "dark" : "light";
  state.currentMapStyleKey = initialStyleKey;

  state.map = new maplibregl.Map({
    container: "map",
    style: MAP_STYLES[initialStyleKey],
    interactive: false,
    center: [initialViewState.longitude, initialViewState.latitude],
    zoom: initialViewState.zoom,
    bearing: initialViewState.bearing,
    pitch: initialViewState.pitch,
  });

  state.deck = new Deck({
    canvas: "deck-canvas",
    width: "100%",
    height: "100%",
    initialViewState,
    controller: true,
    map: true,
    onViewStateChange: ({ viewState }) => {
      hideTooltip();
      state.map.jumpTo({
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
  initControls({ onChange: updateLayers });

  state.viewerInitialized = true;
  updateDatasetInfoPanel();
  updateLayers();
}
