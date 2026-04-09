/*
 * Copyright (c) Flowmap.gl contributors
 * SPDX-License-Identifier: MIT
 */

import {Deck} from "@deck.gl/core";
import { FlowmapLayer, PickingType } from '@flowmap.gl/layers';
import {getViewStateForLocations} from "@flowmap.gl/data";
import {csv} from "d3-fetch";
import maplibregl from "maplibre-gl";
import GUI from 'lil-gui';

const MAPLIBRE_STYLE =
  "https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json";

// Dataset original del ejemplo base
const DATA_PATH = `https://gist.githubusercontent.com/ilyabo/68d3dba61d86164b940ffe60e9d36931/raw/a72938b5d51b6df9fa7bba9aa1fb7df00cd0f06a`;

const tooltipEl = document.getElementById('tooltip');

const config = {
  darkMode: true,
  opacity: 1.0,
  fadeAmount: 0,
  animationEnabled: true,
  locationsEnabled: true,
  locationTotalsEnabled: true,
  locationLabelsEnabled: false,
  clusteringEnabled: false,
  adaptiveScalesEnabled: true,
  maxTopFlowsDisplayNum: 5000,
};

let map;
let deck;
let flowmapData;

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
  const { locations, flows } = flowmapData;

  return new FlowmapLayer({
    id: 'my-flowmap-layer',
    data: { locations, flows },

    pickable: true,

    opacity: config.opacity,
    darkMode: config.darkMode,
    fadeAmount: config.fadeAmount,
    animationEnabled: config.animationEnabled,
    locationsEnabled: config.locationsEnabled,
    locationTotalsEnabled: config.locationTotalsEnabled,
    locationLabelsEnabled: config.locationLabelsEnabled,
    clusteringEnabled: config.clusteringEnabled,
    adaptiveScalesEnabled: config.adaptiveScalesEnabled,
    maxTopFlowsDisplayNum: config.maxTopFlowsDisplayNum,

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
        console.log('clicked', info.object.type, info.object, info);
      }
    },
  });
}

function updateLayers() {
  if (!deck || !flowmapData) return;
  deck.setProps({
    layers: [buildLayer()],
  });
}

function initGui() {
  const gui = new GUI({ title: 'Flowmap controls' });

  gui
    .add(config, 'darkMode')
    .name('Dark mode')
    .onChange(updateLayers);

  gui
    .add(config, 'opacity', 0, 1, 0.01)
    .name('Opacity')
    .onChange(updateLayers);

  gui
    .add(config, 'fadeAmount', 0, 100, 1)
    .name('Fade amount')
    .onChange(updateLayers);

  gui
    .add(config, 'animationEnabled')
    .name('Animation')
    .onChange(updateLayers);

  gui
    .add(config, 'locationsEnabled')
    .name('Show locations')
    .onChange(updateLayers);

  gui
    .add(config, 'locationTotalsEnabled')
    .name('Location totals')
    .onChange(updateLayers);

  gui
    .add(config, 'locationLabelsEnabled')
    .name('Location labels')
    .onChange(updateLayers);

  gui
    .add(config, 'clusteringEnabled')
    .name('Clustering')
    .onChange(updateLayers);

  gui
    .add(config, 'adaptiveScalesEnabled')
    .name('Adaptive scales')
    .onChange(updateLayers);

  gui
    .add(config, 'maxTopFlowsDisplayNum', 100, 10000, 100)
    .name('Max top flows')
    .onChange(updateLayers);

  return gui;
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

  map = new maplibregl.Map({
    container: 'map',
    style: MAPLIBRE_STYLE,
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

  initGui();
  updateLayers();
});

window.addEventListener('resize', () => {
  hideTooltip();
});