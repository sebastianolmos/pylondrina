import { PickingType } from "@flowmap.gl/layers";
import { INFO_PANEL_DESCRIPTION, INFO_PANEL_TITLE } from "../config.js";
import { state } from "../state.js";
import { escapeHtml, formatRoundedInt, getTripMetric } from "../utils/format.js";

// Referencia al tooltip principal del mapa, usado para hover sobre flujos y locations.
const tooltipEl = document.getElementById("tooltip");

// Referencia al contenedor del mapa base renderizado con MapLibre.
const mapEl = document.getElementById("map");

// Referencia al canvas donde deck.gl dibuja los flujos y elementos interactivos.
const deckCanvasEl = document.getElementById("deck-canvas");

// Referencia lazy al tooltip de ayuda contextual asociado a los controles del panel.
let controlHelpTooltipEl = null;

// Referencia lazy al banner que indica cuando el viewer está filtrado por un nodo seleccionado.
let focusModeBannerEl = null;

// Referencia lazy al panel informativo principal ubicado en la esquina superior izquierda.
let datasetInfoPanelEl = null;

// Referencia al cuerpo expandible del panel informativo principal.
let datasetInfoPanelBodyEl = null;

// Referencia lazy al overlay de advertencia mostrado antes de iniciar el viewer con datos segmentados.
let segmentedWarningOverlayEl = null;

/**
 * Expone un conjunto mínimo de referencias a nodos base del viewer para que
 * otros módulos puedan reutilizarlos sin consultar repetidamente el DOM.
 */
export function getDomRefs() {
  return { tooltipEl, mapEl, deckCanvasEl };
}

/** Oculta el tooltip principal del mapa. */
export function hideTooltip() {
  tooltipEl.style.display = "none";
  tooltipEl.innerHTML = "";
}

/** Muestra el tooltip principal del mapa en una posición de pantalla. */
export function showTooltip(x, y, html) {
  tooltipEl.style.left = `${x}px`;
  tooltipEl.style.top = `${y}px`;
  tooltipEl.innerHTML = html;
  tooltipEl.style.display = "block";
}

/** Genera el HTML del tooltip para hover sobre un flujo o una location. */
function getTooltipHtml(info) {
  if (!info?.object) return null;

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
        <div class="tooltip-title">${object.origin?.id ?? "-"} → ${object.dest?.id ?? "-"}</div>
        <div class="tooltip-row">Trips: <span class="tooltip-value">${object.count ?? 0}</span></div>
      `;

    default:
      return null;
  }
}

/** Maneja el hover de FlowmapLayer y actualiza el tooltip principal. */
export function handleMapHover(info) {
  const html = getTooltipHtml(info);

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
  focusModeBannerEl.className = "overlay-banner overlay-banner--top-center is-hidden";

  document.body.appendChild(focusModeBannerEl);
  return focusModeBannerEl;
}

/** Actualiza el contenido y visibilidad del banner del modo foco por nodo. */
export function updateFocusModeBanner() {
  const el = ensureFocusModeBanner();

  if (!state.selectedLocation) {
    el.classList.add("is-hidden");
    el.innerHTML = "";
    return;
  }

  el.innerHTML = `
    Mostrando solo flujos y locations relacionados a
    <strong>${escapeHtml(state.selectedLocation.name)}</strong>
    <span class="overlay-banner__muted">(${escapeHtml(state.selectedLocation.id)})</span>
  `;
  el.classList.remove("is-hidden");
}

/** Calcula el resumen global del dataset cargado para el panel informativo. */
function getDatasetSummary() {
  if (!state.flowmapData) {
    return { totalTrips: 0, totalFlows: 0 };
  }

  const totalTrips = state.flowmapData.flows.reduce(
    (accumulator, flow) => accumulator + getTripMetric(flow),
    0
  );

  return {
    totalTrips,
    totalFlows: state.flowmapData.flows.length,
  };
}

/** Calcula el resumen del nodo seleccionado en modo foco. */
function getSelectedLocationSummary() {
  if (!state.flowmapData || !state.selectedLocation) return null;

  const selectedId = state.selectedLocation.id;
  const relatedFlows = state.flowmapData.flows.filter(
    (flow) => flow.origin === selectedId || flow.dest === selectedId
  );

  const outgoingFlows = relatedFlows.filter((flow) => flow.origin === selectedId);
  const incomingFlows = relatedFlows.filter((flow) => flow.dest === selectedId);

  const totalTrips = relatedFlows.reduce(
    (accumulator, flow) => accumulator + getTripMetric(flow),
    0
  );

  const outgoingTrips = outgoingFlows.reduce(
    (accumulator, flow) => accumulator + getTripMetric(flow),
    0
  );

  const incomingTrips = incomingFlows.reduce(
    (accumulator, flow) => accumulator + getTripMetric(flow),
    0
  );

  return {
    id: state.selectedLocation.id,
    name: state.selectedLocation.name ?? state.selectedLocation.id,
    totalTrips,
    totalFlows: relatedFlows.length,
    incomingTrips,
    outgoingTrips,
    incomingFlows: incomingFlows.length,
    outgoingFlows: outgoingFlows.length,
  };
}

/** Crea, si no existe, el panel informativo superior izquierdo del viewer. */
export function ensureDatasetInfoPanel() {
  if (datasetInfoPanelEl) return datasetInfoPanelEl;

  datasetInfoPanelEl = document.createElement("div");
  datasetInfoPanelEl.id = "dataset-info-panel";
  datasetInfoPanelEl.className = "viewer-panel viewer-panel--info";

  const headerEl = document.createElement("div");
  headerEl.className = "viewer-panel__header";

  const titleEl = document.createElement("div");
  titleEl.className = "viewer-panel__title";
  titleEl.textContent = INFO_PANEL_TITLE;

  const toggleButtonEl = document.createElement("button");
  toggleButtonEl.type = "button";
  toggleButtonEl.className = "viewer-panel__toggle";
  toggleButtonEl.textContent = "-";

  datasetInfoPanelBodyEl = document.createElement("div");
  datasetInfoPanelBodyEl.className = "viewer-panel__body";

  toggleButtonEl.addEventListener("click", () => {
    const collapsed = datasetInfoPanelBodyEl.classList.toggle("is-collapsed");
    toggleButtonEl.textContent = collapsed ? "+" : "-";
  });

  headerEl.appendChild(titleEl);
  headerEl.appendChild(toggleButtonEl);
  datasetInfoPanelEl.appendChild(headerEl);
  datasetInfoPanelEl.appendChild(datasetInfoPanelBodyEl);

  document.body.appendChild(datasetInfoPanelEl);
  return datasetInfoPanelEl;
}

/** Actualiza el panel informativo principal según dataset, nodo seleccionado y warnings activos. */
export function updateDatasetInfoPanel() {
  if (!state.flowmapData) return;
  ensureDatasetInfoPanel();

  const summary = getDatasetSummary();
  const selectedSummary = getSelectedLocationSummary();

  const selectedSectionHtml = selectedSummary
    ? `
      <section class="viewer-panel__section">
        <div class="viewer-panel__section-title">
          Nodo: ${escapeHtml(selectedSummary.name)}
          <span class="viewer-panel__muted">(${escapeHtml(selectedSummary.id)})</span>
        </div>

        <div><strong>Viajes relacionados:</strong> ${formatRoundedInt(selectedSummary.totalTrips)}</div>
        <div><strong>Flujos relacionados:</strong> ${formatRoundedInt(selectedSummary.totalFlows)}</div>

        <div class="viewer-panel__subsection-spacer"><strong>Viajes de entrada:</strong> ${formatRoundedInt(selectedSummary.incomingTrips)}</div>
        <div><strong>Viajes de salida:</strong> ${formatRoundedInt(selectedSummary.outgoingTrips)}</div>
        <div><strong>Flujos de entrada:</strong> ${formatRoundedInt(selectedSummary.incomingFlows)}</div>
        <div><strong>Flujos de salida:</strong> ${formatRoundedInt(selectedSummary.outgoingFlows)}</div>
      </section>
    `
    : "";

  const segmentedWarningHtml =
    state.flowmapData.hasSegmentedFlows && state.segmentedWarningAccepted
      ? `
        <section class="viewer-panel__section viewer-panel__section--warning">
          <strong>Advertencia:</strong> se está visualizando un dataset con flujos segmentados no soportados por el viewer actual. Los flujos pueden verse solapados o interpretarse de forma engañosa.
        </section>
      `
      : "";

  datasetInfoPanelBodyEl.innerHTML = `
    <div class="viewer-panel__description">${escapeHtml(INFO_PANEL_DESCRIPTION)}</div>
    <div><strong>Viajes totales:</strong> ${formatRoundedInt(summary.totalTrips)}</div>
    <div><strong>Flujos totales:</strong> ${formatRoundedInt(summary.totalFlows)}</div>
    ${selectedSectionHtml}
    ${segmentedWarningHtml}
  `;
}

/** Crea, si no existe, la pantalla de advertencia para datasets segmentados. */
function ensureSegmentedWarningScreen() {
  if (segmentedWarningOverlayEl) return segmentedWarningOverlayEl;

  segmentedWarningOverlayEl = document.createElement("div");
  segmentedWarningOverlayEl.id = "segmented-warning-overlay";
  segmentedWarningOverlayEl.className = "warning-overlay is-hidden";

  const panelEl = document.createElement("div");
  panelEl.id = "segmented-warning-panel";
  panelEl.className = "warning-overlay__panel";

  const titleEl = document.createElement("div");
  titleEl.className = "warning-overlay__title";
  titleEl.textContent = "Dataset segmentado detectado";

  const messageEl = document.createElement("div");
  messageEl.id = "segmented-warning-message";
  messageEl.className = "warning-overlay__message";

  const buttonEl = document.createElement("button");
  buttonEl.type = "button";
  buttonEl.className = "warning-overlay__button";
  buttonEl.textContent = "Continuar de todas maneras";

  panelEl.appendChild(titleEl);
  panelEl.appendChild(messageEl);
  panelEl.appendChild(buttonEl);
  segmentedWarningOverlayEl.appendChild(panelEl);
  document.body.appendChild(segmentedWarningOverlayEl);

  return segmentedWarningOverlayEl;
}

/** Muestra la advertencia previa cuando se detectan columnas extra en flows.csv. */
export function showSegmentedWarningScreen({ onContinue }) {
  const overlayEl = ensureSegmentedWarningScreen();
  const messageEl = overlayEl.querySelector("#segmented-warning-message");
  const buttonEl = overlayEl.querySelector(".warning-overlay__button");

  const extraColumnsText = state.flowmapData.extraFlowColumns
    .map((column) => escapeHtml(column))
    .join(", ");

  messageEl.innerHTML = `
    <div class="warning-overlay__message-block">
      Se detectaron campos extra en <code>flows.csv</code>, por lo que se asume que el dataset contiene <strong>flujos segmentados</strong>.
    </div>

    <div class="warning-overlay__message-block">
      El visualizador actual no soporta este modo. Si continúas, los flujos segmentados se renderizarán igualmente y pueden verse <strong>solapados</strong> o no interpretarse correctamente.
    </div>

    <div class="warning-overlay__message-meta">
      <strong>Campos detectados:</strong> ${extraColumnsText || "sin detalle"}
    </div>
  `;

  buttonEl.onclick = () => {
    overlayEl.classList.add("is-hidden");
    onContinue();
  };

  overlayEl.classList.remove("is-hidden");
}

/** Crea, si no existe, el tooltip de ayuda contextual para controles del panel. */
export function ensureControlHelpTooltip() {
  if (controlHelpTooltipEl) return controlHelpTooltipEl;

  controlHelpTooltipEl = document.createElement("div");
  controlHelpTooltipEl.id = "control-help-tooltip";
  controlHelpTooltipEl.className = "control-help-tooltip is-hidden";

  document.body.appendChild(controlHelpTooltipEl);
  return controlHelpTooltipEl;
}

/** Oculta el tooltip de ayuda contextual del panel de controles. */
export function hideControlHelpTooltip() {
  const el = ensureControlHelpTooltip();
  el.classList.add("is-hidden");
  el.textContent = "";
}

/** Muestra el tooltip de ayuda contextual junto al cursor. */
export function showControlHelpTooltip(x, y, text) {
  const el = ensureControlHelpTooltip();
  el.textContent = text;
  el.classList.remove("is-hidden");

  requestAnimationFrame(() => {
    const rect = el.getBoundingClientRect();
    const margin = 12;

    let left = x - rect.width - margin;
    let top = y - rect.height / 2;

    left = Math.max(12, left);
    top = Math.max(12, Math.min(window.innerHeight - rect.height - 12, top));

    el.style.left = `${left}px`;
    el.style.top = `${top}px`;
  });
}

/** Vincula un texto de ayuda a una fila del panel de controles. */
export function bindControllerHelp(controller, text) {
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

  rowEl.addEventListener("mouseenter", handleMouseEnter);
  rowEl.addEventListener("mousemove", handleMouseMove);
  rowEl.addEventListener("mouseleave", hideControlHelpTooltip);
  rowEl.addEventListener("focusin", handleMouseEnter);
  rowEl.addEventListener("focusout", hideControlHelpTooltip);
}
