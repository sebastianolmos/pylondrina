/**
 * Viewer web de Pylondrina para visualizar flujos OD en Flowmap layout.
 *
 * Responsabilidades principales:
 * - Cargar el registry jerárquico de datasets.
 * - Mostrar la vista de selección previa al mapa.
 * - Cargar un dataset Flowmap layout concreto.
 * - Inicializar mapa base y FlowmapLayer.
 * - Renderizar overlays/paneles de apoyo.
 * - Exponer controles visuales mediante lil-gui.
 */

import "./styles/base.css";
import "./styles/overlays.css";
import "./styles/controls.css";
import "./styles/selector.css";

import { state } from "./state.js";
import { fetchViewerRegistry } from "./data/loadViewerRegistry.js";
import { fetchFlowmapData } from "./data/loadFlowmapData.js";
import {
  hideControlHelpTooltip,
  hideSegmentedWarningScreen,
  hideTooltip,
  showSegmentedWarningScreen,
} from "./ui/overlays.js";
import { startViewer } from "./map/viewer.js";
import {
  hideDatasetSelector,
  initializeDatasetSelector,
  showDatasetSelector,
} from "./ui/datasetSelector.js";

/** Carga un dataset seleccionado desde el registry y resuelve el arranque del viewer. */
async function loadSelectedDataset(datasetNode) {
  state.selectedLocation = null;
  state.segmentedWarningAccepted = false;
  state.selectedDatasetNode = datasetNode;

  const data = await fetchFlowmapData(datasetNode);
  state.flowmapData = data;

  if (state.flowmapData.hasSegmentedFlows) {
    hideDatasetSelector();

    showSegmentedWarningScreen({
      onContinue: () => {
        state.segmentedWarningAccepted = true;
        startViewer();
      },
      onBack: () => {
        showDatasetSelector();
      },
    });
    return;
  }

  hideSegmentedWarningScreen();
  hideDatasetSelector();
  startViewer();
}

/** Orquesta el arranque del viewer: carga el registry y muestra la vista inicial del selector. */
async function bootstrap() {
  const registry = await fetchViewerRegistry();
  initializeDatasetSelector(registry, {
    onDatasetSelected: loadSelectedDataset,
  });
  showDatasetSelector();
}

bootstrap();

// Al cambiar el tamaño de la ventana, oculta tooltips flotantes para evitar
// posiciones desfasadas respecto del cursor o de la geometría ya renderizada.
window.addEventListener("resize", () => {
  hideTooltip();
  hideControlHelpTooltip();
});

// Al perder foco la pestaña o ventana, oculta tooltips activos para evitar que
// queden visibles cuando el usuario vuelve al viewer en otro contexto.
window.addEventListener("blur", () => {
  hideTooltip();
  hideControlHelpTooltip();
});
