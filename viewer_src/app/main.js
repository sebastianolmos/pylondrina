/**
 * Viewer web de Pylondrina para visualizar flujos OD en Flowmap layout.
 *
 * Responsabilidades principales:
 * - Cargar el registry jerárquico de datasets.
 * - Mostrar la vista de selección previa al mapa.
 * - Cargar datasets Flowmap layout o Flujos Golondrina.
 * - Resolver overlays de carga y warnings por segmentación.
 * - Inicializar mapa base y FlowmapLayer.
 */

import "./styles/base.css";
import "./styles/overlays.css";
import "./styles/controls.css";
import "./styles/selector.css";

import { GOLONDRINA_LOADING_MESSAGE, GOLONDRINA_LOADING_TITLE } from "./config.js";
import { fetchDatasetData } from "./data/loadDatasetData.js";
import { fetchViewerRegistry } from "./data/loadViewerRegistry.js";
import { startViewer } from "./map/viewer.js";
import { state } from "./state.js";
import {
  hideDatasetSelector,
  initializeDatasetSelector,
  showDatasetSelector,
} from "./ui/datasetSelector.js";
import { hideLoadingOverlay, showLoadingOverlay } from "./ui/loadingOverlay.js";
import {
  hideControlHelpTooltip,
  hideSegmentedWarningScreen,
  hideTooltip,
  showSegmentedWarningScreen,
} from "./ui/overlays.js";

/** Carga un dataset seleccionado desde el registry y resuelve el arranque del viewer. */
async function loadSelectedDataset(datasetNode) {
  state.selectedLocation = null;
  state.segmentedWarningAccepted = false;
  state.selectedDatasetNode = datasetNode;

  const isGolondrinaDataset = ["golondrina_parquet", "golondrina_feather"].includes(
    datasetNode?.format
  );

  try {
    if (isGolondrinaDataset) {
      showLoadingOverlay({
        title: GOLONDRINA_LOADING_TITLE,
        message: GOLONDRINA_LOADING_MESSAGE,
      });
      await new Promise(requestAnimationFrame);
    }

    const data = await fetchDatasetData(datasetNode);
    state.flowmapData = data;

    if (state.flowmapData.hasSegmentedFlows) {
      hideLoadingOverlay();
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
    hideLoadingOverlay();
    hideDatasetSelector();
    startViewer();
  } catch (error) {
    hideLoadingOverlay();
    console.error(error);
    showDatasetSelector();
    window.alert(`No se pudo cargar el dataset seleccionado.\n\n${error.message}`);
  }
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
