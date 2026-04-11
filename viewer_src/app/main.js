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

import "./styles/base.css";
import "./styles/overlays.css";
import "./styles/controls.css";

import { state } from "./state.js";
import { fetchFlowmapData } from "./data/loadFlowmapData.js";
import {
  hideControlHelpTooltip,
  hideTooltip,
  showSegmentedWarningScreen,
} from "./ui/overlays.js";
import { startViewer } from "./map/viewer.js";

/**
 * Orquesta el arranque del viewer: carga el dataset, actualiza el estado global
 * y decide si iniciar directamente la visualización o mostrar la advertencia
 * previa cuando se detectan flujos segmentados.
 */
async function bootstrap() {
  const data = await fetchFlowmapData();
  state.flowmapData = data;

  if (state.flowmapData.hasSegmentedFlows) {
    showSegmentedWarningScreen({
      onContinue: () => {
        state.segmentedWarningAccepted = true;
        startViewer();
      },
    });
    return;
  }

  startViewer();
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
