import {
  DATASET_FORMAT_LABELS,
  DATASET_SELECTOR_DESCRIPTION,
  DATASET_SELECTOR_FORMAT_HELP,
  DATASET_SELECTOR_TITLE,
} from "../config.js";
import { state } from "../state.js";
import { getVisibleDirectoryChildren, isDirectoryNode } from "../data/loadViewerRegistry.js";
import { hideSegmentedWarningScreen } from "./overlays.js";
import { ICONS } from "./icons.js";

let selectorOverlayEl = null;
let selectorPanelEl = null;
let selectorBreadcrumbEl = null;
let selectorDirectoryListEl = null;
let returnToSelectorButtonEl = null;
let datasetSelectionHandler = null;

/** Retorna una etiqueta legible para el formato del dataset. */
function getDatasetFormatLabel(format) {
  return DATASET_FORMAT_LABELS[format] ?? format;
}

/** Retorna el SVG inline apropiado para una carpeta o dataset según su formato. */
function getSelectorItemIcon(node) {
  if (node.type === "directory") return ICONS.folder;
  if (node.format === "flowmap_layout") return ICONS.flowmapLayout;
  if (node.format === "golondrina_flows") return ICONS.golondrinaFlows;
  return ICONS.flowmapLayout;
}

/** Retorna la clase de color visual correspondiente al tipo de ítem del selector. */
function getSelectorItemIconClass(node) {
  if (node.type === "directory") return "dataset-selector-item__icon--folder";
  if (node.format === "flowmap_layout") return "dataset-selector-item__icon--flowmap";
  if (node.format === "golondrina_flows") return "dataset-selector-item__icon--golondrina";
  return "dataset-selector-item__icon--flowmap";
}

/** Construye el nodo visual reutilizable para el ícono y el bloque textual de una fila del selector. */
function appendSelectorItemContent(buttonEl, node, metaText) {
  const iconEl = document.createElement("span");
  iconEl.className = `dataset-selector-item__icon ${getSelectorItemIconClass(node)}`;
  iconEl.innerHTML = getSelectorItemIcon(node);

  const mainEl = document.createElement("span");
  mainEl.className = "dataset-selector-item__main";

  const titleEl = document.createElement("span");
  titleEl.className = "dataset-selector-item__title";
  titleEl.textContent = node.type === "dataset" ? node.label : node.name;

  const metaEl = document.createElement("span");
  metaEl.className = "dataset-selector-item__meta";
  metaEl.textContent = metaText;

  mainEl.appendChild(titleEl);
  mainEl.appendChild(metaEl);

  buttonEl.appendChild(iconEl);
  buttonEl.appendChild(mainEl);
}

/** Crea, si no existe, la vista modal usada para seleccionar datasets desde el registry. */
function ensureDatasetSelectorOverlay() {
  if (selectorOverlayEl) return selectorOverlayEl;

  selectorOverlayEl = document.createElement("div");
  selectorOverlayEl.id = "dataset-selector-overlay";
  selectorOverlayEl.className = "dataset-selector-overlay is-hidden";

  selectorPanelEl = document.createElement("div");
  selectorPanelEl.className = "dataset-selector-panel";

  const titleEl = document.createElement("div");
  titleEl.className = "dataset-selector-panel__title";
  titleEl.textContent = DATASET_SELECTOR_TITLE;

  const descriptionEl = document.createElement("div");
  descriptionEl.className = "dataset-selector-panel__description";
  descriptionEl.textContent = DATASET_SELECTOR_DESCRIPTION;

  const helpSectionEl = document.createElement("section");
  helpSectionEl.className = "dataset-selector-panel__section";

  const helpTitleEl = document.createElement("div");
  helpTitleEl.className = "dataset-selector-panel__section-title";
  helpTitleEl.textContent = "Formatos soportados";

  const helpBodyEl = document.createElement("div");
  helpBodyEl.className = "dataset-selector-format-help";
  helpBodyEl.textContent = DATASET_SELECTOR_FORMAT_HELP;

  const legendEl = document.createElement("div");
  legendEl.className = "dataset-selector-legend";

  [
    { type: "dataset", format: "flowmap_layout", label: DATASET_FORMAT_LABELS.flowmap_layout },
    { type: "dataset", format: "golondrina_flows", label: DATASET_FORMAT_LABELS.golondrina_flows },
    { type: "directory", name: "Carpeta", label: "Carpeta" },
  ].forEach((node) => {
    const itemEl = document.createElement("div");
    itemEl.className = "dataset-selector-legend__item";

    const iconEl = document.createElement("span");
    iconEl.className = `dataset-selector-item__icon ${getSelectorItemIconClass(node)}`;
    iconEl.innerHTML = getSelectorItemIcon(node);

    const labelEl = document.createElement("span");
    labelEl.textContent = node.label;

    itemEl.appendChild(iconEl);
    itemEl.appendChild(labelEl);
    legendEl.appendChild(itemEl);
  });

  helpSectionEl.appendChild(helpTitleEl);
  helpSectionEl.appendChild(helpBodyEl);
  helpSectionEl.appendChild(legendEl);

  const browserSectionEl = document.createElement("section");
  browserSectionEl.className = "dataset-selector-panel__section";

  const browserHeaderEl = document.createElement("div");
  browserHeaderEl.className = "dataset-selector-browser__header";

  const browserTitleEl = document.createElement("div");
  browserTitleEl.className = "dataset-selector-panel__section-title";
  browserTitleEl.textContent = "Datasets disponibles";

  const backButtonEl = document.createElement("button");
  backButtonEl.type = "button";
  backButtonEl.className = "dataset-selector-browser__back-button";
  backButtonEl.textContent = "Volver";
  backButtonEl.addEventListener("click", () => navigateToParentDirectory());

  browserHeaderEl.appendChild(browserTitleEl);
  browserHeaderEl.appendChild(backButtonEl);

  selectorBreadcrumbEl = document.createElement("div");
  selectorBreadcrumbEl.className = "dataset-selector-breadcrumb";

  selectorDirectoryListEl = document.createElement("div");
  selectorDirectoryListEl.className = "dataset-selector-directory-list";

  browserSectionEl.appendChild(browserHeaderEl);
  browserSectionEl.appendChild(selectorBreadcrumbEl);
  browserSectionEl.appendChild(selectorDirectoryListEl);

  selectorPanelEl.appendChild(titleEl);
  selectorPanelEl.appendChild(descriptionEl);
  selectorPanelEl.appendChild(helpSectionEl);
  selectorPanelEl.appendChild(browserSectionEl);

  selectorOverlayEl.appendChild(selectorPanelEl);
  document.body.appendChild(selectorOverlayEl);

  return selectorOverlayEl;
}

/** Crea, si no existe, el botón para volver desde el mapa al selector de datasets. */
function ensureReturnToSelectorButton() {
  if (returnToSelectorButtonEl) return returnToSelectorButtonEl;

  returnToSelectorButtonEl = document.createElement("button");
  returnToSelectorButtonEl.type = "button";
  returnToSelectorButtonEl.id = "return-to-selector-button";
  returnToSelectorButtonEl.className = "viewer-return-button is-hidden";
  returnToSelectorButtonEl.textContent = "Cambiar dataset";
  returnToSelectorButtonEl.addEventListener("click", () => {
    showDatasetSelector();
  });

  document.body.appendChild(returnToSelectorButtonEl);
  return returnToSelectorButtonEl;
}

/** Construye el breadcrumb legible a partir del stack de navegación actual. */
function buildBreadcrumbText() {
  const pathNodes = [...state.navigationStack, state.currentDirectoryNode].filter(Boolean);
  return pathNodes.map((node) => node.name).join(" / ");
}

/** Navega hacia un subdirectorio del árbol jerárquico del registry. */
function navigateToDirectory(directoryNode) {
  if (!isDirectoryNode(directoryNode)) return;

  if (state.currentDirectoryNode) {
    state.navigationStack.push(state.currentDirectoryNode);
  }

  state.currentDirectoryNode = directoryNode;
  renderDatasetSelectorDirectory();
}

/** Retrocede un nivel dentro de la navegación jerárquica del selector. */
function navigateToParentDirectory() {
  if (state.navigationStack.length === 0) return;
  state.currentDirectoryNode = state.navigationStack.pop();
  renderDatasetSelectorDirectory();
}

/** Renderiza el contenido visible del directorio actual según la jerarquía del registry. */
export function renderDatasetSelectorDirectory() {
  if (!selectorOverlayEl || !selectorDirectoryListEl || !state.currentDirectoryNode) return;

  const { directories, datasets } = getVisibleDirectoryChildren(state.currentDirectoryNode);

  selectorBreadcrumbEl.textContent = buildBreadcrumbText();
  selectorDirectoryListEl.innerHTML = "";

  if (directories.length === 0 && datasets.length === 0) {
    const emptyEl = document.createElement("div");
    emptyEl.className = "dataset-selector-empty";
    emptyEl.textContent = "No hay datasets disponibles en este nivel.";
    selectorDirectoryListEl.appendChild(emptyEl);
    return;
  }

  directories.forEach((directoryNode) => {
    const buttonEl = document.createElement("button");
    buttonEl.type = "button";
    buttonEl.className = "dataset-selector-item dataset-selector-item--directory";
    appendSelectorItemContent(buttonEl, directoryNode, "Carpeta");
    buttonEl.addEventListener("click", () => navigateToDirectory(directoryNode));
    selectorDirectoryListEl.appendChild(buttonEl);
  });

  datasets.forEach((datasetNode) => {
    const buttonEl = document.createElement("button");
    buttonEl.type = "button";
    buttonEl.className = "dataset-selector-item dataset-selector-item--dataset";
    appendSelectorItemContent(buttonEl, datasetNode, getDatasetFormatLabel(datasetNode.format));
    buttonEl.addEventListener("click", () => {
      state.selectedDatasetNode = datasetNode;
      if (datasetSelectionHandler) {
        datasetSelectionHandler(datasetNode);
      }
    });
    selectorDirectoryListEl.appendChild(buttonEl);
  });
}

/** Inicializa el estado base de navegación del selector usando la raíz del registry. */
export function initializeDatasetSelector(registry, { onDatasetSelected }) {
  state.registry = registry;
  state.currentDirectoryNode = registry.root;
  state.navigationStack = [];
  datasetSelectionHandler = onDatasetSelected;

  ensureDatasetSelectorOverlay();
  ensureReturnToSelectorButton();
  renderDatasetSelectorDirectory();
}

/** Muestra la vista de selección de datasets como overlay principal del viewer. */
export function showDatasetSelector() {
  hideSegmentedWarningScreen();
  ensureDatasetSelectorOverlay().classList.remove("is-hidden");
  ensureReturnToSelectorButton().classList.add("is-hidden");
  renderDatasetSelectorDirectory();
}

/** Oculta la vista de selección de datasets después de cargar uno concreto. */
export function hideDatasetSelector() {
  ensureDatasetSelectorOverlay().classList.add("is-hidden");
  ensureReturnToSelectorButton().classList.remove("is-hidden");
}
