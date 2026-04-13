import {
  DATASET_FORMAT_OPTIONS,
  DATASET_SELECTOR_DESCRIPTION,
  DATASET_SELECTOR_TITLE,
} from "../config.js";
import { state } from "../state.js";
import {
  getVisibleDirectoryChildren,
  isDirectoryNode,
} from "../data/loadViewerRegistry.js";
import { hideSegmentedWarningScreen } from "./overlays.js";

let selectorOverlayEl = null;
let selectorPanelEl = null;
let selectorBreadcrumbEl = null;
let selectorDirectoryListEl = null;
let selectorFormatHelpEl = null;
let returnToSelectorButtonEl = null;
let datasetSelectionHandler = null;

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

  const formatSectionEl = document.createElement("section");
  formatSectionEl.className = "dataset-selector-panel__section";

  const formatTitleEl = document.createElement("div");
  formatTitleEl.className = "dataset-selector-panel__section-title";
  formatTitleEl.textContent = "Formato";

  const formatOptionsEl = document.createElement("div");
  formatOptionsEl.className = "dataset-selector-format-options";

  DATASET_FORMAT_OPTIONS.forEach((option) => {
    const labelEl = document.createElement("label");
    labelEl.className = `dataset-selector-format-option${option.enabled ? "" : " dataset-selector-format-option--disabled"}`;
    labelEl.title = option.description;

    const inputEl = document.createElement("input");
    inputEl.type = "radio";
    inputEl.name = "dataset-format";
    inputEl.value = option.value;
    inputEl.checked = state.selectedFormatFilter === option.value;
    inputEl.disabled = !option.enabled;

    inputEl.addEventListener("change", () => {
      state.selectedFormatFilter = option.value;
      renderDatasetSelectorDirectory();
      selectorFormatHelpEl.textContent = option.description;
    });

    const textEl = document.createElement("span");
    textEl.textContent = option.label;

    labelEl.addEventListener("mouseenter", () => {
      selectorFormatHelpEl.textContent = option.description;
    });

    labelEl.appendChild(inputEl);
    labelEl.appendChild(textEl);
    formatOptionsEl.appendChild(labelEl);
  });

  selectorFormatHelpEl = document.createElement("div");
  selectorFormatHelpEl.className = "dataset-selector-format-help";
  selectorFormatHelpEl.textContent =
    DATASET_FORMAT_OPTIONS.find((option) => option.value === state.selectedFormatFilter)
      ?.description ?? "";

  formatSectionEl.appendChild(formatTitleEl);
  formatSectionEl.appendChild(formatOptionsEl);
  formatSectionEl.appendChild(selectorFormatHelpEl);

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
  selectorPanelEl.appendChild(formatSectionEl);
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

/** Renderiza el contenido visible del directorio actual según formato y jerarquía. */
export function renderDatasetSelectorDirectory() {
  if (!selectorOverlayEl || !selectorDirectoryListEl || !state.currentDirectoryNode) return;

  const { directories, datasets } = getVisibleDirectoryChildren(
    state.currentDirectoryNode,
    state.selectedFormatFilter
  );

  selectorBreadcrumbEl.textContent = buildBreadcrumbText();
  selectorDirectoryListEl.innerHTML = "";

  if (directories.length === 0 && datasets.length === 0) {
    const emptyEl = document.createElement("div");
    emptyEl.className = "dataset-selector-empty";
    emptyEl.textContent = "No hay datasets compatibles en este nivel.";
    selectorDirectoryListEl.appendChild(emptyEl);
    return;
  }

  directories.forEach((directoryNode) => {
    const buttonEl = document.createElement("button");
    buttonEl.type = "button";
    buttonEl.className = "dataset-selector-item dataset-selector-item--directory";
    buttonEl.innerHTML = `
      <span class="dataset-selector-item__icon">📁</span>
      <span class="dataset-selector-item__main">
        <span class="dataset-selector-item__title">${directoryNode.name}</span>
        <span class="dataset-selector-item__meta">Carpeta</span>
      </span>
    `;
    buttonEl.addEventListener("click", () => navigateToDirectory(directoryNode));
    selectorDirectoryListEl.appendChild(buttonEl);
  });

  datasets.forEach((datasetNode) => {
    const buttonEl = document.createElement("button");
    buttonEl.type = "button";
    buttonEl.className = "dataset-selector-item dataset-selector-item--dataset";
    buttonEl.innerHTML = `
      <span class="dataset-selector-item__icon">🗂️</span>
      <span class="dataset-selector-item__main">
        <span class="dataset-selector-item__title">${datasetNode.label}</span>
        <span class="dataset-selector-item__meta">${datasetNode.format}</span>
      </span>
    `;
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
