let loadingOverlayEl = null;
let loadingTitleEl = null;
let loadingMessageEl = null;

/** Crea, si no existe, el overlay de carga usado para procesamiento de datasets más pesados. */
function ensureLoadingOverlay() {
  if (loadingOverlayEl) return loadingOverlayEl;

  loadingOverlayEl = document.createElement("div");
  loadingOverlayEl.id = "loading-overlay";
  loadingOverlayEl.className = "loading-overlay is-hidden";

  const panelEl = document.createElement("div");
  panelEl.className = "loading-overlay__panel";

  const spinnerEl = document.createElement("div");
  spinnerEl.className = "loading-overlay__spinner";
  spinnerEl.setAttribute("aria-hidden", "true");

  loadingTitleEl = document.createElement("div");
  loadingTitleEl.className = "loading-overlay__title";

  loadingMessageEl = document.createElement("div");
  loadingMessageEl.className = "loading-overlay__message";

  panelEl.appendChild(spinnerEl);
  panelEl.appendChild(loadingTitleEl);
  panelEl.appendChild(loadingMessageEl);
  loadingOverlayEl.appendChild(panelEl);
  document.body.appendChild(loadingOverlayEl);

  return loadingOverlayEl;
}

/** Muestra el overlay de carga con textos configurables para la operación actual. */
export function showLoadingOverlay({ title, message }) {
  const overlayEl = ensureLoadingOverlay();
  loadingTitleEl.textContent = title;
  loadingMessageEl.textContent = message;
  overlayEl.classList.remove("is-hidden");
}

/** Oculta el overlay de carga cuando el dataset ya fue preparado o falló su lectura. */
export function hideLoadingOverlay() {
  ensureLoadingOverlay().classList.add("is-hidden");
}
