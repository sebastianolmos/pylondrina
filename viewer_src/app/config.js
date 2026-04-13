// Estilos base disponibles para el mapa de fondo.
export const MAP_STYLES = {
  dark: "https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json",
  light: "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
};

// Ruta única hardcodeada del registry de datasets consumido por el selector.
export const VIEWER_REGISTRY_URL = "/data/flows/viewer_registry.json";

// Columnas mínimas esperadas para considerar flows.csv como layout Flowmap puro.
export const FLOW_REQUIRED_COLUMNS = ["origin", "dest", "count"];

// Columnas mínimas del formato Golondrina para cargar un FlowDataset en el viewer.
export const GOLONDRINA_FLOW_REQUIRED_COLUMNS = [
  "flow_id",
  "origin_h3_index",
  "destination_h3_index",
  "flow_count",
  "flow_value",
];

// Etiquetas legibles por formato usadas en el selector y paneles auxiliares.
export const DATASET_FORMAT_LABELS = {
  flowmap_layout: "Flowmap layout",
  golondrina_flows: "Flujos Golondrina",
};

// Paletas de color disponibles para FlowmapLayer.
export const COLOR_SCHEMES = [
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

// Texto de ayuda asociado a cada control del panel lil-gui.
export const PARAM_HELP = {
  darkMode:
    "Cambia la estética interna del flowmap y además, en este viewer, cambia entre mapa base oscuro y claro.",
  baseMapOpacity:
    "Controla cuánta presencia visual tiene el mapa base. En 0 casi desaparece; en 1 se ve completamente.",
  colorScheme:
    "Paleta de colores usada por los flujos y nodos del flowmap.",
  highlightColor:
    "Color con que se resalta el flujo o nodo bajo hover.",
  opacity: "Opacidad global del flowmap.",
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

// Textos del panel informativo superior izquierdo.
export const INFO_PANEL_TITLE = "Visualizador de Pylondrina";
export const INFO_PANEL_DESCRIPTION =
  "Visualizador de flujos OD para Pylondrina. Soporta datasets en Flowmap layout y artefactos de flujos Golondrina, convirtiéndolos a una vista común sobre el mapa.";

// Textos de la vista de selección de datasets.
export const DATASET_SELECTOR_TITLE = "Selecciona el dataset de flujos";
export const DATASET_SELECTOR_DESCRIPTION =
  "Explora datasets disponibles desde el registry del viewer. El selector muestra tanto Flowmap layout (CSV) como Flujos Golondrina (Parquet), usando iconos distintos para cada formato.";

// Texto breve de ayuda para la leyenda del selector.
export const DATASET_SELECTOR_FORMAT_HELP =
  "Formatos disponibles: Flowmap layout (flows.csv + locations.csv) y Flujos Golondrina (flows.parquet del artefacto persistido).";

// Mensajes del overlay de carga al leer datasets Golondrina.
export const GOLONDRINA_LOADING_TITLE = "Cargando dataset Golondrina";
export const GOLONDRINA_LOADING_MESSAGE =
  "Leyendo Parquet Golondrina y preparando locations + flows para el mapa.";

// Estado de configuración visual editable desde el panel de controles.
export const config = {
  darkMode: true,
  baseMapOpacity: 0.75,
  colorScheme: "Blues",
  highlightColor: "#ff9b29",
  opacity: 0.6,
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
