/**
 * Estado compartido del viewer en tiempo de ejecución.
 *
 * Centraliza referencias a mapa, deck.gl, dataset cargado, selector jerárquico
 * y señales de UI que deben ser accesibles desde distintos módulos sin acoplar
 * toda la lógica a un único archivo.
 */
export const state = {
  map: null,
  deck: null,
  flowmapData: null,
  currentMapStyleKey: null,
  clusteringLevelController: null,
  controlsGui: null,
  selectedLocation: null,
  viewerInitialized: false,
  segmentedWarningAccepted: false,
  registry: null,
  currentDirectoryNode: null,
  navigationStack: [],
  selectedDatasetNode: null,
};
