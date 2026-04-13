import { VIEWER_REGISTRY_URL } from "../config.js";

/** Carga el registry jerárquico de datasets consumido por la vista de selección. */
export async function fetchViewerRegistry() {
  const response = await fetch(VIEWER_REGISTRY_URL, { cache: "no-store" });

  if (!response.ok) {
    throw new Error(
      `No se pudo cargar viewer_registry.json (${response.status} ${response.statusText})`
    );
  }

  const registry = await response.json();

  if (!registry?.root || registry.root.type !== "directory") {
    throw new Error("El viewer_registry.json no contiene un nodo raíz de tipo directory.");
  }

  return registry;
}

/** Determina si un nodo del registry representa una carpeta navegable. */
export function isDirectoryNode(node) {
  return node?.type === "directory";
}

/** Determina si un nodo del registry representa un dataset seleccionable. */
export function isDatasetNode(node) {
  return node?.type === "dataset";
}

/** Retorna los hijos visibles de un directorio sin aplicar filtros adicionales de formato. */
export function getVisibleDirectoryChildren(directoryNode) {
  const children = Array.isArray(directoryNode?.children)
    ? directoryNode.children
    : [];

  const directories = children.filter((child) => child.type === "directory");
  const datasets = children.filter((child) => child.type === "dataset");

  return { directories, datasets };
}
