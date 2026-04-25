/**
 * Escapa caracteres especiales para insertar texto en HTML sin interpretar su
 * contenido como marcado.
 */
export function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

/**
 * Obtiene la magnitud principal de demanda asociada a un flujo.
 *
 * Prioriza `count`, porque en el viewer esa es la magnitud usada para dibujar
 * el mapa. Si no existe, cae a `flow_value` y finalmente a `flow_count` como
 * último fallback compatible.
 */
export function getDemandMetric(flow) {
  const count = Number(flow?.count);
  if (Number.isFinite(count)) return count;

  const flowValue = Number(flow?.flow_value);
  if (Number.isFinite(flowValue)) return flowValue;

  const flowCount = Number(flow?.flow_count);
  return Number.isFinite(flowCount) ? flowCount : 0;
}

/**
 * Alias de compatibilidad con el nombre anterior usado por partes del viewer.
 */
export const getTripMetric = getDemandMetric;

/**
 * Redondea un valor numérico y lo formatea como entero localizado para mostrar
 * métricas resumidas en la interfaz.
 */
export function formatRoundedInt(value) {
  return Math.round(value).toLocaleString("es-CL");
}

/**
 * Retorna una copia del objeto sin propiedades con valor `undefined`, útil para
 * construir configuraciones limpias antes de pasarlas a FlowmapLayer.
 */
export function compactProps(obj) {
  return Object.fromEntries(
    Object.entries(obj).filter(([, value]) => value !== undefined)
  );
}
