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
 * Obtiene la métrica de viajes asociada a un flujo, priorizando `flow_count`
 * cuando existe y usando `count` como fallback compatible con Flowmap layout.
 */
export function getTripMetric(flow) {
  const explicitFlowCount = Number(flow.flow_count);
  if (Number.isFinite(explicitFlowCount)) return explicitFlowCount;

  const count = Number(flow.count);
  return Number.isFinite(count) ? count : 0;
}

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
