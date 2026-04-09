# Visualizador web de flujos - área de trabajo

## Propósito

Esta carpeta contiene el código fuente del visualizador web usado para explorar flujos OD de manera nativa dentro del repositorio del proyecto.

Su propósito es servir como área de desarrollo del visualizador. El entregable estático final se generará por separado en la carpeta `viewer/`.

## Origen

Este visualizador parte como una adaptación basada en el ejemplo público:

- `flowmap.gl-purejs-example`
- Repositorio base: `https://github.com/ilyabo/flowmap.gl-purejs-example`

La primera meta es verificar que el ejemplo base puede ejecutarse correctamente en modo desarrollo y compilarse como build estática. Luego se extenderá gradualmente para ajustarlo a las necesidades de Pylondrina.

## Objetivo final esperado

Se busca obtener un visualizador web estático que:

1. permita visualizar flujos OD como flowmap;
2. incorpore un menú básico para elegir datasets;
3. soporte primero datasets en layout Flowmap (`locations.csv` + `flows.csv`);
4. y posteriormente permita cargar datos de flujos en formato Golondrina de manera directa.

## Estado actual

- [x] Copiar/adaptar el ejemplo base en esta carpeta
- [x] Instalar dependencias y verificar ejecución en modo desarrollo
- [x] Verificar generación de build estática
- [x] Probar visualización de un dataset hardcodeado
- [ ] Reemplazar dataset remoto por dataset local
- [ ] Agregar menú básico de selección de dataset
- [ ] Evaluar soporte directo para formato Golondrina

## Notas

- Esta carpeta contiene el proyecto fuente del visualizador.
- La carpeta `viewer/` contendrá la build final generada.
- Los datasets de prueba se ubicarán fuera de esta carpeta, bajo la estructura general del repositorio.

## Como ejecutar

### En modo dev
Ejecutar
```
yarn
yarn dev
```

### Generar build
Ejecutar 
```
yarn build
```

Luego para visualizar la build, ejecutar desde la raiz del proyecto:
```
python -m http.server --directory viewer 8000
```

Finalmente visitar:
```
http://localhost:8000
```