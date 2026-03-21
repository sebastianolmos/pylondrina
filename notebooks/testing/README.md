## Ejecución de notebooks

Estos notebooks fueron configurados para ejecutarse desde la ubicación en la que se encuentran actualmente dentro del repositorio. El bloque inicial de cada notebook define `REPO_ROOT` usando una ruta relativa fija, por lo que su correcto funcionamiento depende de que no se modifique su posición en el árbol de directorios.

Si un notebook se mueve a otra carpeta, o si se desea reutilizar en una estructura distinta, será necesario ajustar manualmente la definición de `REPO_ROOT` y las rutas derivadas de esta.