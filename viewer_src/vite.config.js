import path from "node:path";
import { defineConfig } from "vite";

const REPO_ROOT = path.resolve(__dirname, "..");

export default defineConfig(({ command }) => ({
  // Mantiene assets del build con rutas relativas dentro de /viewer/.
  base: "./",

  // En dev, se expone la raíz del repo para que el viewer pueda pedir
  // /data/flows/viewer_registry.json y los datasets listados en el registry.
  // En build, no se copia nada extra al directorio viewer/.
  publicDir: command === "serve" ? REPO_ROOT : false,

  build: {
    outDir: "../viewer",
    emptyOutDir: true,
  },
}));