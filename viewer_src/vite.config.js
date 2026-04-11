import path from "node:path";
import { defineConfig } from "vite";

const FLOW_EXPORTS_DIR = path.resolve(
  __dirname,
  "../data/synthetic/demo_outputs/flow_exports"
);

export default defineConfig(({ command }) => ({
  base: "./",

  // En dev, Vite sirve flow_exports/ en "/"
  // En build, no usa publicDir para no copiar nada a viewer/
  publicDir: command === "serve" ? FLOW_EXPORTS_DIR : false,

  define: {
    __FLOW_EXPORTS_BASE_PATH__: JSON.stringify(
      command === "serve"
        ? ""
        : "/data/synthetic/demo_outputs/flow_exports"
    ),
  },

  build: {
    outDir: "../viewer",
    emptyOutDir: true,
  },
}));