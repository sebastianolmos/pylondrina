import { defineConfig } from 'vite';

export default defineConfig({
  base: './',
  build: {
    outDir: '../viewer',
    emptyOutDir: true,
  },
});