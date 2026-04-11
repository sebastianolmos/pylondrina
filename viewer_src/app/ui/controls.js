import GUI from "lil-gui";
import { COLOR_SCHEMES, PARAM_HELP, config } from "../config.js";
import { state } from "../state.js";
import { bindControllerHelp } from "./overlays.js";

/** Inicializa el panel de controles visuales basado en lil-gui. */
export function initControls({ onChange }) {
  const gui = new GUI({ title: "Flowmap controls" });

  const darkModeController = gui
    .add(config, "darkMode")
    .name("Dark mode")
    .onChange(onChange);
  bindControllerHelp(darkModeController, PARAM_HELP.darkMode);

  const baseMapOpacityController = gui
    .add(config, "baseMapOpacity", 0, 1, 0.01)
    .name("Base map")
    .onChange(onChange);
  bindControllerHelp(baseMapOpacityController, PARAM_HELP.baseMapOpacity);

  const colorSchemeController = gui
    .add(config, "colorScheme", COLOR_SCHEMES)
    .name("Color scheme")
    .onChange(onChange);
  bindControllerHelp(colorSchemeController, PARAM_HELP.colorScheme);

  const highlightColorController = gui
    .addColor(config, "highlightColor")
    .name("Highlight color")
    .onChange(onChange);
  bindControllerHelp(highlightColorController, PARAM_HELP.highlightColor);

  const opacityController = gui
    .add(config, "opacity", 0, 1, 0.01)
    .name("Opacity")
    .onChange(onChange);
  bindControllerHelp(opacityController, PARAM_HELP.opacity);

  const fadeAmountController = gui
    .add(config, "fadeAmount", 0, 100, 1)
    .name("Fade amount")
    .onChange(onChange);
  bindControllerHelp(fadeAmountController, PARAM_HELP.fadeAmount);

  const animationController = gui
    .add(config, "animationEnabled")
    .name("Animation")
    .onChange(onChange);
  bindControllerHelp(animationController, PARAM_HELP.animationEnabled);

  const locationsController = gui
    .add(config, "locationsEnabled")
    .name("Show locations")
    .onChange(onChange);
  bindControllerHelp(locationsController, PARAM_HELP.locationsEnabled);

  const locationLabelsController = gui
    .add(config, "locationLabelsEnabled")
    .name("Location labels")
    .onChange(onChange);
  bindControllerHelp(locationLabelsController, PARAM_HELP.locationLabelsEnabled);

  const clusteringFolder = gui.addFolder("Clustering");

  const clusteringEnabledController = clusteringFolder
    .add(config, "clusteringEnabled")
    .name("Enabled")
    .onChange(onChange);
  bindControllerHelp(clusteringEnabledController, PARAM_HELP.clusteringEnabled);

  const clusteringAutoController = clusteringFolder
    .add(config, "clusteringAuto")
    .name("Auto")
    .onChange(onChange);
  bindControllerHelp(clusteringAutoController, PARAM_HELP.clusteringAuto);

  state.clusteringLevelController = clusteringFolder
    .add(config, "clusteringLevel", 1, 12, 1)
    .name("Level")
    .onChange(onChange);
  bindControllerHelp(state.clusteringLevelController, PARAM_HELP.clusteringLevel);

  const adaptiveScalesController = gui
    .add(config, "adaptiveScalesEnabled")
    .name("Adaptive scales")
    .onChange(onChange);
  bindControllerHelp(adaptiveScalesController, PARAM_HELP.adaptiveScalesEnabled);

  const maxTopFlowsController = gui
    .add(config, "maxTopFlowsDisplayNum", 100, 10000, 100)
    .name("Max top flows")
    .onChange(onChange);
  bindControllerHelp(maxTopFlowsController, PARAM_HELP.maxTopFlowsDisplayNum);

  return gui;
}
