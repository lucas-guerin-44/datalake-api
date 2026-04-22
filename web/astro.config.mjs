import { defineConfig } from "astro/config";
import tailwind from "@astrojs/tailwind";

export default defineConfig({
  site: "https://datalake.lucasguerin.fr",
  integrations: [tailwind()],
  build: {
    assets: "_assets",
  },
  compressHTML: true,
});
