import { defineConfig } from "astro/config";
import tailwind from "@astrojs/tailwind";

export default defineConfig({
  site: "https://datalake.example.com",
  integrations: [tailwind()],
  build: {
    assets: "_assets",
  },
  compressHTML: true,
});
