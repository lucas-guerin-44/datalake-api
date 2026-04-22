/** @type {import('tailwindcss').Config} */
export default {
  content: ["./src/**/*.{astro,html,js,jsx,md,mdx,svelte,ts,tsx,vue}"],
  theme: {
    extend: {
      fontFamily: {
        sans: [
          "Inter",
          "ui-sans-serif",
          "system-ui",
          "-apple-system",
          "Segoe UI",
          "Roboto",
          "Helvetica Neue",
          "Arial",
          "sans-serif",
        ],
        mono: [
          "JetBrains Mono",
          "ui-monospace",
          "SFMono-Regular",
          "Menlo",
          "Monaco",
          "Consolas",
          "monospace",
        ],
      },
      colors: {
        ink: {
          50:  "#f6f7f8",
          100: "#eceef0",
          200: "#d5d9de",
          300: "#b0b6bf",
          400: "#808894",
          500: "#5c6471",
          600: "#434a55",
          700: "#30363f",
          800: "#1d2128",
          900: "#0f1115",
          950: "#08090c",
        },
        accent: {
          DEFAULT: "#7cc7f0",
          muted: "#2d5a6a",
        },
      },
    },
  },
  plugins: [],
};
