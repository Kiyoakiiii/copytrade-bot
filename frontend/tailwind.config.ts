import type { Config } from "tailwindcss";

const config: Config = {
  content: ["./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      fontFamily: {
        sans: [
          "Avenir Next",
          "Avenir",
          "Segoe UI Variable Text",
          "Segoe UI Variable",
          "SF Pro Text",
          "SF Pro Display",
          "Inter",
          "Noto Sans SC",
          "PingFang SC",
          "Microsoft YaHei UI",
          "ui-sans-serif",
          "system-ui",
          "sans-serif",
        ],
        mono: [
          "SFMono-Regular",
          "Cascadia Code",
          "Roboto Mono",
          "Noto Sans Mono",
          "ui-monospace",
          "Menlo",
          "Monaco",
          "Consolas",
          "monospace",
        ],
      },
      colors: {
        ink: "#132238",
        panel: "#f5f7fb",
        line: "#dce3ec",
        accent: "#0f8f83",
        danger: "#c43d4d",
        warn: "#b7791f",
        navy: "#0d1b2a"
      }
    }
  },
  plugins: []
};

export default config;
