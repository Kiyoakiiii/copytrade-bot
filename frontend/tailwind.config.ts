import type { Config } from "tailwindcss";

const config: Config = {
  content: ["./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        ink: "#182026",
        panel: "#f7f8fa",
        line: "#d9dee5",
        accent: "#0f766e",
        danger: "#b91c1c",
        warn: "#b7791f"
      }
    }
  },
  plugins: []
};

export default config;

