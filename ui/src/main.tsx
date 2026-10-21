import "@mantine/core/styles.css";
import { MantineProvider, createTheme, type MantineColorsTuple } from "@mantine/core";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { App } from "./App";

const shade = (hex: string): MantineColorsTuple => [hex, hex, hex, hex, hex, hex, hex, hex, hex, hex];

const theme = createTheme({
  fontFamily: "Quicksand, system-ui, sans-serif",
  primaryColor: "slate",
  primaryShade: 6,
  colors: {
    pink: shade("#F599C6"),
    butter: shade("#FFEA88"),
    mint: shade("#7DCCAD"),
    slate: shade("#4D6787"),
  },
  defaultRadius: "md",
});

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <MantineProvider theme={theme} forceColorScheme="dark">
      <App />
    </MantineProvider>
  </StrictMode>,
);
