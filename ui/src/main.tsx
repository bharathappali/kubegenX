import "@mantine/core/styles.css";
import "./shell.css";
import { MantineProvider } from "@mantine/core";
import { StrictMode, useState } from "react";
import { createRoot } from "react-dom/client";
import { App } from "./App";
import { applyTheme, loadTheme, mantineTheme } from "./theme";

applyTheme(loadTheme());

function Root() {
  const [themeId, setThemeId] = useState(loadTheme);
  const choose = (id: number) => {
    applyTheme(id);
    setThemeId(id);
  };
  return (
    <MantineProvider theme={mantineTheme(themeId)} forceColorScheme="dark">
      <App themeId={themeId} onTheme={choose} />
    </MantineProvider>
  );
}

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <Root />
  </StrictMode>,
);
