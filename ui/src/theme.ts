import { createTheme, type MantineColorsTuple } from "@mantine/core";
import { useEffect, useState } from "react";

export type Palette = { pink: string; butter: string; mint: string; slate: string };

export const THEMES: { id: number; colors: [string, string, string, string] }[] = [
  { id: 1, colors: ["#F599C6", "#FFEA88", "#7DCCAD", "#4D6787"] },
  { id: 2, colors: ["#FF9A86", "#FFB399", "#FFD6A6", "#FFF0BE"] },
  { id: 3, colors: ["#FFB6A6", "#FFEBD3", "#9BCEC1", "#67A2C5"] },
  { id: 4, colors: ["#249D8F", "#E9C46A", "#E76F51", "#FDF0D5"] },
  { id: 5, colors: ["#B7B1F2", "#FDB7EA", "#FFDCCC", "#FBF3B9"] },
];

const KEY = "kubegenx-theme";

const shade = (hex: string): MantineColorsTuple => [hex, hex, hex, hex, hex, hex, hex, hex, hex, hex];

export function loadTheme(): number {
  const n = Number(localStorage.getItem(KEY));
  return THEMES.some((t) => t.id === n) ? n : 1;
}

export function paletteOf(id: number): Palette {
  const theme = THEMES.find((t) => t.id === id) ?? THEMES[0];
  return { pink: theme.colors[0], butter: theme.colors[1], mint: theme.colors[2], slate: theme.colors[3] };
}

function channel(hex: string, index: number): number {
  return parseInt(hex.slice(1 + index * 2, 3 + index * 2), 16);
}

function linear(value: number): number {
  const s = value / 255;
  return s <= 0.04045 ? s / 12.92 : ((s + 0.055) / 1.055) ** 2.4;
}

function luminance(hex: string): number {
  return 0.2126 * linear(channel(hex, 0)) + 0.7152 * linear(channel(hex, 1)) + 0.0722 * linear(channel(hex, 2));
}

function contrast(a: string, b: string): number {
  const left = luminance(a);
  const right = luminance(b);
  const [hi, lo] = left > right ? [left, right] : [right, left];
  return (hi + 0.05) / (lo + 0.05);
}

function saturation(hex: string): number {
  const r = channel(hex, 0);
  const g = channel(hex, 1);
  const b = channel(hex, 2);
  const max = Math.max(r, g, b);
  const min = Math.min(r, g, b);
  return max === 0 ? 0 : (max - min) / max;
}

const surface = "#1B2430";
const inkLight = "#F7F8FB";

// Selected controls sit on the dark glass. Use the most saturated theme color
// that still keeps the label readable, so a pale paper color is not the highlight.
export function highlightOf(colors: string[]): { fill: string; ink: string } {
  const ranked = colors
    .map((fill) => {
      const onDark = contrast(fill, surface);
      const onLight = contrast(fill, inkLight);
      const ink = onDark >= onLight ? surface : inkLight;
      return { fill, ink, text: Math.max(onDark, onLight), stand: contrast(fill, surface), saturation: saturation(fill) };
    })
    .filter((item) => item.text >= 4.5 && item.stand >= 2)
    .sort((a, b) => b.saturation - a.saturation);
  const pick = ranked[0];
  if (!pick) return { fill: colors[0], ink: inkFor(colors[0]) };
  return { fill: pick.fill, ink: pick.ink };
}

export function applyTheme(id: number) {
  const colors = paletteOf(id);
  const highlight = highlightOf([colors.pink, colors.butter, colors.mint, colors.slate]);
  const root = document.documentElement;
  root.dataset.theme = String(THEMES.some((t) => t.id === id) ? id : 1);
  root.style.setProperty("--pink", colors.pink);
  root.style.setProperty("--butter", colors.butter);
  root.style.setProperty("--mint", colors.mint);
  root.style.setProperty("--slate", colors.slate);
  root.style.setProperty("--highlight", highlight.fill);
  root.style.setProperty("--highlight-ink", highlight.ink);
  localStorage.setItem(KEY, root.dataset.theme);
}

export function mantineTheme(id: number) {
  const colors = paletteOf(id);
  return createTheme({
    fontFamily: "Quicksand, system-ui, sans-serif",
    primaryColor: "slate",
    primaryShade: 6,
    colors: {
      pink: shade(colors.pink),
      butter: shade(colors.butter),
      mint: shade(colors.mint),
      slate: shade(colors.slate),
    },
    defaultRadius: "md",
  });
}

export function inkFor(hex: string): string {
  const h = hex.replace("#", "");
  if (h.length < 6) return "#F7F8FB";
  const r = parseInt(h.slice(0, 2), 16);
  const g = parseInt(h.slice(2, 4), 16);
  const b = parseInt(h.slice(4, 6), 16);
  const y = (r * 299 + g * 587 + b * 114) / 1000;
  return y > 160 ? "#1B2430" : "#F7F8FB";
}

export function usePalette(): Palette {
  const read = (): Palette => {
    const style = getComputedStyle(document.documentElement);
    const pick = (name: string, fallback: string) => style.getPropertyValue(name).trim() || fallback;
    const base = paletteOf(1);
    return {
      pink: pick("--pink", base.pink),
      butter: pick("--butter", base.butter),
      mint: pick("--mint", base.mint),
      slate: pick("--slate", base.slate),
    };
  };
  const [palette, setPalette] = useState<Palette>(read);
  useEffect(() => {
    const push = () => setPalette(read());
    push();
    const obs = new MutationObserver(push);
    obs.observe(document.documentElement, { attributes: true, attributeFilter: ["data-theme", "style"] });
    return () => obs.disconnect();
  }, []);
  return palette;
}
