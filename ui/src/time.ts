function unit(n: number, one: string, many: string): string {
  return `${n} ${n === 1 ? one : many}`;
}

// formatDuration prints a span as minutes and seconds, and adds hours once it reaches an hour.
export function formatDuration(seconds: number): string {
  const s = Math.max(0, Math.round(seconds));
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const sec = s % 60;
  if (h > 0) return `${unit(h, "hr", "hrs")} ${unit(m, "min", "mins")} ${unit(sec, "sec", "secs")}`;
  if (m > 0) return `${unit(m, "min", "mins")} ${unit(sec, "sec", "secs")}`;
  return unit(sec, "sec", "secs");
}

export function elapsedSeconds(started?: number, finished?: number, now = Date.now()): number | null {
  if (!started) return null;
  const end = finished && finished >= started ? finished : now / 1000;
  return Math.max(0, end - started);
}
