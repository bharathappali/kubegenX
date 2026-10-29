import { useEffect, useMemo, useRef, useState } from "react";
import { createPortal } from "react-dom";
import { Button, Card, Group, Modal, Progress, SegmentedControl, SimpleGrid, Stack, Text } from "@mantine/core";
import { api, type Cell, type DatasetItem, type Layout, type LayoutNamespace, type Progress as Prog, type WorkloadPageItem } from "./api";
import { ExportRail } from "./export";
import { Icon, IconButton, EmptyMark } from "./icons";
import { inkFor, usePalette, type Palette } from "./theme";
import { elapsedSeconds, formatDuration } from "./time";

type Focus = "days" | "namespace" | "workload";

export function datasetFromHash(): string {
  const raw = location.hash.replace(/^#/, "");
  const q = raw.includes("?") ? raw.slice(raw.indexOf("?") + 1) : "";
  return new URLSearchParams(q).get("name") ?? "";
}

function formatBytes(n: number): string {
  if (!n) return "0 B";
  const units = ["B", "KiB", "MiB", "GiB", "TiB"];
  let v = n;
  let i = 0;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i++;
  }
  return `${v.toFixed(i === 0 ? 0 : 1)} ${units[i]}`;
}

function formatEta(seconds: number): string {
  if (!Number.isFinite(seconds) || seconds <= 0) return "0s";
  const s = Math.round(seconds);
  if (s < 90) return `${s}s`;
  const m = Math.round(s / 60);
  if (m < 90) return `${m}m`;
  return `${Math.floor(m / 60)}h ${m % 60}m`;
}

function applyMessage(msg: Prog): Prog {
  const kpi = msg.kpi;
  if (!kpi) return msg;
  return {
    ...msg,
    percent: kpi.percent ?? msg.percent,
    bytes: kpi.bytes ?? msg.bytes,
    items: {
      total: kpi.itemsTotal ?? msg.items?.total ?? 0,
      done: kpi.itemsDone ?? msg.items?.done ?? 0,
      failed: kpi.itemsFailed ?? msg.items?.failed ?? 0,
      leased: kpi.itemsLeased ?? msg.items?.leased ?? 0,
    },
  };
}

function mergeProg(prev: Prog, msg: Prog): Prog {
  const next = applyMessage(msg);
  const prevDone = prev.items?.done ?? 0;
  const nextDone = next.items?.done ?? 0;
  const sameTotal = (next.items?.total ?? 0) > 0 && next.items?.total === prev.items?.total;
  if (sameTotal && nextDone < prevDone) return prev;
  if ((next.cells?.length ?? 0) === 0 && (prev.cells?.length ?? 0) > 0) {
    return { ...next, cells: prev.cells, percent: Math.max(next.percent ?? 0, prev.percent ?? 0) };
  }
  if ((next.percent ?? 0) < (prev.percent ?? 0) && sameTotal) {
    return { ...next, percent: prev.percent };
  }
  return next;
}

function covers(cell: Cell, workloadId: number): boolean {
  return workloadId >= cell.wlLo && workloadId < cell.wlHi;
}

function chunkHint(done: number, total: number, leased: number, failed: number): string {
  if (failed > 0) return "failed";
  if (leased > 0) return "writing";
  if (total > 0 && done >= total) return "complete";
  return "waiting";
}

const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

function dayStamp(epoch: string | undefined, day: number): string {
  if (!epoch) return "";
  const base = new Date(epoch);
  const when = new Date(base.getTime() + day * 24 * 60 * 60 * 1000);
  if (Number.isNaN(when.getTime())) return "";
  return `${when.getUTCDate()} ${MONTHS[when.getUTCMonth()]}`;
}

export function Live({ onNotice }: { onNotice: (s: string) => void }) {
  const palette = usePalette();
  const [name, setName] = useState(datasetFromHash);
  const [prog, setProg] = useState<Prog>({});
  const [layout, setLayout] = useState<Layout | null>(null);
  const [focus, setFocus] = useState<Focus>("days");
  const [known, setKnown] = useState(false);
  const [connected, setConnected] = useState(false);
  const [rail, setRail] = useState(false);
  const [dock, setDock] = useState<HTMLElement | null>(null);
  const [confirmDel, setConfirmDel] = useState(false);
  const samples = useRef<{ t: number; done: number }[]>([]);

  useEffect(() => {
    setDock(document.getElementById("export-dock"));
  }, []);

  useEffect(() => {
    let stop = false;
    const pull = async () => {
      try {
        const r = await api<{ items: DatasetItem[] }>("/api/v1/datasets");
        if (stop) return;
        const items = r.items ?? [];
        setKnown(true);
        const fromHash = datasetFromHash();
        const current = fromHash && items.some((d) => d.name === fromHash)
          ? fromHash
          : items.find((d) => (d.phase ?? d.progress?.phase) === "Generating")?.name ?? items[0]?.name ?? "";
        setName(current);
        if (current) {
          const hash = "#/status?name=" + encodeURIComponent(current);
          if (location.hash !== hash) location.hash = hash;
        }
      } catch {
        /* controller may still be starting */
      }
    };
    void pull();
    const id = setInterval(pull, 4000);
    return () => {
      stop = true;
      clearInterval(id);
    };
  }, []);

  useEffect(() => {
    if (!name) return;
    let stop = false;
    samples.current = [];
    setLayout(null);
    let layoutReady = false;
    const pull = async () => {
      try {
        const p = await api<Prog>("/api/v1/datasets/" + encodeURIComponent(name) + "/progress");
        if (!stop) setProg((prev) => mergeProg(prev, p));
      } catch {
        /* dataset may still be planning */
      }
      if (layoutReady || stop) return;
      try {
        const l = await api<Layout>("/api/v1/datasets/" + encodeURIComponent(name) + "/layout");
        if (stop) return;
        setLayout(l);
        layoutReady = true;
      } catch {
        /* the plan lands after generation starts */
      }
    };
    void pull();
    const id = setInterval(pull, 1000);
    const proto = location.protocol === "https:" ? "wss" : "ws";
    const ws = new WebSocket(`${proto}://${location.host}/api/v1/ws?topics=dataset:${encodeURIComponent(name)}`);
    ws.onopen = () => setConnected(true);
    ws.onclose = () => setConnected(false);
    ws.onerror = () => setConnected(false);
    ws.onmessage = (ev) => {
      setProg((prev) => mergeProg(prev, JSON.parse(ev.data) as Prog));
    };
    return () => {
      stop = true;
      clearInterval(id);
      ws.close();
    };
  }, [name]);

  const cells = prog.cells ?? [];
  const total = prog.items?.total ?? cells.length;
  const done = prog.items?.done ?? cells.filter((c) => c.state === "done").length;
  const leased = prog.items?.leased ?? cells.filter((c) => c.state === "leased").length;
  const failed = prog.items?.failed ?? cells.filter((c) => c.state === "failed").length;
  const pct = prog.percent ?? (total ? (done / total) * 100 : 0);

  const creating = prog.phase === "Generating" && !!prog.startedAt && !prog.finishedAt;
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    if (!creating) return;
    const id = window.setInterval(() => setNow(Date.now()), 1000);
    return () => window.clearInterval(id);
  }, [creating]);
  const creationSpan = elapsedSeconds(prog.startedAt, creating ? undefined : prog.finishedAt, now);
  const creation = creationSpan == null ? "—" : formatDuration(creationSpan);

  const eta = useMemo(() => {
    const now = Date.now();
    const prev = samples.current[samples.current.length - 1];
    if (!prev || prev.done !== done) samples.current.push({ t: now, done });
    if (samples.current.length > 12) samples.current.shift();
    if (pct >= 100) return "0s";
    const first = samples.current[0];
    const last = samples.current[samples.current.length - 1];
    const dt = (last.t - first.t) / 1000;
    const dd = last.done - first.done;
    if (dt < 1 || dd <= 0) return "—";
    return formatEta(((total - done) * dt) / dd);
  }, [done, pct, total]);

  const chunkHours = layout?.chunkSeconds ? layout.chunkSeconds / 3600 : 6;
  const byChunk = new Map<number, Cell[]>();
  for (const cell of cells) {
    const list = byChunk.get(cell.chunk) ?? [];
    list.push(cell);
    byChunk.set(cell.chunk, list);
  }
  const chunkKeys = [...byChunk.keys()].sort((a, b) => a - b);

  const removeDataset = async () => {
    if (!name) return;
    try {
      await api("/api/v1/datasets/" + encodeURIComponent(name), { method: "DELETE" });
      location.hash = "#/datasets";
    } catch (err) {
      onNotice(err instanceof Error ? err.message : "Could not delete the dataset");
    }
  };

  if (known && !name) {
    return (
      <div className="center-empty">
        <div className="empty glass">
          <EmptyMark />
          <strong>No data is available</strong>
          <p>Create a dataset and its progress will show up here.</p>
          <a className="create-link" href="#/wizard">Create data</a>
        </div>
      </div>
    );
  }

  return (
    <>
    <Stack gap="md">
      <Group justify="space-between" align="flex-end" wrap="wrap">
        <div>
          <div className="crumb-row">
            <a className="icon-btn crumb-back" href="#/datasets" aria-label="Back to datasets"><Icon name="chevron-left" /></a>
            <div className="crumbs">
              <a href="#/datasets">Datasets</a>
              <span aria-hidden="true">›</span>
              <span>{name || "…"}</span>
              <span aria-hidden="true">›</span>
              <b>Status</b>
            </div>
          </div>
          <Text c="dimmed" size="sm">Each day is four 6h blocks. A block pulses while it is written and takes its color when it is full.</Text>
        </div>
        <Group gap={8} wrap="nowrap">
          <IconButton label="Export" pressed={rail} onClick={() => setRail((v) => !v)}><Icon name="export" /></IconButton>
          <IconButton label="Delete dataset" onClick={() => setConfirmDel(true)}><Icon name="trash" /></IconButton>
          <span className="chip" data-tone={connected ? "ok" : "work"}>{connected ? "Live" : "Polling"}</span>
        </Group>
      </Group>
      <SimpleGrid cols={rail ? 2 : { base: 2, sm: 5 }} spacing="sm">
        <Stat label="Progress" value={`${pct.toFixed(1)}%`} hint={prog.phase ?? "waiting"} />
        <Stat label="Chunks" value={`${done}/${total}`} hint={chunkHint(done, total, leased, failed)} />
        <Stat label="ETA" value={eta} hint={formatBytes(prog.bytes ?? 0)} />
        <Stat label="Creation" value={creation} hint={creating ? "so far" : prog.finishedAt ? "done" : "not recorded"} />
        <Stat label="Workloads" value={layout ? String(layout.workloads) : "—"} hint={layout ? `${layout.namespaces.length} namespaces` : "planning"} />
      </SimpleGrid>
      <Progress value={pct} color="mint" size="lg" radius="xl" animated={leased > 0 && pct < 100} />
      <div className="option-row">
        <div className="tabs-center">
          <SegmentedControl
            value={focus}
            onChange={(v) => setFocus(v as Focus)}
            data={[
              { label: "Days", value: "days" },
              { label: "Namespace", value: "namespace" },
              { label: "Workload", value: "workload" },
            ]}
            color="slate"
          />
        </div>
      </div>
      {prog.phase === "Queued" ? <Text c="dimmed">This dataset is queued. Generation starts when the export finishes.</Text> : name && cells.length === 0 ? <Text c="dimmed">Waiting for the plan to land…</Text> : null}
      {focus === "days" ? <ChunkGrid chunks={chunkKeys} byChunk={byChunk} hours={chunkHours} epoch={layout?.epoch} palette={palette} /> : null}
      {focus === "namespace" ? <NamespaceGrid layout={layout} cells={cells} /> : null}
      {focus === "workload" ? <WorkloadGrid name={name} cells={cells} palette={palette} /> : null}
      <Group gap="md">
        <Legend swatch={palette.mint} label="done" />
        <Legend swatch={palette.butter} label="writing" />
        <Legend swatch={palette.slate} label="queued" />
        <Legend swatch={palette.pink} label="failed" />
      </Group>
    </Stack>
    {rail && name && dock ? createPortal(<ExportRail dataset={name} onNotice={onNotice} />, dock) : null}
    <Modal opened={confirmDel} onClose={() => setConfirmDel(false)} title="Delete dataset" centered>
      <Text size="sm">Remove {name} and the data it generated?</Text>
      <Group justify="flex-end" mt="md">
        <Button variant="default" onClick={() => setConfirmDel(false)}>Cancel</Button>
        <Button color="pink" c="#1B2430" onClick={() => { setConfirmDel(false); void removeDataset(); }}>Delete</Button>
      </Group>
    </Modal>
    </>
  );
}

function Stat({ label, value, hint }: { label: string; value: string; hint: string }) {
  return (
    <Card withBorder padding="sm" radius="md" className="glass" style={{ background: "transparent" }}>
      <Text size="xs" c="dimmed" tt="uppercase">{label}</Text>
      <Text fw={700} fz={{ base: 22, sm: 28 }} lh={1.1}>{value}</Text>
      <Text size="sm" style={{ color: "var(--butter)" }}>{hint}</Text>
    </Card>
  );
}

function fillFor(state: string, palette: Palette): string {
  if (state === "done") return palette.mint;
  if (state === "leased") return palette.butter;
  if (state === "failed") return palette.pink;
  return palette.slate;
}

function Circle({ state, title, palette }: { state: string; title: string; palette: Palette }) {
  const writing = state === "leased";
  return (
    <span
      title={title}
      style={{
        width: 16,
        height: 16,
        borderRadius: 999,
        display: "inline-block",
        background: fillFor(state, palette),
        boxShadow: writing ? "0 0 0 3px color-mix(in srgb, var(--butter) 28%, transparent)" : undefined,
        animation: writing ? "kgxpulse 1.2s ease-in-out infinite" : undefined,
      }}
    />
  );
}

function chunkTone(cells: Cell[]): { state: string; done: number; total: number } {
  const done = cells.filter((c) => c.state === "done").length;
  const state = cells.some((c) => c.state === "failed")
    ? "failed"
    : cells.some((c) => c.state === "leased")
      ? "leased"
      : done === cells.length && cells.length > 0
        ? "done"
        : "queued";
  return { state, done, total: cells.length };
}

function clock(hour: number): string {
  return `${String(Math.floor(hour)).padStart(2, "0")}:00`;
}

function ChunkGrid({ chunks, byChunk, hours, epoch, palette }: { chunks: number[]; byChunk: Map<number, Cell[]>; hours: number; epoch?: string; palette: Palette }) {
  const perDay = hours > 0 && 24 % hours === 0 ? Math.round(24 / hours) : 0;
  const days = new Map<number, number[]>();
  for (const chunk of chunks) {
    const day = perDay > 0 ? Math.floor(chunk / perDay) : 0;
    const list = days.get(day) ?? [];
    list.push(chunk);
    days.set(day, list);
  }
  const dayKeys = [...days.keys()].sort((a, b) => a - b);
  return (
    <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(240px, 1fr))", gap: 12 }}>
      <style>{`@keyframes kgxpulse { 50% { transform: scale(0.96); opacity: 0.72 } }`}</style>
      {dayKeys.map((day) => {
        const list = days.get(day) ?? [];
        const done = list.reduce((n, chunk) => n + chunkTone(byChunk.get(chunk) ?? []).done, 0);
        const total = list.reduce((n, chunk) => n + chunkTone(byChunk.get(chunk) ?? []).total, 0);
        const stamp = dayStamp(epoch, day);
        return (
          <Card key={day} withBorder padding="sm" radius="md" className="glass" style={{ background: "transparent" }}>
            <Group justify="space-between" mb={8} wrap="nowrap">
              <span className="day-chip">Day {day + 1}{stamp ? ` · ${stamp}` : ""}</span>
              <Text size="xs" style={{ color: "var(--mint)" }}>{done}/{total}</Text>
            </Group>
            <div style={{ display: "grid", gridTemplateColumns: perDay > 0 ? `repeat(${perDay}, minmax(0, 1fr))` : "repeat(auto-fill, minmax(64px, 1fr))", gap: 8 }}>
              {list.map((chunk) => {
                const cells = byChunk.get(chunk) ?? [];
                const tone = chunkTone(cells);
                const start = chunk * hours;
                const pct = tone.total ? (tone.done / tone.total) * 100 : 0;
                const full = tone.total > 0 && tone.done >= tone.total && !cells.some((c) => c.state === "failed");
                const updating = tone.state === "leased" && !full;
                const fill = full
                  ? palette.mint
                  : tone.state === "failed"
                    ? palette.pink
                    : `conic-gradient(${palette.mint} ${pct}%, ${palette.slate} 0)`;
                const ink = inkFor(full ? palette.mint : pct >= 50 ? palette.mint : palette.slate);
                return (
                  <div
                    key={chunk}
                    title={`${clock(start % 24)}–${clock((start + hours) % 24 || 24)} · ${tone.done}/${tone.total}${updating ? " · writing" : ""}`}
                    style={{
                      minHeight: 64,
                      borderRadius: 12,
                      background: fill,
                      display: "flex",
                      alignItems: "center",
                      justifyContent: "center",
                      animation: updating ? "kgxpulse 1.2s ease-in-out infinite" : undefined,
                    }}
                  >
                    <span style={{ color: ink, fontWeight: 700, fontSize: 12 }}>{clock(start % 24)}</span>
                  </div>
                );
              })}
            </div>
          </Card>
        );
      })}
    </div>
  );
}

function rangeShare(cells: Cell[], lo: number, hi: number): { done: number; total: number; pct: number; writing: boolean } {
  let done = 0;
  let total = 0;
  let writing = false;
  for (const cell of cells) {
    const a = Math.max(cell.wlLo, lo);
    const b = Math.min(cell.wlHi, hi);
    if (b <= a) continue;
    const w = b - a;
    total += w;
    if (cell.state === "done") done += w;
    if (cell.state === "leased") writing = true;
  }
  return { done, total, pct: total ? (done / total) * 100 : 0, writing };
}

function NamespaceGrid({ layout, cells }: { layout: Layout | null; cells: Cell[] }) {
  if (!layout) return <Text c="dimmed">Namespace layout is still loading.</Text>;
  const ranges = layout.namespaces as LayoutNamespace[];
  return (
    <SimpleGrid cols={{ base: 1, sm: 4 }} spacing="sm">
      {ranges.map((ns) => {
        const s = rangeShare(cells, ns.wlLo ?? 0, ns.wlHi ?? 0);
        const count = ns.count ?? Math.max(0, (ns.wlHi ?? 0) - (ns.wlLo ?? 0));
        return (
          <Card key={ns.id} withBorder padding="sm" radius="md" className="glass" style={{ background: "transparent" }}>
            <Group justify="space-between" wrap="nowrap">
              <Text fw={700} style={{ overflow: "hidden", textOverflow: "ellipsis" }}>{ns.name}</Text>
              <Text size="sm" style={{ color: "var(--mint)" }}>{s.pct.toFixed(0)}%</Text>
            </Group>
            <Progress value={s.pct} color="mint" size="sm" mt={6} animated={s.writing} />
            <Text size="xs" c="dimmed" mt={6}>{count} workloads · {s.writing ? "writing" : `${s.done}/${s.total} blocks`}</Text>
          </Card>
        );
      })}
    </SimpleGrid>
  );
}

function WorkloadGrid({ name, cells, palette }: { name: string; cells: Cell[]; palette: Palette }) {
  const limit = 10;
  const [page, setPage] = useState(0);
  const [rows, setRows] = useState<WorkloadPageItem[]>([]);
  const [total, setTotal] = useState(0);
  const [err, setErr] = useState("");
  useEffect(() => {
    if (!name) return;
    let stop = false;
    const load = () => {
      api<{ total: number; items: WorkloadPageItem[] }>(
        "/api/v1/datasets/" + encodeURIComponent(name) + "/workloads?offset=" + page * limit + "&limit=" + limit,
      )
        .then((r) => {
          if (stop) return;
          setRows(r.items ?? []);
          setTotal(r.total ?? 0);
          setErr("");
        })
        .catch((e: unknown) => {
          if (!stop) setErr(e instanceof Error ? e.message : "Workloads are still loading");
        });
    };
    load();
    const id = window.setInterval(load, 2000);
    return () => {
      stop = true;
      window.clearInterval(id);
    };
  }, [name, page]);
  if (err && rows.length === 0) return <Text c="dimmed">{err}</Text>;
  const pages = Math.max(1, Math.ceil(total / limit));
  return (
    <Stack gap="xs">
      <Group justify="space-between">
        <Text size="sm" c="dimmed">{total ? `${page * limit + 1}–${Math.min(total, page * limit + rows.length)} of ${total}` : "Loading workloads"}</Text>
        <Group gap={6}>
          <Button size="xs" variant="default" disabled={page <= 0} onClick={() => setPage((p) => Math.max(0, p - 1))}>Previous</Button>
          <Button size="xs" variant="default" disabled={page + 1 >= pages} onClick={() => setPage((p) => p + 1)}>Next</Button>
        </Group>
      </Group>
      {rows.map((wl) => {
        const mine = cells.filter((c) => covers(c, wl.id));
        return (
          <div key={wl.namespace + wl.id} className="wl-row">
            <div className="wl-name">
              <span className="wl-title" title={wl.name}>{wl.name}</span>
              <span className="wl-sub">{wl.namespace} · {wl.replicas} {wl.replicas === 1 ? "replica" : "replicas"}</span>
            </div>
            <div className="wl-cells">
              {mine.map((cell, i) => (
                <Circle key={i} state={cell.state} palette={palette} title={`${wl.namespace} · ${wl.name} · chunk ${cell.chunk} · ${cell.state}`} />
              ))}
            </div>
          </div>
        );
      })}
    </Stack>
  );
}

function Legend({ swatch, label }: { swatch: string; label: string }) {
  return (
    <Group gap={6}>
      <span style={{ width: 12, height: 12, borderRadius: 99, background: swatch, display: "inline-block" }} />
      <Text size="xs" c="dimmed">{label}</Text>
    </Group>
  );
}
