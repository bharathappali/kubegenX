import { useEffect, useRef, useState } from "react";
import { Button, Group, Modal, NumberInput, Progress, SegmentedControl, Stack, Text, TextInput } from "@mantine/core";
import { api, type ExportItem, type Layout } from "./api";
import { EffortField } from "./effort";
import { Icon, IconButton } from "./icons";
import { elapsedSeconds, formatDuration } from "./time";

const NOTE_MS = 6000;

const SAMPLE_QUERIES = [
  { title: "Pods by namespace", expr: "count by (namespace) (kube_pod_info)" },
  { title: "CPU by namespace", expr: "sum by (namespace) (rate(container_cpu_usage_seconds_total[5m]))" },
  { title: "Memory by namespace", expr: "sum by (namespace) (container_memory_working_set_bytes)" },
];

function pad(n: number): string {
  return String(n).padStart(2, "0");
}

function localStamp(d: Date): string {
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`;
}

function plain(d: Date): string {
  return d.toLocaleString("en-GB", {
    day: "numeric", month: "short", year: "numeric", hour: "2-digit", minute: "2-digit",
    timeZone: "UTC", hourCycle: "h23",
  });
}

function promRange(seconds: number): string {
  if (seconds % 86400 === 0) return `${seconds / 86400}d`;
  if (seconds % 3600 === 0) return `${seconds / 3600}h`;
  return `${seconds}s`;
}

function thanosURL(raw: string): string {
  try {
    return new URL(raw).origin;
  } catch {
    return raw.replace(/\/graph.*$/, "");
  }
}

function queryLink(raw: string, start: Date, end: Date, expr: string): string {
  if (!raw) return "";
  const seconds = Math.max(1, Math.round((end.getTime() - start.getTime()) / 1000));
  const stamp = `${end.getUTCFullYear()}-${pad(end.getUTCMonth() + 1)}-${pad(end.getUTCDate())} ${pad(end.getUTCHours())}:${pad(end.getUTCMinutes())}:${pad(end.getUTCSeconds())}`;
  const base = raw.includes("g0.range_input") ? raw : raw.replace(/\/$/, "") + "/graph";
  const u = new URL(base);
  u.searchParams.set("g0.expr", expr);
  u.searchParams.set("g0.tab", "0");
  u.searchParams.set("g0.range_input", promRange(seconds));
  u.searchParams.set("g0.end_input", stamp);
  u.searchParams.set("g0.moment_input", stamp);
  return u.toString();
}

function liveLine(row: ExportItem): string {
  if (!row.workers) return "";
  return ` · ${row.workers} workers`;
}

function etaLine(row: ExportItem): string {
  const done = row.blocks ?? 0;
  const total = row.blocksTotal ?? 0;
  if (!done) return "writing block 1";
  if (!row.startedAt || !total || done >= total) return `${done} of ${total || done} blocks`;
  const elapsed = Date.now() / 1000 - row.startedAt;
  const left = Math.max(0, (elapsed / done) * (total - done));
  const mins = Math.max(1, Math.round(left / 60));
  return `${done} of ${total} blocks · about ${mins < 90 ? `${mins}m` : `${Math.floor(mins / 60)}h ${mins % 60}m`} left`;
}

function blockPercent(row: ExportItem): number | null {
  const total = row.blocksTotal ?? 0;
  if (total <= 0) return null;
  return ((row.blocks ?? 0) / total) * 100;
}

function TimedNote({ message, onClose }: { message: string; onClose: () => void }) {
  const [left, setLeft] = useState(100);
  const close = useRef(onClose);
  close.current = onClose;
  useEffect(() => {
    const started = Date.now();
    const id = window.setInterval(() => {
      const remain = Math.max(0, 1 - (Date.now() - started) / NOTE_MS);
      setLeft(remain * 100);
      if (remain <= 0) close.current();
    }, 40);
    return () => window.clearInterval(id);
  }, [message]);
  return (
    <div className="timed-note" role="status">
      <div className="timed-note-row">
        <span>{message}</span>
        <button type="button" aria-label="Close" onClick={onClose}>×</button>
      </div>
      <div className="timed-bar" aria-hidden="true"><span style={{ width: `${left}%` }} /></div>
    </div>
  );
}

function CopyLine({ label, value }: { label: string; value: string }) {
  const [done, setDone] = useState(false);
  const copy = async () => {
    try {
      await navigator.clipboard.writeText(value);
      setDone(true);
      window.setTimeout(() => setDone(false), 1200);
    } catch {
      setDone(false);
    }
  };
  return (
    <div className="copy-line">
      <span className="copy-label">{label}</span>
      <div className="copy-row">
        <span className="copy-value">{value}</span>
        <button type="button" className="icon-btn" aria-label={done ? "Copied" : `Copy ${label}`} title={done ? "Copied" : "Copy"} onClick={() => void copy()}>
          {done ? "✓" : <Icon name="copy" />}
        </button>
      </div>
    </div>
  );
}

function SessionCard({ dataset, row, start, end, onServe, onDelete }: {
  dataset: string;
  row: ExportItem;
  start: Date | null;
  end: Date | null;
  onServe: () => void;
  onDelete: () => void;
}) {
  const [namespaces, setNamespaces] = useState<string[]>([]);
  const id = row.sessionId || row.name;
  const base = `${location.origin}/api/v1/datasets/${encodeURIComponent(dataset)}/sessions/${encodeURIComponent(id)}`;
  const deletes = row.createdAt ? new Date((row.createdAt + 24 * 3600) * 1000) : null;
  useEffect(() => {
    if (row.phase !== "Serving") return;
    let stop = false;
    api<{ namespaces: string[] }>(`/api/v1/datasets/${encodeURIComponent(dataset)}/sessions/${encodeURIComponent(id)}/namespaces`)
      .then((r) => { if (!stop) setNamespaces(r.namespaces ?? []); })
      .catch(() => undefined);
    return () => { stop = true; };
  }, [dataset, id, row.phase]);
  return (
    <article className="session-card">
      <div className="session-head">
        <p><b>{id}</b> · {row.phase}{row.interval ? ` · ${row.interval}` : ""}</p>
        <Group gap={6} wrap="nowrap">
          {row.phase === "Configured" || row.phase === "Failed" ? (
            <Button size="xs" color="mint" c="#1B2430" onClick={onServe}>Serve</Button>
          ) : null}
          <IconButton label="Delete session" onClick={onDelete}><Icon name="trash" /></IconButton>
        </Group>
      </div>
      {start ? <p>Start {plain(start)} UTC</p> : null}
      {end ? <p>End {plain(end)} UTC</p> : null}
      {deletes ? <p>Deletes {plain(deletes)} UTC</p> : null}
      {row.phase === "Serving" ? (
        <>
          <CopyLine label="Namespaces" value={`${base}/namespaces`} />
          <CopyLine label="Results" value={`${base}/results?page=1`} />
          {namespaces.slice(0, 3).map((ns) => (
            <CopyLine key={ns} label={`Workloads · ${ns}`} value={`${base}/namespaces/${encodeURIComponent(ns)}/workloads`} />
          ))}
        </>
      ) : <p>URLs are ready once this session is serving.</p>}
    </article>
  );
}

export function ExportRail({ dataset, onNotice }: { dataset: string; onNotice: (s: string) => void }) {
  const [items, setItems] = useState<ExportItem[]>([]);
  const [effort, setEffort] = useState("xhigh");
  const [layout, setLayout] = useState<Layout | null>(null);
  const [tab, setTab] = useState<"thanos" | "agg">("thanos");
  const [form, setForm] = useState(false);
  const [edge, setEdge] = useState("start");
  const [when, setWhen] = useState("");
  const [amount, setAmount] = useState<number | string>(15);
  const [unit, setUnit] = useState("m");
  const [drop, setDrop] = useState<ExportItem | null>(null);
  const [note, setNote] = useState("");

  const pull = async () => {
    const r = await api<{ items: ExportItem[] }>("/api/v1/exports");
    setItems((r.items ?? []).filter((e) => e.dataset === dataset));
  };

  useEffect(() => {
    void pull().catch((e: Error) => onNotice(e.message));
    void api<Layout>("/api/v1/datasets/" + encodeURIComponent(dataset) + "/layout").then(setLayout).catch(() => setLayout(null));
    const id = window.setInterval(() => void pull().catch(() => undefined), 2000);
    return () => window.clearInterval(id);
  }, [dataset]);

  useEffect(() => {
    if (!layout?.epoch || when) return;
    const d = new Date(layout.epoch);
    if (!Number.isNaN(d.getTime())) setWhen(localStamp(d));
  }, [layout, when]);

  const durationMs = layout ? layout.chunks * layout.chunkSeconds * 1000 : 0;
  const chosen = when ? new Date(when) : null;
  const other = chosen && durationMs && !Number.isNaN(chosen.getTime())
    ? new Date(edge === "end" ? chosen.getTime() - durationMs : chosen.getTime() + durationMs)
    : null;
  const thanos = items.find((row) => (row.type || "thanos") !== "generic");
  const converting = thanos?.phase === "Building" && !!thanos.startedAt;
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    if (!converting) return;
    const id = window.setInterval(() => setNow(Date.now()), 1000);
    return () => window.clearInterval(id);
  }, [converting]);
  const conversion = (() => {
    if (!thanos?.startedAt) return "";
    if (thanos.phase === "Building") {
      const span = elapsedSeconds(thanos.startedAt, undefined, now);
      return span == null ? "" : formatDuration(span);
    }
    if (!thanos.finishedAt) return "";
    const span = elapsedSeconds(thanos.startedAt, thanos.finishedAt, now);
    return span == null ? "" : formatDuration(span);
  })();
  const sessions = items.filter((row) => row.type === "generic");

  const bounds = (row: ExportItem): { start: Date | null; end: Date | null } => {
    const start = row.startTime ? new Date(row.startTime) : null;
    const end = row.endTime ? new Date(row.endTime) : start && durationMs ? new Date(start.getTime() + durationMs) : null;
    return {
      start: start && !Number.isNaN(start.getTime()) ? start : null,
      end: end && !Number.isNaN(end.getTime()) ? end : null,
    };
  };

  const openForm = () => {
    if (tab === "thanos" && thanos) {
      setNote("This dataset already has a Thanos export.");
      setForm(false);
      return;
    }
    setForm(true);
  };

  const create = async () => {
    if (tab === "thanos" && thanos) {
      setNote("This dataset already has a Thanos export.");
      return;
    }
    if (!chosen || Number.isNaN(chosen.getTime())) {
      onNotice("Pick a time");
      return;
    }
    try {
      await api("/api/v1/exports", {
        method: "POST",
        body: JSON.stringify({
          dataset,
          type: tab === "agg" ? "generic" : "thanos",
          anchor: edge,
          time: chosen.toISOString(),
          interval: tab === "agg" ? `${amount}${unit}` : "",
          effort,
        }),
      });
      setForm(false);
      await pull();
    } catch (err) {
      const message = err instanceof Error ? err.message : "Could not create the export";
      if (message.toLowerCase().includes("thanos")) setNote(message.endsWith(".") ? message : message + ".");
      else onNotice(message);
    }
  };

  const serve = async (name: string) => {
    try {
      await api("/api/v1/exports/" + encodeURIComponent(name) + "/serve", { method: "POST" });
      await pull();
    } catch (err) {
      onNotice(err instanceof Error ? err.message : "Could not serve the export");
    }
  };

  const remove = async (name: string) => {
    try {
      await api("/api/v1/exports/" + encodeURIComponent(name), { method: "DELETE" });
      await pull();
    } catch (err) {
      onNotice(err instanceof Error ? err.message : "Could not delete the export");
    }
  };

  const thanosView = () => {
    if (!thanos) {
      return form ? null : <p>No Thanos export. Configure one.</p>;
    }
    const { start, end } = bounds(thanos);
    const host = thanos.url ? thanosURL(thanos.url) : "";
    const pct = blockPercent(thanos);
    return (
      <div className="export-row">
        <div className="session-head">
          <p><b>{thanos.phase}</b>{pct != null ? ` · ${pct.toFixed(2)}%` : ""}</p>
          <IconButton label="Delete export" onClick={() => setDrop(thanos)}><Icon name="trash" /></IconButton>
        </div>
        {pct != null ? <Progress value={pct} color="mint" size="md" radius="xl" animated={thanos.phase === "Building"} /> : null}
        {thanos.phase === "Building" ? <p>Converting · {etaLine(thanos)}{liveLine(thanos)}</p> : null}
        {conversion ? <p>Time {conversion}</p> : null}
        {thanos.phase === "Queued" ? <p>Waiting. Generation or another export is using the cluster.</p> : null}
        {start ? <p>Start {plain(start)} UTC</p> : null}
        {end ? <p>End {plain(end)} UTC</p> : null}
        {host ? <CopyLine label="Thanos" value={host} /> : thanos.phase === "Serving" ? <p>The address appears when the route is ready.</p> : null}
        {host && start && end ? (
          <div className="sample-queries">
            <p>Try these</p>
            {SAMPLE_QUERIES.map((q) => (
              <CopyLine key={q.expr} label={q.title} value={queryLink(thanos.url || "", start, end, q.expr)} />
            ))}
          </div>
        ) : null}
        {thanos.phase === "Configured" || thanos.phase === "Failed" ? (
          <Group gap={6} mt={8}>
            <Button size="xs" color="mint" c="#1B2430" onClick={() => void serve(thanos.name)}>Serve</Button>
          </Group>
        ) : null}
      </div>
    );
  };

  return (
    <aside className="export-rail glass">
      {note ? <TimedNote message={note} onClose={() => setNote("")} /> : null}
      <div className="export-head">
        <h3>Export</h3>
        {!form ? <IconButton label="Configure export" onClick={openForm}><Icon name="plus" /></IconButton> : null}
      </div>
      <div className="export-tabs">
        <button type="button" aria-pressed={tab === "thanos"} onClick={() => { setTab("thanos"); setForm(false); }}>Thanos</button>
        <button type="button" aria-pressed={tab === "agg"} onClick={() => { setTab("agg"); setForm(false); }}>Aggregation API</button>
      </div>
      <div className="export-body">
        {form ? (
          <Stack gap="sm">
            <SegmentedControl value={edge} onChange={setEdge} data={[{ label: "Start", value: "start" }, { label: "End", value: "end" }]} />
            {tab === "thanos" ? <EffortField value={effort} onChange={setEffort} /> : null}
            <TextInput label={edge === "end" ? "End" : "Start"} type="datetime-local" value={when} onChange={(e) => setWhen(e.currentTarget.value)} />
            {other ? <Text size="sm">{edge === "end" ? "Start" : "End"} {plain(other)} UTC. The window is the full generated duration.</Text> : null}
            {tab === "agg" ? (
              <>
                <Group align="flex-end" grow>
                  <NumberInput label="Aggregation" min={1} allowDecimal={false} value={amount} onChange={setAmount} />
                  <SegmentedControl value={unit} onChange={setUnit} data={[{ label: "min", value: "m" }, { label: "hours", value: "h" }, { label: "days", value: "d" }]} />
                </Group>
                <Text size="sm">A whole number of minutes, hours, or days. It has to be longer than the data step and an exact multiple of it. The session is removed after 24 hours.</Text>
              </>
            ) : null}
            <Group>
              <Button color="mint" c="#1B2430" onClick={() => void create()}>Create</Button>
              <Button variant="default" onClick={() => setForm(false)}>Cancel</Button>
            </Group>
          </Stack>
        ) : null}
        {tab === "thanos" ? thanosView() : (
          sessions.length === 0 && !form ? <p>No aggregation sessions. Configure one.</p> : (
            <div className="session-list">
              {sessions.map((row) => {
                const { start, end } = bounds(row);
                return (
                  <SessionCard
                    key={row.name}
                    dataset={dataset}
                    row={row}
                    start={start}
                    end={end}
                    onServe={() => void serve(row.name)}
                    onDelete={() => setDrop(row)}
                  />
                );
              })}
            </div>
          )
        )}
      </div>
      <Modal opened={drop !== null} onClose={() => setDrop(null)} title={drop?.type === "generic" ? "Delete session" : "Delete export"} centered>
        <Text size="sm">
          {drop?.type === "generic"
            ? `Remove session ${drop.sessionId || drop.name}? Other sessions stay, and the dataset stays.`
            : `Remove the Thanos export for ${dataset}? The dataset stays.`}
        </Text>
        <Group justify="flex-end" mt="md">
          <Button variant="default" onClick={() => setDrop(null)}>Cancel</Button>
          <Button color="pink" c="#1B2430" onClick={() => {
            const pending = drop;
            setDrop(null);
            if (pending) void remove(pending.name);
          }}>Delete</Button>
        </Group>
      </Modal>
    </aside>
  );
}
