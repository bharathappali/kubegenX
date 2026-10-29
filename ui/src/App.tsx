import { useEffect, useRef, useState } from "react";
import {
  Button,
  Group,
  Modal,
  NumberInput,
  Popover,
  Progress,
  SegmentedControl,
  Select,
  Stack,
  Text,
  TextInput,
  Title,
} from "@mantine/core";
import { api, type DatasetItem, type ExportItem, type StorageView } from "./api";
import { Icon, IconButton, EmptyMark } from "./icons";
import { EffortField } from "./effort";
import { datasetFromHash, Live } from "./live";
import { randomName } from "./names";
import { THEMES } from "./theme";

type Page = "wizard" | "status" | "datasets" | "storage";

const nav: { id: Exclude<Page, "status">; label: string; icon: string }[] = [
  { id: "wizard", label: "Create", icon: "plus" },
  { id: "datasets", label: "Datasets", icon: "grid" },
  { id: "storage", label: "Storage", icon: "storage" },
];

function hashPage(): string {
  return location.hash.replace(/^#\/?/, "").split("?")[0];
}

function pageFromHash(): Page {
  const id = hashPage();
  if (id === "live" || id === "status") return "status";
  if (id === "exports" || id === "datasets" || id === "sessions") return "datasets";
  if (id === "storage" || id === "wizard") return id;
  return "wizard";
}

function phaseWord(phase: string): string {
  switch (phase) {
    case "Generating":
      return "GENERATING";
    case "Ready":
      return "READY";
    case "Failed":
      return "FAILED";
    case "Building":
      return "BUILDING";
    case "Serving":
      return "SERVING";
    case "Pending":
    case "Planning":
    case "Queued":
      return "QUEUED";
    default:
      return phase ? "QUEUED" : "IDLE";
  }
}

const RANK = ["GENERATING", "BUILDING", "FAILED", "QUEUED", "SERVING", "READY", "IDLE"];

function fleetWord(page: Page, datasets: DatasetItem[], exports: ExportItem[]): string {
  if (datasets.some((d) => (d.phase ?? d.progress?.phase) === "Queued")) return "QUEUED";
  if (page === "status") {
    const name = datasetFromHash();
    const ds = datasets.find((d) => d.name === name);
    if (!ds) return datasets.length ? "QUEUED" : "IDLE";
    return phaseWord(ds.phase ?? ds.progress?.phase ?? "");
  }
  const words = [
    ...datasets.map((d) => phaseWord(d.phase ?? d.progress?.phase ?? "")),
    ...exports.map((e) => phaseWord(e.phase)),
  ];
  if (words.length === 0) return "IDLE";
  return RANK.find((word) => words.includes(word)) ?? "IDLE";
}

function toneFor(word: string): string {
  if (word === "FAILED") return "bad";
  if (word === "GENERATING" || word === "BUILDING" || word === "QUEUED") return "work";
  return "ok";
}

type Notice = { id: number; title: string; body: string; at: number };

let noticeSeq = 1;

function phaseOf(d: DatasetItem): string {
  return d.phase ?? d.progress?.phase ?? "";
}

export function App({ themeId, onTheme }: { themeId: number; onTheme: (id: number) => void }) {
  const [page, setPage] = useState<Page>(pageFromHash());
  const [hash, setHash] = useState(location.hash);
  const [notice, setNotice] = useState("");
  const [word, setWord] = useState("IDLE");
  const [notes, setNotes] = useState<Notice[]>([]);
  const [unread, setUnread] = useState(0);
  const [bell, setBell] = useState(false);
  const phases = useRef<Map<string, string> | null>(null);
  const bellOpen = useRef(false);
  const note = (message: string) => {
    setNotice(message);
    window.setTimeout(() => setNotice((current) => (current === message ? "" : current)), 4000);
  };
  useEffect(() => {
    const onHash = () => {
      const id = hashPage();
      if (id === "live") {
        const q = location.hash.includes("?") ? location.hash.slice(location.hash.indexOf("?")) : "";
        location.replace("#/status" + q);
        return;
      }
      if (id === "exports" || id === "sessions") {
        location.replace("#/datasets");
        return;
      }
      setPage(pageFromHash());
      setHash(location.hash);
    };
    if (hashPage() === "live") onHash();
    window.addEventListener("hashchange", onHash);
    return () => window.removeEventListener("hashchange", onHash);
  }, []);
  useEffect(() => {
    let stop = false;
    const pull = async () => {
      try {
        const [ds, ex] = await Promise.all([
          api<{ items: DatasetItem[] }>("/api/v1/datasets"),
          api<{ items: ExportItem[] }>("/api/v1/exports"),
        ]);
        if (stop) return;
        const items = ds.items ?? [];
        const fresh: Notice[] = [];
        const next = new Map<string, string>();
        for (const item of items) {
          const phase = phaseOf(item);
          next.set(item.name, phase);
          const before = phases.current?.get(item.name);
          if (!phases.current) {
            if (phase === "Queued") fresh.push({ id: noticeSeq++, title: `${item.name} is queued`, body: "Generation waits until the export finishes.", at: Date.now() });
            continue;
          }
          if (phase === "Queued" && before !== "Queued") {
            fresh.push({ id: noticeSeq++, title: `${item.name} is queued`, body: "Generation waits until the export finishes.", at: Date.now() });
          }
          if (before === "Queued" && phase && phase !== "Queued") {
            fresh.push({ id: noticeSeq++, title: `${item.name} resumed`, body: `Generation is ${phase.toLowerCase()}.`, at: Date.now() });
          }
        }
        phases.current = next;
        if (fresh.length) {
          setNotes((list) => [...fresh].reverse().concat(list).slice(0, 40));
          if (!bellOpen.current) setUnread((n) => n + fresh.length);
        }
        setWord(fleetWord(page, items, ex.items ?? []));
      } catch {
        /* keep the last status word */
      }
    };
    void pull();
    const id = window.setInterval(() => void pull(), 3000);
    return () => {
      stop = true;
      window.clearInterval(id);
    };
  }, [page, hash]);
  const go = (id: Exclude<Page, "status">) => {
    location.hash = "#/" + id;
  };
  return (
    <div className="app">
      <header className="topbar">
        <div className="tb-card glass brand-card">
          <span className="brand-mark" />
          <span className="brand">KubeGenX</span>
        </div>
        <nav className="tb-card glass tab-card">
          {nav.map((item) => (
            <button
              key={item.id}
              type="button"
              aria-current={page === item.id || (item.id === "datasets" && page === "status") ? "page" : undefined}
              onClick={() => go(item.id)}
            >
              <Icon name={item.icon} />
              <span>{item.label}</span>
            </button>
          ))}
        </nav>
        <div className="tb-card glass right-card">
          <div className="themes">
            {THEMES.map((theme) => (
              <button
                key={theme.id}
                type="button"
                className="theme-dot"
                aria-label={`Theme ${theme.id}`}
                title={`Theme ${theme.id}`}
                aria-pressed={themeId === theme.id}
                style={{ background: `conic-gradient(${theme.colors[0]} 0 25%, ${theme.colors[1]} 0 50%, ${theme.colors[2]} 0 75%, ${theme.colors[3]} 0 100%)` }}
                onClick={() => onTheme(theme.id)}
              />
            ))}
          </div>
          <Popover opened={bell} onChange={(v) => { setBell(v); bellOpen.current = v; if (v) setUnread(0); }} position="bottom-end" shadow="md" withinPortal>
            <Popover.Target>
              <button type="button" className="bell" aria-label="Notifications" aria-expanded={bell}>
                <Icon name="bell" />
                {unread > 0 ? <span className="bell-dot" /> : null}
              </button>
            </Popover.Target>
            <Popover.Dropdown className="note-dropdown">
              <div className="note-pop">
                <strong>Notifications</strong>
                {notes.length === 0 ? <p>No notifications yet.</p> : notes.map((item) => (
                  <div key={item.id} className="note-item">
                    <b>{item.title}</b>
                    <span>{item.body}</span>
                    <time>{new Date(item.at).toLocaleTimeString("en-GB", { hour: "2-digit", minute: "2-digit" })}</time>
                  </div>
                ))}
              </div>
            </Popover.Dropdown>
          </Popover>
          <span className="chip" data-tone={toneFor(word)}><span className="dot" />{word}</span>
        </div>
      </header>
      {notice ? <div className="notice glass">{notice}</div> : null}
      <div className="body">
        <main className={page === "wizard" ? "stage glass pane form-stage" : "stage glass pane"}>
          {page === "wizard" && <Wizard onNotice={note} />}
          {page === "status" && <Live onNotice={note} />}
          {page === "datasets" && <Datasets onNotice={note} />}
          {page === "storage" && <Storage />}
        </main>
        <div id="export-dock" className="export-dock" />
      </div>
    </div>
  );
}

function Empty({ title, text, create }: { title: string; text: string; create?: boolean }) {
  return (
    <div className="empty glass">
      <EmptyMark />
      <strong>{title}</strong>
      <p>{text}</p>
      {create ? <a className="create-link" href="#/wizard">Create data</a> : null}
    </div>
  );
}

function Wizard({ onNotice }: { onNotice: (s: string) => void }) {
  const [name, setName] = useState(randomName);
  const [mode, setMode] = useState("NamespaceMatrix");
  const [namespaces, setNamespaces] = useState<number | string>(3);
  const [perNs, setPerNs] = useState<number | string>(5);
  const [total, setTotal] = useState<number | string>(15);
  const [gpuPercent, setGpuPercent] = useState<number | string>(7);
  const [days, setDays] = useState<number | string>(1);
  const [step, setStep] = useState("15s");
  const [effort, setEffort] = useState("xhigh");
  const [busy, setBusy] = useState(false);
  const workloadCount = mode === "NamespaceMatrix" ? Number(namespaces) * Number(perNs) : Number(total);
  const percent = Number(gpuPercent) || 0;
  const gpuCount = percent <= 0 || workloadCount <= 0 ? 0 : Math.max(1, Math.round((percent / 100) * workloadCount));
  const submit = async () => {
    setBusy(true);
    try {
      const ratio = percent / 100;
      const spec = {
        seed: 424242,
        topology: {
          mode,
          totalWorkloads: Number(total),
          namespaces: mode === "TotalWorkloads"
            ? { count: 0, distribution: "Zipf", zipfS: 1 }
            : { count: Number(namespaces), distribution: "Uniform", zipfS: 0 },
          workloadsPerNamespace: Number(perNs),
          kinds: { Deployment: 0.8, StatefulSet: 0.2 },
          replicas: {
            buckets: [{ range: [1, 3], share: 70 }, { range: [4, 7], share: 22 }, { range: [8, 10], share: 8 }],
            statefulSetBuckets: [{ range: [1, 3], share: 85 }, { range: [4, 7], share: 15 }],
          },
          containersPerPod: { "1": 0.75, "2": 0.18, "3": 0.07 },
        },
        profiles: {
          cpu: { idle: 0.15, low: 0.4, medium: 0.3, high: 0.15 },
          memory: { low: 0.35, medium: 0.45, high: 0.2 },
          sizing: { rightSized: 0.45, over: 0.3, under: 0.1, noLimits: 0.1, bestEffort: 0.05 },
          behaviours: { steady: 0.3, diurnal: 0.4, bursty: 0.15, batchOnOff: 0.1, idleWindows: 0.05 },
          anomalies: { throttleAggressive: 0.08, memoryLeak: 0.03, oomLoop: 0.01, spikes: 0.1 },
        },
        gpu: { enabled: percent > 0, workloadRatio: ratio, nodePools: ["p4d.24xlarge"], migRatio: 0.3, labelProfile: "kruize" },
        nodes: { pools: [{ instanceType: "m7i.4xlarge", weight: 1 }], maxPodsPerNode: 250 },
        time: { step, duration: `${Number(days)}d`, chunk: "6h" },
        features: ["cpu", "memory", "gpu", "ksm"],
        workloadLabels: {
          scope: "namespace",
          keys: [
            { key: "org_id" },
            { key: "cluster_id" },
          ],
          carryOn: ["namespace", "ksm-pod", "ksm-container", "recorded", "cadvisor"],
        },
        workers: { cpu: "500m", memory: "1Gi", threads: 2 },
        workItem: { workloadsPerItem: 1024 },
      };
      await api("/api/v1/templates", { method: "POST", body: JSON.stringify({ name, spec }) });
      await api("/api/v1/datasets", { method: "POST", body: JSON.stringify({ name, template: name, effort, workers: { cpu: "500m", memory: "1Gi", threads: 2 } }) });
      location.hash = "#/status?name=" + encodeURIComponent(name);
    } catch (err) {
      onNotice(err instanceof Error ? err.message : "Could not create the dataset");
    } finally {
      setBusy(false);
    }
  };
  return (
    <div className="form-sheet">
    <Stack>
      <Title order={2}>Create</Title>
      <Group align="flex-end">
        <TextInput style={{ flex: 1 }} label="Name" value={name} onChange={(e) => setName(e.currentTarget.value)} />
        <IconButton label="New name" onClick={() => setName(randomName())}><Icon name="shuffle" /></IconButton>
      </Group>
      <div>
        <Text size="sm" fw={700} mb={6}>Shape</Text>
        <div className="tabs-center">
          <SegmentedControl
            value={mode}
            onChange={setMode}
            data={[
              { label: "By namespace", value: "NamespaceMatrix" },
              { label: "By workloads", value: "TotalWorkloads" },
            ]}
            color="slate"
          />
        </div>
      </div>
      {mode === "NamespaceMatrix" ? (
        <Group grow>
          <NumberInput label="Namespaces" value={namespaces} onChange={setNamespaces} min={1} />
          <NumberInput label="Workloads in each" value={perNs} onChange={setPerNs} min={1} />
        </Group>
      ) : (
        <NumberInput label="Workloads" description="Spread unevenly, so some namespaces have more than others." value={total} onChange={setTotal} min={1} />
      )}
      <Group grow align="flex-end">
        <NumberInput label="GPU workloads" description={`${percent}% · ${gpuCount} of ${workloadCount || 0}`} value={gpuPercent} onChange={setGpuPercent} min={0} max={100} suffix="%" />
        <NumberInput label="Days" value={days} onChange={setDays} min={1} />
        <Select
          label="Data interval"
          value={step}
          onChange={(v) => setStep(v ?? "15s")}
          data={[
            { value: "15s", label: "15 seconds" },
            { value: "30s", label: "30 seconds" },
            { value: "45s", label: "45 seconds" },
            { value: "60s", label: "60 seconds" },
          ]}
        />
      </Group>
      <EffortField value={effort} onChange={setEffort} />
      <Text c="dimmed" size="sm">Most workloads have 1–3 replicas. Usage is a mix of idle, low, medium, and high.</Text>
      <Button color="mint" c="#1B2430" onClick={() => void submit()} loading={busy}>Create dataset</Button>
    </Stack>
    </div>
  );
}

function exporterLabel(rows: ExportItem[]): string {
  let thanos = false;
  let generic = false;
  for (const row of rows) {
    if (row.phase !== "Serving" && row.phase !== "Building" && row.phase !== "Queued") continue;
    if (row.type === "generic") generic = true;
    else thanos = true;
  }
  if (thanos && generic) return "Thanos and Aggregation";
  if (thanos) return "Thanos";
  if (generic) return "Aggregation";
  return "No exporters";
}

function intervalLabel(step?: number): string {
  if (!step) return "—";
  return `${step}s`;
}

function dayLabel(days?: number): string {
  if (days == null || Number.isNaN(days)) return "—";
  const whole = Math.abs(days - Math.round(days)) < 0.05;
  const n = whole ? Math.round(days) : Math.round(days * 10) / 10;
  return `${n} ${n === 1 ? "day" : "days"}`;
}

function shortCount(n: number): string {
  const sign = n < 0 ? "-" : "";
  let abs = Math.abs(n);
  const units = ["", "k", "M", "B"];
  let i = 0;
  while (abs >= 1000 && i < units.length - 1) {
    abs /= 1000;
    i++;
  }
  if (i === 0) return n.toLocaleString();
  let rounded = abs >= 100 ? Math.round(abs) : Math.round(abs * 10) / 10;
  if (rounded >= 1000 && i < units.length - 1) {
    rounded /= 1000;
    i++;
    rounded = Math.round(rounded * 10) / 10;
  }
  const text = Number.isInteger(rounded) ? String(rounded) : rounded.toFixed(1);
  return sign + text + units[i];
}

function countLabel(n: number | undefined, one: string, many: string): string {
  if (n == null) return "—";
  return `${shortCount(n)} ${n === 1 ? one : many}`;
}

function openStatus(name: string) {
  location.hash = "#/status?name=" + encodeURIComponent(name);
}

function Datasets({ onNotice }: { onNotice: (s: string) => void }) {
  const [items, setItems] = useState<DatasetItem[]>([]);
  const [exports, setExports] = useState<ExportItem[]>([]);
  const [view, setView] = useState<"grid" | "table">("grid");
  const [loaded, setLoaded] = useState(false);
  const [drop, setDrop] = useState<string | null>(null);
  const load = async () => {
    const [ds, ex] = await Promise.all([
      api<{ items: DatasetItem[] }>("/api/v1/datasets"),
      api<{ items: ExportItem[] }>("/api/v1/exports"),
    ]);
    setItems(ds.items ?? []);
    setExports(ex.items ?? []);
    setLoaded(true);
  };
  useEffect(() => {
    void load().catch((e: Error) => onNotice(e.message));
    const id = window.setInterval(() => void load().catch(() => undefined), 3000);
    return () => window.clearInterval(id);
  }, []);
  const remove = async (name: string) => {
    try {
      await api("/api/v1/datasets/" + encodeURIComponent(name), { method: "DELETE" });
      await load();
    } catch (err) {
      onNotice(err instanceof Error ? err.message : "Could not delete the dataset");
    }
  };
  const facts = (d: DatasetItem) => (
    <>
      <div className="meta">{countLabel(d.namespaces, "namespace", "namespaces")} · {countLabel(d.workloads, "workload", "workloads")}</div>
      <div className="meta">
        <span title={d.samples?.toLocaleString()}>{countLabel(d.samples, "sample", "samples")}</span>
        {" · "}
        <span title={d.entries?.toLocaleString()}>{countLabel(d.entries, "entry", "entries")}</span>
      </div>
      <div className="meta">{dayLabel(d.days)} · {intervalLabel(d.stepSeconds)}</div>
    </>
  );
  const body = !loaded ? null : items.length === 0 ? (
    <div className="center-empty">
      <Empty title="No data is available" text="Create a dataset and it will show up here while it generates." create />
    </div>
  ) : view === "grid" ? (
    <div className="ds-grid">
      {items.map((d) => {
        const pct = d.progress?.percent ?? 0;
        const ph = d.phase ?? d.progress?.phase ?? "unknown";
        const active = exporterLabel(exports.filter((e) => e.dataset === d.name));
        return (
          <div key={d.name} className="ds-card glass">
            <button type="button" className="ds-open" onClick={() => openStatus(d.name)}>
              <span className="name">{d.name}</span>
              <div className="meta">{ph === "Queued" ? "Queued · waiting for the export to finish" : `${ph} · ${active}`}</div>
              {facts(d)}
              <div className="bar"><span style={{ width: `${Math.max(0, Math.min(100, pct))}%` }} /></div>
            </button>
            <button type="button" className="icon-btn ds-delete" aria-label={`Delete ${d.name}`} onClick={() => setDrop(d.name)}><Icon name="trash" /></button>
          </div>
        );
      })}
    </div>
  ) : (
    <div className="table-wrap">
      <table className="data">
        <thead><tr><th>Name</th><th>Phase</th><th>Exporter</th><th>Namespaces</th><th>Workloads</th><th>Samples</th><th>Entries</th><th>Days</th><th>Interval</th><th /></tr></thead>
        <tbody>
          {items.map((d) => (
            <tr key={d.name} onClick={() => openStatus(d.name)}>
              <td>{d.name}</td>
              <td>{(d.phase ?? d.progress?.phase) === "Queued" ? "Queued · waiting for the export" : (d.phase ?? d.progress?.phase)}</td>
              <td>{exporterLabel(exports.filter((e) => e.dataset === d.name))}</td>
              <td>{d.namespaces ?? "—"}</td>
              <td>{d.workloads ?? "—"}</td>
              <td title={d.samples?.toLocaleString()}>{d.samples == null ? "—" : shortCount(d.samples)}</td>
              <td title={d.entries?.toLocaleString()}>{d.entries == null ? "—" : shortCount(d.entries)}</td>
              <td>{dayLabel(d.days)}</td>
              <td>{intervalLabel(d.stepSeconds)}</td>
              <td>
                <button type="button" className="icon-btn row-delete" aria-label={`Delete ${d.name}`} onClick={(e) => { e.stopPropagation(); setDrop(d.name); }}><Icon name="trash" /></button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
  return (
    <div className="pane-fill">
      <Group justify="space-between" mb="md">
        <Title order={2}>Datasets</Title>
        <Group gap={6}>
          <IconButton label="Grid" pressed={view === "grid"} onClick={() => setView("grid")}><Icon name="grid" /></IconButton>
          <IconButton label="Table" pressed={view === "table"} onClick={() => setView("table")}><Icon name="table" /></IconButton>
        </Group>
      </Group>
      {body}
      <Modal opened={drop !== null} onClose={() => setDrop(null)} title="Delete dataset" centered>
        <Text size="sm">Remove {drop} and the data it generated?</Text>
        <Group justify="flex-end" mt="md">
          <Button variant="default" onClick={() => setDrop(null)}>Cancel</Button>
          <Button color="pink" c="#1B2430" onClick={() => {
            const name = drop;
            setDrop(null);
            if (name) void remove(name);
          }}>Delete</Button>
        </Group>
      </Modal>
    </div>
  );
}

function gib(n: number): string {
  return (n / 1024 ** 3).toFixed(3);
}

function Storage() {
  const [view, setView] = useState<StorageView | null>(null);
  useEffect(() => { void api<StorageView>("/api/v1/storage").then(setView); }, []);
  const cap = view?.capacityBytes ?? 1500 * 1024 * 1024 * 1024;
  const used = view?.usedBytes ?? 0;
  const pct = cap ? (used / cap) * 100 : 0;
  const empty = (view?.datasets ?? []).length === 0 && view;
  return (
    <div className="pane-fill">
      <Title order={2} mb="md">Storage</Title>
      <div className="glass" style={{ borderRadius: 18, padding: 16 }}>
        <Text>kubegenx-data · {(used / (1024 ** 3)).toFixed(3)} GiB of {(cap / (1024 ** 3)).toFixed(0)} GiB</Text>
        <Progress value={pct} color="pink" mt="sm" />
      </div>
      {empty ? (
        <div className="center-empty">
          <Empty title="No data is available" text="The volume stays empty until a dataset is created." create />
        </div>
      ) : view ? (
        <div className="table-wrap" style={{ marginTop: 14 }}>
          <table className="data">
            <thead><tr><th>Dataset</th><th>Phase</th><th>GiB</th><th>Reserved GiB</th></tr></thead>
            <tbody>
              {(view.datasets ?? []).map((d) => (
                <tr key={d.name}><td>{d.name}</td><td>{d.phase}</td><td>{gib(d.bytes)}</td><td>{gib(d.reserved)}</td></tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : null}
    </div>
  );
}
