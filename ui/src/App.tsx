import { useEffect, useMemo, useRef, useState } from "react";
import {
  AppShell,
  Badge,
  Button,
  Card,
  Code,
  Container,
  Group,
  Modal,
  NavLink,
  NumberInput,
  Progress,
  Select,
  Stack,
  Switch,
  Table,
  Text,
  TextInput,
  Title,
} from "@mantine/core";
import { api, type DatasetItem, type ExportItem, type Progress as Prog, type StorageView } from "./api";

type Page = "wizard" | "live" | "datasets" | "exports" | "storage" | "sessions";

const nav: { id: Page; label: string }[] = [
  { id: "wizard", label: "Template" },
  { id: "live", label: "Live" },
  { id: "datasets", label: "Datasets" },
  { id: "exports", label: "Exports" },
  { id: "storage", label: "Storage" },
  { id: "sessions", label: "Sessions" },
];

function pageFromHash(): Page {
  const id = location.hash.replace("#/", "") as Page;
  return nav.some((n) => n.id === id) ? id : "wizard";
}

export function App() {
  const [page, setPage] = useState<Page>(pageFromHash());
  const [toast, setToast] = useState("");
  useEffect(() => {
    const onHash = () => setPage(pageFromHash());
    window.addEventListener("hashchange", onHash);
    return () => window.removeEventListener("hashchange", onHash);
  }, []);
  const go = (id: Page) => {
    location.hash = "#/" + id;
  };
  return (
    <AppShell header={{ height: 56 }} navbar={{ width: 200, breakpoint: "sm" }} padding="md" styles={{ main: { background: "#1F2A3A" }, header: { background: "#4D6787" }, navbar: { background: "#243044" } }}>
      <AppShell.Header>
        <Group h="100%" px="md" justify="space-between">
          <Title order={3} c="#F7F8FB">KubeGenX</Title>
          {toast ? <Badge color="butter" c="#1F2A3A">{toast}</Badge> : null}
        </Group>
      </AppShell.Header>
      <AppShell.Navbar p="sm">
        {nav.map((n) => (
          <NavLink key={n.id} label={n.label} active={page === n.id} onClick={() => go(n.id)} color="pink" />
        ))}
      </AppShell.Navbar>
      <AppShell.Main>
        <Container size="lg">
          {page === "wizard" && <Wizard onToast={setToast} />}
          {page === "live" && <Live onToast={setToast} />}
          {page === "datasets" && <Datasets onToast={setToast} />}
          {page === "exports" && <Exports onToast={setToast} />}
          {page === "storage" && <Storage />}
          {page === "sessions" && <Sessions />}
        </Container>
      </AppShell.Main>
    </AppShell>
  );
}

function Wizard({ onToast }: { onToast: (s: string) => void }) {
  const [name, setName] = useState("demo");
  const [mode, setMode] = useState("NamespaceMatrix");
  const [namespaces, setNamespaces] = useState<number | string>(3);
  const [perNs, setPerNs] = useState<number | string>(5);
  const [total, setTotal] = useState<number | string>(15);
  const [gpu, setGpu] = useState<number | string>(1);
  const [days, setDays] = useState<number | string>(1);
  const [step, setStep] = useState("15s");
  const [busy, setBusy] = useState(false);
  const submit = async () => {
    setBusy(true);
    try {
      const n = Number(mode === "NamespaceMatrix" ? Number(namespaces) * Number(perNs) : total);
      const ratio = n > 0 ? Number(gpu) / n : 0;
      const spec = {
        seed: 424242,
        topology: {
          mode,
          totalWorkloads: Number(total),
          namespaces: { count: Number(namespaces), distribution: "Uniform", zipfS: 0 },
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
        gpu: { enabled: Number(gpu) > 0, workloadRatio: ratio, nodePools: ["p4d.24xlarge"], migRatio: 0.3, labelProfile: "kruize" },
        nodes: { pools: [{ instanceType: "m7i.4xlarge", weight: 1 }], maxPodsPerNode: 250 },
        time: { step, duration: `${Number(days)}d`, chunk: "6h", epoch: "2026-10-05T00:00:00Z" },
        features: ["cpu", "memory", "gpu", "ksm"],
        workloadLabels: {
          scope: "namespace",
          keys: [
            { key: "org_id", values: [{ value: "acme-retail", weight: 3 }, { value: "globex", weight: 1 }] },
            { key: "cluster_id", values: [{ value: "ocp-east-1", weight: 2 }, { value: "ocp-west-2", weight: 1 }] },
          ],
          carryOn: ["namespace", "ksm-pod", "ksm-container", "recorded", "cadvisor"],
        },
        workers: { count: 2, cpu: "500m", memory: "1Gi", threads: 2 },
        workItem: { workloadsPerItem: 1024 },
      };
      await api("/api/v1/templates", { method: "POST", body: JSON.stringify({ name, spec }) });
      await api("/api/v1/datasets", { method: "POST", body: JSON.stringify({ name, template: name, epoch: "2026-10-05T00:00:00Z", workers: { count: 2, cpu: "500m", memory: "1Gi", threads: 2 } }) });
      onToast("dataset " + name + " queued");
      location.hash = "#/live";
    } catch (err) {
      onToast(err instanceof Error ? err.message : "failed");
    } finally {
      setBusy(false);
    }
  };
  return (
    <Stack>
      <Title order={2} c="#F7F8FB">Template wizard</Title>
      <TextInput label="Name" value={name} onChange={(e) => setName(e.currentTarget.value)} />
      <Select label="Topology" data={["NamespaceMatrix", "TotalWorkloads"]} value={mode} onChange={(v) => setMode(v ?? "NamespaceMatrix")} />
      {mode === "NamespaceMatrix" ? (
        <Group grow>
          <NumberInput label="Namespaces" value={namespaces} onChange={setNamespaces} min={1} />
          <NumberInput label="Workloads per namespace" value={perNs} onChange={setPerNs} min={1} />
        </Group>
      ) : (
        <NumberInput label="Total workloads" value={total} onChange={setTotal} min={1} />
      )}
      <Group grow>
        <NumberInput label="GPU workloads" value={gpu} onChange={setGpu} min={0} />
        <NumberInput label="Days" value={days} onChange={setDays} min={1} />
        <TextInput label="Step" value={step} onChange={(e) => setStep(e.currentTarget.value)} />
      </Group>
      <Text c="dimmed" size="sm">Replica buckets 70/22/8. Usage bands idle, low, medium, high. Labels org_id and cluster_id per namespace.</Text>
      <Button color="mint" c="#1F2A3A" onClick={submit} loading={busy}>Create dataset</Button>
    </Stack>
  );
}

function Live({ onToast }: { onToast: (s: string) => void }) {
  const [name, setName] = useState("demo");
  const [prog, setProg] = useState<Prog>({});
  const [bar, setBar] = useState(false);
  const canvas = useRef<HTMLCanvasElement>(null);
  const started = useRef<number>(Date.now());
  useEffect(() => {
    let stop = false;
    const pull = async () => {
      try {
        const p = await api<Prog>("/api/v1/datasets/" + encodeURIComponent(name) + "/progress");
        if (!stop) setProg(p);
      } catch {
        /* dataset may not exist yet */
      }
    };
    void pull();
    const id = setInterval(pull, 2000);
    const proto = location.protocol === "https:" ? "wss" : "ws";
    const ws = new WebSocket(`${proto}://${location.host}/api/v1/ws?topics=dataset:${encodeURIComponent(name)}`);
    ws.onmessage = (ev) => {
      const msg = JSON.parse(ev.data) as { phase?: string; kpi?: { percent?: number; itemsDone?: number; itemsTotal?: number; bytes?: number } };
      setProg((prev) => ({
        ...prev,
        phase: msg.phase ?? prev.phase,
        percent: msg.kpi?.percent ?? prev.percent,
        bytes: msg.kpi?.bytes ?? prev.bytes,
        items: {
          total: msg.kpi?.itemsTotal ?? prev.items?.total ?? 0,
          done: msg.kpi?.itemsDone ?? prev.items?.done ?? 0,
          failed: prev.items?.failed ?? 0,
          leased: prev.items?.leased ?? 0,
        },
      }));
      if (msg.phase === "Ready") onToast("generation complete");
    };
    return () => {
      stop = true;
      clearInterval(id);
      ws.close();
    };
  }, [name, onToast]);
  const total = prog.items?.total ?? 0;
  const done = prog.items?.done ?? 0;
  const pct = prog.percent ?? (total ? (done / total) * 100 : 0);
  const eta = useMemo(() => {
    const elapsed = (Date.now() - started.current) / 1000;
    if (pct <= 0 || pct >= 100) return pct >= 100 ? "0s" : "—";
    const left = elapsed * ((100 - pct) / pct);
    return Math.round(left) + "s";
  }, [pct]);
  useEffect(() => {
    const c = canvas.current;
    if (!c) return;
    const ctx = c.getContext("2d");
    if (!ctx) return;
    const cells = Math.max(total, 1);
    const cols = Math.min(24, cells);
    const rows = Math.ceil(cells / cols);
    c.width = cols * 22;
    c.height = rows * 22;
    ctx.clearRect(0, 0, c.width, c.height);
    for (let i = 0; i < cells; i++) {
      const x = (i % cols) * 22 + 10;
      const y = Math.floor(i / cols) * 22 + 10;
      ctx.beginPath();
      ctx.arc(x, y, 8, 0, Math.PI * 2);
      ctx.fillStyle = i < done ? "#7DCCAD" : "#4D6787";
      ctx.fill();
    }
  }, [total, done]);
  return (
    <Stack>
      <Title order={2} c="#F7F8FB">Live progress</Title>
      <TextInput label="Dataset" value={name} onChange={(e) => { started.current = Date.now(); setName(e.currentTarget.value); }} />
      <Group>
        <Badge color="pink">{prog.phase ?? "unknown"}</Badge>
        <Text c="#F7F8FB">{pct.toFixed(1)}%</Text>
        <Text c="#FFEA88">ETA {eta}</Text>
        <Text c="dimmed">{done}/{total} chunks</Text>
      </Group>
      <Switch label="Torrent bar" checked={bar} onChange={(e) => setBar(e.currentTarget.checked)} />
      {bar ? <Progress value={pct} color="mint" /> : <canvas ref={canvas} style={{ background: "#243044", borderRadius: 8 }} />}
    </Stack>
  );
}

function Datasets({ onToast }: { onToast: (s: string) => void }) {
  const [items, setItems] = useState<DatasetItem[]>([]);
  const load = () => api<{ items: DatasetItem[] }>("/api/v1/datasets").then((r) => setItems(r.items ?? [])).catch((e: Error) => onToast(e.message));
  useEffect(() => { void load(); }, []);
  return (
    <Stack>
      <Group justify="space-between"><Title order={2} c="#F7F8FB">Datasets</Title><Button variant="light" onClick={() => void load()}>Refresh</Button></Group>
      <Table striped highlightOnHover>
        <Table.Thead><Table.Tr><Table.Th>Name</Table.Th><Table.Th>Phase</Table.Th><Table.Th /></Table.Tr></Table.Thead>
        <Table.Tbody>
          {items.map((d) => (
            <Table.Tr key={d.name}>
              <Table.Td>{d.name}</Table.Td>
              <Table.Td>{d.phase ?? d.progress?.phase}</Table.Td>
              <Table.Td><Button size="xs" color="pink" onClick={async () => { await api("/api/v1/datasets/" + d.name, { method: "DELETE" }); onToast("deleting " + d.name); void load(); }}>Delete</Button></Table.Td>
            </Table.Tr>
          ))}
        </Table.Tbody>
      </Table>
    </Stack>
  );
}

function Exports({ onToast }: { onToast: (s: string) => void }) {
  const [items, setItems] = useState<ExportItem[]>([]);
  const [name, setName] = useState("oct");
  const [dataset, setDataset] = useState("demo");
  const [open, setOpen] = useState<ExportItem | null>(null);
  const [curl, setCurl] = useState("");
  const load = () => api<{ items: ExportItem[] }>("/api/v1/exports").then((r) => setItems(r.items ?? []));
  useEffect(() => { void load(); }, []);
  const create = async () => {
    await api("/api/v1/exports", { method: "POST", body: JSON.stringify({ name, dataset, startTime: "2026-10-05T00:00:00Z" }) });
    onToast("export " + name);
    void load();
  };
  const session = async (ex: ExportItem) => {
    const created = await api<{ sessionId: string; totals: { experiments: number; experimentPages: number; resultsPagesPerExperiment: number } }>(
      `/api/v1/datasets/${ex.dataset}/exporters/kruize/sessions`,
      { method: "POST", body: JSON.stringify({ name: ex.name, endTime: "2026-10-06T00:00:00Z" }) },
    );
    const base = `${location.origin}/api/v1/datasets/${ex.dataset}/exporters/kruize/sessions/${created.sessionId}`;
    setCurl([
      `curl -s '${base}/experiments?page=1&pageSize=100'`,
      `curl -s '${base}/experiments/<experiment>/results?page=0'`,
      `# ${created.totals.experiments} experiments, ${created.totals.experimentPages} pages, ${created.totals.resultsPagesPerExperiment} result pages`,
    ].join("\n"));
    setOpen(ex);
  };
  return (
    <Stack>
      <Title order={2} c="#F7F8FB">Exports</Title>
      <Group align="end">
        <TextInput label="Name" value={name} onChange={(e) => setName(e.currentTarget.value)} />
        <TextInput label="Dataset" value={dataset} onChange={(e) => setDataset(e.currentTarget.value)} />
        <Button color="mint" c="#1F2A3A" onClick={() => void create()}>Build</Button>
      </Group>
      <Table>
        <Table.Thead><Table.Tr><Table.Th>Name</Table.Th><Table.Th>Phase</Table.Th><Table.Th>Blocks</Table.Th><Table.Th>Route</Table.Th><Table.Th /></Table.Tr></Table.Thead>
        <Table.Tbody>
          {items.map((ex) => (
            <Table.Tr key={ex.name}>
              <Table.Td>{ex.name}</Table.Td>
              <Table.Td>{ex.phase}</Table.Td>
              <Table.Td>{ex.blocks ?? 0}</Table.Td>
              <Table.Td>{ex.url ? <a href={ex.url} style={{ color: "#7DCCAD" }}>{ex.url}</a> : "—"}</Table.Td>
              <Table.Td><Button size="xs" onClick={() => void session(ex)}>Session</Button></Table.Td>
            </Table.Tr>
          ))}
        </Table.Tbody>
      </Table>
      <Modal opened={open !== null} onClose={() => setOpen(null)} title="Kruize session">
        <Code block>{curl}</Code>
      </Modal>
    </Stack>
  );
}

function Storage() {
  const [view, setView] = useState<StorageView | null>(null);
  useEffect(() => { void api<StorageView>("/api/v1/storage").then(setView); }, []);
  const cap = view?.capacityBytes ?? 750 * 1024 * 1024 * 1024;
  const used = view?.usedBytes ?? 0;
  const pct = cap ? (used / cap) * 100 : 0;
  return (
    <Stack>
      <Title order={2} c="#F7F8FB">Storage</Title>
      <Card withBorder>
        <Text c="#F7F8FB">kubegenx-data · {(used / (1024 ** 3)).toFixed(3)} GiB of {(cap / (1024 ** 3)).toFixed(0)} GiB</Text>
        <Progress value={pct} color="pink" mt="sm" />
      </Card>
      <Table>
        <Table.Thead><Table.Tr><Table.Th>Dataset</Table.Th><Table.Th>Phase</Table.Th><Table.Th>Bytes</Table.Th><Table.Th>Reserved</Table.Th></Table.Tr></Table.Thead>
        <Table.Tbody>
          {(view?.datasets ?? []).map((d) => (
            <Table.Tr key={d.name}><Table.Td>{d.name}</Table.Td><Table.Td>{d.phase}</Table.Td><Table.Td>{d.bytes}</Table.Td><Table.Td>{d.reserved}</Table.Td></Table.Tr>
          ))}
        </Table.Tbody>
      </Table>
    </Stack>
  );
}

function Sessions() {
  const [dataset, setDataset] = useState("demo");
  const [rows, setRows] = useState<{ id: string; endTime: string; closed: boolean }[]>([]);
  const [page, setPage] = useState(1);
  const load = () => api<{ items: { id: string; endTime: string; closed: boolean }[] }>(`/api/v1/datasets/${dataset}/exporters/kruize/sessions`).then((r) => setRows(r.items ?? []));
  const pageSize = 10;
  const slice = rows.slice((page - 1) * pageSize, page * pageSize);
  return (
    <Stack>
      <Title order={2} c="#F7F8FB">Kruize sessions</Title>
      <Group>
        <TextInput label="Dataset" value={dataset} onChange={(e) => setDataset(e.currentTarget.value)} />
        <Button mt={24} onClick={() => void load()}>Load</Button>
      </Group>
      <Table>
        <Table.Thead><Table.Tr><Table.Th>Session</Table.Th><Table.Th>End</Table.Th><Table.Th>Closed</Table.Th><Table.Th>Curl</Table.Th></Table.Tr></Table.Thead>
        <Table.Tbody>
          {slice.map((s) => (
            <Table.Tr key={s.id}>
              <Table.Td>{s.id}</Table.Td>
              <Table.Td>{s.endTime}</Table.Td>
              <Table.Td>{String(s.closed)}</Table.Td>
              <Table.Td><Code>{`curl -s '${location.origin}/api/v1/datasets/${dataset}/exporters/kruize/sessions/${s.id}/experiments?page=1&pageSize=100'`}</Code></Table.Td>
            </Table.Tr>
          ))}
        </Table.Tbody>
      </Table>
      <Group>
        <Button variant="default" disabled={page <= 1} onClick={() => setPage(page - 1)}>Prev</Button>
        <Text c="#F7F8FB">page {page}</Text>
        <Button variant="default" disabled={page * pageSize >= rows.length} onClick={() => setPage(page + 1)}>Next</Button>
      </Group>
    </Stack>
  );
}
