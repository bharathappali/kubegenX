export async function api<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(path, {
    ...init,
    headers: { "Content-Type": "application/json", ...(init?.headers ?? {}) },
  });
  const text = await res.text();
  const body = text ? JSON.parse(text) : {};
  if (!res.ok) {
    const reason = (body as { reason?: string; error?: string }).reason ?? (body as { error?: string }).error ?? res.statusText;
    throw new Error(reason);
  }
  return body as T;
}

export interface Cell {
  chunk: number;
  state: string;
  wlLo: number;
  wlHi: number;
}

export interface Progress {
  name?: string;
  phase?: string;
  percent?: number;
  items?: { total: number; done: number; failed: number; leased: number };
  bytes?: number;
  startedAt?: number;
  finishedAt?: number;
  cells?: Cell[];
  kpi?: { percent?: number; itemsDone?: number; itemsTotal?: number; itemsLeased?: number; itemsFailed?: number; bytes?: number };
}

export interface LayoutWorkload {
  id: number;
  name: string;
  kind: string;
  gpu: boolean;
  replicas: number;
}

export interface LayoutNamespace {
  id: number;
  name: string;
  wlLo: number;
  wlHi: number;
  count: number;
  workloads?: LayoutWorkload[];
}

export interface WorkloadPageItem {
  id: number;
  namespaceId: number;
  namespace: string;
  name: string;
  kind: string;
  replicas: number;
  gpu: boolean;
}

export interface Layout {
  chunks: number;
  chunkSeconds: number;
  stepSeconds: number;
  epoch?: string;
  workloads: number;
  namespaces: LayoutNamespace[];
}

export interface DatasetItem {
  name: string;
  phase?: string;
  progress?: Progress;
  namespaces?: number;
  workloads?: number;
  samples?: number;
  entries?: number;
  days?: number;
  stepSeconds?: number;
}

export interface ExportItem {
  name: string;
  dataset: string;
  phase: string;
  type?: string;
  sessionId?: string;
  anchor?: string;
  startTime?: string;
  endTime?: string;
  interval?: string;
  url?: string;
  blocks?: number;
  blocksTotal?: number;
  bytes?: number;
  workers?: number;
  heap?: number;
  budget?: number;
  startedAt?: number;
  finishedAt?: number;
  createdAt?: number;
}

export interface ExportBlock {
  id: string;
  state: string;
  stepLo?: number;
  stepHi?: number;
}

export interface StorageView {
  capacityBytes: number;
  usedBytes: number;
  datasets: { name: string; bytes: number; reserved: number; phase: string }[];
  exports: ExportItem[];
}
