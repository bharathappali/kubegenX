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

export interface Progress {
  phase?: string;
  percent?: number;
  items?: { total: number; done: number; failed: number; leased: number };
  bytes?: number;
}

export interface DatasetItem {
  name: string;
  phase?: string;
  progress?: Progress;
}

export interface ExportItem {
  name: string;
  dataset: string;
  phase: string;
  url?: string;
  blocks?: number;
  bytes?: number;
  startTime?: string;
}

export interface StorageView {
  capacityBytes: number;
  usedBytes: number;
  datasets: { name: string; bytes: number; reserved: number; phase: string }[];
  exports: ExportItem[];
}
