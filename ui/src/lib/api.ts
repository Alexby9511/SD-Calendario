import type { Envelope, CommandResponse, SyncEntry } from "../types/protocol";

const API_BASE = "";

export async function applyCommand(command: Envelope): Promise<CommandResponse> {
  const response = await fetch(`${API_BASE}/v1/commands`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(command)
  });
  const data = await response.json();
  if (!response.ok) return { ...(data.detail ?? data), code: response.status };
  return data;
}

export async function sync(since = 0, limit = 100) {
  const response = await fetch(`${API_BASE}/v1/sync?since=${since}&limit=${limit}`);
  if (!response.ok) throw new Error(`Sync failed: ${response.status}`);
  return await response.json() as { entries: SyncEntry[]; next: number; as_of: number };
}

export async function availability(users: string[], start?: string, end?: string) {
  const params = new URLSearchParams();
  if (users.length) params.set("users", users.join(","));
  if (start) params.set("start", start);
  if (end) params.set("end", end);
  const response = await fetch(`${API_BASE}/v1/availability?${params}`);
  if (!response.ok) throw new Error(`Availability failed: ${response.status}`);
  return await response.json() as { users: Record<string, { start: string; end: string; visibility: string }[]>; as_of: number };
}

export async function getStatus() {
  const [statusResponse, healthResponse] = await Promise.all([
    fetch(`${API_BASE}/v1/status`),
    fetch(`${API_BASE}/health`)
  ]);
  if (!statusResponse.ok || !healthResponse.ok) throw new Error("Backend unavailable");
  return {
    status: await statusResponse.json(),
    health: await healthResponse.json()
  };
}
