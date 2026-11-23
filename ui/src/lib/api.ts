import type { Envelope, CommandResponse, SyncPage } from "../types/protocol";

const API_BASE = "";

function authHeaders(token: string): HeadersInit {
  if (!token.trim()) throw new Error("Configura el token Bearer de este usuario");
  return { Authorization: `Bearer ${token.trim()}` };
}

export async function applyCommand(command: Envelope, token: string): Promise<CommandResponse> {
  const response = await fetch(`${API_BASE}/v1/commands`, {
    method: "POST",
    headers: { ...authHeaders(token), "Content-Type": "application/json" },
    body: JSON.stringify(command)
  });
  const data = await response.json();
  if (!response.ok) return { ...(data.detail ?? data), code: response.status };
  return data;
}

export async function sync(token: string, since = 0, limit = 100): Promise<SyncPage> {
  const response = await fetch(`${API_BASE}/v1/sync?since=${since}&limit=${limit}`, {
    headers: authHeaders(token)
  });
  if (!response.ok) throw new Error(`Sync failed: ${response.status}`);
  return await response.json() as SyncPage;
}

export async function availability(users: string[], token: string, start?: string, end?: string) {
  const params = new URLSearchParams();
  if (users.length) params.set("users", users.join(","));
  if (start) params.set("start", start);
  if (end) params.set("end", end);
  const response = await fetch(`${API_BASE}/v1/availability?${params}`, {
    headers: authHeaders(token)
  });
  if (!response.ok) throw new Error(`Availability failed: ${response.status}`);
  return await response.json() as { users: Record<string, "busy" | "available">; as_of: number };
}

export async function getStatus(token: string) {
  const [statusResponse, healthResponse] = await Promise.all([
    fetch(`${API_BASE}/v1/status`, { headers: authHeaders(token) }),
    fetch(`${API_BASE}/health`)
  ]);
  if (!statusResponse.ok || !healthResponse.ok) throw new Error("Backend unavailable");
  return {
    status: await statusResponse.json(),
    health: await healthResponse.json()
  };
}
