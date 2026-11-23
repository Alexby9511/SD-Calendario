import type { Envelope, CommandResponse, SyncPage } from "../types/protocol";

const API_BASE = "";

export interface AuthSession {
  user_id: string;
  access_token: string;
  token_type: "bearer";
}

async function authRequest(path: string, user_id: string, password: string): Promise<AuthSession> {
  const response = await fetch(`${API_BASE}/v1/auth/${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ user_id, password })
  });
  const data = await response.json();
  if (!response.ok) {
    const reason = data.detail?.reason;
    const messages: Record<string, string> = {
      invalid_user_id: "Usa un identificador de 3 a 32 caracteres: letras, números, punto, guion o guion bajo.",
      weak_password: "La contraseña debe tener al menos 8 caracteres.",
      user_id_taken: "Ese identificador ya está en uso.",
      invalid_credentials: "Identificador o contraseña incorrectos.",
      authentication_not_configured: "El backend todavía no está configurado para autenticar usuarios."
    };
    throw new Error(messages[reason] ?? "No se pudo completar la autenticación. Inténtalo de nuevo.");
  }
  return data as AuthSession;
}

export function register(user_id: string, password: string): Promise<AuthSession> {
  return authRequest("register", user_id, password);
}

export function login(user_id: string, password: string): Promise<AuthSession> {
  return authRequest("login", user_id, password);
}

export async function validateSession(token: string): Promise<string> {
  const response = await fetch(`${API_BASE}/v1/auth/me`, {
    headers: authHeaders(token)
  });
  if (!response.ok) throw new Error("Tu sesión venció. Inicia sesión de nuevo.");
  const data = await response.json() as { user_id: string };
  return data.user_id;
}

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

export async function sync(
  token: string,
  since = 0,
  limit = 100,
  includeSnapshot = true
): Promise<SyncPage> {
  const params = new URLSearchParams({
    since: String(since),
    limit: String(limit),
    include_snapshot: String(includeSnapshot)
  });
  const response = await fetch(`${API_BASE}/v1/sync?${params}`, {
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
