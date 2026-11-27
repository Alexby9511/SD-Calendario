import type { Envelope, CommandResponse, SyncPage } from "../types/protocol";

const API_BASE = "";

export interface AuthSession {
  user_id: string;
  access_token: string;
  token_type: "bearer";
}

async function readJson(response: Response): Promise<unknown> {
  try {
    return await response.json();
  } catch {
    return undefined;
  }
}

async function authRequest(path: string, user_id: string, password: string): Promise<AuthSession> {
  const response = await fetch(`${API_BASE}/v1/auth/${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ user_id, password })
  });
  const data = await readJson(response) as
    | {
        detail?: { reason?: string };
        access_token?: unknown;
        user_id?: unknown;
      }
    | undefined;
  if (!response.ok) {
    const reason = data?.detail?.reason;
    const messages: Record<string, string> = {
      invalid_user_id: "Usa un identificador de 3 a 32 caracteres: letras, números, punto, guion o guion bajo.",
      weak_password: "La contraseña debe tener al menos 8 caracteres.",
      user_id_taken: "Ese identificador ya está en uso.",
      invalid_credentials: "Identificador o contraseña incorrectos.",
      authentication_not_configured: "El backend todavía no está configurado para autenticar usuarios."
    };
    throw new Error(messages[reason ?? ""] ?? `No se pudo completar la autenticación (HTTP ${response.status}).`);
  }
  if (!data || typeof data.access_token !== "string" || typeof data.user_id !== "string") {
    throw new Error("El backend devolvió una respuesta inesperada.");
  }
  return { user_id: data.user_id, access_token: data.access_token, token_type: "bearer" };
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
  const data = await readJson(response) as { user_id?: unknown } | undefined;
  if (!data || typeof data.user_id !== "string") {
    throw new Error("El backend devolvió una respuesta inesperada.");
  }
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
  const data = await readJson(response);
  if (!response.ok) {
    if (!data || typeof data !== "object") {
      // Throwing keeps the outbox item retryable instead of parking it as a
      // permanent rejection when a proxy returns a non-JSON error page.
      throw new Error(`El backend devolvió una respuesta no JSON (HTTP ${response.status}).`);
    }
    const detail = (data as { detail?: unknown }).detail;
    return { ...((detail ?? data) as Record<string, unknown>), code: response.status } as CommandResponse;
  }
  if (!data || typeof data !== "object") {
    throw new Error("El backend devolvió una respuesta inesperada.");
  }
  return data as CommandResponse;
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
