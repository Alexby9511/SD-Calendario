import type { Envelope, SyncEntry } from "../types/protocol";

export type OutboxStatus = "PENDING" | "SENT" | "STALE" | "409 Conflict" | "503 Retry" | "Rejected";

export interface OutboxItem {
  envelope: Envelope;
  state: OutboxStatus;
  attempts: number;
  error?: string;
  updatedAt: string;
}

const OUTBOX_KEY = "sd-calendar-outbox";
const CURSOR_PREFIX = "sd-calendar-sync-cursor";

function cursorKey(deployment: string, user: string): string {
  return `${CURSOR_PREFIX}:${encodeURIComponent(deployment)}:${encodeURIComponent(user)}`;
}

export function getOutbox(): OutboxItem[] {
  const raw = localStorage.getItem(OUTBOX_KEY);
  if (!raw) return [];
  try {
    const parsed: unknown = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [];
    return parsed.filter(
      (item): item is OutboxItem =>
        typeof item === "object" &&
        item !== null &&
        typeof (item as OutboxItem).envelope?.msg_id === "string"
    );
  } catch {
    // A corrupt value must not white-screen the app; drop it and start fresh.
    return [];
  }
}

export function enqueue(envelope: Envelope) {
  const items = getOutbox();
  if (!items.some((item) => item.envelope.msg_id === envelope.msg_id)) {
    items.push({
      envelope,
      state: "PENDING",
      attempts: 0,
      updatedAt: new Date().toISOString()
    });
  }
  localStorage.setItem(OUTBOX_KEY, JSON.stringify(items));
}

export function updateOutbox(msgId: string, patch: Partial<OutboxItem>) {
  const items = getOutbox().map((item) =>
    item.envelope.msg_id === msgId
      ? { ...item, ...patch, updatedAt: new Date().toISOString() }
      : item
  );
  localStorage.setItem(OUTBOX_KEY, JSON.stringify(items));
}

export function getCursor(deployment: string, user: string): number {
  const stored = Number(
    localStorage.getItem(cursorKey(deployment, user)) ?? "0"
  );
  return Number.isInteger(stored) && stored >= 0 ? stored : 0;
}

export function advanceCursor(
  entries: SyncEntry[],
  next: number,
  deployment: string,
  user: string
): number {
  let cursor = getCursor(deployment, user);
  if (!Number.isInteger(next) || next < cursor) throw new Error("Invalid sync cursor");
  if (entries.some((entry) => entry.index <= cursor || entry.index > next)) {
    throw new Error("Sync entry is outside the reconciled cursor range");
  }
  cursor = next;
  localStorage.setItem(cursorKey(deployment, user), String(cursor));
  return cursor;
}
