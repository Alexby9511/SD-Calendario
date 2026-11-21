import type { Envelope, SyncEntry } from "../types/protocol";

export type OutboxStatus = "PENDING" | "SENT" | "409 Conflict" | "503 Retry";

export interface OutboxItem {
  envelope: Envelope;
  state: OutboxStatus;
  attempts: number;
  error?: string;
  updatedAt: string;
}

const OUTBOX_KEY = "sd-calendar-outbox";
const CURSOR_KEY = "sd-calendar-sync-cursor";

export function getOutbox(): OutboxItem[] {
  return JSON.parse(localStorage.getItem(OUTBOX_KEY) ?? "[]");
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

export function getCursor(): number {
  return Number(localStorage.getItem(CURSOR_KEY) ?? "0");
}

export function reconcile(entries: SyncEntry[]): number {
  let cursor = getCursor();
  for (const entry of entries) {
    if (entry.index !== cursor + 1) throw new Error("Sync entries are not contiguous");
    cursor = entry.index;
  }
  localStorage.setItem(CURSOR_KEY, String(cursor));
  return cursor;
}
