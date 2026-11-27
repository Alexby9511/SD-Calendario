import type { GroupEvent } from "../types/protocol";

function timestamp(value: string): number | null {
  const parsed = Date.parse(value.replace(" ", "T"));
  return Number.isNaN(parsed) ? null : parsed;
}

export function overlaps(startA: string, endA: string, startB: string, endB: string) {
  const aStart = timestamp(startA);
  const aEnd = timestamp(endA);
  const bStart = timestamp(startB);
  const bEnd = timestamp(endB);
  if (aStart === null || aEnd === null || bStart === null || bEnd === null) {
    return false;
  }
  return aStart < bEnd && bStart < aEnd;
}

export function localConflict(event: GroupEvent, personalEvents: GroupEvent[]) {
  const a = event.payload;
  if (!a.start || !a.end) return false;
  return personalEvents.some((personal) =>
    personal.kind === "personal" &&
    personal.payload.start &&
    personal.payload.end &&
    overlaps(a.start!, a.end!, personal.payload.start, personal.payload.end)
  );
}
