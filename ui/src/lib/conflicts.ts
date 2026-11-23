import type { GroupEvent } from "../types/protocol";

export function overlaps(startA: string, endA: string, startB: string, endB: string) {
  return startA < endB && startB < endA;
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
