export type EnvelopeType =
  | "REQUEST" | "REPLY" | "COUNTER" | "DECLINECOUNTER" | "CANCEL"
  | "CLOSE_POLL" | "REOPEN" | "REANCHOR" | "BUSY_UPDATE" | "EVALUATE";

export type PartStat = "ACCEPTED" | "DECLINED" | "TENTATIVE" | "NEEDS_ACTION" | "STALE_ACCEPTED" | "STALE_DECLINED" | "STALE_TENTATIVE" | "LATE_ACCEPTED" | "LATE_DECLINED" | "LATE_TENTATIVE";
export type FsmState = "PROPOSED" | "CONFIRMED" | "NOT_CONFIRMED" | "EXPIRED" | "CANCELLED";

export interface Envelope {
  deployment_id: string;
  group_id: string;
  msg_id: string;
  type: EnvelopeType;
  uid: string;
  sequence: number;
  seen_sequence: number;
  sender_id: string;
  key_id: string;
  client_sent_at: string;
  signature: string;
  payload: Record<string, unknown>;
}

export interface CommandResponse {
  code: number;
  deduplicated: boolean;
  applied_index: number;
  result: Record<string, unknown>;
  detail?: Record<string, unknown>;
}

export interface SyncEntry {
  index: number;
  msg_id: string;
  command: Envelope;
  result: Record<string, unknown>;
}

export interface BusyWindow {
  start: string;
  end: string;
  visibility: "busy" | "details";
}

export interface GroupEvent {
  uid: string;
  sequence: number;
  state: FsmState;
  organizer: string;
  kind: "personal" | "group";
  participants: string[];
  payload: {
    title?: string;
    location?: string;
    start?: string;
    end?: string;
    timezone?: string;
    confirmation_rule?: "ALL" | "MAJORITY" | "QUORUM" | "ORGANIZER";
    quorum?: number;
    privacy?: "private" | "busy" | "details";
    deadline_utc?: string;
  };
}
