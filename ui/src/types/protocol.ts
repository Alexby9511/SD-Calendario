export type EnvelopeType =
  | "REQUEST" | "REPLY" | "COUNTER" | "DECLINECOUNTER" | "CANCEL"
  | "CLOSE_POLL" | "REOPEN" | "REANCHOR" | "BUSY_UPDATE" | "SHARE_CALENDAR" | "EVALUATE";

export type PartStat = "ACCEPTED" | "DECLINED" | "TENTATIVE" | "NEEDS_ACTION" | "STALE_ACCEPTED" | "STALE_DECLINED" | "STALE_TENTATIVE" | "LATE_ACCEPTED" | "LATE_DECLINED" | "LATE_TENTATIVE";
export type FsmState = "PROPOSED" | "CONFIRMED" | "NOT_CONFIRMED" | "EXPIRED" | "CANCELLED";
export type SharedCalendarId = "personal" | "group" | (string & {});
export type CalendarSharePermission = "freebusy" | "details" | "edit";

export interface CalendarShare {
  owner: string;
  calendar: SharedCalendarId;
  users: string[];
  permission: CalendarSharePermission;
}

export interface CustomCalendar {
  id: string;
  name: string;
  color: string;
  visibility: "private" | "public";
  visible: boolean;
}

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
  event?: GroupEvent;
  attendance?: Record<string, string>;
  counters?: Record<string, { proposed_start?: string; proposed_end?: string }>;
}

export interface SyncPage {
  entries: SyncEntry[];
  next: number;
  as_of: number;
  snapshot?: {
    events: Record<string, GroupEvent>;
    attendance: Record<string, Record<string, string>>;
    counters: Record<string, Record<string, { proposed_start?: string; proposed_end?: string }>>;
    calendar_shares: CalendarShare[];
    applied_index: number;
  };
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
    description?: string;
    calendar_id?: string;
    location?: string;
    start?: string;
    end?: string;
    timezone?: string;
    confirmation_rule?: "ALL" | "MAJORITY" | "QUORUM" | "ORGANIZER";
    quorum?: number;
    privacy?: "private" | "public" | "busy" | "details";
    visibility?: "private" | "public";
    availability?: "busy" | "free";
    hide_from_freebusy?: boolean;
    deadline_utc?: string;
  };
}
