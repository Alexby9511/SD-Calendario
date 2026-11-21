import { useEffect, useMemo, useState } from "react";
import { TopBar } from "./components/TopBar";
import { Sidebar } from "./components/Sidebar";
import { CalendarGrid } from "./components/CalendarGrid";
import { EventModal } from "./components/EventModal";
import { EventDetailModal } from "./components/EventDetailModal";
import { DistributedInspector } from "./components/DistributedInspector";
import { ulid } from "./lib/canonical";
import { ensureDeviceKeys, keyIdFor, signEnvelope } from "./lib/crypto";
import { getCursor, getOutbox, reconcile, enqueue, updateOutbox } from "./lib/outbox";
import { applyCommand, availability, getStatus, sync as syncServer } from "./lib/api";
import type { Envelope, EnvelopeType, GroupEvent, SyncEntry } from "./types/protocol";

const EVENTS_KEY = "sd-calendar-events";

export default function App() {
  const [user, setUser] = useState("alice");
  const [group, setGroup] = useState("grupo-core");
  const [deployment] = useState("local");
  const [date, setDate] = useState(() => new Date());
  const [view, setView] = useState<"Día" | "Semana" | "Mes" | "Año" | "Agenda">("Semana");
  const [navDirection, setNavDirection] = useState<"next" | "prev">("next");
  const [offline, setOffline] = useState(false);
  const [online, setOnline] = useState(false);
  const [createOpen, setCreateOpen] = useState(false);
  const [createMode, setCreateMode] = useState<"personal" | "group">("personal");
  const [detailEvent, setDetailEvent] = useState<GroupEvent | null>(null);
  const [showCancelled, setShowCancelled] = useState(false);
  const [busyUsers, setBusyUsers] = useState(["bob"]);
  const [availabilityMap, setAvailabilityMap] = useState<Record<string, { start: string; end: string }[]>>({});
  const [events, setEvents] = useState<GroupEvent[]>(() => {
    try {
      return JSON.parse(localStorage.getItem(EVENTS_KEY) ?? "[]");
    } catch {
      return [];
    }
  });
  const [outbox, setOutbox] = useState(getOutbox());
  const [cursor, setCursor] = useState(getCursor());
  const [entries, setEntries] = useState<SyncEntry[]>([]);
  const [inspectorOpen, setInspectorOpen] = useState(false);
  const [sidebarOpen, setSidebarOpen] = useState(true);
  const [theme, setTheme] = useState<"dark" | "light">(
    () => (localStorage.getItem("sd-calendar-theme") as "dark" | "light") ?? "dark"
  );
  const [statusIndex, setStatusIndex] = useState(0);
  const [attendance, setAttendance] = useState<Record<string, string>>({ bob: "ACCEPTED", carol: "NEEDS_ACTION" });

  const personalEvents = useMemo(() => events.filter((event) => event.kind === "personal"), [events]);

  useEffect(() => {
    document.documentElement.dataset.theme = theme;
    localStorage.setItem("sd-calendar-theme", theme);
  }, [theme]);

  useEffect(() => {
    localStorage.setItem(EVENTS_KEY, JSON.stringify(events));
  }, [events]);

  useEffect(() => {
    ensureDeviceKeys();
    const refresh = async () => {
      if (offline) return;
      try {
        const status = await getStatus();
        setOnline(true);
        setStatusIndex(status.status.applied_index ?? 0);
      } catch {
        setOnline(false);
      }
    };
    refresh();
    const timer = window.setInterval(refresh, 5000);
    return () => window.clearInterval(timer);
  }, [offline]);

  useEffect(() => {
    const loadAvailability = async () => {
      if (offline) return;
      try {
        const result = await availability(busyUsers);
        setAvailabilityMap(result.users);
      } catch {
        // The UI remains usable from the local projection.
      }
    };
    loadAvailability();
  }, [offline, busyUsers]);

  const title = date.toLocaleDateString("es", { month: "long", year: "numeric" });

  const openCreate = (mode: "personal" | "group" = "personal") => {
    setCreateMode(mode);
    setCreateOpen(true);
  };

  const buildEnvelope = (type: EnvelopeType, uid: string, sequence: number, payload: Record<string, unknown>): Promise<Envelope> =>
    signEnvelope({
      deployment_id: deployment,
      group_id: group,
      msg_id: ulid(),
      type,
      uid,
      sequence,
      seen_sequence: cursor,
      sender_id: user,
      key_id: keyIdFor(user),
      client_sent_at: new Date().toISOString(),
      payload
    });

  const syncNow = async () => {
    setOutbox(getOutbox());
    if (offline) return;
    try {
      const page = await syncServer(cursor, 100);
      const next = reconcile(page.entries);
      setCursor(next);
      setEntries(page.entries);
      for (const entry of page.entries) {
        const command = entry.command;
        const payload = command.payload as Record<string, any>;
        if (command.type === "REQUEST") {
          setEvents((current) => {
            const existing = current.find((event) => event.uid === command.uid);
            const kind = payload.kind === "personal" ? "personal" : "group";
            const event: GroupEvent = {
              uid: command.uid,
              sequence: command.sequence,
              state: kind === "personal" ? "CONFIRMED" : "PROPOSED",
              organizer: command.sender_id,
              kind,
              participants: (payload.participants ?? []).map((participant: any) =>
                typeof participant === "string" ? participant : participant.user_id
              ),
              payload: {
                title: payload.title,
                location: payload.location,
                start: payload.start,
                end: payload.end,
                privacy: payload.privacy,
                confirmation_rule: payload.confirmation_rule,
                quorum: payload.quorum
              }
            };
            return existing
              ? current.map((item) => item.uid === command.uid ? event : item)
              : [...current, event];
          });
        } else if (command.type === "CANCEL") {
          setEvents((current) =>
            current.map((event) => event.uid === command.uid ? { ...event, state: "CANCELLED" } : event)
          );
        }
      }
      for (const item of getOutbox()) {
        if (item.state === "PENDING") {
          try {
            const response = await applyCommand(item.envelope);
            if (response.code === 200) updateOutbox(item.envelope.msg_id, { state: "SENT" });
            else if (response.code === 409) updateOutbox(item.envelope.msg_id, { state: "409 Conflict", error: String(response.result?.reason ?? "") });
            else if (response.code === 503) updateOutbox(item.envelope.msg_id, { state: "503 Retry", attempts: item.attempts + 1 });
          } catch {
            updateOutbox(item.envelope.msg_id, { state: "503 Retry", attempts: item.attempts + 1 });
          }
        }
      }
      setOutbox(getOutbox());
    } catch {
      setOnline(false);
    }
  };

  const queueEnvelope = async (envelope: Envelope) => {
    enqueue(envelope);
    setOutbox(getOutbox());
    if (!offline) await syncNow();
  };

  const savePersonal = async (value: { title: string; start: string; end: string; tzid: string; privacy: "private" | "busy" | "details"; hide: boolean }) => {
    const uid = `${user}:personal:${ulid()}`;
    const envelope = await buildEnvelope("REQUEST", uid, 1, {
      kind: "personal",
      title: value.title,
      start: value.start,
      end: value.end,
      tzid: value.tzid,
      privacy: value.privacy,
      hide_from_freebusy: value.hide
    });
    setEvents((current) => [...current, {
      uid,
      sequence: 1,
      state: "CONFIRMED",
      organizer: user,
      kind: "personal",
      participants: [],
      payload: { title: value.title, start: value.start, end: value.end, privacy: value.privacy }
    }]);
    await queueEnvelope(envelope);
    if (!value.hide) {
      const busy = await buildEnvelope("BUSY_UPDATE", uid, 1, { pub_seq: 1, windows: [{ start: value.start, end: value.end, visibility: "busy" }] });
      await queueEnvelope(busy);
    }
    setCreateOpen(false);
  };

  const saveGroup = async (value: { title: string; location: string; start: string; end: string; participants: { user_id: string; role: "REQUIRED" | "OPTIONAL" }[]; rule: "ALL" | "MAJORITY" | "QUORUM" | "ORGANIZER"; quorum: number }) => {
    const uid = `${user}:meeting:${ulid()}`;
    const envelope = await buildEnvelope("REQUEST", uid, 1, {
      kind: "group",
      title: value.title,
      location: value.location,
      start: value.start,
      end: value.end,
      tzid: "America/New_York",
      participants: value.participants,
      confirmation_rule: value.rule,
      quorum: value.quorum
    });
    setEvents((current) => [...current, {
      uid,
      sequence: 1,
      state: "PROPOSED",
      organizer: user,
      kind: "group",
      participants: value.participants.map((participant) => participant.user_id),
      payload: { title: value.title, location: value.location, start: value.start, end: value.end, confirmation_rule: value.rule, quorum: value.quorum }
    }]);
    await queueEnvelope(envelope);
    setCreateOpen(false);
  };

  const sendReply = async (partstat: "ACCEPTED" | "DECLINED" | "TENTATIVE") => {
    if (!detailEvent) return;
    const envelope = await buildEnvelope("REPLY", detailEvent.uid, detailEvent.sequence, { partstat });
    setAttendance((current) => ({ ...current, [user]: partstat }));
    await queueEnvelope(envelope);
  };

  const sendSimpleCommand = async (type: "COUNTER" | "CANCEL" | "CLOSE_POLL" | "REOPEN") => {
    if (!detailEvent) return;
    const payload = type === "COUNTER"
      ? { proposed_start: "2026-11-23 12:00", proposed_end: "2026-11-23 13:00" }
      : type === "REOPEN"
        ? { extend_deadline: true, reask: true }
        : {};
    const envelope = await buildEnvelope(type, detailEvent.uid, detailEvent.sequence, payload);
    await queueEnvelope(envelope);
  };

  const deleteEvent = async () => {
    if (!detailEvent) return;
    if (detailEvent.kind === "personal") {
      setEvents((current) => current.filter((event) => event.uid !== detailEvent.uid));
    } else {
      await sendSimpleCommand("CANCEL");
      setEvents((current) =>
        current.map((event) => event.uid === detailEvent.uid ? { ...event, state: "CANCELLED" } : event)
      );
    }
    setDetailEvent(null);
  };

  const moveDate = (direction: -1 | 1) => {
    setNavDirection(direction === 1 ? "next" : "prev");
    setDate((current) => {
      const next = new Date(current);
      if (view === "Día") {
        next.setDate(next.getDate() + direction);
      } else if (view === "Semana" || view === "Agenda") {
        next.setDate(next.getDate() + direction * 7);
      } else if (view === "Mes") {
        const day = next.getDate();
        next.setDate(1);
        next.setMonth(next.getMonth() + direction);
        const lastDay = new Date(next.getFullYear(), next.getMonth() + 1, 0).getDate();
        next.setDate(Math.min(day, lastDay));
      } else if (view === "Año") {
        next.setFullYear(next.getFullYear() + direction);
      }
      return next;
    });
  };

  return (
    <div className={`app-shell ${sidebarOpen ? "" : "sidebar-collapsed"}`}>
      <TopBar
        user={user} group={group} keyId={keyIdFor(user)}
        offline={offline} view={view} title={title}
        pending={outbox.filter((item) => item.state !== "SENT").length}
        cursor={cursor} online={online} theme={theme}
        onUser={setUser} onGroup={setGroup} onOffline={setOffline}
        onView={(nextView) => {
          setNavDirection("next");
          setView(nextView);
        }}
        onToday={() => {
          setNavDirection("prev");
          setDate(new Date());
        }}
        onMove={moveDate}
        onSync={syncNow}
        onMenu={() => setSidebarOpen((open) => !open)}
        onTheme={() => setTheme((current) => current === "dark" ? "light" : "dark")}
        onInspector={() => setInspectorOpen(true)}
      />
      <div className="workspace">
        <Sidebar date={date} events={events} busyUsers={busyUsers} onBusyUsers={setBusyUsers}
          onCreate={openCreate} onSelectEvent={setDetailEvent}
          showCancelled={showCancelled} onShowCancelled={setShowCancelled} view={view}
          onMoveMonth={(direction) => setDate((current) => {
            const next = new Date(current);
            const day = next.getDate();
            next.setDate(1);
            next.setMonth(next.getMonth() + direction);
            const last = new Date(next.getFullYear(), next.getMonth() + 1, 0).getDate();
            next.setDate(Math.min(day, last));
            return next;
          })} />
        <div className={`calendar-transition calendar-${navDirection}`} key={`${view}-${date.toISOString().slice(0, 10)}`}>
          <CalendarGrid view={view} date={date} events={events} personalEvents={personalEvents}
            busyUsers={busyUsers} availability={availabilityMap} showCancelled={showCancelled}
            onSelect={setDetailEvent} onQuickCreate={() => openCreate()}
            onNavigateDate={(nextDate, nextView = "Día") => {
              setNavDirection("next");
              setDate(nextDate);
              setView(nextView);
            }} />
        </div>
      </div>
      <DistributedInspector open={inspectorOpen} outbox={outbox} entries={entries} cursor={cursor}
        onToggle={() => setInspectorOpen((open) => !open)} />
      <EventModal open={createOpen} initialMode={createMode} user={user} availability={availabilityMap}
        onClose={() => setCreateOpen(false)} onSavePersonal={savePersonal} onSaveGroup={saveGroup} />
      <EventDetailModal event={detailEvent} attendance={attendance}
        onClose={() => setDetailEvent(null)} onReply={sendReply}
        onCounter={() => sendSimpleCommand("COUNTER")}
        onClosePoll={() => sendSimpleCommand("CLOSE_POLL")}
        onReopen={() => sendSimpleCommand("REOPEN")}
        onCancel={() => sendSimpleCommand("CANCEL")}
        onDelete={deleteEvent} />
    </div>
  );
}
