import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { TopBar } from "./components/TopBar";
import { Sidebar } from "./components/Sidebar";
import { CalendarGrid } from "./components/CalendarGrid";
import { EventModal } from "./components/EventModal";
import type { CalendarShare, CalendarSharePermission, CustomCalendar, SharedCalendarId } from "./types/protocol";
import { EventDetailModal } from "./components/EventDetailModal";
import { DistributedInspector } from "./components/DistributedInspector";
import { ulid } from "./lib/canonical";
import { ensureDeviceKeys, keyIdFor, signEnvelope } from "./lib/crypto";
import { getCursor, getOutbox, reconcile, enqueue, updateOutbox } from "./lib/outbox";
import { applyCommand, availability, getStatus, sync as syncServer } from "./lib/api";
import type { Envelope, EnvelopeType, GroupEvent, SyncEntry } from "./types/protocol";

const EVENTS_KEY = "sd-calendar-events";
const SHARES_KEY = "sd-calendar-shares";
const CUSTOM_CALENDARS_KEY = "sd-calendar-custom-calendars";
const CREDENTIALS_KEY = "sd-calendar-api-credentials";
const ACTIVE_USER_KEY = "sd-calendar-active-user";

export default function App() {
  const [user, setUserState] = useState(() => localStorage.getItem(ACTIVE_USER_KEY) ?? "alice");
  const loadedEventsUser = useRef(user);
  const loadedSharesUser = useRef(user);
  const activeUser = useRef(user);
  const [credentials, setCredentials] = useState<Record<string, string>>(() => {
    try {
      return JSON.parse(localStorage.getItem(CREDENTIALS_KEY) ?? "{}");
    } catch {
      return {};
    }
  });
  const token = credentials[user] ?? "";
  const [group, setGroup] = useState("grupo-core");
  const [deployment] = useState("local");
  const [date, setDate] = useState(() => new Date());
  const [view, setView] = useState<"Día" | "Semana" | "Mes" | "Año" | "Agenda">("Semana");
  const [navDirection, setNavDirection] = useState<"next" | "prev">("next");
  const [offline, setOffline] = useState(false);
  const [online, setOnline] = useState(false);
  const [createOpen, setCreateOpen] = useState(false);
  const [createMode, setCreateMode] = useState<"personal" | "group">("personal");
  const [editingEvent, setEditingEvent] = useState<GroupEvent | null>(null);
  const [createRange, setCreateRange] = useState<{ start: string; end: string } | null>(null);
  const [detailEvent, setDetailEvent] = useState<GroupEvent | null>(null);
  const [showCancelled, setShowCancelled] = useState(false);
  const [busyUsers, setBusyUsers] = useState(["bob"]);
  const [personalVisible, setPersonalVisible] = useState(true);
  const [groupVisible, setGroupVisible] = useState(true);
  const [availabilityMap, setAvailabilityMap] = useState<Record<string, "busy" | "available">>({});
  const [events, setEvents] = useState<GroupEvent[]>(() => {
    try {
      const activeUser = localStorage.getItem(ACTIVE_USER_KEY) ?? "alice";
      return JSON.parse(localStorage.getItem(`${EVENTS_KEY}:${activeUser}`) ?? "[]");
    } catch {
      return [];
    }
  });
  const [calendarShares, setCalendarShares] = useState<CalendarShare[]>(() => {
    try {
      const activeUser = localStorage.getItem(ACTIVE_USER_KEY) ?? "alice";
      const saved = JSON.parse(localStorage.getItem(`${SHARES_KEY}:${activeUser}`) ?? "[]");
      return Array.isArray(saved) ? saved : [];
    } catch {
      return [];
    }
  });
  const [customCalendars, setCustomCalendars] = useState<CustomCalendar[]>(() => {
    try {
      const saved = JSON.parse(localStorage.getItem(CUSTOM_CALENDARS_KEY) ?? "[]");
      return Array.isArray(saved) ? saved : [];
    } catch {
      return [];
    }
  });
  const [outbox, setOutbox] = useState(getOutbox());
  const [cursor, setCursor] = useState(getCursor());
  const [entries, setEntries] = useState<SyncEntry[]>([]);
  const [syncError, setSyncError] = useState("");
  const [inspectorOpen, setInspectorOpen] = useState(false);
  const [sidebarOpen, setSidebarOpen] = useState(true);
  const [theme, setTheme] = useState<"dark" | "light">(
    () => (localStorage.getItem("sd-calendar-theme") as "dark" | "light") ?? "dark"
  );
  const [statusIndex, setStatusIndex] = useState(0);
  const [attendance, setAttendance] = useState<Record<string, Record<string, string>>>({});
  const [counters, setCounters] = useState<Record<string, Record<string, { proposed_start?: string; proposed_end?: string }>>>({});
  const syncRunning = useRef(false);

  const personalEvents = useMemo(() => events.filter((event) => event.kind === "personal"), [events]);
  const currentDetailEvent = detailEvent
    ? events.find((event) => event.uid === detailEvent.uid) ?? detailEvent
    : null;
  const shareUsers = useMemo(
    () => Array.from(new Set([
      "bob",
      "carol",
      ...Object.keys(availabilityMap),
      ...events.flatMap((event) => [event.organizer, ...event.participants])
    ])).filter((candidate) => candidate !== user),
    [availabilityMap, events, user]
  );

  useEffect(() => {
    document.documentElement.dataset.theme = theme;
    localStorage.setItem("sd-calendar-theme", theme);
  }, [theme]);

  useEffect(() => {
    if (loadedEventsUser.current === user) {
      localStorage.setItem(`${EVENTS_KEY}:${user}`, JSON.stringify(events));
    }
  }, [events, user]);

  useEffect(() => {
    localStorage.setItem(ACTIVE_USER_KEY, user);
    if (loadedEventsUser.current === user) return;
    try {
      const saved = JSON.parse(localStorage.getItem(`${EVENTS_KEY}:${user}`) ?? "[]");
      setEvents(Array.isArray(saved) ? saved : []);
    } catch {
      setEvents([]);
    } finally {
      loadedEventsUser.current = user;
    }
  }, [user]);

  useEffect(() => {
    localStorage.setItem(CREDENTIALS_KEY, JSON.stringify(credentials));
  }, [credentials]);

  useEffect(() => {
    if (loadedSharesUser.current === user) {
      localStorage.setItem(`${SHARES_KEY}:${user}`, JSON.stringify(calendarShares));
    }
  }, [calendarShares, user]);

  useEffect(() => {
    if (loadedSharesUser.current === user) return;
    try {
      const saved = JSON.parse(localStorage.getItem(`${SHARES_KEY}:${user}`) ?? "[]");
      setCalendarShares(Array.isArray(saved) ? saved : []);
    } catch {
      setCalendarShares([]);
    } finally {
      loadedSharesUser.current = user;
    }
  }, [user]);

  useEffect(() => {
    localStorage.setItem(CUSTOM_CALENDARS_KEY, JSON.stringify(customCalendars));
  }, [customCalendars]);

  useEffect(() => {
    ensureDeviceKeys();
    let active = true;
    const refresh = async () => {
      if (offline) return;
      try {
        if (!token) {
          if (active) setOnline(false);
          return;
        }
        const status = await getStatus(token);
        if (active && activeUser.current === user) {
          setOnline(true);
          setStatusIndex(status.status.applied_index ?? 0);
        }
      } catch {
        if (active && activeUser.current === user) setOnline(false);
      }
    };
    refresh();
    const timer = window.setInterval(refresh, 5000);
    return () => {
      active = false;
      window.clearInterval(timer);
    };
  }, [offline, token, user]);

  useEffect(() => {
    let active = true;
    const loadAvailability = async () => {
      if (offline) return;
      try {
        if (!token) {
          if (active && activeUser.current === user) setAvailabilityMap({});
          return;
        }
        if (busyUsers.length === 0) {
          if (active && activeUser.current === user) setAvailabilityMap({});
          return;
        }
        const start = new Date(date.getFullYear(), date.getMonth(), date.getDate());
        const end = new Date(start);
        end.setDate(end.getDate() + 1);
        const result = await availability(
          busyUsers,
          token,
          start.toISOString(),
          end.toISOString()
        );
        if (active && activeUser.current === user) setAvailabilityMap(result.users);
      } catch {
        // The UI remains usable from the local projection.
      }
    };
    loadAvailability();
    return () => { active = false; };
  }, [offline, busyUsers, token, date, statusIndex, user]);

  const checkAvailability = useCallback(async (users: string[], start: string, end: string) => {
    if (!token) throw new Error("Configura el token Bearer de este usuario");
    const result = await availability(users, token, start, end);
    return result.users;
  }, [token]);

  const changeUser = (nextUser: string) => {
    if (nextUser === user) return;
    activeUser.current = nextUser;
    setEvents([]);
    setDetailEvent(null);
    setAttendance({});
    setCounters({});
    setCalendarShares([]);
    setAvailabilityMap({});
    setUserState(nextUser);
  };

  const title = date.toLocaleDateString("es", { month: "long", year: "numeric" });

  const openCreate = (mode: "personal" | "group" = "personal") => {
    setEditingEvent(null);
    setCreateMode(mode);
    setCreateRange(null);
    setCreateOpen(true);
  };

  const saveCalendarShare = async (
    calendar: SharedCalendarId,
    users: string[],
    permission: CalendarSharePermission
  ) => {
    setCalendarShares((current) => {
      const withoutCurrent = current.filter(
        (share) => share.owner !== user || share.calendar !== calendar
      );
      return users.length
        ? [...withoutCurrent, { owner: user, calendar, users, permission }]
        : withoutCurrent;
    });
    const envelope = await buildEnvelope("SHARE_CALENDAR", `${user}:calendar:${calendar}`, 1, {
      calendar,
      users,
      permission
    });
    await queueEnvelope(envelope);
  };

  const addCustomCalendar = (calendar: { name: string; color: string; visibility: "private" | "public" }) => {
    setCustomCalendars((current) => [
      ...current,
      {
        id: ulid(),
        name: calendar.name,
        color: calendar.color,
        visibility: calendar.visibility,
        visible: true
      }
    ]);
  };

  const toggleCustomCalendar = (id: string) => {
    setCustomCalendars((current) =>
      current.map((calendar) =>
        calendar.id === id ? { ...calendar, visible: !calendar.visible } : calendar
      )
    );
  };

  const deleteCustomCalendar = (id: string) => {
    setCustomCalendars((current) => current.filter((calendar) => calendar.id !== id));
    setCalendarShares((current) =>
      current.filter((share) => share.owner !== user || share.calendar !== id)
    );
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
    if (syncRunning.current) return;
    setOutbox(getOutbox());
    if (offline) return;
    if (!token) {
      setOnline(false);
      setSyncError("Guarda el token Bearer del usuario para conectar con el backend.");
      return;
    }
    syncRunning.current = true;
    try {
      for (const item of getOutbox()) {
        if (item.state !== "PENDING" && item.state !== "503 Retry") continue;
        const senderToken = credentials[item.envelope.sender_id];
        if (!senderToken) continue;
        try {
          const response = await applyCommand(item.envelope, senderToken);
          if (response.code === 200) {
            updateOutbox(item.envelope.msg_id, { state: "SENT", error: undefined });
          } else if (response.code === 409) {
            updateOutbox(item.envelope.msg_id, { state: "409 Conflict", error: String(response.result?.reason ?? "") });
          } else if (response.code === 503) {
            updateOutbox(item.envelope.msg_id, { state: "503 Retry", attempts: item.attempts + 1, error: String(response.result?.reason ?? "") });
          } else {
            updateOutbox(item.envelope.msg_id, { state: "Rejected", error: String(response.result?.reason ?? response.detail?.reason ?? `HTTP ${response.code}`) });
          }
        } catch (error) {
          updateOutbox(item.envelope.msg_id, {
            state: "503 Retry",
            attempts: item.attempts + 1,
            error: error instanceof Error ? error.message : String(error)
          });
        }
      }

      let since = getCursor();
      let page = await syncServer(token, since, 100);
      const allEntries: SyncEntry[] = [];
      let pages = 0;
      while (true) {
        allEntries.push(...page.entries);
        if (page.next < since) throw new Error("El cursor del servidor retrocedió.");
        if (page.next === since || page.next >= page.as_of) break;
        since = page.next;
        if (++pages >= 100) throw new Error("La sincronización superó el límite de páginas.");
        page = await syncServer(token, since, 100);
      }
      if (activeUser.current !== user) return;
      const next = reconcile(allEntries, page.next);
      setCursor(next);
      setEntries(allEntries);
      const serverEvents = Object.values(page.snapshot?.events ?? {});
      const serverAttendance = page.snapshot?.attendance ?? {};
      const serverCounters = page.snapshot?.counters ?? {};
      const pendingItems = getOutbox().filter((item) =>
        item.state === "PENDING" || item.state === "503 Retry"
      );
      const reconciledShares = [...(page.snapshot?.calendar_shares ?? [])];
      for (const { envelope } of pendingItems.filter(({ envelope }) =>
        envelope.type === "SHARE_CALENDAR"
        && (envelope.sender_id === user
          || (Array.isArray(envelope.payload.users) && envelope.payload.users.includes(user)))
      )) {
        const calendar = String(envelope.payload.calendar ?? "");
        const users = Array.isArray(envelope.payload.users)
          ? envelope.payload.users.filter((candidate): candidate is string => typeof candidate === "string")
          : [];
        const permission = envelope.payload.permission;
        const withoutCurrent = reconciledShares.filter(
          (share) => share.owner !== envelope.sender_id || share.calendar !== calendar
        );
        if (users.length && (permission === "freebusy" || permission === "details" || permission === "edit")) {
          withoutCurrent.push({
            owner: envelope.sender_id,
            calendar,
            users,
            permission
          });
        }
        reconciledShares.splice(0, reconciledShares.length, ...withoutCurrent);
      }
      const pendingEvents: GroupEvent[] = pendingItems
        .filter((item) => item.envelope.type === "REQUEST")
        .map(({ envelope }): GroupEvent => {
          const payload = envelope.payload;
          const participants = Array.isArray(payload.participants)
            ? payload.participants.map((participant) =>
                typeof participant === "string"
                  ? participant
                  : String((participant as { user_id?: unknown }).user_id ?? "")
              ).filter(Boolean)
            : [];
          const kind: GroupEvent["kind"] = payload.kind === "personal" ? "personal" : "group";
          return {
            uid: envelope.uid,
            sequence: envelope.sequence,
            state: kind === "personal" ? "CONFIRMED" : "PROPOSED",
            organizer: envelope.sender_id,
            kind,
            participants,
            payload: payload as GroupEvent["payload"]
          };
        })
        .filter((event) => event.organizer === user || event.participants.includes(user));
      const reconciledEvents = [...serverEvents];
      for (const pending of pendingEvents) {
        if (!reconciledEvents.some((event) => event.uid === pending.uid)) reconciledEvents.push(pending);
      }
      for (const item of pendingItems.filter(({ envelope }) => envelope.type === "CANCEL")) {
        const event = reconciledEvents.find(({ uid }) => uid === item.envelope.uid);
        if (event) event.state = "CANCELLED";
      }
      setEvents(reconciledEvents);
      setAttendance(serverAttendance);
      setCounters(serverCounters);
      setCalendarShares(reconciledShares);
      for (const { envelope } of pendingItems.filter(({ envelope }) => envelope.type === "REPLY")) {
        const partstat = String(envelope.payload.partstat ?? "");
        setAttendance((current) => ({
          ...current,
          [envelope.uid]: { ...current[envelope.uid], [envelope.sender_id]: partstat }
        }));
      }
      setStatusIndex(page.as_of);
      setOnline(true);
      setSyncError("");
      setOutbox(getOutbox());
    } catch (error) {
      if (activeUser.current === user) {
        setOnline(false);
        setSyncError(error instanceof Error ? error.message : String(error));
      }
    } finally {
      syncRunning.current = false;
    }
  };

  useEffect(() => {
    if (offline || !token) return;
    void syncNow();
    const timer = window.setInterval(() => { void syncNow(); }, 3000);
    return () => window.clearInterval(timer);
  }, [offline, token, user, credentials]);

  const queueEnvelope = async (envelope: Envelope) => {
    enqueue(envelope);
    setOutbox(getOutbox());
    if (!offline) await syncNow();
  };

  const savePersonal = async (
    value: { title: string; start: string; end: string; tzid: string; visibility: "private" | "public"; availability: "busy" | "free"; privacy: "private" | "public"; hide: boolean },
    existing?: GroupEvent
  ) => {
    const uid = existing?.uid ?? `${user}:personal:${ulid()}`;
    const sequence = existing ? existing.sequence + 1 : 1;
    const envelope = await buildEnvelope("REQUEST", uid, sequence, {
      kind: "personal",
      title: value.title,
      start: value.start,
      end: value.end,
      tzid: value.tzid,
      privacy: value.privacy,
      visibility: value.visibility,
      availability: value.availability,
      hide_from_freebusy: value.hide
    });
    const event: GroupEvent = {
      uid,
      sequence,
      state: "CONFIRMED",
      organizer: existing?.organizer ?? user,
      kind: "personal",
      participants: [],
      payload: {
        title: value.title,
        start: value.start,
        end: value.end,
        privacy: value.privacy,
        visibility: value.visibility,
        availability: value.availability,
        hide_from_freebusy: value.hide
      }
    };
    setEvents((current) => existing
      ? current.map((item) => item.uid === uid ? event : item)
      : [...current, event]);
    await queueEnvelope(envelope);
    setEditingEvent(null);
    setCreateOpen(false);
  };

  const saveGroup = async (
    value: { title: string; location: string; start: string; end: string; participants: { user_id: string; role: "REQUIRED" | "OPTIONAL" }[]; rule: "ALL" | "MAJORITY" | "QUORUM" | "ORGANIZER"; quorum: number },
    existing?: GroupEvent
  ) => {
    const uid = existing?.uid ?? `${user}:meeting:${ulid()}`;
    const sequence = existing ? existing.sequence + 1 : 1;
    const envelope = await buildEnvelope("REQUEST", uid, sequence, {
      kind: "group",
      title: value.title,
      location: value.location,
      start: value.start,
      end: value.end,
      tzid: Intl.DateTimeFormat().resolvedOptions().timeZone,
      participants: value.participants,
      confirmation_rule: value.rule,
      quorum: value.quorum
    });
    const event: GroupEvent = {
      uid,
      sequence,
      state: "PROPOSED",
      organizer: existing?.organizer ?? user,
      kind: "group",
      participants: value.participants.map((participant) => participant.user_id),
      payload: { title: value.title, location: value.location, start: value.start, end: value.end, confirmation_rule: value.rule, quorum: value.quorum }
    };
    setEvents((current) => existing
      ? current.map((item) => item.uid === uid ? event : item)
      : [...current, event]);
    await queueEnvelope(envelope);
    setEditingEvent(null);
    setCreateOpen(false);
  };

  const openEdit = (event: GroupEvent) => {
    setDetailEvent(null);
    setCreateMode(event.kind);
    setEditingEvent(event);
    setCreateOpen(true);
  };

  const sendReply = async (partstat: "ACCEPTED" | "DECLINED" | "TENTATIVE") => {
    if (!currentDetailEvent) return;
    const envelope = await buildEnvelope("REPLY", currentDetailEvent.uid, currentDetailEvent.sequence, { partstat });
    setAttendance((current) => ({
      ...current,
      [currentDetailEvent.uid]: { ...current[currentDetailEvent.uid], [user]: partstat }
    }));
    await queueEnvelope(envelope);
  };

  const sendSimpleCommand = async (
    type: "COUNTER" | "CANCEL" | "CLOSE_POLL" | "REOPEN",
    counter?: { start: string; end: string }
  ) => {
    if (!currentDetailEvent) return;
    const payload = type === "COUNTER"
      ? { proposed_start: counter?.start, proposed_end: counter?.end }
      : type === "REOPEN"
        ? { extend_deadline: true, reask: true }
        : {};
    const envelope = await buildEnvelope(type, currentDetailEvent.uid, currentDetailEvent.sequence, payload);
    await queueEnvelope(envelope);
  };

  const deleteEvent = async () => {
    if (!currentDetailEvent) return;
    await sendSimpleCommand("CANCEL");
    setEvents((current) =>
      current.map((event) => event.uid === currentDetailEvent.uid ? { ...event, state: "CANCELLED" } : event)
    );
    setDetailEvent(null);
  };

  const canEditDetailEvent = Boolean(currentDetailEvent && (
    currentDetailEvent.organizer === user
    || calendarShares.some((share) =>
      share.owner === currentDetailEvent.organizer
      && share.calendar === currentDetailEvent.kind
      && share.permission === "edit"
      && share.users.includes(user)
    )
  ));

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
        user={user} token={token} group={group} keyId={keyIdFor(user)}
        offline={offline} view={view} title={title} syncError={syncError}
        pending={outbox.filter((item) => item.state !== "SENT").length}
        cursor={cursor} online={online} theme={theme}
        onUser={changeUser}
        onToken={(value) => setCredentials((current) => ({ ...current, [user]: value.trim() }))}
        onGroup={setGroup} onOffline={setOffline}
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
        <Sidebar date={date} events={events} busyUsers={busyUsers} availability={availabilityMap}
          personalVisible={personalVisible} groupVisible={groupVisible}
          onPersonalVisible={setPersonalVisible} onGroupVisible={setGroupVisible}
          onBusyUsers={setBusyUsers}
          onCreate={openCreate} onSelectEvent={setDetailEvent}
          showCancelled={showCancelled} onShowCancelled={setShowCancelled} view={view}
          currentUser={user} shareUsers={shareUsers} shares={calendarShares}
          onSaveCalendarShare={saveCalendarShare}
          customCalendars={customCalendars}
          onAddCustomCalendar={addCustomCalendar}
          onToggleCustomCalendar={toggleCustomCalendar}
          onDeleteCustomCalendar={deleteCustomCalendar}
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
            busyUsers={busyUsers} personalVisible={personalVisible} groupVisible={groupVisible} showCancelled={showCancelled}
              onSelect={setDetailEvent} onQuickCreate={(_day, start, end) => {
                setCreateMode("personal");
                setCreateRange({ start, end });
                setCreateOpen(true);
              }}
            onNavigateDate={(nextDate, nextView = "Día") => {
              setNavDirection("next");
              setDate(nextDate);
              setView(nextView);
            }} />
        </div>
      </div>
      <DistributedInspector open={inspectorOpen} outbox={outbox} entries={entries} cursor={cursor}
        onToggle={() => setInspectorOpen((open) => !open)} />
      <EventModal open={createOpen} initialMode={createMode} initialStart={createRange?.start} initialEnd={createRange?.end} user={user} availability={availabilityMap} checkAvailability={checkAvailability} editingEvent={editingEvent}
        onClose={() => { setCreateOpen(false); setEditingEvent(null); }} onSavePersonal={savePersonal} onSaveGroup={saveGroup} />
      <EventDetailModal event={currentDetailEvent} attendance={currentDetailEvent ? attendance[currentDetailEvent.uid] ?? {} : {}}
        counters={currentDetailEvent ? counters[currentDetailEvent.uid] ?? {} : {}}
        currentUser={user} canEdit={canEditDetailEvent}
        onClose={() => setDetailEvent(null)} onReply={sendReply}
        onCounter={(start, end) => sendSimpleCommand("COUNTER", { start, end })}
        onClosePoll={() => sendSimpleCommand("CLOSE_POLL")}
        onReopen={() => sendSimpleCommand("REOPEN")}
        onCancel={() => sendSimpleCommand("CANCEL")}
        onDelete={deleteEvent} onEdit={currentDetailEvent ? () => openEdit(currentDetailEvent) : () => undefined} />
    </div>
  );
}
