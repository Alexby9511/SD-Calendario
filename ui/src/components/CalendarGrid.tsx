import { useEffect, useRef, useState } from "react";
import type { CSSProperties, MouseEvent as ReactMouseEvent } from "react";
import { Megaphone, ShieldCheck, UserRound, UsersRound, VolumeX } from "lucide-react";
import type { CustomCalendar, GroupEvent } from "../types/protocol";
import { localConflict } from "../lib/conflicts";
import { eventCustomCalendarId, isEventCalendarVisible } from "../lib/calendars";

interface Props {
  view: "Día" | "Semana" | "Mes" | "Año" | "Agenda";
  date: Date;
  events: GroupEvent[];
  pendingInvitationUids: ReadonlySet<string>;
  personalEvents: GroupEvent[];
  customCalendars: CustomCalendar[];
  busyUsers: string[];
  personalVisible: boolean;
  groupVisible: boolean;
  currentUser: string;
  sharedCalendarColors: Record<string, string>;
  showCancelled: boolean;
  onSelect: (event: GroupEvent) => void;
  onQuickCreate?: (date: Date, start: string, end: string) => void;
  onNavigateDate?: (date: Date, view?: "Día" | "Mes") => void;
}

const hours = Array.from({ length: 24 }, (_, i) => i);
const MINUTES_PER_DAY = 24 * 60;
const SLOT_MINUTES = 15;
const HOUR_HEIGHT = 64;
const HEADER_HEIGHT = 62;

interface DragSelection {
  day: Date;
  dayIndex: number;
  anchorMinute: number;
  quickMinute: number;
  anchorClientY: number;
  currentMinute: number;
  moved: boolean;
}

interface PreviewSelection {
  dayIndex: number;
  startMinute: number;
  endMinute: number;
}

type EventStyle = CSSProperties & {
  "--shared-calendar-color"?: string;
  "--event-accent-color"?: string;
  "--event-layer-color"?: string;
};

interface EventVisuals {
  sharedColor?: string;
  layer?: CustomCalendar;
  style: EventStyle;
}

type EventCardView = "grid" | "month" | "agenda";

function getEventBackgroundColor(
  event: GroupEvent,
  view: EventCardView,
  isPendingInvitation: boolean,
  sharedColor?: string
) {
  if (isPendingInvitation) return "rgba(168,199,250,.12)";
  if (sharedColor) return sharedColor;
  if (view === "agenda") return "transparent";
  if (view === "month") {
    return event.state === "CONFIRMED" ? "#6e93cc" : "rgba(168,199,250,.14)";
  }
  if (event.state === "CANCELLED") return "transparent";
  if (event.state === "CONFIRMED") return "#6e93cc";
  if (event.state === "PROPOSED") return "rgba(168,199,250,.12)";
  return event.kind === "personal" ? "#81c995" : "#8ab4f8";
}

function getEventLeftAccentColor(
  event: GroupEvent,
  layer: CustomCalendar | undefined,
  eventBackgroundColor: string
) {
  if (layer?.color.trim()) return layer.color;

  const backgroundColor = eventBackgroundColor.trim() || (
    event.state === "CONFIRMED" ? "#6e93cc" : "#8ab4f8"
  );
  return `color-mix(in srgb, ${backgroundColor} 72%, black)`;
}

function eventVisuals(
  event: GroupEvent,
  customCalendars: CustomCalendar[],
  currentUser: string,
  sharedCalendarColors: Record<string, string>,
  view: EventCardView,
  isPendingInvitation: boolean
): EventVisuals {
  const isMyInvitation = event.kind === "group"
    && event.organizer !== currentUser
    && event.participants.includes(currentUser);
  const sharedColor = isMyInvitation || event.organizer === currentUser
    ? undefined
    : sharedCalendarColors[event.organizer];
  const calendarId = eventCustomCalendarId(event, customCalendars);
  const layer = calendarId
    ? customCalendars.find((calendar) => calendar.id === calendarId)
    : undefined;
  const accentColor = sharedColor
    ?? (event.state === "CONFIRMED"
      ? "#6e93cc"
      : event.kind === "personal"
        ? "#81c995"
        : "#8ab4f8");
  const backgroundColor = getEventBackgroundColor(event, view, isPendingInvitation, sharedColor);
  const layerColor = getEventLeftAccentColor(event, layer, backgroundColor);
  const hasDashedBorder = isPendingInvitation || event.state === "PROPOSED";

  return {
    sharedColor,
    layer,
    style: {
      "--event-accent-color": accentColor,
      "--event-layer-color": layerColor,
      ...(!hasDashedBorder ? { borderLeft: `4px solid ${layerColor}` } : {}),
      ...(sharedColor ? { "--shared-calendar-color": sharedColor } : {})
    }
  };
}

function EventIndicators({ event, layer }: { event: GroupEvent; layer?: CustomCalendar }) {
  const visibility = event.payload.visibility ?? event.payload.privacy;
  return (
    <span className="event-indicators">
      {event.kind === "personal"
        ? <span role="img" aria-label="Evento personal" title="Evento personal"><UserRound size={11} /></span>
        : <span role="img" aria-label="Reunión" title="Reunión"><UsersRound size={11} /></span>}
      {visibility === "private" && (
        <span role="img" aria-label="Privado" title="Privado"><VolumeX size={11} /></span>
      )}
      {visibility === "public" && (
        <span role="img" aria-label="Público" title="Público"><Megaphone size={11} /></span>
      )}
      {event.kind === "group" && event.state === "CONFIRMED" && (
        <span role="img" aria-label="Reunión confirmada" title="Reunión confirmada"><ShieldCheck size={11} /></span>
      )}
      {layer && (
        <span
          className="event-layer-indicator"
          style={{ backgroundColor: layer.color }}
          aria-label={`Capa ${layer.name}`}
          title={`Capa ${layer.name}`}
        />
      )}
    </span>
  );
}

function formatHour(hour: number) {
  const normalized = hour % 12 || 12;
  return `${normalized} ${hour < 12 ? "AM" : "PM"}`;
}

function formatTime(date: Date) {
  const hoursValue = date.getHours();
  const normalized = hoursValue % 12 || 12;
  return `${normalized}:${String(date.getMinutes()).padStart(2, "0")} ${hoursValue < 12 ? "AM" : "PM"}`;
}

function dateTimeAt(day: Date, minutes: number) {
  const date = new Date(day);
  date.setHours(0, minutes, 0, 0);
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, "0")}-${String(date.getDate()).padStart(2, "0")}T${String(date.getHours()).padStart(2, "0")}:${String(date.getMinutes()).padStart(2, "0")}`;
}

function dateAt(day: Date, minutes: number) {
  const date = new Date(day);
  date.setHours(0, minutes, 0, 0);
  return date;
}

function stateLabel(state: GroupEvent["state"]) {
  if (state === "PROPOSED") return "PROPUESTA";
  if (state === "CONFIRMED") return "CONFIRMADA";
  if (state === "NOT_CONFIRMED") return "NO CONFIRMADA";
  if (state === "EXPIRED") return "EXPIRADA";
  return "CANCELADA";
}

export function CalendarGrid(props: Props) {
  const timezone = Intl.DateTimeFormat().resolvedOptions().timeZone;
  const [now, setNow] = useState(() => new Date());
  const [gridWidth, setGridWidth] = useState(0);
  const [isDragging, setIsDragging] = useState(false);
  const [preview, setPreview] = useState<PreviewSelection | null>(null);
  const gridRef = useRef<HTMLElement>(null);
  const dragRef = useRef<DragSelection | null>(null);
  useEffect(() => {
    const timer = window.setInterval(() => setNow(new Date()), 60_000);
    return () => window.clearInterval(timer);
  }, []);
  useEffect(() => {
    const grid = gridRef.current;
    if (!grid) return;
    const updateWidth = () => setGridWidth(grid.clientWidth);
    updateWidth();
    const observer = new ResizeObserver(updateWidth);
    observer.observe(grid);
    return () => observer.disconnect();
  }, [props.view]);
  const visibleEvents = props.events.filter((event) =>
    isEventCalendarVisible(event, props.customCalendars, props.personalVisible, props.groupVisible, props.currentUser)
    && (props.showCancelled || event.state !== "CANCELLED")
  );
  const days = (props.view === "Día" ? [0] : [0, 1, 2, 3, 4, 5, 6]).map((i) => {
    const date = new Date(props.date);
    if (props.view === "Día") {
      date.setDate(props.date.getDate());
    } else {
      const startOffset = (props.date.getDay() + 6) % 7;
      date.setDate(props.date.getDate() - startOffset + i);
    }
    return date;
  });

  useEffect(() => {
    if (props.view !== "Día" && props.view !== "Semana") return;
    const frame = window.requestAnimationFrame(() => {
      const grid = gridRef.current;
      if (!grid) return;
      const today = new Date();
      const todayVisible = days.some((day) => day.toDateString() === today.toDateString());
      const minutes = todayVisible
        ? today.getHours() * 60 + today.getMinutes()
        : 8 * 60;
      const target = 62 + (minutes / 60) * 64;
      const top = Math.max(0, target - grid.clientHeight / 2);
      grid.scrollTo({ top, behavior: "smooth" });
    });
    return () => window.cancelAnimationFrame(frame);
  }, [props.view, props.date.toISOString().slice(0, 10)]);

  useEffect(() => {
    if (!isDragging) return;
    const previousUserSelect = document.body.style.userSelect;
    document.body.style.userSelect = "none";
    document.body.classList.add("calendar-dragging");

    const updateSelection = (clientY: number, detectMovement = true) => {
      const grid = gridRef.current;
      const drag = dragRef.current;
      if (!grid || !drag) return;
      if (detectMovement && Math.abs(clientY - drag.anchorClientY) >= 8) {
        drag.moved = true;
      }
      const gridRect = grid.getBoundingClientRect();
      const contentY = clientY - gridRect.top + grid.scrollTop - HEADER_HEIGHT;
      const minute = Math.max(
        0,
        Math.min(
          MINUTES_PER_DAY - SLOT_MINUTES,
          Math.floor(contentY / (HOUR_HEIGHT / 4)) * SLOT_MINUTES
        )
      );
      drag.currentMinute = minute;
      drag.moved ||= minute !== drag.anchorMinute;
      setPreview({
        dayIndex: drag.dayIndex,
        startMinute: Math.min(drag.anchorMinute, minute),
        endMinute: Math.min(
          MINUTES_PER_DAY,
          Math.max(drag.anchorMinute, minute) + SLOT_MINUTES
        )
      });
    };

    const onMouseMove = (event: MouseEvent) => updateSelection(event.clientY);
    const onMouseUp = (event: MouseEvent) => {
      updateSelection(event.clientY, false);
      const drag = dragRef.current;
      dragRef.current = null;
      setPreview(null);
      setIsDragging(false);
      if (!drag || !props.onQuickCreate) return;

      const startMinute = drag.moved
        ? Math.min(drag.anchorMinute, drag.currentMinute)
        : drag.quickMinute;
      const endMinute = drag.moved
        ? Math.min(
            MINUTES_PER_DAY,
            Math.max(drag.anchorMinute, drag.currentMinute) + SLOT_MINUTES
          )
        : Math.min(MINUTES_PER_DAY, drag.quickMinute + 60);
      props.onQuickCreate(
        drag.day,
        dateTimeAt(drag.day, startMinute),
        dateTimeAt(drag.day, endMinute)
      );
    };

    window.addEventListener("mousemove", onMouseMove);
    window.addEventListener("mouseup", onMouseUp);
    return () => {
      document.body.style.userSelect = previousUserSelect;
      document.body.classList.remove("calendar-dragging");
      window.removeEventListener("mousemove", onMouseMove);
      window.removeEventListener("mouseup", onMouseUp);
    };
  }, [isDragging, props.onQuickCreate]);

  const beginSelection = (event: ReactMouseEvent<HTMLDivElement>, day: Date, dayIndex: number, hour: number) => {
    if (event.button !== 0 || (props.view !== "Día" && props.view !== "Semana")) return;
    const grid = gridRef.current;
    if (!grid) return;
    event.preventDefault();
    const gridRect = grid.getBoundingClientRect();
    const contentY = event.clientY - gridRect.top + grid.scrollTop - HEADER_HEIGHT;
    const anchorMinute = Math.max(
      0,
      Math.min(
        MINUTES_PER_DAY - SLOT_MINUTES,
        Math.floor(contentY / (HOUR_HEIGHT / 4)) * SLOT_MINUTES
      )
    );
    const quickMinute = hour * 60;
    dragRef.current = {
      day,
      dayIndex,
      anchorMinute,
      quickMinute,
      anchorClientY: event.clientY,
      currentMinute: anchorMinute,
      moved: false
    };
    setPreview({
      dayIndex,
      startMinute: quickMinute,
      endMinute: Math.min(MINUTES_PER_DAY, quickMinute + 60)
    });
    setIsDragging(true);
  };

  if (props.view === "Agenda") {
    return (
      <main className="agenda-view">
        {visibleEvents.map((event) => {
          const isPendingInvitation = props.pendingInvitationUids.has(event.uid);
          const visuals = eventVisuals(
            event,
            props.customCalendars,
            props.currentUser,
            props.sharedCalendarColors,
            "agenda",
            isPendingInvitation
          );
          return (
            <button
              key={event.uid}
              onClick={() => props.onSelect(event)}
              className={`agenda-item state-${event.state.toLowerCase()}${isPendingInvitation ? " pending-invitation" : ""}${visuals.sharedColor ? " shared-calendar-event" : ""}${visuals.layer ? " layer-calendar-event" : ""}`}
              style={visuals.style}
            >
              <time>{event.payload.start?.replace("T", " ").slice(0, 16)}</time>
              <div>
                <EventIndicators event={event} layer={visuals.layer} />
                <strong>{event.payload.title ?? "Evento"}</strong>
                <small>
                  {props.pendingInvitationUids.has(event.uid) ? "Pendiente de respuesta" : stateLabel(event.state)}
                </small>
              </div>
            </button>
          );
        })}
      </main>
    );
  }

  if (props.view === "Año") {
    const monthNames = ["Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"];
    const year = props.date.getFullYear();
    const weekdayHeaders = ["L", "M", "X", "J", "V", "S", "D"];
    return (
      <main className="year-grid">
        {monthNames.map((name, month) => (
          <section
            key={name}
            onDoubleClick={() => props.onNavigateDate?.(new Date(year, month, 1), "Mes")}
            className={
            month === new Date().getMonth() && year === new Date().getFullYear()
              ? "today-year"
              : month === props.date.getMonth()
                ? "active-year"
                : ""
          }>
            <h3 onClick={() => props.onNavigateDate?.(new Date(year, month, 1), "Mes")}>{name}</h3>
            <div className="year-weekdays">
              {weekdayHeaders.map((day) => <b key={day}>{day}</b>)}
            </div>
            <div className="year-days">
              {(() => {
                const first = new Date(year, month, 1);
                const leading = (first.getDay() + 6) % 7;
                const last = new Date(year, month + 1, 0).getDate();
                return Array.from({ length: leading + last }, (_, i) => {
                  const dayNumber = i - leading + 1;
                  if (dayNumber < 1) return <span key={`empty-${i}`} />;
                  const day = new Date(year, month, dayNumber);
                  const isToday = day.toDateString() === new Date().toDateString();
                  const isActive = day.toDateString() === props.date.toDateString();
                  return (
                    <button
                      key={dayNumber}
                      className={`${isToday ? "today" : ""} ${isActive ? "active-date" : ""}`}
                      onClick={() => props.onNavigateDate?.(day, "Día")}
                    >
                      {dayNumber}
                    </button>
                  );
                });
              })()}
            </div>
          </section>
        ))}
      </main>
    );
  }

  if (props.view === "Mes") {
    const year = props.date.getFullYear();
    const month = props.date.getMonth();
    const mondayIndex = (new Date(year, month, 1).getDay() + 6) % 7;
    const first = new Date(year, month, 1 - mondayIndex);
    const cells = Array.from({ length: 42 }, (_, i) => {
      const date = new Date(first);
      date.setDate(first.getDate() + i);
      return date;
    });
    return (
      <main className="month-grid">
        {["Lun", "Mar", "Mié", "Jue", "Vie", "Sáb", "Dom"].map((day) => <b key={day}>{day}</b>)}
        {cells.map((day, i) => (
          <div key={i} className={[
            i % 7 > 4 ? "weekend" : "",
            day.getMonth() !== month ? "outside-month" : ""
          ].filter(Boolean).join(" ")}>
            <span className={
              day.toDateString() === new Date().toDateString() ? "today" :
              day.toDateString() === props.date.toDateString() ? "active-date" : ""
            }>{day.getDate()}</span>
            {visibleEvents.filter((event) => event.payload.start?.slice(0, 10) === `${day.getFullYear()}-${String(day.getMonth() + 1).padStart(2, "0")}-${String(day.getDate()).padStart(2, "0")}`).map((event) => {
              const isPendingInvitation = props.pendingInvitationUids.has(event.uid);
              const visuals = eventVisuals(
                event,
                props.customCalendars,
                props.currentUser,
                props.sharedCalendarColors,
                "month",
                isPendingInvitation
              );
              return (
                <button
                  key={event.uid}
                  onClick={() => props.onSelect(event)}
                  className={`month-event state-${event.state.toLowerCase()}${isPendingInvitation ? " pending-invitation" : ""}${visuals.sharedColor ? " shared-calendar-event" : ""}${visuals.layer ? " layer-calendar-event" : ""}`}
                  style={visuals.style}
                  title={isPendingInvitation ? "Pendiente de respuesta" : undefined}
                >
                  <EventIndicators event={event} layer={visuals.layer} />
                  {event.payload.title}
                </button>
              );
            })}
          </div>
        ))}
      </main>
    );
  }

  return (
    <main ref={gridRef} className={`calendar-grid ${props.view === "Día" ? "day-view" : ""}`}>
      <div className="timezone">{timezone}</div>
      {days.map((day, i) => (
        <div key={i} className="day-heading">
          <span>{day.toLocaleDateString("es", { weekday: "short" }).toUpperCase()}</span>
          <b className={[
            day.toDateString() === now.toDateString() ? "today" : "",
            day.toDateString() === props.date.toDateString() ? "active-date" : ""
          ].filter(Boolean).join(" ")}>{day.getDate()}</b>
        </div>
      ))}
      {hours.map((hour) => (
        <div className="hour-row" key={hour}>
          <time>{formatHour(hour)}</time>
          {days.map((day, dayIndex) => <div className="hour-cell" key={dayIndex} onMouseDown={(event) => beginSelection(event, day, dayIndex, hour)} />)}
        </div>
      ))}
      {visibleEvents.map((event, i) => {
        const isPendingInvitation = props.pendingInvitationUids.has(event.uid);
        const visuals = eventVisuals(
          event,
          props.customCalendars,
          props.currentUser,
          props.sharedCalendarColors,
          "grid",
          isPendingInvitation
        );
        const start = event.payload.start ? new Date(event.payload.start.replace(" ", "T")) : null;
        const end = event.payload.end ? new Date(event.payload.end.replace(" ", "T")) : null;
        const dayIndex = start
          ? days.findIndex((day) => day.toDateString() === start.toDateString())
          : -1;
        if (dayIndex < 0) return null;
        const minutes = start ? start.getHours() * 60 + start.getMinutes() : 9 * 60;
        const top = 62 + (minutes / 60) * 64;
        const durationMinutes = start && end && end > start
          ? (end.getTime() - start.getTime()) / 60_000
          : 60;
        const conflict = localConflict(event, props.personalEvents);
        return (
          <button
            key={event.uid}
            onClick={() => props.onSelect(event)}
            className={`calendar-event state-${event.state.toLowerCase()} ${event.kind === "personal" ? "personal-event" : ""}${conflict ? " has-conflict" : ""}${isPendingInvitation ? " pending-invitation" : ""}${visuals.sharedColor ? " shared-calendar-event" : ""}${visuals.layer ? " layer-calendar-event" : ""}`}
            style={{
              ...visuals.style,
              left: props.view === "Día"
                ? 74
                : 68 + ((gridWidth - 68) / 7) * dayIndex + 6,
              width: props.view === "Día"
                ? undefined
                : Math.max(60, (gridWidth - 68) / 7 - 12),
              top,
              height: (durationMinutes / 60) * HOUR_HEIGHT,
              minHeight: 0
            }}
          >
            <EventIndicators event={event} layer={visuals.layer} />
            <strong>{event.payload.title ?? "Evento"}</strong>
            <span>{start ? formatTime(start) : "9:00 AM"} · {isPendingInvitation ? "PENDIENTE" : stateLabel(event.state)}</span>
          </button>
        );
      })}
      {preview && (
        <div
          className="calendar-event preview-event"
          aria-hidden="true"
          style={{
            left: props.view === "Día"
              ? 74
              : 68 + ((gridWidth - 68) / 7) * preview.dayIndex + 6,
            width: props.view === "Día"
              ? undefined
              : Math.max(60, (gridWidth - 68) / 7 - 12),
            top: HEADER_HEIGHT + (preview.startMinute / 60) * HOUR_HEIGHT,
            height: Math.max(16, ((preview.endMinute - preview.startMinute) / 60) * HOUR_HEIGHT)
          }}
        >
          <strong>(Sin título)</strong>
          <span>{formatTime(dateAt(days[preview.dayIndex], preview.startMinute))} – {formatTime(dateAt(days[preview.dayIndex], preview.endMinute))}</span>
        </div>
      )}
      {(() => {
        const todayIndex = days.findIndex((day) => day.toDateString() === now.toDateString());
        if (todayIndex < 0) return null;
        const lineWidth = props.view === "Día"
          ? Math.max(0, gridWidth - 68)
          : Math.max(0, (gridWidth - 68) / 7);
        const left = props.view === "Día"
          ? 68
          : 68 + ((gridWidth - 68) / 7) * todayIndex;
        return (
          <div className="now-line" style={{
            top: 62 + ((now.getHours() * 60 + now.getMinutes()) / 60) * 64,
            left,
            width: lineWidth
          }}><span /></div>
        );
      })()}
    </main>
  );
}
