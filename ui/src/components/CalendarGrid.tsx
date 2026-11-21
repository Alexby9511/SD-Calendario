import { useEffect, useRef, useState } from "react";
import { AlertTriangle, Lock, ShieldCheck } from "lucide-react";
import type { GroupEvent } from "../types/protocol";
import { localConflict } from "../lib/conflicts";

interface Props {
  view: "Día" | "Semana" | "Mes" | "Año" | "Agenda";
  date: Date;
  events: GroupEvent[];
  personalEvents: GroupEvent[];
  busyUsers: string[];
  availability: Record<string, { start: string; end: string }[]>;
  showCancelled: boolean;
  onSelect: (event: GroupEvent) => void;
  onQuickCreate?: (date: Date, hour: number) => void;
  onNavigateDate?: (date: Date, view?: "Día" | "Mes") => void;
}

const hours = Array.from({ length: 24 }, (_, i) => i);

function formatHour(hour: number) {
  const normalized = hour % 12 || 12;
  return `${normalized} ${hour < 12 ? "AM" : "PM"}`;
}

function formatTime(date: Date) {
  const hoursValue = date.getHours();
  const normalized = hoursValue % 12 || 12;
  return `${normalized}:${String(date.getMinutes()).padStart(2, "0")} ${hoursValue < 12 ? "AM" : "PM"}`;
}

function stateLabel(state: GroupEvent["state"]) {
  if (state === "PROPOSED") return "PROPUESTA";
  if (state === "CONFIRMED") return "CONFIRMADA";
  if (state === "NOT_CONFIRMED") return "NO CONFIRMADA";
  if (state === "EXPIRED") return "EXPIRADA";
  return "CANCELADA";
}

export function CalendarGrid(props: Props) {
  const [now, setNow] = useState(() => new Date());
  const [gridWidth, setGridWidth] = useState(0);
  const gridRef = useRef<HTMLElement>(null);
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
  const visibleEvents = props.events.filter((event) => props.showCancelled || event.state !== "CANCELLED");
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

  if (props.view === "Agenda") {
    return (
      <main className="agenda-view">
        {visibleEvents.map((event) => (
          <button key={event.uid} onClick={() => props.onSelect(event)} className={`agenda-item state-${event.state.toLowerCase()}`}>
            <time>{event.payload.start?.replace("T", " ").slice(0, 16)}</time>
            <div><strong>{event.payload.title ?? "Evento"}</strong><small>{stateLabel(event.state)}</small></div>
          </button>
        ))}
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
            {visibleEvents.filter((event) => event.payload.start?.slice(0, 10) === `${day.getFullYear()}-${String(day.getMonth() + 1).padStart(2, "0")}-${String(day.getDate()).padStart(2, "0")}`).map((event) => (
              <button key={event.uid} onClick={() => props.onSelect(event)} className={`month-event state-${event.state.toLowerCase()}`}>
                {event.kind === "personal" ? <Lock size={10} /> : null}{event.payload.title}
              </button>
            ))}
          </div>
        ))}
      </main>
    );
  }

  return (
    <main ref={gridRef} className={`calendar-grid ${props.view === "Día" ? "day-view" : ""}`}>
      <div className="timezone">GMT−4</div>
      {days.map((day, i) => (
        <div key={i} className="day-heading">
          <span>{day.toLocaleDateString("es", { weekday: "short" }).toUpperCase()}</span>
          <b className={
            day.toDateString() === now.toDateString() ? "today" :
            day.toDateString() === props.date.toDateString() ? "active-date" : ""
          }>{day.getDate()}</b>
        </div>
      ))}
      {hours.map((hour) => (
        <div className="hour-row" key={hour}>
          <time>{formatHour(hour)}</time>
          {days.map((day, dayIndex) => <div className="hour-cell" key={dayIndex} onDoubleClick={() => props.onQuickCreate?.(day, hour)}>
            {props.busyUsers.map((user) => props.availability[user]?.map((window, i) =>
              window.start && window.end ? <span key={`${user}-${i}`} className={`busy-block busy-${user}`} /> : null
            ))}
          </div>)}
        </div>
      ))}
      {visibleEvents.map((event, i) => {
        const start = event.payload.start ? new Date(event.payload.start.replace(" ", "T")) : null;
        const dayIndex = start
          ? days.findIndex((day) => day.toDateString() === start.toDateString())
          : -1;
        if (dayIndex < 0) return null;
        const minutes = start ? start.getHours() * 60 + start.getMinutes() : 9 * 60;
        const top = 62 + (minutes / 60) * 64;
        const conflict = localConflict(event, props.personalEvents);
        return (
          <button
            key={event.uid}
            onClick={() => props.onSelect(event)}
            className={`calendar-event state-${event.state.toLowerCase()} ${event.kind === "personal" ? "personal-event" : ""}`}
            style={{
              left: props.view === "Día"
                ? 74
                : 68 + ((gridWidth - 68) / 7) * dayIndex + 6,
              width: props.view === "Día"
                ? undefined
                : Math.max(60, (gridWidth - 68) / 7 - 12),
              top
            }}
          >
            {event.kind === "personal" && <Lock size={12} />}
            {event.state === "CONFIRMED" && <ShieldCheck size={12} />}
            <strong>{event.payload.title ?? "Evento"}</strong>
            <span>{start ? formatTime(start) : "9:00 AM"} · {stateLabel(event.state)}</span>
            {conflict && <em><AlertTriangle size={11} /> Conflicto de horario</em>}
          </button>
        );
      })}
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
