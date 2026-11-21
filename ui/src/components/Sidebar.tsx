import { useState } from "react";
import { CalendarDays, ChevronLeft, ChevronRight, Plus, Search } from "lucide-react";
import type { GroupEvent } from "../types/protocol";

interface Props {
  date: Date;
  events: GroupEvent[];
  busyUsers: string[];
  onBusyUsers: (users: string[]) => void;
  onCreate: () => void;
  onSelectEvent: (event: GroupEvent) => void;
  showCancelled: boolean;
  onShowCancelled: (show: boolean) => void;
  view: "Día" | "Semana" | "Mes" | "Año" | "Agenda";
  onMoveMonth: (direction: -1 | 1) => void;
}

export function Sidebar(props: Props) {
  const [createMenu, setCreateMenu] = useState(false);
  const [personalVisible, setPersonalVisible] = useState(true);
  const [groupVisible, setGroupVisible] = useState(true);
  const year = props.date.getFullYear();
  const month = props.date.getMonth();
  const first = new Date(year, month, 1).getDay();
  const days = new Date(year, month + 1, 0).getDate();
  const cells = Array.from({ length: 42 }, (_, i) => i - first + 2);
  const invitations = props.events.filter((event) => event.state === "PROPOSED");
  const today = new Date();
  const active = props.date;
  const mondayIndex = (new Date(year, month, 1).getDay() + 6) % 7;
  const firstDay = new Date(year, month, 1 - mondayIndex);
  const activeWeek = new Date(active);
  activeWeek.setDate(activeWeek.getDate() - ((activeWeek.getDay() + 6) % 7));
  activeWeek.setHours(0, 0, 0, 0);
  const activeWeekEnd = new Date(activeWeek);
  activeWeekEnd.setDate(activeWeekEnd.getDate() + 6);
  activeWeekEnd.setHours(23, 59, 59, 999);
  const sameDate = (a: Date, b: Date) =>
    a.getFullYear() === b.getFullYear() &&
    a.getMonth() === b.getMonth() &&
    a.getDate() === b.getDate();
  const sameMonth = year === today.getFullYear() && month === today.getMonth();

  const toggleUser = (user: string) => {
    props.onBusyUsers(
      props.busyUsers.includes(user)
        ? props.busyUsers.filter((item) => item !== user)
        : [...props.busyUsers, user]
    );
  };

  return (
    <aside className="sidebar">
      <div className="create-wrap">
        <button className="create-button" onClick={() => setCreateMenu((open) => !open)}><Plus size={20} /> Crear</button>
        {createMenu && <div className="create-menu">
          <button onClick={() => { props.onCreate(); setCreateMenu(false); }}>Evento personal</button>
          <button onClick={() => { props.onCreate(); setCreateMenu(false); }}>Reunión de grupo</button>
        </div>}
      </div>

      <section className="mini-calendar">
        <div className="mini-title">
          <span>{props.date.toLocaleDateString("es", { month: "long", year: "numeric" })}</span>
          <div><button onClick={() => props.onMoveMonth(-1)}><ChevronLeft size={14} /></button><button onClick={() => props.onMoveMonth(1)}><ChevronRight size={14} /></button></div>
        </div>
        <div className="weekday-row">{["L", "M", "X", "J", "V", "S", "D"].map((day) => <span key={day}>{day}</span>)}</div>
        <div className="mini-grid">
          {Array.from({ length: 42 }, (_, i) => {
            const cellDate = new Date(firstDay);
            cellDate.setDate(firstDay.getDate() + i);
            const isToday = sameDate(cellDate, today);
            const isActive = sameDate(cellDate, active);
            const inActiveWeek = props.view === "Semana" &&
              cellDate >= activeWeek &&
              cellDate <= activeWeekEnd;
            return (
              <button
                key={i}
                className={[
                  cellDate.getMonth() !== month ? "muted" : "",
                  isToday ? "today" : "",
                  isActive && !isToday ? "active-date" : "",
                  inActiveWeek ? "active-week" : ""
                ].filter(Boolean).join(" ")}
              >
                {cellDate.getDate()}
              </button>
            );
          })}
        </div>
      </section>

      <section className="availability-search">
        <div className="search-pill"><Search size={14} /><input placeholder="Buscar a gente" /></div>
      </section>

      <section className="calendar-layers">
        <h3>Capas</h3>
        <label><input type="checkbox" checked={personalVisible} onChange={(e) => setPersonalVisible(e.target.checked)} /> Mi calendario personal</label>
        <label><input type="checkbox" checked={groupVisible} onChange={(e) => setGroupVisible(e.target.checked)} /> Reuniones de grupo</label>
        <label><input type="checkbox" checked={props.showCancelled} onChange={(e) => props.onShowCancelled(e.target.checked)} /> Mostrar cancelados / expirados</label>
      </section>

      <section className="calendar-layers">
        <h3>Disponibilidad</h3>
        {["bob", "carol"].map((user) => (
          <label key={user}>
            <input type="checkbox" checked={props.busyUsers.includes(user)} onChange={() => toggleUser(user)} />
            <span className={`avatar ${user}`}>{user[0].toUpperCase()} </span>{user}
          </label>
        ))}
      </section>

      <section className="invitations">
        <h3><CalendarDays size={15} /> Invitaciones <b>{invitations.length}</b></h3>
        {invitations.slice(0, 3).map((event) => (
          <button key={event.uid} onClick={() => props.onSelectEvent(event)}>
            <span className="invite-dot" />
            <span><strong>{event.payload.title ?? "Reunión"}</strong><small>{event.payload.start?.slice(11, 16) ?? "Pendiente"}</small></span>
          </button>
        ))}
      </section>
    </aside>
  );
}
