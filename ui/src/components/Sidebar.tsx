import { useState } from "react";
import type { CSSProperties } from "react";
import { CalendarDays, ChevronLeft, ChevronRight, Plus, Search, Share2, Trash2, User, Users, X } from "lucide-react";
import type { CalendarShare, CalendarSharePermission, CustomCalendar, GroupEvent, SharedCalendarId } from "../types/protocol";
import { ShareCalendarModal } from "./ShareCalendarModal";
import { CreateCalendarModal } from "./CreateCalendarModal";

const emptyUsers: string[] = [];
const invitationDateFormatter = new Intl.DateTimeFormat("es", {
  day: "numeric",
  month: "short"
});
const invitationTimeFormatter = new Intl.DateTimeFormat("en-US", {
  hour: "numeric",
  minute: "2-digit",
  hour12: true
});

function formatInvitationStart(value?: string) {
  if (!value) return "Sin fecha";
  const date = new Date(value.replace(" ", "T"));
  if (Number.isNaN(date.getTime())) return "Fecha no válida";
  return `${invitationDateFormatter.format(date)} ${invitationTimeFormatter.format(date)}`;
}

interface Props {
  date: Date;
  events: GroupEvent[];
  pendingInvitationUids: ReadonlySet<string>;
  busyUsers: string[];
  availability: Record<string, "busy" | "available">;
  personalVisible: boolean;
  groupVisible: boolean;
  onBusyUsers: (users: string[]) => void;
  onPersonalVisible: (visible: boolean) => void;
  onGroupVisible: (visible: boolean) => void;
  onCreate: (mode: "personal" | "group") => void;
  onSelectEvent: (event: GroupEvent) => void;
  showCancelled: boolean;
  onShowCancelled: (show: boolean) => void;
  view: "Día" | "Semana" | "Mes" | "Año" | "Agenda";
  onMoveMonth: (direction: -1 | 1) => void;
  onSelectDate: (date: Date) => void;
  currentUser: string;
  friendUsers: string[];
  shareUsers: string[];
  sharedCalendarColors: Record<string, string>;
  shares: CalendarShare[];
  onSaveCalendarShare: (calendar: SharedCalendarId, users: string[], permission: CalendarSharePermission) => void;
  customCalendars: CustomCalendar[];
  onAddCustomCalendar: (calendar: { name: string; color: string; visibility: "private" | "public" }) => void;
  onToggleCustomCalendar: (id: string) => void;
  onDeleteCustomCalendar: (id: string) => void;
}

export function Sidebar(props: Props) {
  const [createMenu, setCreateMenu] = useState(false);
  const [userSearch, setUserSearch] = useState("");
  const [shareCalendar, setShareCalendar] = useState<SharedCalendarId | null>(null);
  const [createCalendarOpen, setCreateCalendarOpen] = useState(false);
  const [deleteCalendar, setDeleteCalendar] = useState<CustomCalendar | null>(null);
  const year = props.date.getFullYear();
  const month = props.date.getMonth();
  const invitations = props.events.filter((event) => props.pendingInvitationUids.has(event.uid));
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
  const existingShare = shareCalendar
    ? props.shares.find((share) => share.owner === props.currentUser && share.calendar === shareCalendar)
    : undefined;
  const shareCalendarName = shareCalendar === "personal"
    ? "Mi calendario personal"
    : shareCalendar === "group"
      ? "Reuniones de grupo"
      : props.customCalendars.find((calendar) => calendar.id === shareCalendar)?.name ?? "Calendario";

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
          <button onClick={() => { props.onCreate("personal"); setCreateMenu(false); }}><User size={14} /> Evento personal</button>
          <button onClick={() => { props.onCreate("group"); setCreateMenu(false); }}><Users size={14} /> Reunión de grupo</button>
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
                  isActive ? "active-date" : "",
                  inActiveWeek ? "active-week" : ""
                ].filter(Boolean).join(" ")}
                onClick={() => props.onSelectDate(new Date(cellDate))}
              >
                {cellDate.getDate()}
              </button>
            );
          })}
        </div>
      </section>

      <section className="availability-search">
        <div className="search-pill"><Search size={14} /><input value={userSearch} onChange={(event) => setUserSearch(event.target.value)} placeholder="Buscar a gente" /></div>
      </section>

      <section className="calendar-layers">
        <div className="calendar-layers-heading">
          <h3>Capas</h3>
          <button
            type="button"
            className="add-calendar-button"
            aria-label="Crear nuevo calendario"
            title="Crear nuevo calendario"
            onClick={() => setCreateCalendarOpen(true)}
          >
            <Plus size={15} />
          </button>
        </div>
        <div className="calendar-layer-row">
          <label><input type="checkbox" checked={props.personalVisible} onChange={(e) => props.onPersonalVisible(e.target.checked)} /> Mi calendario personal</label>
          <button
            type="button"
            className={`share-calendar-button ${existingShare && shareCalendar === "personal" ? "has-shares" : ""}`}
            aria-label="Compartir Mi calendario personal"
            title="Compartir Mi calendario personal"
            onClick={() => setShareCalendar("personal")}
          ><Share2 size={15} /></button>
        </div>
        <div className="calendar-layer-row">
          <label><input type="checkbox" checked={props.groupVisible} onChange={(e) => props.onGroupVisible(e.target.checked)} /> Reuniones de grupo</label>
          <button
            type="button"
            className={`share-calendar-button ${props.shares.some((share) => share.owner === props.currentUser && share.calendar === "group") ? "has-shares" : ""}`}
            aria-label="Compartir Reuniones de grupo"
            title="Compartir Reuniones de grupo"
            onClick={() => setShareCalendar("group")}
          ><Share2 size={15} /></button>
        </div>
        {props.customCalendars.map((calendar) => (
          <div className="calendar-layer-row custom-calendar-row" key={calendar.id}>
            <label>
              <input
                type="checkbox"
                checked={calendar.visible}
                onChange={() => props.onToggleCustomCalendar(calendar.id)}
                style={{
                  "--calendar-color": calendar.color,
                  "--calendar-check-color": "#ffffff",
                  borderColor: calendar.color,
                  backgroundColor: calendar.visible ? calendar.color : "transparent"
                } as CSSProperties}
              />
              {calendar.name}
            </label>
            <div className="custom-calendar-actions">
              <button
                type="button"
                className={`share-calendar-button ${props.shares.some((share) => share.owner === props.currentUser && share.calendar === calendar.id) ? "has-shares" : ""}`}
                aria-label={`Compartir ${calendar.name}`}
                title={`Compartir ${calendar.name}`}
                onClick={() => setShareCalendar(calendar.id)}
              >
                <Share2 size={14} />
              </button>
              <button
                type="button"
                className="delete-calendar-button"
                aria-label={`Eliminar ${calendar.name}`}
                title={`Eliminar ${calendar.name}`}
                onClick={() => setDeleteCalendar(calendar)}
              >
                <Trash2 size={14} />
              </button>
            </div>
          </div>
        ))}
        <label><input type="checkbox" checked={props.showCancelled} onChange={(e) => props.onShowCancelled(e.target.checked)} /> Mostrar cancelados / expirados</label>
      </section>

      <section className="calendar-layers">
        <h3>Amigos</h3>
        {props.friendUsers
          .filter((user) => user.toLocaleLowerCase().includes(userSearch.trim().toLocaleLowerCase()))
          .map((user) => (
          <label className="friend-row" key={user}>
            <input
              type="checkbox"
              checked={props.busyUsers.includes(user)}
              onChange={() => toggleUser(user)}
              style={{
                "--calendar-check-color": "#17212f",
                borderColor: props.sharedCalendarColors[user],
                backgroundColor: props.busyUsers.includes(user)
                  ? props.sharedCalendarColors[user]
                  : "transparent"
              } as CSSProperties}
            />
            {user}
            <small className={`availability-state ${props.availability[user] ?? "unknown"}`}>
              {props.availability[user] ?? "sin datos"}
            </small>
          </label>
        ))}
      </section>

      <section className="invitations">
        <h3><CalendarDays size={15} /> Invitaciones <b>{invitations.length}</b></h3>
        {invitations.slice(0, 3).map((event) => (
          <button key={event.uid} onClick={() => props.onSelectEvent(event)}>
            <span className="invite-dot" />
            <span><strong>{event.payload.title ?? "Reunión"}</strong><small>Pendiente de respuesta · {formatInvitationStart(event.payload.start)}</small></span>
          </button>
        ))}
      </section>
      <ShareCalendarModal
        open={shareCalendar !== null}
        calendar={shareCalendar}
        calendarName={shareCalendarName}
        currentUser={props.currentUser}
        users={props.shareUsers}
        initialUsers={existingShare?.users ?? emptyUsers}
        initialPermission={existingShare?.permission ?? "freebusy"}
        onClose={() => setShareCalendar(null)}
        onSave={(users, permission) => {
          if (!shareCalendar) return;
          props.onSaveCalendarShare(shareCalendar, users, permission);
          setShareCalendar(null);
        }}
      />
      <CreateCalendarModal
        open={createCalendarOpen}
        onClose={() => setCreateCalendarOpen(false)}
        onSave={(calendar) => {
          props.onAddCustomCalendar(calendar);
          setCreateCalendarOpen(false);
        }}
      />
      {deleteCalendar && (
        <div className="modal-backdrop" onMouseDown={(event) => event.target === event.currentTarget && setDeleteCalendar(null)}>
          <section className="delete-calendar-modal" role="dialog" aria-modal="true" aria-labelledby="delete-calendar-title">
            <header>
              <h2 id="delete-calendar-title">Eliminar calendario</h2>
              <button className="icon-button" type="button" aria-label="Cerrar" onClick={() => setDeleteCalendar(null)}>
                <X size={18} />
              </button>
            </header>
            <div className="delete-calendar-body">
              ¿Eliminar <strong>{deleteCalendar.name}</strong>? Esta acción quitará la capa de tu lista.
            </div>
            <footer>
              <button className="subtle-button" type="button" onClick={() => setDeleteCalendar(null)}>Cancelar</button>
              <button
                className="danger-button"
                type="button"
                onClick={() => {
                  props.onDeleteCustomCalendar(deleteCalendar.id);
                  setDeleteCalendar(null);
                }}
              >
                Eliminar
              </button>
            </footer>
          </section>
        </div>
      )}
    </aside>
  );
}
