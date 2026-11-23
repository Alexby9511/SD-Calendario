import { useEffect, useState } from "react";
import { AlertTriangle, CalendarPlus, MapPin, Plus, User, UserPlus, Users, X } from "lucide-react";
import type { GroupEvent } from "../types/protocol";

function defaultLocalTime(hourOffset: number) {
  const date = new Date();
  date.setMinutes(0, 0, 0);
  date.setHours(date.getHours() + hourOffset);
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, "0")}-${String(date.getDate()).padStart(2, "0")}T${String(date.getHours()).padStart(2, "0")}:${String(date.getMinutes()).padStart(2, "0")}`;
}

interface Props {
  open: boolean;
  initialMode: "personal" | "group";
  initialStart?: string;
  initialEnd?: string;
  user: string;
  availability: Record<string, "busy" | "available">;
  checkAvailability: (users: string[], start: string, end: string) => Promise<Record<string, "busy" | "available">>;
  editingEvent: GroupEvent | null;
  onClose: () => void;
  onSavePersonal: (event: { title: string; start: string; end: string; tzid: string; visibility: "private" | "public"; availability: "busy" | "free"; privacy: "private" | "public"; hide: boolean }, existing?: GroupEvent) => void;
  onSaveGroup: (event: { title: string; location: string; start: string; end: string; participants: { user_id: string; role: "REQUIRED" | "OPTIONAL" }[]; rule: "ALL" | "MAJORITY" | "QUORUM" | "ORGANIZER"; quorum: number }, existing?: GroupEvent) => void;
}

function inputDateTime(value?: string) {
  if (!value) return "";
  const date = new Date(value.replace(" ", "T"));
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, "0")}-${String(date.getDate()).padStart(2, "0")}T${String(date.getHours()).padStart(2, "0")}:${String(date.getMinutes()).padStart(2, "0")}`;
}

export function EventModal(props: Props) {
  const [tab, setTab] = useState<"personal" | "group">("personal");
  const [title, setTitle] = useState("Nueva actividad");
  const [location, setLocation] = useState("");
  const [start, setStart] = useState(() => defaultLocalTime(1));
  const [end, setEnd] = useState(() => defaultLocalTime(2));
  const [visibility, setVisibility] = useState<"private" | "public">("private");
  const [availability, setAvailability] = useState<"busy" | "free">("busy");
  const [hideFromFreebusy, setHideFromFreebusy] = useState(false);
  const [rule, setRule] = useState<"ALL" | "MAJORITY" | "QUORUM" | "ORGANIZER">("ALL");
  const [participants, setParticipants] = useState<string[]>([]);
  const [participantPickerOpen, setParticipantPickerOpen] = useState(false);
  const [checkedAvailability, setCheckedAvailability] = useState<Record<string, "busy" | "available">>({});
  const [availabilityWarning, setAvailabilityWarning] = useState("");
  const availableParticipants = Array.from(new Set(["alice", "bob", "carol", ...Object.keys(props.availability)]))
    .filter((user) => user !== props.user);

  useEffect(() => {
    if (!props.open || !start || !end || new Date(start) >= new Date(end)) return;
    let active = true;
    const users = tab === "group" ? Array.from(new Set([props.user, ...participants])) : [props.user];
    props.checkAvailability(users, new Date(start).toISOString(), new Date(end).toISOString())
      .then((result) => {
        if (active) {
          setCheckedAvailability(result);
          setAvailabilityWarning("");
        }
      })
      .catch(() => {
        if (active) setAvailabilityWarning("No se pudo consultar la disponibilidad; se verificará al guardar.");
      });
    return () => { active = false; };
  }, [props.open, props.user, props.checkAvailability, tab, participants, start, end]);

  const conflicts = (tab === "group" ? [props.user, ...participants] : [props.user])
    .some((candidate) => checkedAvailability[candidate] === "busy");
  const invalidWindow = !title.trim() || !start || !end || new Date(start) >= new Date(end);

  useEffect(() => {
    if (props.open) {
      const event = props.editingEvent;
      setTab(event?.kind ?? props.initialMode);
      setTitle(event?.payload.title ?? "Nueva actividad");
      setLocation(event?.payload.location ?? "");
      setStart(inputDateTime(event?.payload.start) || props.initialStart || defaultLocalTime(1));
      setEnd(inputDateTime(event?.payload.end) || props.initialEnd || defaultLocalTime(2));
      setVisibility(event?.payload.visibility ?? "private");
      setAvailability(event?.payload.availability ?? "busy");
      setHideFromFreebusy(event?.payload.hide_from_freebusy ?? false);
      setRule(event?.payload.confirmation_rule ?? "ALL");
      setParticipants(event?.participants ?? []);
      setParticipantPickerOpen(false);
    }
  }, [props.open, props.initialMode, props.initialStart, props.initialEnd, props.editingEvent]);

  if (!props.open) return null;

  const changeTab = (mode: "personal" | "group") => {
    setTab(mode);
    setVisibility("private");
    setAvailability("busy");
    setHideFromFreebusy(false);
    setRule("ALL");
    setParticipants([]);
    setParticipantPickerOpen(false);
  };

  const submit = () => {
    if (tab === "personal") {
      props.onSavePersonal({
        title,
        start: new Date(start).toISOString(),
        end: new Date(end).toISOString(),
        tzid: Intl.DateTimeFormat().resolvedOptions().timeZone,
        visibility,
        availability,
        privacy: visibility,
        hide: hideFromFreebusy
      }, props.editingEvent ?? undefined);
    } else {
      if (participants.length === 0) return;
      props.onSaveGroup({
        title,
        location,
        start: new Date(start).toISOString(),
        end: new Date(end).toISOString(),
        participants: participants.map((user_id) => ({ user_id, role: "REQUIRED" })),
        rule,
        quorum: Math.min(2, participants.length)
      }, props.editingEvent ?? undefined);
    }
  };

  return (
    <div className="modal-backdrop" onMouseDown={(e) => e.target === e.currentTarget && props.onClose()}>
      <section className="event-modal">
        <header>
          <h2><CalendarPlus size={18} /> {props.editingEvent ? "Editar" : "Crear"}</h2>
          <button className="icon-button" onClick={props.onClose}><X size={18} /></button>
        </header>
        <div className="modal-tabs">
          <button className={tab === "personal" ? "active" : ""} onClick={() => changeTab("personal")}><User size={14} /> Evento personal</button>
          <button className={tab === "group" ? "active" : ""} onClick={() => changeTab("group")}><Users size={14} /> Reunión de grupo</button>
        </div>
        <div className="modal-body">
          <label>Título<input value={title} onChange={(e) => setTitle(e.target.value)} /></label>
          {tab === "group" && <label><MapPin size={14} /> Ubicación<input value={location} onChange={(e) => setLocation(e.target.value)} placeholder="Sala Atlas" /></label>}
          <div className="field-row">
            <label>Inicio<input type="datetime-local" value={start} onChange={(e) => setStart(e.target.value)} /></label>
            <label>Fin<input type="datetime-local" value={end} onChange={(e) => setEnd(e.target.value)} /></label>
          </div>
          {tab === "personal" ? (
            <>
              <div className="field-row">
                <label>Visibilidad<select value={visibility} onChange={(e) => setVisibility(e.target.value as typeof visibility)}>
                  <option value="private">Privado</option>
                  <option value="public">Público</option>
                </select></label>
                <label>Disponibilidad<select value={availability} onChange={(e) => setAvailability(e.target.value as typeof availability)}>
                  <option value="busy">Ocupado</option>
                  <option value="free">Disponible</option>
                </select></label>
              </div>
              <label className="switch-row">
                <span><strong>Ocultar en disponibilidad</strong><small>Los demás no verán si estás ocupado</small></span>
                <input type="checkbox" checked={hideFromFreebusy} onChange={(e) => setHideFromFreebusy(e.target.checked)} />
              </label>
            </>
          ) : (
            <>
              <label>Participantes</label>
              <div className="participant-picker-wrap">
                <div className="participant-picker">
                  {participants.map((user) => (
                    <span className="participant-chip" key={user}>
                      {user}
                      <button type="button" aria-label={`Eliminar ${user}`} onClick={() => setParticipants((current) => current.filter((item) => item !== user))}>
                        <X size={13} />
                      </button>
                    </span>
                  ))}
                  <button type="button" className="add-participants-button" onClick={() => setParticipantPickerOpen((open) => !open)}>
                    {participantPickerOpen ? <Plus size={15} /> : <UserPlus size={15} />}
                    Añadir participantes
                  </button>
                </div>
                {participantPickerOpen && (
                  <div className="participant-options">
                    {availableParticipants.filter((user) => !participants.includes(user)).map((user) => (
                      <button type="button" key={user} onClick={() => {
                        setParticipants((current) => [...current, user]);
                        setParticipantPickerOpen(false);
                      }}>{user}</button>
                    ))}
                    {availableParticipants.every((user) => participants.includes(user)) && (
                      <span className="empty">No hay más participantes disponibles</span>
                    )}
                  </div>
                )}
              </div>
              <label>Confirmación<select value={rule} onChange={(e) => setRule(e.target.value as typeof rule)}>
                <option value="ALL">Todos deben aceptar</option>
                <option value="MAJORITY">Mayoría</option>
                <option value="QUORUM">Quórum — hasta 2</option>
                <option value="ORGANIZER">Organizador</option>
              </select></label>
            </>
          )}
          {conflicts && <div className="conflict-warning"><AlertTriangle size={15} /> Hay un conflicto de disponibilidad para este horario.</div>}
          {availabilityWarning && <div className="conflict-warning"><AlertTriangle size={15} /> {availabilityWarning}</div>}
        </div>
        <footer>
          <button className="subtle-button" onClick={props.onClose}>Cancelar</button>
          <button className="primary-button" onClick={submit} disabled={invalidWindow || (tab === "group" && participants.length === 0)}>Guardar</button>
        </footer>
      </section>
    </div>
  );
}
