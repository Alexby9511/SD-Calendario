import { useEffect, useState } from "react";
import { AlertTriangle, CalendarPlus, MapPin, Plus, User, UserPlus, Users, X } from "lucide-react";

interface Props {
  open: boolean;
  initialMode: "personal" | "group";
  initialStart?: string;
  initialEnd?: string;
  user: string;
  availability: Record<string, { start: string; end: string }[]>;
  onClose: () => void;
  onSavePersonal: (event: { title: string; start: string; end: string; tzid: string; visibility: "private" | "public"; availability: "busy" | "free"; privacy: "private" | "public"; hide: boolean }) => void;
  onSaveGroup: (event: { title: string; location: string; start: string; end: string; participants: { user_id: string; role: "REQUIRED" | "OPTIONAL" }[]; rule: "ALL" | "MAJORITY" | "QUORUM" | "ORGANIZER"; quorum: number }) => void;
}

export function EventModal(props: Props) {
  const [tab, setTab] = useState<"personal" | "group">("personal");
  const [title, setTitle] = useState("Nueva actividad");
  const [location, setLocation] = useState("");
  const [start, setStart] = useState("2026-11-23T09:00");
  const [end, setEnd] = useState("2026-11-23T10:00");
  const [visibility, setVisibility] = useState<"private" | "public">("private");
  const [availability, setAvailability] = useState<"busy" | "free">("busy");
  const [rule, setRule] = useState<"ALL" | "MAJORITY" | "QUORUM" | "ORGANIZER">("ALL");
  const [participants, setParticipants] = useState<string[]>([]);
  const [participantPickerOpen, setParticipantPickerOpen] = useState(false);
  const availableParticipants = Array.from(new Set(["bob", "carol", ...Object.keys(props.availability)]));
  const conflicts = participants.some((user) =>
    (props.availability[user] ?? []).some((window) => start.replace("T", " ") < window.end && window.start < end.replace("T", " "))
  );

  useEffect(() => {
    if (props.open) {
      setTab(props.initialMode);
      setTitle("Nueva actividad");
      setLocation("");
      setStart(props.initialStart ?? "2026-11-23T09:00");
      setEnd(props.initialEnd ?? "2026-11-23T10:00");
      setVisibility("private");
      setAvailability("busy");
      setRule("ALL");
      setParticipants([]);
      setParticipantPickerOpen(false);
    }
  }, [props.open, props.initialMode, props.initialStart, props.initialEnd]);

  if (!props.open) return null;

  const changeTab = (mode: "personal" | "group") => {
    setTab(mode);
    setVisibility("private");
    setAvailability("busy");
    setRule("ALL");
    setParticipants([]);
    setParticipantPickerOpen(false);
  };

  const submit = () => {
    if (tab === "personal") {
      props.onSavePersonal({
        title,
        start: start.replace("T", " "),
        end: end.replace("T", " "),
        tzid: "America/New_York",
        visibility,
        availability,
        privacy: visibility,
        hide: availability === "free"
      });
    } else {
      if (participants.length === 0) return;
      props.onSaveGroup({
        title,
        location,
        start: start.replace("T", " "),
        end: end.replace("T", " "),
        participants: participants.map((user_id) => ({ user_id, role: "REQUIRED" })),
        rule,
        quorum: 2
      });
    }
  };

  return (
    <div className="modal-backdrop" onMouseDown={(e) => e.target === e.currentTarget && props.onClose()}>
      <section className="event-modal">
        <header>
          <h2><CalendarPlus size={18} /> Crear</h2>
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
                <option value="QUORUM">Quórum — 2</option>
                <option value="ORGANIZER">Organizador</option>
              </select></label>
              {conflicts && <div className="conflict-warning"><AlertTriangle size={15} /> Posible conflicto con disponibilidad publicada.</div>}
            </>
          )}
        </div>
        <footer>
          <button className="subtle-button" onClick={props.onClose}>Cancelar</button>
          <button className="primary-button" onClick={submit} disabled={tab === "group" && participants.length === 0}>Guardar</button>
        </footer>
      </section>
    </div>
  );
}
