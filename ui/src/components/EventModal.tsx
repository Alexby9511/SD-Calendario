import { useState } from "react";
import { AlertTriangle, CalendarPlus, MapPin, Users, X } from "lucide-react";

interface Props {
  open: boolean;
  user: string;
  availability: Record<string, { start: string; end: string }[]>;
  onClose: () => void;
  onSavePersonal: (event: { title: string; start: string; end: string; tzid: string; privacy: "private" | "busy" | "details"; hide: boolean }) => void;
  onSaveGroup: (event: { title: string; location: string; start: string; end: string; participants: { user_id: string; role: "REQUIRED" | "OPTIONAL" }[]; rule: "ALL" | "MAJORITY" | "QUORUM" | "ORGANIZER"; quorum: number }) => void;
}

export function EventModal(props: Props) {
  const [tab, setTab] = useState<"personal" | "group">("personal");
  const [title, setTitle] = useState("Nueva actividad");
  const [location, setLocation] = useState("");
  const [start, setStart] = useState("2026-11-23T09:00");
  const [end, setEnd] = useState("2026-11-23T10:00");
  const [privacy, setPrivacy] = useState<"private" | "busy" | "details">("busy");
  const [hide, setHide] = useState(false);
  const [rule, setRule] = useState<"ALL" | "MAJORITY" | "QUORUM" | "ORGANIZER">("ALL");
  const [participants, setParticipants] = useState(["bob", "carol"]);
  const conflicts = participants.some((user) =>
    (props.availability[user] ?? []).some((window) => start.replace("T", " ") < window.end && window.start < end.replace("T", " "))
  );

  if (!props.open) return null;

  const submit = () => {
    if (tab === "personal") {
      props.onSavePersonal({ title, start: start.replace("T", " "), end: end.replace("T", " "), tzid: "America/New_York", privacy, hide });
    } else {
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
          <button className={tab === "personal" ? "active" : ""} onClick={() => setTab("personal")}>Evento personal</button>
          <button className={tab === "group" ? "active" : ""} onClick={() => setTab("group")}><Users size={14} /> Reunión de grupo</button>
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
              <label>Privacidad<select value={privacy} onChange={(e) => setPrivacy(e.target.value as typeof privacy)}>
                <option value="private">Private — no compartir detalles</option>
                <option value="busy">Busy — solo ventana opaca</option>
                <option value="details">Details — detalles limitados</option>
              </select></label>
              <label className="checkbox-field"><input type="checkbox" checked={hide} onChange={(e) => setHide(e.target.checked)} /> Ocultar de Free/Busy</label>
            </>
          ) : (
            <>
              <label>Participantes</label>
              <div className="participant-picker">
                {["bob", "carol"].map((user) => (
                  <button key={user} className={participants.includes(user) ? "selected" : ""} onClick={() => setParticipants((current) =>
                    current.includes(user) ? current.filter((item) => item !== user) : [...current, user]
                  )}>{user}</button>
                ))}
              </div>
              <label>Confirmación<select value={rule} onChange={(e) => setRule(e.target.value as typeof rule)}>
                <option value="ALL">ALL — todos aceptan</option>
                <option value="MAJORITY">MAJORITY</option>
                <option value="QUORUM">QUORUM — 2</option>
                <option value="ORGANIZER">ORGANIZER</option>
              </select></label>
              {conflicts && <div className="conflict-warning"><AlertTriangle size={15} /> Posible conflicto con disponibilidad publicada.</div>}
            </>
          )}
        </div>
        <footer><button className="subtle-button" onClick={props.onClose}>Cancelar</button><button className="primary-button" onClick={submit}>Guardar</button></footer>
      </section>
    </div>
  );
}
