import { useState } from "react";
import { Check, CircleX, Clock3, RotateCcw, X } from "lucide-react";
import type { GroupEvent } from "../types/protocol";

interface Props {
  event: GroupEvent | null;
  attendance: Record<string, string>;
  counters: Record<string, { proposed_start?: string; proposed_end?: string }>;
  currentUser: string;
  canEdit: boolean;
  onClose: () => void;
  onReply: (partstat: "ACCEPTED" | "DECLINED" | "TENTATIVE") => void;
  onCounter: (start: string, end: string) => void;
  onClosePoll: () => void;
  onReopen: () => void;
  onCancel: () => void;
  onDelete: () => void;
  onEdit: () => void;
}

export function EventDetailModal(props: Props) {
  const [counterOpen, setCounterOpen] = useState(false);
  const [counterStart, setCounterStart] = useState("");
  const [counterEnd, setCounterEnd] = useState("");
  const event = props.event;
  if (!event) return null;
  const isOrganizer = event.organizer === props.currentUser;
  const isParticipant = event.participants.includes(props.currentUser);
  return (
    <div className="modal-backdrop" onMouseDown={(e) => e.target === e.currentTarget && props.onClose()}>
      <section className="detail-modal">
        <header><div><small>UID {event.uid.slice(0, 18)}…</small><h2>{event.payload.title ?? "Reunión"}</h2></div><button className="icon-button" onClick={props.onClose}><X size={18} /></button></header>
        <div className="detail-meta">
          <span><b>Estado</b>{event.state}</span>
          <span><b>Secuencia</b>#{event.sequence}</span>
          <span><b>Organizador</b>{event.organizer}</span>
          <span><b>Horario</b>{event.payload.start?.replace("T", " ")} – {event.payload.end?.replace("T", " ")}</span>
        </div>
        <section>
          <h3>Asistencia</h3>
          {event.participants.map((user) => (
            <div className="attendee" key={user}>
              <span className={`avatar ${user}`}>{user[0].toUpperCase()}</span>
              <strong>{user}</strong>
              <em>{props.attendance[user] ?? "NEEDS_ACTION"}</em>
            </div>
          ))}
        </section>
        {Object.keys(props.counters).length > 0 && (
          <section>
            <h3>Horarios propuestos</h3>
            {Object.entries(props.counters).map(([user, counter]) => (
              <div className="attendee" key={user}>
                <strong>{user}</strong>
                <em>{counter.proposed_start?.replace("T", " ").slice(0, 16)} – {counter.proposed_end?.replace("T", " ").slice(0, 16)}</em>
              </div>
            ))}
          </section>
        )}
        {event.state === "PROPOSED" && isParticipant && (
          <div className="decision-actions">
            <button className="accept" onClick={() => props.onReply("ACCEPTED")}><Check size={15} /> Aceptar</button>
            <button onClick={() => props.onReply("DECLINED")}><CircleX size={15} /> Rechazar</button>
            <button onClick={() => props.onReply("TENTATIVE")}><Clock3 size={15} /> Quizá</button>
            <button onClick={() => {
              const start = event.payload.start ? new Date(event.payload.start.replace(" ", "T")) : new Date();
              const end = event.payload.end ? new Date(event.payload.end.replace(" ", "T")) : new Date(start.getTime() + 60 * 60 * 1000);
              start.setHours(start.getHours() + 1);
              end.setHours(end.getHours() + 1);
              const toLocal = (date: Date) => `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, "0")}-${String(date.getDate()).padStart(2, "0")}T${String(date.getHours()).padStart(2, "0")}:${String(date.getMinutes()).padStart(2, "0")}`;
              setCounterStart(toLocal(start));
              setCounterEnd(toLocal(end));
              setCounterOpen(true);
            }}>Proponer otro horario</button>
          </div>
        )}
        {counterOpen && event.state === "PROPOSED" && isParticipant && (
          <div className="decision-actions">
            <label>Nuevo inicio<input type="datetime-local" value={counterStart} onChange={(e) => setCounterStart(e.target.value)} /></label>
            <label>Nuevo fin<input type="datetime-local" value={counterEnd} onChange={(e) => setCounterEnd(e.target.value)} /></label>
            <button disabled={!counterStart || !counterEnd || new Date(counterStart) >= new Date(counterEnd)}
              onClick={() => {
                props.onCounter(new Date(counterStart).toISOString(), new Date(counterEnd).toISOString());
                setCounterOpen(false);
              }}>Enviar propuesta</button>
          </div>
        )}
        <footer>
          {isOrganizer && event.state === "PROPOSED" && <button onClick={props.onClosePoll}>Cerrar votación</button>}
          {isOrganizer && event.state !== "PROPOSED" && event.state !== "CANCELLED" && event.state !== "EXPIRED" && <button onClick={props.onReopen}><RotateCcw size={14} /> Reabrir</button>}
          {props.canEdit && event.state !== "CANCELLED" && event.state !== "EXPIRED" && <button onClick={props.onEdit}>Editar evento</button>}
          {props.canEdit && event.kind === "personal" && event.state !== "CANCELLED" && <button className="danger" onClick={props.onDelete}>Eliminar</button>}
          {props.canEdit && event.kind === "group" && event.state !== "CANCELLED" && <button className="danger" onClick={props.onCancel}>Cancelar reunión</button>}
        </footer>
      </section>
    </div>
  );
}
