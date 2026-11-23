import { Check, CircleX, Clock3, RotateCcw, ShieldCheck, X } from "lucide-react";
import type { GroupEvent } from "../types/protocol";

interface Props {
  event: GroupEvent | null;
  attendance: Record<string, string>;
  onClose: () => void;
  onReply: (partstat: "ACCEPTED" | "DECLINED" | "TENTATIVE") => void;
  onCounter: () => void;
  onClosePoll: () => void;
  onReopen: () => void;
  onCancel: () => void;
  onDelete: () => void;
}

export function EventDetailModal(props: Props) {
  const event = props.event;
  if (!event) return null;
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
        {event.state === "PROPOSED" && (
          <div className="decision-actions">
            <button className="accept" onClick={() => props.onReply("ACCEPTED")}><Check size={15} /> Aceptar</button>
            <button onClick={() => props.onReply("DECLINED")}><CircleX size={15} /> Rechazar</button>
            <button onClick={() => props.onReply("TENTATIVE")}><Clock3 size={15} /> Quizá</button>
            <button onClick={props.onCounter}>Proponer otro horario</button>
          </div>
        )}
        <footer>
          <button onClick={props.onClosePoll}>Cerrar votación</button>
          <button onClick={props.onReopen}><RotateCcw size={14} /> Reabrir</button>
          <button className="danger" onClick={props.onDelete}>Eliminar</button>
          <button className="danger" onClick={props.onCancel}>Cancelar</button>
        </footer>
      </section>
    </div>
  );
}
