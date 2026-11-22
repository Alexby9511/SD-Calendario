import { useEffect, useState } from "react";
import { CalendarPlus, X } from "lucide-react";

interface Props {
  open: boolean;
  onClose: () => void;
  onSave: (calendar: { name: string; color: string; visibility: "private" | "public" }) => void;
}

const calendarColors = ["#81c995", "#c58af9", "#e5a84b", "#f28b82", "#dc5f67", "#4db6ac"];

export function CreateCalendarModal(props: Props) {
  const [name, setName] = useState("");
  const [color, setColor] = useState(calendarColors[0]);
  const [visibility, setVisibility] = useState<"private" | "public">("private");

  useEffect(() => {
    if (!props.open) return;
    setName("");
    setColor(calendarColors[0]);
    setVisibility("private");
  }, [props.open]);

  if (!props.open) return null;

  return (
    <div className="modal-backdrop" onMouseDown={(event) => event.target === event.currentTarget && props.onClose()}>
      <section className="create-calendar-modal" role="dialog" aria-modal="true" aria-labelledby="create-calendar-title">
        <header>
          <h2 id="create-calendar-title"><CalendarPlus size={18} /> Nuevo calendario</h2>
          <button className="icon-button" type="button" aria-label="Cerrar" onClick={props.onClose}><X size={18} /></button>
        </header>
        <div className="create-calendar-body">
          <label htmlFor="calendar-name">Nombre</label>
          <input
            id="calendar-name"
            value={name}
            onChange={(event) => setName(event.target.value)}
            placeholder="Ej. Trabajo"
            autoFocus
          />
          <label>Color</label>
          <div className="calendar-color-options" aria-label="Color del calendario">
            {calendarColors.map((option) => (
              <button
                type="button"
                key={option}
                className={color === option ? "selected" : ""}
                style={{ backgroundColor: option }}
                aria-label={`Usar color ${option}`}
                onClick={() => setColor(option)}
              />
            ))}
          </div>
          <label htmlFor="calendar-visibility">Visibilidad</label>
          <select
            id="calendar-visibility"
            value={visibility}
            onChange={(event) => setVisibility(event.target.value as "private" | "public")}
          >
            <option value="private">Privado</option>
            <option value="public">Público</option>
          </select>
        </div>
        <footer>
          <button className="subtle-button" type="button" onClick={props.onClose}>Cancelar</button>
          <button
            className="primary-button"
            type="button"
            disabled={!name.trim()}
            onClick={() => props.onSave({ name: name.trim(), color, visibility })}
          >
            Crear calendario
          </button>
        </footer>
      </section>
    </div>
  );
}
