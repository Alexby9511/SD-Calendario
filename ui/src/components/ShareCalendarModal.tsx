import { useEffect, useState } from "react";
import { Search, UserPlus, X } from "lucide-react";
import type { CalendarSharePermission, SharedCalendarId } from "../types/protocol";

interface Props {
  open: boolean;
  calendar: SharedCalendarId | null;
  calendarName: string;
  currentUser: string;
  users: string[];
  initialUsers: string[];
  initialPermission: CalendarSharePermission;
  onClose: () => void;
  onSave: (users: string[], permission: CalendarSharePermission) => void;
}

const permissionDescription: Record<CalendarSharePermission, string> = {
  freebusy: "Solo verá cuándo estás ocupado, sin detalles de los eventos.",
  details: "Podrá ver los títulos y descripciones de tus eventos.",
  edit: "Podrá editar y cancelar eventos existentes."
};

export function ShareCalendarModal(props: Props) {
  const [query, setQuery] = useState("");
  const [selectedUsers, setSelectedUsers] = useState<string[]>([]);
  const [permission, setPermission] = useState<CalendarSharePermission>("freebusy");

  useEffect(() => {
    if (!props.open) return;
    setQuery("");
    setSelectedUsers(props.initialUsers);
    setPermission(props.initialPermission);
  }, [props.open, props.calendar, props.initialUsers, props.initialPermission]);

  if (!props.open || !props.calendar) return null;

  const availableUsers = props.users
    .filter((user) => user !== props.currentUser && !selectedUsers.includes(user))
    .filter((user) => user.toLocaleLowerCase().includes(query.trim().toLocaleLowerCase()));
  const typedUser = query.trim().toLowerCase();
  const canAddTypedUser = /^[a-z0-9][a-z0-9_.-]{2,31}$/.test(typedUser)
    && typedUser !== props.currentUser
    && !selectedUsers.includes(typedUser);

  const addUser = (user: string) => {
    setSelectedUsers((current) => current.includes(user) ? current : [...current, user]);
    setQuery("");
  };

  return (
    <div className="modal-backdrop" onMouseDown={(event) => event.target === event.currentTarget && props.onClose()}>
      <section className="share-modal" role="dialog" aria-modal="true" aria-labelledby="share-modal-title">
        <header>
          <div>
            <h2 id="share-modal-title">Compartir calendario</h2>
            <small>{props.calendarName}</small>
          </div>
          <button className="icon-button" aria-label="Cerrar" onClick={props.onClose}><X size={18} /></button>
        </header>
        <div className="share-modal-body">
          <label htmlFor="share-user-search">Añadir amigos o usuarios</label>
          <div className="share-search">
            <Search size={16} />
            <input
              id="share-user-search"
              value={query}
              onChange={(event) => setQuery(event.target.value)}
              placeholder="Buscar usuarios"
              autoComplete="off"
            />
          </div>
          {query && (
            <div className="share-user-options">
              {availableUsers.map((user) => (
                <button key={user} type="button" onClick={() => addUser(user)}>
                  <UserPlus size={14} /> {user}
                </button>
              ))}
              {canAddTypedUser && (
                <button type="button" onClick={() => addUser(typedUser)}>
                  <UserPlus size={14} /> Añadir {typedUser}
                </button>
              )}
              {availableUsers.length === 0 && !canAddTypedUser && <span className="empty">No se encontraron usuarios</span>}
            </div>
          )}
          <div className="share-user-chips" aria-label="Usuarios añadidos">
            {selectedUsers.map((user) => (
              <span className="participant-chip" key={user}>
                {user}
                <button
                  type="button"
                  aria-label={`Quitar a ${user}`}
                  onClick={() => setSelectedUsers((current) => current.filter((item) => item !== user))}
                >
                  <X size={13} />
                </button>
              </span>
            ))}
            {selectedUsers.length === 0 && <span className="empty">Aún no has añadido a nadie</span>}
          </div>
          <label htmlFor="share-permission">Permiso para las personas seleccionadas</label>
          <select
            id="share-permission"
            value={permission}
            onChange={(event) => setPermission(event.target.value as CalendarSharePermission)}
          >
            <option value="freebusy">Ver solo disponible/ocupado</option>
            <option value="details">Ver los detalles de los eventos</option>
            <option value="edit">Realizar cambios</option>
          </select>
          <p>{permissionDescription[permission]}</p>
          <small className="share-local-note">
            Los permisos se sincronizan con el servidor y se aplican al calendario de cada usuario.
          </small>
        </div>
        <footer>
          <button className="subtle-button" onClick={props.onClose}>Cancelar</button>
          <button className="primary-button" onClick={() => props.onSave(selectedUsers, permission)}>Guardar</button>
        </footer>
      </section>
    </div>
  );
}
