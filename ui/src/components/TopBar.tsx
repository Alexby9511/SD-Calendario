import { useEffect, useRef, useState } from "react";
import { ChevronDown, ChevronLeft, ChevronRight, Menu, Moon, RefreshCw, Sun } from "lucide-react";

interface Props {
  user: string;
  group: string;
  keyId: string;
  offline: boolean;
  view: "Día" | "Semana" | "Mes" | "Año" | "Agenda";
  title: string;
  pending: number;
  cursor: number;
  online: boolean;
  theme: "dark" | "light";
  onUser: (value: string) => void;
  onGroup: (value: string) => void;
  onOffline: (value: boolean) => void;
  onView: (value: "Día" | "Semana" | "Mes" | "Año" | "Agenda") => void;
  onToday: () => void;
  onMove: (direction: -1 | 1) => void;
  onSync: () => void;
  onMenu: () => void;
  onTheme: () => void;
  onInspector: () => void;
}

export function TopBar(props: Props) {
  const [networkOpen, setNetworkOpen] = useState(false);
  const [accountOpen, setAccountOpen] = useState(false);
  const [viewOpen, setViewOpen] = useState(false);
  const networkRef = useRef<HTMLDivElement>(null);
  const accountRef = useRef<HTMLDivElement>(null);
  const viewRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const close = (event: MouseEvent) => {
      const target = event.target as Node;
      if (!networkRef.current?.contains(target)) setNetworkOpen(false);
      if (!accountRef.current?.contains(target)) setAccountOpen(false);
      if (!viewRef.current?.contains(target)) setViewOpen(false);
    };
    document.addEventListener("mousedown", close);
    return () => document.removeEventListener("mousedown", close);
  }, []);

  return (
    <header className="topbar">
      <div className="brand-controls">
        <button className="icon-button menu-button" aria-label="Mostrar menú" onClick={props.onMenu}><Menu size={20} /></button>
        <span className="brand-mark"><span>31</span></span>
        <strong>SD Calendar</strong>
      </div>

      <nav className="time-controls">
        <button className="today-pill" onClick={props.onToday}>Hoy</button>
        <button className="icon-button" aria-label="Anterior" onClick={() => props.onMove(-1)}><ChevronLeft size={17} /></button>
        <button className="icon-button" aria-label="Siguiente" onClick={() => props.onMove(1)}><ChevronRight size={17} /></button>
        <strong className="month-title">{props.title}</strong>
      </nav>

      <div className="network-controls">
        <div className="popover-anchor" ref={viewRef}>
          <button className="view-pill" onClick={() => setViewOpen((open) => !open)}>{props.view}<ChevronDown size={14} /></button>
          {viewOpen && <div className="top-popover view-popover">
            {(["Día", "Semana", "Mes", "Año", "Agenda"] as const).map((view) => (
              <button key={view} className={props.view === view ? "selected" : ""} onClick={() => { props.onView(view); setViewOpen(false); }}>{view}</button>
            ))}
          </div>}
        </div>

        <div className="popover-anchor" ref={networkRef}>
          <button className={`network-pill ${props.offline ? "offline" : ""}`} onClick={() => setNetworkOpen((open) => !open)}>
            <span className="status-dot" />{props.offline ? "Offline" : props.online ? "Online" : "Sin red"}{props.pending > 0 && <b>{props.pending}</b>}
          </button>
          {networkOpen && <div className="top-popover network-popover">
            <div className="popover-heading"><span>Estado distribuido</span><small>sync #{props.cursor}</small></div>
            <label className="switch-row"><span><strong>Modo offline</strong><small>Acumular en el Outbox</small></span><input type="checkbox" checked={props.offline} onChange={(e) => props.onOffline(e.target.checked)} /></label>
            <button className="popover-action" onClick={props.onSync}><RefreshCw size={15} /> Sincronizar ahora</button>
            <button className="popover-action" onClick={props.onInspector}>Abrir inspector técnico</button>
          </div>}
        </div>

        <button className="icon-button theme-button" aria-label="Cambiar tema" onClick={props.onTheme}>
          {props.theme === "dark" ? <Sun size={18} /> : <Moon size={18} />}
        </button>

        <div className="popover-anchor" ref={accountRef}>
          <button className="user-avatar" aria-label="Cuenta activa" onClick={() => setAccountOpen((open) => !open)}>{props.user[0].toUpperCase()}</button>
          {accountOpen && <div className="top-popover account-popover">
            <div className="account-identity"><span className="user-avatar large">{props.user[0].toUpperCase()}</span><div><strong>{props.user}</strong><small>{props.keyId}</small></div></div>
            <label className="account-label">Usuario activo</label>
            <div className="account-users">
              {["alice", "bob", "carol"].map((user) => <button key={user} className={props.user === user ? "selected" : ""} onClick={() => props.onUser(user)}>{user[0].toUpperCase()}</button>)}
            </div>
            <label className="account-label" htmlFor="group-id">Group ID</label>
            <input id="group-id" className="group-input" value={props.group} onChange={(e) => props.onGroup(e.target.value)} />
          </div>}
        </div>
      </div>
    </header>
  );
}
