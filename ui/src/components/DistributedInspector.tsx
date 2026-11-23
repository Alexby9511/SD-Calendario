import { Database, ListTree, Signature, X } from "lucide-react";
import type { OutboxItem } from "../lib/outbox";
import type { SyncEntry } from "../types/protocol";

interface Props {
  open: boolean;
  outbox: OutboxItem[];
  entries: SyncEntry[];
  cursor: number;
  onToggle: () => void;
}

export function DistributedInspector(props: Props) {
  if (!props.open) return null;
  return (
    <aside className="inspector-drawer open">
      <header><span><Database size={16} /> Inspector técnico</span><button className="icon-button" onClick={props.onToggle}><X size={16} /></button></header>
      <div className="inspector-grid">
        <section>
          <h4>Outbox <small>{props.outbox.length}</small></h4>
          {props.outbox.length === 0 && <p className="empty">Sin mensajes pendientes</p>}
          {props.outbox.map((item) => (
            <div className="drawer-row" key={item.envelope.msg_id} title={item.error ?? ""}>
              <span className={`state-dot ${item.state.replace(" ", "-").toLowerCase()}`} />
              <b>{item.envelope.type}</b><code>{item.envelope.msg_id.slice(0, 10)}</code><em>{item.state}</em>
            </div>
          ))}
        </section>
        <section>
          <h4><ListTree size={13} /> Log reconciliado</h4>
          {props.entries.map((entry) => (
            <div className="drawer-row" key={entry.index}>
              <span className="log-index">#{entry.index}</span><b>{entry.command.type}</b><code>{entry.command.uid.slice(0, 14)}</code>
            </div>
          ))}
        </section>
        <section>
          <h4><Signature size={13} /> Firmas Ed25519</h4>
          {props.outbox.slice(-4).map((item) => (
            <div className="signature-row" key={item.envelope.msg_id}>
              <code>{item.envelope.signature.slice(0, 22)}…</code><small>{item.envelope.key_id}</small>
            </div>
          ))}
        </section>
      </div>
    </aside>
  );
}
