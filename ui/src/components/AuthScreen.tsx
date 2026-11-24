import { FormEvent, useState } from "react";
import { login, register } from "../lib/api";
import type { AuthSession } from "../lib/api";

interface Props {
  onAuthenticated: (session: AuthSession) => void;
}

export function AuthScreen({ onAuthenticated }: Props) {
  const [mode, setMode] = useState<"register" | "login">("register");
  const [userId, setUserId] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");
  const [submitting, setSubmitting] = useState(false);

  const submit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setError("");
    setSubmitting(true);
    try {
      const session = await (mode === "register"
        ? register(userId, password)
        : login(userId, password));
      onAuthenticated(session);
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : "Ocurrió un error inesperado.");
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <main className="auth-screen">
      <section className="auth-card" aria-labelledby="auth-title">
        <div className="auth-brand">
          <span className="brand-mark"><span>31</span></span>
          <strong>SD Calendar</strong>
        </div>
        <div className="auth-heading">
          <h1 id="auth-title">{mode === "register" ? "Crea tu cuenta" : "Inicia sesión"}</h1>
          <p>
            {mode === "register"
              ? "Regístrate para empezar a organizar tus eventos."
              : "Entra para ver tus eventos e invitaciones."}
          </p>
        </div>
        <div className="auth-tabs" role="tablist" aria-label="Acceso a la cuenta">
          <button
            type="button"
            role="tab"
            aria-selected={mode === "register"}
            className={mode === "register" ? "active" : ""}
            onClick={() => { setMode("register"); setError(""); }}
          >
            Registrarme
          </button>
          <button
            type="button"
            role="tab"
            aria-selected={mode === "login"}
            className={mode === "login" ? "active" : ""}
            onClick={() => { setMode("login"); setError(""); }}
          >
            Iniciar sesión
          </button>
        </div>
        <form className="auth-form" onSubmit={submit}>
          <label htmlFor="auth-user-id">Identificador único</label>
          <input
            id="auth-user-id"
            value={userId}
            onChange={(event) => setUserId(event.target.value)}
            autoComplete="username"
            minLength={3}
            maxLength={32}
            pattern="[A-Za-z0-9][A-Za-z0-9_.-]{2,31}"
            placeholder="p. ej. ana_garcia"
            required
          />
          <small>3–32 caracteres: letras, números, punto, guion o guion bajo.</small>
          <label htmlFor="auth-password">Contraseña</label>
          <input
            id="auth-password"
            type="password"
            value={password}
            onChange={(event) => setPassword(event.target.value)}
            maxLength={256}
            autoComplete={mode === "register" ? "new-password" : "current-password"}
            minLength={mode === "register" ? 8 : undefined}
            placeholder={mode === "register" ? "Al menos 8 caracteres" : "Tu contraseña"}
            required
          />
          {error && <p className="auth-error" role="alert">{error}</p>}
          <button className="primary-button auth-submit" type="submit" disabled={submitting}>
            {submitting
              ? "Un momento..."
              : mode === "register" ? "Crear cuenta" : "Entrar al calendario"}
          </button>
        </form>
      </section>
    </main>
  );
}
