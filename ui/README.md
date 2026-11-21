# SD Calendario UI

Aplicación React + TypeScript + Vite para el calendario local-first de SD-Calendario.

## Requisitos

- Node.js 18+
- Backend FastAPI disponible en `http://localhost:8081`

## Instalación y ejecución

```bash
cd ui
npm install
npm run dev
```

Abrir `http://localhost:5173`.

## Construcción

```bash
npm run build
npm run preview
```

## Demostración offline

1. Active el interruptor **Offline** en la barra superior.
2. Cree eventos personales o reuniones de grupo.
3. Los sobres firmados se almacenan en `localStorage` como outbox pendiente.
4. Desactive **Offline** y pulse **Sync**.
5. La aplicación enviará el outbox a `/v1/commands`, consultará `/v1/sync`, reconciliará el cursor local y mostrará la auditoría en el Inspector distribuido.
