# SD Calendario UI

Aplicación React + TypeScript + Vite para el calendario local-first de SD-Calendario.

## Requisitos

- Node.js 18+
- Backend FastAPI disponible en `http://localhost:8081`

## Instalación y ejecución

Inicie el backend en `8081` con un token configurado para cada usuario de prueba:

```powershell
$env:SDCAL_API_TOKENS='{"alice":"<token-alice-de-32-caracteres-o-mas>","bob":"<token-bob-de-32-caracteres-o-mas>"}'
python -m server.cmd.sdcald --no-ray --port 8081
```

Inicie la UI en otra terminal:

```bash
cd ui
npm install
npm run dev
```

Abrir `http://localhost:5173`.

En cada navegador o perfil, abra **Cuenta**, seleccione el usuario e introduzca
su token Bearer. La UI guarda los tokens en el almacenamiento local de ese
navegador y los envía a la API; el servidor valida que coincidan con el emisor
de cada comando. Para probar una invitación entre dos usuarios, use perfiles
distintos, configure Alice en uno y Bob en el otro, cree la reunión con Bob como
participante y acepte o rechace desde su perfil.

## Construcción

```bash
npm run build
npm run preview
```

## Demostración offline

1. Active el interruptor **Offline** en la barra superior.
2. Cree eventos personales o reuniones de grupo.
3. Los sobres firmados se almacenan en `localStorage` como outbox pendiente.
4. Vuelva a conectarse y la aplicación reintentará el outbox automáticamente; también puede pulsar **Sincronizar ahora**.
5. La aplicación enviará los comandos autenticados a `/v1/commands`, consultará `/v1/sync`, reconciliará el cursor local y mostrará la auditoría en el Inspector distribuido.
