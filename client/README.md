# Cliente de escritorio (Go + Wails v3)

El cliente es un **proceso Go con una ventana nativa**: SQLite, outbox y firma
viven en Go; el webview solo pinta. No hay servidor HTTP local ni IndexedDB.

## Estado

Pendiente de andamiaje. No se puede compilar en este entorno porque faltan:

| Requisito | Comprobación | Instalación |
| --- | --- | --- |
| Go 1.24+ (Wails v3) | `go version` | tarball de <https://go.dev/dl/> en `/usr/local/go` |
| `libwebkit2gtk-4.1-dev` | `dpkg -s libwebkit2gtk-4.1-dev` | `sudo apt install libwebkit2gtk-4.1-dev` |
| Node 22 (build del front) | `node --version` | ya instalado |

Con eso disponible:

```bash
go install github.com/wailsapp/wails/v3/cmd/wails3@v3.0.0
cd client
wails3 init -n sdcal -t vanilla
wails3 dev
```

`wails3 init` crea `Taskfile.yml`, `build/` y `frontend/dist`; luego se sustituye
`main.go` por el que describe la configuración de ventana de abajo.

## Configuración de ventana decidida

Una sola ventana principal, con navegación y modales **dentro** de la página.
Las ventanas secundarias nativas existen en Wails v3, pero `AttachModal` solo
funciona en macOS; apoyar la UX en ventanas hijas daría comportamiento distinto
en Linux.

```go
app := application.New(application.Options{
    Name: "SD Calendario",
    Services: []application.Service{
        application.NewService(&calendar.Service{}),
    },
    Assets: application.AssetOptions{
        Handler: application.AssetFileServerFS(assets),
    },
})

main := app.Window.NewWithOptions(application.WebviewWindowOptions{
    Name:             "main",
    Title:            "SD Calendario",
    Width:            1280,
    Height:           800,
    MinWidth:         960,
    MinHeight:        600,
    InitialPosition:  application.WindowCentered,
    BackgroundColour: application.NewRGB(24, 25, 29), // evita el flash blanco
    Mac: application.MacWindow{
        TitleBar: application.MacTitleBar{AppearsTransparent: true},
    },
})
```

Opciones útiles que quedan disponibles sin tocar el diseño:

| Necesidad | Campo / método |
| --- | --- |
| Tamaño y posición inicial | `Width`, `Height`, `Min/MaxWidth`, `Min/MaxHeight`, `InitialPosition` |
| Arrancar maximizado | `StartState: application.WindowStateMaximised` |
| Ventana sin bordes | `Frameless: true` + CSS `--wails-draggable` en la barra propia |
| Evitar flash blanco | `Hidden: true` + `window.Show()` al cargar |
| Menú de aplicación | `app.Menu.Set(...)` + `UseApplicationMenu: true` |
| Icono en Linux | `Linux: application.LinuxWindow{Icon: icon}` |
| Diálogos nativos `.ics` | `app.Dialog.OpenFile()` |
| Cerrar con cambios sin guardar | `RegisterHook(events.Common.WindowClosing, ...)` + `event.Cancel()` |

Si más adelante hace falta una ventana aparte (por ejemplo, vista de día
destacada), se crea con `app.Window.NewWithOptions` y se registra por `Name`
para no duplicarla; el patrón singleton está en la documentación de Wails
(*Multiple Windows*).
