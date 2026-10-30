# SD Calendario — Calendario distribuido y planificador de grupos

Sistema de calendario compartido orientado a la coordinación entre múltiples
usuarios, con soporte para desconexión temporal, edición concurrente, distintos
niveles de privacidad y desacuerdos en la aceptación de reuniones.

> **Estado del proyecto:** fase de diseño. Aún no hay código ni stack decidido.

## Motivación

Un calendario compartido no es solo un CRUD de eventos: introduce coordinación
distribuida real. Varios usuarios pueden modificar el mismo evento a la vez,
pueden estar desconectados y reconectar con cambios pendientes, y necesitan
reglas claras sobre qué información es visible para quién y cuándo una reunión
se considera realmente confirmada.

Este proyecto usa ese contexto para estudiar **consistencia, transacciones,
identidad y reconciliación**.

## Objetivo general

Construir un sistema que gestione:

- Eventos personales.
- Invitaciones y reuniones de grupo.
- Flujos de aceptación y rechazo.
- Consultas de disponibilidad.
- Detección de conflictos de horario.
- Controles de privacidad.
- Funcionamiento durante desconexiones temporales.
- Sincronización y reconciliación posterior.

## Alcance funcional

### Eventos personales

Alta, edición, borrado y consulta de eventos propios, con horarios, duración,
zona horaria y metadatos básicos.

### Invitaciones y reuniones de grupo

Un organizador propone una reunión a uno o varios participantes. Cada invitado
gestiona su respuesta de forma independiente.

### Aceptar / rechazar

Flujo explícito de respuesta por invitado, con estados diferenciados y
visibilidad del agregado para el organizador.

### Consulta de disponibilidad

Preguntar por huecos libres de un conjunto de usuarios sin exponer el detalle
de sus eventos (ver *Privacidad*).

### Conflictos de horario

Detección de solapamientos tanto en el calendario personal como al proponer una
reunión de grupo.

### Privacidad

Niveles de visibilidad por evento y por consulta: desde "ocupado" sin detalle
hasta información completa, según permisos.

### Desconexión y sincronización

El cliente debe poder operar sin conexión y reconciliar sus cambios al volver a
estar en línea, resolviendo conflictos de forma explícita.

### Propuesta vs. invitación aceptada vs. evento confirmado

Distinción de estados clave del dominio:

| Estado                              | Significado                                                         |
| ----------------------------------- | ------------------------------------------------------------------- |
| **Propuesta**                       | El organizador plantea fecha/hora; no compromete a nadie.           |
| **Invitación aceptada**             | Un invitado concreto ha aceptado participar.                        |
| **Evento de grupo confirmado**      | La reunión se considera cerrada según la regla acordada.            |

La regla de confirmación (unanimidad, quórum, mínimos, etc.) es una decisión de
diseño pendiente.

## Retos técnicos previstos

- **Consistencia:** ¿qué garantía se ofrece ante ediciones concurrentes?
- **Reconciliación:** estrategia de resolución de conflictos tras reconexión
  (last-write-wins, versiones vectoriales, merges por campo…).
- **Identidad:** usuarios, sesiones y autoría de cada cambio.
- **Privacidad:** separar "cuándo está ocupado" de "qué está haciendo".
- **Interoperabilidad:** representación de eventos independiente del transporte.

## Referencias de comparación

- **Google Calendar** — comparación funcional: calendarios compartidos,
  invitaciones y modelo de permisos.
- **Nextcloud Calendar / CalDAV** — comparación abierta: sincronización e
  interoperabilidad entre clientes.
- **iCalendar (RFC 5545)** — estándar de representación de eventos,
  independiente de la arquitectura distribuida.

## Stack

**Pendiente de decisión.** Candidatos según las convenciones del entorno:

- Flutter/Dart (escritorio + móvil)
- Rust (backend/servicio)
- Python (prototipo/servicio)

## Estructura del repositorio

Sin definir hasta elegir stack. Previsto:

```text
.
├── README.md
├── docs/          # diseño, decisiones y diagramas
└── (código)       # según stack elegido
```

## Próximos pasos

1. Decidir stack y arquitectura (cliente único vs. cliente + servidor).
2. Definir el modelo de datos del evento y sus estados.
3. Especificar la política de privacidad y la de confirmación de reuniones.
4. Diseñar el protocolo de sincronización y reconciliación offline.
5. Implementar el núcleo de eventos personales como primer vertical slice.
