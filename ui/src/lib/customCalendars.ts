import type { CustomCalendar } from "../types/protocol";

const STORAGE_KEY = "sd-calendar-custom-calendars";

type CalendarStorage = Pick<Storage, "getItem" | "setItem" | "removeItem">;

function isCustomCalendar(value: unknown): value is CustomCalendar {
  return typeof value === "object"
    && value !== null
    && !Array.isArray(value)
    && "id" in value
    && typeof value.id === "string"
    && "name" in value
    && typeof value.name === "string"
    && "color" in value
    && typeof value.color === "string"
    && "visibility" in value
    && (value.visibility === "private" || value.visibility === "public")
    && "visible" in value
    && typeof value.visible === "boolean";
}

function parseCustomCalendars(serialized: string): CustomCalendar[] {
  let parsed: unknown;
  try {
    parsed = JSON.parse(serialized);
  } catch (error) {
    if (error instanceof SyntaxError) return [];
    throw error;
  }
  return Array.isArray(parsed) ? parsed.filter(isCustomCalendar) : [];
}

export function loadCustomCalendars(user: string, storage: CalendarStorage = localStorage) {
  const userKey = `${STORAGE_KEY}:${user}`;
  const userCalendars = storage.getItem(userKey);
  const legacyCalendars = storage.getItem(STORAGE_KEY);
  if (userCalendars !== null) {
    if (legacyCalendars !== null) storage.removeItem(STORAGE_KEY);
    return parseCustomCalendars(userCalendars);
  }
  if (legacyCalendars === null) return [];

  const calendars = parseCustomCalendars(legacyCalendars);
  storage.setItem(userKey, JSON.stringify(calendars));
  storage.removeItem(STORAGE_KEY);
  return calendars;
}

export function saveCustomCalendars(
  user: string,
  calendars: CustomCalendar[],
  storage: CalendarStorage = localStorage
) {
  storage.setItem(`${STORAGE_KEY}:${user}`, JSON.stringify(calendars));
  storage.removeItem(STORAGE_KEY);
}
