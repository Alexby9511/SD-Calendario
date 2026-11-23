import type { CustomCalendar, GroupEvent } from "../types/protocol";

export function eventCustomCalendarId(event: GroupEvent, customCalendars: CustomCalendar[]) {
  const calendarId = event.payload.calendar_id;
  if (typeof calendarId === "string" && customCalendars.some((calendar) => calendar.id === calendarId)) {
    return calendarId;
  }
  return null;
}

export function isEventCalendarVisible(
  event: GroupEvent,
  customCalendars: CustomCalendar[],
  personalVisible: boolean,
  groupVisible: boolean
) {
  const baseVisible = event.kind === "personal" ? personalVisible : groupVisible;
  const customCalendarId = eventCustomCalendarId(event, customCalendars);
  const customVisible = customCalendarId
    ? customCalendars.find((calendar) => calendar.id === customCalendarId)?.visible ?? false
    : false;
  return baseVisible || customVisible;
}

export function isEventVisibleToUser(event: GroupEvent, currentUser: string, visibleUsers: string[]) {
  return event.organizer === currentUser || visibleUsers.includes(event.organizer);
}
