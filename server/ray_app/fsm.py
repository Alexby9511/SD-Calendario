import copy
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pkg.protocol.envelope import RESERVED_USER_IDS, SYSTEM_USER_ID, Envelope

MAX_SYNC_LIMIT = 1000
# The state machine keeps only a bounded window of applied commands for sync
# and dedupe. Snapshots no longer carry the whole history, so a long-lived
# cluster does not rewrite every command on each snapshot.
MAX_HISTORY_ENTRIES = 10_000
MAX_SEEN_PER_SENDER = 1_000
_OWNER_ONLY_EVENT_FIELDS = ("visibility", "availability", "privacy", "hide_from_freebusy")


def _without_owner_only_event_fields(value: Dict[str, Any]) -> Dict[str, Any]:
    for field in _OWNER_ONLY_EVENT_FIELDS:
        value.pop(field, None)
    payload = value.get("payload")
    if isinstance(payload, dict):
        for field in _OWNER_ONLY_EVENT_FIELDS:
            payload.pop(field, None)
    return value


def event_view_for_user(event: Dict[str, Any], user_id: str) -> Dict[str, Any]:
    view = copy.deepcopy(event)
    if event.get("organizer") != user_id:
        _without_owner_only_event_fields(view)
    return view


def event_calendar_shares(
    event: Dict[str, Any],
    calendar_shares: Dict[str, Dict[str, Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    owner_shares = calendar_shares.get(event.get("organizer"), {})
    if not isinstance(owner_shares, dict):
        return []
    payload = event.get("payload")
    calendar_id = payload.get("calendar_id") if isinstance(payload, dict) else None
    calendar_ids = [
        calendar
        for calendar in (event.get("kind"), calendar_id)
        if isinstance(calendar, str)
    ]
    return [
        owner_shares[calendar]
        for calendar in dict.fromkeys(calendar_ids)
        if isinstance(calendar, str) and calendar in owner_shares
    ]


def command_view_for_user(
    command: Dict[str, Any], event: Dict[str, Any], user_id: str
) -> Dict[str, Any]:
    view = copy.deepcopy(command)
    if event.get("organizer") != user_id:
        _without_owner_only_event_fields(view)
    return view


class CalendarFSM:
    """Deterministic, in-memory state machine. No clocks or randomness."""

    def __init__(self) -> None:
        self.accounts: Dict[str, Dict[str, str]] = {}
        self.events: Dict[str, Dict[str, Any]] = {}
        self.attendance: Dict[str, Dict[str, str]] = {}
        self.counters: Dict[str, Dict[str, Any]] = {}
        self.calendar_shares: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self.busy: Dict[str, Dict[str, Any]] = {}
        self.seen: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self.log: List[Dict[str, Any]] = []
        self.applied_index = 0

    def apply(self, data: Dict[str, Any]) -> Dict[str, Any]:
        if data.get("internal_type") == "REGISTER_ACCOUNT":
            return self._register_account(data)

        envelope = Envelope.from_dict(data)
        envelope.validate()

        msg_id = envelope.msg_id
        sender_seen = self.seen.get(envelope.sender_id, {})
        previous = sender_seen.get(msg_id)
        if previous is None:
            previous = self.seen.get("*", {}).get(msg_id)
        if previous is not None:
            previous = copy.deepcopy(previous)
            return {
                "code": 200,
                "deduplicated": True,
                "applied_index": previous["applied_index"],
                "result": previous["result"],
            }

        if envelope.type == "REQUEST":
            result = self._request(envelope)
        elif envelope.type == "REPLY":
            result = self._reply(envelope)
        elif envelope.type == "COUNTER":
            result = self._counter(envelope)
        elif envelope.type == "DECLINECOUNTER":
            result = self._decline_counter(envelope)
        elif envelope.type == "REANCHOR":
            result = self._reanchor(envelope)
        elif envelope.type == "CANCEL":
            result = self._cancel(envelope)
        elif envelope.type == "CLOSE_POLL":
            result = self._close_poll(envelope)
        elif envelope.type == "REOPEN":
            result = self._reopen(envelope)
        elif envelope.type == "BUSY_UPDATE":
            result = self._busy_update(envelope)
        elif envelope.type == "SHARE_CALENDAR":
            result = self._share_calendar(envelope)
        elif envelope.type == "EVALUATE":
            result = self._evaluate(envelope)
        else:
            result = {"code": 422, "reason": "unsupported_command"}

        code = int(result.pop("code", 200))
        if code != 200:
            return {
                "code": code,
                "deduplicated": False,
                "applied_index": self.applied_index,
                "result": result,
            }

        self.applied_index += 1
        sender_seen = self.seen.setdefault(envelope.sender_id, {})
        sender_seen[msg_id] = {
            "applied_index": self.applied_index,
            "result": copy.deepcopy(result),
        }
        if len(sender_seen) > MAX_SEEN_PER_SENDER:
            for stale in list(sender_seen)[: len(sender_seen) - MAX_SEEN_PER_SENDER]:
                del sender_seen[stale]
        self._append_history(
            {
                "index": self.applied_index,
                "msg_id": msg_id,
                "command": copy.deepcopy(data),
                "result": copy.deepcopy(result),
            }
        )
        return {
            "code": 200,
            "deduplicated": False,
            "applied_index": self.applied_index,
            "result": result,
        }

    def _append_history(self, entry: Dict[str, Any]) -> None:
        self.log.append(entry)
        overflow = len(self.log) - MAX_HISTORY_ENTRIES
        if overflow > 0:
            del self.log[:overflow]

    def _register_account(self, command: Dict[str, Any]) -> Dict[str, Any]:
        user_id = command.get("user_id")
        password_salt = command.get("password_salt")
        password_hash = command.get("password_hash")
        if (
            not isinstance(user_id, str)
            or user_id in RESERVED_USER_IDS
            or re.fullmatch(r"[a-z0-9][a-z0-9_.-]{2,31}", user_id) is None
            or not isinstance(password_salt, str)
            or re.fullmatch(r"[0-9a-f]{32}", password_salt) is None
            or not isinstance(password_hash, str)
            or re.fullmatch(r"[0-9a-f]{64}", password_hash) is None
        ):
            return {
                "code": 422,
                "deduplicated": False,
                "applied_index": self.applied_index,
                "result": {"reason": "invalid_account"},
            }
        if user_id in self.accounts:
            return {
                "code": 409,
                "deduplicated": False,
                "applied_index": self.applied_index,
                "result": {"reason": "user_id_taken"},
            }

        self.accounts[user_id] = {
            "password_salt": password_salt,
            "password_hash": password_hash,
        }
        self.applied_index += 1
        result = {"ok": True, "user_id": user_id}
        self._append_history(
            {
                "index": self.applied_index,
                "msg_id": f"register:{user_id}",
                "command": copy.deepcopy(command),
                "result": copy.deepcopy(result),
            }
        )
        return {
            "code": 200,
            "deduplicated": False,
            "applied_index": self.applied_index,
            "result": result,
        }

    def _request(self, envelope: Envelope) -> Dict[str, Any]:
        uid = envelope.uid
        existing = self.events.get(uid)
        try:
            if existing is None:
                payload = self._event_payload(envelope.payload)
            else:
                payload = self._event_payload(
                    envelope.payload,
                    default_kind=existing["kind"],
                    default_visibility=existing.get("visibility", "private"),
                    default_participants=existing["participants"],
                )
        except ValueError as exc:
            return {"code": 422, "reason": str(exc)}

        kind = payload["kind"]
        participants = payload["participants"]
        if kind == "group" and not participants:
            return {"code": 422, "reason": "group_requires_participants"}

        if existing is None:
            if envelope.sequence != 1:
                return {"code": 409, "reason": "creation_sequence_required"}
            if self._has_conflict(envelope.sender_id, payload):
                return {"code": 409, "reason": "organizer_schedule_conflict"}
            self.events[uid] = {
                "uid": uid,
                "sequence": envelope.sequence,
                "state": "CONFIRMED" if kind == "personal" else "PROPOSED",
                "organizer": envelope.sender_id,
                "kind": kind,
                "participants": participants,
                "visibility": payload["visibility"],
                "payload": payload,
            }
            self.attendance.setdefault(uid, {})
            return {"ok": True, "uid": uid, "state": self.events[uid]["state"]}

        if not self._can_edit_event(existing, envelope.sender_id):
            return {"code": 403, "reason": "organizer_only"}
        if envelope.sender_id != existing["organizer"]:
            for field in _OWNER_ONLY_EVENT_FIELDS:
                if field == "visibility":
                    payload[field] = existing.get(field, "private")
                elif field in existing["payload"]:
                    payload[field] = copy.deepcopy(existing["payload"][field])
                else:
                    payload.pop(field, None)
        if envelope.sequence <= existing["sequence"]:
            return {"code": 409, "reason": "stale_sequence"}
        if existing["state"] in ("EXPIRED", "CANCELLED"):
            code = 410 if existing["state"] == "CANCELLED" else 409
            return {"code": code, "reason": "terminal_event"}
        if envelope.sequence != existing["sequence"] + 1:
            return {"code": 409, "reason": "sequence_gap"}
        if kind != existing["kind"]:
            return {"code": 422, "reason": "event_kind_is_immutable"}
        if self._has_conflict(envelope.sender_id, payload, exclude_uid=uid):
            return {"code": 409, "reason": "organizer_schedule_conflict"}

        existing["sequence"] = envelope.sequence
        existing["payload"] = payload
        existing["visibility"] = payload["visibility"]
        existing["participants"] = participants
        self.counters.pop(uid, None)
        if existing["kind"] == "group":
            self.attendance[uid] = {}
            existing["state"] = "PROPOSED"
        else:
            existing["state"] = "CONFIRMED"
        return {"ok": True, "uid": uid, "state": existing["state"]}

    def _reply(self, envelope: Envelope) -> Dict[str, Any]:
        event = self.events.get(envelope.uid)
        if event is None:
            return {"code": 409, "reason": "unknown_event"}
        if event["state"] in ("EXPIRED", "CANCELLED"):
            return {"code": 410, "reason": "terminal_event"}
        if envelope.sender_id not in event["participants"]:
            return {"code": 403, "reason": "not_an_attendee"}

        response = str(envelope.payload.get("partstat", "")).upper()
        if response not in ("ACCEPTED", "DECLINED", "TENTATIVE"):
            return {"code": 422, "reason": "invalid_partstat"}
        if envelope.sequence != event["sequence"]:
            return {"code": 409, "reason": "stale_sequence"}
        if event["state"] != "PROPOSED":
            late = "LATE_" + response
            self.attendance.setdefault(envelope.uid, {})[envelope.sender_id] = late
            return {"ok": True, "uid": envelope.uid, "partstat": late}

        if response == "ACCEPTED" and self._has_conflict(
            envelope.sender_id, event["payload"], exclude_uid=envelope.uid
        ):
            return {"code": 409, "reason": "attendee_schedule_conflict"}

        self.attendance.setdefault(envelope.uid, {})[envelope.sender_id] = response
        if self._confirmation_reached(event):
            event["state"] = "CONFIRMED"
        elif not self._confirmation_is_possible(event):
            event["state"] = "NOT_CONFIRMED"
        return {"ok": True, "uid": envelope.uid, "partstat": response}

    def _counter(self, envelope: Envelope) -> Dict[str, Any]:
        event = self.events.get(envelope.uid)
        if event is None:
            return {"code": 409, "reason": "unknown_event"}
        if envelope.sender_id not in event["participants"]:
            return {"code": 403, "reason": "not_an_attendee"}
        sequence_error = self._next_event_sequence(event, envelope.sequence)
        if sequence_error:
            return sequence_error
        if event["state"] in ("EXPIRED", "CANCELLED"):
            return {"code": 410, "reason": "terminal_event"}
        if event["state"] != "PROPOSED":
            return {"code": 409, "reason": "poll_closed"}
        start = envelope.payload.get("proposed_start")
        end = envelope.payload.get("proposed_end")
        if not self._valid_window(start, end):
            return {"code": 422, "reason": "invalid_counter_window"}
        if self._has_conflict(envelope.sender_id, {"start": start, "end": end}, exclude_uid=envelope.uid):
            return {"code": 409, "reason": "attendee_schedule_conflict"}
        self.counters.setdefault(envelope.uid, {})[envelope.sender_id] = copy.deepcopy(
            envelope.payload
        )
        event["sequence"] = envelope.sequence
        return {"ok": True, "uid": envelope.uid, "counter": envelope.payload}

    def _decline_counter(self, envelope: Envelope) -> Dict[str, Any]:
        event = self.events.get(envelope.uid)
        if event is None:
            return {"code": 409, "reason": "unknown_event"}
        if event["organizer"] != envelope.sender_id:
            return {"code": 403, "reason": "organizer_only"}
        sequence_error = self._next_event_sequence(event, envelope.sequence)
        if sequence_error:
            return sequence_error
        counter_user = envelope.payload.get("counter_user")
        counters = self.counters.get(envelope.uid, {})
        if not isinstance(counter_user, str) or counter_user not in counters:
            return {"code": 409, "reason": "unknown_counter"}
        del counters[counter_user]
        if not counters:
            self.counters.pop(envelope.uid, None)
        event["sequence"] = envelope.sequence
        return {
            "ok": True,
            "uid": envelope.uid,
            "counter_user": counter_user,
            "state": event["state"],
        }

    def _reanchor(self, envelope: Envelope) -> Dict[str, Any]:
        event = self.events.get(envelope.uid)
        if event is None:
            return {"code": 409, "reason": "unknown_event"}
        if event["organizer"] != envelope.sender_id:
            return {"code": 403, "reason": "organizer_only"}
        sequence_error = self._next_event_sequence(event, envelope.sequence)
        if sequence_error:
            return sequence_error
        if event["state"] in ("CANCELLED", "EXPIRED"):
            return {"code": 410, "reason": "terminal_event"}
        counter_user = envelope.payload.get("counter_user")
        if not isinstance(counter_user, str):
            return {"code": 409, "reason": "unknown_counter"}
        counter = self.counters.get(envelope.uid, {}).get(counter_user)
        if counter is None:
            return {"code": 409, "reason": "unknown_counter"}
        start = counter.get("proposed_start")
        end = counter.get("proposed_end")
        if not self._valid_window(start, end):
            return {"code": 422, "reason": "invalid_counter_window"}
        if self._has_conflict(
            envelope.sender_id,
            {"start": start, "end": end},
            exclude_uid=envelope.uid,
        ):
            return {"code": 409, "reason": "organizer_schedule_conflict"}
        event["payload"]["start"] = start
        event["payload"]["end"] = end
        event["payload"]["deadline_utc"] = start
        event["sequence"] = envelope.sequence
        event["state"] = "PROPOSED"
        self.attendance[envelope.uid] = {}
        self.counters.pop(envelope.uid, None)
        return {
            "ok": True,
            "uid": envelope.uid,
            "state": "PROPOSED",
            "start": start,
            "end": end,
        }

    def _evaluate(self, envelope: Envelope) -> Dict[str, Any]:
        if envelope.sender_id != SYSTEM_USER_ID:
            return {"code": 403, "reason": "system_command_only"}
        now = self._parse_datetime(envelope.payload.get("now"))
        if now is None:
            return {"code": 422, "reason": "invalid_evaluation_time"}
        expired = []
        for uid, event in self.events.items():
            deadline = self._parse_datetime(event["payload"].get("deadline_utc"))
            if (
                event["state"] == "PROPOSED"
                and deadline is not None
                and deadline <= now
            ):
                event["state"] = "EXPIRED"
                event["sequence"] += 1
                self.counters.pop(uid, None)
                expired.append(uid)
        return {"ok": True, "type": "EVALUATE", "expired": expired}

    def has_expired_events(self, now: str) -> bool:
        evaluated_at = self._parse_datetime(now)
        if evaluated_at is None:
            return False
        return any(
            event["state"] == "PROPOSED"
            and (deadline := self._parse_datetime(event["payload"].get("deadline_utc")))
            is not None
            and deadline <= evaluated_at
            for event in self.events.values()
        )

    def _close_poll(self, envelope: Envelope) -> Dict[str, Any]:
        event = self.events.get(envelope.uid)
        if event is None:
            return {"code": 409, "reason": "unknown_event"}
        if event["organizer"] != envelope.sender_id:
            return {"code": 403, "reason": "organizer_only"}
        sequence_error = self._next_event_sequence(event, envelope.sequence)
        if sequence_error:
            return sequence_error
        if event["state"] != "PROPOSED":
            return {"code": 409, "reason": "poll_closed"}
        if (
            str(event["payload"].get("confirmation_rule", "ALL")).upper() == "ORGANIZER"
            or self._confirmation_reached(event)
        ):
            event["state"] = "CONFIRMED"
        else:
            event["state"] = "NOT_CONFIRMED"
        event["sequence"] = envelope.sequence
        return {"ok": True, "uid": envelope.uid, "state": event["state"]}

    def _reopen(self, envelope: Envelope) -> Dict[str, Any]:
        event = self.events.get(envelope.uid)
        if event is None:
            return {"code": 409, "reason": "unknown_event"}
        if event["organizer"] != envelope.sender_id:
            return {"code": 403, "reason": "organizer_only"}
        sequence_error = self._next_event_sequence(event, envelope.sequence)
        if sequence_error:
            return sequence_error
        if event["state"] in ("CANCELLED", "EXPIRED"):
            return {"code": 410, "reason": "terminal_event"}
        if event["state"] == "PROPOSED":
            return {"code": 409, "reason": "poll_already_open"}
        event["state"] = "PROPOSED"
        event["sequence"] = envelope.sequence
        if envelope.payload.get("reask", True):
            self.attendance[envelope.uid] = {}
        return {"ok": True, "uid": envelope.uid, "state": "PROPOSED"}

    def _confirmation_reached(self, event: Dict[str, Any]) -> bool:
        participants = event["participants"]
        rule = str(event["payload"].get("confirmation_rule", "ALL")).upper()
        if rule == "ORGANIZER":
            return False
        if rule == "MAJORITY":
            required = len(participants) // 2 + 1
        elif rule == "QUORUM":
            required = event["payload"].get("quorum", len(participants))
            if not isinstance(required, int) or isinstance(required, bool) or required < 1:
                required = len(participants)
        else:
            required = len(participants)
        accepted = sum(
            self.attendance.get(event["uid"], {}).get(user) == "ACCEPTED"
            for user in participants
        )
        return bool(participants) and accepted >= required

    def _confirmation_is_possible(self, event: Dict[str, Any]) -> bool:
        participants = event["participants"]
        rule = str(event["payload"].get("confirmation_rule", "ALL")).upper()
        if rule == "ORGANIZER":
            return True
        if rule == "MAJORITY":
            required = len(participants) // 2 + 1
        elif rule == "QUORUM":
            required = event["payload"].get("quorum", len(participants))
            if not isinstance(required, int) or isinstance(required, bool) or required < 1:
                required = len(participants)
        else:
            required = len(participants)
        attendance = self.attendance.get(event["uid"], {})
        accepted = sum(attendance.get(user) == "ACCEPTED" for user in participants)
        unanswered = sum(user not in attendance for user in participants)
        return accepted + unanswered >= required

    def _cancel(self, envelope: Envelope) -> Dict[str, Any]:
        event = self.events.get(envelope.uid)
        if event is None:
            return {"code": 409, "reason": "unknown_event"}
        if not self._can_edit_event(event, envelope.sender_id):
            return {"code": 403, "reason": "organizer_only"}
        sequence_error = self._next_event_sequence(event, envelope.sequence)
        if sequence_error:
            return sequence_error
        event["sequence"] = envelope.sequence
        if event["state"] == "CANCELLED":
            return {"ok": True, "uid": envelope.uid, "state": "CANCELLED", "idempotent": True}
        event["state"] = "CANCELLED"
        return {"ok": True, "uid": envelope.uid, "state": "CANCELLED"}

    @staticmethod
    def _next_event_sequence(
        event: Dict[str, Any], sequence: int
    ) -> Optional[Dict[str, Any]]:
        current = event["sequence"]
        if sequence <= current:
            return {"code": 409, "reason": "stale_sequence"}
        if sequence != current + 1:
            return {"code": 409, "reason": "sequence_gap"}
        return None

    def _busy_update(self, envelope: Envelope) -> Dict[str, Any]:
        pub_seq = envelope.payload.get("pub_seq")
        if not isinstance(pub_seq, int) or isinstance(pub_seq, bool) or pub_seq < 1:
            return {"code": 422, "reason": "invalid_pub_seq"}
        windows = envelope.payload.get("windows", [])
        if not isinstance(windows, list):
            return {"code": 422, "reason": "invalid_windows"}
        for window in windows:
            if (
                not isinstance(window, dict)
                or set(window) - {"start", "end", "availability"}
                or not isinstance(window.get("start"), str)
                or not isinstance(window.get("end"), str)
                or window.get("availability") not in ("available", "busy")
                or not self._valid_window(window["start"], window["end"])
            ):
                return {"code": 422, "reason": "invalid_window"}
        key = envelope.sender_id
        previous = self.busy.get(key)
        if previous is not None and pub_seq <= previous["pub_seq"]:
            return {"ok": True, "stale": True}
        self.busy[key] = {"pub_seq": pub_seq, "windows": copy.deepcopy(windows)}
        return {"ok": True, "pub_seq": pub_seq}

    def _share_calendar(self, envelope: Envelope) -> Dict[str, Any]:
        calendar = envelope.payload.get("calendar")
        users = envelope.payload.get("users")
        permission = envelope.payload.get("permission")
        if not isinstance(calendar, str) or not calendar.strip():
            return {"code": 422, "reason": "invalid_calendar"}
        if not isinstance(users, list) or any(
            not isinstance(user, str) or not user.strip() for user in users
        ):
            return {"code": 422, "reason": "invalid_share_users"}
        if permission not in ("freebusy", "details", "edit"):
            return {"code": 422, "reason": "invalid_share_permission"}
        recipients = list(dict.fromkeys(users))
        if envelope.sender_id in recipients:
            return {"code": 422, "reason": "cannot_share_with_self"}
        owner_shares = self.calendar_shares.setdefault(envelope.sender_id, {})
        if not recipients:
            owner_shares.pop(calendar, None)
            if not owner_shares:
                self.calendar_shares.pop(envelope.sender_id, None)
        else:
            owner_shares[calendar] = {
                "users": recipients,
                "permission": permission,
            }
        return {
            "ok": True,
            "owner": envelope.sender_id,
            "calendar": calendar,
            "users": recipients,
            "permission": permission,
        }

    def availability(
        self,
        users: Optional[List[str]] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
        viewer: Optional[str] = None,
    ) -> Dict[str, Any]:
        if (start is None) != (end is None):
            raise ValueError("availability_requires_start_and_end")
        if start is not None and end is not None and not self._valid_window(start, end):
            raise ValueError("invalid_window")
        selected = users if users else sorted(
            set(self.busy)
            | {
                user
                for event in self.events.values()
                for user in [event["organizer"], *event["participants"]]
            }
        )
        if viewer is not None:
            # Free/busy is only exposed to the user itself and to users the
            # owner shared a calendar with. Without this check any authenticated
            # user could poll anyone's availability and the ``freebusy``
            # permission would have no effect.
            selected = [user for user in selected if self._can_view_freebusy(user, viewer)]
        result: Dict[str, str] = {}
        for user in selected:
            result[user] = (
                "busy" if self._is_busy(user, start, end, viewer) else "available"
            )
        return {"users": result, "as_of": self.applied_index}

    def conflicts(
        self, user: str, start: str, end: str, exclude_uid: Optional[str] = None
    ) -> Dict[str, Any]:
        if not self._valid_window(start, end):
            raise ValueError("invalid_window")
        matches = []
        for uid, event in self.events.items():
            if (
                uid == exclude_uid
                or not self._event_involves(event, user)
                or not self._event_is_busy_for(event, user)
            ):
                continue
            payload = event["payload"]
            if self._overlaps(start, end, payload.get("start"), payload.get("end")):
                matches.append(
                    {
                        "uid": uid,
                        "state": event["state"],
                        "availability": "busy",
                    }
                )
        for window in self.busy.get(user, {}).get("windows", []):
            if (
                window.get("availability") == "busy"
                and self._overlaps(start, end, window.get("start"), window.get("end"))
            ):
                matches.append(
                    {
                        "start": window["start"],
                        "end": window["end"],
                        "availability": "busy",
                    }
                )
        return {"user": user, "conflicts": matches, "as_of": self.applied_index}

    def sync(self, since: int = 0, limit: int = 100) -> Dict[str, Any]:
        if (
            not isinstance(limit, int)
            or isinstance(limit, bool)
            or limit <= 0
            or limit > MAX_SYNC_LIMIT
        ):
            raise ValueError("invalid_sync_limit")
        if (
            not isinstance(since, int)
            or isinstance(since, bool)
            or since < 0
        ):
            raise ValueError("invalid_sync_cursor")
        scanned = [item for item in self.log if item["index"] > since][:limit]
        entries = [
            item
            for item in scanned
            if item["command"].get("type") != "BUSY_UPDATE"
        ]
        next_cursor = scanned[-1]["index"] if scanned else since
        return {"entries": copy.deepcopy(entries), "next": next_cursor, "as_of": self.applied_index}

    def sync_for(
        self,
        user_id: str,
        since: int = 0,
        limit: int = 100,
        include_snapshot: bool = True,
    ) -> Dict[str, Any]:
        page = self.sync(since, limit)
        if self.log and since < self.log[0]["index"] - 1:
            # The cursor points before the retained history (a trimmed window
            # or a snapshot restore). Incremental entries alone cannot rebuild
            # the client state, so force the snapshot.
            include_snapshot = True
        visible_entries = []
        for item in page["entries"]:
            if not self._can_view_command(item["command"], user_id):
                continue
            uid = item["command"]["uid"]
            event = self.events[uid]
            entry = copy.deepcopy(item)
            entry["command"] = command_view_for_user(item["command"], event, user_id)
            visible_entries.append(
                {
                    **entry,
                    "event": event_view_for_user(event, user_id),
                    "attendance": copy.deepcopy(self.attendance.get(uid, {})),
                    "counters": copy.deepcopy(self.counters.get(uid, {})),
                }
            )
        page["entries"] = visible_entries
        if not include_snapshot:
            return page
        visible_uids = [
            uid
            for uid, event in self.events.items()
            if self._can_view_event(event, user_id)
        ]
        page["snapshot"] = {
            "events": {
                uid: event_view_for_user(self.events[uid], user_id)
                for uid in visible_uids
            },
            "attendance": {
                uid: copy.deepcopy(self.attendance.get(uid, {}))
                for uid in visible_uids
            },
            "calendar_shares": self._shares_for(user_id),
            "counters": {
                uid: copy.deepcopy(self.counters.get(uid, {}))
                for uid in visible_uids
            },
            "applied_index": self.applied_index,
        }
        return page

    @staticmethod
    def _participants(value: Any) -> List[str]:
        if not isinstance(value, list):
            raise ValueError("participants_must_be_a_list")
        participants = []
        for item in value:
            user = item if isinstance(item, str) else item.get("user_id") if isinstance(item, dict) else None
            if not isinstance(user, str) or not user.strip():
                raise ValueError("invalid_participant")
            if user not in participants:
                participants.append(user)
        return participants

    def _has_conflict(
        self, user: str, payload: Dict[str, Any], exclude_uid: Optional[str] = None
    ) -> bool:
        start, end = payload.get("start"), payload.get("end")
        if not isinstance(start, str) or not isinstance(end, str):
            return False
        return bool(self.conflicts(user, start, end, exclude_uid)["conflicts"])

    def _is_busy(
        self,
        user: str,
        start: Optional[str],
        end: Optional[str],
        viewer: Optional[str] = None,
    ) -> bool:
        if start is None and end is None:
            return any(
                window.get("availability") == "busy"
                for window in self.busy.get(user, {}).get("windows", [])
            ) or any(
                self._event_is_busy_for(event, user, viewer)
                and self._valid_window(
                    event["payload"].get("start"), event["payload"].get("end")
                )
                for event in self.events.values()
            )
        for event in self.events.values():
            if (
                not self._event_is_busy_for(event, user, viewer)
            ):
                continue
            payload = event["payload"]
            if self._overlaps(start, end, payload.get("start"), payload.get("end")):
                return True
        return any(
            window.get("availability") == "busy"
            and self._overlaps(start, end, window.get("start"), window.get("end"))
            for window in self.busy.get(user, {}).get("windows", [])
        )

    @classmethod
    def _event_payload(
        cls,
        value: Dict[str, Any],
        default_kind: Optional[str] = None,
        default_visibility: str = "private",
        default_participants: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        if not isinstance(value, dict):
            raise ValueError("event_payload_must_be_an_object")
        payload = copy.deepcopy(value)
        description = payload.get("description", "")
        if not isinstance(description, str):
            raise ValueError("invalid_event_description")
        payload["description"] = description
        description = payload.get("description", "")
        if not isinstance(description, str):
            raise ValueError("invalid_event_description")
        payload["description"] = description
        kind = str(
            payload.get(
                "kind",
                default_kind
                or ("group" if payload.get("participants") else "personal"),
            )
        ).lower()
        if kind not in ("personal", "group"):
            raise ValueError("invalid_event_kind")
        visibility = payload.get("visibility", default_visibility)
        if visibility not in ("private", "public"):
            raise ValueError("invalid_event_visibility")
        participants = cls._participants(
            payload.get("participants", default_participants or [])
        )
        if "start" in payload or "end" in payload:
            if not isinstance(payload.get("start"), str) or not isinstance(
                payload.get("end"), str
            ):
                raise ValueError("event_requires_start_and_end")
            if not cls._valid_window(payload["start"], payload["end"]):
                raise ValueError("invalid_window")
        if kind == "group" and "start" in payload:
            payload.setdefault("deadline_utc", payload["start"])
        if "deadline_utc" in payload and cls._parse_datetime(
            payload["deadline_utc"]
        ) is None:
            raise ValueError("invalid_deadline")
        payload["kind"] = kind
        payload["participants"] = participants
        payload["visibility"] = visibility
        confirmation_rule = str(payload.get("confirmation_rule", "ALL")).upper()
        if confirmation_rule not in ("ALL", "MAJORITY", "QUORUM", "ORGANIZER"):
            raise ValueError("invalid_confirmation_rule")
        payload["confirmation_rule"] = confirmation_rule
        if confirmation_rule == "QUORUM":
            quorum = payload.get("quorum")
            if (
                not isinstance(quorum, int)
                or isinstance(quorum, bool)
                or quorum < 1
                or quorum > len(participants)
            ):
                raise ValueError("invalid_quorum")
        return payload

    @staticmethod
    def _parse_datetime(value: Any) -> Optional[datetime]:
        if not isinstance(value, str):
            return None
        normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError:
            return None
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            return None
        return parsed.astimezone(timezone.utc)

    @classmethod
    def _valid_window(cls, start: Any, end: Any) -> bool:
        parsed_start = cls._parse_datetime(start)
        parsed_end = cls._parse_datetime(end)
        return (
            parsed_start is not None
            and parsed_end is not None
            and parsed_start < parsed_end
        )

    @staticmethod
    def _overlaps(start: Any, end: Any, other_start: Any, other_end: Any) -> bool:
        parsed = [
            CalendarFSM._parse_datetime(value)
            for value in (start, end, other_start, other_end)
        ]
        if any(value is None for value in parsed):
            return False
        start_time, end_time, other_start_time, other_end_time = parsed
        return start_time < other_end_time and other_start_time < end_time

    @staticmethod
    def _event_involves(event: Dict[str, Any], user: str) -> bool:
        return event["organizer"] == user or user in event.get("participants", [])

    def _event_is_busy_for(
        self, event: Dict[str, Any], user: str, viewer: Optional[str] = None
    ) -> bool:
        if event["state"] in ("CANCELLED", "EXPIRED", "NOT_CONFIRMED"):
            return False
        if event["kind"] == "personal":
            if event["payload"].get("availability", "busy") != "busy":
                return False
            if user != event["organizer"]:
                # A personal event only occupies its organizer calendar; it
                # never makes another user busy.
                return False
            if (
                event["payload"].get("hide_from_freebusy", False)
                and viewer is not None
                and viewer != event["organizer"]
            ):
                return False
            return True
        if user == event["organizer"]:
            return True
        if user not in event.get("participants", []):
            return False
        if event["state"] == "CONFIRMED":
            return True
        return self.attendance.get(event["uid"], {}).get(user) in (
            "ACCEPTED",
            "TENTATIVE",
        )

    def _can_view_freebusy(self, owner: str, viewer: str) -> bool:
        if owner == viewer:
            return True
        return any(
            viewer in share.get("users", [])
            and share.get("permission") in ("freebusy", "details", "edit")
            for share in self.calendar_shares.get(owner, {}).values()
        )

    def _can_view_command(self, command: Dict[str, Any], user_id: str) -> bool:
        event = self.events.get(command.get("uid"))
        if event is None:
            return False
        return self._can_view_event(event, user_id)

    def _can_view_event(self, event: Dict[str, Any], user_id: str) -> bool:
        if event.get("visibility", "private") == "public" or self._event_involves(
            event, user_id
        ):
            return True
        return bool(
            any(
                user_id in share.get("users", [])
                and share.get("permission") in ("details", "edit")
                for share in event_calendar_shares(event, self.calendar_shares)
            )
        )

    def _can_edit_event(self, event: Dict[str, Any], user_id: str) -> bool:
        if event["organizer"] == user_id:
            return True
        return bool(
            any(
                user_id in share.get("users", [])
                and share.get("permission") == "edit"
                for share in event_calendar_shares(event, self.calendar_shares)
            )
        )

    def _shares_for(self, user_id: str) -> List[Dict[str, Any]]:
        return [
            {
                "owner": owner,
                "calendar": calendar,
                "users": copy.deepcopy(
                    share["users"] if owner == user_id else [user_id]
                ),
                "permission": share["permission"],
            }
            for owner, calendars in self.calendar_shares.items()
            for calendar, share in calendars.items()
            if owner == user_id or user_id in share["users"]
        ]

    def snapshot(self, include_log: bool = False) -> Dict[str, Any]:
        """Serialize the state machine for replication.

        The sync history is intentionally excluded: it is bounded and
        disposable, and including it made every snapshot O(total commands).
        ``from_snapshot`` still accepts older snapshots that carry ``log``.
        """
        snapshot = {
            "accounts": copy.deepcopy(self.accounts),
            "events": copy.deepcopy(self.events),
            "attendance": copy.deepcopy(self.attendance),
            "counters": copy.deepcopy(self.counters),
            "calendar_shares": copy.deepcopy(self.calendar_shares),
            "busy": copy.deepcopy(self.busy),
            "seen": copy.deepcopy(self.seen),
            "applied_index": self.applied_index,
        }
        if include_log:
            snapshot["log"] = copy.deepcopy(self.log)
        return snapshot

    def read_snapshot(self) -> Dict[str, Any]:
        """Return only the domain projection needed by calendar reads."""
        return {
            "events": copy.deepcopy(self.events),
            "attendance": copy.deepcopy(self.attendance),
            "counters": copy.deepcopy(self.counters),
            "calendar_shares": copy.deepcopy(self.calendar_shares),
            "busy": copy.deepcopy(self.busy),
            "applied_index": self.applied_index,
        }

    def accounts_snapshot(self) -> Dict[str, Dict[str, str]]:
        return copy.deepcopy(self.accounts)

    @classmethod
    def from_snapshot(cls, snapshot: Dict[str, Any]) -> "CalendarFSM":
        fsm = cls()
        fsm.accounts = copy.deepcopy(snapshot.get("accounts", {}))
        fsm.events = copy.deepcopy(snapshot.get("events", {}))
        fsm.attendance = copy.deepcopy(snapshot.get("attendance", {}))
        fsm.counters = copy.deepcopy(snapshot.get("counters", {}))
        fsm.calendar_shares = copy.deepcopy(snapshot.get("calendar_shares", {}))
        fsm.busy = copy.deepcopy(snapshot.get("busy", {}))
        stored_seen = copy.deepcopy(snapshot.get("seen", {}))
        if stored_seen and all(
            isinstance(value, dict) and "applied_index" in value
            for value in stored_seen.values()
        ):
            fsm.seen = {"*": stored_seen}
        else:
            fsm.seen = stored_seen
        fsm.log = copy.deepcopy(snapshot.get("log", []))
        fsm.applied_index = int(snapshot.get("applied_index", 0))
        return fsm
