import copy
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pkg.protocol.envelope import Envelope


class CalendarFSM:
    """Deterministic, in-memory state machine. No clocks or randomness."""

    def __init__(self) -> None:
        self.events: Dict[str, Dict[str, Any]] = {}
        self.attendance: Dict[str, Dict[str, str]] = {}
        self.counters: Dict[str, Dict[str, Any]] = {}
        self.busy: Dict[str, Dict[str, Any]] = {}
        self.seen: Dict[str, Dict[str, Any]] = {}
        self.log: List[Dict[str, Any]] = []
        self.applied_index = 0

    def apply(self, data: Dict[str, Any]) -> Dict[str, Any]:
        envelope = Envelope.from_dict(data)
        envelope.validate()

        msg_id = envelope.msg_id
        if msg_id in self.seen:
            previous = copy.deepcopy(self.seen[msg_id])
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
            result = {"code": 422, "reason": "unsupported_command"}
        elif envelope.type == "CANCEL":
            result = self._cancel(envelope)
        elif envelope.type == "BUSY_UPDATE":
            result = self._busy_update(envelope)
        elif envelope.type == "EVALUATE":
            result = {"ok": True, "type": "EVALUATE"}
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
        self.seen[msg_id] = {
            "applied_index": self.applied_index,
            "result": copy.deepcopy(result),
        }
        self.log.append(
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

        if envelope.sequence <= existing["sequence"]:
            return {"ok": True, "uid": uid, "state": existing["state"], "stale": True}
        if existing["organizer"] != envelope.sender_id:
            return {"code": 403, "reason": "organizer_only"}
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
        if response == "DECLINED":
            event["state"] = "NOT_CONFIRMED"
        elif event["participants"] and all(
            self.attendance[envelope.uid].get(user) == "ACCEPTED"
            for user in event["participants"]
        ):
            event["state"] = "CONFIRMED"
        return {"ok": True, "uid": envelope.uid, "partstat": response}

    def _counter(self, envelope: Envelope) -> Dict[str, Any]:
        event = self.events.get(envelope.uid)
        if event is None:
            return {"code": 409, "reason": "unknown_event"}
        if event["state"] in ("EXPIRED", "CANCELLED"):
            return {"code": 410, "reason": "terminal_event"}
        if event["state"] != "PROPOSED":
            return {"code": 409, "reason": "poll_closed"}
        if envelope.sender_id not in event["participants"]:
            return {"code": 403, "reason": "not_an_attendee"}
        self.counters.setdefault(envelope.uid, {})[envelope.sender_id] = copy.deepcopy(
            envelope.payload
        )
        return {"ok": True, "uid": envelope.uid, "counter": envelope.payload}

    def _cancel(self, envelope: Envelope) -> Dict[str, Any]:
        event = self.events.get(envelope.uid)
        if event is None:
            return {"code": 409, "reason": "unknown_event"}
        if event["organizer"] != envelope.sender_id:
            return {"code": 403, "reason": "organizer_only"}
        if event["state"] == "CANCELLED":
            return {"ok": True, "uid": envelope.uid, "state": "CANCELLED", "idempotent": True}
        event["state"] = "CANCELLED"
        return {"ok": True, "uid": envelope.uid, "state": "CANCELLED"}

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

    def availability(
        self, users: Optional[List[str]] = None, start: Optional[str] = None, end: Optional[str] = None
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
        result: Dict[str, str] = {}
        for user in selected:
            result[user] = (
                "busy" if self._is_busy(user, start, end) else "available"
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
        if since < 0 or limit <= 0:
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
        self, user_id: str, since: int = 0, limit: int = 100
    ) -> Dict[str, Any]:
        page = self.sync(since, limit)
        visible_entries = []
        for item in page["entries"]:
            if not self._can_view_command(item["command"], user_id):
                continue
            uid = item["command"]["uid"]
            visible_entries.append(
                {
                    **item,
                    "event": copy.deepcopy(self.events[uid]),
                    "attendance": copy.deepcopy(self.attendance.get(uid, {})),
                }
            )
        page["entries"] = visible_entries
        visible_uids = [
            uid
            for uid, event in self.events.items()
            if self._can_view_event(event, user_id)
        ]
        page["snapshot"] = {
            "events": {
                uid: copy.deepcopy(self.events[uid]) for uid in visible_uids
            },
            "attendance": {
                uid: copy.deepcopy(self.attendance.get(uid, {}))
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
        self, user: str, start: Optional[str], end: Optional[str]
    ) -> bool:
        if start is None and end is None:
            return any(
                window.get("availability") == "busy"
                for window in self.busy.get(user, {}).get("windows", [])
            ) or any(
                self._event_is_busy_for(event, user)
                and self._valid_window(
                    event["payload"].get("start"), event["payload"].get("end")
                )
                for event in self.events.values()
            )
        for event in self.events.values():
            if (
                not self._event_is_busy_for(event, user)
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
        payload["kind"] = kind
        payload["participants"] = participants
        payload["visibility"] = visibility
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

    def _event_is_busy_for(self, event: Dict[str, Any], user: str) -> bool:
        if event["state"] in ("CANCELLED", "EXPIRED", "NOT_CONFIRMED"):
            return False
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

    def _can_view_command(self, command: Dict[str, Any], user_id: str) -> bool:
        event = self.events.get(command.get("uid"))
        if event is None:
            return False
        return self._can_view_event(event, user_id)

    @classmethod
    def _can_view_event(cls, event: Dict[str, Any], user_id: str) -> bool:
        return event.get("visibility", "private") == "public" or cls._event_involves(
            event, user_id
        )

    def snapshot(self) -> Dict[str, Any]:
        return {
            "events": copy.deepcopy(self.events),
            "attendance": copy.deepcopy(self.attendance),
            "counters": copy.deepcopy(self.counters),
            "busy": copy.deepcopy(self.busy),
            "seen": copy.deepcopy(self.seen),
            "log": copy.deepcopy(self.log),
            "applied_index": self.applied_index,
        }

    @classmethod
    def from_snapshot(cls, snapshot: Dict[str, Any]) -> "CalendarFSM":
        fsm = cls()
        fsm.events = copy.deepcopy(snapshot.get("events", {}))
        fsm.attendance = copy.deepcopy(snapshot.get("attendance", {}))
        fsm.counters = copy.deepcopy(snapshot.get("counters", {}))
        fsm.busy = copy.deepcopy(snapshot.get("busy", {}))
        fsm.seen = copy.deepcopy(snapshot.get("seen", {}))
        fsm.log = copy.deepcopy(snapshot.get("log", []))
        fsm.applied_index = int(snapshot.get("applied_index", 0))
        return fsm
