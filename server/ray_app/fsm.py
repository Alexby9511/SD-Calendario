import copy
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
        elif envelope.type in ("COUNTER", "DECLINECOUNTER"):
            result = self._counter(envelope)
        elif envelope.type == "CANCEL":
            result = self._cancel(envelope)
        elif envelope.type == "BUSY_UPDATE":
            result = self._busy_update(envelope)
        elif envelope.type == "EVALUATE":
            result = {"ok": True, "type": "EVALUATE"}
        else:
            result = {"ok": True, "type": envelope.type}

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
        if existing is None:
            if envelope.sequence != 1:
                return {"code": 409, "reason": "creation_sequence_required"}
            payload = copy.deepcopy(envelope.payload)
            kind = str(payload.get("kind", "group" if payload.get("participants") else "personal")).lower()
            if kind not in ("personal", "group"):
                return {"code": 422, "reason": "invalid_event_kind"}
            participants = self._participants(payload.get("participants", []))
            if kind == "group" and not participants:
                return {"code": 422, "reason": "group_requires_participants"}
            if self._has_conflict(envelope.sender_id, payload):
                return {"code": 409, "reason": "organizer_schedule_conflict"}
            self.events[uid] = {
                "uid": uid,
                "sequence": envelope.sequence,
                "state": "CONFIRMED" if kind == "personal" else "PROPOSED",
                "organizer": envelope.sender_id,
                "kind": kind,
                "participants": participants,
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
        if self._has_conflict(envelope.sender_id, envelope.payload, exclude_uid=uid):
            return {"code": 409, "reason": "organizer_schedule_conflict"}

        existing["sequence"] = envelope.sequence
        existing["payload"] = copy.deepcopy(envelope.payload)
        if existing["state"] in ("CONFIRMED", "NOT_CONFIRMED"):
            existing["state"] = "PROPOSED"
        return {"ok": True, "uid": uid, "state": existing["state"]}

    def _reply(self, envelope: Envelope) -> Dict[str, Any]:
        event = self.events.get(envelope.uid)
        if event is None:
            return {"code": 409, "reason": "unknown_event"}
        if event["state"] in ("EXPIRED", "CANCELLED"):
            return {"code": 410, "reason": "terminal_event"}
        if envelope.sender_id != event["organizer"] and envelope.sender_id not in event["participants"]:
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
        if not isinstance(pub_seq, int):
            return {"code": 422, "reason": "invalid_pub_seq"}
        windows = envelope.payload.get("windows", [])
        if not isinstance(windows, list):
            return {"code": 422, "reason": "invalid_windows"}
        for window in windows:
            if (
                not isinstance(window, dict)
                or set(window) - {"start", "end", "visibility"}
                or not isinstance(window.get("start"), str)
                or not isinstance(window.get("end"), str)
                or window["start"] >= window["end"]
            ):
                return {"code": 422, "reason": "invalid_window"}
        key = envelope.sender_id
        previous = self.busy.get(key)
        if previous is not None and pub_seq < previous["pub_seq"]:
            return {"ok": True, "stale": True}
        self.busy[key] = {"pub_seq": pub_seq, "windows": copy.deepcopy(windows)}
        return {"ok": True, "pub_seq": pub_seq}

    def availability(
        self, users: Optional[List[str]] = None, start: Optional[str] = None, end: Optional[str] = None
    ) -> Dict[str, Any]:
        selected = users if users else sorted(self.busy)
        result: Dict[str, List[Dict[str, str]]] = {}
        for user in selected:
            windows = []
            for window in self.busy.get(user, {}).get("windows", []):
                if start and window["end"] <= start:
                    continue
                if end and window["start"] >= end:
                    continue
                windows.append(
                    {
                        "start": window["start"],
                        "end": window["end"],
                        "visibility": "busy",
                    }
                )
            result[user] = windows
        return {"users": result, "as_of": self.applied_index}

    def conflicts(
        self, user: str, start: str, end: str, exclude_uid: Optional[str] = None
    ) -> Dict[str, Any]:
        matches = []
        for uid, event in self.events.items():
            if uid == exclude_uid or not self._event_involves(event, user):
                continue
            payload = event["payload"]
            if self._overlaps(start, end, payload.get("start"), payload.get("end")):
                matches.append({"uid": uid, "state": event["state"], "visibility": "busy"})
        return {"user": user, "conflicts": matches, "as_of": self.applied_index}

    def sync(self, since: int = 0, limit: int = 100) -> Dict[str, Any]:
        if since < 0 or limit <= 0:
            raise ValueError("invalid_sync_cursor")
        entries = [item for item in self.log if item["index"] > since][:limit]
        next_cursor = entries[-1]["index"] if entries else since
        return {"entries": copy.deepcopy(entries), "next": next_cursor, "as_of": self.applied_index}

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
        if not isinstance(start, str) or not isinstance(end, str) or start >= end:
            return False
        return bool(self.conflicts(user, start, end, exclude_uid)["conflicts"])

    @staticmethod
    def _overlaps(start: Any, end: Any, other_start: Any, other_end: Any) -> bool:
        return (
            isinstance(start, str)
            and isinstance(end, str)
            and isinstance(other_start, str)
            and isinstance(other_end, str)
            and start < other_end
            and other_start < end
        )

    @staticmethod
    def _event_involves(event: Dict[str, Any], user: str) -> bool:
        return event["organizer"] == user or user in event.get("participants", [])

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
