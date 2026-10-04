"""A single versioned record per run; locks cover individual operations only."""
import asyncio
from copy import deepcopy


class VersionedState:
    def __init__(self):
        self._lock = asyncio.Lock()
        self._history = {}

    async def initialize(self, key, value, writer_event_id):
        async with self._lock:
            if self._history:
                raise ValueError("Stage 4 uses one initialized shared record per run")
            record = {"key": key, "version": 0, "value": deepcopy(value), "writer_event_id": writer_event_id}
            self._history[key] = [record]
            return deepcopy(record)

    async def read(self, key, version=None):
        async with self._lock:
            history = self._history[key]
            current = history[-1]
            if version is not None and (not isinstance(version, int) or version < 0 or version > current["version"]):
                raise ValueError("requested state version does not exist")
            selected = current if version is None else history[version]
            return {"record": deepcopy(selected), "current_record": deepcopy(current)}

    async def write(self, key, value, base_version, policy, writer_event_id):
        if policy not in {"unconditional", "compare_and_set"}:
            raise ValueError("unknown state write policy")
        async with self._lock:
            history = self._history[key]
            previous = history[-1]
            accepted = policy == "unconditional" or base_version == previous["version"]
            result = {"key": key, "policy": policy, "base_version": base_version,
                      "expected_version": base_version, "previous_current_version": previous["version"],
                      "previous_value": deepcopy(previous["value"]), "previous_writer_event_id": previous["writer_event_id"],
                      "accepted": accepted, "rejection_reason": None if accepted else "version_conflict",
                      "new_version": previous["version"] + 1 if accepted else None,
                      "new_value": deepcopy(value) if accepted else None,
                      "writer_event_id": writer_event_id if accepted else None}
            if accepted:
                history.append({"key": key, "version": result["new_version"],
                                "value": deepcopy(value), "writer_event_id": writer_event_id})
            return result

    def history(self, key):
        # Called by the controller only after all agent tasks have settled.
        return deepcopy(self._history.get(key, []))
