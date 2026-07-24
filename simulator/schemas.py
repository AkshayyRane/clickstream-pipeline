"""The event contract shared by the producer (simulator) and the consumer.

Plain dataclasses + a str enum, not a schema library -- deliberately minimal for
Phase 1. A real schema registry (Avro/Protobuf via the Redpanda schema registry
we already run in docker-compose) is the natural "what I'd do in production"
upgrade to mention, since it gives producers/consumers compile-time-checked,
evolvable schemas instead of "hope the JSON shape doesn't drift."
"""

from __future__ import annotations

import dataclasses
import enum
import json


class EventType(str, enum.Enum):
    PAGE_VIEW = "page_view"
    CLICK = "click"
    ADD_TO_CART = "add_to_cart"
    SIGNUP = "signup"
    PURCHASE = "purchase"


@dataclasses.dataclass
class ClickstreamEvent:
    event_id: str
    user_id: str
    session_id: str
    event_type: EventType
    event_timestamp: str  # ISO 8601, UTC, e.g. "2026-07-23T14:03:21.512Z"
    event_properties: dict

    def to_json(self) -> str:
        payload = dataclasses.asdict(self)
        payload["event_type"] = self.event_type.value
        return json.dumps(payload)
