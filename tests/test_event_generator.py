"""Sanity checks for the event generator's funnel state machine.

These aren't exhaustive -- the goal is to catch an obviously-broken generator
(bad event shape, funnel steps out of order, purchase totals that don't match
the cart) before it silently produces garbage into Redpanda.
"""

from __future__ import annotations

from collections import defaultdict

from simulator.event_generator import FUNNEL_STEPS, ClickstreamSimulator
from simulator.schemas import EventType

REQUIRED_KEYS = {"event_id", "user_id", "session_id", "event_type", "event_timestamp", "event_properties"}


def _generate(n: int, num_concurrent_users: int = 20):
    simulator = ClickstreamSimulator(num_concurrent_users=num_concurrent_users)
    return [simulator.next_event() for _ in range(n)]


def test_event_has_required_fields_and_valid_type():
    for event in _generate(500):
        payload = event.__dict__
        assert REQUIRED_KEYS.issubset(payload.keys())
        assert isinstance(event.event_type, EventType)
        assert isinstance(event.event_properties, dict)


def test_timestamps_non_decreasing_within_a_session():
    by_session = defaultdict(list)
    for event in _generate(2000):
        by_session[event.session_id].append(event.event_timestamp)

    for session_id, timestamps in by_session.items():
        assert timestamps == sorted(timestamps), f"timestamps out of order for session {session_id}"


def test_funnel_steps_progress_in_order_within_a_session():
    by_session = defaultdict(list)
    for event in _generate(3000):
        by_session[event.session_id].append(event.event_type)

    funnel_index = {step: i for i, step in enumerate(FUNNEL_STEPS)}

    for session_id, event_types in by_session.items():
        funnel_only = [t for t in event_types if t != EventType.SIGNUP]
        indices = [funnel_index[t] for t in funnel_only]
        assert indices == sorted(indices), f"funnel steps out of order for session {session_id}: {funnel_only}"

        if EventType.SIGNUP in event_types:
            assert event_types[0] == EventType.SIGNUP, f"signup should be the first event for session {session_id}"


def test_purchase_total_matches_cart_contents():
    by_session_events = defaultdict(list)
    for event in _generate(4000, num_concurrent_users=30):
        by_session_events[event.session_id].append(event)

    checked_at_least_one_purchase = False
    for events in by_session_events.values():
        purchase = next((e for e in events if e.event_type == EventType.PURCHASE), None)
        if purchase is None:
            continue
        checked_at_least_one_purchase = True

        cart_items = [e.event_properties for e in events if e.event_type == EventType.ADD_TO_CART]
        expected_total = round(sum(item["price"] * item["quantity"] for item in cart_items), 2)
        expected_item_count = sum(item["quantity"] for item in cart_items)

        assert purchase.event_properties["total_amount"] == expected_total
        assert purchase.event_properties["item_count"] == expected_item_count

    assert checked_at_least_one_purchase, "generated sample had no purchase events -- funnel probabilities may need tuning"
