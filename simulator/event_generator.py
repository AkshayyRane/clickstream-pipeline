"""Generates a stream of clickstream events with a realistic funnel shape.

The core idea: rather than emitting independent random events, we simulate a pool
of concurrent *sessions*, each a small state machine walking through
page_view -> click -> add_to_cart -> purchase, with a drop-off probability at each
step. That shrinking-funnel shape is what makes the downstream dbt funnel-analysis
mart meaningful -- if events were fully independent/random there'd be nothing
funnel-shaped to analyze.

New users occasionally emit a signup event before their first page_view.
"""

from __future__ import annotations

import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone

from faker import Faker

from simulator.schemas import ClickstreamEvent, EventType

fake = Faker()

# Ordered funnel steps. A session always starts at index 0 (page_view).
FUNNEL_STEPS = [
    EventType.PAGE_VIEW,
    EventType.CLICK,
    EventType.ADD_TO_CART,
    EventType.PURCHASE,
]

# Probability a session advances from `step` to the next one, rather than ending
# after emitting `step`. Tuned so the funnel narrows the way a real e-commerce
# funnel does (most visitors just look around; few buy).
CONTINUE_PROBABILITY = {
    EventType.PAGE_VIEW: 0.45,
    EventType.CLICK: 0.35,
    EventType.ADD_TO_CART: 0.30,
}

# A session may also just browse multiple pages before clicking/leaving.
EXTRA_PAGE_VIEW_PROBABILITY = 0.5

SIGNUP_PROBABILITY_FOR_NEW_USER = 0.5

CLICK_ELEMENTS = ["nav-cta", "product-card", "banner", "search-button", "footer-link"]
PAYMENT_METHODS = ["credit_card", "paypal", "apple_pay", "gift_card"]
REFERRAL_SOURCES = ["organic_search", "paid_ad", "email", "social", "direct"]

# Small shared catalog so add_to_cart and purchase events within a session refer
# to the same products, and purchase.total_amount is a real sum of cart items.
PRODUCT_CATALOG = [
    {"product_id": f"P{1000 + i}", "product_name": fake.word().title() + " " + name, "price": price}
    for i, (name, price) in enumerate(
        [
            ("Sneakers", 79.99),
            ("Backpack", 49.50),
            ("Water Bottle", 18.00),
            ("Headphones", 129.99),
            ("Notebook", 6.50),
            ("Desk Lamp", 34.25),
            ("Sunglasses", 89.00),
            ("Coffee Mug", 12.75),
        ]
    )
]


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _new_event(user_id: str, session_id: str, event_type: EventType, properties: dict) -> ClickstreamEvent:
    return ClickstreamEvent(
        event_id=str(uuid.uuid4()),
        user_id=user_id,
        session_id=session_id,
        event_type=event_type,
        event_timestamp=_now_iso(),
        event_properties=properties,
    )


@dataclass
class _Session:
    user_id: str
    session_id: str
    is_new_user: bool
    funnel_index: int = 0
    cart: list = field(default_factory=list)
    pending_signup: bool = False
    ended: bool = False


class ClickstreamSimulator:
    """Drives a fixed-size pool of concurrent sessions forward one event at a time."""

    def __init__(self, num_concurrent_users: int, existing_user_pool_size: int = 500):
        self.num_concurrent_users = num_concurrent_users
        # A pool of "returning" user ids, distinct from brand-new signups.
        self._existing_users = [str(uuid.uuid4()) for _ in range(existing_user_pool_size)]
        self._active_sessions: list[_Session] = []

    def _spawn_session(self) -> _Session:
        # ~20% of new sessions are a brand-new user; the rest are returning users.
        is_new_user = random.random() < 0.2
        user_id = str(uuid.uuid4()) if is_new_user else random.choice(self._existing_users)
        session = _Session(
            user_id=user_id,
            session_id=str(uuid.uuid4()),
            is_new_user=is_new_user,
            pending_signup=is_new_user and random.random() < SIGNUP_PROBABILITY_FOR_NEW_USER,
        )
        self._active_sessions.append(session)
        return session

    def _properties_for(self, event_type: EventType, session: _Session) -> dict:
        if event_type == EventType.PAGE_VIEW:
            return {"url": "/" + fake.uri_path(), "referrer": random.choice(REFERRAL_SOURCES)}
        if event_type == EventType.CLICK:
            return {"element_id": random.choice(CLICK_ELEMENTS)}
        if event_type == EventType.ADD_TO_CART:
            product = random.choice(PRODUCT_CATALOG)
            quantity = random.randint(1, 3)
            session.cart.append({**product, "quantity": quantity})
            return {**product, "quantity": quantity}
        if event_type == EventType.SIGNUP:
            return {"referral_source": random.choice(REFERRAL_SOURCES)}
        if event_type == EventType.PURCHASE:
            if not session.cart:
                # Defensive fallback -- shouldn't happen since PURCHASE only follows
                # ADD_TO_CART in the funnel, but keeps this function total.
                product = random.choice(PRODUCT_CATALOG)
                session.cart.append({**product, "quantity": 1})
            total = round(sum(item["price"] * item["quantity"] for item in session.cart), 2)
            return {
                "order_id": str(uuid.uuid4()),
                "total_amount": total,
                "item_count": sum(item["quantity"] for item in session.cart),
                "payment_method": random.choice(PAYMENT_METHODS),
            }
        raise ValueError(f"Unhandled event type: {event_type}")

    def _next_event_for(self, session: _Session) -> ClickstreamEvent:
        if session.pending_signup:
            session.pending_signup = False
            return _new_event(session.user_id, session.session_id, EventType.SIGNUP, self._properties_for(EventType.SIGNUP, session))

        current_step = FUNNEL_STEPS[session.funnel_index]
        event = _new_event(session.user_id, session.session_id, current_step, self._properties_for(current_step, session))

        if current_step == EventType.PAGE_VIEW and random.random() < EXTRA_PAGE_VIEW_PROBABILITY:
            pass  # stay on PAGE_VIEW step next tick -- session browses another page
        elif current_step in CONTINUE_PROBABILITY and random.random() < CONTINUE_PROBABILITY[current_step]:
            session.funnel_index += 1
        else:
            session.ended = True

        return event

    def next_event(self) -> ClickstreamEvent:
        """Advance one randomly-chosen active session and return the event it emits."""
        while len(self._active_sessions) < self.num_concurrent_users:
            self._spawn_session()

        session = random.choice(self._active_sessions)
        event = self._next_event_for(session)

        if session.ended:
            self._active_sessions.remove(session)

        return event

    def generate(self):
        """Infinite generator of events -- convenience wrapper around next_event()."""
        while True:
            yield self.next_event()
