"""A reconnect that replays the same event is not progress.

Companion to `test_reconnect_no_progress.py`. That file covers a stream that drops having
delivered *nothing*; this one covers a peer that keeps accepting the connection, replays the
same event on every open, and drops immediately. Deduplication is what makes the two cases
the same case: with a stable `event_id` the replay is filtered, `delivered` stays at one, and
the reconnect budget is spent. Without an id every replay counts as progress and the loop
never ends (measured against 0.4.11: 5,000 opens before the fake's own cap).

"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest

from langgraph_sdk.stream.controller import StreamController
from langgraph_sdk.stream.transport.base import EventStreamHandle

MAX_RECONNECT_ATTEMPTS = 3
MAX_OPENS = 50


class ReplaySameEventTransport:
    """Every open yields one event with the *same* id, then drops with a read timeout."""

    thread_id = "t-1"

    def __init__(self, *, event_id: str | None, opens_cap: int = MAX_OPENS) -> None:
        self.opens = 0
        self.event_id = event_id
        self.opens_cap = opens_cap

    async def send_command(self, command: dict[str, Any]) -> dict[str, Any] | None:  # noqa: ARG002
        return None

    async def close(self) -> None:
        return None

    def open_event_stream(self, params: dict[str, Any]) -> EventStreamHandle:  # noqa: ARG002
        self.opens += 1
        if self.opens > self.opens_cap:
            raise RuntimeError(f"opened more than {self.opens_cap} streams")
        loop = asyncio.get_running_loop()
        ready: asyncio.Future[None] = loop.create_future()
        ready.set_result(None)
        done: asyncio.Future[BaseException | None] = loop.create_future()
        done.set_result(httpx.ReadTimeout("read operation timed out"))
        event: dict[str, Any] = {
            "method": "values",
            "params": {"namespace": [], "data": {"seq": 1}},
        }
        if self.event_id is not None:
            event["event_id"] = self.event_id

        async def aiter() -> AsyncIterator[Any]:
            yield event

        async def close() -> None:
            return None

        return EventStreamHandle(events=aiter(), ready=ready, done=done, close=close)


async def _run(transport: ReplaySameEventTransport, timeout: float = 5.0) -> list[Any]:
    controller = StreamController(
        transport=transport,
        max_reconnect_attempts=MAX_RECONNECT_ATTEMPTS,
        reconnect_backoff_base=0.001,
        reconnect_backoff_cap=0.002,
    )
    seen: list[Any] = []

    async def consume() -> None:
        async for event in controller.subscribe(["values"]):
            seen.append(event)

    try:
        await asyncio.wait_for(consume(), timeout=timeout)
    finally:
        await controller.close()
    return seen


@pytest.mark.anyio
async def test_replayed_events_do_not_refill_the_reconnect_budget() -> None:
    """Same id every open: dedup filters the replays, so the budget is spent and the loop ends."""

    transport = ReplaySameEventTransport(event_id="same-event")

    seen = await _run(transport)

    assert len(seen) == 1, f"delivered {len(seen)} events; dedup should have filtered the replays"
    assert transport.opens <= 1 + MAX_RECONNECT_ATTEMPTS + 1, (
        f"reconnected {transport.opens} times after filtering every replay"
    )
