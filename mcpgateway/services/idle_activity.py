# -*- coding: utf-8 -*-
"""Location: ./mcpgateway/services/idle_activity.py
Copyright contributors to the MCP-CONTEXT-FORGE project
SPDX-License-Identifier: Apache-2.0

Process-local activity gate for optional serverless maintenance.

Enabled gates start idle. Successful requests wake maintenance without polling.
Pending requests preserve an existing active window until their ASGI calls finish.
A pending request cannot wake an idle gate before its response succeeds.
"""

# Standard
import asyncio
from collections.abc import Callable, Iterator
from contextlib import contextmanager
import math
import os
import time


class IdleActivityGate:
    """Suspend optional maintenance between successful requests on one event loop.

    The gate never suspends or cancels request processing or maintenance already running.
    Callers must check the gate before each optional maintenance operation.
    """

    def __init__(self, enabled: bool = False, idle_timeout_seconds: float = 60.0, clock: Callable[[], float] = time.monotonic) -> None:
        """Initialize an idle gate without starting tasks or reading the clock.

        Args:
            enabled: Whether optional maintenance can become idle.
            idle_timeout_seconds: Active window after the last successful request.
            clock: Monotonic time source, injectable for deterministic tests.

        Raises:
            ValueError: If the idle timeout is not positive and finite.
        """
        if not math.isfinite(idle_timeout_seconds) or idle_timeout_seconds <= 0:
            raise ValueError("idle_timeout_seconds must be positive and finite")
        self.enabled = enabled
        self.idle_timeout_seconds = idle_timeout_seconds
        self._clock = clock
        self._last_activity: float | None = None
        self._active_requests = 0
        self._awake = False
        self._activity = asyncio.Event()

    @property
    def active_requests(self) -> int:
        """Return the number of tracked ASGI calls still running."""
        return self._active_requests

    @property
    def is_idle(self) -> bool:
        """Return whether optional maintenance must wait for successful activity."""
        if not self.enabled:
            return False
        if not self._awake:
            return True
        if self._active_requests:
            return False
        if self._last_activity is None or self._clock() - self._last_activity >= self.idle_timeout_seconds:
            self._awake = False
            self._activity.clear()
            return True
        return False

    def record_activity(self) -> None:
        """Wake maintenance and extend the active window after a successful response."""
        if not self.enabled:
            return
        self._last_activity = self._clock()
        self._awake = True
        self._activity.set()

    @contextmanager
    def request_activity(self) -> Iterator[None]:
        """Track a complete ASGI call without treating pending traffic as successful.

        Request callers record successful activity separately.
        Synchronous cleanup keeps the counter balanced during cancellation.

        Yields:
            None while the tracked request runs.
        """
        if not self.enabled:
            yield
            return
        self._awake = not self.is_idle
        self._active_requests += 1
        try:
            yield
        finally:
            self._active_requests -= 1

    async def wait_until_active(self) -> None:
        """Wait without polling until successful traffic enables optional maintenance.

        Cancellation propagates to support normal service shutdown.
        This method never records activity or extends the active window.
        """
        while self.is_idle:
            await self._activity.wait()


_idle_activity_gate: IdleActivityGate | None = None
_gate_process_id: int | None = None


def get_idle_activity_gate() -> IdleActivityGate:
    """Return the configured singleton, replacing inherited state after a fork.

    Returns:
        The activity gate for the current worker process.
    """
    global _idle_activity_gate, _gate_process_id  # pylint: disable=global-statement
    process_id = os.getpid()
    if _idle_activity_gate is None or _gate_process_id != process_id:
        # First-Party
        from mcpgateway.config import settings  # pylint: disable=import-outside-toplevel

        _idle_activity_gate = IdleActivityGate(enabled=settings.serverless_idle_enabled, idle_timeout_seconds=settings.serverless_idle_timeout_seconds)
        _gate_process_id = process_id
    return _idle_activity_gate


def reset_idle_activity_gate() -> None:
    """Discard the singleton between isolated tests or stopped application lifecycles.

    Callers must cancel and await existing gate users before resetting the singleton.
    """
    global _idle_activity_gate, _gate_process_id  # pylint: disable=global-statement
    _idle_activity_gate = None
    _gate_process_id = None
