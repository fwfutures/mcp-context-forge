# -*- coding: utf-8 -*-
"""Location: ./tests/unit/mcpgateway/services/test_idle_activity.py
Copyright contributors to the MCP-CONTEXT-FORGE project
SPDX-License-Identifier: Apache-2.0

Deterministic tests for process-local optional maintenance gating.
"""

# Standard
import asyncio
from types import SimpleNamespace
from unittest.mock import Mock, patch

# Third-Party
import pytest

# First-Party
from mcpgateway.services.idle_activity import get_idle_activity_gate, IdleActivityGate, reset_idle_activity_gate


@pytest.mark.parametrize("timeout", [0, -1, float("inf"), float("nan")])
def test_rejects_invalid_idle_timeout(timeout):
    """Require a finite positive idle timeout."""
    with pytest.raises(ValueError, match="positive and finite"):
        IdleActivityGate(idle_timeout_seconds=timeout)


@pytest.mark.asyncio
async def test_disabled_gate_does_not_read_clock_or_track_requests():
    """Preserve existing maintenance behavior when the feature is disabled."""
    clock = Mock(side_effect=AssertionError("disabled gate read the clock"))
    gate = IdleActivityGate(clock=clock)
    with gate.request_activity():
        gate.record_activity()
        await gate.wait_until_active()
        assert not gate.is_idle
        assert gate.active_requests == 0
    clock.assert_not_called()


def test_enabled_gate_starts_idle_without_reading_clock():
    """Wait for useful traffic instead of opening a startup maintenance window."""
    clock = Mock(return_value=100.0)
    gate = IdleActivityGate(enabled=True, clock=clock)
    assert gate.is_idle
    assert gate.active_requests == 0
    clock.assert_not_called()


@pytest.mark.parametrize("elapsed,idle", [(59.999, False), (60.0, True), (60.001, True)])
def test_idle_timeout_uses_monotonic_boundary(elapsed, idle):
    """Become idle at the configured timeout after useful activity."""
    clock = Mock(return_value=100.0)
    gate = IdleActivityGate(enabled=True, clock=clock)
    gate.record_activity()
    clock.return_value += elapsed
    assert gate.is_idle is idle


def test_cold_pending_requests_do_not_wake_maintenance():
    """An unknown response cannot activate optional maintenance."""
    gate = IdleActivityGate(enabled=True)
    with gate.request_activity():
        assert gate.active_requests == 1
        assert gate.is_idle
    assert gate.active_requests == 0
    assert gate.is_idle


def test_warm_pending_request_preserves_window_without_extending_timestamp():
    """Keep active work protected until a pending request returns."""
    clock = Mock(return_value=100.0)
    gate = IdleActivityGate(enabled=True, clock=clock)
    gate.record_activity()
    clock.return_value = 159.0
    with gate.request_activity():
        clock.return_value = 1000.0
        assert not gate.is_idle
    assert gate.is_idle


def test_pending_request_after_expiry_does_not_reopen_window():
    """Evaluate expiration before pending traffic can preserve an active window."""
    clock = Mock(return_value=100.0)
    gate = IdleActivityGate(enabled=True, clock=clock)
    gate.record_activity()
    clock.return_value = 160.0
    with gate.request_activity():
        assert gate.is_idle
    assert gate.is_idle


def test_overlapping_requests_preserve_useful_activity_until_all_return():
    """Keep maintenance active while tracked requests overlap a useful response."""
    clock = Mock(return_value=100.0)
    gate = IdleActivityGate(enabled=True, clock=clock)
    with gate.request_activity():
        with gate.request_activity():
            gate.record_activity()
            clock.return_value = 1000.0
            assert gate.active_requests == 2
            assert not gate.is_idle
        assert gate.active_requests == 1
        assert not gate.is_idle
    assert gate.active_requests == 0
    assert gate.is_idle


def test_request_exception_balances_counter():
    """Release the in-flight guard when a request raises an exception."""
    gate = IdleActivityGate(enabled=True)
    with pytest.raises(RuntimeError, match="request failed"):
        with gate.request_activity():
            raise RuntimeError("request failed")
    assert gate.active_requests == 0
    assert gate.is_idle


@pytest.mark.asyncio
async def test_waiters_wake_together_and_wait_again_after_expiry():
    """Wake every suspended loop through an event without renewing activity."""
    clock = Mock(return_value=100.0)
    gate = IdleActivityGate(enabled=True, clock=clock)
    waiters = [asyncio.create_task(gate.wait_until_active()) for _ in range(3)]
    await asyncio.sleep(0)
    assert all(not task.done() for task in waiters)
    gate.record_activity()
    await asyncio.gather(*waiters)
    clock.return_value = 159.0
    await gate.wait_until_active()
    clock.return_value = 160.0
    waiter = asyncio.create_task(gate.wait_until_active())
    await asyncio.sleep(0)
    assert not waiter.done()
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    assert gate.is_idle
    gate.record_activity()
    await gate.wait_until_active()


@pytest.mark.asyncio
async def test_cancelled_request_balances_counter():
    """Cancellation runs synchronous guard cleanup."""
    gate = IdleActivityGate(enabled=True)
    entered = asyncio.Event()

    async def request():
        """Hold an in-flight request until cancellation."""
        with gate.request_activity():
            entered.set()
            await asyncio.Event().wait()

    task = asyncio.create_task(request())
    await entered.wait()
    assert gate.active_requests == 1
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert gate.active_requests == 0


def test_singleton_reset_and_fork_discard_previous_activity():
    """Build new gate state for isolated tests and post-fork workers."""
    config = SimpleNamespace(serverless_idle_enabled=True, serverless_idle_timeout_seconds=17.0)
    reset_idle_activity_gate()
    try:
        with patch("mcpgateway.config.settings", config), patch("mcpgateway.services.idle_activity.os.getpid", return_value=100) as process_id:
            original = get_idle_activity_gate()
            original.record_activity()
            assert original.idle_timeout_seconds == 17.0
            assert get_idle_activity_gate() is original
            process_id.return_value = 101
            child = get_idle_activity_gate()
            assert child is not original
            assert child.is_idle
            assert child.active_requests == 0
            reset_idle_activity_gate()
            reset = get_idle_activity_gate()
            assert reset is not child
            assert reset.is_idle
    finally:
        reset_idle_activity_gate()
