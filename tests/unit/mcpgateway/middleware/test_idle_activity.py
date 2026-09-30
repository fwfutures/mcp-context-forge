# -*- coding: utf-8 -*-
"""Location: ./tests/unit/mcpgateway/middleware/test_idle_activity.py
Copyright contributors to the MCP-CONTEXT-FORGE project
SPDX-License-Identifier: Apache-2.0

ASGI lifecycle and deny-path tests for idle activity tracking.
"""

# Standard
import asyncio
from unittest.mock import AsyncMock, Mock, patch

# Third-Party
import pytest

# First-Party
from mcpgateway.middleware.idle_activity import IdleActivityMiddleware
from mcpgateway.services.idle_activity import IdleActivityGate


@pytest.mark.asyncio
@pytest.mark.parametrize("status,useful", [(200, True), (204, True), (302, False), (303, False), (307, False), (400, False), (401, False), (403, False), (404, False), (429, False), (500, False)])
async def test_only_successful_http_responses_wake_maintenance(status, useful):
    """Pass all messages unchanged and exclude error responses from activity."""
    gate = IdleActivityGate(enabled=True)
    scope = {"type": "http", "path": "/tools", "headers": [(b"authorization", b"test-value")]}
    receive = AsyncMock()
    send = AsyncMock()
    start = {"type": "http.response.start", "status": status, "headers": []}
    body = {"type": "http.response.body", "body": b"response"}

    async def app(seen_scope, seen_receive, seen_send):
        """Emit a controlled response without modifying the request."""
        assert seen_scope is scope
        assert seen_receive is receive
        assert gate.active_requests == 1
        assert gate.is_idle
        await seen_send(start)
        assert gate.is_idle is not useful
        await seen_send(body)

    waiter = asyncio.create_task(gate.wait_until_active())
    await asyncio.sleep(0)
    await IdleActivityMiddleware(app, gate)(scope, receive, send)
    await asyncio.sleep(0)
    assert waiter.done() is useful
    assert gate.is_idle is not useful
    assert gate.active_requests == 0
    assert send.call_args_list[0].args[0] is start
    assert send.call_args_list[1].args[0] is body
    receive.assert_not_called()
    if useful:
        await waiter
    else:
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["/health", "/health/", "/health/security", "/ready", "/metrics", "/metrics/", "/metrics/prometheus"])
@pytest.mark.parametrize("root_path", ["", "/gateway"])
async def test_probes_do_not_wake_or_hold_maintenance(path, root_path):
    """Ignore observed probe endpoints with optional reverse-proxy prefixes."""
    gate = IdleActivityGate(enabled=True)
    scope = {"type": "http", "path": root_path + path, "root_path": root_path}
    receive, send = AsyncMock(), AsyncMock()

    async def app(seen_scope, seen_receive, seen_send):
        """Observe unchanged probe calls."""
        assert (seen_scope, seen_receive, seen_send) == (scope, receive, send)
        assert gate.active_requests == 0
        await seen_send({"type": "http.response.start", "status": 200})

    await IdleActivityMiddleware(app, gate)(scope, receive, send)
    assert gate.is_idle


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["/metrics/reset", "/healthcare", "/ready-made"])
async def test_probe_exclusions_match_only_known_endpoints(path):
    """Track useful requests whose paths merely resemble probe endpoints."""
    gate = IdleActivityGate(enabled=True)

    async def app(_scope, _receive, send):
        """Send a successful response for a non-probe path."""
        await send({"type": "http.response.start", "status": 200})

    await IdleActivityMiddleware(app, gate)({"type": "http", "path": path}, AsyncMock(), AsyncMock())
    assert not gate.is_idle


@pytest.mark.asyncio
async def test_disabled_middleware_passes_original_callables():
    """Avoid wrappers and request accounting when idle tracking is disabled."""
    clock = Mock(side_effect=AssertionError("disabled middleware read the clock"))
    gate = IdleActivityGate(clock=clock)
    app, receive, send = AsyncMock(), AsyncMock(), AsyncMock()
    scope = {"type": "http", "path": "/tools"}
    await IdleActivityMiddleware(app, gate)(scope, receive, send)
    app.assert_awaited_once_with(scope, receive, send)
    assert gate.active_requests == 0
    clock.assert_not_called()


@pytest.mark.asyncio
async def test_lifespan_bypasses_gate_lookup():
    """Forward lifespan messages without creating or activating the gate."""
    app, receive, send = AsyncMock(), AsyncMock(), AsyncMock()
    scope = {"type": "lifespan"}
    with patch("mcpgateway.middleware.idle_activity.get_idle_activity_gate", side_effect=AssertionError("lifespan resolved gate")):
        await IdleActivityMiddleware(app)(scope, receive, send)
    app.assert_awaited_once_with(scope, receive, send)


@pytest.mark.asyncio
async def test_stream_and_background_work_hold_activity_until_application_returns():
    """Keep the complete streaming and background-task lifecycle active."""
    clock = Mock(return_value=100.0)
    gate = IdleActivityGate(enabled=True, clock=clock)
    streaming, finish_stream, background, finish_background = (asyncio.Event() for _ in range(4))

    async def app(_scope, _receive, send):
        """Hold streaming and background work at independent checkpoints."""
        await send({"type": "http.response.start", "status": 200})
        await send({"type": "http.response.body", "body": b"chunk", "more_body": True})
        streaming.set()
        await finish_stream.wait()
        await send({"type": "http.response.body", "body": b"", "more_body": False})
        background.set()
        await finish_background.wait()

    task = asyncio.create_task(IdleActivityMiddleware(app, gate)({"type": "http", "path": "/mcp"}, AsyncMock(), AsyncMock()))
    await streaming.wait()
    clock.return_value = 1000.0
    assert not gate.is_idle
    assert gate.active_requests == 1
    finish_stream.set()
    await background.wait()
    clock.return_value = 2000.0
    assert not gate.is_idle
    assert gate.active_requests == 1
    finish_background.set()
    await task
    assert gate.active_requests == 0
    assert not gate.is_idle
    clock.return_value = 2059.999
    assert not gate.is_idle
    clock.return_value = 2060.0
    assert gate.is_idle


@pytest.mark.asyncio
@pytest.mark.parametrize("accepted", [True, False])
async def test_websocket_acceptance_controls_activity_and_preserves_lifetime(accepted):
    """Track accepted WebSockets while rejected connections remain idle."""
    clock = Mock(return_value=100.0)
    gate = IdleActivityGate(enabled=True, clock=clock)
    entered, finish = asyncio.Event(), asyncio.Event()
    message = {"type": "websocket.accept"} if accepted else {"type": "websocket.close", "code": 1008}
    receive, send = AsyncMock(), AsyncMock()

    async def app(_scope, seen_receive, seen_send):
        """Keep the WebSocket ASGI call open after its handshake."""
        assert seen_receive is receive
        await seen_send(message)
        entered.set()
        await finish.wait()

    task = asyncio.create_task(IdleActivityMiddleware(app, gate)({"type": "websocket", "path": "/ws"}, receive, send))
    await entered.wait()
    clock.return_value = 1000.0
    assert gate.active_requests == 1
    assert gate.is_idle is not accepted
    finish.set()
    await task
    assert gate.active_requests == 0
    assert gate.is_idle is not accepted
    assert send.call_args.args[0] is message


@pytest.mark.asyncio
@pytest.mark.parametrize("accepted", [True, False])
async def test_request_cancellation_releases_guard(accepted):
    """Release guards on cancellation before or after a successful response."""
    gate = IdleActivityGate(enabled=True)
    entered = asyncio.Event()

    async def app(_scope, _receive, send):
        """Wait until the request task is cancelled."""
        if accepted:
            await send({"type": "http.response.start", "status": 200})
        entered.set()
        await asyncio.Event().wait()

    task = asyncio.create_task(IdleActivityMiddleware(app, gate)({"type": "http", "path": "/tools"}, AsyncMock(), AsyncMock()))
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert gate.active_requests == 0
    assert gate.is_idle is not accepted


@pytest.mark.asyncio
async def test_failed_response_send_does_not_record_activity():
    """Propagate send failures and balance the request counter."""
    gate = IdleActivityGate(enabled=True)

    async def app(_scope, _receive, send):
        """Attempt a response through a broken send channel."""
        await send({"type": "http.response.start", "status": 200})

    with pytest.raises(RuntimeError, match="send failed"):
        await IdleActivityMiddleware(app, gate)({"type": "http", "path": "/tools"}, AsyncMock(), AsyncMock(side_effect=RuntimeError("send failed")))
    assert gate.active_requests == 0
    assert gate.is_idle


@pytest.mark.asyncio
async def test_default_middleware_resolves_current_process_gate():
    """Resolve the singleton per request to avoid inherited pre-fork gate state."""
    gate = IdleActivityGate(enabled=True)
    app = AsyncMock()
    with patch("mcpgateway.middleware.idle_activity.get_idle_activity_gate", return_value=gate) as getter:
        await IdleActivityMiddleware(app)({"type": "http", "path": "/tools"}, AsyncMock(), AsyncMock())
    getter.assert_called_once_with()
    assert gate.active_requests == 0


@pytest.mark.asyncio
async def test_concurrent_requests_remain_active_until_last_call_returns():
    """Count independent ASGI tasks and release each guard exactly once."""
    clock = Mock(return_value=100.0)
    gate = IdleActivityGate(enabled=True, clock=clock)
    entered = [asyncio.Event(), asyncio.Event()]
    finish = [asyncio.Event(), asyncio.Event()]

    async def app(scope, _receive, send):
        """Run two independently controlled successful requests."""
        index = scope["request_index"]
        await send({"type": "http.response.start", "status": 200})
        entered[index].set()
        await finish[index].wait()

    middleware = IdleActivityMiddleware(app, gate)
    tasks = [asyncio.create_task(middleware({"type": "http", "path": "/tools", "request_index": index}, AsyncMock(), AsyncMock())) for index in range(2)]
    await asyncio.gather(*(event.wait() for event in entered))
    assert gate.active_requests == 2
    clock.return_value = 1000.0
    finish[0].set()
    await tasks[0]
    assert gate.active_requests == 1
    clock.return_value = 2000.0
    assert not gate.is_idle
    finish[1].set()
    await tasks[1]
    assert gate.active_requests == 0
    assert not gate.is_idle
    clock.return_value = 2060.0
    assert gate.is_idle


@pytest.mark.asyncio
async def test_failed_traffic_does_not_extend_previous_success():
    """Preserve the original deadline across unsuccessful requests."""
    clock = Mock(return_value=100.0)
    gate = IdleActivityGate(enabled=True, clock=clock)
    gate.record_activity()
    clock.return_value = 159.0

    async def app(_scope, _receive, send):
        """Reject a request near the end of the activity window."""
        await send({"type": "http.response.start", "status": 401})

    await IdleActivityMiddleware(app, gate)({"type": "http", "path": "/tools"}, AsyncMock(), AsyncMock())
    clock.return_value = 160.0
    assert gate.is_idle
