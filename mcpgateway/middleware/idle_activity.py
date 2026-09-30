# -*- coding: utf-8 -*-
"""Location: ./mcpgateway/middleware/idle_activity.py
Copyright contributors to the MCP-CONTEXT-FORGE project
SPDX-License-Identifier: Apache-2.0

Track successful HTTP and WebSocket activity across complete ASGI calls.
"""

# Third-Party
from starlette.types import ASGIApp, Message, Receive, Scope, Send

# First-Party
from mcpgateway.services.idle_activity import get_idle_activity_gate, IdleActivityGate

_PROBE_PATHS = frozenset({"/health", "/health/security", "/ready", "/metrics", "/metrics/prometheus"})


def _is_probe(scope: Scope) -> bool:
    """Identify probe endpoints using the application-relative request path.

    Args:
        scope: The unmodified ASGI request scope.

    Returns:
        Whether this request targets a known health or metrics endpoint.
    """
    path = scope.get("path", "")
    root_path = scope.get("root_path", "").rstrip("/")
    if root_path and (path == root_path or path.startswith(root_path + "/")):
        path = path[len(root_path) :]
    return path.rstrip("/") in _PROBE_PATHS


class IdleActivityMiddleware:
    """Observe successful traffic without changing authentication or ASGI messages.

    HTTP 2xx responses and accepted WebSockets wake optional maintenance.
    Probes and unsuccessful responses do not extend the activity window.
    Accepted streams and background tasks remain tracked until the application returns.
    """

    def __init__(self, app: ASGIApp, gate: IdleActivityGate | None = None) -> None:
        """Store the wrapped application and an optional injected activity gate.

        Args:
            app: The ASGI application to wrap.
            gate: An isolated activity gate for tests, or the process singleton.
        """
        self.app = app
        self._gate = gate

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Track useful activity through response streaming and application cleanup.

        Args:
            scope: The unmodified ASGI connection scope.
            receive: The original ASGI receive callable.
            send: The original ASGI send callable.
        """
        if scope["type"] not in {"http", "websocket"}:
            await self.app(scope, receive, send)
            return
        gate = self._gate if self._gate is not None else get_idle_activity_gate()
        if not gate.enabled or _is_probe(scope):
            await self.app(scope, receive, send)
            return

        useful = False

        async def track_response(message: Message) -> None:
            """Forward a message and record accepted responses.

            Args:
                message: The original ASGI response message.
            """
            nonlocal useful
            await send(message)
            if message["type"] == "websocket.accept" or (message["type"] == "http.response.start" and 200 <= message["status"] < 300):
                useful = True
                gate.record_activity()

        with gate.request_activity():
            try:
                await self.app(scope, receive, track_response)
            finally:
                if useful:
                    gate.record_activity()
