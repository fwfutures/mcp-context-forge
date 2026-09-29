# -*- coding: utf-8 -*-
"""Location: ./mcpgateway/services/all_tools_service.py
Copyright contributors to the MCP-CONTEXT-FORGE project
SPDX-License-Identifier: Apache-2.0

"All tools" virtual server.

When ``ALL_TOOLS_SERVER_ID`` names an existing virtual server, its tool, prompt
and resource associations are kept equal to every enabled item in the catalog,
so normal visibility and invocation code paths keep working unchanged. For that
server only, tools from OAuth authorization-code gateways are listed just for
users who have connected the gateway, and a ``connections_list`` tool returns
authorize deep links for the ones they have not connected yet.
"""

# Standard
import logging
import time
from typing import Any, Dict, Iterable, List, Optional, Set

# Third-Party
from sqlalchemy import delete, insert, select
from sqlalchemy.orm import Session

# First-Party
from mcpgateway.config import settings
from mcpgateway.db import Gateway as DbGateway
from mcpgateway.db import OAuthToken, Prompt, Resource, server_prompt_association, server_resource_association, server_tool_association
from mcpgateway.db import Server as DbServer
from mcpgateway.db import Tool as DbTool

logger = logging.getLogger(__name__)

CONNECTIONS_TOOL_NAME = "connections_list"
CONNECTIONS_TOOL_DESCRIPTION = (
    "List services on this gateway that you have not connected yet, with a link to connect each one. "
    "Call this when a service you expect (e.g. Notion, GitHub) has no tools available."
)

_last_sync_monotonic: float = 0.0


def all_tools_server_id() -> Optional[str]:
    """Return the configured "All tools" server id, if any.

    Returns:
        The server id, or ``None`` when the feature is disabled.
    """
    value = getattr(settings, "all_tools_server_id", None)
    return value.strip() if isinstance(value, str) and value.strip() else None


def is_all_tools_server(server_id: Optional[str]) -> bool:
    """Return whether ``server_id`` is the configured "All tools" server.

    Args:
        server_id: Virtual server id from the request path.

    Returns:
        ``True`` for the "All tools" server.

    Examples:
        >>> is_all_tools_server(None)
        False
    """
    configured = all_tools_server_id()
    return bool(configured and server_id and server_id == configured)


def _sync_association(db: Session, table: Any, column: str, server_id: str, wanted: Set[str]) -> int:
    """Make ``table`` link ``server_id`` to exactly ``wanted`` item ids.

    Args:
        db: Database session.
        table: Server association table.
        column: Item id column name in ``table``.
        server_id: Server to sync.
        wanted: Item ids that should be linked.

    Returns:
        Number of rows inserted or deleted.
    """
    item_col = table.c[column]
    current = set(db.execute(select(item_col).where(table.c.server_id == server_id)).scalars().all())
    to_add = wanted - current
    to_remove = current - wanted
    if to_add:
        db.execute(insert(table), [{"server_id": server_id, column: item_id} for item_id in to_add])
    if to_remove:
        db.execute(delete(table).where(table.c.server_id == server_id, item_col.in_(to_remove)))
    return len(to_add) + len(to_remove)


def sync_all_tools_server(db: Session, force: bool = False) -> bool:
    """Link the "All tools" server to every enabled tool, prompt and resource.

    Throttled by ``ALL_TOOLS_SYNC_INTERVAL`` seconds unless ``force`` is set.

    Args:
        db: Database session (committed when anything changes).
        force: Skip the throttle.

    Returns:
        ``True`` when a sync ran.
    """
    global _last_sync_monotonic  # pylint: disable=global-statement
    server_id = all_tools_server_id()
    if not server_id:
        return False
    now = time.monotonic()
    interval = int(getattr(settings, "all_tools_sync_interval", 30))
    if not force and _last_sync_monotonic and now - _last_sync_monotonic < interval:
        return False
    _last_sync_monotonic = now

    if db.get(DbServer, server_id) is None:
        logger.warning("ALL_TOOLS_SERVER_ID %s does not match a virtual server; create it first", server_id)
        return False

    changed = _sync_association(db, server_tool_association, "tool_id", server_id, set(db.execute(select(DbTool.id).where(DbTool.enabled)).scalars().all()))
    changed += _sync_association(db, server_prompt_association, "prompt_id", server_id, set(db.execute(select(Prompt.id).where(Prompt.enabled)).scalars().all()))
    changed += _sync_association(db, server_resource_association, "resource_id", server_id, set(db.execute(select(Resource.id).where(Resource.enabled)).scalars().all()))
    if changed:
        db.commit()
        logger.info("Synced All tools server %s (%d association changes)", server_id, changed)
    return True


def _requires_user_oauth(gateway: Any) -> bool:
    """Return whether a gateway needs a per-user OAuth token (authorization code).

    Args:
        gateway: Gateway ORM object or ``None``.

    Returns:
        ``True`` for OAuth authorization-code gateways.
    """
    oauth_config = getattr(gateway, "oauth_config", None)
    return getattr(gateway, "auth_type", None) == "oauth" and isinstance(oauth_config, dict) and oauth_config.get("grant_type") == "authorization_code"


def connected_gateway_ids(db: Session, user_email: Optional[str], gateway_ids: Iterable[str]) -> Set[str]:
    """Return the gateways (of ``gateway_ids``) the user holds an OAuth token for.

    Args:
        db: Database session.
        user_email: ContextForge user email.
        gateway_ids: Candidate gateway ids.

    Returns:
        Connected gateway ids.
    """
    ids = {gid for gid in gateway_ids if gid}
    if not user_email or not ids:
        return set()
    rows = db.execute(select(OAuthToken.gateway_id).where(OAuthToken.app_user_email == user_email, OAuthToken.gateway_id.in_(ids))).scalars().all()
    return set(rows)


def filter_connected_tools(db: Session, tools: List[Any], user_email: Optional[str]) -> List[Any]:
    """Drop tools whose gateway needs a per-user OAuth token the user lacks.

    Args:
        db: Database session.
        tools: Tool read models (with ``gateway_id``) already visibility-filtered.
        user_email: ContextForge user email.

    Returns:
        Tools the user can actually call.
    """
    gateway_ids = {getattr(tool, "gateway_id", None) for tool in tools} - {None}
    if not gateway_ids:
        return tools
    gateways = db.execute(select(DbGateway).where(DbGateway.id.in_(gateway_ids))).scalars().all()
    user_oauth = {gw.id for gw in gateways if _requires_user_oauth(gw)}
    if not user_oauth:
        return tools
    connected = connected_gateway_ids(db, user_email, user_oauth)
    return [tool for tool in tools if getattr(tool, "gateway_id", None) not in user_oauth or getattr(tool, "gateway_id", None) in connected]


def unconnected_gateways(db: Session, tools: List[Any], user_email: Optional[str]) -> List[Dict[str, str]]:
    """Describe OAuth gateways behind ``tools`` that the user has not connected.

    Args:
        db: Database session.
        tools: Visibility-filtered tools on the "All tools" server.
        user_email: ContextForge user email.

    Returns:
        ``[{"name", "description", "connect_url", "tool_count"}]`` sorted by name.
    """
    counts: Dict[str, int] = {}
    for tool in tools:
        gid = getattr(tool, "gateway_id", None)
        if gid:
            counts[gid] = counts.get(gid, 0) + 1
    if not counts:
        return []
    gateways = [gw for gw in db.execute(select(DbGateway).where(DbGateway.id.in_(counts.keys()))).scalars().all() if _requires_user_oauth(gw)]
    connected = connected_gateway_ids(db, user_email, [gw.id for gw in gateways])
    return sorted(
        (
            {
                "name": gw.name,
                "description": gw.description or "",
                "connect_url": authorize_url(gw.id),
                "tool_count": str(counts[gw.id]),
            }
            for gw in gateways
            if gw.id not in connected
        ),
        key=lambda item: item["name"].lower(),
    )


def authorize_url(gateway_id: str) -> str:
    """Absolute deep link to start a user's OAuth connection for a gateway.

    Args:
        gateway_id: Gateway id.

    Returns:
        ``<APP_DOMAIN><root>/oauth/authorize/<id>``.
    """
    base = str(settings.app_domain).rstrip("/") + str(getattr(settings, "app_root_path", "") or "").rstrip("/")
    return f"{base}/oauth/authorize/{gateway_id}"


def format_connections(items: List[Dict[str, str]]) -> str:
    """Render ``unconnected_gateways`` output for the model and the user.

    Args:
        items: Output of :func:`unconnected_gateways`.

    Returns:
        Markdown text listing connect links.

    Examples:
        >>> format_connections([])
        'All services on this gateway are connected.'
        >>> print(format_connections([{"name": "Notion", "description": "", "connect_url": "https://gw/oauth/authorize/1", "tool_count": "4"}]))
        Not connected yet (open a link to connect, then reconnect or refresh tools):
        - Notion (4 tools): https://gw/oauth/authorize/1
    """
    if not items:
        return "All services on this gateway are connected."
    lines = ["Not connected yet (open a link to connect, then reconnect or refresh tools):"]
    for item in items:
        suffix = f" — {item['description']}" if item.get("description") else ""
        lines.append(f"- {item['name']} ({item['tool_count']} tools): {item['connect_url']}{suffix}")
    return "\n".join(lines)
