# -*- coding: utf-8 -*-
"""Location: ./tests/unit/mcpgateway/services/test_all_tools_service.py
Copyright contributors to the MCP-CONTEXT-FORGE project
SPDX-License-Identifier: Apache-2.0

Tests for the "All tools" virtual server helpers.
"""

# Standard
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

# Third-Party
import pytest

# First-Party
from mcpgateway.services import all_tools_service as svc


def _gateway(gid, name, oauth=True):
    return SimpleNamespace(
        id=gid,
        name=name,
        description=f"{name} workspace",
        auth_type="oauth" if oauth else None,
        oauth_config={"grant_type": "authorization_code"} if oauth else None,
    )


def _db_returning(gateways):
    db = MagicMock()
    db.execute.return_value.scalars.return_value.all.return_value = gateways
    return db


@pytest.fixture
def configured():
    with patch.object(svc.settings, "all_tools_server_id", "all-1", create=True), patch.object(svc.settings, "app_domain", "https://gw.example.com"), patch.object(
        svc.settings, "app_root_path", "", create=True
    ):
        yield


def test_is_all_tools_server(configured):
    assert svc.is_all_tools_server("all-1")
    assert not svc.is_all_tools_server("other")
    assert not svc.is_all_tools_server(None)


def test_disabled_when_unset():
    with patch.object(svc.settings, "all_tools_server_id", None, create=True):
        assert not svc.is_all_tools_server("all-1")
        assert svc.sync_all_tools_server(MagicMock(), force=True) is False


def test_filter_hides_unconnected_oauth_tools_only(configured):
    tools = [
        SimpleNamespace(name="notion-search", gateway_id="notion"),
        SimpleNamespace(name="github-search", gateway_id="github"),
        SimpleNamespace(name="weather", gateway_id="public-api"),
        SimpleNamespace(name="local", gateway_id=None),
    ]
    db = _db_returning([_gateway("notion", "Notion"), _gateway("github", "GitHub"), _gateway("public-api", "Weather", oauth=False)])
    with patch.object(svc, "connected_gateway_ids", return_value={"notion"}) as connected:
        result = svc.filter_connected_tools(db, tools, "ben@example.com")

    assert [t.name for t in result] == ["notion-search", "weather", "local"]
    connected.assert_called_once_with(db, "ben@example.com", {"notion", "github"})


def test_unconnected_gateways_returns_deep_links(configured):
    tools = [SimpleNamespace(gateway_id="github"), SimpleNamespace(gateway_id="github"), SimpleNamespace(gateway_id="notion")]
    db = _db_returning([_gateway("notion", "Notion"), _gateway("github", "GitHub")])
    with patch.object(svc, "connected_gateway_ids", return_value={"notion"}):
        items = svc.unconnected_gateways(db, tools, "ben@example.com")

    assert items == [
        {"name": "GitHub", "description": "GitHub workspace", "connect_url": "https://gw.example.com/oauth/authorize/github", "tool_count": "2"},
    ]
    assert "https://gw.example.com/oauth/authorize/github" in svc.format_connections(items)


def test_connected_gateway_ids_requires_user():
    assert svc.connected_gateway_ids(MagicMock(), None, ["a"]) == set()
    assert svc.connected_gateway_ids(MagicMock(), "ben@example.com", []) == set()


def test_sync_links_every_enabled_item_and_throttles(configured):
    db = MagicMock()
    db.get.return_value = object()
    calls = []

    def fake_sync(_db, table, column, server_id, wanted):
        calls.append((column, server_id))
        return 1

    with patch.object(svc, "_sync_association", side_effect=fake_sync), patch.object(svc, "_last_sync_monotonic", 0.0):
        assert svc.sync_all_tools_server(db) is True
        assert [c[0] for c in calls] == ["tool_id", "prompt_id", "resource_id"]
        assert all(c[1] == "all-1" for c in calls)
        db.commit.assert_called_once()
        assert svc.sync_all_tools_server(db) is False  # throttled
        assert svc.sync_all_tools_server(db, force=True) is True


def test_sync_skips_missing_server(configured):
    db = MagicMock()
    db.get.return_value = None
    with patch.object(svc, "_last_sync_monotonic", 0.0):
        assert svc.sync_all_tools_server(db, force=True) is False
