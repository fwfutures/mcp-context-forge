# -*- coding: utf-8 -*-
"""Location: ./tests/unit/mcpgateway/services/test_idle_maintenance.py
Copyright contributors to the MCP-CONTEXT-FORGE project
SPDX-License-Identifier: Apache-2.0

Regression tests for opt-in maintenance suspension and profile validation.
"""

# Standard
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

# Third-Party
import pytest

# First-Party
from mcpgateway.config import Settings
from mcpgateway.services.gateway_service import GatewayService
from mcpgateway.services.metrics_cleanup_service import MetricsCleanupService
from mcpgateway.services.metrics_rollup_service import MetricsRollupService
from mcpgateway.cache.session_registry import SessionRegistry


@pytest.mark.parametrize("field,value", [
    ("cache_type", "redis"),
    ("primary_worker_election_backend", "redis"),
    ("mcpgateway_session_affinity_enabled", True),
    ("gateway_async_lifecycle_enabled", True),
    ("gateway_modern_listeners_enabled", True),
    ("use_stateful_sessions", True),
    ("siem_export_enabled", True),
    ("dataplane_publisher", True),
    ("otel_enable_observability", True),
    ("hot_cold_classification_enabled", True),
])
def test_idle_profile_rejects_incompatible_features(field, value):
    """Reject features whose continuous work cannot safely pause."""
    with pytest.raises(ValueError, match="SERVERLESS_IDLE_ENABLED"):
        Settings(serverless_idle_enabled=True, **{field: value})


def test_idle_profile_defaults_off():
    """Preserve the always-on profile."""
    assert Settings().serverless_idle_enabled is False


def test_postgres_idle_requires_null_pool():
    """Require connection closure rather than background TCP keepalives."""
    database_url = "postgresql+psycopg://localhost/test_idle"
    with pytest.raises(ValueError, match="DB_POOL_CLASS=null"):
        Settings(serverless_idle_enabled=True, database_url=database_url)
    assert Settings(serverless_idle_enabled=True, database_url=database_url, db_pool_class="null")
    assert Settings(serverless_idle_enabled=True, database_url="sqlite:///:memory:")


@pytest.mark.asyncio
async def test_gateway_waits_before_database_and_leadership():
    """Idle health maintenance performs no leader or database query."""
    service = SimpleNamespace(_get_gateways=MagicMock(), _health_check_interval=60)
    leader = AsyncMock(return_value=True)
    gate = SimpleNamespace(wait_until_active=AsyncMock(side_effect=asyncio.CancelledError))
    with patch("mcpgateway.services.gateway_service.get_idle_activity_gate", return_value=gate):
        with pytest.raises(asyncio.CancelledError):
            await GatewayService._run_gateway_maintenance_cycle(service, "test@example.com", require_leader=leader)
    service._get_gateways.assert_not_called()
    leader.assert_not_awaited()


@pytest.mark.asyncio
async def test_cleanup_waits_before_database():
    """Idle retention cleanup can be cancelled without touching storage."""
    service = MetricsCleanupService()
    service.cleanup_all = AsyncMock()
    gate = SimpleNamespace(wait_until_active=AsyncMock(side_effect=asyncio.CancelledError))

    async def elapsed_interval(awaitable, **kwargs):
        """Close the unused wait coroutine before simulating an elapsed interval."""
        awaitable.close()
        raise asyncio.TimeoutError

    with patch("mcpgateway.services.metrics_cleanup_service.get_idle_activity_gate", return_value=gate), patch("asyncio.wait_for", side_effect=elapsed_interval):
        with pytest.raises(asyncio.CancelledError):
            await service._cleanup_loop()
    service.cleanup_all.assert_not_awaited()


@pytest.mark.asyncio
async def test_rollup_waits_before_startup_backfill():
    """Idle startup avoids the rollup backfill database scan."""
    service = MetricsRollupService()
    service._detect_backfill_hours = MagicMock()
    gate = SimpleNamespace(wait_until_active=AsyncMock(side_effect=asyncio.CancelledError))
    with patch("mcpgateway.services.metrics_rollup_service.get_idle_activity_gate", return_value=gate):
        with pytest.raises(asyncio.CancelledError):
            await service._rollup_loop()
    service._detect_backfill_hours.assert_not_called()


@pytest.mark.asyncio
async def test_empty_session_cleanup_waits_before_database():
    """An unused database-backed registry does not poll for expired rows."""
    registry = SessionRegistry(backend="database", database_url="sqlite:///:memory:")
    gate = SimpleNamespace(wait_until_active=AsyncMock(side_effect=asyncio.CancelledError))
    with patch("mcpgateway.cache.session_registry.get_idle_activity_gate", return_value=gate), patch("mcpgateway.cache.session_registry.get_db") as get_db:
        with pytest.raises(asyncio.CancelledError):
            await registry._db_cleanup_task()
    get_db.assert_not_called()


@pytest.mark.asyncio
async def test_plugins_cannot_enable_during_idle_profile(monkeypatch):
    """Reject runtime plugin activation before the shared toggle changes."""
    from mcpgateway import plugins
    from mcpgateway.config import settings

    monkeypatch.setattr(settings, "serverless_idle_enabled", True)
    monkeypatch.setattr(plugins, "_PLUGINS_ENABLED", False)
    with pytest.raises(ValueError, match="SERVERLESS_IDLE_ENABLED"):
        plugins.enable_plugins(True)
    assert plugins._PLUGINS_ENABLED is False
    with pytest.raises(ValueError, match="SERVERLESS_IDLE_ENABLED"):
        await plugins.enable_plugins_shared(True)
    assert plugins._PLUGINS_ENABLED is False
