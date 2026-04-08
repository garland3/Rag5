"""Tests for authentication: DEBUG bypass and proxy header support."""

from unittest.mock import patch

import pytest
from httpx import ASGITransport, AsyncClient

from app.api.deps import get_current_user
from app.main import app
from app.services.authz import UserContext


@pytest.mark.asyncio
async def test_debug_mode_bypasses_auth():
    """When DEBUG=true, the user should be the configured TESTUSER."""
    with patch("app.api.deps.settings") as mock_settings:
        mock_settings.debug = True
        mock_settings.testuser = "bob@test.com"

        from starlette.testclient import TestClient
        from fastapi import Request

        # Simulate a request with no auth headers
        from starlette.requests import Request as StarletteRequest
        scope = {
            "type": "http",
            "method": "GET",
            "path": "/",
            "headers": [],
        }
        request = Request(scope)
        user = await get_current_user(request=request, x_user_id=None, x_user_groups=None)
        assert user.user_id == "bob@test.com"


@pytest.mark.asyncio
async def test_proxy_header_auth():
    """When DEBUG=false, user_id should come from the proxy header."""
    with patch("app.api.deps.settings") as mock_settings:
        mock_settings.debug = False
        mock_settings.proxy_user_header = "x-forwarded-user"

        from fastapi import Request
        scope = {
            "type": "http",
            "method": "GET",
            "path": "/",
            "headers": [
                (b"x-forwarded-user", b"alice@example.com"),
            ],
        }
        request = Request(scope)
        user = await get_current_user(request=request, x_user_id=None, x_user_groups=None)
        assert user.user_id == "alice@example.com"


@pytest.mark.asyncio
async def test_fallback_to_x_user_id_header():
    """When DEBUG=false and no proxy header, fall back to x-user-id."""
    with patch("app.api.deps.settings") as mock_settings:
        mock_settings.debug = False
        mock_settings.proxy_user_header = "x-forwarded-user"

        from fastapi import Request
        scope = {
            "type": "http",
            "method": "GET",
            "path": "/",
            "headers": [],
        }
        request = Request(scope)
        user = await get_current_user(
            request=request, x_user_id="charlie@example.com", x_user_groups=None
        )
        assert user.user_id == "charlie@example.com"
