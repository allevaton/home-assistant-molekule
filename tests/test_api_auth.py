import asyncio
from datetime import datetime, timedelta

import aiohttp
import pytest

from molekule.api import MolekuleApi

pytestmark = pytest.mark.asyncio

# A hang is the failure under test, so every await is bounded.
DEADLINE = 1.0


class FakeCognito:
    def __init__(self, n, renew_error=None):
        self.id_token = f"token-{n}"
        self._renew_error = renew_error

    def renew_access_token(self):
        if self._renew_error:
            raise self._renew_error
        self.id_token += "-renewed"


class FakeResponse:
    def __init__(self, status, body=None):
        self.status = status
        self._body = body or {}

    async def json(self):
        return self._body

    async def text(self):
        return str(self._body)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class FakeSession:
    """Plays back a script of responses or exceptions, one per request."""

    def __init__(self, script):
        self._script = list(script)
        self.auth_headers = []

    def request(self, method, url, headers=None, **kwargs):
        self.auth_headers.append(headers["Authorization"])
        step = self._script.pop(0)
        if isinstance(step, Exception):
            raise step
        return step


async def run_blocking(func, *args):
    return func(*args)


def make_api(session, cognitos):
    logins = []

    def factory():
        cognito = cognitos[len(logins)]
        logins.append(cognito)
        return cognito

    api = MolekuleApi(
        "user@example.com",
        "pw",
        session=session,
        run_blocking=run_blocking,
        cognito_factory=factory,
    )
    api._retry_delay = 0
    return api, logins


def expire(api):
    api.token_expiration = datetime.now() - timedelta(minutes=1)


async def test_failed_refresh_falls_back_to_full_login():
    api, logins = make_api(
        FakeSession([]),
        [FakeCognito(1, renew_error=RuntimeError("refresh rejected")), FakeCognito(2)],
    )
    await asyncio.wait_for(api.ensure_token_valid(), DEADLINE)
    expire(api)

    await asyncio.wait_for(api.ensure_token_valid(), DEADLINE)

    assert len(logins) == 2
    assert api.token == "token-2"


async def test_refresh_still_works_after_network_errors():
    dns_error = aiohttp.ClientConnectionError("Timeout while contacting DNS servers")
    session = FakeSession([dns_error, dns_error, dns_error])
    api, logins = make_api(session, [FakeCognito(1)])

    assert await asyncio.wait_for(api.get_devices(), DEADLINE) is None
    expire(api)

    await asyncio.wait_for(api.ensure_token_valid(), DEADLINE)

    assert api.token == "token-1-renewed"
    assert len(logins) == 1


async def test_unauthorized_response_logs_in_again_and_retries():
    session = FakeSession([FakeResponse(401), FakeResponse(200, {"content": []})])
    api, logins = make_api(session, [FakeCognito(1), FakeCognito(2)])

    data = await asyncio.wait_for(api.get_devices(), DEADLINE)

    assert data == {"content": []}
    assert session.auth_headers == ["token-1", "token-2"]


async def test_concurrent_callers_share_one_login():
    api, logins = make_api(FakeSession([]), [FakeCognito(1), FakeCognito(2)])

    await asyncio.wait_for(
        asyncio.gather(*(api.ensure_token_valid() for _ in range(5))), DEADLINE
    )

    assert len(logins) == 1


async def test_action_post_reports_failure_after_network_errors():
    dns_error = aiohttp.ClientConnectionError("dns")
    api, _ = make_api(FakeSession([dns_error, dns_error, dns_error]), [FakeCognito(1)])

    ok = await asyncio.wait_for(api.set_fan_speed("SERIAL", 3), DEADLINE)

    assert ok is False
