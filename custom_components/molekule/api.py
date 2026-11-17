import aiohttp
import asyncio
from pycognito import Cognito
from .const import API_CLIENT_ID, API_POOL_ID, API_URL, API_REGION
import logging
import time
from datetime import datetime, timedelta
from typing import Any, Awaitable, Callable, Dict, Optional

_LOGGER = logging.getLogger(__name__)

# hass.async_add_executor_job's shape: run a blocking callable off the event loop.
RunBlocking = Callable[..., Awaitable[Any]]

_TOKEN_LIFETIME = timedelta(hours=1)
_REFRESH_MARGIN = timedelta(minutes=5)


def clean_none_values(d):
    """Recursively remove all None values from dictionaries and lists, and convert to empty string"""
    if isinstance(d, dict):
        return {k: clean_none_values(v) for k, v in d.items() if v is not None}
    elif isinstance(d, list):
        return [clean_none_values(v) for v in d if v is not None]
    elif d is None:
        return ""
    else:
        return d


class MolekuleApiError(Exception):
    """Base exception for Molekule API errors."""


class AuthenticationError(MolekuleApiError):
    """Authentication related errors."""


class ApiConnectionError(MolekuleApiError):
    """Connection related errors."""


class MolekuleApi:
    """Molekule cloud client.

    Owns no session or thread pool: Home Assistant's are passed in, so a
    network error can never leave the client without one.
    """

    def __init__(
        self,
        email: str,
        password: str,
        session: aiohttp.ClientSession,
        run_blocking: RunBlocking,
        cognito_factory: Optional[Callable[[], Cognito]] = None,
    ):
        self.email = email
        self.password = password
        self._session = session
        self._run_blocking = run_blocking
        self._cognito_factory = cognito_factory or self._create_and_authenticate_cognito
        self._cognito: Optional[Cognito] = None
        self.token: Optional[str] = None
        self.token_expiration: Optional[datetime] = None
        self._auth_lock = asyncio.Lock()
        self._request_timeout = aiohttp.ClientTimeout(total=30)
        self._retry_attempts = 3
        self._retry_delay = 1  # seconds

    # --- Authentication -------------------------------------------------
    # _auth_lock is taken only in the two public methods below; the private
    # helpers assume it is held and never take it, so it cannot deadlock.

    async def authenticate(self) -> None:
        """Log in from scratch with email and password."""
        async with self._auth_lock:
            await self._login()

    async def ensure_token_valid(self) -> None:
        """Log in or refresh as needed so self.token is usable."""
        async with self._auth_lock:
            if not self.token or not self.token_expiration:
                await self._login()
            elif datetime.now() >= self.token_expiration - _REFRESH_MARGIN:
                await self._refresh()

    async def _reauthenticate_if_current(self, rejected_token: Optional[str]) -> None:
        """Log in again after a 401, unless another caller already has."""
        async with self._auth_lock:
            if self.token == rejected_token:
                await self._login()

    async def _login(self) -> None:
        issued_at = datetime.now()
        try:
            self._cognito = await self._run_blocking(self._cognito_factory)
        except Exception as err:
            _LOGGER.error("Authentication failed: %s", err)
            raise AuthenticationError(f"Failed to authenticate: {err}") from err
        self._set_token(issued_at)
        _LOGGER.debug("Logged in to Molekule API")

    async def _refresh(self) -> None:
        if self._cognito is None:
            await self._login()
            return
        issued_at = datetime.now()
        try:
            await self._run_blocking(self._cognito.renew_access_token)
        except Exception as err:
            _LOGGER.warning("Token refresh failed, logging in again: %s", err)
            await self._login()
            return
        self._set_token(issued_at)
        _LOGGER.debug("Refreshed Molekule API token")

    def _set_token(self, issued_at: datetime) -> None:
        self.token = self._cognito.id_token
        self.token_expiration = issued_at + _TOKEN_LIFETIME

    def _create_and_authenticate_cognito(self) -> Cognito:
        """Blocking: build a Cognito client and log in. Runs in the executor."""
        cognito = Cognito(
            API_POOL_ID,
            API_CLIENT_ID,
            username=self.email,
            user_pool_region=API_REGION,
        )
        cognito.authenticate(password=self.password)
        return cognito

    # --- Transport ------------------------------------------------------

    async def _send(self, method: str, url: str, **kwargs) -> tuple[int, Any]:
        """Send one request with auth and retries; return (status, body).

        Body is parsed JSON for 200, text for other non-204 statuses, None
        for 204. Raises ApiConnectionError once retries are exhausted.
        """
        await self.ensure_token_valid()
        last_err: Optional[BaseException] = None

        for attempt in range(self._retry_attempts):
            token = self.token
            headers = {
                "Authorization": token,
                "x-api-version": "1.0",
                "Content-Type": "application/json",
            }
            try:
                async with self._session.request(
                    method, url, headers=headers, timeout=self._request_timeout, **kwargs
                ) as response:
                    if response.status == 401:
                        await self._reauthenticate_if_current(token)
                        continue
                    if response.status == 204:
                        return 204, None
                    if response.status == 200:
                        return 200, await response.json()
                    return response.status, await response.text()
            except (aiohttp.ClientError, asyncio.TimeoutError) as err:
                last_err = err
                _LOGGER.warning(
                    "%s %s failed (attempt %d/%d): %s",
                    method, url, attempt + 1, self._retry_attempts, err,
                )
                if attempt + 1 < self._retry_attempts:
                    await asyncio.sleep(self._retry_delay * (attempt + 1))

        raise ApiConnectionError(
            f"{method} {url} failed after {self._retry_attempts} attempts: {last_err or 'unauthorized'}"
        )

    async def _get_json(self, url: str) -> Optional[Dict[str, Any]]:
        """GET a JSON resource; None on any failure, which is logged."""
        try:
            status, body = await self._send("GET", url)
        except MolekuleApiError as err:
            _LOGGER.error("GET %s failed: %s", url, err)
            return None
        if status != 200:
            if status != 204:
                _LOGGER.error("GET %s returned %s: %s", url, status, body)
            return None
        return body

    async def _post_action(self, url: str, body: dict | None = None) -> bool:
        """POST a device action. Return True only for HTTP 200/204."""
        if body is not None:
            kwargs: Dict[str, Any] = {"json": body}
        else:
            # Mini Auto Protect (homebridge AutoFunctionality=1) expects an empty body.
            kwargs = {"data": b""}
        try:
            status, text = await self._send("POST", url, **kwargs)
        except MolekuleApiError as err:
            _LOGGER.error("Action POST %s failed: %s", url, err)
            return False
        if status in (200, 204):
            return True
        _LOGGER.error("Action POST %s failed with status %s: %s", url, status, text)
        return False

    # --- Endpoints ------------------------------------------------------

    async def get_devices(self) -> Optional[Dict[str, Any]]:
        """Get all devices associated with the account."""
        data = await self._get_json(API_URL)
        return clean_none_values(data) if data else None

    async def get_sensor_data(self, serial: str) -> Optional[Dict[str, Any]]:
        """Get sensor data for a specific device."""
        end_time = int(time.time() * 1000)
        start_time = end_time - 3600000  # 1 hour ago

        url = (f"{API_URL}{serial}/sensordata"
               f"?aggregation=false&fromDate={start_time}"
               f"&resolution=5&toDate={end_time}")

        data = await self._get_json(url)
        return self._process_sensor_data(data) if data else None

    def _process_sensor_data(
        self, data: Dict[str, Any]
    ) -> Optional[Dict[str, Optional[float]]]:
        """Process raw sensor data into a cleaned format."""
        if not data or 'sensorData' not in data:
            return None

        processed_data = {
            "PM2_5": None,
            "PM10": None,
            "RH": None,
            "TVOC": None,
            "CO2": None,
        }

        try:
            for pollutant in data['sensorData']:
                pollutant_type = pollutant['type']
                if pollutant_type in processed_data:
                    values = pollutant.get('sensorDataValue', [])
                    valid_values = [v['v'] for v in values if v['v'] != -1]
                    if valid_values:
                        processed_data[pollutant_type] = valid_values[-1]

            return processed_data
        except Exception as err:
            _LOGGER.error("Error processing sensor data: %s", str(err))
            return None

    async def get_aqi(self, serial: str) -> Optional[Dict[str, Any]]:
        """Get air quality index for a device."""
        data = await self._get_json(f"{API_URL}{serial}/air-quality-index")
        return clean_none_values(data) if data else None

    async def set_power_status(self, serial: str, status: bool) -> bool:
        """Set device power / standby status."""
        url = f"{API_URL}{serial}/actions/set-power-status"
        return await self._post_action(
            url, {"status": "on" if status else "off"}
        )

    async def set_fan_speed(self, serial: str, speed: int) -> bool:
        """Set device fan speed."""
        url = f"{API_URL}{serial}/actions/set-fan-speed"
        return await self._post_action(url, {"fanSpeed": int(speed)})

    async def set_mode_action(
        self, serial: str, action: str, body: dict | None = None
    ) -> bool:
        """Invoke a device-mode action with its API request body."""
        url = f"{API_URL}{serial}/actions/{action}"
        return await self._post_action(url, body)

    async def set_auto_mode(
        self, serial: str, auto: bool, silent: bool = False
    ) -> bool:
        """Enable or leave Auto Protect / smart mode.

        Mini Plus accepts ``enable-smart-mode`` with an empty body. Air Pro
        may want ``{"silent": "0"|"1"}``. Fall back through known variants
        and only report success on HTTP 200/204.
        """
        if auto:
            enable_url = f"{API_URL}{serial}/actions/enable-smart-mode"
            if silent:
                if await self._post_action(enable_url, {"silent": "1"}):
                    return True
            else:
                # Empty body first — required for Air Mini Plus.
                if await self._post_action(enable_url, None):
                    return True
                if await self._post_action(enable_url, {"silent": "0"}):
                    return True
            # jpcaldwell-style fallback
            if await self._post_action(f"{API_URL}{serial}/actions/smart", None):
                return True
            _LOGGER.error("Failed to enable smart mode for %s", serial)
            return False

        # Leaving auto: prefer /actions/manual, else set an explicit fan speed
        # (homebridge exits auto by posting set-fan-speed).
        if await self._post_action(f"{API_URL}{serial}/actions/manual", None):
            return True
        return await self.set_fan_speed(serial, 1)
