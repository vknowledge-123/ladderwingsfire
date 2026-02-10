import logging
import pandas as pd
import base64
from datetime import datetime, timedelta
from dhanhq import dhanhq
from dhanhq import marketfeed
import time
import requests
import io
import threading
import asyncio
import websockets
from functools import lru_cache
from collections import deque
from pathlib import Path
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from websockets.exceptions import ConnectionClosedError, InvalidStatus

try:
    import ujson as json
except ImportError:
    import json

logger = logging.getLogger(__name__)


class RateLimiter:
    """Token bucket rate limiter for API calls."""
    
    def __init__(self, max_requests_per_second=1.0, max_connections=5):
        """
        Initialize rate limiter.
        
        Args:
            max_requests_per_second: Maximum API requests per second (default: 1)
            max_connections: Maximum concurrent connections (default: 5)
        """
        try:
            max_requests_per_second = float(max_requests_per_second)
        except Exception:
            max_requests_per_second = 1.0
        try:
            max_connections = int(max_connections)
        except Exception:
            max_connections = 5

        if max_requests_per_second <= 0:
            logger.warning(f"Invalid max_requests_per_second={max_requests_per_second}; using 1.0")
            max_requests_per_second = 1.0
        if max_connections <= 0:
            logger.warning(f"Invalid max_connections={max_connections}; using 5")
            max_connections = 5

        self.max_requests_per_second = max_requests_per_second
        self.max_connections = max_connections

        # Avoid an initial burst (token bucket starts empty).
        self.tokens = 0.0
        self.last_update = time.time()
        self.lock = threading.Lock()
        self.active_connections = 0
        self.connection_lock = threading.Lock()

        # Temporary server-rate-limit penalty window.
        self._penalty_until = 0.0
        self._penalty_rps = None
        
        logger.info(f"RateLimiter initialized: {max_requests_per_second} req/sec, {max_connections} max connections")

    def penalize(self, cooldown_seconds: float = 10.0, penalty_rps: float = None) -> None:
        """Temporarily reduce effective request rate after server rate-limit responses."""
        try:
            cooldown_seconds = float(cooldown_seconds)
        except Exception:
            cooldown_seconds = 10.0
        if cooldown_seconds <= 0:
            return

        now = time.time()
        with self.lock:
            self._penalty_until = max(self._penalty_until, now + cooldown_seconds)
            if penalty_rps is not None:
                try:
                    penalty_rps = float(penalty_rps)
                except Exception:
                    penalty_rps = None
            if penalty_rps is not None and penalty_rps > 0:
                if self._penalty_rps is None:
                    self._penalty_rps = penalty_rps
                else:
                    self._penalty_rps = min(self._penalty_rps, penalty_rps)

    def _effective_rps(self, now: float) -> float:
        rps = float(self.max_requests_per_second)
        if now < float(self._penalty_until) and self._penalty_rps is not None:
            rps = min(rps, float(self._penalty_rps))
        return max(0.0001, rps)
    
    def acquire(self, retry_on_limit=True, max_retries=3, max_wait_seconds=None):
        """
        Acquire a token to make an API request.
        Blocks until a token is available.
        
        Args:
            retry_on_limit: Whether to retry on rate limit
            max_retries: Maximum number of retries
            max_wait_seconds: Maximum total wait time (None = no limit)
        
        Returns:
            True if token acquired, False if max retries exceeded
        """
        retries = 0
        start = time.time()

        # If max_retries is None, we wait indefinitely (subject to max_wait_seconds if set).
        while True:
            with self.lock:
                now = time.time()
                rps = self._effective_rps(now)

                # Refill tokens based on time elapsed
                elapsed = now - self.last_update
                # Keep bucket capacity at 1 to avoid bursts (smooth request spacing).
                self.tokens = min(1.0, self.tokens + elapsed * rps)
                self.last_update = now
                
                # Check if we have a token available
                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return True
                
                # Calculate wait time
                wait_time = (1.0 - self.tokens) / rps
            
            # If not retrying, return False
            if not retry_on_limit:
                return False

            if max_wait_seconds is not None and (time.time() - start) >= float(max_wait_seconds):
                logger.warning(f"Rate limiter wait exceeded {max_wait_seconds}s")
                return False

            if max_retries is not None and retries >= int(max_retries):
                logger.warning(f"Max retries ({max_retries}) exceeded for rate limiter")
                return False
            
            # Wait and retry
            limit_label = "∞" if max_retries is None else str(max_retries)
            logger.debug(
                f"Rate limit reached, waiting {wait_time:.2f}s (attempt {retries + 1}/{limit_label})"
            )
            time.sleep(wait_time)
            retries += 1
    
    def acquire_connection(self):
        """Acquire a connection slot. Blocks until available."""
        while True:
            with self.connection_lock:
                if self.active_connections < self.max_connections:
                    self.active_connections += 1
                    return
            time.sleep(0.1)
    
    def release_connection(self):
        """Release a connection slot."""
        with self.connection_lock:
            self.active_connections = max(0, self.active_connections - 1)

class DhanClientWrapper:
    def __init__(self, max_requests_per_second=1.0, max_connections=5):
        """
        Initialize Dhan client wrapper with rate limiting.
        
        Args:
            max_requests_per_second: Max API requests per second (default: 1.0)
            max_connections: Max concurrent connections (default: 5)
        """
        self.dhan: dhanhq = None
        self.client_id = None
        self.access_token = None
        self.is_connected = False
        self.symbol_map = {}
        self.id_map = {}
        self.feed = None
        self.ws_thread = None
        self._ws_stop = threading.Event()
        self._ws_lock = threading.Lock()

        # Live Order Update WebSocket
        self._order_ws_thread = None
        self._order_ws_stop = threading.Event()
        self._order_ws_lock = threading.Lock()
        self._order_ws_loop = None
        self._order_ws = None
        self._order_update_callback = None
        self._order_update_connected = False

        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 10
        self.reconnect_delay = 5
        
        # Rate limiting
        self.rate_limiter = RateLimiter(
            max_requests_per_second=max_requests_per_second,
            max_connections=max_connections
        )
        
        # Performance optimizations
        self.tick_batch = []
        self.tick_batch_lock = threading.Lock()
        self.last_batch_process = time.time()
        self.batch_interval = 0.1  # 100ms batching
        
        # Tick diagnostics
        self._tick_count = 0
        self._last_tick_log = 0.0
        self._last_unknown_tick_log = 0.0
        self._mapping_lock = threading.Lock()
        self._security_master_refresh_attempted = False

        # Security master cache (avoid re-downloading CSV on every run)
        self._security_master_cache_path = Path(__file__).resolve().parent / "security_master_nse_eq_cache.json"
        self._security_master_cache_max_age_days = 7

    def set_order_update_callback(self, callback):
        """Register callback to receive raw order-update payloads (dict)."""
        self._order_update_callback = callback

    def start_order_updates(self):
        """Start Dhan Live Order Update WebSocket in a background thread."""
        if not self.is_connected or not self.client_id or not self.access_token:
            return

        with self._order_ws_lock:
            if self._order_ws_thread and self._order_ws_thread.is_alive():
                return
            self._order_ws_stop.clear()
            self._order_update_connected = False

            def _run_thread():
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                self._order_ws_loop = loop
                try:
                    loop.run_until_complete(self._order_ws_run())
                finally:
                    try:
                        loop.close()
                    except Exception:
                        pass
                    self._order_ws_loop = None

            self._order_ws_thread = threading.Thread(
                target=_run_thread,
                name="dhan-order-updates",
                daemon=True,
            )
            self._order_ws_thread.start()

    def stop_order_updates(self):
        """Stop order update websocket and prevent reconnection attempts."""
        self._order_ws_stop.set()

        loop = self._order_ws_loop
        ws = self._order_ws
        if loop and ws:
            try:
                fut = asyncio.run_coroutine_threadsafe(ws.close(), loop)
                fut.result(timeout=1)
            except Exception:
                pass

        t = self._order_ws_thread
        if t and t.is_alive():
            try:
                t.join(timeout=1)
            except Exception:
                pass
        self._order_ws_thread = None
        self._order_ws = None
        self._order_update_connected = False

    async def _order_ws_run(self):
        """Async loop that connects and consumes Dhan order updates."""
        backoff = 1.0
        url = "wss://api-order-update.dhan.co"
        auth_message = {
            "LoginReq": {
                "MsgCode": 42,
                "ClientId": str(self.client_id),
                "Token": str(self.access_token),
            },
            "UserType": "SELF",
        }

        while not self._order_ws_stop.is_set():
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=20) as websocket:
                    self._order_ws = websocket
                    await websocket.send(json.dumps(auth_message))
                    self._order_update_connected = True
                    backoff = 1.0

                    async for message in websocket:
                        if self._order_ws_stop.is_set():
                            break
                        try:
                            data = json.loads(message)
                        except Exception:
                            continue

                        cb = self._order_update_callback
                        if cb:
                            try:
                                cb(data)
                            except Exception as e:
                                logger.error(f"Order update callback error: {e}", exc_info=True)
            except Exception as e:
                self._order_update_connected = False
                if self._order_ws_stop.is_set():
                    break
                logger.warning(f"Order update websocket disconnected: {e}")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2.0, 30.0)
            finally:
                self._order_ws = None
                self._order_update_connected = False

    @staticmethod
    def _extract_client_id_from_token(access_token: str):
        """Best-effort extract Dhan client id from JWT access token payload."""
        if not access_token or access_token.count(".") < 2:
            return None
        try:
            payload_b64 = access_token.split(".", 2)[1]
            payload_b64 += "=" * (-len(payload_b64) % 4)
            payload_raw = base64.urlsafe_b64decode(payload_b64.encode("utf-8"))
            payload = json.loads(payload_raw.decode("utf-8", errors="replace"))
        except Exception:
            return None

        cid = (
            payload.get("dhanClientId")
            or payload.get("dhanClientID")
            or payload.get("clientId")
            or payload.get("client_id")
        )
        if cid is None:
            return None
        return str(cid).strip() or None

    def connect(self, client_id, access_token, prefetch_security_master: bool = True):
        """Connects to Dhan API."""
        try:
            token_client_id = self._extract_client_id_from_token(access_token)
            if token_client_id and token_client_id.isdigit():
                provided = str(client_id).strip() if client_id is not None else ""
                if not provided.isdigit() or provided != token_client_id:
                    if provided:
                        logger.warning(
                            f"Client ID mismatch: provided={provided} token={token_client_id}. "
                            "Using token client id."
                        )
                    client_id = token_client_id

            self.dhan = dhanhq(client_id, access_token)
            self.client_id = client_id
            self.access_token = access_token
            
            # Verify connection by fetching something simple
            self.dhan.get_fund_limits() 
            self.is_connected = True
            logger.info("Connected to Dhan API Successfully")
            
            # Fetch Security Master (can be slow; skip during app startup)
            if prefetch_security_master and not self.symbol_map:
                self.fetch_security_mapping()

            # Build reverse mapping for fast lookups (only if we have data)
            if self.symbol_map and not self.id_map:
                self._build_reverse_mapping()

            # Start Live Order Updates WebSocket (instant order status/fill updates)
            self.start_order_updates()
            
            return True, "Connected"
        except Exception as e:
            self.is_connected = False
            logger.error(f"Failed to connect to Dhan: {e}")
            return False, str(e)

    def fetch_security_mapping(self):
        """Fetches Dhan Scrip Master to map symbols to Security IDs."""
        with self._mapping_lock:
            try:
                if self._try_load_security_master_cache():
                    return

                url = "https://images.dhan.co/api-data/api-scrip-master.csv"
                logger.info("Fetching Security Master CSV...")
                
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                }
                r = requests.get(url, headers=headers, timeout=30)
                r.raise_for_status()
                
                df = pd.read_csv(io.StringIO(r.text))
                
                # Filter for NSE Equity
                equity_df = df[
                    (df['SEM_EXM_EXCH_ID'] == 'NSE') & 
                    (df['SEM_INSTRUMENT_NAME'] == 'EQUITY')
                ]
                
                if equity_df.empty:
                    logger.warning("No NSE Equity records found, trying broad filter")
                    equity_df = df
                
                # Create symbol mapping
                self.symbol_map = dict(zip(
                    equity_df['SEM_TRADING_SYMBOL'], 
                    equity_df['SEM_SMST_SECURITY_ID']
                ))
                
                logger.info(f"Loaded {len(self.symbol_map)} security mappings")
                self._save_security_master_cache()
                
            except Exception as e:
                logger.error(f"Failed to fetch Security Master: {e}")

    def _try_load_security_master_cache(self) -> bool:
        """Load cached NSE_EQ security master mapping if fresh enough."""
        try:
            if not self._security_master_cache_path.exists():
                return False

            age_seconds = time.time() - self._security_master_cache_path.stat().st_mtime
            max_age_seconds = self._security_master_cache_max_age_days * 86400
            if age_seconds > max_age_seconds:
                return False

            raw = self._security_master_cache_path.read_text(encoding="utf-8")
            payload = json.loads(raw) if raw else {}
            symbol_map = payload.get("symbol_map") or {}
            if not isinstance(symbol_map, dict) or not symbol_map:
                return False

            self.symbol_map = symbol_map
            logger.info(
                f"Loaded security master mapping from cache ({len(self.symbol_map)} symbols)"
            )
            return True
        except Exception as e:
            logger.debug(f"Security master cache load failed: {e}")
            return False

    def _save_security_master_cache(self) -> None:
        """Persist current symbol_map to disk for faster next startup."""
        try:
            if not self.symbol_map:
                return
            payload = {
                "saved_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                "symbol_map": self.symbol_map,
            }
            self._security_master_cache_path.write_text(
                json.dumps(payload), encoding="utf-8"
            )
        except Exception as e:
            logger.debug(f"Security master cache save failed: {e}")

    def ensure_security_mapping_loaded(self) -> bool:
        """Ensure symbol/security-id mappings are available (fetch lazily if missing)."""
        if self.symbol_map and self.id_map:
            return True

        if not self.symbol_map:
            self.fetch_security_mapping()

        with self._mapping_lock:
            if self.symbol_map and not self.id_map:
                self._build_reverse_mapping()
            return bool(self.symbol_map)

    def _build_reverse_mapping(self):
        """Build reverse mapping for O(1) lookups."""
        # Keep IDs as integers in reverse mapping
        self.id_map = {int(v): k for k, v in self.symbol_map.items()}
        logger.info(f"Built reverse mapping with {len(self.id_map)} entries")

    @lru_cache(maxsize=1000)
    def get_security_id(self, symbol):
        """Returns Security ID for a symbol as an integer (cached)."""
        symbol = str(symbol).strip().upper()
        if not symbol:
            return None

        # Try direct match
        if symbol in self.symbol_map:
            return int(self.symbol_map[symbol])
            
        # Try appending '-EQ'
        if f"{symbol}-EQ" in self.symbol_map:
             return int(self.symbol_map[f"{symbol}-EQ"])
             
        return None

    def subscribe(self, symbols, callback):
        """Subscribes to real-time feed for the list of symbols."""
        if not self.is_connected:
            logger.error("Cannot subscribe: Not connected")
            return
        
        # CRITICAL: Validate credentials before attempting WebSocket connection
        if not self.client_id or not self.access_token:
            logger.error(f"Cannot subscribe: Missing credentials (client_id={bool(self.client_id)}, access_token={bool(self.access_token)})")
            return
        
        logger.info(f"WebSocket will use: client_id={self.client_id[:10]}..., access_token={'*' * len(self.access_token) if self.access_token else 'NONE'}")

        try:
            # Ensure we have security-id mapping (may have been skipped during startup connect)
            if not self.ensure_security_mapping_loaded():
                logger.error("Cannot subscribe: security master mapping not loaded")
                return

            logger.info(f"Subscribing to {len(symbols)} symbols...")
            
            # Map symbols to IDs (as integers, then convert to string for websocket subscription)
            sub_list = []
            for s in symbols:
                sid = self.get_security_id(s)  # Returns int
                if sid:
                    sub_list.append(int(sid))
                else:
                    logger.warning(f"Could not map {s} for subscription")

            if not sub_list:
                logger.warning("No valid symbols to subscribe")
                return

            # Prepare instruments list
            # Use Dhan SDK constants; subscribe to Quote to get volume (needed for turnover filters)
            exch_code = marketfeed.NSE  # NSE
            instruments = [(exch_code, str(sid), marketfeed.Quote) for sid in sub_list]
            
            logger.info(f"Starting DhanFeed for {len(instruments)} instruments")
            
            # Initialize Feed
            # Use v2 WebSocket as per Dhan docs
            self.feed = marketfeed.DhanFeed(
                self.client_id,
                self.access_token,
                instruments,
                version="v2",
            )

            # Patch SDK server-disconnect handler to log reason (SDK prints to stdout by default)
            try:
                import struct
                import types

                def _server_disconnection(self_feed, data):
                    try:
                        packet = struct.unpack("<BHBIH", data[0:10])
                        code = int(packet[4])
                    except Exception:
                        code = None

                    reasons = {
                        805: "Active websocket connections exceeded",
                        806: "Subscribe to Data APIs to continue",
                        807: "Access token is expired",
                        808: "Invalid client ID",
                        809: "Authentication failed",
                    }
                    reason = reasons.get(code, "Server disconnected")

                    setattr(self_feed, "on_close", True)
                    setattr(self_feed, "last_disconnect_code", code)
                    setattr(self_feed, "last_disconnect_reason", reason)
                    logger.error(f"Dhan server disconnect: code={code} reason={reason}")
                    return None

                self.feed.server_disconnection = types.MethodType(_server_disconnection, self.feed)
            except Exception as e:
                logger.debug(f"Could not patch server disconnection handler: {e}")
            
            # Store callback for reconnection
            self._callback = callback
            self._symbols = list(symbols)
            
            # Run WebSocket loop in a dedicated thread
            def run_feed_in_thread():
                """Run DhanFeed in separate thread and consume ticks."""
                import asyncio

                try:
                    import nest_asyncio
                    nest_asyncio.apply()
                    logger.info("Applied nest_asyncio for better compatibility")
                except ImportError:
                    logger.warning("nest_asyncio not available, continuing without it")
                except Exception as e:
                    logger.warning(f"Could not apply nest_asyncio: {e}")

                async def _run():
                    self.reconnect_attempts = 0
                    while not self._ws_stop.is_set():
                        try:
                            await self.feed.connect()
                            logger.info("Dhan WebSocket Connected Successfully")
                            self._on_ws_connect(self.feed)

                            # Reset attempts after a successful connect
                            self.reconnect_attempts = 0

                            while not self._ws_stop.is_set():
                                tick = await self.feed.get_instrument_data()
                                if getattr(self.feed, "on_close", False):
                                    code = getattr(self.feed, "last_disconnect_code", None)
                                    reason = getattr(self.feed, "last_disconnect_reason", "unknown")
                                    fatal_codes = {806, 807, 808, 809}
                                    if code in fatal_codes:
                                        logger.error(
                                            f"Fatal Dhan disconnect (code={code}): {reason}. "
                                            "Stopping feed; refresh credentials / subscription."
                                        )
                                        self._ws_stop.set()
                                    raise ConnectionError(f"Dhan server disconnect (code={code}): {reason}")

                                if tick:
                                    self._on_tick(tick, callback)
                        except ConnectionError as e:
                            logger.error(str(e))
                            self._on_ws_error(self.feed, e)
                            self.reconnect_attempts += 1

                            code = getattr(self.feed, "last_disconnect_code", None)
                            if code == 805:
                                # Too many active connections; back off hard
                                backoff = min(10 * (2 ** min(self.reconnect_attempts, 6)), 300)
                            else:
                                backoff = min(2 * (2 ** min(self.reconnect_attempts, 6)), 60)

                            if self._ws_stop.is_set():
                                break
                            await asyncio.sleep(backoff)
                        except ConnectionClosedError as e:
                            logger.warning(f"WebSocket closed unexpectedly: {e}")
                            self._on_ws_error(self.feed, e)
                            self.reconnect_attempts += 1
                            backoff = min(2 * (2 ** min(self.reconnect_attempts, 6)), 60)
                            if self._ws_stop.is_set():
                                break
                            await asyncio.sleep(backoff)
                        except InvalidStatus as e:
                            # 429 = too many requests / connections; back off hard
                            logger.error(f"WebSocket handshake rejected: {e}", exc_info=True)
                            self._on_ws_error(self.feed, e)
                            self.reconnect_attempts += 1
                            backoff = min(10 * (2 ** min(self.reconnect_attempts, 6)), 300)
                            if self._ws_stop.is_set():
                                break
                            await asyncio.sleep(backoff)
                        except Exception as e:
                            logger.error(f"WebSocket error in thread: {e}", exc_info=True)
                            self._on_ws_error(self.feed, e)
                            self.reconnect_attempts += 1
                            backoff = min(2 * (2 ** min(self.reconnect_attempts, 6)), 60)
                            if self._ws_stop.is_set():
                                break
                            await asyncio.sleep(backoff)
                        finally:
                            try:
                                if getattr(self.feed, "ws", None):
                                    await self.feed.ws.close()
                            except Exception:
                                pass
                            try:
                                setattr(self.feed, "ws", None)
                            except Exception:
                                pass
                            self._on_ws_close(self.feed)

                asyncio.run(_run())

            with self._ws_lock:
                # Ensure we don't start multiple websocket threads
                if self.ws_thread and self.ws_thread.is_alive():
                    logger.info("WebSocket thread already running; skipping new start")
                    return
                self._ws_stop.clear()
                self.ws_thread = threading.Thread(target=run_feed_in_thread, daemon=True)
                self.ws_thread.start()
            
            logger.info("WebSocket Thread Started")
            
        except Exception as e:
            logger.error(f"Subscription failed: {e}")

    def _on_ws_connect(self, instance):
        logger.info("Dhan WebSocket Connected")
        self.reconnect_attempts = 0

    def _on_ws_error(self, instance, error):
        logger.error(f"Dhan WebSocket Error: {error}")

    def _on_ws_close(self, instance):
        logger.warning("Dhan WebSocket Closed")
        # Reconnect is handled inside the websocket thread loop.

    def stop_feed(self):
        """Stop websocket feed and prevent reconnection attempts."""
        self._ws_stop.set()
        self.stop_order_updates()

    def _handle_reconnect(self):
        """Handle WebSocket reconnection with exponential backoff."""
        if self.reconnect_attempts >= self.max_reconnect_attempts:
            logger.error("Max reconnection attempts reached")
            return
            
        self.reconnect_attempts += 1
        delay = self.reconnect_delay * (2 ** (self.reconnect_attempts - 1))
        delay = min(delay, 60)  # Cap at 60 seconds
        
        logger.info(f"Attempting reconnection in {delay}s (attempt {self.reconnect_attempts})")
        time.sleep(delay)
        
        try:
            if hasattr(self, '_callback') and hasattr(self, '_symbols'):
                self.subscribe(self._symbols, self._callback)
        except Exception as e:
            logger.error(f"Reconnection failed: {e}")

    def _on_tick(self, tick_data, callback):
        """Process tick data with batching for performance."""
        start_time = time.time()
        
        try:
            # Handle list/batched tick payloads
            if isinstance(tick_data, list):
                for item in tick_data:
                    self._on_tick(item, callback)
                return
            
            if isinstance(tick_data, dict) and isinstance(tick_data.get('data'), list):
                for item in tick_data['data']:
                    self._on_tick(item, callback)
                return

            if not isinstance(tick_data, dict):
                return

            # Extract security id (support multiple key names)
            sid_val = (
                tick_data.get('security_id')
                or tick_data.get('securityId')
                or tick_data.get('sec_id')
            )

            # Extract LTP (support multiple key names)
            ltp_val = (
                tick_data.get('LTP')
                or tick_data.get('ltp')
                or tick_data.get('last_traded_price')
                or tick_data.get('last_price')
            )

            if sid_val is None or ltp_val is None:
                now = time.time()
                if now - self._last_unknown_tick_log > 5.0:
                    logger.warning(f"Unknown tick format keys: {list(tick_data.keys())}")
                    self._last_unknown_tick_log = now
                return

            sid = int(sid_val)
            ltp = float(ltp_val)
                
            # Extract Volume
            volume = 0.0
            if 'volume' in tick_data:
                volume = float(tick_data['volume'])
            elif 'total_volume' in tick_data:
                volume = float(tick_data['total_volume'])
                
            # Get symbol from reverse mapping
            symbol = self.id_map.get(sid)
            if symbol and symbol.endswith("-EQ"):
                symbol = symbol[:-3]
            if symbol:
                try:
                    # Call callback directly for low latency
                    callback(symbol, ltp, volume)
                    
                    self._tick_count += 1
                    now = time.time()
                    if now - self._last_tick_log > 10.0:
                        logger.info(f"Ticks received: {self._tick_count} (last: {symbol} {ltp})")
                        self._last_tick_log = now
                    
                    # Record performance
                    latency_ms = (time.time() - start_time) * 1000
                    if latency_ms > 10:  # Log only if > 10ms
                        logger.debug(f"Tick processing took {latency_ms:.2f}ms")
                        
                except TypeError:
                    # Fallback for old signature
                    callback(symbol, ltp)
                    
        except Exception as e:
            logger.error(f"Tick processing error: {e}")

    async def get_historical_data_async(self, symbol, exchange_segment="NSE_EQ", days=15):
        """Async version of historical data fetching."""
        # For now, call sync version in thread pool
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None, 
            self.get_historical_data, 
            symbol, 
            exchange_segment, 
            days
        )

    def get_historical_data(self, symbol, exchange_segment="NSE_EQ", days=15):
        """Fetches historical data for the last N days with rate limiting."""
        if not self.is_connected:
            return None

        try:
            symbol = str(symbol).strip().upper()
            if not symbol:
                return None

            # Ensure we have security-id mapping (may have been skipped during connect)
            if not self.ensure_security_mapping_loaded():
                logger.error("Security master mapping not loaded (cannot fetch historical data)")
                return None

            security_id = self.get_security_id(symbol)
            if not security_id and not self._security_master_refresh_attempted:
                # Best-effort refresh once (handles stale/partial cache)
                self._security_master_refresh_attempted = True
                self.fetch_security_mapping()
                self.ensure_security_mapping_loaded()
                security_id = self.get_security_id(symbol)

            if not security_id:
                logger.warning(f"Security ID not found for {symbol}")
                return None

            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=days)

            def _is_server_rate_limit(resp) -> bool:
                if not isinstance(resp, dict):
                    return False
                if resp.get("status") != "failure":
                    return False
                remarks = resp.get("remarks") or {}
                data = resp.get("data") or {}
                code = remarks.get("error_code") or data.get("errorCode")
                etype = remarks.get("error_type") or data.get("errorType") or ""
                msg = remarks.get("error_message") or data.get("errorMessage") or ""
                if str(code).strip().upper() == "DH-904":
                    return True
                if "RATE_LIMIT" in str(etype).strip().upper():
                    return True
                if "rate limit" in str(msg).lower():
                    return True
                return False

            attempt = 0
            backoff_seconds = 1.0
            max_attempts = 12

            while True:
                attempt += 1

                # Acquire rate limit token (wait instead of skipping)
                if not self.rate_limiter.acquire(max_retries=None):
                    logger.error(f"Rate limiter could not acquire token for {symbol}")
                    return None

                # Acquire connection slot
                self.rate_limiter.acquire_connection()

                try:
                    # DEBUG: Log what we're about to send
                    logger.debug(
                        f"Fetching history for {symbol}: security_id={security_id} (type={type(security_id).__name__})"
                    )

                    response = self.dhan.historical_daily_data(
                        security_id=str(security_id),  # SDK expects string
                        exchange_segment=exchange_segment,  # Use string like "NSE_EQ"
                        instrument_type="EQUITY",
                        from_date=start_date.strftime("%Y-%m-%d"),
                        to_date=end_date.strftime("%Y-%m-%d"),
                    )
                except Exception as e:
                    # Treat transient transport errors as retryable (bounded)
                    if attempt >= max_attempts:
                        logger.error(f"Exception fetching history for {symbol} (attempt {attempt}): {e}")
                        return None
                    sleep_for = min(10.0, backoff_seconds)
                    logger.warning(
                        f"Exception fetching history for {symbol} (attempt {attempt}/{max_attempts}); "
                        f"sleeping {sleep_for:.1f}s then retrying: {e}"
                    )
                    time.sleep(sleep_for)
                    backoff_seconds = min(20.0, backoff_seconds * 2.0)
                    continue
                finally:
                    # Always release connection (if not already released in exception path)
                    try:
                        self.rate_limiter.release_connection()
                    except Exception:
                        pass

                if isinstance(response, dict) and response.get("status") == "success":
                    data = response.get("data")
                    return pd.DataFrame(data)

                if _is_server_rate_limit(response):
                    if attempt >= max_attempts:
                        logger.error(f"Server rate-limit persists for {symbol} after {attempt} attempts: {response}")
                        return None

                    # Penalize limiter and retry with backoff.
                    effective_now = self.rate_limiter._effective_rps(time.time())
                    penalty_rps = max(0.5, float(effective_now) * 0.7)
                    self.rate_limiter.penalize(cooldown_seconds=60.0, penalty_rps=penalty_rps)

                    sleep_for = min(20.0, backoff_seconds)
                    logger.warning(
                        f"Server rate-limited historical data for {symbol} (DH-904). "
                        f"Cooling down: sleep {sleep_for:.1f}s then retry (attempt {attempt}/{max_attempts}); "
                        f"effective_rps≤{penalty_rps:.2f}"
                    )
                    time.sleep(sleep_for)
                    backoff_seconds = min(20.0, backoff_seconds * 2.0)
                    continue

                logger.error(f"Failed to fetch history for {symbol}: {response}")
                return None
                
        except Exception as e:
            logger.error(f"Exception fetching history for {symbol}: {e}")
            return None

    def place_order(self, symbol, transaction_type, quantity, 
                   exchange_segment="NSE_EQ", product_type="INTRADAY", 
                   order_type="MARKET"):
        """Places a market order with timing."""
        if not self.is_connected:
            return None
        
        start_time = time.time()
        
        try:
            security_id = self.get_security_id(symbol)
            if not security_id:
                # Lazy-load security master if needed
                self.ensure_security_mapping_loaded()
                security_id = self.get_security_id(symbol)
            if not security_id:
                logger.error(f"Cannot place order: Security ID not found for {symbol}")
                return None

            # According to Dhan SDK:
            # security_id: string
            # exchange_segment: string like "NSE_EQ", "BSE_EQ"
            # transaction_type: string "BUY" or "SELL"  
            # order_type: string "MARKET" or "LIMIT"
            # product_type: string "INTRADAY" or "CNC"
            # quantity: integer
            # price: float (0 for market orders)
            
            response = self.dhan.place_order(
                security_id=str(security_id),
                exchange_segment=exchange_segment,  # Use "NSE_EQ" directly
                transaction_type=transaction_type,   # Use "BUY"/"SELL" directly
                quantity=int(quantity),
                order_type=order_type,               # Use "MARKET"/"LIMIT" directly  
                product_type=product_type,           # Use "INTRADAY"/"CNC" directly
                price=0.0 if order_type == "MARKET" else 0.0
            )
            
            latency_ms = (time.time() - start_time) * 1000
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    "Order placed in %.2fms: %s %s %s -> %s",
                    latency_ms,
                    symbol,
                    transaction_type,
                    quantity,
                    response,
                )
            
            return response
            
        except Exception as e:
            logger.error(f"Order Placement Failed: {e}")
            return None

    def get_positions(self):
        """Fetch current positions from Dhan."""
        if not self.is_connected:
            return []
        
        try:
            response = self.dhan.get_positions()
            if response.get('status') == 'success':
                return response.get('data', [])
            return []
        except Exception as e:
            logger.error(f"Failed to fetch positions: {e}")
            return []

    def get_top_movers(self, symbols, top_n_gainers=5, top_n_losers=5, exchange_segment="NSE_EQ"):
        """
        Fetch LTP/prev close via REST and compute top gainers/losers.
        Uses batching (100 instruments per request) to respect API limits.
        """
        if not self.is_connected:
            return {"gainers": [], "losers": [], "errors": ["Not connected"]}

        if not self.ensure_security_mapping_loaded():
            return {"gainers": [], "losers": [], "errors": ["Security master not loaded"]}

        # Map symbols to security IDs
        security_ids = []
        for s in symbols:
            sid = self.get_security_id(s)
            if sid:
                security_ids.append(int(sid))
            else:
                logger.debug(f"Top movers: no security id for {s}")

        if not security_ids:
            return {"gainers": [], "losers": [], "errors": ["No valid security ids"]}

        def _iter_batches(items, size=100):
            for i in range(0, len(items), size):
                yield items[i:i + size]

        movers = []
        errors = []

        for batch in _iter_batches(security_ids, 100):
            try:
                response = self.dhan.ohlc_data({exchange_segment: batch})
            except Exception as e:
                errors.append(str(e))
                continue

            if not response or response.get("status") != "success":
                errors.append(str(response))
                continue

            data = response.get("data", {})
            segment_data = data.get(exchange_segment, data)

            # segment_data can be dict keyed by security_id or list of entries
            if isinstance(segment_data, dict):
                items = segment_data.items()
            elif isinstance(segment_data, list):
                items = [(d.get("securityId") or d.get("security_id"), d) for d in segment_data]
            else:
                items = []

            for sid, payload in items:
                try:
                    if payload is None:
                        continue
                    sid_int = int(sid) if sid is not None else None
                    symbol = self.id_map.get(sid_int)
                    if not symbol:
                        continue

                    ltp = (
                        payload.get("ltp")
                        or payload.get("LTP")
                        or payload.get("last_traded_price")
                        or payload.get("last_price")
                    )
                    prev_close = (
                        payload.get("prev_close")
                        or payload.get("prevClose")
                        or payload.get("close")
                    )
                    volume = payload.get("volume") or payload.get("total_volume") or 0.0

                    if ltp is None or prev_close is None:
                        continue

                    ltp = float(ltp)
                    prev_close = float(prev_close)
                    if prev_close <= 0:
                        continue

                    change_pct = ((ltp - prev_close) / prev_close) * 100.0
                    turnover = float(volume) * ltp if volume else 0.0

                    movers.append({
                        "symbol": symbol,
                        "ltp": ltp,
                        "prev_close": prev_close,
                        "change_pct": change_pct,
                        "turnover": turnover,
                    })
                except Exception:
                    continue

        movers.sort(key=lambda x: x["change_pct"], reverse=True)
        gainers = movers[:top_n_gainers]
        losers = list(reversed(movers[-top_n_losers:])) if movers else []

        return {"gainers": gainers, "losers": losers, "errors": errors}

    def get_ohlc_snapshot(self, symbols, exchange_segment="NSE_EQ"):
        """
        Fetch OHLC-like snapshot (LTP, prev close, volume) for a symbol universe.

        Returns a dict: symbol -> {ltp, prev_close, volume, turnover, change_pct}
        """
        if not self.is_connected:
            return {}

        if not self.ensure_security_mapping_loaded():
            return {}

        # Map symbols to security IDs
        security_ids = []
        for s in symbols:
            sid = self.get_security_id(s)
            if sid:
                security_ids.append(int(sid))

        if not security_ids:
            return {}

        def _iter_batches(items, size=100):
            for i in range(0, len(items), size):
                yield items[i:i + size]

        def _is_server_rate_limit(resp) -> bool:
            if not isinstance(resp, dict):
                return False
            if resp.get("status") != "failure":
                return False
            remarks = resp.get("remarks") or {}
            data = resp.get("data") or {}
            code = remarks.get("error_code") or data.get("errorCode")
            etype = remarks.get("error_type") or data.get("errorType") or ""
            msg = remarks.get("error_message") or data.get("errorMessage") or ""
            if str(code).strip().upper() == "DH-904":
                return True
            if "RATE_LIMIT" in str(etype).strip().upper():
                return True
            if "rate limit" in str(msg).lower():
                return True
            return False

        snapshot = {}

        for batch in _iter_batches(security_ids, 100):
            attempt = 0
            backoff = 1.0
            max_attempts = 6

            while True:
                attempt += 1

                # Throttle REST calls too
                self.rate_limiter.acquire(max_retries=None)
                self.rate_limiter.acquire_connection()
                try:
                    response = self.dhan.ohlc_data({exchange_segment: batch})
                except Exception as e:
                    response = {"status": "failure", "remarks": {"error_message": str(e)}}
                finally:
                    self.rate_limiter.release_connection()

                if _is_server_rate_limit(response):
                    if attempt >= max_attempts:
                        logger.error(f"OHLC snapshot batch rate-limited after {attempt} attempts: {response}")
                        break

                    effective_now = self.rate_limiter._effective_rps(time.time())
                    penalty_rps = max(0.5, float(effective_now) * 0.7)
                    self.rate_limiter.penalize(cooldown_seconds=60.0, penalty_rps=penalty_rps)

                    sleep_for = min(20.0, backoff)
                    logger.warning(
                        f"OHLC snapshot rate-limited (DH-904). Sleeping {sleep_for:.1f}s then retrying "
                        f"(attempt {attempt}/{max_attempts}); effective_rps≤{penalty_rps:.2f}"
                    )
                    time.sleep(sleep_for)
                    backoff = min(20.0, backoff * 2.0)
                    continue

                if not response or response.get("status") != "success":
                    logger.warning(f"OHLC snapshot failed for batch (attempt {attempt}): {response}")
                    break

                data = response.get("data", {})
                segment_data = data.get(exchange_segment, data)

                if isinstance(segment_data, dict):
                    items = segment_data.items()
                elif isinstance(segment_data, list):
                    items = [(d.get("securityId") or d.get("security_id"), d) for d in segment_data]
                else:
                    items = []

                for sid, payload in items:
                    try:
                        if payload is None:
                            continue
                        sid_int = int(sid) if sid is not None else None
                        symbol = self.id_map.get(sid_int)
                        if not symbol:
                            continue

                        ltp = (
                            payload.get("ltp")
                            or payload.get("LTP")
                            or payload.get("last_traded_price")
                            or payload.get("last_price")
                        )
                        prev_close = (
                            payload.get("prev_close")
                            or payload.get("prevClose")
                            or payload.get("close")
                        )
                        volume = payload.get("volume") or payload.get("total_volume") or 0.0

                        if ltp is None or prev_close is None:
                            continue

                        ltp = float(ltp)
                        prev_close = float(prev_close)
                        volume = float(volume or 0.0)
                        if prev_close <= 0:
                            continue

                        change_pct = ((ltp - prev_close) / prev_close) * 100.0
                        turnover = volume * ltp if volume else 0.0

                        snapshot[symbol] = {
                            "ltp": ltp,
                            "prev_close": prev_close,
                            "volume": volume,
                            "turnover": turnover,
                            "change_pct": change_pct,
                        }
                    except Exception:
                        continue

                break

        return snapshot

    def square_off_position(self, symbol, quantity, transaction_type):
        """Square off a position."""
        opposite_type = "SELL" if transaction_type == "BUY" else "BUY"
        return self.place_order(
            symbol=symbol,
            transaction_type=opposite_type,
            quantity=quantity,
            order_type="MARKET",
            product_type="INTRADAY"
        )
