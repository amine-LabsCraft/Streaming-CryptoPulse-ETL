"""
CryptoPulse ETL - Binance WebSocket Extractor.

Receives live trade data from Binance Spot WebSocket streams,
validates each trade, normalizes the payload, and forwards valid
records to the transformation layer.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import random

from collections import Counter, deque
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Awaitable, Callable, Deque, Mapping, Optional, Sequence

import aiohttp
from aiohttp import ClientWebSocketResponse, WSMsgType

from config.settings import BINANCE_WS_URL, SYMBOLS


logger = logging.getLogger(__name__)


TradeRecord = dict[str, Any]

TradeCallback = Callable[
    [TradeRecord],
    Optional[Awaitable[None]],
]


class BinanceWebSocketExtractor:
    """
    Extract live trades from Binance Spot WebSocket.

    Responsibilities
    ----------------
    1. Build the Binance WebSocket URL.
    2. Establish and maintain the WebSocket connection.
    3. Reconnect automatically after recoverable failures.
    4. Decode combined or raw Binance payloads.
    5. Validate trade messages.
    6. Normalize prices and quantities with Decimal.
    7. Forward valid trades to a callback.
    8. Track extraction and data-quality counters.

    Notes
    -----
    Raw trades are retained only in a bounded in-memory deque for
    debugging. They are not intended for durable storage.
    """

    def __init__(
        self,
        symbols: Optional[Sequence[str]] = None,
        callback: Optional[TradeCallback] = None,
        *,
        websocket_base_url: str = BINANCE_WS_URL,
        max_buffer_size: int = 1_000,
        connect_timeout_seconds: float = 15.0,
        reconnect_initial_delay_seconds: float = 1.0,
        reconnect_max_delay_seconds: float = 60.0,
        callback_timeout_seconds: Optional[float] = 10.0,
    ) -> None:
        """
        Initialize the Binance WebSocket extractor.

        Parameters
        ----------
        symbols:
            Binance symbols to subscribe to.

            Examples:
            - btcusdt
            - ethusdt

        callback:
            Synchronous or asynchronous function invoked for each
            valid normalized trade.

        websocket_base_url:
            Binance WebSocket base endpoint.

            Recommended value:
            wss://stream.binance.com:9443

        max_buffer_size:
            Maximum number of validated trades retained in memory.

            Set to 0 to disable the debugging buffer.

        connect_timeout_seconds:
            Maximum time allowed to establish the WebSocket connection.

        reconnect_initial_delay_seconds:
            Initial delay before reconnecting.

        reconnect_max_delay_seconds:
            Maximum delay between reconnect attempts.

        callback_timeout_seconds:
            Maximum execution time for an asynchronous callback.

            Set to None to disable the callback timeout.
        """
        self.symbols = self._normalize_symbols(symbols or SYMBOLS)

        if not self.symbols:
            raise ValueError(
                "At least one Binance symbol must be configured."
            )

        if max_buffer_size < 0:
            raise ValueError(
                "max_buffer_size must be greater than or equal to zero."
            )

        if connect_timeout_seconds <= 0:
            raise ValueError(
                "connect_timeout_seconds must be greater than zero."
            )

        if reconnect_initial_delay_seconds <= 0:
            raise ValueError(
                "reconnect_initial_delay_seconds must be greater than zero."
            )

        if reconnect_max_delay_seconds < reconnect_initial_delay_seconds:
            raise ValueError(
                "reconnect_max_delay_seconds must be greater than or "
                "equal to reconnect_initial_delay_seconds."
            )

        self.callback = callback

        self.websocket_base_url = websocket_base_url.rstrip("/")

        self.connect_timeout_seconds = connect_timeout_seconds
        self.reconnect_initial_delay_seconds = (
            reconnect_initial_delay_seconds
        )
        self.reconnect_max_delay_seconds = reconnect_max_delay_seconds
        self.callback_timeout_seconds = callback_timeout_seconds

        self._tracked_symbols = frozenset(
            symbol.upper()
            for symbol in self.symbols
        )

        self._stop_event = asyncio.Event()
        self._websocket: Optional[ClientWebSocketResponse] = None

        self.running = False
        self.connected = False

        self.started_at: Optional[datetime] = None
        self.stopped_at: Optional[datetime] = None
        self.last_message_at: Optional[datetime] = None
        self.last_valid_trade_at: Optional[datetime] = None

        self.messages_received = 0
        self.messages_valid = 0
        self.messages_rejected = 0
        self.websocket_messages_received = 0

        self.connection_attempts = 0
        self.successful_connections = 0
        self.reconnect_count = 0
        self.callback_errors = 0
        self.json_decode_errors = 0

        self.rejection_reasons: Counter[str] = Counter()

        self.received_trades: Deque[TradeRecord] = deque(
            maxlen=max_buffer_size or None
        )

        self._store_received_trades = max_buffer_size > 0

    @staticmethod
    def _normalize_symbols(
        symbols: Sequence[str],
    ) -> tuple[str, ...]:
        """
        Normalize and deduplicate Binance symbols.

        Symbols are stored in lowercase because Binance WebSocket
        stream names require lowercase symbols.
        """
        normalized_symbols: list[str] = []
        seen: set[str] = set()

        for symbol in symbols:
            if not isinstance(symbol, str):
                raise TypeError(
                    "Every configured symbol must be a string."
                )

            normalized_symbol = symbol.strip().lower()

            if not normalized_symbol:
                raise ValueError(
                    "Configured symbols cannot be empty."
                )

            if not normalized_symbol.isalnum():
                raise ValueError(
                    f"Invalid Binance symbol: {symbol!r}"
                )

            if normalized_symbol not in seen:
                normalized_symbols.append(normalized_symbol)
                seen.add(normalized_symbol)

        return tuple(normalized_symbols)

    def _build_websocket_url(self) -> str:
        """
        Build a Binance WebSocket URL.

        A raw stream is used for one symbol.

        A combined stream is used for multiple symbols.
        """
        streams = [
            f"{symbol}@trade"
            for symbol in self.symbols
        ]

        if len(streams) == 1:
            return (
                f"{self.websocket_base_url}"
                f"/ws/{streams[0]}"
            )

        combined_streams = "/".join(streams)

        return (
            f"{self.websocket_base_url}"
            f"/stream?streams={combined_streams}"
        )

    def _reject(
        self,
        reason: str,
        *,
        trade_id: Any = None,
        details: Any = None,
        log_level: int = logging.DEBUG,
    ) -> None:
        """
        Register a rejected message with a standardized reason.
        """
        self.messages_rejected += 1
        self.rejection_reasons[reason] += 1

        logger.log(
            log_level,
            "Trade rejected: reason=%s trade_id=%r details=%r",
            reason,
            trade_id,
            details,
        )

    @staticmethod
    def _parse_positive_decimal(
        value: Any,
        field_name: str,
    ) -> Decimal:
        """
        Convert a value to a strictly positive Decimal.
        """
        if value is None:
            raise ValueError(f"{field_name} is missing")

        if isinstance(value, bool):
            raise ValueError(
                f"{field_name} cannot be a boolean"
            )

        try:
            decimal_value = Decimal(str(value))
        except (InvalidOperation, ValueError, TypeError) as exc:
            raise ValueError(
                f"{field_name} is not numeric"
            ) from exc

        if not decimal_value.is_finite():
            raise ValueError(
                f"{field_name} must be finite"
            )

        if decimal_value <= 0:
            raise ValueError(
                f"{field_name} must be greater than zero"
            )

        return decimal_value

    @staticmethod
    def _parse_trade_id(value: Any) -> int:
        """
        Validate and normalize a Binance trade identifier.
        """
        if value is None or isinstance(value, bool):
            raise ValueError("trade_id is missing or invalid")

        try:
            trade_id = int(value)
        except (ValueError, TypeError) as exc:
            raise ValueError(
                "trade_id must be an integer"
            ) from exc

        if trade_id < 0:
            raise ValueError(
                "trade_id must be greater than or equal to zero"
            )

        return trade_id

    @staticmethod
    def _parse_timestamp_ms(value: Any) -> datetime:
        """
        Convert a Unix timestamp expressed in milliseconds to UTC.
        """
        if value is None or isinstance(value, bool):
            raise ValueError(
                "timestamp is missing or invalid"
            )

        try:
            timestamp_ms = int(value)
        except (ValueError, TypeError) as exc:
            raise ValueError(
                "timestamp must be an integer"
            ) from exc

        if timestamp_ms <= 0:
            raise ValueError(
                "timestamp must be greater than zero"
            )

        try:
            trade_datetime = datetime.fromtimestamp(
                timestamp_ms / 1_000,
                tz=timezone.utc,
            )
        except (OverflowError, OSError, ValueError) as exc:
            raise ValueError(
                "timestamp is outside the supported range"
            ) from exc

        return trade_datetime

    @staticmethod
    def _unwrap_payload(
        payload: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        """
        Extract the trade object from a raw or combined Binance payload.

        Raw stream payload:
            {"e": "trade", "s": "BTCUSDT", ...}

        Combined stream payload:
            {
                "stream": "btcusdt@trade",
                "data": {
                    "e": "trade",
                    "s": "BTCUSDT",
                    ...
                }
            }
        """
        nested_data = payload.get("data")

        if isinstance(nested_data, Mapping):
            return nested_data

        return payload

    async def _invoke_callback(
        self,
        trade_record: TradeRecord,
    ) -> None:
        """
        Invoke a synchronous or asynchronous callback safely.
        """
        if self.callback is None:
            return

        try:
            callback_result = self.callback(trade_record)

            if inspect.isawaitable(callback_result):
                if self.callback_timeout_seconds is None:
                    await callback_result
                else:
                    await asyncio.wait_for(
                        callback_result,
                        timeout=self.callback_timeout_seconds,
                    )

        except asyncio.CancelledError:
            raise

        except asyncio.TimeoutError:
            self.callback_errors += 1

            logger.exception(
                "Trade callback timed out: symbol=%s trade_id=%s",
                trade_record["symbol"],
                trade_record["trade_id"],
            )

        except Exception:
            self.callback_errors += 1

            logger.exception(
                "Trade callback failed: symbol=%s trade_id=%s",
                trade_record["symbol"],
                trade_record["trade_id"],
            )

    async def _process_trade(
        self,
        payload: Mapping[str, Any],
    ) -> Optional"""
        Validate and normalize a Binance trade payload.

        Returns
        -------
        Optional[TradeRecord]
            The normalized trade when valid, otherwise None.
        """
        trade = self._unwrap_payload(payload)

        self.messages_received += 1

        event_type = trade.get("e")
        trade_id_raw = trade.get("t")

        if event_type != "trade":
            self._reject(
                "unexpected_event_type",
                trade_id=trade_id_raw,
                details=event_type,
            )
            return None

        symbol_raw = trade.get("s")

        if not isinstance(symbol_raw, str):
            self._reject(
                "missing_or_invalid_symbol",
                trade_id=trade_id_raw,
                details=symbol_raw,
            )
            return None

        symbol = symbol_raw.strip().upper()

        if symbol not in self._tracked_symbols:
            self._reject(
                "untracked_symbol",
                trade_id=trade_id_raw,
                details=symbol,
            )
            return None

        try:
            trade_id = self._parse_trade_id(trade_id_raw)

        except ValueError as exc:
            self._reject(
                "invalid_trade_id",
                trade_id=trade_id_raw,
                details=str(exc),
            )
            return None

        try:
            price = self._parse_positive_decimal(
                trade.get("p"),
                "price",
            )

        except ValueError as exc:
            self._reject(
                "invalid_price",
                trade_id=trade_id,
                details=str(exc),
            )
            return None

        try:
            quantity = self._parse_positive_decimal(
                trade.get("q"),
                "quantity",
            )

        except ValueError as exc:
            self._reject(
                "invalid_quantity",
                trade_id=trade_id,
                details=str(exc),
            )
            return None

        try:
            trade_timestamp = self._parse_timestamp_ms(
                trade.get("T")
            )

        except ValueError as exc:
            self._reject(
                "invalid_timestamp",
                trade_id=trade_id,
                details=str(exc),
            )
            return None

        received_at = datetime.now(timezone.utc)

        trade_record: TradeRecord = {
            "symbol": symbol,
            "trade_id": trade_id,
            "price": price,
            "quantity": quantity,
            "timestamp": trade_timestamp,
            "timestamp_ms": int(trade["T"]),
            "received_at": received_at,
            "is_buyer_market_maker": trade.get("m"),
        }

        self.messages_valid += 1
        self.last_valid_trade_at = received_at

        if self._store_received_trades:
            self.received_trades.append(trade_record)

        await self._invoke_callback(trade_record)

        return trade_record

    async def _consume_websocket(
        self,
        websocket: ClientWebSocketResponse,
    ) -> None:
        """
        Consume WebSocket messages until closure or stop request.
        """
        async for message in websocket:
            if self._stop_event.is_set():
                break

            self.websocket_messages_received += 1
            self.last_message_at = datetime.now(timezone.utc)

            if message.type == WSMsgType.TEXT:
                try:
                    payload = json.loads(message.data)

                except json.JSONDecodeError:
                    self.json_decode_errors += 1

                    self._reject(
                        "invalid_json",
                        details=message.data[:200],
                        log_level=logging.WARNING,
                    )
                    continue

                if not isinstance(payload, Mapping):
                    self._reject(
                        "unexpected_payload_type",
                        details=type(payload).__name__,
                    )
                    continue

                await self._process_trade(payload)

            elif message.type == WSMsgType.BINARY:
                self._reject(
                    "unexpected_binary_message",
                    details=f"{len(message.data)} bytes",
                )

            elif message.type == WSMsgType.ERROR:
                exception = websocket.exception()

                logger.error(
                    "WebSocket error: %r",
                    exception,
                )
                break

            elif message.type in {
                WSMsgType.CLOSE,
                WSMsgType.CLOSED,
                WSMsgType.CLOSING,
            }:
                logger.info(
                    "WebSocket closed: close_code=%s",
                    websocket.close_code,
                )
                break

    async def _connect_once(
        self,
        session: aiohttp.ClientSession,
        websocket_url: str,
    ) -> None:
        """
        Establish one WebSocket connection and consume messages.
        """
        self.connection_attempts += 1

        logger.info(
            "Connecting to Binance WebSocket: attempt=%d symbols=%s",
            self.connection_attempts,
            ",".join(self.symbols),
        )

        async with session.ws_connect(
            websocket_url,
            timeout=self.connect_timeout_seconds,
            autoping=True,
            autoclose=True,
            max_msg_size=1_048_576,
        ) as websocket:
            self._websocket = websocket
            self.connected = True
            self.successful_connections += 1

            logger.info(
                "Binance WebSocket connection established: symbols=%s",
                ",".join(self.symbols),
            )

            try:
                await self._consume_websocket(websocket)

            finally:
                self.connected = False
                self._websocket = None

    async def start(self) -> None:
        """
        Start the extractor and reconnect automatically when necessary.

        This method continues running until `stop()` is called or the
        task is cancelled.
        """
        if self.running:
            logger.warning(
                "Binance WebSocket extractor is already running."
            )
            return

        self.running = True
        self.connected = False
        self.started_at = datetime.now(timezone.utc)
        self.stopped_at = None

        self._stop_event.clear()

        websocket_url = self._build_websocket_url()
        reconnect_delay = self.reconnect_initial_delay_seconds

        client_timeout = aiohttp.ClientTimeout(
            total=None,
            sock_connect=self.connect_timeout_seconds,
            sock_read=None,
        )

        logger.info(
            "Starting Binance extractor: symbols=%s url=%s",
            ",".join(self.symbols),
            websocket_url,
        )

        try:
            async with aiohttp.ClientSession(
                timeout=client_timeout,
                raise_for_status=False,
            ) as session:
                while not self._stop_event.is_set():
                    try:
                        connection_started_at = (
                            asyncio.get_running_loop().time()
                        )

                        await self._connect_once(
                            session,
                            websocket_url,
                        )

                        connection_duration = (
                            asyncio.get_running_loop().time()
                            - connection_started_at
                        )

                        if self._stop_event.is_set():
                            break

                        if connection_duration >= 30:
                            reconnect_delay = (
                                self.reconnect_initial_delay_seconds
                            )

                    except asyncio.CancelledError:
                        logger.info(
                            "Binance extractor task cancelled."
                        )
                        raise

                    except (
                        aiohttp.ClientConnectionError,
                        aiohttp.ClientError,
                        asyncio.TimeoutError,
                    ) as exc:
                        logger.warning(
                            "Recoverable WebSocket failure: "
                            "error_type=%s error=%s",
                            type(exc).__name__,
                            exc,
                        )

                    except Exception:
                        logger.exception(
                            "Unexpected Binance WebSocket failure."
                        )

                    if self._stop_event.is_set():
                        break

                    self.reconnect_count += 1

                    jitter = random.uniform(
                        0,
                        min(1.0, reconnect_delay * 0.1),
                    )

                    sleep_duration = reconnect_delay + jitter

                    logger.info(
                        "Reconnecting to Binance in %.2f seconds: "
                        "reconnect_count=%d",
                        sleep_duration,
                        self.reconnect_count,
                    )

                    try:
                        await asyncio.wait_for(
                            self._stop_event.wait(),
                            timeout=sleep_duration,
                        )

                    except asyncio.TimeoutError:
                        pass

                    reconnect_delay = min(
                        reconnect_delay * 2,
                        self.reconnect_max_delay_seconds,
                    )

        finally:
            self.connected = False
            self.running = False
            self.stopped_at = datetime.now(timezone.utc)

            await self._close_active_websocket()

            logger.info(
                "Binance extractor stopped: "
                "received=%d valid=%d rejected=%d "
                "connections=%d reconnects=%d "
                "callback_errors=%d rejection_reasons=%s",
                self.messages_received,
                self.messages_valid,
                self.messages_rejected,
                self.successful_connections,
                self.reconnect_count,
                self.callback_errors,
                dict(self.rejection_reasons),
            )

    async def _close_active_websocket(self) -> None:
        """
        Close the currently active WebSocket, when present.
        """
        websocket = self._websocket

        if websocket is not None and not websocket.closed:
            try:
                await websocket.close(
                    code=aiohttp.WSCloseCode.GOING_AWAY,
                    message=b"CryptoPulse extractor stopping",
                )

            except Exception:
                logger.exception(
                    "Error while closing Binance WebSocket."
                )

        self._websocket = None

    async def stop(self) -> None:
        """
        Request a graceful extractor shutdown.
        """
        if not self.running:
            return

        logger.info(
            "Stopping Binance WebSocket extractor."
        )

        self._stop_event.set()
        await self._close_active_websocket()

    def get_stats(self) -> dict[str, Any]:
        """
        Return a snapshot of extractor statistics.
        """
        total_processed = (
            self.messages_valid
            + self.messages_rejected
        )

        validity_rate = (
            self.messages_valid / total_processed * 100
            if total_processed
            else 0.0
        )

        return {
            "running": self.running,
            "connected": self.connected,
            "symbols": list(self.symbols),
            "started_at": self.started_at,
            "stopped_at": self.stopped_at,
            "last_message_at": self.last_message_at,
            "last_valid_trade_at": self.last_valid_trade_at,
            "websocket_messages_received": (
                self.websocket_messages_received
            ),
            "messages_received": self.messages_received,
            "messages_valid": self.messages_valid,
            "messages_rejected": self.messages_rejected,
            "validity_rate_pct": round(validity_rate, 4),
            "connection_attempts": self.connection_attempts,
            "successful_connections": (
                self.successful_connections
            ),
            "reconnect_count": self.reconnect_count,
            "json_decode_errors": self.json_decode_errors,
            "callback_errors": self.callback_errors,
            "buffer_size": len(self.received_trades),
            "buffer_capacity": self.received_trades.maxlen,
            "rejection_reasons": dict(
                self.rejection_reasons
            ),
        }

    def reset_stats(self) -> None:
        """
        Reset counters without modifying the connection state.

        This operation should generally be performed only when the
        extractor is stopped.
        """
        if self.running:
            raise RuntimeError(
                "Statistics cannot be reset while the extractor "
                "is running."
            )

        self.messages_received = 0
        self.messages_valid = 0
        self.messages_rejected = 0
        self.websocket_messages_received = 0

        self.connection_attempts = 0
        self.successful_connections = 0
        self.reconnect_count = 0
        self.callback_errors = 0
        self.json_decode_errors = 0

        self.rejection_reasons.clear()
        self.received_trades.clear()

        self.started_at = None
        self.stopped_at = None
        self.last_message_at = None
        self.last_valid_trade_at = None