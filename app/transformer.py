"""
CryptoPulse ETL - Transformer.

Transforms validated Binance trades into minute-level market snapshots.

Main responsibilities:
- validate normalized trades received from the extractor;
- group trades by symbol and UTC minute;
- calculate OHLC, average price, VWAP, volume and variation;
- handle out-of-order trades;
- reject duplicate and excessively late trades;
- finalize only completed minute windows;
- avoid storing raw trades in memory.
"""

from __future__ import annotations

import logging

from collections import Counter, defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Deque, Mapping, Optional


logger = logging.getLogger(__name__)


Snapshot = dict[str, Any]
TradeOrderKey = tuple[datetime, int]
BucketKey = tuple[str, datetime]


@dataclass(slots=True)
class MinuteBucket:
    """
    Aggregation state for one symbol and one UTC minute.

    The bucket stores only values required to calculate the final
    snapshot. Raw trades are not retained.
    """

    symbol: str
    minute_ts: datetime

    open_price: Decimal
    min_price: Decimal
    max_price: Decimal
    close_price: Decimal

    sum_price: Decimal
    sum_price_quantity: Decimal
    total_volume: Decimal

    trade_count: int

    first_order_key: TradeOrderKey
    last_order_key: TradeOrderKey

    first_trade_id: int
    last_trade_id: int

    first_trade_ts: datetime
    last_trade_ts: datetime

    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_trade(
        cls,
        *,
        symbol: str,
        minute_ts: datetime,
        trade_id: int,
        price: Decimal,
        quantity: Decimal,
        trade_ts: datetime,
        received_at: datetime,
    ) -> "MinuteBucket":
        """
        Create a new bucket from its first trade.
        """
        order_key = (trade_ts, trade_id)

        return cls(
            symbol=symbol,
            minute_ts=minute_ts,
            open_price=price,
            min_price=price,
            max_price=price,
            close_price=price,
            sum_price=price,
            sum_price_quantity=price * quantity,
            total_volume=quantity,
            trade_count=1,
            first_order_key=order_key,
            last_order_key=order_key,
            first_trade_id=trade_id,
            last_trade_id=trade_id,
            first_trade_ts=trade_ts,
            last_trade_ts=trade_ts,
            created_at=received_at,
            updated_at=received_at,
        )

    def add_trade(
        self,
        *,
        trade_id: int,
        price: Decimal,
        quantity: Decimal,
        trade_ts: datetime,
        received_at: datetime,
    ) -> None:
        """
        Add one trade to the bucket.

        Open and close prices are determined using event time and trade
        identifier, not network arrival order.
        """
        order_key = (trade_ts, trade_id)

        self.min_price = min(
            self.min_price,
            price,
        )

        self.max_price = max(
            self.max_price,
            price,
        )

        self.sum_price += price
        self.sum_price_quantity += price * quantity
        self.total_volume += quantity
        self.trade_count += 1

        if order_key < self.first_order_key:
            self.first_order_key = order_key
            self.open_price = price
            self.first_trade_id = trade_id
            self.first_trade_ts = trade_ts

        if order_key > self.last_order_key:
            self.last_order_key = order_key
            self.close_price = price
            self.last_trade_id = trade_id
            self.last_trade_ts = trade_ts

        self.updated_at = received_at

    def to_snapshot(
        self,
        *,
        finalized_at: datetime,
        is_partial: bool = False,
    ) -> Snapshot:
        """
        Convert the aggregation state into a snapshot.
        """
        self._validate_internal_state()

        decimal_trade_count = Decimal(
            self.trade_count
        )

        avg_price = (
            self.sum_price
            / decimal_trade_count
        )

        vwap_price = (
            self.sum_price_quantity
            / self.total_volume
        )

        variation_pct = (
            (self.close_price - self.open_price)
            / self.open_price
            * Decimal("100")
        )

        range_pct = (
            (self.max_price - self.min_price)
            / self.open_price
            * Decimal("100")
        )

        return {
            "symbol": self.symbol,
            "minute_ts": self.minute_ts,
            "avg_price": avg_price,
            "vwap_price": vwap_price,
            "min_price": self.min_price,
            "max_price": self.max_price,
            "open_price": self.open_price,
            "close_price": self.close_price,
            "total_volume": self.total_volume,
            "trade_count": self.trade_count,
            "variation_pct": variation_pct,
            "range_pct": range_pct,
            "first_trade_id": self.first_trade_id,
            "last_trade_id": self.last_trade_id,
            "first_trade_ts": self.first_trade_ts,
            "last_trade_ts": self.last_trade_ts,
            "bucket_created_at": self.created_at,
            "bucket_updated_at": self.updated_at,
            "finalized_at": finalized_at,
            "is_partial": is_partial,
        }

    def _validate_internal_state(self) -> None:
        """
        Validate the bucket before producing a snapshot.
        """
        if self.trade_count <= 0:
            raise RuntimeError(
                "A minute bucket must contain at least one trade."
            )

        if self.open_price <= 0:
            raise RuntimeError(
                "Open price must be greater than zero."
            )

        if self.close_price <= 0:
            raise RuntimeError(
                "Close price must be greater than zero."
            )

        if self.min_price <= 0:
            raise RuntimeError(
                "Minimum price must be greater than zero."
            )

        if self.max_price <= 0:
            raise RuntimeError(
                "Maximum price must be greater than zero."
            )

        if self.min_price > self.max_price:
            raise RuntimeError(
                "Minimum price cannot exceed maximum price."
            )

        if not self.min_price <= self.open_price <= self.max_price:
            raise RuntimeError(
                "Open price must be between minimum and maximum."
            )

        if not self.min_price <= self.close_price <= self.max_price:
            raise RuntimeError(
                "Close price must be between minimum and maximum."
            )

        if self.total_volume <= 0:
            raise RuntimeError(
                "Total volume must be greater than zero."
            )


class MinuteAggregator:
    """
    Incrementally aggregate trades into minute-level snapshots.

    Parameters
    ----------
    allowed_lateness_seconds:
        Number of seconds to wait after a minute ends before finalizing
        the bucket.

        Example:
        - 0 finalizes immediately after the minute ends;
        - 5 accepts trades arriving up to five seconds late.

    snapshot_history_size:
        Maximum number of finalized snapshots retained in memory.

        This history is intended only for monitoring, debugging and API
        health endpoints. It is not durable storage.

    deduplicate_trades:
        Reject duplicate trade identifiers while their minute bucket is
        open.
    """

    def __init__(
        self,
        *,
        allowed_lateness_seconds: int = 5,
        snapshot_history_size: int = 1_000,
        deduplicate_trades: bool = True,
    ) -> None:
        self._validate_configuration(
            allowed_lateness_seconds,
            snapshot_history_size,
        )

        self.allowed_lateness = timedelta(
            seconds=allowed_lateness_seconds
        )

        self.deduplicate_trades = deduplicate_trades

        self._buckets: dict[
            str,
            dict[datetime, MinuteBucket],
        ] = defaultdict(dict)

        self._seen_trade_ids: dict[
            BucketKey,
            set[int],
        ] = defaultdict(set)

        self._completed_snapshots: Deque[Snapshot] = deque(
            maxlen=(
                snapshot_history_size
                if snapshot_history_size > 0
                else None
            )
        )

        self._history_enabled = (
            snapshot_history_size > 0
        )

        self._finalized_until: dict[
            str,
            datetime,
        ] = {}

        self._max_event_time: dict[
            str,
            datetime,
        ] = {}

        self.trades_received = 0
        self.trades_accepted = 0
        self.trades_rejected = 0

        self.duplicate_trades = 0
        self.late_trades = 0
        self.snapshots_created = 0
        self.partial_snapshots_created = 0

        self.rejection_reasons: Counter[str] = Counter()

        self.last_trade_at: Optional[datetime] = None
        self.last_snapshot_at: Optional[datetime] = None

    @staticmethod
    def _validate_configuration(
        allowed_lateness_seconds: int,
        snapshot_history_size: int,
    ) -> None:
        """
        Validate aggregator configuration.
        """
        if isinstance(
            allowed_lateness_seconds,
            bool,
        ):
            raise ValueError(
                "allowed_lateness_seconds must be an integer."
            )

        if not isinstance(
            allowed_lateness_seconds,
            int,
        ):
            raise TypeError(
                "allowed_lateness_seconds must be an integer."
            )

        if allowed_lateness_seconds < 0:
            raise ValueError(
                "allowed_lateness_seconds cannot be negative."
            )

        if isinstance(
            snapshot_history_size,
            bool,
        ):
            raise ValueError(
                "snapshot_history_size must be an integer."
            )

        if not isinstance(
            snapshot_history_size,
            int,
        ):
            raise TypeError(
                "snapshot_history_size must be an integer."
            )

        if snapshot_history_size < 0:
            raise ValueError(
                "snapshot_history_size cannot be negative."
            )

    @staticmethod
    def _utc_now() -> datetime:
        """
        Return a timezone-aware UTC datetime.
        """
        return datetime.now(timezone.utc)

    @staticmethod
    def _normalize_symbol(
        value: Any,
    ) -> str:
        """
        Normalize and validate a symbol.
        """
        if not isinstance(value, str):
            raise ValueError(
                "symbol must be a string"
            )

        symbol = value.strip().upper()

        if not symbol:
            raise ValueError(
                "symbol cannot be empty"
            )

        if not symbol.isalnum():
            raise ValueError(
                "symbol must be alphanumeric"
            )

        if len(symbol) > 20:
            raise ValueError(
                "symbol cannot exceed 20 characters"
            )

        return symbol

    @staticmethod
    def _normalize_decimal(
        value: Any,
        field_name: str,
        *,
        positive: bool = False,
    ) -> Decimal:
        """
        Normalize a numeric value to a finite Decimal.
        """
        if value is None:
            raise ValueError(
                f"{field_name} is missing"
            )

        if isinstance(value, bool):
            raise ValueError(
                f"{field_name} cannot be a boolean"
            )

        try:
            normalized_value = Decimal(
                str(value)
            )

        except (
            InvalidOperation,
            ValueError,
            TypeError,
        ) as exc:
            raise ValueError(
                f"{field_name} must be numeric"
            ) from exc

        if not normalized_value.is_finite():
            raise ValueError(
                f"{field_name} must be finite"
            )

        if positive and normalized_value <= 0:
            raise ValueError(
                f"{field_name} must be greater than zero"
            )

        return normalized_value

    @staticmethod
    def _normalize_trade_id(
        value: Any,
    ) -> int:
        """
        Normalize and validate a trade identifier.
        """
        if value is None or isinstance(value, bool):
            raise ValueError(
                "trade_id is missing or invalid"
            )

        try:
            trade_id = int(value)

        except (
            TypeError,
            ValueError,
        ) as exc:
            raise ValueError(
                "trade_id must be an integer"
            ) from exc

        if trade_id < 0:
            raise ValueError(
                "trade_id cannot be negative"
            )

        return trade_id

    @staticmethod
    def _normalize_datetime(
        value: Any,
        field_name: str,
    ) -> datetime:
        """
        Normalize a datetime or Unix timestamp to UTC.

        Supported inputs:
        - timezone-aware datetime;
        - naive datetime, interpreted as UTC;
        - Unix timestamp in seconds;
        - Unix timestamp in milliseconds;
        - numeric timestamp stored as a string.
        """
        if isinstance(value, datetime):
            if value.tzinfo is None:
                logger.warning(
                    "%s has no timezone; UTC is assumed.",
                    field_name,
                )

                return value.replace(
                    tzinfo=timezone.utc
                )

            return value.astimezone(
                timezone.utc
            )

        if value is None or isinstance(value, bool):
            raise ValueError(
                f"{field_name} is missing or invalid"
            )

        try:
            numeric_timestamp = Decimal(
                str(value)
            )

        except (
            InvalidOperation,
            TypeError,
            ValueError,
        ) as exc:
            raise ValueError(
                f"{field_name} must be a datetime "
                "or a Unix timestamp"
            ) from exc

        if not numeric_timestamp.is_finite():
            raise ValueError(
                f"{field_name} must be finite"
            )

        if numeric_timestamp <= 0:
            raise ValueError(
                f"{field_name} must be greater than zero"
            )

        milliseconds_threshold = Decimal(
            "100000000000"
        )

        if numeric_timestamp >= milliseconds_threshold:
            numeric_timestamp /= Decimal("1000")

        try:
            return datetime.fromtimestamp(
                float(numeric_timestamp),
                tz=timezone.utc,
            )

        except (
            OverflowError,
            OSError,
            ValueError,
        ) as exc:
            raise ValueError(
                f"{field_name} is outside the supported range"
            ) from exc

    @staticmethod
    def _minute_start(
        value: datetime,
    ) -> datetime:
        """
        Return the UTC start of a minute.
        """
        return value.astimezone(
            timezone.utc
        ).replace(
            second=0,
            microsecond=0,
        )

    def _normalize_trade(
        self,
        trade: Mapping[str, Any],
    ) -> dict[str, Any]:
        """
        Validate and normalize an extractor trade.
        """
        if not isinstance(trade, Mapping):
            raise TypeError(
                "trade must be a mapping"
            )

        symbol = self._normalize_symbol(
            trade.get("symbol")
        )

        trade_id = self._normalize_trade_id(
            trade.get("trade_id")
        )

        price = self._normalize_decimal(
            trade.get("price"),
            "price",
            positive=True,
        )

        quantity = self._normalize_decimal(
            trade.get("quantity"),
            "quantity",
            positive=True,
        )

        trade_ts = self._normalize_datetime(
            trade.get("timestamp"),
            "timestamp",
        )

        received_at_value = trade.get(
            "received_at"
        )

        received_at = (
            self._normalize_datetime(
                received_at_value,
                "received_at",
            )
            if received_at_value is not None
            else self._utc_now()
        )

        minute_ts = self._minute_start(
            trade_ts
        )

        return {
            "symbol": symbol,
            "trade_id": trade_id,
            "price": price,
            "quantity": quantity,
            "trade_ts": trade_ts,
            "minute_ts": minute_ts,
            "received_at": received_at,
        }

    def _reject(
        self,
        reason: str,
        *,
        symbol: Any = None,
        trade_id: Any = None,
        details: Any = None,
    ) -> None:
        """
        Register a transformation rejection.
        """
        self.trades_rejected += 1
        self.rejection_reasons[reason] += 1

        logger.debug(
            "Transformer rejected trade: "
            "reason=%s symbol=%r trade_id=%r details=%r",
            reason,
            symbol,
            trade_id,
            details,
        )

    def add_trade(
        self,
        trade: Mapping[str, Any],
    ) -> list"""
        Add one trade and return newly completed snapshots.

        Usually, the returned list is empty. When a newer minute is
        observed, older eligible buckets can be finalized.
        """
        self.trades_received += 1

        try:
            normalized = self._normalize_trade(
                trade
            )

        except (
            TypeError,
            ValueError,
        ) as exc:
            symbol = (
                trade.get("symbol")
                if isinstance(trade, Mapping)
                else None
            )

            trade_id = (
                trade.get("trade_id")
                if isinstance(trade, Mapping)
                else None
            )

            self._reject(
                "invalid_trade",
                symbol=symbol,
                trade_id=trade_id,
                details=str(exc),
            )

            return []

        symbol = normalized["symbol"]
        trade_id = normalized["trade_id"]
        minute_ts = normalized["minute_ts"]
        trade_ts = normalized["trade_ts"]

        finalized_until = self._finalized_until.get(
            symbol
        )

        if (
            finalized_until is not None
            and minute_ts <= finalized_until
        ):
            self.late_trades += 1

            self._reject(
                "trade_for_finalized_minute",
                symbol=symbol,
                trade_id=trade_id,
                details=minute_ts.isoformat(),
            )

            return []

        bucket_key: BucketKey = (
            symbol,
            minute_ts,
        )

        if self.deduplicate_trades:
            seen_trade_ids = self._seen_trade_ids[
                bucket_key
            ]

            if trade_id in seen_trade_ids:
                self.duplicate_trades += 1

                self._reject(
                    "duplicate_trade_id",
                    symbol=symbol,
                    trade_id=trade_id,
                    details=minute_ts.isoformat(),
                )

                return []

            seen_trade_ids.add(trade_id)

        bucket = self._buckets[
            symbol
        ].get(minute_ts)

        if bucket is None:
            bucket = MinuteBucket.from_trade(
                symbol=symbol,
                minute_ts=minute_ts,
                trade_id=trade_id,
                price=normalized["price"],
                quantity=normalized["quantity"],
                trade_ts=trade_ts,
                received_at=normalized["received_at"],
            )

            self._buckets[
                symbol
            ][minute_ts] = bucket

        else:
            bucket.add_trade(
                trade_id=trade_id,
                price=normalized["price"],
                quantity=normalized["quantity"],
                trade_ts=trade_ts,
                received_at=normalized["received_at"],
            )

        previous_max_event_time = (
            self._max_event_time.get(symbol)
        )

        if (
            previous_max_event_time is None
            or trade_ts > previous_max_event_time
        ):
            self._max_event_time[
                symbol
            ] = trade_ts

        self.trades_accepted += 1
        self.last_trade_at = normalized["received_at"]

        return self.flush_completed(
            watermark=self._max_event_time[symbol],
            symbol=symbol,
        )

    def flush_completed(
        self,
        *,
        watermark: Optional[datetime] = None,
        symbol: Optional[str] = None,
    ) -> list"""
        Finalize completed buckets.

        A bucket is complete when:

            minute start
            + one minute
            + allowed lateness
            <= watermark

        When no watermark is provided, the current UTC processing time
        is used.
        """
        normalized_watermark = (
            self._normalize_datetime(
                watermark,
                "watermark",
            )
            if watermark is not None
            else self._utc_now()
        )

        if symbol is not None:
            symbols = [
                self._normalize_symbol(symbol)
            ]
        else:
            symbols = list(
                self._buckets.keys()
            )

        completed_snapshots: list[Snapshot] = []

        for current_symbol in symbols:
            symbol_buckets = self._buckets.get(
                current_symbol
            )

            if not symbol_buckets:
                continue

            completed_minutes = [
                minute_ts
                for minute_ts in symbol_buckets
                if (
                    minute_ts
                    + timedelta(minutes=1)
                    + self.allowed_lateness
                    <= normalized_watermark
                )
            ]

            for minute_ts in sorted(
                completed_minutes
            ):
                snapshot = self._finalize_bucket(
                    symbol=current_symbol,
                    minute_ts=minute_ts,
                    finalized_at=normalized_watermark,
                    is_partial=False,
                )

                completed_snapshots.append(
                    snapshot
                )

        return completed_snapshots

    def _finalize_bucket(
        self,
        *,
        symbol: str,
        minute_ts: datetime,
        finalized_at: datetime,
        is_partial: bool,
    ) -> Snapshot:
        """
        Finalize and remove one minute bucket.
        """
        symbol_buckets = self._buckets.get(
            symbol
        )

        if not symbol_buckets:
            raise KeyError(
                f"No open buckets exist for symbol {symbol}."
            )

        bucket = symbol_buckets.pop(
            minute_ts
        )

        self._seen_trade_ids.pop(
            (symbol, minute_ts),
            None,
        )

        snapshot = bucket.to_snapshot(
            finalized_at=finalized_at,
            is_partial=is_partial,
        )

        previous_finalized = self._finalized_until.get(
            symbol
        )

        if (
            previous_finalized is None
            or minute_ts > previous_finalized
        ):
            self._finalized_until[
                symbol
            ] = minute_ts

        self.snapshots_created += 1

        if is_partial:
            self.partial_snapshots_created += 1

        self.last_snapshot_at = finalized_at

        if self._history_enabled:
            self._completed_snapshots.append(
                snapshot.copy()
            )

        if not symbol_buckets:
            self._buckets.pop(
                symbol,
                None,
            )

        logger.info(
            "Minute snapshot finalized: "
            "symbol=%s minute_ts=%s trade_count=%d "
            "volume=%s variation_pct=%s partial=%s",
            symbol,
            minute_ts.isoformat(),
            snapshot["trade_count"],
            snapshot["total_volume"],
            snapshot["variation_pct"],
            is_partial,
        )

        return snapshot

    def flush_all(
        self,
        *,
        finalized_at: Optional[datetime] = None,
        include_partial: bool = False,
    ) -> list"""
        Finalize all eligible buckets during graceful shutdown.

        Parameters
        ----------
        finalized_at:
            Shutdown time. Current UTC time is used when omitted.

        include_partial:
            When false, the current incomplete minute remains excluded.

            When true, all buckets are returned and incomplete minutes
            are marked with `is_partial=True`.
        """
        shutdown_time = (
            self._normalize_datetime(
                finalized_at,
                "finalized_at",
            )
            if finalized_at is not None
            else self._utc_now()
        )

        snapshots: list[Snapshot] = []

        for symbol in sorted(
            list(self._buckets.keys())
        ):
            minute_keys = sorted(
                list(self._buckets[symbol].keys())
            )

            for minute_ts in minute_keys:
                bucket_end = (
                    minute_ts
                    + timedelta(minutes=1)
                )

                is_partial = (
                    bucket_end > shutdown_time
                )

                if is_partial and not include_partial:
                    continue

                snapshot = self._finalize_bucket(
                    symbol=symbol,
                    minute_ts=minute_ts,
                    finalized_at=shutdown_time,
                    is_partial=is_partial,
                )

                snapshots.append(snapshot)

        return snapshots

    def discard_open_buckets(self) -> int:
        """
        Discard all remaining open buckets.

        This can be used after persisting every completed snapshot
        during shutdown when partial snapshots must not be stored.
        """
        discarded_count = self.get_open_bucket_count()

        self._buckets.clear()
        self._seen_trade_ids.clear()
        self._max_event_time.clear()

        if discarded_count:
            logger.warning(
                "Discarded open minute buckets: count=%d",
                discarded_count,
            )

        return discarded_count

    def get_open_snapshot(
        self,
        symbol: str,
        *,
        minute_ts: Optional[datetime] = None,
    ) -> Optional"""
        Return a non-destructive preview of an open bucket.
        """
        normalized_symbol = self._normalize_symbol(
            symbol
        )

        symbol_buckets = self._buckets.get(
            normalized_symbol
        )

        if not symbol_buckets:
            return None

        if minute_ts is None:
            selected_minute = max(
                symbol_buckets
            )

        else:
            normalized_datetime = (
                self._normalize_datetime(
                    minute_ts,
                    "minute_ts",
                )
            )

            selected_minute = self._minute_start(
                normalized_datetime
            )

        bucket = symbol_buckets.get(
            selected_minute
        )

        if bucket is None:
            return None

        preview = bucket.to_snapshot(
            finalized_at=self._utc_now(),
            is_partial=True,
        )

        preview["is_preview"] = True

        return preview

    def get_latest_snapshot(
        self,
        symbol: str,
        *,
        include_open: bool = False,
    ) -> Optional"""
        Return the latest snapshot without modifying aggregator state.
        """
        normalized_symbol = self._normalize_symbol(
            symbol
        )

        finalized_candidates = [
            snapshot
            for snapshot in self._completed_snapshots
            if snapshot["symbol"] == normalized_symbol
        ]

        latest_finalized = (
            max(
                finalized_candidates,
                key=lambda item: item["minute_ts"],
            )
            if finalized_candidates
            else None
        )

        if not include_open:
            return (
                latest_finalized.copy()
                if latest_finalized is not None
                else None
            )

        latest_open = self.get_open_snapshot(
            normalized_symbol
        )

        if latest_open is None:
            return (
                latest_finalized.copy()
                if latest_finalized is not None
                else None
            )

        if latest_finalized is None:
            return latest_open

        if (
            latest_open["minute_ts"]
            > latest_finalized["minute_ts"]
        ):
            return latest_open

        return latest_finalized.copy()

    def get_completed_snapshots(
        self,
        symbol: Optional[str] = None,
        *,
        limit: Optional[int] = None,
    ) -> list"""
        Return snapshots retained in bounded local history.
        """
        if symbol is None:
            snapshots = list(
                self._completed_snapshots
            )

        else:
            normalized_symbol = (
                self._normalize_symbol(symbol)
            )

            snapshots = [
                snapshot
                for snapshot in self._completed_snapshots
                if snapshot["symbol"] == normalized_symbol
            ]

        snapshots.sort(
            key=lambda item: (
                item["minute_ts"],
                item["symbol"],
            )
        )

        if limit is not None:
            if isinstance(limit, bool):
                raise ValueError(
                    "limit must be an integer."
                )

            if not isinstance(limit, int):
                raise TypeError(
                    "limit must be an integer."
                )

            if limit < 0:
                raise ValueError(
                    "limit cannot be negative."
                )

            if limit == 0:
                return []

            snapshots = snapshots[-limit:]

        return [
            snapshot.copy()
            for snapshot in snapshots
        ]

    def get_open_bucket_count(
        self,
        symbol: Optional[str] = None,
    ) -> int:
        """
        Return the number of open buckets.
        """
        if symbol is not None:
            normalized_symbol = (
                self._normalize_symbol(symbol)
            )

            return len(
                self._buckets.get(
                    normalized_symbol,
                    {},
                )
            )

        return sum(
            len(symbol_buckets)
            for symbol_buckets in self._buckets.values()
        )

    def get_stats(self) -> dict[str, Any]:
        """
        Return transformer metrics for monitoring.
        """
        acceptance_rate = (
            self.trades_accepted
            / self.trades_received
            * 100
            if self.trades_received
            else 0.0
        )

        rejection_rate = (
            self.trades_rejected
            / self.trades_received
            * 100
            if self.trades_received
            else 0.0
        )

        return {
            "trades_received": self.trades_received,
            "trades_accepted": self.trades_accepted,
            "trades_rejected": self.trades_rejected,
            "acceptance_rate_pct": round(
                acceptance_rate,
                4,
            ),
            "rejection_rate_pct": round(
                rejection_rate,
                4,
            ),
            "duplicate_trades": self.duplicate_trades,
            "late_trades": self.late_trades,
            "snapshots_created": self.snapshots_created,
            "partial_snapshots_created": (
                self.partial_snapshots_created
            ),
            "open_bucket_count": (
                self.get_open_bucket_count()
            ),
            "open_buckets_by_symbol": {
                symbol: len(symbol_buckets)
                for symbol, symbol_buckets
                in self._buckets.items()
            },
            "history_size": len(
                self._completed_snapshots
            ),
            "history_capacity": (
                self._completed_snapshots.maxlen
            ),
            "allowed_lateness_seconds": (
                self.allowed_lateness.total_seconds()
            ),
            "last_trade_at": self.last_trade_at,
            "last_snapshot_at": self.last_snapshot_at,
            "max_event_time_by_symbol": dict(
                self._max_event_time
            ),
            "finalized_until_by_symbol": dict(
                self._finalized_until
            ),
            "rejection_reasons": dict(
                self.rejection_reasons
            ),
        }

    def reset(
        self,
        *,
        clear_history: bool = True,
        reset_statistics: bool = True,
    ) -> None:
        """
        Reset aggregator state.

        Call this method only when the pipeline is stopped.
        """
        self._buckets.clear()
        self._seen_trade_ids.clear()
        self._finalized_until.clear()
        self._max_event_time.clear()

        if clear_history:
            self._completed_snapshots.clear()

        if reset_statistics:
            self.trades_received = 0
            self.trades_accepted = 0
            self.trades_rejected = 0

            self.duplicate_trades = 0
            self.late_trades = 0
            self.snapshots_created = 0
            self.partial_snapshots_created = 0

            self.rejection_reasons.clear()

            self.last_trade_at = None
            self.last_snapshot_at = None
            