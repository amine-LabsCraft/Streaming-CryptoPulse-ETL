"""
CryptoPulse ETL - MySQL Loader.

Stores structured analytical data in MySQL:
- instrument dimensions;
- minute-level market snapshots;
- pipeline execution statistics;
- data-quality metrics.

Market events are not stored by this loader.
They belong to the MongoDB event store.
"""

from __future__ import annotations

import logging

from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Generator, Iterable, Mapping, Optional, Sequence

try:
    import pymysql
    from pymysql.connections import Connection
    from pymysql.cursors import DictCursor
except ImportError as exc:
    raise RuntimeError(
        "PyMySQL is required to use MySQLLoader. "
        "Install it with: pip install PyMySQL"
    ) from exc

from config.settings import MYSQL_CONFIG


logger = logging.getLogger(__name__)


class MySQLLoader:
    """
    Load structured CryptoPulse data into MySQL.

    The loader manages one reusable PyMySQL connection.

    Important
    ---------
    PyMySQL is synchronous. When this loader is called from an
    asynchronous pipeline, database operations should be executed
    through `asyncio.to_thread()` to avoid blocking the event loop.
    """

    ALLOWED_RUN_STATUSES = frozenset(
        {
            "STARTING",
            "RUNNING",
            "SUCCESS",
            "FAILED",
            "STOPPED",
            "PARTIAL",
        }
    )

    ALLOWED_SEVERITIES = frozenset(
        {
            "DEBUG",
            "INFO",
            "WARNING",
            "ERROR",
            "CRITICAL",
        }
    )

    ALLOWED_RUN_UPDATE_FIELDS = frozenset(
        {
            "finished_at",
            "status",
            "messages_received",
            "messages_valid",
            "messages_rejected",
            "snapshots_created",
            "events_created",
            "storage_used_mb",
            "error_message",
        }
    )

    REQUIRED_SNAPSHOT_FIELDS = frozenset(
        {
            "minute_ts",
            "avg_price",
            "vwap_price",
            "min_price",
            "max_price",
            "open_price",
            "close_price",
            "total_volume",
            "trade_count",
            "variation_pct",
            "range_pct",
            "is_partial",
        }
    )

    def __init__(
        self,
        connection_config: Optional[Mapping[str, Any]] = None,
    ) -> None:
        """
        Initialize the MySQL loader.

        Parameters
        ----------
        connection_config:
            Optional MySQL configuration.

            When omitted, `MYSQL_CONFIG` from settings is used.
        """
        self.connection_config = dict(
            connection_config or MYSQL_CONFIG
        )

        self._validate_connection_config()

        self._connection: Optional[Connection] = None

    def _validate_connection_config(self) -> None:
        """
        Validate required MySQL connection settings.
        """
        required_fields = {
            "host",
            "port",
            "user",
            "password",
            "database",
        }

        missing_fields = required_fields.difference(
            self.connection_config
        )

        if missing_fields:
            missing = ", ".join(sorted(missing_fields))

            raise ValueError(
                f"Missing MySQL configuration fields: {missing}"
            )

        port = self.connection_config["port"]

        if isinstance(port, bool):
            raise ValueError(
                "MySQL port must be an integer."
            )

        try:
            normalized_port = int(port)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "MySQL port must be an integer."
            ) from exc

        if not 1 <= normalized_port <= 65535:
            raise ValueError(
                "MySQL port must be between 1 and 65535."
            )

        self.connection_config["port"] = normalized_port

    def _create_connection(self) -> Connection:
        """
        Create a new MySQL connection.
        """
        logger.info(
            "Opening MySQL connection: host=%s port=%s database=%s",
            self.connection_config["host"],
            self.connection_config["port"],
            self.connection_config["database"],
        )

        try:
            connection = pymysql.connect(
                host=self.connection_config["host"],
                port=self.connection_config["port"],
                user=self.connection_config["user"],
                password=self.connection_config["password"],
                database=self.connection_config["database"],
                charset="utf8mb4",
                cursorclass=DictCursor,
                autocommit=False,
                connect_timeout=int(
                    self.connection_config.get(
                        "connect_timeout",
                        10,
                    )
                ),
                read_timeout=int(
                    self.connection_config.get(
                        "read_timeout",
                        30,
                    )
                ),
                write_timeout=int(
                    self.connection_config.get(
                        "write_timeout",
                        30,
                    )
                ),
                init_command="SET time_zone = '+00:00'",
            )

        except pymysql.MySQLError:
            logger.exception(
                "Unable to connect to MySQL: host=%s "
                "port=%s database=%s user=%s",
                self.connection_config["host"],
                self.connection_config["port"],
                self.connection_config["database"],
                self.connection_config["user"],
            )
            raise

        logger.info(
            "MySQL connection established successfully."
        )

        return connection

    @property
    def connection(self) -> Connection:
        """
        Return an active MySQL connection.

        The connection is created when necessary and checked with
        `ping()` before reuse.
        """
        if self._connection is None:
            self._connection = self._create_connection()
            return self._connection

        try:
            self._connection.ping(reconnect=True)

        except pymysql.MySQLError:
            logger.warning(
                "Existing MySQL connection is unavailable. "
                "Creating a new connection."
            )

            self.close()
            self._connection = self._create_connection()

        return self._connection

    @contextmanager
    def transaction(
        self,
    ) -> Generator[tuple[Connection, DictCursor], None, None]:
        """
        Open a database transaction.

        The transaction is committed when the block succeeds and
        rolled back when an exception occurs.
        """
        connection = self.connection

        try:
            with connection.cursor() as cursor:
                yield connection, cursor

            connection.commit()

        except Exception:
            try:
                connection.rollback()
            except pymysql.MySQLError:
                logger.exception(
                    "MySQL rollback failed."
                )

            raise

    def _execute_write(
        self,
        query: str,
        params: Optional[Sequence[Any]] = None,
        *,
        operation: str = "sql_write",
    ) -> int:
        """
        Execute one SQL write statement.

        Returns
        -------
        int
            Number of affected rows.
        """
        try:
            with self.transaction() as (_, cursor):
                affected_rows = cursor.execute(
                    query,
                    params,
                )

            return affected_rows

        except pymysql.MySQLError as exc:
            logger.exception(
                "MySQL write failed: operation=%s "
                "error_code=%s",
                operation,
                exc.args[0] if exc.args else None,
            )
            raise

    def _execute_many(
        self,
        query: str,
        params: Iterable[Sequence[Any]],
        *,
        operation: str = "sql_batch_write",
    ) -> int:
        """
        Execute a batch write in one transaction.
        """
        values = list(params)

        if not values:
            return 0

        try:
            with self.transaction() as (_, cursor):
                affected_rows = cursor.executemany(
                    query,
                    values,
                )

            return affected_rows

        except pymysql.MySQLError as exc:
            logger.exception(
                "MySQL batch write failed: operation=%s "
                "batch_size=%d error_code=%s",
                operation,
                len(values),
                exc.args[0] if exc.args else None,
            )
            raise

    def _fetch_one(
        self,
        query: str,
        params: Optional[Sequence[Any]] = None,
        *,
        operation: str = "sql_fetch_one",
    ) -> Optional[dict[str, Any]]:
        """
        Execute a query and return one row.
        """
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, params)
                row = cursor.fetchone()

            return row

        except pymysql.MySQLError as exc:
            logger.exception(
                "MySQL read failed: operation=%s "
                "error_code=%s",
                operation,
                exc.args[0] if exc.args else None,
            )
            raise

    def _fetch_all(
        self,
        query: str,
        params: Optional[Sequence[Any]] = None,
        *,
        operation: str = "sql_fetch_all",
    ) -> list[dict[str, Any]]:
        """
        Execute a query and return all rows.
        """
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, params)
                rows = cursor.fetchall()

            return list(rows)

        except pymysql.MySQLError as exc:
            logger.exception(
                "MySQL read failed: operation=%s "
                "error_code=%s",
                operation,
                exc.args[0] if exc.args else None,
            )
            raise

    def init_tables(self) -> None:
        """
        Create the CryptoPulse MySQL tables and indexes.
        """
        logger.info(
            "Initializing CryptoPulse MySQL tables."
        )

        schema_statements = [
            """
            CREATE TABLE IF NOT EXISTS dim_instrument (
                instrument_id BIGINT UNSIGNED
                    NOT NULL AUTO_INCREMENT,
                symbol VARCHAR(20) NOT NULL,
                base_asset VARCHAR(20) NOT NULL,
                quote_asset VARCHAR(20) NOT NULL,
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                created_at TIMESTAMP
                    NOT NULL DEFAULT CURRENT_TIMESTAMP,

                PRIMARY KEY (instrument_id),
                UNIQUE KEY uk_dim_instrument_symbol (symbol),

                CONSTRAINT chk_instrument_symbol_not_empty
                    CHECK (CHAR_LENGTH(TRIM(symbol)) > 0),

                CONSTRAINT chk_base_asset_not_empty
                    CHECK (CHAR_LENGTH(TRIM(base_asset)) > 0),

                CONSTRAINT chk_quote_asset_not_empty
                    CHECK (CHAR_LENGTH(TRIM(quote_asset)) > 0)
            )
            ENGINE=InnoDB
            DEFAULT CHARSET=utf8mb4
            COLLATE=utf8mb4_unicode_ci
            """,
            """
            CREATE TABLE IF NOT EXISTS fact_pipeline_run (
                run_id CHAR(36) NOT NULL,
                started_at DATETIME(6) NOT NULL,
                finished_at DATETIME(6) NULL,
                status VARCHAR(20) NOT NULL,

                messages_received BIGINT UNSIGNED
                    NOT NULL DEFAULT 0,

                messages_valid BIGINT UNSIGNED
                    NOT NULL DEFAULT 0,

                messages_rejected BIGINT UNSIGNED
                    NOT NULL DEFAULT 0,

                snapshots_created BIGINT UNSIGNED
                    NOT NULL DEFAULT 0,

                events_created BIGINT UNSIGNED
                    NOT NULL DEFAULT 0,

                storage_used_mb DECIMAL(14, 4)
                    NOT NULL DEFAULT 0,

                error_message VARCHAR(1000) NULL,

                created_at TIMESTAMP
                    NOT NULL DEFAULT CURRENT_TIMESTAMP,

                updated_at TIMESTAMP
                    NOT NULL DEFAULT CURRENT_TIMESTAMP
                    ON UPDATE CURRENT_TIMESTAMP,

                PRIMARY KEY (run_id),

                KEY idx_pipeline_run_started_at (started_at),
                KEY idx_pipeline_run_status (status),

                CONSTRAINT chk_pipeline_run_counters
                    CHECK (
                        messages_received >= 0
                        AND messages_valid >= 0
                        AND messages_rejected >= 0
                        AND snapshots_created >= 0
                        AND events_created >= 0
                    ),

                CONSTRAINT chk_pipeline_run_storage
                    CHECK (storage_used_mb >= 0),

                CONSTRAINT chk_pipeline_run_message_counts
                    CHECK (
                        messages_valid + messages_rejected
                        <= messages_received
                    )
            )
            ENGINE=InnoDB
            DEFAULT CHARSET=utf8mb4
            COLLATE=utf8mb4_unicode_ci
            """,
            """
            CREATE TABLE IF NOT EXISTS fact_market_minute (
                market_minute_id BIGINT UNSIGNED
                    NOT NULL AUTO_INCREMENT,

                instrument_id BIGINT UNSIGNED NOT NULL,
                minute_ts DATETIME(6) NOT NULL,

                avg_price DECIMAL(24, 10) NOT NULL,
                vwap_price DECIMAL(24, 10) NULL,
                min_price DECIMAL(24, 10) NOT NULL,
                max_price DECIMAL(24, 10) NOT NULL,
                open_price DECIMAL(24, 10) NOT NULL,
                close_price DECIMAL(24, 10) NOT NULL,

                total_volume DECIMAL(30, 12) NOT NULL,
                trade_count BIGINT UNSIGNED NOT NULL,
                variation_pct DECIMAL(18, 8) NOT NULL,
                range_pct DECIMAL(18, 8) NULL,
                is_partial BOOLEAN NOT NULL DEFAULT FALSE,

                snapshot_created_at TIMESTAMP
                    NOT NULL DEFAULT CURRENT_TIMESTAMP,

                snapshot_updated_at TIMESTAMP
                    NOT NULL DEFAULT CURRENT_TIMESTAMP
                    ON UPDATE CURRENT_TIMESTAMP,

                PRIMARY KEY (market_minute_id),

                UNIQUE KEY uk_market_minute_instrument_ts (
                    instrument_id,
                    minute_ts
                ),

                KEY idx_market_minute_ts (minute_ts),

                KEY idx_market_minute_instrument_ts (
                    instrument_id,
                    minute_ts
                ),

                CONSTRAINT fk_market_minute_instrument
                    FOREIGN KEY (instrument_id)
                    REFERENCES dim_instrument (instrument_id)
                    ON UPDATE RESTRICT
                    ON DELETE RESTRICT,

                CONSTRAINT chk_market_prices_positive
                    CHECK (
                        avg_price > 0
                        AND min_price > 0
                        AND max_price > 0
                        AND open_price > 0
                        AND close_price > 0
                    ),

                CONSTRAINT chk_market_price_order
                    CHECK (
                        min_price <= max_price
                        AND open_price
                            BETWEEN min_price AND max_price
                        AND close_price
                            BETWEEN min_price AND max_price
                        AND avg_price
                            BETWEEN min_price AND max_price
                    ),

                CONSTRAINT chk_market_volume_nonnegative
                    CHECK (total_volume >= 0),

                CONSTRAINT chk_market_trade_count
                    CHECK (trade_count > 0)
            )
            ENGINE=InnoDB
            DEFAULT CHARSET=utf8mb4
            COLLATE=utf8mb4_unicode_ci
            """,
            """
            CREATE TABLE IF NOT EXISTS fact_data_quality (
                issue_id BIGINT UNSIGNED
                    NOT NULL AUTO_INCREMENT,

                run_id CHAR(36) NULL,
                instrument_id BIGINT UNSIGNED NULL,

                issue_type VARCHAR(100) NOT NULL,
                field_name VARCHAR(100) NULL,
                severity VARCHAR(20) NOT NULL,
                detected_at DATETIME(6) NOT NULL,
                issue_count BIGINT UNSIGNED NOT NULL DEFAULT 1,
                example_value VARCHAR(500) NULL,
                details VARCHAR(1000) NULL,

                PRIMARY KEY (issue_id),

                KEY idx_data_quality_run_id (run_id),
                KEY idx_data_quality_instrument (
                    instrument_id
                ),
                KEY idx_data_quality_issue_type (
                    issue_type
                ),
                KEY idx_data_quality_detected_at (
                    detected_at
                ),

                CONSTRAINT fk_data_quality_run
                    FOREIGN KEY (run_id)
                    REFERENCES fact_pipeline_run (run_id)
                    ON UPDATE RESTRICT
                    ON DELETE SET NULL,

                CONSTRAINT fk_data_quality_instrument
                    FOREIGN KEY (instrument_id)
                    REFERENCES dim_instrument (instrument_id)
                    ON UPDATE RESTRICT
                    ON DELETE SET NULL,

                CONSTRAINT chk_data_quality_count
                    CHECK (issue_count > 0)
            )
            ENGINE=InnoDB
            DEFAULT CHARSET=utf8mb4
            COLLATE=utf8mb4_unicode_ci
            """,
        ]

        try:
            with self.transaction() as (_, cursor):
                for statement in schema_statements:
                    cursor.execute(statement)

        except pymysql.MySQLError:
            logger.exception(
                "Unable to initialize MySQL tables."
            )
            raise

        logger.info(
            "CryptoPulse MySQL tables initialized successfully."
        )

    @staticmethod
    def _normalize_symbol(symbol: str) -> str:
        """
        Normalize an instrument symbol.
        """
        if not isinstance(symbol, str):
            raise TypeError(
                "Instrument symbol must be a string."
            )

        normalized_symbol = symbol.strip().upper()

        if not normalized_symbol:
            raise ValueError(
                "Instrument symbol cannot be empty."
            )

        if not normalized_symbol.isalnum():
            raise ValueError(
                f"Invalid instrument symbol: {symbol!r}"
            )

        if len(normalized_symbol) > 20:
            raise ValueError(
                "Instrument symbol cannot exceed 20 characters."
            )

        return normalized_symbol

    @staticmethod
    def _normalize_asset(asset: str) -> str:
        """
        Normalize a base or quote asset.
        """
        if not isinstance(asset, str):
            raise TypeError(
                "Asset must be a string."
            )

        normalized_asset = asset.strip().upper()

        if not normalized_asset:
            raise ValueError(
                "Asset cannot be empty."
            )

        if not normalized_asset.isalnum():
            raise ValueError(
                f"Invalid asset: {asset!r}"
            )

        if len(normalized_asset) > 20:
            raise ValueError(
                "Asset cannot exceed 20 characters."
            )

        return normalized_asset

    def upsert_instrument(
        self,
        symbol: str,
        base_asset: str,
        quote_asset: str,
        *,
        is_active: bool = True,
    ) -> int:
        """
        Insert or update an instrument and return its identifier.
        """
        normalized_symbol = self._normalize_symbol(symbol)
        normalized_base = self._normalize_asset(base_asset)
        normalized_quote = self._normalize_asset(quote_asset)

        query = """
            INSERT INTO dim_instrument (
                symbol,
                base_asset,
                quote_asset,
                is_active
            )
            VALUES (%s, %s, %s, %s) AS new_instrument
            ON DUPLICATE KEY UPDATE
                base_asset = new_instrument.base_asset,
                quote_asset = new_instrument.quote_asset,
                is_active = new_instrument.is_active
        """

        self._execute_write(
            query,
            (
                normalized_symbol,
                normalized_base,
                normalized_quote,
                bool(is_active),
            ),
            operation="upsert_instrument",
        )

        instrument_id = self.get_instrument_id(
            normalized_symbol
        )

        if instrument_id is None:
            raise RuntimeError(
                "Instrument was upserted but its identifier "
                "could not be retrieved."
            )

        return instrument_id

    def get_instrument_id(
        self,
        symbol: str,
    ) -> Optional"""
        Return an instrument identifier from its symbol.
        """
        normalized_symbol = self._normalize_symbol(symbol)

        row = self._fetch_one(
            """
            SELECT instrument_id
            FROM dim_instrument
            WHERE symbol = %s
            LIMIT 1
            """,
            (normalized_symbol,),
            operation="get_instrument_id",
        )

        if row is None:
            return None

        return int(row["instrument_id"])

    def seed_default_instruments(self) -> dict[str, int]:
        """
        Insert the initial CryptoPulse instruments.
        """
        instruments = [
            ("BTCUSDT", "BTC", "USDT"),
            ("ETHUSDT", "ETH", "USDT"),
        ]

        result: dict[str, int] = {}

        for symbol, base_asset, quote_asset in instruments:
            result[symbol] = self.upsert_instrument(
                symbol=symbol,
                base_asset=base_asset,
                quote_asset=quote_asset,
            )

        logger.info(
            "Default instruments initialized: %s",
            ",".join(result),
        )

        return result

    @staticmethod
    def _to_decimal(
        value: Any,
        field_name: str,
        *,
        positive: bool = False,
        nonnegative: bool = False,
    ) -> Decimal:
        """
        Convert a value into a finite Decimal.
        """
        if value is None or isinstance(value, bool):
            raise ValueError(
                f"{field_name} is missing or invalid."
            )

        try:
            decimal_value = Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise ValueError(
                f"{field_name} must be numeric."
            ) from exc

        if not decimal_value.is_finite():
            raise ValueError(
                f"{field_name} must be finite."
            )

        if positive and decimal_value <= 0:
            raise ValueError(
                f"{field_name} must be greater than zero."
            )

        if nonnegative and decimal_value < 0:
            raise ValueError(
                f"{field_name} cannot be negative."
            )

        return decimal_value

    @staticmethod
    def _normalize_utc_datetime(
        value: Any,
        field_name: str,
    ) -> datetime:
        """
        Normalize a datetime to a naive UTC datetime for MySQL.

        MySQL DATETIME does not store timezone information. CryptoPulse
        therefore stores a normalized UTC value and documents that all
        DATETIME columns use UTC.
        """
        if not isinstance(value, datetime):
            raise TypeError(
                f"{field_name} must be a datetime."
            )

        if value.tzinfo is None:
            logger.warning(
                "%s has no timezone. It is assumed to be UTC.",
                field_name,
            )

            return value.replace(microsecond=value.microsecond)

        utc_value = value.astimezone(timezone.utc)

        return utc_value.replace(tzinfo=None)

    def _validate_snapshot(
        self,
        snapshot: Mapping[str, Any],
        instrument_id: int,
    ) -> dict[str, Any]:
        """
        Validate and normalize a minute snapshot.
        """
        if not isinstance(snapshot, Mapping):
            raise TypeError(
                "snapshot must be a mapping."
            )

        missing_fields = self.REQUIRED_SNAPSHOT_FIELDS.difference(
            snapshot
        )

        if missing_fields:
            missing = ", ".join(sorted(missing_fields))

            raise ValueError(
                f"Snapshot is missing required fields: {missing}"
            )

        if isinstance(instrument_id, bool):
            raise ValueError(
                "instrument_id must be a positive integer."
            )

        try:
            normalized_instrument_id = int(instrument_id)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "instrument_id must be a positive integer."
            ) from exc

        if normalized_instrument_id <= 0:
            raise ValueError(
                "instrument_id must be greater than zero."
            )

        minute_ts = self._normalize_utc_datetime(
            snapshot["minute_ts"],
            "minute_ts",
        )

        avg_price = self._to_decimal(
            snapshot["avg_price"],
            "avg_price",
            positive=True,
        )

        vwap_price = self._to_decimal(
            snapshot["vwap_price"],
            "vwap_price",
            positive=True,
        ) if snapshot.get("vwap_price") is not None else None

        min_price = self._to_decimal(
            snapshot["min_price"],
            "min_price",
            positive=True,
        )

        max_price = self._to_decimal(
            snapshot["max_price"],
            "max_price",
            positive=True,
        )

        open_price = self._to_decimal(
            snapshot["open_price"],
            "open_price",
            positive=True,
        )

        close_price = self._to_decimal(
            snapshot["close_price"],
            "close_price",
            positive=True,
        )

        total_volume = self._to_decimal(
            snapshot["total_volume"],
            "total_volume",
            nonnegative=True,
        )

        variation_pct = self._to_decimal(
            snapshot["variation_pct"],
            "variation_pct",
        )

        range_pct = self._to_decimal(
            snapshot["range_pct"],
            "range_pct",
        ) if snapshot.get("range_pct") is not None else None

        is_partial = bool(snapshot["is_partial"])

        trade_count_value = snapshot["trade_count"]

        if isinstance(trade_count_value, bool):
            raise ValueError(
                "trade_count must be a positive integer."
            )

        try:
            trade_count = int(trade_count_value)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "trade_count must be a positive integer."
            ) from exc

        if trade_count <= 0:
            raise ValueError(
                "trade_count must be greater than zero."
            )

        if min_price > max_price:
            raise ValueError(
                "min_price cannot be greater than max_price."
            )

        bounded_prices = {
            "avg_price": avg_price,
            "open_price": open_price,
            "close_price": close_price,
        }

        for field_name, price in bounded_prices.items():
            if not min_price <= price <= max_price:
                raise ValueError(
                    f"{field_name} must be between "
                    "min_price and max_price."
                )

        return {
            "instrument_id": normalized_instrument_id,
            "minute_ts": minute_ts,
            "avg_price": avg_price,
            "vwap_price": vwap_price,
            "min_price": min_price,
            "max_price": max_price,
            "open_price": open_price,
            "close_price": close_price,
            "total_volume": total_volume,
            "trade_count": trade_count,
            "variation_pct": variation_pct,
            "range_pct": range_pct,
            "is_partial": is_partial,
        }

    def insert_snapshot(
        self,
        snapshot: Mapping[str, Any],
        instrument_id: int,
    ) -> int:
        """
        Insert or update one minute snapshot.

        The unique key `(instrument_id, minute_ts)` makes this operation
        idempotent.
        """
        normalized = self._validate_snapshot(
            snapshot,
            instrument_id,
        )

        query = """
            INSERT INTO fact_market_minute (
                instrument_id,
                minute_ts,
                avg_price,
                vwap_price,
                min_price,
                max_price,
                open_price,
                close_price,
                total_volume,
                trade_count,
                variation_pct,
                range_pct,
                is_partial
            )
            VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s
            ) AS new_snapshot
            ON DUPLICATE KEY UPDATE
                avg_price = new_snapshot.avg_price,
                vwap_price = new_snapshot.vwap_price,
                min_price = new_snapshot.min_price,
                max_price = new_snapshot.max_price,
                open_price = new_snapshot.open_price,
                close_price = new_snapshot.close_price,
                total_volume = new_snapshot.total_volume,
                trade_count = new_snapshot.trade_count,
                variation_pct = new_snapshot.variation_pct,
                range_pct = new_snapshot.range_pct,
                is_partial = new_snapshot.is_partial,
                snapshot_updated_at = CURRENT_TIMESTAMP
        """

        affected_rows = self._execute_write(
            query,
            (
                normalized["instrument_id"],
                normalized["minute_ts"],
                normalized["avg_price"],
                normalized["vwap_price"],
                normalized["min_price"],
                normalized["max_price"],
                normalized["open_price"],
                normalized["close_price"],
                normalized["total_volume"],
                normalized["trade_count"],
                normalized["variation_pct"],
                normalized["range_pct"],
                normalized["is_partial"],
            ),
            operation="insert_snapshot",
        )

        logger.debug(
            "Minute snapshot persisted: "
            "instrument_id=%d minute_ts=%s affected_rows=%d",
            normalized["instrument_id"],
            normalized["minute_ts"].isoformat(),
            affected_rows,
        )

        return affected_rows

    def insert_snapshots_batch(
        self,
        snapshots: Sequence[
            tuple[Mapping[str, Any], int]
        ],
    ) -> int:
        """
        Insert or update several snapshots in one transaction.
        """
        if not snapshots:
            return 0

        normalized_snapshots = [
            self._validate_snapshot(
                snapshot,
                instrument_id,
            )
            for snapshot, instrument_id in snapshots
        ]

        query = """
            INSERT INTO fact_market_minute (
                instrument_id,
                minute_ts,
                avg_price,
                vwap_price,
                min_price,
                max_price,
                open_price,
                close_price,
                total_volume,
                trade_count,
                variation_pct,
                range_pct,
                is_partial
            )
            VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s
            ) AS new_snapshot
            ON DUPLICATE KEY UPDATE
                avg_price = new_snapshot.avg_price,
                vwap_price = new_snapshot.vwap_price,
                min_price = new_snapshot.min_price,
                max_price = new_snapshot.max_price,
                open_price = new_snapshot.open_price,
                close_price = new_snapshot.close_price,
                total_volume = new_snapshot.total_volume,
                trade_count = new_snapshot.trade_count,
                variation_pct = new_snapshot.variation_pct,
                range_pct = new_snapshot.range_pct,
                is_partial = new_snapshot.is_partial,
                snapshot_updated_at = CURRENT_TIMESTAMP
        """

        parameters = [
            (
                item["instrument_id"],
                item["minute_ts"],
                item["avg_price"],
                item["vwap_price"],
                item["min_price"],
                item["max_price"],
                item["open_price"],
                item["close_price"],
                item["total_volume"],
                item["trade_count"],
                item["variation_pct"],
                item["range_pct"],
                item["is_partial"],
            )
            for item in normalized_snapshots
        ]

        affected_rows = self._execute_many(
            query,
            parameters,
            operation="insert_snapshots_batch",
        )

        logger.info(
            "Minute snapshot batch persisted: "
            "batch_size=%d affected_rows=%d",
            len(parameters),
            affected_rows,
        )

        return affected_rows

    def insert_pipeline_run(
        self,
        run_id: str,
        status: str = "RUNNING",
        *,
        started_at: Optional[datetime] = None,
    ) -> int:
        """
        Insert a pipeline execution record.

        The operation is idempotent for a given run identifier.
        """
        normalized_run_id = self._validate_run_id(run_id)
        normalized_status = self._validate_run_status(status)

        normalized_started_at = self._normalize_utc_datetime(
            started_at or datetime.now(timezone.utc),
            "started_at",
        )

        query = """
            INSERT INTO fact_pipeline_run (
                run_id,
                started_at,
                status
            )
            VALUES (%s, %s, %s) AS new_run
            ON DUPLICATE KEY UPDATE
                status = new_run.status,
                updated_at = CURRENT_TIMESTAMP
        """

        return self._execute_write(
            query,
            (
                normalized_run_id,
                normalized_started_at,
                normalized_status,
            ),
            operation="insert_pipeline_run",
        )

    @staticmethod
    def _validate_run_id(run_id: str) -> str:
        """
        Validate a pipeline run identifier.
        """
        if not isinstance(run_id, str):
            raise TypeError(
                "run_id must be a string."
            )

        normalized_run_id = run_id.strip()

        if not normalized_run_id:
            raise ValueError(
                "run_id cannot be empty."
            )

        if len(normalized_run_id) > 36:
            raise ValueError(
                "run_id cannot exceed 36 characters."
            )

        return normalized_run_id

    def _validate_run_status(self, status: str) -> str:
        """
        Validate a pipeline run status.
        """
        if not isinstance(status, str):
            raise TypeError(
                "Pipeline status must be a string."
            )

        normalized_status = status.strip().upper()

        if normalized_status not in self.ALLOWED_RUN_STATUSES:
            allowed = ", ".join(
                sorted(self.ALLOWED_RUN_STATUSES)
            )

            raise ValueError(
                f"Invalid pipeline status: {status!r}. "
                f"Allowed statuses: {allowed}"
            )

        return normalized_status

    def update_pipeline_run(
        self,
        run_id: str,
        **updates: Any,
    ) -> int:
        """
        Update explicitly authorized fields of a pipeline run.
        """
        normalized_run_id = self._validate_run_id(run_id)

        if not updates:
            logger.debug(
                "No pipeline run updates provided: run_id=%s",
                normalized_run_id,
            )
            return 0

        unknown_fields = set(updates).difference(
            self.ALLOWED_RUN_UPDATE_FIELDS
        )

        if unknown_fields:
            invalid = ", ".join(sorted(unknown_fields))

            raise ValueError(
                f"Unauthorized pipeline run fields: {invalid}"
            )

        normalized_updates = dict(updates)

        if "status" in normalized_updates:
            normalized_updates["status"] = (
                self._validate_run_status(
                    normalized_updates["status"]
                )
            )

        if "finished_at" in normalized_updates:
            normalized_updates["finished_at"] = (
                self._normalize_utc_datetime(
                    normalized_updates["finished_at"],
                    "finished_at",
                )
            )

        counter_fields = {
            "messages_received",
            "messages_valid",
            "messages_rejected",
            "snapshots_created",
            "events_created",
        }

        for field_name in counter_fields.intersection(
            normalized_updates
        ):
            value = normalized_updates[field_name]

            if isinstance(value, bool):
                raise ValueError(
                    f"{field_name} must be a nonnegative integer."
                )

            try:
                normalized_value = int(value)
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    f"{field_name} must be a nonnegative integer."
                ) from exc

            if normalized_value < 0:
                raise ValueError(
                    f"{field_name} cannot be negative."
                )

            normalized_updates[field_name] = normalized_value

        if "storage_used_mb" in normalized_updates:
            normalized_updates["storage_used_mb"] = (
                self._to_decimal(
                    normalized_updates["storage_used_mb"],
                    "storage_used_mb",
                    nonnegative=True,
                )
            )

        if "error_message" in normalized_updates:
            error_message = normalized_updates[
                "error_message"
            ]

            if error_message is not None:
                error_message = str(error_message)[:1000]

            normalized_updates[
                "error_message"
            ] = error_message

        ordered_fields = sorted(normalized_updates)

        set_clause = ", ".join(
            f"`{field_name}` = %s"
            for field_name in ordered_fields
        )

        parameters = [
            normalized_updates[field_name]
            for field_name in ordered_fields
        ]

        parameters.append(normalized_run_id)

        query = f"""
            UPDATE fact_pipeline_run
            SET {set_clause}
            WHERE run_id = %s
        """

        affected_rows = self._execute_write(
            query,
            tuple(parameters),
            operation="update_pipeline_run",
        )

        if affected_rows == 0:
            logger.warning(
                "Pipeline run was not updated: run_id=%s",
                normalized_run_id,
            )

        return affected_rows

    def finish_pipeline_run(
        self,
        run_id: str,
        *,
        status: str,
        messages_received: int,
        messages_valid: int,
        messages_rejected: int,
        snapshots_created: int,
        events_created: int,
        storage_used_mb: Any,
        error_message: Optional[str] = None,
    ) -> int:
        """
        Finalize a pipeline run with its execution statistics.
        """
        if status.upper() not in {
            "SUCCESS",
            "FAILED",
            "STOPPED",
            "PARTIAL",
        }:
            raise ValueError(
                "A finished run must use SUCCESS, FAILED, "
                "STOPPED, or PARTIAL."
            )

        return self.update_pipeline_run(
            run_id,
            finished_at=datetime.now(timezone.utc),
            status=status,
            messages_received=messages_received,
            messages_valid=messages_valid,
            messages_rejected=messages_rejected,
            snapshots_created=snapshots_created,
            events_created=events_created,
            storage_used_mb=storage_used_mb,
            error_message=error_message,
        )

    def insert_data_quality_issue(
        self,
        *,
        issue_type: str,
        severity: str,
        issue_count: int = 1,
        run_id: Optional[str] = None,
        instrument_id: Optional[int] = None,
        field_name: Optional[str] = None,
        example_value: Any = None,
        details: Optional[str] = None,
        detected_at: Optional[datetime] = None,
    ) -> int:
        """
        Insert one aggregated data-quality issue.
        """
        if not isinstance(issue_type, str):
            raise TypeError(
                "issue_type must be a string."
            )

        normalized_issue_type = issue_type.strip()

        if not normalized_issue_type:
            raise ValueError(
                "issue_type cannot be empty."
            )

        if len(normalized_issue_type) > 100:
            raise ValueError(
                "issue_type cannot exceed 100 characters."
            )

        if not isinstance(severity, str):
            raise TypeError(
                "severity must be a string."
            )

        normalized_severity = severity.strip().upper()

        if normalized_severity not in self.ALLOWED_SEVERITIES:
            allowed = ", ".join(
                sorted(self.ALLOWED_SEVERITIES)
            )

            raise ValueError(
                f"Invalid severity. Allowed values: {allowed}"
            )

        if isinstance(issue_count, bool):
            raise ValueError(
                "issue_count must be a positive integer."
            )

        try:
            normalized_count = int(issue_count)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "issue_count must be a positive integer."
            ) from exc

        if normalized_count <= 0:
            raise ValueError(
                "issue_count must be greater than zero."
            )

        normalized_run_id = (
            self._validate_run_id(run_id)
            if run_id is not None
            else None
        )

        normalized_instrument_id: Optional[int] = None

        if instrument_id is not None:
            if isinstance(instrument_id, bool):
                raise ValueError(
                    "instrument_id must be positive."
                )

            normalized_instrument_id = int(instrument_id)

            if normalized_instrument_id <= 0:
                raise ValueError(
                    "instrument_id must be positive."
                )

        normalized_detected_at = (
            self._normalize_utc_datetime(
                detected_at or datetime.now(timezone.utc),
                "detected_at",
            )
        )

        normalized_field_name = (
            str(field_name)[:100]
            if field_name is not None
            else None
        )

        normalized_example = (
            str(example_value)[:500]
            if example_value is not None
            else None
        )

        normalized_details = (
            str(details)[:1000]
            if details is not None
            else None
        )

        query = """
            INSERT INTO fact_data_quality (
                run_id,
                instrument_id,
                issue_type,
                field_name,
                severity,
                detected_at,
                issue_count,
                example_value,
                details
            )
            VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s
            )
        """

        return self._execute_write(
            query,
            (
                normalized_run_id,
                normalized_instrument_id,
                normalized_issue_type,
                normalized_field_name,
                normalized_severity,
                normalized_detected_at,
                normalized_count,
                normalized_example,
                normalized_details,
            ),
            operation="insert_data_quality_issue",
        )

    def get_database_size_mb(self) -> Decimal:
        """
        Return the estimated MySQL database size in megabytes.
        """
        query = """
            SELECT
                COALESCE(
                    SUM(data_length + index_length)
                    / 1024 / 1024,
                    0
                ) AS size_mb
            FROM information_schema.tables
            WHERE table_schema = %s
        """

        row = self._fetch_one(
            query,
            (self.connection_config["database"],),
            operation="get_database_size_mb",
        )

        if row is None:
            return Decimal("0")

        return Decimal(str(row["size_mb"] or 0))

    def health_check(self) -> dict[str, Any]:
        """
        Verify that MySQL is accessible.
        """
        checked_at = datetime.now(timezone.utc)

        try:
            row = self._fetch_one(
                """
                SELECT
                    1 AS healthy,
                    UTC_TIMESTAMP(6) AS database_time
                """,
                operation="mysql_health_check",
            )

            return {
                "status": "healthy",
                "checked_at": checked_at,
                "database_time": (
                    row["database_time"]
                    if row
                    else None
                ),
            }

        except pymysql.MySQLError as exc:
            return {
                "status": "unhealthy",
                "checked_at": checked_at,
                "error_type": type(exc).__name__,
            }

    def close(self) -> None:
        """
        Close the active MySQL connection.
        """
        connection = self._connection
        self._connection = None

        if connection is None:
            return

        try:
            if connection.open:
                connection.close()

                logger.info(
                    "MySQL connection closed."
                )

        except pymysql.MySQLError:
            logger.exception(
                "Error while closing the MySQL connection."
            )

    def __enter__(self) -> MySQLLoader:
        """
        Enter the loader context.
        """
        return self

    def __exit__(
        self,
        exc_type: Any,
        exc_value: Any,
        traceback: Any,
    ) -> None:
        """
        Close the connection when leaving the context.
        """
        self.close()