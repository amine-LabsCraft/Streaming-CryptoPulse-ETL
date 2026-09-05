#!/usr/bin/env python3
"""
CryptoPulse ETL - Utility Commands.

Provides maintenance, diagnostics and demonstration commands for:
- configuration validation;
- database health checks;
- schema initialization;
- synthetic snapshot generation;
- statistics reporting;
- storage monitoring;
- pipeline execution.

Examples
--------
Validate configuration:

    python -m scripts.utilities validate-config

Test database connectivity:

    python -m scripts.utilities test-connections

Initialize database structures:

    python -m scripts.utilities init-databases

Generate synthetic snapshots:

    python -m scripts.utilities sample-data

Show MongoDB statistics:

    python -m scripts.utilities stats

Monitor combined storage:

    python -m scripts.utilities storage

Run the pipeline for one minute:

    python -m scripts.utilities run --duration 60
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import random
import sys

from collections import Counter
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Optional
from urllib.parse import urlparse

from app.loaders import MySQLLoader
from app.mongo_loader import MongoLoader
from app.pipeline import run_pipeline_async
from app.transformer import MinuteAggregator
from config.settings import (
    BINANCE_WS_URL,
    MONGO_CONFIG,
    MYSQL_CONFIG,
    PIPELINE_CONFIG,
    SETTINGS,
    SYMBOLS,
    get_safe_settings,
)


logger = logging.getLogger(__name__)


class UtilityJSONEncoder(json.JSONEncoder):
    """
    Encode CryptoPulse values for console JSON output.
    """

    def default(self, value: Any) -> Any:
        if isinstance(value, Decimal):
            return str(value)

        if isinstance(value, datetime):
            return value.isoformat()

        if isinstance(value, set):
            return sorted(value)

        return super().default(value)


def configure_logging(
    verbose: bool,
) -> None:
    """
    Configure utility logging.
    """
    logging.basicConfig(
        level=(
            logging.DEBUG
            if verbose
            else logging.INFO
        ),
        format=(
            "%(asctime)s | %(levelname)-8s | "
            "%(name)s | %(message)s"
        ),
        force=True,
    )

    logging.getLogger("pymongo").setLevel(
        logging.WARNING
    )


def print_json(value: Any) -> None:
    """
    Print a JSON-compatible representation.
    """
    print(
        json.dumps(
            value,
            cls=UtilityJSONEncoder,
            ensure_ascii=False,
            indent=2,
        )
    )


def validate_config() -> int:
    """
    Display the validated non-sensitive configuration.

    The complete settings module validates values during import.
    Reaching this function therefore means structural validation
    succeeded.
    """
    print("CryptoPulse Configuration Validation")
    print("=" * 48)

    safe_settings = get_safe_settings()

    print_json(safe_settings)

    warnings: list[str] = []

    if not BINANCE_WS_URL.startswith("wss://"):
        warnings.append(
            "Binance WebSocket does not use secure WSS."
        )

    if BINANCE_WS_URL.endswith("/ws"):
        warnings.append(
            "BINANCE_WS_URL should be a base URL "
            "without the /ws suffix."
        )

    if not SYMBOLS:
        warnings.append(
            "No Binance symbols are configured."
        )

    if not MYSQL_CONFIG.get("password"):
        warnings.append(
            "MYSQL_PASSWORD is empty. This should only "
            "be accepted in a local development environment."
        )

    mongo_uri = MONGO_CONFIG.get("uri", "")
    parsed_mongo_uri = urlparse(mongo_uri)

    if not parsed_mongo_uri.hostname:
        warnings.append(
            "MONGO_URI does not contain a valid host."
        )

    if warnings:
        print()
        print("Configuration warnings:")

        for warning in warnings:
            print(f"  - {warning}")

    print()
    print("Configuration loaded successfully.")

    return 0


def test_connections() -> int:
    """
    Test MySQL and MongoDB without inserting test data.
    """
    print("Database Connection Test")
    print("=" * 48)

    mysql_success = False
    mongo_success = False

    mysql: Optional[MySQLLoader] = None
    mongo: Optional[MongoLoader] = None

    try:
        print("Testing MySQL...")

        mysql = MySQLLoader()
        result = mysql.health_check()

        if result.get("status") == "healthy":
            mysql_success = True
            print("  MySQL connection successful.")
        else:
            print(
                "  MySQL connection unhealthy:",
                result.get("error_type", "unknown"),
            )

    except Exception as exc:
        logger.exception(
            "MySQL connection test failed."
        )

        print(
            "  MySQL connection failed:",
            type(exc).__name__,
        )

    finally:
        if mysql is not None:
            mysql.close()

    try:
        print("Testing MongoDB...")

        mongo = MongoLoader()
        result = mongo.health_check()

        if result.get("status") == "healthy":
            mongo_success = True
            print("  MongoDB connection successful.")
        else:
            print(
                "  MongoDB connection unhealthy:",
                result.get("error_type", "unknown"),
            )

    except Exception as exc:
        logger.exception(
            "MongoDB connection test failed."
        )

        print(
            "  MongoDB connection failed:",
            type(exc).__name__,
        )

    finally:
        if mongo is not None:
            mongo.close()

    print()

    if mysql_success and mongo_success:
        print("All database connections are healthy.")
        return 0

    print("At least one database connection failed.")
    return 1


def initialize_databases() -> int:
    """
    Initialize MySQL tables and MongoDB collections.
    """
    print("Database Initialization")
    print("=" * 48)

    mysql: Optional[MySQLLoader] = None
    mongo: Optional[MongoLoader] = None

    try:
        mysql = MySQLLoader()
        mysql.init_tables()

        instrument_ids = (
            mysql.seed_default_instruments()
        )

        print("MySQL tables initialized.")
        print("Instrument identifiers:")

        for symbol, instrument_id in sorted(
            instrument_ids.items()
        ):
            print(
                f"  {symbol}: {instrument_id}"
            )

        mongo = MongoLoader()
        index_names = mongo.init_collections()

        print()
        print("MongoDB collections initialized.")
        print("Indexes:")

        for collection, indexes in index_names.items():
            print(f"  {collection}:")

            for index_name in indexes:
                print(f"    - {index_name}")

        return 0

    except Exception:
        logger.exception(
            "Database initialization failed."
        )

        return 1

    finally:
        if mysql is not None:
            mysql.close()

        if mongo is not None:
            mongo.close()


def generate_sample_data(
    *,
    minute_count: int = 10,
    trades_per_minute: int = 100,
    seed: int = 42,
) -> list[dict[str, Any]]:
    """
    Generate deterministic synthetic minute snapshots.

    The generated data is synthetic and is not sourced from Binance.
    No generated snapshot is inserted into a database.
    """
    print("Synthetic Snapshot Generation")
    print("=" * 48)
    print(
        "The generated values are synthetic "
        "and intended only for testing."
    )
    print()

    if minute_count <= 0:
        raise ValueError(
            "minute_count must be greater than zero."
        )

    if trades_per_minute <= 0:
        raise ValueError(
            "trades_per_minute must be greater than zero."
        )

    random_generator = random.Random(seed)

    aggregator = MinuteAggregator(
        allowed_lateness_seconds=0,
        snapshot_history_size=max(
            minute_count * 2,
            100,
        ),
        deduplicate_trades=True,
    )

    start_time = datetime(
        2026,
        1,
        1,
        12,
        0,
        tzinfo=timezone.utc,
    )

    base_price = Decimal("65000")
    trade_id = 1
    completed_snapshots: list[
        dict[str, Any]
    ] = []

    for minute_index in range(minute_count):
        minute_start = (
            start_time
            + timedelta(minutes=minute_index)
        )

        for trade_index in range(
            trades_per_minute
        ):
            second_offset = (
                trade_index % 60
            )

            microsecond_offset = (
                trade_index // 60
            ) * 1_000

            trade_timestamp = (
                minute_start
                + timedelta(
                    seconds=second_offset,
                    microseconds=microsecond_offset,
                )
            )

            variation_bps = Decimal(
                str(
                    random_generator.uniform(
                        -0.5,
                        0.5,
                    )
                )
            )

            price = (
                base_price
                * (
                    Decimal("1")
                    + variation_bps
                    / Decimal("100")
                )
            )

            quantity = Decimal(
                str(
                    random_generator.uniform(
                        0.001,
                        0.01,
                    )
                )
            )

            trade = {
                "symbol": "BTCUSDT",
                "trade_id": trade_id,
                "price": price,
                "quantity": quantity,
                "timestamp": trade_timestamp,
                "received_at": trade_timestamp,
            }

            snapshots = aggregator.add_trade(
                trade
            )

            completed_snapshots.extend(
                snapshots
            )

            trade_id += 1

    final_watermark = (
        start_time
        + timedelta(
            minutes=minute_count,
            seconds=1,
        )
    )

    completed_snapshots.extend(
        aggregator.flush_completed(
            watermark=final_watermark
        )
    )

    completed_snapshots.sort(
        key=lambda snapshot: (
            snapshot["minute_ts"],
            snapshot["symbol"],
        )
    )

    print(
        f"Generated {len(completed_snapshots)} "
        "completed minute snapshots."
    )

    for snapshot in completed_snapshots[:5]:
        print(
            f"  {snapshot['symbol']} "
            f"{snapshot['minute_ts'].isoformat()} | "
            f"avg={snapshot['avg_price']:.2f} | "
            f"min={snapshot['min_price']:.2f} | "
            f"max={snapshot['max_price']:.2f} | "
            f"volume={snapshot['total_volume']:.8f} | "
            f"trades={snapshot['trade_count']} | "
            f"variation={snapshot['variation_pct']:.4f}%"
        )

    remaining = len(
        completed_snapshots
    ) - 5

    if remaining > 0:
        print(
            f"  ... and {remaining} more snapshots."
        )

    return completed_snapshots


def export_stats_report(
    *,
    limit: int = 50,
) -> int:
    """
    Display MongoDB document and event statistics.
    """
    print("Pipeline Statistics Report")
    print("=" * 48)

    mongo: Optional[MongoLoader] = None

    try:
        mongo = MongoLoader()

        stats = mongo.get_pipeline_stats()

        events = mongo.get_recent_events(
            limit=limit
        )

        event_type_counts = Counter(
            event.get(
                "event_type",
                "unknown",
            )
            for event in events
        )

        symbol_counts = Counter(
            event.get(
                "symbol",
                "unknown",
            )
            for event in events
        )

        print("MongoDB statistics:")
        print(
            f"  Market events: "
            f"{stats['total_market_events']}"
        )
        print(
            f"  Pipeline logs: "
            f"{stats['total_pipeline_logs']}"
        )
        print(
            f"  Raw samples: "
            f"{stats['total_raw_samples']}"
        )
        print(
            f"  Data size: "
            f"{stats['data_size_mb']:.3f} MB"
        )
        print(
            f"  Storage size: "
            f"{stats['storage_size_mb']:.3f} MB"
        )
        print(
            f"  Index size: "
            f"{stats['index_size_mb']:.3f} MB"
        )
        print(
            f"  Total allocated: "
            f"{stats['total_size_mb']:.3f} MB"
        )

        print()
        print(
            f"Event types in the latest "
            f"{len(events)} events:"
        )

        if not event_type_counts:
            print("  No events found.")

        for event_type, count in (
            event_type_counts.most_common()
        ):
            print(
                f"  {event_type}: {count}"
            )

        print()
        print("Symbols:")

        if not symbol_counts:
            print("  No symbols found.")

        for symbol, count in (
            symbol_counts.most_common()
        ):
            print(
                f"  {symbol}: {count}"
            )

        print()
        print("Recent events:")

        for event in events[:10]:
            event_time = event.get(
                "event_time"
            )

            formatted_time = (
                event_time.isoformat()
                if isinstance(
                    event_time,
                    datetime,
                )
                else "unknown"
            )

            message = str(
                event.get(
                    "message",
                    "",
                )
            )

            print(
                f"  {formatted_time} | "
                f"{event.get('event_type', 'unknown')} | "
                f"{event.get('symbol', 'unknown')} | "
                f"{message[:80]}"
            )

        return 0

    except Exception:
        logger.exception(
            "Unable to generate statistics report."
        )

        return 1

    finally:
        if mongo is not None:
            mongo.close()


def monitor_storage() -> int:
    """
    Measure combined MySQL and MongoDB storage usage.
    """
    print("Storage Monitoring")
    print("=" * 48)

    mysql: Optional[MySQLLoader] = None
    mongo: Optional[MongoLoader] = None

    try:
        mysql = MySQLLoader()
        mongo = MongoLoader()

        mysql_size_mb = Decimal(
            str(
                mysql.get_database_size_mb()
            )
        )

        mongo_stats = (
            mongo.get_pipeline_stats()
        )

        mongo_size_mb = Decimal(
            str(
                mongo_stats.get(
                    "total_size_mb",
                    0,
                )
            )
        )

        total_size_mb = (
            mysql_size_mb
            + mongo_size_mb
        )

        warning_mb = Decimal(
            str(
                PIPELINE_CONFIG[
                    "alert_storage_mb"
                ]
            )
        )

        critical_mb = Decimal(
            str(
                PIPELINE_CONFIG[
                    "critical_storage_mb"
                ]
            )
        )

        limit_mb = Decimal(
            str(
                PIPELINE_CONFIG[
                    "storage_limit_mb"
                ]
            )
        )

        remaining_mb = max(
            limit_mb - total_size_mb,
            Decimal("0"),
        )

        percentage_used = (
            total_size_mb
            / limit_mb
            * Decimal("100")
        )

        if total_size_mb >= limit_mb:
            status = "STOP"
        elif total_size_mb >= critical_mb:
            status = "CRITICAL"
        elif total_size_mb >= warning_mb:
            status = "WARNING"
        else:
            status = "NORMAL"

        print(
            f"MySQL: {mysql_size_mb:.3f} MB"
        )

        print(
            f"MongoDB: {mongo_size_mb:.3f} MB"
        )

        print(
            f"  Data: "
            f"{Decimal(str(mongo_stats['data_size_mb'])):.3f} MB"
        )

        print(
            f"  Storage: "
            f"{Decimal(str(mongo_stats['storage_size_mb'])):.3f} MB"
        )

        print(
            f"  Indexes: "
            f"{Decimal(str(mongo_stats['index_size_mb'])):.3f} MB"
        )

        print()
        print(
            f"Total: {total_size_mb:.3f} MB"
        )

        print(
            f"Remaining: {remaining_mb:.3f} MB"
        )

        print(
            f"Used: {percentage_used:.2f}%"
        )

        print(
            f"Status: {status}"
        )

        print()
        print(
            f"Warning threshold: {warning_mb} MB"
        )

        print(
            f"Critical threshold: {critical_mb} MB"
        )

        print(
            f"Stop threshold: {limit_mb} MB"
        )

        return (
            2
            if status == "STOP"
            else 1
            if status in {
                "WARNING",
                "CRITICAL",
            }
            else 0
        )

    except Exception:
        logger.exception(
            "Unable to measure storage usage."
        )

        return 1

    finally:
        if mysql is not None:
            mysql.close()

        if mongo is not None:
            mongo.close()


async def run_snapshot_daemon(
    duration_seconds: Optional[float],
    symbols: list[str],
) -> int:
    """
    Run the complete pipeline asynchronously.
    """
    print("CryptoPulse Snapshot Daemon")
    print("=" * 48)

    try:
        stats = await run_pipeline_async(
            duration_seconds=duration_seconds,
            symbols=symbols,
        )

        print_json(stats)

        return 0

    except asyncio.CancelledError:
        logger.info(
            "Snapshot daemon cancelled."
        )

        raise

    except Exception:
        logger.exception(
            "Snapshot daemon failed."
        )

        return 1


def positive_integer(value: str) -> int:
    """
    Parse a strictly positive integer.
    """
    try:
        result = int(value)

    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "Expected an integer."
        ) from exc

    if result <= 0:
        raise argparse.ArgumentTypeError(
            "Value must be greater than zero."
        )

    return result


def nonnegative_float(value: str) -> float:
    """
    Parse a nonnegative floating-point value.
    """
    try:
        result = float(value)

    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "Expected a numeric value."
        ) from exc

    if result < 0:
        raise argparse.ArgumentTypeError(
            "Value cannot be negative."
        )

    return result


def build_parser() -> argparse.ArgumentParser:
    """
    Build the utility command parser.
    """
    parser = argparse.ArgumentParser(
        prog="cryptopulse-utilities",
        description=(
            "CryptoPulse maintenance and diagnostic commands."
        ),
        formatter_class=(
            argparse.ArgumentDefaultsHelpFormatter
        ),
    )

    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging.",
    )

    subparsers = parser.add_subparsers(
        dest="command",
        required=True,
    )

    subparsers.add_parser(
        "validate-config",
        help="Validate and display safe configuration.",
    )

    subparsers.add_parser(
        "test-connections",
        help="Test MySQL and MongoDB connectivity.",
    )

    subparsers.add_parser(
        "init-databases",
        help="Create tables, collections and indexes.",
    )

    sample_parser = subparsers.add_parser(
        "sample-data",
        help="Generate synthetic minute snapshots.",
    )

    sample_parser.add_argument(
        "--minutes",
        type=positive_integer,
        default=10,
        help="Number of synthetic minutes.",
    )

    sample_parser.add_argument(
        "--trades-per-minute",
        type=positive_integer,
        default=100,
        help="Trades generated per minute.",
    )

    sample_parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Deterministic random seed.",
    )

    stats_parser = subparsers.add_parser(
        "stats",
        help="Display MongoDB pipeline statistics.",
    )

    stats_parser.add_argument(
        "--limit",
        type=positive_integer,
        default=50,
        help="Maximum recent events to analyze.",
    )

    subparsers.add_parser(
        "storage",
        help="Measure MySQL and MongoDB storage.",
    )

    run_parser = subparsers.add_parser(
        "run",
        help="Run the complete ETL pipeline.",
    )

    run_parser.add_argument(
        "--duration",
        type=nonnegative_float,
        default=60.0,
        help=(
            "Duration in seconds. "
            "Use 0 to run until interrupted."
        ),
    )

    run_parser.add_argument(
        "--symbols",
        nargs="+",
        default=list(SYMBOLS),
        help="Binance symbols to track.",
    )

    return parser


def main() -> int:
    """
    Run the selected utility command.
    """
    parser = build_parser()
    args = parser.parse_args()

    configure_logging(
        args.verbose
    )

    try:
        if args.command == "validate-config":
            return validate_config()

        if args.command == "test-connections":
            return test_connections()

        if args.command == "init-databases":
            return initialize_databases()

        if args.command == "sample-data":
            generate_sample_data(
                minute_count=args.minutes,
                trades_per_minute=(
                    args.trades_per_minute
                ),
                seed=args.seed,
            )

            return 0

        if args.command == "stats":
            return export_stats_report(
                limit=args.limit
            )

        if args.command == "storage":
            return monitor_storage()

        if args.command == "run":
            duration = (
                None
                if args.duration == 0
                else args.duration
            )

            symbols = [
                symbol.strip().lower()
                for symbol in args.symbols
                if symbol.strip()
            ]

            return asyncio.run(
                run_snapshot_daemon(
                    duration_seconds=duration,
                    symbols=symbols,
                )
            )

        parser.error(
            f"Unknown command: {args.command}"
        )

        return 2

    except KeyboardInterrupt:
        print()
        print("Operation stopped by user.")

        return 130

    except Exception:
        logger.exception(
            "Utility command failed."
        )

        return 1


if __name__ == "__main__":
    sys.exit(main())
