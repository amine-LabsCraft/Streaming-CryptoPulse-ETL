#!/usr/bin/env python3
"""
CryptoPulse ETL - Pipeline Runner.

Command-line entry point for running the CryptoPulse ETL pipeline.

Examples
--------
Run for 60 seconds:

    python -m scripts.run_pipeline

Run for five minutes:

    python -m scripts.run_pipeline --duration 300

Run until interrupted:

    python -m scripts.run_pipeline --duration 0

Run with selected symbols:

    python -m scripts.run_pipeline \
        --symbols btcusdt ethusdt

Enable debug logs:

    python -m scripts.run_pipeline --verbose
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys

from datetime import datetime
from decimal import Decimal
from typing import Any, Optional, Sequence

from app.pipeline import run_pipeline_async
from config.settings import (
    APP_CONFIG,
    PIPELINE_CONFIG,
    SYMBOLS,
)


logger = logging.getLogger(__name__)


class JSONConsoleEncoder(json.JSONEncoder):
    """
    JSON encoder supporting common CryptoPulse data types.
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
    *,
    verbose: bool = False,
    quiet: bool = False,
) -> None:
    """
    Configure application logging.

    Parameters
    ----------
    verbose:
        Enable DEBUG logging.

    quiet:
        Display only warnings and errors.
    """
    if verbose and quiet:
        raise ValueError(
            "--verbose and --quiet cannot be used together."
        )

    if verbose:
        log_level = logging.DEBUG
    elif quiet:
        log_level = logging.WARNING
    else:
        configured_level = str(
            APP_CONFIG.get("log_level", "INFO")
        ).upper()

        log_level = getattr(
            logging,
            configured_level,
            logging.INFO,
        )

    logging.basicConfig(
        level=log_level,
        format=(
            "%(asctime)s | %(levelname)-8s | "
            "%(name)s | %(message)s"
        ),
        datefmt="%Y-%m-%dT%H:%M:%S%z",
        force=True,
    )

    logging.getLogger("pymongo").setLevel(
        logging.WARNING
    )

    logging.getLogger("aiohttp").setLevel(
        logging.WARNING
    )


def positive_duration(value: str) -> float:
    """
    Parse a nonnegative pipeline duration.

    A value of zero means that the pipeline runs until interrupted.
    """
    try:
        duration = float(value)

    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "Duration must be numeric."
        ) from exc

    if duration < 0:
        raise argparse.ArgumentTypeError(
            "Duration cannot be negative."
        )

    return duration


def positive_integer(value: str) -> int:
    """
    Parse a strictly positive integer.
    """
    try:
        parsed_value = int(value)

    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "Value must be an integer."
        ) from exc

    if parsed_value <= 0:
        raise argparse.ArgumentTypeError(
            "Value must be greater than zero."
        )

    return parsed_value


def normalize_cli_symbols(
    symbols: Sequence[str],
) -> list"""
    Normalize and deduplicate CLI symbols.
    """
    normalized: list[str] = []
    seen: set[str] = set()

    for value in symbols:
        for item in value.split(","):
            symbol = item.strip().lower()

            if not symbol:
                continue

            if not symbol.isalnum():
                raise argparse.ArgumentTypeError(
                    f"Invalid Binance symbol: {item!r}"
                )

            if symbol not in seen:
                normalized.append(symbol)
                seen.add(symbol)

    if not normalized:
        raise argparse.ArgumentTypeError(
            "At least one symbol is required."
        )

    return normalized


def build_parser() -> argparse.ArgumentParser:
    """
    Build the command-line parser.
    """
    parser = argparse.ArgumentParser(
        prog="cryptopulse",
        description=(
            "Run the CryptoPulse real-time ETL pipeline."
        ),
        formatter_class=(
            argparse.ArgumentDefaultsHelpFormatter
        ),
    )

    parser.add_argument(
        "--duration",
        type=positive_duration,
        default=60.0,
        help=(
            "Execution duration in seconds. "
            "Use 0 to run until interrupted."
        ),
    )

    parser.add_argument(
        "--symbols",
        nargs="+",
        default=list(SYMBOLS),
        metavar="SYMBOL",
        help=(
            "Binance symbols separated by spaces or commas."
        ),
    )

    parser.add_argument(
        "--snapshot-queue-size",
        type=positive_integer,
        default=int(
            PIPELINE_CONFIG.get(
                "snapshot_queue_size",
                500,
            )
        ),
        help="Maximum snapshot queue size.",
    )

    parser.add_argument(
        "--event-queue-size",
        type=positive_integer,
        default=int(
            PIPELINE_CONFIG.get(
                "event_queue_size",
                500,
            )
        ),
        help="Maximum event queue size.",
    )

    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable DEBUG logging.",
    )

    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Display only warnings and errors.",
    )

    parser.add_argument(
        "--json",
        action="store_true",
        help="Print final statistics as JSON.",
    )

    return parser


async def run_from_arguments(
    args: argparse.Namespace,
) -> dict[str, Any]:
    """
    Run the pipeline using parsed CLI arguments.
    """
    symbols = normalize_cli_symbols(
        args.symbols
    )

    duration: Optional[float] = (
        None
        if args.duration == 0
        else args.duration
    )

    print()
    print("CryptoPulse ETL Pipeline")
    print("=" * 48)

    print(
        "Duration:",
        (
            "until interrupted"
            if duration is None
            else f"{duration:g} seconds"
        ),
    )

    print(
        "Symbols:",
        ", ".join(
            symbol.upper()
            for symbol in symbols
        ),
    )

    print(
        "Snapshot queue:",
        args.snapshot_queue_size,
    )

    print(
        "Event queue:",
        args.event_queue_size,
    )

    print()

    from app.pipeline import CryptoPulsePipeline

    pipeline = CryptoPulsePipeline(
        symbols=symbols,
        snapshot_queue_size=(
            args.snapshot_queue_size
        ),
        event_queue_size=(
            args.event_queue_size
        ),
        flush_interval_seconds=float(
            PIPELINE_CONFIG.get(
                "flush_interval_seconds",
                1.0,
            )
        ),
        storage_check_interval_seconds=float(
            PIPELINE_CONFIG.get(
                "storage_check_interval_seconds",
                60.0,
            )
        ),
        store_partial_snapshots=bool(
            PIPELINE_CONFIG.get(
                "store_partial_snapshots",
                False,
            )
        ),
    )

    return await pipeline.run(
        duration_seconds=duration
    )


def print_final_stats(
    stats: dict[str, Any],
    *,
    as_json: bool,
) -> None:
    """
    Print final pipeline statistics.
    """
    if as_json:
        print(
            json.dumps(
                stats,
                cls=JSONConsoleEncoder,
                ensure_ascii=False,
                indent=2,
            )
        )

        return

    print()
    print("Pipeline completed")
    print("=" * 48)
    print(f"Run ID: {stats.get('run_id')}")
    print(
        f"Duration: "
        f"{stats.get('duration_seconds', 0):.2f}s"
    )
    print(
        f"Messages received: "
        f"{stats.get('messages_received', 0)}"
    )
    print(
        f"Messages valid: "
        f"{stats.get('messages_valid', 0)}"
    )
    print(
        f"Messages rejected: "
        f"{stats.get('messages_rejected', 0)}"
    )
    print(
        f"Snapshots persisted: "
        f"{stats.get('snapshots_persisted', 0)}"
    )
    print(
        f"Events persisted: "
        f"{stats.get('events_persisted', 0)}"
    )
    print(
        f"Storage: "
        f"{stats.get('storage_used_mb', 0)} MB"
    )
    print(
        f"Storage status: "
        f"{stats.get('storage_status', 'UNKNOWN')}"
    )


def main() -> int:
    """
    Run the CryptoPulse command-line application.

    Returns
    -------
    int
        Process exit code.
    """
    parser = build_parser()
    args = parser.parse_args()

    try:
        configure_logging(
            verbose=args.verbose,
            quiet=args.quiet,
        )

    except ValueError as exc:
        parser.error(str(exc))

    try:
        stats = asyncio.run(
            run_from_arguments(args)
        )

        print_final_stats(
            stats,
            as_json=args.json,
        )

        return 0

    except KeyboardInterrupt:
        print()
        print("Pipeline stopped by user.")

        return 130

    except argparse.ArgumentTypeError as exc:
        logger.error(
            "Invalid command-line argument: %s",
            exc,
        )

        return 2

    except Exception:
        logger.exception(
            "CryptoPulse pipeline terminated with an error."
        )

        return 1


if __name__ == "__main__":
    sys.exit(main())