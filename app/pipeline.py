"""
CryptoPulse ETL - Pipeline
Main orchestrator that coordinates Extract, Transform, Load
"""

import asyncio
import logging
import time
from typing import Dict, List, Optional, Any

from config.settings import PIPELINE_CONFIG
from app.extractor import BinanceWebSocketExtractor
from app.transformer import MinuteAggregator
from app.loaders import MySQLLoader
from app.mongo_loader import MongoLoader

logger = logging.getLogger(__name__)


class CryptoPulsePipeline:
    """Main ETL pipeline orchestrating extract, transform, and load."""

    def __init__(
        self,
        symbols: List[str] = None,
        minute_interval: int = None,
    ):
        self.symbols = symbols or ["btcusdt", "ethusdt"]
        self.minute_interval = minute_interval or PIPELINE_CONFIG["minute_interval"]

        # Components
        self.extractor = None
        self.aggregator = MinuteAggregator()
        self.mysql_loader = MySQLLoader()
        self.mongo_loader = MongoLoader()

        # Pipeline state
        self.running = False
        self.run_id = None
        self.pipeline_stats = {
            "messages_received": 0,
            "messages_valid": 0,
            "messages_rejected": 0,
            "snapshots_created": 0,
            "events_created": 0,
            "start_time": None,
            "end_time": None,
        }

    def _initialize(self):
        """Initialize pipeline components (DB connections, collections)."""
        logger.info("Initializing pipeline components...")

        # Initialize MySQL tables
        self.mysql_loader.init_tables()

        # Initialize MongoDB collections
        self.mongo_loader.init_collections()

        # Insert initial pipeline run record
        from datetime import datetime
        self.run_id = f"run_{int(time.time())}"
        self.mysql_loader.insert_pipeline_run(self.run_id, status="STARTED")

        logger.info(f"Pipeline initialized with run_id: {self.run_id}")

    async def _process_trade(self, trade: dict):
        """Process a single trade: transform and load."""
        self.pipeline_stats["messages_received"] += 1

        # Add trade to aggregator buffer
        self.aggregator.add_trade(trade)

        # Check if we should create a snapshot
        # We create snapshots when the pipeline run finishes or on interval
        # For now, we'll create snapshots periodically

    async def _run_minute_cycle(self):
        """Run a single minute aggregation cycle."""
        snapshots = self.aggregator.get_snapshots()

        for snapshot in snapshots:
            symbol = snapshot.get("symbol", "")
            instrument_id = self._get_instrument_id(symbol)

            # Insert into MySQL
            self.mysql_loader.insert_snapshot(snapshot, instrument_id)

            # Check for events (price spike, high volume, etc.)
            self._check_and_create_events(snapshot)

            self.pipeline_stats["snapshots_created"] += 1

        logger.info(
            f"Minute cycle: created {len(snapshots)} snapshots, "
            f"total so far: {self.pipeline_stats['snapshots_created']}"
        )

    def _get_instrument_id(self, symbol: str) -> int:
        """Get or create instrument ID in MySQL."""
        # Simple lookup - in production would use proper cache
        query = "SELECT instrument_id FROM dim_instrument WHERE symbol = %s"
        with self.mysql_connection.cursor() as cursor:
            cursor.execute(query, (symbol.upper(),))
            result = cursor.fetchone()
            if result:
                return result["instrument_id"]

        # Create new instrument
        insert_query = """
            INSERT INTO dim_instrument (symbol, base_asset, quote_asset, is_active)
            VALUES (%s, %s, %s, %s)
        """
        # Extract base/quote
        parts = symbol.upper().split("USDT")
        base_asset = parts[0]
        quote_asset = "USDT"

        self.mysql_connection.execute(
            insert_query, (symbol.upper(), base_asset, quote_asset, True)
        )
        return self.mysql_connection.lastrowid

    def _check_and_create_events(self, snapshot: dict):
        """Check snapshot conditions and create market events if needed."""
        variation_pct = snapshot.get("variation_pct", 0)
        total_volume = snapshot.get("total_volume", 0)
        trade_count = snapshot.get("trade_count", 0)
        symbol = snapshot.get("symbol", "")

        # Rule 13: Price spike (variation > threshold)
        threshold = PIPELINE_CONFIG["price_spike_threshold"]
        if variation_pct and abs(variation_pct) > threshold:
            severity = "WARNING" if variation_pct > 0 else "INFO"
            self.mongo_loader.insert_market_event(
                event_type="price_spike",
                symbol=symbol,
                severity=severity,
                message=f"Prix en variation forte: {variation_pct:.2f}%",
                context={"variation_pct": variation_pct},
            )
            self.pipeline_stats["events_created"] += 1

        # Rule 15: High volume (volume > 2x average)
        # For now, just log if volume is significant
        if total_volume > 100:  # arbitrary threshold
            self.mongo_loader.insert_market_event(
                event_type="high_volume",
                symbol=symbol,
                severity="INFO",
                message=f"Volume élevé: {total_volume:.2f}",
                context={"total_volume": total_volume, "trade_count": trade_count},
            )
            self.pipeline_stats["events_created"] += 1

        # Rule 16: High volatility (max - min is large)
        price_range = snapshot.get("max_price", 0) - snapshot.get("min_price", 0)
        if price_range > 100:  # arbitrary threshold
            self.mongo_loader.insert_market_event(
                event_type="high_volatility",
                symbol=symbol,
                severity="WARNING",
                message=f"Haute volatilité: range {price_range:.2f}",
                context={"price_range": price_range, "max_price": snapshot.get("max_price"), "min_price": snapshot.get("min_price")},
            )
            self.pipeline_stats["events_created"] += 1

    async def run(self, duration_seconds: int = None):
        """Run the ETL pipeline."""
        logger.info("Starting CryptoPulse ETL Pipeline...")

        self._initialize()
        self.running = True
        self.pipeline_stats["start_time"] = time.time()

        # Set run ID
        if not self.run_id:
            from datetime import datetime
            self.run_id = f"run_{int(time.time())}"
            self.mysql_loader.insert_pipeline_run(self.run_id, status="RUNNING")

        try:
            # Start WebSocket extractor
            self.extractor = BinanceWebSocketExtractor(symbols=self.symbols)
            
            # Run WebSocket listening and minute cycles
            await self._run_pipeline(duration_seconds)

        except Exception as e:
            logger.error(f"Pipeline error: {e}")
            raise
        finally:
            self._finalize()

    async def _run_pipeline(self, duration_seconds: int = None):
        """Run the main pipeline loop."""
        self.extractor = BinanceWebSocketExtractor(symbols=self.symbols)

        # Start WebSocket in background
        ws_task = asyncio.create_task(self.extractor.start())

        # Wait for specified duration or indefinitely
        start_time = time.time()
        
        while self.running:
            # Check if we've run for the specified duration
            if duration_seconds and (time.time() - start_time) >= duration_seconds:
                logger.info(f"Pipeline duration {duration_seconds}s reached, stopping")
                break

            # Run a minute cycle
            await self._run_minute_cycle()

            # Sleep until next minute (adjust for processing time)
            processing_time = time.time() - start_time % 60
            sleep_time = max(0, 60 - processing_time)
            await asyncio.sleep(sleep_time)

        # Cancel WebSocket task
        ws_task.cancel()
        try:
            await ws_task
        except asyncio.CancelledError:
            pass

    def _finalize(self):
        """Finalize the pipeline run."""
        self.running = False
        self.pipeline_stats["end_time"] = time.time()
        self.pipeline_stats["duration"] = self.pipeline_stats["end_time"] - self.pipeline_stats["start_time"]

        # Update pipeline run status in MySQL
        if self.run_id:
            self.mysql_loader.update_pipeline_run(
                self.run_id,
                finished_at=datetime.fromtimestamp(self.pipeline_stats["end_time"]),
                status="COMPLETED",
                messages_received=self.pipeline_stats["messages_received"],
                messages_valid=self.pipeline_stats["messages_valid"],
                messages_rejected=self.pipeline_stats["messages_rejected"],
                snapshots_created=self.pipeline_stats["snapshots_created"],
                events_created=self.pipeline_stats["events_created"],
                storage_used_mb=0.0,  # Would calculate actual storage
            )

        logger.info(
            f"Pipeline finalized - Run: {self.run_id}\n"
            f"Stats: Received={self.pipeline_stats['messages_received']}, "
            f"Valid={self.pipeline_stats['messages_valid']}, "
            f"Rejected={self.pipeline_stats['messages_rejected']}, "
            f"Snapshots={self.pipeline_stats['snapshots_created']}, "
            f"Events={self.pipeline_stats['events_created']}, "
            f"Duration={self.pipeline_stats['duration']:.1f}s"
        )

        # Close connections
        self.mysql_loader.close()
        self.mongo_loader.close()


def run_pipeline(duration_seconds: int = 60, symbols: List[str] = None):
    """ Convenience function to run the pipeline. """
    pipeline = CryptoPulsePipeline(symbols=symbols, minute_interval=60)
    asyncio.run(pipeline.run(duration_seconds=duration_seconds))