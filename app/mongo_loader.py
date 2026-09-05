"""
CryptoPulse ETL - MongoDB Loader
Stores events and logs in MongoDB database
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime

from pymongo import MongoClient

from config.settings import MONGO_CONFIG

logger = logging.getLogger(__name__)


class MongoLoader:
    """Loads data into MongoDB collections."""

    def __init__(self):
        self.client = MongoClient(
            host=MONGO_CONFIG["host"],
            port=MONGO_CONFIG["port"],
        )
        self.database = self.client[MONGO_CONFIG["database"]]

    @property
    def market_events(self):
        """Get the market_events collection."""
        return self.database["market_events"]

    @property
    def pipeline_logs(self):
        """Get the pipeline_logs collection."""
        return self.database["pipeline_logs"]

    @property
    def raw_samples(self):
        """Get the raw_samples_optional collection."""
        return self.database["raw_samples_optional"]

    def init_collections(self):
        """Initialize MongoDB collections with indexes."""
        logger.info("Initializing MongoDB collections...")

        # Create indexes for market_events
        self.market_events.create_index(
            [("instrument_id", 1), ("event_time", -1)]
        )
        self.market_events.create_index([("event_type", 1), ("severity", 1)])

        # Create indexes for pipeline_logs
        self.pipeline_logs.create_index([("run_id", 1), ("created_at", -1)])
        self.pipeline_logs.create_index([("level", 1)])

        # Create indexes for raw_samples
        self.raw_samples.create_index([("symbol", 1), ("created_at", -1)])

        logger.info("MongoDB collections initialized successfully")

    def insert_market_event(
        self,
        event_type: str,
        symbol: str,
        severity: str = "INFO",
        message: str = "",
        context: dict = None,
    ):
        """Insert a market event into MongoDB."""
        event_doc = {
            "event_type": event_type,
            "symbol": symbol,
            "severity": severity,
            "message": message,
            "context": context or {},
            "event_time": datetime.utcnow(),
            "created_at": datetime.utcnow(),
        }

        result = self.market_events.insert_one(event_doc)
        logger.debug(f"Inserted market event: {event_type} on {symbol}, id: {result.inserted_id}")
        return result

    def insert_pipeline_log(
        self,
        run_id: str,
        level: str = "INFO",
        message: str = "",
        details: dict = None,
    ):
        """Insert a pipeline log entry."""
        log_doc = {
            "run_id": run_id,
            "level": level,
            "message": message,
            "details": details or {},
            "created_at": datetime.utcnow(),
        }

        result = self.pipeline_logs.insert_one(log_doc)
        logger.debug(f"Inserted pipeline log: {message}, id: {result.inserted_id}")
        return result

    def insert_raw_sample(self, symbol: str, trade: dict):
        """Insert an optional raw trade sample for debugging."""
        sample_doc = {
            "symbol": symbol,
            "trade": trade,
            "created_at": datetime.utcnow(),
        }

        result = self.raw_samples.insert_one(sample_doc)
        logger.debug(f"Inserted raw sample: {symbol}, id: {result.inserted_id}")
        return result

    def get_recent_events(
        self,
        symbol: str = None,
        event_type: str = None,
        limit: int = 100,
    ) -> List[dict]:
        """Get recent market events, optionally filtered."""
        query = {}
        if symbol:
            query["symbol"] = symbol
        if event_type:
            query["event_type"] = event_type

        cursor = self.market_events.find(query).sort("event_time", -1).limit(limit)
        return list(cursor)

    def get_pipeline_stats(self) -> dict:
        """Get pipeline statistics from MongoDB."""
        total_events = self.market_events.count_documents({})
        total_logs = self.pipeline_logs.count_documents({})

        return {
            "total_market_events": total_events,
            "total_pipeline_logs": total_logs,
        }

    def close(self):
        """Close the MongoDB connection."""
        self.client.close()