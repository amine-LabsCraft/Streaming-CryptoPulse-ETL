"""
CryptoPulse ETL - Configuration Settings.

This module is the central configuration source for:
- application metadata;
- Binance WebSocket extraction;
- MySQL persistence;
- MongoDB persistence;
- minute-level transformation;
- event detection;
- pipeline queues and workers;
- storage monitoring;
- Power BI integration.

Configuration is loaded from environment variables.

For local development, a `.env` file can be loaded with
python-dotenv. Existing operating-system environment variables are
not overridden.
"""

from __future__ import annotations

import os

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Final, Optional
from urllib.parse import urlparse


try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None


PROJECT_ROOT: Final[Path] = Path(
    __file__
).resolve().parent.parent

ENV_FILE: Final[Path] = PROJECT_ROOT / ".env"


if load_dotenv is not None:
    load_dotenv(
        dotenv_path=ENV_FILE,
        override=False,
    )


class ConfigurationError(RuntimeError):
    """
    Raised when one or more configuration values are invalid.
    """


def _get_string(
    name: str,
    default: Optional[str] = None,
    *,
    required: bool = False,
    allow_empty: bool = False,
) -> str:
    """
    Read and validate a string environment variable.
    """
    raw_value = os.getenv(name)

    if raw_value is None:
        raw_value = default

    if raw_value is None:
        if required:
            raise ConfigurationError(
                f"Missing required environment variable: {name}"
            )

        return ""

    value = str(raw_value).strip()

    if not value and required:
        raise ConfigurationError(
            f"Environment variable {name} cannot be empty."
        )

    if not value and not allow_empty:
        if default is not None:
            return str(default).strip()

    return value


def _get_int(
    name: str,
    default: int,
    *,
    minimum: Optional[int] = None,
    maximum: Optional[int] = None,
) -> int:
    """
    Read and validate an integer environment variable.
    """
    raw_value = os.getenv(
        name,
        str(default),
    )

    try:
        value = int(raw_value)

    except (TypeError, ValueError) as exc:
        raise ConfigurationError(
            f"{name} must be an integer. "
            f"Received: {raw_value!r}"
        ) from exc

    if minimum is not None and value < minimum:
        raise ConfigurationError(
            f"{name} must be greater than or equal "
            f"to {minimum}. Received: {value}"
        )

    if maximum is not None and value > maximum:
        raise ConfigurationError(
            f"{name} must be less than or equal "
            f"to {maximum}. Received: {value}"
        )

    return value


def _get_float(
    name: str,
    default: float,
    *,
    minimum: Optional[float] = None,
    maximum: Optional[float] = None,
) -> float:
    """
    Read and validate a floating-point environment variable.
    """
    raw_value = os.getenv(
        name,
        str(default),
    )

    try:
        value = float(raw_value)

    except (TypeError, ValueError) as exc:
        raise ConfigurationError(
            f"{name} must be numeric. "
            f"Received: {raw_value!r}"
        ) from exc

    if minimum is not None and value < minimum:
        raise ConfigurationError(
            f"{name} must be greater than or equal "
            f"to {minimum}. Received: {value}"
        )

    if maximum is not None and value > maximum:
        raise ConfigurationError(
            f"{name} must be less than or equal "
            f"to {maximum}. Received: {value}"
        )

    return value


def _get_decimal(
    name: str,
    default: str,
    *,
    minimum: Optional[Decimal] = None,
    maximum: Optional[Decimal] = None,
) -> Decimal:
    """
    Read and validate a finite Decimal environment variable.
    """
    raw_value = os.getenv(
        name,
        default,
    )

    try:
        value = Decimal(
            str(raw_value)
        )

    except (
        InvalidOperation,
        TypeError,
        ValueError,
    ) as exc:
        raise ConfigurationError(
            f"{name} must be a decimal number. "
            f"Received: {raw_value!r}"
        ) from exc

    if not value.is_finite():
        raise ConfigurationError(
            f"{name} must be finite. "
            f"Received: {raw_value!r}"
        )

    if minimum is not None and value < minimum:
        raise ConfigurationError(
            f"{name} must be greater than or equal "
            f"to {minimum}. Received: {value}"
        )

    if maximum is not None and value > maximum:
        raise ConfigurationError(
            f"{name} must be less than or equal "
            f"to {maximum}. Received: {value}"
        )

    return value


def _get_bool(
    name: str,
    default: bool,
) -> bool:
    """
    Read and validate a boolean environment variable.

    Accepted true values:
    - true
    - 1
    - yes
    - y
    - on

    Accepted false values:
    - false
    - 0
    - no
    - n
    - off
    """
    raw_value = os.getenv(name)

    if raw_value is None:
        return default

    normalized = raw_value.strip().lower()

    true_values = {
        "true",
        "1",
        "yes",
        "y",
        "on",
    }

    false_values = {
        "false",
        "0",
        "no",
        "n",
        "off",
    }

    if normalized in true_values:
        return True

    if normalized in false_values:
        return False

    raise ConfigurationError(
        f"{name} must be a boolean value. "
        f"Received: {raw_value!r}"
    )


def _get_csv(
    name: str,
    default: str,
    *,
    lowercase: bool = False,
) -> tuple[str, ...]:
    """
    Read, normalize and deduplicate a comma-separated variable.
    """
    raw_value = os.getenv(
        name,
        default,
    )

    normalized_values: list[str] = []
    seen: set[str] = set()

    for raw_item in raw_value.split(","):
        item = raw_item.strip()

        if not item:
            continue

        if lowercase:
            item = item.lower()

        if item not in seen:
            normalized_values.append(item)
            seen.add(item)

    if not normalized_values:
        raise ConfigurationError(
            f"{name} must contain at least one value."
        )

    return tuple(normalized_values)


def _validate_url(
    name: str,
    value: str,
    *,
    allowed_schemes: set[str],
) -> str:
    """
    Validate a URL and its scheme.
    """
    parsed = urlparse(value)

    if parsed.scheme not in allowed_schemes:
        allowed = ", ".join(
            sorted(allowed_schemes)
        )

        raise ConfigurationError(
            f"{name} must use one of these schemes: "
            f"{allowed}. Received: {value!r}"
        )

    if not parsed.hostname:
        raise ConfigurationError(
            f"{name} must contain a valid hostname. "
            f"Received: {value!r}"
        )

    return value.rstrip("/")


def _validate_symbol(
    symbol: str,
) -> str:
    """
    Validate one Binance stream symbol.
    """
    normalized = symbol.strip().lower()

    if not normalized:
        raise ConfigurationError(
            "Binance symbols cannot be empty."
        )

    if not normalized.isalnum():
        raise ConfigurationError(
            f"Invalid Binance symbol: {symbol!r}"
        )

    if len(normalized) > 20:
        raise ConfigurationError(
            f"Binance symbol is too long: {symbol!r}"
        )

    return normalized


def _validate_log_level(
    value: str,
) -> str:
    """
    Validate the application logging level.
    """
    normalized = value.strip().upper()

    allowed_levels = {
        "DEBUG",
        "INFO",
        "WARNING",
        "ERROR",
        "CRITICAL",
    }

    if normalized not in allowed_levels:
        allowed = ", ".join(
            sorted(allowed_levels)
        )

        raise ConfigurationError(
            f"LOG_LEVEL must be one of: {allowed}. "
            f"Received: {value!r}"
        )

    return normalized


@dataclass(frozen=True, slots=True)
class ApplicationSettings:
    """
    General application settings.
    """

    name: str
    environment: str
    version: str
    log_level: str
    debug: bool


@dataclass(frozen=True, slots=True)
class BinanceSettings:
    """
    Binance WebSocket settings.
    """

    websocket_base_url: str
    symbols: tuple[str, ...]
    stream_name: str

    connect_timeout_seconds: float
    callback_timeout_seconds: float

    reconnect_initial_delay_seconds: float
    reconnect_max_delay_seconds: float

    max_buffer_size: int
    max_message_size_bytes: int


@dataclass(frozen=True, slots=True)
class MySQLSettings:
    """
    MySQL connection and pool settings.
    """

    host: str
    port: int
    user: str
    password: str
    database: str

    connect_timeout: int
    read_timeout: int
    write_timeout: int

    charset: str


@dataclass(frozen=True, slots=True)
class MongoSettings:
    """
    MongoDB connection and retention settings.
    """

    uri: str
    database: str

    server_selection_timeout_ms: int
    connect_timeout_ms: int
    socket_timeout_ms: int

    max_pool_size: int
    min_pool_size: int

    retry_writes: bool
    raw_sample_ttl_seconds: int


@dataclass(frozen=True, slots=True)
class TransformerSettings:
    """
    Minute aggregation settings.
    """

    minute_interval_seconds: int
    allowed_lateness_seconds: int
    snapshot_history_size: int
    deduplicate_trades: bool
    store_partial_snapshots: bool


@dataclass(frozen=True, slots=True)
class EventDetectionSettings:
    """
    Market event detection settings.
    """

    price_spike_threshold_pct: Decimal
    high_volume_multiplier: Decimal
    high_volatility_threshold_pct: Decimal

    volume_window_size: int
    minimum_volume_history: int


@dataclass(frozen=True, slots=True)
class QueueSettings:
    """
    Bounded pipeline queue settings.
    """

    snapshot_queue_size: int
    event_queue_size: int

    flush_interval_seconds: float
    storage_check_interval_seconds: float


@dataclass(frozen=True, slots=True)
class StorageSettings:
    """
    Pipeline storage limits.
    """

    warning_mb: Decimal
    critical_mb: Decimal
    limit_mb: Decimal

    retention_days: int


@dataclass(frozen=True, slots=True)
class PowerBISettings:
    """
    Power BI integration settings.
    """

    mysql_table: str
    refresh_interval_minutes: int


@dataclass(frozen=True, slots=True)
class Settings:
    """
    Complete immutable CryptoPulse configuration.
    """

    application: ApplicationSettings
    binance: BinanceSettings
    mysql: MySQLSettings
    mongo: MongoSettings
    transformer: TransformerSettings
    event_detection: EventDetectionSettings
    queues: QueueSettings
    storage: StorageSettings
    power_bi: PowerBISettings


def _load_settings() -> Settings:
    """
    Load and validate all application settings.
    """
    environment = _get_string(
        "APP_ENV",
        "development",
    ).lower()

    log_level = _validate_log_level(
        _get_string(
            "LOG_LEVEL",
            "INFO",
        )
    )

    application_settings = ApplicationSettings(
        name=_get_string(
            "APP_NAME",
            "CryptoPulse ETL",
        ),
        environment=environment,
        version=_get_string(
            "APP_VERSION",
            "1.0.0",
        ),
        log_level=log_level,
        debug=_get_bool(
            "APP_DEBUG",
            environment == "development",
        ),
    )

    websocket_base_url = _validate_url(
        "BINANCE_WS_URL",
        _get_string(
            "BINANCE_WS_URL",
            "wss://stream.binance.com:9443",
        ),
        allowed_schemes={
            "ws",
            "wss",
        },
    )

    configured_symbols = _get_csv(
        "BINANCE_SYMBOLS",
        "btcusdt,ethusdt",
        lowercase=True,
    )

    validated_symbols = tuple(
        _validate_symbol(symbol)
        for symbol in configured_symbols
    )

    stream_name = _get_string(
        "BINANCE_STREAM_NAME",
        "trade",
    ).lower()

    if stream_name not in {
        "trade",
        "aggtrade",
    }:
        raise ConfigurationError(
            "BINANCE_STREAM_NAME must be either "
            "'trade' or 'aggtrade'."
        )

    binance_settings = BinanceSettings(
        websocket_base_url=websocket_base_url,
        symbols=validated_symbols,
        stream_name=stream_name,
        connect_timeout_seconds=_get_float(
            "BINANCE_CONNECT_TIMEOUT_SECONDS",
            15.0,
            minimum=1.0,
            maximum=300.0,
        ),
        callback_timeout_seconds=_get_float(
            "BINANCE_CALLBACK_TIMEOUT_SECONDS",
            10.0,
            minimum=0.1,
            maximum=300.0,
        ),
        reconnect_initial_delay_seconds=_get_float(
            "BINANCE_RECONNECT_INITIAL_DELAY_SECONDS",
            1.0,
            minimum=0.1,
            maximum=300.0,
        ),
        reconnect_max_delay_seconds=_get_float(
            "BINANCE_RECONNECT_MAX_DELAY_SECONDS",
            60.0,
            minimum=1.0,
            maximum=3_600.0,
        ),
        max_buffer_size=_get_int(
            "BINANCE_MAX_BUFFER_SIZE",
            100,
            minimum=0,
            maximum=100_000,
        ),
        max_message_size_bytes=_get_int(
            "BINANCE_MAX_MESSAGE_SIZE_BYTES",
            1_048_576,
            minimum=1_024,
            maximum=16_777_216,
        ),
    )

    if (
        binance_settings.reconnect_max_delay_seconds
        < binance_settings.reconnect_initial_delay_seconds
    ):
        raise ConfigurationError(
            "BINANCE_RECONNECT_MAX_DELAY_SECONDS must be "
            "greater than or equal to "
            "BINANCE_RECONNECT_INITIAL_DELAY_SECONDS."
        )

    mysql_password = _get_string(
        "MYSQL_PASSWORD",
        "",
        allow_empty=True,
    )

    if (
        environment in {
            "production",
            "staging",
        }
        and not mysql_password
    ):
        raise ConfigurationError(
            "MYSQL_PASSWORD is required outside development."
        )

    mysql_settings = MySQLSettings(
        host=_get_string(
            "MYSQL_HOST",
            "localhost",
        ),
        port=_get_int(
            "MYSQL_PORT",
            3306,
            minimum=1,
            maximum=65_535,
        ),
        user=_get_string(
            "MYSQL_USER",
            "cryptopulse_app",
        ),
        password=mysql_password,
        database=_get_string(
            "MYSQL_DATABASE",
            "cryptopulse",
        ),
        connect_timeout=_get_int(
            "MYSQL_CONNECT_TIMEOUT",
            10,
            minimum=1,
            maximum=3_600,
        ),
        read_timeout=_get_int(
            "MYSQL_READ_TIMEOUT",
            30,
            minimum=1,
            maximum=3_600,
        ),
        write_timeout=_get_int(
            "MYSQL_WRITE_TIMEOUT",
            30,
            minimum=1,
            maximum=3_600,
        ),
        charset="utf8mb4",
    )

    mongo_uri = _validate_url(
        "MONGO_URI",
        _get_string(
            "MONGO_URI",
            "mongodb://localhost:27017",
        ),
        allowed_schemes={
            "mongodb",
            "mongodb+srv",
        },
    )

    mongo_settings = MongoSettings(
        uri=mongo_uri,
        database=_get_string(
            "MONGO_DATABASE",
            "cryptopulse",
        ),
        server_selection_timeout_ms=_get_int(
            "MONGO_SERVER_SELECTION_TIMEOUT_MS",
            5_000,
            minimum=100,
            maximum=300_000,
        ),
        connect_timeout_ms=_get_int(
            "MONGO_CONNECT_TIMEOUT_MS",
            5_000,
            minimum=100,
            maximum=300_000,
        ),
        socket_timeout_ms=_get_int(
            "MONGO_SOCKET_TIMEOUT_MS",
            30_000,
            minimum=100,
            maximum=3_600_000,
        ),
        max_pool_size=_get_int(
            "MONGO_MAX_POOL_SIZE",
            20,
            minimum=1,
            maximum=10_000,
        ),
        min_pool_size=_get_int(
            "MONGO_MIN_POOL_SIZE",
            0,
            minimum=0,
            maximum=10_000,
        ),
        retry_writes=_get_bool(
            "MONGO_RETRY_WRITES",
            True,
        ),
        raw_sample_ttl_seconds=_get_int(
            "RAW_SAMPLE_TTL_SECONDS",
            86_400,
            minimum=60,
            maximum=2_592_000,
        ),
    )

    if (
        mongo_settings.min_pool_size
        > mongo_settings.max_pool_size
    ):
        raise ConfigurationError(
            "MONGO_MIN_POOL_SIZE cannot exceed "
            "MONGO_MAX_POOL_SIZE."
        )

    transformer_settings = TransformerSettings(
        minute_interval_seconds=_get_int(
            "MINUTE_INTERVAL_SECONDS",
            60,
            minimum=60,
            maximum=60,
        ),
        allowed_lateness_seconds=_get_int(
            "ALLOWED_LATENESS_SECONDS",
            5,
            minimum=0,
            maximum=300,
        ),
        snapshot_history_size=_get_int(
            "SNAPSHOT_HISTORY_SIZE",
            1_000,
            minimum=0,
            maximum=100_000,
        ),
        deduplicate_trades=_get_bool(
            "DEDUPLICATE_TRADES",
            True,
        ),
        store_partial_snapshots=_get_bool(
            "STORE_PARTIAL_SNAPSHOTS",
            False,
        ),
    )

    event_detection_settings = EventDetectionSettings(
        price_spike_threshold_pct=_get_decimal(
            "PRICE_SPIKE_THRESHOLD_PCT",
            "0.5",
            minimum=Decimal("0.0001"),
        ),
        high_volume_multiplier=_get_decimal(
            "HIGH_VOLUME_MULTIPLIER",
            "2.0",
            minimum=Decimal("1.0"),
        ),
        high_volatility_threshold_pct=_get_decimal(
            "HIGH_VOLATILITY_THRESHOLD_PCT",
            "5.0",
            minimum=Decimal("0.0001"),
        ),
        volume_window_size=_get_int(
            "VOLUME_WINDOW_SIZE",
            20,
            minimum=1,
            maximum=10_000,
        ),
        minimum_volume_history=_get_int(
            "MINIMUM_VOLUME_HISTORY",
            5,
            minimum=1,
            maximum=10_000,
        ),
    )

    if (
        event_detection_settings.minimum_volume_history
        > event_detection_settings.volume_window_size
    ):
        raise ConfigurationError(
            "MINIMUM_VOLUME_HISTORY cannot exceed "
            "VOLUME_WINDOW_SIZE."
        )

    queue_settings = QueueSettings(
        snapshot_queue_size=_get_int(
            "SNAPSHOT_QUEUE_SIZE",
            500,
            minimum=1,
            maximum=100_000,
        ),
        event_queue_size=_get_int(
            "EVENT_QUEUE_SIZE",
            500,
            minimum=1,
            maximum=100_000,
        ),
        flush_interval_seconds=_get_float(
            "SNAPSHOT_FLUSH_INTERVAL_SECONDS",
            1.0,
            minimum=0.1,
            maximum=60.0,
        ),
        storage_check_interval_seconds=_get_float(
            "STORAGE_CHECK_INTERVAL_SECONDS",
            60.0,
            minimum=1.0,
            maximum=86_400.0,
        ),
    )

    storage_settings = StorageSettings(
        warning_mb=_get_decimal(
            "STORAGE_WARNING_MB",
            "800",
            minimum=Decimal("0"),
        ),
        critical_mb=_get_decimal(
            "STORAGE_CRITICAL_MB",
            "950",
            minimum=Decimal("0"),
        ),
        limit_mb=_get_decimal(
            "STORAGE_LIMIT_MB",
            "1000",
            minimum=Decimal("1"),
        ),
        retention_days=_get_int(
            "RETENTION_DAYS",
            30,
            minimum=1,
            maximum=3_650,
        ),
    )

    if not (
        storage_settings.warning_mb
        < storage_settings.critical_mb
        < storage_settings.limit_mb
    ):
        raise ConfigurationError(
            "Storage thresholds must satisfy: "
            "STORAGE_WARNING_MB < STORAGE_CRITICAL_MB "
            "< STORAGE_LIMIT_MB."
        )

    mysql_table = _get_string(
        "POWER_BI_MYSQL_TABLE",
        "fact_market_minute",
    )

    if not mysql_table.replace(
        "_",
        "",
    ).isalnum():
        raise ConfigurationError(
            "POWER_BI_MYSQL_TABLE may contain only "
            "letters, digits and underscores."
        )

    power_bi_settings = PowerBISettings(
        mysql_table=mysql_table,
        refresh_interval_minutes=_get_int(
            "POWER_BI_REFRESH_INTERVAL_MINUTES",
            1,
            minimum=1,
            maximum=1_440,
        ),
    )

    return Settings(
        application=application_settings,
        binance=binance_settings,
        mysql=mysql_settings,
        mongo=mongo_settings,
        transformer=transformer_settings,
        event_detection=event_detection_settings,
        queues=queue_settings,
        storage=storage_settings,
        power_bi=power_bi_settings,
    )


SETTINGS: Final[Settings] = _load_settings()


APP_CONFIG: Final[dict[str, Any]] = {
    "name": SETTINGS.application.name,
    "environment": SETTINGS.application.environment,
    "version": SETTINGS.application.version,
    "log_level": SETTINGS.application.log_level,
    "debug": SETTINGS.application.debug,
}


BINANCE_WS_URL: Final[str] = (
    SETTINGS.binance.websocket_base_url
)

SYMBOLS: Final[list[str]] = list(
    SETTINGS.binance.symbols
)


BINANCE_CONFIG: Final[dict[str, Any]] = {
    "websocket_base_url": (
        SETTINGS.binance.websocket_base_url
    ),
    "symbols": list(
        SETTINGS.binance.symbols
    ),
    "stream_name": SETTINGS.binance.stream_name,
    "connect_timeout_seconds": (
        SETTINGS.binance.connect_timeout_seconds
    ),
    "callback_timeout_seconds": (
        SETTINGS.binance.callback_timeout_seconds
    ),
    "reconnect_initial_delay_seconds": (
        SETTINGS.binance.reconnect_initial_delay_seconds
    ),
    "reconnect_max_delay_seconds": (
        SETTINGS.binance.reconnect_max_delay_seconds
    ),
    "max_buffer_size": (
        SETTINGS.binance.max_buffer_size
    ),
    "max_message_size_bytes": (
        SETTINGS.binance.max_message_size_bytes
    ),
}


MYSQL_CONFIG: Final[dict[str, Any]] = {
    "host": SETTINGS.mysql.host,
    "port": SETTINGS.mysql.port,
    "user": SETTINGS.mysql.user,
    "password": SETTINGS.mysql.password,
    "database": SETTINGS.mysql.database,
    "connect_timeout": SETTINGS.mysql.connect_timeout,
    "read_timeout": SETTINGS.mysql.read_timeout,
    "write_timeout": SETTINGS.mysql.write_timeout,
    "charset": SETTINGS.mysql.charset,
}


MONGO_CONFIG: Final[dict[str, Any]] = {
    "uri": SETTINGS.mongo.uri,
    "database": SETTINGS.mongo.database,
    "server_selection_timeout_ms": (
        SETTINGS.mongo.server_selection_timeout_ms
    ),
    "connect_timeout_ms": (
        SETTINGS.mongo.connect_timeout_ms
    ),
    "socket_timeout_ms": (
        SETTINGS.mongo.socket_timeout_ms
    ),
    "max_pool_size": (
        SETTINGS.mongo.max_pool_size
    ),
    "min_pool_size": (
        SETTINGS.mongo.min_pool_size
    ),
    "retry_writes": SETTINGS.mongo.retry_writes,
    "raw_sample_ttl_seconds": (
        SETTINGS.mongo.raw_sample_ttl_seconds
    ),
}


PIPELINE_CONFIG: Final[dict[str, Any]] = {
    "minute_interval": (
        SETTINGS.transformer.minute_interval_seconds
    ),
    "allowed_lateness_seconds": (
        SETTINGS.transformer.allowed_lateness_seconds
    ),
    "snapshot_history_size": (
        SETTINGS.transformer.snapshot_history_size
    ),
    "deduplicate_trades": (
        SETTINGS.transformer.deduplicate_trades
    ),
    "store_partial_snapshots": (
        SETTINGS.transformer.store_partial_snapshots
    ),

    "price_spike_threshold": (
        SETTINGS.event_detection.price_spike_threshold_pct
    ),
    "high_volume_threshold": (
        SETTINGS.event_detection.high_volume_multiplier
    ),
    "high_volatility_threshold": (
        SETTINGS.event_detection
        .high_volatility_threshold_pct
    ),
    "volume_window_size": (
        SETTINGS.event_detection.volume_window_size
    ),
    "minimum_volume_history": (
        SETTINGS.event_detection.minimum_volume_history
    ),

    "extractor_buffer_size": (
        SETTINGS.binance.max_buffer_size
    ),
    "snapshot_queue_size": (
        SETTINGS.queues.snapshot_queue_size
    ),
    "event_queue_size": (
        SETTINGS.queues.event_queue_size
    ),
    "flush_interval_seconds": (
        SETTINGS.queues.flush_interval_seconds
    ),
    "storage_check_interval_seconds": (
        SETTINGS.queues.storage_check_interval_seconds
    ),

    "storage_limit_mb": (
        SETTINGS.storage.limit_mb
    ),
    "alert_storage_mb": (
        SETTINGS.storage.warning_mb
    ),
    "critical_storage_mb": (
        SETTINGS.storage.critical_mb
    ),
    "retention_days": (
        SETTINGS.storage.retention_days
    ),
}


POWER_BI_CONFIG: Final[dict[str, Any]] = {
    "mysql_table": SETTINGS.power_bi.mysql_table,
    "refresh_interval_minutes": (
        SETTINGS.power_bi.refresh_interval_minutes
    ),
}


def get_safe_settings() -> dict[str, Any]:
    """
    Return configuration values safe for logs and health endpoints.

    Passwords and complete connection URIs are intentionally excluded.
    """
    parsed_mongo_uri = urlparse(
        SETTINGS.mongo.uri
    )

    return {
        "application": {
            "name": SETTINGS.application.name,
            "environment": (
                SETTINGS.application.environment
            ),
            "version": SETTINGS.application.version,
            "log_level": (
                SETTINGS.application.log_level
            ),
            "debug": SETTINGS.application.debug,
        },
        "binance": {
            "websocket_host": urlparse(
                SETTINGS.binance.websocket_base_url
            ).hostname,
            "symbols": list(
                SETTINGS.binance.symbols
            ),
            "stream_name": (
                SETTINGS.binance.stream_name
            ),
        },
        "mysql": {
            "host": SETTINGS.mysql.host,
            "port": SETTINGS.mysql.port,
            "database": SETTINGS.mysql.database,
            "user": SETTINGS.mysql.user,
        },
        "mongo": {
            "host": parsed_mongo_uri.hostname,
            "port": parsed_mongo_uri.port,
            "database": SETTINGS.mongo.database,
        },
        "transformer": {
            "minute_interval_seconds": (
                SETTINGS.transformer
                .minute_interval_seconds
            ),
            "allowed_lateness_seconds": (
                SETTINGS.transformer
                .allowed_lateness_seconds
            ),
            "store_partial_snapshots": (
                SETTINGS.transformer
                .store_partial_snapshots
            ),
        },
        "storage": {
            "warning_mb": str(
                SETTINGS.storage.warning_mb
            ),
            "critical_mb": str(
                SETTINGS.storage.critical_mb
            ),
            "limit_mb": str(
                SETTINGS.storage.limit_mb
            ),
            "retention_days": (
                SETTINGS.storage.retention_days
            ),
        },
    }