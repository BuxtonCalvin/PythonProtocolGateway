# bridge transport module for TimescaleDB, implementing a high-performance transport that writes protocol metrics
# to a TimescaleDB database with support for hypertables, continuous aggregates, rollups, and persistent disk backlog.
"""
File: timescaledb.py
timescaledb transport bridge module is free software written by Kevin Burke: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as published by
the Free Software Foundation, either version 3 of the License, or any later version.

Copyright 2026 Kevin Burke

timescaledb transport bridge module is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

You can find a copy of the GNU Affero General Public License in the documentation/bridges/timescaledb folder.
If not, see <https://www.gnu.org>.
----------------------------------------------------------------------------------------------------------------------
timescaledb transport bridge module (with rollup continuous aggregates) and persistent disk backlog.
python > 3.10 is required, 3.13 is recommended for best performance and latest features.
The transport uses the latest SQLAlchemy version for database interactions and supports automatic schema management,
including dynamic column creation based on the protocol registry, hypertable setup, and continuous
aggregate rollups for efficient querying of historical data.

Features:
 - Auto-create database (default "solar", configurable)
 - device_info (multi unique transport scraper devices)
 - device_metrics_wide hypertable per protocol with dynamic column creation based on protocol registry metrics,
        with intelligent type mapping and coercion for optimal compression
 - device_metrics_narrow hypertable
 - Hypertable compression & retention (idempotent)
 - Continuous aggregates rollups: hourly_rollup, daily_rollup, weekly_rollup, monthly_rollup with hierarchical dependencies and policies
 - Async flushing + persistent disk backlog
 - OS-local timestamps

Terminology:
Continuous Aggregates	The official TimescaleDB feature name. It's an automatically and incrementally updated
    materialized SQL view that pre-computes aggregate data (e.g., averages, sums over a time window: minute, hour, day, week or month)
    from raw data and stores it in a separate hypertable view.

Continuous Rollups	 This term refers to the process of downsampling data into successively
    coarser time granularities (e.g., from raw data to hourly summaries, then to daily summaries, then to weekly summaries,
    then to monthly summaries). This is achieved using the hierarchical continuous aggregates feature,
    where a continuous aggregate based on the output of a previous one is created. For example, a daily rollup continuous
    aggregate would be defined based on the hourly rollup continuous aggregate, and so on.

"""

from __future__ import annotations

import json
import logging
import math
import queue
import re
import sqlite3
import threading
import time
import warnings
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Lock, RLock
from typing import (
    Any,
    Callable,
    Generator,
    List,
    Literal,
    Optional,
    Protocol,
    Sequence,
    Tuple,
    Union,
)

from sqlalchemy import (
    Boolean,
    Connection,
    DateTime,
    Engine,
    Float,
    ForeignKey,
    Identity,
    Index,
    Inspector,
    Integer,
    PrimaryKeyConstraint,
    Result,
    Row,
    Table,
    Text,
    TextClause,
    UniqueConstraint,
    create_engine,
    inspect,
    text,
)
from sqlalchemy.dialects.postgresql import Insert
from sqlalchemy.dialects.postgresql import insert as pg_insert

# from sqlalchemy.engine.interfaces import ReflectedColumn
from sqlalchemy.engine.interfaces import ReflectedColumn
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    mapped_column,
    relationship,
    sessionmaker,
)
from sqlalchemy.orm.session import Session
from sqlalchemy.sql.dml import ReturningInsert
from tzlocal import get_localzone_name

from classes.protocol_settings import (
    Registry_Type,
    registry_map_entry,
)
from defs.common import TransportSettings

from .transport_base import transport_base


class TimezoneEngine:
    def __init__(self) -> None:
        self.use_utc: bool = False
        self.machine_timezone: str = "UTC"

    def configure(self, use_utc: bool) -> None:
        """Called once at application startup to lock in the timezone configuration."""
        self.use_utc = use_utc
        self.machine_timezone = "UTC" if use_utc else get_localzone_name()

# Instantiate the single, global state engine
_TZengine = TimezoneEngine()

def configure_application_timezone(use_utc: bool) -> None:
    """Public function to set up the global timestamp configuration."""
    _TZengine.configure(use_utc)

def get_machine_timezone() -> str:
    """Returns the correct timezone string ('UTC' or IANA name) for TimescaleDB."""
    return _TZengine.machine_timezone

def _now_tz() -> datetime:
    """
    The universal timestamp generator.
    For SQLAlchemy models and all application logic.
    """
    if _TZengine.use_utc:
        return datetime.now(timezone.utc)
    return datetime.now().astimezone()

# base class for all tables.
class Base(DeclarativeBase):
    pass

class ProtocolRegistry(Base):
    __tablename__ = "protocol_registry"

    protocol_id: Mapped[int] = mapped_column(Integer, Identity(cache=1),primary_key=True, autoincrement=True)
    protocol_name: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    wide_table_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    # None = narrow only (> WIDE_TABLE_COLUMN_LIMIT 160 metrics)

    metric_count: Mapped[int] = mapped_column(Integer, default=0)

    # Rollup state — enough for RollupManager to rediscover and manage views
    rollup_prefix: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    # e.g. "rollup_wide__eg4_18kpv" — the base name views are derived from
    # None if rollups are disabled or not yet set up for this protocol

    rollup_enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    rollup_setup_complete: Mapped[bool] = mapped_column(Boolean, default=False)
    # False until setup_narrow_rollup completes successfully — allows restart recovery

    last_refresh_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    # Tracks when rollups were last successfully refreshed per protocol

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now_tz)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now_tz, onupdate=_now_tz)

    metriccatalog: Mapped[List["MetricCatalog"]] = relationship(
        "MetricCatalog", back_populates="protocolregistry"
    )
    deviceinfo: Mapped[List["DeviceInfo"]] = relationship(
        "DeviceInfo", back_populates="protocolregistry"
    )

class MetricCatalog(Base):
    __tablename__: str = "metric_catalog"

    catalog_id: Mapped[int] = mapped_column(Integer, Identity(cache=1), primary_key=True, autoincrement=True)
    protocol_id: Mapped[int] = mapped_column(ForeignKey("protocol_registry.protocol_id"), nullable=False)
    metric_name: Mapped[str] = mapped_column(Text, nullable=False)
    clean_column_name: Mapped[str] = mapped_column(Text, nullable=False)
    data_type: Mapped[str] = mapped_column(Text, default='double precision', nullable=False)
    unit_mod: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now_tz, onupdate=_now_tz)
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    __table_args__ = (
        # Same metric name can exist in multiple protocols
        UniqueConstraint('protocol_id', 'metric_name', name='uq_metric_catalog_protocol_metric'),
        # Same clean column name can exist in multiple protocols' wide tables
        # but must be unique within a protocol
        UniqueConstraint('protocol_id', 'clean_column_name', name='uq_metric_catalog_protocol_column'),)

    protocolregistry: Mapped["ProtocolRegistry"] = relationship(
        "ProtocolRegistry", back_populates="metriccatalog")
class DeviceInfo(Base):
    __tablename__: str = "device_info"

    device_info_id: Mapped[int] = mapped_column(Integer, Identity(cache=1), primary_key=True, autoincrement=True)
    protocol_id: Mapped[Optional[int]] = mapped_column(ForeignKey("protocol_registry.protocol_id"), nullable=True, index=True)
    device_identifier: Mapped[Optional[str]] = mapped_column(Text, index=True)
    device_serial_number: Mapped[Optional[str]] = mapped_column(Text)
    device_name: Mapped[Optional[str]] = mapped_column(Text)
    device_manufacturer: Mapped[Optional[str]] = mapped_column(Text)
    device_model: Mapped[Optional[str]] = mapped_column(Text)
    device_firmware: Mapped[Optional[str]] = mapped_column(Text)
    device_location: Mapped[Optional[str]] = mapped_column(Text)
    transport: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    # Actual scraped metric count for this device (post variable_mask
    # filtering), captured once at _get_or_create_device's first upsert
    # for the transport each session -- not protocol_registry.metric_
    # count, which is a schema-wide (wide table column count) figure that
    # wrongly assumes every device of a protocol scrapes the same metric
    # set. Different devices of the same protocol can have very different
    # variable_mask configurations (e.g. one battery gets the full
    # register list, another only a curated few) -- this column reflects
    # each device's own actual figure. Used by RollupManager's dynamic
    # chunk/compression sizing (see get_dynamic_raw_table_settings_helper
    # / get_dynamic_view_settings_helper).
    metric_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now_tz)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now_tz, onupdate=_now_tz)

    # Relationships
    protocolregistry: Mapped[Optional["ProtocolRegistry"]] = relationship(
        "ProtocolRegistry", back_populates="deviceinfo")
    devicemetricsnarrow: Mapped[List["DeviceMetricsNarrow"]] = relationship(
        "DeviceMetricsNarrow", back_populates="deviceinfo")

class DeviceMetricsNarrow(Base):
    __tablename__: str = "device_metrics_narrow"

    # In TimescaleDB/SQLAlchemy, mark columns that are part of PK as primary_key=True in mapped_column
    # as well as defining the constraint in __table_args__ to ensure correct behavior and indexing.
    # metric_value is forced float for all numerics and booleans. Ascii values are stored in metric_ascii column,
    # which is nullable and only populated for string types.
    m_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now_tz, primary_key=True)
    device_info_id: Mapped[int] = mapped_column(ForeignKey("device_info.device_info_id"), primary_key=True)
    metric_name: Mapped[str] = mapped_column(Text, primary_key=True)
    metric_value: Mapped[float] = mapped_column(Float)
    metric_ascii: Mapped[str] = mapped_column(Text, nullable=True)

    __table_args__ = (
        PrimaryKeyConstraint('m_time', 'device_info_id', 'metric_name', name='device_metrics_narrow_pkey'),
        # create the index on device_metrics_narrow for faster grafana lookups by device_info_id and metric_name, descending by m_time
        Index(
            "device_metrics_narrow_lookup_idx",
            "device_info_id",
            "metric_name",
            m_time.desc(),
        )
    )

    deviceinfo: Mapped["DeviceInfo"] = relationship("DeviceInfo", back_populates="devicemetricsnarrow")

# The TimescaleDBConnectionManager class encapsulates the SQLAlchemy engine and connection pool management for a single TimescaleDB database.
class TimescaleDBConnectionManager:
    """
    Owns the SQLAlchemy engine and connection pool for a single TimescaleDB database.
    Shared across all protocol instances pointing at the same database.

    Usage:
        manager = TimescaleDBConnectionManager(host, port, database, username, password)
        manager.connect()

        protocol_a = timescaledb(settings_a, connection_manager=manager)
        protocol_b = timescaledb(settings_b, connection_manager=manager)

        manager.dispose()  # clean shutdown of shared pool
    """

    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        username: str,
        password: str,
        pool_size: int = 5,
        max_overflow: int = 10,
        pool_recycle: int = 3600,
        log: logging.Logger | None = None
    ) -> None:

        self.host: str = host
        self.port: int = port
        self.database: str = database
        self.username: str = username
        self.password: str = password
        self.pool_size: int = pool_size
        self.max_overflow: int = max_overflow
        self.pool_recycle: int = pool_recycle
        self._log: logging.Logger = log or logging.getLogger(__name__)

        self._engine: Engine | None = None
        self._lock: threading.RLock = threading.RLock()
        self._ref_count: int = 0  # tracks how many protocol instances are using this manager
        self._connected: bool = False

    @classmethod
    def from_settings(cls, settings: TransportSettings, log: logging.Logger | None = None ) -> "TimescaleDBConnectionManager":
        """
        Construct a ConnectionManager directly from a configparser SectionProxy.
        Connection settings are read from the section, with the same defaults
        as the timescaledb class uses.
        """
        return cls(
            host=settings.get("host", fallback="localhost"),
            port=settings.getint("port", fallback=5432),
            database=settings.get("database", fallback="solar"),
            username=settings.get("username", fallback=""),
            password=settings.get("password", fallback=""),
            pool_size=settings.getint("pool_size", fallback=5),
            max_overflow=settings.getint("max_overflow", fallback=10),
            pool_recycle=settings.getint("pool_recycle", fallback=3600),
            log=log
        )

    # -------------------------
    # Connection lifecycle
    # -------------------------

    def connect(self) -> None:
        """
        Create the shared engine and verify the connection.
        Safe to call multiple times — only creates the engine once.
        """
        with self._lock:
            if self._engine is not None:
                return
            try:
                self._create_database_if_missing()
                url: str = (
                    f"postgresql+psycopg2://{self.username}:{self.password}"
                    f"@{self.host}:{self.port}/{self.database}"
                )
                self._engine = create_engine(
                    url,
                    pool_pre_ping=True,
                    future=True,
                    pool_recycle=self.pool_recycle,
                    pool_size=self.pool_size,
                    max_overflow=self.max_overflow
                )
                with self._engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                self._connected = True
                self._log.info(f"ConnectionManager: connected to '{self.database}'")
            except OperationalError as e:
                self._connected = False
                self._log.error(f"ConnectionManager: failed to connect: {e}")
                self._log.error(f"❌ [COMMUNICATION LOST] --- Name: {self.database} ---")
                raise

    def dispose(self) -> None:
        """
        Dispose the shared engine. Should only be called when all protocol
        instances have been closed — use ref counting to enforce this.
        """
        with self._lock:
            if self._ref_count > 0:
                self._log.warning(
                    f"ConnectionManager: dispose() called with {self._ref_count} "
                    f"active references — deferring."
                )
                return
            if self._engine is not None:
                self._engine.dispose()
                self._engine = None
                self._connected = False
                self._log.info("ConnectionManager: engine disposed.")

    # -------------------------
    # Session factory
    # -------------------------

    def make_session_factory(self) -> sessionmaker[Session]:
        """
        Returns a new sessionmaker bound to the shared engine.
        Each protocol instance calls this once during its own init
        and holds the returned factory for its lifetime.
        """
        if self._engine is None:
            raise RuntimeError("ConnectionManager: connect() must be called before make_session_factory()")
        return sessionmaker(
            bind=self._engine,
            autocommit=False,
            expire_on_commit=False,
            autoflush=False
        )

    # -------------------------
    # Reference counting
    # -------------------------

    def register(self) -> None:
        """Called by each protocol instance during __init__."""
        with self._lock:
            self._ref_count += 1
            self._log.debug(f"ConnectionManager: registered instance (total: {self._ref_count})")

    def unregister(self) -> None:
        """
        Called by each protocol instance during close().
        Automatically disposes the engine when the last instance unregisters.
        """
        with self._lock:
            self._ref_count = max(0, self._ref_count - 1)
            self._log.debug(f"ConnectionManager: unregistered instance (total: {self._ref_count})")
            if self._ref_count == 0:
                self._log.info("ConnectionManager: last instance unregistered — disposing engine.")
                self.dispose()

    # -------------------------
    # Engine access
    # -------------------------

    @property
    def engine(self) -> Engine:
        if self._engine is None:
            raise RuntimeError("ConnectionManager: engine is not initialized. Call connect() first.")
        return self._engine

    @property
    def connected(self) -> bool:
        return self._connected


    def _create_database_if_missing(self) -> None:
        default_url: str = (
            f"postgresql+psycopg2://{self.username}:{self.password}"
            f"@{self.host}:{self.port}/postgres"
        )
        try:
            default_engine: Engine = create_engine(default_url, isolation_level="AUTOCOMMIT", pool_pre_ping=True)
            with default_engine.connect() as conn:
                row: Row[Any] | None = conn.execute(
                    text("SELECT 1 FROM pg_database WHERE datname = :d"),
                    {"d": self.database}
                ).fetchone()
                if not row:
                    self._log.info(f"Database '{self.database}' not found — creating")
                    conn.execute(text(f'CREATE DATABASE "{self.database}"'))
                    self._log.info(f"Database '{self.database}' created")
                else:
                    self._log.debug(f"Database '{self.database}' already exists")
            default_engine.dispose()
        except Exception as e:
            self._log.error(f"Failed to verify/create database '{self.database}': {e}")
            raise


# TimescaleDB transport bridge class
class timescaledb(transport_base):


    transport_type = "bridge"
    """
    TimescaleDB transport bridge with hypertable, continuous aggregates and rollup support.
    The class uses background threads for auto-refresh of rollups and stale data detection.
    Uses a global sqlalchemy session for most database operations.
    """
    __version__: str = "1.0.0"

    # -------------------------
    # Default settings (overridable by settings SectionProxy)
    # -------------------------
    # Database connection
    host: str = "localhost"
    port: int = 5432
    database: str = "solar"
    username: str = ""
    password: str = ""

    force_float: bool = True

    timescale_type_map: dict[str, str] = {
        # 8-bit & 16-bit Unsigned/Signed -> SMALLINT or INTEGER
        # USHORT is mapped to INTEGER to accommodate values > 32767
        "BYTE": "SMALLINT",
        "USHORT": "INTEGER",
        "SHORT": "SMALLINT",
        # 32-bit Unsigned/Signed
        "UINT": "BIGINT",
        "INT": "INTEGER",
        "UINT64": "NUMERIC",
        "ACC32": "BIGINT",
        # Flags & Bits (Integers for Delta-Delta compression)
        "_8BIT_FLAGS": "SMALLINT",
        "_16BIT_FLAGS": "INTEGER",
        "_32BIT_FLAGS": "BIGINT",
        # Strings (Dictionary compression)
        "ASCII": "TEXT",
        "HEX": "TEXT",
        "STRING": "TEXT",
        "STRING16": "TEXT",
        "STRING32": "TEXT",
        "TEXT": "TEXT",
        "_1BIT": "BOOLEAN",
        "BOOLEAN": "BOOLEAN",

        # Unsigned Bit-lengths (_2BIT to _15BIT)
        **{f"_{i}BIT": "SMALLINT" for i in range(2, 16)},
        "_16BIT": "INTEGER", # 16-bit unsigned needs INTEGER

        # Signed Bits (_2SBIT to _16SBIT)
        **{f"_{i}SBIT": "SMALLINT" for i in range(2, 17)},

        # Signed Magnitude (_2SMBIT to _16SMBIT)
        **{f"_{i}SMBIT": "SMALLINT" for i in range(2, 17)},

        # Float (Add these to the register maps if you use them - Gorilla compression)
        "FLOAT32": "REAL",
        "FLOAT64": "DOUBLE PRECISION"
    }

    # Type Coercion based on timescale_type_map values for field definitions.
    # data is coerced to improve compression in metrics' tables.
    INT_TYPES: set[str] = {"SMALLINT", "INTEGER", "BIGINT"}
    FLOAT_TYPES: set[str] = {"REAL", "DOUBLE PRECISION", "NUMERIC", "FLOAT"}
    WIDE_TABLE_COLUMN_LIMIT = 160  # Max columns in a wide table before falling back to narrow table

    # persistent storage/backlog settings. Default folder name and file name are the same but can be user configured.
    enable_persistent_storage: bool = True
    backlog_storage_path: Path = Path(__file__).resolve().parent.parent.parent / "backlogs"
    backlog_file_name: str  = "timescaledb_backlog"
    max_backlog_size: int = 10000
    max_backlog_age: int = 86400   # seconds

    # reconnect/backoff
    reconnect_attempts: int = 5
    reconnect_delay: int = 5
    use_exponential_backoff: bool = True
    max_reconnect_delay: int = 300
    tsdb_connected = False

    ### Hypertable and rollup settings defaults.  These can be overridden by settings SectionProxy, but defaults are provided here for clarity.

    # whether to attempt to migrate existing data when creating hypertables and rollups.  Set to False to skip migration and start fresh with new schema.
    migrate_data: bool = True
    # whether to enable compression on hypertables at startup.
    #Compression policies are created regardless, but this controls whether existing data is compressed on init.
    enable_compression: bool = True
    enable_dynamic_chunk_sizing: bool = True  # whether to dynamically size hypertable chunks based on the number of metrics and devices for a given protocol.
    enable_rollups: bool = True  # whether to create continuous aggregate rollups on init and start the auto-refresh thread.
    auto_refresh_interval: int = 21600  # seconds (default 6 hours), auto-refresh rollup
    enable_auto_refresh: bool = True  # whether to auto-refresh rollups periodically
    drop_after: str = "1 year"  # default retention policy for raw data in tables and views, can be overridden by settings SectionProxy

    # stale data settings and fields.  Stale data is read per transport batch based on the timestamp of the last row of metrics data received.
    # If the current time exceeds that timestamp by more than the stale_data_timeout, then the transport will consider the data to be stale
    # and trigger a cleanup of incomplete batches in the database, as well as an optional upstream reconnect if request_upstream_reconnect
    # callback is set by the user.
    stale_data_timeout: int = 300       # seconds before considering data stale for incomplete batch cleanup
    max_stale_attempts: int = 3         # Number of times to read the data stream to determine if it's stale.
    retry_delay_mins: int = 5

    current_metric_count: int = 0

    # write_requires_complete_cycle is used to prevent writing incomplete batches of data to the database,
    # which can cause issues with rollup continuous aggregates and data integrity.
    # When True, the transport will only write data to the database once it has received a complete cycle
    # of metrics for a given protocol, as determined by the registry map.  This allows the transport
    # to ensure that all expected metrics for a given timestamp have been received before writing to the database,
    # which is important for maintaining data integrity and ensuring that rollups are accurate.
    write_requires_complete_cycle: bool = True


    def __init__(self, settings: TransportSettings, connection_manager: TimescaleDBConnectionManager | None = None) -> None:

        """
        Initialize the TimescaleDB transport bridge.

        Args:
            settings (SectionProxy): Configuration section containing database and transport options.

        Configuration options:
            - auto_refresh_interval (int): Seconds between rollup refreshes (default: 21600)
            - backlog_storage_path (str): Path for backlog files (default: "parent/timescaledb_backlog")
            - database (str): Database name (default: "solar")
            - device_name (str): Name for the bridge (default: "TimeScaleDB MPG Bridge")
            - enable_auto_refresh (bool): Enable periodic rollup refresh (default: True)
            - enable_compression (bool): Enable compression on hypertables at startup (default: True)
            - enable_persistent_storage (bool): Enable disk backlog (default: True)
            - force_float (bool): Force metric values to float (default: True)
            - host (str): Database host (default: "localhost")
            - hypertable_defaults: Dicts for hypertable narrow and wide creation and policies
            - max_backlog_age (int): Max age (seconds) for backlog points (default: 86400) 24 hours
            - max_backlog_size (int): Max backlog points (default: 10000)
            - max_reconnect_delay (int): Max reconnect delay (default: 300)
            - migrate_data: whether to attempt to migrate existing data when creating hypertables and rollups. Set to False to skip migration and start fresh with new schema.
            - password (str): Database password
            - port (int): Database port (default: 5432)
            - reconnect_attempts (int): Max reconnect attempts (default: 5)  if set to 0, no limit.
            - reconnect_delay (int): Initial reconnect delay (default: 5)
            - rollup_defaults: dict for rollup settings
            - stale_data_timeout (int): Seconds before considering data stale for incomplete batch cleanup (default: 300)
            - use_exponential_backoff (bool): Use exponential backoff (default: True)
            - use_utc_timestamp (bool): Use UTC timezone for all timestamps instead of local machine timezone (default: False)
            - username (str): Database username

        Thread behavior:
            - Starts a background thread for batch flushing of metrics.
            - Starts a background thread for periodic rollup refreshes.
            - Thread safety is ensured for internal operations.
        """

        """
        1.0.0 Initial Production Version

        """
        # Per-protocol state — populated as init_bridge is called for each scraper
        self._registered_protocols: set[str] = set()

        # Maps protocol_name -> wide_table_name (or None if narrow-only)
        self._protocol_wide_table_map: dict[str, str | None] = {}

        # Maps protocol_name -> metric_mapping dict
        # Each protocol has its own metric_name -> (clean_column_name, data_type) map
        self.protocol_metric_mappings: dict[str, dict[str, tuple[str, str]]] = {}
        # -------------------------
        # load user settings from SectionProxy
        # -------------------------

        # load connect configuration settings
        self.host = settings.get("host", fallback=self.host)
        self.port = settings.getint("port", fallback=self.port)
        self.database = settings.get("database", fallback=self.database)
        self.username = settings.get("username", fallback=self.username)
        self.password = settings.get("password", fallback=self.password)

        if not self.username:
            raise ValueError("TimeScaleDB User is not set")

        if not self.password:
            warnings.warn("TimeScaleDB Password is empty", RuntimeWarning)

        # load reconnect/backoff settings
        self.use_exponential_backoff: bool = settings.getboolean("use_exponential_backoff", fallback=self.use_exponential_backoff)
        self.max_reconnect_delay: int = settings.getint("max_reconnect_delay", fallback=self.max_reconnect_delay)
        self.reconnect_attempts: int = settings.getint("reconnect_attempts", fallback=self.reconnect_attempts)
        self.reconnect_delay: int = settings.getint("reconnect_delay", fallback=self.reconnect_delay)
        self.force_float: bool = settings.getboolean("force_float", fallback=self.force_float)

        # stale data settings
        self.stale_data_timeout: int = settings.getint("stale_data_timeout", fallback=self.stale_data_timeout)
        # wait for complete data to write to db.
        self.write_requires_complete_cycle: bool = settings.getboolean("write_requires_complete_cycle", fallback=self.write_requires_complete_cycle)

        # UTC timestamp mode setting - set context for this transport instance
        self.use_utc_timestamp: bool = settings.getboolean("use_utc_timestamp", fallback=False)
        configure_application_timezone(self.use_utc_timestamp)
        self.machine_timezone: str = get_machine_timezone()

        # persistent backlog settings
        project_root: Path = Path(__file__).resolve().parents[2]
        # Force path to look relative by stripping leading slashes/drives
        self.enable_persistent_storage: bool = settings.getboolean("enable_persistent_storage", fallback=self.enable_persistent_storage)
        self.backlog_storage_path_value: str | Path = settings.get("backlog_storage_path", fallback=self.backlog_storage_path)
        if not isinstance(self.backlog_storage_path_value, Path):
            clean_setting: str = self.backlog_storage_path_value.lstrip("\\/")
            self.backlog_storage_path_value = Path(clean_setting)

        self.backlog_storage_path = (project_root /self.backlog_storage_path_value).resolve()

        self.backlog_file_name: str = settings.get("backlog_file_name", fallback=self.backlog_file_name)
        self.max_backlog_size: int = settings.getint("max_backlog_size", fallback=self.max_backlog_size)
        self.max_backlog_age: int = settings.getint("max_backlog_age", fallback=self.max_backlog_age)

        # hypertable / rollup options that are user defined.  These are sent to the rollup manager class
        # which will handle the hypertable and rollup creation and maintenance based on these settings.

        self.rollup_policy: dict[str,Any] = {
            # Hypertable Settings
            "current_metric_count": self.current_metric_count,
            "tsdb_connected": self.tsdb_connected,
            "machine_timezone": self.machine_timezone,
            "drop_after": settings.get("drop_after", fallback=self.drop_after),
            "migrate_data": settings.getboolean("migrate_data", fallback=self.migrate_data),
            "enable_compression": settings.getboolean("enable_compression", fallback=self.enable_compression),
            "enable_dynamic_chunk_sizing": settings.getboolean("enable_dynamic_chunk_sizing", fallback=self.enable_dynamic_chunk_sizing),
            # Rollup Settings
            "auto_refresh_interval": settings.getint("auto_refresh_interval", fallback=self.auto_refresh_interval),
            "enable_auto_refresh": settings.getboolean("enable_auto_refresh", fallback=self.enable_auto_refresh),
            "enable_rollups": settings.getboolean("enable_rollups", fallback=self.enable_rollups),
        }

        # Explicitly set bridge name if not set by user, since this transport doesn't have a device name from an
        # upstream transport to pull from.
        self.device_name: str = settings.get("device_name", fallback="TimescaleDB MPG Bridge")

        super().__init__(settings)

        # registry map for the last batch of data received, used for stale data detection.
        self._stale_registry: dict[str, dict[str, Any]] = {}
        self._last_cleanup_time: float = time.time()

        self._verified_devices: set[str] = set()
        # transport_name -> device_info_id cache to minimize DB lookups for device_info_id during batch writes
        self._device_cache: dict[str, int] = {}

        # end user settings
        #*********************************

        # Instance-scoped metadata — isolates schema per connection
        #self._metadata: MetaData = MetaData()
        self._base: DeclarativeBase  # will be constructed after engine is ready

        # load all registry metrics' map.
        self.registry_metrics: dict[Registry_Type, List[registry_map_entry]]  = {}

        # keyed as: self._wide_columns_cache[wide_table_name] = {col1, col2, ...}
        self._wide_columns_cache: dict[str, set[str]] = {}
        self.device_info_id: int | None = None  # will be set after data scrape, per transport batch.

        # SQLAlchemy init runtime
        # If a shared manager is provided, use it.
        # Otherwise create an instance-scoped one.
        # number of extra connections to add to the pool size beyond the number of scrapers,
        # to ensure there are enough connections for background threads and other operations.

        #  multiple scrapers registering via init_bridge can each trigger schema work +
        # # rollup refresh thread + schema/init_bridge thread + reconnect thread -- pool overhead.
        POOL_OVERHEAD = 3
        pool_size: int = max(2, self.scraper_count + POOL_OVERHEAD)

        if connection_manager is not None:
            self._connection_manager: TimescaleDBConnectionManager = connection_manager
        else:
            self._connection_manager = TimescaleDBConnectionManager(
                host=self.host,
                port=self.port,
                database=self.database,
                username=self.username,
                password=self.password,
                pool_size=pool_size,
                log=self._log
            )
            self._connection_manager.connect()

        self._connection_manager.register()

        # Engine and SessionFactory come from the manager
        self.engine: Engine = self._connection_manager.engine
        self.SessionFactory: sessionmaker[Session] = self._connection_manager.make_session_factory()

        # -------------------------
        # threading
        # -------------------------

        # Initialize async flush queue and worker thread.  Start it here so it's ready at full init.
        self._flush_queue: queue.Queue[dict[str, Any] | None] = queue.Queue(maxsize=0)
        self._flush_thread = threading.Thread(target=self._flush_worker, daemon=True, name="FlushWorker")
        self._flush_lock: Lock = threading.Lock()
        self._flush_event: threading.Event = getattr(self, "_flush_event", threading.Event())
        # event used to stop all threads
        self._stop_event: threading.Event = getattr(self, "_stop_event", threading.Event())

        # init runtime backoff settings for connection attempts
        self._reconnect_lock: RLock = threading.RLock()     # prevents multiple concurrent TSDB reconnect triggers
        self._reconnect_thread_running = False      # guard to prevent duplicate reconnect threads
        self._stop_reconnect_event:  threading.Event = getattr(self, "_stop_reconnect_event", threading.Event())

        self.migration_in_progress = threading.Event()  # event to pause flushes during rollup migration

        # Protects the BacklogManager (the list and the .jsonl file).
        self._backlog_lock: RLock = threading.RLock()  # lock for backlog operations

        # Lock for protecting schema mutations and metadata reflection
        # RLock to allow nested calls within the same thread
        # Protects the SQLAlchemy Metadata and Table Identifiers (the "structure" of the Wide Table).
        self.schema_lock: RLock = threading.RLock()

        # lock for protecting device_info appends when incoming data is from two or more source transports
        self._device_lock: Lock = threading.Lock()

        # persistent backlog file and path, both the file path and the in-memory backlog file are initialized here
         # Ensure the backlog storage path is a valid directory
        if not self.backlog_storage_path.exists():
            try:
                self.backlog_storage_path.mkdir(parents=True, exist_ok=True)
            except Exception as e:
                self._log.error(f"Failed to create backlog storage directory '{self.backlog_storage_path}': {e}")
                self.send_message(
                    message=f"Error: Unable to create backlog storage directory at '{self.backlog_storage_path}'. Check logs for details.",
                    title="MPG Backlog Storage Error",
                    priority=1
                )
                raise
        # full path to backlog file
        self.backlog_file_path: Path = self.backlog_storage_path / f"{self.backlog_file_name}.db"

        self.backlog = BacklogManager(
            backlog_file_path=self.backlog_file_path,
            max_backlog_age=self.max_backlog_age,
            max_backlog_size=self.max_backlog_size,
            send_message = self.send_message,
            flush_queue=self._flush_queue,
            flush_event=self._flush_event,
            backlog_lock=self._backlog_lock,
            log=self._log
        )
        # HyperTableManager and RollupManager are initialized after the
        # database connection is established, since they need the session
        # factory and engine to set up the schema and policies. Declared
        # here (as None) so we don't AttributeError if init raises early.
        # HyperTableManager is constructed first -- it's the lower-level
        # primitive RollupManager is built on top of -- then RollupManager
        # is constructed with a reference to it. See
        # _init_or_update_rollup_manager.
        self.hypertable_mgr: "HyperTableManager | None" = None
        self.rollup_mgr: "RollupManager | None" = None

        if self.enable_persistent_storage:
            self.backlog.load_from_disk()

        # attempt tsdb connection now
        try:
            self.connect_tsdb()
        except Exception as e:
            self._log.error(f"Initial connect failed: {e}")
            self._set_tsdb_connected(conn_value = False, conn_reason = "Initial TSDB connect was not successful")
        """
            Attribute:
            request_upstream_reconnect (Callable[[], None] | None):
            Optional callback function that, if set by the user,
            will be called to trigger an source data reconnect when stale data is detected or a reconnect is required.

            The Timescaledb bridge transport itself will handle reconnecting to the TimescaleDB database when a connection issue is detected,
            but this callback allows users to also trigger a reconnect of the upstream data source (e.g., inverter or scraper)
            if they want to attempt to resolve stale data conditions or other issues that might be mitigated by refreshing the
            data source connection.
        """
        self.request_upstream_reconnect: Callable[[str], None] | None = None

    def connect_tsdb(self) -> None:
        """
        Connect to DB, build device_metrics_wide__* table from metrics data, and ensure schema/hypertable/policies exist.
        If from_transport data provided, ensure device_info insert for that transport.
        """
        try:
        # create database if missing.  Connect to standard default "postgres" database first to then check/create target database structure.
            self._log.info(f"Starting Timescaledb {self.__version__} and attempting connection:")
            try:
                with self.engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                    conn.execute(text("CREATE EXTENSION IF NOT EXISTS timescaledb_toolkit"))
                self._set_tsdb_connected(conn_value=True, conn_reason="Connection verified")
            except OperationalError as e:
                self._set_tsdb_connected(conn_value=False, conn_reason="Connection failed")
                self._log.error(f"Connection verification failed: {e}")
                self._log.error(f"❌ [COMMUNICATION LOST] --- Name: {self.transport_name} ---")
                raise

            # Clean up any orphaned locks from a previous crashed session
            # before attempting schema work that could deadlock against them.
            try:
                self._cleanup_orphaned_locks()
            except Exception as e:
                self._log.error(f"Orphaned lock cleanup failed: {e}")

            # create ORM tables. DeviceInfo, DeviceMetricsNarrow.  MetricCatalog for dynamic column names.
            # ProtocolRegistry for protocol metadata.
            try:
                self._create_tables()

            except Exception as e:
                self._log.error(f"ORM table creation error: {e}")

            try:
                self._start_flush_thread()
            except Exception as e:
                self._log.error(f"thread start failed: {e}")

        except Exception as e:
            self._set_tsdb_connected(conn_value = False, conn_reason ="Connect unsuccessful at startup")
            self._log.error(f"connect() failed: {e}")
            raise

    # Centralize state transitions for tsdb_connected helper
    def _set_tsdb_connected(self, conn_value: bool, conn_reason: str) -> None:
        """
        This helper centralizes all updates to the tsdb_connected state variable, ensuring that related state and logging are consistently handled.
        Connection status messaging is handled separately in the connect base property setter to avoid issues with calling
        send_message before the transport is fully initialized.  This method focuses on internal state management and logging.
        """
        with self._reconnect_lock:
            if self.tsdb_connected != conn_value:   # if we change state
                self.tsdb_connected: bool = conn_value  # new state
                self.connected = self.tsdb_connected  # set the MPG connected flag here to mimic central connection state.
                self.rollup_policy["tsdb_connected"] = conn_value  # set the connect status in the rollup class.
                self._log.info(f"TimescaleDB is connected -> {conn_value} ({conn_reason})")

    # -------------------------
    # reconnect/backoff
    # -------------------------
    def _attempt_reconnect(self) -> None:
        """
        Background reconnect worker. Uses class-configured retry/backoff settings.
        The method must clear self._reconnect_thread_running before returning.
        """
        try:
            self._log.warning("Auto-reconnect: connection lost, attempting to reconnect...")

            attempts: int = self.reconnect_attempts if getattr(self, "reconnect_attempts", None) is not None else 5
            delay: int = self.reconnect_delay if getattr(self, "reconnect_delay", None) is not None else 5
            use_exp = bool(getattr(self, "use_exponential_backoff", True))
            max_delay: int = getattr(self, "max_reconnect_delay", 300)

            attempt_no: int = 0
            while (attempts <= 0) or (attempt_no < attempts):  # attempts <= 0 => unlimited attempts
                self._log.info(f"Reconnect attempt {attempt_no} starting")
                if self._stop_reconnect_event.is_set():
                    self._log.info("Auto-reconnect: stop requested, exiting reconnect loop.")
                    break

                attempt_no: int = attempt_no + 1
                self._log.info(f"Reconnect attempt {attempt_no}{'' if attempts <= 0 else f'/{attempts}'} — waiting {delay}s before connect.")
                # Wait but allow early exit on stop
                waited: float = 0.0
                while waited < delay:
                    if self._stop_reconnect_event.is_set():
                        self._log.info("Auto-reconnect: stop requested during delay.")
                        break
                    sleep_chunk: float = min(1.0, delay - waited)
                    time.sleep(sleep_chunk)
                    waited += sleep_chunk
                if self._stop_reconnect_event.is_set():
                    break

                try:
                    # Attempt to re-establish DB connection.
                    with self.engine.connect() as conn:
                        conn.execute(text("SELECT 1"))
                    self._set_tsdb_connected(conn_value = True, conn_reason = f"Reconnect attempt {attempt_no} successful")
                    self._log.info(f"Reconnect attempt {attempt_no} successful.")
                except Exception as e:
                    self._set_tsdb_connected(conn_value = False, conn_reason = f"Reconnect attempt {attempt_no} unsuccessful")
                    self._log.warning(f"Reconnect attempt {attempt_no} failed during connect(): {e}")
                    self._log.warning(f"❌ [COMMUNICATION LOST] {e} --- to TimescaleDB --- Attempt {attempt_no}{'' if attempts <= 0 else f'/{attempts}'} --- Will retry in {delay}s.")

                with self._reconnect_lock:
                    tsdb_connected: bool = self.tsdb_connected

                if tsdb_connected:
                    self._log.info("Auto-reconnect: connection re-established.")

                    # Restore runtime state before allowing flush worker to resume
                    try:
                        self._rediscover_protocols()
                        # repopulate rollup views after reconnect to ensure they're up to date before any flushes occur.
                        # This is important in case the connection issue was due to a transient schema issue or if the
                        # schema changed during the disconnect.
                        if self.rollup_mgr is not None:
                            try:
                                self.rollup_mgr.repopulate_known_rollup_views()
                            except Exception as e:
                                self._log.error(f"Failed to repopulate rollup views after reconnect: {e}")
                    except Exception as e:
                        self._log.error( f"Protocol rediscovery failed after reconnect: {e}" )
                        # Non-fatal — protocols will re-register via init_bridge
                        # on next MPG restart if rediscovery fails here

                    try:
                        if getattr(self, "enable_persistent_storage", False):
                            with self._backlog_lock:
                                self.backlog.replay_to_queue()
                    except Exception as e:
                        self._log.error(f"Backlog flush failed after reconnect: {e}")
                        self.send_message(
                            message="Error: Backlog flush failed after reconnect. Check logs for details.",
                            title="MPG TimescaleDB Backlog Flush Error",
                            priority=1
                        )

                    break

                # not tsdb_connected: compute next delay (exponential if configured)
                if use_exp:
                    delay = min(delay * 2, max_delay)
                else:
                    delay = min(delay, max_delay)

            if not getattr(self, "tsdb_connected", False):
                # Final log if exhausted
                if attempts > 0 and attempt_no >= attempts:
                    self._log.error("Auto-reconnect: exhausted reconnect attempts. Will continue buffering to backlog.")
                    msg: str = (f"❌ [COMMUNICATION LOST] --- to TimescaleDB --- Attempt "
                                f"{attempt_no}{'' if attempts <= 0 else f'/{attempts}'} --- Will retry in {delay}s.")
                    self._log.error(msg)
                else:
                    self._log.info("Auto-reconnect: stopped without establishing connection.")

        finally:
            # clear the thread-running guard so a future outage can spawn a new reconnect thread
            with self._reconnect_lock:
                self._reconnect_thread_running = False
            self._log.debug("Auto-reconnect thread exiting.")


    def _trigger_reconnect(self) -> None:
        """Prevent concurrent reconnect threads from being spawned """

        if not hasattr(self, "_reconnect_lock"):
            self._reconnect_lock = threading.RLock()
            self._reconnect_thread_running = False

        if not hasattr(self, "_stop_reconnect_event"):
            self._stop_reconnect_event = threading.Event()

        if self._stop_event.is_set():
            return

        with self._reconnect_lock:
            if self._reconnect_thread_running:
                return
            self._reconnect_thread_running = True
            # set tsdb_connected False immediately to cause upstream to stop DB work
            self._set_tsdb_connected(conn_value = False, conn_reason = "Connect unsuccessful")
            self._stop_reconnect_event.clear()
            threading.Thread(target=self._attempt_reconnect, daemon=True, name= "TSDB ReconnectThread").start()
            self._log.info("Reconnect thread started.")

    def _stop_thread_reconnect(self) -> None:
        """
        Cleanly stop the reconnect thread.
        """
        if hasattr(self, "_stop_reconnect_event"):
            self._stop_reconnect_event.set()
            self._log.info("reconnect thread stopped.")


    def _ensure_wide_table_exists(self, table_name: str) -> None:
        """
        Ensures a protocol-specific wide metrics table exists in PostgreSQL.
        Creates it if missing with the same base structure as device_metrics_wide__*
        (m_time, device_info_id) — dynamic metric columns are added later
        by _ensure_columns_for_metrics.

        The table is created as a plain PostgreSQL table. RollupManager's
        ensure_hypertables() converts it to a TimescaleDB hypertable
        during setup_narrow_rollup(), which is called after this method completes.

        Safe to call on every startup — the IF NOT EXISTS guard makes it
        idempotent.

        Args:
            table_name: The protocol-specific wide table name, e.g.
                        'device_metrics_wide__eg4_18kpv'. Must be a
                        pre-validated SQL-safe name from _safe_table_name().

        Raises:
            ConnectionError: if not connected to TimescaleDB.
            SQLAlchemyError: if table creation fails unexpectedly.
        """
        with self._reconnect_lock:
            tsdb_connected: bool = self.tsdb_connected

        if not tsdb_connected:
            msg: str = f"Cannot create wide table '{table_name}' — not connected."
            raise ConnectionError(msg)

        with self.SessionFactory() as session:
            try:
                with session.begin():
                    self.schema_advisory_lock(session)

                    # 1. Create the base table if it doesn't exist.
                    #    Only the structural columns are created here —
                    #    metric columns are added by _ensure_columns_for_metrics.
                    #    table_name comes from _safe_table_name() so f-string is safe.
                    session.execute(text(f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            m_time          TIMESTAMPTZ     NOT NULL,
                            device_info_id  INTEGER         NOT NULL
                                REFERENCES device_info(device_info_id)
                                ON DELETE CASCADE,
                            CONSTRAINT {table_name}_pkey
                                PRIMARY KEY (m_time, device_info_id)
                        );
                    """))

                    # 2. Create the descending index on (m_time, device_info_id)
                    #    TimescaleDB will add its own chunk indexes on top of this
                    #    when the table is converted to a hypertable.
                    session.execute(text(f"""
                        CREATE INDEX IF NOT EXISTS {table_name}_time_device_idx
                            ON {table_name} (m_time DESC, device_info_id);
                    """))

                # 3. Register the new table in SQLAlchemy metadata so the ORM
                #    and flush worker can reference it by name immediately.
                #    Uses extend_existing=True so re-running on restart is safe.
                with self.schema_lock:
                    Table(table_name, Base.metadata, autoload_with=self.engine, extend_existing=True)

                self._log.info(f"Wide table '{table_name}' created/verified successfully.")

            except SQLAlchemyError as e:
                self._log.error(f"_ensure_wide_table_exists failed for '{table_name}': {e}")
                raise

    # -------------------------
    # 3. Ensure ORM tables exist
    # -------------------------

    def _create_tables(self) -> None:
        """
         Create ORM tables for device_info, device_metrics_narrow, protocol_registry and metric_catalog.
            The wide tables for each protocol/device are created dynamically by _ensure_wide_table_exists when protocols register via init_bridge,

        """
        with self.SessionFactory() as session:
            with self._reconnect_lock:
                tsdb_connected: bool = self.tsdb_connected

            if not tsdb_connected or not session:
                self._log.debug("Cannot create tables, not tsdb_connected")
                return

            try:
                Base.metadata.create_all(self.engine)
                with session.begin():
                    # create the index on device_metrics_narrow for faster grafana lookups by device_info_id and metric_name, descending by m_time
                    session.execute(text("CREATE INDEX IF NOT EXISTS device_metrics_narrow_lookup_idx ON device_metrics_narrow (device_info_id, metric_name, m_time DESC);"))
                    # drop the old indexes on device_metrics_narrow for m_time, which is no longer needed since the new index above covers it and is more efficient
                    session.execute(text("DROP INDEX IF EXISTS device_metrics_narrow_time_idx;"))
                    session.execute(text("DROP INDEX IF EXISTS device_metrics_narrow_m_time_idx;"))
                    # create_all() only creates NEW tables -- it does not add
                    # new columns to a device_info table that already exists
                    # from a prior version of this app. metric_count is new;
                    # ensure it exists on upgrade the same way wide-table
                    # columns are added elsewhere in this file.
                    session.execute(text("ALTER TABLE device_info ADD COLUMN IF NOT EXISTS metric_count INTEGER;"))
                self._log.info("ORM tables and indexes created/ensured")
            except Exception as e:
                self._log.error(f"ORM tables and indexes creation error: {e}")
                raise

    # -------------------------
    #  4. Write device information metadata
    # -------------------------

    def _get_or_create_device(self, from_transport: transport_base, metric_count: int = 0) -> int:
        """
        Upserts a row into device_info for the given transport and returns
        its device_info_id.

        First call per session hits the DB to insert or update all metadata
        fields (name, model, serial, location, protocol_id, metric_count).
        Subsequent calls for the same transport_name are served from the
        two-level cache (_device_cache + _verified_devices) with no DB
        round-trip.

        metric_count is the caller's actual scraped metric count for this
        transport (write_data passes len(data)) -- captured once here, at
        the first write of each app session, not tracked live thereafter.
        Metric counts don't change during a running app's lifetime (only
        between restarts, if a variable_mask config changes), so there's
        nothing to gain from re-recording it on every write -- this mirrors
        RollupManager.record_transport_interval's identical reasoning for
        read_interval. On_conflict_do_update still refreshes it on each
        new session (first write after a restart), so a config change
        between restarts is correctly picked up.

        The database doesn't know which field changed between restarts — it
        only knows the unique key (transport). on_conflict_do_update ensures
        any changed metadata (e.g. device_name in config) is kept current.
        """

        t_name: str = from_transport.transport_name

        # Fast path — verified and cached this session, no DB needed.  False if transport_name is missing
        # from the cache for any reason, including a name change, which is safe since the DB will be the source of truth on the first
        # call and will populate the cache.
        if t_name in self._device_cache and t_name in self._verified_devices:
            return self._device_cache.get(t_name) or 100

        # Slow path — first packet for this transport this session.
        # Lock prevents two concurrent threads racing to insert the same row.
        with self._device_lock:

            # Double-check inside the lock in case another thread just resolved it.
            if t_name in self._device_cache and t_name in self._verified_devices:
                return self._device_cache[t_name]

            with self.SessionFactory() as session:
                try:
                    # Resolve protocol_id within the same session so we don't
                    # need a separate round-trip.  nullable=True on the column
                    # means a None here is safe.
                    protocol_name: str = from_transport.protocol_name
                    protocol_id: int | None = None
                    if protocol_name:
                        protocol_id = session.execute(
                            text(
                                "SELECT protocol_id FROM protocol_registry "
                                "WHERE protocol_name = :p"
                            ),
                            {"p": protocol_name}
                        ).scalar()

                    # Build the upsert — transport is the unique key.
                    stmt = pg_insert(DeviceInfo).values(
                        transport=t_name,
                        protocol_id=protocol_id,
                        device_identifier=from_transport.device_identifier,
                        device_name=from_transport.device_name,
                        device_manufacturer=from_transport.device_manufacturer,
                        device_model=from_transport.device_model,
                        device_serial_number=from_transport.device_serial_number,
                        device_location=from_transport.device_location,
                        metric_count=metric_count,
                        created_at=_now_tz(),
                        updated_at=_now_tz()
                    )

                    upsert_stmt = stmt.on_conflict_do_update(
                        index_elements=['transport'],
                        set_={
                            "protocol_id":         stmt.excluded.protocol_id,
                            "device_identifier":   stmt.excluded.device_identifier,
                            "device_name":         stmt.excluded.device_name,
                            "device_manufacturer": stmt.excluded.device_manufacturer,
                            "device_model":        stmt.excluded.device_model,
                            "device_serial_number":stmt.excluded.device_serial_number,
                            "device_location":     stmt.excluded.device_location,
                            "metric_count":        stmt.excluded.metric_count,
                            "updated_at":          stmt.excluded.updated_at,
                        }
                    ).returning(DeviceInfo.device_info_id)

                    db_id: int = session.execute(upsert_stmt).scalar_one()
                    # Update the sequence to avoid duplicate key errors and key bloating on future inserts.
                    session.execute(
                        text(
                            "SELECT setval('device_info_device_info_id_seq', "
                            "(SELECT COALESCE(MAX(device_info_id), 0) + 1 FROM device_info));"
                        )
                    )
                    session.commit()

                except Exception as e:
                    session.rollback()
                    self._log.error(f"Upsert failed for '{t_name}': {e}")
                    # Return cached id if available, otherwise a sentinel value.
                    return self._device_cache.get(t_name, 100)
                else:
                    self._device_cache[t_name] = db_id
                    self._verified_devices.add(t_name)

                    # Tell RollupManager real data now exists for this
                    # transport, in case a dynamic-sizing retune is
                    # pending for its protocol (or narrow) -- see
                    # HyperTableManager.note_device_metric_count_known. Only
                    # meaningful once metric_count is a real, positive
                    # figure; a 0/unknown count wouldn't move the load
                    # score anyway (_compute_metric_writes_per_day_helper
                    # skips it), so there's nothing to check off yet.
                    if metric_count > 0 and self.hypertable_mgr is not None:
                        try:
                            self.hypertable_mgr.note_device_metric_count_known(t_name, protocol_name)
                        except Exception as e:
                            self._log.warning(
                                f"note_device_metric_count_known failed for '{t_name}': {e}"
                            )

                    return db_id

    def _extract_metric_names(
        self,
        registry_map: dict[Registry_Type, list[registry_map_entry]],
        synthetic_fields: list[tuple[str, str, Any, Any]] | None = None,
    ) -> list[tuple[str, str, Any, Any]]:

        """
        Extracts metric names, data types, unit modifiers and notes from a
        transport's registry map for use in schema creation.

        accepts any transport's registry_map directly, enabling
        multi-protocol schema registration.

        Args:
            registry_map: The registry map from a transport_base instance,
                        accessed via from_transport.registry_map.
                        Keyed by Registry_Type, values are lists of
                        registry_map_entry objects.
            synthetic_fields: Optional list of (variable_name, data_type,
                        unit_mod, note) tuples — or (variable_name, data_type,
                        unit_mod, note, registry_type) tuples, the trailing
                        registry_type ignored here — from
                        transport_base.synthetic_fields_metadata.  These are
                        appended to the registry-derived metrics so that
                        columns computed by post_process_data are registered
                        in the wide table schema with the correct types at
                        init_bridge time.  This prevents _validate_wide_row
                        from seeing them as unknown extra_keys and crashing
                        the flush worker.

        Returns:
            Sorted list of (variable_name, data_type, unit_mod, note) tuples,
            covering every registry type actually present in registry_map —
            not just the modbus-shaped INPUT/HOLDING/COIL/DISCRETE types, so
            transports that key everything under Registry_Type.CUSTOM_BUS
            (e.g. serial_pylon, canbus — anything calling get_registry_map()
            with no argument) still get a wide table instead of silently
            computing a metric_count of 0. Iteration order is by enum value
            for deterministic seen_names/dedup precedence when the same
            variable_name appears under more than one registry type.
            Duplicates are removed by variable_name (first type wins).
            Returns empty list if registry_map is None, empty, or malformed.

            Mirrors services.bridge_service._active_metric_names_for_protocol,
            which reads registry_map.values() unfiltered for the same reason
            — keep both in sync if this logic changes.
        """
        if not registry_map:
            self._log.warning("_extract_metric_names: registry_map is None or empty — no metrics to extract")
            return []

        results: list[tuple[str, str, Any, Any]] = []
        seen_names: set[str] = set()

        for registry_type in sorted(registry_map.keys(), key=lambda rt: rt.value):

            entries: list[registry_map_entry] | None = registry_map.get(registry_type)

            if not entries:
                continue

            for entry in entries:
                if not hasattr(entry, 'variable_name') or not entry.variable_name:
                    continue

                if entry.variable_name in seen_names:
                    continue
                seen_names.add(entry.variable_name)

                results.append((
                    entry.variable_name,
                    getattr(entry, 'data_type', ''),
                    getattr(entry, 'unit_mod', 1.0),   # default to 1.0 — no scaling
                    getattr(entry, 'note', '')
                ))

        # Append transport-declared synthetic fields, skipping any that
        # collide with registry-derived names (registry takes precedence).
        # Tuples may be 4-length (legacy: variable_name, data_type,
        # unit_mod, note) or 5-length (with a trailing registry_type tag —
        # see transport_base.synthetic_fields_metadata /
        # eg4_metadata.eg4_synthetic_fields_metadata); the registry tag
        # isn't needed for schema registration, so it's simply ignored here
        # via the `*_` catch-all rather than requiring an exact tuple length.
        if synthetic_fields:
            for variable_name, data_type, unit_mod, note, *_ in synthetic_fields:
                if variable_name in seen_names:
                    self._log.debug(
                        f"_extract_metric_names: synthetic field '{variable_name}' "
                        f"already present in registry map — skipping duplicate"
                    )
                    continue
                seen_names.add(variable_name)
                results.append((variable_name, data_type, unit_mod, note))
                self._log.debug(
                    f"_extract_metric_names: registered synthetic field "
                    f"'{variable_name}' ({data_type}) from transport"
                )

        return sorted(results)

    def _upsert_protocol_registry(self, protocol: str, wide_table_name: str | None, metric_count: int ) -> int:
        """
        Upserts a row into protocol_registry for the given protocol.

        On first insert:
            - Creates the row with rollup_setup_complete = False
            - rollup_prefix derived from wide_table_name if provided

        On subsequent updates:
            - Updates wide_table_name, metric_count, rollup_prefix, updated_at
            - Does NOT reset rollup_setup_complete — a completed setup
            survives restarts

        Returns the protocol_id for use by downstream methods such as
        _ensure_columns_for_metrics.

        Raises:
            ConnectionError: if not connected to TimescaleDB
            RuntimeError: if the upsert executes but returns no protocol_id
        """
        with self._reconnect_lock:
            tsdb_connected: bool = self.tsdb_connected

        if not tsdb_connected:
            msg: str = f"Cannot upsert protocol_registry for '{protocol}' — not connected."
            raise ConnectionError(msg)

        # Derive rollup_prefix from wide_table_name if provided.
        # None for narrow-only protocols (metric_count >= WIDE_TABLE_COLUMN_LIMIT 160).
        # e.g. wide_table_name = "device_metrics_wide__eg4_18kpv"
        #      rollup_prefix   = "rollup_wide__eg4_18kpv"
        rollup_prefix: str | None = None
        if wide_table_name is not None:
            # Strip the "device_metrics_" prefix to get the protocol suffix,
            # then prepend "rollup_" to form the rollup view base name.
            # e.g. "device_metrics_wide__eg4_18kpv"
            #   -> "wide__eg4_18kpv"
            #   -> "rollup_wide__eg4_18kpv"
            suffix = wide_table_name.removeprefix("device_metrics_")
            rollup_prefix = f"rollup_{suffix}"

        with self.SessionFactory() as session:
            try:
                with session.begin():

                    stmt: ReturningInsert[Tuple[int]] = pg_insert(ProtocolRegistry).values(
                        protocol_name=protocol,
                        wide_table_name=wide_table_name,
                        metric_count=metric_count,
                        rollup_prefix=rollup_prefix,
                        rollup_enabled=True,
                        rollup_setup_complete=False,   # safe default on first insert
                        created_at=_now_tz(),
                        updated_at=_now_tz()
                    ).on_conflict_do_update(
                        index_elements=["protocol_name"],
                        set_={
                            # Update structural fields that may change between restarts
                            # e.g. metric_count grows if CSV is updated
                            "wide_table_name": pg_insert(ProtocolRegistry).excluded.wide_table_name,
                            "metric_count": pg_insert(ProtocolRegistry).excluded.metric_count,
                            "rollup_prefix": pg_insert(ProtocolRegistry).excluded.rollup_prefix,
                            "updated_at": _now_tz(),
                            # rollup_setup_complete is intentionally not updated here —
                            # once True it should only be reset explicitly via
                            # mark_rollup_setup_complete(False) when a schema change
                            # requires rollup views to be rebuilt
                        }
                    ).returning(ProtocolRegistry.protocol_id)

                    protocol_id: int | None = session.execute(stmt).scalar_one_or_none()
                    # Update the sequence to avoid duplicate key errors and key bloating on future inserts.
                    session.execute(
                        text(
                            "SELECT setval('protocol_registry_protocol_id_seq', "
                            "(SELECT COALESCE(MAX(protocol_id), 0) + 1 FROM protocol_registry));"
                        )
                    )

            except Exception as e:
                self._log.error(f"_upsert_protocol_registry failed for '{protocol}': {e}")
                raise

            else:

                if protocol_id is None:
                    msg1: str = f"_upsert_protocol_registry: no protocol_id returned for protocol '{protocol}' — upsert may have silently failed."
                    raise RuntimeError(msg1)

                self._log.info(
                    f"Protocol registry upserted: '{protocol}' "
                    f"(id={protocol_id}, table='{wide_table_name}', "
                    f"metrics={metric_count})"
                )
                return protocol_id

    # -------------------------
    #  Dynamically create wide table columns for metrics
    # -------------------------

    def _ensure_columns_for_metrics(self, metric_start_names: List[Tuple[str, str, Any, Any]], table_name: str, protocol: str) -> bool:
        """
        Ensure each metric name as defined in the variable_mask/variable_screen filters has a corresponding column in device_metrics_wide__*,
        and an entry in the metric_catalog table-- which describes the wide table field definitions.
        Due to the memory limits of postgres, no more than WIDE_TABLE_COLUMN_LIMIT 160 metrics as determined in the calling method.
        Using metric_name to map, return clean_column_name in the metric_catalog to create the SQL column in device_metrics_wide__*.

        For metrics that already have a column, also reconciles metric_catalog's
        recorded data_type against the column's actual Postgres type, migrating
        the column (ALTER COLUMN ... TYPE ... USING) when they've drifted apart,
        so metric_catalog never claims a type the physical column doesn't
        actually have — see the in-loop comment for why that matters (a stale
        catalog type let a TEXT column through the "safe for stats_agg()"
        rollup filter and crashed rollup view creation).

        There could potentially be thousands of metrics, which cannot all be ingested as columns. If over WIDE_TABLE_COLUMN_LIMIT 160 metrics we only save metrics to the
        device_metrics_narrow table, so this method is not needed and is bypassed at the calling method connection stage.
        """

        with self.SessionFactory() as session:

            # Must be connected to tsdb to write metric names
            with self._reconnect_lock:
                tsdb_connected: bool = self.tsdb_connected

            if not tsdb_connected or not session:
                self._log.error("Cannot create columns — not tsdb_connected.")
                raise ConnectionError("Not connected to TimescaleDB.")


            if not metric_start_names:
                self._log.error("No metric column names were detected")
                raise ValueError("No metric column names were detected.")

            try:
                with self.schema_lock:
                    with session.begin():
                        # advisory lock to serialize schema changes
                        self.schema_advisory_lock(session)

                        # lookup the protocol ID from the protocol name
                        protocol_id: int = session.execute(
                            text("SELECT protocol_id FROM protocol_registry WHERE protocol_name = :p"),
                            {"p": protocol}
                        ).scalar_one()

                        # metric name, data_type, unit_mod, note
                        for m, d, u, n in metric_start_names:
                            # 1. Check for existing clean name that has already been mapped.
                            clean_value: Any | None = session.execute(
                                text("SELECT clean_column_name FROM metric_catalog WHERE metric_name = :m  AND protocol_id = :p""" ),
                                {"m": m, "p": protocol_id}
                            ).scalar()

                            d_type: str = self._timescale_type(d,u)

                            if clean_value:
                                # Guard against metric_catalog.data_type drifting away from the
                                # column's actual Postgres type. The rollup-descriptor query
                                # (_resolve_rollup_metric_descriptors_helper) trusts
                                # metric_catalog.data_type alone to decide whether a column is
                                # safe to pass to stats_agg() — but the UPDATE below only ever
                                # changed the catalog's *opinion*, never the column itself, so a
                                # column created under an earlier/incorrect type declaration (or
                                # changed out-of-band) would keep silently reporting a type it no
                                # longer has.
                                physical_type: str | None = session.execute(
                                    text("""
                                        SELECT data_type FROM information_schema.columns
                                        WHERE table_name = :tname AND column_name = :col
                                    """),
                                    {"tname": table_name, "col": clean_value},
                                ).scalar()

                                effective_d_type: str = d_type
                                if physical_type is not None and physical_type.upper() != d_type.upper():
                                    self._log.warning(
                                        f"Column '{clean_value}' on {table_name} is physically "
                                        f"{physical_type.upper()} but metric '{m}' now declares "
                                        f"{d_type} — attempting to migrate the column."
                                    )
                                    try:
                                        # SAVEPOINT via begin_nested(): if this specific ALTER
                                        # fails (e.g. existing data won't cast cleanly), only this
                                        # savepoint rolls back — the outer transaction, and every
                                        # other metric already processed/pending in this same
                                        # call, is unaffected.
                                        with session.begin_nested():
                                            session.execute(text(
                                                f"ALTER TABLE {table_name} ALTER COLUMN {clean_value} "
                                                f"TYPE {d_type} USING {clean_value}::{d_type};"
                                            ))
                                        self._log.info(
                                            f"Migrated column '{clean_value}' on {table_name} from "
                                            f"{physical_type.upper()} to {d_type}."
                                        )
                                    except Exception as migrate_exc:
                                        # Migration failed — leave the catalog matching physical
                                        # reality (not the aspirational new type) so the rollup
                                        # filter keeps excluding/handling it safely (e.g. as TEXT)
                                        effective_d_type = physical_type.upper()
                                        self._log.error(
                                            f"Could not migrate column '{clean_value}' on "
                                            f"{table_name} from {physical_type.upper()} to "
                                            f"{d_type}: {migrate_exc}. Leaving "
                                            f"metric_catalog.data_type as {effective_d_type} until "
                                            f"this is resolved manually."
                                        )

                                # Update the data_type, unit_mod and notes fields in metric_catalog for a matching metric
                                # from the csv files.  All corrections/updates should take place in the CSV or the UI in the webserver.
                                session.execute(
                                    text("""
                                        UPDATE metric_catalog SET data_type = :d, unit_mod = :u, notes = :n
                                        WHERE metric_name = :m AND protocol_id = :p
                                    """),
                                    {"d": effective_d_type, "u": u, "m": m, "n": n, "p": protocol_id}
                                )
                                # protocol_metric_mappings is used to process raw metrics data for coercion.
                                self.protocol_metric_mappings.setdefault(protocol, {})[m] = (clean_value, effective_d_type)
                                # metric exists so return to the top of the loop.
                                continue

                            # 2. If clean_value was false, clean metric name for safe sql column naming.
                            col: str = self._clean_column_name(m)

                            # check if column name (cleaned name) exists in postgres information_schema
                            exists_wide: Any | None = session.execute(text("""
                                SELECT 1 FROM information_schema.columns
                                WHERE table_name = :tname AND column_name = :col
                            """), {"tname": table_name, "col": col}).scalar()

                            # Add column if missing.  Initial column creation is alphabetic due to sorted metric names.
                            # Per postgres docs, subsequent columns added after first init are always appended to the end of the table.
                            # ie if you want a new column in the middle of the wide table, you must manually delete all tables,
                            # (losing your data) and restart MPG to recreate the table with the new column in the desired location.

                            if not exists_wide:
                                session.execute(text(
                                    f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {col} {d_type};"
                                ))

                            params: dict[str, Any] = {
                                'm': m,         # metric_name
                                'p': protocol_id, # protocol ID
                                'col': col,     # clean_column_name
                                'dtype': d_type or 'double precision', # data_type mapped from registry, with default
                                'umod': u,
                                'col_date': _now_tz(),
                                'n': n          # note from registry
                            }
                            # just in case the if clean_value: check failed.
                            # Take the existing record's field values and overwrite them with the value from the row that failed to insert".
                            session.execute(text("""
                                INSERT INTO metric_catalog (protocol_id, metric_name, clean_column_name, data_type, unit_mod, created_at, notes)
                                VALUES (:p, :m, :col, :dtype, :umod, :col_date, :n)
                                ON CONFLICT (protocol_id, metric_name) DO UPDATE SET
                                    clean_column_name = EXCLUDED.clean_column_name,
                                    data_type = EXCLUDED.data_type,
                                    unit_mod = EXCLUDED.unit_mod,
                                    notes = EXCLUDED.notes
                            """), params)

                            self.protocol_metric_mappings.setdefault(protocol, {})[m] = (col, d_type)
                            # Update the sequence to avoid duplicate key errors and key bloating on future inserts.
                        session.execute(
                            text(
                                "SELECT setval('metric_catalog_catalog_id_seq', "
                                "(SELECT COALESCE(MAX(catalog_id), 0) + 1 FROM metric_catalog));"
                            )
                        )

                    self._cache_wide_table_columns(table_name)  # cache existing wide table columns for fast lookup validation during writes
                    self.sync_single_table_schema(table_name)  # resync ORM table after dynamic column changes

                    self._log.info(f"Ensured {len(metric_start_names)} metric columns.")
                    return True

            except Exception as e:
                self._log.error(f"_ensure_columns_for_metrics failed (rolled back): {e}")
                return False

    # advisory lock for schema changes
    def schema_advisory_lock(self, session: Session, key_text: str = "timescaledb_schema_lock") -> None:
        """ transaction scoped advisory lock based on hash of key_text
            Locks wide table schema in postgres DB to allow dynamic wide table refactor.
        """

        session.execute(text("SELECT pg_advisory_xact_lock(hashtext(:k));"), {"k": key_text})

    def _timescale_type(self, data_type: Any, unit: Any) -> str:
        dt_name: str = getattr(data_type, "name", data_type)

        # --- base type from lookup, use DOUBLE PRECISION as fall back to be safe.
        base_type: str = self.timescale_type_map.get(dt_name, "DOUBLE PRECISION")

        # --- text and boolean never scale ---
        if base_type in ("TEXT", "BOOLEAN"):
            return base_type

        # --- scaling override ---
        # if a numeric value has a unit modifier, store as float
        if unit != 1.0:
            return "DOUBLE PRECISION"

        return base_type

    # clean column name
    def _clean_column_name(self, metric_name: str) -> str:
        """
        Cleans a string to be a safe SQL column name.

        Removes potentially unsafe characters and ensures column name starts with a letter or underscore.
        PostgreSQL identifiers must start with a letter (or underscore) and can contain
        letters, numbers, and underscores. If not quoted, they are also case-insensitive.
        """
        # Replace non-alphanumeric characters (except underscore) with underscores
        cleaned_name = metric_name.strip().lower()
        cleaned_name = re.sub(r'[^a-zA-Z0-9_]', '_', cleaned_name)

        # Ensure it starts with a letter or an underscore
        if not re.match(r'^[a-zA-Z_]', cleaned_name):
            cleaned_name: str = '_' + cleaned_name

        # Truncate to length if necessary (PostgreSQL limit is 63 chars for identifiers)
        return cleaned_name[:63]

    # wide table columns helper for write operations.  Per protocol wide table name.
    def _cache_wide_table_columns(self, table_name: str) -> None:
        try:
            insp: Inspector = inspect(self.engine)
            cols: List[ReflectedColumn] = insp.get_columns(table_name, schema='public')
            self._wide_columns_cache[table_name] = {
                col['name'] for col in cols
                if col['name'] not in ("m_time", "device_info_id")
            }
        except Exception as e:
            self._log.error(f"Failed to cache wide columns for {table_name}: {e}")

    # wide table row validation
    def _validate_wide_row(self, row: dict[str, Any], table_name: str) -> tuple[bool, str | None]:
        # 1. Strip metadata keys instantly to prevent false-positive resyncs
        METADATA_KEYS: set[str] = {"m_time", "device_info_id"}
        row_keys: set[str] = set(row) - METADATA_KEYS

        with self.schema_lock:
            wide_columns: set[str] = self._wide_columns_cache.get(table_name, set())
            if not wide_columns:
                self._log.debug(f"_validate_wide_row: no cache entry for '{table_name}'. Cache keys: {list(self._wide_columns_cache.keys())}")

            extra_keys: set[str] = row_keys - wide_columns
            fewer_keys: set[str] = wide_columns - row_keys
            fewer_keys_count: int = len(fewer_keys)

            if extra_keys:
                self._log.info(f"New metrics detected: {extra_keys}. Triggering resync...")

                # Safe to call because it's an RLock, but it will block other threads during the sync
                self.sync_single_table_schema(table_name)

                # Re-read the cache inside the lock context
                refreshed_cols: set[str] = self._wide_columns_cache.get(table_name, set())
                still_extra: set[str] = row_keys - refreshed_cols

                if still_extra:
                    msg = f"Database schema is still missing columns after resync: {sorted(still_extra)}"
                    self._log.error(msg)
                    raise ValueError(msg)
                return True, None

            elif fewer_keys:
                self._log.warning(f"TimescaleDB Wide-table schema mismatch; missing {fewer_keys_count} keys in scrape data:"
                                  f"{sorted(fewer_keys)} consider deleting {sorted(fewer_keys)} "
                                  f"column from the wide table or adding them to the scrape data.")
                msg: str = f"Missing {len(sorted(fewer_keys))} columns: {sorted(fewer_keys)}"
                return False, msg

            else:
                return True, None


    # resync single wide table schema after dynamic column changes
    def sync_single_table_schema(self, table_name: str) -> None:
        with self.schema_lock:
            self._log.info(f"Resyncing schema for {table_name}...")

            old_table: Table | None = Base.metadata.tables.get(table_name)
            if old_table is not None:
                Base.metadata.remove(old_table)

            Table(table_name, Base.metadata, autoload_with=self.engine, extend_existing=True)

            # Update column cache for this specific table
            self._cache_wide_table_columns(table_name)
            self._log.info(f"Schema resync complete for {table_name}")


    def _rediscover_protocols(self) -> None:
        """
        Called during reconnect to rebuild runtime state from protocol_registry.
        Restores _protocol_wide_table_map and retries any incomplete
        rollup setups via RollupManager.add_wide_rollup.

        Separated from _register_protocol_schema because on reconnect the
        physical tables and metric_catalog entries already exist — only the
        in-memory state and any incomplete rollup views need recovery.
        """
        with self.SessionFactory() as session:
            try:
                rows: Sequence[Row[Any]] = session.execute(
                    text("""
                        SELECT protocol_name, wide_table_name,
                            rollup_setup_complete
                        FROM protocol_registry
                        WHERE rollup_enabled = true
                        ORDER BY created_at ASC
                    """)
                ).fetchall()

            except SQLAlchemyError as e:
                self._log.error(f"_rediscover_protocols query failed: {e}")
                return

        if not rows:
            self._log.info("_rediscover_protocols: no registered protocols found.")
            return

        for protocol_name, wide_table_name, setup_complete in rows:

            # Restore runtime mapping — this is what lets write_data and
            # _flush_worker route correctly without re-running full schema setup
            self._protocol_wide_table_map[protocol_name] = wide_table_name
            self._registered_protocols.add(protocol_name)

            # Re-reflect the wide table into Base.metadata so the flush worker
            # can look it up by name after reconnect.
            if wide_table_name is not None:
                with self.schema_lock:
                    Table(
                        wide_table_name,
                        Base.metadata,
                        autoload_with=self.engine,
                        extend_existing=True
                    )
                self._cache_wide_table_columns(wide_table_name)

            # Restore per-protocol metric mapping from metric_catalog
            # so _process_raw_metrics can coerce values correctly
            self._restore_metric_mapping(protocol_name)

            self._log.info(f"Rediscovered protocol '{protocol_name}' (table='{wide_table_name}', setup_complete={setup_complete})")

            if not setup_complete and self.rollup_mgr is not None:
                self._log.warning(f"Protocol '{protocol_name}' has incomplete rollup setup — retrying via RollupManager.add_wide_rollup.")
                try:
                    self.rollup_mgr.add_wide_rollup(protocol_name, wide_table_name)
                except Exception as e:
                    self._log.error(f"Recovery failed for protocol '{protocol_name}': {e} — will retry on next startup.")
                    # Non-fatal — protocol data can still flow via narrow table
                    # even if wide table rollups are incomplete

    def _restore_metric_mapping(self, protocol: str) -> None:
        """
        Rebuilds protocol_metric_mappings[protocol] from metric_catalog
        after a reconnect, so _process_raw_metrics can coerce values
        correctly without re-running _ensure_columns_for_metrics.
        """
        with self.SessionFactory() as session:
            try:
                rows: Sequence[Row[Any]] = session.execute(
                    text("""
                        SELECT mc.metric_name, mc.clean_column_name, mc.data_type
                        FROM metric_catalog mc
                        JOIN protocol_registry pr
                            ON mc.protocol_id = pr.protocol_id
                        WHERE pr.protocol_name = :p
                    """),
                    {"p": protocol}
                ).fetchall()

                mapping: dict[str, tuple[str, str]] = {
                    metric_name: (clean_column_name, data_type)
                    for metric_name, clean_column_name, data_type in rows
                }

                self.protocol_metric_mappings[protocol] = mapping

                self._log.debug(f"Restored {len(mapping)} metric mappings for protocol '{protocol}'")

            except SQLAlchemyError as e:
                self._log.error(f"_restore_metric_mapping failed for '{protocol}': {e}")
                raise

    # # 10g
    def _start_flush_thread(self) -> None:
        """
        Launch background thread to process metrics.
        """
        if self._flush_thread.is_alive():
            return
        self._flush_thread.start()
        self._log.debug("Flush thread started.")


    def _register_protocol_schema(
        self,
        protocol: str,
        registry_map: dict[Registry_Type, list[registry_map_entry]],
        synthetic_fields: list[tuple[str, str, Any, Any]] | None = None,
    ) -> None:
        """
        Ensures wide table columns and metric_catalog entries exist for
        all metrics in this protocol's registry map that have been filtered
        in by the variable_mask/variable_screen config, plus any synthetic
        fields declared by the transport via synthetic_fields_metadata.

        Synthetic fields are registered with the correct data type at
        schema-creation time so _validate_wide_row never encounters them
        as unknown extra_keys during the flush worker cycle.

        Each protocol gets its own wide table: device_metrics_wide__{protocol}
        The narrow table is shared across all protocols.
        """
        # Derive the table name for this protocol up front
        # so it can be passed to every downstream method that needs it
        wide_table_name: str = self._safe_table_name(protocol)

        # Extract metric names from this transport's registry map,
        # appending transport-declared synthetic fields
        metric_names: list[tuple[str, str, Any, Any]] = self._extract_metric_names(
            registry_map,
            synthetic_fields=synthetic_fields,
        )
        metric_count: int = len(metric_names)

        if metric_count == 0:
            self._log.error(f"No metrics found for protocol '{protocol}'")
            return

        #  Upsert into protocol_registry — creates the protocol row
        #  and anchors the table name before metric_catalog FKs are written
        self._upsert_protocol_registry(protocol, wide_table_name, metric_count)

        if metric_count >= self.WIDE_TABLE_COLUMN_LIMIT:
            self._log.warning(f"Protocol '{protocol}' has {metric_count} metrics — exceeds {self.WIDE_TABLE_COLUMN_LIMIT} column limit, using narrow table only.")
            self._protocol_wide_table_map[protocol] = None
            return

        # 5. Ensure the wide table exists as a physical table in postgres
        #    before _ensure_columns_for_metrics tries to ALTER it
        self._ensure_wide_table_exists(wide_table_name)

        # 6. Create columns and metric_catalog entries —
        #    table_name and protocol are both passed through
        success: bool = self._ensure_columns_for_metrics(metric_names, table_name=wide_table_name, protocol=protocol)

        if not success:
            self._log.error(f"Failed to ensure metric columns for protocol '{protocol}'. Wide table will not be used.")
            self._protocol_wide_table_map[protocol] = None
            return

        self._protocol_wide_table_map[protocol] = wide_table_name
        self._log.info(f"Schema ready for protocol '{protocol}' → table '{wide_table_name}' ({metric_count} metrics)")

        self._init_or_update_rollup_manager(protocol)

    def _init_or_update_rollup_manager(self, protocol: str) -> None:
        """
        Creates RollupManager on first call, adds new protocol views on
        subsequent calls as more protocols register via init_bridge.
        """
        if not self.rollup_policy.get("enable_rollups", True):
            return

        if self.rollup_mgr is None:
            # First protocol registered — create HyperTableManager first
            # (the lower-level hypertable primitive RollupManager is
            # built on top of), then RollupManager with a reference to
            # it, then wire HyperTableManager's late-bound back-reference
            # to RollupManager for the few genuinely bidirectional calls
            # (dynamic-sizing retune triggering a rollup rebuild, and the
            # shared performance-tier lookup needing the rollup-view
            # count) -- see HyperTableManager's class docstring.
            if self.hypertable_mgr is None:
                self.hypertable_mgr = HyperTableManager(
                    rollup_policy=self.rollup_policy,
                    my_session_factory=self.SessionFactory,
                    log=self._log,
                )
            self.rollup_mgr = RollupManager(
                rollup_policy=self.rollup_policy,
                my_session_factory=self.SessionFactory,
                my_engine=self.engine,
                migration_in_progress=self.migration_in_progress,
                log=self._log,
                send_message=self.send_message,
                backlog_lock=self._backlog_lock,
                flush_queue=self._flush_queue,
                backlog=self.backlog,
                reconnect_lock=self._reconnect_lock,
                hypertable_mgr=self.hypertable_mgr,
            )
            self.hypertable_mgr.rollup_mgr = self.rollup_mgr
            self.rollup_mgr.setup_narrow_rollup()
            # First protocol also needs per-protocol setup.
            # setup_narrow_rollup() only creates shared narrow rollups.
            self.rollup_mgr.add_wide_rollup(
                protocol_name=protocol,
                wide_table_name=self._protocol_wide_table_map[protocol]
            )

            if self.rollup_policy.get("enable_auto_refresh", True):
                self.rollup_mgr.start_auto_refresh()

            # Replay backlog now that schema is confirmed ready.
            # Must be after setup_narrow_rollup releases migration_in_progress.
            try:
                if self.enable_persistent_storage:
                    self.backlog.replay_to_queue()
            except Exception as e:
                self._log.error(f"Backlog replay after schema setup failed: {e}")
        else:
            # Subsequent protocol — add its views to existing manager
            self.rollup_mgr.add_wide_rollup( protocol_name=protocol, wide_table_name=self._protocol_wide_table_map[protocol])

    def _safe_table_name(self, protocol: str) -> str:
        """Converts a protocol name to a safe PostgreSQL table name."""
        safe: str = re.sub(r'[^a-zA-Z0-9_]', '_', protocol.strip().lower())
        if not re.match(r'^[a-zA-Z_]', safe):
            safe = '_' + safe
        return f"device_metrics_wide__{safe}"[:63]

    # In timescaledb — override init_bridge to register each scraper's protocol
    def init_bridge(self, from_transport: transport_base) -> None:
        """
        Called by MPG after all transports are constructed, once per
        scraper transport wired to this bridge. This is where per-protocol
        schema setup should happen, since we now know which transports
        (and therefore which registry maps) are actually feeding us.
        """
        if not from_transport.registry_map:
            self._log.debug(
                f"init_bridge: skipping '{from_transport.transport_name}' "
                f"— no registry map (likely a bridge transport)"
            )
            return

        protocol: str = from_transport.protocol_name
        if not protocol:
            self._log.warning(
                f"init_bridge: '{from_transport.transport_name}' has a registry_map but no "
                f"protocol_name — cannot register schema or track scrape interval for it."
            )
            return

        # Register schema FIRST, before recording read_interval below.
        # self.rollup_mgr is constructed lazily, inside
        # _register_protocol_schema, the first time ANY protocol is
        # registered on this bridge -- so checking `if self.rollup_mgr is
        # not None` before this ran (the original ordering here) was
        # silently a no-op for whichever transport happened to be the
        # very first to trigger protocol registration: rollup_mgr simply
        # didn't exist yet at that moment. That transport's read_interval
        # then never made it into transport_read_intervals, which
        # permanently excluded it from get_dynamic_raw_table_settings_
        # helper's load-score roster AND from
        # RollupManager.note_device_metric_count_known's "every expected
        # transport reported" check for its protocol -- so that
        # protocol's dynamic-sizing retune could only ever fire via the
        # bounded timeout, never the normal path, no matter how much
        # live data it actually had.
        already_registered: bool = protocol in self._registered_protocols
        if not already_registered:
            self._log.info(f"Registering protocol '{protocol}' from transport '{from_transport.transport_name}'")
            try:
                synthetic: list[tuple[str, str, Any, Any]] = getattr(
                    from_transport, 'synthetic_fields_metadata', []
                )
                if synthetic:
                    self._log.info(
                        f"Transport '{from_transport.transport_name}' declares "
                        f"{len(synthetic)} synthetic field(s) for schema registration: "
                        f"{[s[0] for s in synthetic]}"
                    )
                self._register_protocol_schema(
                    protocol,
                    from_transport.registry_map,
                    synthetic_fields=synthetic or None,
                )
                self._registered_protocols.add(protocol)
            except Exception as e:
                self._log.error(f"Failed to register schema for protocol '{protocol}': {e}")
        else:
            self._log.debug(f"Protocol '{protocol}' already registered, skipping.")

        # Record read_interval AFTER the schema-registration step above,
        # so self.rollup_mgr is guaranteed to exist by now unless schema
        # registration itself just failed (in which case there's nothing
        # to track against anyway -- same as the prior behavior in that
        # failure case). Deliberately still called even when the protocol
        # was ALREADY registered (by an earlier transport) -- two
        # transport instances can share one protocol/wide table (e.g. two
        # inverters of the same model) and, in principle, run at
        # different intervals; recording only the first one seen would
        # silently undercount that protocol's real combined load.
        if self.hypertable_mgr is not None:
            read_interval: float = getattr(from_transport, "read_interval", 0.0) or 0.0
            if read_interval <= 0:
                self._log.warning(
                    f"init_bridge: '{from_transport.transport_name}' (protocol '{protocol}') has no "
                    f"usable read_interval ({read_interval!r}) — dynamic chunk/compression sizing "
                    f"will fall back to static settings for this protocol until a valid interval is seen."
                )
            self.hypertable_mgr.record_transport_interval(from_transport.transport_name, protocol, read_interval)
        else:
            self._log.debug(
                f"init_bridge: hypertable_mgr unavailable for '{from_transport.transport_name}' "
                f"(protocol '{protocol}') — read_interval not recorded."
            )

        if protocol in self._registered_protocols:
            self._log.debug(f"Protocol '{protocol}' already registered, skipping.")
            return

        self._log.info(f"Registering protocol '{protocol}' from transport '{from_transport.transport_name}'")

        try:
            synthetic: list[tuple[str, str, Any, Any]] = getattr(
                from_transport, 'synthetic_fields_metadata', []
            )
            if synthetic:
                self._log.info(
                    f"Transport '{from_transport.transport_name}' declares "
                    f"{len(synthetic)} synthetic field(s) for schema registration: "
                    f"{[s[0] for s in synthetic]}"
                )
            self._register_protocol_schema(
                protocol,
                from_transport.registry_map,
                synthetic_fields=synthetic or None,
            )
            self._registered_protocols.add(protocol)
        except Exception as e:
            self._log.error(f"Failed to register schema for protocol '{protocol}': {e}")

    # using  override transport_base.write_data method

    def write_data(self, data: dict[str, int | float | str ], from_transport: transport_base) -> None:

        if not data:
            return

        protocol: str = from_transport.protocol_name
        device_id: int = self._get_or_create_device(from_transport, metric_count=len(data))

        if not device_id:
            self._log.error("Could not resolve Device ID. Dropping packet.")
            return

        # Look up the wide table for this transport's protocol
        wide_table_name: str | None = self._protocol_wide_table_map.get(protocol)

        payload: dict[str, Any] = {
            "device_info_id": device_id,
            "metrics": data.copy(),
            "m_time": _now_tz(),
            "transport_name": from_transport.transport_name,
            "protocol": protocol,                    # carry protocol through to flush worker
            "wide_table_name": wide_table_name,      # carry resolved table name
        }

        self._flush_queue.put(payload)

    # Flush worker thread to handle data writes to the database.
    def _flush_worker(self) -> None:
        """Async flush worker created during init.  Handles data appends to tables. Routing to backlog if needed.
            datacopy  -> wide dict of unaltered metrics passed from MPG or backlog
            wide_data  -> wide dict of processed datacopy for safe sql coercion.
            narrow_data  -> dict of appended new_data with deviceid and timestamp.  Needed because narrow table
            applies timestamp to individual metrics.
        """
        # open a session for the lifetime of the flush worker thread to reuse across flushes and improve performance.
        # We can do this because the flush worker is the only thread writing to the database,
        # so we don't have to worry about session concurrency.
        session: Session = self.SessionFactory()
        try:
            while True:

                # 24hr cleanup interval in seconds for stale registry entries based on transport timestamp vs last known timestamp
                # in the registry map for that transport. This is to prevent the registry map from growing indefinitely
                # with old transports that are no longer sending data.
                if time.time() - self._last_cleanup_time > 86400:
                    self._cleanup_stale_registry()
                    self._last_cleanup_time = time.time()

                try:
                    self._flush_event.wait(timeout=0)
                    #************** Drain the queue with a 1s timeout to stay responsive************
                    data_in: dict[str, Any] | None = self._flush_queue.get(block=True)

                    if data_in is None:
                        self._log.info("No data in flush queue. Exiting flush worker.")
                        self._flush_queue.task_done()
                        break # Exit the loop cleanly and immediately

                    # Now that we have data, wait here if a migration is running
                    while self.migration_in_progress.is_set():
                        if self._stop_event.is_set():
                            break
                        time.sleep(0.25)

                    # 2. Extract the metadata from data_in
                    device_info_id: int = data_in["device_info_id"]
                    timestamp: datetime = data_in["m_time"]
                    # Backlog replay deserializes datetime as ISO string — convert back.
                    if isinstance(timestamp, str):
                        timestamp = datetime.fromisoformat(timestamp)
                    metrics_only: dict[str, Any] = data_in["metrics"]
                    transport_name: str = data_in["transport_name"]
                    protocol: str = data_in["protocol"]
                    wide_table_name: str | None = data_in["wide_table_name"]

                    # Check for stale data before attempting to process/write to the database. This is based on the timestamp from the transport
                    # and the previous saved timestamp.

                    is_stale: bool = self._check_is_stale(transport_name, metrics_only, timestamp)

                    if is_stale:
                        self._commit_transport_state(transport_name, metrics_only, timestamp, is_stale=True)
                        self._log.debug("Stale data detected, skipping DB write.")
                        self._flush_queue.task_done()
                        continue

                    # pre-process data to coerce floating point, integer as values (from metric_catalog definitions) for safe insertion to the wide table.
                    # Also applies SQL-safe column renaming. Only process metric values for the wide table path.
                    # The narrow table stores raw key/values as default double precision and is not subject to the same schema
                    # constraints, so we can skip processing for narrow table entries with only metric names safe SQL cleaned for
                    # consistency.

                    wide_data, narrow_data = self._process_raw_metrics(metrics_only, protocol)

                    if not wide_data and not narrow_data:
                        self._log.debug("No data detected, skipping DB write.")
                        self._flush_queue.task_done()
                        continue
                    else:
                        # Add device_info_id and timestamp to wide_data for wide table insertion.  This is needed because the narrow table
                        # applies timestamp and device_info_id to individual rows, whereas the wide table has one row per
                        # timestamp/device with multiple metric columns.

                        valid_row: bool = False
                        msg: str | None = None
                        if wide_data and wide_table_name is not None:
                            valid_row, msg = self._validate_wide_row(wide_data, wide_table_name)
                            wide_data: dict[str, Any] = wide_data | {
                                "device_info_id": device_info_id,
                                "m_time": timestamp,
                            }

                    if self._stop_event.is_set():
                        break

                    while self.migration_in_progress.is_set():
                        if self._stop_event.is_set():
                            break
                        time.sleep(0.5)
                    try:
                        with self.schema_lock:
                            with session.begin():
                                # Further process the narrow data with the timestamp and device_info_id for insertion
                                # to the narrow table, by applying the timestamp and device_info_id to each metric/value pair.
                                self._flush_batch_narrow(narrow_data, device_info_id, timestamp, session, transport_name)

                                # Only attempt to write to the wide table if the row is valid and the table name is known.
                                # If the row fails validation, it may indicate a schema mismatch between the incoming data
                                # and the existing wide table columns — in this case we skip the wide table write to prevent data loss,
                                # but still write to the narrow table which is schema-flexible and can accept all incoming data.
                                if wide_table_name is not None:
                                    target_table: Table = Base.metadata.tables[wide_table_name]
                                    stmt: Insert = pg_insert(target_table).values(**wide_data)
                                    session.execute(stmt)
                                    if valid_row:
                                        self._log.info(f"data write complete from [{transport_name}] to timescaledb wide table for {len(metrics_only)} metrics.")
                                    else:
                                        self._log.info(f"Not a complete valid row for the wide table because {msg}, but wrote metrics anyway.")

                            self._commit_transport_state(transport_name, metrics_only, timestamp, is_stale=False)

                    except (SQLAlchemyError, ValueError) as e1:
                        session.rollback()
                        self._log.error(f"metrics data write failed.{e1}")

                        # Only backlog if setting enabled and DB is down
                        with self._reconnect_lock:
                            tsdb_connected: bool = self.tsdb_connected

                        if self.enable_persistent_storage and not tsdb_connected:
                            # Check if we can get the lock
                            acquired: bool = self._backlog_lock.acquire(blocking=False)
                            try:
                                if acquired:
                                    payload: dict[str, Any] = {
                                        "metrics": metrics_only,
                                        "device_info_id": device_info_id,
                                        "m_time": timestamp,
                                        "transport_name": transport_name,
                                        "protocol": protocol,
                                        "wide_table_name": wide_table_name,
                                    }
                                    self.backlog.enqueue(payload)

                            finally:
                                if acquired:
                                    self._backlog_lock.release()

                        # Handle recovery
                        if isinstance(e1, SQLAlchemyError):
                            self._set_tsdb_connected(conn_value = False, conn_reason = "Connection failure")
                            msg = ("Connection failure detected during flush worker write. Data has been "
                            "enqueued to backlog and will be retried when connection is restored.")
                            self._trigger_reconnect()

                    finally:

                        self._flush_queue.task_done()

                        # Clear flush event if queue is empty
                        if self._flush_queue.empty():
                            self._flush_event.clear()
                except Exception as e:
                    self._log.critical(f" Fatal Flush Worker Crash: {e}")
                    self._flush_queue.task_done()

        finally:
            session.close()

    def _process_raw_metrics(self, datacopy: dict[str, Any], protocol: str) -> tuple[dict[str, Any], dict[str, Any]]:
        try:
            processed_wide_data: dict[str, Any] = {}
            processed_narrow_data: dict[str, Any] = {}
            metric_mapping: dict[str, tuple[str, str]] = self.protocol_metric_mappings.get(protocol, {})

            for k, v in datacopy.items():
                mapping_info: tuple[str, str] | None = metric_mapping.get(k)

                # 1. Resolve naming and type info
                if mapping_info is not None:
                    clean_key, field_type = mapping_info
                    field_type_upper: str = field_type.upper()
                else:
                    clean_key = k
                    field_type_upper = "UNKNOWN"

                # 2. Prepare Narrow Data
                # We send the raw value so _flush_batch_narrow can sort it into numeric value vs ascii
                processed_narrow_data[clean_key] = v

                # 3. Prepare Wide Data (Apply coercion)
                try:
                    if v is None:
                        processed_wide_data[clean_key] = None
                        continue

                    if field_type_upper in self.INT_TYPES:
                        processed_wide_data[clean_key] = int(float(v))
                    elif field_type_upper in self.FLOAT_TYPES:
                        processed_wide_data[clean_key] = float(v)
                    elif field_type_upper == "BOOLEAN":
                        processed_wide_data[clean_key] = bool(int(v))
                    else:
                        # ascii and unknown types are stored as text in the wide table, with best effort coercion
                        # to string for non-string values.  This is to prevent data loss in the wide table for unexpected types,
                        # while still allowing all values to be stored in the narrow table.
                        processed_wide_data[clean_key] = str(v)

                except (ValueError, TypeError) as e1:
                    self._log.warning(f"Coercion failed for metric '{k}' (Value: {v}). Error: {e1}.")
                    processed_wide_data[clean_key] = v

        except Exception as e2:
            self._log.error(f"Error in _process_raw_metrics {e2}")
            return {}, {}
        else:
            self._log.debug("All metrics processed for wide and narrow paths")
            return processed_wide_data, processed_narrow_data

    def _flush_batch_narrow(self, newData: dict[str, Any], device_info_id: int, timestamp: datetime, session: Session, transport_name: str) -> None:
        try:
            reading_time: datetime = timestamp
            if isinstance(reading_time, str):
                reading_time = datetime.fromisoformat(reading_time)

            narrow_mappings: list[Any] = []
            processed_descriptions: dict[str, Any] = {}

            # Pass 1: Process and harvest all descriptions first
            for key, value in newData.items():
                if key.endswith("_desc"):
                    clean_key: str = key.removesuffix("_desc")
                    processed_descriptions[clean_key] = value

            # Pass 2: Build the rows and skip the original '_desc' keys
            for key, value in newData.items():
                if key.endswith("_desc"):
                    continue  # Skips this iteration so no row is created or appended

                row: dict[str,Any]  = {
                    "m_time": reading_time,
                    "device_info_id": device_info_id,
                    "metric_name": key,
                    "metric_value": 0.0,
                    "metric_ascii": None
                }

                # Numeric/Bool Logic
                if isinstance(value, (int, float)) and not isinstance(value, bool):
                    row["metric_value"] = float(value)
                elif isinstance(value, bool):
                    row["metric_value"] = 1.0 if value else 0.0
                # String Logic
                else:
                    row["metric_ascii"] = str(value) if value is not None else None
                    row["metric_value"] = 0.0

                # Upsert the matching description to the ascii field from Pass 1 for the code metric
                if row["metric_name"] in processed_descriptions:
                    row["metric_ascii"] = processed_descriptions[row["metric_name"]]

                narrow_mappings.append(row)

            if narrow_mappings:
                stmt: Insert = pg_insert(DeviceMetricsNarrow).values(narrow_mappings)
                upsert_stmt: Insert = stmt.on_conflict_do_nothing(index_elements=['m_time', 'device_info_id', 'metric_name'])
                session.execute(upsert_stmt)
                self._log.info(f"data write complete from [{transport_name}] to timescaledb narrow table for {len(narrow_mappings)} metrics.")

        except SQLAlchemyError as e:
            self._log.exception(f"Narrow flush failed: {e}")
            try:
                session.rollback()
            except SQLAlchemyError as e2:
                self._log.exception(f"Narrow flush rollback failed: {e2}")

            # === Auto Reconnect handling ===
            self._set_tsdb_connected(conn_value = False, conn_reason = "Connect unsuccessful")
            self._trigger_reconnect()

            raise  # Re-raise to be caught by outer handler for potential backlog queuing


    def _check_is_stale(self, transport_id: str, row: dict[str, Any], timestamp: datetime) -> bool:
        """_summary_   Stale Data Detection and Handling
            The following methods implement a mechanism to detect when incoming data from a transport has become stale
            (i.e., unchanged for a certain period) and to handle such situations by triggering reconnects and notifications.

        Args:
            transport_id (str):  the unique identifier for the transport whose data is being evaluated for staleness.
            row (dict): the latest data row received from the transport, which is compared against the last received data to determine if it has changed.
            timestamp (datetime):  the timestamp of when the latest data was received, used to calculate how long the data has been unchanged.

        Returns:
            bool: Returns True if the data is considered stale (unchanged for longer than the defined timeout),
                otherwise False.
        """
        state: dict[str, Any] | None = self._stale_registry.get(transport_id)
        if not state or state["last_row"] is None:
            return False

        # Comparison Logic (Numeric + Strict)
        for key, val in row.items():
            prev = state["last_row"].get(key)
            if isinstance(val, (int, float)) and isinstance(prev, (int, float)):
                if not math.isclose(val, prev, rel_tol=1e-4, abs_tol=1e-6):
                    return False
            elif val != prev:
                return False

        # Data is unchanged; check elapsed time
        elapsed = timestamp - state["start_ts"]
        return elapsed > timedelta(seconds=int(self.stale_data_timeout))

    def _commit_transport_state(self, transport_id: str, row: dict[str, Any], timestamp: datetime, is_stale: bool) -> None:
        """_summary_ The method _commit_transport_state is responsible for tracking the state of each transport's
            data freshness. It maintains a registry (_stale_registry) that records the last received data, the timestamp of when that
            data was received, and whether the data is currently considered stale. When new data arrives, this method
            updates the registry accordingly and triggers events if stale data is detected.

        Args:
            transport_id (str): The unique identifier for the transport whose state is being committed.
            row (dict): The latest data row received from the transport, which is used to compare against previous data for staleness checks.
            timestamp (datetime): The timestamp of when the data was received.
            is_stale (bool): A flag indicating whether the data is considered stale.
        """
        # Initialize if missing — start_ts and stale_event_count begin fresh here
        if transport_id not in self._stale_registry:
            self._stale_registry[transport_id] = {
                "last_row": row.copy(), "start_ts": timestamp, "is_stale": False,
                "last_seen": timestamp, "stale_event_count": 0, "last_event_ts": None
            }

        state: dict[str, Any] = self._stale_registry[transport_id]
        state["last_seen"] = timestamp
        self._log.debug(
            f"Committing state for transport: {transport_id} | "
            f"is_stale: {is_stale} | "
            f"elapsed: {timestamp - state['start_ts']}"
        )

        if not is_stale:
            # Reset everything on fresh data/successful write
            state.update({
                "last_row": row.copy(), "start_ts": timestamp,
                "is_stale": False, "stale_event_count": 0,   # ← counter resets here on recovery
                "last_event_ts": None                        # ← also reset the throttle timer
            })
        elif is_stale and not state["is_stale"]:
            # Only trigger the event ONCE per stale period
            state["is_stale"] = True
            state["last_event_ts"] = timestamp

            elapsed = timestamp - state["start_ts"]
            self._handle_stale_event(transport_id, timestamp, elapsed)

    def _cleanup_orphaned_locks(self) -> None:
        """
        Terminates stale backend connections that are holding locks on our
        tables but have no active query — typically left by a crashed client.

        Runs once at startup after connection is verified, before any schema
        work. Targets only connections to our specific database that are idle
        or idle-in-transaction and have been so for more than 60 seconds,
        excluding our own connection and the TimescaleDB background workers.

        Non-fatal — a failure here is logged and skipped, not raised.
        """
        try:
            with self.engine.connect().execution_options(
                isolation_level="AUTOCOMMIT"
            ) as conn:

                # Find stale idle connections holding locks on our tables.
                # Excludes our own pid, TimescaleDB workers, and active queries.
                stale = conn.execute(text("""
                    SELECT
                        pid,
                        state,
                        application_name,
                        now() - state_change AS idle_duration,
                        query
                    FROM pg_stat_activity
                    WHERE datname = :dbname
                    AND pid <> pg_backend_pid()
                    AND application_name NOT LIKE 'TimescaleDB%'
                    AND state IN ('idle in transaction', 'idle in transaction (aborted)')
                    AND now() - state_change > INTERVAL '60 seconds'
                """), {"dbname": self.database}).fetchall()

                if not stale:
                    self._log.debug("No orphaned backend connections found.")
                    return

                for pid, state, app_name, idle_duration, query in stale:
                    self._log.warning(
                        f"Terminating orphaned connection: pid={pid}, "
                        f"state='{state}', app='{app_name}', "
                        f"idle={idle_duration}, query='{str(query)[:80]}'"
                    )
                    try:
                        conn.execute(
                            text("SELECT pg_terminate_backend(:pid);"),
                            {"pid": pid}
                        )
                    except Exception as e:
                        self._log.error(
                            f"Failed to terminate pid {pid}: {e}"
                        )
                        # Non-fatal — continue with remaining connections

                self._log.info(f"Orphaned connection cleanup complete: {len(stale)} connection(s) terminated.")

        except Exception as e:
            self._log.error(f"_cleanup_orphaned_locks failed: {e}")
            # Non-fatal — proceed with startup even if cleanup fails

    def _cleanup_stale_registry(self) -> None:
        """Removes transports that haven't been seen in 7 days."""
        now: datetime = _now_tz()
        to_delete: List[str] = [
            tid for tid, s in self._stale_registry.items()
            if (now - s["last_seen"]).days >= 7
        ]
        for tid in to_delete:
            self._log.info(f"Decommissioning stale state for transport: {tid}")
            del self._stale_registry[tid]

    def _handle_stale_event(self, transport_id: str, current_time: datetime, total_stale_elapsed: timedelta) -> None:
        """
        Triggers a reconnect for a specific transport (max X times) with a gap between attempts.
        """
        state = self._stale_registry.get(transport_id)
        if not state:
            return

        # 1. Check max attempts for this transport
        if state["stale_event_count"] >= self.max_stale_attempts:
            self._log.debug(f"[{transport_id}] Max stale retry attempts reached. No further reconnects.")
            return

        # 2. Throttling: Has enough time passed since this transport's last attempt?
        if state["last_event_ts"] is not None:
            time_since_last = current_time - state["last_event_ts"]
            if time_since_last < timedelta(minutes=int(self.retry_delay_mins)):
                return

        # 3. Increment counters
        state["stale_event_count"] += 1
        state["last_event_ts"] = current_time

        # 4. Trigger Reconnect with Transport ID
        if self.request_upstream_reconnect:
            try:
                self._log.warning(f"[{transport_id}] Data stale. Requesting reconnect (Attempt {state['stale_event_count']}/{self.max_stale_attempts}).")
                # Pass the transport ID here
                self.request_upstream_reconnect(transport_id)
            except Exception:
                self._log.exception(f"[{transport_id}] Failed requesting upstream reconnect.")

        # 5. Notify
        try:
            minutes: float = total_stale_elapsed.total_seconds() / 60
            msg: str = (f"Transport [{transport_id}] stale for {minutes:.1f} mins. "
                f"Attempt {state['stale_event_count']} of {self.max_stale_attempts}.")
            self.send_message(message=msg,title="MPG Stale Event Alert", priority=1)
        except Exception:
            self._log.exception(f"[{transport_id}] Failed sending MPG notification.")


    # -------------------------
    # Bridge info pane — read-only snapshots for the admin UI's
    # "Bridge Health" and "Storage Overview" panels (see routers/timescale
    # .py GET /health and GET /storage). Both are purely observational:
    # nothing here creates, drops, or modifies anything.
    # -------------------------


    # -------------------------
    # Close / cleanup
    # -------------------------
    def close(self) -> None:
        self._log.debug("Closing transport")

        if self.rollup_mgr:
            try:
                self.rollup_mgr.stop_auto_refresh()
            except Exception as e:
                self._log.error(f"Error stopping auto refresh thread: {e}")
                self.rollup_mgr = None
                self.hypertable_mgr = None

        try:
            self._stop_thread_reconnect()
        except Exception as e:
            self._log.error(f"Error stopping reconnect thread: {e}")
        # Stop flush thread
        try:
            self._stop_event.set()
            self._flush_event.set()
            self._flush_queue.put(None)
            if self._flush_thread.is_alive():
                self._flush_thread.join(timeout=5.0)

        except Exception as e:
            self._log.error(f"Error stopping flush thread: {e}")

        try:
            self._connection_manager.unregister()
        except Exception as e:
            self._log.error(f"Error disposing engine: {e}")

    def __del__(self) -> None:
        try:
            if hasattr(self, "close") and callable(getattr(self, "close", None)):
                self.close()
        except Exception as e:
            if hasattr(self, '_log'):
                try:
                    self._log.error(f"Exception in __del__: {e}")
                except Exception:
                    self._log.error(f"Exception in __del__: {e}")
class SendMessageProtocol(Protocol):
    def __call__(
        self,
        message: str,
        title: str = "",
        priority: int = 0,
        services: Optional[Union[list[str], str]] = None,
        **kwargs: Any,
    ) -> None:
        ...
class BacklogManager:
    """
    Manages persistent backlog storage and replay for TimescaleDB writes.

    Owns:
    - in-memory backlog list
    - disk persistence (sqlite)
    - backlog synchronization
    - replay into the flush queue

    Does NOT:
    - talk to the database
    - manage sessions
    - manage reconnect logic

    Design: dual in-memory / SQLite storage.
    ----------------------------------------
    The in-memory list (backlog_points) is the fast working set. enqueue() and replay_to_queue()
    operate on it directly; SQLite is updated as a side effect and acts purely as a crash-recovery
    journal. If the process dies mid-outage, the points survive on disk and are reloaded on startup
    via load_from_disk(). The dual strategy is retained because many inverters may be connected
    through a single timescaledb instance, making the in-memory working set important for throughput
    during an outage when enqueue() may be called at high frequency.

    Atomicity contract:
    - enqueue():         SQLite write first -> memory update only if disk succeeds.
    - overflow eviction: SQLite DELETE first -> memory pop only if disk succeeds.
    - replay_to_queue(): memory cleared, then SQLite wiped via _clear_disk().
    This ordering ensures SQLite is always the authoritative record on crash. At worst a point
    exists on disk but not in memory (recovered on next load_from_disk()), never the reverse.

    rowid tracking:
    A parallel list (_backlog_rowids) stores the SQLite rowid for each entry in backlog_points
    at the same index. This enables O(1) single-row deletes on overflow, avoiding a full table
    rewrite (_sync_to_disk) on every eviction — important at scale with many connected inverters.
    """

    def __init__(
        self,
        backlog_file_path: Path,
        max_backlog_age: int,
        max_backlog_size: int,
        send_message: SendMessageProtocol,
        flush_queue: queue.Queue[dict[str, Any] | None],
        flush_event: threading.Event,
        backlog_lock: threading.RLock,
        log: logging.Logger
    ) -> None:

        self.backlog_file_path: Path = backlog_file_path
        self.max_backlog_age: int = max_backlog_age
        self.max_backlog_size: int = max_backlog_size
        self.send_message: SendMessageProtocol = send_message
        self._flush_queue: queue.Queue[dict[str, Any] | None] = flush_queue
        self._flush_event: threading.Event = flush_event
        self._backlog_lock: threading.RLock = backlog_lock
        self._log: logging.Logger = log

        self.backlog_points: list[dict[str, Any]] = []
        self._backlog_rowids: list[int] = []    # parallel to backlog_points: rowid[i] belongs to backlog_points[i]

    # -------------------------
    # Load Persistent backlog
    # -------------------------

    def _db_connect(self) -> sqlite3.Connection:
        """Open (and initialise if needed) the SQLite backlog database."""
        self.backlog_file_path.parent.mkdir(parents=True, exist_ok=True)
        con: sqlite3.Connection = sqlite3.connect(str(self.backlog_file_path))
        con.execute(
            """CREATE TABLE IF NOT EXISTS backlog (
                id      INTEGER PRIMARY KEY AUTOINCREMENT,
                m_time  TEXT    NOT NULL,
                payload TEXT    NOT NULL
            )"""
        )
        con.commit()
        return con

    def load_from_disk(self) -> None:
        """
        Load backlog points from SQLite into memory, applying age-based expiration and removing
        corrupted entries. Valid points populate backlog_points and _backlog_rowids together so
        subsequent enqueue/eviction operations can address rows directly by rowid.
        """
        if not self.backlog_file_path:
            return

        cutoff: datetime = _now_tz() - timedelta(seconds=int(self.max_backlog_age))
        loaded_points: list[dict[str, Any]] = []
        loaded_rowids: list[int] = []
        expired_ids: list[int] = []

        try:
            with self._backlog_lock:
                con: sqlite3.Connection = self._db_connect()
                try:
                    rows: List[Any] = con.execute(
                        "SELECT id, m_time, payload FROM backlog ORDER BY id"
                    ).fetchall()

                    for row_id, m_time_str, payload_str in rows:
                        try:
                            m_time: datetime = datetime.fromisoformat(m_time_str)
                            if m_time < cutoff:
                                expired_ids.append(row_id)
                                continue
                            point: dict[str, Any] = json.loads(payload_str)
                            loaded_points.append(point)
                            loaded_rowids.append(row_id)
                        except (json.JSONDecodeError, ValueError) as e:
                            self._log.info("Skipping corrupted backlog row %s: %s", row_id, e)
                            expired_ids.append(row_id)

                    if expired_ids:
                        # Use a static SQL query with a single placeholder
                        # Format the IDs as a list of single-item tuples: [(1,), (2,), (3,)]
                        id_parameters: List[Tuple[int]] = [(row_id,) for row_id in expired_ids]

                        con.executemany(
                            "DELETE FROM backlog WHERE id = ?",
                            id_parameters
                        )
                        con.commit()
                finally:
                    con.close()

                self.backlog_points = loaded_points
                self._backlog_rowids = loaded_rowids

            self._log.info("Loaded %d points from disk", len(loaded_points))

        except Exception as e:
            self._log.exception(f"Failed to process backlog file: {e}")
            self.send_message(
                message="Failed to load backlog from disk. Check logs for details.",
                title="MPG Backlog Load Error",
                priority=1
            )

    def enqueue(self, point: dict[str, Any]) -> None:
        """
        Add a point to the backlog. SQLite is always written before memory is updated so that
        a crash mid-call leaves SQLite as the authoritative record.

        On overflow, the oldest row is deleted from SQLite first, then removed from memory.
        The new point is then inserted into SQLite first, then appended to memory. If either
        disk operation fails the in-memory state is not modified and the exception propagates.

            example point dict:
            {
                "device_info_id": 3,                          # int — FK into device_info table
                "m_time": datetime(2026, 6, 2, 14, 23, 11,    # datetime (or ISO string after SQLite round-trip)
                                tzinfo=tzlocal()),
                "transport_name": "eg4_18kpv_inverter_1",     # str — identifies the source inverter
                "protocol":       "eg4_18kpv",                # str — used to route to the right wide table
                "wide_table_name": "wide__eg4_18kpv",         # str | None — None means narrow-only protocol

                "metrics": {                                  # dict — the raw readings from that poll cycle
                    "battery_voltage":   52.4,
                    "battery_soc":       87,
                    "pv_power":          3120,
                    "grid_frequency":    60.01,
                    "output_power":      2800,
                    "inverter_temp":     42.5,
                    # ... one key/value per register in the protocol registry
                }
}
        """
        if isinstance(point, list):
            raise TypeError("enqueue() does not accept lists, only single dict or None.")

        with self._backlog_lock:
            if len(self.backlog_points) >= self.max_backlog_size:
                self._log.warning(f"Max backlog size ({self.max_backlog_size}) reached. Dropping oldest point.")
                # Delete oldest from SQLite first — memory is unchanged until disk succeeds.
                oldest_rowid: int = self._backlog_rowids[0]
                self._delete_row_from_disk(oldest_rowid)
                # Disk succeeded — safe to evict from memory.
                self.backlog_points.pop(0)
                self._backlog_rowids.pop(0)

            # Insert new point into SQLite first — memory is unchanged until disk succeeds.
            new_rowid: int = self._append_to_disk(point)
            # Disk succeeded — safe to append to memory.
            self.backlog_points.append(point)
            self._backlog_rowids.append(new_rowid)

    def replay_to_queue(self) -> int:
        """
        Transfer all backlog points to the flush queue, then clear both memory and disk.
        Returns the number of points replayed.
        """
        with self._backlog_lock:
            if not self.backlog_points:
                return 0
            count: int = len(self.backlog_points)
            self._log.debug(f"Replaying {count} points to flush queue.")
            for payload in self.backlog_points:
                self._flush_queue.put(payload)
            # Clear memory first — the flush queue holds the authoritative copy during the
            # brief window before _clear_disk() completes.
            self.backlog_points.clear()
            self._backlog_rowids.clear()
            self._clear_disk()
            self.send_message(
                message=f"Replayed {count} backlog points to flush queue.",
                title="MPG Backlog Sent to Queue",
                priority=1
            )
            self._log.info(f"Backlog replay complete: {count} points sent to flush queue.")
        return count

    # -------------------------
    # Disk helpers
    # -------------------------

    def _append_to_disk(self, point: dict[str, Any]) -> int:
        """
        Insert a single point into SQLite. Returns the new rowid.
        Raises on failure — caller must not update memory if this raises.
        """
        if not self.backlog_file_path:
            return -1
        m_time_str: str = str(point.get("m_time", _now_tz().isoformat()))
        payload_str: str = json.dumps(point, default=str)

        try:
            con: sqlite3.Connection = self._db_connect()
            try:
                cur: sqlite3.Cursor = con.execute(
                    "INSERT INTO backlog (m_time, payload) VALUES (?, ?)",
                    (m_time_str, payload_str)
                )
                con.commit()
                if cur.lastrowid is None:
                    raise RuntimeError("Failed to retrieve rowid after insert.")
                return cur.lastrowid
            finally:
                con.close()
        except Exception as e:
            self._log.error(f"Failed to append point to backlog disk: {e}")
            self.send_message(
                message="Failed to write point to backlog on disk. Check logs for details.",
                title="MPG Backlog Write Error",
                priority=1
            )
            raise

    def _delete_row_from_disk(self, rowid: int) -> None:
        """
        Delete a single row from SQLite by its rowid.
        Raises on failure — caller must not update memory if this raises.
        """
        if not self.backlog_file_path:
            return
        try:
            con: sqlite3.Connection = self._db_connect()
            try:
                con.execute("DELETE FROM backlog WHERE id = ?", (rowid,))
                con.commit()
            finally:
                con.close()
        except Exception as e:
            self._log.error(f"Failed to delete row {rowid} from backlog disk: {e}")
            self.send_message(
                message="Failed to delete oldest point from backlog on disk. Check logs for details.",
                title="MPG Backlog Delete Error",
                priority=1
            )
            raise

    def _clear_disk(self) -> None:
        """
        Delete all rows from the SQLite backlog table. Called after replay_to_queue()
        once memory has already been cleared.
        """
        if not self.backlog_file_path:
            return
        try:
            con: sqlite3.Connection = self._db_connect()
            try:
                con.execute("DELETE FROM backlog")
                con.commit()
            finally:
                con.close()
        except Exception as e:
            self._log.error(f"Failed to clear backlog from disk: {e}")
            self.send_message(
                message="Failed to clear backlog on disk. Check logs for details.",
                title="MPG Backlog Clear Error",
                priority=1
            )
            raise
class HyperTableManager:
    """
    Owns TimescaleDB hypertable-level mechanics: creating hypertables,
    compression policies, retention policies, dynamic chunk/compression
    sizing for the two raw source tables (device_metrics_narrow and each
    device_metrics_wide__*), and the generic background-job / resource-
    tier plumbing that both raw-table maintenance and rollup-view
    maintenance lean on.

    Why this exists as its own class (and not folded back into
    RollupManager): TimescaleDB itself draws this same line. A
    continuous aggregate is, under the hood, its own hypertable -- the
    same create_hypertable/add_compression_policy/add_retention_policy/
    timescaledb_information.jobs machinery this class wraps is what
    TimescaleDB uses internally for a CAGG's backing storage too. What's
    genuinely CONTINUOUS-AGGREGATE-specific -- the view SQL itself,
    add_continuous_aggregate_policy, refresh scheduling/watchdog,
    cardinality-based view sizing -- stays on RollupManager. Everything
    here is the lower-level "how a hypertable is stored, chunked,
    compressed and retained" substrate that both the two raw tables AND
    RollupManager's own CAGGs are built on.

    Relationship to RollupManager: this class has no hard dependency on
    RollupManager -- it's the lower-level primitive and is constructed
    first (see timescaledb._init_or_update_rollup_manager). But a few
    operations are genuinely bidirectional (TimescaleDB's own
    interlocking of hypertable policy and CAGG definitions doesn't
    factor into one strict direction): dynamic-sizing retune
    (_do_retune_work) needs to trigger RollupManager.add_wide_rollup /
    setup_narrow_rollup to rebuild the CAGGs on top of a newly-retuned
    raw table, and the shared performance-tier lookup
    (get_dynamic_settings_helper) needs to know how many rollup views
    are currently registered. Rather than forcing those through an
    awkward one-directional API, this class takes a late-bound
    `rollup_mgr` reference, set once by the bridge right after both
    managers are constructed. Every call site that uses it guards for
    None, since this manager's own constructor unavoidably runs before
    RollupManager's.

    Relationship to BridgeAdminManager (the wide-table field-editing /
    UI-diagnostics class): BridgeAdminManager calls into this class for
    the underlying pause/resume-compression-job and decompress-chunks
    mechanics it needs around a column drop, and composes its
    diagnostic summaries (get_compression_retention_summary,
    get_background_jobs) from this class's config/overview methods
    together with RollupManager's rollup-side ones -- it does not
    duplicate any hypertable mechanics of its own.
    """

    def __init__(
        self,
        rollup_policy: dict[str, Any],
        my_session_factory: sessionmaker[Session],
        log: logging.Logger,
        ) -> None:

        self.rollup_policy: dict[str, Any] = rollup_policy
        self.SessionFactory: sessionmaker[Session] = my_session_factory
        self._log: logging.Logger = log

        # Late-bound back-reference to the RollupManager sharing this
        # bridge -- see class docstring. Set once by
        # timescaledb._init_or_update_rollup_manager right after both
        # managers exist. None only during the brief construction window
        # before that happens; every internal usage below guards for it.
        self.rollup_mgr: "RollupManager | None" = None

        # Performance tiers for heavier DDL/refresh-style operations
        # (decompress/recompress, CAGG refreshes, etc.), scaled by how
        # many hypertables + rollup views this bridge is currently
        # managing. Shared by both raw-table maintenance (this class)
        # and RollupManager's refresh/rebuild paths (via
        # self.rollup_mgr / hypertable_mgr cross-calls) -- see
        # get_dynamic_settings_helper.
        self.performance_tiers: dict[str, dict[str, Any]] = {
        "tier_low":    {"count": 50,  "work_mem": "32MB",  "lock_timeout": "10s", "flush_batch_size": 10},
        "tier_medium": {"count": 100, "work_mem": "64MB",  "lock_timeout": "15s", "flush_batch_size": 20},
        "tier_high":   {"count": 160, "work_mem": "128MB", "lock_timeout": "30s", "flush_batch_size": 40},
        }

        # Tracks metric_count per protocol, keyed by protocol_name.
        # Populated in RollupManager.add_wide_rollup from
        # protocol_registry, read here (via _max_wide_column_count_helper)
        # so get_dynamic_settings_helper can determine tier without a DB hit.
        self.protocol_wide_column_counts: dict[str, int] = {}

        # Hypertable defaults. These are not configurable by the user but
        # can be overridden by changing the below rollup_policy if needed.
        # These settings are critical for ensuring that the hypertable is
        # created with the appropriate chunking and compression settings
        # to optimize performance and storage efficiency for time-series
        # data. RollupManager reads the four rollup-view granularity
        # entries (hourly/daily/weekly/monthly_*) out of this same dict
        # for its own CAGG chunk/compress-after settings, rather than
        # this class duplicating a second copy -- see RollupManager.
        # __init__.
        self.hypertable_defaults: dict[str, str] = {
            "compress_segmentby_narrow": "device_info_id, metric_name",
            "compress_segmentby_wide": "device_info_id",
            "time_column": "m_time",
            "compress_orderby": "m_time DESC",
            "hourly_chunk_time_interval": "1 day",
            "hourly_compress_after_interval": "3 days",
            "daily_chunk_time_interval": "7 days",
            "daily_compress_after_interval": "2 weeks",
            "weekly_chunk_time_interval": "1 month",
            "weekly_compress_after_interval": "2 months",
            "monthly_chunk_time_interval": "4 months",
            "monthly_compress_after_interval": "6 months",
            # Compression scheduling for the two raw tables — deliberately
            # SEPARATE settings, not one shared value, same reasoning as
            # the chunk-interval split below: narrow and wide have very
            # different write/lock-contention profiles, so one compress_
            # after was never going to be safe for both.
            #
            # TimescaleDB's own guidance is that compress_after should be
            # roughly 2-3x a table's OWN chunk_time_interval — enough
            # margin that a chunk is unambiguously past its active write
            # window before compression touches it, avoiding lock
            # contention with the flush thread. That means compress_after
            # has to be derived per-table from that table's own chunk
            # interval, not picked as one independent constant: applied to
            # narrow's 1-day chunks, ~2-3 days is correct; applied to
            # wide's 7-day chunks, that same "2-3 days" would be
            # attempting to compress a chunk barely a third of the way
            # through its still-active window.
            #
            # Defaults below follow that same ~2-3x multiple of each
            # table's own chunk_time_interval above, and are starting
            # points, not guarantees -- actual safe values depend on real
            # write volume and lock contention on your hardware. Watch the
            # Background Job Status panel's policy_compression rows for
            # this table; repeated failures there is the signal to widen
            # the corresponding interval.
            "raw_narrow_compress_after_interval": "3 days",   # ~3x raw_narrow_chunk_time_interval (1 day)
            "raw_wide_compress_after_interval": "14 days",    # ~2x raw_wide_chunk_time_interval (7 days)
            # Chunk sizing for the two raw tables — deliberately SEPARATE
            # settings, not one shared value. TimescaleDB's own sizing
            # guidance is to pick chunk_time_interval so a single chunk
            # (with its indexes) stays roughly within 25% of available
            # memory, and so chunk count stays in a sane range — neither
            # too few oversized chunks nor too many tiny ones. That target
            # is fundamentally about row/byte volume per chunk, not wall-
            # clock time.
            #
            # device_metrics_narrow and each device_metrics_wide__* table
            # do NOT have comparable row density for the same time span:
            # every write cycle adds ONE row per device to a wide table,
            # but up to ~160 rows (one per metric) to the shared narrow
            # table — and narrow accumulates that multiplied volume from
            # EVERY protocol's wide table at once, not just one.
            #
            # Defaults below are starting points, not guarantees for any
            # specific deployment — actual row/byte density per chunk
            # depends on device count, protocol count, metric count, and
            # scrape interval, none of which this code can see in advance.
            # Use the Storage Overview panel's chunk_count and size columns
            # to check whether real chunks are landing in a healthy range
            # (TimescaleDB's own rule of thumb: comfortably under ~25% of
            # available memory per chunk) and tune from there.
            "raw_narrow_chunk_time_interval": "1 day",
            "raw_wide_chunk_time_interval": "7 days",
        }

        self.if_not_exists = True

        self.enable_compression = bool(self.rollup_policy.get("enable_compression", True))
        self.drop_after: str = self.rollup_policy.get("drop_after", "1 year")

        # When True (default), raw-table chunk_time_interval and compress_
        # after are computed dynamically from live scrape load (see
        # get_dynamic_raw_table_settings_helper) instead of using the
        # static raw_narrow_*/raw_wide_* settings directly. Those static
        # settings remain as the fallback when there isn't yet enough live
        # data to classify a table (e.g. right after a fresh connect,
        # before any scraper has written through this bridge), or when
        # this is disabled.
        self.enable_dynamic_chunk_sizing = bool(self.rollup_policy.get("enable_dynamic_chunk_sizing", True))

        # Compression settings
        self.compress_segmentby_narrow: str = self.hypertable_defaults["compress_segmentby_narrow"]
        self.compress_segmentby_wide: str = self.hypertable_defaults["compress_segmentby_wide"]
        self.compress_orderby: str = self.hypertable_defaults["compress_orderby"]
        self.time_column: str = self.hypertable_defaults["time_column"]

        self.raw_narrow_compress_after_interval: str = self.hypertable_defaults["raw_narrow_compress_after_interval"]
        self.raw_wide_compress_after_interval: str = self.hypertable_defaults["raw_wide_compress_after_interval"]
        self.raw_narrow_chunk_time_interval: str = self.hypertable_defaults["raw_narrow_chunk_time_interval"]
        self.raw_wide_chunk_time_interval: str = self.hypertable_defaults["raw_wide_chunk_time_interval"]

        # Live-tracked (protocol_name, read_interval) per transport_name,
        # populated once per transport by timescaledb.init_bridge() ->
        # record_transport_interval() -- read_interval is fixed for a
        # transport's lifetime, so this is captured once at registration.
        # This is the one piece of load-sizing data that can't come from
        # protocol_registry or from any static setting -- it only exists
        # on the live transport object itself. Deliberately separate from
        # the older protocol_wide_column_counts / performance_tiers
        # mechanism above, which tunes REFRESH/DDL lock and memory
        # behavior, not chunk/compression sizing -- different concern,
        # kept as a distinct dict rather than overloading that one.
        self.transport_read_intervals: dict[str, tuple[str, float]] = {}

        # -------------------------
        # Dynamic-sizing retune tracking (see note_device_metric_count_known).
        #
        # setup_narrow_rollup() / add_wide_rollup() run once, synchronously,
        # during schema registration -- before any transport has written
        # real data -- so a brand-new protocol/device almost always lands
        # on get_dynamic_raw_table_settings_helper's "Static (no live data
        # yet)" fallback and stays there forever, since nothing else ever
        # calls those methods again. This state lets a single, targeted
        # re-run happen the first time enough real data actually exists,
        # without polling or periodically re-checking tables that are
        # already sized correctly.
        #
        # Keyed by protocol_name for a wide table's own retune, or None for
        # the shared narrow table (whose load score sums every protocol at
        # once -- see _compute_metric_writes_per_day_helper).
        #
        # A wide table can be fed by MULTIPLE transports of the same
        # protocol (e.g. several inverters of the same model, each with its
        # own variable_mask and therefore its own device_info.metric_count).
        # Firing the retune after only the first of those transports
        # reports would still undercount the others (their metric_count is
        # still unknown at that point) and lock in the wrong band just as
        # permanently as the original bug. _retune_reported tracks which
        # transports HAVE reported for a pending key; the retune only fires
        # once that covers every transport transport_read_intervals
        # currently knows about for that key -- or once that key's bounded
        # timeout elapses, whichever comes first, so one offline device
        # can't keep the table on static settings indefinitely.
        self._retune_reported: dict[str | None, set[str]] = {}
        self.retune_timer: dict[str | None, threading.Timer] = {}
        self._retuned: set[str | None] = set()
        self.retune_lock: threading.Lock = threading.Lock()
        # How long to wait, after the first transport for a pending key
        # reports, before retuning anyway even if not every expected
        # transport has reported yet (covers an offline/misconfigured
        # device in a multi-transport protocol). Not a periodic re-check --
        # this timer is armed once per key and fires at most once.
        self.retune_timeout_seconds: float = float(self.rollup_policy.get("retune_timeout_seconds", 300))

    @property
    def tsdb_connected(self) -> bool:
        """Always returns the live connection state from the shared policy dict.
            Mirrors RollupManager.tsdb_connected -- both classes take the
            same rollup_policy dict, so this is duplicated rather than
            forced through the rollup_mgr back-reference (which isn't set
            yet during this class's own construction window).
        """
        val: Any = self.rollup_policy.get("tsdb_connected", False)
        if val is True or val == "True":
            return val is True
        else:
            return False

    # === moved: record_transport_interval (3644-3671) ===
    def record_transport_interval(self, transport_name: str, protocol_name: str, read_interval: float) -> None:
        """
        Records the read_interval for one scraper transport, keyed by
        transport_name. Called once from timescaledb.init_bridge() when
        that transport registers with this bridge -- not on every write.
        read_interval is fixed for a transport's lifetime (it isn't
        reloaded mid-run). init_bridge is called exactly once per transport,
        which is the natural one-time hook for this, same as it already is
        for per-protocol schema registration.

        Deliberately called even for a transport whose protocol has
        already been registered by another transport instance (init_bridge
        checks that separately, for schema setup, and returns early only
        after this call) -- two transport instances can share one protocol
        (e.g. two battery packs on the same wide table) and, in principle,
        run at different intervals; recording only the first one seen
        would silently undercount that protocol's real combined load.

        This is the only source this class has for scrape cadence -- there
        is no cross-database lookup here, no reference to the gateway or
        the staging settings DB, just whatever the live transport object
        reports. See get_dynamic_raw_table_settings_helper for how this
        feeds into chunk/compression sizing.
        """
        if read_interval and read_interval > 0:
            self.transport_read_intervals[transport_name] = (protocol_name, read_interval)



    # === moved: ensure_hypertables (3798-3875) ===
    def ensure_hypertables(self, tables: list[str], chunk_time_interval: str | None = None) -> None:
        """
        Ensure the provided source tables are TimescaleDB hypertables.

        The helper preserves the original `create_hypertable(... if_not_exists)`
        behavior while letting both narrow and wide orchestration paths pass in
        the tables they need processed.

        chunk_time_interval, if given, is applied two ways:
        1. Passed to create_hypertable() itself, for the true first-ever
           creation of a hypertable.
        2. Also applied via a separate set_chunk_time_interval() call,
           unconditionally, for every table -- because create_hypertable(
           if_not_exists => TRUE) against an ALREADY EXISTING hypertable
           is a no-op: it does not update chunk_time_interval on an
           existing table, the same class of silent-no-op issue already
           fixed for compression/retention policies (see ensure_
           compression_policy's docstring). Without the explicit set_
           chunk_time_interval() call, changing raw_narrow_chunk_time_
           interval or raw_wide_chunk_time_interval in settings would only
           ever take effect on a brand-new hypertable.

        set_chunk_time_interval() only affects chunks created AFTER the
        call -- like every other policy-style setting in this class, it
        never touches or resizes chunks that already exist.
        """
        params: dict[str, Any] = {
            "time_col": getattr(self, "time_column", "m_time"),
            "if_exists": getattr(self, "if_not_exists", True),
            "migrate": getattr(self, "migrate_data", True),
        }

        try:
            with self.SessionFactory() as session:
                with session.begin():
                    for table in tables:
                        if chunk_time_interval:
                            session.execute(
                                text(
                                    f"""
                                    SELECT create_hypertable(
                                        '{table}',
                                        :time_col,
                                        chunk_time_interval => INTERVAL '{chunk_time_interval}',
                                        if_not_exists => :if_exists,
                                        migrate_data => :migrate
                                    )
                                    """
                                ),
                                {
                                    **params,
                                },
                            )
                            session.execute(
                                text(f"SELECT set_chunk_time_interval('{table}', INTERVAL '{chunk_time_interval}');")
                            )
                        else:
                            session.execute(
                                text(
                                    f"""
                                    SELECT create_hypertable(
                                        '{table}',
                                        :time_col,
                                        if_not_exists => :if_exists,
                                        migrate_data => :migrate
                                    )
                                    """
                                ),
                                {
                                    **params,
                                },
                            )
                self._log.debug(f"Hypertable creation ensured for: {', '.join(tables)}")

        except SQLAlchemyError as e:
            self._log.error("Failed to ensure hypertables: %s", e)
            raise


    # === moved: configure_compression (3876-3901) ===
    def configure_compression(self, table_name: str, segment_by: str, compress_after_interval: str) -> None:
        """
        Apply compression settings and the compression policy to one source table.

        The table structure is immutable after creation, but policy scheduling
        remains configurable. This helper keeps those concerns together for
        both shared narrow and protocol-specific wide sources.

        compress_after_interval is a single value: `table_name` is one raw
        hypertable, which can only ever have one compression policy — see
        setup_rollup's docstring for why this used to (incorrectly) take a
        list of 4 granularity-specific intervals instead.
        """
        with self.SessionFactory() as session:
            self._log.info("Setting up compression policy")
            if not session:
                self._log.error("Cannot set up compression — not tsdb_connected.")
                return

            self._apply_compression_helper(session, table_name, segment_by)

            with session.begin():
                self.ensure_compression_policy(table_name, compress_after_interval)

                session.commit()


    # === moved: policy_config_matches_helper (3902-3973) ===
    def policy_config_matches_helper(
        self,
        session: Session,
        target_name: str,
        proc_name: str,
        config_key: str,
        desired_interval: str
        ) -> bool:
        """
        True if `target_name` (a hypertable or continuous-aggregate view
        name) already has an active `proc_name` job (policy_compression or
        policy_retention) whose config[config_key] is semantically equal to
        desired_interval -- i.e. removing and re-adding it right now would
        be a pure no-op.

        Used to skip the unconditional remove-then-add pattern in
        ensure_compression_policy / apply_retention_policy /
        _add_aggregate_policy_helper on passes where nothing actually
        changed -- e.g. a dynamic-sizing retune (see RollupManager.
        note_device_metric_count_known) that happens to compute the same
        band as before, or a plain reconnect that re-walks every
        protocol's setup regardless of whether anything's different.
        Skipping that churn matters beyond tidiness: dropping a job and
        immediately recreating it can race TimescaleDB's own background
        scheduler if a run for that job happens to already be due --
        the in-flight execution can fail against a job_id that no longer
        exists the instant remove_*_policy() executes. Observed in
        practice as a one-off Failed run on a freshly retuned protocol's
        rollup-view compression jobs, with FAILURES staying at 1 (not
        climbing) once the scheduler's own retry picked it back up --
        i.e. harmless, but avoidable.

        Deliberately conservative: only ever returns True on a clean,
        confirmed match. "No existing job for this target/proc_name",
        "config key missing from that job", "value doesn't parse as an
        interval", and "the query itself failed" are all treated the
        same as "go ahead and remove-then-add" (i.e. return False) --
        a false negative here only costs the caller the normal,
        already-safe unconditional path it always used before; a false
        positive would leave a genuinely stale policy running.
        """
        try:
            current: str | None = session.execute(
                text("""
                    SELECT config->>:config_key
                    FROM timescaledb_information.jobs
                    WHERE proc_name = :proc_name AND hypertable_name = :target_name
                    ORDER BY job_id DESC
                    LIMIT 1
                """),
                {"config_key": config_key, "proc_name": proc_name, "target_name": target_name},
            ).scalar()
            if not current:
                return False
            # Same "let Postgres judge interval equality" idiom already
            # used for the CAGG bucket-interval comparison above (see
            # rollup_needs_rebuild) -- correctly treats e.g. '1 day' and
            # '24 hours' as equal rather than doing brittle string
            # matching.
            return bool(
                session.execute(
                    text("SELECT (:current)::interval = (:desired)::interval"),
                    {"current": current, "desired": desired_interval},
                ).scalar()
            )
        except SQLAlchemyError as e:
            self._log.debug(
                f"policy_config_matches_helper: could not compare '{proc_name}' config['{config_key}'] "
                f"on '{target_name}': {e}"
            )
            return False


    # === moved: apply_retention_policy (3974-4017) ===
    def apply_retention_policy(self, table_name: str) -> None:
        """
        Apply the configured retention policy to one source table.

        This preserves the previous remove-and-readd behavior so changes to the
        retention interval still take effect on restart -- but skips that
        remove-then-add entirely when the existing policy_retention job's
        drop_after already matches self.drop_after (see
        policy_config_matches_helper), since that's unconditional churn
        on every reconnect/retune pass in the common case where drop_after
        hasn't actually changed.
        """
        with self.SessionFactory() as session:
            if not session:
                self._log.error("Cannot add retention policies — not tsdb_connected.")
                return

            try:
                if self.policy_config_matches_helper(
                    session, table_name, "policy_retention", "drop_after", self.drop_after
                ):
                    self._log.debug(
                        f"apply_retention_policy: '{table_name}' already drop_after={self.drop_after}, "
                        f"skipping remove/re-add."
                    )
                    return

                # Remove and re-add policy to ensure interval updates apply
                session.execute(text(f"SELECT remove_retention_policy('{table_name}', if_exists => True);"))
                session.execute(text(f"SELECT add_retention_policy('{table_name}', INTERVAL '{self.drop_after}');"))

                session.commit()
                self._log.debug(f"Retention policy updated for {table_name}")

            except SQLAlchemyError as e:
                self._log.error(f"Error updating retention policy for {table_name}: {e}")
                try:
                    session.rollback()
                except SQLAlchemyError as e2:
                    self._log.error(f"Rollback failed for {table_name}: {e2}")
                raise


    # 5 Start the rollup thread.  Called from TimescaleDB class upon connection to the database.

    # === moved: _apply_compression_helper (4029-4048) ===
    def _apply_compression_helper(self, session: Session, table_name: str, segment_by: str) -> None:
        """Apply compression ALTER TABLE settings to one source table."""
        try:
            sql: TextClause = text(
                f"ALTER TABLE {table_name} SET ("
                f"timescaledb.compress, "
                f"timescaledb.compress_orderby = '{self.compress_orderby}', "
                f"timescaledb.compress_segmentby = '{segment_by}'"
                ");"
            )
            session.execute(sql)
            session.commit()
            self._log.debug(f"Compression enabled on {table_name}")
        except SQLAlchemyError as e:
            self._log.error(f"Error enabling compression on {table_name}: {e}")
            try:
                session.rollback()
            except SQLAlchemyError as e2:
                self._log.error(f"Rollback error for {table_name}: {e2}")


    # === moved: ensure_compression_policy (4049-4116) ===
    def ensure_compression_policy(self, source: str, chunk_interval: str) -> None:
        """
        Automatically compress chunks older than chunk_interval on `source`.

        Removes and re-adds the policy every time, rather than relying on
        add_compression_policy(..., if_not_exists => TRUE) alone -- that
        call is a genuine no-op when a policy already exists for `source`:
        TimescaleDB issues a NOTICE and leaves the existing job's
        compress_after untouched, it does not update it to the new
        interval. Without the remove-then-add here, editing e.g.
        hourly_compress_after_interval in code would silently never reach
        an already-configured hypertable -- only a brand new one would
        ever pick up the new value, while the old job kept running (and
        potentially kept failing) at its original interval forever. This
        mirrors apply_retention_policy's own fix for the identical
        problem -- see that method's docstring.

        Removing and re-adding only affects the *ongoing* job's future
        schedule -- it does not touch chunks TimescaleDB has already
        compressed under the previous interval, the same way changing a
        retention policy doesn't retroactively undo already-dropped data.
        Both statements run in the same transaction, committed together,
        so a failure on the add rolls back the remove too -- this never
        leaves `source` with no compression policy at all.

        Skips the remove-then-add entirely when the existing policy_
        compression job's compress_after already matches chunk_interval
        (see policy_config_matches_helper) -- avoids unnecessary churn
        on a reconnect or dynamic-sizing retune that recomputes the same
        band as before, and avoids racing TimescaleDB's own background
        scheduler if a run happens to already be due for that job.

        Parameters:
            source (str): The hypertable to which the compression policy will be applied (e.g. "device_metrics_narrow").
            chunk_interval (str): The time interval after which chunks should be compressed (e.g. "1 day", "7 days").
        """
        with self.SessionFactory() as session:

            if not session:
                    self._log.error("Cannot add compression policy — not tsdb_connected.")
                    return

            try:
                if self.policy_config_matches_helper(
                    session, source, "policy_compression", "compress_after", chunk_interval
                ):
                    self._log.debug(
                        f"ensure_compression_policy: '{source}' already compress_after={chunk_interval}, "
                        f"skipping remove/re-add."
                    )
                    return

                # Remove and re-add policy to ensure interval updates apply
                # -- same reasoning as apply_retention_policy.
                session.execute(text(f"SELECT remove_compression_policy('{source}', if_exists => TRUE);"))
                session.execute(
                    text(f"SELECT add_compression_policy('{source}', compress_after => INTERVAL '{chunk_interval}');")
                )
                session.commit()

                self._log.debug(f"Compression policy on {source} set to compress_after={chunk_interval}")
            except SQLAlchemyError as e:
                self._log.error(f"ensure_compression_policy {source} for {chunk_interval} error: {e}")
                try:
                    session.rollback()
                except SQLAlchemyError as e2:
                    self._log.error(f"ensure_compression_policy {source} for {chunk_interval} rollback error: {e2}")


    # === moved: _compression_group_tables (5559-5624) ===
    def _compression_group_tables(self, protocol_names: set[str] | None = None) -> list[dict[str, Any]]:
        """
        Resolves each compression group's member tables -- the group's raw
        hypertable plus its hourly/daily/weekly/monthly rollup views (all
        five are independently compressible hypertables in TimescaleDB;
        continuous aggregates materialize into their own hypertable).

        Uses the same group keys as rebuild_all_rollups() /
        refresh_selected_rollups() above (a wide-table protocol_name, plus
        the literal "shared_narrow" for the shared narrow stack) and the
        same ordering (shared narrow stack first, then wide protocols
        alphabetically) list_rollup_views() produces, so a group's
        position/selection here always lines up with what the admin sees
        on the Rebuild Rollup Views screen's checkboxes.

        protocol_names=None resolves every group with selected=True. An
        empty set resolves every group with selected=False -- callers use
        `selected` to decide whether to act on a group, not whether to
        include it in the returned list, so a group always has a result
        row even when it's skipped.
        """
        groups: list[dict[str, Any]] = [{
            "protocol_name": "shared_narrow",
            "wide_table_name": "device_metrics_narrow",
            "selected": protocol_names is None or "shared_narrow" in protocol_names,
            "tables": [
                "device_metrics_narrow",
                "hourly_rollup_narrow",
                "daily_rollup_narrow",
                "weekly_rollup_narrow",
                "monthly_rollup_narrow",
            ],
        }]

        with self.SessionFactory() as session:
            protocol_rows: Sequence[Row[Any]] = session.execute(
                text("""
                    SELECT protocol_name, rollup_prefix, wide_table_name
                    FROM protocol_registry
                    WHERE wide_table_name IS NOT NULL
                    ORDER BY protocol_name
                """)
            ).fetchall()

        for protocol_name, rollup_prefix, wide_table_name in protocol_rows:
            tables: list[str] = [wide_table_name]
            if rollup_prefix:
                # Registered but rollup setup hasn't finished yet -- the
                # wide table itself is still a valid compression target,
                # it just doesn't have rollup views to add yet.
                tables.extend(f"{gran}_{rollup_prefix}" for gran in ("hourly", "daily", "weekly", "monthly"))
            groups.append({
                "protocol_name": protocol_name,
                "wide_table_name": wide_table_name,
                "selected": protocol_names is None or protocol_name in protocol_names,
                "tables": tables,
            })

        return groups

    # Every group's tables follow the same [raw, hourly, daily, weekly,
    # monthly] shape (see _compression_group_tables) -- this is just the
    # granularity label for a table at a given position in that list, used
    # to annotate each inventory row for the UI.
    _GRANULARITY_BY_POSITION: tuple[str, ...] = ("raw", "hourly", "daily", "weekly", "monthly")


    # === moved: list_compression_groups (5625-5792) ===
    def list_compression_groups(self) -> list[dict[str, Any]]:
        """
        Read-only compression inventory for the Rebuild Compression screen
        -- one row per group (shared narrow stack, plus each wide-table
        protocol), each carrying a per-table breakdown (raw table + all
        four rollup views) of chunk counts and on-disk size, so the admin
        can see the actual cost of a rebuild before committing to it
        rather than clicking blind.

        Answered with a SINGLE query across every group's tables at once,
        rather than the 2-queries-per-table loop this replaced. Two
        reasons that's the better trade here, not just a micro-
        optimization:
          1. Round trips dominate: with ~5-10 groups x 5 tables each, the
             old loop was 50-100 sequential round trips to Postgres before
             the page could render. One batched query is one round trip,
             and GROUP BY lets Postgres answer it off a single scan of
             timescaledb_information.chunks instead of one scan per table.
          2. Simpler error handling: to_regclass()/hypertable_size() on a
             table that doesn't exist yet (rollups mid-setup for a
             protocol) just yields NULL/0 inline, so there's no per-table
             try/except -- one missing view no longer needs special-casing
             to avoid blanking out an otherwise-valid group.

        A view-vs-hypertable subtlety the single query has to account for:
        a rollup view's NAME (e.g. "hourly_rollup_wide_eg4_18kpv") is not
        the name TimescaleDB's chunk/size catalogs index by -- continuous
        aggregates materialize into a separate, internally-named
        hypertable, and that's what timescaledb_information.chunks and
        hypertable_size() need. The query below resolves that via
        timescaledb_information.continuous_aggregates (the public,
        version-stable catalog view) before joining chunks/computing size,
        falling back to the table's own name unchanged for the raw wide/
        narrow tables, which aren't continuous aggregates and don't need
        resolving.

        raw_size_bytes is the sum of chunk_compression_stats().
        before_compression_total_bytes across the table's currently-
        compressed chunks -- the size that data would take up UNcompressed,
        i.e. what "Rebuild Compression" is shrinking it back down from.
        Unlike size_bytes (chunk_compression_stats() is called directly
        with the table/view's own name here, same as rebuild_compression()
        already does -- TimescaleDB resolves a continuous aggregate's
        public view to its materialization hypertable internally for this
        function, no manual resolution needed the way size_bytes needs
        for hypertable_size()). Deliberately only covers chunks that ARE
        compressed: an uncompressed chunk has no separate before/after
        distinction to query -- its current on-disk size already IS its
        raw size, so a table with nothing compressed yet reports
        raw_size_bytes=0 rather than double-counting via size_bytes.

        (There's also a lower-level variant of this same query that joins
        _timescaledb_catalog.* system tables directly by OID instead of
        timescaledb_information.* -- probably marginally faster, but those
        catalog tables are TimescaleDB's internal implementation detail,
        not its supported public API, and can change shape across
        versions without notice. This inventory isn't a hot path -- it
        loads once per page view and once after a rebuild -- so the public
        catalog views are the safer choice here.)
        """
        groups: list[dict[str, Any]] = self._compression_group_tables(protocol_names=None)

        granularity_by_table: dict[str, str] = {}
        all_table_names: list[str] = []
        for group in groups:
            for i, table_name in enumerate(group["tables"]):
                granularity_by_table[table_name] = self._GRANULARITY_BY_POSITION[i]
                all_table_names.append(table_name)

        with self.SessionFactory() as session:
            stat_rows: Sequence[Row[Any]] = session.execute(
                text("""
                    WITH target_tables AS (
                        SELECT unnest(CAST(:table_names AS text[])) AS view_name
                    ),
                    resolved AS (
                        SELECT
                            t.view_name,
                            coalesce(
                                ca.materialization_hypertable_schema || '.' || ca.materialization_hypertable_name,
                                t.view_name
                            ) AS qualified_relation_name,
                            coalesce(ca.materialization_hypertable_name, t.view_name) AS hypertable_name
                        FROM target_tables t
                        LEFT JOIN timescaledb_information.continuous_aggregates ca
                            ON ca.view_name = t.view_name
                    ),
                    chunk_counts AS (
                        SELECT
                            r.view_name,
                            r.qualified_relation_name,
                            count(ch.chunk_name) AS total_chunks,
                            count(ch.chunk_name) FILTER (WHERE ch.is_compressed) AS compressed_chunks
                        FROM resolved r
                        LEFT JOIN timescaledb_information.chunks ch
                            ON ch.hypertable_name = r.hypertable_name
                        GROUP BY r.view_name, r.qualified_relation_name
                    ),
                    raw_size AS (
                        SELECT
                            t.view_name,
                            coalesce(sum(s.before_compression_total_bytes), 0) AS raw_size_bytes
                        FROM target_tables t
                        LEFT JOIN LATERAL (
                            SELECT before_compression_total_bytes
                            FROM chunk_compression_stats(to_regclass(t.view_name))
                        ) s ON to_regclass(t.view_name) IS NOT NULL
                        GROUP BY t.view_name
                    )
                    SELECT
                        c.view_name,
                        c.total_chunks,
                        c.compressed_chunks,
                        coalesce(hypertable_size(to_regclass(c.qualified_relation_name)), 0) AS size_bytes,
                        coalesce(rs.raw_size_bytes, 0) AS raw_size_bytes
                    FROM chunk_counts c
                    LEFT JOIN raw_size rs ON rs.view_name = c.view_name
                """),
                {"table_names": all_table_names},
            ).fetchall()

        stats_by_table: dict[str, dict[str, int]] = {
            row.view_name: {
                "total_chunks": row.total_chunks or 0,
                "compressed_chunks": row.compressed_chunks or 0,
                "size_bytes": row.size_bytes or 0,
                "raw_size_bytes": row.raw_size_bytes or 0,
            }
            for row in stat_rows
        }

        rows_out: list[dict[str, Any]] = []
        for group in groups:
            table_rows: list[dict[str, Any]] = []
            total_chunks = 0
            compressed_chunks = 0
            size_bytes = 0
            raw_size_bytes = 0
            for table_name in group["tables"]:
                stats: dict[str, int] = stats_by_table.get(
                    table_name,
                    {"total_chunks": 0, "compressed_chunks": 0, "size_bytes": 0, "raw_size_bytes": 0},
                )
                table_rows.append({
                    "table_name": table_name,
                    "granularity": granularity_by_table[table_name],
                    "total_chunks": stats["total_chunks"],
                    "compressed_chunks": stats["compressed_chunks"],
                    "size_bytes": stats["size_bytes"],
                    "raw_size_bytes": stats["raw_size_bytes"],
                })
                total_chunks += stats["total_chunks"]
                compressed_chunks += stats["compressed_chunks"]
                size_bytes += stats["size_bytes"]
                raw_size_bytes += stats["raw_size_bytes"]

            rows_out.append({
                "protocol_name": group["protocol_name"],
                "wide_table_name": group["wide_table_name"],
                "tables": table_rows,
                "total_chunks": total_chunks,
                "compressed_chunks": compressed_chunks,
                "size_bytes": size_bytes,
                "raw_size_bytes": raw_size_bytes,
            })

        return rows_out


    # === moved: pause_compression_job_for_table (5793-5824) ===
    def pause_compression_job_for_table(self, session: Session, table_name: str) -> list[int]:
        """
        Temporarily disables (scheduled => false) any compression policy
        job(s) TimescaleDB has configured specifically for table_name.
        Single canonical implementation used by both this class's own
        Rebuild Compression flow and BridgeAdminManager.delete_fields
        (via bridge.hypertable_mgr) around its DROP COLUMN -- previously
        each kept its own private copy of this same query. Returns the
        job_id(s) paused, so resume_compression_job_for_table can
        re-enable them afterward.
        """
        try:
            rows: Sequence[Row[Any]] = session.execute(
                text("""
                    SELECT job_id FROM timescaledb_information.jobs
                    WHERE hypertable_name = :table_name
                      AND proc_name ILIKE :search_term
                      AND scheduled = true
                """),
                {"table_name": table_name, "search_term": "%compress%"}
            ).fetchall()
            job_ids: list[int] = [r[0] for r in rows]
            for job_id in job_ids:
                session.execute(text("SELECT alter_job(:job_id, scheduled := false)"), {"job_id": job_id})
            if job_ids:
                self._log.info(f"pause_compression_job_for_table: paused job(s) {job_ids} for '{table_name}'")
        except Exception as e:
            self._log.warning(
                f"pause_compression_job_for_table: could not pause compression job for '{table_name}': {e}"
            )
            return []
        else:
            return job_ids


    # === moved: resume_compression_job_for_table (5825-5834) ===
    def resume_compression_job_for_table(self, session: Session, job_ids: list[int]) -> None:
        """Re-enables (scheduled => true) job_ids previously paused by pause_compression_job_for_table."""
        for job_id in job_ids:
            try:
                session.execute(text("SELECT alter_job(:job_id, scheduled => true)"), {"job_id": job_id})
            except Exception as e:
                self._log.warning(f"resume_compression_job_for_table: could not resume job {job_id}: {e}")
        if job_ids:
            self._log.info(f"resume_compression_job_for_table: resumed job(s) {job_ids}")


    # === moved: decompress_table_chunks_best_effort (5835-5911) ===
    def decompress_table_chunks_best_effort(self, session: Session, table_name: str) -> None:
        """
        Decompresses every currently-compressed chunk of table_name,
        best-effort. This is the one place that kind of decompress lives
        now -- it used to be a private copy
        (BridgeAdminManager._decompress_chunks_best_effort) kept next
        to the Delete Columns flow, but decompressing a table's chunks is
        exactly what this class's Rebuild Compression machinery already
        does as half of its decompress -> compress cycle, so
        BridgeAdminManager.delete_fields now calls this instead of
        keeping its own copy of the same logic. Support for dropping
        columns directly on compressed hypertable chunks is inconsistent
        across TimescaleDB versions, so delete_fields calls this
        proactively, ahead of its ALTER TABLE ... DROP COLUMN. Newly
        written data is recompressed on the normal compression policy
        schedule; running the full "Rebuild Compression" screen
        afterward is what re-compresses these particular chunks under
        the now-smaller column set (see rebuild_compression's docstring).

        Best-effort: a table with no compressed chunks (or not yet a
        hypertable) simply no-ops here rather than failing the caller's
        edit.

        Runs inside a SAVEPOINT (session.begin_nested()), not directly in
        the caller's transaction. Postgres aborts an entire transaction --
        not just the one failing statement -- the moment any statement in
        it errors, and that abort can only be cleared by a ROLLBACK (or
        ROLLBACK TO SAVEPOINT), never by simply catching the Python
        exception. Without the SAVEPOINT here, a failure in this
        best-effort step would silently poison the caller's transaction:
        the except block below would swallow the Python-level exception,
        but every statement after it -- including the ALTER TABLE DROP
        COLUMN this is meant to prepare for -- would then fail with
        "current transaction is aborted, commands ignored until end of
        transaction block", surfacing as a confusing failure far from its
        real cause. begin_nested() ensures only the SAVEPOINT is rolled
        back on failure, leaving the rest of the caller's transaction
        (including the DROP COLUMN that follows) usable.

        Deliberately takes and reuses the CALLER's session/transaction
        (unlike list_compression_groups/rebuild_compression above, which
        each open their own short-lived sessions) -- delete_fields needs
        this decompress step and its subsequent DROP COLUMN to be atomic
        with each other, inside the single advisory-locked transaction it
        already holds.

        Applies the same dynamic lock_timeout (_get_dynamic_settings_
        helper -> set_lock_timeout) every other lock-risky operation in
        this class applies before touching a hypertable/CAGG (see
        _create_rollup_helper, drop_protocol_rollup,
        _drop_all_continuous_aggregates) -- decompress_chunk() takes an
        exclusive lock same as those, and can be blocked by the same
        sources (the flush thread, an in-flight refresh). Without it this
        step would hang on that lock indefinitely instead of failing fast
        like everything else here does; it was inconsistent for this
        decompress to be the one exception.
        """
        r_settings: dict[str, Any] = self.get_dynamic_settings_helper()
        try:
            with session.begin_nested():
                self.set_lock_timeout(session, r_settings["lock_timeout"])
                session.execute(
                    text("""
                        SELECT decompress_chunk(c, true)
                        FROM show_chunks(:tname) AS c;
                    """),
                    {"tname": table_name}
                )
        except Exception as e:
            # the common case -- no compressed chunks yet
            # -- is entirely expected, but logged at warning so an
            # unexpected failure here is never silent again.
            self._log.warning(
                f"decompress_table_chunks_best_effort: could not decompress chunks for '{table_name}' "
                f"(continuing -- this step is best-effort): {e}"
            )


    # === moved: _get_hypertable_total_sizes (5912-5955) ===
    def _get_hypertable_total_sizes(self, table_names: list[str]) -> dict[str, int]:
        """
        Returns {table_name: hypertable_size_bytes} for the given table/
        view names -- same view-resolution technique as
        list_compression_groups (a rollup view's public name isn't what
        timescaledb_information.chunks/hypertable_size() index by; this
        resolves it via timescaledb_information.continuous_aggregates
        first), but scoped to just total on-disk size, no chunk counts.
        This is what rebuild_compression uses for its before/after size
        snapshot, so that a rebuild's reported size change is the SAME
        metric list_compression_groups' "Size" column already shows the
        admin, not some other narrower figure that happens to look
        similar. A name that doesn't resolve to an existing relation
        yields 0 rather than raising. Empty input short-circuits to `{}`
        without a round trip.
        """
        if not table_names:
            return {}
        with self.SessionFactory() as session:
            rows: Sequence[Row[Any]] = session.execute(
                text("""
                    WITH target_tables AS (
                        SELECT unnest(CAST(:table_names AS text[])) AS view_name
                    ),
                    resolved AS (
                        SELECT
                            t.view_name,
                            coalesce(
                                ca.materialization_hypertable_schema || '.' || ca.materialization_hypertable_name,
                                t.view_name
                            ) AS qualified_relation_name
                        FROM target_tables t
                        LEFT JOIN timescaledb_information.continuous_aggregates ca
                            ON ca.view_name = t.view_name
                    )
                    SELECT
                        r.view_name,
                        coalesce(hypertable_size(to_regclass(r.qualified_relation_name)), 0) AS size_bytes
                    FROM resolved r
                """),
                {"table_names": table_names},
            ).fetchall()
        return {row.view_name: row.size_bytes or 0 for row in rows}


    # === moved: rebuild_compression (5956-6291) ===
    def rebuild_compression(self, protocol_names: set[str] | None = None) -> Generator[dict[str, Any], None, None]:
        """
        Full decompress -> recompress pass across every already-compressed
        chunk of the selected group(s)' raw table and all four rollup
        views, for the admin's "Rebuild Compression" button.

        This is a GENERATOR, not a plain method -- see routers/timescale
        .py's PATCH /compression/rebuild, which streams each yielded
        event straight to the browser as one line of newline-delimited
        JSON per event, over the single request/response connection
        already open for the rebuild itself. No background job, no
        second polling endpoint: the request just stays open and the
        browser reads it incrementally (see triggerRebuildCompression in
        timescale_rebuild_compression.html). Trade-off worth knowing:
        if that connection drops mid-run (tab closed, laptop sleeps),
        the rebuild keeps going server-side same as always, but nothing
        is watching it anymore -- there's no job ID to reattach to.

        Yields:
            {"type": "progress", "protocol_name", "table_name",
             "bytes_processed", "bytes_total"} once per chunk (success or
             failure) -- bytes_total is fixed for the whole run, computed
             during the planning pass below from the pre-rebuild size of
             every table that actually has compressed chunks to touch
             (tables with nothing compressed contribute nothing to the
             denominator, so progress reaches 100% exactly when the last
             real chunk finishes, not before). bytes_processed is
             cumulative and WEIGHTED BY EACH TABLE'S BYTE SIZE, split
             evenly across that table's own chunks. This matters because
             chunk counts are wildly incomparable across a group's own
             members -- an hourly rollup view routinely has 4-5x the raw
             table's chunk count for the same wall-clock span, while the
             raw table's chunks are individually far heavier -- so a
             naive chunks-done/chunks-total fraction would race through
             a big rollup view and then appear to stall on the raw
             table, or the reverse, depending on iteration order. There's
             no cheaper way to get a single chunk's actual individual
             byte size without yet another query per chunk; splitting a
             table's already-known total evenly across its own chunks is
             a much better proxy than treating every chunk everywhere as
             equal, since within ONE table chunks are far more
             comparable to each other than they are across tables/views.
            {"type": "done", "result": {"groups": [...]}} exactly once,
             at the end -- the same per-group/per-table/per-chunk result
             shape this method used to simply return, unchanged.

        Unlike rebuild_all_rollups() / refresh_selected_rollups(), this
        never drops or recreates a view or a chunk -- it only rewrites
        each already-compressed chunk's on-disk storage format in place
        (decompress_chunk() followed by compress_chunk() against the
        hypertable's CURRENT compress_segmentby/compress_orderby/column
        set). No row data is touched or lost. This is what actually
        applies a compress_segmentby/compress_orderby change, or cleans
        up the physical layout after Delete Columns has dropped wide-
        table columns, to data that was compressed under the old
        settings -- editing hypertable_defaults or dropping a column only
        changes what NEW compression uses; it does nothing to chunks
        already on disk until something like this rewrites them.

        Args:
            protocol_names: Same group keys as rebuild_all_rollups() -- a
                wide-table protocol_name, plus "shared_narrow". None acts
                on every group. An empty set does nothing -- every group
                comes back in the result with skipped=True.

        Only touches chunks TimescaleDB already reports as compressed
        (is_compressed=true in timescaledb_information.chunks) -- an
        uncompressed chunk (typically the newest one, still inside its
        compress_after window) is left alone, so this never fights the
        compression policy schedule or compresses data early.

        Runs in two passes rather than one combined loop:
          1. PLANNING -- for every table in a selected group, look up
             which chunks are currently compressed (a read-only query,
             nothing mutated yet). This is what makes bytes_total known
             BEFORE the first progress event, instead of discovering it
             incrementally as tables happen to get visited.
          2. WORK -- decompress/recompress each planned chunk, in the
             same order the plan was built (group order, then each
             group's [raw, hourly, daily, weekly, monthly] order),
             yielding a progress event after every chunk.
        Each table's scheduled compression job is paused for the
        duration of that table's chunk loop in the work pass (mirrors
        BridgeAdminManager.delete_fields' pause-around-DDL pattern) so
        the background scheduler can't race a chunk this is already
        mid-rewrite on, and is always resumed afterward even if a chunk
        failed. Every chunk gets its own try/except: one chunk failing to
        decompress or recompress (e.g. a concurrent lock) does not abort
        the rest of that table, the rest of the group, or any other
        selected group.

        size_before_bytes/size_after_bytes on the final "done" result (at
        both the table and group level) are the TABLE's/GROUP's total
        on-disk size -- via _get_hypertable_total_sizes(), i.e. the exact
        same hypertable_size() metric list_compression_groups' "Size"
        column already shows -- snapshotted once for every table the
        planning pass found work for, right before the work pass starts,
        and again once for all of them right after the work pass
        finishes. Two round trips total (not per-table, not per-chunk),
        regardless of how many groups/tables/chunks are involved.

        This is deliberately NOT a sum of chunk_compression_stats() before/
        after figures per chunk (an earlier version of this method did
        that). That approach only measured the chunks actually rewritten,
        which is a different, narrower quantity than a table's total size
        (it excludes any never-compressed chunk sitting alongside them,
        and TOAST/index overhead) -- comparable to nothing else on this
        screen, and prone to reporting a "before" and "after" that don't
        visibly relate to the table's actual "Size" column at all. Taking
        the same total-size measurement before and after means: a rebuild
        that doesn't change compress_segmentby/compress_orderby/the
        column set correctly reports ~0% (nothing to shrink), and a
        rebuild that DOES change one of those reports a percentage that
        lines up with what the admin already sees in "Size".

        chunk_name values come back from timescaledb_information.chunks
        unqualified (e.g. "_hyper_3_42_chunk", no schema prefix) -- fine
        for the WHERE chunk_name comparison, but decompress_chunk()/
        compress_chunk() need a regclass that actually resolves, which
        means schema-qualifying it against _timescaledb_internal (where
        TimescaleDB always creates chunks) before passing it to either
        call.

        Applies the same dynamic work_mem/lock_timeout tier
        (get_dynamic_settings_helper) every other lock-risky operation
        in this class applies before touching a hypertable/CAGG (see
        _create_rollup_helper, drop_protocol_rollup,
        _refresh_single_rollup_helper) -- decompress_chunk()/
        compress_chunk() take the same kind of exclusive chunk lock as
        those and decompression is memory-heavy, so this was the one
        compression-adjacent operation in the class NOT getting that
        tuning. Computed once up front (cheap -- reads in-memory counts,
        no DB hit) and applied per chunk transaction below, rather than
        per table, since it doesn't change over the course of one run.
        """
        r_settings: dict[str, Any] = self.get_dynamic_settings_helper()
        all_groups: list[dict[str, Any]] = self._compression_group_tables(protocol_names=protocol_names)

        selected_table_names: list[str] = [
            table_name for group in all_groups if group["selected"] for table_name in group["tables"]
        ]
        old_total_sizes: dict[str, int] = self._get_hypertable_total_sizes(selected_table_names)

        # -------- Planning pass: find out what actually needs doing, and
        # its total byte weight, before touching anything. This is what
        # lets the very first progress event already know a real,
        # correct bytes_total instead of discovering it incrementally.
        plan: list[dict[str, Any]] = []  # [{protocol_name, table_name, chunk_names}, ...]

        for group in all_groups:
            if not group["selected"]:
                continue
            for table_name in group["tables"]:
                try:
                    with self.SessionFactory() as session:
                        with session.begin():
                            self.set_lock_timeout(session, r_settings["lock_timeout"])
                            chunk_rows: Sequence[Row[Any]] = session.execute(
                                text("""
                                    SELECT chunk_name
                                    FROM timescaledb_information.chunks
                                    WHERE hypertable_name = :tname AND is_compressed = true
                                """),
                                {"tname": table_name},
                            ).fetchall()
                except SQLAlchemyError as e:
                    # Table/view doesn't exist yet (rollups not set up for
                    # this protocol), or has no compressed chunks at all --
                    # nothing to rebuild here, but don't fail the rest of
                    # the group over it.
                    self._log.debug(f"rebuild_compression: skipping '{table_name}': {e}")
                    continue

                if not chunk_rows:
                    continue

                plan.append({
                    "protocol_name": group["protocol_name"],
                    "table_name": table_name,
                    "chunk_names": [row.chunk_name for row in chunk_rows],
                })

        touched_table_names: list[str] = [item["table_name"] for item in plan]
        bytes_total: int = sum(old_total_sizes.get(t, 0) for t in touched_table_names)
        bytes_processed: float = 0.0

        # -------- Per-group accumulators for the eventual "done" event,
        # seeded for every group up front so a selected group that ends
        # up with nothing to do (every table had zero compressed chunks)
        # still reports cleanly rather than being absent.
        group_acc: dict[str, dict[str, Any]] = {
            group["protocol_name"]: {"chunk_results": [], "table_results": [], "chunks_ok": 0, "chunks_failed": 0}
            for group in all_groups
            if group["selected"]
        }

        # -------- Work pass: actually decompress/recompress, in plan
        # order, yielding one progress event per chunk.
        for item in plan:
            protocol_name: str = item["protocol_name"]
            table_name: str = item["table_name"]
            chunk_names: list[str] = item["chunk_names"]
            acc: dict[str, Any] = group_acc[protocol_name]

            table_weight: int = old_total_sizes.get(table_name, 0)
            per_chunk_weight: float = (table_weight / len(chunk_names)) if chunk_names else 0.0

            table_chunks_ok = 0
            table_chunks_failed = 0

            paused_job_ids: list[int] = []
            try:
                with self.SessionFactory() as session:
                    with session.begin():
                        paused_job_ids = self.pause_compression_job_for_table(session, table_name)

                for chunk_name in chunk_names:
                    # Ensure the chunk name is fully qualified with its internal schema
                    if not chunk_name.startswith("_timescaledb_internal."):
                        qualified_chunk: str = f"_timescaledb_internal.{chunk_name}"
                    else:
                        qualified_chunk = chunk_name

                    try:
                        with self.SessionFactory() as session:
                            with session.begin():
                                self.set_lock_timeout(session, r_settings["lock_timeout"])
                                session.execute(
                                    text(f"SET LOCAL work_mem = '{r_settings['work_mem']}';")
                                )
                                session.execute(text("SELECT decompress_chunk(:c, true)"), {"c": qualified_chunk})
                                session.execute(text("SELECT compress_chunk(:c, true)"), {"c": qualified_chunk})

                        acc["chunks_ok"] += 1
                        table_chunks_ok += 1
                        acc["chunk_results"].append({
                            "table_name": table_name, "chunk_name": chunk_name,
                            "ok": True, "error": None,
                        })
                    except Exception as e:
                        acc["chunks_failed"] += 1
                        table_chunks_failed += 1
                        self._log.error(
                            f"rebuild_compression: chunk '{chunk_name}' on '{table_name}' failed: {e}"
                        )
                        acc["chunk_results"].append({
                            "table_name": table_name, "chunk_name": chunk_name,
                            "ok": False, "error": str(e),
                        })

                    bytes_processed += per_chunk_weight
                    yield {
                        "type": "progress",
                        "protocol_name": protocol_name,
                        "table_name": table_name,
                        "bytes_processed": int(min(bytes_processed, bytes_total)),
                        "bytes_total": bytes_total,
                    }
            finally:
                if paused_job_ids:
                    try:
                        with self.SessionFactory() as session:
                            with session.begin():
                                self.resume_compression_job_for_table(session, paused_job_ids)
                    except Exception as e:
                        # Never let a resume failure mask chunk results
                        # already recorded above -- logged so a human
                        # notices and re-enables the job manually.
                        self._log.warning(
                            f"rebuild_compression: could not resume compression job(s) "
                            f"{paused_job_ids} for '{table_name}': {e}"
                        )

            acc["table_results"].append({
                "table_name": table_name,
                "ok": table_chunks_failed == 0,
                "chunks_ok": table_chunks_ok,
                "chunks_failed": table_chunks_failed,
                "size_before_bytes": old_total_sizes.get(table_name, 0),
                # size_after_bytes filled in below, once every table has
                # finished -- see the batched "after" snapshot below.
            })

        # Single batched "after" snapshot covering every table the
        # planning pass found work for -- one round trip for everything,
        # same reasoning as old_total_sizes above.
        new_total_sizes: dict[str, int] = self._get_hypertable_total_sizes(touched_table_names)

        group_results: list[dict[str, Any]] = []
        for group in all_groups:
            protocol_name = group["protocol_name"]
            if not group["selected"]:
                group_results.append({
                    "protocol_name": protocol_name,
                    "skipped": True,
                    "ok": True,
                    "chunks_attempted": 0,
                    "chunks_ok": 0,
                    "chunks_failed": 0,
                    "size_before_bytes": 0,
                    "size_after_bytes": 0,
                    "tables": [],
                    "chunks": [],
                })
                continue

            acc = group_acc[protocol_name]
            group_before = 0
            group_after = 0
            for table_entry in acc["table_results"]:
                after: int = new_total_sizes.get(table_entry["table_name"], 0)
                table_entry["size_after_bytes"] = after
                group_before += table_entry["size_before_bytes"]
                group_after += after

            group_results.append({
                "protocol_name": protocol_name,
                "skipped": False,
                "ok": acc["chunks_failed"] == 0,
                "chunks_attempted": acc["chunks_ok"] + acc["chunks_failed"],
                "chunks_ok": acc["chunks_ok"],
                "chunks_failed": acc["chunks_failed"],
                "tables": acc["table_results"],
                "chunks": acc["chunk_results"],
                "size_before_bytes": group_before,
                "size_after_bytes": group_after,
            })

        ok_count: int = sum(1 for g in group_results if g["ok"] and not g["skipped"])
        attempted_count: int = sum(1 for g in group_results if not g["skipped"])
        self._log.info(
            "rebuild_compression: %d/%d selected group(s) fully ok.", ok_count, attempted_count,
        )

        yield {"type": "done", "result": {"groups": group_results}}


    # === moved: get_dynamic_raw_table_overview (6292-6355) ===
    def get_dynamic_raw_table_overview(self) -> list[dict[str, Any]]:
        """
        Read-only, per-raw-table snapshot of the values get_dynamic_raw_
        table_settings_helper would currently resolve — the shared narrow
        table plus every protocol with a wide table — for the Compression
        & Retention Status panel. Unlike the static raw_narrow_*/raw_wide_*
        settings (which apply the same value to every wide table
        regardless of its own load), this shows each table's OWN band,
        since two different protocols' wide tables can easily land in
        different bands depending on their own metric count and scrape
        interval.

        Purely observational: computing this never changes any policy or
        creates/drops anything, unlike calling get_dynamic_raw_table_
        settings_helper from setup_narrow_rollup/add_wide_rollup (which
        this reuses read-only).
        """
        rows: list[dict[str, Any]] = []

        narrow_chunk, narrow_compress, narrow_band = self.get_dynamic_raw_table_settings_helper(
            target_protocol_name=None,
            static_chunk_interval=self.raw_narrow_chunk_time_interval,
            static_compress_after=self.raw_narrow_compress_after_interval,
        )
        rows.append({
            "table_name": "device_metrics_narrow",
            "protocol_name": "shared_narrow",
            "load_score": self._compute_metric_writes_per_day_helper(None),
            "band_name": narrow_band,
            "chunk_time_interval": narrow_chunk,
            "compress_after_interval": narrow_compress,
        })

        try:
            with self.SessionFactory() as session:
                protocol_rows: Sequence[Row[Any]] = session.execute(
                    text("""
                        SELECT protocol_name, wide_table_name
                        FROM protocol_registry
                        WHERE wide_table_name IS NOT NULL
                        ORDER BY protocol_name
                    """)
                ).fetchall()
        except SQLAlchemyError as e:
            self._log.error(f"get_dynamic_raw_table_overview: protocol_registry query failed: {e}")
            protocol_rows = []

        for protocol_name, wide_table_name in protocol_rows:
            chunk, compress, band = self.get_dynamic_raw_table_settings_helper(
                target_protocol_name=protocol_name,
                static_chunk_interval=self.raw_wide_chunk_time_interval,
                static_compress_after=self.raw_wide_compress_after_interval,
            )
            rows.append({
                "table_name": wide_table_name,
                "protocol_name": protocol_name,
                "load_score": self._compute_metric_writes_per_day_helper(protocol_name),
                "band_name": band,
                "chunk_time_interval": chunk,
                "compress_after_interval": compress,
            })

        return rows


    # === moved: set_lock_timeout (6500-6522) ===
    def set_lock_timeout(self, session: Session, timeout: str) -> None:
        """
        Sets a transaction-scoped lock_timeout via SET LOCAL.

        SET LOCAL only takes effect (and only avoids Postgres's "SET LOCAL
        can only be used in transaction blocks" warning) when an explicit
        transaction is already open on the connection. Session normally
        autobegins one lazily on first use, but several callers here issue
        SET LOCAL right after a mid-function session.commit()/rollback()
        (e.g. drop_protocol_rollup's compression-disable retry, or the
        per-view commit loop in _drop_all_continuous_aggregates) -- exactly
        the gap where that lazy autobegin can lose the race and leave the
        statement running outside any transaction block.

        session.begin() is a no-op if a transaction is already open (guarded
        by in_transaction() so it never raises "already begun"), and
        guarantees one is in place -- with BEGIN actually sent -- before
        SET LOCAL runs.
        """
        if not session.in_transaction():
            session.begin()
        session.execute(text(f"SET LOCAL lock_timeout = '{timeout}';"))


    # === moved: get_dynamic_settings_helper (6523-6559) ===
    def get_dynamic_settings_helper(self) -> dict[str, Any]:
        """
        Returns performance tier settings based on the actual workload drivers
        in a multi-protocol environment.

        Two axes determine lock pressure:
        1. Protocol count — more protocols = more CAGG views refreshed per cycle
            = longer total lock hold time across the refresh window.
        2. Wide table presence — wide rollups generate one stats_agg() expression
            per metric column, which is significantly more CPU and memory intensive
            than a single stats_agg(metric_value) on the narrow table.

        Tier selection:
        tier_low    — many protocols (>4) or any wide table with many columns (>100):
                        conservative work_mem and short lock_timeout to yield quickly.
        tier_medium — few protocols with wide tables, or many protocols narrow-only:
                        balanced settings.
        tier_high   — single or few protocols, narrow-only or small wide tables:
                        can afford longer lock windows and higher memory.
        """
        protocol_count: int = len(self.rollup_mgr.known_rollup_views) if self.rollup_mgr is not None else 0
        # Proxy for wide table complexity: any wide views registered means
        # at least one protocol has per-column rollup expressions in play.
        has_wide_views: bool = any("wide__" in v for v in self.rollup_mgr.known_rollup_views) if self.rollup_mgr is not None else False

        # Max column count across all wide protocols from protocol_registry.
        # Cached after registration, so no DB hit on the hot path.
        max_wide_columns: int = self._max_wide_column_count_helper()

        if protocol_count > 4 or max_wide_columns > 100:
            return self.performance_tiers["tier_low"]
        elif has_wide_views or protocol_count > 2:
            return self.performance_tiers["tier_medium"]
        else:
            return self.performance_tiers["tier_high"]



    # === moved: _max_wide_column_count_helper (6560-6612) ===
    def _max_wide_column_count_helper(self) -> int:
        """
        Returns the highest metric_count across all registered wide-table protocols.
        Used by _get_dynamic_settings to tune lock/memory settings.
        Reads from the in-memory protocol_wide_column_counts dict populated
        during add_wide_rollup, so there is no DB hit on the hot path.
        """
        if not self.protocol_wide_column_counts:
            return 0
        return max(self.protocol_wide_column_counts.values(), default=0)


    # -------------------------
    #  Dynamic raw-table chunk/compression sizing — a DIFFERENT concern
    #  from get_dynamic_settings_helper above (which tunes rollup-refresh
    #  lock/memory behavior). This sizes chunk_time_interval and compress_
    #  after for the two raw tables based on modeled real-world write load,
    #  per TimescaleDB's own sizing guidance (chunks should hold a roughly
    #  comparable, bounded amount of data; compress_after should scale with
    #  a table's own chunk_time_interval, not be picked independently).
    # -------------------------

    # Band edges are in "metric-writes/day" -- (metric_count contributed by
    # a protocol) x (writes/day from its transport(s)' read_interval),
    # summed across whichever protocols are relevant (all of them, for
    # narrow; just the one protocol sharing a wide table, for wide). This
    # single unit works for BOTH raw tables despite their very different
    # row-count behavior: narrow's actual row rate literally equals
    # metric_count x writes/day (one row per metric per scrape), while a
    # wide table's row COUNT stays flat at 1 row/scrape regardless of
    # column count -- but its BYTE width per row scales with metric_count,
    # so metric_count x writes/day approximates its data-volume rate too.
    #
    # Boundaries were translated from a 50/100/160/350-metric tiering
    # example at a 15-second scrape interval (50 x 5760 = 288,000, etc.)
    # into this interval-independent unit, so the same bands apply
    # correctly regardless of a deployment's actual scrape interval(s).
    # chunk_time_interval / compress_after pairs follow TimescaleDB's own
    # ~2-3x multiple guidance at each tier.
    #
    # This table is intentionally a plain, easily-edited list rather than
    # a settings-UI-exposed structure -- tune the values here directly if
    # real chunk sizes (see the Storage Overview panel) suggest a boundary
    # or preset needs adjusting for your hardware.
    _DYNAMIC_CHUNK_BANDS: list[tuple[float | None, str, str, str]] = [
        # (upper bound metric-writes/day, chunk_time_interval, compress_after_interval, band_name)
        (288_000.0,   "7 days",   "14 days",   "Group 1 (light)"),
        (576_000.0,   "3 days",   "7 days",    "Group 2"),
        (1_152_000.0, "1 day",    "3 days",    "Group 3"),
        (2_016_000.0, "12 hours", "36 hours",  "Group 4"),
        (None,        "6 hours",  "18 hours",  "Group 5 (heavy)"),
    ]


    # === moved: _classify_chunk_band_helper (6613-6626) ===
    def _classify_chunk_band_helper(self, metric_writes_per_day: float) -> tuple[str, str, str]:
        """
        Maps a metric-writes/day load score to a (chunk_time_interval,
        compress_after_interval, band_name) preset from _DYNAMIC_CHUNK_
        BANDS. Bands are checked in ascending order; the first whose upper
        bound the score doesn't exceed wins, and the final (None-bounded)
        entry always matches as the top band.
        """
        for upper_bound, chunk_interval, compress_after, band_name in self._DYNAMIC_CHUNK_BANDS:
            if upper_bound is None or metric_writes_per_day <= upper_bound:
                return chunk_interval, compress_after, band_name
        # Unreachable -- the table's last entry has upper_bound=None.
        return self._DYNAMIC_CHUNK_BANDS[-1][1], self._DYNAMIC_CHUNK_BANDS[-1][2], self._DYNAMIC_CHUNK_BANDS[-1][3]


    # === moved: _compute_metric_writes_per_day_helper (6627-6685) ===
    def _compute_metric_writes_per_day_helper(self, target_protocol_name: str | None) -> float:
        """
        Computes the metric-writes/day load score for one raw table.

        target_protocol_name=None -> narrow's score: sum across EVERY
            transport currently tracked in transport_read_intervals (every
            protocol's metrics land in the one shared narrow table).
        target_protocol_name=<name> -> that one wide table's score: only
            transports whose protocol matches this name (more than one
            transport instance of the same protocol -- e.g. two battery
            packs sharing one wide table -- correctly sums their combined
            contribution here, since this operates per transport_name).

        Computed PER TRANSPORT (per physical device), not per protocol.
        protocol_registry.metric_count is a schema-wide figure (the wide
        table's column count, sized for the union of every device's
        metric set) and wrongly assumes every device of a protocol
        scrapes the same metric set. In practice, different devices of
        the same protocol can have very different variable_mask
        configurations -- one battery might get the full register list,
        another only a curated few. Each device's own actual figure --
        captured once at startup in device_info.metric_count, from the
        real write payload size (see _get_or_create_device) -- is used
        instead, and summed per transport rather than assumed uniform.

        Returns 0.0 if there's no live tracking data yet for the relevant
        protocol(s) (e.g. right after a fresh connect, before any scraper
        has written through this bridge) -- callers should treat 0.0 as
        "not enough data, fall back to static config" rather than as a
        genuinely idle table.
        """
        relevant_transports: dict[str, float] = {
            transport_name: read_interval
            for transport_name, (protocol_name, read_interval) in self.transport_read_intervals.items()
            if (target_protocol_name is None or protocol_name == target_protocol_name) and read_interval > 0
        }
        if not relevant_transports:
            return 0.0

        try:
            with self.SessionFactory() as session:
                rows: Sequence[Row[Any]] = session.execute(
                    text("SELECT transport, metric_count FROM device_info WHERE transport = ANY(:transports)"),
                    {"transports": list(relevant_transports.keys())},
                ).fetchall()
        except SQLAlchemyError as e:
            self._log.warning(f"_compute_metric_writes_per_day_helper: device_info lookup failed: {e}")
            return 0.0

        metric_counts_by_transport: dict[str, int] = {row.transport: (row.metric_count or 0) for row in rows}

        total: float = 0.0
        for transport_name, read_interval in relevant_transports.items():
            metric_count: int = metric_counts_by_transport.get(transport_name, 0)
            if metric_count > 0:
                total += metric_count * (86400.0 / read_interval)

        return total


    # === moved: get_dynamic_raw_table_settings_helper (6686-6729) ===
    def get_dynamic_raw_table_settings_helper(
        self, target_protocol_name: str | None, static_chunk_interval: str, static_compress_after: str
    ) -> tuple[str, str, str]:
        """
        Resolves the (chunk_time_interval, compress_after_interval, band_
        name) to actually use for one raw table -- narrow (target_
        protocol_name=None) or one wide table (target_protocol_name=that
        protocol).

        Falls back to (static_chunk_interval, static_compress_after,
        "Static (manual)") when enable_dynamic_chunk_sizing is False, or
        when there isn't yet enough live data to compute a load score
        (returns 0.0 -- see _compute_metric_writes_per_day_helper). This
        means a freshly connected bridge behaves exactly as it did before
        dynamic sizing existed until real scrape data starts flowing
        through it, rather than defaulting to the lightest or heaviest
        band by guesswork.
        """
        if not self.enable_dynamic_chunk_sizing:
            return static_chunk_interval, static_compress_after, "Static (manual)"

        load_score: float = self._compute_metric_writes_per_day_helper(target_protocol_name)
        if load_score <= 0.0:
            return static_chunk_interval, static_compress_after, "Static (no live data yet)"

        chunk_interval, compress_after, band_name = self._classify_chunk_band_helper(load_score)
        return chunk_interval, compress_after, band_name

    # -------------------------
    #  Dynamic-sizing retune -- see the _retune_reported block in __init__
    #  for the full rationale. Four pieces:
    #    flag_pending_retune       -- called from setup_narrow_rollup /
    #                                  add_wide_rollup when they land on the
    #                                  static fallback, to start tracking.
    #    note_device_metric_count_known -- called by timescaledb.
    #                                  _get_or_create_device the first time
    #                                  a transport's real metric_count is
    #                                  known; checks it off and fires the
    #                                  retune once its key is fully covered.
    #    _start_retune_timer_if_needed / _fire_retune -- the bounded
    #                                  fallback and the actual (threaded,
    #                                  off the caller's thread) re-run.
    # -------------------------


    # === moved: flag_pending_retune (6730-6744) ===
    def flag_pending_retune(self, key: str | None) -> None:
        """
        Marks `key` (a protocol_name, or None for narrow) as needing a
        one-shot dynamic-sizing retune once enough live data exists.
        No-op if this key was already retuned this process lifetime, or is
        already pending -- setup_narrow_rollup/add_wide_rollup can be
        called more than once for the same key (reconnect, force rebuild)
        and this must not reset progress each time.
        """
        with self.retune_lock:
            if key in self._retuned or key in self._retune_reported:
                return
            self._retune_reported[key] = set()
            self._log.debug(f"Dynamic sizing retune armed for {key!r} — waiting on live data.")


    # === moved: note_device_metric_count_known (6745-6791) ===
    def note_device_metric_count_known(self, transport_name: str, protocol_name: str) -> None:
        """
        Called once per transport, the first time its real
        device_info.metric_count becomes known (see timescaledb.
        _get_or_create_device) -- i.e. the first successful write for that
        transport this session.

        Checks transport_name off against every key currently pending a
        retune that it's relevant to: its own protocol (wide), and narrow
        (key None, since narrow's load score sums every protocol). A
        transport irrelevant to a given key (already retuned, or never
        flagged pending) is a cheap no-op for that key.

        Fires the retune for a key once every transport
        transport_read_intervals currently knows about for that key has
        reported -- computed fresh here rather than snapshotted when the
        key was flagged, since add_wide_rollup for a protocol can run (and
        fall back to static) before every transport sharing that protocol
        has even called record_transport_interval yet, e.g. protocol
        registration is driven by whichever of several same-protocol
        transports happens to initialize first.
        """
        do_fire: list[str | None] = []
        with self.retune_lock:
            for key in (None, protocol_name):
                if key not in self._retune_reported or key in self._retuned:
                    continue
                reported: set[str] = self._retune_reported[key]
                first_report_for_key: bool = not reported
                reported.add(transport_name)

                if first_report_for_key:
                    self._start_retune_timer_if_needed(key)

                expected: set[str] = {
                    t_name for t_name, (p_name, _interval) in self.transport_read_intervals.items()
                    if key is None or p_name == key
                }
                if expected and expected <= reported:
                    do_fire.append(key)

        # Fired outside the lock -- _fire_retune only hands off to a
        # background thread, but keeping that handoff out of the lock
        # avoids holding it across anything unexpected.
        for key in do_fire:
            self._fire_retune(key)


    # === moved: _start_retune_timer_if_needed (6792-6808) ===
    def _start_retune_timer_if_needed(self, key: str | None) -> None:
        """
        Arms the bounded fallback timer for `key`, once, the first time any
        transport relevant to it reports. Must be called with
        self.retune_lock held. If not every expected transport ever
        reports (an offline/misconfigured device sharing a wide table with
        others that ARE reporting), this fires the retune anyway after
        retune_timeout_seconds using whatever device_info rows exist by
        then, rather than leaving the table on static settings forever.
        """
        if key in self.retune_timer:
            return
        timer = threading.Timer(self.retune_timeout_seconds, self._fire_retune, args=(key,))
        timer.daemon = True
        self.retune_timer[key] = timer
        timer.start()


    # === moved: _fire_retune (6809-6838) ===
    def _fire_retune(self, key: str | None) -> None:
        """
        Marks `key` as retuned (idempotent -- safe to call from both the
        "every expected transport reported" path and the timeout path,
        whichever wins the race) and hands the actual re-run off to a
        short-lived background thread. Must never run the DB work
        synchronously on the caller's thread: the "every transport
        reported" path is called from note_device_metric_count_known,
        which is called from timescaledb._get_or_create_device on a live
        scraper/write thread, and add_wide_rollup / setup_narrow_rollup do
        real DDL (ALTER TABLE, compression policy changes, CAGG view
        checks) that has no business blocking a write.
        """
        with self.retune_lock:
            if key in self._retuned:
                return
            self._retuned.add(key)
            self._retune_reported.pop(key, None)
            timer: threading.Timer | None = self.retune_timer.pop(key, None)

        if timer is not None:
            timer.cancel()

        threading.Thread(
            target=self._do_retune_work,
            args=(key,),
            daemon=True,
            name=f"DynamicSizingRetune-{key or 'narrow'}",
        ).start()


    # === moved: _do_retune_work (6839-6883) ===
    def _do_retune_work(self, key: str | None) -> None:
        """
        Actual re-run, on its own thread. Re-invoking setup_narrow_rollup /
        add_wide_rollup with force=False is deliberate and sufficient here
        -- it unconditionally re-applies chunk_time_interval (see
        ensure_hypertables' docstring) and the compression policy (see
        ensure_compression_policy's docstring) even when nothing changed,
        and only re-materializes CAGGs if their bucket intervals actually
        differ from what's already there (_ensure_cagg_views_for_protocol).
        force=True is for the admin's explicit "Force Rebuild" action, not
        this — there's no reason to purge and re-materialize views whose
        bucket intervals haven't changed just because we're retuning the
        raw table's chunk/compression sizing.
        """
        try:
            if key is None:
                self._log.info(
                    "Dynamic sizing retune: live data now available for every known "
                    "transport — re-tuning the narrow raw table."
                )
                if self.rollup_mgr is not None:
                    self.rollup_mgr.setup_narrow_rollup()
                return

            wide_table_name: str | None = self._lookup_wide_table_name_helper(key)
            if wide_table_name is None:
                self._log.debug(
                    f"Dynamic sizing retune: '{key}' has no wide table (narrow-only "
                    f"protocol) — nothing to retune."
                )
                return

            self._log.info(
                f"Dynamic sizing retune: live data now available from every known "
                f"transport of '{key}' — re-tuning '{wide_table_name}'."
            )
            if self.rollup_mgr is not None:
                self.rollup_mgr.add_wide_rollup(protocol_name=key, wide_table_name=wide_table_name)

        except Exception as e:
            # Best-effort: a failed retune leaves the table on its current
            # (static) settings rather than failing anything user-facing.
            # _retuned already has `key` at this point, so this is a
            # one-time miss, not a retry loop — logged at error since a
            # human should notice and can trigger a manual Force Rebuild.
            self._log.error(f"Dynamic sizing retune failed for {key!r}: {e}")


    # === moved: _lookup_wide_table_name_helper (6884-6964) ===
    def _lookup_wide_table_name_helper(self, protocol_name: str) -> str | None:
        """Looks up protocol_registry.wide_table_name for one protocol."""
        try:
            with self.SessionFactory() as session:
                return session.execute(
                    text("SELECT wide_table_name FROM protocol_registry WHERE protocol_name = :p"),
                    {"p": protocol_name}
                ).scalar()
        except SQLAlchemyError as e:
            self._log.warning(f"_lookup_wide_table_name_helper failed for '{protocol_name}': {e}")
            return None

    # Only understands the small set of unit words this module's own
    # interval settings ever use -- not a general PostgreSQL INTERVAL
    # parser. "month" approximated as 30 days, consistent with
    # _GRANULARITY_BUCKETS_PER_DAY's monthly bucket constant above.
    _INTERVAL_UNIT_HOURS: dict[str, float] = {
        "hour": 1.0, "hours": 1.0,
        "day": 24.0, "days": 24.0,
        "week": 168.0, "weeks": 168.0,
        "month": 720.0, "months": 720.0,
    }


    # moved: parse_interval_to_hours_helper
    def parse_interval_to_hours_helper(self, interval_str: str) -> float:
        """
        Parses a simple "<number> <unit>" interval string (e.g. "1 day",
        "2 weeks", "4 months") into hours, using _INTERVAL_UNIT_HOURS.
        Returns 0.0 for anything that doesn't match that shape -- callers
        treat 0.0 as "unparseable, fall back to the static value" rather
        than guessing.
        """
        parts: list[str] = interval_str.strip().split()
        if len(parts) != 2:
            return 0.0
        try:
            value: float = float(parts[0])
        except ValueError:
            return 0.0
        hours_per_unit: float | None = self._INTERVAL_UNIT_HOURS.get(parts[1].lower())
        if hours_per_unit is None:
            return 0.0
        return value * hours_per_unit


    # moved: format_hours_to_interval_helper
    def format_hours_to_interval_helper(self, hours: float) -> str:
        """
        Formats an hours value back into a clean "<int> <unit>" interval
        string, picking the coarsest unit that keeps the value >= 1.
        Floors at 1 hour so a very aggressive scale-down never produces a
        zero or negative interval.
        """
        hours = max(1.0, hours)
        if hours >= 720:
            return f"{max(1, round(hours / 720))} months"
        if hours >= 168:
            return f"{max(1, round(hours / 168))} weeks"
        if hours >= 24:
            return f"{max(1, round(hours / 24))} days"
        return f"{round(hours)} hours"




class RollupManager:
    """ summary logic
        The RollupManger class creates the structures that enable TimescaleDB rollup views.
        After all structures are built:
        1 RollupManager wakes up.
        2 RollupManager tells BacklogManager to put everything into the _flush_queue if backlog data exists.
            The _flush_queue is the threaded queue object that accepts MPG data obtained from the source transport.
        3 RollupManager calls _flush_queue.join() (it pauses here).
        4 _flush_worker finishes writing everything to the Hypertable and calls task_done() for each.
        5 RollupManager resumes and calls refresh_continuous_aggregate.

        Backlog Safety: The _refresh_rollup_loop wraps replay_to_queue() in the _backlog_lock and waits for completion via .join().
        Attribute Persistence: All granular intervals (e.g., hourly_compress_after_interval) are mapped from the rollup_policy in __init__.
        Live State: tsdb_connected and current_metric_count are implemented as @property to track the timescaledb class state in real-time.
        SQL Execution: The SET work_mem uses the single-quote fix to prevent f-string placeholder errors.
    """

    # This dataclass is used to describe the SQL expressions needed to create rollup continuous aggregates for a given metric.
    @dataclass(frozen=True)
    class RollupMetricDescriptor:
        """
        Describe one logical metric stream for generic rollup SQL generation.

        Narrow rollups use a single descriptor keyed by `metric_name`.
        Wide rollups use one descriptor per protocol-specific metric column.
        """

        group_key_sql: str | None
        raw_value_sql: str
        rolled_min_sql: str
        rolled_max_sql: str
        rolled_summary_sql: str
        min_alias: str
        max_alias: str
        summary_alias: str

    def __init__(
        self,
        rollup_policy: dict[str, Any],
        my_session_factory: sessionmaker[Session],
        my_engine: Engine,
        migration_in_progress: threading.Event,
        log: logging.Logger,
        send_message:  SendMessageProtocol,
        backlog_lock: threading.RLock,
        flush_queue: queue.Queue[dict[str, Any] | None],
        backlog: BacklogManager,
        reconnect_lock: threading.RLock,
        hypertable_mgr: "HyperTableManager",
        ) -> None:

        self.rollup_policy: dict[str, Any] = rollup_policy
        self.SessionFactory: sessionmaker[Session] = my_session_factory
        self.engine: Engine = my_engine
        self.migration_in_progress: threading.Event = migration_in_progress
        self._log: logging.Logger = log
        self.send_message:  SendMessageProtocol = send_message
        self._backlog_lock: threading.RLock = backlog_lock
        self._flush_queue: queue.Queue[dict[str, Any] | None]  = flush_queue
        self.backlog: 'BacklogManager'  = backlog
        self._reconnect_lock: threading.RLock = reconnect_lock

        # Hypertable-level mechanics (create/compress/retain the raw
        # source tables, dynamic chunk sizing, pause/resume compression
        # jobs, background-job/resource-tier plumbing) live on
        # HyperTableManager, constructed by the bridge before this class
        # and passed in here -- see HyperTableManager's class docstring
        # for why the split follows TimescaleDB's own hypertable/
        # continuous-aggregate distinction rather than an arbitrary one.
        self.hypertable_mgr: "HyperTableManager" = hypertable_mgr

        self._refresh_rollup_thread = threading.Thread(target=self._refresh_rollup_loop_helper, daemon=True, name="RollupAutoRefreshThread")
        self._stop_refresh_rollup_event: threading.Event = getattr(self, "_stop_refresh_rollup_event", threading.Event())

        """
        Rollup and Compression Timing Defaults comments only:
        Rollup Type
            refresh rollup    start_offset   compress_after   reason
            1 Hour	          3 hours	     3 days	          Allows a 3-hour window for late data before locking it via compression.
            1 Day	          3 days	     2 weeks	      Ensures daily rollups are finalized before compressing.
            1 Week	          3 weeks	     2 months	      Larger window helps capture any delayed source data updates.
            1 Month	          3 months	     6 months	      Maximum safety for long-term historical accuracy.
        """

        # rollup defaults, continuous aggregate bucket sizes.
        self.rollup_defaults: dict[str, str] = {
            "hourly_rollup_bucket": "1 hour",
            "hourly_rollup_start": "3 hours",
            "daily_rollup_bucket": "1 day",
            "daily_rollup_start": "3 days",
            "weekly_rollup_bucket": "1 week",
            "weekly_rollup_start": "3 weeks",
            "monthly_rollup_bucket": "1 month",
            "monthly_rollup_start": "3 months",
            "anchor_start_time_utc": "2000-01-01 00:00:00+00",  # default anchor time for rollup alignment
        }

        # Rollup /Hypertable Settings extracted from rollup_policy  (user can override defaults via rollup_policy)
        self.current_metric_count: int = self.rollup_policy.get("current_metric_count", 0)
        self.machine_timezone: str = self.rollup_policy.get("machine_timezone", "UTC")
        self.auto_refresh_interval: int = self.rollup_policy.get("auto_refresh_interval",21600)
        self.enable_auto_refresh:bool = self.rollup_policy.get("enable_auto_refresh", True)
        self.enable_rollups = bool(self.rollup_policy.get("enable_rollups", True))
        self.migrate_data = bool(self.rollup_policy.get("migrate_data",True))

        # Rollup-view (CAGG) granularity settings -- each of the four
        # hierarchical views' own chunk_time_interval / compress_after.
        # Read out of HyperTableManager.hypertable_defaults (the one
        # canonical copy of this config dict -- see that class's
        # __init__) rather than duplicating it here. NOT the same thing
        # as HyperTableManager's raw_narrow_*/raw_wide_* settings, which
        # govern the two RAW hypertables instead -- see setup_rollup's
        # docstring for the history of that distinction.
        _htd = self.hypertable_mgr.hypertable_defaults
        self.hourly_chunk_time_interval: str = _htd["hourly_chunk_time_interval"]
        self.daily_chunk_time_interval: str = _htd["daily_chunk_time_interval"]
        self.weekly_chunk_time_interval: str = _htd["weekly_chunk_time_interval"]
        self.monthly_chunk_time_interval: str = _htd["monthly_chunk_time_interval"]

        self.hourly_compress_after_interval: str = _htd["hourly_compress_after_interval"]
        self.daily_compress_after_interval: str = _htd["daily_compress_after_interval"]
        self.weekly_compress_after_interval: str = _htd["weekly_compress_after_interval"]
        self.monthly_compress_after_interval: str = _htd["monthly_compress_after_interval"]

        # Rollup view settings
        self.anchor_start_time_utc: str = self.rollup_defaults["anchor_start_time_utc"]

        self.hourly_rollup_bucket: str = self.rollup_defaults["hourly_rollup_bucket"]
        self.daily_rollup_bucket: str = self.rollup_defaults["daily_rollup_bucket"]
        self.weekly_rollup_bucket: str = self.rollup_defaults["weekly_rollup_bucket"]
        self.monthly_rollup_bucket: str = self.rollup_defaults["monthly_rollup_bucket"]

        self.hourly_rollup_start: str = self.rollup_defaults["hourly_rollup_start"]
        self.daily_rollup_start: str = self.rollup_defaults["daily_rollup_start"]
        self.weekly_rollup_start: str = self.rollup_defaults["weekly_rollup_start"]
        self.monthly_rollup_start: str = self.rollup_defaults["monthly_rollup_start"]

        # Tracks whether the shared narrow CAGG views have been created.
        # Narrow rollup views (hourly/daily/weekly/monthly_rollup_narrow) are shared
        # across all protocols, so they only need to be built once.
        self._narrow_rollups_created: bool = False

        # Registry of all CAGG view names this manager is responsible for refreshing.
        # Populated by add_wide_rollup and _ensure_narrow_cagg_views.
        # Keyed as view_name -> start_offset string, preserving insertion order
        # so the refresh loop always runs hourly -> daily -> weekly -> monthly.
        self.known_rollup_views: dict[str, str] = {}


    @property
    def tsdb_connected(self) -> bool:
        """Always returns the live connection state from the shared policy dict.
            This allows the RollupManager to react immediately to changes in TSDB connection status,
            which is critical for coordinating rollup refreshes and backlog replays.
        """
        val: Any = self.rollup_policy.get("tsdb_connected", False)
        # If val is the boolean False, the expression 'val is True or val == "True"' will return False.
        # basically tries to capture string "True" as well as boolean True if somehow the config was passed as a string.
        if val is True or val == "True":

            return val is True
        else:
            return False

    def setup_narrow_rollup(self) -> None:
        """
        Build the shared narrow-table post-creation stack.

        This method preserves the original narrow bootstrap behavior while
        routing the work through a single lifecycle helper so shared and
        protocol-specific paths use the same orchestration shape.
        """
        chunk_interval, compress_after, band_name = self.hypertable_mgr.get_dynamic_raw_table_settings_helper(
            target_protocol_name=None,
            static_chunk_interval=self.hypertable_mgr.raw_narrow_chunk_time_interval,
            static_compress_after=self.hypertable_mgr.raw_narrow_compress_after_interval,
        )
        self._log.info(f"Narrow raw table sizing: {band_name} -> chunk={chunk_interval}, compress_after={compress_after}")
        if band_name == "Static (no live data yet)":
            self.hypertable_mgr.flag_pending_retune(None)

        self.setup_rollup(
            table_name="device_metrics_narrow",
            segment_by=self.hypertable_mgr.compress_segmentby_narrow,
            # Dynamically sized (or falls back to the static raw_narrow_*
            # settings) -- see get_dynamic_raw_table_settings_helper.
            compress_after_interval=compress_after,
            chunk_time_interval=chunk_interval,
            protocol_name=None,
            wide_table_name=None,
            use_shared_rollup_flow=True,
        )

    def setup_rollup(
        self,
        table_name: str,
        segment_by: str,
        compress_after_interval: str,
        chunk_time_interval: str,
        protocol_name: str | None,
        wide_table_name: str | None,
        use_shared_rollup_flow: bool,
        force: bool = False,
    ) -> None:
        """
        Run the common post-table-processing lifecycle for both narrow and wide sources.

        The shared narrow stack and protocol wide stack both apply the same
        major categories of work: hypertable setup, compression setup,
        retention policy, rollup creation, and initial refresh. This helper
        centralizes that lifecycle while still allowing narrow-specific and
        wide-specific rollup creation to diverge where required.

        compress_after_interval / chunk_time_interval: both single values,
        not lists. `table_name` is one raw hypertable, which can only ever
        have one compression policy and one chunk_time_interval —
        TimescaleDB doesn't support "4 different values" of either on one
        object.
        Callers should pass raw_narrow_compress_after_interval / raw_
        narrow_chunk_time_interval (from setup_narrow_rollup) or raw_wide_
        compress_after_interval / raw_wide_chunk_time_interval (from add_
        wide_rollup) — settings dedicated to each raw table, deliberately
        separate both from each other (narrow and wide have very different
        write volume and row density, so their chunk sizing and
        compression scheduling shouldn't be tied together either — see
        hypertable_defaults' comments) and from the four rollup-view
        granularity settings
        (hourly_compress_after_interval, hourly_chunk_time_interval,
        etc.), since reusing one of those for the raw table would be an
        undocumented assumption discoverable only by reading this code.

        force: passed straight through to the rollup-creation step (see
        ensure_rollups / _ensure_cagg_views_for_protocol) — when True, the
        stack is purged and fully re-materialized even if its bucket
        intervals already match config. Used by the admin's "Force Rebuild"
        button; normal startup/reconnect calls leave this False.
        """
        # 1 Hypertable & Policies
        try:
            self.hypertable_mgr.ensure_hypertables([table_name], chunk_time_interval=chunk_time_interval)
            self._log.info("Hypertable check/creation complete")
        except Exception as e:
            self._log.error(f"Hypertable creation failed: {e}")

        # 2 Enable compression (if configured)
        try:
            if self.hypertable_mgr.enable_compression:
                self.hypertable_mgr.configure_compression(
                    table_name=table_name,
                    segment_by=segment_by,
                    compress_after_interval=compress_after_interval,
                )
        except Exception as e:
            self._log.error(f"Enable compression failed: {e}")

        # 3 Add retention policy
        try:
            self.hypertable_mgr.apply_retention_policy(table_name)
        except Exception as e:
            self._log.error(f"Add retention policy failed: {e}")

        # 4 Setup continuous aggregate rollups
        if self.enable_rollups:
            if use_shared_rollup_flow:
                # 4a
                try:
                    self.setup_with_retry(force=force)
                except Exception as e:
                    self._log.error(f"Aggregate Rollup setup failed: {e}")
                # 4b
                try:
                    self.refresh_rollups(force_full=True)
                except Exception as e:
                    self._log.error(f"Refresh Rollup failed: {e}")
            elif protocol_name is not None:
                # 4a
                try:
                    self._ensure_cagg_views_for_protocol(protocol_name, wide_table_name, force=force)
                except Exception as e:
                    self._log.error(f"CAGG view creation failed for protocol '{protocol_name}': {e}")

                # 4b
                try:
                    self._refresh_protocol_rollups_helper(protocol_name, wide_table_name)
                except Exception as e:
                    self._log.error(
                        f"Initial rollup refresh failed for '{protocol_name}': {e}"
                        f" — views exist but data may not be pre-aggregated."
                    )

    def start_auto_refresh(self) -> None:

        if self._refresh_rollup_thread.is_alive():
            self._log.debug("Auto refresh thread already running.")
            return
        else:
            # start _refresh_rollup_thread after connect completes successfully
            self._refresh_rollup_thread.start()
            self._log.debug("Auto rollup refresh thread started.")


    def setup_with_retry(self, force: bool = False) -> None:
        """_summary_
         This method wraps the ensure_rollups() call with retry Logic to handle potential lock timeouts that can occur if the flush
            thread is actively writing to the source tables while we attempt to set up or refresh continuous aggregates.
            If a lock timeout is detected, the method will wait for 5 seconds and retry, up to a maximum of 3 attempts.
            This allows the flush thread to complete its current batch and release locks before we try again, improving
            the chances of a successful rollup setup without manual intervention.
            If the error is not a lock timeout, it will be raised immediately without retrying, as it likely indicates
            a different issue that needs attention.

        force: passed straight through to ensure_rollups() — see there for
        what it does.
        """
        max_rollup_retries = 3
        for attempt in range(max_rollup_retries):
            try:
                self.ensure_rollups(force=force)
                break # Success!
            except Exception as e:
                if "lock_timeout" in str(e):
                    self._log.warning(f"Lock timeout on attempt {attempt+1}. Retrying...")
                    time.sleep(5) # Wait for flush thread to clear
                else:
                    raise  # Real error, don't retry


    def ensure_rollups(self, force: bool = False) -> None:
        """
        Sets up continuous aggregate rollups based on predefined configurations.
        Uses a Scan-then-Purge approach to handle hierarchical dependencies safely.
        Checks if rollups need to be rebuilt and creates them accordingly.
        The method scans existing rollup views to determine if any bucket interval changes have occurred.
        If a change is detected, it purges all rollups in the correct order (weekly -> daily -> hourly) to ensure
        a clean slate for rebuilding. After purging, it creates or verifies each rollup view from the
        bottom up (hourly -> daily -> weekly) to maintain hierarchical integrity.
        The entire process is wrapped in a migration lock to pause the flush thread and prevent conflicts during schema changes.

        force: when True, skips trusting the bucket-interval scan and always
        purges + fully re-materializes the whole narrow stack, even if every
        view's bucket already matches config. Used by the admin's "Force
        Rebuild" button; the default "Rebuild Rollups" button and all
        startup/reconnect callers leave this False so an already-correct
        stack is only re-verified, not needlessly re-materialized.
        """
        self.migration_in_progress.set()
        self._log.info("Pausing flush thread for migration...")

        try:
            with self.SessionFactory() as session:
                with self._reconnect_lock:
                    tsdb_connected: bool = self.tsdb_connected

                if not tsdb_connected or not session:
                    self._log.error("Cannot set up rollups — not tsdb_connected.")
                    return

                self._log.info("Starting continuous aggregate setup...")

                # 1. Configuration Setup
                contexts: list[dict[str, Any]] = [
                    {
                        "table_name": "device_metrics_narrow",
                        "segments": {
                            "hourly_rollup": "hourly_rollup_narrow",
                            "daily_rollup": "daily_rollup_narrow",
                            "weekly_rollup": "weekly_rollup_narrow",
                            "monthly_rollup": "monthly_rollup_narrow",
                        }
                    }
                ]

                view_configs: list[tuple[str, str, str, str]] = [
                    ("hourly_rollup", self.hourly_rollup_bucket, self.hourly_rollup_start, self.hourly_chunk_time_interval),
                    ("daily_rollup", self.daily_rollup_bucket, self.daily_rollup_start, self.daily_chunk_time_interval),
                    ("weekly_rollup", self.weekly_rollup_bucket, self.weekly_rollup_start, self.weekly_chunk_time_interval),
                    ("monthly_rollup", self.monthly_rollup_bucket, self.monthly_rollup_start, self.monthly_chunk_time_interval),
                ]

                # 2. Scan Phase: Detect if any bucket change exists across the whole stack
                #    Scan Phase: Detect whether the stack is missing, mismatched, or already valid
                any_rebuild_needed: bool = False
                any_missing_views: bool = False

                for context in contexts:
                    for view_key, bucket, _, _ in view_configs:
                        view_name: str = context["segments"][view_key]
                        rollup_state: str = self.rollup_needs_rebuild(session, view_name, bucket)

                        if rollup_state == "rebuild":
                            any_rebuild_needed = True
                            break

                        if rollup_state == "missing":
                            any_missing_views = True

                    if any_rebuild_needed:
                        break

                if force and not any_rebuild_needed:
                    self._log.info("Force rebuild requested. Purging all rollups regardless of scan result.")
                    any_rebuild_needed = True

                # 3. Purge Phase: Only purge when an existing rollup stack is mismatched
                if any_rebuild_needed:
                    self._log.info("Bucket change detected. Purging all rollups for clean rebuild.")
                    # This method must drop Weekly -> Daily -> Hourly with sequential commits
                    self._drop_all_continuous_aggregates(session)
                    # Purge orphaned scheduler jobs
                    self._purge_ghost_jobs_helper(session)
                elif any_missing_views:
                    self._log.info("Missing rollups detected. Creating required views without purge.")


                # 4. Creation Phase: Build/Verify Bottom-Up (Hourly -> Daily -> Weekly)
                for context in contexts:
                    source_table: str = context["table_name"]
                    current_source: str = source_table  # Reset source for each context (Narrow vs Wide)
                    rollup_segments: dict[str, str] = context["segments"]

                    for view_key, bucket, start_offset, _ in view_configs:
                        # gran: str = view_key.split("_")[0]  # "hourly", "daily", etc.
                        view_name = rollup_segments[view_key]

                        # Create if missing and register into known_rollup_views
                        # regardless, so the refresh loop picks it up.
                        self._ensure_single_cagg_view_helper(
                            session=session,
                            view_name=view_name,
                            source_table=current_source,
                            bucket_interval=bucket,
                            start_offset=start_offset,
                            protocol_name="shared_narrow"
                        )

                        # Update current_source for Hierarchical Aggregation
                        # Note: Monthly is kept on the source_table
                        if view_key == 'weekly_rollup':
                            current_source = source_table
                        elif view_key != 'monthly_rollup':
                            current_source = view_name

                self._log.info("Continuous aggregate setup completed successfully.")
        finally:
            # 2. Always re-enable flushing, even if migration fails
            self.migration_in_progress.clear()
            self._log.info("Resuming flush thread.")


    def _create_rollup_helper(
        self,
        session: Session,
        source: str,
        view_name: str,
        bucket_interval: str,
        start_offset: str,
        base_wide_table_name: str | None = None
        ) -> None:
        """
        Create one continuous aggregate view using the shared narrow/wide SQL pipeline.

        Narrow and wide rollups follow the same structural flow:
        time bucket, device grouping, metric dimension, and aggregate fields.
        The real variation is the metric dimension itself, so this method builds
        one generic SELECT using typed metric descriptors and then applies the
        standard view policies.
        """
        r_settings: dict[str, Any] = self.hypertable_mgr.get_dynamic_settings_helper()

        if not session:
            self._log.error("Cannot create rollup — not connected.")
            return

        # Set local lock timeout to fail fast if blocked by flush thread.  Set dynamically from settings.
        self.hypertable_mgr.set_lock_timeout(session, r_settings['lock_timeout'])

        # Determine Aggregation Mode
        # If source is another view, we MUST use rollup(). If it's a hypertable, use stats_agg().
        reading_from_raw: bool = (source == "device_metrics_narrow" or source.startswith("device_metrics_wide__"))

        try:
            descriptors: list[RollupManager.RollupMetricDescriptor] = self._resolve_rollup_metric_descriptors_helper(
                session=session,
                source=source,
                base_wide_table_name=base_wide_table_name,
            )

            select_clauses: list[str] = [
                f"time_bucket(INTERVAL '{bucket_interval}', m_time, '{self.machine_timezone}') AS m_time",
                "device_info_id",
            ]
            group_by_positions: list[str] = ["1", "2"]

            for descriptor in descriptors:
                if descriptor.group_key_sql is not None:
                    select_clauses.append(descriptor.group_key_sql)
                    group_by_positions.append(str(len(group_by_positions) + 1))

                if reading_from_raw:
                    select_clauses.append(f"MIN({descriptor.raw_value_sql}) AS {descriptor.min_alias}")
                    select_clauses.append(f"MAX({descriptor.raw_value_sql}) AS {descriptor.max_alias}")
                    select_clauses.append(
                        f"stats_agg({descriptor.raw_value_sql}) AS {descriptor.summary_alias}"
                    )
                else:
                    select_clauses.append(f"MIN({descriptor.rolled_min_sql}) AS {descriptor.min_alias}")
                    select_clauses.append(f"MAX({descriptor.rolled_max_sql}) AS {descriptor.max_alias}")
                    select_clauses.append(
                        f"rollup({descriptor.rolled_summary_sql}) AS {descriptor.summary_alias}"
                    )

            session.execute(text(f"""
                CREATE MATERIALIZED VIEW {view_name}
                WITH (timescaledb.continuous = true) AS
                SELECT
                    {', '.join(select_clauses)}
                FROM {source}
                GROUP BY {', '.join(group_by_positions)}
                WITH NO DATA;
            """))  # noqa: S608


            # Apply Policies & Index
            self._add_aggregate_policy_helper(session, view_name, bucket_interval, start_offset)


            # Finalize the view so it is available as a 'source' for the next view in the loop
            session.commit()
            self._log.info(f"Successfully created hierarchical rollup: {view_name}")

        except Exception as e:
            session.rollback()
            self._log.error(f"Failed to create {view_name}: {e}")
            raise

    def _add_aggregate_policy_helper(self, session: Session, view_name: str, bucket_interval: str, start_offset: str) -> None:
        """
        Applies (or re-applies) refresh, retention, and compression
        policies to a continuous aggregate view, using granularity-
        specific settings from hypertable_policy.

        Every policy here is removed and re-added, not just added with
        if_not_exists => TRUE. That call is a genuine no-op when a policy
        already exists for `view_name`: TimescaleDB issues a NOTICE and
        leaves the existing job's schedule untouched rather than updating
        it. Without the remove-then-add here, this method would only ever
        set correct policies at a view's very first creation -- a later
        settings change (daily_compress_after_interval, drop_after, a
        rollup start-offset, ...) would silently never reach an already-
        existing view, since views are only dropped and recreated when
        their BUCKET interval changes (see rollup_needs_rebuild); policy-
        only drift was never separately detected or corrected. This
        mirrors the identical fix already applied to raw-table compression
        in ensure_compression_policy — see that method's docstring.

        Called from two places: _create_rollup_helper (a freshly created
        view) AND _ensure_single_cagg_view_helper's already-exists branch
        (an existing view, on every connect/reconnect/Rebuild-Rollups
        pass) -- so a policy setting change reaches every view on the very
        next such pass, with no purge-and-recreate of the view's actual
        data required. Removing and re-adding a policy only affects its
        ongoing schedule going forward; it never touches data the view
        already holds, or chunks already compressed/dropped under the
        previous schedule.

        compress_after and the view's own chunk_time_interval are both
        resolved dynamically here (via get_dynamic_view_settings_helper),
        falling back to the static hourly_compress_after_interval /
        hourly_chunk_time_interval-style settings (or their daily/weekly/
        monthly siblings) when dynamic sizing is off or there's no live
        cardinality data yet. Both granularity and the owning protocol are
        derived purely from `view_name` (see _derive_target_protocol_
        from_view_name_helper). Set via set_chunk_time_interval(), unconditionally on
        every pass (create or resync) for the same reason the policies
        above are unconditional -- TimescaleDB's create-time chunk_time_
        interval argument is itself a no-op against an already-existing
        object, so without a standalone set_chunk_time_interval() call, a
        chunk-sizing change would only ever reach a view at its very first
        creation.

        Parameters:
            session: Active database session for executing SQL commands.
            view_name: The name of the continuous aggregate view to which policies will be applied.
            bucket_interval: The time bucket size for the continuous aggregate (e.g., "1 hour", "1 day").
            start_offset: The offset for the continuous aggregate policy (e.g., "3 hours", "3 days").
        """
        # 1. Map view name to its granularity key for lookup
        name_lower: str = view_name.lower()

        # Define the supported granularities
        granularities: List[str] = ["hourly", "daily", "weekly", "monthly"]

        # Find the first matching granularity or use "default"
        granularity: str = next((g for g in granularities if g in name_lower), "default")

        # Resolve compress_after AND chunk_time_interval together — dynamic
        # (cardinality-based) if enabled and there's live data, else the
        # static per-granularity setting. See get_dynamic_view_settings_
        # helper's docstring for the full reasoning.
        static_compress_after: str = getattr(self, f"{granularity}_compress_after_interval", self.daily_compress_after_interval)
        static_chunk_interval: str = getattr(self, f"{granularity}_chunk_time_interval", self.daily_chunk_time_interval)
        target_protocol_name: str | None = self._derive_target_protocol_from_view_name_helper(session, view_name, granularity)
        chunk_interval, compress_after, sizing_band = self.get_dynamic_view_settings_helper(
            granularity, target_protocol_name,
            static_chunk_interval=static_chunk_interval,
            static_compress_after=static_compress_after,
        )
        self._log.debug(f"View sizing for '{view_name}': {sizing_band} -> chunk={chunk_interval}, compress_after={compress_after}")

        drop_after: str = getattr(self, "drop_after", "1 year")  # Default retention if not specified

        try:
            self.hypertable_mgr.set_lock_timeout(session, "10s")

            # 2. View's own chunk sizing — see docstring above for why this
            # is applied unconditionally here rather than only at creation.
            session.execute(text(f"SELECT set_chunk_time_interval('{view_name}', INTERVAL '{chunk_interval}');"))

            # 3. Continuous Aggregate Refresh Policy — remove-then-add so a
            # start_offset/schedule change reaches an already-existing view.
            session.execute(text(f"SELECT remove_continuous_aggregate_policy('{view_name}', if_exists => true);"))
            session.execute(text(f"""
                SELECT add_continuous_aggregate_policy(
                    '{view_name}',
                    start_offset      => INTERVAL '{start_offset}',
                    end_offset        => INTERVAL '{bucket_interval}',
                    initial_start     => '{self.anchor_start_time_utc}'::timestamptz,
                    schedule_interval => INTERVAL '{bucket_interval}'
                );
            """))

            # 4. Data Retention Policy (specific to the view) — remove-
            # then-add so a drop_after change reaches an already-existing
            # view, but skipped entirely when nothing would actually
            # change (see policy_config_matches_helper).
            if self.hypertable_mgr.policy_config_matches_helper(session, view_name, "policy_retention", "drop_after", drop_after):
                self._log.debug(
                    f"_add_aggregate_policy_helper: '{view_name}' already drop_after={drop_after}, "
                    f"skipping retention remove/re-add."
                )
            else:
                session.execute(text(f"SELECT remove_retention_policy('{view_name}', if_exists => true);"))
                session.execute(text(f"""
                    SELECT add_retention_policy(
                        '{view_name}',
                        drop_after => INTERVAL '{drop_after}'
                    );
                """))

            # 5. Compression Policy — remove-then-add so a compress_after
            # change reaches an already-existing view, but skipped
            # entirely when nothing would actually change (see
            # policy_config_matches_helper). This is the specific
            # remove/re-add that raced TimescaleDB's own job scheduler
            # and produced a one-off Failed run against a freshly
            # retuned protocol's rollup-view compression job in practice.
            if self.hypertable_mgr.enable_compression:
                # ALTER ... SET is inherently idempotent (not a "create"
                # call), safe to repeat on every pass.
                session.execute(text(f"ALTER MATERIALIZED VIEW {view_name} SET (timescaledb.compress = true);"))

                if self.hypertable_mgr.policy_config_matches_helper(
                    session, view_name, "policy_compression", "compress_after", compress_after
                ):
                    self._log.debug(
                        f"_add_aggregate_policy_helper: '{view_name}' already compress_after={compress_after}, "
                        f"skipping compression remove/re-add."
                    )
                else:
                    session.execute(text(f"SELECT remove_compression_policy('{view_name}', if_exists => true);"))
                    session.execute(text(f"""
                        SELECT add_compression_policy(
                            '{view_name}',
                            compress_after => INTERVAL '{compress_after}'
                        );
                    """))

            # 6. Performance Index
            safe_view_name: str = view_name.replace('"', '').replace('.', '_')
            index_name: str = f"idx_{safe_view_name}_time"

            session.execute(text(f"""
                CREATE INDEX IF NOT EXISTS {index_name} ON {view_name} (m_time DESC);
            """))

            session.commit()
            self._log.info(
                f"Policies applied to {view_name}: Chunk={chunk_interval}, "
                f"Retention={drop_after}, Compression After={compress_after}"
            )

        except Exception as e:
            session.rollback()
            self._log.error(f"Failed to apply policies to {view_name}: {e}")
            raise


    def _resolve_rollup_metric_descriptors_helper(
        self,
        session: Session,
        source: str,
        base_wide_table_name: str | None
        ) -> list[RollupMetricDescriptor]:
        """
        Resolve the metric dimension for generic rollup SQL creation.

        Narrow rollups return a single descriptor keyed by `metric_name`.
        Wide rollups return one descriptor per protocol metric column, allowing
        the same creation loop to build either shape with identical aggregate
        semantics.
        """
        if "wide" not in source and base_wide_table_name is None:
            return [
                RollupManager.RollupMetricDescriptor(
                    group_key_sql="metric_name",
                    raw_value_sql="metric_value",
                    rolled_min_sql="min_value",
                    rolled_max_sql="max_value",
                    rolled_summary_sql="stats_summary",
                    min_alias="min_value",
                    max_alias="max_value",
                    summary_alias="stats_summary",
                )
            ]

        wide_table_name: str = base_wide_table_name or source
        self._log.debug(f"_resolve_rollup_metric_descriptors_helper: wide_table_name='{wide_table_name}'")

        result = session.execute(
            text(
                """
                SELECT mc.clean_column_name
                FROM metric_catalog mc
                JOIN protocol_registry pr ON mc.protocol_id = pr.protocol_id
                WHERE pr.wide_table_name = :tname
                AND mc.data_type NOT IN ('TEXT', 'BOOLEAN')
                ORDER BY mc.clean_column_name
                """
            ),
            {"tname": wide_table_name},
        )

        column_names: list[str] = list(result.scalars())

        return [
            RollupManager.RollupMetricDescriptor(
                group_key_sql=None,
                raw_value_sql=column_name,
                rolled_min_sql=f"min_{column_name}",
                rolled_max_sql=f"max_{column_name}",
                rolled_summary_sql=f"stats_summary_{column_name}",
                min_alias=f"min_{column_name}",
                max_alias=f"max_{column_name}",
                summary_alias=f"stats_summary_{column_name}",
            )
            for column_name in column_names
        ]

    def add_wide_rollup(self, protocol_name: str, wide_table_name: str | None, force: bool = False) -> None:
        """
        Build the protocol-specific wide-table rollup stack for one protocol.

        This method delegates the common table lifecycle work to the same
        post-processing helper used by the shared narrow stack, while
        preserving protocol-specific metric count caching and completion
        tracking.

        force: passed straight through to setup_rollup() -> _ensure_cagg_
        views_for_protocol() — when True, this protocol's stack is purged
        and fully re-materialized even if its bucket intervals already
        match config. Used by the admin's "Force Rebuild" button.
        """

        self.migration_in_progress.set()

        try:
            if wide_table_name is None:
                self._log.info(f"Protocol '{protocol_name}' is narrow-only — skipping protocol-specific wide stack.")
                self.mark_rollup_setup_complete_helper(protocol_name, complete=True)
                return

            chunk_interval, compress_after, band_name = self.hypertable_mgr.get_dynamic_raw_table_settings_helper(
                target_protocol_name=protocol_name,
                static_chunk_interval=self.hypertable_mgr.raw_wide_chunk_time_interval,
                static_compress_after=self.hypertable_mgr.raw_wide_compress_after_interval,
            )
            self._log.info(
                f"Wide raw table sizing for '{protocol_name}': {band_name} -> "
                f"chunk={chunk_interval}, compress_after={compress_after}"
            )
            if band_name == "Static (no live data yet)":
                self.hypertable_mgr.flag_pending_retune(protocol_name)

            self.setup_rollup(
                table_name=wide_table_name,
                segment_by=self.hypertable_mgr.compress_segmentby_wide,
                # Dynamically sized (or falls back to the static raw_wide_*
                # settings) -- see get_dynamic_raw_table_settings_helper.
                # Narrow uses its own, independently-computed values — see
                # the sibling call in setup_narrow_rollup.
                compress_after_interval=compress_after,
                chunk_time_interval=chunk_interval,
                protocol_name=protocol_name,
                wide_table_name=wide_table_name,
                use_shared_rollup_flow=False,
                force=force,
            )

            # Retention and Catalog Count
            # None should have returned early, but this is a sanity check
            if wide_table_name is not None:  # type: ignore[reportUnnecessaryComparison]

                # Record this protocol's column count for dynamic settings tuning.
                # wide_table_name is None for narrow-only and column count is irrelevant there.
                try:
                    with self.SessionFactory() as session:
                        cat_count: int = session.execute(
                            text("SELECT metric_count FROM protocol_registry WHERE protocol_name = :p"),
                            {"p": protocol_name}
                        ).scalar() or 0
                    self.hypertable_mgr.protocol_wide_column_counts[protocol_name] = cat_count
                except SQLAlchemyError:
                    pass  # Non-fatal — _get_dynamic_settings falls back to 0

            # Mark complete — protocol_name is in scope here naturally
            self.mark_rollup_setup_complete_helper(protocol_name, complete=True)

            self._log.info(f"RollupManager.add_wide_rollup: setup complete for '{protocol_name}'")

        except Exception as e:
            self._log.error(f"RollupManager.add_wide_rollup failed for '{protocol_name}': {e}")
            # rollup_setup_complete stays False — next startup retries
            raise

        finally:
            self.migration_in_progress.clear()


    def mark_rollup_setup_complete_helper(self, protocol: str, complete: bool = True ) -> None:
        """
        Sets rollup_setup_complete for the given protocol in protocol_registry.
        Called by RollupManager after successful setup_narrow_rollup (complete=True),
        or when a schema change requires rollup views to be rebuilt (complete=False).
        """
        with self.SessionFactory() as session:
            try:
                with session.begin():
                    session.execute(
                        text("""
                            UPDATE protocol_registry
                            SET rollup_setup_complete = :complete,
                                updated_at = :now
                            WHERE protocol_name = :p
                        """),
                        {
                            "complete": complete,
                            "p": protocol,
                            "now": _now_tz()
                        }
                    )
                self._log.debug(f"rollup_setup_complete = {complete} for protocol '{protocol}'")
            except Exception as e:
                self._log.error(f"Failed to mark rollup setup complete for '{protocol}': {e}")
                raise


    def _ensure_cagg_views_for_protocol(self, protocol_name: str, wide_table_name: str | None, force: bool = False) -> None:
        """
        Create or rebuild the four hierarchical CAGG views for a protocol's wide table.

        This method mirrors the narrow rebuild-detection behavior:
        it scans the protocol-specific view stack for bucket drift, purges that
        stack if needed, and then recreates the views bottom-up using the shared
        generic CAGG builder.

        View names follow the pattern:
            hourly_rollup_wide__eg4_18kpv
            daily_rollup_wide__eg4_18kpv
            weekly_rollup_wide__eg4_18kpv
            monthly_rollup_wide__eg4_18kpv

        For narrow-only protocols (wide_table_name=None), creates views
        against device_metrics_narrow scoped by device_info_id instead.

        force: when True, skips trusting the bucket-interval scan and always
        purges + fully re-materializes this protocol's whole stack, even if
        every view's bucket already matches config. Used by the admin's
        "Force Rebuild" button; the default "Rebuild Rollups" button and all
        startup/reconnect callers leave this False.
        """
        if wide_table_name is None:
            self._log.info(f"Protocol '{protocol_name}' is narrow-only — skipping wide table CAGG views.")
            # Narrow rollup views are shared across all protocols —
            # only create them once
            if not self._narrow_rollups_created:
                self._ensure_narrow_cagg_views_helper()
                self._narrow_rollups_created = True
            return

        # Derive rollup prefix from wide table name
        # "device_metrics_wide__eg4_18kpv" -> "rollup_wide__eg4_18kpv"
        suffix: str = wide_table_name.removeprefix("device_metrics_")
        rollup_prefix: str = f"rollup_{suffix}"

        granularities: list[tuple[str, str, str, str]] = [
            # (granularity, bucket, start_offset, compress_after)
            ("hourly",  self.hourly_rollup_bucket,  self.hourly_rollup_start,
                        self.hourly_compress_after_interval),
            ("daily",   self.daily_rollup_bucket,   self.daily_rollup_start,
                        self.daily_compress_after_interval),
            ("weekly",  self.weekly_rollup_bucket,  self.weekly_rollup_start,
                        self.weekly_compress_after_interval),
            ("monthly", self.monthly_rollup_bucket, self.monthly_rollup_start,
                        self.monthly_compress_after_interval),
        ]

        try:
            with self.SessionFactory() as session:
                view_specs: list[tuple[str, str, str, str]] = []

                # Hierarchical — each view depends on the previous one
                # so they must be created in order
                previous_view: str = wide_table_name

                for gran, bucket, start_offset, _ in granularities:
                    view_name: str = f"{gran}_{rollup_prefix}"

                    # Monthly sources directly from the base wide table,
                    # not from the weekly view — a month is not a fixed
                    # multiple of 7 days so TimescaleDB rejects that hierarchy.
                    source_for_this_view: str = wide_table_name if gran == "monthly" else previous_view
                    view_specs.append((view_name, source_for_this_view, bucket, start_offset))
                    previous_view = view_name

                any_rebuild_needed: bool = False
                any_missing_views: bool = False

                for view_name, _, bucket, _ in view_specs:
                    rollup_state: str = self.rollup_needs_rebuild(session, view_name, bucket)

                    if rollup_state == "rebuild":
                        any_rebuild_needed = True
                        break

                    if rollup_state == "missing":
                        any_missing_views = True

                if force and not any_rebuild_needed:
                    self._log.info(f"Force rebuild requested for protocol '{protocol_name}'. Purging regardless of scan result.")
                    any_rebuild_needed = True

                if any_rebuild_needed:
                    self._log.info(f"Bucket change detected for protocol '{protocol_name}'. Purging protocol rollups for clean rebuild.")
                    self.drop_protocol_rollup(session=session, view_names=[view_name for view_name, _, _, _ in view_specs])
                    self._purge_ghost_jobs_helper(session)

                elif any_missing_views:
                    self._log.info(f"Missing rollups detected for protocol '{protocol_name}'. Creating required views without purge.")

                for view_name, source_for_this_view, bucket, start_offset in view_specs:
                    self._ensure_single_cagg_view_helper(
                        session=session,
                        view_name=view_name,
                        source_table=source_for_this_view,
                        bucket_interval=bucket,
                        start_offset=start_offset,
                        protocol_name=protocol_name,
                        base_wide_table_name=wide_table_name,
                    )

                session.commit()

            self._log.info(f"CAGG views created for protocol '{protocol_name}' with prefix '{rollup_prefix}'")

        except SQLAlchemyError as e:
            self._log.error(f"CAGG view creation failed for protocol '{protocol_name}': {e}")
            raise

    def drop_protocol_rollup(self, session: Session, view_names: list[str]) -> None:
        """
        Drop one protocol-specific rollup stack in dependency-safe reverse order.

        Only existing views are touched. If any per-view cleanup step fails, the
        transaction is rolled back before continuing so the session does not remain
        in the aborted state.
        """
        r_settings: dict[str, Any] = self.hypertable_mgr.get_dynamic_settings_helper()

        ordered_view_names: list[str] = sorted(
            view_names,
            key=lambda name: 0 if "hourly" in name else 1 if "daily" in name else 2 if "weekly" in name else 3,
            reverse=True,
        )

        for view_name in ordered_view_names:
            full_name: str = f'"public"."{view_name}"'

            # Skip views that do not exist yet. This avoids poisoning the transaction
            # on ALTER / policy removal calls against missing materialized views.
            if not self._view_exists_helper(session, view_name):
                self._log.info(f"Rollup does not exist, skipping purge: {full_name}")
                continue

            self._log.info(f"Purging rollup: {full_name}")

            try:
                self.hypertable_mgr.set_lock_timeout(session, r_settings['lock_timeout'])

                # Disable compression first when present.
                try:
                    session.execute(text(f"ALTER MATERIALIZED VIEW {full_name} SET (timescaledb.compress = false);"))
                except Exception:
                    # IMPORTANT: clear failed transaction state before continuing
                    session.rollback()
                    self._log.info(f"View was already uncompressed: {full_name}")

                    # Re-enter a clean transaction for the remaining cleanup
                    self.hypertable_mgr.set_lock_timeout(session, r_settings['lock_timeout'])

                session.execute(text(f"SELECT remove_continuous_aggregate_policy('{full_name}', if_exists => true);"))
                session.execute(text(f"SELECT remove_retention_policy('{full_name}', if_exists => true);"))
                session.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {full_name} CASCADE;"))

                session.commit()
                self.known_rollup_views.pop(view_name, None)

            except Exception as e:
                session.rollback()
                self._log.error(f"Failed to purge rollup {full_name}: {e}")
                raise

    # -------------------------
    # Helpers for per-protocol CAGG management
    # -------------------------

    def _ensure_narrow_cagg_views_helper(self) -> None:
        """
        Creates the four shared narrow CAGG views (hourly/daily/weekly/monthly_rollup_narrow).
        These are shared across all protocols — device_info_id already partitions by device.
        Called at most once per RollupManager lifetime, guarded by _narrow_rollups_created.
        """
        granularities: list[tuple[str, str, str]] = [
            ("hourly",  self.hourly_rollup_bucket,  self.hourly_rollup_start),
            ("daily",   self.daily_rollup_bucket,   self.daily_rollup_start),
            ("weekly",  self.weekly_rollup_bucket,  self.weekly_rollup_start),
            ("monthly", self.monthly_rollup_bucket, self.monthly_rollup_start),
        ]

        try:
            with self.SessionFactory() as session:
                previous_source = "device_metrics_narrow"
                for gran, bucket, start_offset in granularities:
                    view_name: str = f"{gran}_rollup_narrow"

                    # Monthly sources directly from the base hypertable —
                    # 1 month is not a fixed multiple of 7 days so TimescaleDB
                    # rejects monthly-over-weekly hierarchies.
                    source_for_this_view: str = ("device_metrics_narrow" if gran == "monthly" else previous_source)

                    self._ensure_single_cagg_view_helper(
                        session=session,
                        view_name=view_name,
                        source_table=source_for_this_view,
                        bucket_interval=bucket,
                        start_offset=start_offset,
                        protocol_name="shared_narrow"
                    )
                    previous_source: str = view_name

            self._log.info("Shared narrow CAGG views created/verified.")

        except SQLAlchemyError as e:
            self._log.error(f"_ensure_narrow_cagg_views failed: {e}")
            raise

    def _ensure_single_cagg_view_helper(
        self,
        session: Session,
        view_name: str,
        source_table: str,
        bucket_interval: str,
        start_offset: str,
        protocol_name: str,
        base_wide_table_name: str | None = None,
    ) -> None:
        """
        Idempotently creates one CAGG view if it does not already exist —
        AND, either way, ensures its refresh/retention/compression
        policies match current settings, then records it in
        known_rollup_views for the refresh loop.

        For a brand-new view, delegates to _create_rollup_helper (which
        branches wide vs narrow internally based on whether 'wide' appears
        in source_table); that helper applies policies as its final step.
        For an already-existing view, calls _add_aggregate_policy_helper
        directly to re-sync its policies without touching the view's data
        or definition — see that method's docstring for why this matters:
        without it, a settings change would only ever reach a view at its
        very first creation.

        Args:
            session:          Active session (caller owns the transaction).
            view_name:        Target materialized view name.
            source_table:     Hypertable or parent CAGG view to aggregate from.
            bucket_interval:  Time-bucket width, e.g. '1 hour'.
            start_offset:     Refresh policy start offset, e.g. '3 hours'.
            protocol_name:    Used for log context only.
        """
        if self._view_exists_helper(session, view_name):
            self._log.debug(f"CAGG view '{view_name}' already exists for protocol '{protocol_name}' — re-syncing policies to current settings.")
            # Re-apply (remove-then-add) refresh/retention/compression
            # policies even though the view itself doesn't need rebuilding
            # -- this is what lets a settings change (compress_after,
            # drop_after, start_offset, ...) reach an already-existing view
            # without a full purge-and-recreate. See _add_aggregate_policy_
            # helper's docstring for the full reasoning.
            try:
                self._add_aggregate_policy_helper(session, view_name, bucket_interval, start_offset)
            except Exception as e:
                self._log.error(f"Policy re-sync failed for existing view '{view_name}': {e} — view itself is untouched, will retry next pass.")
        else:
            self._log.info(f"Creating CAGG view '{view_name}' from '{source_table}' during startup.")
            self._create_rollup_helper(session, source_table, view_name, bucket_interval, start_offset, base_wide_table_name=base_wide_table_name)

        # Register in the refresh registry regardless of whether we just created it
        # so the refresh loop picks it up on every run.
        if view_name not in self.known_rollup_views:
            self.known_rollup_views[view_name] = start_offset


    def _refresh_protocol_rollups_helper(self, protocol_name: str, wide_table_name: str | None) -> None:
        """
        Performs the initial full refresh for all CAGG views that belong to
        a specific protocol.  Called once at the end of add_wide_rollup after
        views are created.

        For wide protocols the views are named:
            hourly_rollup_wide__<suffix>
            daily_rollup_wide__<suffix>
            ...
        For narrow-only protocols the shared narrow views are refreshed:
            hourly_rollup_narrow, daily_rollup_narrow, ...

        Args:
            protocol_name:   Used to derive the rollup_prefix for wide protocols.
            wide_table_name: The wide table name, or None for narrow-only.
        """
        granularities: list[tuple[str, str]] = [
            ("hourly",  self.hourly_rollup_start),
            ("daily",   self.daily_rollup_start),
            ("weekly",  self.weekly_rollup_start),
            ("monthly", self.monthly_rollup_start),
        ]

        if wide_table_name is not None:
            suffix: str = wide_table_name.removeprefix("device_metrics_")
            rollup_prefix: str = f"rollup_{suffix}"
            view_names: List[Tuple[str, str]] = [(f"{gran}_{rollup_prefix}", start) for gran, start in granularities]
        else:
            view_names = [(f"{gran}_rollup_narrow", start) for gran, start in granularities]

        successfully_refreshed = False
        for view_name, start_offset in view_names:
            with self.SessionFactory() as session:
                if not self._view_exists_helper(session, view_name):
                    self._log.warning(f"_refresh_protocol_rollups: '{view_name}' not found, skipping.")
                    continue
                try:
                    self._refresh_single_rollup_helper(view_name, start_offset, force_full=True)
                    successfully_refreshed = True
                except Exception as e:
                    self._log.error(
                        f"Initial refresh failed for '{view_name}': {e} — background loop will retry.")
        if successfully_refreshed:
            self._update_last_refresh_helper(protocol_name)


    def rollup_needs_rebuild(self, session: Session, view_name: str, bucket_interval: str) -> str:
        """
        Checks if a rollup exists and if its bucket matches the current config.
        Returns True if the rollup is missing or configuration is mismatched.
        Parameters:
            session: Active database session for executing SQL commands.
            view_name: The name of the continuous aggregate view to check.
            bucket_interval: The expected time bucket size for the view (e.g., "1 hour", "1 day").
        returns:
            bool: True if the rollup needs to be rebuilt (missing or config mismatch),
                  False if it exists and matches the expected bucket interval.

            This method compares the configured bucket interval with the actual interval used in the view definition.
            It first maps friendly terms to their equivalent PostgreSQL interval strings, then extracts the interval from
            the view definition using a regex, and finally compares the two intervals using PostgreSQL's interval comparison
            to ensure semantic correctness.  It's funky and may break if timescaledb changes their view definition format,
            but it is necessary to accurately detect when a rebuild is needed due to bucket changes,
            while avoiding unnecessary rebuilds when the intervals are semantically equivalent but textually different (e.g., '1 hour' vs '01:00:00').
        """
        # Map friendly terms to PG Interval inputs
        # This allows 'monthly' -> '1 month', while letting '2 hours' pass through as-is
        mapping: dict[str, str] = {
            "monthly": "1 month",
            "weekly": "7 days",
            "daily": "1 day",
            "hourly": "1 hour"
        }
        target_pg_val: str = mapping.get(bucket_interval.lower(), bucket_interval)

        try:

            # Query the TimescaleDB catalog for the view definition
            # Use a bind parameter :view_name for security and performance
            check_sql: TextClause = text("""
                SELECT view_definition
                FROM timescaledb_information.continuous_aggregates
                WHERE view_name = :view_name
            """)
            view_def: Optional[str] = session.scalar(check_sql, {"view_name": view_name})
            # 1. missing: not found in catalog, definitely needs build
            # 2. rebuild: exists but mismatched
            # 3. OK: exists and matches

            # If it doesn't exist, we definitely need to build it
            if not view_def:
                self._log.debug(f"Rollup {view_name} does not exist. Rebuild required.")
                return "missing"

            # If the result exists, extract the 'interval' string from the definition
            # We use Postgres regex_match to find the first argument of time_bucket()
            # Pattern looks for: time_bucket('interval_text', ...)
            extract_sql: TextClause = text("""
                SELECT (regexp_match(:vdef, 'time_bucket\\(''([^'']+)''', 'i'))[1]
            """)
            current_interval_str: Optional[str] = session.scalar(extract_sql, {"vdef": view_def})

            if not current_interval_str:
                self._log.warning(f"Could not parse time_bucket interval from {view_name} definition.")
                return "rebuild"  # Safer to rebuild if we can't verify the bucket

            # Final Comparison: Let PostgreSQL handle the semantic equality
            # This correctly recognizes that '01:00:00'::interval = '1 hour'::interval
            match_sql: TextClause = text("SELECT (:current)::interval = (:target)::interval")
            is_match: bool = session.scalar(match_sql, {"current": current_interval_str, "target": target_pg_val})

            if not is_match:
                self._log.info(
                    f"Config mismatch for {view_name}. "
                    f"Found: {current_interval_str}, Expected: {target_pg_val}. Rebuild required."
                )
                return "rebuild"
            else:
                # 4. Exists and matches config
                self._log.info(f"Rollup config matches for {view_name}. Expected: {bucket_interval} and received {target_pg_val}. No rebuild required.")
                return "OK"

        except SQLAlchemyError as e:
            self._log.error(f"Database error while checking rollup {view_name}: {e}")
            # Default to True to ensure we don't skip a necessary build on error
            return "rebuild"


    # -------------------------
    #  Admin UI support — read-only inventory + on-demand full rebuild.
    #  Backs the "Timescale DB -> Rebuild Rollup Views" admin screen, the
    #  same way BridgeAdminManager backs "Delete Columns". Both entry
    #  points below are thin orchestrators over methods that already exist
    #  (rollup_needs_rebuild, setup_with_retry/ensure_rollups, add_wide_rollup)
    #  rather than new rebuild logic, so a manual rebuild behaves identically
    #  to the automatic one that already runs on connect/reconnect.
    # -------------------------

    def list_rollup_views(self) -> list[dict[str, Any]]:
        """
        Read-only inventory of every rollup view this bridge manages or
        should manage: the shared narrow stack (hourly/daily/weekly/monthly
        on device_metrics_narrow) plus each wide-table protocol's own
        hourly/daily/weekly/monthly stack. Never creates or drops anything.

        Each row's `status` comes straight from rollup_needs_rebuild():
          - "OK"      view exists and its bucket matches current config
          - "rebuild" view exists but its bucket no longer matches config
          - "missing" view does not exist yet

        Used to populate the Rebuild Rollup Views screen before the admin
        presses "Rebuild Rollups" -- see WebServer.services.bridge_service
        .list_rollup_views and routers/timescale.py's GET /rollups.
        """
        granularities: list[tuple[str, str]] = [
            ("hourly", self.hourly_rollup_bucket),
            ("daily", self.daily_rollup_bucket),
            ("weekly", self.weekly_rollup_bucket),
            ("monthly", self.monthly_rollup_bucket),
        ]

        rows_out: list[dict[str, Any]] = []

        with self.SessionFactory() as session:
            # Shared narrow stack -- fixed view names, not tied to a protocol.
            narrow_segments: dict[str, str] = {
                "hourly": "hourly_rollup_narrow",
                "daily": "daily_rollup_narrow",
                "weekly": "weekly_rollup_narrow",
                "monthly": "monthly_rollup_narrow",
            }
            for gran, bucket in granularities:
                view_name: str = narrow_segments[gran]
                rows_out.append({
                    "protocol_name": "shared_narrow",
                    "wide_table_name": "device_metrics_narrow",
                    "view_name": view_name,
                    "granularity": gran,
                    "bucket_interval": bucket,
                    "status": self.rollup_needs_rebuild(session, view_name, bucket),
                })

            # One 4-view stack per wide-table protocol. rollup_prefix comes
            # from protocol_registry (see timescaledb._register_protocol_
            # schema) and follows the same "rollup_wide__<suffix>" naming
            # _ensure_cagg_views_for_protocol derives from wide_table_name --
            # the two must stay in sync.
            protocol_rows: Sequence[Row[Any]] = session.execute(
                text("""
                    SELECT protocol_name, rollup_prefix, wide_table_name
                    FROM protocol_registry
                    WHERE rollup_enabled = true AND wide_table_name IS NOT NULL
                    ORDER BY protocol_name
                """)
            ).fetchall()

            for protocol_name, rollup_prefix, wide_table_name in protocol_rows:
                if not rollup_prefix:
                    # Registered but rollup setup hasn't run yet (rollup_prefix
                    # is set alongside wide_table_name at registration time --
                    # see timescaledb._register_protocol_schema) -- nothing to
                    # report for this protocol until that happens.
                    continue
                for gran, bucket in granularities:
                    view_name = f"{gran}_{rollup_prefix}"
                    rows_out.append({
                        "protocol_name": protocol_name,
                        "wide_table_name": wide_table_name,
                        "view_name": view_name,
                        "granularity": gran,
                        "bucket_interval": bucket,
                        "status": self.rollup_needs_rebuild(session, view_name, bucket),
                    })

        return rows_out

    def rebuild_all_rollups(
        self, protocol_names: set[str] | None = None, force: bool = False
    ) -> Generator[dict[str, Any], None, None]:
        """
        Runs a rebuild pass across the rollup stacks this bridge manages, for
        the admin's "Rebuild Rollups" / "Force Rebuild" buttons.

        This is a GENERATOR, not a plain method -- mirrors rebuild_compression's
        streaming shape below, so routers/timescale.py's PATCH /rollups/rebuild
        can stream progress to the browser the same way PATCH /compression/
        rebuild already does (see triggerRebuildRollups in
        timescale_rebuild_rollups.html). The granularity is coarser than
        compression's per-chunk events, though: setup_with_retry() and
        add_wide_rollup() each rebuild their whole stack as one internal call
        this class doesn't get to peek inside mid-flight, so the finest
        progress available here is "one whole group (the shared narrow stack,
        or one wide-table protocol) just finished" -- there's no cheaper way
        to get a partial view within a single group without instrumenting
        those two methods directly, which is out of scope for what's just a
        progress indicator.

        Yields:
            {"type": "progress", "group", "groups_processed", "groups_total"}
             once per selected group as it finishes (success or failure).
             groups_total is fixed up front at the number of selected groups
             (the shared narrow stack counts as one, same as any one
             protocol) -- deliberately NOT weighted by each group's view
             count the way rebuild_compression weights by byte size, since
             every group here is a single opaque unit of work rather than a
             set of independently-measurable chunks; weighting by view count
             would imply a granularity of progress this method can't
             actually observe.
            {"type": "done", "result": {...}} exactly once, at the end -- the
             same "narrow" + "protocols" + "force" shape this method used to
             simply return, unchanged.

        Args:
            protocol_names: Which rollup groups to act on, keyed the same
                way list_rollup_views() groups its rows -- a wide-table
                protocol_name, plus the literal "shared_narrow" for the
                shared narrow stack. None (the default) acts on every group,
                matching the startup/reconnect behavior this method mirrors.
                An empty set does nothing -- every group comes back in the
                result with skipped=True.

                Selection only ever happens at this per-source-table
                granularity, never per individual hourly/daily/weekly/
                monthly view: the finer-grained views in a stack are built
                hierarchically on top of the coarser ones (see
                _ensure_cagg_views_for_protocol's view_specs chain), so
                rebuilding one view in isolation while leaving the rest of
                its own stack untouched would risk building it against a
                stale or mismatched source. The whole stack is the smallest
                unit that can be safely rebuilt on its own.

            force: "Rebuild Rollups" (False) vs "Force Rebuild" (True).
                False only purges + re-materializes a selected group if
                rollup_needs_rebuild() actually finds a mismatch or missing
                view -- a group that already matches its configured buckets
                is left untouched, just re-verified, which is normally very
                fast. True skips that check for every selected group and
                always purges + fully re-materializes them, regardless of
                whether anything looked wrong.

        Delegates to the same entry points startup/reconnect already use
        (setup_with_retry for the shared narrow stack, add_wide_rollup per
        wide-table protocol) instead of duplicating their purge-then-recreate
        logic -- see ensure_rollups() and _ensure_cagg_views_for_protocol().

        Never raises: each selected group is attempted independently and its
        outcome recorded, so one broken wide table doesn't block fixing
        everyone else's rollups. Callers should surface any ok=False entries
        to the admin, and use each group's `changed` flag (computed from a
        pre-rebuild snapshot, not from force alone) to tell "verified, already
        correct" apart from "actually rebuilt" when reporting results.
        """
        # Snapshot status before doing anything, so the result can report
        # whether a selected group actually needed work -- independent of
        # `force`, which forces the purge+recreate regardless of status.
        pre_views: list[dict[str, Any]] = self.list_rollup_views()
        narrow_pre_ok: bool = all(
            v["status"] == "OK" for v in pre_views if v["protocol_name"] == "shared_narrow"
        )
        protocol_pre_ok: dict[str, bool] = {}
        for v in pre_views:
            if v["protocol_name"] == "shared_narrow":
                continue
            protocol_pre_ok.setdefault(v["protocol_name"], True)
            if v["status"] != "OK":
                protocol_pre_ok[v["protocol_name"]] = False

        rebuild_narrow: bool = protocol_names is None or "shared_narrow" in protocol_names

        with self.SessionFactory() as session:
            protocol_rows: Sequence[Row[Any]] = session.execute(
                text("""
                    SELECT protocol_name, wide_table_name
                    FROM protocol_registry
                    WHERE rollup_enabled = true AND wide_table_name IS NOT NULL
                    ORDER BY protocol_name
                """)
            ).fetchall()

        # groups_total is fixed before any work starts (narrow, plus every
        # SELECTED protocol row) so the very first progress event already
        # carries a correct denominator -- same reasoning as bytes_total in
        # rebuild_compression, just counted in groups instead of bytes.
        selected_protocol_count: int = sum(
            1 for protocol_name, _ in protocol_rows
            if protocol_names is None or protocol_name in protocol_names
        )
        groups_total: int = (1 if rebuild_narrow else 0) + selected_protocol_count
        groups_processed: int = 0

        narrow_ok: bool = True
        narrow_error: str | None = None
        narrow_changed: bool = False
        if rebuild_narrow:
            narrow_changed = force or not narrow_pre_ok
            try:
                self.setup_with_retry(force=force)
            except Exception as e:
                self._log.error(f"rebuild_all_rollups: shared narrow rollup rebuild failed: {e}")
                narrow_ok = False
                narrow_error = str(e)
            groups_processed += 1
            yield {
                "type": "progress", "group": "shared_narrow",
                "groups_processed": groups_processed, "groups_total": groups_total,
            }
        else:
            self._log.debug("rebuild_all_rollups: shared narrow stack not selected -- skipping.")

        protocol_results: list[dict[str, Any]] = []
        for protocol_name, wide_table_name in protocol_rows:
            selected: bool = protocol_names is None or protocol_name in protocol_names
            if not selected:
                protocol_results.append({
                    "protocol_name": protocol_name, "ok": True, "error": None,
                    "skipped": True, "changed": False,
                })
                continue

            changed: bool = force or not protocol_pre_ok.get(protocol_name, True)
            try:
                self.add_wide_rollup(protocol_name, wide_table_name, force=force)
                protocol_results.append({
                    "protocol_name": protocol_name, "ok": True, "error": None,
                    "skipped": False, "changed": changed,
                })
            except Exception as e:
                self._log.error(f"rebuild_all_rollups: rebuild failed for protocol '{protocol_name}': {e}")
                protocol_results.append({
                    "protocol_name": protocol_name, "ok": False, "error": str(e),
                    "skipped": False, "changed": changed,
                })

            groups_processed += 1
            yield {
                "type": "progress", "group": protocol_name,
                "groups_processed": groups_processed, "groups_total": groups_total,
            }

        ok_count: int = sum(1 for p in protocol_results if p["ok"] and not p["skipped"])
        attempted_count: int = sum(1 for p in protocol_results if not p["skipped"])
        self._log.info(
            "rebuild_all_rollups: force=%s narrow selected=%s ok=%s changed=%s; %d/%d selected protocol(s) ok.",
            force, rebuild_narrow, narrow_ok, narrow_changed, ok_count, attempted_count,
        )

        result: dict[str, Any] = {
            "narrow": {
                "ok": narrow_ok, "error": narrow_error,
                "skipped": not rebuild_narrow, "changed": narrow_changed,
            },
            "protocols": protocol_results,
            "force": force,
        }
        yield {"type": "done", "result": result}

    def refresh_selected_rollups(
        self, protocol_names: set[str] | None = None, force_full: bool = False
    ) -> Generator[dict[str, Any], None, None]:
        """
        Refreshes the selected rollup groups' *existing* views in place --
        pulls the latest raw data into them via CALL refresh_continuous_
        aggregate, same as the background refresh policy runs on its own
        schedule. Never drops or recreates a view (unlike rebuild_all_
        rollups()), so it's safe to run far more often and normally
        finishes much faster -- there's no re-materialization of the whole
        historical range unless force_full=True, and even then it's still a
        refresh, not a purge+rebuild.

        This does NOT fix a structural problem (a missing view, or one
        whose bucket interval no longer matches config) -- that's what
        "Rebuild Rollups" / "Force Rebuild" are for. A view that doesn't
        exist yet is skipped here, not created.

        Args:
            protocol_names: Same group keys as rebuild_all_rollups() -- a
                wide-table protocol_name, plus "shared_narrow". None
                refreshes every known view. An empty set refreshes nothing.
            force_full: False performs each view's normal incremental
                refresh (its configured start_offset window, e.g. the last
                3 hours for an hourly rollup). True refreshes the view's
                entire time range from the beginning of time to now --
                still just a refresh of existing data, but touches far more
                history and can take meaningfully longer.

        Never raises: each view is refreshed independently and its outcome
        recorded, so one bad view doesn't block refreshing the rest.

        This is a GENERATOR, not a plain method -- mirrors rebuild_all_
        rollups() above so routers/timescale.py's PATCH /rollups/refresh
        can stream progress the same way. Unlike that method, this one
        already loops over individual views, so progress here is
        naturally per-view rather than per-group -- the finest
        granularity this screen's thermometer gets anywhere.

        Yields:
            {"type": "progress", "view_name", "protocol_name",
             "views_processed", "views_total"} once per candidate view
             (every known view whose group passes the protocol_names
             filter) as it's handled -- including a view that turns out
             not to exist yet and gets skipped, since that's still one
             candidate "handled" for progress purposes, not work still
             pending. views_total is fixed up front, before the loop
             starts, from that same candidate list.
            {"type": "done", "result": {"views": [...], "force_full": ...}}
             exactly once, at the end -- the same shape this method used
             to simply return, unchanged.
        """
        # Map each known view name to the group key it belongs to, using
        # the same naming rules as list_rollup_views(), so a view's group
        # here always matches what the admin sees (and checks boxes for)
        # on the inventory table.
        view_to_group: dict[str, str] = {
            "hourly_rollup_narrow": "shared_narrow",
            "daily_rollup_narrow": "shared_narrow",
            "weekly_rollup_narrow": "shared_narrow",
            "monthly_rollup_narrow": "shared_narrow",
        }

        with self.SessionFactory() as session:
            protocol_rows: Sequence[Row[Any]] = session.execute(
                text("""
                    SELECT protocol_name, rollup_prefix
                    FROM protocol_registry
                    WHERE rollup_enabled = true AND wide_table_name IS NOT NULL
                """)
            ).fetchall()

        for protocol_name, rollup_prefix in protocol_rows:
            if not rollup_prefix:
                continue
            for gran in ("hourly", "daily", "weekly", "monthly"):
                view_to_group[f"{gran}_{rollup_prefix}"] = protocol_name

        # Resolve the candidate list (and therefore views_total) up front,
        # before touching anything, same reasoning as bytes_total in
        # rebuild_compression -- the first progress event should already
        # know a correct denominator.
        candidates: list[tuple[str, str, Any]] = []
        for view_name, start_offset in self.known_rollup_views.items():
            group: str | None = view_to_group.get(view_name)
            if group is None:
                # A view we're tracking that no longer maps to a registered
                # protocol/the narrow stack -- shouldn't normally happen,
                # but skip it rather than guess which group it belongs to.
                continue
            if protocol_names is not None and group not in protocol_names:
                continue
            candidates.append((view_name, group, start_offset))

        views_total: int = len(candidates)
        views_processed: int = 0

        view_results: list[dict[str, Any]] = []
        for view_name, group, start_offset in candidates:
            try:
                with self.SessionFactory() as session:
                    if not self._view_exists_helper(session, view_name):
                        self._log.warning(f"refresh_selected_rollups: skipping '{view_name}' -- does not exist.")
                        views_processed += 1
                        yield {
                            "type": "progress", "view_name": view_name, "protocol_name": group,
                            "views_processed": views_processed, "views_total": views_total,
                        }
                        continue
                self._refresh_single_rollup_helper(view_name, start_offset, force_full)
                view_results.append({
                    "view_name": view_name, "protocol_name": group, "ok": True, "error": None,
                })
            except Exception as e:
                self._log.error(f"refresh_selected_rollups: refresh failed for '{view_name}': {e}")
                view_results.append({
                    "view_name": view_name, "protocol_name": group, "ok": False, "error": str(e),
                })

            views_processed += 1
            yield {
                "type": "progress", "view_name": view_name, "protocol_name": group,
                "views_processed": views_processed, "views_total": views_total,
            }

        ok_count: int = sum(1 for r in view_results if r["ok"])
        self._log.info(
            "refresh_selected_rollups: force_full=%s refreshed %d/%d view(s) ok.",
            force_full, ok_count, len(view_results),
        )

        yield {"type": "done", "result": {"views": view_results, "force_full": force_full}}

    # -------------------------
    #  Bridge info pane — read-only snapshots for the "Compression &
    #  Retention Status" and "Background Job Status" panels (see
    #  routers/timescale.py GET /compression-retention and GET /jobs).
    #  Both are purely observational: nothing here creates, drops, or
    #  modifies a policy or job.
    # -------------------------

    # -------------------------
    #  Dynamic ROLLUP VIEW chunk/compression sizing — a further extension
    #  of the raw-table dynamic sizing above, for the four hourly/daily/
    #  weekly/monthly views (narrow's shared ones AND each wide table's
    #  own set). This uses a DIFFERENT formula than the raw-table one,
    #  not the same one reapplied: a rollup view holds exactly one row per
    #  (device, metric) series per time bucket, so its row rate is driven
    #  by CARDINALITY (how many distinct series exist) x how many buckets
    #  that granularity produces per day -- NOT by scrape frequency, which
    #  aggregation already normalizes away entirely (an hourly view
    #  produces 24 rows/day per series whether the raw data was scraped
    #  every 15 seconds or every 5 minutes).
    #
    #  Also fixes a real, separate pre-existing bug found while building
    #  this: hourly_chunk_time_interval and its three siblings were
    #  defined and loaded in __init__, referenced in the view-creation
    #  orchestrators' config tuples, and then silently discarded at every
    #  unpacking site (`for ..., _ in view_configs`) -- never once applied
    #  to any view's own underlying materialization hypertable. Every
    #  rollup view has been running on TimescaleDB's own implicit default
    #  chunk sizing this whole time, regardless of this setting. See
    #  _add_aggregate_policy_helper, which now actually applies it (and
    #  the dynamically-computed replacement below) via set_chunk_time_
    #  interval(), the same mechanism already used for the raw tables.
    # -------------------------

    # Bucket-per-day constants used to convert a view's cardinality into a
    # modeled rows/day figure, comparable across granularities.
    _GRANULARITY_BUCKETS_PER_DAY: dict[str, float] = {
        "hourly": 24.0,
        "daily": 1.0,
        "weekly": 1.0 / 7.0,
        "monthly": 1.0 / 30.0,   # 30-day month approximation, matches this module's own "N months" convention elsewhere
    }

    # Unlike the raw-table bands (independent absolute presets per band),
    # views scale each GRANULARITY'S OWN static baseline by a factor --
    # a view's natural time-scale already varies enormously by granularity
    # (hours vs. months), so "shrink/grow this granularity's own baseline"
    # preserves that relative ordering at every band; one shared absolute
    # preset table (as used for the raw tables) would not. Band edges are
    # in modeled rows/day (cardinality x buckets/day for that view's
    # granularity) -- deliberately much smaller than the raw-table bands,
    # since aggregation inherently produces far fewer rows/day than the
    # raw scrape stream does. For most small/medium deployments, every
    # view will land in Group 1 (i.e. exactly the existing static
    # defaults) -- that's expected, not a sign dynamic sizing "isn't
    # doing anything": view row-rates only become chunk-sizing-relevant
    # at genuinely large device/metric fleets.
    _VIEW_LOAD_BANDS: list[tuple[float | None, float, str]] = [
        # (upper bound modeled rows/day, scale factor on this granularity's own baseline, band_name)
        (10_000.0,   1.0,    "Group 1 (light)"),
        (50_000.0,   0.5,    "Group 2"),
        (200_000.0,  0.25,   "Group 3"),
        (800_000.0,  0.125,  "Group 4"),
        (None,       0.0625, "Group 5 (heavy)"),
    ]

    def get_dynamic_view_overview(self) -> list[dict[str, Any]]:
        """
        Read-only, per-VIEW snapshot of the values get_dynamic_view_
        settings_helper would currently resolve — narrow's four rollup
        views (hourly/daily/weekly/monthly_rollup_narrow) plus every wide
        table's own four — for the Compression & Retention Status panel.

        Unlike get_dynamic_raw_table_overview (one row per raw table, four
        possible granularities collapsed into a single load score), this
        is one row per (table, granularity) pair, since a view's load
        score is granularity-specific (see get_dynamic_view_settings_
        helper's docstring: cardinality x that granularity's buckets/day)
        -- an hourly view and a monthly view backed by the exact same
        cardinality can land in very different bands.

        Purely observational: computing this never changes any policy or
        creates/drops anything, unlike calling get_dynamic_view_settings_
        helper from _add_aggregate_policy_helper (which this reuses
        read-only).
        """
        granularities: list[str] = ["hourly", "daily", "weekly", "monthly"]
        rows: list[dict[str, Any]] = []

        narrow_cardinality: int = self._compute_view_cardinality_helper(None)
        for gran in granularities:
            static_chunk: str = getattr(self, f"{gran}_chunk_time_interval")
            static_compress: str = getattr(self, f"{gran}_compress_after_interval")
            chunk, compress, band = self.get_dynamic_view_settings_helper(
                gran, target_protocol_name=None,
                static_chunk_interval=static_chunk,
                static_compress_after=static_compress,
            )
            rows.append({
                "view_name": f"{gran}_rollup_narrow",
                "protocol_name": "shared_narrow",
                "granularity": gran,
                "load_score": narrow_cardinality * self._GRANULARITY_BUCKETS_PER_DAY.get(gran, 1.0),
                "band_name": band,
                "chunk_time_interval": chunk,
                "compress_after_interval": compress,
            })

        try:
            with self.SessionFactory() as session:
                protocol_rows: Sequence[Row[Any]] = session.execute(
                    text("""
                        SELECT protocol_name, rollup_prefix
                        FROM protocol_registry
                        WHERE wide_table_name IS NOT NULL
                        ORDER BY protocol_name
                    """)
                ).fetchall()
        except SQLAlchemyError as e:
            self._log.error(f"get_dynamic_view_overview: protocol_registry query failed: {e}")
            protocol_rows = []

        for protocol_name, rollup_prefix in protocol_rows:
            if not rollup_prefix:
                continue
            protocol_cardinality: int = self._compute_view_cardinality_helper(protocol_name)
            for gran in granularities:
                static_chunk = getattr(self, f"{gran}_chunk_time_interval")
                static_compress = getattr(self, f"{gran}_compress_after_interval")
                chunk, compress, band = self.get_dynamic_view_settings_helper(
                    gran, target_protocol_name=protocol_name,
                    static_chunk_interval=static_chunk,
                    static_compress_after=static_compress,
                )
                rows.append({
                    "view_name": f"{gran}_{rollup_prefix}",
                    "protocol_name": protocol_name,
                    "granularity": gran,
                    "load_score": protocol_cardinality * self._GRANULARITY_BUCKETS_PER_DAY.get(gran, 1.0),
                    "band_name": band,
                    "chunk_time_interval": chunk,
                    "compress_after_interval": compress,
                })

        return rows


    # -------------------------
    #  Determine wide vs narrow table usage for resource settings
    # -------------------------
    def _compute_view_cardinality_helper(self, target_protocol_name: str | None) -> int:
        """
        Computes the number of distinct (device, metric) series feeding
        one rollup-view stack: narrow's shared views (target_protocol_
        name=None, summed across every protocol currently tracked) or one
        wide table's own views (target_protocol_name=<protocol>, that
        protocol only).

        Summed PER TRANSPORT (per physical device) from device_info.
        metric_count, NOT protocol_registry.metric_count x device_count.
        The latter assumes every device of a protocol scrapes the same
        metric set, which doesn't hold in practice -- one battery might
        be configured with the full register list, another with only a
        curated few, and cardinality needs to reflect each device's own
        actual series count, not a uniform per-protocol figure applied to
        however many devices exist.

        Returns 0 if there's no live tracking data for the relevant
        protocol(s) yet.
        """
        relevant_transports: list[str] = [
            transport_name
            for transport_name, (protocol_name, _read_interval) in self.hypertable_mgr.transport_read_intervals.items()
            if target_protocol_name is None or protocol_name == target_protocol_name
        ]
        if not relevant_transports:
            return 0

        try:
            with self.SessionFactory() as session:
                total: int | None = session.execute(
                    text("SELECT COALESCE(SUM(metric_count), 0) FROM device_info WHERE transport = ANY(:transports)"),
                    {"transports": relevant_transports},
                ).scalar()
        except SQLAlchemyError as e:
            self._log.warning(f"_compute_view_cardinality_helper: device_info lookup failed: {e}")
            return 0

        return int(total or 0)

    def _classify_view_band_helper(self, rows_per_day: float) -> tuple[float, str]:
        """
        Maps a view's modeled rows/day to a (scale_factor, band_name) from
        _VIEW_LOAD_BANDS, checked in ascending order.
        """
        for upper_bound, scale_factor, band_name in self._VIEW_LOAD_BANDS:
            if upper_bound is None or rows_per_day <= upper_bound:
                return scale_factor, band_name
        return self._VIEW_LOAD_BANDS[-1][1], self._VIEW_LOAD_BANDS[-1][2]

    def get_dynamic_view_settings_helper(
        self, granularity: str, target_protocol_name: str | None,
        static_chunk_interval: str, static_compress_after: str,
    ) -> tuple[str, str, str]:
        """
        Resolves (chunk_time_interval, compress_after_interval, band_name)
        for ONE rollup view -- narrow's (target_protocol_name=None) or one
        wide table's (target_protocol_name=<protocol>) view at the given
        granularity ("hourly"/"daily"/"weekly"/"monthly").

        Falls back to (static_chunk_interval, static_compress_after, ...)
        when enable_dynamic_chunk_sizing is False, when there's no live
        cardinality data yet, or when the static baseline itself doesn't
        parse (a safety net, not an expected path) -- same reasoning as
        get_dynamic_raw_table_settings_helper.
        """
        if not self.hypertable_mgr.enable_dynamic_chunk_sizing:
            return static_chunk_interval, static_compress_after, "Static (manual)"

        cardinality: int = self._compute_view_cardinality_helper(target_protocol_name)
        if cardinality <= 0:
            return static_chunk_interval, static_compress_after, "Static (no live data yet)"

        buckets_per_day: float = self._GRANULARITY_BUCKETS_PER_DAY.get(granularity, 1.0)
        rows_per_day: float = cardinality * buckets_per_day
        scale_factor, band_name = self._classify_view_band_helper(rows_per_day)

        static_chunk_hours: float = self.hypertable_mgr.parse_interval_to_hours_helper(static_chunk_interval)
        static_compress_hours: float = self.hypertable_mgr.parse_interval_to_hours_helper(static_compress_after)
        if static_chunk_hours <= 0.0 or static_compress_hours <= 0.0:
            return static_chunk_interval, static_compress_after, "Static (unparseable baseline)"

        new_chunk: str = self.hypertable_mgr.format_hours_to_interval_helper(static_chunk_hours * scale_factor)
        new_compress: str = self.hypertable_mgr.format_hours_to_interval_helper(static_compress_hours * scale_factor)
        return new_chunk, new_compress, band_name

    def _derive_target_protocol_from_view_name_helper(self, session: Session, view_name: str, granularity: str) -> str | None:
        """
        Reverse-maps a rollup view name back to the protocol it belongs
        to, purely from the name itself -- None for a shared narrow view
        (e.g. "hourly_rollup_narrow"), else looks up protocol_registry by
        rollup_prefix (e.g. "hourly_rollup_wide__eg4_18kpv" strips its
        granularity prefix down to "rollup_wide__eg4_18kpv", which IS the
        stored rollup_prefix for that protocol).

        This lets _add_aggregate_policy_helper resolve dynamic view sizing
        using only its existing view_name parameter -- no signature changes
        needed on any caller in the view-creation call graph.
        """
        if "narrow" in view_name.lower():
            return None
        rollup_prefix: str = view_name
        if granularity != "default" and view_name.startswith(f"{granularity}_"):
            rollup_prefix = view_name[len(f"{granularity}_"):]
        try:
            result: str | None = session.execute(
                text("SELECT protocol_name FROM protocol_registry WHERE rollup_prefix = :prefix"),
                {"prefix": rollup_prefix},
            ).scalar()

        except SQLAlchemyError as e:
            self._log.warning(f"_derive_target_protocol_from_view_name_helper: lookup failed for '{view_name}': {e}")
            return None
        else:
            return result

    def _view_exists_helper(self, session: Session, view_name: str) -> bool:
        """Check to see if a continuous aggregate exists in the catalog.
        Parameters:
            session: Active database session for executing SQL commands.
            view_name: The name of the continuous aggregate view to check for existence.
        Returns:
            bool: True if the view exists, False otherwise.
        """
        check_sql: TextClause = text("SELECT 1 FROM timescaledb_information.continuous_aggregates WHERE view_name = :name")

        try:
            # Keep ONLY the risky operation in the try block
            result = session.execute(check_sql, {"name": view_name}).fetchone()
        except Exception as e:
            # Handle the error and clean up
            self._log.error(f"Error checking view existence for {view_name}: {e}")
            session.rollback()
            return False
        else:
            # The 'else' block runs ONLY if the try block succeeded
            session.rollback()  # Resets state for your next AUTOCOMMIT call
            return result is not None

    def repopulate_known_rollup_views(self) -> None:
        """
        Rebuilds known_rollup_views from protocol_registry after a reconnect.
        Queries all registered protocols and reconstructs view name -> start_offset
        mappings in the correct hierarchical order (hourly -> daily -> weekly -> monthly).
        """
        granularities: list[tuple[str, str]] = [
            ("hourly",  self.hourly_rollup_start),
            ("daily",   self.daily_rollup_start),
            ("weekly",  self.weekly_rollup_start),
            ("monthly", self.monthly_rollup_start),
        ]

        self.known_rollup_views.clear()

        # Always add the shared narrow views first
        for gran, start_offset in granularities:
            view_name = f"{gran}_rollup_narrow"
            self.known_rollup_views[view_name] = start_offset

        # Then per-protocol wide views
        try:
            with self.SessionFactory() as session:
                rows: Sequence[Row[Any]] = session.execute(
                    text("""
                        SELECT rollup_prefix
                        FROM protocol_registry
                        WHERE rollup_enabled = true
                        AND rollup_prefix IS NOT NULL
                        ORDER BY created_at ASC
                    """)
                ).fetchall()

            for (rollup_prefix,) in rows:
                for gran, start_offset in granularities:
                    view_name: str = f"{gran}_{rollup_prefix}"
                    self.known_rollup_views[view_name] = start_offset

            self._log.info(
                f"Repopulated {len(self.known_rollup_views)} "
                f"rollup views after reconnect."
            )
        except SQLAlchemyError as e:
            self._log.error(f"repopulate_known_rollup_views failed: {e}")
            raise

    def _view_exists_conn_helper(self, conn: Connection, view_name: str) -> bool:
        """Session-free variant of _view_exists for use inside autocommit engine.connect() blocks."""
        try:
            result: Row[Any] | None = conn.execute(
                text("SELECT 1 FROM timescaledb_information.continuous_aggregates WHERE view_name = :name"),
                {"name": view_name}
            ).fetchone()
        except Exception as e:
            self._log.error(f"_view_exists_conn error for {view_name} : {e}")
            return False
        else:
            return result is not None


    def _drop_all_continuous_aggregates(self, session: Session) -> None:
        """
        Teardown all rollups in correct dependency order (Top-Down) to
        fix the 'DependentObjectsStillExist' error.
        Process:
        1. Fetch all existing continuous aggregates from the TSDB catalog.
        2. Determine the drop order based on naming conventions (Weekly -> Daily -> Hourly).
        3. For each view:
            1) Set a short lock timeout to fail fast if blocked by flush thread,
            2) Disable compression to avoid issues with compressed CAGGs,
            3) Remove policies to prevent orphaned jobs,
            4) Acquire an exclusive lock on the view to ensure no concurrent access,
            5) Drop the view with CASCADE to clean up dependencies,
            6) Commit after each drop to release locks and allow the next drop to proceed.
        """
        r_settings: dict[str, Any] = self.hypertable_mgr.get_dynamic_settings_helper()
        try:
            # 1. Fetch current aggregates
            result: Result[Any] = session.execute(text("""
                SELECT view_schema, view_name
                FROM timescaledb_information.continuous_aggregates;
            """))
            views: Sequence[Row[Any]] = result.fetchall()

            if not views:
                self._log.info("No continuous aggregates found to drop.")
                return

            # 2. Define Priority: Child views (Weekly) MUST be dropped before Parent views (Daily)
            # This prevents internal _partial_view dependencies from blocking the drop.
            priority_map: dict[str, int] = {"monthly": 4, "weekly": 3, "daily": 2, "hourly": 1}

            def get_drop_rank(v_tuple: Row[Any]) -> int:
                name_lower: str = v_tuple[1].lower()
                for key, val in priority_map.items():
                    if key in name_lower:
                        return val
                return 0

            # Sort descending: 4 (Weekly) drops first, 1 (Hourly) drops last.
            sorted_views: List[Row[Any]] = sorted(views, key=get_drop_rank, reverse=True)

            # 3. Iterate and drop each view safely
            for schema, name in sorted_views:
                full_name: str = f'"{schema}"."{name}"'
                self._log.info(f"Purging rollup: {full_name}")

                # 3b. Fail-fast if locked by background flush or refresh jobs
                self.hypertable_mgr.set_lock_timeout(session, r_settings['lock_timeout'])

                # 4. Disable Compression (Mandatory for a clean drop of compressed CAGGs)
                try:
                    session.execute(text(f"ALTER MATERIALIZED VIEW {full_name} SET (timescaledb.compress = false);"))
                except Exception:
                    self._log.info(f"View was already uncompressed: {full_name}")
                    pass # Already uncompressed or doesn't support it

                # 5. Remove policies first
                session.execute(text(f"SELECT remove_continuous_aggregate_policy('{full_name}', if_exists => true);"))
                session.execute(text(f"SELECT remove_retention_policy('{full_name}', if_exists => true);"))

                # 6. Acquire Exclusive Lock & Drop
                session.execute(text(f"LOCK TABLE {full_name} IN ACCESS EXCLUSIVE MODE;"))
                session.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {full_name} CASCADE;"))

                # 7. Critical: Commit after each view
                # This releases internal metadata locks and allows the next DROP in the stack to succeed.
                session.commit()
                self._log.info(f"Successfully purged {full_name}")

            self._log.info("Full rollup stack teardown complete.")
            # Clear the refresh registry — views are dropped, _ensure_single_cagg_view
            # will re-populate it during the creation phase.
            self.known_rollup_views.clear()

        except Exception as e:
            session.rollback()
            self._log.error(f"Failed to purge rollup stack: {e}")
            raise

    # 10e

    def refresh_rollups(self, force_full: bool = False) -> None:
        """
        Refreshes rollups in Bottom-Up order (Hourly -> Daily -> Weekly -> Monthly).
        This ensures parent views have data available from their child sources.
            Params: force_full (bool): If True, refreshes the entire range of data.
            This is useful for scenarios where incremental refreshes may not be sufficient,
            such as after a bucket interval change or if there are concerns about data consistency.
            When set to False, the method performs an incremental refresh based on the predefined
            start offsets for each rollup, which is more efficient for regular maintenance.
            The method includes robust error handling and logging to track the progress and
            duration of each refresh operation, as well as a watchdog mechanism to monitor
            long-running refreshes and send alerts if they become blocked by locks.
        """
        with self.SessionFactory() as session:
            with self._reconnect_lock:
                tsdb_connected: bool = self.tsdb_connected

            if not tsdb_connected or not session:
                self._log.error("Cannot refresh rollups — not tsdb_connected.")
                return

            try:
                # 1. Define the refresh sequence: Smallest Grain -> Largest Grain
                # Monthly is independent (from hypertable), so it can go anywhere.
                for view_name, start_offset in self.known_rollup_views.items():
                    if self._view_exists_helper(session, view_name):
                        self._refresh_single_rollup_helper(view_name, start_offset, force_full)
                    else:
                        self._log.warning(f"Skipping refresh: '{view_name}' does not exist.")
            except Exception as e:
                self._log.error(f"Rollup refresh failed: {e}")

    def _refresh_single_rollup_helper(self, view_name: str, start_offset: str, force_full: bool = False) -> None:
        """
        Refreshes a rollup with duration tracking and performance logging.
        The watchdog monitors the refresh process and sends alerts if it detects that the refresh is blocked
        by locks for an extended period.
        parameters:
            session: Active database session for executing SQL commands.
            view_name: The name of the continuous aggregate view to refresh.
            start_offset: The offset for incremental refresh (e.g., "3 hours", "3 days").
            force_full: If True, performs a full refresh from the beginning of time to now. If
            False, performs an incremental refresh based on the start_offset.
        """
        r_settings: dict[str, Any] = self.hypertable_mgr.get_dynamic_settings_helper()

        stop_signal: List[bool] = self._start_refresh_watchdog_helper(view_name)

        start_time: float = time.perf_counter()
        mode: Literal['FULL'] | Literal['INCREMENTAL'] = "FULL" if force_full else "INCREMENTAL"

        self._log.info(f"Starting {mode} refresh for {view_name}...")

        # AUTOCOMMIT is mandatory for CALL refresh_continuous_aggregate
        with self.engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            conn.execute(text(f"SET work_mem = '{r_settings['work_mem']}';"))
            try:
                if force_full:
                    # Refresh from the beginning of time to now
                    conn.execute(text(f"CALL refresh_continuous_aggregate('{view_name}', NULL, now());"))
                else:
                    # Incremental refresh
                    conn.execute(text(f"""
                        CALL refresh_continuous_aggregate(
                            '{view_name}',
                            now() - INTERVAL '{start_offset}',
                            now()
                        );
                    """))

                end_time: float = time.perf_counter()
                duration_seconds: float = end_time - start_time

                # Log the duration in a scannable format
                self._log.info(
                    f"{mode} refresh COMPLETED for {view_name}. "
                    f"Duration: {duration_seconds:.2f}s "
                    f"({int(duration_seconds // 60)}m {int(duration_seconds % 60)}s)"
                )

            except Exception as e:
                self._log.error(f"{mode} refresh FAILED for {view_name} after {time.perf_counter() - start_time:.2f}s: {e}")
                raise # Re-raise to let the parent caller decide if it should continue

            finally:
                # Ensure the watchdog thread stops
                stop_signal[0] = True

    # watchdog refresh management
    def _stop_existing_watchdog_helper(self) -> None:
        """Signals the existing watchdog to exit immediately.
        The watchdog thread checks the signal at short intervals and will terminate if the signal is set to True.
        """
        if hasattr(self, '_current_watchdog_signal') and self._current_watchdog_signal:
            self._current_watchdog_signal[0] = True
            self._current_watchdog_signal = None

    # watchdog thread to monitor long-running refreshes
    def _start_refresh_watchdog_helper(self, view_name: str) -> List[bool]:
        """
        This watchdog runs in a separate thread to monitor the progress of a continuous aggregate refresh.
        It periodically checks the pg_stat_activity for the refresh query and sends an MPG alert
        if it detects that the refresh is blocked by locks for an extended period (e.g., 30 seconds).
        Parameters:
            view_name (str): The name of the continuous aggregate view being refreshed, used for monitoring and alerting purposes.

        Returns:
            List[bool]: A mutable list containing a single boolean value that serves as a stop signal for the watchdog thread.
        """
        # 1. Kill any existing watchdog before starting a new one
        self._stop_existing_watchdog_helper()

        # 2. Create the new stop signal
        stop_signal: List[bool] = [False]
        self._current_watchdog_signal: List[bool] | None = stop_signal

        def monitor() -> None:
            # Use a short-lived session specifically for monitoring
            with self.SessionFactory() as session:
                # Check both the signal and the DB state
                while not stop_signal[0]:
                    try:
                        # We check if the refresh is still running
                        sql: TextClause = text("""
                            SELECT wait_event_type FROM pg_stat_activity
                            WHERE query LIKE :pattern
                            AND state != 'idle'
                            AND pid != pg_backend_pid()
                        """)
                        res: Row[Any] | None = session.execute(sql, {"pattern": f"%refresh_continuous_aggregate%'{view_name}'%"}).fetchone()

                        # If the query is gone from pg_stat_activity, the refresh is done
                        if not res:
                            break

                        if res.wait_event_type == Lock:
                            msg: str = (
                                f"⚠️ {view_name} is BLOCKED by a lock.\n"
                                f"The refresh for {view_name} has been running for over 30 seconds and is "
                                f"currently blocked by a lock. Please investigate the database locks to ensure "
                                f"the refresh can complete successfully."
                            )

                            self.send_message(message=msg, title="MPG Blocked View Alert", priority=1)


                        # Sleep in small increments to remain responsive to the stop_signal
                        for _ in range(30):
                            if stop_signal[0]:
                                return
                            time.sleep(1)

                    except Exception:
                        break

        t = threading.Thread(target=monitor, name=f"Watchdog_{view_name}", daemon=True)
        t.start()
        return stop_signal

    def _refresh_rollup_loop_helper(self) -> None:
        """
        Background loop: Replays backlog, then refreshes hierarchical aggregates.
        Uses @property tsdb_connected for live state awareness.
        The loop includes multiple safeguards:
            1. Live Connection Check: Before each refresh cycle, it checks if the database connection is healthy.
                If not, it skips the refresh and waits before retrying.
            2. Migration Gatekeeper: If a schema migration is currently in progress, it pauses the refresh loop to avoid conflicts.
            3. Backlog Drain: Before refreshing, it ensures that any backlogged data is replayed to the hypertable,
                so that the continuous aggregates include the most recent data.
            4. Sequential Hierarchical Refresh: It refreshes the continuous aggregates in the correct
                order (Hourly -> Daily -> Weekly -> Monthly) to ensure that parent views have their child data available.
            5. Dynamic Sleep: The loop uses the auto_refresh_interval from the policy to determine how long to wait
                between refresh cycles, allowing for dynamic adjustment of refresh frequency based on performance needs.

        """
        self._log.info("Background rollup refresh loop started.")

        while not self._stop_refresh_rollup_event.is_set():
            try:
                # 1. Live Connection Check
                # Uses the @property to peek into the shared rollup_policy dict
                with self._reconnect_lock:
                    tsdb_connected: bool = self.tsdb_connected

                if not tsdb_connected:
                    self._log.debug("Refresh skipped: Database not connected.")
                    # Wait 60s or until thread stop event is set
                    self._stop_refresh_rollup_event.wait(timeout=60)
                    continue

                # 2. Migration Gatekeeper
                # Pause if a schema migration (rebuild) is currently running
                if self.migration_in_progress.is_set():
                    self._log.debug("Refresh skipped: Migration in progress.")
                    self._stop_refresh_rollup_event.wait(timeout=30)
                    continue

                # 3. Drain Backlog Before Refresh
                # This ensures late/backlogged data is in the hypertable
                # so the CAGG refresh includes it.
                with self._backlog_lock:
                    count: int = self.backlog.replay_to_queue()
                    if count > 0:
                        self._log.debug(f"Replayed {count} points. Waiting for flush...")
                        # Wait for flush worker to finish writing replayed points
                        self._flush_queue.join()

                # 4. Sequential Hierarchical Refresh
                # Order: Hourly -> Daily -> Weekly -> Monthly
                with self.engine.connect() as conn:
                    conn: Connection = conn.execution_options(isolation_level="AUTOCOMMIT")
                    # Apply dynamic session settings from performance tiers
                    tier: dict[str, Any] = self.hypertable_mgr.get_dynamic_settings_helper()
                    conn.execute(text(f"SET work_mem = '{tier['work_mem']}';"))
                    conn.execute(text(f"SET lock_timeout = '{tier['lock_timeout']}';"))

                    # Refresh all registered CAGG views in insertion order
                    # (hourly -> daily -> weekly -> monthly, narrow before wide per protocol).
                    for view_name in list(self.known_rollup_views):
                        if not self._view_exists_conn_helper(conn, view_name):
                            self._log.warning(f"Skipping refresh: '{view_name}' not found.")
                            continue
                        self._log.debug(f"Refreshing continuous aggregate: {view_name}")
                        conn.execute(
                            text("CALL refresh_continuous_aggregate(:view, NULL, NULL);"),
                            {"view": view_name}
                        )

                # 5. Dynamic Sleep
                # Use the interval from policy (convert seconds to float for wait)
                wait_time = float(self.auto_refresh_interval)
                if self._stop_refresh_rollup_event.wait(timeout=wait_time):
                    break

            except Exception as e:
                self._log.error(f"Rollup refresh cycle failed: {e}")
                # Exponential backoff/safety sleep on error
                self._stop_refresh_rollup_event.wait(timeout=300)

        self._log.info("Background rollup refresh loop exiting.")

    def _update_last_refresh_helper(self, protocol: str) -> None:
        """
        Updates last_refresh_at in protocol_registry after a successful
        rollup refresh for the given protocol.
        Called by RollupManager after each refresh cycle completes.
        """
        with self.SessionFactory() as session:
            try:
                with session.begin():
                    session.execute(
                        text("""
                            UPDATE protocol_registry
                            SET last_refresh_at = :now,
                                updated_at = :now
                            WHERE protocol_name = :p
                        """),
                        {"now": _now_tz(), "p": protocol}
                    )
            except Exception as e:
                self._log.error(f"Failed to update last_refresh_at for '{protocol}': {e}")
                # Non-fatal — don't raise, refresh already completed successfully

    def stop_auto_refresh(self) -> None:
        """
        Cleanly stop the auto rollup refresh thread.
        This method signals the background thread to exit and waits for it to finish,
        ensuring that no refresh operations are left in an inconsistent state.
        It also logs the shutdown of the refresh thread for monitoring purposes.
        """
        if hasattr(self, "_stop_refresh_rollup_event"):
            self._stop_refresh_rollup_event.set()
            self._log.info("Auto rollup refresh thread stopped.")

        # Cancel any still-pending dynamic-sizing retune timers (see
        # _start_retune_timer_if_needed). These are daemon timers so they
        # wouldn't block process exit on their own, but a bridge that
        # disconnects/reconnects within one process lifetime should not
        # leave stale timers armed against a RollupManager instance that's
        # being torn down.
        with self.hypertable_mgr.retune_lock:
            pending_timers: list[threading.Timer] = list(self.hypertable_mgr.retune_timer.values())
            self.hypertable_mgr.retune_timer.clear()
        for timer in pending_timers:
            timer.cancel()

    def _purge_ghost_jobs_helper(self, session: Session) -> None:
        """
        Cleans up aberrant TimescaleDB processes using dynamic type-based thresholds.
        This method identifies and purges three types of aberrant jobs:
            1. Orphaned Metadata Jobs: Jobs that reference non-existent continuous aggregates,
                often due to failed refreshes or manual drops without policy cleanup.
            2. Stale Execution Jobs: Jobs that have been in a 'Started' state for longer than
                their expected duration based on their type (e.g., Refresh, Compression, Retention).
            3. Ghost Processes: Jobs that are marked as 'Started' but have no corresponding active backend process,
                indicating they are stuck in a limbo state.

            The method uses a single SQL query with CASE statements to classify jobs into these categories based on
            their application_name, last run status, and timestamps. It then iterates through the identified aberrant jobs,
            logs the issues, and takes appropriate actions such as terminating stale backend processes and deleting
            the jobs to clean up the TimescaleDB environment. Error handling ensures that any issues during the
            purge process are logged and do not disrupt the overall system stability.
        """
        self._log.info("Initiating dynamic ghost & stale job sweep...")

        # Define thresholds based on TimescaleDB common job patterns
        # Refresh: Short (30m), Compression/Retention: Long (6h), Others: Standard (1h)
        """ The backend_pid subquery uses the same application_name LIKE '%job_id%' pattern as the GHOST_PROCESS check, so it's consistent
            with how TimescaleDB names its workers. If TimescaleDB changes that naming convention, both checks break together, which is easier to spot.
            The LIMIT 1 guard is important — a job could theoretically have multiple pg_stat_activity rows if it spawned subprocesses, and
            you only want to terminate the main worker.
            The backend_pid will be NULL for ORPHANED_METADATA and GHOST_PROCESS rows (by definition — ghost processes have no active PID),
            so checking if backend_pid is not None before calling pg_terminate_backend prevents a spurious error on those branches."""

        detect_sql: TextClause = text("""
            WITH job_thresholds AS (
                SELECT
                    j.job_id,
                    j.application_name,
                    js.last_run_started_at,
                    js.last_run_status,
                    CASE
                        WHEN j.application_name LIKE 'Refresh Continuous Aggregate%' THEN INTERVAL '30 minutes'
                        WHEN j.application_name LIKE 'Compression Policy%' THEN INTERVAL '6 hours'
                        WHEN j.application_name LIKE 'Retention Policy%' THEN INTERVAL '2 hours'
                        ELSE INTERVAL '1 hour'
                    END as allowed_duration
                FROM timescaledb_information.jobs j
                LEFT JOIN timescaledb_information.job_stats js ON j.job_id = js.job_id
            ),
            classified AS (
                SELECT
                    t.job_id,
                    t.application_name,
                    CASE
                        WHEN (t.application_name LIKE 'Refresh%' OR t.application_name LIKE 'Retention%')
                            AND NOT EXISTS (
                                SELECT 1 FROM timescaledb_information.continuous_aggregates c
                                JOIN timescaledb_information.jobs j2 ON t.job_id = j2.job_id
                                WHERE j2.hypertable_name = c.view_name
                            ) THEN 'ORPHANED_METADATA'
                        WHEN t.last_run_status = 'Started' AND t.last_run_started_at < now() - t.allowed_duration
                            THEN 'STALE_EXECUTION'
                        WHEN t.last_run_status = 'Started'
                            AND NOT EXISTS (
                                SELECT 1 FROM pg_stat_activity p
                                WHERE p.application_name LIKE '%' || t.job_id || '%'
                            ) THEN 'GHOST_PROCESS'
                        ELSE NULL
                    END as aberrancy_type,
                    (
                        SELECT p.pid
                        FROM pg_stat_activity p
                        WHERE p.application_name LIKE '%' || t.job_id || '%'
                        LIMIT 1
                    ) as backend_pid
                FROM job_thresholds t
            )
            SELECT job_id, application_name, aberrancy_type, backend_pid
            FROM classified
            WHERE aberrancy_type IS NOT NULL
        """)

        try:
            ghosts: Sequence[Row[Any]] = session.execute(detect_sql).fetchall()

            if not ghosts:
                self._log.info("All background workers healthy.")
                return

            for job_id, app_name, issue, backend_pid in ghosts:
                self._log.warning(f"Purging {issue}: {app_name} (Job {job_id})")

                # Terminate hanging backend if it still technically exists (Stale case)
                if issue == 'STALE_EXECUTION' and backend_pid is not None:
                    session.execute(text("SELECT pg_terminate_backend(:pid);"), {"pid": backend_pid})

                # delete_job() terminates any remaining worker and removes the schedule
                session.execute(text("SELECT delete_job(:job_id);"), {"job_id": job_id})

            session.commit()
        except Exception as e:
            session.rollback()
            self._log.error(f"Sweep failed: {e}")


# ----------------------------------------------------------------------------------------------------------------------
# Wide table field (column) administration
# ----------------------------------------------------------------------------------------------------------------------

@dataclass
class WideTableField:
    """
    A single deletable field (dynamic metric column) belonging to a
    protocol's wide table, as presented to an administrative UI.

    column_name is the physical, sanitized column that lives in the wide
    table (see timescaledb._clean_column_name); metric_name is the original
    source/registry name it was derived from. The two can differ, so the UI
    should key off column_name and echo it back unchanged to
    BridgeAdminManager.delete_fields().

    stale is True when metric_name is no longer produced by the protocol's
    current variable_mask/variable_screen-filtered registry map -- the same
    "column exists in the wide table but the live scrape data doesn't have
    it" condition timescaledb._validate_wide_row logs as `fewer_keys` at
    row-scrape time, computed proactively here instead of waiting for it to
    show up in a log line. See BridgeAdminManager.list_fields().
    """
    metric_name: str
    column_name: str
    data_type: str
    unit_mod: float | None
    notes: str | None
    stale: bool = False


@dataclass
class WideTableFieldDeletionResult:
    """Outcome of a BridgeAdminManager.delete_fields() call."""
    protocol_name: str
    wide_table_name: str
    deleted: list[str]              # column_names actually dropped
    not_found: list[str]            # requested column_names that didn't match any existing field
    remaining_fields: list[str]     # column_names still present on the wide table after the operation
    rollups_rebuilt: bool           # whether the protocol's rollups were successfully rebuilt


class BridgeAdminManager:
    """
    Backs the bridge-level admin/diagnostics surface of the web UI:
    editing a protocol's wide-table columns ("fields"), and composing the
    read-only diagnostic summaries every admin panel shows (Bridge
    Health, Storage Overview, Indexes, Compression & Retention Status,
    Background Job Status). Originally only did the former (as
    WideTableFieldManager); get_compression_retention_summary and
    get_background_jobs moved in as the admin UI grew panels that blend
    HyperTableManager's raw-table state with RollupManager's rollup-view
    state, and get_health_snapshot/get_storage_overview/get_index_overview
    joined from the bridge class itself for the same reason -- every one
    of these panels is bridge-administration/diagnostics, and splitting
    that surface across two classes by historical accident of where each
    was first written didn't reflect any real difference in what they
    are. Intended to be driven by the encapsulating web app -- the UI
    lists fields/panels, the admin acts, and this class carries out the
    DB work.

    Why field deletion can't be a plain ALTER TABLE:
    Wide tables are dynamically shaped -- every metric a protocol reports
    becomes its own column (see timescaledb._ensure_columns_for_metrics).
    RollupManager then builds four hierarchical continuous aggregates
    (hourly/daily/weekly/monthly) on top of each wide table, and every one
    of those materialized views has a SELECT list built directly from the
    wide table's current columns (see
    RollupManager._resolve_rollup_metric_descriptors_helper). A column
    can't simply disappear out from under that -- the dependent rollups
    have to be torn down first, or TimescaleDB will either block the
    ALTER TABLE or leave the views referencing a column that no longer
    exists. This class always performs a delete as a single
    tear-down / alter / rebuild sequence, never a bare column drop. The
    underlying pause/resume-compression-job and decompress-chunks
    mechanics that step needs live on HyperTableManager (bridge.
    hypertable_mgr) -- this class calls into those rather than keeping
    its own copy.

    Typical usage from the web app:

        admin_mgr = BridgeAdminManager(bridge)

        # populate the admin screen
        protocols = admin_mgr.list_editable_protocols()
        fields = admin_mgr.list_fields("eg4_18kpv")

        # ... admin checks some boxes and hits "Delete" ...
        result = admin_mgr.delete_fields("eg4_18kpv", ["soc_percent", "grid_voltage"])

        # diagnostic panels
        summary = admin_mgr.get_compression_retention_summary()
        jobs = admin_mgr.get_background_jobs()
    """

    # Structural columns that exist on every wide table and can never be
    # removed through this class.
    PROTECTED_COLUMNS: frozenset[str] = frozenset({"m_time", "device_info_id"})

    def __init__(self, bridge: "timescaledb") -> None:

        self._bridge: "timescaledb" = bridge
        self.engine: Engine = bridge.engine
        self.SessionFactory: sessionmaker[Session] = bridge.SessionFactory
        self._log: logging.Logger = logging.getLogger(__name__)
    # -------------------------
    # Resolution helpers
    # -------------------------

    def _resolve_wide_table(self, protocol_name: str) -> tuple[int, str]:
        """
        Looks up the protocol_id and wide_table_name for protocol_name.

        Raises:
            ValueError: protocol is unknown, or is narrow-only (>WIDE_TABLE_COLUMN_LIMIT 160
                        metrics) and therefore has no wide table to edit.
        """
        with self.SessionFactory() as session:
            row: Row[Any] | None = session.execute(
                text("""
                    SELECT protocol_id, wide_table_name
                    FROM protocol_registry
                    WHERE protocol_name = :p
                """),
                {"p": protocol_name}
            ).fetchone()

        if row is None:
            msg: str = f"Protocol '{protocol_name}' is not registered in protocol_registry."
            raise ValueError(msg)

        protocol_id, wide_table_name = row
        if wide_table_name is None:
            msg: str = f"Protocol '{protocol_name}' is narrow-only (>WIDE_TABLE_COLUMN_LIMIT 160 metrics) and has no wide table to edit."
            raise ValueError(msg)

        return protocol_id, wide_table_name

    def _wide_view_names(self, wide_table_name: str) -> list[str]:
        """
        Reproduces RollupManager's per-protocol view-naming convention, e.g.
        'device_metrics_wide__eg4_18kpv' ->
            ['hourly_rollup_wide__eg4_18kpv', 'daily_rollup_wide__eg4_18kpv',
             'weekly_rollup_wide__eg4_18kpv', 'monthly_rollup_wide__eg4_18kpv']

        Must stay in sync with RollupManager._ensure_cagg_views_for_protocol.
        """
        suffix: str = wide_table_name.removeprefix("device_metrics_")
        rollup_prefix: str = f"rollup_{suffix}"
        return [f"{gran}_{rollup_prefix}" for gran in ("hourly", "daily", "weekly", "monthly")]

    # -------------------------
    # Read-only listing for the UI
    # -------------------------

    def list_editable_protocols(self) -> list[tuple[str, str]]:
        """
        Returns (protocol_name, wide_table_name) pairs for every protocol
        that has a wide table and is therefore editable via this class
        (narrow-only protocols are excluded). Useful for populating a
        wide-table selector in the admin UI without an extra round trip
        per protocol to resolve its table name.
        """
        with self.SessionFactory() as session:
            rows: Sequence[Row[Any]] = session.execute(
                text("""
                    SELECT protocol_name, wide_table_name FROM protocol_registry
                    WHERE wide_table_name IS NOT NULL
                    ORDER BY protocol_name
                """)
            ).fetchall()
        return [(r[0], r[1]) for r in rows]

    def resolve_wide_table_name(self, protocol_name: str) -> str:
        """
        Public wrapper around _resolve_wide_table for callers (e.g. the web
        UI layer) that only need the table name, not the protocol_id.

        Raises:
            ValueError: unknown or narrow-only protocol.
        """
        _protocol_id, wide_table_name = self._resolve_wide_table(protocol_name)
        return wide_table_name

    def list_fields(self, protocol_name: str, active_metric_names: set[str] | None = None) -> list[WideTableField]:
        """
        Returns the current set of deletable metric columns for a
        protocol's wide table, for the calling web UI to render as a
        checkbox list.

        Args:
            protocol_name: The protocol whose wide table is being listed.
            active_metric_names: The metric/variable names the protocol's
                        live transport is currently configured (via
                        variable_mask/variable_screen) to produce -- i.e.
                        what timescaledb._extract_metric_names would derive
                        from from_transport.registry_map right now, plus
                        any transport-declared synthetic fields. Typically
                        built by the caller (see
                        timescale_service._active_metric_names_for_protocol)
                        rather than by this class, since resolving "the
                        live transport for this protocol" is a gateway
                        concern, not a wide-table-schema concern.

                        When provided, every returned WideTableField whose
                        metric_name is not in this set is flagged
                        `stale=True`, so the UI can highlight columns the
                        mask/screen config no longer produces -- the same
                        condition _validate_wide_row would otherwise only
                        surface as a warning log line the next time data is
                        scraped. When None (the protocol has no live
                        transport attached right now, e.g. it's connected
                        only for wide-table history), no field is flagged --
                        there's nothing to compare against, and guessing
                        would risk flagging every column red.

        Raises:
            ValueError: unknown or narrow-only protocol (see
                        _resolve_wide_table).
        """
        protocol_id, _wide_table_name = self._resolve_wide_table(protocol_name)

        with self.SessionFactory() as session:
            rows: Sequence[Row[Any]] = session.execute(
                text("""
                    SELECT metric_name, clean_column_name, data_type, unit_mod, notes
                    FROM metric_catalog
                    WHERE protocol_id = :pid
                    ORDER BY clean_column_name
                """),
                {"pid": protocol_id}
            ).fetchall()

        return [
            WideTableField(
                metric_name=metric_name,
                column_name=column_name,
                data_type=data_type,
                unit_mod=unit_mod,
                notes=notes,
                stale=active_metric_names is not None and metric_name not in active_metric_names,
            )
            for metric_name, column_name, data_type, unit_mod, notes in rows
            if column_name not in self.PROTECTED_COLUMNS
        ]

    # -------------------------
    # Deletion
    # -------------------------

    def delete_fields(self, protocol_name: str, field_names: list[str]) -> WideTableFieldDeletionResult:
        """
        Deletes the given fields (wide-table column names, as returned by
        list_fields()) from protocol_name's wide table, then rebuilds that
        protocol's rollups, indexes, and compression/retention policies.

        Safe to call with a mix of valid/unrecognized names -- anything not
        found in metric_catalog for this protocol is reported back in
        `not_found` and simply skipped rather than raising, so a UI
        double-submit or a stale checkbox list doesn't hard-fail the whole
        request.

        Args:
            protocol_name: The protocol whose wide table is being edited.
            field_names: column_name values from list_fields(), i.e. the
                         checked boxes the admin committed for deletion.

        Returns:
            WideTableFieldDeletionResult summarizing what happened.

        Raises:
            ValueError: unknown/narrow-only protocol, empty selection, only
                        protected columns requested, or the request would
                        delete every remaining metric column (an empty wide
                        table isn't supported -- disable/remove the
                        protocol instead).
            RuntimeError: RollupManager isn't initialized yet (bridge not
                        connected to TimescaleDB).
            Exception: any failure partway through the drop/rebuild
                        sequence is logged and re-raised so the caller
                        (and the admin) know the operation did not
                        complete cleanly. rollup_setup_complete is left
                        False in that case, so the background reconnect /
                        rediscovery path will retry the rollup rebuild
                        automatically (see timescaledb._rediscover_protocols).
        """
        if not field_names:
            raise ValueError("No fields were provided to delete.")

        # de-dupe, preserve order, drop blanks
        requested: list[str] = [f for f in dict.fromkeys(field_names) if f]

        protected_requested: list[str] = [f for f in requested if f in self.PROTECTED_COLUMNS]
        if protected_requested:
            msg: str = f"Refusing to delete protected columns: {protected_requested}"
            raise ValueError(msg)

        if self._bridge.rollup_mgr is None:
            raise RuntimeError(
                "RollupManager is not initialized -- bridge must be connected before editing wide tables."
            )
        if self._bridge.hypertable_mgr is None:
            raise RuntimeError(
                "HyperTableManager is not initialized -- bridge must be connected before editing wide tables."
            )

        protocol_id, wide_table_name = self._resolve_wide_table(protocol_name)
        rollup_mgr: "RollupManager" = self._bridge.rollup_mgr
        hypertable_mgr: "HyperTableManager" = self._bridge.hypertable_mgr

        # Pause the flush worker for the duration of the edit. add_wide_rollup()
        # (called below) also sets/clears this same event around its own work;
        # setting it here first just widens the window to cover the column
        # drop that happens before the rebuild.
        self._bridge.migration_in_progress.set()
        rebuilt: bool = False
        paused_job_ids: list[int] = []

        try:
            with self.SessionFactory() as session:
                # Whitelist requested names against what's actually in
                # metric_catalog for this protocol -- column names handed in
                # from the UI layer are never trusted directly in DDL.
                catalog_rows = session.execute(
                    text("""
                        SELECT catalog_id, clean_column_name
                        FROM metric_catalog
                        WHERE protocol_id = :pid
                    """),
                    {"pid": protocol_id}
                ).fetchall()

            existing_columns: dict[str, int] = {col: cid for cid, col in catalog_rows}
            to_delete: list[str] = [f for f in requested if f in existing_columns]
            not_found: list[str] = [f for f in requested if f not in existing_columns]

            if not to_delete:
                self._log.info(
                    f"BridgeAdminManager: none of {requested} matched existing fields on "
                    f"'{wide_table_name}' — nothing to delete."
                )
                return WideTableFieldDeletionResult(
                    protocol_name=protocol_name,
                    wide_table_name=wide_table_name,
                    deleted=[],
                    not_found=not_found,
                    remaining_fields=sorted(existing_columns.keys()),
                    rollups_rebuilt=False,
                )

            if len(to_delete) >= len(existing_columns):
                raise ValueError(  # noqa: TRY301
                    "Refusing to delete every remaining field -- a wide table needs at least "
                    "one metric column. Disable or remove the protocol instead if it's no "
                    "longer needed."
                )

            self._log.info(
                f"BridgeAdminManager: deleting {len(to_delete)} field(s) from "
                f"'{wide_table_name}' (protocol='{protocol_name}'): {to_delete}"
            )

            # Pause any compression policy job configured for this specific
            # wide table before touching its schema. ALTER TABLE DROP
            # COLUMN needs a lock across every chunk of the hypertable; a
            # background compression job concurrently compressing one of
            # those chunks holds a lock TimescaleDB doesn't always resolve
            # cleanly against that ALTER TABLE, which can stall or deadlock
            # the drop. Scoped to this table only (see
            # _pause_compression_job) -- not a blanket pause of every
            # compression job in the database, which would needlessly
            # affect unrelated protocols. Always resumed in the finally
            # block below, including on failure.
            with self.SessionFactory() as session:
                paused_job_ids = hypertable_mgr.pause_compression_job_for_table(session, wide_table_name)
                session.commit()

            # 1. Tear down this protocol's rollups first. They SELECT every
            #    current wide-table column by name, so they cannot survive
            #    the ALTER TABLE below and must be rebuilt afterward anyway.
            view_names: list[str] = self._wide_view_names(wide_table_name)
            with self.SessionFactory() as session:
                rollup_mgr.drop_protocol_rollup(session=session, view_names=view_names)

            # Mark the protocol's rollup setup incomplete for the duration of
            # the edit. This mirrors the crash-safety pattern used during
            # initial setup: if the process dies mid-edit, restart-time
            # rediscovery (_rediscover_protocols) will see setup_complete =
            # False and retry the rollup rebuild automatically instead of
            # silently leaving the protocol without rollups.
            rollup_mgr.mark_rollup_setup_complete_helper(protocol_name, complete=False)

            # 2. Drop the columns and their metric_catalog rows, serialized
            #    against other schema changes with the same advisory lock
            #    that column *additions* use.
            with self._bridge.schema_lock:
                with self.SessionFactory() as session:
                    with session.begin():
                        self._bridge.schema_advisory_lock(session)

                        hypertable_mgr.decompress_table_chunks_best_effort(session, wide_table_name)

                        # table_name/col come from _safe_table_name() /
                        # _clean_column_name() at creation time and are
                        # re-validated against metric_catalog above, so the
                        # f-string is safe here (same pattern used by
                        # _ensure_columns_for_metrics' ADD COLUMN).
                        for col in to_delete:
                            session.execute(text(f"ALTER TABLE {wide_table_name} DROP COLUMN IF EXISTS {col};"))

                        catalog_ids: list[int] = [existing_columns[col] for col in to_delete]
                        session.execute(
                            text("DELETE FROM metric_catalog WHERE catalog_id = ANY(:ids)"),
                            {"ids": catalog_ids},
                        )

                        session.execute(
                            text("""
                                UPDATE protocol_registry
                                SET metric_count = GREATEST(metric_count - :n, 0),
                                    updated_at = :now
                                WHERE protocol_id = :pid
                            """),
                            {"n": len(to_delete), "now": _now_tz(), "pid": protocol_id}
                        )

                    # 3. Resync the ORM reflection + write-path column cache
                    #    so in-flight/next writes see the new shape
                    #    immediately, same as after a column addition.
                    self._bridge.sync_single_table_schema(wide_table_name)

            # 4. Drop any now-stale metric_name -> column mappings used to
            #    coerce incoming raw values before insert.
            mapping: dict[str, tuple[str, str]] = self._bridge.protocol_metric_mappings.get(protocol_name, {})
            for metric_name, (col, _dtype) in list(mapping.items()):
                if col in to_delete:
                    mapping.pop(metric_name, None)

            # 5. Rebuild rollups (indexes, compression policy, retention
            #    policy, and the four hierarchical CAGGs) from whatever
            #    columns remain in metric_catalog.
            rollup_mgr.add_wide_rollup(protocol_name, wide_table_name)
            rebuilt = True

            remaining_fields: list[str] = sorted(set(existing_columns.keys()) - set(to_delete))

            self._log.info(f"BridgeAdminManager: deleted {to_delete} from '{wide_table_name}' and rebuilt rollups.")

            return WideTableFieldDeletionResult(
                protocol_name=protocol_name,
                wide_table_name=wide_table_name,
                deleted=to_delete,
                not_found=not_found,
                remaining_fields=remaining_fields,
                rollups_rebuilt=rebuilt,
            )

        except Exception as e:
            self._log.error(f"BridgeAdminManager.delete_fields failed for '{protocol_name}': {e}")
            raise

        finally:
            # add_wide_rollup() clears this same event in its own finally
            # block on the success path, so this is a no-op by the time we
            # get here normally -- it's a safety net for paths that raised
            # before reaching the rebuild step.
            self._bridge.migration_in_progress.clear()

            if paused_job_ids:
                try:
                    with self.SessionFactory() as session:
                        hypertable_mgr.resume_compression_job_for_table(session, paused_job_ids)
                        session.commit()
                except Exception as e:
                    # Never let a resume failure mask the original error (if
                    # any) or fail an otherwise-successful edit. Logged at
                    # warning rather than silently swallowed, since a job
                    # left paused needs a human to notice and re-enable it
                    # (e.g. via TimescaleDB's own alter_job) until this is
                    # retried successfully.
                    self._log.warning(
                        f"delete_fields: could not resume compression job(s) {paused_job_ids} "
                        f"for '{wide_table_name}': {e}"
                    )

    # -------------------------
    # Bridge-level diagnostic panels -- moved here from the timescaledb
    # bridge class itself (get_health_snapshot/get_storage_overview/
    # get_index_overview), which is now purely transport/write-path
    # plumbing plus a couple of read-only bridge-status properties;
    # every panel-facing read lives here instead, alongside the
    # compression/jobs composers just below. Each queries bridge-level
    # state (self._bridge) and plain source-table metadata directly --
    # unlike get_compression_retention_summary/get_background_jobs
    # further down, none of these three need to know HyperTableManager's
    # or RollupManager's internal shapes.
    # -------------------------

    def get_health_snapshot(self) -> dict[str, Any]:
        """
        Read-only snapshot of the bridge's live connection and background-
        worker state, for the "Bridge Health" panel. Pulls together state
        that's otherwise scattered across the bridge class, its backlog
        manager, and RollupManager. The only DB round trip is a single
        cheap COUNT against protocol_registry (skipped entirely if not
        currently connected).
        """
        reconnecting: bool = bool(getattr(self._bridge, "_reconnect_thread_running", False))
        migration_in_progress: bool | None = (
            self._bridge.rollup_mgr.migration_in_progress.is_set() if self._bridge.rollup_mgr is not None else None
        )
        backlog_count: int = (
            len(self._bridge.backlog.backlog_points) if getattr(self._bridge, "backlog", None) is not None else 0
        )

        protocols_rollup_complete: int = 0
        protocols_rollup_total: int = 0
        if self._bridge.tsdb_connected:
            try:
                with self.SessionFactory() as session:
                    row: Row[Any] = session.execute(
                        text("""
                            SELECT
                                COUNT(*) FILTER (WHERE rollup_setup_complete = true) AS complete,
                                COUNT(*) AS total
                            FROM protocol_registry
                            WHERE rollup_enabled = true
                        """)
                    ).one()
                protocols_rollup_complete = row.complete or 0
                protocols_rollup_total = row.total or 0
            except SQLAlchemyError as e:
                self._log.error(f"get_health_snapshot: protocol_registry tally failed: {e}")

        return {
            "tsdb_connected": self._bridge.tsdb_connected,
            "reconnecting": reconnecting,
            "migration_in_progress": migration_in_progress,
            "backlog_count": backlog_count,
            "max_backlog_size": self._bridge.max_backlog_size,
            "max_backlog_age": self._bridge.max_backlog_age,
            "enable_auto_refresh": self._bridge.rollup_mgr.enable_auto_refresh if self._bridge.rollup_mgr else None,
            "auto_refresh_interval": self._bridge.rollup_mgr.auto_refresh_interval if self._bridge.rollup_mgr else None,
            "protocols_rollup_complete": protocols_rollup_complete,
            "protocols_rollup_total": protocols_rollup_total,
        }

    def get_storage_overview(self) -> list[dict[str, Any]]:
        """
        Read-only per-source-table storage snapshot for the "Storage
        Overview" panel: the shared narrow table plus every wide table
        this bridge has created. Row counts use TimescaleDB's own
        approximate_row_count(), which sums each chunk's analyzed
        reltuples rather than an exact COUNT(*) (too slow on a large
        hypertable purely to populate an info panel). This is NOT the
        same as querying pg_stat_user_tables directly on the hypertable
        name -- a hypertable's own catalog row holds no tuples itself
        (the data lives in its per-chunk child tables), so n_live_tup on
        the parent is always ~0 regardless of how much data exists.

        Each table is queried independently and failures are recorded
        per-row rather than aborting the whole snapshot, so one table
        having a transient issue doesn't blank out the rest.
        """
        if not self._bridge.tsdb_connected:
            return []

        tables: list[tuple[str, str]] = [("shared_narrow", "device_metrics_narrow")]
        try:
            with self.SessionFactory() as session:
                wide_rows: Sequence[Row[Any]] = session.execute(
                    text("""
                        SELECT protocol_name, wide_table_name
                        FROM protocol_registry
                        WHERE wide_table_name IS NOT NULL
                        ORDER BY protocol_name
                    """)
                ).fetchall()
        except SQLAlchemyError as e:
            self._log.error(f"get_storage_overview: protocol_registry query failed: {e}")
            wide_rows = []

        tables.extend((protocol_name, wide_table_name) for protocol_name, wide_table_name in wide_rows)

        results: list[dict[str, Any]] = []
        for protocol_name, table_name in tables:
            try:
                with self.SessionFactory() as session:
                    row: Row[Any] = session.execute(
                        text(f"""
                            SELECT
                                approximate_row_count('{table_name}') AS approx_rows,
                                hypertable_size('{table_name}'::regclass) AS size_bytes,
                                (SELECT count(*) FROM timescaledb_information.chunks
                                    WHERE hypertable_name = '{table_name}') AS chunk_count,
                                (SELECT min(m_time) FROM {table_name}) AS oldest,
                                (SELECT max(m_time) FROM {table_name}) AS newest
                        """)  # noqa: S608
                    ).one()
                results.append({
                    "protocol_name": protocol_name,
                    "table_name": table_name,
                    "approx_rows": row.approx_rows or 0,
                    "size_bytes": row.size_bytes or 0,
                    "chunk_count": row.chunk_count or 0,
                    "oldest": row.oldest,
                    "newest": row.newest,
                    "error": None,
                })
            except SQLAlchemyError as e:
                self._log.error(f"get_storage_overview: query failed for '{table_name}': {e}")
                results.append({
                    "protocol_name": protocol_name, "table_name": table_name,
                    "approx_rows": None, "size_bytes": None, "chunk_count": None,
                    "oldest": None, "newest": None, "error": str(e),
                })

        return results

    # Plain (non-hypertable) validation/lookup tables to include in the
    # Indexes panel alongside the hypertables below -- these back the
    # protocol/metric/device registries every write path validates
    # against, so their indexes matter for the same "is this table
    # healthy" question the panel is answering, even though they're not
    # time-series data. Queried the same way regardless of which bridge
    # instance is asking, so this is safe as a module-level constant.
    _LOOKUP_TABLE_NAMES: tuple[str, ...] = ("protocol_registry", "metric_catalog", "device_info")

    # SQL shared by both branches of get_index_overview() for pulling an
    # index's ordered key column list out of pg_index.indkey (a raw int2
    # vector of attnums, one entry per key column in index order --
    # WITH ORDINALITY over unnest() is what keeps that order, since a
    # plain join would give Postgres no reason to preserve it). Returns
    # NULL for an expression index (attnum 0 has no pg_attribute row),
    # which the panel falls back to displaying the raw index definition
    # for instead.
    _INDEX_KEY_COLUMNS_SQL: str = """
        (
            SELECT array_agg(a.attname ORDER BY k.ord)
            FROM unnest(idx.indkey) WITH ORDINALITY AS k(attnum, ord)
            JOIN pg_attribute a ON a.attrelid = idx.indrelid AND a.attnum = k.attnum
        ) AS key_columns
    """

    def get_index_overview(self) -> list[dict[str, Any]]:
        """
        Read-only per-index snapshot for the "Indexes" panel: every index
        on the shared narrow table, every wide table (the same source-
        table scope as get_storage_overview -- not the rollup views
        themselves), and the plain validation/lookup tables in
        _LOOKUP_TABLE_NAMES (protocol_registry, metric_catalog,
        device_info).

        Hypertables (narrow/wide) and plain tables need different queries
        here, not one -- an index defined on a hypertable is real DDL on
        the parent relation, but the parent itself never holds rows (data
        lives in per-chunk child tables, same point get_storage_overview's
        docstring makes about hypertable_size() vs n_live_tup), so a
        naive pg_relation_size()/pg_stat_user_indexes lookup keyed off the
        parent index only ever sees an almost-empty catalog entry --
        every hypertable index reporting the same ~8kB minimum-page size
        regardless of table size, and idx_scan permanently stuck at 0
        since Postgres always executes the actual scan against a chunk's
        own copy of the index, never the parent's. For the hypertable
        branch below:
          - size_bytes uses hypertable_index_size(), TimescaleDB's own
            public sizing function, which sums the index across every
            chunk instead of reading the parent's near-empty entry.
          - idx_scan/idx_tup_read/idx_tup_fetch are summed from pg_stat_
            user_indexes across every chunk's own copy of the index (chunk
            list from timescaledb_information.chunks, a public, version-
            stable view -- NOT from TimescaleDB's internal
            _timescaledb_catalog schema, which an earlier version of this
            method used to map a chunk-local index back to its parent by
            name; that schema is TimescaleDB's own private implementation
            detail, not a supported API, and turned out not to have the
            expected shape in practice, exactly the risk flagged for a
            similar shortcut in list_compression_groups' docstring).
            Lacking that name-based mapping, chunk-level index rows are
            matched back to the parent index by key-column signature
            instead (same ordered column list computed for key_columns
            below) -- reliable here since a chunk's indexes are always
            struct-for-struct copies of the parent's, propagated
            automatically when an index is created on the hypertable.
          - This scan-stats query is best-effort and kept separate from
            the metadata query below: if it fails for any reason (e.g. a
            restricted role, or a chunk mid-creation), that table's rows
            still get real names/sizes/key columns, just with idx_scan
            etc. left as None ("not available") rather than blanking the
            whole table out the way one shared query would.
        Plain lookup tables need neither workaround -- pg_relation_size()
        and pg_stat_user_indexes already read the real (only) copy of
        the table/index directly, in one query.

        Each table's metadata query is independent and failures are
        recorded via a single synthetic error row for that table rather
        than aborting the whole snapshot, so one table having a
        transient issue doesn't blank out the rest — same pattern as
        get_storage_overview.
        """
        if not self._bridge.tsdb_connected:
            return []

        tables: list[tuple[str, bool]] = [("device_metrics_narrow", True)]
        try:
            with self.SessionFactory() as session:
                wide_table_names: Sequence[str] = session.execute(
                    text("""
                        SELECT wide_table_name
                        FROM protocol_registry
                        WHERE wide_table_name IS NOT NULL
                        ORDER BY protocol_name
                    """)
                ).scalars().all()
        except SQLAlchemyError as e:
            self._log.error(f"get_index_overview: protocol_registry query failed: {e}")
            wide_table_names = []

        tables.extend((name, True) for name in wide_table_names)
        tables.extend((name, False) for name in self._LOOKUP_TABLE_NAMES)

        hypertable_metadata_sql: TextClause = text(f"""
            SELECT
                pi.indexname AS index_name,
                pi.indexdef,
                hypertable_index_size(quote_ident(pi.indexname)::regclass) AS size_bytes,
                idx.indisunique AS is_unique,
                idx.indisprimary AS is_primary,
                {self._INDEX_KEY_COLUMNS_SQL}
            FROM pg_indexes pi
            JOIN pg_index idx ON idx.indexrelid = quote_ident(pi.indexname)::regclass
            WHERE pi.tablename = :table_name
            ORDER BY pi.indexname
        """)  # noqa: S608

        # Best-effort only -- see docstring. Sums pg_stat_user_indexes
        # across every chunk of the hypertable (chunk list from the
        # public timescaledb_information.chunks view) and groups by each
        # chunk-local index's own key-column signature, so the result can
        # be matched back to the parent index of the same signature in
        # Python without needing any TimescaleDB-internal name mapping.
        hypertable_scan_stats_sql: TextClause = text(f"""
            WITH chunk_list AS (
                SELECT chunk_schema, chunk_name
                FROM timescaledb_information.chunks
                WHERE hypertable_name = :table_name
            ),
            chunk_idx AS (
                SELECT
                    {self._INDEX_KEY_COLUMNS_SQL},
                    psui.idx_scan,
                    psui.idx_tup_read,
                    psui.idx_tup_fetch
                FROM chunk_list cl
                JOIN pg_indexes pi ON pi.schemaname = cl.chunk_schema AND pi.tablename = cl.chunk_name
                JOIN pg_index idx
                    ON idx.indexrelid = (quote_ident(pi.schemaname) || '.' || quote_ident(pi.indexname))::regclass
                LEFT JOIN pg_stat_user_indexes psui
                    ON psui.schemaname = pi.schemaname AND psui.indexrelname = pi.indexname
            )
            SELECT
                key_columns,
                coalesce(sum(idx_scan), 0) AS idx_scan,
                coalesce(sum(idx_tup_read), 0) AS idx_tup_read,
                coalesce(sum(idx_tup_fetch), 0) AS idx_tup_fetch
            FROM chunk_idx
            GROUP BY key_columns
        """)  # noqa: S608

        plain_table_sql: TextClause = text(f"""
            SELECT
                pi.indexname AS index_name,
                pi.indexdef,
                pg_relation_size(quote_ident(pi.indexname)::regclass) AS size_bytes,
                idx.indisunique AS is_unique,
                idx.indisprimary AS is_primary,
                {self._INDEX_KEY_COLUMNS_SQL},
                coalesce(psui.idx_scan, 0) AS idx_scan,
                coalesce(psui.idx_tup_read, 0) AS idx_tup_read,
                coalesce(psui.idx_tup_fetch, 0) AS idx_tup_fetch
            FROM pg_indexes pi
            JOIN pg_index idx ON idx.indexrelid = quote_ident(pi.indexname)::regclass
            LEFT JOIN pg_stat_user_indexes psui
                ON psui.indexrelname = pi.indexname AND psui.schemaname = pi.schemaname
            WHERE pi.tablename = :table_name
            ORDER BY pi.indexname
        """)  # noqa: S608

        results: list[dict[str, Any]] = []
        for table_name, is_hypertable in tables:
            if not is_hypertable:
                try:
                    with self.SessionFactory() as session:
                        rows: Sequence[Row[Any]] = session.execute(
                            plain_table_sql, {"table_name": table_name}
                        ).fetchall()
                    for r in rows:
                        results.append({
                            "table_name": table_name,
                            "is_hypertable": False,
                            "index_name": r.index_name,
                            "size_bytes": r.size_bytes or 0,
                            "idx_scan": r.idx_scan or 0,
                            "idx_tup_read": r.idx_tup_read or 0,
                            "idx_tup_fetch": r.idx_tup_fetch or 0,
                            "is_unique": bool(r.is_unique),
                            "is_primary": bool(r.is_primary),
                            "key_columns": list(r.key_columns) if r.key_columns else [],
                            "indexdef": r.indexdef,
                            "error": None,
                        })
                except SQLAlchemyError as e:
                    self._log.error(f"get_index_overview: query failed for '{table_name}': {e}")
                    results.append({
                        "table_name": table_name, "is_hypertable": False,
                        "index_name": None, "size_bytes": None, "idx_scan": None,
                        "idx_tup_read": None, "idx_tup_fetch": None,
                        "is_unique": None, "is_primary": None, "key_columns": [],
                        "indexdef": None, "error": str(e),
                    })
                continue

            try:
                with self.SessionFactory() as session:
                    meta_rows: Sequence[Row[Any]] = session.execute(
                        hypertable_metadata_sql, {"table_name": table_name}
                    ).fetchall()
            except SQLAlchemyError as e:
                self._log.error(f"get_index_overview: metadata query failed for '{table_name}': {e}")
                results.append({
                    "table_name": table_name, "is_hypertable": True,
                    "index_name": None, "size_bytes": None, "idx_scan": None,
                    "idx_tup_read": None, "idx_tup_fetch": None,
                    "is_unique": None, "is_primary": None, "key_columns": [],
                    "indexdef": None, "error": str(e),
                })
                continue

            scan_stats_by_key: dict[tuple[str, ...], Row[Any]] = {}
            try:
                with self.SessionFactory() as session:
                    scan_rows: Sequence[Row[Any]] = session.execute(
                        hypertable_scan_stats_sql, {"table_name": table_name}
                    ).fetchall()
                scan_stats_by_key = {
                    tuple(r.key_columns) if r.key_columns else (): r for r in scan_rows
                }
            except SQLAlchemyError as e:
                # Degrade gracefully -- names/sizes/key columns from the
                # metadata query above are still good, just without scan
                # counts for this table. Logged at warning, not error:
                # this is an expected fallback path, not a broken bridge.
                self._log.warning(f"get_index_overview: scan-stats query failed for '{table_name}': {e}")

            for r in meta_rows:
                key_columns: list[str] = list(r.key_columns) if r.key_columns else []
                scan_row: Row[Any] | None = scan_stats_by_key.get(tuple(key_columns))
                results.append({
                    "table_name": table_name,
                    "is_hypertable": True,
                    "index_name": r.index_name,
                    "size_bytes": r.size_bytes or 0,
                    "idx_scan": scan_row.idx_scan if scan_row is not None else None,
                    "idx_tup_read": scan_row.idx_tup_read if scan_row is not None else None,
                    "idx_tup_fetch": scan_row.idx_tup_fetch if scan_row is not None else None,
                    "is_unique": bool(r.is_unique),
                    "is_primary": bool(r.is_primary),
                    "key_columns": key_columns,
                    "indexdef": r.indexdef,
                    "error": None,
                })

        return results

    # -------------------------
    # Diagnostic composers -- backs the Bridge Health, Storage Overview,
    # Indexes, Compression & Retention Status, and Background Job Status
    # panels. get_health_snapshot/get_storage_overview/get_index_overview
    # (just above) query bridge-level connection/backlog state and plain
    # source-table metadata directly; get_compression_retention_summary/
    # get_background_jobs below additionally blend HyperTableManager's
    # raw-table config/state with RollupManager's rollup-view config/
    # state. Moved here (from the bridge class itself, for the first
    # three) so every admin/diagnostic panel's data has exactly one home,
    # rather than being split across the bridge class and this one by
    # historical accident of where each was originally written.
    # -------------------------

    def get_compression_retention_summary(self) -> dict[str, Any]:
        """
        Read-only summary of this bridge's compression and retention
        configuration, for the "Compression & Retention Status" panel.
        Every value here is a plain attribute read (set from rollup_policy
        / hypertable_defaults in HyperTableManager/RollupManager __init__)
        -- no DB round trip, since this is config, not live state. Returns
        {} if the bridge isn't connected yet (hypertable_mgr/rollup_mgr
        don't exist until then) -- callers treat that the same as "still
        connecting", matching the other panels' `not summary` template check.

        Compression is scheduled per rollup granularity (see
        RollupManager.hourly_compress_after_interval and siblings) — each
        interval is how long TimescaleDB waits after a chunk's time range
        closes before compressing it. Retention (drop_after) applies
        uniformly to raw data across the narrow and wide hypertables.

        `raw_narrow_compress_after_interval` / `raw_wide_compress_after_
        interval` / `raw_narrow_chunk_time_interval` / `raw_wide_chunk_
        time_interval` are listed separately, not folded into `schedule`:
        they govern the RAW hypertables' own compression policy and chunk
        sizing (HyperTableManager), while `schedule`'s four rows each
        govern a distinct ROLLUP VIEW (RollupManager, four separate
        objects). Conflating raw with rollup-view settings was the source
        of a real bug — see RollupManager.setup_rollup's docstring for the
        history. Narrow and wide additionally get their own values for
        BOTH settings, not shared ones — a wide table's row density (one
        row per device per write cycle) is far lower than narrow's (up to
        ~160 rows per write cycle, aggregated across every protocol's wide
        table into the one shared narrow table), so neither the same
        chunk_time_interval nor the same compress_after (which
        TimescaleDB's own guidance ties to a table's own chunk_time_
        interval, at roughly 2-3x it) produces safe, comparable results
        for both. See HyperTableManager.hypertable_defaults' raw_narrow_
        chunk_time_interval and raw_narrow_compress_after_interval
        comments for the full reasoning.
        """
        htm: "HyperTableManager | None" = self._bridge.hypertable_mgr
        rm: "RollupManager | None" = self._bridge.rollup_mgr
        if htm is None or rm is None:
            return {}

        return {
            "enable_compression": htm.enable_compression,
            "enable_dynamic_chunk_sizing": htm.enable_dynamic_chunk_sizing,
            "retention_interval": htm.drop_after,
            "segment_by_narrow": htm.compress_segmentby_narrow,
            "segment_by_wide": htm.compress_segmentby_wide,
            "compress_orderby": htm.compress_orderby,
            "raw_narrow_compress_after_interval": htm.raw_narrow_compress_after_interval,
            "raw_wide_compress_after_interval": htm.raw_wide_compress_after_interval,
            "raw_narrow_chunk_time_interval": htm.raw_narrow_chunk_time_interval,
            "raw_wide_chunk_time_interval": htm.raw_wide_chunk_time_interval,
            "schedule": [
                {"granularity": "hourly", "compress_after": rm.hourly_compress_after_interval},
                {"granularity": "daily", "compress_after": rm.daily_compress_after_interval},
                {"granularity": "weekly", "compress_after": rm.weekly_compress_after_interval},
                {"granularity": "monthly", "compress_after": rm.monthly_compress_after_interval},
            ],
            # Freshly computed on every panel load, not stored config --
            # see HyperTableManager.get_dynamic_raw_table_overview.
            # Already in the flat, per-table shape the
            # bridge_timescale_compression_panel.html template expects
            # for summary.dynamic_raw_tables.
            #
            # dynamic_view_groups is deliberately NOT built here:
            # RollupManager.get_dynamic_view_overview() returns a flat
            # list (one row per (table, granularity) pair), and the
            # caller (timescale_service.get_compression_retention_
            # summary) groups it into the {protocol_name, views: [...]}
            # shape the template needs via itertools.groupby, preserving
            # get_dynamic_view_overview()'s narrow-first ordering. That
            # grouping is a display-shape concern for the service layer,
            # not something this composer should duplicate.
            "dynamic_raw_tables": htm.get_dynamic_raw_table_overview(),
        }

    def get_background_jobs(self) -> list[dict[str, Any]]:
        """
        Read-only snapshot of TimescaleDB's own background scheduler jobs
        (compression, retention, and continuous-aggregate refresh
        policies) for every hypertable/view this bridge manages -- the
        shared narrow table, every wide table, and every rollup view
        currently registered on RollupManager. Lets an admin catch a job
        that's silently failing or falling behind before it becomes a
        bigger problem; see RollupManager._purge_ghost_jobs_helper for the
        related cleanup path when a job's target no longer exists at all.
        Returns [] if the bridge isn't connected yet.
        """
        rm: "RollupManager | None" = self._bridge.rollup_mgr
        if rm is None:
            return []

        try:
            with self.SessionFactory() as session:
                wide_table_names: Sequence[str] = session.execute(
                    text("SELECT wide_table_name FROM protocol_registry WHERE wide_table_name IS NOT NULL")
                ).scalars().all()
        except SQLAlchemyError as e:
            self._log.error(f"get_background_jobs: protocol_registry query failed: {e}")
            wide_table_names = []

        target_names: list[str] = (
            ["device_metrics_narrow"] + list(wide_table_names) + list(rm.known_rollup_views.keys())
        )
        if not target_names:
            return []

        try:
            with self.SessionFactory() as session:
                rows: Sequence[Row[Any]] = session.execute(
                    text("""
                        SELECT
                            j.job_id, j.proc_name, j.hypertable_name, j.schedule_interval,
                            js.last_run_status, js.last_successful_finish, js.next_start,
                            js.total_runs, js.total_failures
                        FROM timescaledb_information.jobs j
                        LEFT JOIN timescaledb_information.job_stats js ON j.job_id = js.job_id
                        WHERE j.hypertable_name = ANY(:names)
                        ORDER BY j.hypertable_name, j.proc_name
                    """),
                    {"names": target_names},
                ).fetchall()
        except SQLAlchemyError as e:
            self._log.error(f"get_background_jobs: jobs query failed: {e}")
            return []

        return [
            {
                "job_id": r.job_id,
                "proc_name": r.proc_name,
                "hypertable_name": r.hypertable_name,
                "schedule_interval": str(r.schedule_interval) if r.schedule_interval is not None else None,
                "last_run_status": r.last_run_status,
                "last_successful_finish": r.last_successful_finish,
                "next_start": r.next_start,
                "total_runs": r.total_runs,
                "total_failures": r.total_failures,
            }
            for r in rows
        ]

