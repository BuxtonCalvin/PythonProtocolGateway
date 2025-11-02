"""
timescaledb_out transport module (with rollup continuous aggregates)

Features:
 - Auto-create database (default "solar", configurable)
 - device_info (unique devices)
 - device_metrics hypertable (composite PK or FK-based depending on design)
 - Hypertable compression & retention (idempotent)
 - Continuous aggregates: hourly, daily (configurable bucket sizes)
 - Optional Continuous rollups: daily_rollup, weekly_rollup, monthly_rollup (configurable)
 - Async batch flushing + persistent disk backlog
 - Registry-aware validation of incoming data dicts
 - OS-local timestamps
"""

import os
import json
import threading
import time
from datetime import datetime, timezone
from queue import Queue
from typing import Any, Dict, List, Optional

from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    Float,
    Text,
    DateTime,
    text,
    select,
)
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
from sqlalchemy.exc import SQLAlchemyError

from defs.common import strtobool
from ..protocol_settings import Registry_Type
from .transport_base import transport_base

# ORM base / session factory
Base = declarative_base()
Session = sessionmaker()


# ORM models
class DeviceInfo(Base):
    __tablename__ = "device_info"

    id = Column(Integer, primary_key=True)
    device_identifier = Column(Text, unique=True, nullable=False, index=True)
    device_name = Column(Text)
    device_manufacturer = Column(Text)
    device_model = Column(Text)
    device_serial_number = Column(Text)
    transport = Column(Text)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now().astimezone())

    # relationship omitted for simplicity (we insert metrics directly)


class DeviceMetrics(Base):
    __tablename__ = "device_metrics"

    # Use simple columns and rely on Timescale's recommendations for partitioning
    id = Column(Integer, primary_key=True, autoincrement=True)
    time = Column(DateTime(timezone=True), nullable=False, index=True)
    sensor_id = Column(Integer, nullable=False, index=True)  # will store DeviceInfo.id when available
    metric_name = Column(Text, nullable=False, index=True)
    metric_value = Column(Float, nullable=True)


class timescaledb_out(transport_base):
    """
    TimescaleDB transport with hypertable, continuous aggregates and rollup support.
    """

    # Default settings (overrideable by settings SectionProxy)
    host: str = "localhost"
    port: int = 5432
    database: str = "solar"
    username: str = ""
    password: str = ""

    include_device_info: bool = True
    force_float: bool = True

    batch_size: int = 100
    batch_timeout: float = 10.0

    # persistent backlog settings
    enable_persistent_storage: bool = True
    persistent_storage_path: str = "timescaledb_backlog"
    backlog_file: Optional[str] = None
    max_backlog_size: int = 10000
    max_backlog_age: int = 86400  # seconds

    # reconnect/backoff
    reconnect_attempts: int = 5
    reconnect_delay: float = 5.0
    use_exponential_backoff: bool = True
    max_reconnect_delay: float = 300.0

    # hypertable defaults
    hypertable_options_default = {
        "time_column": "time",
        "compress_orderby": "time DESC",
        "compress_segmentby": "sensor_id,metric_name",
        "chunk_time_interval": "7 days",
        "drop_after": "1 year",
        "migrate_data": True,
        "if_not_exists": True,
    }

    # continuous aggregate defaults (configurable)
    continuous_defaults = {
        "hourly_bucket": "1 hour",
        "daily_bucket": "1 day",
        "weekly_bucket": "7 days",
        "monthly_bucket": "1 month",
    }

    # rollup defaults
    rollup_defaults = {
        "daily_rollup_bucket": "1 day",
        "weekly_rollup_bucket": "1 week",
        "monthly_rollup_bucket": "1 month",
        "daily_rollup_start": "14 days",
        "weekly_rollup_start": "60 days",
        "monthly_rollup_start": "180 days",
        "enable_rollups": True,
    }

    def __init__(self, settings):
        # load configuration from SectionProxy
        self.host = settings.get("host", fallback=self.host)
        self.port = settings.getint("port", fallback=self.port)
        self.database = settings.get("database", fallback=self.database)
        self.username = settings.get("username", fallback=self.username)
        self.password = settings.get("password", fallback=self.password)

        self.include_device_info = strtobool(settings.get("include_device_info", fallback=self.include_device_info))
        self.force_float = strtobool(settings.get("force_float", fallback=self.force_float))

        self.batch_size = settings.getint("batch_size", fallback=self.batch_size)
        self.batch_timeout = settings.getfloat("batch_timeout", fallback=self.batch_timeout)

        self.enable_persistent_storage = strtobool(settings.get("enable_persistent_storage", fallback=self.enable_persistent_storage))
        self.persistent_storage_path = settings.get("persistent_storage_path", fallback=self.persistent_storage_path)
        self.max_backlog_size = settings.getint("max_backlog_size", fallback=self.max_backlog_size)
        self.max_backlog_age = settings.getint("max_backlog_age", fallback=self.max_backlog_age)

        self.reconnect_attempts = settings.getint("reconnect_attempts", fallback=self.reconnect_attempts)
        self.reconnect_delay = settings.getfloat("reconnect_delay", fallback=self.reconnect_delay)
        self.use_exponential_backoff = strtobool(settings.get("use_exponential_backoff", fallback=self.use_exponential_backoff))
        self.max_reconnect_delay = settings.getfloat("max_reconnect_delay", fallback=self.max_reconnect_delay)

        # hypertable options copy + overrides from settings
        self.hypertable_options = dict(self.hypertable_options_default)
        self.hypertable_options["time_column"] = settings.get("time_column", fallback=self.hypertable_options["time_column"])
        self.hypertable_options["compress_orderby"] = settings.get("compress_orderby", fallback=self.hypertable_options["compress_orderby"])
        self.hypertable_options["compress_segmentby"] = settings.get("compress_segmentby", fallback=self.hypertable_options["compress_segmentby"])
        self.hypertable_options["chunk_time_interval"] = settings.get("chunk_time_interval", fallback=self.hypertable_options["chunk_time_interval"])
        self.hypertable_options["drop_after"] = settings.get("drop_after", fallback=self.hypertable_options["drop_after"])
        self.hypertable_options["migrate_data"] = strtobool(settings.get("migrate_data", fallback=str(self.hypertable_options["migrate_data"])))
        self.hypertable_options["if_not_exists"] = strtobool(settings.get("if_not_exists", fallback=str(self.hypertable_options["if_not_exists"])))

        # continuous aggregate buckets (configurable)
        self.hourly_bucket = settings.get("hourly_bucket", fallback=self.continuous_defaults["hourly_bucket"])
        self.daily_bucket = settings.get("daily_bucket", fallback=self.continuous_defaults["daily_bucket"])
        self.weekly_bucket = settings.get("weekly_bucket", fallback=self.continuous_defaults["weekly_bucket"])
        self.monthly_bucket = settings.get("monthly_bucket", fallback=self.continuous_defaults["monthly_bucket"])

        # rollup settings
        self.enable_rollups = strtobool(settings.get("enable_rollups", fallback=str(self.rollup_defaults["enable_rollups"])))
        self.daily_rollup_bucket = settings.get("daily_rollup_bucket", fallback=self.rollup_defaults["daily_rollup_bucket"])
        self.weekly_rollup_bucket = settings.get("weekly_rollup_bucket", fallback=self.rollup_defaults["weekly_rollup_bucket"])
        self.monthly_rollup_bucket = settings.get("monthly_rollup_bucket", fallback=self.rollup_defaults["monthly_rollup_bucket"])
        self.daily_rollup_start = settings.get("daily_rollup_start", fallback=self.rollup_defaults["daily_rollup_start"])
        self.weekly_rollup_start = settings.get("weekly_rollup_start", fallback=self.rollup_defaults["weekly_rollup_start"])
        self.monthly_rollup_start = settings.get("monthly_rollup_start", fallback=self.rollup_defaults["monthly_rollup_start"])

        # init runtime
        self.engine = None
        self.session = None
        self.connected = False

        self.batch_points: List[Dict[str, Any]] = []
        self.last_batch_time = 0.0

        # persistent backlog file path (per transport)
        os.makedirs(self.persistent_storage_path, exist_ok=True)
        self.backlog_file = os.path.join(self.persistent_storage_path, f"timescaledb_backlog_{self.transport_name}.json")
        self.backlog_points: List[Dict[str, Any]] = []
        if self.enable_persistent_storage:
            self._load_backlog()

        # threading
        self.flush_event = threading.Event()
        self.stop_event = threading.Event()
        self.flush_thread = threading.Thread(target=self._flush_worker, daemon=True)
        self.flush_thread.start()

        # attempt connection now
        try:
            self.connect()
        except Exception as e:
            self._log.error(f"[TimescaleDB] Initial connect failed: {e}")
            self.connected = False

    # -------------------------
    # Persistent backlog (disk)
    # -------------------------
    def _load_backlog(self):
        if not self.backlog_file or not os.path.exists(self.backlog_file):
            self.backlog_points = []
            return
        try:
            with open(self.backlog_file, "r", encoding="utf-8") as f:
                lines = f.readlines()
            points = []
            now = time.time()
            for ln in lines:
                try:
                    p = json.loads(ln)
                    if now - p.get("_backlog_time", now) < self.max_backlog_age:
                        points.append(p)
                except Exception:
                    continue
            self.backlog_points = points
            # rewrite file with only filtered items to avoid growth
            self._save_backlog()
            self._log.info(f"[TimescaleDB] Loaded {len(self.backlog_points)} backlog points from disk")
        except Exception as e:
            self._log.error(f"[TimescaleDB] Failed to load backlog file: {e}")
            self.backlog_points = []

    def _save_backlog(self):
        if not self.enable_persistent_storage or not self.backlog_file:
            return
        try:
            with open(self.backlog_file, "w", encoding="utf-8") as f:
                for p in self.backlog_points:
                    f.write(json.dumps(p, default=str) + "\n")
        except Exception as e:
            self._log.error(f"[TimescaleDB] Failed to save backlog: {e}")

    def _add_to_backlog(self, point: Dict[str, Any]):
        if not self.enable_persistent_storage:
            return
        point["_backlog_time"] = time.time()
        self.backlog_points.append(point)
        if len(self.backlog_points) > self.max_backlog_size:
            removed = self.backlog_points.pop(0)
            self._log.warning(f"[TimescaleDB] Backlog full — removed oldest point: {removed.get('metric_name', 'unknown')}")
        self._save_backlog()
        self._log.debug(f"[TimescaleDB] Added point to backlog. Backlog size: {len(self.backlog_points)}")

    def _flush_backlog(self):
        if not self.connected or not self.backlog_points:
            return
        self._log.info(f"[TimescaleDB] Flushing {len(self.backlog_points)} backlog points to DB")
        try:
            points = []
            for p in self.backlog_points:
                try:
                    sid = self._resolve_sensor_id(p.get("sensor_id"))
                    if not isinstance(sid, int):
                        self._log.debug(f"[TimescaleDB] Could not resolve backlog sensor_id: {p.get('sensor_id')}")
                        continue
                    points.append({
                        "sensor_id": sid,
                        "time": p.get("time", datetime.now().astimezone()),
                        "metric_name": p.get("metric_name"),
                        "metric_value": p.get("metric_value"),
                    })
                except Exception as e:
                    self._log.error(f"[TimescaleDB] Error preparing backlog point: {e}")
            if points:
                with self.engine.begin() as conn:
                    conn.execute(DeviceMetrics.__table__.insert(), points)
                self._log.info(f"[TimescaleDB] Flushed {len(points)} backlog points")
                self.backlog_points = []
                self._save_backlog()
        except Exception as e:
            self._log.error(f"[TimescaleDB] Failed to flush backlog: {e}")

    # -------------------------
    # DB creation & connect
    # -------------------------
    def _create_database_if_missing(self):
        try:
            self._log.debug(f"[TimescaleDB] Checking database '{self.database}' existence")
            default_url = f"postgresql+psycopg2://{self.username}:{self.password}@{self.host}:{self.port}/postgres"
            default_engine = create_engine(default_url, isolation_level="AUTOCOMMIT", pool_pre_ping=True)
            with default_engine.connect() as conn:
                row = conn.execute(text("SELECT 1 FROM pg_database WHERE datname = :d"), {"d": self.database}).fetchone()
                if not row:
                    self._log.info(f"[TimescaleDB] Database '{self.database}' not found — creating")
                    conn.execute(text(f'CREATE DATABASE "{self.database}"'))
                    self._log.info(f"[TimescaleDB] Database '{self.database}' created")
                else:
                    self._log.debug(f"[TimescaleDB] Database '{self.database}' already exists")
            default_engine.dispose()
        except Exception as e:
            self._log.error(f"[TimescaleDB] Failed to verify/create database '{self.database}': {e}")
            raise

    def connect(self): #, from_transport: Optional[transport_base] = None
        """
        Connect to DB and ensure schema/hypertable/policies exist.
        If from_transport provided, ensure device_info insert for that transport.
        """
        try:
            self._create_database_if_missing()

            url = f"postgresql+psycopg2://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
            self.engine = create_engine(url, pool_pre_ping=True, pool_recycle=3600)
            Session.configure(bind=self.engine)
            self.session = Session()
            self.connected = True
            self._log.info(f"[TimescaleDB] Connected to database '{self.database}'")

            # create ORM tables
            try:
                Base.metadata.create_all(self.engine)
                self._log.info("[TimescaleDB] ORM tables created/ensured")
            except Exception as e:
                self._log.error(f"[TimescaleDB] ORM table creation error: {e}")

            # timescaledb extension, hypertable, compression, retention
            try:
                with self.engine.connect() as conn:
                    conn.execute(text("CREATE EXTENSION IF NOT EXISTS timescaledb"))
                    sql_ht = (
                        "SELECT create_hypertable('device_metrics', "
                        f"'{self.hypertable_options.get('time_column','time')}', "
                        f"if_not_exists => {str(self.hypertable_options.get('if_not_exists', True)).lower()}, "
                        f"migrate_data => {str(self.hypertable_options.get('migrate_data', True)).lower()}"
                        ");"
                    )
                    conn.execute(text(sql_ht))

                    # enable compression
                    orderby = self.hypertable_options.get("compress_orderby", "time DESC")
                    segmentby = self.hypertable_options.get("compress_segmentby", "sensor_id,metric_name")
                    conn.execute(text(
                        "ALTER TABLE device_metrics SET ("
                        "timescaledb.compress, "
                        f"timescaledb.compress_orderby = '{orderby}', "
                        f"timescaledb.compress_segmentby = '{segmentby}'"
                        ");"
                    ))

                    # policies (idempotent)
                    interval = self.hypertable_options.get("chunk_time_interval", "7 days")
                    conn.execute(text(f"SELECT add_compression_policy('device_metrics', INTERVAL '{interval}', if_not_exists => TRUE);"))
                    drop_after = self.hypertable_options.get("drop_after", "1 year")
                    conn.execute(text(f"SELECT add_retention_policy('device_metrics', INTERVAL '{drop_after}', if_not_exists => TRUE);"))

                    # continuous aggregates (hourly/daily/optional weekly/monthly)
                    # hourly
                    conn.execute(text(f"""
                        CREATE MATERIALIZED VIEW IF NOT EXISTS device_metrics_hourly
                        WITH (timescaledb.continuous) AS
                        SELECT time_bucket('{self.hourly_bucket}', time) AS bucket,
                               sensor_id,
                               metric_name,
                               AVG(metric_value) AS avg_value,
                               MIN(metric_value) AS min_value,
                               MAX(metric_value) AS max_value
                        FROM device_metrics
                        GROUP BY bucket, sensor_id, metric_name;
                    """))
                    conn.execute(text(f"""
                        SELECT add_continuous_aggregate_policy('device_metrics_hourly',
                            start_offset => INTERVAL '1 day',
                            end_offset => INTERVAL '{self.hourly_bucket}',
                            schedule_interval => INTERVAL '{self.hourly_bucket}',
                            if_not_exists => TRUE);
                    """))

                    # daily
                    conn.execute(text(f"""
                        CREATE MATERIALIZED VIEW IF NOT EXISTS device_metrics_daily
                        WITH (timescaledb.continuous) AS
                        SELECT time_bucket('{self.daily_bucket}', time) AS bucket,
                               sensor_id,
                               metric_name,
                               AVG(metric_value) AS avg_value,
                               MIN(metric_value) AS min_value,
                               MAX(metric_value) AS max_value
                        FROM device_metrics
                        GROUP BY bucket, sensor_id, metric_name;
                    """))
                    conn.execute(text(f"""
                        SELECT add_continuous_aggregate_policy('device_metrics_daily',
                            start_offset => INTERVAL '7 days',
                            end_offset => INTERVAL '{self.daily_bucket}',
                            schedule_interval => INTERVAL '{self.daily_bucket}',
                            if_not_exists => TRUE);
                    """))

                    # optional weekly/monthly from raw table
                    if self.weekly_bucket:
                        conn.execute(text(f"""
                            CREATE MATERIALIZED VIEW IF NOT EXISTS device_metrics_weekly
                            WITH (timescaledb.continuous) AS
                            SELECT time_bucket('{self.weekly_bucket}', time) AS bucket,
                                   sensor_id,
                                   metric_name,
                                   AVG(metric_value) AS avg_value,
                                   MIN(metric_value) AS min_value,
                                   MAX(metric_value) AS max_value
                            FROM device_metrics
                            GROUP BY bucket, sensor_id, metric_name;
                        """))
                        conn.execute(text(f"""
                            SELECT add_continuous_aggregate_policy('device_metrics_weekly',
                                start_offset => INTERVAL '30 days',
                                end_offset => INTERVAL '{self.weekly_bucket}',
                                schedule_interval => INTERVAL '{self.weekly_bucket}',
                                if_not_exists => TRUE);
                        """))

                    if self.monthly_bucket:
                        conn.execute(text(f"""
                            CREATE MATERIALIZED VIEW IF NOT EXISTS device_metrics_monthly
                            WITH (timescaledb.continuous) AS
                            SELECT time_bucket('{self.monthly_bucket}', time) AS bucket,
                                   sensor_id,
                                   metric_name,
                                   AVG(metric_value) AS avg_value,
                                   MIN(metric_value) AS min_value,
                                   MAX(metric_value) AS max_value
                            FROM device_metrics
                            GROUP BY bucket, sensor_id, metric_name;
                        """))
                        conn.execute(text(f"""
                            SELECT add_continuous_aggregate_policy('device_metrics_monthly',
                                start_offset => INTERVAL '90 days',
                                end_offset => INTERVAL '{self.monthly_bucket}',
                                schedule_interval => INTERVAL '{self.monthly_bucket}',
                                if_not_exists => TRUE);
                        """))

                    # create rollup aggregates (chained) if enabled
                    try:
                        self._create_rollup_aggregates(conn)
                    except Exception as e:
                        self._log.error(f"[TimescaleDB] Rollup creation error: {e}")

                self._log.info("[TimescaleDB] Hypertable, compression, retention and aggregates ensured")
            except Exception as e:
                self._log.error(f"[TimescaleDB] Error ensuring hypertable/policies/aggregates: {e}")

            # flush persisted backlog if any
            if self.enable_persistent_storage:
                self._flush_backlog()

            # optionally ensure device_info record for from_transport
            #if from_transport is not None:
            #    try:
            #        self._ensure_device_info(from_transport)
            #    except Exception as e:
            #        self._log.error(f"[TimescaleDB] _ensure_device_info error: {e}")

        except Exception as e:
            self.connected = False
            self._log.error(f"[TimescaleDB] connect() failed: {e}")
            raise

    # -------------------------
    # Rollup creation helper
    # -------------------------
    def _create_rollup_aggregates(self, conn):
        """
        Create chained rollup continuous aggregates:
        - device_metrics_daily_rollup (from device_metrics_hourly)
        - device_metrics_weekly_rollup (from device_metrics_daily_rollup)
        - device_metrics_monthly_rollup (from device_metrics_weekly_rollup)
        This uses config options:
          - self.enable_rollups
          - self.daily_rollup_bucket, weekly_rollup_bucket, monthly_rollup_bucket
          - self.daily_rollup_start, weekly_rollup_start, monthly_rollup_start
        """
        if not getattr(self, "enable_rollups", True):
            self._log.debug("[TimescaleDB] Rollups are disabled by config")
            return

        def _create_rollup(source_view: str, rollup_view: str, bucket_interval: str, start_offset: str, schedule_interval: str):
            try:
                conn.execute(text(f"""
                    CREATE MATERIALIZED VIEW IF NOT EXISTS {rollup_view}
                    WITH (timescaledb.continuous) AS
                    SELECT time_bucket('{bucket_interval}', bucket) AS bucket,
                           sensor_id,
                           metric_name,
                           AVG(avg_value) AS avg_value,
                           MIN(min_value) AS min_value,
                           MAX(max_value) AS max_value
                    FROM {source_view}
                    GROUP BY bucket, sensor_id, metric_name;
                """))
                conn.execute(text(f"""
                    SELECT add_continuous_aggregate_policy('{rollup_view}',
                        start_offset => INTERVAL '{start_offset}',
                        end_offset => INTERVAL '{schedule_interval}',
                        schedule_interval => INTERVAL '{schedule_interval}',
                        if_not_exists => TRUE);
                """))
                self._log.info(f"[TimescaleDB] Ensured rollup {rollup_view} from {source_view} (bucket={bucket_interval})")
            except Exception as e:
                # some TimescaleDB versions may not support if_not_exists on add_continuous_aggregate_policy;
                # log but continue
                self._log.warning(f"[TimescaleDB] Failed to ensure rollup {rollup_view}: {e}")

        # DAILY ROLLUP: hourly -> daily_rollup
        if self.daily_rollup_bucket:
            _create_rollup("device_metrics_hourly", "device_metrics_daily_rollup", self.daily_rollup_bucket, self.daily_rollup_start, self.daily_rollup_bucket)

        # WEEKLY ROLLUP: daily_rollup -> weekly_rollup (fallback to device_metrics_daily if daily_rollup missing)
        if self.weekly_rollup_bucket:
            source = "device_metrics_daily_rollup" if self.daily_rollup_bucket else "device_metrics_daily"
            _create_rollup(source, "device_metrics_weekly_rollup", self.weekly_rollup_bucket, self.weekly_rollup_start, self.weekly_rollup_bucket)

        # MONTHLY ROLLUP: weekly_rollup -> monthly_rollup (fallback cascade)
        if self.monthly_rollup_bucket:
            if self.weekly_rollup_bucket:
                source = "device_metrics_weekly_rollup"
            elif self.daily_rollup_bucket:
                source = "device_metrics_daily_rollup"
            else:
                source = "device_metrics_daily"
            _create_rollup(source, "device_metrics_monthly_rollup", self.monthly_rollup_bucket, self.monthly_rollup_start, self.monthly_rollup_bucket)

    # -------------------------
    # Ensure single device_info entry
    # -------------------------
    def _ensure_device_info(self, from_transport: transport_base):
        if not self.include_device_info or not self.connected:
            return
        try:
            existing = self.session.execute(
                select(DeviceInfo).where(
                    (DeviceInfo.device_identifier == from_transport.device_identifier) &
                    (DeviceInfo.device_name == from_transport.device_name) &
                    (DeviceInfo.device_manufacturer == from_transport.device_manufacturer) &
                    (DeviceInfo.device_model == from_transport.device_model) &
                    (DeviceInfo.device_serial_number == from_transport.device_serial_number) &
                    (DeviceInfo.transport == from_transport.transport_name)
                )
            ).scalar_one_or_none()
            if existing:
                self._log.debug("[TimescaleDB] Exact device_info exists — skipping insert")
                return
            dev = DeviceInfo(
                device_identifier=from_transport.device_identifier,
                device_name=from_transport.device_name,
                device_manufacturer=from_transport.device_manufacturer,
                device_model=from_transport.device_model,
                device_serial_number=from_transport.device_serial_number,
                transport=from_transport.transport_name,
                created_at=datetime.now().astimezone(),
            )
            self.session.add(dev)
            self.session.commit()
            self._log.info(f"[TimescaleDB] Inserted device_info for {from_transport.device_identifier}")
        except Exception as e:
            self._log.error(f"[TimescaleDB] _ensure_device_info error: {e}")
            try:
                self.session.rollback()
            except Exception:
                pass

    # -------------------------
    # Data collection (main entry)
    # -------------------------
    def collect_and_queue_metrics(self, data: Dict[str, Any], from_transport: transport_base):
        """
        data: live readings dict from polling callback (variable_name -> value)
        from_transport: transport object (for device metadata and protocolSettings registry maps)
        """
        try:
            if not self.connected:
                self._log.warning("[TimescaleDB] Not connected — storing points to persistent backlog")
                now = datetime.now().astimezone()
                for key, raw_value in data.items():
                    try:
                        val = float(raw_value) if self.force_float else raw_value
                    except Exception:
                        val = raw_value
                    point = {
                        "sensor_id": from_transport.device_identifier,
                        "time": now.isoformat(),
                        "metric_name": key,
                        "metric_value": val,
                    }
                    self._add_to_backlog(point)
                return

            # upsert device to get numeric PK sensor_id
            device_id = from_transport.device_identifier
            if self.include_device_info:
                try:
                    device_id = self._upsert_device(from_transport)
                except Exception as e:
                    self._log.warning(f"[TimescaleDB] _upsert_device failed; using identifier: {e}")
                    device_id = from_transport.device_identifier

            # load registry metadata for validation (optional)
            registry_metadata = {}
            try:
                for registry_type in [Registry_Type.INPUT, Registry_Type.HOLDING]:
                    registry_map = from_transport.protocolSettings.get_registry_map(registry_type)
                    for entry in registry_map:
                        registry_metadata[entry.variable_name] = entry
            except Exception:
                self._log.debug("[TimescaleDB] Registry metadata not available")

            now = datetime.now().astimezone()
            for key, raw_value in data.items():
                meta = registry_metadata.get(key)
                try:
                    if meta and hasattr(meta, "data_type"):
                        dt = str(meta.data_type).lower()
                        if dt in ("int", "integer"):
                            value = int(raw_value)
                        elif dt in ("float", "double"):
                            value = float(raw_value)
                        else:
                            value = raw_value
                    else:
                        value = float(raw_value) if self.force_float else raw_value

                    point = {
                        "sensor_id": device_id,
                        "time": now,
                        "metric_name": key,
                        "metric_value": value,
                    }
                    self.batch_points.append(point)
                except Exception as e:
                    self._log.error(f"[TimescaleDB] Failed to process metric '{key}': {e}")

            # trigger async flush
            if len(self.batch_points) >= self.batch_size or (time.time() - self.last_batch_time) >= self.batch_timeout:
                self.flush_event.set()

        except Exception as e:
            self._log.error(f"[TimescaleDB] collect_and_queue_metrics outer error: {e}")

    # -------------------------
    # Resolve sensor id helper
    # -------------------------
    def _resolve_sensor_id(self, sensor_ref):
        try:
            if isinstance(sensor_ref, int):
                return sensor_ref
            row = self.session.execute(select(DeviceInfo).where(DeviceInfo.device_identifier == sensor_ref)).scalar_one_or_none()
            if row:
                return row.id
            return sensor_ref
        except Exception:
            return sensor_ref

    # -------------------------
    # Async flush worker
    # -------------------------
    def _flush_worker(self):
        self._log.debug("[TimescaleDB] Flush worker started")
        while not self.stop_event.is_set():
            self.flush_event.wait(self.batch_timeout)
            try:
                if self.batch_points:
                    self._flush_batch()
                if self.enable_persistent_storage and self.backlog_points and self.connected:
                    self._flush_backlog()
            except Exception as e:
                self._log.error(f"[TimescaleDB] Exception in flush worker: {e}")
            finally:
                self.flush_event.clear()
        self._log.debug("[TimescaleDB] Flush worker stopped")

    def _flush_batch(self):
        if not self.batch_points:
            return
        if not self.connected:
            self._log.debug("[TimescaleDB] Not connected — moving batch to backlog")
            for p in self.batch_points:
                self._add_to_backlog(p)
            self.batch_points = []
            return

        points_to_insert = []
        for p in self.batch_points:
            try:
                sid = self._resolve_sensor_id(p.get("sensor_id"))
                if not isinstance(sid, int):
                    self._log.debug(f"[TimescaleDB] Could not resolve sensor_id '{p.get('sensor_id')}', adding to backlog")
                    self._add_to_backlog(p)
                    continue
                points_to_insert.append({
                    "sensor_id": sid,
                    "time": p.get("time", datetime.now().astimezone()),
                    "metric_name": p.get("metric_name"),
                    "metric_value": p.get("metric_value"),
                })
            except Exception as e:
                self._log.error(f"[TimescaleDB] _flush_batch prepare error: {e}")
                self._add_to_backlog(p)

        # clear current batch; we will re-add failed inserts into backlog
        self.batch_points = []

        if not points_to_insert:
            return

        try:
            with self.engine.begin() as conn:
                conn.execute(DeviceMetrics.__table__.insert(), points_to_insert)
            self.last_batch_time = time.time()
            self._log.info(f"[TimescaleDB] Flushed {len(points_to_insert)} points to device_metrics")
        except SQLAlchemyError as e:
            self._log.error(f"[TimescaleDB] Failed to write batch: {e}")
            for p in points_to_insert:
                self._add_to_backlog(p)
            self.connected = False
            threading.Thread(target=self._attempt_reconnect, daemon=True).start()
        except Exception as e:
            self._log.error(f"[TimescaleDB] Unexpected error flushing batch: {e}")
            for p in points_to_insert:
                self._add_to_backlog(p)
            self.connected = False
            threading.Thread(target=self._attempt_reconnect, daemon=True).start()

    # -------------------------
    # reconnect/backoff
    # -------------------------
    def _attempt_reconnect(self):
        self._log.info("[TimescaleDB] Attempting reconnect")
        for attempt in range(self.reconnect_attempts):
            try:
                self.connect()
                if self.connected:
                    self._log.info("[TimescaleDB] Reconnected")
                    if self.enable_persistent_storage:
                        self._flush_backlog()
                    return True
            except Exception as e:
                self._log.warning(f"[TimescaleDB] Reconnect attempt {attempt+1} failed: {e}")
            if attempt < self.reconnect_attempts - 1:
                delay = self.reconnect_delay
                if self.use_exponential_backoff:
                    delay = min(self.reconnect_delay * (2 ** attempt), self.max_reconnect_delay)
                self._log.info(f"[TimescaleDB] Waiting {delay:.1f}s before next reconnect")
                time.sleep(delay)
        self._log.error("[TimescaleDB] All reconnect attempts failed")
        self.connected = False
        return False

    # -------------------------
    # Device upsert
    # -------------------------
    def _upsert_device(self, from_transport: transport_base) -> int:
        if not self.session:
            raise RuntimeError("DB session not initialized")
        try:
            device_identifier = from_transport.device_identifier
            existing = self.session.execute(select(DeviceInfo).where(DeviceInfo.device_identifier == device_identifier)).scalar_one_or_none()
            if existing:
                return existing.id
            rec = DeviceInfo(
                device_identifier=device_identifier,
                device_name=getattr(from_transport, "device_name", None),
                device_manufacturer=getattr(from_transport, "device_manufacturer", None),
                device_model=getattr(from_transport, "device_model", None),
                device_serial_number=getattr(from_transport, "device_serial_number", None),
                transport=getattr(from_transport, "transport_name", None),
                created_at=datetime.now().astimezone(),
            )
            self.session.add(rec)
            self.session.commit()
            self._log.info(f"[TimescaleDB] Inserted DeviceInfo for {device_identifier}")
            return rec.id
        except Exception as e:
            self._log.error(f"[TimescaleDB] _upsert_device error: {e}")
            try:
                self.session.rollback()
            except Exception:
                pass
            raise

    # -------------------------
    # Close / cleanup
    # -------------------------
    def close(self):
        self._log.debug("[TimescaleDB] Closing transport")
        try:
            self.stop_event.set()
            self.flush_event.set()
            if self.flush_thread and self.flush_thread.is_alive():
                self.flush_thread.join(timeout=5.0)
        except Exception as e:
            self._log.error(f"[TimescaleDB] Error stopping flush thread: {e}")

        try:
            if self.session:
                self.session.close()
        except Exception as e:
            self._log.error(f"[TimescaleDB] Error closing session: {e}")

        try:
            if self.engine:
                self.engine.dispose()
        except Exception as e:
            self._log.error(f"[TimescaleDB] Error disposing engine: {e}")

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass
