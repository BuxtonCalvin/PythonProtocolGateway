# Prometheus Output Transport

The Prometheus output transport exposes data from your devices as a `/metrics` endpoint for a Prometheus server to scrape. Unlike the InfluxDB or TimescaleDB transports, this is a **pull-model** transport: instead of pushing batches out over the network on a schedule it controls, this bridge just keeps an in-memory metrics registry up to date, and an external Prometheus server decides when to read it.

## How It Differs From Push Transports

Every other output transport in this codebase (`influxdb_out`, `timescaledb`, `mqtt`) is a *push* transport — the gateway's own worker loops decide when data leaves the process, batch it, and send it over the network. Prometheus inverts that:

- **Background (this bridge's job)**: every scraper's `write_data()` call — one per completed read cycle, at whatever `read_interval` that scraper is configured with — updates an in-memory `prometheus_client` registry. This is pure in-process object mutation. No network I/O, no batching, nothing to reconnect.
- **External I/O (the Prometheus server's job)**: a Prometheus server scrapes `/metrics` on its own schedule and reads straight out of that same in-memory registry.

Because each metric object retains whatever value was last set, a slow 60-second-interval device simply keeps re-serving its last known reading to every 10-second Prometheus scrape in between — there's no separate cache to keep in sync; the metric object *is* the cache.

This also means there's no connection to lose, no reconnect logic, and no backlog/persistent-storage story the way the InfluxDB transport has — if nobody scrapes `/metrics` for a while, values just sit in memory until someone does.

## Features

- **Pull-model exposition**: serves a standard `/metrics` endpoint for any Prometheus-compatible scraper
- **Dynamic metric typing**: creates Gauge, Counter, or Histogram metrics lazily on first sight of a field name — no metric schema to predefine
- **Multi-machine labeling**: every machine reporting the same field name (e.g. `voltage`) shares one metric name (`device_voltage`), distinguished by a `device_name` label — so PromQL queries and Grafana panels work across a whole fleet without per-machine edits
- **Automatic name sanitizing**: arbitrary protocol field names (e.g. `"Battery Voltage (V)"`) are converted to Prometheus-legal metric names (e.g. `device_battery_voltage_v_`)
- **Safe value coercion**: numeric fields are converted to floats; non-numeric fields (strings, `None`, NaN/Inf) are skipped without crashing the write cycle
- **Counter delta handling**: counter fields report absolute snapshots (like a meter reading), and the bridge computes the `.inc()` delta itself, treating a decrease as a device reset rather than an error
- **Target-health tracking**: tracks per-machine staleness, scrape counts, and failure counts for a diagnostics dashboard

## Configuration

### Basic Configuration

```ini
[prometheus_output]
type = prometheus_out
metric_prefix = device_
metrics_path = /metrics
```

### Advanced Configuration

```ini
[prometheus_output]
type = prometheus_out

# Metric naming / labeling
metric_prefix = device_
include_protocol_label = true
max_label_value_length = 128
counter_metric_suffixes = _total
histogram_fields =
histogram_buckets =

# Mount path (this bridge is always mounted onto the WebServer's own
# FastAPI app, on whatever port the web UI already runs on, e.g. 1717 —
# see classes/WebServer/routers/bridges.py's mount_prometheus_bridges())
metrics_path = /metrics

# Every bridge gets its own dedicated listening socket, in addition to
# the main WebServer port -- this is NOT a second server, it's one extra
# socket on the SAME uvicorn.Server/event loop, serving the exact same
# app (config UI included) as the main port -- there is no access
# restriction on it, and no way to disable it / share the main port
# instead. Default: 9110. Set this explicitly to change it, or to run
# more than one Prometheus bridge on the same host without a clash.
metrics_port = 9110

# Staleness / target-health monitoring
staleness_multiplier = 3.0
stale_check_interval = 5.0
```

### Configuration Options

| Option | Default | Description |
| --- | --- | --- |
| `metric_prefix` | `device_` | Prefix applied to every device metric name, e.g. field `voltage` → `device_voltage` |
| `include_protocol_label` | `true` | Add a second `protocol` label alongside the mandatory `device_name` label |
| `max_label_value_length` | `128` | Truncate label values longer than this, to avoid bloating cardinality with raw device strings |
| `counter_metric_suffixes` | `_total` | Comma-separated field-name suffixes treated as Prometheus Counters rather than Gauges |
| `histogram_fields` | `` (none) | Comma-separated exact field names to record as Histograms instead of Gauges — opt-in only, never inferred automatically |
| `histogram_buckets` | `` (default buckets) | Comma-separated float bucket boundaries for histogram fields |
| `metrics_path` | `/metrics` | Path this bridge's metrics are served under, on both the main WebServer port and its own dedicated `metrics_port` |
| `metrics_port` | `9110` | This bridge's own dedicated port, always active — one extra socket on the same server/event loop, not a second process, not access-restricted, not optional. See "Dedicated Metrics Port" below |
| `staleness_multiplier` | `3.0` | A machine is flagged stale once this many multiples of its own `read_interval` have elapsed since its last write |
| `staleness_check_interval` | `5.0` | Seconds between background staleness sweeps |
| `device_name` | `Prometheus MPG Bridge` | Name for the bridge itself, used only in logs/notifications — distinct from any individual machine's `device_name` label value |

The dashboard also shows a `host`/`port` for this bridge, but those aren't settings you configure — see "Dashboard Host/Port Display" below.

## Dedicated Metrics Port

Every `prometheus_out` bridge gets its own dedicated listening socket, in addition to the main MPG web UI port (1717 by default). Out of the box, before you set anything, that dedicated port is **9110**:

```ini
[prometheus_output]
type = prometheus_out
metrics_path = /metrics
metrics_port = 9500
```

**This is not a second web server.** It's the exact same `uvicorn.Server`, event loop, thread, and FastAPI app as the main UI — just one more bound socket (see `classes/transports/prometheus_out.py`'s `collect_metrics_ports()`/`mount_bridges()`, wired up from `classes/WebServer/routers/bridges.py`'s `collect_prometheus_metrics_ports()`/`mount_prometheus_bridges()`, and called by `classes/WebServer/main.py`'s `start_webserver()`, which does the actual binding). **It is not access-restricted**: the dedicated port serves the exact same app as the main port, config UI included — anyone who can reach `9500` in the example above can reach everything `1717` can. If you want that dedicated port reachable from a monitoring network without exposing the config UI there too, put a firewall or reverse proxy in front of MPG; that separation is intentionally left outside this codebase rather than implemented as an in-process ASGI restriction.

**There is no way to turn this off or share the main WebServer port instead.** Change `metrics_port` if 9110 doesn't suit you (e.g. it clashes with something else on the host, or you're running more than one Prometheus bridge and need each on a distinct port) — but every bridge always has a dedicated port.

If more than one Prometheus bridge is configured with the same `metrics_port`, they share that one socket; each bridge's own `metrics_path` still needs to be distinct (see the config-loading error logged otherwise). If the port is already in use by something else on the host, MPG logs an error at startup and that bridge simply falls back to being reachable only on the main WebServer port — it does not crash the whole process. (The one exception: if `metrics_port` happens to equal the main WebServer's own port, MPG recognizes that as "already covered" and logs an informational message instead of an error — no separate socket is needed in that case, since the main port already serves it.)

## Dashboard Host/Port Display

The "Configured Devices" dashboard (the app's home page) shows a Host column for every scraper and bridge. For most bridges (timescaledb, influxdb3_out, etc.) that column is a real, literal `host`/`port` config key the bridge connects *out* to. `prometheus_out` is different — it doesn't connect out anywhere, it's *scraped*, so those two keys don't exist there naturally.

To make the dashboard show something meaningful anyway, the config scanner (`classes/WebServer/scanner.py`) automatically **derives** `host`/`port` for every `prometheus_out` section on every scan, delegating the actual derivation to `classes/transports/prometheus_out.py`'s `derive_dashboard_host_port()`:

- `host` is always `0.0.0.0`.
- `port` is `metrics_port` if it's set in `config.cfg`, otherwise this bridge's own default dedicated port (9110, see that module's `DEFAULT_METRICS_PORT`) -- this fallback only matters for a `config.cfg` section that predates every bridge getting a dedicated port by default and has no `metrics_port` line at all yet; scanning it once writes that line in for you (see `get_known_transport_keys()`/`transport_defaults.json`).

This happens on every scan (startup, or whenever `config.cfg` changes and MPG rescans) — so if you change `metrics_port`, the dashboard's `port` updates automatically the next time MPG rescans. There's nothing to keep in sync by hand.

**`host`/`port` are not independently configurable for this bridge.** If you write literal `host = ...` / `port = ...` lines under `[prometheus_output]` in `config.cfg`, they're intentionally ignored — the scanner overwrites them with the derived value on the next scan. `metrics_port` (above) is the only setting that actually controls this bridge's serving address; `host`/`port` are a read-only reflection of it, purely for the dashboard.

## Metric Type Classification

The bridge doesn't know ahead of time what fields a given protocol will send, so it classifies each field the first time it's seen, most specific rule first:

1. **Histogram** — if the field name is explicitly listed in `histogram_fields`. A single scalar reading has no natural histogram semantics on its own, so this is never inferred automatically.
2. **Counter** — if the field name ends with one of `counter_metric_suffixes` (default `_total`), following Prometheus's own naming convention for monotonic counters.
3. **Gauge** — everything else. This is the safe default for arbitrary sensor readings that can legitimately go up or down.

Once a field is classified, that metric object is reused for every future write from any machine — a name/type mismatch (e.g. a config change that later adds an already-seen field to `histogram_fields`) is caught and logged rather than crashing the collection loop.

### Counter Semantics

Prometheus's `Counter` type only supports `.inc()`, not `.set()`, but incoming values are always absolute snapshots (like a meter reading), never pre-computed deltas. The bridge bridges that gap itself:

- It remembers the last absolute value seen per metric+label combination.
- On each write, it computes `delta = new_value - previous_value` and calls `.inc(delta)`.
- If the new value is *lower* than the previous one, that's treated as a counter reset (device reboot, meter rollover) — logged at debug level, with the new value stored as the fresh baseline. Nothing is decremented and nothing raises.

## Data Structure

### Labels

Every metric this bridge creates carries:

- `device_name` — always present, always first. This is what distinguishes two machines reporting the same field name under one shared metric.
- `protocol` — included only if `include_protocol_label = true` (default).

### Metric Names

Field names are sanitized to Prometheus's legal character set (`[a-zA-Z_:][a-zA-Z0-9_:]*`): disallowed characters become underscores, a leading digit gets an underscore prefix, and an empty result falls back to `unnamed_metric`. The configured `metric_prefix` is prepended before this sanitizing is finalized.

### Internal Bridge Telemetry

Alongside device metrics, the bridge exposes its own health metrics, labeled by `machine_id` (the scraper's `transport_name`, not `device_name`, so history survives a `device_name` edit):

- `mpg_prometheus_bridge_scrape_duration_seconds` — time spent processing one `write_data()` cycle for a machine
- `mpg_prometheus_bridge_last_scrape_timestamp_seconds` — Unix timestamp of a machine's last successful write
- `mpg_prometheus_bridge_scrape_failures_total` — count of stale *transitions* for a machine (an hour-long outage counts once, not once per staleness sweep)

## Example Bridge Configuration

```ini
# Source device (e.g., Modbus RTU)
[growatt_inverter]
type = modbus_rtu
port = /dev/ttyUSB0
baudrate = 9600
protocol_version = growatt_2020_v1.24
device_serial_number = 123456789
device_manufacturer = Growatt
device_model = SPH3000
read_interval = 10
bridge = prometheus_output

# Prometheus output
[prometheus_output]
type = prometheus_out
metric_prefix = device_
metrics_path = /metrics
```

## Installation  (if not using docker)

1. Install the required dependency:

   ```bash
   pip install prometheus_client
   ```

2. Or add to your requirements.txt:

   ```ini
   prometheus_client
   ```

This uses `prometheus_client.make_asgi_app()` under the hood, auto-mounted at `bridge.metrics_path` (default `/metrics`) onto the WebServer's own FastAPI app by `classes/transports/prometheus_out.py`'s `mount_bridges()` (wired up from `classes/WebServer/routers/bridges.py`'s `mount_prometheus_bridges()`, called from `classes/WebServer/main.py` at startup) — every configured `prometheus_out` bridge is picked up automatically at startup, on both the main web UI port (default 1717) and its own dedicated `metrics_port` (default 9110). No separate process is needed, since MPG always runs its WebServer. If you mount more than one Prometheus bridge, give each a distinct `metrics_path`.

## Prometheus Server Setup

Point a Prometheus server's scrape config at the MPG WebServer's own host/port:

```yaml
scrape_configs:
  - job_name: "mpg_devices"
    scrape_interval: 15s
    static_configs:
      - targets: ["localhost:1717"]
    metrics_path: /metrics
```

Set `scrape_interval` to something reasonable relative to your fastest device's `read_interval` — scraping faster than data changes just re-reads the same cached values.

## Querying Data

Once Prometheus is scraping successfully, query with PromQL:

```promql
# Current value of a gauge across all devices
device_battery_voltage

# A specific machine
device_battery_voltage{device_name="inverter_1"}

# Rate of increase for a counter field over 5 minutes
rate(device_energy_total[5m])

# Which machines have gone stale
mpg_prometheus_bridge_scrape_failures_total
```

## Integration with Grafana

1. Add Prometheus as a data source in Grafana, pointing at your Prometheus server (not this bridge directly)
2. Build panels using PromQL, filtering/grouping by the `device_name` label to compare machines
3. Because every machine shares metric names, one dashboard panel works across your whole fleet with no per-machine edits

## Troubleshooting

### No Data Appearing in Prometheus

- Confirm the endpoint is reachable: `curl http://<mpg-host>:1717/metrics` (main WebServer port) or `curl http://<mpg-host>:<metrics_port>/metrics` (this bridge's own dedicated port, default 9110)
- Check the MPG startup logs for a line like `Prometheus bridge '...' mounted at /metrics on the web UI app` — if it's missing, confirm `[prometheus_output]` has `type = prometheus_out` and `prometheus_client` is installed
- If `metrics_port` isn't reachable, check the startup logs for a bind error (port already in use) — MPG falls back to the main port only in that case, without crashing; look for `Could not bind Prometheus metrics_port ...`
- `metrics_port` serves the exact same app as the main WebServer port — including the config UI — since there's no access restriction on it (see "Dedicated Metrics Port" above); if you expected it to serve *only* metrics, that's a firewall/reverse-proxy job in front of MPG, not something MPG does for you
- Verify your Prometheus server's `scrape_configs` target matches either the MPG WebServer's actual host/port (default 1717) or this bridge's dedicated `metrics_port` (default 9110), and this bridge's `metrics_path`
- Check Prometheus's own "Targets" page (`<prometheus>:9090/targets`) for scrape errors

### A Field Isn't Showing Up

- Non-numeric fields (strings, `None`) are silently skipped — check `last_skipped_count` in the target-health data, or enable debug logging to see per-field skip messages
- NaN/Inf values are also dropped rather than exposed, to avoid poisoning PromQL aggregations

### A Machine Shows as Stale / `connected: false`

- Staleness is judged as `elapsed_since_last_write > read_interval * staleness_multiplier`. A slow-polling device with a long `read_interval` needs proportionally longer before it's flagged — this is expected, not a bug
- A machine with `read_interval == 0` (never loaded a read interval) is never flagged stale; it's reported connected purely on whether it has ever sent anything
- Check `mpg_prometheus_bridge_scrape_failures_total{machine_id="..."}` for a history of stale transitions

### Duplicate Metric Name / Type Collision

**Symptoms:** a debug log line like `Skipped metric '...' (gauge) for labels ...: <error>`

**Cause:** the same field name was first seen as one metric kind (e.g. Gauge) and a later config change (e.g. adding it to `histogram_fields`) tries to register it under a different kind. Prometheus doesn't allow one name to back two types.

**Solution:** pick one classification for a given field name and keep `histogram_fields` / `counter_metric_suffixes` consistent across restarts, or rename the field.

### High Label Cardinality

**Symptoms:** slow scrapes, large `/metrics` payloads, high Prometheus memory usage

**Solutions:**

- Lower `max_label_value_length` if raw device strings are leaking into the `device_name` or `protocol` label
- Keep `device_name` values stable and human-scale (a handful of machines, not one label value per reading)
- Avoid feeding high-cardinality data (raw timestamps, unique IDs) into fields that get exposed as metrics

## Comparison With the Push Transports

| | InfluxDB / TimescaleDB (push) | Prometheus (pull) |
| --- | --- | --- |
| Who decides transmission timing | The gateway, on `batch_size`/`batch_timeout` | The external Prometheus server's scrape interval |
| Network I/O in the bridge itself | Yes — outbound writes, with retry/backoff | No — the bridge only serves inbound HTTP reads |
| Data loss during an outage | Backlog/persistent storage needed to avoid it | None to lose — the metric just holds its last value until scraped |
| Reconnection logic | Yes (exponential backoff, periodic reconnect) | Not applicable — there's no connection to the destination to lose |
| "Is data flowing" signal | Reconnect/backlog logs | Target-health state + `mpg_prometheus_bridge_*` metrics |
