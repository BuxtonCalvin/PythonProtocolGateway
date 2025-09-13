# InfluxDB Output Transport

The InfluxDB output transport allows you to send data from your devices directly to an InfluxDB v1 server for time-series data storage and visualization.

## Features

- **Batch Writing**: Efficiently batches data points to reduce network overhead
- **Automatic Database Creation**: Creates the database if it doesn't exist
- **Device Information Tags**: Includes device metadata as InfluxDB tags for easy querying
- **Flexible Data Types**: Automatically converts data to appropriate InfluxDB field types
- **Configurable Timeouts**: Adjustable batch size and timeout settings
- **Connection Monitoring**: Automatic connection health checks and reconnection logic
- **Robust Error Handling**: Retries failed writes after reconnection attempts

## Configuration

### Basic Configuration

```ini
[influxdb_output]
type = influxdb_out
host = localhost
port = 8086
database = solar
measurement = device_data
```

### Advanced Configuration

```ini
[influxdb_output]
type = influxdb_out
host = localhost
port = 8086
database = solar
username = admin
password = your_password
measurement = device_data
include_timestamp = true
include_device_info = true
batch_size = 100
batch_timeout = 10.0
log_level = INFO

# Connection monitoring settings
reconnect_attempts = 5
reconnect_delay = 5.0
connection_timeout = 10
```

### Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `host` | `localhost` | InfluxDB server hostname or IP address |
| `port` | `8086` | InfluxDB server port |
| `database` | `solar` | Database name (will be created if it doesn't exist) |
| `username` | `` | Username for authentication (optional) |
| `password` | `` | Password for authentication (optional) |
| `measurement` | `device_data` | InfluxDB measurement name |
| `include_timestamp` | `true` | Include timestamp in data points |
| `include_device_info` | `true` | Include device information as tags |
| `batch_size` | `100` | Number of points to batch before writing |
| `batch_timeout` | `10.0` | Maximum time (seconds) to wait before flushing batch |
| `reconnect_attempts` | `5` | Number of reconnection attempts before giving up |
| `reconnect_delay` | `5.0` | Delay between reconnection attempts (seconds) |
| `connection_timeout` | `10` | Connection timeout for InfluxDB client (seconds) |

## Connection Monitoring

The InfluxDB transport includes robust connection monitoring to handle network issues and server restarts:

### Automatic Health Checks
- Performs connection health checks every 30 seconds
- Uses InfluxDB ping command to verify connectivity
- Automatically attempts reconnection if connection is lost

### Reconnection Logic
- Attempts reconnection up to `reconnect_attempts` times
- Waits `reconnect_delay` seconds between attempts
- Preserves buffered data during reconnection attempts
- Retries failed writes after successful reconnection

### Error Recovery
- Gracefully handles network timeouts and connection drops
- Maintains data integrity by not losing buffered points
- Provides detailed logging for troubleshooting

## Data Structure

The InfluxDB output creates data points with the following structure:

### Tags (if `include_device_info = true`)
- `device_identifier`: Device serial number (lowercase)
- `device_name`: Device name
- `device_manufacturer`: Device manufacturer
- `device_model`: Device model
- `device_serial_number`: Device serial number
- `transport`: Source transport name

### Fields
All device data values are stored as fields. The transport automatically converts:
- Numeric strings to integers or floats
- Non-numeric strings remain as strings

### Time
- Uses current timestamp in nanoseconds (if `include_timestamp = true`)
- Can be disabled for custom timestamp handling

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
bridge = influxdb_output

# InfluxDB output
[influxdb_output]
type = influxdb_out
host = localhost
port = 8086
database = solar
measurement = inverter_data
```

## Installation

1. Install the required dependency:
   ```bash
   pip install influxdb
   ```

2. Or add to your requirements.txt:
   ```
   influxdb
   ```

## InfluxDB Setup

1. Install InfluxDB v1:
   ```bash
   # Ubuntu/Debian
   sudo apt install influxdb influxdb-client
   sudo systemctl enable influxdb
   sudo systemctl start influxdb
   
   # Or download from https://portal.influxdata.com/downloads/
   ```

2. Create a database (optional - will be created automatically):
   ```bash
   echo "CREATE DATABASE solar" | influx
   ```

## Querying Data

Once data is flowing, you can query it using InfluxDB's SQL-like query language:

```sql
-- Show all measurements
SHOW MEASUREMENTS

-- Query recent data
SELECT * FROM device_data WHERE time > now() - 1h

-- Query specific device
SELECT * FROM device_data WHERE device_identifier = '123456789'

-- Aggregate data
SELECT mean(value) FROM device_data WHERE field_name = 'battery_voltage' GROUP BY time(5m)
```

## Integration with Grafana

InfluxDB data can be easily visualized in Grafana:

1. Add InfluxDB as a data source in Grafana
2. Use the same connection details as your configuration
3. Create dashboards using InfluxDB queries

## Troubleshooting

### Connection Issues
- Verify InfluxDB is running: `systemctl status influxdb`
- Check firewall settings for port 8086
- Verify host and port configuration
- Check connection timeout settings if using slow networks

### Authentication Issues
- Ensure username/password are correct
- Check InfluxDB user permissions

### Data Not Appearing
- Check log levels for detailed error messages
- Verify database exists and is accessible
- Check batch settings - data may be buffered
- Look for reconnection messages in logs

### Data Stops After Some Time
- **Most Common Issue**: Network connectivity problems or InfluxDB server restarts
- Check logs for reconnection attempts and failures
- Verify InfluxDB server is stable and not restarting
- Consider increasing `reconnect_attempts` and `reconnect_delay` for unstable networks
- Monitor network connectivity between gateway and InfluxDB server

### Performance
- Adjust `batch_size` and `batch_timeout` for your use case
- Larger batches reduce network overhead but increase memory usage
- Shorter timeouts provide more real-time data but increase network traffic
- Increase `connection_timeout` for slow networks 