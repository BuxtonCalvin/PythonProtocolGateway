# InfluxDB Advanced Features: Exponential Backoff & Persistent Storage

## Overview

The InfluxDB transport now includes advanced features to handle network instability and long-term outages:

1. **Exponential Backoff**: Intelligent reconnection timing to avoid overwhelming the server
2. **Persistent Storage**: Local data storage to prevent data loss during extended outages
3. **Periodic Reconnection**: Regular connection health checks even during quiet periods

## Exponential Backoff

### How It Works

Instead of using a fixed delay between reconnection attempts, exponential backoff increases the delay exponentially:

- **Attempt 1**: 5 seconds delay
- **Attempt 2**: 10 seconds delay  
- **Attempt 3**: 20 seconds delay
- **Attempt 4**: 40 seconds delay
- **Attempt 5**: 80 seconds delay (capped at max_reconnect_delay)

### Configuration

```ini
[influxdb_output]
# Enable exponential backoff
use_exponential_backoff = true

# Base delay between attempts (seconds)
reconnect_delay = 5.0

# Maximum delay cap (seconds)
max_reconnect_delay = 300.0

# Number of reconnection attempts
reconnect_attempts = 5
```

### Benefits

- **Reduces Server Load**: Prevents overwhelming the InfluxDB server during recovery
- **Network Friendly**: Respects network conditions and server capacity
- **Configurable**: Adjust timing based on your environment

### Example Scenarios

#### Short Network Glitch
```
Attempt 1: 5s delay → Success
Total time: ~5 seconds
```

#### Server Restart
```
Attempt 1: 5s delay → Fail
Attempt 2: 10s delay → Fail  
Attempt 3: 20s delay → Success
Total time: ~35 seconds
```

#### Extended Outage
```
Attempt 1: 5s delay → Fail
Attempt 2: 10s delay → Fail
Attempt 3: 20s delay → Fail
Attempt 4: 40s delay → Fail
Attempt 5: 80s delay → Fail
Total time: ~155 seconds, then data stored in backlog
```

## Periodic Reconnection

### How It Works

Periodic reconnection ensures the connection to InfluxDB remains healthy even during periods when no data is being written:

- **Regular Health Checks**: Performs connection tests at configurable intervals
- **Connection Refresh**: Re-establishes connection even if it appears healthy
- **Quiet Period Handling**: Maintains connection during low-activity periods
- **Proactive Recovery**: Detects and fixes connection issues before data loss

### Configuration

```ini
[influxdb_output]
# Periodic reconnection interval (seconds)
periodic_reconnect_interval = 14400.0  # 4 hours (default)

# Disable periodic reconnection
periodic_reconnect_interval = 0
```

### Benefits

- **Connection Stability**: Prevents connection timeouts during quiet periods
- **Proactive Monitoring**: Detects issues before they affect data transmission
- **Network Resilience**: Handles network changes and server restarts
- **Configurable**: Adjust interval based on your environment

### Example Scenarios

#### Quiet Periods (No Data)
```
10:00 AM: Last data written
11:00 AM: Periodic reconnection check → Connection healthy
12:00 PM: Periodic reconnection check → Connection healthy
01:00 PM: Periodic reconnection check → Connection healthy
02:00 PM: New data arrives → Immediate transmission
```

#### Network Issues During Quiet Period
```
10:00 AM: Last data written
11:00 AM: Periodic reconnection check → Connection failed
11:00 AM: Attempting reconnection → Success
12:00 PM: Periodic reconnection check → Connection healthy
```

#### Server Restart During Quiet Period
```
10:00 AM: Last data written
11:00 AM: Periodic reconnection check → Connection failed
11:00 AM: Attempting reconnection → Success (server restarted)
12:00 PM: Periodic reconnection check → Connection healthy
```

## Persistent Storage (Data Backlog)

### How It Works

When InfluxDB is unavailable, data is stored locally in pickle files:

1. **Data Collection**: Points are stored in memory and on disk
2. **Automatic Cleanup**: Old data is removed based on age limits
3. **Recovery**: When connection is restored, backlog is flushed to InfluxDB
4. **Size Management**: Backlog is limited to prevent disk space issues

### Configuration

```ini
[influxdb_output]
# Enable persistent storage
enable_persistent_storage = true

# Storage directory (relative to gateway directory)
persistent_storage_path = influxdb_backlog

# Maximum number of points to store
max_backlog_size = 10000

# Maximum age of points in seconds (24 hours)
max_backlog_age = 86400
```

### Storage Structure

```
influxdb_backlog/
├── influxdb_backlog_influxdb_output.pkl
├── influxdb_backlog_another_transport.pkl
└── ...
```

### Data Recovery Process

1. **Connection Lost**: Data continues to be collected and stored locally
2. **Reconnection**: When InfluxDB becomes available, backlog is detected
3. **Batch Upload**: All stored points are sent to InfluxDB in batches
4. **Cleanup**: Backlog is cleared after successful upload

### Example Recovery Log

```
[2024-01-15 10:30:00] Connection check failed: Connection refused
[2024-01-15 10:30:00] Not connected to InfluxDB, storing data in backlog
[2024-01-15 10:30:00] Added point to backlog. Backlog size: 1
...
[2024-01-15 18:45:00] Attempting to reconnect to InfluxDB at localhost:8086
[2024-01-15 18:45:00] Successfully reconnected to InfluxDB
[2024-01-15 18:45:00] Flushing 2847 backlog points to InfluxDB
[2024-01-15 18:45:00] Successfully wrote 2847 backlog points to InfluxDB
```

## Configuration Examples

### For Stable Networks (Local InfluxDB)

```ini
[influxdb_output]
transport = influxdb_out
host = localhost
port = 8086
database = solar

# Standard reconnection
reconnect_attempts = 3
reconnect_delay = 2.0
use_exponential_backoff = false

# Periodic reconnection
periodic_reconnect_interval = 1800.0  # 30 minutes

# Minimal persistent storage
enable_persistent_storage = true
max_backlog_size = 1000
max_backlog_age = 3600  # 1 hour
```

### For Unstable Networks (Remote InfluxDB)

```ini
[influxdb_output]
transport = influxdb_out
host = remote.influxdb.com
port = 8086
database = solar

# Aggressive reconnection with exponential backoff
reconnect_attempts = 10
reconnect_delay = 5.0
use_exponential_backoff = true
max_reconnect_delay = 600.0  # 10 minutes

# Frequent periodic reconnection
periodic_reconnect_interval = 900.0  # 15 minutes

# Large persistent storage for extended outages
enable_persistent_storage = true
max_backlog_size = 50000
max_backlog_age = 604800  # 1 week
```

### For High-Volume Data

```ini
[influxdb_output]
transport = influxdb_out
host = localhost
port = 8086
database = solar

# Fast reconnection for high availability
reconnect_attempts = 5
reconnect_delay = 1.0
use_exponential_backoff = true
max_reconnect_delay = 60.0

# Less frequent periodic reconnection (data keeps connection alive)
periodic_reconnect_interval = 14400.0  # 4 hours (default)

# Large backlog for high data rates
enable_persistent_storage = true
max_backlog_size = 100000
max_backlog_age = 86400  # 24 hours

# Optimized batching
batch_size = 500
batch_timeout = 5.0
```

## Monitoring and Maintenance

### Check Backlog Status

```bash
# Check backlog file sizes
ls -lh influxdb_backlog/

# Check backlog contents (Python script)
python3 -c "
import pickle
import os
for file in os.listdir('influxdb_backlog'):
    if file.endswith('.pkl'):
        with open(f'influxdb_backlog/{file}', 'rb') as f:
            data = pickle.load(f)
            print(f'{file}: {len(data)} points')
"
```

### Monitor Logs

```bash
# Monitor backlog activity
grep -i "backlog\|persistent" /var/log/protocol_gateway.log

# Monitor reconnection attempts
grep -i "reconnect\|exponential" /var/log/protocol_gateway.log

# Monitor periodic reconnection
grep -i "periodic.*reconnect" /var/log/protocol_gateway.log
```

### Cleanup Old Backlog Files

```bash
# Remove backlog files older than 7 days
find influxdb_backlog/ -name "*.pkl" -mtime +7 -delete
```

## Performance Considerations

### Memory Usage

- **Backlog Storage**: Each point uses ~200-500 bytes in memory
- **10,000 points**: ~2-5 MB memory usage
- **100,000 points**: ~20-50 MB memory usage

### Disk Usage

- **Backlog Files**: Compressed pickle format
- **10,000 points**: ~1-2 MB disk space
- **100,000 points**: ~10-20 MB disk space

### Network Impact

- **Recovery Upload**: Large batches may take time to upload
- **Bandwidth**: Consider network capacity during recovery
- **Server Load**: InfluxDB may experience high load during recovery

## Troubleshooting

### Backlog Not Flushing

**Symptoms:**
- Backlog points remain after reconnection
- No "Flushing X backlog points" messages

**Solutions:**
- Check InfluxDB server capacity
- Verify database permissions
- Monitor InfluxDB logs for errors

### Excessive Memory Usage

**Symptoms:**
- High memory consumption
- Slow performance

**Solutions:**
- Reduce `max_backlog_size`
- Decrease `max_backlog_age`
- Monitor system resources

### Disk Space Issues

**Symptoms:**
- "Backlog full" warnings
- Disk space running low

**Solutions:**
- Clean up old backlog files
- Reduce `max_backlog_size`
- Move `persistent_storage_path` to larger disk

### Reconnection Too Aggressive

**Symptoms:**
- High CPU usage during outages
- Network congestion

**Solutions:**
- Increase `reconnect_delay`
- Reduce `reconnect_attempts`
- Enable `use_exponential_backoff`

## Best Practices

### 1. Size Your Backlog Appropriately

```ini
# For 1-minute intervals, 24-hour outage
max_backlog_size = 1440  # 24 * 60

# For 5-minute intervals, 1-week outage  
max_backlog_size = 2016  # 7 * 24 * 12
```

### 2. Monitor and Clean

- Regularly check backlog file sizes
- Clean up old files automatically
- Monitor disk space usage

### 3. Test Recovery

- Simulate outages to test recovery
- Verify data integrity after recovery
- Monitor performance during recovery

### 4. Plan for Scale

- Estimate data volume and outage duration
- Size backlog accordingly
- Monitor system resources

## Migration from Previous Version

If upgrading from a version without these features:

1. **No Configuration Changes Required**: Features are enabled by default with sensible defaults
2. **Backward Compatible**: Existing configurations continue to work
3. **Gradual Adoption**: Disable features if not needed:

```ini
[influxdb_output]
# Disable exponential backoff
use_exponential_backoff = false

# Disable persistent storage
enable_persistent_storage = false
``` 