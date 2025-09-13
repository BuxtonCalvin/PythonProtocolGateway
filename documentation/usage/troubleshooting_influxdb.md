# InfluxDB Troubleshooting Guide

## Common Issue: Data Stops Being Written to InfluxDB

This guide helps you diagnose and fix the issue where data stops being written to InfluxDB after some time.

## Quick Diagnosis

### 1. Check Logs
First, enable debug logging to see what's happening:

```ini
[influxdb_output]
transport = influxdb_out
host = localhost
port = 8086
database = solar
log_level = DEBUG
```

Look for these log messages:
- `"Not connected to InfluxDB, skipping data write"`
- `"Connection check failed"`
- `"Attempting to reconnect to InfluxDB"`
- `"Failed to write batch to InfluxDB"`

### 2. Check InfluxDB Server
Verify InfluxDB is running and accessible:

```bash
# Check if InfluxDB is running
systemctl status influxdb

# Test connection
curl -i http://localhost:8086/ping

# Check if database exists
echo "SHOW DATABASES" | influx
```

### 3. Check Network Connectivity
Test network connectivity between your gateway and InfluxDB:

```bash
# Test basic connectivity
ping your_influxdb_host

# Test port connectivity
telnet your_influxdb_host 8086
```

## Root Causes and Solutions

### 1. Network Connectivity Issues

**Symptoms:**
- Connection timeouts
- Intermittent data loss
- Reconnection attempts in logs

**Solutions:**
```ini
[influxdb_output]
# Increase timeouts for slow networks
connection_timeout = 30
reconnect_attempts = 10
reconnect_delay = 10.0
```

### 2. InfluxDB Server Restarts

**Symptoms:**
- Connection refused errors
- Sudden data gaps
- Reconnection success after delays

**Solutions:**
- Monitor InfluxDB server stability
- Check InfluxDB logs for crashes
- Consider using InfluxDB clustering for high availability

### 3. Memory/Resource Issues

**Symptoms:**
- Slow response times
- Connection hangs
- Batch write failures

**Solutions:**
```ini
[influxdb_output]
# Reduce batch size to lower memory usage
batch_size = 50
batch_timeout = 5.0
```

### 4. Authentication Issues

**Symptoms:**
- Authentication errors in logs
- Connection succeeds but writes fail

**Solutions:**
- Verify username/password in configuration
- Check InfluxDB user permissions
- Test authentication manually:

```bash
curl -i -u username:password http://localhost:8086/query?q=SHOW%20DATABASES
```

### 5. Database/Measurement Issues

**Symptoms:**
- Data appears in InfluxDB but not in expected measurement
- Type conflicts in logs

**Solutions:**
- Verify database and measurement names
- Check for field type conflicts
- Use `force_float = true` to avoid type issues

## Configuration Best Practices

### Recommended Configuration
```ini
[influxdb_output]
transport = influxdb_out
host = localhost
port = 8086
database = solar
measurement = device_data
include_timestamp = true
include_device_info = true

# Connection monitoring
reconnect_attempts = 5
reconnect_delay = 5.0
connection_timeout = 10

# Batching (adjust based on your data rate)
batch_size = 100
batch_timeout = 10.0

# Data handling
force_float = true
log_level = INFO
```

### For Unstable Networks
```ini
[influxdb_output]
# More aggressive reconnection
reconnect_attempts = 10
reconnect_delay = 10.0
connection_timeout = 30

# Smaller batches for faster recovery
batch_size = 50
batch_timeout = 5.0
```

### For High-Volume Data
```ini
[influxdb_output]
# Larger batches for efficiency
batch_size = 500
batch_timeout = 30.0

# Faster reconnection
reconnect_attempts = 3
reconnect_delay = 2.0
```

## Monitoring and Alerts

### 1. Monitor Connection Status
Add this to your monitoring system:
```bash
# Check if gateway is writing data
curl -s "http://localhost:8086/query?db=solar&q=SELECT%20count(*)%20FROM%20device_data%20WHERE%20time%20%3E%20now()%20-%201h"
```

### 2. Set Up Alerts
Monitor these conditions:
- No data points in the last hour
- Reconnection attempts > 5 in 10 minutes
- Connection failures > 3 in 5 minutes

### 3. Log Monitoring
Watch for these log patterns:
```bash
# Monitor for connection issues
grep -i "connection\|reconnect\|failed" /var/log/protocol_gateway.log

# Monitor for data flow
grep -i "wrote.*points\|batch.*flush" /var/log/protocol_gateway.log
```

## Testing Your Setup

### 1. Test Connection Monitoring
Run the connection test script:
```bash
python test_influxdb_connection.py
```

### 2. Test Data Flow
Create a simple test configuration:
```ini
[test_source]
transport = modbus_rtu
port = /dev/ttyUSB0
baudrate = 9600
protocol_version = test_protocol
read_interval = 5
bridge = influxdb_output

[influxdb_output]
transport = influxdb_out
host = localhost
port = 8086
database = test
measurement = test_data
log_level = DEBUG
```

### 3. Verify Data in InfluxDB
```sql
-- Check if data is being written
SELECT * FROM test_data ORDER BY time DESC LIMIT 10

-- Check data rate
SELECT count(*) FROM test_data WHERE time > now() - 1h
```

## Advanced Troubleshooting

### 1. Enable Verbose Logging
```ini
[general]
log_level = DEBUG

[influxdb_output]
log_level = DEBUG
```

### 2. Check Multiprocessing Issues
If using multiple transports, verify bridge configuration:
```ini
# Ensure bridge names match exactly
[source_transport]
bridge = influxdb_output

[influxdb_output]
transport = influxdb_out
# No bridge needed for output transports
```

### 3. Monitor System Resources
```bash
# Check memory usage
free -h

# Check disk space
df -h

# Check network connections
netstat -an | grep 8086
```

### 4. InfluxDB Performance Tuning
```ini
# InfluxDB configuration (influxdb.conf)
[data]
wal-fsync-delay = "1s"
cache-max-memory-size = "1g"
series-id-set-cache-size = 100
```

## Common Error Messages

### "Failed to connect to InfluxDB"
- Check if InfluxDB is running
- Verify host and port
- Check firewall settings

### "Failed to write batch to InfluxDB"
- Check InfluxDB server resources
- Verify database permissions
- Check for field type conflicts

### "Not connected to InfluxDB, skipping data write"
- Connection was lost, reconnection in progress
- Check network connectivity
- Monitor reconnection attempts

### "Connection check failed"
- Network issue or InfluxDB restart
- Check InfluxDB server status
- Verify network connectivity

## Getting Help

If you're still experiencing issues:

1. **Collect Information:**
   - Gateway logs with DEBUG level
   - InfluxDB server logs
   - Network connectivity test results
   - Configuration file (remove sensitive data)

2. **Test Steps:**
   - Run the connection test script
   - Verify InfluxDB is accessible manually
   - Test with a simple configuration

3. **Provide Details:**
   - Operating system and version
   - Python version
   - InfluxDB version
   - Network setup (local/remote InfluxDB)
   - Data volume and frequency

## Prevention

### 1. Regular Monitoring
- Set up automated monitoring for data flow
- Monitor InfluxDB server health
- Check network connectivity regularly

### 2. Configuration Validation
- Test configurations before deployment
- Use connection monitoring settings
- Validate InfluxDB permissions

### 3. Backup Strategies
- Consider multiple InfluxDB instances
- Implement data backup procedures
- Use InfluxDB clustering for high availability 