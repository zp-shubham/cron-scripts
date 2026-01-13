# Cassandra to Azure Blob Storage Exporter

A production-ready Python script for daily batch export of Cassandra data to Azure Blob Storage in Parquet format.

## Features

- ✅ Connects to Cassandra and fetches all rows from a specified table
- ✅ Writes data to Parquet files with Snappy compression using PyArrow
- ✅ Uploads Parquet files to Azure Blob Storage
- ✅ All credentials loaded from environment variables (secure, no hardcoding)
- ✅ Automatic file naming with table name and date (e.g., `table_2026-01-10.parquet`)
- ✅ Efficient batching/paging for medium-size datasets
- ✅ Comprehensive logging for monitoring and debugging
- ✅ Automatic cleanup of local temporary files after upload
- ✅ Modular, self-contained, and ready for cloud schedulers

## Requirements

- Python 3.8+
- Cassandra database
- Azure Blob Storage account

## Installation

1. Clone or download this script to your desired location

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Set up environment variables (see Configuration section below)

## Configuration

All configuration is done via environment variables. Copy `.env.example` to `.env` and fill in your values:

### Required Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `CASSANDRA_CONTACT_POINTS` | Comma-separated list of Cassandra nodes | `127.0.0.1,127.0.0.2` |
| `CASSANDRA_KEYSPACE` | Cassandra keyspace name | `my_keyspace` |
| `CASSANDRA_TABLE` | Table name to export | `my_table` |
| `AZURE_BLOB_CONNECTION_STRING` | Azure Storage connection string | `DefaultEndpointsProtocol=https;...` |
| `AZURE_BLOB_CONTAINER_NAME` | Azure Blob container name | `my-container` |

### Optional Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `CASSANDRA_USERNAME` | Cassandra username (if auth enabled) | None |
| `CASSANDRA_PASSWORD` | Cassandra password (if auth enabled) | None |
| `LOCAL_PARQUET_DIRECTORY` | Local directory for temporary Parquet files | `/tmp` |
| `BATCH_SIZE` | Number of rows to fetch per batch | `10000` |

## Usage

### Running Locally

```bash
# Export environment variables
export CASSANDRA_CONTACT_POINTS="127.0.0.1"
export CASSANDRA_KEYSPACE="my_keyspace"
export CASSANDRA_TABLE="my_table"
export AZURE_BLOB_CONNECTION_STRING="your_connection_string"
export AZURE_BLOB_CONTAINER_NAME="my-container"

# Run the script
python cassandra_to_azure_export.py
```

### Using a .env file

```bash
# Load environment variables from .env file
set -a
source .env
set +a

# Run the script
python cassandra_to_azure_export.py
```

### Running with Cloud Schedulers

#### AWS EventBridge + Lambda
1. Package the script and dependencies as a Lambda layer
2. Set environment variables in Lambda configuration
3. Create EventBridge rule with cron expression (e.g., `cron(0 2 * * ? *)` for daily at 2 AM UTC)

#### Azure Functions + Timer Trigger
1. Create Azure Function with Timer Trigger
2. Set environment variables in Application Settings
3. Configure timer schedule (e.g., `0 0 2 * * *` for daily at 2 AM)

#### Google Cloud Scheduler + Cloud Run
1. Deploy script as Cloud Run service
2. Set environment variables in Cloud Run configuration
3. Create Cloud Scheduler job with cron expression (e.g., `0 2 * * *`)

#### Kubernetes CronJob
```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: cassandra-export
spec:
  schedule: "0 2 * * *"  # Daily at 2 AM
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: exporter
            image: your-image:latest
            env:
            - name: CASSANDRA_CONTACT_POINTS
              valueFrom:
                secretKeyRef:
                  name: cassandra-secret
                  key: contact-points
            # ... other env vars
          restartPolicy: OnFailure
```

## Output

The script generates Parquet files with the following naming convention:
```
{table_name}_{YYYY-MM-DD}.parquet
```

Example: `users_2026-01-10.parquet`

## Logging

The script provides comprehensive logging:
- Connection status
- Data fetch progress (logged every batch)
- Number of rows exported
- File size and upload status
- Cleanup operations
- Error details (if any)

Example output:
```
2026-01-10 14:55:20 - __main__ - INFO - Starting Cassandra to Azure Blob Storage export
2026-01-10 14:55:20 - __main__ - INFO - Connecting to Cassandra at ['127.0.0.1']
2026-01-10 14:55:21 - __main__ - INFO - Successfully connected to Cassandra
2026-01-10 14:55:21 - __main__ - INFO - Fetching data from table: my_table
2026-01-10 14:55:25 - __main__ - INFO - Fetched 10000 rows (1 batches)
2026-01-10 14:55:28 - __main__ - INFO - Successfully fetched 15000 total rows from Cassandra
2026-01-10 14:55:29 - __main__ - INFO - Writing 15000 rows to Parquet file: /tmp/my_table_2026-01-10.parquet
2026-01-10 14:55:30 - __main__ - INFO - Successfully wrote Parquet file (2.34 MB)
2026-01-10 14:55:30 - __main__ - INFO - Uploading to Azure Blob Storage: my_table_2026-01-10.parquet
2026-01-10 14:55:32 - __main__ - INFO - Successfully uploaded my_table_2026-01-10.parquet to Azure Blob Storage
2026-01-10 14:55:32 - __main__ - INFO - Removed local file: /tmp/my_table_2026-01-10.parquet
2026-01-10 14:55:32 - __main__ - INFO - Export completed successfully in 12.45 seconds
2026-01-10 14:55:32 - __main__ - INFO - Total rows exported: 15000
```

## Error Handling

The script includes robust error handling:
- Validates all required environment variables on startup
- Handles connection failures gracefully
- Cleans up temporary files even on failure
- Returns appropriate exit codes (0 for success, 1 for failure)
- Logs detailed error messages for troubleshooting

## Performance Considerations

- **Batching**: Uses configurable batch size (default 10,000 rows) for efficient memory usage
- **Compression**: Snappy compression reduces file size and upload time
- **Pagination**: Cassandra driver handles pagination automatically
- **Memory**: Processes data in batches to avoid loading entire dataset into memory at once

For very large datasets (millions of rows), consider:
- Increasing `BATCH_SIZE` if you have sufficient memory
- Partitioning exports by date range or other criteria
- Using multiple parallel exports for different table partitions

## Security Best Practices

✅ **Implemented:**
- All credentials stored in environment variables
- No hardcoded secrets in code
- Connection strings and passwords never logged

⚠️ **Recommendations:**
- Use secret management services (AWS Secrets Manager, Azure Key Vault, etc.)
- Rotate credentials regularly
- Use managed identities when running in cloud environments
- Restrict network access to Cassandra and Azure Storage

## Troubleshooting

### Connection Issues
- Verify Cassandra contact points are reachable
- Check firewall rules and security groups
- Ensure credentials are correct (if authentication is enabled)

### Azure Upload Failures
- Verify connection string is valid
- Ensure container exists
- Check Azure Storage account permissions

### Memory Issues
- Reduce `BATCH_SIZE` to use less memory
- Ensure sufficient disk space in `LOCAL_PARQUET_DIRECTORY`

## License

This script is provided as-is for use in your projects.
