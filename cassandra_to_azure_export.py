#!/usr/bin/env python3
"""
Daily Batch Export from Cassandra to Azure Blob Storage

This script connects to a Cassandra database, fetches all rows from a specified table,
writes the data to a Parquet file with Snappy compression, and uploads it to Azure Blob Storage.

Configuration is defined in the CONFIG section below.
"""

import os
import sys
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement
from azure.storage.blob import BlobServiceClient


# ============================================================================
# CONFIGURATION - Edit these values according to your setup
# ============================================================================

CONFIG = {
    # Cassandra Configuration
    'CASSANDRA_CONTACT_POINTS': ['10.3.0.6', '10.3.0.7', '10.3.0.9'],  # Cassandra cluster nodes
    'CASSANDRA_KEYSPACE': 'location_functional',
    'CASSANDRA_TABLE': 'vehicle_location',
    'CASSANDRA_LATEST_TABLE': 'vehicle_location_latest',  # Table with unique vehicle IDs
    'CASSANDRA_USERNAME': 'YOUR_CASSANDRA_USERNAME',
    'CASSANDRA_PASSWORD': 'YOUR_CASSANDRA_PASSWORD',
    
    # Azure Blob Storage Configuration
    'AZURE_BLOB_CONNECTION_STRING': 'YOUR_AZURE_CONNECTION_STRING_HERE',
    'AZURE_BLOB_CONTAINER_NAME': 'test',
    
    # Optional Configuration
    'LOCAL_PARQUET_DIRECTORY': '/tmp',
    'BATCH_SIZE': 10000,
    
    # Delete After Upload Configuration
    'ENABLE_DELETE_AFTER_UPLOAD': True,  # Set to False to disable deletion
    'DELETE_RETENTION_DAYS': 30,         # Keep last N days in Cassandra
    'DELETE_BATCH_SIZE': 1000,
}

# ============================================================================


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class CassandraToAzureExporter:
    """Handles export of Cassandra data to Azure Blob Storage via Parquet files."""
    
    def __init__(self):
        """Initialize exporter with configuration from CONFIG dictionary."""
        # Cassandra configuration
        self.cassandra_contact_points = CONFIG['CASSANDRA_CONTACT_POINTS']
        self.cassandra_keyspace = CONFIG['CASSANDRA_KEYSPACE']
        self.cassandra_table = CONFIG['CASSANDRA_TABLE']
        self.cassandra_latest_table = CONFIG['CASSANDRA_LATEST_TABLE']
        self.cassandra_username = CONFIG.get('CASSANDRA_USERNAME')
        self.cassandra_password = CONFIG.get('CASSANDRA_PASSWORD')
        
        # Azure configuration
        self.azure_connection_string = CONFIG['AZURE_BLOB_CONNECTION_STRING']
        self.azure_container_name = CONFIG['AZURE_BLOB_CONTAINER_NAME']
        
        # Optional configuration
        self.local_directory = CONFIG.get('LOCAL_PARQUET_DIRECTORY', '/tmp')
        self.batch_size = CONFIG.get('BATCH_SIZE', 10000)
        
        # Delete after upload configuration
        self.enable_delete = CONFIG.get('ENABLE_DELETE_AFTER_UPLOAD', False)
        self.retention_days = CONFIG.get('DELETE_RETENTION_DAYS', 30)
        self.delete_batch_size = CONFIG.get('DELETE_BATCH_SIZE', 1000)
        
        # Validate configuration
        self._validate_config()
        
        logger.info("Exporter initialized successfully")
        logger.info(f"Target: {self.cassandra_keyspace}.{self.cassandra_table}")
        logger.info(f"Azure Container: {self.azure_container_name}")
        if self.enable_delete:
            logger.info(f"Delete after upload: ENABLED (retention: {self.retention_days} days)")
        else:
            logger.info("Delete after upload: DISABLED")
    
    # ========================================================================
    # CONFIGURATION VALIDATION
    # ========================================================================
    # Validates that all configuration values are correct and creates
    # necessary directories before starting the export process
    # ========================================================================
    def _validate_config(self):
        """Validate configuration values."""
        if self.batch_size <= 0:
            raise ValueError("BATCH_SIZE must be a positive integer")
        
        # Ensure local directory exists
        Path(self.local_directory).mkdir(parents=True, exist_ok=True)
    
    # ========================================================================
    # FILENAME GENERATION
    # ========================================================================
    # Generates the Parquet filename using the pattern: {table}_{date}.parquet
    # Example: vehicle_location_2025-12-13.parquet
    # ========================================================================
    def _get_parquet_filename(self, export_date: Optional[datetime] = None, vehicle_id: Optional[str] = None) -> str:
        """Generate Parquet filename with vehicle_id and export date."""
        if export_date is None:
            export_date = datetime.now()
        date_str = export_date.strftime('%Y-%m-%d')
        
        if vehicle_id:
            # Per-vehicle filename: VEH123_2025-12-13.parquet
            return f"{vehicle_id}_{date_str}.parquet"
        else:
            # Legacy filename: vehicle_location_2025-12-13.parquet
            return f"{self.cassandra_table}_{date_str}.parquet"
    
    def _get_local_filepath(self, export_date: Optional[datetime] = None, vehicle_id: Optional[str] = None) -> str:
        """Get full local path for Parquet file."""
        filename = self._get_parquet_filename(export_date, vehicle_id)
        return os.path.join(self.local_directory, filename)
    
    # ========================================================================
    # RETENTION POLICY CALCULATION
    # ========================================================================
    # Calculates the cutoff date by subtracting retention days from today
    # Data older than this date will be exported and optionally deleted
    # Example: Today=2026-01-12, Retention=30 days → Cutoff=2025-12-13
    # ========================================================================
    def _calculate_cutoff_date(self) -> datetime:
        """Calculate the cutoff date based on retention policy."""
        cutoff = datetime.now() - pd.Timedelta(days=self.retention_days)
        logger.info(f"Cutoff date for export: {cutoff.strftime('%Y-%m-%d')} (data older than {self.retention_days} days)")
        return cutoff
    
    # ========================================================================
    # CASSANDRA CONNECTION
    # ========================================================================
    # Connects to the Cassandra cluster using configured contact points
    # Handles authentication if username/password are provided
    # Returns both cluster and session objects for database operations
    # ========================================================================
    def connect_to_cassandra(self) -> Cluster:
        """Establish connection to Cassandra cluster."""
        logger.info(f"Connecting to Cassandra at {self.cassandra_contact_points}")
        
        auth_provider = None
        if self.cassandra_username and self.cassandra_password:
            auth_provider = PlainTextAuthProvider(
                username=self.cassandra_username,
                password=self.cassandra_password
            )
        
        cluster = Cluster(
            contact_points=self.cassandra_contact_points,
            auth_provider=auth_provider
        )
        
        session = cluster.connect(self.cassandra_keyspace)
        logger.info("Successfully connected to Cassandra")
        return cluster, session
    
    # ========================================================================
    # FETCH UNIQUE VEHICLE IDS
    # ========================================================================
    # Fetches list of unique vehicle IDs from vehicle_location_latest table
    # This list is used to create separate export files for each vehicle
    # ========================================================================
    def fetch_vehicle_ids(self, session) -> list:
        """
        Fetch unique vehicle IDs from the latest table.
        
        Args:
            session: Active Cassandra session
            
        Returns:
            List of unique vehicle IDs
        """
        logger.info(f"Fetching unique vehicle IDs from table: {self.cassandra_latest_table}")
        
        try:
            query = f"SELECT vehicle_id FROM {self.cassandra_latest_table}"
            statement = SimpleStatement(query, fetch_size=self.batch_size)
            result_set = session.execute(statement)
            
            vehicle_ids = []
            for row in result_set:
                vehicle_id = row.vehicle_id
                if vehicle_id:  # Skip null values
                    vehicle_ids.append(vehicle_id)
            
            logger.info(f"Found {len(vehicle_ids)} unique vehicle IDs")
            return vehicle_ids
            
        except Exception as e:
            logger.error(f"Error fetching vehicle IDs: {e}")
            raise
    
    # ========================================================================
    # DATA FETCHING FROM CASSANDRA
    # ========================================================================
    # Fetches data from Cassandra using ALLOW FILTERING for date-based queries
    # Converts Cassandra-specific types (Date, UUID, Duration) to Python types
    # Returns a pandas DataFrame with all fetched rows
    # ========================================================================
    def fetch_data_from_cassandra(self, session, cutoff_date: Optional[datetime] = None, vehicle_id: Optional[str] = None) -> pd.DataFrame:
        """
        Fetch rows from Cassandra table using pagination.
        If cutoff_date is provided, uses ALLOW FILTERING to fetch data older than cutoff date.
        If vehicle_id is provided, filters data for that specific vehicle.
        
        Args:
            session: Active Cassandra session
            cutoff_date: Optional cutoff date - only fetch data older than this date
            vehicle_id: Optional vehicle ID - only fetch data for this vehicle
            
        Returns:
            DataFrame containing rows from the table
        """
        from cassandra.util import Date, Time, Duration
        from uuid import UUID
        from datetime import date, timedelta
        import decimal
        
        logger.info(f"Fetching data from table: {self.cassandra_table}")
        
        # Build query based on filters
        if vehicle_id and cutoff_date:
            # Filter by both vehicle_id and date
            cutoff_date_only = cutoff_date.date() if isinstance(cutoff_date, datetime) else cutoff_date
            query = f"SELECT * FROM {self.cassandra_table} WHERE vehicle_id = ? AND day < ? ALLOW FILTERING"
            statement = session.prepare(query)
            statement.fetch_size = self.batch_size
            logger.info(f"Querying vehicle {vehicle_id}, data older than {cutoff_date_only} using ALLOW FILTERING")
            result_set = session.execute(statement, [vehicle_id, cutoff_date_only])
        elif cutoff_date is not None:
            # Use ALLOW FILTERING to query data older than cutoff date
            cutoff_date_only = cutoff_date.date() if isinstance(cutoff_date, datetime) else cutoff_date
            query = f"SELECT * FROM {self.cassandra_table} WHERE day < ? ALLOW FILTERING"
            statement = session.prepare(query)
            statement.fetch_size = self.batch_size
            logger.info(f"Querying data older than {cutoff_date_only} using ALLOW FILTERING")
            result_set = session.execute(statement, [cutoff_date_only])
        else:
            # Query all data (for backward compatibility)
            query = f"SELECT * FROM {self.cassandra_table}"
            statement = SimpleStatement(query, fetch_size=self.batch_size)
            result_set = session.execute(statement)
        
        all_rows = []
        row_count = 0
        batch_count = 0
        
        def convert_cassandra_value(val):
            """Convert Cassandra types to Python types immediately."""
            if val is None:
                return None
            elif isinstance(val, Date):
                return date(1970, 1, 1) + timedelta(days=val.days_from_epoch)
            elif isinstance(val, (Time, Duration)):
                return str(val)
            elif isinstance(val, UUID):
                return str(val)
            elif isinstance(val, decimal.Decimal):
                return float(val)
            elif isinstance(val, bytes):
                return val.hex()
            else:
                return val
        
        try:
            # Process results in batches
            for row in result_set:
                # Convert Cassandra types immediately
                row_dict = {}
                try:
                    for key, value in row._asdict().items():
                        row_dict[key] = convert_cassandra_value(value)
                    all_rows.append(row_dict)
                    row_count += 1
                except Exception as row_error:
                    logger.error(f"Error converting row: {row_error}")
                    logger.error(f"Row data types: {[(k, type(v).__name__) for k, v in row._asdict().items()]}")
                    raise
                
                # Log progress periodically
                if row_count % self.batch_size == 0:
                    batch_count += 1
                    logger.info(f"Fetched {row_count} rows ({batch_count} batches)")
            
            logger.info(f"Successfully fetched {row_count} total rows from Cassandra (filtered at database level)")
            
            # Convert to DataFrame
            if all_rows:
                df = pd.DataFrame(all_rows)
                return df
            else:
                logger.warning("No data found in table")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Error fetching data from Cassandra: {e}")
            raise
    
    # ========================================================================
    # TYPE CONVERSION FOR PARQUET COMPATIBILITY
    # ========================================================================
    # Converts Cassandra-specific types (Date, UUID, Duration, Decimal) to
    # Python types that are compatible with PyArrow/Parquet format
    # This prevents errors when writing to Parquet files
    # ========================================================================
    def _convert_cassandra_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert Cassandra-specific data types to Python/Pandas compatible types.
        
        Args:
            df: DataFrame with potential Cassandra types
            
        Returns:
            DataFrame with converted types
        """
        from cassandra.util import Date, Time, Duration
        from uuid import UUID
        from datetime import date, time, datetime
        import decimal
        
        logger.info(f"Starting type conversion for {len(df.columns)} columns...")
        df_converted = df.copy()
        
        def convert_value(val):
            """Convert a single value to a PyArrow-compatible type."""
            if val is None:
                return None
            elif isinstance(val, Date):
                # Convert Cassandra Date to Python date
                return date(1970, 1, 1) + pd.Timedelta(days=val.days_from_epoch)
            elif isinstance(val, (Time, Duration)):
                # Convert Time and Duration to string representation
                return str(val)
            elif isinstance(val, UUID):
                # Convert UUID to string
                return str(val)
            elif isinstance(val, decimal.Decimal):
                # Convert Decimal to float
                return float(val)
            elif isinstance(val, (datetime, date, time)):
                # Keep datetime objects as-is
                return val
            elif isinstance(val, (int, float, str, bool)):
                # Keep basic types as-is
                return val
            elif isinstance(val, bytes):
                # Convert bytes to string (base64 or hex)
                return val.hex()
            else:
                # For any other unknown type, convert to string
                return str(val)
        
        # Apply conversion to all columns
        for column in df_converted.columns:
            logger.info(f"Converting column: {column}")
            df_converted[column] = df_converted[column].apply(convert_value)
            sample = df_converted[column].dropna().iloc[0] if not df_converted[column].dropna().empty else None
            logger.info(f"  -> {column}: {type(sample).__name__}")
        
        logger.info("Type conversion completed successfully")
        return df_converted
    
    # ========================================================================
    # PARQUET FILE WRITING
    # ========================================================================
    # Writes the DataFrame to a Parquet file with Snappy compression
    # Handles type conversion and provides detailed error logging
    # Returns the number of rows successfully written
    # ========================================================================
    def write_to_parquet(self, df: pd.DataFrame, filepath: str) -> int:
        """
        Write DataFrame to Parquet file with Snappy compression.
        
        Args:
            df: DataFrame to write
            filepath: Path to output Parquet file
            
        Returns:
            Number of rows written
        """
        if df.empty:
            logger.warning("DataFrame is empty, skipping Parquet write")
            return 0
        
        logger.info(f"Writing {len(df)} rows to Parquet file: {filepath}")
        
        try:
            # Convert Cassandra-specific types
            df_converted = self._convert_cassandra_types(df)
            
            # Log column types for debugging
            logger.debug("Column types after conversion:")
            for col in df_converted.columns:
                sample = df_converted[col].dropna().iloc[0] if not df_converted[col].dropna().empty else None
                logger.debug(f"  {col}: {type(sample).__name__} = {sample}")
            
            # Convert DataFrame to PyArrow Table
            try:
                table = pa.Table.from_pandas(df_converted)
            except Exception as e:
                logger.error(f"PyArrow conversion failed: {e}")
                logger.error("Attempting column-by-column conversion to identify problematic column...")
                
                # Try converting each column individually to find the problem
                for col in df_converted.columns:
                    try:
                        test_df = df_converted[[col]]
                        test_table = pa.Table.from_pandas(test_df)
                        logger.debug(f"  Column '{col}' converted successfully")
                    except Exception as col_error:
                        logger.error(f"  Column '{col}' FAILED: {col_error}")
                        logger.error(f"    Sample values: {df_converted[col].head().tolist()}")
                raise
            
            # Write to Parquet with Snappy compression
            pq.write_table(
                table,
                filepath,
                compression='snappy',
                use_dictionary=True,
                write_statistics=True
            )
            
            file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
            logger.info(f"Successfully wrote Parquet file ({file_size_mb:.2f} MB)")
            
            return len(df)
            
        except Exception as e:
            logger.error(f"Error writing Parquet file: {e}")
            raise
    
    # ========================================================================
    # AZURE BLOB STORAGE UPLOAD
    # ========================================================================
    # Uploads the Parquet file to Azure Blob Storage with metadata
    # Metadata includes: row count, export timestamp, table name, keyspace
    # This metadata is used later for verification before deletion
    # ========================================================================
    def upload_to_azure(self, local_filepath: str, row_count: int, export_date: Optional[datetime] = None, vehicle_id: Optional[str] = None) -> bool:
        """
        Upload Parquet file to Azure Blob Storage with metadata.
        
        Args:
            local_filepath: Path to local Parquet file
            row_count: Number of rows in the file
            export_date: Date of the exported data
            vehicle_id: Vehicle ID for per-vehicle exports
            
        Returns:
            True if upload successful, False otherwise
        """
        blob_name = self._get_parquet_filename(export_date, vehicle_id)
        logger.info(f"Uploading to Azure Blob Storage: {blob_name}")
        
        try:
            # Create BlobServiceClient
            blob_service_client = BlobServiceClient.from_connection_string(
                self.azure_connection_string
            )
            
            # Get container client
            container_client = blob_service_client.get_container_client(
                self.azure_container_name
            )
            
            # Get blob client
            blob_client = container_client.get_blob_client(blob_name)
            
            # Prepare metadata
            metadata = {
                'row_count': str(row_count),
                'export_timestamp': datetime.now().isoformat(),
                'table_name': self.cassandra_table,
                'keyspace': self.cassandra_keyspace
            }
            if export_date:
                metadata['export_date'] = export_date.strftime('%Y-%m-%d')
            
            # Upload file with metadata
            with open(local_filepath, 'rb') as data:
                blob_client.upload_blob(data, overwrite=True, metadata=metadata)
            
            logger.info(f"Successfully uploaded {blob_name} to Azure Blob Storage (rows: {row_count})")
            return True
            
        except Exception as e:
            logger.error(f"Error uploading to Azure Blob Storage: {e}")
            raise
    
    # ========================================================================
    # BLOB UPLOAD VERIFICATION
    # ========================================================================
    # Verifies that the blob was uploaded successfully by checking:
    # 1. Blob exists in Azure Storage
    # 2. Metadata is present and complete
    # 3. Row count in metadata matches expected count
    # 4. Blob size is reasonable (not corrupted/empty)
    # Only if ALL checks pass will deletion proceed
    # ========================================================================
    def verify_blob_upload(self, blob_name: str, expected_row_count: int) -> bool:
        """
        Verify that blob was uploaded successfully and metadata is correct.
        
        Args:
            blob_name: Name of the blob to verify
            expected_row_count: Expected number of rows in the blob
            
        Returns:
            True if verification successful, False otherwise
        """
        logger.info(f"Verifying blob upload: {blob_name}")
        
        try:
            # Create BlobServiceClient
            blob_service_client = BlobServiceClient.from_connection_string(
                self.azure_connection_string
            )
            
            # Get blob client
            blob_client = blob_service_client.get_blob_client(
                container=self.azure_container_name,
                blob=blob_name
            )
            
            # Check if blob exists
            if not blob_client.exists():
                logger.error(f"Blob {blob_name} does not exist in Azure Storage")
                return False
            
            # Get blob properties and metadata
            properties = blob_client.get_blob_properties()
            metadata = properties.metadata
            
            # Verify metadata exists
            if not metadata:
                logger.error(f"Blob {blob_name} has no metadata")
                return False
            
            # Verify row count in metadata
            stored_row_count = metadata.get('row_count')
            if not stored_row_count:
                logger.error(f"Blob {blob_name} metadata missing row_count")
                return False
            
            stored_row_count = int(stored_row_count)
            if stored_row_count != expected_row_count:
                logger.error(f"Row count mismatch: expected {expected_row_count}, got {stored_row_count}")
                return False
            
            # Verify blob size is reasonable (not empty or corrupted)
            blob_size = properties.size
            if blob_size == 0:
                logger.error(f"Blob {blob_name} is empty (0 bytes)")
                return False
            
            logger.info(f"✓ Blob verification successful: {blob_name} ({stored_row_count} rows, {blob_size} bytes)")
            return True
            
        except Exception as e:
            logger.error(f"Error verifying blob: {e}")
            return False
    
    # ========================================================================
    # CASSANDRA DATA DELETION
    # ========================================================================
    # Deletes exported data from Cassandra after successful upload verification
    # Uses composite partition key (vehicle_id, day) to delete rows individually
    # Only executes if ENABLE_DELETE_AFTER_UPLOAD is True and verification passed
    # Logs progress every 100 rows for large datasets
    # ========================================================================
    def delete_cassandra_data(self, session, export_date: datetime, df: pd.DataFrame) -> int:
        """
        Delete data from Cassandra older than the specified export date.
        Deletes rows individually using all partition key columns.
        
        Args:
            session: Active Cassandra session
            export_date: Cutoff date - delete data older than this
            df: DataFrame containing the data that was exported (to get partition keys)
            
        Returns:
            Number of rows deleted
        """
        from datetime import date
        
        if df.empty:
            logger.info("No data to delete (DataFrame is empty)")
            return 0
        
        # Convert datetime to date for Cassandra query
        cutoff_date = export_date.date() if isinstance(export_date, datetime) else export_date
        
        logger.info(f"Deleting Cassandra data older than: {cutoff_date}")
        
        try:
            # Check if we have the required columns
            if 'vehicle_id' not in df.columns or 'day' not in df.columns:
                logger.error("Required partition key columns (vehicle_id, day) not found in DataFrame")
                return 0
            
            total_rows = len(df)
            logger.info(f"Deleting {total_rows} rows individually (composite partition key: vehicle_id, day)")
            
            # Delete each row using both partition key columns
            delete_query = f"DELETE FROM {self.cassandra_table} WHERE vehicle_id = ? AND day = ?"
            prepared_stmt = session.prepare(delete_query)
            
            deleted_count = 0
            for index, row in df.iterrows():
                vehicle_id = row['vehicle_id']
                day_value = row['day']
                
                # Convert to date if needed
                if isinstance(day_value, pd.Timestamp):
                    day_value = day_value.date()
                
                session.execute(prepared_stmt, [vehicle_id, day_value])
                deleted_count += 1
                
                # Log progress periodically
                if deleted_count % 100 == 0:
                    logger.info(f"Deleted {deleted_count}/{total_rows} rows...")
            
            logger.info(f"✓ Successfully deleted {deleted_count} rows from Cassandra")
            
            return deleted_count
            
        except Exception as e:
            logger.error(f"Error deleting Cassandra data: {e}")
            raise
    
    # ========================================================================
    # LOCAL FILE CLEANUP
    # ========================================================================
    # Removes the temporary Parquet file from local storage after successful upload
    # Helps prevent disk space buildup from repeated exports
    # ========================================================================
    def cleanup_local_file(self, filepath: str):
        """
        Remove local Parquet file after successful upload.
        
        Args:
            filepath: Path to file to remove
        """
        try:
            if os.path.exists(filepath):
                os.remove(filepath)
                logger.info(f"Removed local file: {filepath}")
            else:
                logger.warning(f"Local file not found for cleanup: {filepath}")
        except Exception as e:
            logger.error(f"Error removing local file: {e}")
            # Don't raise - cleanup failure shouldn't fail the entire process
    
    # ========================================================================
    # MAIN EXPORT WORKFLOW
    # ========================================================================
    # Orchestrates the complete export-verify-delete process:
    # 1. Calculate cutoff date (data older than retention period)
    # 2. Connect to Cassandra cluster
    # 3. Fetch data using ALLOW FILTERING
    # 4. Write to local Parquet file
    # 5. Upload to Azure Blob Storage with metadata
    # 6. Verify upload (check blob exists, row count matches)
    # 7. Delete from Cassandra (only if verification passes and deletion enabled)
    # 8. Cleanup local temporary file
    # Returns a dictionary with export statistics and status
    # ========================================================================
    def run(self) -> dict:
        """
        Execute the complete export process for all vehicles.
        Creates separate files for each vehicle.
        
        Returns:
            Dictionary with export statistics
        """
        logger.info("=" * 80)
        logger.info("Starting Cassandra to Azure Blob Storage export (Per-Vehicle)")
        logger.info("=" * 80)
        
        start_time = datetime.now()
        cluster = None
        
        try:
            # Calculate cutoff date for retention policy
            cutoff_date = self._calculate_cutoff_date()
            # Export date is the last date included (cutoff - 1 day)
            # Example: cutoff=2025-12-13 means data < 2025-12-13, so last date included is 2025-12-12
            export_date = cutoff_date - pd.Timedelta(days=1)
            
            # Connect to Cassandra
            cluster, session = self.connect_to_cassandra()
            
            # Fetch list of unique vehicle IDs
            vehicle_ids = self.fetch_vehicle_ids(session)
            
            if not vehicle_ids:
                logger.warning("No vehicles found to export")
                return {
                    'status': 'success',
                    'vehicles_processed': 0,
                    'total_rows_exported': 0,
                    'message': 'No vehicles found'
                }
            
            # Track overall statistics
            total_rows_exported = 0
            total_rows_deleted = 0
            vehicles_processed = 0
            vehicles_with_data = 0
            failed_vehicles = []
            
            # Process each vehicle separately
            for vehicle_id in vehicle_ids:
                logger.info("=" * 80)
                logger.info(f"Processing vehicle: {vehicle_id}")
                logger.info("=" * 80)
                
                local_filepath = None
                
                try:
                    # Fetch data for this vehicle
                    df = self.fetch_data_from_cassandra(session, cutoff_date, vehicle_id)
                    
                    if df.empty:
                        logger.info(f"No data to export for vehicle {vehicle_id}")
                        vehicles_processed += 1
                        continue
                    
                    vehicles_with_data += 1
                    rows_exported = len(df)
                    total_rows_exported += rows_exported
                    
                    # Write to Parquet (per-vehicle file)
                    local_filepath = self._get_local_filepath(export_date, vehicle_id)
                    rows_written = self.write_to_parquet(df, local_filepath)
                    
                    # Upload to Azure with metadata
                    self.upload_to_azure(local_filepath, rows_written, export_date, vehicle_id)
                    
                    # Verify upload
                    blob_name = self._get_parquet_filename(export_date, vehicle_id)
                    verification_passed = self.verify_blob_upload(blob_name, rows_written)
                    
                    # Delete from Cassandra if verification passed and deletion is enabled
                    if verification_passed and self.enable_delete:
                        logger.info(f"Verification passed for {vehicle_id} - proceeding with deletion")
                        
                        try:
                            deleted_count = self.delete_cassandra_data(session, export_date, df)
                            total_rows_deleted += deleted_count
                            logger.info(f"✓ Deleted {deleted_count} rows for vehicle {vehicle_id}")
                        except Exception as delete_error:
                            logger.error(f"Deletion failed for {vehicle_id}: {delete_error}")
                            failed_vehicles.append(f"{vehicle_id} (deletion failed)")
                    elif not verification_passed:
                        logger.warning(f"⚠ Verification failed for {vehicle_id} - skipping deletion")
                        failed_vehicles.append(f"{vehicle_id} (verification failed)")
                    elif not self.enable_delete:
                        logger.info(f"Deletion disabled - data retained for vehicle {vehicle_id}")
                    
                    # Cleanup local file
                    self.cleanup_local_file(local_filepath)
                    
                    vehicles_processed += 1
                    logger.info(f"✓ Completed processing vehicle {vehicle_id} ({rows_written} rows)")
                    
                except Exception as vehicle_error:
                    logger.error(f"Error processing vehicle {vehicle_id}: {vehicle_error}")
                    failed_vehicles.append(f"{vehicle_id} (error: {str(vehicle_error)})")
                    
                    # Attempt cleanup on failure
                    if local_filepath and os.path.exists(local_filepath):
                        self.cleanup_local_file(local_filepath)
                    
                    # Continue with next vehicle
                    continue
            
            # Calculate duration
            duration = (datetime.now() - start_time).total_seconds()
            
            # Summary
            logger.info("=" * 80)
            logger.info(f"Export completed in {duration:.2f} seconds")
            logger.info(f"Vehicles processed: {vehicles_processed}/{len(vehicle_ids)}")
            logger.info(f"Vehicles with data: {vehicles_with_data}")
            logger.info(f"Total rows exported: {total_rows_exported}")
            if self.enable_delete:
                logger.info(f"Total rows deleted: {total_rows_deleted}")
            if failed_vehicles:
                logger.warning(f"Failed vehicles: {', '.join(failed_vehicles)}")
            logger.info("=" * 80)
            
            return {
                'status': 'success',
                'vehicles_total': len(vehicle_ids),
                'vehicles_processed': vehicles_processed,
                'vehicles_with_data': vehicles_with_data,
                'total_rows_exported': total_rows_exported,
                'total_rows_deleted': total_rows_deleted if self.enable_delete else 0,
                'failed_vehicles': failed_vehicles,
                'duration_seconds': duration
            }
            
        except Exception as e:
            logger.error("=" * 80)
            logger.error(f"Export failed: {e}")
            logger.error("=" * 80)
            
            return {
                'status': 'failed',
                'error': str(e)
            }
            
        finally:
            # Close Cassandra connection
            if cluster:
                cluster.shutdown()
                logger.info("Cassandra connection closed")


def main():
    """Main entry point for the script."""
    try:
        exporter = CassandraToAzureExporter()
        result = exporter.run()
        
        if result['status'] == 'success':
            sys.exit(0)
        else:
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
