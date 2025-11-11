#!/usr/bin/env python3
"""
Camera Location Data Migration Script
Extract camera location data from coordinate_speed table and migrate to new camera_locations table
"""

import json
import logging
import os
import sys
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CameraMigration:
    """Handle migration of camera data from coordinate_speed to camera_locations table"""
    
    def __init__(self, connection_string: str):
        """Initialize with database connection string"""
        self.connection_string = connection_string
        self.conn = None
        self.cursor = None
        
    def connect(self):
        """Establish database connection"""
        try:
            self.conn = psycopg2.connect(self.connection_string)
            self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)
            logger.info("Successfully connected to database")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
            
    def disconnect(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("Database connection closed")
        
    def create_camera_tables(self):
        """Create new tables for camera locations if they don't exist"""
        try:
            # Create camera_locations table
            create_camera_locations = """
            CREATE TABLE IF NOT EXISTS public.camera_locations (
                id SERIAL PRIMARY KEY,
                location_id VARCHAR(50) UNIQUE NOT NULL,
                longitude DOUBLE PRECISION NOT NULL,
                latitude DOUBLE PRECISION NOT NULL,
                altitude DOUBLE PRECISION,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            
            # Create camera_devices table
            create_camera_devices = """
            CREATE TABLE IF NOT EXISTS public.camera_devices (
                id SERIAL PRIMARY KEY,
                location_id VARCHAR(50) NOT NULL,
                camera_pid VARCHAR(100) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (location_id) REFERENCES camera_locations(location_id) ON DELETE CASCADE,
                UNIQUE(location_id, camera_pid)
            );
            """
            
            # Create spatial index for geographic queries
            create_spatial_index = """
            CREATE INDEX IF NOT EXISTS idx_camera_locations_coords 
            ON public.camera_locations USING GIST (
                ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
            );
            """
            
            # Add processed flag column to coordinate_speed if not exists
            add_processed_column = """
            ALTER TABLE public.coordinate_speed 
            ADD COLUMN IF NOT EXISTS camera_processed BOOLEAN DEFAULT FALSE;
            """
            
            # Create index on processed column
            create_processed_index = """
            CREATE INDEX IF NOT EXISTS idx_coordinate_speed_camera_processed 
            ON public.coordinate_speed(camera_processed) 
            WHERE camera_processed = FALSE;
            """
            
            self.cursor.execute(create_camera_locations)
            self.cursor.execute(create_camera_devices)
            
            # Check if PostGIS is available
            self.cursor.execute("SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'postgis');")
            has_postgis = self.cursor.fetchone()['exists']
            
            if has_postgis:
                self.cursor.execute(create_spatial_index)
                logger.info("Created spatial index for geographic queries")
            else:
                logger.warning("PostGIS not installed - skipping spatial index creation")
                
            self.cursor.execute(add_processed_column)
            self.cursor.execute(create_processed_index)
            
            self.conn.commit()
            logger.info("Successfully created/verified camera tables")
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to create tables: {e}")
            raise
            
    def fetch_unprocessed_records(self, batch_size: int = 1000) -> List[Dict]:
        """Fetch unprocessed records from coordinate_speed table"""
        try:
            query = """
            SELECT id, camera_response 
            FROM public.coordinate_speed 
            WHERE camera_response IS NOT NULL 
            AND camera_response != ''
            AND (camera_processed IS FALSE OR camera_processed IS NULL)
            LIMIT %s;
            """
            
            self.cursor.execute(query, (batch_size,))
            records = self.cursor.fetchall()
            logger.info(f"Fetched {len(records)} unprocessed records")
            return records
            
        except Exception as e:
            logger.error(f"Failed to fetch records: {e}")
            raise
            
    def parse_camera_response(self, response_text: str) -> Optional[List[Dict]]:
        """Parse camera response JSON and extract data if valid"""
        try:
            if not response_text or response_text == 'NULL':
                return None
                
            response = json.loads(response_text)
            
            # Check if response is successful
            if (response.get('message') == 'Thành công!' and 
                response.get('statusCode') == 200 and 
                'data' in response):
                return response['data']
                
            return None
            
        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse JSON: {e}")
            return None
        except Exception as e:
            logger.warning(f"Unexpected error parsing response: {e}")
            return None
            
    def insert_camera_data(self, camera_data: List[Dict]) -> int:
        """Insert camera location and device data"""
        inserted_count = 0
        
        for location in camera_data:
            try:
                location_id = location.get('_id')
                coords = location.get('coords', [])
                pids = location.get('pids', [])
                
                if not location_id or len(coords) < 2:
                    logger.warning(f"Skipping invalid location data: {location}")
                    continue
                    
                # Extract coordinates
                longitude = coords[0]
                latitude = coords[1]
                altitude = coords[2] if len(coords) > 2 else None
                
                # Insert or update camera location
                upsert_location = """
                INSERT INTO public.camera_locations 
                    (location_id, longitude, latitude, altitude, updated_at)
                VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (location_id) 
                DO UPDATE SET
                    longitude = EXCLUDED.longitude,
                    latitude = EXCLUDED.latitude,
                    altitude = EXCLUDED.altitude,
                    updated_at = CURRENT_TIMESTAMP;
                """
                
                self.cursor.execute(upsert_location, 
                    (location_id, longitude, latitude, altitude))
                
                # Insert camera devices
                for pid in pids:
                    if pid:  # Skip empty PIDs
                        insert_device = """
                        INSERT INTO public.camera_devices (location_id, camera_pid)
                        VALUES (%s, %s)
                        ON CONFLICT (location_id, camera_pid) DO NOTHING;
                        """
                        self.cursor.execute(insert_device, (location_id, pid))
                        
                inserted_count += 1
                
            except Exception as e:
                logger.error(f"Failed to insert location {location}: {e}")
                raise
                
        return inserted_count
        
    def mark_as_processed(self, record_ids: List[int]):
        """Mark records as processed in coordinate_speed table"""
        try:
            if not record_ids:
                return
                
            update_query = """
            UPDATE public.coordinate_speed 
            SET camera_processed = TRUE 
            WHERE id = ANY(%s);
            """
            
            self.cursor.execute(update_query, (record_ids,))
            logger.info(f"Marked {len(record_ids)} records as processed")
            
        except Exception as e:
            logger.error(f"Failed to mark records as processed: {e}")
            raise
            
    def process_batch(self, batch_size: int = 1000) -> Tuple[int, int]:
        """Process a batch of records"""
        try:
            # Fetch unprocessed records
            records = self.fetch_unprocessed_records(batch_size)
            
            if not records:
                logger.info("No unprocessed records found")
                return 0, 0
                
            processed_ids = []
            total_locations = 0
            
            for record in records:
                record_id = record['id']
                camera_response = record['camera_response']
                
                # Parse camera response
                camera_data = self.parse_camera_response(camera_response)
                
                if camera_data:
                    # Insert camera data
                    inserted = self.insert_camera_data(camera_data)
                    total_locations += inserted
                    
                # Mark as processed regardless of whether data was valid
                processed_ids.append(record_id)
                
            # Mark records as processed
            self.mark_as_processed(processed_ids)
            
            # Commit transaction
            self.conn.commit()
            
            logger.info(f"Processed batch: {len(processed_ids)} records, {total_locations} locations")
            return len(processed_ids), total_locations
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to process batch: {e}")
            raise
            
    def get_migration_stats(self) -> Dict:
        """Get migration statistics"""
        try:
            stats = {}
            
            # Total records in coordinate_speed
            self.cursor.execute("""
                SELECT COUNT(*) as total,
                       COUNT(CASE WHEN camera_processed = TRUE THEN 1 END) as processed,
                       COUNT(CASE WHEN camera_response IS NOT NULL 
                             AND camera_response != '' 
                             AND (camera_processed IS FALSE OR camera_processed IS NULL) 
                             THEN 1 END) as pending
                FROM public.coordinate_speed;
            """)
            record_stats = self.cursor.fetchone()
            stats['records'] = record_stats
            
            # Camera locations count
            self.cursor.execute("SELECT COUNT(*) as count FROM public.camera_locations;")
            stats['locations'] = self.cursor.fetchone()['count']
            
            # Camera devices count
            self.cursor.execute("SELECT COUNT(*) as count FROM public.camera_devices;")
            stats['devices'] = self.cursor.fetchone()['count']
            
            return stats
            
        except Exception as e:
            logger.error(f"Failed to get statistics: {e}")
            return {}
            
    def run_migration(self, batch_size: int = 1000, max_batches: int = None):
        """Run the complete migration process"""
        try:
            self.connect()
            
            # Create tables if needed
            self.create_camera_tables()
            
            # Get initial stats
            initial_stats = self.get_migration_stats()
            logger.info(f"Initial stats: {initial_stats}")
            
            # Process batches
            batch_count = 0
            total_records = 0
            total_locations = 0
            
            while True:
                if max_batches and batch_count >= max_batches:
                    logger.info(f"Reached maximum batch limit ({max_batches})")
                    break
                    
                records_processed, locations_inserted = self.process_batch(batch_size)
                
                if records_processed == 0:
                    logger.info("No more records to process")
                    break
                    
                batch_count += 1
                total_records += records_processed
                total_locations += locations_inserted
                
                logger.info(f"Batch {batch_count} completed: {records_processed} records, {locations_inserted} locations")
                
            # Get final stats
            final_stats = self.get_migration_stats()
            logger.info(f"Final stats: {final_stats}")
            
            logger.info(f"Migration completed: {total_records} records processed, {total_locations} locations inserted")
            
        except Exception as e:
            logger.error(f"Migration failed: {e}")
            raise
        finally:
            self.disconnect()


def main():
    """Main entry point"""
    # Get database connection from environment or use default
    db_config = os.environ.get('DATABASE_URL')
    
    if not db_config:
        logger.error("DATABASE_URL environment variable not set")
        sys.exit(1)
        
    # Parse batch size from environment
    batch_size = int(os.environ.get('BATCH_SIZE', '1000'))
    max_batches = os.environ.get('MAX_BATCHES')
    max_batches = int(max_batches) if max_batches else None
    
    # Run migration
    migration = CameraMigration(db_config)
    
    try:
        migration.run_migration(batch_size, max_batches)
        logger.info("Migration completed successfully")
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
