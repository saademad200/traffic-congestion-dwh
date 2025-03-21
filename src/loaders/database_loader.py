import logging
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import text
from datetime import datetime
from typing import List, Dict, Any, Optional

from src.models.schema import Location, TimeRecord, Weather, TrafficMeasurement, Event, Vehicle, Infrastructure


class DatabaseLoader:
    """Loader for database operations"""
    
    def __init__(self, db_config: Dict[str, Any]):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.db_url = f"postgresql+asyncpg://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        self.engine = create_async_engine(self.db_url)
    
    async def load_locations(self, locations: List[Location]) -> Dict[str, int]:
        """Load locations into dimension table with SCD Type 2"""
        location_keys = {}
        
        try:
            async with AsyncSession(self.engine) as session:
                for location in locations:
                    # Check if location already exists
                    query = """
                    SELECT location_key FROM dim_location 
                    WHERE intersection_id = :intersection_id 
                    AND is_current = TRUE
                    """
                    result = await session.execute(
                        text(query), 
                        {"intersection_id": location.intersection_id}
                    )
                    existing = result.fetchone()
                    
                    if existing:
                        existing_key = existing[0]
                        
                        # Check if data has changed
                        query = """
                        SELECT * FROM dim_location 
                        WHERE location_key = :location_key
                        """
                        result = await session.execute(
                            text(query), 
                            {"location_key": existing_key}
                        )
                        existing_data = result.fetchone()
                        
                        # Compare data (simplified - in production would compare all fields)
                        if (existing_data.street_name != location.street_name or
                            existing_data.district != location.district or
                            existing_data.road_type != location.road_type):
                            
                            # Update existing record (set end date and is_current=False)
                            query = """
                            UPDATE dim_location 
                            SET valid_to = :valid_to, is_current = FALSE 
                            WHERE location_key = :location_key
                            """
                            await session.execute(
                                text(query), 
                                {
                                    "valid_to": datetime.now(),
                                    "location_key": existing_key
                                }
                            )
                            
                            # Insert new record
                            location_key = await self._insert_location(session, location)
                            location_keys[location.intersection_id] = location_key
                        else:
                            # No change
                            location_keys[location.intersection_id] = existing_key
                    else:
                        # Insert new record
                        location_key = await self._insert_location(session, location)
                        location_keys[location.intersection_id] = location_key
                
                await session.commit()
                
            return location_keys
        
        except Exception as e:
            self.logger.error(f"Error loading locations: {str(e)}")
            raise
    
    async def _insert_location(self, session: AsyncSession, location: Location) -> int:
        """Insert a new location record and return its key"""
        query = """
        INSERT INTO dim_location (
            intersection_id, street_name, latitude, longitude, 
            district, road_type, lanes, speed_limit, 
            valid_from, is_current
        ) VALUES (
            :intersection_id, :street_name, :latitude, :longitude,
            :district, :road_type, :lanes, :speed_limit,
            :valid_from, :is_current
        ) RETURNING location_key
        """
        result = await session.execute(
            text(query),
            {
                "intersection_id": location.intersection_id,
                "street_name": location.street_name,
                "latitude": location.latitude,
                "longitude": location.longitude,
                "district": location.district,
                "road_type": location.road_type,
                "lanes": location.lanes,
                "speed_limit": location.speed_limit,
                "valid_from": datetime.now(),
                "is_current": True
            }
        )
        location_key = result.scalar_one()
        return location_key
    
    async def load_time_records(self, time_records: List[TimeRecord]) -> Dict[datetime, int]:
        """Load time records into dimension table"""
        time_keys = {}
        
        try:
            async with AsyncSession(self.engine) as session:
                for time_record in time_records:
                    # Check if time record already exists
                    query = """
                    SELECT time_key FROM dim_time 
                    WHERE timestamp = :timestamp
                    """
                    result = await session.execute(
                        text(query), 
                        {"timestamp": time_record.timestamp}
                    )
                    existing = result.fetchone()
                    
                    if existing:
                        # Use existing key
                        time_keys[time_record.timestamp] = existing[0]
                    else:
                        # Insert new record
                        query = """
                        INSERT INTO dim_time (
                            timestamp, hour, day, day_of_week, month, quarter, year, is_holiday
                        ) VALUES (
                            :timestamp, :hour, :day, :day_of_week, :month, :quarter, :year, :is_holiday
                        ) RETURNING time_key
                        """
                        result = await session.execute(
                            text(query),
                            {
                                "timestamp": time_record.timestamp,
                                "hour": time_record.hour,
                                "day": time_record.day,
                                "day_of_week": time_record.day_of_week,
                                "month": time_record.month,
                                "quarter": time_record.quarter,
                                "year": time_record.year,
                                "is_holiday": time_record.is_holiday
                            }
                        )
                        time_key = result.scalar_one()
                        time_keys[time_record.timestamp] = time_key
                
                await session.commit()
                
            return time_keys
        
        except Exception as e:
            self.logger.error(f"Error loading time records: {str(e)}")
            raise
    
    async def load_weather_records(self, weather_records: List[Weather]) -> Dict[datetime, int]:
        """Load weather records into dimension table with SCD Type 2"""
        weather_keys = {}
        
        try:
            async with AsyncSession(self.engine) as session:
                for weather in weather_records:
                    # Find the timestamp of this weather record
                    timestamp = weather.valid_from
                    
                    # Check if weather record already exists for this timestamp
                    query = """
                    SELECT weather_key FROM dim_weather 
                    WHERE valid_from <= :timestamp 
                    AND (valid_to IS NULL OR valid_to > :timestamp)
                    AND is_current = TRUE
                    """
                    result = await session.execute(
                        text(query), 
                        {"timestamp": timestamp}
                    )
                    existing = result.fetchone()
                    
                    if existing:
                        # Use existing key
                        weather_keys[timestamp] = existing[0]
                    else:
                        # Insert new record
                        query = """
                        INSERT INTO dim_weather (
                            condition, temperature, precipitation, visibility, wind_speed, 
                            valid_from, is_current
                        ) VALUES (
                            :condition, :temperature, :precipitation, :visibility, :wind_speed,
                            :valid_from, :is_current
                        ) RETURNING weather_key
                        """
                        result = await session.execute(
                            text(query),
                            {
                                "condition": weather.condition,
                                "temperature": weather.temperature,
                                "precipitation": weather.precipitation,
                                "visibility": weather.visibility,
                                "wind_speed": weather.wind_speed,
                                "valid_from": weather.valid_from,
                                "is_current": True
                            }
                        )
                        weather_key = result.scalar_one()
                        weather_keys[timestamp] = weather_key
                
                await session.commit()
                
            return weather_keys
        
        except Exception as e:
            self.logger.error(f"Error loading weather records: {str(e)}")
            raise
    
    async def load_traffic_measurements(
        self, 
        measurements: List[TrafficMeasurement],
        location_keys: Dict[str, int],
        time_keys: Dict[datetime, int],
        weather_keys: Dict[datetime, int]
    ) -> None:
        """Load traffic measurements into fact table"""
        try:
            async with AsyncSession(self.engine) as session:
                # Process in batches for better performance
                batch_size = 1000
                for i in range(0, len(measurements), batch_size):
                    batch = measurements[i:i+batch_size]
                    
                    # Prepare values for batch insert
                    values = []
                    for measurement in batch:
                        # Get dimension keys
                        # Note: In a real implementation, you would need to map the correct keys
                        # This is simplified for the example
                        time_key = time_keys.get(measurement.measurement_timestamp)
                        
                        # Find the nearest weather timestamp
                        # In practice you might want a more sophisticated approach
                        weather_key = None
                        for weather_time, key in weather_keys.items():
                            if abs((weather_time - measurement.measurement_timestamp).total_seconds()) < 3600:  # Within 1 hour
                                weather_key = key
                                break
                        
                        # Use location_key from measurement or first available
                        # This is a simplification - in practice you need proper mapping
                        location_key = measurement.location_key or next(iter(location_keys.values()), None)
                        
                        values.append({
                            "location_key": location_key,
                            "time_key": time_key,
                            "weather_key": weather_key,
                            "vehicle_count": measurement.vehicle_count,
                            "avg_speed": measurement.avg_speed,
                            "occupancy_rate": measurement.occupancy_rate,
                            "queue_length": measurement.queue_length,
                            "travel_time": measurement.travel_time,
                            "measurement_timestamp": measurement.measurement_timestamp
                        })
                    
                    # Insert batch
                    if values:
                        query = """
                        INSERT INTO fact_traffic_measurements (
                            location_key, time_key, weather_key,
                            vehicle_count, avg_speed, occupancy_rate,
                            queue_length, travel_time, measurement_timestamp
                        ) VALUES (
                            :location_key, :time_key, :weather_key,
                            :vehicle_count, :avg_speed, :occupancy_rate,
                            :queue_length, :travel_time, :measurement_timestamp
                        )
                        """
                        await session.execute(text(query), values)
                
                await session.commit()
                
        except Exception as e:
            self.logger.error(f"Error loading traffic measurements: {str(e)}")
            raise

    async def load_events(self, events: List[Event]) -> Dict[str, int]:
        """Load events into dimension table with SCD Type 2"""
        event_keys = {}
        
        try:
            async with AsyncSession(self.engine) as session:
                for event in events:
                    # Check if event already exists
                    query = """
                    SELECT event_key FROM dim_event 
                    WHERE event_type = :event_type 
                    AND event_location = :event_location
                    AND start_time = :start_time
                    AND is_current = TRUE
                    """
                    result = await session.execute(
                        text(query), 
                        {
                            "event_type": event.event_type,
                            "event_location": event.event_location,
                            "start_time": event.start_time
                        }
                    )
                    existing = result.fetchone()
                    
                    if existing:
                        # Use existing key
                        event_keys[f"{event.event_type}_{event.event_location}_{event.start_time}"] = existing[0]
                    else:
                        # Insert new record
                        query = """
                        INSERT INTO dim_event (
                            event_type, event_location, event_size, start_time, end_time, impact_radius,
                            valid_from, is_current
                        ) VALUES (
                            :event_type, :event_location, :event_size, :start_time, :end_time, :impact_radius,
                            :valid_from, :is_current
                        ) RETURNING event_key
                        """
                        result = await session.execute(
                            text(query),
                            {
                                "event_type": event.event_type,
                                "event_location": event.event_location,
                                "event_size": event.event_size,
                                "start_time": event.start_time,
                                "end_time": event.end_time,
                                "impact_radius": event.impact_radius,
                                "valid_from": datetime.now(),
                                "is_current": True
                            }
                        )
                        event_key = result.scalar_one()
                        event_keys[f"{event.event_type}_{event.event_location}_{event.start_time}"] = event_key
                
                await session.commit()
                
            return event_keys
        
        except Exception as e:
            self.logger.error(f"Error loading events: {str(e)}")
            raise

    async def load_vehicles(self, vehicles: List[Vehicle]) -> Dict[str, int]:
        """Load vehicles into dimension table with SCD Type 2"""
        vehicle_keys = {}
        
        try:
            async with AsyncSession(self.engine) as session:
                for vehicle in vehicles:
                    # Check if vehicle already exists
                    query = """
                    SELECT vehicle_key FROM dim_vehicle 
                    WHERE vehicle_type = :vehicle_type 
                    AND vehicle_class = :vehicle_class
                    AND is_current = TRUE
                    """
                    result = await session.execute(
                        text(query), 
                        {
                            "vehicle_type": vehicle.vehicle_type,
                            "vehicle_class": vehicle.vehicle_class
                        }
                    )
                    existing = result.fetchone()
                    
                    if existing:
                        # Use existing key
                        vehicle_keys[f"{vehicle.vehicle_type}_{vehicle.vehicle_class}"] = existing[0]
                    else:
                        # Insert new record
                        query = """
                        INSERT INTO dim_vehicle (
                            vehicle_type, vehicle_class, size_category, passenger_capacity,
                            valid_from, is_current
                        ) VALUES (
                            :vehicle_type, :vehicle_class, :size_category, :passenger_capacity,
                            :valid_from, :is_current
                        ) RETURNING vehicle_key
                        """
                        result = await session.execute(
                            text(query),
                            {
                                "vehicle_type": vehicle.vehicle_type,
                                "vehicle_class": vehicle.vehicle_class,
                                "size_category": vehicle.size_category,
                                "passenger_capacity": vehicle.passenger_capacity,
                                "valid_from": datetime.now(),
                                "is_current": True
                            }
                        )
                        vehicle_key = result.scalar_one()
                        vehicle_keys[f"{vehicle.vehicle_type}_{vehicle.vehicle_class}"] = vehicle_key
                
                await session.commit()
                
            return vehicle_keys
        
        except Exception as e:
            self.logger.error(f"Error loading vehicles: {str(e)}")
            raise

    async def load_infrastructure(self, infrastructure_items: List[Infrastructure]) -> Dict[str, int]:
        """Load infrastructure into dimension table with SCD Type 2"""
        infrastructure_keys = {}
        
        try:
            async with AsyncSession(self.engine) as session:
                for infra in infrastructure_items:
                    # Check if infrastructure already exists
                    query = """
                    SELECT infrastructure_key FROM dim_infrastructure 
                    WHERE signal_type = :signal_type 
                    AND is_current = TRUE
                    """
                    result = await session.execute(
                        text(query), 
                        {
                            "signal_type": infra.signal_type
                        }
                    )
                    existing = result.fetchone()
                    
                    if existing:
                        # Use existing key
                        infrastructure_keys[infra.signal_type] = existing[0]
                    else:
                        # Insert new record
                        query = """
                        INSERT INTO dim_infrastructure (
                            signal_type, road_condition, construction_status, 
                            last_maintenance_date, capacity, special_features,
                            valid_from, is_current
                        ) VALUES (
                            :signal_type, :road_condition, :construction_status, 
                            :last_maintenance_date, :capacity, :special_features,
                            :valid_from, :is_current
                        ) RETURNING infrastructure_key
                        """
                        result = await session.execute(
                            text(query),
                            {
                                "signal_type": infra.signal_type,
                                "road_condition": infra.road_condition,
                                "construction_status": infra.construction_status,
                                "last_maintenance_date": infra.last_maintenance_date,
                                "capacity": infra.capacity,
                                "special_features": infra.special_features,
                                "valid_from": datetime.now(),
                                "is_current": True
                            }
                        )
                        infrastructure_key = result.scalar_one()
                        infrastructure_keys[infra.signal_type] = infrastructure_key
                
                await session.commit()
                
            return infrastructure_keys
        
        except Exception as e:
            self.logger.error(f"Error loading infrastructure: {str(e)}")
            raise 