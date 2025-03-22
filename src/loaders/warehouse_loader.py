import pandas as pd
import os
from sqlalchemy import create_engine
import logging
from typing import Dict, Any
from sqlalchemy.sql import text

logger = logging.getLogger(__name__)

class WarehouseLoader:
    """Loader for warehouse tables (dimensions and facts)"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.db_config = config['database']
        self.output_dir = os.environ.get('OUTPUT_DIR', '/app/output')
        self.use_db = os.environ.get('USE_DATABASE', 'True').lower() == 'true'
        
        if self.use_db:
            # Create database connection
            self.connection_string = f"{self.db_config['type']}://{self.db_config['user']}:{self.db_config['password']}@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}"
            self.engine = create_engine(self.connection_string)
            logger.info(f"Database connection established to {self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}")
        else:
            # Ensure output directory exists
            os.makedirs(self.output_dir, exist_ok=True)
            logger.info(f"CSV output directory set to {self.output_dir}")
    
    def load_table(self, table_name: str, df: pd.DataFrame):
        """Load dataframe to database or CSV file"""
        if df.empty:
            logger.warning(f"Empty dataframe for {table_name}, skipping load")
            return
            
        if self.use_db:
            # Load to database
            schema = self.db_config['schema']
            table = f"{schema}.{table_name}"
            
            try:
                df.to_sql(
                    name=table_name,
                    schema=schema,
                    con=self.engine,
                    if_exists='replace',
                    index=False,
                    chunksize=1000
                )
                logger.info(f"Loaded {len(df)} rows to database table {table}")
            except Exception as e:
                logger.error(f"Error loading {table_name} to database: {str(e)}")
                raise
        else:
            # Save to CSV
            output_path = os.path.join(self.output_dir, f"{table_name}.csv")
            df.to_csv(output_path, index=False)
            logger.info(f"Saved {len(df)} rows to CSV file {output_path}")

    async def load_dimension(self, dimension_name, data):
        try:
            async with self.engine.begin() as conn:
                # SCD Type 2 implementation
                await self._handle_scd2_updates(conn, dimension_name, data)
                
        except Exception as e:
            self.logger.error(f"Dimension load failed: {str(e)}")
            raise

    async def load_facts(self, fact_data):
        try:
            async with self.engine.begin() as conn:
                await conn.execute(
                    text("INSERT INTO fact_traffic_measurements VALUES (:values)"),
                    fact_data
                )
        except Exception as e:
            self.logger.error(f"Fact load failed: {str(e)}")
            raise 