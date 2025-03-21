from sqlalchemy import create_engine
from sqlalchemy.sql import text
from utils.logger import setup_logger

class WarehouseLoader:
    def __init__(self, db_config):
        self.engine = create_engine(db_config['url'])
        self.logger = setup_logger(__name__)

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