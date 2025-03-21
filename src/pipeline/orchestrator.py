class ETLOrchestrator:
    def __init__(self, config):
        self.config = config
        self.logger = setup_logger(__name__)

    async def run_pipeline(self):
        try:
            # 1. Extract data from all sources
            raw_data = await self._extract_all_sources()
            
            # 2. Transform data
            transformed_data = await self._transform_data(raw_data)
            
            # 3. Load dimensions first
            await self._load_dimensions(transformed_data)
            
            # 4. Load facts
            await self._load_facts(transformed_data)
            
            self.logger.info("ETL pipeline completed successfully")
            
        except Exception as e:
            self.logger.error(f"Pipeline failed: {str(e)}")
            raise 