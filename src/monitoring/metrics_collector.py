class MetricsCollector:
    def __init__(self):
        self.prometheus_client = PrometheusClient()
        
    def record_pipeline_metrics(self, pipeline_stats):
        # Record extraction metrics
        self.prometheus_client.gauge(
            'etl_records_extracted',
            pipeline_stats['extracted_count']
        )
        
        # Record processing time
        self.prometheus_client.histogram(
            'etl_processing_time',
            pipeline_stats['processing_time']
        ) 