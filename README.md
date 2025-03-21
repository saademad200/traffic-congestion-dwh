# Traffic Flow Data Warehouse

A comprehensive data warehousing solution for analyzing and optimizing urban traffic patterns and congestion management.

## Project Overview

This project implements a data warehouse for urban traffic flow analysis with a complete ETL (Extract, Transform, Load) pipeline. It integrates data from multiple sources related to traffic measurements, locations, weather conditions, events, vehicles, and infrastructure to provide a holistic view of traffic patterns and congestion factors.

### Key Features

- **Multi-dimensional analysis** of traffic patterns
- **Complete ETL pipeline** with robust error handling
- **Data quality validation** throughout the pipeline
- **Containerized deployment** using Docker
- **Comprehensive testing suite**
- **Advanced analytical queries** for traffic insights

## Data Warehouse Schema

This data warehouse follows a star schema design with:

### Fact Tables
- **Traffic Measurements**: Vehicle counts, speeds, occupancy rates, and other metrics

### Dimension Tables
- **Location**: Intersections, road types, coordinates
- **Time**: Date/time hierarchy with holiday flags
- **Weather**: Conditions, temperature, precipitation
- **Event**: Scheduled events impacting traffic
- **Vehicle**: Vehicle types and classifications
- **Infrastructure**: Traffic signals, road conditions

## Setup Instructions

### Prerequisites
- Python 3.9+
- PostgreSQL 13+
- Docker and Docker Compose (optional)

### Local Development Setup

1. Clone the repository:
```bash
git clone https://github.com/yourusername/traffic-congestion-dwh.git
cd traffic-congestion-dwh
```

2. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Configure the application:
   - Update `src/config/config.py` with your database and data source settings

5. Run the ETL pipeline:
```bash
python src/main.py
```

### Docker Setup

1. Build and start the containers:
```bash
docker-compose up -d
```

2. Check the logs:
```bash
docker-compose logs -f
```

## Using the Data Warehouse

### Running the ETL Pipeline

The ETL pipeline can be run in several ways:

1. **Full load**:
```bash
python src/main.py
```

2. **Specify an Excel data source**:
```bash
EXCEL_FILE_PATH=data/traffic_flow_data.xlsx python src/main.py
```

### Exploratory Data Analysis

The project includes a Jupyter notebook for exploratory data analysis:

1. Start Jupyter:
```bash
jupyter notebook
```

2. Open `eda/traffic_analysis.ipynb`

### Example Queries

The `queries/` directory contains example SQL queries for common analytical needs:

1. **Basic aggregation** - Daily traffic volume by district
2. **Time series analysis** - Hourly traffic patterns by day of week
3. **Weather impact** - Traffic metrics by weather condition
4. **Event analysis** - Traffic before, during, and after events
5. **Infrastructure analysis** - Performance by signal type and road condition
6. **Congestion ranking** - Top congested locations with window functions
7. **Peak hour detection** - Using rolling averages
8. **Multidimensional analysis** - Using CUBE for cross-dimensional insights
9. **Year-over-year comparison** - Trend analysis
10. **Complex traffic pattern detection** - Identifying bottlenecks during events

## Testing

### Running Tests

Execute the test suite:

```bash
# Run all tests
pytest

# Run specific test categories
pytest tests/test_extractors.py
pytest tests/test_transformers.py
pytest tests/test_etl_pipeline.py
pytest tests/test_integration.py
pytest tests/test_data_quality.py

# Run with coverage
pytest --cov=src tests/
```

### Test Data

Sample test data is located in `tests/data/` directory. To generate new test data:

```bash
python -m src.utils.sample_data_generator
```

## CI/CD Pipeline

This project uses GitHub Actions for continuous integration, running automated tests and code quality checks on every push and pull request.

### CI Workflow

The CI pipeline performs:
- Code linting with flake8
- Unit tests with pytest
- Test coverage reporting

To see the CI pipeline results, check the "Actions" tab in the GitHub repository.

## Design Documentation

The data warehouse design is documented in the following files:

- `Design/DWH.Design.Template.xlsx` - Dimension and fact table specifications
- `Design/Traffic.Flow.pdf` - Conceptual design and data flow diagrams
- `Design/Traffic.xlsx` - Source data specifications

## Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature-name`
3. Commit your changes: `git commit -am 'Add feature'`
4. Push to the branch: `git push origin feature-name`
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
