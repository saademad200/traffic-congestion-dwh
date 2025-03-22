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
- **Traffic Events**: Vehicle counts, speeds, accident reports, congestion levels, and other metrics

### Dimension Tables
- **Location**: Traffic measurement locations
- **Date**: Calendar attributes including weekends and holidays
- **Time**: Hour and minute with day segments and peak hour flags
- **Vehicle**: Vehicle types and classifications
- **Event Type**: Types of traffic events with severity scales
- **Environmental**: Weather conditions and temperature

## Setup Instructions

### Prerequisites
- Python 3.12+
- PostgreSQL 13
- Docker and Docker Compose (for containerized deployment)

### Local Development Setup

1. Clone the repository:
```bash
git clone https://github.com/yourusername/traffic-flow-dwh.git
cd traffic-flow-dwh
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
   - Copy the `.env.example` to `.env` (if available) or create a new `.env` file
   - Update environment variables in the `.env` file with your database and data source settings

5. Run the ETL pipeline:
```bash
python src/main.py
```

### Docker Setup

1. Ensure your `.env` file is configured properly
   - The `.env` file contains database credentials and configuration settings

2. Build and start the containers:
```bash
docker-compose up -d
```

3. Check the logs:
```bash
docker-compose logs -f etl_service
```

4. Access PgAdmin:
   - Navigate to http://localhost:5050
   - Login with credentials from docker-compose.yml (default: admin@admin.com / admin)
   - Connect to the database using the warehouse_db service name

## Using the Data Warehouse

### Running the ETL Pipeline

The ETL pipeline can be run in several ways:

1. **Full load**:
```bash
python src/main.py
```

2. **Specify an Excel data source** (using environment variable):
```bash
SOURCE_FILE=data/traffic_flow_data.xlsx python src/main.py
```

### Exploratory Data Analysis

The project includes Jupyter notebooks for exploratory data analysis:

1. Start the Jupyter container (if using Docker):
```bash
docker-compose up -d jupyter
```
   Or start Jupyter locally:
```bash
jupyter notebook
```

2. Access Jupyter Lab:
   - Navigate to http://localhost:8888 (no token required if using Docker setup)
   - Open `notebooks/traffic_flow_analysis.ipynb` or `notebooks/exploratory_data_analysis.ipynb`


## CI/CD Pipeline

This project uses GitHub Actions for continuous integration, running automated tests and code quality checks on every push and pull request.

### CI Workflow

The CI pipeline performs:
- Code linting with flake8

To see the CI pipeline results, check the "Actions" tab in the GitHub repository.
