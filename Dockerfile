FROM python:3.11-slim

WORKDIR /app

# Install system dependencies required for psycopg2
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create necessary directories
RUN mkdir -p /app/output /app/logs

# Copy the source code
COPY src/ /app/src/

# Set Python path to include the app directory
ENV PYTHONPATH=/app

# Set executable permissions for main.py
RUN chmod +x /app/src/main.py

# Run the ETL process
CMD ["python", "/app/src/main.py"] 