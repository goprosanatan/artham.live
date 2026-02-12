FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Install build deps and runtime libs if needed
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY services/requirements.txt /app/services-requirements.txt
RUN pip install --no-cache-dir -r /app/services-requirements.txt

# Copy the whole repo so local 'library' package is available
COPY . /app/

# Ensure Python can import local library package
ENV PYTHONPATH="/app/__library:/app"

WORKDIR /app/services

# # Default command can be overridden per service in docker-compose
# CMD ["python", "tick_01_ingestor.py"]
