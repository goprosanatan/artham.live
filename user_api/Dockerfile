FROM python:3.11-slim
LABEL stage=main

# The enviroment variable ensures that the python output is set straight
# to the terminal with out buffering it first
ENV PYTHONUNBUFFERED 1
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONPATH=/app:/app/__library

WORKDIR /app

# installing all requirements
COPY requirements.txt /
RUN pip install -r /requirements.txt

COPY ./ ./

# CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]

# If running behind a proxy like Nginx or Traefik add --proxy-headers
# CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "80", "--proxy-headers"]
