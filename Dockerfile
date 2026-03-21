FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY . .

# Create logs dir
RUN mkdir -p logs data

# Environment
ENV PYTHONUNBUFFERED=1

# Entrypoint
CMD ["python", "api_wrapper.py"]