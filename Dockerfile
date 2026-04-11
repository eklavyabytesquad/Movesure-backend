FROM python:3.12-slim

WORKDIR /app

# Install dependencies first (cached layer)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Non-root user for security
RUN useradd -r appuser && chown -R appuser /app
USER appuser

EXPOSE 5000

# Gunicorn with uvicorn workers — 2 workers, 120s timeout
CMD ["gunicorn", "app:app", "-k", "uvicorn.workers.UvicornWorker", "-w", "2", "--bind", "0.0.0.0:5000", "--timeout", "120", "--graceful-timeout", "30"]
