FROM python:3.11-slim

WORKDIR /app

# Install Java for Spark (optional)
# RUN apt-get update && apt-get install -y default-jdk-headless && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir flask flask-cors pandas requests python-dateutil

COPY . .

# Initialize DB at build time
RUN python database/init_db.py

EXPOSE 5000

CMD ["python", "backend/app.py"]
