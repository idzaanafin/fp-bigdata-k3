# Kita kunci OS-nya pakai slim-bookworm (Debian 12) agar versinya stabil dan tidak berubah sendiri
FROM python:3.9-slim-bookworm

# Install Java OpenJDK 17 & dependensi dasar Linux (Pasti tersedia di Debian Bookworm)
RUN apt-get update && apt-get install -y --fix-missing \
    openjdk-17-jre-headless \
    procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME diarahkan ke lokasi Java 17
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

WORKDIR /app

# Copy requirements dari root context ke container
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Port 5000 untuk Flask Web Dashboard
EXPOSE 5000

# Default command saat container naik
CMD ["python", "src/app/app.py"]