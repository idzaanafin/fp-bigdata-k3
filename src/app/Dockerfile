# ============================================================
# Dockerfile — Runtime container (Flask + PySpark + Hadoop Client)
# ============================================================
FROM python:3.9-slim-bookworm

# Copy Hadoop installation from apache/hadoop:3 image
# (image ini sudah di-pull oleh docker-compose untuk namenode & datanode)
COPY --from=apache/hadoop:3 /opt/hadoop /opt/hadoop

# Install Java OpenJDK 17 & tools
RUN apt-get update && apt-get install -y --fix-missing \
    openjdk-17-jre-headless \
    procps \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
ENV HADOOP_USER_NAME=hadoop
ENV PATH=$HADOOP_HOME/bin:$PATH
ENV LD_LIBRARY_PATH=/opt/hadoop/lib/native:$LD_LIBRARY_PATH

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Hadoop core-site.xml — point ke HDFS namenode
COPY hadoop-conf/core-site.xml $HADOOP_HOME/etc/hadoop/core-site.xml

EXPOSE 5000
CMD ["python", "src/app/app.py"]
