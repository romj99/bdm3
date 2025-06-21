FROM python:3.11-slim

# Install OpenJDK 17 (consistent with other services)
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk-headless procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME for OpenJDK 17 (consistent with other services)
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8501