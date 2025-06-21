FROM python:3.11-slim

# Install OpenJDK 17 and other dependencies
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk-headless procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Auto-detect JAVA_HOME by finding the actual installed path
# This works for both ARM64 and AMD64 architectures
RUN JAVA_HOME_PATH=$(find /usr/lib/jvm -name "java-17-openjdk-*" -type d | head -1) && \
    echo "export JAVA_HOME=$JAVA_HOME_PATH" >> /etc/environment && \
    echo "export PATH=\$PATH:\$JAVA_HOME/bin" >> /etc/environment && \
    echo "JAVA_HOME detected and set to: $JAVA_HOME_PATH"

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8501

# Use bash to ensure environment variables are loaded
CMD ["bash", "-c", "source /etc/environment && streamlit run app.py --server.port 8501 --server.enableCORS=false"]