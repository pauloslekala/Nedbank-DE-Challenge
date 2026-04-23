FROM nedbank-de-challenge/base:1.0

# Install any additional Python dependencies you need beyond the base image.
# Leave requirements.txt empty if the base packages are sufficient.
WORKDIR /app

#procps (suppresses the 'ps: command not found' warning)
RUN apt-get update && apt-get install -y --no-install-recommends procps \
    && rm -rf /var/lib/apt/lists/*

#patch /etc/hosts at build time with a stable alias
#         We can't know the runtime hostname, but we CAN make 'localhost' resolve
RUN echo "127.0.0.1 localhost" >> /etc/hosts || true

#Download Delta JARs at build time (network available here)
# and place them directly in PySpark's jars dir so Spark loads them automatically
RUN curl -fL https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.1.0/delta-spark_2.12-3.1.0.jar \
         -o /usr/local/lib/python3.11/site-packages/pyspark/jars/delta-spark_2.12-3.1.0.jar && \
    curl -fL https://repo1.maven.org/maven2/io/delta/delta-storage/3.1.0/delta-storage-3.1.0.jar \
         -o /usr/local/lib/python3.11/site-packages/pyspark/jars/delta-storage-3.1.0.jar


#tell Spark to never look up the hostname
ENV SPARK_LOCAL_IP=127.0.0.1
ENV SPARK_LOCAL_HOSTNAME=localhost

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy pipeline code and configuration into the image.
# Do NOT copy data files or output directories — these are injected at runtime
# via Docker volume mounts by the scoring system.
#COPY pipeline/ pipeline/
#COPY config/ config/
# Copy your pipeline code and config
COPY pipeline/ /app/pipeline/
COPY config/ /app/config/
COPY requirements.txt /app/

# Entry point — must run the complete pipeline end-to-end without interactive input.
# The scoring system uses this CMD directly; do not require TTY or stdin.
CMD ["python", "pipeline/run_all.py"]
