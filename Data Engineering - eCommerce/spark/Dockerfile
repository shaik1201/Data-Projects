FROM bitnami/spark:latest

WORKDIR /app

# Set the working directory inside the container
USER root

# Install necessary system dependencies
# - `default-mysql-client`: Enables interaction with MySQL from the command line for testing/debugging
# - Clean up apt cache to minimize image size
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y \
    wget \
    default-mysql-client \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Download Spark and Kafka integration dependencies
# - These are required to enable Spark to process Kafka streams
RUN wget https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar -P /opt/bitnami/spark/jars/
RUN wget https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar -P /opt/bitnami/spark/jars/
RUN wget https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar -P /opt/bitnami/spark/jars/
RUN wget https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar -P /opt/bitnami/spark/jars/

# Download MySQL JDBC connector
# - Required for Spark to write/read data to/from MySQL
RUN wget https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar -P /opt/bitnami/spark/jars/

COPY spark_processor.py /app/
COPY log4j.properties /app/

# Ensure the logging directory exists and is writable
# - Spark writes logs to this directory during execution
RUN mkdir -p /var/log/spark && chmod 777 /var/log/spark

# Set environment variables
# - `PYTHONUNBUFFERED`: Ensures Python output is flushed immediately, improving log visibility
ENV PYTHONUNBUFFERED=1

CMD ["spark-submit", \
    "--master", "spark://spark:7077", \
    "--name", "ECommerceProcessor", \
    "--deploy-mode", "client", \
    "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.mysql:mysql-connector-j:8.0.33", \
    "--conf", "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:///app/log4j.properties", \
    "--conf", "spark.executor.extraClassPath=/opt/bitnami/spark/jars/*", \
    "--conf", "spark.driver.extraClassPath=/opt/bitnami/spark/jars/*", \
    "spark_processor.py" \
]