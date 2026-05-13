FROM apache/spark:3.5.1-python3

USER root
RUN pip install --no-cache-dir pyspark==3.5.1

WORKDIR /app
COPY wiki_streaming_job.py .

CMD ["spark-submit", \
  "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1", \
  "--conf", "spark.cassandra.connection.host=cassandra", \
  "--conf", "spark.cassandra.connection.port=9042", \
  "wiki_streaming_job.py"]
