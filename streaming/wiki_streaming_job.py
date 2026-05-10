from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json,
    col,
    to_timestamp,
    window,
    count,
    approx_count_distinct,
    avg,
    sum as spark_sum,
    when,
    lit,
    current_timestamp,
    explode,
    split,
    lower,
    regexp_replace,
    collect_set,
    size,
    concat_ws,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    BooleanType,
    IntegerType,
)


KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
INPUT_TOPIC = "wiki-page-create"
CASSANDRA_KEYSPACE = "wiki_analytics"


schema = StructType([
    StructField("domain", StringType(), True),
    StructField("page_id", LongType(), True),
    StructField("page_title", StringType(), True),
    StructField("user_id", LongType(), True),
    StructField("user_name", StringType(), True),
    StructField("is_bot", BooleanType(), True),
    StructField("created_at", StringType(), True),
    StructField("title_length", IntegerType(), True),
])


def write_raw_pages(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        print(f"[SPARK] Batch {batch_id}: no raw pages")
        return

    pages = batch_df.select(
        "domain",
        "created_at_ts",
        "page_id",
        "page_title",
        "user_id",
        "user_name",
        "is_bot",
        "title_length",
    ).withColumnRenamed("created_at_ts", "created_at")

    pages.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="raw_pages", keyspace=CASSANDRA_KEYSPACE) \
        .save()

    pages.select(
        "user_id",
        "created_at",
        "page_id",
        "domain",
        "page_title",
        "user_name",
        "is_bot",
    ).write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="pages_by_user", keyspace=CASSANDRA_KEYSPACE) \
        .save()

    pages.select(
        "page_id",
        "domain",
        "created_at",
        "page_title",
        "user_id",
        "user_name",
        "is_bot",
        "title_length",
    ).write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="pages_by_id", keyspace=CASSANDRA_KEYSPACE) \
        .save()

    print(f"[SPARK] Batch {batch_id}: raw pages written to Cassandra")


def write_language_activity(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        print(f"[SPARK] Batch {batch_id}: no language activity")
        return

    batch_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="language_activity", keyspace=CASSANDRA_KEYSPACE) \
        .save()

    print(f"[SPARK] Batch {batch_id}: language activity written to Cassandra")


def write_bot_activity(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        print(f"[SPARK] Batch {batch_id}: no bot activity")
        return

    batch_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="bot_activity_metrics", keyspace=CASSANDRA_KEYSPACE) \
        .save()

    print(f"[SPARK] Batch {batch_id}: bot activity written to Cassandra")


def write_breaking_news_alerts(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        print(f"[SPARK] Batch {batch_id}: no breaking news alerts")
        return

    alerts = batch_df.withColumn(
        "alert_time",
        current_timestamp()
    ).withColumn(
        "alert_type",
        lit("keyword_burst")
    ).withColumn(
        "spike_ratio",
        lit(None).cast("double")
    ).select(
        "domain",
        "alert_time",
        "alert_type",
        "keyword",
        col("occurrences").alias("pages_count"),
        col("sample_pages"),
        "spike_ratio",
    )

    alerts.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="breaking_news_alerts", keyspace=CASSANDRA_KEYSPACE) \
        .save()

    print(f"[SPARK] Batch {batch_id}: breaking news alerts written to Cassandra")


def write_spam_alerts(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        print(f"[SPARK] Batch {batch_id}: no events for spam check")
        return

    suspicious = batch_df.withColumn(
        "reason",
        when(
            col("page_title").rlike("(?i).*https?://.*|.*www\\..*"),
            lit("title_contains_url")
        )
        .when(
            col("page_title").rlike(".*[0-9]{7,}.*"),
            lit("title_contains_many_digits")
        )
        .when(
            col("title_length") < 3,
            lit("title_too_short")
        )
        .when(
            col("title_length") > 100,
            lit("title_too_long")
        )
        .otherwise(lit(None))
    ).filter(
        (col("reason").isNotNull()) & (col("is_bot") == False)
    ).withColumn(
        "severity",
        when(col("reason") == "title_contains_url", lit("high"))
        .when(col("reason") == "title_contains_many_digits", lit("medium"))
        .otherwise(lit("low"))
    ).withColumn(
        "alert_time",
        current_timestamp()
    ).select(
        "domain",
        "alert_time",
        "severity",
        "user_id",
        "user_name",
        "reason",
        "page_title",
        "page_id",
    )

    if suspicious.rdd.isEmpty():
        print(f"[SPARK] Batch {batch_id}: no spam alerts")
        return

    suspicious.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="spam_alerts", keyspace=CASSANDRA_KEYSPACE) \
        .save()

    print(f"[SPARK] Batch {batch_id}: spam alerts written to Cassandra")


def main():
    spark = SparkSession.builder \
        .appName("WikipediaStreamingAnalytics") \
        .config("spark.cassandra.connection.host", "cassandra") \
        .config("spark.cassandra.connection.port", "9042") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    raw_kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", INPUT_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    events = raw_kafka_df.selectExpr("CAST(value AS STRING) AS json_value") \
        .select(from_json(col("json_value"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("created_at_ts", to_timestamp(col("created_at"))) \
        .filter(col("domain").isNotNull()) \
        .filter(col("page_id").isNotNull()) \
        .filter(col("created_at_ts").isNotNull()) \
        .filter(col("page_title").isNotNull()) \
        .filter(col("user_id").isNotNull())

    raw_query = events.writeStream \
        .foreachBatch(write_raw_pages) \
        .outputMode("append") \
        .option("checkpointLocation", "/tmp/checkpoints/wiki_raw_pages") \
        .trigger(processingTime="30 seconds") \
        .start()

    language_activity = events \
        .withWatermark("created_at_ts", "2 minutes") \
        .groupBy(
            col("domain"),
            window(col("created_at_ts"), "1 minute")
        ) \
        .agg(
            count("*").alias("pages_created"),
            approx_count_distinct("user_id").alias("unique_authors"),
            avg("title_length").alias("avg_title_length")
        ) \
        .select(
            col("domain"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("pages_created").cast("int"),
            col("unique_authors").cast("int"),
            col("avg_title_length").cast("double"),
            lit("stable").alias("trend")
        )

    language_query = language_activity.writeStream \
        .foreachBatch(write_language_activity) \
        .outputMode("update") \
        .option("checkpointLocation", "/tmp/checkpoints/wiki_language_activity") \
        .trigger(processingTime="30 seconds") \
        .start()

    bot_activity = events \
        .withWatermark("created_at_ts", "2 minutes") \
        .groupBy(
            col("domain"),
            window(col("created_at_ts"), "1 minute")
        ) \
        .agg(
            spark_sum(when(col("is_bot") == True, 1).otherwise(0)).alias("bot_pages"),
            spark_sum(when(col("is_bot") == False, 1).otherwise(0)).alias("human_pages"),
            count("*").alias("total_pages")
        ) \
        .select(
            col("domain"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("bot_pages").cast("int"),
            col("human_pages").cast("int"),
            when(
                col("total_pages") > 0,
                ((col("bot_pages") / col("total_pages")) * 100)
            ).otherwise(0.0).cast("double").alias("bot_percent"),
            lit("[]").alias("top_bots"),
            lit("[]").alias("top_humans")
        )

    bot_query = bot_activity.writeStream \
        .foreachBatch(write_bot_activity) \
        .outputMode("update") \
        .option("checkpointLocation", "/tmp/checkpoints/wiki_bot_activity") \
        .trigger(processingTime="30 seconds") \
        .start()



    stop_words = [
    "the", "and", "of", "in", "on", "for", "to", "a", "an",
    "та", "і", "в", "у", "на", "з", "до", "для",
    "de", "la", "le", "el", "en", "der", "die", "das"
    ]

    words = events.select(
        "domain",
        "created_at_ts",
        "page_title",
        explode(
            split(
                lower(
                    regexp_replace(col("page_title"), "[^A-Za-zА-Яа-яІіЇїЄєҐґ0-9]+", " ")
                ),
                "\\s+"
            )
        ).alias("keyword")
    ).filter(
        col("keyword") != ""
    ).filter(
        ~col("keyword").isin(stop_words)
    ).filter(
        col("keyword").rlike(".{4,}")
    ).filter(
        ~col("keyword").rlike("^[0-9]+$")
    )

    breaking_news = words \
        .withWatermark("created_at_ts", "2 minutes") \
        .groupBy(
            col("domain"),
            col("keyword"),
            window(col("created_at_ts"), "10 minutes")
        ) \
        .agg(
            count("*").alias("occurrences"),
            collect_set("page_title").alias("pages")
        ) \
        .filter(col("occurrences") >= 5) \
        .select(
            "domain",
            "keyword",
            "occurrences",
            concat_ws(", ", col("pages")).alias("sample_pages")
        )

    breaking_query = breaking_news.writeStream \
        .foreachBatch(write_breaking_news_alerts) \
        .outputMode("update") \
        .option("checkpointLocation", "/tmp/checkpoints/wiki_breaking_news") \
        .trigger(processingTime="30 seconds") \
        .start()
    spam_query = events.writeStream \
        .foreachBatch(write_spam_alerts) \
        .outputMode("append") \
        .option("checkpointLocation", "/tmp/checkpoints/wiki_spam_alerts") \
        .trigger(processingTime="30 seconds") \
        .start()

    print("[SPARK] Wikipedia streaming job started")

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()