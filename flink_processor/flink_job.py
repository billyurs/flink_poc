from pyflink.table import (EnvironmentSettings, TableEnvironment)
from threading import Thread

# Create a TableEnvironment with in_streaming_mode
table_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())

# Set the necessary classpath for the Kafka connector and parallelism to 1
table_env.get_config().get_configuration().set_string("pipeline.classpaths", "file:///Users/symphonyai/Documents/work/flink-tutorial/flink_poc/compose/jars/flink-sql-connector-kafka-1.17.0.jar;"
                                                                             "file:///Users/symphonyai/Documents/work/flink-tutorial/flink_poc/compose/jars/flink-shaded-force-shading-16.1.jar")
table_env.get_config().set("parallelism.default", "1")

# Create source table "measurements" with a watermark of 30 seconds
table_env.execute_sql("""
    CREATE TABLE measurements (
        channel_id STRING,
        channel_value DOUBLE,
        hourly_frequency DOUBLE,
        eventTime_ltz AS TO_TIMESTAMP_LTZ(`timestamp`, 3),
        `ts` TIMESTAMP(3) METADATA FROM 'timestamp',
        `timestamp` BIGINT,
        WATERMARK FOR eventTime_ltz AS eventTime_ltz - INTERVAL '30' SECONDS
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'channel.measurements',
        'properties.bootstrap.servers' = 'localhost:9092',
        'properties.group.id' = 'group.channel.measurements',
        'format' = 'json',
        'scan.startup.mode' = 'earliest-offset',
        'json.timestamp-format.standard' = 'ISO-8601',
        'json.fail-on-missing-field' = 'false',
        'json.ignore-parse-errors' = 'true'
    );
""")

# Query to calculate the rolling 1 minute window statistics
every_five_minute_result_query = table_env.sql_query("""
    SELECT
        channel_id,
        window_start,
        window_end,
        1 as hourly_frequency,
        COUNT(channel_value) AS totalReadings,
        LISTAGG(CAST(channel_value AS STRING)) AS readingsList,
        ROUND(AVG(channel_value),1) as averageReading,
        ROUND(MAX(channel_value),1) as  maxReading,
        ROUND(MIN(channel_value),1) as  minReading,
        ROUND(STDDEV(channel_value),1) as  stdReading
    FROM TABLE(TUMBLE(TABLE measurements, DESCRIPTOR(eventTime_ltz), INTERVAL '5' MINUTE))
    GROUP BY channel_id, window_start, window_end;
 """)

# Create sink table to output the 5 minute rolling window statistics
table_env.execute_sql("""
    CREATE TABLE sink (
        channel_id STRING,
        window_start TIMESTAMP(3) NOT NULL,
        window_end TIMESTAMP(3) NOT NULL,
        hourly_frequency DOUBLE,
        totalReadings BIGINT NOT NULL,
        readingsList STRING,
        averageReading DOUBLE,
        maxReading DOUBLE,
        minReading DOUBLE,
        stdReading DOUBLE
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'sink.measurement_reports',
        'properties.bootstrap.servers' = 'localhost:9092',
        'properties.group.id' = 'group.channel.measurements',
        'format' = 'json',
        'scan.startup.mode' = 'earliest-offset',
        'json.timestamp-format.standard' = 'ISO-8601',
        'json.fail-on-missing-field' = 'false',
        'json.ignore-parse-errors' = 'true'
    );
""")
def dump_to_sink_minute():
    every_five_minute_result_query.execute_insert("sink").print()

thread2 = Thread(target=dump_to_sink_minute)
thread2.start()
