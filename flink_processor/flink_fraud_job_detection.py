from pyflink.table import (EnvironmentSettings, TableEnvironment)
from threading import Thread
from pyflink.table.udf import udf
from pyflink.table.types import DataTypes
def timestamp_filter(channel_id,timestamp):
    if channel_id not in globals():
        globals()[channel_id] = {'last_recorded_timestamp': timestamp}
        return True
    # Return True to keep the record, False to filter it out
    elif (timestamp - globals()[channel_id]['last_recorded_timestamp']).seconds >= 240:
        globals()[channel_id] = {'last_recorded_timestamp': timestamp}
        return True
    else:
        last_recorded_timestamp = globals()[channel_id]['last_recorded_timestamp']
        dt_string = last_recorded_timestamp.strftime("%Y-%m-%d %H:%M:%S %Z%z")
        print(f'Last recorded {dt_string}')
        return False


# Create a TableEnvironment with in_streaming_mode
table_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
# Create a Python UDF from the scalar function
# Register the UDF with the TableEnvironment
table_env.create_temporary_function("timestamp_filter", udf(timestamp_filter, result_type=DataTypes.BOOLEAN()))



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

# Create sink table which has non spammed measurements
table_env.execute_sql("""
    CREATE TABLE cleaned_measurements (
        channel_id STRING,
        `ts` TIMESTAMP(3) METADATA FROM 'timestamp',
        channel_value DOUBLE
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'sink.cleaned_measurements',
        'properties.bootstrap.servers' = 'localhost:9092',
        'properties.group.id' = 'group.channel.cleaned_measurements',
        'format' = 'json',
        'scan.startup.mode' = 'earliest-offset',
        'json.timestamp-format.standard' = 'ISO-8601',
        'json.fail-on-missing-field' = 'false',
        'json.ignore-parse-errors' = 'true'
    );
""")

def dump_to_sink_minute():
    table_env.sql_query("""
        SELECT channel_id, eventTime_ltz, channel_value
        WHERE timestamp_filter(channel_id, eventTime_ltz)
           """).execute_insert("cleaned_measurements").print()

thread2 = Thread(target=dump_to_sink_minute)
thread2.start()
