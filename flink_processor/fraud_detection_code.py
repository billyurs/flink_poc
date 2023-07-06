from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.table import StreamTableEnvironment

# Initialize the StreamExecutionEnvironment
env = StreamExecutionEnvironment.get_execution_environment()
# Add the JAR file to the classpath
env.add_jars("file:///Users/symphonyai/Documents/work/flink-tutorial/flink_poc/compose/jars/flink-sql-connector-kafka-1.17.0.jar;"
                                                                             "file:///Users/symphonyai/Documents/work/flink-tutorial/flink_poc/compose/jars/flink-shaded-force-shading-16.1.jar")
# Set the necessary classpath for the Kafka connector and parallelism to 1
env.set_parallelism(1)

# Initialize the StreamTableEnvironment
t_env = StreamTableEnvironment.create(env)

# Define the Kafka consumer properties
consumer_properties = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'fraud_detection_group'
}

# Define the Kafka producer properties
producer_properties = {
    'bootstrap.servers': 'localhost:9092'
}

# Define the input Kafka topic
input_topic = 'input_topic'

# Define the output Kafka topic
output_topic = 'output_topic'

# Create a FlinkKafkaConsumer for reading input events from Kafka
consumer = FlinkKafkaConsumer(
    input_topic,
    SimpleStringSchema(),
    consumer_properties
)

# Add the consumer as a source to the execution environment
input_stream = env.add_source(consumer)


# Define the fraud detection logic
def fraud_detection(event):
    # Perform fraud detection logic here
    # For example, check if the transaction amount exceeds a threshold
    if float(event['amount']) > 1000.0:
        return 'Suspicious Transaction: ' + event['transaction_id']
    else:
        return ''


# Register the fraud detection function as a table function
t_env.register_function('fraud_detection', fraud_detection)

# Convert the input stream to a table
input_table = t_env.from_data_stream(input_stream)

# Apply the fraud detection function to the input table
result_table = input_table \
    .select("fraud_detection(*) as fraud_result") \
    .filter("fraud_result != ''")

# Convert the result table to a data stream
result_stream = t_env.to_append_stream(result_table, Types.STRING())

# Create a FlinkKafkaProducer for writing the output events to Kafka
producer = FlinkKafkaProducer(
    output_topic,
    SimpleStringSchema(),
    producer_properties
)

# Add the producer as a sink to the result stream
result_stream.add_sink(producer)

# Execute the job
env.execute("Fraud Detection Job")
