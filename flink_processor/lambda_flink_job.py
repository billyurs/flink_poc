from pyflink.datastream import StreamExecutionEnvironment

env = StreamExecutionEnvironment.get_execution_environment()

data_stream = env.from_collection(collection=[1, 2, 3, 4, 5])
result_stream = data_stream.map(lambda x: 2 * x)

# Print the transformed elements
result_stream.print()

# Execute the job
env.execute("Example")