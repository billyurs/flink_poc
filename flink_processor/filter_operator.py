from pyflink.datastream import StreamExecutionEnvironment

env = StreamExecutionEnvironment.get_execution_environment()

data_stream = env.from_collection(collection=[0, 1, 2, 3, 4, 5])
filter_stream = data_stream.filter(lambda x: x != 0)

# Print the transformed elements
filter_stream.print()
env.execute("Example")