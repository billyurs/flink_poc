from pyflink.datastream import StreamExecutionEnvironment

env = StreamExecutionEnvironment.get_execution_environment()

str_stream = env.from_collection(collection=['hello apache flink', 'streaming compute'])
split_stream = str_stream.flat_map(lambda x: x.split(' '))

# Print the transformed elements
split_stream.print()
env.execute("Example")