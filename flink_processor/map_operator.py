from pyflink.datastream import StreamExecutionEnvironment

env = StreamExecutionEnvironment.get_execution_environment()

str_stream = env.from_collection(collection=['hello apache flink', 'streaming compute'])
map_stream = str_stream.map(lambda x: x.split(' '))
# Print the transformed elements
map_stream.print()


# Execute the job
env.execute("Example")