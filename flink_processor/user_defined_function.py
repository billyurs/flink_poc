from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes
from pyflink.table.udf import udf

# Define your custom Python UDF
def my_udf(x):
    return x * 2

# Create a TableEnvironment
settings = EnvironmentSettings.new_instance().in_batch_mode().build()
table_env = TableEnvironment.create(settings)

# Register the UDF with the TableEnvironment
table_env.create_temporary_function("my_udf", udf(my_udf, result_type=DataTypes.INT()))

# Create a table named 'my_table' with a single column
table_env.execute_sql("CREATE TABLE my_table (column_name INT)")

# Execute the SQL query referencing the 'my_table'
table_env.execute_sql("SELECT my_udf(column_name) FROM my_table").print()
