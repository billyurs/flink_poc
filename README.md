# APP details

This proof of concept (POC) application showcases how Kafka measurements can be processed in Flink to calculate the average, maximum, minimum, standard deviation, and total readings using a 1-minute tumbling window. The application provides an example of how Kafka and Flink can be integrated to perform real-time data processing tasks.


Development Environment
To set up your development environment:

## Create a virtual environment: 
`mkvirtualenv -p /usr/local/bin/python3.8 flink-poc`

* Install dependencies: 
  * pip install -r requirements.txt


## Running the Application
  You can use the provided docker-compose.yml file to install and run Kafka, Flink, and ZooKeeper containers. To start the containers, navigate to the compose directory and run the following command:

```docker-compose up -d```


## Producer details:
Generates fake channel measurements using the Faker library and produce them to a Kafka broker using the Confluent Kafka Python client library. The produced messages are serialized to JSON format before being sent to the broker. The application also includes a callback function to handle message production results and a logger object to record produced messages in a log file

## Running the Producer

To run the producer code, navigate to the producer directory and run the following command:

````
cd producer
python channel_producer.py
````

## Submitting the Flink Job
To submit the Flink job, edit the flink_job.py file and replace the following line with the path to the necessary JAR files which are present in compose folder:


````
table_env.get_config().get_configuration().set_string("pipeline.classpaths", "file:///Users/symphonyai/Documents/work/flink-tutorial/flink_poc/compose/jars/flink-sql-connector-kafka-1.17.0.jar;file:///Users/symphonyai/Documents/work/flink-tutorial/flink_poc/compose/jars/flink-shaded-force-shading-16.1.jar")
````

Then, run the following command:

```
python flink_job.py
```

This will submit the Flink job to the Flink cluster running in the Docker container.