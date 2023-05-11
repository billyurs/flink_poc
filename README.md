# Flink POC app

## Development environment

Create virtual environment:


`mkvirtualenv -p /usr/local/bin/python3.8 flink-poc`

* Install dependencies: 

pip install -r requirements.txt 

You can use the provided docker-compose file to install and run Kafka, Flink, and ZooKeeper containers
Navigate to the directory compose
Run the following command
docker-compose up -d

Producer details:
The code generates fake channel measurements using the Faker library and produces them to a Kafka broker using the Confluent Kafka Python client library. The produced messages are serialized to JSON format before being sent to the broker. The code also includes a callback function to handle message production results and a logger object to record produced messages in a log file

