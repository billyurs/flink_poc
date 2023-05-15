from confluent_kafka import Producer
from faker import Faker
import json
import time
import logging
import random

# Instantiate a Faker object to generate fake data
fake = Faker()

# Set up logging configuration
logging.basicConfig(format='%(asctime)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='producer.log',
                    filemode='w')

# Create a logger object
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Create a Kafka producer object with specified Kafka broker details
p = Producer({'bootstrap.servers': 'localhost:9092'})

# Print message to indicate that Kafka Producer has been initiated
print('Kafka Producer has been initiated...')


class Measurement:
    """
    A class to represent a single channel measurement with its attributes
    """

    def __init__(self, channel_id=None, channel_value=None, timestamp=None):
        """
        Initialize a new instance of Measurement class with given attributes
        """
        self.channel_id = channel_id
        self.channel_value = channel_value
        self.timestamp = timestamp

    def to_dict(self):
        """
        Convert the Measurement object to a dictionary
        """
        return self.__dict__


def receipt(err, msg):
    """
    A callback function to handle message production result
    """
    if err is not None:
        print('Error: {}'.format(err))
    else:
        message = 'Produced message on topic {} with value of {}\n'.format(msg.topic(), msg.value().decode('utf-8'))
        logger.info(message)
        print(message)


def main():
    """
    The main function to generate fake channel measurements and produce them to Kafka broker
    """
    topic_name = 'channel.measurements'
    while(True):
        for i in range(1, 11):
            # Generate random channel value and timestamp
            channel_value = fake.random_int(min=10, max=2000)
            timestamp = int((time.time() + 0.5) * 1000)

            # Create a new Measurement object with generated values
            channel_measurement = Measurement(channel_id=i, channel_value=channel_value,
                                              timestamp=timestamp).to_dict()

            # Serialize the Measurement object to a JSON string
            message = json.dumps(channel_measurement)

            # Poll Kafka producer for events
            p.poll(1)

            # Produce the message to Kafka broker with receipt callback function
            p.produce(topic_name, message.encode('utf-8'), callback=receipt)

            # Flush any remaining messages in the producer buffer
            p.flush()

            # Sleep for a while before generating next set of measurements
            time.sleep(3)

        # Sleep for a longer time before generating measurements for next cycle
        time.sleep(15)


if __name__ == '__main__':
    main()
