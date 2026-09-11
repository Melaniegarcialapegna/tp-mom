import pika
import random
import string
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
from .middleware import (
    MessageMiddlewareQueue,
    MessageMiddlewareMessageError,
    MessageMiddlewareDisconnectedError,
)

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.queue_name = queue_name

        # Establish a blocking connection to RabbitMQ server
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))

        self.channel = self.connection.channel()

        # Declare a queue with the specified name
        self.channel.queue_declare(queue=self.queue_name)

    def send(self, message):
        try:
            # In routing key goes the name of the queue to which the message will be sent
            self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=message)

        except pika.exceptions.AMQPConnectionError as error:
            raise MessageMiddlewareDisconnectedError(str(error))
        except pika.exceptions.AMQPError as error:
            raise MessageMiddlewareMessageError(str(error))
        
        
    def start_consuming(self, on_message_callback):
        pass

    def stop_consuming(self):
        pass

    def close(self):
        pass

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        pass

    def send(self, message):
        pass
        
    def start_consuming(self, on_message_callback):
        pass

    def stop_consuming(self):
        pass

    def close(self):
        pass