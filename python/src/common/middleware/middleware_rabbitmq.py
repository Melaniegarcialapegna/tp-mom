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

        self.consuming = False

    def send(self, message):
        try:
            # In routing key goes the name of the queue to which the message will be sent
            self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=message)

        except pika.exceptions.AMQPConnectionError as error:
            raise MessageMiddlewareDisconnectedError(str(error))
        
        except pika.exceptions.AMQPError as error:
            raise MessageMiddlewareMessageError(str(error))
        
    def start_consuming(self, on_message_callback):
        # Transform the format of pika to the format of the middleware
        def _on_message_callback_internal(channel,method,propierties,body):
            def ack():
                channel.basic_ack(delivery_tag=method.delivery_tag)
            def nack():
                channel.basic_nack(delivery_tag=method.delivery_tag)

            # Who uses the middleware just need to call ack or nack
            # "abstracting" the details of the middleware
            on_message_callback(body, ack, nack)

        try:
            self.channel.basic_consume(queue=self.queue_name, on_message_callback=_on_message_callback_internal)
            self.consuming = True

            self.channel.start_consuming()

        except pika.exceptions.AMQPConnectionError as error:
            raise MessageMiddlewareDisconnectedError(str(error))
        
        except pika.exceptions.AMQPError as error:
            raise MessageMiddlewareMessageError(str(error))

        finally:
            # Warranty that always the consuming flag is going to
            # be false when the consuming finish or in case of error
            self.consuming = False

    def stop_consuming(self):
        if not self.consuming:
            return
        
        try: 
            self.channel.stop_consuming()

        except pika.exceptions.AMQPConnectionError as error:
            raise MessageMiddlewareDisconnectedError(str(error))

    def close(self):
        try:
            self.connection.close()

        except pika.exceptions.AMQPError as error:
            raise MessageMiddlewareDisconnectedError(str(error))
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