import pika
import json

class Queue(object):

    def __init__(self, url, exchange):
        """ Initializes a blocking connection to RabbitMQ out of a connection
            URL and an exchange name
        """
        parameters = pika.URLParameters(url)
        connection = pika.BlockingConnection(parameters)
        self.channel = connection.channel()
        self.exchange = exchange

    def notify_participation(self, experiment, alternative, client_id):
        """ Publishes a json encoded message to the configured exchange and routing
            key=participate containing the passed paramenters
        """
        self.channel.basic_publish(
               exchange=self.exchange,
               routing_key='participate',
               body=json.dumps({
                   'experiment': experiment,
                   'alternative': alternative,
                   'client_id': client_id}))

    def notify_conversion(self, experiment, kpi, client_id):
        """ Publishes a json encoded message to the configured exchange and routing
            key=convert containing the passed parameters
        """
        self.channel.basic_publish(
               exchange=self.exchange,
               routing_key='convert',
               body=json.dumps({
                   'experiment': experiment,
                   'kpi': kpi,
                   'client_id': client_id}))
