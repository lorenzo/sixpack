from kombu import Connection, Exchange
from kombu.pools import producers

class Queue(object):

    def __init__(self, url, exchange):
        """ Initializes a blocking connection to RabbitMQ out of a connection
            URL and an exchange name
        """
        self.connection = Connection(url)
        self.exchange = Exchange(exchange)

    def notify_participation(self, experiment, alternative, client_id):
        """ Publishes a json encoded message to the configured exchange and routing
            key=participate containing the passed paramenters
        """
        with producers[self.connection].acquire(block=True) as producer:
            message = {'experiment': experiment,
                       'alternative': alternative,
                       'client_id': client_id}
            producer.publish(message, exchange=self.exchange,
                                    routing_key='participate')

    def notify_conversion(self, experiment, kpi, client_id):
        """ Publishes a json encoded message to the configured exchange and routing
            key=convert containing the passed parameters
        """
        with producers[self.connection].acquire(block=True) as producer:
            message = {
                   'experiment': experiment,
                   'kpi': kpi,
                   'client_id': client_id}
            producer.publish(message, exchange=self.exchange,
                                    routing_key='convert')
