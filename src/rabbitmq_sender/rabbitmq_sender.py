"""
Abstraction for the rmq sending logic
"""
import pika
import json
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from utils.logger import Logger


class RabbitmqSender(object):
    """
    Sends objects via rabbitmq server

    :param hostname: rabbit mq server name or ip address
    :param username: server username
    :param password: server pwd

    """

    def __init__(self, hostname, username, password):
        self.__channel = None
        self.__hostname = hostname
        self.__username = username
        self.__password = password

    def connect(self):
        """
        Connects to the given rabbitmq credentials
        """
        __credentials = pika.PlainCredentials(
                    username=self.__username,
                    password=self.__password)

        Logger.log.info("Connecting to the rabbitmq server at: {}".format(self.__hostname))
        __conn_params = pika.ConnectionParameters(host=self.__hostname,
                                                  credentials=__credentials)
        self.__channel = pika.BlockingConnection(__conn_params).channel()
        Logger.log.info("Rabbitmq connected successfully")

    def send_msg(self, queue_name, msg):
        """
        Sends the message object over the given queue name
        :param queue_name: rabbitmq queue name
        :type queue_name: str
        :param msg: message object
        :type msg: message.message.Message
        """


        #TODO:edit open new connection every sending

        self.connect()

        Logger.log.info("Sending message to queue: '{}'".format(queue_name))
        msg_body = json.dumps(msg.__dict__)
        self.__channel.queue_declare(queue=queue_name,
                                     durable=True,
                                     exclusive=False,
                                     auto_delete=False)
        self.__channel.basic_publish(exchange='',
                                     routing_key=queue_name,
                                     body=msg_body)

        Logger.log.info("Message sent to queue: '{}' successfully".format(queue_name))
