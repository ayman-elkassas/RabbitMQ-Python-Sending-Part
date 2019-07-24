"""
Abstraction for the message object sent via rabbitmq
"""


class Message(object):
    """
    Abstraction for the message object sent to the notification
    client
    """


    def __init__(self, msg, url, m_type, reminder_date, reminder_msg):
        self.message = msg
        self.url = url
        self.type = m_type
        self.REMINDER_DATE = reminder_date
        self.REMINDER_MESSAGE = reminder_msg
