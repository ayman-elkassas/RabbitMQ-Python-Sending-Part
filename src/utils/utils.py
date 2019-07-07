class DBNotificationException(Exception):
    def __init__(self, m):
        self._message = m

    def __str__(self):
        return self._message


