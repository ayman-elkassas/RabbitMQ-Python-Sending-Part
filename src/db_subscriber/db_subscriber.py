"""
A class for subscribing to database events
"""
import json
import cx_Oracle
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from utils.logger import Logger
from utils.utils import DBNotificationException
from rabbitmq_sender.rabbitmq_sender import RabbitmqSender
from message.message import Message


def _parse_changed_row(method_name, r, cur):
    try:
        row_id = r.rowid
        op_code = r.operation
        sql_ret = cur.callfunc(method_name, cx_Oracle.CURSOR, [row_id, op_code])
        Logger.log.info("sql_method {} "
                        "on rowid: {} "
                        "of type: {} executed successfully"
                        .format(method_name, row_id, op_code))
        return sql_ret
    except Exception as e:
        Logger.log.error("An error occurred while reading a changed table data. "
                         "Exception details are printed below")
        Logger.log.exception(e)


class DBSubscriber(object):
    """
    Subscribes to database change event and push the sql function returned value
    to rabbitmq server
    once a change happens
    :param json_config_dir: The json directory containing all the required db info
    """

    def __init__(self, json_config_dir):
        self.__json_dir = json_config_dir
        self.__json_conf = None
        self.__db = None
        self.__sender = RabbitmqSender(
            hostname="192.134.101.85",
            username="admin",
            password="admin123"
        )
        self.__subs = dict()

    def __read_json_config(self, json_file_dir):
        """
        loads the json configuration into the object
        :param json_file_dir: the directory for the json file
        """
        actual_dir = os.path.abspath(json_file_dir)
        Logger.log.info("Reading the json configuration from : '{}'".format(actual_dir))
        with open(actual_dir, 'r') as f:
            self.__json_conf = json.load(f)
        Logger.log.info("configuration loaded successfully")

    def __get_oracle_db_config(self, config_name):
        """
        Gets the oracle_db_configuration given attribute from
        the json file
        :param config_name: json key in the file
        :return: the json item value
        """
        try:
            return self.__json_conf["oracle_db_configuration"][config_name]
        except Exception as _:
            raise DBNotificationException("JSON file missing "
                                          "'oracle_db_configuration':'{}'".format(config_name))

    def __get_rabbitmq_config(self, config_name):
        """
        Gets the oracle_db_configuration given attribute from
        the json file
        :param config_name: json key in the file
        :return: the json item value
        """
        try:
            return self.__json_conf["rabbitmq_configuration"][config_name]
        except Exception as _:
            raise DBNotificationException("JSON file missing "
                                          "'rabbitmq_configuration':'{}'".format(config_name))

    def __oracle_db_connect(self):
        """
        Connects to the given oracle database credentials
        """
        ora_username = self.__get_oracle_db_config("username")
        ora_password = self.__get_oracle_db_config("password")
        ora_hostname = self.__get_oracle_db_config("hostname")
        ora_port_num = self.__get_oracle_db_config("tcp_port_num")
        ora_service_name = self.__get_oracle_db_config("service_name")

        Logger.log.info("Creating oracle Data Source Name (dsn)")
        dsn = cx_Oracle.makedsn(ora_hostname,
                                ora_port_num,
                                service_name=ora_service_name)
        Logger.log.info("dsn created successfully")
        Logger.log.info("Connecting to oracle db with the given credentials")
        self.__db = cx_Oracle.connect(user=ora_username,
                                      password=ora_password,
                                      dsn=dsn,
                                      events=True)
        Logger.log.info("Oracle db connected successfully")

    def __get_triggers_on(self):
        ret = 0
        for i in self.__get_oracle_db_config("triggers_on"):
            if i == "insert":       # 2
                ret |= cx_Oracle.OPCODE_INSERT
            elif i == "update":     # 4
                ret |= cx_Oracle.OPCODE_UPDATE
            elif i == "delete":     # 8
                ret |= cx_Oracle.OPCODE_DELETE
            else:
                raise DBNotificationException("Invalid event name")
        return ret

    def _m_name(self, t_name):
        abs_t_name = t_name
        if t_name.find('.') != -1:
            abs_t_name = t_name[t_name.rindex('.')+1:]
        methods_dict = self.__get_oracle_db_config("table_on_change_sql_function")
        return methods_dict[abs_t_name]

    def __db_change_callback(self, msg):

        start = 0
        if cx_Oracle.OPCODE_UPDATE == 4:
            start=1

        for index in range(start, len(msg.tables[0].rows)):
            cur = self.__db.cursor()
            for tab in msg.tables:
                try:
                    _m_name = self._m_name(tab.name)
                except Exception as e:
                    Logger.log.error("Table: '{}' has no sql function defined at the json file. "
                                     "Ignoring this table change. Exception details are printed below.")
                    Logger.log.exception(e)
                    continue
                rows = tab.rows
                if rows[0].operation == cx_Oracle.OPCODE_UPDATE and len(rows) > 1:
                    rows = rows[1:]
                for row in rows:
                    sql_ret = _parse_changed_row(_m_name, row, cur)
                    self.__send_sql_cur(sql_ret)




    def __subscribe_to_tables(self, tables_name):
        """
        :param tables_name: table to subscribe
        :type tables_name: list[str]
        """
        Logger.log.info("Running tables subscription ")
        triggers_on = self.__get_triggers_on()
        for t in tables_name:
            self.__subs[t] = self.__db.subscribe(
                callback=self.__db_change_callback,
                operations=triggers_on,
                qos=cx_Oracle.SUBSCR_QOS_ROWIDS)
            self.__subs[t].registerquery('select * from {}'.format(t))
            Logger.log.info("Subscribed to table: '{}' successfully".format(t))

    #TODO:COMMENT
    # def __connect_rabbitmq(self):
    #     username = self.__get_rabbitmq_config("username")
    #     hostname = self.__get_rabbitmq_config("hostname")
    #     password = self.__get_rabbitmq_config("password")
    #     self.__sender = RabbitmqSender(
    #         hostname=hostname,
    #         username=username,
    #         password=password
    #     )
    #     self.__sender.connect()

    def __send_sql_cur(self, cur):
        """
        sends the returned sql ref_cursor
        :param cur: sql function ref_cursor object result
        """
        description = None
        try:
            description = cur.description
            data = [i for i in cur]
            for row in data :

                idx = {str(i[0]): j for j, i in enumerate(cur.description)}
                msg = Message(
                    msg=row[idx["MESSAGE"]],
                    m_type=row[idx["TYPE"]],
                    url=row[idx["URL"]],
                    reminder_date=row[idx["REMINDER_DATE"]],
                    reminder_msg=row[idx["REMINDER_MESSAGE"]]
                )
                # TODO:Check nullable
                q_name = str(row[idx["NID"]])
                if (q_name != None or msg != None):
                    self.__sender.send_msg(q_name, msg)

        except Exception as _:
            Logger.log.warning("Empty sql response returned. Ignoring sending")
            return




    def subscribe(self):
        self.__read_json_config(self.__json_dir)
        self.__oracle_db_connect()
        #TODO:Comment connection
        #self.__connect_rabbitmq()

        tables = self.__get_oracle_db_config("table_on_change_sql_function")
        self.__subscribe_to_tables(tables_name=tables.keys())
