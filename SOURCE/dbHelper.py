import logging
import os
import sys
import cx_Oracle
from Logger import Logger

__level__ = logging.INFO


class Connection(cx_Oracle.Connection):
    def __init__(self, DB_USERNAME, DB_PASSWORD, DB_DSN, ORA_LOGDIR):
        logCon = Logger(filename="__init__", level=__level__,
                     dirname="File-" + os.path.basename(
                         __file__) + "-Class-" + __class__.__name__ + "-Func-" + sys._getframe().f_code.co_name,
                     rootdir=ORA_LOGDIR)

        try:
            # log.info("CONNECT to database is OK")
            return super(Connection, self).__init__(DB_USERNAME, DB_PASSWORD, DB_DSN)
        except cx_Oracle.DatabaseError as e:
            logCon.warning("CONNECT to database Not Ok")
            logCon.error("Error Massage: " + str(e))
            return None

    def cursor(self):
        return Cursor(self)


class Cursor(cx_Oracle.Cursor):
    def execFetchOne(self, statement, ORA_LOGDIR):
        logExecFetchOne = Logger(filename="__init__", level=__level__,
                     dirname="File-" + os.path.basename(
                         __file__) + "-Class-" + __class__.__name__ + "-Func-" + sys._getframe().f_code.co_name,
                     rootdir=ORA_LOGDIR)
        try:
            # log.info("Execute statement: " + statement)
            return super(Cursor, self).execute(statement).fetchone()
        except cx_Oracle.DatabaseError as e:
            logExecFetchOne.warning("Execute statement Not Ok")
            logExecFetchOne.error("Error Massage: " + str(e))
            return None

    def execArgs(self, statement, args, ORA_LOGDIR):
        logExecArgs = Logger(filename="__init__", level=__level__,
                     dirname="File-" + os.path.basename(
                         __file__) + "-Class-" + __class__.__name__ + "-Func-" + sys._getframe().f_code.co_name,
                     rootdir=ORA_LOGDIR)
        try:
            # log.info("Execute statement: " + statement)
            return super(Cursor, self).execute(statement, test=args)
        except cx_Oracle.DatabaseError as e:
            logExecArgs.warning("Execute statement Not Ok")
            logExecArgs.error("Error Massage: " + str(e))
            return None


# ToDo: add export NLS_LANG=<language>_<territory>.<character set> To Class dbHelper dynamic
if __name__ == '__main__':
    os.environ["NLS_LANG"] = "AMERICAN_AMERICA.AR8MSWIN1256"
