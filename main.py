#!/usr/bin/env python
# -*- coding: windows-1256 -*-
from __future__ import print_function
import argparse
import cx_Oracle
import logging
import os
import sys
import dbHelper
from Logger import Logger
from configparser import ConfigParser

prs = argparse.ArgumentParser(description='usage')
prs.add_argument('objName', action='store', help='log directory path')
prs.add_argument('cfg_file', action='store', help='configFile.ini path')
prs.add_argument('log_dir', action='store', help='log directory path')
args = prs.parse_args()

__LOGDIR__ = os.path.abspath(args.log_dir)
__confiFileName__ = os.path.abspath(args.cfg_file)
__objName__ = args.objName

__level__ = logging.INFO

logMain = Logger(filename="main_init", level=__level__,
                 dirname="File-" + os.path.basename(__file__), rootdir=__LOGDIR__)

sql = """

SELECT
    t.column_name   "Name",
    DECODE(t.nullable, 'Y', 'NULL', 'NOT NULL') "Null?",
    DECODE(t.data_type_mod, NULL, '', t.data_type_mod || ' OF ')
    || ( CASE
        WHEN ( t.data_type_owner = upper(t.owner)
               OR t.data_type_owner IS NULL ) THEN ''
        ELSE t.data_type_owner || '.'
    END )
    || DECODE(t.data_type, 'BFILE', 'BINARY FILE LOB', upper(t.data_type))
    || CASE
        WHEN ( t.data_type = 'VARCHAR'
               OR t.data_type = 'VARCHAR2'
               OR t.data_type = 'RAW'
               OR t.data_type = 'CHAR' )
             AND ( t.data_length <> 0
                   AND nvl(t.data_length, - 1) <> - 1 ) THEN CASE
            WHEN ( t.char_used = 'C'
                   AND 'BYTE' = (
                SELECT
                    value
                FROM
                    nls_session_parameters
                WHERE
                    parameter = 'NLS_LENGTH_SEMANTICS'
            ) ) THEN '('
                     || t.char_length
                     || ' CHAR)'
            WHEN ( t.char_used = 'B'
                   AND 'CHAR' = (
                SELECT
                    value
                FROM
                    nls_session_parameters
                WHERE
                    parameter = 'NLS_LENGTH_SEMANTICS'
            ) ) THEN '('
                     || t.data_length
                     || ' BYTE)'
            WHEN ( t.char_used = 'C'
                   AND 'CHAR' = (
                SELECT
                    value
                FROM
                    nls_session_parameters
                WHERE
                    parameter = 'NLS_LENGTH_SEMANTICS'
            ) ) THEN '('
                     || t.char_length
                     || ')'
            WHEN ( t.char_used = 'B'
                   AND 'BYTE' = (
                SELECT
                    value
                FROM
                    nls_session_parameters
                WHERE
                    parameter = 'NLS_LENGTH_SEMANTICS'
            ) ) THEN '('
                     || t.data_length
                     || ')'
            ELSE '('
                 || t.data_length
                 || ' BYTE)'
        END
        WHEN ( t.data_type = 'NVARCHAR2'
               OR t.data_type = 'NCHAR' ) THEN '('
                                               || t.data_length / 2
                                               || ')'
        WHEN ( t.data_type LIKE 'TIMESTAMP%'
               OR t.data_type LIKE 'INTERVAL DAY%'
               OR t.data_type LIKE 'INTERVAL YEAR%'
               OR t.data_type = 'DATE'
               OR ( t.data_type = 'NUMBER'
                    AND ( ( t.data_precision = 0 )
                          OR nvl(t.data_precision, - 1) = - 1 )
                    AND nvl(t.data_scale, - 1) = - 1 ) ) THEN ''
        WHEN ( ( t.data_type = 'NUMBER'
                 AND nvl(t.data_precision, - 1) = - 1 )
               AND ( t.data_scale = 0 ) ) THEN '(38)'
        WHEN ( ( t.data_type = 'NUMBER'
                 AND nvl(t.data_precision, - 1) = - 1 )
               AND ( nvl(t.data_scale, - 1) != - 1 ) ) THEN '(38,'
                                                            || t.data_scale
                                                            || ')'
        WHEN ( t.data_type = 'BINARY_FLOAT'
               OR t.data_type = 'BINARY_DOUBLE' ) THEN ''
        WHEN ( t.data_precision IS NULL
               AND t.data_scale IS NULL ) THEN ''
        WHEN ( t.data_scale = 0
               OR nvl(t.data_scale, - 1) = - 1 ) THEN '('
                                                      || t.data_precision
                                                      || ')'
        WHEN ( t.data_precision != 0
               AND t.data_scale != 0 ) THEN '('
                                            || t.data_precision
                                            || ','
                                            || t.data_scale
                                            || ')'
    END "Type"
FROM
    sys.all_tab_columns t
WHERE
    upper(t.owner) = :OWNER
    AND upper(t.table_name) = :NAME
ORDER BY
    t.column_id

        """


def loadConfigFile():
    logLoadConfigFile = Logger(filename="__init__", level=__level__,
                               dirname="File-" + os.path.basename(
                                   __file__) + "-Func-" + sys._getframe().f_code.co_name, rootdir=__LOGDIR__)
    try:
        configINI = ConfigParser()
        configINI.read(__confiFileName__)

        global V_DB_USERNAME
        global V_DB_PASSWORD
        global V_DB_DSN
        global V_OBJ_PATH

        V_DB_USERNAME = configINI.get('ORACLE_CONNECTION', 'dbUsername')
        V_DB_PASSWORD = configINI.get('ORACLE_CONNECTION', 'dbPassword')
        V_DB_DSN = configINI.get('ORACLE_CONNECTION', 'dbDSN')
        V_OBJ_PATH = configINI.get('DESC_OPTION', 'descPATH')

    except:
        logLoadConfigFile.error("Unexpected error:" + str(sys.exc_info()[0]))


try:
    loadConfigFile()
    connection = dbHelper.Connection(V_DB_USERNAME, V_DB_PASSWORD, V_DB_DSN, __LOGDIR__ + '/ORA')
    cursor = connection.cursor()

    ownerInput = __objName__.split('.')[0]
    objNameInput = __objName__.split('.')[1]

    with open(V_OBJ_PATH, "w", encoding='windows-1256') as f:
        # noinspection PyStringFormat
        f.write('{0:<20} {1:>0} {2:>0}'.format("Name", "Null?", "Type") + "\n")
        # noinspection PyStringFormat
        f.write('{0:<20} {1:>0} {2:>0}'.format("---------------", "-----", "------------") + "\n")

    try:
        cursor.execute(sql, {'OWNER': ownerInput, "NAME": objNameInput})

        records = cursor.fetchall()

        for result in records:
            with open(V_OBJ_PATH, "a", encoding='windows-1256') as f:
                f.write('{0:<20} {1:>0} {2:>0}'.format(result[0], result[1], result[2]) + "\n")

    except cx_Oracle.DatabaseError as ex:
        logMain.warning("General Error Database in -> NewMaxID()")
        logMain.error("Error Massage: " + str(ex))
    except:
        logMain.error("Unexpected error:" + str(sys.exc_info()[0]))

except RuntimeError as e:

    logMain.error(sys.stderr.write("ERROR: %s\n" % e))
