#
# Copyright 2014 Mikhail Titov
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Mikhail Titov, <mikhail.titov@cern.ch>, 2014
#
"""Oracle implementation.
"""

__all__ = ['OracleDatabase']

try:
    import cx_Oracle as oracle
except ImportError:
    print Exception('[ImportError] cx_Oracle is not installed')

from _base import (
    ConnectionMeta, CursorMeta, Database, BaseOperations, BaseStatements)
# from d2om.exception import DatabaseException
from d2om.config.model import OpCode
from d2om.config import DEBUG_MODE

TZ_QUERY = "ALTER SESSION SET TIME_ZONE='0:0'"


class Connection(oracle.Connection):

    """Connection class to connect to Oracle database."""

    __metaclass__ = ConnectionMeta

    def __init__(self, *args, **kwargs):
        """
        Initialization.

        @param args: List of arguments (used for compatibility).
        @type args: list
        @param kwargs: Connection parameters.
        @type kwargs: dict

        @keyword dsn: Database name (local naming parameters - tnsnames.ora).
        @keyword user: User name.
        @keyword password: User password.
        """
        self._connection_params = {
            'dsn': kwargs.get('database'),
            'user': kwargs.get('user'),
            'password': kwargs.get('password')}

    def cursor(self, **kwargs):
        """
        Get cursor.

        @param kwargs: Cursor parameters (used for compatibility).
        @type kwargs: dict
        @return Instance of Cursor class.
        @rtype: Cursor
        """
        is_new = self.ensure()['is_new']
        cursor = Cursor(self)
        if is_new:
            cursor.execute(TZ_QUERY)

            if DEBUG_MODE:
                print '[Connection.cursor] set UTC timezone for current session'

        return cursor


class Cursor(oracle.Cursor):

    """Cursor class corresponds to cx_Oracle.Cursor."""

    __metaclass__ = CursorMeta

    def var_number(self):
        """
        Create a variable associated with the cursor of the given type.

        @return: Oracle variable.
        @rtype: Cursor Variable Object.
        """
        return self.var(oracle.NUMBER)

    def get_bindvar_value(self, id_or_name):
        """
        Get the bind variable used for the last execute.

        @param id_or_name: Variable index number of variable name.
        @type id_or_name: int/str
        @return: Variable value.
        @rtype: any
        """
        return self.bindvars[id_or_name].getvalue()


class Operations(BaseOperations):

    """Operations class contains SQL operations."""

    _templates = BaseOperations._templates
    _templates.update({
        OpCode.IEQ: "REGEXP_LIKE($column, $value, 'i')",
        OpCode.ICONTAINS: "REGEXP_LIKE($column, $value, 'i')",
        OpCode.ISTARTSWITH: "REGEXP_LIKE($column, $value, 'i')"})


class Statements(BaseStatements):

    """Statements class contains SQL statements."""

    _templates = BaseStatements._templates
    _templates.update({
        'insert_with_lastid': (
            'INSERT INTO $table ($columns) VALUES ($values) ' +
            'RETURNING $column INTO $returnvar'),
        'select_with_limit': (
            'SELECT A.*, ROWNUM FROM ($selectquery) A WHERE ROWNUM <= $limit'),
        'select_with_offset': (
            'SELECT A.*, ROWNUM FROM ($selectquery) A WHERE ROWNUM >= $offset'),
        'select_with_pagination': (
            'SELECT * FROM (SELECT A.*, ROWNUM r_num FROM ($selectquery) A ' +
            'WHERE ROWNUM <= $limit) WHERE r_num >= $offset'),
        'select_with_count': 'SELECT COUNT(1) FROM ($selectquery)'})


class OracleDatabase(Database):

    """OracleDatabase class."""

    _connection_cls = Connection

    operations = Operations
    statements = Statements
    interpolation = ':a'

    def execute_write(self, statement, parameters=None, **kwargs):
        """
        Execute modification SQL statement.

        @param statement: SQL query statement.
        @type statement: str
        @param parameters: Bind variables.
        @type parameters: tuple/list/dict/None
        @param kwargs: Cursor parameters.
        @type kwargs: dict
        @return: Cursor object.
        @rtype: Cursor
        @raise DatabaseException: exception in statement execution.
        """
        if statement.lower().startswith('insert'):
            if isinstance(parameters, dict) and 'insert_id' not in parameters:
                # - check method <insert> to confirm the key -
                parameters['insert_id'] = None
        return super(OracleDatabase, self).execute_write(
            statement, parameters, **kwargs)

    def lookup_cast(self, column, lookup, values):
        """
        Prepare query value for the certain operation.

        @param column: Column name.
        @type column: str
        @param lookup: OpCode value (abbrv for SQL operation).
        @type lookup: str
        @param values: List of values.
        @type values: list/None
        @return: Updated list of values.
        @rtype: list/None
        """
        OP_PATTERNS = {
            OpCode.CONTAINS: '%%%s%%',
            OpCode.STARTSWITH: '%s%%',
            OpCode.IEQ: '^%s$',
            OpCode.ICONTAINS: '%s',
            OpCode.ISTARTSWITH: '^%s'}
        if lookup in OP_PATTERNS and values:
            values[0] = OP_PATTERNS[lookup] % values[0]
            if lookup.startswith('i'):
                for old, new in [('.', '\.'), ('*', '.*')]:
                    values[0] = values[0].replace(old, new)
        return values

    def last_insert_id(self, cursor):
        """
        Get id (primary key) for last insert data row.

        @param cursor: Cursor object.
        @type cursor: Cursor
        @return: PK value.
        @rtype: int
        """
        id_or_name = 'insert_id' if isinstance(cursor.bindvars, dict) else -1
        return cursor.bindvars[id_or_name].getvalue()

    def rows_affected(self, cursor):
        """
        Get number of affected rows.

        @param cursor: Instance of Cursor class.
        @type cursor: Cursor
        @return: Number of affected rows.
        @rtype: int
        """
        return cursor.rowcount
