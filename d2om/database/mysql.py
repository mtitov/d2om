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
"""MySQL implementation.
"""

__all__ = ['MySQLDatabase']

import warnings

try:
    import MySQLdb.connections as dbconnections
    import MySQLdb.cursors as dbcursors
    import MySQLdb as mysql
except ImportError:
    print Exception('[ImportError] MySQLdb is not installed')

from d2om.database._base import (
    ConnectionMeta, CursorMeta, Database, BaseStatements)
# from d2om.exception import DatabaseException
from d2om.config.model import OpCode
from d2om.config import DEBUG_MODE

TZ_QUERY = "SET time_zone = '+00:00'"


class Connection(dbconnections.Connection):

    """Connection class to connect to MySQL database."""

    __metaclass__ = ConnectionMeta

    def __init__(self, *args, **kwargs):
        """
        Initialization.

        @param args: List of arguments (used for compatibility).
        @type args: list
        @param kwargs: Connection parameters.
        @type kwargs: dict

        @keyword host: Host name.
        @keyword port: Port number.
        @keyword db: Database name.
        @keyword user: User name.
        @keyword passwd: User password.
        """
        self._connection_params = {
            'host': kwargs.get('host'),
            'port': kwargs.get('port'),
            'db': kwargs.get('database'),
            'user': kwargs.get('user'),
            'passwd': kwargs.get('password')}

    def cursor(self, **kwargs):
        """
        Get cursor.

        @param kwargs: Cursor parameters.
        @type kwargs: dict

        @keyword ss: Get Server Side Cursor.

        @return: Cursor object.
        @rtype: Cursor
        """
        is_new = self.ensure()['is_new']
        cursor = Cursor(self) if not kwargs.get('ss') else SSCursor(self)
        if is_new:
            cursor.execute(TZ_QUERY)

            if DEBUG_MODE:
                print '[Connection.cursor] set UTC timezone for current session'

        return cursor


class Cursor(dbcursors.Cursor):

    """Cursor class corresponds to MySQLdb.cursors.Cursor."""

    __metaclass__ = CursorMeta


class SSCursor(dbcursors.SSCursor):

    """SSCursor class corresponds to MySQLdb.cursors.SSCursor."""

    __metaclass__ = CursorMeta


class Statements(BaseStatements):

    """Statements class contains SQL statements."""

    _templates = BaseStatements._templates
    _templates.update({
        'insert_with_lastid': 'INSERT INTO $table ($columns) VALUES ($values)',
        'select_with_limit': '$selectquery LIMIT $limit',
        'select_with_offset': '$selectquery LIMIT $offset, 18446744073709551615',
        'select_with_pagination': '$selectquery LIMIT $offset, $limit',
        'select_with_count': 'SELECT COUNT(*) FROM ($selectquery) as t0'})


class MySQLDatabase(Database):

    """MySQLDatabase class."""

    _connection_cls = Connection

    statements = Statements
    interpolation = '%s'

    def execute(self, statement, parameters=None, modify=False, **kwargs):
        """
        Execute SQL statement.

        @param statement: SQL query statement.
        @type statement: str
        @param parameters: Bind variables.
        @type parameters: tuple/list/dict/None
        @param modify: Modification flag (defines is commit necessary).
        @type modify: bool
        @param kwargs: Cursor parameters.
        @type kwargs: dict
        @return: Cursor object.
        @rtype: Cursor
        """
        # - warnings -> exceptions -
        warnings.filterwarnings('error', category=mysql.Warning)
        return super(MySQLDatabase, self).execute(
            statement, parameters, modify=modify, **kwargs)

    def executemany(self, statement, parameters, **kwargs):
        """
        Execute SQL statement with multiple inputs.

        @param statement: SQL query statement.
        @type statement: str
        @param parameters: Bind variables.
        @type parameters: tuple/list/dict
        @return: Cursor object.
        @rtype: Cursor
        """
        # - warnings -> exceptions -
        warnings.filterwarnings('error', category=mysql.Warning)
        return super(MySQLDatabase, self).executemany(
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
            OpCode.STARTSWITH: '%s%%'}
        if lookup in OP_PATTERNS and values:
            values[0] = OP_PATTERNS[lookup] % values[0]
        return values

    def last_insert_id(self, cursor):
        """
        Get id (primary key) for last insert data row.

        @param cursor: Cursor object.
        @type cursor: Cursor
        @return: PK value.
        @rtype: int
        """
        return cursor.lastrowid

    def rows_affected(self, cursor):
        """
        Get number of affected rows.

        @param cursor: Instance of Cursor class.
        @type cursor: Cursor
        @return: The number of affected rows.
        @rtype: int
        """
        return cursor.rowcount
