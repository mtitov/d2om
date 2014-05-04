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
"""Database basics definition.
"""

__all__ = [
    'BaseOperations',
    'BaseStatements',
    'ConnectionMeta',
    'CursorMeta',
    'Database',
    'Type'
]

from datetime import datetime, date, time
import threading

from d2om.utils import EnumTypes, IDict, Templates
from d2om.exception import DatabaseException
from d2om.config.model import OpCode, ExprConnector
from d2om.config import DEBUG_MODE


class ConnectionMeta(type):

    """Connection meta class."""

    def __new__(cls, name, bases, attrs):
        """
        Create a new instance of class.

        @param name: New class name.
        @type name: str
        @param bases: Bases classes.
        @type bases: tuple
        @param attrs: Class methods attributes.
        @type: attrs: dict
        @return: New class.
        @rtype: type
        """

        def ensure(cls):
            """
            Ensure database connection or create a new one.

            @return: Flag about connection state {'is_new': <bool>}.
            @rtype: dict
            """
            new_connection = not cls.is_active()
            if new_connection:
                with cls._connection_lock:
                    super(cls.__class__, cls).__init__(**cls._connection_params)
                    cls._active = True

            if DEBUG_MODE:
                _msg = '[Connection.ensure] '
                if new_connection:
                    _msg += 'New connection is established'
                else:
                    _msg += 'Connection is active'
                print '%s %s' % (_msg, cls)

            return {'is_new': new_connection}

        def close(cls):
            """Close database connection."""
            with cls._connection_lock:
                if cls._active:

                    try:
                        super(cls.__class__, cls).close()
                    except Exception, e:
                        if DEBUG_MODE:
                            print '[Connection.close] %s %s' % (e, cls)
                    else:
                        if DEBUG_MODE:
                            print '[Connection.close] Closed %s' % cls

                    cls._active = False

        def is_active(cls):
            """
            Check whether connection is active or not.

            @return: State of connection.
            @rtype: bool
            """
            if cls._active:
                try:
                    cls.ping()
                except Exception, e:
                    cls._active = False

                    if DEBUG_MODE:
                        print '[Connection.is_active] %s' % e

            return cls._active

        attrs.update({
            # - initial class attributes -
            '_active': False,
            '_connection_lock': threading.Lock(),
            '_connection_params': {},
            # - class methods -
            'ensure': ensure,
            'close': close,
            'is_active': is_active})

        return type.__new__(cls, name, bases, attrs)


class CursorMeta(type):

    """Cursor meta class."""

    def __new__(cls, name, bases, attrs):
        """
        Create a new instance of class.

        @param name: New class name.
        @type name: str
        @param bases: Bases classes.
        @type bases: tuple
        @param attrs: Class methods an attributes.
        @type: attrs: dict
        @return: New class.
        @rtype: type
        """

        def set_cursor_columns(cls):
            """Set up column names and corresponding indices."""
            cls._cursor_columns = IDict()

            if not getattr(cls, 'description', None):
                return

            for i in range(len(cls.description)):
                name = cls.description[i][0]
                cls._cursor_columns[name] = i

            if DEBUG_MODE and len(cls.description) > len(cls._cursor_columns):
                print ('[WARNING][Cursor.set_cursor_columns] ' +
                       'Duplicated columns are in db cursor description')

        def get_column_num(cls, name):
            """
            Get index by column name.

            @param name: Column name.
            @type name: str
            @return: Index of column.
            @rtype: int
            @raise ValueError: data is not defined for attr "_cursor_columns".
            """
            if not cls._cursor_columns:
                raise ValueError('[Cursor.get_column_num] ' +
                                 'Attribute "_cursor_columns" is not set')
            return cls._cursor_columns.get(name)

        def get_columns(cls):
            """
            Get list of all column names.

            @return: List of column names.
            @rtype: list
            @raise ValueError: data is not defined for attr "_cursor_columns".
            """
            if not cls._cursor_columns:
                raise ValueError('[Cursor.get_columns] ' +
                                 'Attribute "_cursor_columns" is not set')
            return cls._cursor_columns.keys()

        attrs.update({
            # - initial class attributes -
            '_cursor_columns': None,
            # - class methods -
            'set_cursor_columns': set_cursor_columns,
            'get_column_num': get_column_num,
            'get_columns': get_columns})

        return type.__new__(cls, name, bases, attrs)


class JoinType(EnumTypes):

    """JoinType class with possible types for join clause."""

    _types = {
        'Inner': 'INNER',
        'LeftOuter': 'LEFT OUTER',
        'RightOuter': 'RIGHT OUTER',
        'FullOuter': 'FULL OUTER'}


class OrderType(EnumTypes):

    """OrderType class with directions for ordering."""

    _types = {
        'Asc': 'ASC',
        'Desc': 'DESC'}


class BaseOpConnectors(EnumTypes):

    """OpConnector class contains values to be used in SQL statement."""

    _types = {
        ExprConnector.AND: ' AND ',
        ExprConnector.OR: ' OR ',
        ExprConnector.Comma: ', '}

    @classmethod
    def get(cls, name):
        """
        Get connector value.

        @param name: Name of the connector.
        @type name: str
        @return: Connector value for SQL operations.
        @rtype: str/None
        """
        return getattr(cls, name)


class BaseOperations(Templates):

    """BaseOperations class contains SQL operations."""

    _templates = {
        OpCode.EQ: '$column = $value',
        OpCode.NE: '$column != $value',
        OpCode.LT: '$column < $value',
        OpCode.LE: '$column <= $value',
        OpCode.GT: '$column > $value',
        OpCode.GE: '$column >= $value',
        OpCode.IN: '$column IN ($value)',
        OpCode.NIN: '$column NOT IN ($value)',
        OpCode.BETWEEN: '$column BETWEEN $value',
        OpCode.ISNULL: '$column IS NULL',
        OpCode.ISNOTNULL: '$column IS NOT NULL',
        OpCode.CONTAINS: '$column LIKE $value',
        OpCode.STARTSWITH: '$column LIKE $value'}

    @classmethod
    def get(cls, name, **kwargs):
        """
        Get requested operation.

        @param name: Name of the operation.
        @type name: str
        @param kwargs: Required parameters for operation/
        @type kwargs: dict

        @keyword value: Value that would be used in SQL operation.

        @return: SQL operation.
        @rtype: str
        """
        if kwargs.get('value') and isinstance(kwargs['value'], (tuple, list)):
            if name == OpCode.BETWEEN and len(kwargs['value']) == 2:
                kwargs['value'] = '%s AND %s' % tuple(kwargs['value'])
            elif name in [OpCode.IN, OpCode.NIN]:
                kwargs['value'] = ', '.join(kwargs['value'])
            else:
                kwargs['value'] = kwargs['value'][0]
        return super(BaseOperations, cls).get(name, **kwargs)


class BaseStatements(Templates):

    """BaseStatements class contains SQL statements."""

    _templates = {
        'insert': 'INSERT INTO $table ($columns) VALUES ($values)',
        'update': 'UPDATE $table SET $set $where',
        'delete': 'DELETE FROM $table $where',
        'select': ('SELECT $distinct $columns FROM $table $join ' +
                   '$where $group_by $having $order_by'),
        # - simple statements -
        'distinct': 'DISTINCT',
        'where': 'WHERE $where',
        'group_by': 'GROUP BY $group_by',
        'having': 'HAVING $having',
        'order_by': 'ORDER BY $order_by',
        'table_with_alias': '$table $alias',
        'column_with_alias': '$column AS $alias',
        'combined_column': '$alias.$column',
        'func': '$funcname($column) AS $alias',
        'join_clause': '$join_type JOIN $table ON $columns',
        'negated': 'NOT $statement',
        'combine': '($statement)',
        'negated_combine': 'NOT ($statement)',
        # - re-defined templates -
        'insert_with_lastid': '',
        'select_with_limit': '',
        'select_with_offset': '',
        'select_with_pagination': '',
        'select_with_count': ''}


def commit_on_success(func):
    """
    Apply commit-method if there is no exceptions in func execution.

    @param func: Database function.
    @type func: function
    @return: Wrapped database function.
    @rtype: function
    """
    def inner(self, *args, **kwargs):
        self.begin()
        try:
            result = func(self, *args, **kwargs)
            self.commit()
        except:
            self.rollback()
            raise
        else:
            return result
    return inner


class Database(object):

    """Basic database class."""

    _connection_cls = None
    _connections = {'read': None, 'write': None}

    operations = BaseOperations
    statements = BaseStatements
    op_connectors = BaseOpConnectors
    join_type = JoinType
    order_type = OrderType

    interpolation = '%s'

    def __init__(self, **kwargs):
        """
        Initialization (mainly read/write connections initialization).

        @param kwargs: Connection parameters.
        @type kwargs: dict
        """
        self._connections = {}
        if kwargs.get('read_params'):
            kwargs.update(kwargs['read_params'])
            self._connections.update({
                'read': self._connection_cls(**kwargs)})
        if kwargs.get('write_params'):
            kwargs.update(kwargs['write_params'])
            self._connections.update({
                'write': self._connection_cls(**kwargs)})

    @classmethod
    def get_name(cls):
        """
        Get database class name.

        @return: Class name.
        @rtype: str
        """
        return cls.__name__.lower()

    def __enter__(self):
        """Enter the runtime context related to Database object."""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Exit the runtime context related to Database object."""
        self.close_connections()

    def close_connections(self):
        """Close all (read/write) connections."""
        for conn_type in self._connections:
            self._connections[conn_type].close()

    def begin(self):
        """Transaction begin (used for compatibility)."""
        pass

    def commit(self):
        """Commit modifications."""
        self._connections.get('write').commit()

    def rollback(self):
        """Cancel (rollback) modifications."""
        self._connections.get('write').rollback()

    def get_cursor(self, modify=False, **kwargs):
        """
        Ger cursor object.

        @param modify: Modification flag.
        @type modify: bool
        @param kwargs: Cursor parameters.
        @type kwargs: dict
        @return: Cursor object.
        @rtype: Cursor
        """
        if modify:
            return self._connections.get('write').cursor(**kwargs)
        return self._connections.get('read').cursor(**kwargs)

    def execute(self, statement, parameters=None, modify=False, **kwargs):
        """
        Execute SQL statement.

        @param statement: SQL query statement.
        @type statement: str
        @param parameters: Bind variables.
        @type parameters: tuple/list/dict/None
        @param modify: Modification flag.
        @type modify: bool
        @param kwargs: Cursor parameters.
        @type kwargs: dict

        @keyword arraysize: The number of rows to be fetched.

        @return: Cursor object.
        @rtype: Cursor
        @raise DatabaseException: exception in statement execution.
        """
        cursor = self.get_cursor(modify=modify, **kwargs)
        if kwargs.get('arraysize'):
            cursor.arraysize = kwargs['arraysize']

        if isinstance(parameters, dict) and 'insert_id' in parameters:
            if not parameters['insert_id'] and hasattr(cursor, 'var_number'):
                parameters['insert_id'] = cursor.var_number()

        if DEBUG_MODE:
            print '[Database.execute] %s %s' % (statement, parameters or ())

        try:
            cursor.execute(statement, parameters or ())
        except Exception, e:
            cursor.close()
            raise DatabaseException(
                ('%s ("%s" %s)' % (e, statement, parameters)).replace('\n', ''))
        return cursor

    def executemany(self, statement, parameters, **kwargs):
        """
        Execute SQL statement with multiple inputs.

        @param statement: SQL query statement.
        @type statement: str
        @param parameters: Bind variables.
        @type parameters: tuple/list/dict
        @param kwargs: Cursor parameters.
        @type kwargs: dict
        @return: Cursor object.
        @rtype: Cursor
        @raise DatabaseException: exception in statement execution.
        """
        cursor = self.get_cursor(modify=True, **kwargs)
        try:
            cursor.executemany(statement, parameters)
        except Exception, e:
            cursor.close()
            raise DatabaseException(
                ('%s ("%s" %s)' % (e, statement, parameters)).replace('\n', ''))
        return cursor

    def execute_read(self, statement, parameters=None, **kwargs):
        """
        Execute non modification SQL statement.

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
        return self.execute(statement, parameters, modify=False, **kwargs)

    @commit_on_success
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
        return self.execute(statement, parameters, modify=True, **kwargs)

    @commit_on_success
    def execute_write_many(self, statement, parameters, **kwargs):
        """
        Execute modification SQL statement with multiple inputs.

        @param statement: SQL query statement.
        @type statement: str
        @param parameters: Bind variables.
        @type parameters: tuple/list/dict
        @param kwargs: Cursor parameters.
        @type kwargs: dict
        @return: Cursor object.
        @rtype: Cursor
        @raise DatabaseException: exception in statement execution.
        """
        return self.executemany(statement, parameters, **kwargs)

    def lookup_cast(self, column, lookup, value):
        """
        Prepare query value for the certain operation.

        @param column: Column name.
        @type column: str
        @param lookup: OpCode value (abbrv for SQL operation).
        @type lookup: str
        @param value: Operation value.
        @type value: str/list
        @return Updated operation value.
        @rtype: str/list
        """
        return value

    def last_insert_id(self, *args, **kwargs):
        """
        Get id (primary key) for last insert data row.

        @param args: List of arguments.
        @type args: list
        @param kwargs: Dictionary of parameters.
        @type kwargs: dict
        @raise NotImplementedError: method must be re-defined.
        """
        raise NotImplementedError

    def rows_affected(self, *args, **kwargs):
        """
        Get number of affected rows.

        @param args: List of arguments.
        @type args: list
        @param kwargs: Dictionary of parameters.
        @type kwargs: dict
        @raise NotImplementedError: method must be re-defined.
        """
        raise NotImplementedError


class ColumnType(object):

    """Basic Type class."""

    _py_type = None
    _default_attrs = {}

    def __init__(self, **kwargs):
        """
        Initialization.

        @param kwargs: Attributes.
        @type kwargs: dict
        """
        self._attrs = dict(self._default_attrs)
        if kwargs:
            self._attrs.update(**kwargs)

    @property
    def attributes(self):
        """
        Get attributes.

        @return: Type attributes.
        @rtype: dict
        """
        return self._attrs

    @classmethod
    def convert(cls, value):
        """
        Convert value into class type.

        @param value: Input value
        @type value: any
        @return: Converted value.
        @rtype: cls._py_type
        """
        return cls._py_type(value)

    def db_value(self, value):
        """
        Get correct value to store at database.

        @param value: Input value
        @type value: any
        @return: Converted value.
        @rtype: cls._py_type/None
        """
        if value is not None and not isinstance(value, self._py_type):
            value = self.convert(value)
        return value

    def py_value(self, value):
        """
        Get correct value to operate in application.

        @param value: Input value
        @type value: any
        @return: Converted value.
        @rtype: cls._py_type/None
        """
        if value is not None and not isinstance(value, self._py_type):
            value = self.convert(value)
        return value


class IntegerType(ColumnType):

    """IntegerType class corresponds to python int type."""

    _py_type = int


class NumberType(ColumnType):

    """NumberType class corresponds to python int type."""

    _py_type = int


class FloatType(ColumnType):

    """FloatType class corresponds to python float type."""

    _py_type = float


class BooleanType(ColumnType):

    """BooleanType class corresponds to python bool type."""

    _py_type = bool

    def db_value(self, value):
        """
        Get correct value to store at database.

        @param value: Input value
        @type value: any
        @return: Converted (int) value.
        @rtype: int{0,1}/None
        """
        if value is not None:
            value = self.convert(value) and 1 or 0
        return value

    def py_value(self, value):
        """
        Get correct value to operate in application.

        @param value: Input value
        @type value: any
        @return: Boolean value.
        @rtype: bool/None
        """
        if value is not None:
            value = self.convert(value)
        return value


class CharType(ColumnType):

    """CharType class corresponds to python str type."""

    _py_type = str
    _default_attrs = {'max_length': 1000}

    def db_value(self, value):
        """
        Get correct value to store to database.

        @param value: Input value
        @type value: any
        @return: String value with restricted length.
        @rtype: str/None
        """
        if value is not None:
            value = self.convert(value)[:self.attributes['max_length']]
        return value

    def py_value(self, value):
        """
        Get correct value to operate in application.

        @param value: Input value
        @type value: any
        @return: Boolean value.
        @rtype: str
        """
        value = value or ''
        if not isinstance(value, self._py_type):
            value = self.convert(value)
        return value


class VarCharType(CharType):

    """VarCharType(CharType) class corresponds to python str type."""

    _default_attrs = {'max_length': 3000}


class DateTimeType(ColumnType):

    """DateTimeType class corresponds to python datetime.datetime type."""

    _py_type = datetime
    _default_attrs = {'formats': ['%Y-%m-%d %H:%M:%S',
                                  '%Y-%m-%d %H:%M:%S.%f',
                                  '%Y-%m-%d']}

    def _to_datetime_format(self, value):
        """
        Convert string to datetime object.

        @param value: Datetime in string format.
        @type value: str
        @return: Datetime object.
        @rtype: datetime.datetime
        """
        for f in self.attributes['formats']:
            try:
                return datetime.strptime(value, f)
            except ValueError:
                pass
        return value

    def convert(self, value):
        """
        Convert value into datetime object.

        @param value: Input value.
        @type value: str/int/None
        @return: Datetime object.
        @rtype: datetime.datetime
        """
        if isinstance(value, basestring):
            value = self._to_datetime_format(value)
        elif isinstance(value, int):
            value = datetime.utcfromtimestamp(value)
        else:
            value = super(DateTimeType, self).convert(value)
        return value


class DateType(DateTimeType):

    """DateType class corresponds to python datetime.date type."""

    _py_type = date
    _default_attrs = {'formats': ['%Y-%m-%d',
                                  '%Y-%m-%d %H:%M:%S',
                                  '%Y-%m-%d %H:%M:%S.%f']}

    def convert(self, value):
        """
        Convert value into date object.

        @param value: Input value.
        @type value: str/int/None
        @return: Date object.
        @rtype: datetime.date
        """
        return super(DateType, self).convert(value).date()


class TimeType(ColumnType):

    """TimeType class corresponds to python datetime.time type."""

    _py_type = time
    _default_attrs = {'formats': ['%H:%M:%S.%f',
                                  '%H:%M:%S',
                                  '%H:%M',
                                  '%Y-%m-%d %H:%M:%S.%f',
                                  '%Y-%m-%d %H:%M:%S']}

    def convert(self, value):
        """
        Convert value into time object.

        @param value: Input value.
        @type value: str/int/None
        @return: Time object.
        @rtype: datetime.time
        """
        return super(DateType, self).convert(value).time()


class Type(EnumTypes):

    """Type class contains database types."""

    _types = {
        'Integer': IntegerType,
        'Number': NumberType,
        'Float': FloatType,
        'Boolean': BooleanType,
        'Char': CharType,
        'Varchar': VarCharType,
        'Datetime': DateTimeType,
        'Date': DateType,
        'Time': TimeType}
