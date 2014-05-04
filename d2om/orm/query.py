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
"""Queries definition.
"""

__all__ = [
    'RawQuery',
    'InsertQuery',
    'UpdateQuery',
    'DeleteQuery',
    'SelectQuery'
]

from d2om.orm.field import Field, Expression, ExpressionSet, Ordering
from d2om.orm.queryresult import QueryResult
from d2om.exception import NoDataException, QueryException
from d2om.config.model import ExprConnector
# from d2om.config import DEBUG_MODE


class BaseQuery(object):

    """Base class for Query classes."""

    def __init__(self, model):
        """
        Initialization.

        @param model: Model of requested object.
        @type model: type
        """
        self._model = model
        self._db = model._meta.database

    def sql(self):
        """Get SQL statement and parameters values."""
        raise NotImplementedError

    def execute(self):
        """Execute SQL statement and return cursor (object with records)."""
        raise NotImplementedError

    def clone(self):
        """Clone instance (create a copy of instance)."""
        raise NotImplementedError


class RawQuery(BaseQuery):

    """Class to manage/execute raw SQL statements."""

    def __init__(self, model):
        """
        Initialization.

        @param model: Model class.
        @type model: type
        """
        super(RawQuery, self).__init__(model)
        self._statement = None
        self._data = []

    def __iter__(self):
        """
        Execute SQL statement and return an iterator object.

        @return: New iterator object.
        @rtype: QueryResult
        """
        return iter(self.execute())

    def statement(self, statement):
        """
        Set up SQL statement.

        @param statement: SQL statement.
        @type statement: str
        @return: Self instance.
        @rtype: RawQuery
        """
        self._statement = statement
        return self

    def data(self, *args):
        """
        Set up parameters for SQL statement.

        @param args: List of argumenets.
        @type args: list
        @return: Self instance.
        @rtype: RawQuery
        """
        self._data = list(args)
        return self

    def sql(self):
        """
        Get SQL statement and parameters values.

        @return: SQL statement and corresponding data.
        @rtype: tuple(str, list)
        """
        return self._statement, self._data

    def execute(self):
        """
        Execute SQL statement and return cursor (object with records).

        @return: QueryResult object.
        @rtype: QueryResult
        """
        return QueryResult(self._model, self._db.execute_write(*self.sql()))

    def clone(self):
        """
        Clone instance (create a copy of instance).

        @return: New RawQuery object.
        @rtype: RawQuery
        """
        instance = RawQuery(self._model)
        return instance.statement(self._statement).data(*self._data)


class InsertQuery(BaseQuery):

    """Class to manage/execute insert SQL statements."""

    def __init__(self, model):
        """
        Initialization.

        @param model: Model class.
        @type model: type
        """
        super(InsertQuery, self).__init__(model)
        self._set_defaults()
        self._bulk_insert = False

    def _set_defaults(self):
        """Set up default values."""
        self._data = self._model._meta.get_defaults()

    def _get_insert_clause(self):
        """
        Get insert clause.

        @return: Strings of columns and values, and corresponding arguments.
        @rtype: tuple(str, str, dict/list)
        """
        statement_columns, statement_values = [], []

        db_type = self._db.get_name()
        if db_type.startswith('oracle'):
            data = {}
            def _updated(statement_values, data):
                value_abbr = '%s%s' % (self._db.interpolation[1:], i)
                statement_values.append(':%s' % value_abbr)
                data[value_abbr] = field.db_value(value)
                return statement_values, data
        else:
            data = []
            def _updated(statement_values, data):
                statement_values.append(self._db.interpolation)
                data.append(field.db_value(value))
                return statement_values, data

            # - other than mysql database -
            if not db_type.startswith('mysql'):
                print ('[WARN] InsertQuery._get_insert_clause: ' +
                       'check/set correct handler for data processing')

        for i, (name, value) in enumerate(sorted(self._data.items())):

            field = self._model.get_field(name)
            if not field:
                continue

            if value is not None or field._nullable:
                statement_columns.append(field.column_name)
                statement_values, data = _updated(statement_values, data)

        comma = self._db.op_connectors.get(ExprConnector.Comma)
        return comma.join(statement_columns), comma.join(statement_values), data

    def _get_bulk_insert_clause(self):
        """
        Get insert clause for bulk inserts.

        @return: Strings of columns and values.
        @rtype: tuple(str, str)
        """
        statement_columns, statement_values = [], []

        for name in self._data.get('names', []):

            field = self._model.get_field(name)
            if not field:
                raise QueryException('"%s" is not defined in model' % name)

            statement_columns.append(field.column_name)
            statement_values.append(self._db.interpolation)

        comma = self._db.op_connectors.get(ExprConnector.Comma)
        return comma.join(statement_columns), comma.join(statement_values)

    def bulk(self, is_bulk=True):
        """
        Flag for a type of insert: bulk inserts or not.

        @param is_bulk: Flag value.
        @type is_bulk: bool
        @return: Self instance.
        @rtype: InsertQuery
        """
        self._bulk_insert = is_bulk
        return self

    def set(self, *args, **kwargs):
        """
        Set data for insert SQL statement.

        @param args: Values for names and params (bulk insert).
        @type args: list(list, list)
        @param kwargs: Insert data (in case of bulk insert: "names" & "params").
        @type kwargs: dict

        @keyword names: Names of inserted columns.
        @keyword params: List of lists with parameters that correspond to names.

        @return: Self instance.
        @rtype: InsertQuery
        """
        if self._bulk_insert:
            if len(args) == 2:
                self._data = {
                    'names': args[0],
                    'params': args[1]}
            elif kwargs.get('names') and kwargs.get('params'):
                self._data = {
                    'names': kwargs['names'],
                    'params': kwargs['params']}
            else:
                raise ValueError('[InsertQuery.set] ' +
                                 'Not enough arguments for bulk insert')
        else:
            if 'names' in self._data and 'params' in self._data:
                self._set_defaults()
            self._data.update(kwargs)
        return self

    def sql(self):
        """
        Get SQL statement and parameters values.

        @return: SQL statement and corresponding data.
        @rtype: tuple(str, list)
        """
        if self._bulk_insert:
            statement_columns, statement_values = self._get_bulk_insert_clause()
            statement = self._db.statements.get(**{
                'name': 'insert',
                'table': self._model._meta.table,
                'columns': statement_columns,
                'values': statement_values})
            data = self._data.get('params')
        else:
            statement_columns, statement_values, data = self._get_insert_clause()
            statement = self._db.statements.get(**{
                'name': 'insert_with_lastid',
                'table': self._model._meta.table,
                'columns': statement_columns,
                'values': statement_values,
                'column': self._model.get_pk_column_name(),
                'returnvar': ':insert_id'})
        return statement, data

    def execute(self):
        """
        Execute SQL statement.

        @return: Last insert id.
        @rtype: int
        """
        if self._bulk_insert:
            cursor = self._db.execute_write_many(*self.sql())
        else:
            cursor = self._db.execute_write(*self.sql())
        output = self._db.last_insert_id(cursor)
        cursor.close()
        return output

    def clone(self):
        """
        Clone instance (create a copy of instance).

        @return: New InsertQuery object.
        @rtype: InsertQuery
        """
        instance = InsertQuery(self._model)
        return instance.bulk(self._bulk_insert).set(**self._data)


class SearchQuery(BaseQuery):

    """Base class for Query classes where filter can be applied."""

    def __init__(self, model):
        """
        Initialization.

        @param model: Model class.
        @type model: type
        """
        super(SearchQuery, self).__init__(model)
        self._filter = ExpressionSet(ExprConnector.AND)

    def _has_aliases(self):
        """
        Check that aliases are defined (or not).

        @return: Flag that aliases are defined.
        @rtype: bool
        """
        return bool(getattr(self, '_aliases', None))

    def _get_combined_column(self, field):
        """
        Get column name with table alias.

        @param field: Field object.
        @type field: Field
        @return: (Combined) column name.
        @rtype: str
        """
        if self._has_aliases():
            model_alias = self._get_alias(field.model)
            if model_alias:
                return self._db.statements.get(**{
                    'name': 'combined_column',
                    'alias': model_alias,
                    'column': field.column_name})
        return field.column_name

    def _get_active_model(self):
        """
        Get model that is active (in case there are several models).

        @return: Model class.
        @rtype: type
        """
        if self._has_aliases():
            return self._active_model
        return self._model

    def _parse_expression_set(self, expression_set):
        """
        Parse ExpressionSet object (used in where-clause).

        @param expression_set: ExpressionSet object.
        @type expression_set: ExpressionSet
        @return: Where-clause with corresponding parameters.
        @rtype: tuple(str, list)
        """
        statement_items, statement_data = [], []
        for child in expression_set.children:

            if isinstance(child, ExpressionSet):
                statement, data = self._parse_expression_set(child)

            elif isinstance(child, Expression):

                if isinstance(child.value, SelectQuery):
                    selectquery = child.value.clone()
                    if not selectquery._fields:
                        selectquery.fields(selectquery._model.get_pk_name())
                    statement, data = selectquery.sql()
                elif isinstance(child.value, (tuple, list)):
                    statement = ', '.join(
                        [self._db.interpolation] * len(child.value))
                    data = map(lambda x: child.field.db_value(x), child.value)
                elif child.value is not None:
                    statement = self._db.interpolation
                    data = [child.field.db_value(child.value)]
                else:
                    statement, data = '', []

                # - check aliases if they are used -
                column_name = self._get_combined_column(child.field)
                statement = self._db.operations.get(**{
                    'name': child.op,
                    'column': column_name,
                    'value': statement})
                if child.negated:
                    statement = self._db.statements.get(**{
                        'name': 'negated_combine',
                        'statement': statement})
                data = self._db.lookup_cast(child.field._column, child.op, data)

            statement_items.append(statement)
            statement_data.extend(data)

        connector = self._db.op_connectors.get(expression_set.connector)
        statement = connector.join(statement_items)
        if statement:
            statement = self._db.statements.get(**{
                'name': 'combine',
                'statement': statement})
        if statement and expression_set.negated:
            statement = self._db.statements.get(**{
                'name': 'negated',
                'statement': statement})
        return statement, statement_data

    def _get_where_clause(self):
        """
        Get where-clause with corresponding parameters.

        @return: Where-clause with corresponding parameters.
        @rtype: tuple(str, list)
        """
        return self._parse_expression_set(self._filter)

    def filter(self, *args, **kwargs):
        """
        Set condition for query execution.

        @param args: List of Expressions or ExpressionSets.
        @type args: list
        @param kwargs: Set of field names with corresponding values.
        @type kwargs: dict
        @return: Self instance.
        @rtype: SearchQuery
        """
        if args:
            self._filter &= ExpressionSet(ExprConnector.AND, *args)
        if kwargs:
            items = Expression.convert(self._get_active_model(), **kwargs)
            self._filter &= ExpressionSet(ExprConnector.AND, *items)
        return self


class UpdateQuery(SearchQuery):

    """Class to manage/execute update SQL statements."""

    def __init__(self, model):
        """
        Initialization.

        @param model: Model class.
        @type model: type
        """
        super(UpdateQuery, self).__init__(model)
        self._data = {}

    def _get_set_clause(self):
        """
        Get set-clause with corresponding parameters.

        @return: Set-clause with corresponding parameters.
        @rtype: tuple(str, list)
        """
        statement_items, data = [], []
        for field, value in sorted(self._data.items()):

            if not isinstance(field, Field):
                field = self._model.get_field(field)
                if not field:
                    continue
            elif field.model != self._model:
                continue

            statement = self._db.interpolation
            if value is None and not field._nullable:
                raise ValueError('[UpdateQuery._get_set_clause] ' +
                                 'field "%s" is not nullable' % field.name)

            statement_items.append(self._db.operations.get(**{
                'name': 'eq',
                'column': field.column_name,
                'value': statement}))
            data.append(field.db_value(value))

        comma = self._db.op_connectors.get(ExprConnector.Comma)
        return comma.join(statement_items), data

    def set(self, **kwargs):
        """
        Set parameters that will be updated at database.

        @param kwargs: Dictionary of model attributes and corresponding values.
        @type kwargs: dict
        @return: Self instance.
        @rtype: UpdateQuery
        """
        self._data.update(kwargs)
        return self

    def sql(self):
        """
        Get SQL statement and parameters values.

        @return: SQL statement and corresponding data.
        @rtype: tuple(str, list)
        """
        set_statement, set_data = self._get_set_clause()
        where_statement, where_data = self._get_where_clause()
        statement = self._db.statements.get(**{
            'name': 'update',
            'table': self._model._meta.table,
            'set': set_statement,
            'where': where_statement})
        return statement, set_data + where_data

    def execute(self):
        """
        Execute SQL statement at database.

        @return: Number of affected database rows.
        @rtype: int
        """
        cursor = self._db.execute_write(*self.sql())
        output = self._db.rows_affected(cursor)
        cursor.close()
        return output

    def clone(self):
        """
        Clone instance (create a copy of instance).

        @return: New UpdateQuery object.
        @rtype: UpdateQuery
        """
        instance = UpdateQuery(self._model).set(**self._data)
        instance._filter = self._filter.clone()
        return instance


class DeleteQuery(SearchQuery):

    """Class to manage/execute delete SQL statements."""

    def sql(self):
        """
        Get SQL statement and parameters values.

        @return: SQL statement and corresponding data.
        @rtype: tuple(str, list)
        """
        where_clause, where_data = self._get_where_clause()
        statement = self._db.statements.get(**{
            'name': 'delete',
            'table': self._model._meta.table,
            'where': where_clause})
        return statement, where_data

    def execute(self):
        """
        Execute SQL statement at database.

        @return: Number of affected database rows.
        @rtype: int
        """
        cursor = self._db.execute_write(*self.sql())
        output = self._db.rows_affected(cursor)
        cursor.close()
        return output

    def clone(self):
        """
        Clone instance (create a copy of instance).

        @return: New DeleteQuery object.
        @rtype: DeleteQuery
        """
        instance = DeleteQuery(self._model)
        instance._filter = self._filter.clone()
        return instance


class SelectQuery(SearchQuery):

    """Class to manage/execute select SQL statements."""

    def __init__(self, model):
        """
        Initialization.

        @param model: Model class.
        @type model: type
        """
        super(SelectQuery, self).__init__(model)
        self._active_model = self._model

        self._distinct = False
        self._order_by = []
        self._group_by = []
        self._having = []
        self._limit = None
        self._offset = None

        self._fields = set()
        self._joins = {}
        self._aliases = {}
        self._naive = False

    @classmethod
    def _generate_alias(cls, alias_map, alias=None, counter=None):
        """
        Generate alias.

        @param alias_map: Dictionary of aliases.
        @type alias_map: dict
        @param alias: Alias name.
        @type alias: str/None
        @param counter: Counter for generated aliases.
        @type counter: int/None
        @return: Alias name.
        @rtype: str
        """
        counter = (counter or 0) if not alias else -1
        if not alias:
            alias = 't%s' % (len(alias_map) + counter + 1)
        if alias in alias_map.values():
            return cls._generate_alias(alias_map, counter=(counter + 1))
        return alias

    def _get_alias(self, model):
        """
        Get table alias if aliases are defined.

        @param model: Model of the requested object.
        @type model: type
        @return: Model (table) alias.
        @rtype: str/None
        """
        return self._aliases.get(model._meta.name)

    def _set_alias(self, model, alias=None):
        """
        Set alias for Model (table).

        @param model: Model of the requested object.
        @type model: type
        @param alias: Alias for the model.
        @type alias: str/None
        """
        if not self._aliases.get(self._model._meta.name):
            _alias = self._generate_alias(self._aliases)
            self._aliases[self._model._meta.name] = _alias
        _alias = self._generate_alias(self._aliases, alias=alias)
        self._aliases[model._meta.name] = _alias

    def _get_select_clause(self):
        """
        Get select-clause with corresponding parameters.

        @return: SQL select clause with a list of parameters.
        @rtype: tuple(str, list)
        """
        statement_items, data = [], []
        if not self._fields:
            self.fields(self._model)

        for item in self._fields:

            if isinstance(item, Field):
                statement = self._get_combined_column(item)
                if item._alias:
                    statement = self._db.statements.get(**{
                        'name': 'column_with_alias',
                        'column': statement,
                        'alias': item._alias})
                statement_items.append(statement)

        comma = self._db.op_connectors.get(ExprConnector.Comma)
        return comma.join(statement_items), data

    def _get_join_clause(self):
        """
        Get join-clause.

        @return: SQL join clause.
        @rtype: str
        """
        statement_items = []
        for model in self._joins:
            for l_field, r_field, join_type in self._joins[model]:
                statement_items.append(
                    self._db.statements.get(**{
                        'name': 'join_clause',
                        'join_type': join_type,
                        'table': self._db.statements.get(**{
                            'name': 'table_with_alias',
                            'table': model._meta.table,
                            'alias': self._get_alias(model)}),
                        'columns': self._db.operations.get(**{
                            'name': 'eq',
                            'column': self._get_combined_column(l_field),
                            'value': self._get_combined_column(r_field)})}))
        return ' '.join(statement_items)

    def _get_order_by_clause(self):
        """
        Get order-by-clause.

        @return: SQL order-by clause.
        @rtype: str
        """
        if self._distinct:
            return ''
        comma = self._db.op_connectors.get(ExprConnector.Comma)
        return comma.join(
            map(lambda x: '%s %s' % (self._get_combined_column(x.field),
                                     x.to_string()), self._order_by))

    def _get_group_by_clause(self):
        """
        Get group-by-clause.

        @return: SQL group-by clause.
        @rtype: str
        """
        comma = self._db.op_connectors.get(ExprConnector.Comma)
        return comma.join(
            map(lambda x: self._get_combined_column(x), self._group_by))

    def fields(self, *args, **kwargs):
        """
        Create a set of requested fields.

        @param args: Parameters values (column/field names, Fields, Models).
        @type args: list
        @param kwargs: Parameters values (Models with field names).
        @type kwargs: dict
        @return: Self instance.
        @rtype: SelectQuery
        """
        if not self._distinct and (args or kwargs):
            self._fields.add(self._model.get_pk_field())

        for a in args:
            if isinstance(a, basestring):
                field = self._model.get_field(a)
                if field:
                    self._fields.add(field)
            elif isinstance(a, Field):
                self._fields.add(a)
                if not self._distinct:
                    self._fields.add(a.model.get_pk_field())
            elif hasattr(a, 'get_fields'):
                self._fields.update(a.get_fields())

        for k in kwargs:
            if hasattr(k, 'get_field') and isinstance(kwargs[k], (tuple, list)):
                # self._fields.add(k.get_pk_field())
                for name in kwargs[k]:
                    if isinstance(name, basestring):
                        field = k.get_field(name)
                        if field:
                            self._fields.add(field)
        return self

    def join(self, model, join_type=None, on=None, alias=None):
        """
        Join related model (join types: Inner/LeftOuter/RightOuter/FullOuter).

        @param model: Model class.
        @type model: type
        @param join_type: Join type (d2om.database._base.JoinType).
        @type join_type: str
        @param on: Name of related field.
        @type on: str
        @param alias: Joined model alias.
        @type alias: str/None
        @return: Self instance.
        @rtype: SelectQuery
        """
        if join_type and join_type.upper() not in self._db.join_type.values():
            raise ValueError('[SelectQuery.join] Unknown value of join type')
        join_type = (join_type or self._db.join_type.Inner).upper()

        lhs = self._active_model.get_related_field(model, on)
        if lhs:
            rhs = model.get_pk_field()
        else:
            rhs = self._active_model.get_reverse_related_field(model, on)
            if rhs:
                lhs = self._active_model.get_pk_field()
            else:
                raise AttributeError('[SelectQuery.join] ' +
                                     'No relation found between models: ' +
                                     '%s ' % self._active_model._meta.name +
                                     '%s' % model._meta.name)
        self._joins.setdefault(model, []).append((lhs, rhs, join_type))
        self._set_alias(model, alias)
        return self

    def to(self, model):
        """
        Change active model to defined one.

        @param model: Model class.
        @type model: type
        @return: Self instance.
        @rtype: SelectQuery
        """
        if model in self._joins or model == self._model:
            self._active_model = model
            return self
        raise AttributeError('[SelectQuery.to] Model must be joined first')

    def distinct(self, value=True):
        """
        Set parameter DISTINCT for SQL statement.

        @param value: Flag to set defined parameter.
        @type value: bool
        @return: Self instance.
        @rtype: SelectQuery
        """
        self._distinct = value
        pk_field = self._model.get_pk_field()
        if pk_field in self._fields:
            self._fields.remove(pk_field)
        return self

    def limit(self, num_rows):
        """
        Set limit for SQL statement.

        @param num_rows: Limit for requested records.
        @type num_rows: int
        @return: Self instance.
        @rtype: SelectQuery
        """
        self._limit = num_rows
        return self

    def offset(self, num_rows):
        """
        Set offset for SQL statement.

        @param num_rows: Offset for requested records.
        @type num_rows: int
        @return: Self instance.
        @rtype: SelectQuery
        """
        self._offset = num_rows
        return self

    def page(self, offset, limit):
        """
        Select SQL statement with pagging.

        @param offset: Offset for requested records.
        @type offset: int
        @param limit: Limit for requested records.
        @type limit: int
        @return: Self instance.
        @rtype: SelectQuery
        """
        self._offset = offset
        self._limit = limit
        return self

    def naive(self, naive=True):
        """
        Flag for resulted object: either one object or object with relations.

        @param naive: Flag to return one object with all requested attributes.
        @type naive: bool
        @return: Self instance.
        @rtype: SelectQuery
        """
        self._naive = naive
        return self

    def sort(self, *args, **kwargs):
        """
        Set parameters for order-by-clause.

        @param args: Parameters values (column/field names, Fields, Orderings).
        @type args: list
        @param kwargs: Additional parameters.
        @type kwargs: dict

        @keyword _force: Force to reset the order for sorting.

        @return: Self instance.
        @rtype: SelectQuery
        """
        if kwargs.get('_force', False):
            self._order_by[:] = []
        for a in args:
            if isinstance(a, basestring):
                field = self._active_model.get_field(a)
                if not field:
                    continue
                a = Ordering(field)
            elif isinstance(a, Field):
                a = Ordering(a)
            elif not isinstance(a, Ordering):
                continue
            self._order_by.append(a)
        return self

    def group(self, *args):
        """
        Set parameters for group-by-clause.

        @param args: Parameters values (column/field names, Fields, Models).
        @type args: list
        @return: Self instance.
        @rtype: SelectQuery
        """
        for a in args:
            if isinstance(a, basestring):
                items = [self._active_model.get_field(a)]
            elif isinstance(a, Field):
                items = [a]
            elif hasattr(a, 'get_fields'):
                items = a.get_fields()
            else:
                continue
            self._group_by.extend(items)
        return self

    def having(self, *args):
        """Set having-clause for SQL statement."""
        raise NotImplementedError

    def sql(self):
        """
        Get SQL statement and parameters values.

        @return: SQL statement and corresponding data.
        @rtype: tuple(str, list)
        """
        select_statement, select_data = self._get_select_clause()
        where_statement, where_data = self._get_where_clause()

        if self._has_aliases():
            table = self._db.statements.get(**{
                'name': 'table_with_alias',
                'table': self._model._meta.table,
                'alias': self._get_alias(self._model)})
        else:
            table = self._model._meta.table

        statement = self._db.statements.get(**{
            'name': 'select',
            'distinct': self._distinct,
            'columns': select_statement,
            'table': table,
            'join': self._get_join_clause(),
            'where': where_statement,
            'group_by': self._get_group_by_clause(),
            'having': None,
            'order_by': self._get_order_by_clause()})

        if self._limit and not self._offset:
            statement = self._db.statements.get(**{
                'name': 'select_with_limit',
                'selectquery': statement,
                'limit': self._limit})

        elif not self._limit and self._offset:
            statement = self._db.statements.get(**{
                'name': 'select_with_offset',
                'selectquery': statement,
                'offset': self._offset})

        elif self._limit and self._offset:
            statement = self._db.statements.get(**{
                'name': 'select_with_pagination',
                'selectquery': statement,
                'limit': self._limit,
                'offset': self._offset})

        if not self._naive:
            self._naive = bool(not self._has_aliases())
        return statement, select_data + where_data

    def count(self):
        """
        Get number of rows at database that fulfill criteria.

        @return: Number of rows.
        @rtype: int
        """
        statement, data = self.sql()

        statement = self._db.statements.get(**{
            'name': 'select_with_count',
            'selectquery': statement})

        cursor = self._db.execute_read(statement, data)
        output = (cursor.fetchone() or (0,))[0]
        cursor.close()
        return output

    def exists(self):
        """
        Check that requested data exists at database (sql query has result).

        @return: Flag that requested data exists at database.
        @rtype: bool
        """
        return bool(self.count())

    def one(self):
        """
        Execute SQL statement and return one Model object.

        @return: Model object.
        @rtype: Model
        @raise NoDataException: no data found.
        """
        queryresult = self.execute()
        try:
            instance = queryresult.next()
        except StopIteration:
            raise NoDataException('no data with defined conditions')
        else:
            queryresult.close_cursor()
            return instance

    def first(self):
        """
        Execute SQL statement and return first Model object.

        @return: Model object.
        @rtype: Model
        """
        return self.limit(1).one()

    def all(self, **kwargs):
        """
        Execute SQL statement and return cursor (object with records).

        @param kwargs: Database parameters (optional).
        @type kwargs: dict
        @return: QueryResult object.
        @rtype: QueryResult
        """
        return self.execute(**kwargs)

    def execute(self, ss=False):
        """
        Execute SQL statement and return cursor (object with records).

        @param ss: Save execution result on server side (optional for MySQL).
        @type ss: bool
        @return: QueryResult object.
        @rtype: QueryResult
        """
        return QueryResult(**{
            'model': self._model,
            'cursor': self._db.execute_read(*self.sql(), ss=ss),
            'naive': self._naive,
            'fields': self._fields})

    def clone(self):
        """
        Clone instance (create a copy of instance).

        @return: New SelectQuery object.
        @rtype: SelectQuery
        """
        instance = SelectQuery(self._model)
        instance._fields = set(self._fields)
        instance._active_model = self._active_model

        instance._distinct = self._distinct
        instance._order_by = list(self._order_by)
        instance._group_by = list(self._group_by)
        instance._having = list(self._having)
        instance._limit = self._limit
        instance._offset = self._offset

        instance._filter = self._filter.clone()
        instance._joins = self._joins.copy()
        instance._aliases = self._aliases.copy()
        instance._naive = self._naive
        return instance

    def __iter__(self):
        """
        Execute SQL statement and return an iterator object.

        @return: New iterator object.
        @rtype: QueryResult
        """
        return iter(self.execute())
