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
"""QueryResult definition (object with result after SQL statement execution).
"""

__all__ = ['QueryResult']

from d2om.exception import QueryResultException


class QueryResult(object):

    """QueryResult class (iterator over the results from Query)."""

    def __init__(self, model, cursor, naive=True, fields=None):
        """
        Initialization.

        @param model: Requested model.
        @type model: type
        @param cursor: Database cursor.
        @type cursor: cursor
        @param naive: Flag for single model or model with relations.
        @type naive: bool
        @param fields: Set of requested fields.
        @type fields: list
        """
        self._model = model
        self._cursor = cursor
        self._naive = naive
        self._fields = fields or set()

        if self._naive:
            self._cursor.set_cursor_columns()

        self._keep_cache = False
        self._cached_result = []

        self._with_statement = False

    def __enter__(self):
        """Enter the runtime context related to QueryResult object."""
        self._with_statement = True
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Exit the runtime context related to QueryResult object."""
        self.close_cursor()
        self._with_statement = False

    def close_cursor(self):
        """Close database cursor."""
        if self._cursor:
            try:
                self._cursor.close()
            except Exception:
                pass
            self._cursor = None

    def cache_on(self):
        """Turn cache on."""
        self._keep_cache = True

    def cache_off(self):
        """Turn cache off."""
        self._keep_cache = False

    def get_instance(self, row):
        """
        Construct the instance of one model (simple).

        @param row: Row of data from database.
        @type row: tuple
        @return: Instance of the requested model.
        @rtype: Model
        """
        instance = self._model(**dict(map(
            lambda x: (x, row[self._cursor.get_column_num(x)]),
            self._cursor.get_columns())))
        instance._post_init()
        return instance

    def get_instance_with_relations(self, row):
        """
        Construct the instance with relations by using metadata.

        @param row: Row of data from database.
        @type row: tuple
        @return: Instance of the requested model with joined instances.
        @rtype: Model
        """
        self._models_data = {}
        for i, item in enumerate(self._fields):

            try:
                model = item.model
                itemname = item.name
            except Exception:
                continue

            self._models_data.setdefault(model, {})
            self._models_data[model][itemname] = row[i]
                # {item._alias or item.column_name: row[i]})

        return self._set_joined_instances(self._model)

    def _set_joined_instances(self, model):
        """
        Define instances related to the requested one.

        @param model: Requested or joined model.
        @type model: type
        @return: Instance of the requested or joined model.
        @rtype: Model
        """
        instance = model(**self._models_data[model])
        instance._post_init()
        for rel_name, rel_model in model._meta.relations.iteritems():
            if rel_model not in self._models_data:
                continue
            field = model.get_field(rel_name)
            if field:
                setattr(instance, field.related_name,
                        self._set_joined_instances(rel_model))
        return instance

    def __iter__(self):
        """
        Get QueryResult or QueryResult._cached_result.

        @return: Iteratable object.
        @rtype: iterator object
        @raise QueryResultException
        """
        if self._cursor:
            return self
        else:
            if not self._cached_result:
                raise QueryResultException('requested data was not cached')
            return iter(self._cached_result)

    def next(self):
        """
        Get next instance from cursor.

        @return: Instance of the requested model.
        @rtype: Model
        """
        instance = self.iterate()
        if self._keep_cache:
            self._cached_result.append(instance)
        return instance

    def iterate(self):
        """
        Iteration to get the requested instance.

        @return: Instance of the request model.
        @rtype: Model
        """
        row = self._cursor.fetchone()
        if not row:
            if not self._with_statement:
                self.close_cursor()
            raise StopIteration
        if self._naive:
            return self.get_instance(row)
        return self.get_instance_with_relations(row)
