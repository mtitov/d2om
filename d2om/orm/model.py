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
"""Model definition.
"""

__all__ = ['Model']

from d2om.orm.field import Field
from d2om.orm.query import (
    RawQuery, SelectQuery, InsertQuery, UpdateQuery, DeleteQuery)
from d2om.exception import NoDataException
from d2om.config import DEBUG_MODE


class ModelOptions(object):

    """ModelOptions class represents a structure to store Model metadata."""

    def __init__(self, **kwargs):
        """
        Initialization.

        @param kwargs: Instance parameters.
        @type kwargs: dict

        @keyword model_name: Name of the Model.
        @keyword database: Database object.
        @keyword table: Corresponding database table/collection.
        @keyword ordering: Initial ordering.
        @keyword auto_increment: Flag for autoincrement of PK value.
        """
        self.column_field_mapping = {} # {<columnName>: <fieldName>}
        self.relations = {}
        self.reverse_relations = {}
        self.defaults = {}

        self.pk_name = None
        self.pk_column_name = None

        self.name = kwargs.pop('model_name', None)
        self.database = kwargs.pop('database', None)
        self.table = kwargs.pop('table', None)
        self.ordering = kwargs.pop('ordering', None)
        self.auto_increment = kwargs.pop('auto_increment', False)

        # - configurable options -
        for attr, value in kwargs.iteritems():
            setattr(self, attr, value)

    def _post_init(self):
        """Post init actions."""
        pass

    def get_field_name(self, column_name):
        """
        Get field name.

        @param column_name: Name of the corresponding column.
        @type column_name: str
        @return: Field name.
        @rtype: str
        """
        return self.column_field_mapping.get(column_name, '')

    def get_field_names(self):
        """
        Get fields names.

        @return: List of fields names.
        @rtype: list
        """
        return self.column_field_mapping.values()

    def get_column_names(self):
        """
        Get columns names.

        @return: List of columns names.
        @rtype: list
        """
        return self.column_field_mapping.keys()

    def set_default_value(self, field):
        """
        Set default value for defined field.

        @param field: Model field.
        @type field: Field
        """
        if field._default is not None:
            if callable(field._default):
                default = field._default()
            else:
                default = field._default
            self.defaults[field.name] = default

    def get_defaults(self):
        """
        Get default values.

        @return: Set of default values.
        @rtype: dict
        """
        return self.defaults.copy()


class MetaModel(type):

    """Model meta class."""

    _inheritable_options = ['database', 'auto_increment', 'ordering']

    def __new__(cls, name, bases, attrs):
        if not bases:
            return super(MetaModel, cls).__new__(cls, name, bases, attrs)

        # - prepare meta attributes -
        meta_attrs = {'model_name': name}
        if 'Meta' in attrs:
            meta_attrs.update(attrs.pop('Meta').__dict__)
        for b in bases:
            base_meta_attrs = getattr(b, '_meta', None)
            if not base_meta_attrs:
                continue
            for k, v in base_meta_attrs.__dict__.iteritems():
                if k in cls._inheritable_options and k not in meta_attrs:
                    meta_attrs[k] = v

        model = super(MetaModel, cls).__new__(cls, name, bases, attrs)
        model._meta = ModelOptions(**meta_attrs)
        model._data = {}

        for attr_name, field in filter(lambda (k, v): isinstance(v, Field),
                                       model.__dict__.iteritems()):
            field._add_to_class(model, attr_name)
            model._meta.column_field_mapping[field.column_name] = field.name
            model._meta.set_default_value(field)
            if field._primary and not model._meta.pk_name:
                model._meta.pk_name = field.name
                model._meta.pk_column_name = field.column_name
        model._meta._post_init()

        return model


class Model(object):

    """Base model class for system objects."""

    __metaclass__ = MetaModel

    def __init__(self, **kwargs):
        """
        Initialization.

        @param kwargs: Model object attribute values.
        @type kwargs: dict
        """
        self._is_new_record = True
        self._edited_fields = set()

        _field_values = self._meta.get_defaults()
        _field_values.update(kwargs)
        for name, value in _field_values.iteritems():
            field = self.get_field(name)
            if field:
                name = field.name
            setattr(self, name, value)

        if DEBUG_MODE:
            print ('[Model.__init__] ' +
                   'new instance is initialized: %s' % self)

    def __setattr__(self, name, value):
        """
        Set attribute (field) value at Model object.

        @param name: Field name.
        @type name: str
        @param value: Field value
        @type value: any
        """
        if not hasattr(self, name) or getattr(self, name) != value:
            object.__setattr__(self, name, value)
            if not self._is_new_record:
                if name == self.get_pk_name():
                    # - TBD -
                    pass
                elif name in self.get_field_names():
                    # - save field name after value has been changed -
                    self._edited_fields.add(name)

    def _post_init(self):
        """Prepare instance after it has been populated from database cursor."""
        self.set_new_record_state(False)

    def __eq__(self, model):
        if isinstance(model, Model):
            return (model._meta.name == self._meta.name
                    and self.get_pk() and self.get_pk() == model.get_pk())
        return False

    def __ne__(self, model):
        return not self.__eq__(model)

    @classmethod
    def close_session(cls):
        """Close database connections."""
        cls._meta.database.close_connections()

    @classmethod
    def raw(cls, statement, *args):
        """
        Get RawQuery object.

        @param statement: SQL statement.
        @type: str
        @param args: SQL statement arguments.
        @type args: list
        @return: RawQuery object.
        @rtype: RawQuery
        """
        return RawQuery(cls).statement(statement).data(*args)

    @classmethod
    def insert(cls, **kwargs):
        """
        Get InsertQuery object.

        @param kwargs: Parameters for insert SQL statement.
        @type kwargs: dict
        @return: InsertQuery object.
        @rtype: InsertQuery
        """
        return InsertQuery(cls).set(**kwargs)

    @classmethod
    def insertmany(cls, *args):
        """
        Get InsertQuery object for bulk operation.

        @param args: List of parameters.
        @type args: list
        @return: InsertQuery object.
        @rtype: InsertQuery
        """
        return InsertQuery(cls).bulk(True).set(*args)

    @classmethod
    def update(cls, **kwargs):
        """
        Get UpdateQuery object.

        @param kwargs: Parameters for update SQL statement.
        @type kwargs: dict
        @return: UpdateQuery object.
        @rtype: UpdateQuery
        """
        return UpdateQuery(cls).set(**kwargs)

    @classmethod
    def delete(cls):
        """
        Get DeleteQuery object.

        @return: DeleteQuery object.
        @rtype: DeleteQuery
        """
        return DeleteQuery(cls)

    @classmethod
    def select(cls, *args, **kwargs):
        """
        Get SelectQuery object.

        @param args: Query fields/columns
        @type args: list
        @param kwargs: Query fields/columns grouped by models.
        @type kwargs: dict
        @return: SelectQuery object.
        @rtype: SelectQuery
        """
        selectquery = SelectQuery(cls).fields(*args, **kwargs)
        if cls._meta.ordering:
            selectquery = selectquery.sort(*cls._meta.ordering)
        return selectquery

    @classmethod
    def get(cls, *args, **kwargs):
        """
        Get Model object with all attributes/fields.

        @param args: Condition parameters (Expressions/ExpressionSets).
        @type args: list
        @param kwargs: Set of condition parameters.
        @type kwargs: dict
        @return: Model object.
        @rtype: Model
        """
        return SelectQuery(cls).filter(*args, **kwargs).one()

    @classmethod
    def create(cls, **kwargs):
        """
        Create Model object and save data in database.

        @param kwargs: Model object parameters (attributes/fields).
        @type kwargs: dict
        @return: Model object.
        @rtype: Model
        """
        instance = cls(**kwargs)
        instance.save()
        return instance

    @classmethod
    def get_or_create(cls, defaults=None, **kwargs):
        """
        Get Model object or if it is not found then create one.

        @param defaults: Parameters for new object.
        @type defaults: dict/None
        @param kwargs: Search parameters and input data for new object.
        @type kwargs: dict
        @return: Model object.
        @rtype: Model
        """
        try:
            instance = cls.get(**kwargs)
        except NoDataException:
            if isinstance(defaults, dict):
                kwargs.update(defaults)
            instance = cls.create(**kwargs)
        return instance

    @classmethod
    def get_field_name(cls, column_name):
        """
        Get field name.

        @param column_name: Column name.
        @type column_name: str
        @return: Field name.
        @rtype: str
        """
        return cls._meta.get_field_name(column_name)

    @classmethod
    def get_field_names(cls):
        """
        Get list of all fields names.

        @return: List of fields names.
        @rtype: list
        """
        return cls._meta.get_field_names()

    @classmethod
    def get_column_names(cls):
        """
        Get list of all columns names.

        @return: List of columns names.
        @rtype: list
        """
        return cls._meta.get_column_names()

    @classmethod
    def get_pk_name(cls):
        """
        Get field name of primary key.

        @return: Primary key name.
        @rtype: str
        """
        return cls._meta.pk_name

    @classmethod
    def get_pk_column_name(cls):
        """
        Get column name of primary key.

        @return: Primary key column name.
        @rtype: str
        """
        return cls._meta.pk_column_name

    @classmethod
    def get_pk_field(cls):
        """
        Get field of primary key.

        @return: Primary key field.
        @rtype: Field
        """
        return cls.get_field(cls.get_pk_name())

    @classmethod
    def get_field(cls, name):
        """
        Get field object that has corresponding field or column name.

        @param name: Field or column name.
        @type name: str
        @return: Field object.
        @rtype: Field/None
        """
        if name not in cls.get_field_names():
            # - check field name first, next: column name -
            name = cls.get_field_name(name)
        return getattr(cls, name, None)

    @classmethod
    def get_fields(cls):
        """
        Get list of all fields.

        @return: List of field objects.
        @rtype: list
        """
        return map(lambda x: getattr(cls, x), cls.get_field_names())

    @classmethod
    def get_sorted_fields(cls):
        """
        Get list of fields in order they defined at model definition.

        @return: Sorted list of field objects.
        @rtype: list
        """
        return sorted(
            cls.get_fields(), key=lambda x: (x._primary and 1 or 2, x._order))

    @classmethod
    def get_related_field(cls, model, name=None):
        """
        Get field that is related to model's field (FK).

        @param model: Model class.
        @type model: type
        @param name: Name of the related field.
        @type name: str/None
        @return: Field object.
        @rtype: Field
        """
        for field_name, rel_model in cls._meta.relations.iteritems():
            if rel_model == model and name in [None, field_name]:
                return cls.get_field(field_name)

    @classmethod
    def get_reverse_related_field(cls, model, name=None):
        """
        Get field from model that is related to current model.

        @param model: Model class.
        @type model: type
        @param name: Name of the reverse related field.
        @type name: str/None
        @return: Field object.
        @rtype: Field
        """
        if model in cls._meta.reverse_relations.values():
            return model.get_related_field(model=cls, name=name)

    @classmethod
    def relation_exists(cls, model):
        """
        Check does relation with model exist.

        @param model: Model class.
        @type model: type
        @return: Flag that relation exists or not.
        @rtype: bool
        """
        return bool(cls.get_related_field(model)
                    or cls.get_reverse_related_field(model))

    def get_pk(self):
        """
        Get value of the primary key.

        @return: Primary key value.
        @rtype: int/str/None
        """
        return getattr(self, self.get_pk_name(), None)

    def set_pk(self, value):
        """
        Set up the primary key.

        @param value: Primary key value.
        @type value: Field._column._py_type
        """
        setattr(self, self.get_pk_name(), self.get_pk_field().py_value(value))

    def get_field_dict(self, fields=None):
        """
        Get fields names with corresponding values as dictionary:

        @param fields: List of fields names.
        @type fields: list/None
        @return: Fields with corresponding values.
        @rtype: dict
        """
        output = {}
        for field_name in self.get_field_names():
            if not fields or field_name in fields:
                output[field_name] = getattr(self, field_name)
        return output

    def set(self, **kwargs):
        """
        Set up object attributes (fields).

        @param kwargs: Object attributes.
        @type kwargs: dict
        """
        field_names = self.get_field_names()
        for name, value in kwargs.iteritems():
            if name in field_names:
                setattr(self, name, value)

    def save(self):
        """Save object or changed object attributes at database."""
        pk = self.get_pk()
        if pk and not self._is_new_record and self._edited_fields:
            set_vars = self.get_field_dict(fields=self._edited_fields)
            self.update(**set_vars).filter(**{self.get_pk_name(): pk}).execute()
        elif self._is_new_record:
            insert_vars = self.get_field_dict()
            if self._meta.auto_increment:
                insert_vars.pop(self.get_pk_name())
            new_pk = self.insert(**insert_vars).execute()
            if self._meta.auto_increment:
                self.set_pk(new_pk)
            self.set_new_record_state(False)
        elif not pk and not self._is_new_record:
            raise ValueError('[Model.save] Primary key is not defined ' +
                             'while the data is stored')
        self._edited_fields.clear()

    def delete_instance(self):
        """Delete object from database."""
        return self.delete().filter(**{
            self.get_pk_name(): self.get_pk()}).execute()

    def remove(self):
        """Delete object from database."""
        return self.delete_instance()

    def refresh(self, *args):
        """
        Update object attributes with values from database.

        @param args: Field names that should be updated
        @type args: list
        """
        field_names = args or self.get_field_names()
        instance = self.select(field_names).filter(**{
            self.get_pk_name(): self.get_pk()}).one()
        for name in field_names:
            setattr(self, name, getattr(instance, name))

    def set_new_record_state(self, value=False):
        """
        Set object state, is it new record or not.

        @param value: Flag value that says is object new or not.
        @type value: bool
        """
        self._is_new_record = bool(value)
