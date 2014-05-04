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
"""Model fields definition.
"""

__all__ = [
    'Field',
    'ForeignKeyField',
    'Expression',
    'ExpressionSet',
    'Ordering'
]

from d2om.exception import NoDataException
from d2om.config.model import OpCode, ExprConnector
#from d2om.config import DEBUG_MODE

OP_SEPARATOR = '__'


class FieldDescriptor(object):

    """FieldDescriptor class to store field's value (data container)."""

    def __init__(self, field):
        """
        Initialization.

        @param field: Field object.
        @param type: Field
        """
        self.field = field

    def __get__(self, instance, owner=None):
        """
        Get the attribute of the owner class or of an instance of that class.

        @param instance: Model object.
        @type instance: Model
        @param owner: Type of the Model object.
        @type owner: type
        @return: Field or its corresponding value.
        @rtype: Field/Field._column._py_type
        """
        if not instance:
            return self.field
        return instance._data.get(self.field.name)

    def __set__(self, instance, value):
        """
        Set the attribute of an instance of the owner class to a new value.

        @param instance: Model object.
        @type instance: Model
        @param value: Field's value.
        @type value: Field._column._py_type
        """
        instance._data[self.field.name] = self.field.py_value(value)


class RelatedObjectDescriptor(object):

    """RelatedObjectDescriptor class to store relations with other Models."""

    def __init__(self, field):
        """
        Initialization.

        @param field: Field object.
        @param type: Field
        """
        self.field = field

    def __get__(self, instance, owner=None):
        """
        Get the attribute of the owner class or of an instance of that class.

        @param instance: Model object.
        @type instance: Model
        @param owner: Type of the Model object.
        @type owner: type
        @return: Own Field or related object (Model object).
        @rtype: Field/Model
        @raise NoDataException: Exception if related object is not found.
        """
        if not instance:
            return self.field

        if not instance._data.get(self.field.related_name):
            related_object_id = instance._data.get(self.field.name)
            if related_object_id:
                try:
                    related_instance = self.field.related_field.model.get(
                        **{self.field.related_field.name: related_object_id})
                except NoDataException:
                    if not self.field.nullable:
                        raise
                else:
                    instance._data[self.field.related_name] = related_instance

        return instance._data.get(self.field.related_name)

    def __set__(self, instance, value):
        """
        Set the attribute on an instance of the owner class to a new value.

        @param instance: Model object.
        @type instance: Model
        @param value: Related object.
        @type value: Model/None
        """
        if value is None and self.field.nullable:
            instance._data.update({
                self.field.name: None,
                self.field.related_name: None})
        elif isinstance(value, self.field.related_field.model):
            instance._data.update({
                self.field.name: getattr(value, self.field.related_field.name),
                self.field.related_name: value})


class ReverseRelatedObjectDescriptor(object):

    """ReverseRelatedObjectDescriptor class to store reverse relations."""

    def __init__(self, field):
        """
        Initialization.

        @param field: Field object.
        @param type: Field
        """
        self.field = field

    def __get__(self, instance, owner=None):
        """
        Get the attribute of the owner class or of an instance of that class.

        @param instance: Model object.
        @type instance: Model
        @param owner: Type of the Model object.
        @type owner: type
        @return: Set of objects that are related with current one.
        @rtype: QueryResult
        @raise AttributeError: Exception if instance is not defined.
        """
        if not instance:
            raise AttributeError('[ReverseRelatedObjectDescriptor.__get__] '+
                                 'Accessible only via instances of the class')
        return self.field.model.select().filter(**{
            self.field.name: getattr(instance, self.field.related_field.name)
            }).all()


def set_expression(op):
    def inner(self, rhs):
        """Set up Expression object for the Field object."""
        if op not in OpCode.values:
            raise ValueError('Incorrect operation value for Expression')
        return Expression(self, op, rhs)
    return inner


def set_expressionset(connector):
    def inner(self, rhs):
        """Set up ExpressionSet object."""
        return ExpressionSet(connector, self, rhs)
    return inner


class Field(object):

    """Field class to describe Model attribute."""

    _field_counter = 0
    _order = 0

    def __init__(self, column_name, column_type=None, **kwargs):
        """
        Initialization.

        @param column_name: Name of the corresponding column.
        @type column_name: str
        @param column_type: Database column type.
        @type column_type: ColumnType
        @param kwargs: Column parameters (attributes).
        @type kwargs: dict
        """
        self.column_name = column_name.lower()

        self._column = None
        if isinstance(column_type, type):
            self._column = column_type(**kwargs)

        self._primary = False
        self._nullable = False
        self._default = None
        self._alias = None

        Field._field_counter += 1
        self._order = Field._field_counter

    def _add_to_class(self, model, attr_name):
        """
        Assign a Field instance to class Model with corrsponding name.

        @param model: Model class.
        @type model: type
        @param attr_name: Attribute name that corresponds to Field instance.
        @type attr_name: str
        """
        self.model = model
        self.name = attr_name
        setattr(self.model, self.name, FieldDescriptor(self))

    def primary(self, primary=True):
        """
        Set up flag "primary".

        @param primary: Flag for "primary key" field.
        @type primary: bool
        @return: Self instance.
        @rtype: Field
        """
        if self._nullable and primary:
            raise ValueError('[Field.primary] Primary key cannot be nullable')
        self._primary = bool(primary)
        return self

    def nullable(self, nullable=True):
        """
        Set up flag "nullable".

        @param nullable: Flag shows that field can have NULL value.
        @type nullable: bool
        @return: Self instance.
        @rtype: Field
        """
        if nullable and self._primary:
            raise ValueError('[Field.nullable] Primary key cannot be nullable')
        self._nullable = bool(nullable)
        return self

    def default(self, value):
        """
        Set up default value.

        @param value: Default value.
        @type value: any
        @return: Self instance.
        @rtype: Field
        """
        self._default = value
        return self

    def alias(self, alias):
        """
        Set up field alias.

        @param alias: Field alias.
        @type alias: str
        @return: Self instance.
        @rtype: Field
        """
        self._alias = alias
        return self

    __eq__ = set_expression(OpCode.EQ)
    __ne__ = set_expression(OpCode.NE)
    __lt__ = set_expression(OpCode.LT)
    __le__ = set_expression(OpCode.LE)
    __gt__ = set_expression(OpCode.GT)
    __ge__ = set_expression(OpCode.GE)
    __lshift__ = set_expression(OpCode.IN)
    __rshift__ = set_expression(OpCode.ISNULL)
    __mul__ = set_expression(OpCode.CONTAINS)
    __pow__ = set_expression(OpCode.ICONTAINS)
    __xor__ = set_expression(OpCode.ISTARTSWITH)

    def db_value(self, value):
        """
        Get correct value to store to database.

        @param value: Input value
        @type value: any
        @return: Converted value.
        @rtype: self._column.db_value(value)
        @raise ValueError: Column type is not defined.
        """
        if not self._column:
            raise ValueError('[Field.db_value] Column object is not defined')
        return self._column.db_value(value)

    def py_value(self, value):
        """
        Get correct value to operate in application.

        @param value: Input value
        @type value: any
        @return: Converted value.
        @rtype: self._column.py_value(value)
        @raise ValueError: Column type is not defined.
        """
        if not self._column:
            raise ValueError('[Field.py_value] Column object is not defined')
        return self._column.py_value(value)

    def asc(self):
        """
        Get instance of Ordering class (ASC).

        @return: Instance of Ordering class.
        @rtype: Ordering
        """
        return Ordering(self, True)

    def desc(self):
        """
        Get instance of Ordering class (DESC).

        @return: Instance of Ordering class.
        @rtype: Ordering
        """
        return Ordering(self, False)


class ForeignKeyField(Field):

    """ForeignKeyField class to describe Model FK attribute."""

    def __init__(self, column_name, related_field):
        """
        Initialization.

        @param column_name: Name of the corresponding column.
        @type column_name: str
        @param related_field: Corresponding field from connected Model.
        @type related_field: Field
        """
        super(ForeignKeyField, self).__init__(column_name)
        self._column = related_field._column

        self.related_field = related_field
        self.related_name = None
        self.reverse_related_name = None

    def _add_to_class(self, model, attr_name):
        """
        Assign a Field instance to class Model with corrsponding name.

        @param model: Model class.
        @type model: type
        @param attr_name: Attribute name that corresponds to Field object.
        @type attr_name: str
        """
        super(ForeignKeyField, self)._add_to_class(model, attr_name)

        if not self.related_name:
            self.related_name = self.related_field.model._meta.name.lower()
        if not self.reverse_related_name:
            self.reverse_related_name = '%s_set' % self.model._meta.name.lower()

        self.model._meta.relations.update({
            self.name: self.related_field.model})
        self.related_field.model._meta.reverse_relations.update({
            self.reverse_related_name: self.model})

        setattr(self.model, self.related_name, RelatedObjectDescriptor(self))
        setattr(self.related_field.model, self.reverse_related_name,
                ReverseRelatedObjectDescriptor(self))

    def with_related_name(self, name):
        """
        Set related name.

        @param name: Name that corresponds to related field.
        @type name: str
        @return: Self instance.
        @rtype: ForeignKeyField
        """
        self.related_name = name
        return self

    def with_reverse_related_name(self, name):
        """
        Set reverse related name.

        @param name: Related name for connected Model.
        @type name: str
        @return: Self instance.
        @rtype: ForeignKeyField
        """
        self.reverse_related_name = name
        return self


class Ordering(object):

    """Ordering class to represent direction of sorting."""

    def __init__(self, field, asc=True):
        """
        Initialization.

        @param field: Field object.
        @type field: Field
        @param asc: Flag to define direction of sorting (asc-True, desc-False).
        @type asc: bool
        """
        self.field = field
        self.asc = asc

    def to_string(self):
        """
        Get corresponding string.

        @return: SQL string.
        @rtype: str
        """
        if hasattr(self.field, 'model'):
            if self.asc:
                return self.field.model._meta.database.order_type.Asc
            return self.field.model._meta.database.order_type.Desc
        return ''


class BaseExpression(object):

    """BaseExpression class for SQL conditions representation."""

    def __init__(self, negated=False):
        """
        Initialization.

        @param negated: Negation for expression or expression set.
        @type negated: bool
        """
        self.negated = negated
        self._models_set = set()

    def __invert__(self):
        """
        Set attribute "negated" to opposite (equal to "~obj").

        @return: Self instance.
        @rtype: BaseExpression
        """
        self.negated = not self.negated
        return self

    def __and__(self, rhs):
        """
        The bitwise AND of self-instance and other BaseExpression.

        @param rhs: Any of BaseExpression objects.
        @type rhs: BaseExpression/Expression/ExpressionSet
        @return: ExpressionSet object (sub-class of BaseExpression).
        @rtype: ExpressionSet
        """
        return ExpressionSet(ExprConnector.AND, self, rhs)

    def __or__(self, rhs):
        """
        The bitwise OR of self-instance and other BaseExpression.

        @param rhs: Any of BaseExpression objects.
        @type rhs: BaseExpression/Expression/ExpressionSet
        @return: ExpressionSet object (sub-class of BaseExpression).
        @rtype: ExpressionSet
        """
        return ExpressionSet(ExprConnector.OR, self, rhs)

    def get_models(self):
        """
        Get utilized models.

        @return: Set of models.
        @rtype: set
        """
        return self._models_set

    def set_models(self, *args):
        """
        Add new models.

        @param args: List of models.
        @type args: list
        """
        self._models_set.update(args)

    def clone(self):
        """
        Clone instance (create a copy of instance).

        @raise NotImplementedError: Will be overridden.
        """
        raise NotImplementedError


class Expression(BaseExpression):

    """Expression class represent SQL condition for defined field."""

    _op_separator = OP_SEPARATOR

    def __init__(self, field, op, value, negated=False):
        """
        Initialization.

        @param field: Field object.
        @type field: Field
        @param op: Condition operation.
        @type op: str
        @param value: Condition value
        @type value: any
        @param negated: Negation for expression or expression set.
        @type negated: bool
        """
        super(Expression, self).__init__(negated=negated)
        self.field = field
        self.op = op
        self.value = value
        self.set_models(field.model)

    @classmethod
    def convert(cls, _model, **kwargs):
        """
        Convert key-value pairs into list of Expression objects.

        @param _model: Model class or object.
        @type _model: type/Model
        @param kwargs: Key-value pairs (dictionary of conditions).
        @type kwargs: dict
        @return: List of Expression objects.
        @rtype: list
        """
        expressions = []
        for name, value in kwargs.iteritems():

            items = name.rsplit(cls._op_separator, 1)
            if items[1:] and items[1] in OpCode.values:
                name, op = items
            else:
                op = OpCode.EQ

            field = _model.get_field(name)
            if not field:
                continue

            if op in [OpCode.ISNULL, OpCode.ISNOTNULL]:
                if not value and op == OpCode.ISNULL:
                    op = OpCode.ISNOTNULL
                elif not value:
                    op = OpCode.ISNULL
                value = None
            elif op in [OpCode.IS, OpCode.EQ] and value is None:
                op = OpCode.ISNULL
            elif op == OpCode.IS and value is not None:
                raise ValueError('[Expression.convert] ' +
                                 '__is lookups only accept None')

            expressions.append(cls(field, op, value))
        return expressions

    def clone(self):
        """
        Clone instance (create a copy of instance).

        @return: New object of Expression class.
        @rtype: Expression
        """
        return Expression(self.field, self.op, self.value, self.negated)


class ExpressionSet(BaseExpression):

    """ExpressionSet class represents a set of Expression instances."""

    def __init__(self, connector, *args):
        """
        Initialization.
        
        @param connector: ExprConnector value (and/or).
        @type connector:str
        @param args: Objects of BaseExpression/Expression/ExpressionSet.
        @type args: list
        """
        super(ExpressionSet, self).__init__()
        self.connector = connector
        self.children = []
        for a in args:
            if (isinstance(a, ExpressionSet)
                and self.connector == a.connector and not a.negated):
                self.children.extend(a.children)
            elif isinstance(a, BaseExpression):
                self.children.append(a)
            self.set_models(*a.get_models())

    def clone(self):
        """
        Clone instance (create a copy of instance).
        
        @return: New object of ExpressionSet class.
        @rtype: ExpressionSet
        """
        instance = ExpressionSet(self.connector)
        for child in self.children:
            instance.children.append(child.clone())
        instance.negated = self.negated
        return instance
