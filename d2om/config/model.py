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
"""Predefined constants for SQL queries.
"""

__all__ = [
    'ExprConnector',
    'OpCode'
]

from d2om.utils.enum import EnumTypes


class OpCode(EnumTypes):

    """OpCode class represents a set of abbrs for Expression operations."""

    _types = {
        'EQ': 'eq',
        'NE': 'ne',
        'LT': 'lt',
        'LE': 'lte',
        'GT': 'gt',
        'GE': 'gte',
        'IS': 'isnull',
        'ISNULL': 'isnull',
        'ISNOTNULL': 'isnotnull',
        'IN': 'in',
        'NIN': 'nin',
        'BETWEEN': 'between',
        'CONTAINS': 'contains',
        'STARTSWITH': 'startswith',
        'IEQ': 'ieq',
        'ICONTAINS': 'icontains',
        'ISTARTSWITH': 'istartswith'}


class ExprConnector(EnumTypes):

    """ExprConnector class represents a set of connectors for any operation."""

    _types = {
        'AND': 'and',
        'OR': 'or',
        'Comma': 'comma'}
