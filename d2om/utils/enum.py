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
"""Constants (object types, states, etc.) basic definition.
"""

__all__ = ['EnumTypes']


class EnumTypes(object):

    """EnumTypes class."""

    _types = {}

    class _EnumTypesMeta(type):

        """Meta class."""

        def __new__(cls, name, bases, attrs):
            cls = type.__new__(cls, name, bases, attrs)
            for key, value in cls._types.iteritems():
                setattr(cls, key, value if value else key)
            return cls

        @property
        def attributes(self):
            return sorted(self._types.keys())

        @property
        def values(self):
            return sorted(self._types.values())

        @property
        def pairs(self):
            return sorted(self._types.items())

    __metaclass__ = _EnumTypesMeta
