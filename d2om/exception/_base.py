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
"""Base exception definition.
"""

__all__ = ['ORMBaseException']

from string import Template
import sys


class ORMBaseException(Exception):

    """ORMBaseException class."""

    _template = Template(
        '${exception_type} in ${class_name}.${class_method}${error_message}')

    def __init__(self, value=None, level=1):
        """
        Initialization.

        @param value: Error message.
        @type value: str/None
        @param level: Level of function where exception was raised.
        @type level: int
        """
        f = sys._getframe(level)
        self.message = self._template.substitute(
            exception_type=self.__class__.__name__,
            class_name=f.f_locals['self'].__class__.__name__,
            class_method=f.f_code.co_name,
            error_message=value and ': %s' % value or '')

    def __str__(self):
        return repr(self.message)
