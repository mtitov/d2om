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
"""Query exception(s) definition.
"""

__all__ = ['QueryException']

from d2om.exception._base import ORMBaseException


class QueryException(ORMBaseException):
    """QueryException exception class."""
    pass
