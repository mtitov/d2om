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
# - Alexey Anisyonkov, <anisyonk@cern.ch>, 2012
#
"""Dictionary with case-insensitive keys.
"""

__all__ = ['IDict']


class IDict(dict):

    """IDict class with case-insensitive keys."""

    def __getitem__(self, key):
        k = key.lower()
        return dict.__getitem__(self, k)

    def __setitem__(self, key, value):
        k = key.lower()
        dict.__setitem__(self, k, value)
