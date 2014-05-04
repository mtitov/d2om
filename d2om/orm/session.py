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
"""Session definition (used to manage objects at one session).
"""

__all__ = ['Session']


class Session(object):

    """Session class."""

    def __init__(self, *args):
        """
        Initialization.

        @param args: List of models.
        @type args: list
        """
        self._models = []
        self.addmany(*args)

    def __enter__(self):
        """Enter the runtime context related to Session object."""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Exit the runtime context related to Session object."""
        self.close()

    def add(self, model):
        """
        Add model to Session object.

        @param model: Model class.
        @type model: type
        """
        self._models.append(model)

    def addmany(self, *args):
        """
        Add several models to Session object.

        @param args: List of models.
        @type args: list
        """
        if args:
            self._models.extend(args)

    def close(self):
        """Close models' sessions (connections)."""
        for model in self._models:
            model.close_session()
