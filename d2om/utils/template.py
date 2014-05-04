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
"""Templates basic definition.
"""

__all__ = ['Templates']

import re
from string import Template


class Templates(object):

    """Templates class is basic for statements definition."""

    _templates = {}

    class _TemplateBaseMeta(type):
        
        """Meta class."""

        def __new__(cls, name, bases, attrs):
            cls = type.__new__(cls, name, bases, attrs)
            for key, value in cls._templates.iteritems():
                setattr(cls, key, Template(value))
            return cls

    __metaclass__ = _TemplateBaseMeta

    @classmethod
    def _get_template_str(cls, name, **kwargs):
        """
        Get template string.
        
        @param name: Name of the template.
        @type name: str
        @param kwargs: Parameters for template.
        @type kwargs: dict
        @return: Result string (template with corresponding values).
        @rtype: str
        """
        template = getattr(cls, name, None)
        if template:
            return re.sub(r'\s+', ' ', template.substitute(kwargs).strip())
        return ''

    @classmethod
    def get(cls, name, **kwargs):
        """
        Get string with corresponding values.
        
        @param name: Name of the template.
        @type name: str
        @param kwargs: Parameters for template.
        @type kwargs: dict
        @return: Result string (template with corresponding values).
        @rtype: str
        """
        for k in kwargs:
            if k in cls._templates:
                if isinstance(kwargs[k], bool) and kwargs[k]:
                    kwargs[k] = cls._templates[k]
                elif kwargs[k]:
                    kwargs[k] = cls._get_template_str(k, **kwargs)
                else:
                    kwargs[k] = ''
        return cls._get_template_str(name, **kwargs)
