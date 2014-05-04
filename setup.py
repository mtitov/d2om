#!/usr/bin/env python

from distutils.core import setup

import d2om

setup(
    name='D2OM',
    version=d2om.__version__,
    author=d2om.__author__,
    author_email=d2om.__contact__,
    packages=['d2om'],
    license='Apache License, Version 2.0',
    description='Lightweight ORM (Data to Object Mapper)',
    long_description=open('README').read(),
)
