D2OM
=====

D2OM ORM (Data to Object Mapper) is an object-relational mapping library 
for the Python language, providing a framework for mapping an object-oriented 
domain model to a traditional relational databases.

It's a lightweight implementation with an absence of ability to create/manage 
a database structure. It gives possibility to manage objects based on data 
from database (MySQL, Oracle), AKA data management interface.

This project was encouraged by other ORMs such as autumn and mostly by peewee.
- autumn: http://pypi.python.org/pypi/autumn/
- peewee: https://github.com/coleifer/peewee/



Example
=====

Model definition:

    from d2om.orm import Model, Field
    from d2om.database.mysql import MySQLDatabase
    from d2om.database import Type

    connection_parameters = {
        'host': 'localhost',
        'port': 3306,
        'database': 'db',
        'read_params': {'user': 'user_r', 'password': 'xxx_read'},
        'write_params': {'user': 'user_w', 'password': 'xxx_write'}}

    class User(Model):

         userid = Field('user_id', Type.Number).primary()
         name = Field('name', Type.Varchar)
         score = Field('score', Type.FloatType).default('10.0')
         description = Field('description', Type.Varchar).nullable()
         creationdate = Field('creation_date', Type.Datetime)

        class Meta:
            database = MySQLDatabase(**connection_parameters)
            table = 'users'
            auto_increment = True
            ordering = ('userid',)
