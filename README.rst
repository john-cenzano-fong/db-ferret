db-ferret
=========
A tool for ferreting out metadata about databases. It currently defaults to redshift database and will work for postgresql and snowflake databases as well.


Functionality
=============
It gathers the following metadata:
- Column level metadata grouped by table and schema
- View definitions grouped by schema

It outputs the following results:
- A tab separated file containing:
  - schema
  - table
  - column name
  - data type
  - nullability
  - default values
- A sql file containing DDL create view statements


Installation
============
It is suggested you create a `virtualenv <https://docs.python-guide.org/dev/virtualenvs/>_` before installing. This tool was developed using python 2.7 and would need adjustments for python 3 that are shelved for the moment.

Simply run ``make init`` to install needed dependencies.

Run ``make test`` to run unit tests.


Execution
=========
So far db-ferret has only been tested with snowflake, postgres and redshift but theoretically other db types will work if you populate the correct `SQLAlchemy engine type <https://docs.sqlalchemy.org/en/latest/core/engines.html>_` though there could be proprietary data types in those databases that could be hard for a plain vanilla SQLAlchemy install to handle.


Typically you will run with a command like such is this for redshift:

`` python runner.py --user <user> --pw <password> --hostname <hostname> -d <database>``

For postgres specify the engine_type and perhaps you'll need ssl_mode, and let's say you don't use the default port of 5432 but instead use 5444:

    python runner.py --user <user> --pw <password> --hostname <hostname> -d <database> --engine_type postgresql -p 5444 --ssl_mode True

For snowflake you also need to reference a warehouse:

    python runner.py --user <user> --pw <password> --hostname <hostname> -d <database> -e snowflake -w <warehouse> 

The following command line options are available to go beyond basic assumptions:

-e             The SQLAlchemy engine type, such as snowflake, postgresql or redshift (default is redshift).
-u             The database user used to login. For postgres, at least, any user normally will be able to crawl the database.
-pw            Password to connect to the db with the specified user. Information is passed through to the db but not recorded.
-hn            The hostname where the database is located.
-p             The port used by the database for connections.
-d             The database instance.
-l             A boolean indicating if connections must be encrypted to the database with SSL.
--log_level    Sets the logging severity level.
--schema_list  To specify a subset of schemas to extract, values should be in comma delimited form, such as "public, staging"

