import argparse
from datetime import datetime
import os
import time

from sqlalchemy import create_engine
from sqlalchemy.engine import reflection
from snowflake.sqlalchemy import URL

from utils import elapsed_time, incremental_marker

"""
The purpose of this code is to retrieve basic catalog information 
about a database. Run it something like this:

python ferret.py --db_type redshift --user <user> --pw <pw> --hostname <host> -d <db>

"""

CWD = os.getcwd()
TIMESTAMP = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")


def parse_args():
    """
    Parse command line arguments passed to the script

    :return: args
    """

    parser = argparse.ArgumentParser(
        description="ferret collects metadata from a database via "
                    "reflection, provide connection information to run")

    parser.add_argument(
        "-t",
        "--db_type",
        dest="db_type",
        help="The database type, such as postgres or redshift used "
             "for the connection string by sqlalchemy.",
        default="postgres"
    )
    parser.add_argument(
        "-u",
        "--user",
        dest="user",
        help="The database user used to login. For postgres, at least, "
             "any user normally will be able to crawl the database."
    )
    parser.add_argument(
        "-pw",
        "--pw",
        dest="pw",
        help="Password to connect to the db with the specified user. "
             "Information is passed through to the db but not recorded."
    )
    parser.add_argument(
        "-hn",
        "--hostname",
        dest="hostname",
        help="The host where the database is located."
    )
    parser.add_argument(
        "-p",
        "--port",
        dest="port",
        help="The port used by the database for connections."
    )
    parser.add_argument(
        "-d",
        "--db",
        dest="db",
        help="The database instance of the database."
    )
    parser.add_argument(
        "-l",
        "--ssl_mode",
        dest="ssl_mode",
        help="A boolean indicating if connections must be encrypted "
             "to the database with SSL.",
        default=False
    )
    parser.add_argument(
        "-s",
        "--schema",
        dest="schema",
        help="The schema to operate against for Snowflake databases",
        default="public"
    )
    parser.add_argument(
        "-w",
        "--warehouse",
        dest="warehouse",
        help="The warehouse to operate against for Snowflake databases"
    )
    parser.add_argument(
        "--debug",
        dest="debug",
        help="Collects and diagnoses will be prepared but not run",
        action="store_true",
        default=False
    )
    parser.add_argument(
        "--log_level",
        dest="log_level",
        help="Sets the logging severity level",
        default="INFO"
    )
    args = parser.parse_args()
    return args


def get_db_inspector(db_type, user, pw, hostname, port, db,
                     ssl_mode, schema, warehouse=None):
    # Connect to the db and use reflection to gather db metadata
    if db_type == "snowflake":
        engine = create_engine(URL(
            account=hostname, user=user, password=pw, database=db,
            schema=schema, warehouse=warehouse, client_encoding="utf-8",
            timezone="America/Los_Angeles",))
    else:
        conn_string = "{db_type}://{user}:{pw}@{hostname}:{port}/{db}".format(
            db_type=db_type, user=user, pw=pw, hostname=hostname,
            port=port, db=db)

        if ssl_mode:
            engine = create_engine(
                conn_string, connect_args={"sslmode": "require"})
        else:
            engine = create_engine(conn_string)
    return reflection.Inspector.from_engine(engine)


args = parse_args()
insp = get_db_inspector(
    db_type=args.db_type, user=args.user, pw=args.pw, hostname=args.hostname,
    port=args.port, db=args.db, ssl_mode=args.ssl_mode, schema=args.schema,
    warehouse=args.warehouse)


# Column definitions for entire db
file_name_columns = "{db_type}_{db}_columns_{timestamp}.tsv".format(
    db_type=args.db_type, db=args.db, timestamp=TIMESTAMP)
table_count = 0
column_count = 0
schemas = insp.get_schema_names()
schema_count = len(schemas)

column_time_start = time.time()
print("Outputting column metadata file for {schema_count}"
      " schemas: {file_name}".format(
          schema_count=schema_count,
          file_name=os.path.join(CWD, file_name_columns)))

# Write to a file
with open(file_name_columns, "w") as f:
    f.write('"schema"\t"table"\t"name"\t"type"\t"nullable"\t"default"\n')
    for s, schema in enumerate(schemas):
        table_list = insp.get_table_names(schema=schema)
        schema_column_count = 0
        schema_table_count = len(table_list)
        table_count += schema_table_count

        print("\t {star} {schema}".format(
            star=incremental_marker(s), schema=schema.upper()))
        print("\t\t\t   table count: {table_count}".format(
            table_count=schema_table_count))
        schema_time_start = time.time()
        for i, table in enumerate(table_list):
            table_column_list = insp.get_columns(
                table_name=table, schema=schema)
            schema_column_count += len(table_column_list)
            for column in table_column_list:
                try:
                    f.write(u'"{schema}"\t'
                            u'"{table}"\t'
                            u'"{name}"\t'
                            u'"{type}"\t'
                            u'"{nullable}"\t'
                            u'"{default}"\n'.format(
                                schema=schema, table=table,
                                name=column["name"],
                                type=column["type"],
                                nullable=column["nullable"],
                                default=column["default"]).encode("utf-8"))
                except Exception as e:
                    print("Failed on item {i}: {error}".format(
                        i=i, error=str(e)))
                    try:
                        print("Table {table}".format(table=table))
                    except:
                        pass
        schema_time_end = time.time()
        print("\t\t\t  column count: {schema_column_count}".format(
            schema_column_count=schema_column_count))
        print("\t\t\texecution time: {}".format(
            elapsed_time(schema_time_end - schema_time_start)))
        column_count += schema_column_count

column_time_end = time.time()
print("\nTotal table count: {table_count}".format(table_count=table_count))
print("Total column count: {column_count}".format(column_count=column_count))
print("Total time taken: {}".format(
    elapsed_time(column_time_end - column_time_start)))


# Write view definitions to a separate file
file_name_views = "{db_type}_{db}_views_{timestamp}.tsv".format(
    db_type=args.db_type, db=args.db, timestamp=TIMESTAMP)

view_count = 0
print("Outputting view metadata for {schema_count} schemas:"
      " {file_name}".format(
        schema_count=schema_count,
        file_name=os.path.join(CWD, file_name_views)))
view_time_start = time.time()
with open(file_name_views, "w") as f:
    for s, schema in enumerate(schemas):
        schema_time_start = time.time()
        view_list = insp.get_view_names(schema=schema)
        schema_view_count = len(view_list)
        view_count += schema_view_count

        print("\t {star} {schema}".format(
            star=incremental_marker(s), schema=schema.upper()))
        print("\t\t\t    view count: {view_count}".format(
            view_count=schema_view_count))

        for i, view in enumerate(view_list):
            sql = insp.get_view_definition(view_name=view, schema=schema)
            try:
                f.write(u"CREATE VIEW {schema}.{view} AS {sql}\n\n".format(
                    schema=schema, view=view, sql=sql).encode("utf-8"))
            except Exception as e:
                print("Failed on item {i}: {error}".format(i=i, error=str(e)))
                try:
                    print("Table {table}".format(table=table))
                except:
                    pass

        schema_time_end = time.time()
        print("\t\t\texecution time: {}".format(
            elapsed_time(schema_time_end - schema_time_start)))

view_time_end = time.time()
print("\nTotal view count: {view_count}".format(view_count=view_count))
print("Total time taken: {}".format(
    elapsed_time(view_time_end - view_time_start)))
(corinth-dev) ➜  db-ferret
