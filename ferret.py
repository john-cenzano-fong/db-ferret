from datetime import datetime, timedelta
import os
import time

from sqlalchemy import create_engine
from sqlalchemy.engine import reflection
import sqlalchemy.types as types


# Connection info
db_type = "postgres"
user = ""
pw = ""
host = ""
port = ""
db = ""
ssl_mode = False

def elapsed_time(seconds):
    sec = timedelta(seconds=seconds)
    d = datetime(1, 1, 1) + sec
    return "%d hr %d min %d sec" % (d.hour, d.minute, d.second)


def incremental_marker(count, increment=5):
    if (s + 1) % increment == 0:
        return "*"
    else:
        return " "

# This is in here for right now to deal with more complicated postgres type
class XMLType(types.UserDefinedType):
    def get_col_spec(self):
        return 'XML'

    def bind_processor(self, dialect):
        def process(value):
            if value is not None:
                if isinstance(value, str):
                    return value
                else:
                    return etree.tostring(value)
            else:
                return None
        return process

    def result_processor(self, dialect, coltype):
        def process(value):
            if value is not None:
                value = etree.fromstring(value)
            return value
        return process


CWD = os.getcwd()
TIMESTAMP = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

# Connect to the db and use reflection to gather db metadata
conn_string = "{db_type}://{user}:{pw}@{host}:{port}/{db}".format(
    db_type=db_type, user=user, pw=pw, host=host, port=port, db=db)
if ssl_mode:
    engine = create_engine(conn_string, connect_args={'sslmode':'require'})
else:
    engine = create_engine(conn_string)
insp = reflection.Inspector.from_engine(engine)

# Column definitions for entire db
file_name_columns = "{db_type}_{db}_columns_{timestamp}.tsv".format(
    db_type=db_type, db=db, timestamp=TIMESTAMP)
table_count = 0
column_count = 0
schemas = insp.get_schema_names()
schema_count = len(schemas)

column_time_start = time.time()
print("Outputting column metadata file for {schema_count}"
      " schemas: {file_name}".format(
          schema_count=schema_count, file_name=os.path.join(CWD, file_name_columns)))

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
                                default=column["default"]).encode('utf-8'))
                except Exception as e:
                    print("Failed on item {i}: {error}".format(i=i, error=str(e)))
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
    db_type=db_type, db=db, timestamp=TIMESTAMP)

view_count = 0
print("Outputting view metadata for {schema_count} schemas:"
      " {file_name}".format(
        schema_count=schema_count, file_name=os.path.join(CWD, file_name_views)))
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
                    schema=schema, view=view, sql=sql).encode('utf-8'))
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
