# -*- coding: utf-8 -*-
from datetime import datetime
import logging
import time

from sqlalchemy import create_engine
from sqlalchemy.engine import reflection
from snowflake.sqlalchemy import URL

from dbferret.helpers import incremental_marker, elapsed_time


CONNECTION_MAP = {"redshift": "postgresql",
                  "postgres": "postgresql"}

logging = logging.getLogger(__name__)


class DbFerret(object):

    def __init__(self,
                 hostname,
                 user,
                 pw,
                 db,
                 ssl_mode,
                 engine_type,
                 schema,
                 port,
                 warehouse,
                 schema_list):
        """
        Connect to the db and use reflection to gather db metadata

        :param hostname: string server address for db
        :param user: string username for database
        :param pw: string password for user on database
        :param port: string port number for database
        :param db: string name of database to connect to
        :param ssl_mode: boolean to turn on or off SSL mode
        :param engine_type: the SQLAlchemy identifier used to identify the
                            database type. Use redshift for redshift and
                            postgresql for postgres.
        :param schema: string name of schema to query, primarily for snowflake
        :param warehouse: string name of warehouse if using snowflake
        :param schema_list: Comma delimited list of schemas to retrieve
        :return: db ferret object
        """

        self.timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        self.hostname = hostname
        self.user = user
        self.pw = pw
        self.db = db
        self.ssl_mode = ssl_mode
        self.engine_type = engine_type
        self.schema = schema
        self.port = port
        self.warehouse = warehouse
        self.schema_list = schema_list
        self.conn_string = None
        self.table_metadata = {}
        self.view_ddl = {}

        if self.engine_type in CONNECTION_MAP:
            self.conn_type = CONNECTION_MAP[self.engine_type]
        else:
            self.conn_type = self.engine_type.lower()

        if self.conn_type == "snowflake":
            self.engine = create_engine(URL(
                account=self.hostname, user=self.user, password=self.pw,
                database=self.db, schema=self.schema, warehouse=self.warehouse,
                client_encoding="utf-8", timezone="America/Los_Angeles",))
        else:
            # Connect to the db and use reflection to gather db metadata
            self.conn_string = \
                "{conn_type}://{user}:{pw}@{hostname}:{port}/{db}".format(
                    conn_type=self.conn_type, user=self.user, pw=self.pw,
                    hostname=self.hostname, port=self.port, db=self.db)

            if ssl_mode:
                self.engine = create_engine(
                    self.conn_string, connect_args={"sslmode": "require"})
            else:
                self.engine = create_engine(self.conn_string)
        self.inspector = reflection.Inspector.from_engine(self.engine)

    def extract_table_metadata(self):
        """
        Retrieve table metadata from the database

        :return: dictionary containing table metadata
        """
        if self.schema_list:
            table_metadata = dict.fromkeys(
                self.schema_list.replace(" ", "").split(","), {})
        else:
            table_metadata = dict.fromkeys(
                self.inspector.get_schema_names(), {})
        total_table_count = 0
        total_column_count = 0
        total_time_start = time.time()

        # Log this up front so user knows what they are getting into
        logging.info("EXTRACTING TABLE METADATA")
        logging.info("Total schema count: {schema_count}".format(
            schema_count=len(table_metadata)))

        # Treat dictionary as list so we can get index and then mark every
        # 5th item in logging as a creature comfort
        for s, schema in enumerate(table_metadata):
            table_metadata[schema] = {
                tab: [] for tab in list(self.inspector.get_table_names(
                    schema=schema))}
            table_count = len(table_metadata[schema])
            logging.info("\t {star} {schema}".format(
                star=incremental_marker(s), schema=schema.upper()))
            logging.info("\t\t\t   table count: {table_count}".format(
                table_count=table_count))
            total_table_count += table_count

            # Track how long schema extract takes
            schema_time_start = time.time()

            # Load column data for each table
            for table in table_metadata[schema]:
                table_column_list = self.inspector.get_columns(
                    table_name=table, schema=schema)
                for column in table_column_list:
                    # Just grab basic metadata
                    table_metadata[schema][table].append(
                        {k: column.get(k, None)
                            for k in ("name", "type", "nullable", "default")})
                total_column_count += len(table_metadata[schema][table])

            # For user friendliness
            logging.debug("\t\t\texecution time: {}".format(
                elapsed_time(time.time() - schema_time_start)))
        total_time_end = time.time()
        logging.info("  Total time taken: {}".format(
            elapsed_time(total_time_end - total_time_start)))
        logging.info(" Total table count: {table_count}".format(
            table_count=total_table_count))
        logging.info("Total column count: {column_count}".format(
            column_count=total_column_count))
        self.table_metadata = table_metadata
        with open("table_metadata.json", "w") as tm:
            tm.write(str(self.table_metadata))
        return self.table_metadata

    def extract_view_ddl(self):
        """
        Retrieve view create statements from the database

        :return: dictionary containing table metadata
        """
        view_ddl = {}
        total_view_count = 0
        total_time_start = time.time()

        logging.info("EXTRACTING VIEW METADATA")

        if self.schema_list:
            schemas = self.schema_list.replace(" ", "").split(",")
        else:
            schemas = self.inspector.get_schema_names()

        for s, schema in enumerate(schemas):
            schema_time_start = time.time()
            view_ddl[schema] = dict.fromkeys(
                self.inspector.get_view_names(schema=schema), {})
            view_count = len(view_ddl[schema])
            total_view_count += view_count

            logging.info("\t {star} {schema}".format(
                star=incremental_marker(s), schema=schema.upper()))
            logging.info("\t\t\t    view count: {view_count}".format(
                view_count=view_count))

            for i, view in enumerate(view_ddl[schema]):
                sql = self.inspector.get_view_definition(
                    view_name=view, schema=schema)
                view_ddl[schema][view] = \
                    u"CREATE VIEW {schema}.{view} AS {sql}\n\n".format(
                        schema=schema, view=view, sql=sql).encode("utf-8")

            logging.debug("\t\t\texecution time: {}".format(
                elapsed_time(time.time() - schema_time_start)))

        total_time_end = time.time()
        logging.info("  Total time taken: {}".format(
            elapsed_time(total_time_end - total_time_start)))
        logging.info(" Total view count: {view_count}".format(
            view_count=total_view_count))
        self.view_ddl = view_ddl
        with open("view_ddl.json", "w") as tm:
            tm.write(str(self.view_ddl))
        return self.view_ddl
