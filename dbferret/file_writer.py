# -*- coding: utf-8 -*-
from datetime import datetime
import os
import logging

from dbferret.helpers import create_directory


logging = logging.getLogger(__name__)


class FileWriter(object):

    def __init__(self,
                 db,
                 engine_type):
        """
        Connect to the db and use reflection to gather db metadata

        :param db: string name of database to connect to
        :param engine_type: string type of database, has been tested
                            with redshift, postgres and snowflake
        :return: db ferret object
        """

        self.timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        self.db = db
        self.engine_type = engine_type

    def get_table_tsv_default_path(self):
        """
        Generate a default file path for the table tsv file

        :return: string with path for file
        """
        return os.path.join(
            "data",
            "{engine_type}_{db}_table_column_metadata_{timestamp}.tsv".format(
                engine_type=self.engine_type, db=self.db,
                timestamp=self.timestamp))

    def get_view_ddl_sql_default_path(self):
        """
        Generate a default file path for the view ddl sql file

        :return: string with path for file
        """
        return os.path.join(
            "data",
            "{engine_type}_{db}_view_ddl_{timestamp}.sql".format(
                engine_type=self.engine_type,
                db=self.db, timestamp=self.timestamp))

    def output_table_metadata_to_tsv(self, table_metadata, path=None):
        """
        Generate a tab separated file to store table metadata

        :param path: string with the path where the file should be written
        :return: string with path to the file
        """
        if path:
            tsv_path = path
        else:
            create_directory()
            tsv_path = self.get_table_tsv_default_path()
        logging.info(
            "Outputting table column metadata file for {schema_count}"
            " schemas: {tsv_path}".format(
                schema_count=len(table_metadata),
                tsv_path=tsv_path))
        with open(tsv_path, "w") as f:
            f.write(
                '"schema"\t"table"\t"name"\t'
                '"type"\t"nullable"\t"default"\n')
            for schema in table_metadata:
                for table in table_metadata[schema]:
                    for col in table_metadata[schema][table]:
                        try:
                            f.write(u'"{schema}"\t'
                                    u'"{table}"\t'
                                    u'"{name}"\t'
                                    u'"{type}"\t'
                                    u'"{nullable}"\t'
                                    u'"{default}"\n'.format(
                                        schema=schema,
                                        table=table,
                                        name=col["name"],
                                        type=col["type"],
                                        nullable=col["nullable"],
                                        default=col["default"]).encode("utf-8"))
                        # There may be some obscure values that are hard to logging.info
                        except Exception as e:
                            logging.info(
                                "Failed: {error}".format(error=str(e)))
                            try:
                                logging.info(
                                    "Table {table}".format(table=table))
                            except:
                                pass
        return tsv_path

    def output_view_ddl_to_sql(self, view_ddl, path=None):
        """
        Generate a tab separated file to store table metadata

        :param path: string with the path where the file should be written
        :return: string with path to the file
        """
        if path:
            sql_path = path
        else:
            create_directory()
            sql_path = self.get_view_ddl_sql_default_path()
        logging.info(
            "Outputting view metadata for {schema_count} schemas:"
            " {sql_path}".format(
                schema_count=len(view_ddl),
                sql_path=sql_path))
        with open(sql_path, "w") as f:
            for schema in view_ddl:
                for view in view_ddl[schema]:
                    try:
                        f.write("{sql}\n\n".format(
                            sql=view_ddl[schema][view]))
                    except Exception as e:
                        logging.info(
                            "Failed on view definition: {error}\n{sql}".format(
                                error=str(e), sql=view_ddl[schema][view]))
                        try:
                            logging.info(
                                "View {schema}.{view} had issues".format(
                                    schema=schema, view=view))
                        except:
                            pass
        return sql_path
