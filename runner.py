# -*- coding: utf-8 -*-
import argparse
import logging

from dbferret.retriever import DbFerret
from dbferret.file_writer import FileWriter

"""
Run something like this for a redshift db:

    python driver.py \
       --user <user> --pw <password> \
       --hostname <hostname> -d <database>

For postgres specify the engine_type, port and perhaps you'll need ssl_mode:

    python driver.py \
        --user <user> --pw <password> --hostname <hostname> \
        -d <database> --engine_type postgresql -p 5432 --ssl_mode True

"""


def main():
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), None))

    # Instantiate ferret object to get db metadata in subsequent steps
    dbferret = DbFerret(hostname=args.hostname, user=args.user, pw=args.pw,
                        db=args.db, ssl_mode=args.ssl_mode,
                        engine_type=args.engine_type, schema=args.schema,
                        port=args.port, warehouse=args.warehouse,
                        schema_list=args.schema_list)

    # Collect data
    table_metadata = dbferret.extract_table_metadata()
    view_ddls = dbferret.extract_view_ddl()

    # Write results
    file_writer = FileWriter(db=args.db, engine_type=args.engine_type)
    file_writer.output_table_metadata_to_tsv(table_metadata)
    file_writer.output_view_ddl_to_sql(view_ddls)


def parse_args():
    """
    :return:
    """
    parser = argparse.ArgumentParser(
        description="ferret collects metadata from a database via "
                    "reflection, provide connection information to run")

    parser.add_argument(
        "-e",
        "--engine_type",
        dest="engine_type",
        help="The database type, such as postgres or redshift used "
             "for the connection string by sqlalchemy.",
        default="redshift"
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
        help="The port used by the database for connections.",
        default="5439"
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
    parser.add_argument(
        "--schema_list",
        dest="schema_list",
        help="Comma delimited list of schemas"
    )
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    main()
