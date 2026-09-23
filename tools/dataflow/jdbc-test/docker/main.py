import argparse
import logging
import apache_beam as beam

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.jdbc import ReadFromJdbc

TEST_SQL = {
    "db2": """
        SELECT CURRENT TIMESTAMP AS CURRENT_TS
        FROM SYSIBM.SYSDUMMY1
    """,
    "oracle": """
        SELECT SYSTIMESTAMP AS CURRENT_TS
        FROM DUAL
    """,
    "vertica": """
        SELECT CURRENT_TIMESTAMP AS CURRENT_TS
    """,
}

DRIVER_CLASS = {
    "db2": "com.ibm.db2.jcc.DB2Driver",
    "oracle": "oracle.jdbc.OracleDriver",
    "vertica": "com.vertica.jdbc.Driver",
}


class LogResult(beam.DoFn):
    def process(self, element):
        logging.info("JDBC CONNECTION TEST SUCCESS")
        logging.info("=== JDBC QUERY RESULT === %s", element)
        yield element


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db_type", required=True, choices=["db2", "oracle", "vertica"])
    parser.add_argument("--connection_url", required=True)
    parser.add_argument("--username", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--driver_jars", required=True)

    known_args, pipeline_args = parser.parse_known_args()
    db_type = known_args.db_type.lower()
    query = TEST_SQL[db_type]
    driver_class = DRIVER_CLASS[db_type]

    logging.info("STARTING JDBC connection test")
    logging.info("DB TYPE      : %s", db_type)
    logging.info("JDBC URL     : %s", known_args.connection_url)
    logging.info("DRIVER CLASS : %s", driver_class)
    logging.info("TEST QUERY   : %s", query)

    pipeline_options = PipelineOptions(pipeline_args, save_main_session=True)

    with beam.Pipeline(options=pipeline_options) as pipeline:
        (
            pipeline
            | "Read JDBC"
            >> ReadFromJdbc(
                table_name="jdbc_test",
                query=query,
                driver_class_name=driver_class,
                jdbc_url=known_args.connection_url,
                username=known_args.username,
                password=known_args.password,
                driver_jars=known_args.driver_jars,
            )
            | "Log DB Time"
            >> beam.ParDo(LogResult())
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
