"""sap-hana tap class."""
from singer_sdk import SQLTap
from singer_sdk import typing as th  # JSON schema typing helpers
from tap_sap_hana.client import SapHanaStream


class TapSapHana(SQLTap):
    """sap-hana tap class."""
    name = "tap-sap-hana"
    default_stream_class = SapHanaStream

    config_jsonschema = th.PropertiesList(
        th.Property(
            "host",
            th.StringType,
            description="The FQDN of the Host serving out the SQL Instance"
        ),
        th.Property(
            "port",
            th.IntegerType,
            description="The port on which SQL awaiting connection"
        ),
        th.Property(
            "user",
            th.StringType,
            description="The User Account who has been granted access to the SQL Server"
        ),
        th.Property(
            "password",
            th.StringType,
            description="The Password for the User account"
        ),
        th.Property(
            "schema",
            th.StringType,
            description="The Default Schema for this connection"
        ),
        th.Property(
            "sqlalchemy_eng_params",
            th.ObjectType(
                th.Property(
                "fast_executemany",
                th.StringType,
                description="Fast Executemany Mode: True, False"
                ),
                th.Property(
                "future",
                th.StringType,
                description="Run the engine in 2.0 mode: True, False"
                )
            ),
            description="SQLAlchemy Engine Paramaters: fast_executemany, future"
        ),
        th.Property(
            "sqlalchemy_url_query",
            th.ObjectType(
                th.Property(
                "driver",
                th.StringType,
                description="The Driver to use when connection should match the Driver Type"
                ),
                th.Property(
                "TrustServerCertificate",
                th.StringType,
                description="This is a Yes No option"
                )
            ),
            description="SQLAlchemy URL Query options: driver, TrustServerCertificate"
        ),
        th.Property(
            "batch_config",
            th.ObjectType(
                th.Property(
                    "encoding",
                    th.ObjectType(
                        th.Property(
                            "format",
                            th.StringType,
                            description="Currently the only format is jsonl",
                        ),
                        th.Property(
                            "compression",
                            th.StringType,
                            description="Currently the only compression options is gzip",
                        )
                    )
                ),
                    th.Property(
                    "storage",
                    th.ObjectType(
                        th.Property(
                            "root",
                            th.StringType,
                            description="the directory you want batch messages to be placed in\n"\
                                        "example: file://test/batches",
                        ),
                        th.Property(
                            "prefix",
                            th.StringType,
                            description="What prefix you want your messages to have\n"\
                                        "example: test-batch-",
                        )
                    )
                )
            ),
            description="Optional Batch Message configuration",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync"
        ),
    ).to_dict()


if __name__ == "__main__":
    TapSapHana.cli()
