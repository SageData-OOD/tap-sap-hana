"""SQL client handling.

This includes SapHanaStream and SapHanaConnector.
"""
from __future__ import annotations
from copy import deepcopy

import gzip
import json
from datetime import datetime
from uuid import uuid4
from typing import Any, Dict, Iterable, Optional

# import pendulum

import sqlalchemy
from sqlalchemy.engine import URL
from sqlalchemy.engine import Engine
from sqlalchemy.engine.reflection import Inspector


from singer_sdk import SQLConnector, SQLStream
from singer_sdk.helpers._batch import (
    BaseBatchFileEncoding,
    BatchConfig,
)
from singer_sdk.streams.core import lazy_chunked_generator


class SapHanaConnector(SQLConnector):
    """Connects to the sap hana SQL source."""

    # def discover_catalog_entries(self) -> list[dict]:
    #     result = SQLConnector.discover_catalog_entries(self)

    def get_object_names(
        self, engine: Engine, inspected: Inspector, schema_name: str
    ) -> list[tuple[str, bool]]:
        """Return a list of syncable objects.

        Args:
            engine: SQLAlchemy engine
            inspected: SQLAlchemy inspector instance for engine
            schema_name: Schema name to inspect

        Returns:
            List of tuples (<table_or_view_name>, <is_view>)
        """
        self.logger.info("SCHEMA: %s", schema_name)
        if schema_name.lower() != self.config["schema"].lower():
            return []

        view_names = inspected.get_view_names(schema=schema_name)
        return [(v, True) for v in view_names]

    def get_sqlalchemy_url(cls, config: dict) -> str:
        url_drivername = "hana"
        config_url = URL.create(
            url_drivername,
            config['user'],
            config['password'],
            host = config['host'],
            # database = config['schema']
        )

        if 'port' in config:
            config_url = config_url.set(port=config['port'])

        if 'sqlalchemy_url_query' in config:
            config_url = config_url.update_query_dict(config['sqlalchemy_url_query'])

        return (config_url)

    def create_sqlalchemy_engine(self) -> sqlalchemy.engine.Engine:
        """Return a new SQLAlchemy engine using the provided config.

        Developers can generally override just one of the following:
        `sqlalchemy_engine`, sqlalchemy_url`.

        Returns:
            A newly created SQLAlchemy engine object.
        """
        eng_prefix = "ep."
        eng_config = {f"{eng_prefix}url":self.sqlalchemy_url,f"{eng_prefix}echo":"False"}

        if self.config.get('sqlalchemy_eng_params'):
            for key, value in self.config['sqlalchemy_eng_params'].items():
                eng_config.update({f"{eng_prefix}{key}": value})

        return sqlalchemy.engine_from_config(eng_config, prefix=eng_prefix)

    # @staticmethod
    # def to_jsonschema_type(sql_type: sqlalchemy.types.TypeEngine) -> dict:
    #     """Returns a JSON Schema equivalent for the given SQL type.

    #     Developers may optionally add custom logic before calling the default
    #     implementation inherited from the base class.
    #     """

    #     """
    #     Checks for the HANA type of NUMERIC
    #         if scale = 0 it is typed as a INTEGER
    #         if scale != 0 it is typed as NUMBER
    #     """
    #     if str(sql_type).startswith("NUMERIC"):
    #         if str(sql_type).endswith(", 0)"):
    #            sql_type = "int"
    #         else:
    #            sql_type = "number"

    #     if str(sql_type) in ["MONEY", "SMALLMONEY"]:
    #         sql_type = "number"

    #     return SQLConnector.to_jsonschema_type(sql_type)

    # @staticmethod
    # def to_sql_type(jsonschema_type: dict) -> sqlalchemy.types.TypeEngine:
    #     """Returns a JSON Schema equivalent for the given SQL type.

    #     Developers may optionally add custom logic before calling the default
    #     implementation inherited from the base class.
    #     """
    #     # Optionally, add custom logic before calling the parent SQLConnector method.
    #     # You may delete this method if overrides are not needed.
    #     return SQLConnector.to_sql_type(jsonschema_type)


# Custom class extends json.JSONEncoder
# class CustomJSONEncoder(json.JSONEncoder):

#     # Override default() method
#     def default(self, obj):

#         # Datetime to string
#         if isinstance(obj, datetime):
#             # Format datetime - `Fri, 21 Aug 2020 17:59:59 GMT`
#             #obj = obj.strftime('%a, %d %b %Y %H:%M:%S GMT')
#             obj = pendulum.instance(obj).isoformat()
#             return obj

#         # Default behavior for all other types
#         return super().default(obj)


class SapHanaStream(SQLStream):
    """Stream class for sap hana streams."""

    connector_class = SapHanaConnector

    # Get records from stream
    def get_records(self, context: dict | None) -> Iterable[dict[str, Any]]:
        """Return a generator of record-type dictionary objects.

        If the stream has a replication_key value defined, records will be sorted by the
        incremental key. If the stream also has an available starting bookmark, the
        records will be filtered for values greater than or equal to the bookmark value.

        Args:
            context: If partition context is provided, will read specifically from this
                data slice.

        Yields:
            One dict per record.

        Raises:
            NotImplementedError: If partition is passed in context and the stream does
                not support partitioning.
        """
        if context:
            raise NotImplementedError(
                f"Stream '{self.name}' does not support partitioning."
            )

        selected_column_names = self.get_selected_schema()["properties"].keys()
        table = self.connector.get_table(
            full_table_name=self.fully_qualified_name,
            column_names=selected_column_names,
        )
        query = table.select()

        if self.replication_key:
            replication_key_col = table.columns[self.replication_key]
            query = query.order_by(replication_key_col)

            start_val = self.get_starting_replication_key_value(context)
            if start_val:
                # DP: for whatever reason late parameter binding doesn't work for hdbcli
                query = query.where(
                    sqlalchemy.text(f"\"{self.replication_key}\" >= '{start_val}'")
                )

        if self._MAX_RECORDS_LIMIT is not None:
            query = query.limit(self._MAX_RECORDS_LIMIT)

        for record in self.connector.connection.execute(query):
            yield dict(record)

    def get_batches(
        self,
        batch_config: BatchConfig,
        context: dict | None = None,
        ) -> Iterable[tuple[BaseBatchFileEncoding, list[str]]]:
        """Batch generator function.

        Developers are encouraged to override this method to customize batching
        behavior for databases, bulk APIs, etc.

        Args:
            batch_config: Batch config for this stream.
            context: Stream partition or context dictionary.

        Yields:
            A tuple of (encoding, manifest) for each batch.
        """
        sync_id = f"{self.tap_name}--{self.name}-{uuid4()}"
        prefix = batch_config.storage.prefix or ""

        for i, chunk in enumerate(
            lazy_chunked_generator(
                self._sync_records(context, write_messages=False),
                self.batch_size,
            ),
            start=1,
        ):
            filename = f"{prefix}{sync_id}-{i}.json.gz"
            with batch_config.storage.fs() as fs:
                with fs.open(filename, "wb") as f:
                    # TODO: Determine compression from config.
                    with gzip.GzipFile(fileobj=f, mode="wb") as gz:
                        gz.writelines(
                            (json.dumps(record,
                            default=str,
                            # cls=CustomJSONEncoder,
                            ) + "\n").encode() for record in chunk
                        )
                file_url = fs.geturl(filename)

            yield batch_config.encoding, [file_url]