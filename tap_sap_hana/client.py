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

    @staticmethod
    def to_jsonschema_type(sql_type: sqlalchemy.types.TypeEngine) -> dict:
        """Returns a JSON Schema equivalent for the given SQL type.

        Developers may optionally add custom logic before calling the default
        implementation inherited from the base class.
        """

        """
        Checks for the HANA type of NUMERIC
            if scale = 0 it is typed as a INTEGER
            if scale != 0 it is typed as NUMBER
        """

        # singer_sdk.typing appends T00:00:00Z to dates ? which breaks destination into delivery tables for redshift
        # this is a workaround
        if str(sql_type) == "DATE":
            sql_type = "varchar"

        return SQLConnector.to_jsonschema_type(sql_type)

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
