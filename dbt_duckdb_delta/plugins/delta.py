from typing import Any
from typing import Dict

from deltalake import DeltaTable, write_deltalake
import duckdb

from dbt.adapters.duckdb.plugins import BasePlugin
from dbt.adapters.duckdb.utils import SourceConfig, TargetConfig
import os
import time

class Plugin(BasePlugin):
    def initialize(self, config: Dict[str, Any]):
        self._config = config

    def configure_connection(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn

    def configure_cursor(self, cursor):
        self.cursor = cursor

    def load(self, source_config: SourceConfig):
        if "delta_table_path" not in source_config:
            raise Exception("'delta_table_path' is a required argument for the delta table!")

        table_path = source_config["delta_table_path"]
        storage_options = source_config.get("storage_options", None)

        if storage_options:
            dt = DeltaTable(table_path, storage_options=storage_options)
        else:
            dt = DeltaTable(table_path)

        # delta attributes
        as_of_version = source_config.get("as_of_version", None)
        as_of_datetime = source_config.get("as_of_datetime", None)

        if as_of_version:
            dt.load_as_version(as_of_version)

        if as_of_datetime:
            dt.load_as_version(as_of_datetime)

        df = dt.to_pyarrow_dataset()

        return df

    def default_materialization(self):
        return "view"
    
    def store(self, target_config: TargetConfig):

        mode = target_config.config.get("mode")
        
        external_location = target_config.config.get("location")
        delta_write_options = target_config.config.get("delta_write_options", {})

        assert mode is not None, "mode is required"
        assert external_location is not None, "location is required"

        # deltalake supports r2, but only through s3-interface.
        external_location = external_location.replace("r2://", "s3://")

        database = target_config.relation.database
        schema = target_config.relation.schema
        view = target_config.relation.identifier

        df = self.cursor.sql(f"select * from {database}.{schema}.{view}").arrow()

        if not DeltaTable.is_deltatable(external_location):
            DeltaTable.create(external_location, df.schema)

        write_deltalake(external_location, df, mode=mode, **delta_write_options)


# Future
# TODO add databricks catalog

