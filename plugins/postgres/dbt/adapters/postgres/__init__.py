from dbt.adapters.postgres.connections import PostgresConnectionManager
from dbt.adapters.postgres.connections import PostgresCredentials
from dbt.adapters.postgres.impl import PostgresAdapter

from dbt.adapters.base import AdapterPlugin
from dbt.include import postgres

Plugin = AdapterPlugin(
    adapter=PostgresAdapter,
    credentials=PostgresCredentials,
    include_path=postgres.PACKAGE_PATH)
