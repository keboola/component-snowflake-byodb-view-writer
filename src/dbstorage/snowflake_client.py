import functools
import logging
from contextlib import contextmanager
from dataclasses import dataclass, asdict

import snowflake
from snowflake.connector import SnowflakeConnection
from snowflake.connector.cursor import SnowflakeCursor


@dataclass
class Credentials:
    account: str
    user: str
    password: str
    warehouse: str
    database: str = None
    schema: str = None
    role: str = None


class NotConnectedError(Exception):
    pass


def _check_connection(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        if not self._connection:
            raise NotConnectedError()
        return func(self, *args, **kwargs)

    return wrapper


def validate_sql_placeholders(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        for a in args:
            if ';' in a:
                raise ValueError(f'Invalid SQL parameter {a}')
        return func(self, *args, **kwargs)

    return wrapper


class SnowflakeClient:
    def __init__(self):
        self.__connection = None
        self.__cursor = None

    @contextmanager
    def connect(self, credentials_obj: Credentials, session_parameters=None):
        try:
            if not session_parameters:
                session_parameters = {}
            cfg = asdict(credentials_obj)
            cfg['session_parameters'] = session_parameters
            self.__connection = snowflake.connector.connect(**cfg)
            yield self
        finally:
            self.close()

    @_check_connection
    def execute_query(self, query):
        logging.debug(f"{query}")
        self._cursor.execute(query)

    @validate_sql_placeholders
    def create_or_replace_view(self, name, columns_definition: str, source_table: str):
        statement = f"CREATE OR REPLACE VIEW {name} AS SELECT {columns_definition} FROM {source_table}"
        logging.info(f"Creating view {name}. (Query in detail)", extra={"full_message": statement})
        self.execute_query(statement)

    @validate_sql_placeholders
    def create_or_replace_schema(self, database: str, schema_name: str):
        statement = f'CREATE OR REPLACE SCHEMA "{database}"."{schema_name}";'
        self.execute_query(statement)

    @validate_sql_placeholders
    @_check_connection
    def use_role(self, role: str):
        self.execute_query(f'USE ROLE {role};')

    @property
    def _cursor(self) -> SnowflakeCursor:
        if not self.__cursor:
            self.__cursor = self._connection.cursor(snowflake.connector.DictCursor)
        return self.__cursor

    @property
    def _connection(self) -> SnowflakeConnection:
        return self.__connection

    def close(self):
        if self.__cursor:
            self.__cursor.close()
            self.__cursor = None

        self._connection.close()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
