import functools
import logging
from contextlib import contextmanager
from dataclasses import dataclass, asdict
from cryptography.hazmat.primitives import serialization

import snowflake
from snowflake.connector import SnowflakeConnection
from snowflake.connector.cursor import SnowflakeCursor


@dataclass
class Credentials:
    account: str
    user: str
    warehouse: str
    password: str = ""
    private_key: str = ""
    private_key_pass: str = ""
    database: str = ""
    schema: str = ""
    role: str = ""
    auth_type: str = "key_pair"


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
            if isinstance(a, str) and ";" in a:
                raise ValueError(f"Invalid SQL parameter {a}")
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
            cfg["session_parameters"] = session_parameters
            self.__connection = self._create_snfk_connection(cfg, session_parameters)
            self.__cursor = self.__connection.cursor(snowflake.connector.DictCursor)
            yield self
        finally:
            self.close()

    def _create_snfk_connection(self, config: dict, session_parameters: dict = None):
        auth_type = config.get("auth_type", "key_pair")
        logging.info(f"Using authentication type: {auth_type}")

        if auth_type != "key_pair" or not config.get("private_key"):
            try:
                connection = snowflake.connector.connect(
                    user=config["user"],
                    password=config["password"],
                    account=config["account"],
                    database=config["database"],
                    warehouse=config["warehouse"],
                    role=config["role"],
                    schema=config["schema"],
                    session_parameters=session_parameters,
                )
                logging.info(
                    "Snowflake connection created successfully with password authentication"
                )
                return connection
            except Exception as e:
                logging.error(
                    "Failed to create Snowflake connection with password: %s", str(e)
                )
                raise
        else:
            private_key_pem = config["private_key"].encode("utf-8")
            raw_passphrase = config.get("private_key_pass")

            if raw_passphrase:
                passphrase = raw_passphrase.encode("utf-8")
                private_key = serialization.load_pem_private_key(
                    data=private_key_pem, password=passphrase
                )
            else:
                private_key = serialization.load_pem_private_key(
                    data=private_key_pem, password=None
                )

            private_key_der = private_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )
            try:
                connection = snowflake.connector.connect(
                    user=config["user"],
                    account=config["account"],
                    warehouse=config["warehouse"],
                    private_key=private_key_der,
                    database=config.get("database", ""),
                    role=config.get("role", ""),
                    schema=config.get("schema", ""),
                    session_parameters=session_parameters,
                )
                logging.info(
                    "Snowflake connection created successfully with key_pair authentication"
                )
                return connection

            except Exception as e:
                logging.error(
                    "Failed to create Snowflake connection with key_pair: %s",
                    str(e),
                )
                raise

    @_check_connection
    def execute_query(self, query):
        logging.debug(f"{query}")
        self._cursor.execute(query).fetchall()

    @validate_sql_placeholders
    def create_or_replace_view(
        self,
        name,
        columns_definition: str,
        source_table: str,
        copy_grants: bool = False,
    ):
        copy_grants_query = ""
        if copy_grants:
            copy_grants_query = " COPY GRANTS"
        statement = (
            f"CREATE OR REPLACE VIEW {name}{copy_grants_query} "
            f"AS SELECT {columns_definition} FROM {source_table}"
        )
        logging.info(
            f"Creating view {name}. (Query in detail)",
            extra={"full_message": statement},
        )
        self.execute_query(statement)

    @validate_sql_placeholders
    def create_or_replace_schema(
        self, database: str, schema_name: str, copy_grants: bool = False
    ):
        copy_grants_query = ""
        if copy_grants:
            copy_grants_query = " COPY GRANTS"
        statement = (
            f'CREATE OR REPLACE SCHEMA "{database}"."{schema_name}"{copy_grants_query};'
        )
        self.execute_query(statement)

    @validate_sql_placeholders
    def create_if_not_exist_schema(
        self, database: str, schema_name: str, copy_grants: bool = False
    ):
        copy_grants_query = ""
        if copy_grants:
            copy_grants_query = " COPY GRANTS"
        statement = f'CREATE SCHEMA IF NOT EXISTS "{database}"."{schema_name}"{copy_grants_query};'
        self.execute_query(statement)

    @validate_sql_placeholders
    @_check_connection
    def use_role(self, role: str):
        self.execute_query(f"USE ROLE {role};")

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
        if self._connection:
            self._connection.close()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
