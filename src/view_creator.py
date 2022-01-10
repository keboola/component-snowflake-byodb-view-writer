from dataclasses import dataclass
from typing import Dict

from kbcstorage.client import Client

from dbstorage.snowflake_client import SnowflakeClient, Credentials


@dataclass
class StorageDataType:
    type: str
    length: str = None
    nullable: bool = None
    type_provider: str = ''
    length_provider: str = ''
    nullable_provider: str = ''


class ViewCreator:

    def __init__(self,
                 snowflake_credentials: Credentials,
                 kbc_root_url: str,
                 storage_token: str,
                 project_id: str):

        self._snowflake_client = SnowflakeClient()
        self.__snowflake_credentials = snowflake_credentials
        self._sapi_client = Client(kbc_root_url, storage_token)
        self._project_id = project_id

    def _group_by_timestamp(self, data: dict):
        result = {}
        # Iterate since end (ordered by latest)
        for d in data[::-1]:
            if not result.get(d['timestamp']):
                result[d['timestamp']] = []
            result[d['timestamp']].append(d)

        return result

    def _get_column_datatype(self, metadata: dict, column_name: str) -> StorageDataType:
        """
        Loads datatype metadata of particular column
        Args:
            metadata:
            column_name:

        Returns:

        """
        # KBC converts empty object to list
        metadata = metadata or {}

        column_metadata = metadata.get(column_name, [])
        datatype = StorageDataType('')

        for md_item in column_metadata[::-1]:
            if not datatype.type and md_item['key'] in ['KBC.datatype.type', 'KBC.datatype.baseType']:
                datatype.type = md_item['value']
                datatype.type_provider = md_item['provider']
            if not datatype.length and md_item['key'] == 'KBC.datatype.length':
                datatype.length = md_item['value']
                datatype.length_provider = md_item['provider']
            if datatype.nullable is not None and md_item['key'] == 'KBC.datatype.nullable':
                datatype.nullable = md_item['value']
                datatype.nullable_provider = md_item['provider']
            # stop if all found
            if datatype.type and datatype.length is not None and datatype.nullable is not None:
                break
            else:
                continue

        if not datatype.type:
            datatype = StorageDataType('TEXT')
        return datatype

    def _get_table_columns(self, table_response: dict) -> Dict[str, StorageDataType]:
        columns = table_response['columns']
        metadata = table_response['columnMetadata']

        column_datatypes = dict()

        for column in columns:
            column_datatypes[column] = self._get_column_datatype(metadata, column)

        return column_datatypes

    def _build_column_definitions(self, table_columns: Dict[str, StorageDataType]) -> str:
        column_definitions = []
        for name, dtype in table_columns.items():
            column_def = f'"{name}"::{dtype.type}'
            if dtype.length:
                column_def += f'({dtype.length})'
            column_def += f' AS "{name}"'
            column_definitions.append(column_def)

        return ','.join(column_definitions)

    def _build_view_query(self, bucket_id, table_name, table_columns, destination_database):

        column_definitions = self._build_column_definitions(table_columns)

        table_id = f'"{bucket_id}"."{table_name}"'
        statement = f'''
                CREATE OR REPLACE VIEW "{destination_database}".{table_id} AS
                    SELECT {column_definitions}, 
                           "_timestamp"::TIMESTAMP AS "_timestamp"
                    FROM "{self.project_db_name}".{table_id};'''

        return statement

    def create_views_from_bucket(self, bucket_id: str, destination_database: str, session_id: str = ''):
        """
        Creates views with datatypes for all tables in the bucket.
        Args:
            bucket_id: Source KBC Storage bucket ID
            destination_database: Destination DB name in Snowflake.
            session_id: Optional ID to use in session ID

        Returns:

        """
        tables_resp = self._sapi_client.buckets.list_tables(bucket_id, include=['columns', 'columnMetadata'])
        session_parameters = None
        if session_id:
            session_parameters = {
                'QUERY_TAG': f'{{"runId":"{session_id}"}}'
            }
        with self._snowflake_client.connect(self.__snowflake_credentials, session_parameters=session_parameters):
            if self.__snowflake_credentials.role:
                self._snowflake_client.use_role(self.__snowflake_credentials.role)

            self._snowflake_client.create_or_replace_schema(destination_database, bucket_id)
            for table in tables_resp:
                table_name = table['name']
                table_columns = self._get_table_columns(table)

                self._create_view_in_external_db(bucket_id, table_name, table_columns, destination_database)

    def _create_view_in_external_db(self, bucket_id, table_name, table_columns, destination_database):
        column_definitions = self._build_column_definitions(table_columns)

        table_id = f'"{bucket_id}"."{table_name}"'
        destination_table = f'"{destination_database}".{table_id}'
        source_table = f'"{self.project_db_name}".{table_id}'
        columns_definition = f'{column_definitions}, "_timestamp"::TIMESTAMP AS "_timestamp"'

        self._snowflake_client.create_or_replace_view(destination_table, columns_definition, source_table)

    @property
    def project_db_name(self):
        return f'KEBOOLA_{self._project_id}'
