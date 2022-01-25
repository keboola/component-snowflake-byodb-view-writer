from dataclasses import dataclass
from functools import lru_cache
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
            if not datatype.type and md_item['key'] in ['KBC.datatype.basetype']:
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

    def _build_column_definitions(self, table_columns: Dict[str, StorageDataType],
                                  column_name_case: str = 'original') -> str:
        column_definitions = []
        for name, dtype in table_columns.items():
            # Anything that is not STRING needs to be wrapped in  NULLIF
            if dtype.type.upper() != 'STRING':
                identifier_name = f'NULLIF("{name}", \'\')'
            else:
                identifier_name = f'"{name}"'

            column_def = f'{identifier_name}::{dtype.type}'
            if dtype.length and dtype.type.upper() != 'INTEGER':
                column_def += f'({dtype.length})'
            column_def += f' AS "{self._convert_case(name, column_name_case)}"'
            column_definitions.append(column_def)

        return ','.join(column_definitions)

    @staticmethod
    def _convert_case(identifier: str, case_conversion: str = 'original'):
        """
        Modifies the case of the name identifier.
        'original' to keep the case unchanged, 'upper'/'lower' to force the case of the identifier
        Args:
            identifier: string to convert
            case_conversion: 'original' to keep the case unchanged, 'upper'/'lower'

        Returns:

        """
        if case_conversion == 'original':
            pass
        elif case_conversion == 'upper':
            identifier = identifier.upper()
        elif case_conversion == 'lower':
            identifier = identifier.lower()
        else:
            raise ValueError(
                f"Invalid case option '{case_conversion}', supported values are ['original','upper','lower']")
        return identifier

    def get_all_bucket_ids(self):
        return [b['id'] for b in self._sapi_client.buckets.list()]

    def create_views_from_bucket(self, bucket_id: str, destination_database: str,
                                 view_name_case: str = 'original',
                                 column_name_case: str = 'original',
                                 use_bucket_alias: bool = True,
                                 session_id: str = ''):
        """
        Creates views with datatypes for all tables in the bucket.
        Args:
            bucket_id: Source KBC Storage bucket ID
            destination_database: Destination DB name in Snowflake.
            column_name_case: str: Modifies the case of the COLUMN name identifier.
                                    'original' to keep the case unchanged, 'upper'/'lower' to force the case
                                    of the identifier
            view_name_case: str: Modifies the case of the VIEW name identifier.
                                    'original' to keep the case unchanged, 'upper'/'lower' to force the case
                                    of the identifier
            session_id: Optional ID to use in session ID
            use_bucket_alias: bool: Use bucket alias instead of the Bucket ID for view name

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

            destination_schema = self._get_destination_schema_name(bucket_id, use_bucket_alias)

            self._snowflake_client.create_if_not_exist_schema(destination_database, destination_schema)
            for table in tables_resp:
                table_name = table['name']
                table_columns = self._get_table_columns(table)

                self._create_view_in_external_db(bucket_id, table_name, table_columns, destination_database,
                                                 view_name_case, column_name_case, use_bucket_alias)

    @lru_cache(maxsize=100)
    def _get_destination_schema_name(self, bucket_id, use_alias=True):
        bucket_detail = self._sapi_client.buckets.detail(bucket_id)
        if use_alias:
            view_name = f'{bucket_detail["stage"]}_{bucket_detail["displayName"]}'
        else:
            view_name = bucket_id
        return view_name

    def _create_view_in_external_db(self, bucket_id: str, table_name: str, table_columns: Dict[str, StorageDataType],
                                    destination_database: str,
                                    view_name_case: str = 'original',
                                    column_name_case: str = 'original',
                                    use_bucket_alias: bool = True):
        column_definitions = self._build_column_definitions(table_columns, column_name_case)

        destination_schema = self._get_destination_schema_name(bucket_id, use_bucket_alias)
        destination_table = f'"{destination_database}"."{destination_schema}"' \
                            f'."{self._convert_case(table_name, view_name_case)}"'
        source_table = f'"{self.project_db_name}"."{bucket_id}"."{table_name}"'
        columns_definition = f'{column_definitions}, "_timestamp"::TIMESTAMP AS "_timestamp"'

        self._snowflake_client.create_or_replace_view(destination_table, columns_definition, source_table, True)

    @property
    def project_db_name(self):
        return f'KEBOOLA_{self._project_id}'
