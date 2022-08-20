from dataclasses import dataclass
from typing import Dict, List

from kbcstorage.client import Client
from keboola.component import UserException

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
        self._current_project_id = project_id

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
            if md_item['key'] == 'KBC.datatype.nullable':
                datatype.nullable = bool(md_item['value'])
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
            if dtype.type.upper() != 'STRING' or dtype.nullable:
                identifier_name = f'NULLIF("{name}", \'\')'
            else:
                identifier_name = f'"{name}"'

            column_def = f'{identifier_name}::{dtype.type}'
            # Only NUMERIC types can have length
            if dtype.length and dtype.type.upper() in ['NUMERIC']:
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

    def validate_schema_names(self, bucket_ids: List[str], use_bucket_alias: bool, drop_stage_prefix: bool):
        bucket_details = [self._sapi_client.buckets.detail(bucket_id) for bucket_id in bucket_ids]
        schema_names = [self._get_destination_schema_name(bd, use_bucket_alias, drop_stage_prefix) for bd in
                        bucket_details]
        seen = set()
        duplicates = []
        for s in schema_names:
            if s in seen:
                duplicates.append(s)
            seen.add(s)
        if duplicates:
            raise UserException(f'Current setting would lead to a duplicate schema names. '
                                f'Try to turn off the "drop stage prefix" or "Use bucket alias" options. '
                                f'Duplicate schemas:{duplicates} ')

    def create_views_from_bucket(self, bucket_id: str, destination_database: str,
                                 schema_name_case: str = 'original',
                                 view_name_case: str = 'original',
                                 column_name_case: str = 'original',
                                 use_bucket_alias: bool = True,
                                 drop_stage_prefix: bool = False,
                                 use_table_alias: bool = False,
                                 session_id: str = '',
                                 skip_shared_tables: bool = True):
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
            schema_name_case: str: Modifies the case of the SCHEMA name identifier.
                                    'original' to keep the case unchanged, 'upper'/'lower' to force the case
                                    of the identifier
            session_id: Optional ID to use in session ID
            use_bucket_alias: bool: Use bucket alias instead of the Bucket ID for view name
            use_table_alias: bool: Use user defined table alias instead of the table ID for view name
            skip_shared_tables: skip shared tables from processing
            drop_stage_prefix: drop bucket stage prefix from schema name

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
            bucket_detail = self._sapi_client.buckets.detail(bucket_id)

            # skip shared buckets if requested
            if bucket_detail.get('sourceBucket') and skip_shared_tables:
                return

            destination_schema = self._get_destination_schema_name(bucket_detail, use_bucket_alias, drop_stage_prefix)

            self._snowflake_client.create_if_not_exist_schema(destination_database,
                                                              self._convert_case(destination_schema, schema_name_case))
            for table in tables_resp:
                # update tale def according to alias
                source_table = self._handle_alias(table)
                # skip shared tables if requested
                if source_table.get('is_shared') and skip_shared_tables:
                    continue

                table_columns = self._get_table_columns(table)

                self._create_view_in_external_db(bucket_detail, destination_schema, table, source_table, table_columns,
                                                 destination_database,
                                                 schema_name_case, view_name_case, column_name_case,
                                                 use_table_alias)

    def _handle_alias(self, table: dict):
        """
        Retrieves source table of alias if present and changes the ROLE to appropriate source project
        Args:
            table:

        Returns:

        """
        source_table = {}
        if table['isAlias']:
            source_table = table['sourceTable']
            source_table['bucket_id'] = '.'.join(source_table['id'].split('.')[0:2])
            source_table['table_name'] = '.'.join(source_table['id'].split('.')[-1:])

        if table['isAlias'] and source_table['project']['id'] != int(self._project_id):
            # it is shared bucket, change role to source project
            source_table['is_shared'] = True

        return source_table

    def _get_destination_schema_name(self, bucket_detail: dict, use_alias=True, drop_stage_prefix: bool = False):

        if use_alias:
            view_name = f'{bucket_detail["stage"]}_{bucket_detail["displayName"]}'
        else:
            view_name = bucket_detail['id'].replace('.', '_')
        if drop_stage_prefix:
            view_name = view_name[len(bucket_detail['stage']) + 1:]

        return view_name

    def _create_view_in_external_db(self, bucket_detail: dict, destination_schema_name: str, table: dict,
                                    source_table: dict,
                                    table_columns: Dict[str, StorageDataType],
                                    destination_database: str,
                                    schema_name_case: str = 'original',
                                    view_name_case: str = 'original',
                                    column_name_case: str = 'original',
                                    use_table_alias: bool = False):
        """

        Args:
            bucket_detail: detail of the source bucket
            destination_schema_name: name of the destination schema
            table: detail of the storage table
            source_table: (dict) if not empty defines source of the alias table
            table_columns:
            destination_database:
            view_name_case:
            schema_name_case:
            column_name_case:
            use_table_alias: Use user defined table name

        Returns:

        """
        column_definitions = self._build_column_definitions(table_columns, column_name_case)
        bucket_id = bucket_detail['id']
        # use display or default name
        destination_table_name = table['displayName'] if use_table_alias else table['name']
        destination_table = f'"{destination_database}"' \
                            f'."{self._convert_case(destination_schema_name, schema_name_case)}"' \
                            f'."{self._convert_case(destination_table_name, view_name_case)}"'

        source_table_id = f'"{bucket_id}"."{table["name"]}"'
        source_project_id = self._project_id
        if source_table:
            source_table["id"].split('.')
            source_table_id = f'"{source_table["bucket_id"]}"."{source_table["table_name"]}"'
            source_project_id = source_table['project']['id']

        source_table_identifier = f'"{self.get_project_db_name(source_project_id)}".{source_table_id}'
        columns_definition = f'{column_definitions}, "_timestamp"::TIMESTAMP AS "_timestamp"'

        self._snowflake_client.create_or_replace_view(destination_table, columns_definition, source_table_identifier,
                                                      True)

    def get_project_db_name(self, project_id):
        return f'KEBOOLA_{project_id}'
