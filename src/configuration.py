import dataclasses
import json
from dataclasses import dataclass
from typing import List, Optional

import dataconf
from keboola.component import UserException


class ConfigurationBase:

    @staticmethod
    def _convert_private_value(value: str):
        return value.replace('"#', '"pswd_')

    @staticmethod
    def _convert_private_value_inv(value: str):
        if value and value.startswith('pswd_'):
            return value.replace('pswd_', '#', 1)
        else:
            return value

    @staticmethod
    def load_from_dict(configuration: dict):
        """
        Initialize the configuration dataclass object from dictionary.
        Args:
            configuration: Dictionary loaded from json configuration.

        Returns:

        """
        json_conf = json.dumps(configuration)
        json_conf = ConfigurationBase._convert_private_value(json_conf)
        return dataconf.loads(json_conf, Configuration, ignore_unexpected=True)

    @classmethod
    def get_dataclass_required_parameters(cls) -> List[str]:
        """
        Return list of required parameters based on the dataclass definition (no default value)
        Returns: List[str]

        """
        return [cls._convert_private_value_inv(f.name) for f in dataclasses.fields(cls)
                if f.default == dataclasses.MISSING
                and f.default_factory == dataclasses.MISSING]


@dataclass
class AdditionalOptions(ConfigurationBase):
    column_case: str = 'original'
    view_case: str = 'original'
    schema_case: str = 'original'
    use_bucket_alias: bool = True
    drop_stage_prefix: bool = False
    use_table_alias: bool = False
    ignore_shared_tables: bool = True


@dataclass
class SchemaMapping(ConfigurationBase):
    bucket_id: str
    destination_schema: str


@dataclass
class Configuration(ConfigurationBase):
    # Connection options
    auth_type: str
    account: str
    warehouse: str
    username: str
    role: str
    destination_db: str
    bucket_ids: List[str]
    pswd_password: str = None
    private_key: str = None
    private_key_pass: str = None
    # Row configuration
    additional_options: Optional[AdditionalOptions] = None
    schema_mapping: List[SchemaMapping] = dataclasses.field(default_factory=list)
    debug: bool = False
    pswd_storage_token: str = ''
    db_name_prefix: str = 'KEBOOLA_'

    def validate_schema_mapping(self, bucket_ids: List[str]):
        """
        Validates schema mapping based on provided list of valid bucket IDs
        Args:
            bucket_ids: Valid bucket ids.

        Returns:

        """
        invalid_mapping = [m.bucket_id for m in self.schema_mapping if m.bucket_id not in bucket_ids]
        if self.schema_mapping and invalid_mapping:
            raise UserException(f"Some bucket names are invalid in the schema mapping: {invalid_mapping}. "
                                f"Please use on of the selected buckets: {bucket_ids}")
