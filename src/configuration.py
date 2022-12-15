import dataclasses
import json
from dataclasses import dataclass
from typing import List, Optional

import dataconf


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
        return dataconf.loads(json_conf, Configuration)

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
class Configuration(ConfigurationBase):
    # Connection options
    account: str
    warehouse: str
    username: str
    pswd_password: str
    role: str
    destination_db: str
    # Row configuration
    bucket_ids: List[str]
    additional_options: Optional[AdditionalOptions] = None
    debug: bool = False
    pswd_storage_token: str = False
    db_name_prefix: str = 'KEBOOLA_'
