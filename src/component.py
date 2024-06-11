'''
Template Component main class.

'''
import logging
import traceback
from typing import List

from kbcstorage.client import Client
from keboola.component.base import ComponentBase, sync_action
from keboola.component.exceptions import UserException
# configuration variables
from keboola.component.sync_actions import SelectElement

import configuration
from dbstorage import snowflake_client
from dbstorage.snowflake_client import Credentials
from view_creator import ViewCreator

KEY_API_TOKEN = '#api_token'
KEY_PRINT_HELLO = 'print_hello'

# list of mandatory parameters => if some is missing,
# component will fail with readable message on initialization.
REQUIRED_PARAMETERS = [KEY_PRINT_HELLO]
REQUIRED_IMAGE_PARS = []


class Component(ComponentBase):
    """
        Extends base class for general Python components. Initializes the CommonInterface
        and performs configuration validation.

        For easier debugging the data folder is picked up by default from `../data` path,
        relative to working directory.

        If `debug` parameter is present in the `config.json`, the default logger is set to verbose DEBUG mode.
    """

    def __init__(self):
        super().__init__()
        self._configuration: configuration.Configuration
        self._snowflake_client: snowflake_client.SnowflakeClient()

    def _init_configuration(self):
        self.validate_configuration_parameters(configuration.Configuration.get_dataclass_required_parameters())
        self._configuration: configuration.Configuration = configuration.Configuration.load_from_dict(
            self.configuration.parameters)

    def run(self):
        """
        Main execution code
        """

        # check for missing configuration parameters

        self._init_configuration()

        # config token support
        storage_token = self._get_storage_token()
        view_creator = ViewCreator(Credentials(account=self._configuration.account,
                                               user=self._configuration.username,
                                               password=self._configuration.pswd_password,
                                               warehouse=self._configuration.warehouse,
                                               role=self._configuration.role),
                                   self._get_kbc_root_url(),
                                   storage_token,
                                   self.environment_variables.project_id,
                                   system_name_prefix=self._configuration.db_name_prefix)

        additional_options = self._configuration.additional_options or configuration.AdditionalOptions()

        bucket_ids = self._configuration.bucket_ids
        if not bucket_ids:
            logging.info('No buckets specified, processing all available buckets')
            bucket_ids = view_creator.get_all_bucket_ids()

        # validate schema names, check for duplicates
        # validate schema mapping
        self._configuration.validate_schema_mapping(bucket_ids)
        schema_mapping = self._configuration.schema_mapping

        view_creator.validate_schema_names(bucket_ids, additional_options.use_bucket_alias,
                                           additional_options.drop_stage_prefix, schema_mapping)

        for bucket_id in bucket_ids:
            logging.info(f"Creating views for {bucket_id} in destination database {self._configuration.destination_db}")
            view_creator.create_views_from_bucket(bucket_id, self._configuration.destination_db,
                                                  column_name_case=additional_options.column_case,
                                                  view_name_case=additional_options.view_case,
                                                  schema_name_case=additional_options.schema_case,
                                                  use_bucket_alias=additional_options.use_bucket_alias,
                                                  use_table_alias=additional_options.use_table_alias,
                                                  session_id=self.environment_variables.run_id,
                                                  skip_shared_tables=additional_options.ignore_shared_tables,
                                                  drop_stage_prefix=additional_options.drop_stage_prefix,
                                                  schema_mapping=schema_mapping)

    @sync_action('get_buckets')
    def get_available_buckets(self) -> List[SelectElement]:
        """
        Sync action for getting list of available buckets
        Returns:

        """
        sapi_client = Client(self._get_kbc_root_url(), self._get_storage_token())

        buckets = sapi_client.buckets.list()
        return [SelectElement(value=b['id'], label=f'({b["stage"]}) {b["name"]}') for b in buckets]

    def _get_kbc_root_url(self):
        return f'https://{self.environment_variables.stack_id}'

    def _get_storage_token(self) -> str:
        return self.configuration.parameters.get('#storage_token') or self.environment_variables.token


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.error("app error")
        traceback.print_exc()
        logging.error(exc)
        logging.exception(exc)
        exit(2)
