'''
Template Component main class.

'''
import logging

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

# configuration variables
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
        '''
        Main execution code
        '''

        # ####### EXAMPLE TO REMOVE
        # check for missing configuration parameters

        self._init_configuration()

        # config token support
        storage_token = self.configuration.parameters.get('#storage_token') or self.environment_variables.token
        view_creator = ViewCreator(Credentials(account=self._configuration.account,
                                               user=self._configuration.username,
                                               password=self._configuration.pswd_password,
                                               warehouse=self._configuration.warehouse,
                                               role=self._configuration.role),
                                   self._get_kbc_root_url(),
                                   storage_token,
                                   self.environment_variables.project_id)

        for bucket_id in self._configuration.bucket_ids:
            logging.info(f"Creating views for {bucket_id} in destination database {self._configuration.destination_db}")
            view_creator.create_views_from_bucket(bucket_id, self._configuration.destination_db,
                                                  self.environment_variables.run_id)

    def _get_kbc_root_url(self):
        return self.environment_variables.stack_id


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
        logging.exception(exc)
        exit(2)
