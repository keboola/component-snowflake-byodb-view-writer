Snowflake BYODB View Writer
=============

Takes all tables in selected bucket and creates views containing datatypes in the external db within the same Snowflake
account.

**Table of contents:**

[TOC]

Functionality notes
===================

It is advisable to enable the RO role for the project, so the View creating role has only read access. Also, to support
shared buckets the RO role must be enabled in both projects, otherwise the component will fail when shared tables are
enabled in the configuration.

Following diagram depicts the workflow of the component:

![diagram](docs/imgs/diagram.png)


Prerequisites
=============


Create dedicated user with read access to the KBC project databse and view creation access to the external database:

```sql
-- create user
CREATE
USER "MANAGE_PRJ"
    PASSWORD = "XXXXXX"
    DEFAULT_ROLE = "KEBOOLA_XX";

-- Create role that will be used to create the Views
CREATE ROLE "MANAGE_ROLE";

-- Assign necessary grants for EXTERNAL DB View Creation
GRANT
USAGE
ON
DATABASE
EXTERNALDB TO ROLE MANAGE_ROLE;
GRANT
CREATE SCHEMA ON DATABASE EXTERNALDB TO ROLE MANAGE_ROLE;

-- assign this role to the existing KBC Project role
GRANT
ROLE
KEBOOLA_5689 TO ROLE MANAGE_ROLE;
-- OR GRANT ROLE KEBOOLA_5689_RO TO ROLE MANAGE_ROLE;
-- if you have RO role enabled.

-- Assign the KBC Project role that owns all objects in the Storage to the user
-- this is needed because KBC grants ownership to the existing tables. GRANT SELECT ON FUTURE to different role would break it.

GRANT ROLE
"MANAGE_ROLE" TO USER "MANAGE_PRJ";


-- READ ONLY ROLE FOR THE EXTERNAL SCHEMA
GRANT USAGE ON future
schemas in database "EXTERNALDB" TO ROLE EXAMPLE_ROLE;
GRANT USAGE ON all
schemas in database "EXTERNALDB" TO ROLE EXAMPLE_ROLE;
GRANT
SELECT
ON future views in database "EXTERNALDB" TO ROLE EXAMPLE_ROLE;
GRANT
SELECT
ON all views in database "EXTERNALDB" TO ROLE EXAMPLE_ROLE;

```

Authentication
=============

For authentication the component requires following configuration parameters:

- **Authentication Type** (required) - Choose between:
  - `password` - Standard password authentication
  - `key_pair` - Key pair authentication (default)

- **Storage Token** - KBC Storage API token (optional)
- **User Name** (required) - Snowflake user name
- **Password** (required for password auth) - Snowflake user password
- **Private Key** (required for key pair auth) - Private key in PEM format
- **Private Key Passphrase** (optional) - Passphrase for encrypted private key
- **Account** (required) - Snowflake account identifier (e.g., `cID.eu-central-1`)
- **Warehouse** (required) - Name of the Snowflake warehouse to use
- **Role** (required) - Snowflake role name to use for connection
- **DB Name Prefix** (optional) - Prefix for Keboola generated DB names
  - Default value: `KEBOOLA_`
  - Format: `{PREFIX}{PROJECT_ID}`
  - Common values: `KEBOOLA_` or `SAPI_`

Example Configuration
-------------------
```json
{
  "auth_type": "password",
  "username": "MANAGE_PRJ",
  "#password": "your-password",
  "account": "xy12345.eu-central-1",
  "warehouse": "KEBOOLA_WAREHOUSE",
  "role": "MANAGE_ROLE",
  "db_name_prefix": "KEBOOLA_"
}
```

For key pair authentication:
```json
{
  "auth_type": "key_pair",
  "username": "MANAGE_PRJ",
  "#private_key": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----",
  "#private_key_pass": "optional-passphrase",
  "account": "xy12345.eu-central-1",
  "warehouse": "KEBOOLA_WAREHOUSE",
  "role": "MANAGE_ROLE",
  "db_name_prefix": "KEBOOLA_"
}
```

Row Configuration
================

Each configuration row requires the following parameters:

Required Parameters
-----------------
- **Destination DB name** (required) - Name of the destination database in Snowflake
- **Storage Buckets** (required) - List of storage buckets to process. If empty, all buckets in the project will be used.

Schema Mapping
-------------
- **Custom schema mapping** (optional) - Enable to map buckets to custom schemas
  - Default: `false`
- **Schema Mapping** (required if custom mapping enabled) - Maps source buckets to destination schemas
  - **Storage Bucket** - Source bucket ID
  - **Destination Schema** - Target schema name in Snowflake

Additional Options
----------------
Case Settings:
- **Column case** - Case formatting for column names
  - Options: `original`, `upper`, `lower`
  - Default: `original`
- **View Case** - Case formatting for view names
  - Options: `original`, `upper`, `lower`
  - Default: `original`
- **Schema Case** - Case formatting for schema names
  - Options: `original`, `upper`, `lower`
  - Default: `original`

Naming Options:
- **Use bucket alias** - Use bucket alias instead of Bucket ID in VIEW names
  - Default: `true`
  - Description: Uses user-defined bucket name instead of the technical ID
- **Drop in/out prefix** - Remove in/out prefix from schema names
  - Default: `false`
  - Description: Removes the standard in/out prefix from resulting schema names
- **Use table user defined name** - Use friendly table names
  - Default: `false`
  - Description: Uses table's user-defined name instead of the default ID in VIEW names

Other Options:
- **Ignore shared tables** - Skip processing of shared tables
  - Default: `true`
  - Description: Enable only if RO role is used and enabled in all projects

Example Row Configuration
------------------------
```json
{
  "destination_db": "EXTERNAL_DB",
  "bucket_ids": ["in.c-sales", "in.c-marketing"],
  "custom_schema_mapping": true,
  "schema_mapping": [
    {
      "bucket_id": "in.c-sales",
      "destination_schema": "SALES_DATA"
    },
    {
      "bucket_id": "in.c-marketing",
      "destination_schema": "MARKETING_DATA"
    }
  ],
  "additional_options": {
    "column_case": "lower",
    "view_case": "upper",
    "schema_case": "original",
    "use_bucket_alias": true,
    "drop_stage_prefix": false,
    "use_table_alias": true,
    "ignore_shared_tables": true
  }
}
```

Development
===========

If required, change local data folder (the `CUSTOM_FOLDER` placeholder) path to your custom path in
the `docker-compose.yml` file:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    volumes:
      - ./:/code
      - ./CUSTOM_FOLDER:/data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Clone this repository, init the workspace and run the component with following command:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
git clone git@bitbucket.org:kds_consulting_team/kds-team.app-snowflake-byodb-view-writer.git kds-team.app-snowflake-byodb-view-writer
cd kds-team.app-snowflake-byodb-view-writer
docker-compose build
docker-compose run --rm dev
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Run the test suite and lint check using this command:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
docker-compose run --rm test
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Integration
===========

For information about deployment and integration with KBC, please refer to the
[deployment section of developers documentation](https://developers.keboola.com/extend/component/deployment/)
