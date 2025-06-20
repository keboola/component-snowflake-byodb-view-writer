# Snowflake BYODB View Writer

Takes all tables in selected bucket or buckets and creates views containing datatypes in the external db within the same Snowflake
account.

## Functionality notes

It is advisable to enable the RO role for the project, so the View creating role has only read access. Also, to support
shared buckets the RO role must be enabled in both projects, otherwise the component will fail when shared tables are
enabled in the configuration.

## Prerequisites

No Snowflake account sharing is needed. Views are created **in the same Snowflake account** where the project BYODB backend runs.

To allow the component to work correctly, set up the following:

### 1. (Recommended) Create a dedicated role and user

```sql
-- Replace with your own values
CREATE ROLE IF NOT EXISTS <YOUR_ROLE>;

CREATE USER IF NOT EXISTS <YOUR_USERNAME>
  PASSWORD = 'SomeStrongPassword123!'
  DEFAULT_ROLE = <YOUR_ROLE>
  DEFAULT_WAREHOUSE = YOUR_WAREHOUSE

GRANT ROLE <YOUR_ROLE> TO USER <YOUR_USERNAME>;
```

### 2. Grant read-only access to Keboola Storage tables

```sql
-- Grant read access to one or more storage schemas (e.g., IN_C_SALES) 
GRANT USAGE ON DATABASE <STORAGE_DB> TO ROLE <YOUR_ROLE>;
GRANT USAGE ON SCHEMA <STORAGE_DB>.<STORAGE_BUCKET> TO ROLE YOUR_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA <STORAGE_DB>.<STORAGE_BUCKET> TO ROLE <YOUR_ROLE>;

-- Optionally allow access to future tables
GRANT SELECT ON FUTURE TABLES IN SCHEMA <STORAGE_DB>.<STORAGE_BUCKET> TO ROLE <YOUR_ROLE>;
```

Repeat for all necessary storage schemas (e.g., `OUT_C_ANALYTICS`, etc.).

### 3. Grant write access to the target schema for views

```sql
-- Create if necessary
CREATE DATABASE IF NOT EXISTS <DESTINATION_DB>;
CREATE SCHEMA IF NOT EXISTS <DESTINATION_DB>.<TARGET_SCHEMA>;

-- Grant permission to create views
GRANT USAGE ON DATABASE <DESTINATION_DB> TO ROLE <YOUR_ROLE>;
GRANT USAGE ON SCHEMA <DESTINATION_DB>.<TARGET_SCHEMA> TO ROLE <YOUR_ROLE>;
GRANT CREATE VIEW ON SCHEMA <DESTINATION_DB>.<TARGET_SCHEMA> TO ROLE <YOUR_ROLE>;
```

> ⚠️ Do **not** grant `ALL PRIVILEGES` or `CREATE TABLE` unless strictly required. This component only needs to create views.

> ⚠️ **Role Compatibility Note**  
> Ensure that the role configured for the component (`<YOUR_ROLE>`) has both:  
> – **read access** to source Storage tables (buckets), and  
> – **write access** to create views in the destination schema.  
>  
> If you're using a **restricted Read-Only role (RO)**, make sure it has been explicitly granted the required privileges in both contexts. Otherwise, the component will fail with `not authorized` or `insufficient privileges` errors

### 4. Configure the component in Keboola

When setting up the component:

- Use the BYODB Snowflake credentials (username, password, host, warehouse)
- Set the destination database and schema for views (`<DESTINATION_DB>.<TARGET_SCHEMA>`)
- Select tables from Storage buckets to expose (e.g., `out.c-analytics`)

Each selected table will result in a `VIEW` created in the target schema that directly references the table in Storage.


## Authentication

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

### Example Configuration:
```json
{
  "auth_type": "password",
  "account": "xy12345.eu-central-1",
  "username": "MANAGE_PRJ",
  "#password": "your-password",
  "warehouse": "KEBOOLA_WAREHOUSE",
  "role": "MANAGE_ROLE",
  "db_name_prefix": "KEBOOLA_"
}
```

For key pair authentication:
```json
{
  "auth_type": "key_pair",
  "account": "xy12345.eu-central-1",
  "username": "MANAGE_PRJ",
  "#private_key": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----",
  "#private_key_pass": "optional-passphrase",
  "warehouse": "KEBOOLA_WAREHOUSE",
  "role": "MANAGE_ROLE",
  "db_name_prefix": "KEBOOLA_"
}
```

## Row Configuration

Each configuration row requires the following parameters:

### **Required Parameters**
- **Target DB name** (required) - Name of the destination database in Snowflake
- **Storage Buckets** (required) - List of storage buckets to process. If empty, all buckets in the project will be used.

### Schema Mapping
- **Custom schema mapping** (optional) - Enable to map buckets to custom schemas
  - Default: `false`
- **Schema Mapping** (required if custom mapping enabled) - Maps source buckets to destination schemas
  - **Storage Bucket** - Source bucket ID
  - **Target Schema** - Target schema name in Snowflake

### Additional Options
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

### Example Row Configuration
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

## Development

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

## Integration

For information about deployment and integration with KBC, please refer to the
[deployment section of developers documentation](https://developers.keboola.com/extend/component/deployment/)
