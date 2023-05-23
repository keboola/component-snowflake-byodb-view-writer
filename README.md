Snowflake BYODB View Writer
=============

Takes all tables in selected bucket and creates views containing datatypes in the external db within the same Snowflake account.



**Table of contents:**

[TOC]

Functionality notes
===================

It is advisable to enable the RO role for the project, so the View creating role has only read access.
Also, to support shared buckets the RO role must be enabled in both projects, otherwise the component will fail when shared 
tables are enabled in the configuration.

Following diagram depicts the workflow of the component:

![diagram](images/diagram.png)


Prerequisites
=============


Create dedicated user with read access to the KBC project databse and view creation access to the external database:

```sql
-- create user
CREATE USER "MANAGE_PRJ"
    PASSWORD = "XXXXXX"
    DEFAULT_ROLE = "KEBOOLA_XX";

-- Create role that will be used to create the Views
CREATE ROLE "MANAGE_ROLE";

-- Assign necessary grants for EXTERNAL DB View Creation
GRANT USAGE ON DATABASE EXTERNALDB TO ROLE MANAGE_ROLE;
GRANT CREATE SCHEMA ON DATABASE EXTERNALDB TO ROLE MANAGE_ROLE;

-- assign this role to the existing KBC Project role
GRANT ROLE KEBOOLA_5689 TO ROLE MANAGE_ROLE; 
-- OR GRANT ROLE KEBOOLA_5689_RO TO ROLE MANAGE_ROLE;
-- if you have RO role enabled.

-- Assign the KBC Project role that owns all objects in the Storage to the user
-- this is needed because KBC grants ownership to the existing tables. GRANT SELECT ON FUTURE to different role would break it.

GRANT ROLE "MANAGE_ROLE" TO USER "MANAGE_PRJ";


-- READ ONLY ROLE FOR THE EXTERNAL SCHEMA
GRANT USAGE ON future schemas in database "EXTERNALDB" TO ROLE EXAMPLE_ROLE;
GRANT USAGE ON all  schemas in database "EXTERNALDB" TO ROLE EXAMPLE_ROLE;
GRANT SELECT ON future views in database "EXTERNALDB" TO ROLE EXAMPLE_ROLE;
GRANT SELECT ON all views in database "EXTERNALDB" TO ROLE EXAMPLE_ROLE;

```





Development
-----------

If required, change local data folder (the `CUSTOM_FOLDER` placeholder) path to
your custom path in the `docker-compose.yml` file:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    volumes:
      - ./:/code
      - ./CUSTOM_FOLDER:/data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Clone this repository, init the workspace and run the component with following
command:

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
[deployment section of developers
documentation](https://developers.keboola.com/extend/component/deployment/)
