Snowflake BYODB View Writer
=============

Takes all tables in selected bucket and creates views containing datatypes in the external db within the same Snowflake account.



**Table of contents:**

[TOC]

Functionality notes
===================

Prerequisites
=============

Create dedicated user with read access to the KBC project databse and view creation access to the external database:

```sql
CREATE ROLE "VIEW_CREATOR_ROLE_XX";
CREATE USER "VIEW_CREATOR"
    PASSWORD = "YOUR_PASSWORD"
    DEFAULT_ROLE = "VIEW_CREATOR_ROLE_XX";

GRANT ROLE "VIEW_CREATOR_ROLE_XX" TO USER "VIEW_CREATOR";

-- Assign necessary grants for KBC Project Read Access
GRANT USAGE ON WAREHOUSE "YOUR_BYODB_WAREHOUSE" TO ROLE VIEW_CREATOR_ROLE_XX;
GRANT USAGE ON DATABASE KEBOOLA_XX TO ROLE VIEW_CREATOR_ROLE_XX;

GRANT USAGE ON FUTURE SCHEMAS IN DATABASE KEBOOLA_XX TO ROLE VIEW_CREATOR_ROLE_XX;
GRANT USAGE ON ALL SCHEMAS IN DATABASE KEBOOLA_XX TO ROLE VIEW_CREATOR_ROLE_XX;

GRANT SELECT ON ALL TABLES IN DATABASE KEBOOLA_XX TO ROLE VIEW_CREATOR_ROLE_XX;
GRANT SELECT ON FUTURE TABLES IN DATABASE KEBOOLA_XX TO ROLE VIEW_CREATOR_ROLE_XX;

-- Assign necessary grants for EXTERNAL DB View Creation
GRANT USAGE ON DATABASE YOUR_EXTERNAL_DB TO ROLE VIEW_CREATOR_ROLE_XX;
GRANT CREATE SCHEMA ON DATABASE YOUR_EXTERNAL_DB TO ROLE VIEW_CREATOR_ROLE_XX;
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
