Replicates Storage tables as Views in a selected destination Snowflake database. Creates views that maintain the original data types from the source tables, allowing data access across databases within the same Snowflake account.

Key features:
- Supports both password and key pair authentication
- Flexible schema mapping options
- Customizable naming conventions for columns, views, and schemas
- Option to use bucket aliases and table user-defined names
- Handles shared tables with proper role configuration
- Supports multiple storage buckets in a single configuration

The component requires proper role setup in Snowflake, including read access to the KBC project database and view creation privileges in the destination database. It's recommended to enable the RO role for the project to ensure proper access control.
