import snowflake.connector
import os

# Snowflake connection parameters
account = 'gw81556.central-us.azure'
user = 'pythonrun'
password = 'B4byJ4n3'
warehouse = 'COMPUTE_WH'
database = 'DSS_2_STAGE_DB'

# Snowflake connection
conn = snowflake.connector.connect(
    account=account,
    user=user,
    password=password,
    warehouse=warehouse,
    database=database
)

# Snowflake cursor
cursor = conn.cursor()

# Export path
export_path = 'C:\Projects-Repo\SNOWFLAKE_BOOYAH\DB_Files'   # Specify the path where database artifacts will be exported
os.makedirs(export_path, exist_ok=True)

# Export databases and artifacts - delivered databases contain some items that cannot be exported out
query = f"SHOW DATABASES like 'DSS%' "

# Execute the query to fetch all databases
cursor.execute(query)

# Fetch all the databases
databases = cursor.fetchall()

for db in databases:
    db_name = db[1]
    db_export_path = os.path.join(export_path, db_name)
    os.makedirs(db_export_path, exist_ok=True)

    # Export schemas
    schema_query = f"select * from  {db_name}.information_schema.schemata where schema_name not in ('SCHEMACHANGE', 'INFORMATION_SCHEMA')"
    cursor.execute(schema_query)
    schemas = cursor.fetchall()

    for schema in schemas:
        schema_name = schema[1]
        schema_export_path = os.path.join(db_export_path, schema_name)
        os.makedirs(schema_export_path, exist_ok=True)

        # Export tables
        table_query = f"SHOW TABLES IN SCHEMA {db_name}.{schema_name}"
        cursor.execute(table_query)
        tables = cursor.fetchall()

        #build structure
        table_folder_path = os.path.join(schema_export_path, "TABLES")
        os.makedirs(table_folder_path, exist_ok=True)
        view_folder_path = os.path.join(schema_export_path, "VIEWS")
        os.makedirs(view_folder_path, exist_ok=True)
        procedures_folder_path = os.path.join(schema_export_path, "PROCEDURES")
        os.makedirs(procedures_folder_path, exist_ok=True)

        for table in tables:
            table_name = table[1]
            table_export_path = os.path.join(table_folder_path, table_name + ".sql")
            table_export_query = f"SELECT GET_DDL('TABLE','{db_name}.{schema_name}.{table_name}')"
            cursor.execute(table_export_query)
            table_create_statement = cursor.fetchone()[0]

            with open(table_export_path, 'w') as table_file:
                table_file.write(table_create_statement)

        # Export views
        view_query = f"SHOW VIEWS IN SCHEMA {db_name}.{schema_name}"
        cursor.execute(view_query)
        views = cursor.fetchall()

        for view in views:
            view_name = view[1]
            view_export_path = os.path.join(view_folder_path, view_name + ".sql")
            view_export_query = f"SELECT GET_DDL('VIEW','{db_name}.{schema_name}.{view_name}')"
            cursor.execute(view_export_query)
            view_create_statement = cursor.fetchone()[0]

            with open(view_export_path, 'w') as view_file:
                view_file.write(view_create_statement)

        # Export stored procedures
        sp_query = f"select * from {db_name}.information_schema.procedures"
        cursor.execute(sp_query)
        stored_procedures = cursor.fetchall()

        for sp in stored_procedures:
            sp_name = sp[2]
            sp_arg = sp[4]
            sp_export_path = os.path.join(procedures_folder_path, sp_name + ".sql")
            sp_export_query = f"SELECT GET_DDL('PROCEDURE', '{db_name}.{schema_name}.{sp_name}{sp_arg}')"
            cursor.execute(sp_export_query)
            sp_create_statement = cursor.fetchone()[0]

            with open(sp_export_path, 'w') as sp_file:
                sp_file.write(sp_create_statement)

# Close the cursor and connection
cursor.close()
conn.close()