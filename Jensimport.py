import os
import shutil
import snowflake.connector

from datetime import datetime
now = datetime.now()
current_time = now.strftime("%H:%M:%S")
print("Start Time =", current_time)


#delete directories created by previous export to ensure dropped objects are removed
def delete_folder_contents(export_path):
    try:
        for filename in os.listdir(export_path):
            file_path = os.path.join(export_path, filename)
            if os.path.isfile(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        print(f"Contents of folder '{export_path}' successfully deleted.")
    except FileNotFoundError:
        print(f"Folder '{export_path}' not found.")
    except Exception as e:
        print(f"Error occurred while deleting contents of folder '{export_path}': {str(e)}")


# Export path
export_path = 'C:\Projects-Repo\SNOWFLAKE_BOOYAH\JEN_DB_FILES'   # Specify the path where database artifacts will be exported
delete_folder_contents(export_path)
os.makedirs(export_path, exist_ok=True)

# Snowflake connection parameters
account = 'wt77763.east-us-2.azure'
user = 'pythonrun'
password = 'B4byJ4n3'
warehouse = 'COMPUTE_WH'
database = 'PC_INFORMATICA_DB'

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

# Export databases and artifacts - delivered databases contain some items that cannot be exported out
query = f"select * from information_schema.databases " \
        f"where database_NAME not like 'SNOWFLAKE%' AND TYPE = 'STANDARD';"

# Execute the query to fetch all databases
cursor.execute(query)

# Fetch all the databases
databases = cursor.fetchall()

for db in databases:
    db_name = db[0]
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


        #build structure for elements we are exporting
        dynamic_table_folder_path = os.path.join(schema_export_path, "DYNAMIC_TABLES")
        os.makedirs(dynamic_table_folder_path, exist_ok=True)

        procedures_folder_path = os.path.join(schema_export_path, "PROCEDURES")
        os.makedirs(procedures_folder_path, exist_ok=True)

        streams_folder_path = os.path.join(schema_export_path, "STREAMS")
        os.makedirs(streams_folder_path, exist_ok=True)

        table_folder_path = os.path.join(schema_export_path, "TABLES")
        os.makedirs(table_folder_path, exist_ok=True)

        view_folder_path = os.path.join(schema_export_path, "VIEWS")
        os.makedirs(view_folder_path, exist_ok=True)

        tasks_folder_path = os.path.join(schema_export_path, "TASKS")
        os.makedirs(tasks_folder_path, exist_ok=True)


        # Export tables
        table_query = f"SHOW TABLES IN SCHEMA {db_name}.{schema_name}"
        cursor.execute(table_query)
        tables = cursor.fetchall()

        for table in tables:
            table_name = table[1]
            table_export_path = os.path.join(table_folder_path, table_name + ".sql")
            table_export_query = f"SELECT GET_DDL('TABLE','{db_name}.{schema_name}.{table_name}')"
            cursor.execute(table_export_query)
            table_create_statement = cursor.fetchone()[0]

            with open(table_export_path, 'w') as table_file:
                table_file.write(table_create_statement)

        # Export dynamic_tables
        dynamic_table_query = f"SHOW DYNAMIC TABLES IN SCHEMA {db_name}.{schema_name}"
        cursor.execute(dynamic_table_query)
        dynamic_tables = cursor.fetchall()

        for dynamic_table in dynamic_tables:
            dynamic_table_name = dynamic_table[1]
            dynamic_table_export_path = os.path.join(dynamic_table_folder_path, dynamic_table_name + ".sql")
            dynamic_table_export_query = f"SELECT GET_DDL('DYNAMIC_TABLE','{db_name}.{schema_name}.{dynamic_table_name}')"
            cursor.execute(dynamic_table_export_query)
            dynamic_table_create_statement = cursor.fetchone()[0]

            with open(dynamic_table_export_path, 'w') as dynamic_table_file:
                dynamic_table_file.write(dynamic_table_create_statement)

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

        # Export procedures
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

        # Export streams
        streams_query = f"SHOW STREAMS IN SCHEMA;"
        cursor.execute(streams_query)
        streams = cursor.fetchall()

        for streams in streams:
            streams_name = streams[1]
            streams_export_path = os.path.join(streams_folder_path, streams_name + ".sql")
            streams_export_query = f"SELECT GET_DDL('STREAM', '{db_name}.{schema_name}.{streams_name}')"
            cursor.execute(streams_export_query)
            streams_create_statement = cursor.fetchone()[0]

            with open(streams_export_path, 'w') as streams_file:
                streams_file.write(streams_create_statement)

        # Export tasks
        tasks_query = f"SHOW TASKS IN SCHEMA;"
        cursor.execute(tasks_query)
        tasks = cursor.fetchall()

        for tasks in tasks:
            tasks_name = tasks[1]
            tasks_export_path = os.path.join(tasks_folder_path, tasks_name + ".sql")
            tasks_export_query = f"SELECT GET_DDL('TASK', '{db_name}.{schema_name}.{tasks_name}')"
            cursor.execute(tasks_export_query)
            tasks_create_statement = cursor.fetchone()[0]

            with open(tasks_export_path, 'w') as tasks_file:
                tasks_file.write(tasks_create_statement)

# Close the cursor and connection
cursor.close()
conn.close()

from datetime import datetime
now = datetime.now()
current_time = now.strftime("%H:%M:%S")
print("End Time =", current_time)
