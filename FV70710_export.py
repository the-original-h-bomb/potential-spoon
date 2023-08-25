import os
import shutil
import snowflake.connector
import csv

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
export_path = 'C:\Projects-Repo\SNOWFLAKE_BOOYAH\FV70710_export'   # Specify the path where database artifacts will be exported
delete_folder_contents(export_path)
os.makedirs(export_path, exist_ok=True)

# Snowflake connection parameters
account = 'fv70710.central-us.azure'
user = 'WarlyBard1'
password = 'B4byJ4n3'
warehouse = 'COMPUTE_WH'
database = 'ADMINISTRATIVE'

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

role_query = f"USE SECONDARY ROLES ALL"
cursor.execute(role_query)

### security table dump ####

gtr_export_path = os.path.join(export_path, "GRANTS_TO_ROLES.csv")
gtr_query = f"SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_ROLES;"
cursor.execute(gtr_query)
gtr_file = cursor.fetchall()

with open(gtr_export_path, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow([x[0] for x in cursor.description])  # write header
    for row in gtr_file:
        writer.writerow(row)

gtu_export_path = os.path.join(export_path, "GRANTS_TO_USERS.csv")
gtu_query = f"SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_USERS;"
cursor.execute(gtu_query)
gtu_file = cursor.fetchall()

with open(gtu_export_path, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow([x[0] for x in cursor.description])  # write header
    for row in gtu_file:
        writer.writerow(row)

lh_export_path = os.path.join(export_path, "LOGIN_HISTORY.csv")
lh_query = f"SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY;"
cursor.execute(lh_query)
lh_file = cursor.fetchall()

with open(lh_export_path, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow([x[0] for x in cursor.description])  # write header
    for row in lh_file:
        writer.writerow(row)

PP_export_path = os.path.join(export_path, "PASSWORD_POLICIES.csv")
PP_query = f"SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.PASSWORD_POLICIES;"
cursor.execute(PP_query)
PP_file = cursor.fetchall()

with open(PP_export_path, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow([x[0] for x in cursor.description])  # write header
    for row in PP_file:
        writer.writerow(row)

pr_export_path = os.path.join(export_path, "POLICY_REFERENCES.csv")
pr_query = f"SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.POLICY_REFERENCES;"
cursor.execute(pr_query)
pr_file = cursor.fetchall()

with open(pr_export_path, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow([x[0] for x in cursor.description])  # write header
    for row in pr_file:
        writer.writerow(row)

roles_export_path = os.path.join(export_path, "ROLES.csv")
roles_query = f"SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.ROLES;"
cursor.execute(roles_query)
roles_file = cursor.fetchall()

with open(roles_export_path, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow([x[0] for x in cursor.description])  # write header
    for row in roles_file:
        writer.writerow(row)

SP_export_path = os.path.join(export_path, "SESSION_POLICIES.csv")
SP_query = f"SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.SESSION_POLICIES;"
cursor.execute(SP_query)
SP_file = cursor.fetchall()

with open(SP_export_path, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow([x[0] for x in cursor.description])  # write header
    for row in SP_file:
        writer.writerow(row)

'''
SESSIONS DATA IS MORE THAN GIT WILL ALLOW WITH .COM LICENSE - BUT WE MAY NOT WANT THIS ANYWAY
SESSIONS_export_path = os.path.join(export_path, "SESSIONS.csv")
SESSIONS_query = f"SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.SESSIONS;"
cursor.execute(SESSIONS_query)
SESSIONS_file = cursor.fetchall()

with open(SESSIONS_export_path, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow([x[0] for x in cursor.description])  # write header
    for row in SESSIONS_file:
        writer.writerow(row)
'''

USERS_export_path = os.path.join(export_path, "USERS.csv")
USERS_query = f"SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.USERS;"
cursor.execute(USERS_query)
USERS_file = cursor.fetchall()

with open(USERS_export_path, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow([x[0] for x in cursor.description])  # write header
    for row in USERS_file:
        writer.writerow(row)

# Export databases and artifacts - delivered databases contain some items that cannot be exported out
db_query = f"select * from information_schema.databases " \
        f"where database_NAME not like 'SNOWFLAKE%' AND TYPE = 'STANDARD';"

# Execute the query to fetch all databases
cursor.execute(db_query)

# Fetch all the databases
databases = cursor.fetchall()

for db in databases:
    db_name = db[0]
    db_export_path = os.path.join(export_path, db_name)
    os.makedirs(db_export_path, exist_ok=True)
    db_ddl_export_path = os.path.join(db_export_path, db_name + ".sql")
    db_export_query = f"SELECT GET_DDL('DATABASE','{db_name}')"
    cursor.execute(db_export_query)
    db_create_statement = cursor.fetchone()[0]

    with open(db_ddl_export_path, 'w') as db_file:
        db_file.write(db_create_statement)


    # Export schemas
    schema_query = f"select * from  {db_name}.information_schema.schemata " \
                   f"where schema_name not in ('SCHEMACHANGE', 'INFORMATION_SCHEMA')"
    cursor.execute(schema_query)
    schemas = cursor.fetchall()

    for schema in schemas:
        schema_name = schema[1]
        schema_export_path = os.path.join(db_export_path, schema_name)
        os.makedirs(schema_export_path, exist_ok=True)
        schema_ddl_export_path = os.path.join(schema_export_path, schema_name + ".sql")
        schema_export_query = f"SELECT GET_DDL('SCHEMA','{db_name}.{schema_name}')"
        cursor.execute(schema_export_query)
        schema_create_statement = cursor.fetchone()[0]

        with open(schema_ddl_export_path, 'w') as schema_file:
            schema_file.write(schema_create_statement)

        #build structure for elements we are exporting
        alerts_folder_path = os.path.join(schema_export_path, "ALERTS")
        os.makedirs(alerts_folder_path, exist_ok=True)

        dynamic_table_folder_path = os.path.join(schema_export_path, "DYNAMIC_TABLES")
        os.makedirs(dynamic_table_folder_path, exist_ok=True)

        event_table_folder_path = os.path.join(schema_export_path, "EVENT_TABLES")
        os.makedirs(event_table_folder_path, exist_ok=True)

        external_table_folder_path = os.path.join(schema_export_path, "EXTERNAL_TABLES")
        os.makedirs(external_table_folder_path, exist_ok=True)

        file_formats_folder_path = os.path.join(schema_export_path, "FILE_FORMATS")
        os.makedirs(file_formats_folder_path, exist_ok=True)

        pipes_folder_path = os.path.join(schema_export_path, "PIPES")
        os.makedirs(pipes_folder_path, exist_ok=True)

        policies_folder_path = os.path.join(schema_export_path, "POLICIES")
        os.makedirs(policies_folder_path, exist_ok=True)

        sequences_folder_path = os.path.join(schema_export_path, "SEQUENCES")
        os.makedirs(sequences_folder_path, exist_ok=True)

        stored_procedures_folder_path = os.path.join(schema_export_path, "STORED_PROCEDURES")
        os.makedirs(stored_procedures_folder_path, exist_ok=True)

        streams_folder_path = os.path.join(schema_export_path, "STREAMS")
        os.makedirs(streams_folder_path, exist_ok=True)

        table_folder_path = os.path.join(schema_export_path, "TABLES")
        os.makedirs(table_folder_path, exist_ok=True)

        tags_folder_path = os.path.join(schema_export_path, "TAGS")
        os.makedirs(tags_folder_path, exist_ok=True)

        tasks_folder_path = os.path.join(schema_export_path, "TASKS")
        os.makedirs(tasks_folder_path, exist_ok=True)

        udf_folder_path = os.path.join(schema_export_path, "UDF")
        os.makedirs(udf_folder_path, exist_ok=True)

        view_folder_path = os.path.join(schema_export_path, "VIEWS")
        os.makedirs(view_folder_path, exist_ok=True)

        # Export alerts
        alert_query = f"SHOW ALERTS IN SCHEMA {db_name}.{schema_name}"
        cursor.execute(alert_query)
        alerts = cursor.fetchall()

        for alert in alerts:
            alert_name = alert[1]
            alert_export_path = os.path.join(alerts_folder_path, alert_name + ".sql")
            alert_export_query = f"SELECT GET_DDL('ALERT','{db_name}.{schema_name}.{alert_name}')"
            cursor.execute(alert_export_query)
            alert_create_statement = cursor.fetchone()[0]

            with open(alert_export_path, 'w') as alert_file:
                alert_file.write(alert_create_statement)

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

        # Export Event Tables
        event_table_query = f"SHOW EVENT TABLES IN SCHEMA {db_name}.{schema_name}"
        cursor.execute(event_table_query)
        event_tables = cursor.fetchall()

        for event_table in event_tables:
            event_table_name = event_table[1]
            event_table_export_path = os.path.join(event_table_folder_path, event_table_name + ".sql")
            event_table_export_query = f"SELECT GET_DDL('EVENT TABLE','{db_name}.{schema_name}.{event_table_name}')"
            cursor.execute(event_table_export_query)
            event_table_create_statement = cursor.fetchone()[0]

            with open(event_table_export_path, 'w') as event_table_file:
                event_table_file.write(event_table_create_statement)

        # Export External Tables
        external_table_query = f"SHOW EXTERNAL TABLES IN SCHEMA {db_name}.{schema_name}"
        cursor.execute(external_table_query)
        external_tables = cursor.fetchall()

        for external_table in external_tables:
            external_table_name = external_table[1]
            external_table_export_path = os.path.join(external_table_folder_path, external_table_name + ".sql")
            external_table_export_query = f"SELECT GET_DDL('EXTERNAL TABLE','{db_name}.{schema_name}.{external_table_name}')"
            cursor.execute(external_table_export_query)
            external_table_create_statement = cursor.fetchone()[0]

            with open(external_table_export_path, 'w') as external_table_file:
                external_table_file.write(external_table_create_statement)

        # Export File Formats

        file_formats_query = f"SHOW FILE FORMATS IN SCHEMA {db_name}.{schema_name}"
        cursor.execute(file_formats_query)
        file_formats = cursor.fetchall()

        for file_format in file_formats:
            file_format_name = file_format[1]
            file_format_export_path = os.path.join(file_formats_folder_path, file_format_name + ".sql")
            file_format_export_query = f"SELECT GET_DDL('FILE FORMATS','{db_name}.{schema_name}.{file_format_name}')"
            cursor.execute(file_format_export_query)
            file_format_create_statement = cursor.fetchone()[0]

            with open(file_format_export_path, 'w') as file_format_file:
                file_format_file.write(file_format_create_statement)

        # Export Pipes

        pipes_query = f"SHOW PIPES IN SCHEMA {db_name}.{schema_name}"
        cursor.execute(pipes_query)
        pipes = cursor.fetchall()

        for pipe in pipes:
            pipe_name = pipe[1]
            pipe_export_path = os.path.join(pipes_folder_path, pipe_name + ".sql")
            pipe_export_query = f"SELECT GET_DDL('PIPES','{db_name}.{schema_name}.{pipe_name}')"
            cursor.execute(pipe_export_query)
            pipe_create_statement = cursor.fetchone()[0]

            with open(pipe_export_path, 'w') as pipe_file:
                pipe_file.write(pipe_create_statement)

        # Export Policies (masking, password, row access and session)
        # masking policies
        masking_policies_query = f"SHOW MASKING POLICIES IN SCHEMA {db_name}.{schema_name}"
        cursor.execute(masking_policies_query)
        mpolicies = cursor.fetchall()

        for mpolicy in mpolicies:
            mpolicy_name = mpolicy[1]
            mpolicy_export_path = os.path.join(policies_folder_path, mpolicy_name + ".sql")
            mpolicy_export_query = f"SELECT GET_DDL('POLICY','{db_name}.{schema_name}.{mpolicy_name}')"
            cursor.execute(mpolicy_export_query)
            mpolicy_create_statement = cursor.fetchone()[0]

            with open(mpolicy_export_path, 'w') as mpolicy_file:
                mpolicy_file.write(mpolicy_create_statement)

        # password policies
        password_policies_query = f"SHOW PASSWORD POLICIES IN SCHEMA {db_name}.{schema_name}"
        cursor.execute(password_policies_query)
        ppolicies = cursor.fetchall()

        for ppolicy in ppolicies:
            ppolicy_name = ppolicy[1]
            ppolicy_export_path = os.path.join(policies_folder_path, ppolicy_name + ".sql")
            ppolicy_export_query = f"SELECT GET_DDL('POLICIES','{db_name}.{schema_name}.{ppolicy_name}')"
            cursor.execute(ppolicy_export_query)
            ppolicy_create_statement = cursor.fetchone()[0]

            with open(ppolicy_export_path, 'w') as ppolicy_file:
                ppolicy_file.write(ppolicy_create_statement)

        # row access policies
        row_access_policies_query = f"SHOW ROW ACCESS POLICIES IN SCHEMA {db_name}.{schema_name}"
        cursor.execute(row_access_policies_query)
        rapolicies = cursor.fetchall()

        for rapolicy in rapolicies:
            rapolicy_name = rapolicy[1]
            rapolicy_export_path = os.path.join(policies_folder_path, rapolicy_name + ".sql")
            rapolicy_export_query = f"SELECT GET_DDL('POLICIES','{db_name}.{schema_name}.{rapolicy_name}')"
            cursor.execute(rapolicy_export_query)
            rapolicy_create_statement = cursor.fetchone()[0]

            with open(rapolicy_export_path, 'w') as rapolicy_file:
                rapolicy_file.write(rapolicy_create_statement)

        # session policies
        session_policies_query = f"SHOW SESSION POLICIES IN SCHEMA {db_name}.{schema_name}"
        cursor.execute(session_policies_query)
        spolicies = cursor.fetchall()

        for spolicy in spolicies:
            spolicy_name = spolicy[1]
            spolicy_export_path = os.path.join(policies_folder_path, spolicy_name + ".sql")
            spolicy_export_query = f"SELECT GET_DDL('POLICIES','{db_name}.{schema_name}.{spolicy_name}')"
            cursor.execute(spolicy_export_query)
            spolicy_create_statement = cursor.fetchone()[0]

            with open(spolicy_export_path, 'w') as spolicy_file:
                rapolicy_file.write(spolicy_create_statement)

        # Export Sequences
        sequences_query = f"SHOW SEQUENCES IN SCHEMA {db_name}.{schema_name}"
        cursor.execute(sequences_query)
        sequences = cursor.fetchall()

        for sequence in sequences:
            sequence_name = sequence[1]
            sequence_export_path = os.path.join(sequences_folder_path, sequence_name + ".sql")
            sequence_export_query = f"SELECT GET_DDL('SEQUENCES','{db_name}.{schema_name}.{sequence_name}')"
            cursor.execute(sequence_export_query)
            sequence_create_statement = cursor.fetchone()[0]

            with open(sequence_export_path, 'w') as sequence_file:
                sequence_file.write(sequence_create_statement)

        # # Export stored procedures
        # sp_query = f"select * from {db_name}.information_schema.procedures where procedure_schema = '{schema_name}'"
        # cursor.execute(sp_query)
        # stored_procedures = cursor.fetchall()
        #
        # for sp in stored_procedures:
        #     sp_name = sp[2]
        #     sp_arg = sp[4]
        #     sp_arg_substring = sp_arg.replace("W_DB_NAME ", "")
        #     sp_export_path = os.path.join(stored_procedures_folder_path, sp_name + ".sql")
        #     sp_export_query = f"SELECT GET_DDL('PROCEDURE', '{db_name}.{schema_name}.{sp_name}{sp_arg_substring}')"
        #     cursor.execute(sp_export_query)
        #     sp_create_statement = cursor.fetchone()[0]
        #
        #     with open(sp_export_path, 'w') as sp_file:
        #         sp_file.write(sp_create_statement)

        # Export streams
        streams_query = f"SHOW STREAMS IN SCHEMA {db_name}.{schema_name};"
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

        # Export Tags
        tags_query = f"SHOW TAGS IN SCHEMA {db_name}.{schema_name}"
        cursor.execute(tags_query)
        tags = cursor.fetchall()

        for tag in tags:
            tag_name = tag[1]
            tag_export_path = os.path.join(tags_folder_path, tag_name + ".sql")
            tag_export_query = f"SELECT GET_DDL('TAG','{db_name}.{schema_name}.{tag_name}')"
            cursor.execute(tag_export_query)
            tag_create_statement = cursor.fetchone()[0]

            with open(tag_export_path, 'w') as tag_file:
                tag_file.write(tag_create_statement)

        # Export tasks
        tasks_query = f"SHOW TASKS IN SCHEMA {db_name}.{schema_name};"
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

        # Export User Defined Functions

        functions_query = f"SHOW USER FUNCTIONS IN SCHEMA {db_name}.{schema_name};"
        cursor.execute(functions_query)
        functions = cursor.fetchall()

        for function in functions:
            function_name = function[8]
            delimit = ' RETURN'
            function_name = function_name.split(delimit, 1)[0]
            function_export_path = os.path.join(udf_folder_path, function_name + ".sql")
            function_export_query = f"SELECT GET_DDL('FUNCTION', '{db_name}.{schema_name}.{function_name}')"
            cursor.execute(function_export_query)
            function_create_statement = cursor.fetchone()[0]

            with open(function_export_path, 'w') as function_file:
                function_file.write(function_create_statement)

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


# Close the cursor and connection
cursor.close()
conn.close()

from datetime import datetime
now = datetime.now()
current_time = now.strftime("%H:%M:%S")
print("End Time =", current_time)