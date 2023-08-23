CREATE OR REPLACE PROCEDURE "CREATE_DYNAMIC_TABLES_WITH_MASKING_POLICIES"("W_DB_NAME" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
try {
   // var schemaName = W_SCHEMA_NAME;
    var dbName = W_DB_NAME;
   
    var R2 = snowflake.createStatement({sqlText: `use ROLE MASKING_ADMIN`}).execute();
    var R3 = snowflake.createStatement({sqlText: `use WAREHOUSE SYSADMIN`}).execute();
    var R = snowflake.createStatement({sqlText: `use DATABASE ${dbName}`}).execute();
  
    var sql_command_tables = "show tables like ''%_BT'' in DATABASE  ADVENTUREWORKS"; 
    //var sql_command_tables = "SELECT DISTINCT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ''" + schemaName + "'' AND TABLE_TYPE = ''BASE TABLE'' AND TABLE_NAME like ''%_BT''";
    var tables = snowflake.createStatement({sqlText: sql_command_tables}).execute();
    var results = []; 
    while (tables.next()) {
        var tableName = tables.getColumnValue(2);
        var schemaName = tables.getColumnValue(4);
        var kind = tables.getColumnValue(5);
        if(kind === ''TABLE'') {
              var R = snowflake.createStatement({sqlText: `use SCHEMA ${schemaName}`}).execute();
              
            var newTableName = tableName.replace(''_BT'', '''')
            
            var sql_command = "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ''" + schemaName + "'' AND TABLE_NAME = ''" + tableName + "''";            
            var statement1 = snowflake.createStatement({sqlText: sql_command});
            var resultSet1 = statement1.execute();
            
            // Map data types to corresponding masking policies
            var maskingPolicies = {
                ''STRING'': `${dbName}_MASKING_POLICY_STRING`,
                ''TEXT'': `${dbName}_MASKING_POLICY_STRING`,
                ''VARCHAR'': `${dbName}_MASKING_POLICY_STRING`,
                ''CHAR'': `${dbName}_MASKING_POLICY_STRING`,
                ''DATE'': `${dbName}_MASKING_POLICY_DATE`,
                ''TIMESTAMP'': `${dbName}_MASKING_POLICY_TIMESTAMP`,
                ''INT'': `${dbName}_MASKING_POLICY_NUMERIC`,
                ''NUMBER'': `${dbName}_MASKING_POLICY_NUMERIC`,
                ''FLOAT'': `${dbName}_MASKING_POLICY_NUMERIC`,
                ''BOOLEAN'': `${dbName}_MASKING_POLICY_BOOLEAN`,
                ''TIMESTAMP_NTZ'': `${dbName}_MASKING_POLICY_TIMESTAMP_NTZ`,
                // Add more data types and corresponding policies as needed
            };
            
            var dynamicColumnMasking = [];
            var dynamicViewColumns = [];
            
            while (resultSet1.next()) {
                var columnName = resultSet1.getColumnValue(2);
                var dataType = resultSet1.getColumnValue(3);
                
                var snowflakeMaskingPolicy = maskingPolicies[dataType];           
                if(snowflakeMaskingPolicy) dynamicColumnMasking.push(`COLUMN ${columnName} SET MASKING POLICY ${dbName}.PUBLIC.${snowflakeMaskingPolicy}`);            
                dynamicViewColumns.push(`${columnName}`);  
            }
            
            if (dynamicViewColumns.length > 0) {        
                var dynamicViewColumnsDefinition = dynamicViewColumns.join('', '');
                var createTableCommand = `CREATE OR REPLACE SECURE VIEW ${schemaName}.${newTableName} as
                       select ${dynamicViewColumnsDefinition}
                       from ${schemaName}.${tableName}                
                `;
                results.push(createTableCommand);
                var statement2 = snowflake.createStatement({sqlText: createTableCommand});
                statement2.execute();
            }
            for (let mPolicy of dynamicColumnMasking) {
                    var alterStatement = `ALTER VIEW ${schemaName}.${newTableName} MODIFY ${mPolicy}`;
                    results.push(alterStatement);
                   var alterStatementResult = snowflake.createStatement({sqlText: alterStatement}).execute() ;
            }
        }
    }
    results.push("Masking policies added successfully.")    
    return results.join(''; '');
} catch (err) {
    return "Error: " + err;
}
';