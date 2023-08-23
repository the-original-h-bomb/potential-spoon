CREATE OR REPLACE PROCEDURE "CREATE_DYNAMIC_POLICIES"("W_DB_NAME" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '

   // var schemaName = W_SCHEMA_NAME;
    var dbName = W_DB_NAME;
    const policyNeeded = `${dbName}_READONLY`
    let query = '''';
    const results = [];
    try {
        var R2 = snowflake.createStatement({sqlText: `use ROLE MASKING_ADMIN`}).execute();
        var R3 = snowflake.createStatement({sqlText: `use WAREHOUSE SYSADMIN`}).execute();
        var R = snowflake.createStatement({sqlText: `use DATABASE ${dbName}`}).execute();
    } catch (err) {
        results.push(` Error setting environment:  ${err}`);
        return results.join(''; '');
    }
   query = `CREATE OR REPLACE MASKING POLICY ${dbName}_MASKING_POLICY_STRING AS
            (val string) RETURNS string ->
            CASE
              WHEN IS_ROLE_IN_SESSION(''${policyNeeded}'') then val
              ELSE ''*******''
            END`;
     try {
         snowflake.execute({ sqlText: query });
    } catch (err) {
        results.push(`${query}; Error:  ${err}`);
        return results.join(''; '');
    }

     query = `CREATE OR REPLACE MASKING POLICY ${dbName}_MASKING_POLICY_DATE AS
        (val DATE) RETURNS DATE ->
        CASE
          WHEN IS_ROLE_IN_SESSION(''${policyNeeded}'') then val
          ELSE NULL
        END`;
     try {
         snowflake.execute({ sqlText: query });
    } catch (err) {
        results.push(`${query}; Error:  ${err}`);
        return results.join(''; '');
    }

 

     query = `CREATE OR REPLACE MASKING POLICY ${dbName}_MASKING_POLICY_TIMESTAMP AS
        (val TIMESTAMP) RETURNS TIMESTAMP ->
        CASE
          WHEN IS_ROLE_IN_SESSION(''${policyNeeded}'') then val
          ELSE NULL
        END`;
     try {
         snowflake.execute({ sqlText: query });
    } catch (err) {
        results.push(`${query}; Error:  ${err}`);
        return results.join(''; '');
    }
 

 
    query = `CREATE OR REPLACE MASKING POLICY ${dbName}_MASKING_POLICY_TIMESTAMP_NTZ AS
        (val TIMESTAMP_NTZ) RETURNS TIMESTAMP_NTZ ->
        CASE
          WHEN IS_ROLE_IN_SESSION(''${policyNeeded}'') then val
          ELSE NULL
        END`;
     try {
         snowflake.execute({ sqlText: query });
    } catch (err) {
        results.push(`${query}; Error:  ${err}`);
        return results.join(''; '');
    }
 
     
    query = `CREATE OR REPLACE MASKING POLICY ${dbName}_MASKING_POLICY_NUMERIC AS
        (val NUMBER) RETURNS NUMBER ->
        CASE
          WHEN IS_ROLE_IN_SESSION(''${policyNeeded}'') then val
          ELSE NULL
        END`;
     try {
         snowflake.execute({ sqlText: query });
    } catch (err) {
        results.push(`${query}; Error:  ${err}`);
        return results.join(''; '');
    }
 
 
    query = `CREATE OR REPLACE MASKING POLICY ${dbName}_MASKING_POLICY_BOOLEAN AS
            (val BOOLEAN) RETURNS BOOLEAN ->
            CASE
              WHEN IS_ROLE_IN_SESSION(''${policyNeeded}'') then val
              ELSE NULL
            END`;
     try {
         snowflake.execute({ sqlText: query });
    } catch (err) {
        results.push(`${query}; Error:  ${err}`);
        return results.join(''; '');
    }

    
    return `Success; Result: ` + results.join(''; '');


';