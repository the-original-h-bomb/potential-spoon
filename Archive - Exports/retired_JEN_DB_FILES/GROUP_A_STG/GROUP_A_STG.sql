create or replace database GROUP_A_STG;

create or replace schema INTERNAL;

create or replace tag GROUP_A_EDIT COMMENT='Edit Access to Group A tag'
;
create or replace tag STG_GROUP_A_EDIT COMMENT='Edit Access to Stage Group A tag'
;
create or replace tag STG_GROUP_A_VIEW COMMENT='View Access to Stage Group A tag'
;
alter schema INTERNAL set tag TAGS.PUBLIC.GROUP_A_EDIT='STG';

create or replace schema PUBLIC;

create or replace TABLE INDIANA_CERTIFIED_DIVERSITY_VENDORS (
	COMPANY_NAME VARCHAR(16777216),
	DBA VARCHAR(16777216),
	UNSPSC VARCHAR(16777216),
	UNSPSC_DESCRIPTION VARCHAR(16777216),
	FIRST_NAME VARCHAR(16777216),
	LASTNAME VARCHAR(16777216),
	MAILING_ADDRESS_1 VARCHAR(16777216),
	MAILING_ADDRESS_2 VARCHAR(16777216),
	CITY VARCHAR(16777216),
	STATE VARCHAR(16777216),
	ZIP_CODE VARCHAR(16777216),
	APPLICATION_TYPE VARCHAR(16777216),
	ETHNIC_GROUP VARCHAR(16777216),
	CERTIFICATION_DATE DATE,
	EXPIRATION_DTE DATE,
	BIDDER_ID VARCHAR(16777216),
	EMAIL_ID VARCHAR(16777216),
	PHONE VARCHAR(16777216),
	APPLICATION_STATUS VARCHAR(16777216),
	COMPANY_NAME_UPPER VARCHAR(16777216)
);
create or replace TABLE INDIANA_CERTIFIED_DIVERSITY_VENDORS2 (
	COMPANY_NAME VARCHAR(16777216),
	DBA VARCHAR(16777216),
	UNSPSC VARCHAR(16777216),
	UNSPSC_DESCRIPTION VARCHAR(16777216),
	FIRST_NAME VARCHAR(16777216),
	LASTNAME VARCHAR(16777216),
	MAILING_ADDRESS_1 VARCHAR(16777216),
	MAILING_ADDRESS_2 VARCHAR(16777216),
	CITY VARCHAR(16777216),
	STATE VARCHAR(16777216),
	ZIP_CODE VARCHAR(16777216),
	APPLICATION_TYPE VARCHAR(16777216),
	ETHNIC_GROUP VARCHAR(16777216),
	CERTIFICATION_DATE DATE,
	EXPIRATION_DTE DATE,
	BIDDER_ID VARCHAR(16777216),
	EMAIL_ID VARCHAR(16777216),
	PHONE VARCHAR(16777216),
	APPLICATION_STATUS VARCHAR(16777216),
	COMPANY_NAME_UPPER VARCHAR(16777216)
);
CREATE OR REPLACE PROCEDURE "GRANT_ACCESS_WITH_PRIVILEGES_IF_TAGS_MATCH"()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
  // Set the desired tag naming convention.
  var desiredTagNamingConvention = ''_VIEW'';
  
  // Define the set of privileges for databases and schemas.
  var databasePrivileges = [
    { type: ''USAGE'', grantOption: false },
  //  { type: ''SELECT'', grantOption: true },
    // Add more database privileges as needed.
  ];
  
  var schemaPrivileges = [
    { type: ''USAGE'', grantOption: false },
  //  { type: ''SELECT'', grantOption: true },
   // { type: ''INSERT'', grantOption: false },
    // Add more schema privileges as needed.
  ];
  
  // Function to grant privileges for a specific entity (DATABASE or SCHEMA).
  function grantPrivileges(entityType, entityName, privileges, roleName) {
    for (var k = 0; k < privileges.length; k++) {
      var privilege = privileges[k];
      var grantStatement = "GRANT " + privilege.type + " ON " + entityType + " " + entityName + " TO ROLE " + roleName;
      if (privilege.grantOption) {
        grantStatement += " WITH GRANT OPTION";
      }
      snowflake.execute( { sqlText: grantStatement } );
    }
  }

   // Function to grant privileges for a specific entity (DATABASE or SCHEMA).
  function revokePrivileges(entityType, entityName, privileges, roleName) {
    for (var k = 0; k < privileges.length; k++) {
      var privilege = privileges[k];
      var grantStatement = "REVOKE " + privilege.type + " ON " + entityType + " " + entityName + " FROM  ROLE " + roleName;  
      snowflake.execute( { sqlText: grantStatement } );
    }
  }
  
  // Get the list of databases in the account.
  var databases = snowflake.execute( { sqlText: "SHOW DATABASES" } );
  
  // Loop through the databases.
  while(databases.next()) {
    var databaseName = databases.getColumnValue(2);

    
    // Get the list of schemas in the current database.
    var schemas = snowflake.execute( { sqlText: "SHOW SCHEMAS IN DATABASE " + databaseName } );
    
    // Loop through the schemas.
    while(schemas.next()) {
      var schemaName = schemas.getColumnValue(2);
      
      // Check if the current schema has any tags.
      var tags = snowflake.execute( { sqlText: "SHOW TAGS IN SCHEMA " + databaseName + "." + schemaName } );
      
      // Loop through the tags.
      while (tags.next()) {
        var tagName = tags.getColumnValue(2);
        
        // Filter tags based on the desired naming convention.
        if (tagName.endsWith(desiredTagNamingConvention)) {
          // Get the list of roles in the account.
          var roles = snowflake.execute( { sqlText: "SHOW ROLES " } );
          
          // Loop through the roles.
        while (roles.next()) {
            var roleName = roles.getColumnValue(''name'');           
            var roleHasTag = snowflake.execute( { sqlText: "SELECT ifNULL(SYSTEM$GET_TAG(''" + tagName + "'',''" + roleName + "'', ''role''), ''-1'')"} );
            // If the role has the tag, grant the privileges to the database or schema.
             if (databaseName !== ''_SNOWFLAKE'') {            
                 if (roleHasTag.next()) {                 
                     if( roleHasTag.getColumnValueAsString(1) !== ''-1'') {
                          // Grant privileges for the database.             
                           grantPrivileges(''DATABASE'', databaseName, databasePrivileges, roleName);
                      
                      
                          // Grant privileges for the schema.
                          grantPrivileges(''SCHEMA'', databaseName + "." + schemaName, schemaPrivileges, roleName);
                      } else {
                          if(roleName !== ''ACCOUNTADMIN''){
                            revokePrivileges(''DATABASE'', databaseName, databasePrivileges, roleName);
                      
                            //Revoke privileges for the schema.
                            revokePrivileges(''SCHEMA'', databaseName + "." + schemaName, schemaPrivileges, roleName);
                        }
                    }
                 } else {
                      if(roleName !== ''ACCOUNTADMIN''){
                        revokePrivileges(''DATABASE'', databaseName, databasePrivileges, roleName);
                  
                        //Revoke privileges for the schema.
                        revokePrivileges(''SCHEMA'', databaseName + "." + schemaName, schemaPrivileges, roleName);
                    }
                }
            }
          }
        }
      }
    }
  }
  
  
  
  return ''Access granting process completed successfully.'';
';
alter schema PUBLIC set tag TAGS.PUBLIC.GROUP_A_EDIT='STG', TAGS.PUBLIC.GROUP_A_READONLY='STG';
alter database GROUP_A_STG set tag GROUP_A.PUBLIC.INSTANCE='stg';
