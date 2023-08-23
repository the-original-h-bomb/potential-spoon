create or replace schema PUBLIC;

create or replace tag CRIMSON_EDIT COMMENT='Edit Access to System Crimson'
;
create or replace tag CRIMSON_READONLY COMMENT='Readonly Access to System Crimson'
;
create or replace tag GROUP_A_EDIT COMMENT='Edit Access to Group A Workgroup'
;
create or replace tag GROUP_A_READONLY COMMENT='Readonly Access to Group A Workgroup'
;
create or replace TABLE DEFAULT_ROLE_PERMISSIONS_ASSIGNED (
	LIKE_MATCHES VARCHAR(16777216),
	DB_PERMISSIONS ARRAY,
	SCHEMA_PERMISSIONS ARRAY,
	ADDED_DATE TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP(),
	PUBLIC_ONLY BOOLEAN
)COMMENT='Local to store the mappings of what each role naming conventions permissions should be set to'
;
create or replace TABLE TAGS_ASSIGNED_PERMISSIONS (
	TAG_NAME VARCHAR(16777216),
	DB_PERMISSIONS ARRAY,
	SCHEMA_PERMISSIONS ARRAY,
	ADDED_DATE TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP()
)COMMENT='Local to store the mappings of what each tags permissions shouold be set to'
;
create or replace view TAG_ASSIGNMENTS(
	TAG_NAME,
	ROLE_LIST,
	DB_LIST,
	SCHEMA_LIST
) as Select distinct t.tag_name, role_info.role_list, db_info.db_list,  schema_info.schema_list
  From 
     SNOWFLAKE.ACCOUNT_USAGE.TAGS t
     full outer join
      (select ARRAY_AGG(object_name) WITHIN GROUP (ORDER BY object_name ASC) as role_list,  tag_name as role_tag_name 
          from SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES
          where domain = 'ROLE'
          group by  tag_name
      ) role_info on t.tag_name = role_info.role_tag_name
     FULL outer join (select ARRAY_AGG(object_name) WITHIN GROUP (ORDER BY object_name ASC) as db_list,  tag_name as db_tag_name 
        from SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES
        where domain = 'DATABASE'
        group by  tag_name
      ) as db_info on db_info.db_tag_name = t.tag_name
      FULL  outer join (select ARRAY_AGG( distinct concat(object_database,'.',object_name)) WITHIN GROUP (ORDER BY  concat(object_database,'.',object_name) ASC) as schema_list,   tag_name 
          from SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES
          where domain = 'SCHEMA'
          group by tag_name
      ) schema_info on schema_info.tag_name = t.tag_name
      order by tag_name;
create or replace view TAG_PERMISSIONS_ROWS(
	TAG_NAME,
	VALUE
) as 
SELECT p.tag_name, f.value
FROM TAGS.PUBLIC.tags_assigned_permissions p,
   lateral flatten(input => p.schema_permissions) f;
create or replace view TAG_SECURITY_SHOULD_BE(
	TAG_NAME,
	ROLE_NEEDED,
	SCHEMA_NEEDED,
	VALUE
) as 
 select p.tag_name, r.value as role_needed, s.value as schema_needed, tpr.value 
     from TAGS.PUBLIC.tag_assignments p
      left outer join TAGS.PUBLIC.tag_permissions_rows tpr on tpr.tag_name = p.tag_name,
      lateral flatten(input => p.role_list) r,     
  lateral flatten(input => p.schema_list) s;
CREATE OR REPLACE PROCEDURE "GRANT_ACCESS_WITH_PRIVILEGES_IF_ROLES_MATCH"()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
  // Set the desired tag naming convention.
  var stgTagNamingConvention = ''_STG'';



    const rolePermissions = [];
    var rolePermissionsQuery = snowflake.execute( { sqlText: "Select LIKE_MATCHES, ifNULL(SCHEMA_PERMISSIONS, []) as SCHEMA_PERMISSIONS , PUBLIC_ONLY from TAGS.PUBLIC.DEFAULT_ROLE_PERMISSIONS_ASSIGNED " } ); 
    while (rolePermissionsQuery.next()) {
        var rolePermissionsQueryInform = { likeMatches: rolePermissionsQuery.getColumnValue(1), schemaPermissions: rolePermissionsQuery.getColumnValue(2), publicFolderOnly:rolePermissionsQuery.getColumnValue(3) };       
        rolePermissions.push(rolePermissionsQueryInform);
    }
 
   
 
  // Function to grant privileges for a specific entity (DATABASE or SCHEMA).
  function grantPrivileges(entityType, entityName, privileges, roleName) {
 
    for (var k = 0; k < privileges.length; k++) {
      var privilege = privileges[k];
      var grantStatement = "GRANT " + privilege + " ON " + entityType + " " + entityName + " TO ROLE " + roleName;
      if (privilege === ''OWNERSHIP'') {
        grantStatement += " REVOKE CURRENT GRANTS";
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
  
    const rolesToAdd = new Set();
    const needsDbUsageForRole = new Set();
  // Get the list of databases in the account.
  var databases = snowflake.execute( { sqlText: "select DATABASE_NAME from snowflake.information_schema.databases where DATABASE_NAME not like ''SNOWFLAKE%'' " } );
  
  // Loop through the databases.
  
  while(databases.next()) {
    var databaseName = databases.getColumnValue(1);
    const dbProductionRole = !databaseName.endsWith(stgTagNamingConvention);
  
    
    // Get the list of schemas in the current database.
    var schemas = snowflake.execute( { sqlText: "select SCHEMA_NAME from " + databaseName + ".information_schema.schemata where SCHEMA_NAME != ''INFORMATION_SCHEMA'' " } );
    
    // Loop through the schemas.
    while(schemas.next()) {
        var schemaName = schemas.getColumnValue(1);        
        const dbSchemaRole= !databaseName.endsWith(stgTagNamingConvention); // instance is determined by DB name   
   
    for(let permission of rolePermissions) {       
            var matchingRolesQuery = snowflake.execute( { sqlText: "show roles like ''" + databaseName + "%" + permission.likeMatches + "%''" } );
            
            while(matchingRolesQuery.next()) {
            return JSON.stringify(matchingRolesQuery);
                const roleName = matchingRolesQuery.getColumnValue(2);
                const roleProductionInstance = !roleName.endsWith(stgTagNamingConvention);
                if( (permission.publicFolderOnly && schemaName === ''PUBLIC'') || !permission.publicFolderOnly)
                    if(dbProductionRole === roleProductionInstance) {
                        rolesToAdd.add({ databaseName: databaseName, schemaName: schemaName, roleName: roleName, schemaPermissions: permission.schemaPermissions});
                        needsDbUsageForRole.add({databaseName: databaseName, roleName: roleName});
                }
            }

        }
      }
    }
    
  if(rolesToAdd.size > 0) {      
      for (const privilege of rolesToAdd) {
           grantPrivileges(''SCHEMA'', privilege.databaseName + "." + privilege.schemaName, privilege.schemaPermissions, privilege.roleName);
      }
  }
  if(needsDbUsageForRole.size > 0) {      
      for (const privilege of needsDbUsageForRole) {
           grantPrivileges(''DATABASE'', privilege.databaseName, ["USAGE"], privilege.roleName);
      }
  }
  // return JSON.stringify(sTag) ;
  return ''Access granting process completed successfully.'';
';
CREATE OR REPLACE PROCEDURE "GRANT_ACCESS_WITH_PRIVILEGES_IF_TAGS_MATCH_V2"()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
  // Set the desired tag naming convention.
  var stgTagNamingConvention = ''_STG'';

 const tags = [];
  var tagsQuery = snowflake.execute( { sqlText: "Select TAG_NAME, ifNULL(DB_PERMISSIONS, []) as DB_PERMISSIONS, ifNULL(SCHEMA_PERMISSIONS, []) as SCHEMA_PERMISSIONS from TAGS.PUBLIC.TAGS_ASSIGNED_PERMISSIONS " } ); 
    while (tagsQuery.next()) {
        var tagInform = { tagName: tagsQuery.getColumnValue(1), dbPermissions: tagsQuery.getColumnValue(2),  schemaPermissions: tagsQuery.getColumnValue(3)};       
        tags.push(tagInform);
    }
 
    var rolesQuery = snowflake.execute( { sqlText: "show roles" } );

    const roles = [];
    // Fetch all the rows and store them in the array
    while (rolesQuery.next()) {
        var roleName = rolesQuery.getColumnValue(2);
        const rolesTages = [];
        for (let t of tags) {        
            if(t.dbPermissions.length > 0 || t.schemaPermissions.length > 0) {
                var roleHasTag = snowflake.execute( { sqlText: "SELECT ifNULL(SYSTEM$GET_TAG(''" + t.tagName + "'',''" + roleName + "'', ''role''), ''-1'')"} );
                if (roleHasTag.next()) {       
                    const tagValue = roleHasTag.getColumnValueAsString(1)
                     if( tagValue !== ''-1'') {
                         const roleTagInfo = {tagName: t.tagName, tagValue: tagValue};
                         rolesTages.push(roleTagInfo);
                     }
                }
             }
        }        
        roles.push({roleName, rolesTages, productionRole: !roleName.endsWith(stgTagNamingConvention)});
    }
  
 
  // Function to grant privileges for a specific entity (DATABASE or SCHEMA).
  function grantPrivileges(entityType, entityName, privileges, roleName) {
 
    for (var k = 0; k < privileges.length; k++) {
      var privilege = privileges[k];
      var grantStatement = "GRANT " + privilege + " ON " + entityType + " " + entityName + " TO ROLE " + roleName;
     // if (privilege.grantOption) {
     //   grantStatement += " WITH GRANT OPTION";
     // }
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
  var databases = snowflake.execute( { sqlText: "select DATABASE_NAME from snowflake.information_schema.databases where DATABASE_NAME not like ''SNOWFLAKE%'' " } );
  
  // Loop through the databases.
  while(databases.next()) {
    var databaseName = databases.getColumnValue(1);
    const dbProductionRole = !databaseName.endsWith(stgTagNamingConvention);
    const databaseTages = [];
    for (let t of tags) {
        if(t.dbPermissions.length > 0 ) {
            var dbHasTag = snowflake.execute( { sqlText: "SELECT ifNULL(SYSTEM$GET_TAG(''" + t.tagName + "'',''" + databaseName + "'', ''database''), ''-1'')"} );
            if (dbHasTag.next()) {       
                const tagValue = dbHasTag.getColumnValueAsString(1)
                if( tagValue !== ''-1'') {
                    const dbTagInfo = {tagName: t.tagName, tagValue: tagValue};
                    databaseTages.push(dbTagInfo);
                }
            }
        }
    }        
    
    // Get the list of schemas in the current database.
    var schemas = snowflake.execute( { sqlText: "select SCHEMA_NAME from " + databaseName + ".information_schema.schemata where SCHEMA_NAME != ''INFORMATION_SCHEMA'' " } );
    
    // Loop through the schemas.
    while(schemas.next()) {
        var schemaName = schemas.getColumnValue(1);        
        const dbSchemaRole= !databaseName.endsWith(stgTagNamingConvention); // instance is determined by DB name
        const schemaTages = [];
        for (let t of tags) {
            if(t.schemaPermissions.length > 0 ) {
                var schemaHasTag = snowflake.execute( { sqlText: "SELECT ifNULL(SYSTEM$GET_TAG(''" + t.tagName + "'',''"+ databaseName + "." + schemaName + "'', ''schema''), ''-1'')"} );
                if (schemaHasTag.next()) {       
                    const tagValue = schemaHasTag.getColumnValueAsString(1)
                    if( tagValue !== ''-1'') {
                        const schemaTagInfo = {tagName: t.tagName, tagValue: tagValue, schemaPermissions: t.schemaPermissions, dbPermissions: t.dbPermissions};
                        schemaTages.push(schemaTagInfo);
                    }
                }
            }
        }   
        //if(schemaName === ''INTERNAL'' && databaseName === ''GROUP_A'')
        //    return JSON.stringify(schemaTages) ;
        const needsDbUsageForRole = new Set();
         for (let sTag of schemaTages) {
            const matchesFound = roles.filter(roleRow => roleRow.rolesTages.find(row => row.tagName === sTag.tagName));
             for (let match of matchesFound) {
                // Grant privileges for the schema.
                if(match.roleName !== ''ACCOUNTADMIN'') {
                    if(match.productionRole === dbProductionRole) {                     
                        grantPrivileges(''SCHEMA'', databaseName + "." + schemaName, sTag.schemaPermissions, match.roleName);
                        needsDbUsageForRole.add(match.roleName);
                    }
                }
             }
            
         }


        if(needsDbUsageForRole.size > 0) {
            for (const dbRole of needsDbUsageForRole) {
                grantPrivileges(''DATABASE'', databaseName, ["USAGE"], dbRole);
            }
        }
     
      }
    }
    
  
  // return JSON.stringify(sTag) ;
  return ''Access granting process completed successfully.'';
';