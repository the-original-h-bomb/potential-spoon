create or replace schema PUBLIC;

CREATE OR REPLACE PROCEDURE "CREATE_DATABASE_SCHEMA_AND_ROLES"("W_NAME" VARCHAR(16777216), "W_INSTANCE" VARCHAR(10), "W_SYSTEM_ACCOUNT" BOOLEAN, "W_ADDITIONAL_SCHEMAS" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
// JavaScript source code
const workGroupName = W_NAME;

const devSuffix = ''_DEV'';
const instance = ( W_INSTANCE === null || typeof W_INSTANCE === ''undefined'' )? '''' : (W_INSTANCE.startsWith("_") ? `${W_INSTANCE}`.toUpperCase() : `_${W_INSTANCE.toUpperCase()}`);
const instances = [instance];
const createRolesOnly = false; //W_CREATE_ROLES_ONLY;
const isSystemAccount = W_SYSTEM_ACCOUNT;
const maskingAdminRoleName = ''MASKING_ADMIN'';

const roleAdminSuffix = ''_ADMIN'';
const roleEditorSuffix = ''_EDITOR'';
const roleReadOnlySuffix = ''_READONLY'';

const rolePrefix = '''';

const systemTag = ''SYSTEM'';
const warehousePrefix = ''WH_'';
const warehouseSize = ''X-Small'';
const warehouseAutoSuspend = 300;
const warehouseAutoResume = ''TRUE'';
const warehouseMinClusterCount = 1;
const warehouseMaxClusterCount = 2;
const warehouseType = ''STANDARD'';
const warehouseScalingPolicy = ''ECONOMY'';

const additionalSchemaNames = !isSystemAccount ? [''PUBLIC'', ''INTERNAL''] : [''PUBLIC''];
const excludeReadonlyAccessToSchemas = [''INTERNAL''];
let query = '''';
const results = [];
if (!W_ADDITIONAL_SCHEMAS === false) {
    additionalSchemaNames.push(...W_ADDITIONAL_SCHEMAS)
}

try {
    snowflake.execute({ sqlText: `USE WAREHOUSE SYSADMIN` });
    results.push(`USE WAREHOUSE SYSADMIN`);
} catch (err) {
    results.push(`USE WAREHOUSE SYSADMIN; Error : ${err}`);
    return results.join(''; \\r\\n'');
}

// CHANGE ROLE 
try {
    snowflake.execute({ sqlText: `USE ROLE SYSADMIN` });
    results.push(`USE ROLE SYSADMIN`);
} catch (err) {
    results.push(`USE ROLE SYSADMIN; Error : ${err}`);
    return results.join(''; \\r\\n'');
}

try {
    snowflake.execute({ sqlText: `USE WAREHOUSE SYSADMIN` });
    results.push(`USE WAREHOUSE SYSADMIN`);
} catch (err) {  
    results.push(`USE WAREHOUSE SYSADMIN; Error : ${err}`);
    return results.join(''; \\r\\n'');
}





    // CHANGE ROLE 
    try {
        snowflake.execute({ sqlText: `USE ROLE SYSADMIN` });
        results.push(`USE ROLE SYSADMIN`);
    } catch (err) {
        results.push(`USE ROLE SYSADMIN; Error : ${err}`);
        return results.join(''; \\r\\n'');
    }


    // Warehouse creation
    const warehouseName = `${warehousePrefix}${workGroupName}`;
    const createWarehouseSQL = `CREATE WAREHOUSE IF NOT EXISTS ${warehouseName}
            WAREHOUSE_SIZE = "${warehouseSize}"
            AUTO_SUSPEND = ${warehouseAutoSuspend}
            AUTO_RESUME = ${warehouseAutoResume}
            MIN_CLUSTER_COUNT = ${warehouseMinClusterCount}
            MAX_CLUSTER_COUNT  = ${warehouseMaxClusterCount}
            SCALING_POLICY = ${warehouseScalingPolicy}
            WAREHOUSE_TYPE = ${warehouseType} `;           
    try {
        if (!createRolesOnly) {
            snowflake.execute({ sqlText: createWarehouseSQL });
            results.push(createWarehouseSQL);
        }
    } catch (err) {
        results.push(`Error creating WAREHOUSE: ${createWarehouseSQL} - ${err}`);
        return results.join(''; \\r\\n'');
    }


    // CHANGE ROLE 
    try {
        snowflake.execute({ sqlText: `USE ROLE SYSADMIN` });
        results.push(`USE ROLE SYSADMIN`);
    } catch (err) {
        results.push(`USE ROLE SYSADMIN; Error USE ROLE SYSADMIN: ${err}`);
        return results.join(''; \\r\\n'');
    }

    try {
        snowflake.execute({ sqlText: `USE WAREHOUSE SYSADMIN` });
        results.push(`USE WAREHOUSE SYSADMIN`);
    } catch (err) {
        results.push(`USE WAREHOUSE SYSADMIN; Error : ${err}`);
        return results.join(''; \\r\\n'');
    }

    const databaseName = `${workGroupName}${instance}`
    const createDatabaseSQL = `CREATE DATABASE IF NOT EXISTS ${databaseName}`;
    try {
        if (!createRolesOnly) snowflake.execute({ sqlText: createDatabaseSQL });
        results.push(createDatabaseSQL);
    } catch (err) {
        results.push(`${createDatabaseSQL}; Error creating DATABASE: ${err}`);
        return results.join(''; \\r\\n'');
    }


    // Needs to give USERADMIN USAGE and GRANT ROLE ACCESS
    try {
        if (!createRolesOnly) {
            snowflake.execute({ sqlText: `GRANT USAGE, CREATE DATABASE ROLE ON DATABASE ${databaseName} TO USERADMIN ` });
            results.push(`GRANT USAGE, CREATE DATABASE ROLE ON DATABASE ${databaseName} TO USERADMIN `);
        }
    } catch (err) {
        results.push(`GRANT USAGE, CREATE DATABASE ROLE ON DATABASE ${databaseName} TO USERADMIN ; Error creating DATABASE: ${err}`);
        return results.join(''; \\r\\n'');
    }



   


    // CHANGE ROLE change to security admin for permissions
    try {
        snowflake.execute({ sqlText: `USE ROLE USERADMIN ` });
        results.push(`USE ROLE USERADMIN `);
    } catch (err) {
        results.push(`USE ROLE USERADMIN; Error USE ROLE USERADMIN: ${err}`);
        return results.join(''; \\r\\n'');
    }


    const roleReadOnlyName = `${rolePrefix}${workGroupName}${roleReadOnlySuffix}`
    const createRoleSQL = `CREATE ROLE IF NOT EXISTS ${roleReadOnlyName}`;
    try {
        snowflake.execute({ sqlText: createRoleSQL });
        results.push(`${createRoleSQL}`);
    } catch (err) {
        results.push(`${createRoleSQL}; Error creating ROLE: ${err}`);
        return results.join(''; \\r\\n'');
    }


    const roleEditorName = `${rolePrefix}${workGroupName}${roleEditorSuffix}`
    const createEditorRoleSQL = `CREATE ROLE IF NOT EXISTS ${roleEditorName}`;
    try {
        snowflake.execute({ sqlText: createEditorRoleSQL });
        results.push(`${createEditorRoleSQL}`);
    } catch (err) {
        results.push(`${createEditorRoleSQL};Error creating Editor ROLE: ${err}`);
        return results.join(''; \\r\\n'');
    }
    
    const roleAdminName = `${rolePrefix}${workGroupName}${roleAdminSuffix}`
    const createAdminRoleSQL = `CREATE ROLE IF NOT EXISTS ${roleAdminName}`;
    try {
        snowflake.execute({ sqlText: createAdminRoleSQL });
        results.push(`${createAdminRoleSQL}`);
    } catch (err) {
        results.push(`${createAdminRoleSQL};Error creating Admin ROLE: ${err}`);
        return results.join(''; \\r\\n'');
    }




    // CHANGE ROLE change to security admin for permissions
    try {
        snowflake.execute({ sqlText: `USE ROLE SECURITYADMIN ` });
        results.push(`USE ROLE SECURITYADMIN `);
    } catch (err) {
        results.push(`USE ROLE SECURITYADMIN; Error using administrative database: ${err} `);
        return results.join(''; \\r\\n'');
    }
    // assign READONLY to EDITOR role  
    let grantRoleSQL = `GRANT ROLE ${roleReadOnlyName} TO ROLE ${roleEditorName}`;
    try {
        snowflake.execute({ sqlText: grantRoleSQL });
        results.push(`${grantRoleSQL}`);
    } catch (err) {
        results.push(`${grantRoleSQL}; Error creating GRANT ROLE to Parent: ${err}`);
        return results.join(''; \\r\\n'');
    }
     // assign EDITOR to ADMIN role  
    grantRoleSQL = `GRANT ROLE ${roleEditorName} TO ROLE ${roleAdminName}`;
    try {
        snowflake.execute({ sqlText: grantRoleSQL });
        results.push(`${grantRoleSQL}`);
    } catch (err) {
        results.push(`${grantRoleSQL}; Error: ${err}`);
        return results.join(''; \\r\\n'');
    }
 	// assign READONLY to MASKING_EDITOR role  
    grantRoleSQL = `GRANT ROLE ${roleReadOnlyName} TO ROLE ${maskingAdminRoleName}`;
    try {
        snowflake.execute({ sqlText: grantRoleSQL });
        results.push(`${grantRoleSQL}`);
    } catch (err) {
        results.push(`${grantRoleSQL}; Error : ${err}`);
        return results.join(''; \\r\\n'');
    }

    
  

    /////////// START GRANTING ACCESS ///////////

    // assign editor role to use warehouse  
    query = `GRANT USAGE ON WAREHOUSE ${warehouseName} TO ROLE ${roleEditorName}`;
    try {
        if (!createRolesOnly) {
            snowflake.execute({ sqlText: query });  // can put back in if we check that the warehouse exists first
            results.push(`${query}`);
        }
    } catch (err) {
        results.push(`${query}; Error:  ${err}`);
        return results.join(''; \\r\\n'');
    }

    // assign readonly role to use IUF readonly warehouse 
    query = `GRANT USAGE ON WAREHOUSE WH_IUF TO ROLE ${roleReadOnlyName}`;
    try {
        if (!createRolesOnly) {
        snowflake.execute({ sqlText: query });  // can put back in if we check that the warehouse exists FIRST
        results.push(`${query}`);
       }
    } catch (err) {
        results.push(`${query}; Error:  ${err}`);
        return results.join(''; \\r\\n'');
    }


    const roles = [roleReadOnlyName, roleEditorName,roleAdminName]; //, 

    // DATABASE USAGE
    for (let role of roles) {
    
    	const revokeToDatabase = [];
	    revokeToDatabase.push(`REVOKE ALL PRIVILEGES ON DATABASE ${databaseName} FROM ROLE ${role}`);
	    revokeToDatabase.push(`REVOKE ALL PRIVILEGES ON FUTURE SCHEMAS IN DATABASE ${databaseName} FROM ROLE ${role}`);
	    revokeToDatabase.push(`REVOKE ALL PRIVILEGES ON ALL SCHEMAS IN DATABASE ${databaseName} FROM ROLE ${role}`);
	   
       for (let query of revokeToDatabase) {
       		try {
       			snowflake.execute({ sqlText: query });
                results.push(`${query}`);
            } catch (err) {
                results.push(`${query}; Error:  ${err}`);
                return results.join(''; \\r\\n'');
            }
        }
    
     	const grantToDatabase = [];
	    grantToDatabase.push(`GRANT USAGE ON DATABASE ${databaseName} TO ROLE ${role}`);
	    grantToDatabase.push(`GRANT USAGE ON FUTURE SCHEMAS IN DATABASE ${databaseName} TO ROLE ${role}`);
	    grantToDatabase.push(`GRANT USAGE ON ALL SCHEMAS IN DATABASE ${databaseName} TO ROLE ${role}`);
	   
	   	if ( role.includes(roleReadOnlySuffix)) {
		    if(isSystemAccount === false) {
	                grantToDatabase.push(`GRANT SELECT, REFERENCES ON FUTURE TABLES IN DATABASE ${databaseName} TO ROLE ${role}`);
               	}
                //grantToDatabase.push(`GRANT SELECT ON FUTURE DYNAMIC TABLES IN DATABASE ${databaseName} TO ROLE ${role}`);
                grantToDatabase.push(`GRANT USAGE ON  FUTURE FUNCTIONS IN DATABASE ${databaseName} TO ROLE ${role}`);
                grantToDatabase.push(`GRANT SELECT ON FUTURE MATERIALIZED VIEWS IN DATABASE ${databaseName} TO ROLE ${role}`);
                grantToDatabase.push(`GRANT SELECT ON FUTURE VIEWS IN DATABASE ${databaseName} TO ROLE ${role}`);
                grantToDatabase.push(`GRANT SELECT ON FUTURE EVENT TABLES IN DATABASE ${databaseName} TO ROLE ${role}`);
                grantToDatabase.push(`GRANT SELECT ON FUTURE EXTERNAL TABLES IN DATABASE ${databaseName} TO ROLE ${role}`);
	   }
	   
	   
	   if ( role.includes(roleEditorSuffix)) {
	   
	    	if(isSystemAccount === true) {
	               	grantToReadOnlyQueries.push(`GRANT SELECT, REFERENCES ON ALL TABLES IN DATABASE ${databaseName} TO ROLE ${role}`);
	                grantToReadOnlyQueries.push(`GRANT SELECT, REFERENCES ON FUTURE TABLES IN DATABASE ${databaseName} TO ROLE ${role}`);
               	}
               
                grantToDatabase.push(`GRANT REBUILD ON FUTURE TABLES IN DATABASE ${databaseName} TO ROLE ${role}`);
                grantToDatabase.push(`GRANT REFERENCES ON FUTURE VIEWS IN DATABASE ${databaseName} TO ROLE ${role}`);
                grantToDatabase.push(`GRANT TRUNCATE ON FUTURE TABLES IN DATABASE ${databaseName} TO ROLE ${role}`);
                grantToDatabase.push(`GRANT CREATE TEMPORARY TABLE ON FUTURE SCHEMAS IN DATABASE ${databaseName} TO ROLE ${role}`);
                grantToDatabase.push(`GRANT USAGE ON FUTURE SEQUENCES IN DATABASE ${databaseName} TO ROLE ${role}`);
                grantToDatabase.push(`GRANT CREATE PROCEDURE ON FUTURE SCHEMAS IN DATABASE ${databaseName} TO ROLE ${role}`);
                grantToDatabase.push(`GRANT USAGE ON FUTURE PROCEDURES IN DATABASE ${databaseName} TO ROLE ${role}`);
                grantToDatabase.push(`GRANT DELETE ON FUTURE TABLES IN DATABASE ${databaseName} TO ROLE ${role}`);
                grantToDatabase.push(`GRANT READ ON FUTURE STAGES IN DATABASE ${databaseName} TO ROLE ${role}`);
                grantToDatabase.push(`GRANT REFERENCES ON FUTURE MATERIALIZED VIEWS IN DATABASE ${databaseName} TO ROLE ${role}`);
                grantToDatabase.push(`GRANT CREATE VIEW ON FUTURE SCHEMAS IN DATABASE ${databaseName} TO ROLE ${role}`);
                grantToDatabase.push(`GRANT CREATE TABLE ON FUTURE SCHEMAS IN DATABASE ${databaseName} TO ROLE ${role}`);
                grantToDatabase.push(`GRANT INSERT ON FUTURE TABLES IN DATABASE ${databaseName} TO ROLE ${role}`);
                grantToDatabase.push(`GRANT UPDATE ON FUTURE TABLES IN DATABASE ${databaseName} TO ROLE ${role}`);
                grantToDatabase.push(`GRANT MONITOR ON FUTURE SCHEMAS IN DATABASE ${databaseName} TO ROLE ${role}`);
                grantToDatabase.push(`GRANT CREATE MATERIALIZED VIEW ON FUTURE SCHEMAS IN DATABASE ${databaseName} TO ROLE ${role}`);
                grantToDatabase.push(`GRANT CREATE FILE FORMAT ON FUTURE SCHEMAS IN DATABASE ${databaseName} TO ROLE ${role}`);                
                grantToDatabase.push(`GRANT CREATE FUNCTION ON FUTURE SCHEMAS IN DATABASE ${databaseName} TO ROLE ${role}`);                
                grantToDatabase.push(`GRANT CREATE SEQUENCE ON FUTURE SCHEMAS IN DATABASE ${databaseName} TO ROLE IDENTIFIER(''"${role}"'')`);
               
               grantToDatabase.push(`GRANT CREATE PROCEDURE ON ALL SCHEMAS IN DATABASE ${databaseName} TO ROLE ${role}`);
               grantToDatabase.push(`GRANT CREATE TEMPORARY TABLE ON ALL SCHEMAS IN DATABASE ${databaseName} TO ROLE ${role}`);
               grantToDatabase.push(`GRANT CREATE VIEW ON ALL SCHEMAS IN DATABASE ${databaseName} TO ROLE ${role}`);
               grantToDatabase.push(`GRANT CREATE TABLE ON ALL SCHEMAS IN DATABASE ${databaseName} TO ROLE ${role}`);
               grantToDatabase.push(`GRANT CREATE MATERIALIZED VIEW ON ALL SCHEMAS IN DATABASE ${databaseName} TO ROLE ${role}`);
               grantToDatabase.push(`GRANT CREATE FILE FORMAT ON ALL SCHEMAS IN DATABASE ${databaseName} TO ROLE ${role}`);                
               grantToDatabase.push(`GRANT CREATE FUNCTION ON ALL SCHEMAS IN DATABASE ${databaseName} TO ROLE ${role}`);                
               grantToDatabase.push(`GRANT CREATE SEQUENCE ON ALL SCHEMAS IN DATABASE ${databaseName} TO ROLE IDENTIFIER(''"${role}"'')`);


	   }
	   
	   
	   
	   if (role.includes(roleAdminSuffix)) {
              
              /* 
                 grantToDatabase.push(`GRANT OWNERSHIP ON ALL TABLES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS`);
                 grantToDatabase.push(`GRANT OWNERSHIP ON ALL MATERIALIZED VIEWS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS`);
                 grantToDatabase.push(`GRANT OWNERSHIP ON ALL PROCEDURES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS`);
                 grantToDatabase.push(`GRANT OWNERSHIP ON ALL VIEWS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS`);
                 grantToDatabase.push(`GRANT OWNERSHIP ON ALL SEQUENCES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS`);
                 grantToDatabase.push(`GRANT OWNERSHIP ON ALL FUNCTIONS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS`);
                 grantToDatabase.push(`GRANT OWNERSHIP ON FUTURE TABLES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS`);
                 grantToDatabase.push(`GRANT OWNERSHIP ON FUTURE EVENT TABLES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS`);
                 grantToDatabase.push(`GRANT OWNERSHIP ON  FUTURE MATERIALIZED VIEWS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS`);
                 grantToDatabase.push(`GRANT OWNERSHIP ON  FUTURE PROCEDURES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS`);
                 grantToDatabase.push(`GRANT OWNERSHIP ON  FUTURE VIEWS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS`);
                 grantToDatabase.push(`GRANT OWNERSHIP ON  FUTURE SEQUENCES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS`);
                 grantToDatabase.push(`GRANT OWNERSHIP ON  FUTURE FUNCTIONS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS`);
                 grantToDatabase.push(`GRANT OWNERSHIP ON  FUTURE FILE FORMATS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS`);
                grantToDatabase.push(`GRANT CREATE DYNAMIC TABLE ON FUTURE SCHEMAS IN DATABASE IDENTIFIER(''"${databaseName}"'') TO ROLE IDENTIFIER(''"${role}"'')`);
                */
                grantToDatabase.push(`GRANT CREATE DYNAMIC TABLE ON FUTURE SCHEMAS IN DATABASE ${databaseName}  TO ROLE IDENTIFIER(''"${role}"'')`);
                grantToDatabase.push(`GRANT CREATE EXTERNAL TABLE ON FUTURE SCHEMAS IN DATABASE ${databaseName}  TO ROLE ${role}`);
                grantToDatabase.push(`GRANT CREATE STREAM ON FUTURE SCHEMAS IN DATABASE ${databaseName} TO ROLE ${role}`);     
                grantToDatabase.push(`GRANT CREATE STAGE ON FUTURE SCHEMAS IN DATABASE ${databaseName}  TO ROLE IDENTIFIER(''"${role}"'')`);
               
                grantToDatabase.push(`GRANT CREATE DYNAMIC TABLE ON ALL SCHEMAS IN DATABASE  ${databaseName} TO ROLE IDENTIFIER(''"${role}"'')`);
                grantToDatabase.push(`GRANT CREATE PIPE ON ALL SCHEMAS IN DATABASE ${databaseName} TO ROLE ${role}`);           
                grantToDatabase.push(`GRANT CREATE STAGE ON ALL SCHEMAS IN DATABASE ${databaseName}  TO ROLE IDENTIFIER(''"${role}"'')`);
                grantToDatabase.push(`GRANT CREATE EXTERNAL TABLE ON ALL SCHEMAS IN DATABASE ${databaseName}  TO ROLE ${role}`);

               }
       for (let query of grantToDatabase) {
       		try {
       			snowflake.execute({ sqlText: query });
                results.push(`${query}`);
            } catch (err) {
                results.push(`${query}; Error:  ${err}`);
                return results.join(''; \\r\\n'');
            }
        }
    }

    const grantToMaskingAdminDatabase = [];
    grantToMaskingAdminDatabase.push(`GRANT USAGE ON DATABASE ${databaseName} TO ROLE ${maskingAdminRoleName}`);
    grantToMaskingAdminDatabase.push(`GRANT USAGE ON FUTURE SCHEMAS IN DATABASE ${databaseName} TO ROLE ${maskingAdminRoleName}`);
    grantToMaskingAdminDatabase.push(`GRANT REFERENCES ON FUTURE TABLES IN DATABASE ${databaseName} TO ROLE ${maskingAdminRoleName}`);
    grantToMaskingAdminDatabase.push(`GRANT REFERENCES ON FUTURE VIEWS IN DATABASE ${databaseName} TO ROLE ${maskingAdminRoleName}`);
    grantToMaskingAdminDatabase.push(`GRANT CREATE VIEW ON FUTURE SCHEMAS IN DATABASE ${databaseName} TO ROLE ${maskingAdminRoleName}`);
    grantToMaskingAdminDatabase.push(`GRANT CREATE MASKING POLICY ON FUTURE SCHEMAS IN DATABASE ${databaseName} TO ROLE ${maskingAdminRoleName}`);
    grantToMaskingAdminDatabase.push(`GRANT USAGE ON ALL SCHEMAS IN DATABASE ${databaseName} TO ROLE ${maskingAdminRoleName}`);
    grantToMaskingAdminDatabase.push(`GRANT SELECT, REFERENCES ON ALL TABLES IN DATABASE ${databaseName} TO ROLE ${maskingAdminRoleName}`);
    grantToMaskingAdminDatabase.push(`GRANT CREATE MASKING POLICY ON ALL SCHEMAS IN DATABASE ${databaseName} TO ROLE ${maskingAdminRoleName}`);
    grantToMaskingAdminDatabase.push(`GRANT CREATE VIEW ON ALL SCHEMAS IN DATABASE ${databaseName} TO ROLE ${maskingAdminRoleName}`);
   

    for (let query of grantToMaskingAdminDatabase) {
                try {
                    snowflake.execute({ sqlText: query });
                   results.push(`${query}`);
                } catch (err) {
                    results.push(`${query}; Error:  ${err}`);
                    return results.join(''; \\r\\n'');
                 }
     }
     
     
      // Schema creation
      // CHANGE ROLE 
    try {
        snowflake.execute({ sqlText: `USE ROLE SYSADMIN` });
        results.push(`USE ROLE SYSADMIN`);
    } catch (err) {
        results.push(`USE ROLE SYSADMIN; Error USE ROLE SYSADMIN: ${err}`);
        return results.join(''; \\r\\n'');
    }
    for (let schema of additionalSchemaNames) {
        const createSchemaSQL = `CREATE SCHEMA IF NOT EXISTS ${databaseName}.${schema}  WITH MANAGED ACCESS`;
        try {
            if (!createRolesOnly) {
                snowflake.execute({ sqlText: createSchemaSQL });
                results.push(`${createSchemaSQL}`);
            }
        } catch (err) {
            results.push(`${createSchemaSQL}; Error creating schema: ${err}`);
            return results.join(''; \\r\\n'');
        }
    }
    
    // CHANGE ROLE change to security admin for permissions
    try {
        snowflake.execute({ sqlText: `USE ROLE SECURITYADMIN ` });
        results.push(`USE ROLE SECURITYADMIN `);
    } catch (err) {
        results.push(`USE ROLE SECURITYADMIN; Error using administrative database: ${err} `);
        return results.join(''; \\r\\n'');
    }
    
   // return results.join(''; \\r\\n'');
     /*
    for (let schema of additionalSchemaNames) {

        const grantToMaskingAdmin = [];
        grantToMaskingAdmin.push(`GRANT USAGE ON SCHEMA ${databaseName}.${schema} TO ROLE ${maskingAdminRoleName}`);
        grantToMaskingAdmin.push(`GRANT SELECT, REFERENCES ON ALL TABLES IN SCHEMA ${databaseName}.${schema} TO ROLE ${maskingAdminRoleName}`);
        grantToMaskingAdmin.push(`GRANT CREATE MASKING POLICY ON SCHEMA ${databaseName}.${schema} TO ROLE ${maskingAdminRoleName}`);
        grantToMaskingAdmin.push(`GRANT CREATE VIEW ON SCHEMA ${databaseName}.${schema} TO ROLE ${maskingAdminRoleName}`);


        
         for (let query of grantToMaskingAdmin) {
                    try {
                        snowflake.execute({ sqlText: query });
                       results.push(`${query}`);
                    } catch (err) {
                        results.push(`${query}; Error:  ${err}`);
                        return results.join(''; \\r\\n'');
                    }
                }
      
        for (let role of roles) {
        	const revokeAllPriviges = [];
	    	revokeAllPriviges.push(`REVOKE ALL PRIVILEGES ON ALL ALERTS IN SCHEMA ${databaseName}.${schema}  FROM ROLE ${role}`);
	    	//revokeAllPriviges.push(`REVOKE ALL PRIVILEGES ON ALL DYNAMIC TABLES IN SCHEMA ${databaseName}.${schema}  FROM ROLE ${role}`);
	    	revokeAllPriviges.push(`REVOKE ALL PRIVILEGES ON ALL EVENT TABLES IN SCHEMA ${databaseName}.${schema}  FROM ROLE ${role}`);
	    	revokeAllPriviges.push(`REVOKE ALL PRIVILEGES ON ALL EXTERNAL TABLES IN SCHEMA ${databaseName}.${schema}  FROM ROLE ${role}`);
	   		revokeAllPriviges.push(`REVOKE ALL PRIVILEGES ON ALL FILE FORMATS IN SCHEMA ${databaseName}.${schema}  FROM ROLE ${role}`);
	  		revokeAllPriviges.push(`REVOKE ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA ${databaseName}.${schema}  FROM ROLE ${role}`);
	 		revokeAllPriviges.push(`REVOKE ALL PRIVILEGES ON ALL PROCEDURES IN SCHEMA ${databaseName}.${schema}  FROM ROLE ${role}`);
			revokeAllPriviges.push(`REVOKE ALL PRIVILEGES ON ALL SEQUENCES  IN SCHEMA ${databaseName}.${schema}  FROM ROLE ${role}`);
		
		 	// revokeAllPriviges.push(`REVOKE ALL PRIVILEGES ON ALL PIPES IN SCHEMA ${databaseName}.${schema}  FROM ROLE ${role}`);
	    	revokeAllPriviges.push(`REVOKE ALL PRIVILEGES ON ALL STAGES IN SCHEMA ${databaseName}.${schema}  FROM ROLE ${role}`);
	    	revokeAllPriviges.push(`REVOKE ALL PRIVILEGES ON ALL STREAMS IN SCHEMA ${databaseName}.${schema}  FROM ROLE ${role}`);
	   		revokeAllPriviges.push(`REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA ${databaseName}.${schema}  FROM ROLE ${role}`);
	  		revokeAllPriviges.push(`REVOKE ALL PRIVILEGES ON ALL TASKS IN SCHEMA ${databaseName}.${schema}  FROM ROLE ${role}`);
	 		revokeAllPriviges.push(`REVOKE ALL PRIVILEGES ON ALL VIEWS IN SCHEMA ${databaseName}.${schema}  FROM ROLE ${role}`);
			revokeAllPriviges.push(`REVOKE ALL PRIVILEGES ON ALL MATERIALIZED VIEWS IN SCHEMA ${databaseName}.${schema}  FROM ROLE ${role}`);
		//	revokeAllPriviges.push(`REVOKE ALL PRIVILEGES ON ALL TEMPORARY TABLES IN SCHEMA ${databaseName}.${schema}  FROM ROLE ${role}`);
		
		
			revokeAllPriviges.push(`REVOKE ALL PRIVILEGES ON FUTURE ALERTS IN SCHEMA ${databaseName}.${schema}  FROM ROLE ${role}`);
	    	//revokeAllPriviges.push(`REVOKE ALL PRIVILEGES ON FUTURE DYNAMIC TABLES IN SCHEMA ${databaseName}.${schema}  FROM ROLE ${role}`);
	    	revokeAllPriviges.push(`REVOKE ALL PRIVILEGES ON FUTURE EVENT TABLES IN SCHEMA ${databaseName}.${schema}  FROM ROLE ${role}`);
	    	revokeAllPriviges.push(`REVOKE ALL PRIVILEGES ON FUTURE EXTERNAL TABLES IN SCHEMA ${databaseName}.${schema}  FROM ROLE ${role}`);
	   		revokeAllPriviges.push(`REVOKE ALL PRIVILEGES ON FUTURE FILE FORMATS IN SCHEMA ${databaseName}.${schema}  FROM ROLE ${role}`);
	  		revokeAllPriviges.push(`REVOKE ALL PRIVILEGES ON FUTURE FUNCTIONS IN SCHEMA ${databaseName}.${schema}  FROM ROLE ${role}`);
	 		revokeAllPriviges.push(`REVOKE ALL PRIVILEGES ON FUTURE PROCEDURES IN SCHEMA ${databaseName}.${schema}  FROM ROLE ${role}`);
			revokeAllPriviges.push(`REVOKE ALL PRIVILEGES ON FUTURE SEQUENCES  IN SCHEMA ${databaseName}.${schema}  FROM ROLE ${role}`);		
		 	revokeAllPriviges.push(`REVOKE ALL PRIVILEGES ON FUTURE PIPES IN SCHEMA ${databaseName}.${schema}  FROM ROLE ${role}`);
	    	revokeAllPriviges.push(`REVOKE ALL PRIVILEGES ON FUTURE STAGES IN SCHEMA ${databaseName}.${schema}  FROM ROLE ${role}`);
	    	revokeAllPriviges.push(`REVOKE ALL PRIVILEGES ON FUTURE STREAMS IN SCHEMA ${databaseName}.${schema}  FROM ROLE ${role}`);
	   		revokeAllPriviges.push(`REVOKE ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA ${databaseName}.${schema}  FROM ROLE ${role}`);
	  		revokeAllPriviges.push(`REVOKE ALL PRIVILEGES ON FUTURE TASKS IN SCHEMA ${databaseName}.${schema}  FROM ROLE ${role}`);
	 		revokeAllPriviges.push(`REVOKE ALL PRIVILEGES ON FUTURE VIEWS IN SCHEMA ${databaseName}.${schema}  FROM ROLE ${role}`);
			revokeAllPriviges.push(`REVOKE ALL PRIVILEGES ON FUTURE MATERIALIZED VIEWS IN SCHEMA ${databaseName}.${schema}  FROM ROLE ${role}`);
		//	revokeAllPriviges.push(`REVOKE ALL PRIVILEGES ON FUTURE TEMPORARY TABLES IN SCHEMA ${databaseName}.${schema}  FROM ROLE ${role}`);
	    	
	   
	       for (let query of revokeAllPriviges) {
	       		try {
	       			snowflake.execute({ sqlText: query });
	                results.push(`${query}`);
	            } catch (err) {
	                results.push(`${query}; Error:  ${err}`);
	                return results.join(''; \\r\\n'');
	            }
	        }
        
        
            const grantToReadOnlyQueries = [];
            if (!(excludeReadonlyAccessToSchemas.includes(schema) && role.includes(roleReadOnlySuffix))) {
                grantToReadOnlyQueries.push(`GRANT USAGE ON SCHEMA ${databaseName}.${schema} TO ROLE ${role}`);
                if(isSystemAccount === false) {
	               	grantToReadOnlyQueries.push(`GRANT SELECT, REFERENCES ON ALL TABLES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role}`);
	                grantToReadOnlyQueries.push(`GRANT SELECT, REFERENCES ON FUTURE TABLES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role}`);
               	}
                grantToReadOnlyQueries.push(`GRANT SELECT ON ALL EVENT TABLES IN SCHEMA ${databaseName}.${schema} TO ROLE IDENTIFIER(''"${role}"'')`);
                grantToReadOnlyQueries.push(`GRANT USAGE ON ALL FUNCTIONS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role}`);
                //grantToReadOnlyQueries.push(`GRANT SELECT ON FUTURE DYNAMIC TABLES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role}`);
                grantToReadOnlyQueries.push(`GRANT USAGE ON  FUTURE FUNCTIONS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role}`);
                grantToReadOnlyQueries.push(`GRANT SELECT ON ALL MATERIALIZED VIEWS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role}`);
                grantToReadOnlyQueries.push(`GRANT SELECT ON FUTURE MATERIALIZED VIEWS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role}`);
                grantToReadOnlyQueries.push(`GRANT SELECT ON ALL VIEWS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role}`);
                grantToReadOnlyQueries.push(`GRANT SELECT ON FUTURE VIEWS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role}`);
                grantToReadOnlyQueries.push(`GRANT SELECT ON FUTURE EVENT TABLES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role}`);
                grantToReadOnlyQueries.push(`GRANT SELECT ON FUTURE EXTERNAL TABLES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role}`);
               
                //grantToReadOnlyQueries.push();
                //grantToReadOnlyQueries.push();
                //grantToReadOnlyQueries.push();

                for (let query of grantToReadOnlyQueries) {
                    try {
                       snowflake.execute({ sqlText: query });
                       results.push(`${query}`);
                    } catch (err) {
                        results.push(`${query}; Error:  ${err}`);
                        return results.join(''; \\r\\n'');
                    }
                }
              
            }

            if (role.includes(roleEditorSuffix)) {
                const grantToEditorQueries = [];
                 /*
                 grantToEditorQueries.push(`GRANT OWNERSHIP ON ALL TABLES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS`);
                 grantToEditorQueries.push(`GRANT OWNERSHIP ON ALL MATERIALIZED VIEWS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS`);
                 grantToEditorQueries.push(`GRANT OWNERSHIP ON ALL PROCEDURES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS`);
                 grantToEditorQueries.push(`GRANT OWNERSHIP ON ALL VIEWS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS`);
                 grantToEditorQueries.push(`GRANT OWNERSHIP ON ALL SEQUENCES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS`);
                 grantToEditorQueries.push(`GRANT OWNERSHIP ON ALL FUNCTIONS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS`);
                 grantToEditorQueries.push(`GRANT OWNERSHIP ON FUTURE TABLES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS`);
                 grantToEditorQueries.push(`GRANT OWNERSHIP ON FUTURE EVENT TABLES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS`);
                 grantToEditorQueries.push(`GRANT OWNERSHIP ON  FUTURE MATERIALIZED VIEWS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS`);
                 grantToEditorQueries.push(`GRANT OWNERSHIP ON  FUTURE PROCEDURES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS`);
                 grantToEditorQueries.push(`GRANT OWNERSHIP ON  FUTURE VIEWS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS`);
                 grantToEditorQueries.push(`GRANT OWNERSHIP ON  FUTURE SEQUENCES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS`);
                 grantToEditorQueries.push(`GRANT OWNERSHIP ON  FUTURE FUNCTIONS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS`);
                 grantToEditorQueries.push(`GRANT OWNERSHIP ON  FUTURE FILE FORMATS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS`);
                * /
               if(isSystemAccount === true) {
	               	grantToReadOnlyQueries.push(`GRANT SELECT, REFERENCES ON ALL TABLES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role}`);
	                grantToReadOnlyQueries.push(`GRANT SELECT, REFERENCES ON FUTURE TABLES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role}`);
               	}
               
                grantToEditorQueries.push(`GRANT REBUILD ON FUTURE TABLES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role}`);
                grantToEditorQueries.push(`GRANT REFERENCES ON FUTURE VIEWS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role}`);
                grantToEditorQueries.push(`GRANT TRUNCATE ON FUTURE TABLES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role}`);
                grantToEditorQueries.push(`GRANT CREATE TEMPORARY TABLE ON SCHEMA ${databaseName}.${schema} TO ROLE ${role}`);
                grantToEditorQueries.push(`GRANT USAGE ON FUTURE SEQUENCES IN SCHEMA  ${databaseName}.${schema} TO ROLE ${role}`);
                grantToEditorQueries.push(`GRANT CREATE PROCEDURE ON SCHEMA ${databaseName}.${schema} TO ROLE ${role}`);
                grantToEditorQueries.push(`GRANT USAGE ON FUTURE PROCEDURES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role}`);
                grantToEditorQueries.push(`GRANT DELETE ON FUTURE TABLES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role}`);
                grantToEditorQueries.push(`GRANT READ ON FUTURE STAGES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role}`);
                grantToEditorQueries.push(`GRANT REFERENCES ON FUTURE MATERIALIZED VIEWS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role}`);
                grantToEditorQueries.push(`GRANT CREATE VIEW ON SCHEMA ${databaseName}.${schema} TO ROLE ${role}`);
                grantToEditorQueries.push(`GRANT CREATE TABLE ON SCHEMA ${databaseName}.${schema} TO ROLE ${role}`);
                grantToEditorQueries.push(`GRANT INSERT ON FUTURE TABLES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role}`);
                grantToEditorQueries.push(`GRANT UPDATE ON FUTURE TABLES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role}`);
                grantToEditorQueries.push(`GRANT MONITOR ON SCHEMA ${databaseName}.${schema} TO ROLE ${role}`);
                grantToEditorQueries.push(`GRANT CREATE MATERIALIZED VIEW ON SCHEMA ${databaseName}.${schema} TO ROLE ${role}`);
                grantToEditorQueries.push(`GRANT CREATE FILE FORMAT ON SCHEMA ${databaseName}.${schema} TO ROLE ${role}`);                
                grantToEditorQueries.push(`GRANT CREATE FUNCTION ON SCHEMA ${databaseName}.${schema} TO ROLE ${role}`);                
                grantToEditorQueries.push(`GRANT CREATE SEQUENCE ON SCHEMA  ${databaseName}.${schema} TO ROLE IDENTIFIER(''"${role}"'')`);
               

                for (let query of grantToEditorQueries) {
                    try {
                       snowflake.execute({ sqlText: query });
                       results.push(`${query}`);
                    } catch (err) {
                        results.push(`${query}; Error:  ${err}`);                       
                        if (`${err}`.includes(''already exists'') !== true) {
                            return results.join(''; \\r\\n'');
                        }
                    }
                }


            }
            
            
            if (role.includes(roleAdminSuffix)) {
                const grantToAdminQueries = [];
              /* 
                 grantToAdminQueries.push(`GRANT OWNERSHIP ON ALL TABLES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS`);
                 grantToAdminQueries.push(`GRANT OWNERSHIP ON ALL MATERIALIZED VIEWS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS`);
                 grantToAdminQueries.push(`GRANT OWNERSHIP ON ALL PROCEDURES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS`);
                 grantToAdminQueries.push(`GRANT OWNERSHIP ON ALL VIEWS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS`);
                 grantToAdminQueries.push(`GRANT OWNERSHIP ON ALL SEQUENCES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS`);
                 grantToAdminQueries.push(`GRANT OWNERSHIP ON ALL FUNCTIONS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS`);
                 grantToAdminQueries.push(`GRANT OWNERSHIP ON FUTURE TABLES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS`);
                 grantToAdminQueries.push(`GRANT OWNERSHIP ON FUTURE EVENT TABLES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS`);
                 grantToAdminQueries.push(`GRANT OWNERSHIP ON  FUTURE MATERIALIZED VIEWS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS`);
                 grantToAdminQueries.push(`GRANT OWNERSHIP ON  FUTURE PROCEDURES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS`);
                 grantToAdminQueries.push(`GRANT OWNERSHIP ON  FUTURE VIEWS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS`);
                 grantToAdminQueries.push(`GRANT OWNERSHIP ON  FUTURE SEQUENCES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS`);
                 grantToAdminQueries.push(`GRANT OWNERSHIP ON  FUTURE FUNCTIONS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS`);
                 grantToAdminQueries.push(`GRANT OWNERSHIP ON  FUTURE FILE FORMATS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS`);
                grantToAdminQueries.push(`GRANT CREATE DYNAMIC TABLE ON FUTURE SCHEMAS IN DATABASE IDENTIFIER(''"${databaseName}"'') TO ROLE IDENTIFIER(''"${role}"'')`);
                * /
                grantToAdminQueries.push(`GRANT CREATE DYNAMIC TABLE ON FUTURE SCHEMAS IN DATABASE IDENTIFIER(''"${databaseName}"'') TO ROLE IDENTIFIER(''"${role}"'')`);
                grantToAdminQueries.push(`GRANT CREATE EXTERNAL TABLE ON SCHEMA ${databaseName}.${schema} TO ROLE ${role}`);
                grantToAdminQueries.push(`GRANT CREATE PIPE ON SCHEMA ${databaseName}.${schema} TO ROLE ${role}`);               
                grantToAdminQueries.push(`GRANT CREATE STREAM ON SCHEMA ${databaseName}.${schema} TO ROLE ${role}`);             
                grantToAdminQueries.push(`GRANT CREATE DYNAMIC TABLE ON SCHEMA  ${databaseName}.${schema} TO ROLE IDENTIFIER(''"${role}"'')`);
                grantToAdminQueries.push(`GRANT CREATE STAGE ON SCHEMA  ${databaseName}.${schema} TO ROLE IDENTIFIER(''"${role}"'')`);
               

                for (let query of grantToAdminQueries) {
                    try {
                      snowflake.execute({ sqlText: query });
                       results.push(`${query}`);
                    } catch (err) {
                        results.push(`${query}; Error:  ${err}`);                       
                        if (`${err}`.includes(''already exists'') !== true) {
                            return results.join(''; \\r\\n'');
                        }
                    }
                }


            }
           


        } */
    //}
    return `Success; Result: ` + results.join(''; \\r\\n'');



';
CREATE OR REPLACE PROCEDURE "CREATE_DATABASE_SCHEMA_AND_ROLES"("W_NAME" VARCHAR(16777216), "W_INSTANCE" VARCHAR(10), "W_SYSTEM_ACCOUNT" BOOLEAN, "W_ADDITIONAL_SCHEMAS" ARRAY, "W_PARENT_ROLE" VARCHAR(16777216), "W_ADDITIONAL_GRANTS_TO_EDITOR" ARRAY, "W_ADDITIONAL_GRANTS_TO_READONLY" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
// JavaScript source code
const workGroupName = W_NAME;

const devSuffix = ''_DEV'';
const stgSuffix = ''_STG'';
const instance = !W_INSTANCE ? '''' : W_INSTANCE.beginsWith("_") ? W_INSTANCE.toUpperCase() : `_${W_INSTANCE.toUpperCase()}`;
const instances = [instance];
const parentRole = W_PARENT_ROLE;
const createRolesOnly = false; //W_CREATE_ROLES_ONLY;
const isSystemAccount = W_SYSTEM_ACCOUNT;
const maskingAdminRoleName = ''MASKING_ADMIN'';

const roleEditorSuffix = ''_EDITOR'';
const roleReadOnlySuffix = ''_READONLY'';

const rolePrefix = '''';

const systemTag = ''SYSTEM'';
const warehousePrefix = ''WH_'';
const warehouseSize = ''MEDIUM'';
const warehouseAutoSuspend = 120;
const warehouseAutoResume = ''TRUE'';
const warehouseMinClusterCount = 1;
const warehouseMaxClusterCount = 2;
const warehouseType = ''STANDARD'';
const warehouseScalingPolicy = ''ECONOMY'';
const additionalRoleGrantsToEditor = W_ADDITIONAL_GRANTS_TO_EDITOR;
const additionalRoleGrantsToReadOnly = W_ADDITIONAL_GRANTS_TO_READONLY;
const additionalSchemaNames = !isSystemAccount ? [''PUBLIC'', ''INTERNAL''] : [''PUBLIC''];
const excludeReadonlyAccessToSchemas = [''INTERNAL''];
let query = '''';
const results = [];
if (!W_ADDITIONAL_SCHEMAS === false) {
    additionalSchemaNames.push(...W_ADDITIONAL_SCHEMAS)
}

try {
    snowflake.execute({ sqlText: `USE WAREHOUSE SYSADMIN` });
    results.push(`USE WAREHOUSE SYSADMIN`);
} catch (err) {
    results.push(`USE WAREHOUSE SYSADMIN; Error : ${err}`);
    return results.join(''; '');
}

// CHANGE ROLE 
try {
    snowflake.execute({ sqlText: `USE ROLE SYSADMIN` });
    results.push(`USE ROLE SYSADMIN`);
} catch (err) {
    results.push(`USE ROLE SYSADMIN; Error : ${err}`);
    return results.join(''; '');
}

try {
    snowflake.execute({ sqlText: `USE WAREHOUSE SYSADMIN` });
    results.push(`USE WAREHOUSE SYSADMIN`);
} catch (err) {  
    results.push(`USE WAREHOUSE SYSADMIN; Error : ${err}`);
    return results.join(''; '');
}
//let procedureDatabase = "";
//try {
//    const procedureLocationName = snowflake.createStatement({ sqlText: "select PROCEDURE_CATALOG, PROCEDURE_SCHEMA from information_schema.procedures where procedure_name = ''CREATE_DATABASE_SCHEMA_AND_ROLES''" }).execute();

//    if (procedureLocationName.next()) {
//        procedureDatabase = procedureLocationName.getColumnValue(1);
//    }
//    results.push(`USE DATABASE ${procedureDatabase}`);
//    snowflake.execute({ sqlText: `USE DATABASE ${procedureDatabase}` });
    
//} catch (err) {
//    results.push(`Error:  ${err}`);   
//}

//const instances = [];
//if (procedureDatabase.endsWith(devSuffix)) instances.push(devSuffix);
//else if (procedureDatabase.endsWith(stgSuffix)) instances.push(stgSuffix);
//else instances.push('''');


for (let instance of instances) {

    // CHANGE ROLE 
    try {
        snowflake.execute({ sqlText: `USE ROLE SYSADMIN` });
        results.push(`USE ROLE SYSADMIN`);
    } catch (err) {
        results.push(`USE ROLE SYSADMIN; Error : ${err}`);
        return results.join(''; '');
   
    }


    // Warehouse creation
    const warehouseName = `${warehousePrefix}${workGroupName}${instance}`
    const createWarehouseSQL = `CREATE WAREHOUSE IF NOT EXISTS ${warehouseName}
            WAREHOUSE_SIZE = ${warehouseSize}
            AUTO_SUSPEND = ${warehouseAutoSuspend}
            AUTO_RESUME = ${warehouseAutoResume}
            MIN_CLUSTER_COUNT = ${warehouseMinClusterCount}
            MAX_CLUSTER_COUNT  = ${warehouseMaxClusterCount}
            SCALING_POLICY = ${warehouseScalingPolicy}
            WAREHOUSE_TYPE = ${warehouseType} `;
    try {
        if (!createRolesOnly) {
            snowflake.execute({ sqlText: createWarehouseSQL });
            results.push(createWarehouseSQL);
        }
    } catch (err) {
        results.push(`Error creating WAREHOUSE: ${createWarehouseSQL} - ${err}`);
        return results.join(''; '');
    }


    // CHANGE ROLE 
    try {
        snowflake.execute({ sqlText: `USE ROLE SYSADMIN` });
        results.push(`USE ROLE SYSADMIN`);
    } catch (err) {
        results.push(`USE ROLE SYSADMIN; Error USE ROLE SYSADMIN: ${err}`);
        return results.join(''; '');
    }

    try {
        snowflake.execute({ sqlText: `USE WAREHOUSE SYSADMIN` });
        results.push(`USE WAREHOUSE SYSADMIN`);
    } catch (err) {
        results.push(`USE WAREHOUSE SYSADMIN; Error : ${err}`);
        return results.join(''; '');
    }

    const databaseName = `${workGroupName}${instance}`
    const createDatabaseSQL = `CREATE DATABASE IF NOT EXISTS ${databaseName}`;
    try {
        if (!createRolesOnly) snowflake.execute({ sqlText: createDatabaseSQL });
        results.push(createDatabaseSQL);
    } catch (err) {
        results.push(`${createDatabaseSQL}; {Error creating DATABASE: ${err}`);
        return results.join(''; '');
    }


    // Needs to give USERADMIN USAGE and GRANT ROLE ACCESS
    try {
        if (!createRolesOnly) {
            snowflake.execute({ sqlText: `GRANT USAGE, CREATE DATABASE ROLE ON DATABASE ${databaseName} TO USERADMIN ` });
            results.push(`GRANT USAGE, CREATE DATABASE ROLE ON DATABASE ${databaseName} TO USERADMIN `);
        }
    } catch (err) {
        results.push(`GRANT USAGE, CREATE DATABASE ROLE ON DATABASE ${databaseName} TO USERADMIN ; Error creating DATABASE: ${err}`);
        return results.join(''; '');
    }



    // Schema creation
    for (let schema of additionalSchemaNames) {
        const createSchemaSQL = `CREATE SCHEMA IF NOT EXISTS ${databaseName}.${schema}  WITH MANAGED ACCESS`;
        try {
            if (!createRolesOnly) {
                snowflake.execute({ sqlText: createSchemaSQL });
                results.push(`${createSchemaSQL}`);
            }
        } catch (err) {
            results.push(`${createSchemaSQL}; Error creating schema: ${err}`);
            return results.join(''; '');
        }
    }


    // CHANGE ROLE change to security admin for permissions
    try {
        snowflake.execute({ sqlText: `USE ROLE USERADMIN ` });
        results.push(`USE ROLE USERADMIN `);
    } catch (err) {
        results.push(`USE ROLE USERADMIN; Error USE ROLE USERADMIN: ${err}`);
        return results.join(''; '');
    }


    const roleReadOnlyName = `${rolePrefix}${workGroupName}${instance}${roleReadOnlySuffix}`
    const createRoleSQL = `CREATE ROLE IF NOT EXISTS ${roleReadOnlyName}`;
    try {
        snowflake.execute({ sqlText: createRoleSQL });
        results.push(`${createRoleSQL}`);
    } catch (err) {
        results.push(`${createRoleSQL}; Error creating ROLE: ${err}`);
        return results.join(''; '');
    }


    const roleEditorName = `${rolePrefix}${workGroupName}${instance}${roleEditorSuffix}`
    const createEditorRoleSQL = `CREATE ROLE IF NOT EXISTS ${roleEditorName}`;
    try {
        snowflake.execute({ sqlText: createEditorRoleSQL });
        results.push(`${createEditorRoleSQL}`);
    } catch (err) {
        results.push(`${createEditorRoleSQL};Error creating Editor ROLE: ${err}`);
        return results.join(''; '');
    }



    // CHANGE ROLE change to security admin for permissions
    try {
        snowflake.execute({ sqlText: `USE ROLE SECURITYADMIN ` });
        results.push(`USE ROLE SECURITYADMIN `);
    } catch (err) {
        results.push(`USE ROLE SECURITYADMIN; Error using administrative database: ${err} `);
        return results.join(''; '');
    }
    // assign READONLY to EDITOR role  
    const grantRoleSQL = `GRANT ROLE ${roleReadOnlyName} TO ROLE ${roleEditorName}`;
    try {
        snowflake.execute({ sqlText: grantRoleSQL });
        results.push(`${grantRoleSQL}`);
    } catch (err) {
        results.push(`${grantRoleSQL}; Error creating GRANT ROLE to Parent: ${err}`);
        return results.join(''; '');
    }

    if (!parentRole === false) {
        const grantReadOnlyParentRoleSQL = `GRANT ROLE ${roleReadOnlyName} TO ROLE ${parentRole}${instance}${roleReadOnlySuffix}`;
        try {
            snowflake.execute({ sqlText: grantReadOnlyParentRoleSQL });
            results.push(`${grantReadOnlyParentRoleSQL}`);
        } catch (err) {
            results.push(`${grantReadOnlyParentRoleSQL}; Error creating GRANT ROLE to Additional Readonly Parent: ${err}`);
            return results.join(''; '');
        }

        const grantEditorParentRoleSQL = `GRANT ROLE ${roleEditorName} TO ROLE ${parentRole}${instance}${roleEditorSuffix}`;
        try {
            snowflake.execute({ sqlText: grantEditorParentRoleSQL });
            results.push(`${grantEditorParentRoleSQL}`);
        } catch (err) {
            results.push(`${grantEditorParentRoleSQL}; Error creating GRANT ROLE to Additional Editor Parent: ${err}`);
            return results.join(''; '');
        }
    }
    for (let addRole of additionalRoleGrantsToEditor) {
        const grantRoleName = addRole[''name''];
        const roleType = addRole[''type''].startsWith("_") ? addRole[''type''] : "_" + addRole[''type''];
        query = `GRANT ROLE ${grantRoleName}${instance}${roleType} TO ROLE ${roleEditorName}`;
        try {
            snowflake.execute({ sqlText: query });
            results.push(`${query}`);
        } catch (err) {
            results.push(`${query}; Error:  ${err}`);
            return results.join(''; '');
        }

    }

    for (let addRole of additionalRoleGrantsToReadOnly) {
        const grantRoleName = addRole[''name''];
        const roleType = addRole[''type''].startsWith("_") ? addRole[''type''] : "_" + addRole[''type''];
        query = `GRANT ROLE ${grantRoleName}${instance}${roleType} TO ROLE ${roleReadOnlyName}`;
        try {
            snowflake.execute({ sqlText: query });
            results.push(`${query}`);
        } catch (err) {
            results.push(`${query}; Error:  ${err}`);
            return results.join(''; '');
        }

    }

    /////////// START GRANTING ACCESS ///////////

    // assign editor role to use warehouse  
    query = `GRANT USAGE ON WAREHOUSE ${warehouseName} TO ROLE ${roleEditorName}`;
    try {
        if (!createRolesOnly) {
            snowflake.execute({ sqlText: query });  // can put back in if we check that the warehouse exists first
            results.push(`${query}`);
        }
    } catch (err) {
        results.push(`${query}; Error:  ${err}`);
        return results.join(''; '');
    }

    // assign readonly role to use IUF readonly warehouse 
    query = `GRANT USAGE ON WAREHOUSE WH_IUF${instance} TO ROLE ${roleReadOnlyName}`;
    try {
        if (!createRolesOnly) snowflake.execute({ sqlText: query });  // can put back in if we check that the warehouse exists first
    } catch (err) {
        results.push(`${query}; Error:  ${err}`);
        return results.join(''; '');
    }


    const roles = [roleReadOnlyName, roleEditorName]; //, 

    // DATABASE USAGE
    for (let role of roles) {
        query = `GRANT USAGE ON DATABASE ${databaseName} TO ROLE ${role}`;
        try {
            snowflake.execute({ sqlText: query });
        } catch (err) {
            results.push(`${query}; Error:  ${err}`);
            return results.join(''; '');
        }
    }


    // DATABASE FUTURE USAGE
    for (let role of roles) {
        query = `GRANT USAGE ON FUTURE SCHEMAS IN DATABASE ${databaseName} TO ROLE ${role}`;
        try {
            snowflake.execute({ sqlText: query });
        } catch (err) {
            results.push(`${query}; Error:  ${err}`);
            return results.join(''; '');
        }

    }
    const grantToMaskingAdminDatabase = [];
    grantToMaskingAdminDatabase.push(`GRANT USAGE ON DATABASE ${databaseName} TO ROLE ${maskingAdminRoleName}`);
    grantToMaskingAdminDatabase.push(`GRANT USAGE ON FUTURE SCHEMAS IN DATABASE ${databaseName} TO ROLE ${maskingAdminRoleName}`);
    grantToMaskingAdminDatabase.push(`GRANT REFERENCES ON FUTURE SCHEMAS IN DATABASE ${databaseName} TO ROLE ${maskingAdminRoleName}`);
    grantToMaskingAdminDatabase.push(`GRANT REFERENCES ON ALL SCHEMAS IN DATABASE ${databaseName} TO ROLE ${maskingAdminRoleName}`);
  

    for (let query of grantToMaskingAdminDatabase) {
                try {
                    snowflake.execute({ sqlText: query });
                } catch (err) {
                    results.push(`${query}; Error:  ${err}`);
                    return results.join(''; '');
                 }
             }
    let returnstatement = "~";
    
    for (let schema of additionalSchemaNames) {

        const grantToMaskingAdmin = [];
        grantToMaskingAdmin.push(`GRANT USAGE ON SCHEMA ${databaseName}.${schema} TO ROLE ${maskingAdminRoleName}`);
        grantToMaskingAdmin.push(`GRANT SELECT, REFERENCES ON ALL TABLES IN SCHEMA ${databaseName}.${schema} TO ROLE ${maskingAdminRoleName}`);
        
         for (let query of grantToMaskingAdmin) {
                    try {
                        snowflake.execute({ sqlText: query });
                    } catch (err) {
                        results.push(`${query}; Error:  ${err}`);
                        return results.join(''; '');
                    }
                }
        
        for (let role of roles) {
            const grantToReadOnlyQueries = [];
            if (!(excludeReadonlyAccessToSchemas.includes(schema) && role.includes(roleReadOnlySuffix))) {
                grantToReadOnlyQueries.push(`GRANT USAGE ON SCHEMA ${databaseName}.${schema} TO ROLE ${role}`);
                grantToReadOnlyQueries.push(`GRANT SELECT, REFERENCES ON ALL TABLES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role}`);
                grantToReadOnlyQueries.push(`GRANT SELECT, REFERENCES ON FUTURE TABLES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role}`);
                grantToReadOnlyQueries.push(`GRANT SELECT ON ALL EVENT TABLES IN SCHEMA ${databaseName}.${schema} TO ROLE IDENTIFIER(''"${role}"'')`);
                grantToReadOnlyQueries.push(`GRANT USAGE ON ALL FUNCTIONS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role}`);
                //grantToReadOnlyQueries.push(`GRANT SELECT ON FUTURE DYNAMIC TABLES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role}`);
                grantToReadOnlyQueries.push(`GRANT USAGE ON  FUTURE FUNCTIONS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role}`);
                grantToReadOnlyQueries.push(`GRANT SELECT ON ALL MATERIALIZED VIEWS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role}`);
                grantToReadOnlyQueries.push(`GRANT SELECT ON FUTURE MATERIALIZED VIEWS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role}`);
                grantToReadOnlyQueries.push(`GRANT SELECT ON ALL VIEWS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role}`);
                grantToReadOnlyQueries.push(`GRANT SELECT ON FUTURE VIEWS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role}`);
                grantToReadOnlyQueries.push(`GRANT SELECT ON FUTURE EVENT TABLES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role}`);
                grantToReadOnlyQueries.push(`GRANT SELECT ON FUTURE EXTERNAL TABLES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role}`);
               
                //grantToReadOnlyQueries.push();
                //grantToReadOnlyQueries.push();
                //grantToReadOnlyQueries.push();

                for (let query of grantToReadOnlyQueries) {
                    try {
                        snowflake.execute({ sqlText: query });
                    } catch (err) {
                        results.push(`${query}; Error:  ${err}`);
                        return results.join(''; '');
                    }
                }
              
            }

            if (role.includes(roleEditorSuffix)) {
                const grantToEditorQueries = [];
                -- grantToEditorQueries.push(`GRANT OWNERSHIP ON ALL TABLES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS`);
                -- grantToEditorQueries.push(`GRANT OWNERSHIP ON ALL MATERIALIZED VIEWS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS`);
                -- grantToEditorQueries.push(`GRANT OWNERSHIP ON ALL PROCEDURES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS`);
                -- grantToEditorQueries.push(`GRANT OWNERSHIP ON ALL VIEWS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS`);
                -- grantToEditorQueries.push(`GRANT OWNERSHIP ON ALL SEQUENCES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS`);
                -- grantToEditorQueries.push(`GRANT OWNERSHIP ON ALL FUNCTIONS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS`);
                -- grantToEditorQueries.push(`GRANT OWNERSHIP ON FUTURE TABLES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS`);
                -- grantToEditorQueries.push(`GRANT OWNERSHIP ON FUTURE EVENT TABLES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS`);
                -- grantToEditorQueries.push(`GRANT OWNERSHIP ON  FUTURE MATERIALIZED VIEWS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS`);
                -- grantToEditorQueries.push(`GRANT OWNERSHIP ON  FUTURE PROCEDURES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS`);
                -- grantToEditorQueries.push(`GRANT OWNERSHIP ON  FUTURE VIEWS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS`);
                -- grantToEditorQueries.push(`GRANT OWNERSHIP ON  FUTURE SEQUENCES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS`);
                -- grantToEditorQueries.push(`GRANT OWNERSHIP ON  FUTURE FUNCTIONS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS`);
                -- grantToEditorQueries.push(`GRANT OWNERSHIP ON  FUTURE FILE FORMATS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS`);
                grantToEditorQueries.push(`GRANT CREATE DYNAMIC TABLE ON FUTURE SCHEMAS IN DATABASE IDENTIFIER(''"${databaseName}"'') TO ROLE IDENTIFIER(''"${role}"'')`);
                grantToEditorQueries.push(`GRANT REBUILD ON FUTURE TABLES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role}`);
                grantToEditorQueries.push(`GRANT REFERENCES ON FUTURE VIEWS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role}`);
                grantToEditorQueries.push(`GRANT TRUNCATE ON FUTURE TABLES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role}`);
                grantToEditorQueries.push(`GRANT CREATE TEMPORARY TABLE ON SCHEMA ${databaseName}.${schema} TO ROLE ${role}`);
                grantToEditorQueries.push(`GRANT USAGE ON FUTURE SEQUENCES IN SCHEMA  ${databaseName}.${schema} TO ROLE ${role}`);
                grantToEditorQueries.push(`GRANT CREATE PROCEDURE ON SCHEMA ${databaseName}.${schema} TO ROLE ${role}`);
                grantToEditorQueries.push(`GRANT USAGE ON FUTURE PROCEDURES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role}`);
                grantToEditorQueries.push(`GRANT DELETE ON FUTURE TABLES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role}`);
                grantToEditorQueries.push(`GRANT READ ON FUTURE STAGES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role}`);
                grantToEditorQueries.push(`GRANT REFERENCES ON FUTURE MATERIALIZED VIEWS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role}`);
                grantToEditorQueries.push(`GRANT CREATE VIEW ON SCHEMA ${databaseName}.${schema} TO ROLE ${role}`);
                grantToEditorQueries.push(`GRANT CREATE TABLE ON SCHEMA ${databaseName}.${schema} TO ROLE ${role}`);
                grantToEditorQueries.push(`GRANT INSERT ON FUTURE TABLES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role}`);
                grantToEditorQueries.push(`GRANT UPDATE ON FUTURE TABLES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role}`);
                grantToEditorQueries.push(`GRANT MONITOR ON SCHEMA ${databaseName}.${schema} TO ROLE ${role}`);
                grantToEditorQueries.push(`GRANT CREATE MATERIALIZED VIEW ON SCHEMA ${databaseName}.${schema} TO ROLE ${role}`);
                grantToEditorQueries.push(`GRANT CREATE FILE FORMAT ON SCHEMA ${databaseName}.${schema} TO ROLE ${role}`);
                grantToEditorQueries.push(`GRANT CREATE ON FUTURE FILE FORMAT IN  SCHEMA ${databaseName}.${schema} TO ROLE ${role}`);
                grantToEditorQueries.push(`GRANT CREATE FUNCTION ON SCHEMA ${databaseName}.${schema} TO ROLE ${role}`);
                grantToEditorQueries.push(`GRANT CREATE DYNAMIC TABLE ON SCHEMA  ${databaseName}.${schema} TO ROLE IDENTIFIER(''"${role}"'')`);
                grantToEditorQueries.push(`GRANT CREATE SEQUENCE ON SCHEMA  ${databaseName}.${schema} TO ROLE IDENTIFIER(''"${role}"'')`);
               

                for (let query of grantToEditorQueries) {
                    try {
                        snowflake.execute({ sqlText: query });
                    } catch (err) {
                        results.push(`${query}; Error:  ${err}`);                       
                        if (`${err}`.includes(''already exists'') !== true) {
                            return results.join(''; '');
                        }
                    }
                }


            }


        }
    }
    return `Success; Result: ` + results.join(''; '');

}

';