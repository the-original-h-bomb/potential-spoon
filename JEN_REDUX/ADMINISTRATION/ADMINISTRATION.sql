create or replace database ADMINISTRATION;

create or replace schema PUBLIC;

create or replace tag SYSTEM ;
create or replace TABLE DEFAULT_ROLE_PERMISSIONS_ASSIGNED (
	LIKE_MATCHES VARCHAR(16777216),
	DB_PERMISSIONS ARRAY,
	SCHEMA_PERMISSIONS ARRAY,
	ADDED_DATE TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP(),
	PUBLIC_ONLY BOOLEAN
)COMMENT='Local to store the mappings of what each role naming conventions permissions should be set to'
;
CREATE OR REPLACE PROCEDURE "CREATE_DATABASE_SCHEMA_ROLE"("W_NAME" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
// JavaScript source code
const workGroupName = W_NAME;    
const instances = ['''', ''_STG'', ''_DEV''];
const roles = [''_READONLY'', ''_EDITOR'', ''_ADMIN'']; 

const rolePrefix = '''';
const warehousePrefix = ''WH_'';
const warehouseSize = ''MEDIUM'';
const warehouseAutoSuspend = 120;
const warehouseAutoResume = ''TRUE'';
const warehouseMinClusterCount = 1;
const warehouseMaxClusterCount = 2;
const warehouseType = ''STANDARD'';
const warehouseScalingPolicy = ''ECONOMY'';

const additionalSchemaNames = [''INTERNAL''];

// Warehouse creation
const warehouseName = `${warehousePrefix}${workGroupName}`
const createWarehouseSQL = `CREATE WAREHOUSE  IF NOT EXISTS ${warehouseName}
        WAREHOUSE_SIZE = ${warehouseSize}
        AUTO_SUSPEND = ${warehouseAutoSuspend}
        AUTO_RESUME = ${warehouseAutoResume}
        MIN_CLUSTER_COUNT = ${warehouseMinClusterCount}
        MAX_CLUSTER_COUNT  = ${warehouseMaxClusterCount}
        SCALING_POLICY = ${warehouseScalingPolicy}
        WAREHOUSE_TYPE = ${warehouseType} `;      
try {
    snowflake.execute({ sqlText: createWarehouseSQL });
} catch (err) {
    return `Error creating WAREHOUSE: ${err}`;
}

for (let instance of instances) {

    const databaseName = `${workGroupName}${instance}`
    const createDatabaseSQL = `CREATE DATABASE IF NOT EXISTS ${databaseName}`;
    try {
        snowflake.execute({ sqlText: createDatabaseSQL });
    } catch (err) {
        return `Error creating DATABASE: ${err}`;
    }
     // Schema creation
    for (let schema of additionalSchemaNames) {
        const createSchemaSQL = `CREATE SCHEMA IF NOT EXISTS ${databaseName}.${schema}`;
        try {
            snowflake.execute({ sqlText: createSchemaSQL });
        } catch (err) {
            return `Error creating schema: ${err}`;
        }
    }

   

     // default roles creation
    for (let role of roles) {
        const roleName = `${rolePrefix}${workGroupName}${instance}${role}`
        const createRoleSQL = `CREATE ROLE IF NOT EXISTS ${roleName}`;
        try {
            snowflake.execute({ sqlText: createRoleSQL });
        } catch (err) {
            return `Error creating ROLE: ${err}`;
        }

         // assign role to parent role
        const parentRole = instance === '''' ? ''PRODUCTION'' : instance === ''_STG'' ? ''STAGE'' : ''DEVELOPMENT'';
        const grantRoleSQL = `GRANT ROLE ${roleName} TO ROLE ${parentRole}`;
        try {
            snowflake.execute({ sqlText: grantRoleSQL });
        } catch (err) {
            return `Error creating GRANT ROLE to Parent: ${err}`;
        }

        // assign role to use warehouse        
        const grantWarehouseSQL = `GRANT USAGE ON WAREHOUSE ${warehouseName} TO ROLE ${roleName}`;
        try {
            snowflake.execute({ sqlText: grantWarehouseSQL });
        } catch (err) {
            return `Error creating GRANT Warehouse to Role: ${err}`;
        }
    }


}

';
CREATE OR REPLACE PROCEDURE "CREATE_DATABASE_SCHEMA_ROLE"("W_NAME" VARCHAR(16777216), "W_SYSTEM_ACCOUNT" BOOLEAN)
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
// JavaScript source code
const workGroupName = W_NAME;    
const instances = ['''', ''_STG'', ''_DEV''];
const roles = [''_READONLY'', ''_EDITOR'', ''_ADMIN'']; 


const isSystemAccount = W_SYSTEM_ACCOUNT ;

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

const additionalSchemaNames = !isSystemAccount ? [''INTERNAL''] : [];

try {
    snowflake.execute({ sqlText: `USE DATABASE ADMINISTRATION` });
} catch (err) {
    return `Error using administrative database: ${err}`;
}

// Warehouse creation
const warehouseName = `${warehousePrefix}${workGroupName}`
const createWarehouseSQL = `CREATE WAREHOUSE  IF NOT EXISTS ${warehouseName}
        WAREHOUSE_SIZE = ${warehouseSize}
        AUTO_SUSPEND = ${warehouseAutoSuspend}
        AUTO_RESUME = ${warehouseAutoResume}
        MIN_CLUSTER_COUNT = ${warehouseMinClusterCount}
        MAX_CLUSTER_COUNT  = ${warehouseMaxClusterCount}
        SCALING_POLICY = ${warehouseScalingPolicy}
        WAREHOUSE_TYPE = ${warehouseType} `;      
try {
    snowflake.execute({ sqlText: createWarehouseSQL });
} catch (err) {
    return `Error creating WAREHOUSE: ${err}`;
}

if(isSystemAccount) {
    try {  
        snowflake.execute({ sqlText: `USE DATABASE ADMINISTRATION` });
        snowflake.execute({ sqlText: `ALTER WAREHOUSE IF EXISTS ${warehouseName} SET TAG ${systemTag} = ''${workGroupName}''`});
    } catch (err) {
        return `Error creating Setting Tag on Warehouse: ${err}`;
    }

}
for (let instance of instances) {

    const databaseName = `${workGroupName}${instance}`
    const createDatabaseSQL = `CREATE DATABASE IF NOT EXISTS ${databaseName}`;
    try {
        snowflake.execute({ sqlText: createDatabaseSQL });
    } catch (err) {
        return `Error creating DATABASE: ${err}`;
    }

    if(isSystemAccount) {
        try {
              snowflake.execute({ sqlText: `USE DATABASE ADMINISTRATION` });
            snowflake.execute({ sqlText: ` ALTER DATABASE IF EXISTS ${databaseName} SET TAG ${systemTag} = ''${workGroupName}''`});
        } catch (err) {
            return `Error creating Setting Tag on Database: ${err}`;
        }
    
    }
    
     // Schema creation
    for (let schema of additionalSchemaNames) {
        const createSchemaSQL = `CREATE SCHEMA IF NOT EXISTS ${databaseName}.${schema}`;
        try {
            snowflake.execute({ sqlText: createSchemaSQL });
        } catch (err) {
            return `Error creating schema: ${err}`;
        }
    }

   

     // default roles creation
    for (let role of roles) {
        const roleName = `${rolePrefix}${workGroupName}${instance}${role}`
        const createRoleSQL = `CREATE ROLE IF NOT EXISTS ${roleName}`;
        try {
            snowflake.execute({ sqlText: createRoleSQL });
        } catch (err) {
            return `Error creating ROLE: ${err}`;
        }
        
        if(isSystemAccount) {
            try {
                  snowflake.execute({ sqlText: `USE DATABASE ADMINISTRATION` });
                snowflake.execute({ sqlText: ` ALTER ROLE IF EXISTS ${roleName} SET TAG ${systemTag} = ''${workGroupName}''`});
            } catch (err) {
                return `Error creating Setting Tag on Role: ${err}`;
            }
        
        }
        

         // assign role to parent role
        const parentRole = isSystemAccount ? `SYSTEM${instance}` : (instance === '''' ? ''PRODUCTION'': instance === ''_STG'' ? ''STAGE'' : ''DEVELOPMENT'');
        const grantRoleSQL = `GRANT ROLE ${roleName} TO ROLE ${parentRole}`;
        try {
            snowflake.execute({ sqlText: grantRoleSQL });
        } catch (err) {
            return `Error creating GRANT ROLE to Parent: ${err}`;
        }

        // assign role to use warehouse        
        const grantWarehouseSQL = `GRANT USAGE ON WAREHOUSE ${warehouseName} TO ROLE ${roleName}`;
        try {
            snowflake.execute({ sqlText: grantWarehouseSQL });
        } catch (err) {
            return `Error creating GRANT Warehouse to Role: ${err}`;
        }
    }


}

';
CREATE OR REPLACE PROCEDURE "CREATE_DATABASE_SCHEMA_ROLE"("W_NAME" VARCHAR(16777216), "W_SYSTEM_ACCOUNT" BOOLEAN, "W_PARENT_ROLE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
// JavaScript source code
const workGroupName = W_NAME;    
const instances = ['''', ''_STG'', ''_DEV''];

const ParentRole = W_PARENT_ROLE; // System, Internal, External

const isSystemAccount = W_SYSTEM_ACCOUNT ;

const roles = [''_READONLY'', ''_EDITOR'']; // ''_ADMIN''
if(isSystemAccount) {
    roles.push(''_ADMIN'');
}

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

const additionalSchemaNames = !isSystemAccount ? [''INTERNAL''] : [];

try {
    snowflake.execute({ sqlText: `USE DATABASE ADMINISTRATION` });
} catch (err) {
    return `Error using administrative database: ${err}`;
}

// Warehouse creation
const warehouseName = `${warehousePrefix}${workGroupName}`
const createWarehouseSQL = `CREATE WAREHOUSE  IF NOT EXISTS ${warehouseName}
        WAREHOUSE_SIZE = ${warehouseSize}
        AUTO_SUSPEND = ${warehouseAutoSuspend}
        AUTO_RESUME = ${warehouseAutoResume}
        MIN_CLUSTER_COUNT = ${warehouseMinClusterCount}
        MAX_CLUSTER_COUNT  = ${warehouseMaxClusterCount}
        SCALING_POLICY = ${warehouseScalingPolicy}
        WAREHOUSE_TYPE = ${warehouseType} `;      
try {
    snowflake.execute({ sqlText: createWarehouseSQL });
} catch (err) {
    return `Error creating WAREHOUSE: ${err}`;
}

if(isSystemAccount) {
    try {  
        snowflake.execute({ sqlText: `USE DATABASE ADMINISTRATION` });
        snowflake.execute({ sqlText: `ALTER WAREHOUSE IF EXISTS ${warehouseName} SET TAG ${systemTag} = ''${workGroupName}''`});
    } catch (err) {
        return `Error creating Setting Tag on Warehouse: ${err}`;
    }

}
for (let instance of instances) {

    const databaseName = `${workGroupName}${instance}`
    const createDatabaseSQL = `CREATE DATABASE IF NOT EXISTS ${databaseName}`;
    try {
        snowflake.execute({ sqlText: createDatabaseSQL });
    } catch (err) {
        return `Error creating DATABASE: ${err}`;
    }

    if(isSystemAccount) {
        try {
              snowflake.execute({ sqlText: `USE DATABASE ADMINISTRATION` });
            snowflake.execute({ sqlText: ` ALTER DATABASE IF EXISTS ${databaseName} SET TAG ${systemTag} = ''${workGroupName}''`});
        } catch (err) {
            return `Error creating Setting Tag on Database: ${err}`;
        }
    
    }
    
     // Schema creation
    for (let schema of additionalSchemaNames) {
        const createSchemaSQL = `CREATE SCHEMA IF NOT EXISTS ${databaseName}.${schema}`;
        try {
            snowflake.execute({ sqlText: createSchemaSQL });
        } catch (err) {
            return `Error creating schema: ${err}`;
        }
    }

   

     // default roles creation
    for (let role of roles) {
        const roleName = `${rolePrefix}${workGroupName}${instance}${role}`
        const createRoleSQL = `CREATE ROLE IF NOT EXISTS ${roleName}`;
        try {
            snowflake.execute({ sqlText: createRoleSQL });
        } catch (err) {
            return `Error creating ROLE: ${err}`;
        }
        
        if(isSystemAccount) {
            try {
                  snowflake.execute({ sqlText: `USE DATABASE ADMINISTRATION` });
                snowflake.execute({ sqlText: ` ALTER ROLE IF EXISTS ${roleName} SET TAG ${systemTag} = ''${workGroupName}''`});
            } catch (err) {
                return `Error creating Setting Tag on Role: ${err}`;
            }
        
        }
        

         // assign role to parent role
        const parentRole = ParentRole !== '''' ? `${ParentRole}${instance}` : (instance === '''' ? ''PRODUCTION'': instance === ''_STG'' ? ''STAGE'' : ''DEVELOPMENT'');
        const grantRoleSQL = `GRANT ROLE ${roleName} TO ROLE ${parentRole}`;
        try {
            snowflake.execute({ sqlText: grantRoleSQL });
        } catch (err) {
            return `Error creating GRANT ROLE to Parent: ${err}`;
        }

        // assign role to use warehouse        
        const grantWarehouseSQL = `GRANT USAGE ON WAREHOUSE ${warehouseName} TO ROLE ${roleName}`;
        try {
            snowflake.execute({ sqlText: grantWarehouseSQL });
        } catch (err) {
            return `Error creating GRANT Warehouse to Role: ${err}`;
        }
    }


}

';
CREATE OR REPLACE PROCEDURE "CREATE_DATABASE_SCHEMA_ROLE"("W_NAME" VARCHAR(16777216), "W_SYSTEM_ACCOUNT" BOOLEAN, "W_PARENT_ROLE" VARCHAR(16777216), "W_CREATE_ROLES_ONLY" BOOLEAN)
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
// JavaScript source code
const workGroupName = W_NAME;    
const instances = ['''', ''_STG'', ''_DEV''];

const ParentRole = W_PARENT_ROLE; // System, Internal, External
const createRolesOnly = W_CREATE_ROLES_ONLY;
const isSystemAccount = W_SYSTEM_ACCOUNT ;

const roles = [''_READONLY'', ''_EDITOR'']; // ''_ADMIN''
if(isSystemAccount) {
    roles.push(''_ADMIN'');
}

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

const additionalSchemaNames = !isSystemAccount ? [''INTERNAL''] : [];

try {
    snowflake.execute({ sqlText: `USE DATABASE ADMINISTRATION` });
} catch (err) {
    return `Error using administrative database: ${err}`;
}

// Warehouse creation
const warehouseName = `${warehousePrefix}${workGroupName}`
const createWarehouseSQL = `CREATE WAREHOUSE  IF NOT EXISTS ${warehouseName}
        WAREHOUSE_SIZE = ${warehouseSize}
        AUTO_SUSPEND = ${warehouseAutoSuspend}
        AUTO_RESUME = ${warehouseAutoResume}
        MIN_CLUSTER_COUNT = ${warehouseMinClusterCount}
        MAX_CLUSTER_COUNT  = ${warehouseMaxClusterCount}
        SCALING_POLICY = ${warehouseScalingPolicy}
        WAREHOUSE_TYPE = ${warehouseType} `;      
try {
    if(!createRolesOnly) snowflake.execute({ sqlText: createWarehouseSQL });
} catch (err) {
    return `Error creating WAREHOUSE: ${err}`;
}

if(isSystemAccount) {
    try {  
        if(!createRolesOnly) snowflake.execute({ sqlText: `USE DATABASE ADMINISTRATION` });
        if(!createRolesOnly) snowflake.execute({ sqlText: `ALTER WAREHOUSE IF EXISTS ${warehouseName} SET TAG ${systemTag} = ''${workGroupName}''`});
    } catch (err) {
        return `Error creating Setting Tag on Warehouse: ${err}`;
    }

}
for (let instance of instances) {

    const databaseName = `${workGroupName}${instance}`
    const createDatabaseSQL = `CREATE DATABASE IF NOT EXISTS ${databaseName}`;
    try {
        if(!createRolesOnly) snowflake.execute({ sqlText: createDatabaseSQL });
    } catch (err) {
        return `Error creating DATABASE: ${err}`;
    }

    if(isSystemAccount) {
        try {
            if(!createRolesOnly) snowflake.execute({ sqlText: `USE DATABASE ADMINISTRATION` });
            if(!createRolesOnly) snowflake.execute({ sqlText: `ALTER DATABASE IF EXISTS ${databaseName} SET TAG ${systemTag} = ''${workGroupName}''`});
        } catch (err) {
            return `Error creating Setting Tag on Database: ${err}`;
        }
    
    }
    
     // Schema creation
    for (let schema of additionalSchemaNames) {
        const createSchemaSQL = `CREATE SCHEMA IF NOT EXISTS ${databaseName}.${schema}`;
        try {
            if(!createRolesOnly) snowflake.execute({ sqlText: createSchemaSQL });
        } catch (err) {
            return `Error creating schema: ${err}`;
        }
    }

   

     // default base roles creation
     const baseRoleName = `${rolePrefix}${workGroupName}${instance}`
        const createBaseRoleSQL = `CREATE ROLE IF NOT EXISTS ${baseRoleName}`;
        try {
            snowflake.execute({ sqlText: createBaseRoleSQL });
        } catch (err) {
            return `Error creating ROLE: ${err}`;
        }
        
        if(isSystemAccount) {
            try {
                snowflake.execute({ sqlText: `USE DATABASE ADMINISTRATION` });
                snowflake.execute({ sqlText: ` ALTER ROLE IF EXISTS ${baseRoleName} SET TAG ${systemTag} = ''${workGroupName}''`});
            } catch (err) {
                return `Error creating Setting Tag on Role: ${err}`;
            }        
        }
         // assign role to parent role
        const baseParentRole = ParentRole !== '''' ? `${ParentRole}${instance}` : (instance === '''' ? ''PRODUCTION'': instance === ''_STG'' ? ''STAGE'' : ''DEVELOPMENT'');
        const grantBaseRoleSQL = `GRANT ROLE ${baseRoleName} TO ROLE ${baseParentRole}`;
        try {
       //     snowflake.execute({ sqlText: grantBaseRoleSQL });
        } catch (err) {
            return `Error creating GRANT ROLE to Parent: ${err}`;
        }

        
     
    for (let role of roles) {
        const roleName = `${rolePrefix}${workGroupName}${instance}${role}`
        const createRoleSQL = `CREATE ROLE IF NOT EXISTS ${roleName}`;
        try {
            snowflake.execute({ sqlText: createRoleSQL });
        } catch (err) {
            return `Error creating ROLE: ${err}`;
        }
        
        if(isSystemAccount) {
            try {
                snowflake.execute({ sqlText: `USE DATABASE ADMINISTRATION` });
                snowflake.execute({ sqlText: ` ALTER ROLE IF EXISTS ${roleName} SET TAG ${systemTag} = ''${workGroupName}''`});
            } catch (err) {
                return `Error creating Setting Tag on Role: ${err}`;
            }
        
        }
        

         // assign role to parent role
        const parentRole = `${baseRoleName}`;
        const grantRoleSQL = `GRANT ROLE ${roleName} TO ROLE ${parentRole}`;
        try {
        //    snowflake.execute({ sqlText: grantRoleSQL });
        } catch (err) {
            return `Error creating GRANT ROLE to Parent: ${err}`;
        }

        // assign role to use warehouse        
        const grantWarehouseSQL = `GRANT USAGE ON WAREHOUSE ${warehouseName} TO ROLE ${roleName}`;
        try {
            if(!createRolesOnly) snowflake.execute({ sqlText: grantWarehouseSQL });  // can put back in if we check that the warehouse exists first
        } catch (err) {
            return `Error creating GRANT Warehouse to Role: ${err}`;
        }
    }


}

';
CREATE OR REPLACE PROCEDURE "CREATE_DATABASE_SCHEMA_ROLE_V2"("W_NAME" VARCHAR(16777216), "W_SYSTEM_ACCOUNT" BOOLEAN, "W_ADDITIONAL_SCHEMAS" ARRAY, "W_ADDITIONAL_GRANTS" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
// JavaScript source code
const workGroupName = W_NAME;
const instances = ['''', ''_STG'', ''_DEV''];

//const ParentRole = W_PARENT_ROLE; // System, Internal, External
const createRolesOnly = false; //W_CREATE_ROLES_ONLY;
const isSystemAccount = W_SYSTEM_ACCOUNT;

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
const additionalRoleGrants = W_ADDITIONAL_GRANTS;
const additionalSchemaNames = !isSystemAccount ? [''PUBLIC'', ''INTERNAL''] : [''PUBLIC''];
const excludeReadonlyAccessToSchemas = [''INTERNAL''];

if (!W_ADDITIONAL_SCHEMAS === false) {
    additionalSchemaNames.push(...W_ADDITIONAL_SCHEMAS)
}

// CHANGE ROLE 
try {
    snowflake.execute({ sqlText: `USE ROLE SYSADMIN` });
} catch (err) {
    return `Error USE ROLE SYSADMIN: ${err}`;
}

try {
    snowflake.execute({ sqlText: `USE DATABASE ADMINISTRATION` });
} catch (err) {
    return `Error using administrative database: ${err}`;
}

// Warehouse creation
const warehouseName = `${warehousePrefix}${workGroupName}`
const createWarehouseSQL = `CREATE WAREHOUSE  IF NOT EXISTS ${warehouseName}
        WAREHOUSE_SIZE = ${warehouseSize}
        AUTO_SUSPEND = ${warehouseAutoSuspend}
        AUTO_RESUME = ${warehouseAutoResume}
        MIN_CLUSTER_COUNT = ${warehouseMinClusterCount}
        MAX_CLUSTER_COUNT  = ${warehouseMaxClusterCount}
        SCALING_POLICY = ${warehouseScalingPolicy}
        WAREHOUSE_TYPE = ${warehouseType} `;
try {
    if (!createRolesOnly) snowflake.execute({ sqlText: createWarehouseSQL });
} catch (err) {
    return `Error creating WAREHOUSE: ${err}`;
}


for (let instance of instances) {
    // CHANGE ROLE 
    try {
        snowflake.execute({ sqlText: `USE ROLE SYSADMIN` });
    } catch (err) {
        return `Error USE ROLE SYSADMIN: ${err}`;
    }

    const databaseName = `${workGroupName}${instance}`
    const createDatabaseSQL = `CREATE DATABASE IF NOT EXISTS ${databaseName}`;
    try {
        if (!createRolesOnly) snowflake.execute({ sqlText: createDatabaseSQL });
    } catch (err) {
        return `Error creating DATABASE: ${err}`;
    }

    
    // Needs to give USERADMIN USAGE and GRANT ROLE ACCESS
    try {
        if (!createRolesOnly) snowflake.execute({ sqlText: `GRANT USAGE, CREATE DATABASE ROLE ON DATABASE ${databaseName} TO USERADMIN ` });
    } catch (err) {
        return `Error creating DATABASE: ${err}`;
    }



    // Schema creation
    for (let schema of additionalSchemaNames) {
        const createSchemaSQL = `CREATE SCHEMA IF NOT EXISTS ${databaseName}.${schema}`;
        try {
            if (!createRolesOnly) snowflake.execute({ sqlText: createSchemaSQL });
        } catch (err) {
            return `Error creating schema: ${err}`;
        }
    }


    // CHANGE ROLE change to security admin for permissions
    try {
        snowflake.execute({ sqlText: `USE ROLE USERADMIN ` });
    } catch (err) {
        return `Error USE ROLE USERADMIN: ${err}`;
    }


    const roleReadOnlyName = `${rolePrefix}${workGroupName}${instance}${roleReadOnlySuffix}`
    const createRoleSQL = `CREATE ROLE IF NOT EXISTS ${roleReadOnlyName}`;
    try {
        snowflake.execute({ sqlText: createRoleSQL });
    } catch (err) {
        return `Error creating ROLE: ${err}`;
    }


    const roleEditorName = `${rolePrefix}${workGroupName}${instance}${roleEditorSuffix}`
    const createEditorRoleSQL = `CREATE ROLE IF NOT EXISTS ${roleEditorName}`;
    try {
        snowflake.execute({ sqlText: createEditorRoleSQL });
    } catch (err) {
        return `Error creating Editor ROLE: ${err}`;
    }

    

    // CHANGE ROLE change to security admin for permissions
    try {
        snowflake.execute({ sqlText: `USE ROLE SECURITYADMIN ` });
    } catch (err) {
        return `Error using administrative database: ${err}`;
    }
    // assign READONLY to EDITOR role  
    const grantRoleSQL = `GRANT ROLE ${roleReadOnlyName} TO ROLE ${roleEditorName}`;
    try {
            snowflake.execute({ sqlText: grantRoleSQL });
    } catch (err) {
        return `Error creating GRANT ROLE to Parent: ${err}`;
    }

    // assign role to use warehouse        
    const grantWarehouseSQL = `GRANT USAGE ON WAREHOUSE ${warehouseName} TO ROLE ${roleReadOnlyName}`;
    try {
        if (!createRolesOnly) snowflake.execute({ sqlText: grantWarehouseSQL });  // can put back in if we check that the warehouse exists first
    } catch (err) {
        return `Error creating GRANT Warehouse to Role: ${err}`;
    }


    // grant standard usage access 
    const roles = [roleReadOnlyName]; //, roleEditorName

    // DATABASE USAGE
    for (let role of roles) {
        try {
            snowflake.execute({ sqlText: `GRANT USAGE ON DATABASE ${databaseName} TO ROLE ${role}` });
        } catch (err) {
            return `Error GRANT USAGE ROLE: ${err}`;
        }
    }


    // DATABASE FUTURE USAGE
    for (let role of roles) {
        try {
            snowflake.execute({ sqlText: `GRANT USAGE ON FUTURE SCHEMAS IN DATABASE ${databaseName} TO ROLE ${role}` });
        } catch (err) {
            return `Error creating ROLE: ${err}`;
        }
    }


    for (let schema of additionalSchemaNames) {
        for (let role of roles) {

            if (!(excludeReadonlyAccessToSchemas.includes(schema) && role.includes(roleReadOnlySuffix))) {
                try {
                    snowflake.execute({ sqlText: `GRANT SELECT, REFERENCES ON ALL TABLES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role}` });
                } catch (err) {
                    return `Error creating schema: ${err}`;
                }

                try {
                    snowflake.execute({ sqlText: `GRANT SELECT, REFERENCES ON  FUTURE TABLES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role}` });
                } catch (err) {
                    return `Error creating schema: ${err}`;
                }

                try {
                    snowflake.execute({ sqlText: `GRANT SELECT ON ALL DYNAMIC TABLES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role}` });
                } catch (err) {
                    return `Error creating schema: ${err}`;
                }

                try {
                    snowflake.execute({ sqlText: `GRANT SELECT ON  FUTURE DYNAMIC TABLES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role}` });
                } catch (err) {
                    return `Error creating schema: ${err}`;
                }

                try {
                    snowflake.execute({ sqlText: `GRANT USAGE ON ALL FUNCTIONS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role}` });
                } catch (err) {
                    return `Error creating schema: ${err}`;
                }

                try {
                    snowflake.execute({ sqlText: `GRANT USAGE ON  FUTURE FUNCTIONS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role}` });
                } catch (err) {
                    return `Error creating schema: ${err}`;
                }
                try {
                    snowflake.execute({ sqlText: `GRANT SELECT ON ALL MATERIALIZED VIEWS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role}` });
                } catch (err) {
                    return `Error creating schema: ${err}`;
                }

                try {
                    snowflake.execute({ sqlText: `GRANT SELECT ON FUTURE MATERIALIZED VIEWS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role}` });
                } catch (err) {
                    return `Error creating schema: ${err}`;
                }
                try {
                    snowflake.execute({ sqlText: `GRANT SELECT ON ALL VIEWS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role}` });
                } catch (err) {
                    return `Error creating schema: ${err}`;
                }

                try {
                    snowflake.execute({ sqlText: `GRANT SELECT ON FUTURE VIEWS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role}` });
                } catch (err) {
                    return `Error creating schema: ${err}`;
                }
            }

            if (role.includes(roleEditorSuffix)) {
                try {
                    snowflake.execute({ sqlText: `GRANT OWNERSHIP ON ALL TABLES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS` });
                } catch (err) {
                    return `Error creating schema: ${err}`;
                }
                try {
                    snowflake.execute({ sqlText: `GRANT OWNERSHIP ON ALL MATERIALIZED VIEWS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS` });
                } catch (err) {
                    return `Error creating schema: ${err}`;
                }
                try {
                    snowflake.execute({ sqlText: `GRANT OWNERSHIP ON ALL PROCEDURES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS` });
                } catch (err) {
                    return `Error creating schema: ${err}`;
                }
                try {
                    snowflake.execute({ sqlText: `GRANT OWNERSHIP ON ALL VIEWS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS` });
                } catch (err) {
                    return `Error creating schema: ${err}`;
                }
                try {
                    snowflake.execute({ sqlText: `GRANT OWNERSHIP ON ALL SEQUENCES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS` });
                } catch (err) {
                    return `Error creating schema: ${err}`;
                }
                try {
                    snowflake.execute({ sqlText: `GRANT OWNERSHIP ON ALL FUNCTIONS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS` });
                } catch (err) {
                    return `Error creating schema: ${err}`;
                }
                try {
                    snowflake.execute({ sqlText: `GRANT OWNERSHIP ON ALL FUTURE TABLES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS` });
                } catch (err) {
                    return `Error creating schema: ${err}`;
                }
                try {
                    snowflake.execute({ sqlText: `GRANT OWNERSHIP ON ALL FUTURE MATERIALIZED VIEWS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS` });
                } catch (err) {
                    return `Error creating schema: ${err}`;
                }
                try {
                    snowflake.execute({ sqlText: `GRANT OWNERSHIP ON ALL FUTURE PROCEDURES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS` });
                } catch (err) {
                    return `Error creating schema: ${err}`;
                }
                try {
                    snowflake.execute({ sqlText: `GRANT OWNERSHIP ON ALL FUTURE VIEWS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS` });
                } catch (err) {
                    return `Error creating schema: ${err}`;
                }
                try {
                    snowflake.execute({ sqlText: `GRANT OWNERSHIP ON ALL FUTURE SEQUENCES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS` });
                } catch (err) {
                    return `Error creating schema: ${err}`;
                }
                try {
                    snowflake.execute({ sqlText: `GRANT OWNERSHIP ON ALL FUTURE FUNCTIONS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS` });
                } catch (err) {
                    return `Error creating schema: ${err}`;
                }


            }
           

        }
    }


}

';
CREATE OR REPLACE PROCEDURE "CREATE_DATABASE_SCHEMA_ROLE_V3"("W_NAME" VARCHAR(16777216), "W_SYSTEM_ACCOUNT" BOOLEAN, "W_ADDITIONAL_SCHEMAS" ARRAY, "W_ADDITIONAL_GRANTS" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
// JavaScript source code
const workGroupName = W_NAME;
const instances = ['''', ''_STG'', ''_DEV''];

//const ParentRole = W_PARENT_ROLE; // System, Internal, External
const createRolesOnly = false; //W_CREATE_ROLES_ONLY;
const isSystemAccount = W_SYSTEM_ACCOUNT;

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
const additionalRoleGrants = W_ADDITIONAL_GRANTS;
const additionalSchemaNames = !isSystemAccount ? [''PUBLIC'', ''INTERNAL''] : [''PUBLIC''];
const excludeReadonlyAccessToSchemas = [''INTERNAL''];

if (!W_ADDITIONAL_SCHEMAS === false) {
    additionalSchemaNames.push(...W_ADDITIONAL_SCHEMAS)
}

// CHANGE ROLE 
try {
    snowflake.execute({ sqlText: `USE ROLE SYSADMIN` });
} catch (err) {
    return `Error USE ROLE SYSADMIN: ${err}`;
}

try {
    snowflake.execute({ sqlText: `USE DATABASE ADMINISTRATION` });
} catch (err) {
    return `Error using administrative database: ${err}`;
}

// Warehouse creation
const warehouseName = `${warehousePrefix}${workGroupName}`
const createWarehouseSQL = `CREATE WAREHOUSE  IF NOT EXISTS ${warehouseName}
        WAREHOUSE_SIZE = ${warehouseSize}
        AUTO_SUSPEND = ${warehouseAutoSuspend}
        AUTO_RESUME = ${warehouseAutoResume}
        MIN_CLUSTER_COUNT = ${warehouseMinClusterCount}
        MAX_CLUSTER_COUNT  = ${warehouseMaxClusterCount}
        SCALING_POLICY = ${warehouseScalingPolicy}
        WAREHOUSE_TYPE = ${warehouseType} `;
try {
    if (!createRolesOnly) snowflake.execute({ sqlText: createWarehouseSQL });
} catch (err) {
    return `Error creating WAREHOUSE: ${err}`;
}


for (let instance of instances) {
    // CHANGE ROLE 
    try {
        snowflake.execute({ sqlText: `USE ROLE SYSADMIN` });
    } catch (err) {
        return `Error USE ROLE SYSADMIN: ${err}`;
    }

    const databaseName = `${workGroupName}${instance}`
    const createDatabaseSQL = `CREATE DATABASE IF NOT EXISTS ${databaseName}`;
    try {
        if (!createRolesOnly) snowflake.execute({ sqlText: createDatabaseSQL });
    } catch (err) {
        return `Error creating DATABASE: ${err}`;
    }

    
    // Needs to give USERADMIN USAGE and GRANT ROLE ACCESS
    try {
        if (!createRolesOnly) snowflake.execute({ sqlText: `GRANT USAGE, CREATE DATABASE ROLE ON DATABASE ${databaseName} TO USERADMIN ` });
    } catch (err) {
        return `Error creating DATABASE: ${err}`;
    }



    // Schema creation
    for (let schema of additionalSchemaNames) {
        const createSchemaSQL = `CREATE SCHEMA IF NOT EXISTS ${databaseName}.${schema}`;
        try {
            if (!createRolesOnly) snowflake.execute({ sqlText: createSchemaSQL });
        } catch (err) {
            return `Error creating schema: ${err}`;
        }
    }


    // CHANGE ROLE change to security admin for permissions
    try {
        snowflake.execute({ sqlText: `USE ROLE USERADMIN ` });
    } catch (err) {
        return `Error USE ROLE USERADMIN: ${err}`;
    }


    const roleReadOnlyName = `${rolePrefix}${workGroupName}${instance}${roleReadOnlySuffix}`
    const createRoleSQL = `CREATE ROLE IF NOT EXISTS ${roleReadOnlyName}`;
    try {
        snowflake.execute({ sqlText: createRoleSQL });
    } catch (err) {
        return `Error creating ROLE: ${err}`;
    }


    const roleEditorName = `${rolePrefix}${workGroupName}${instance}${roleEditorSuffix}`
    const createEditorRoleSQL = `CREATE ROLE IF NOT EXISTS ${roleEditorName}`;
    try {
        snowflake.execute({ sqlText: createEditorRoleSQL });
    } catch (err) {
        return `Error creating Editor ROLE: ${err}`;
    }

    

    // CHANGE ROLE change to security admin for permissions
    try {
        snowflake.execute({ sqlText: `USE ROLE SECURITYADMIN ` });
    } catch (err) {
        return `Error using administrative database: ${err}`;
    }
    // assign READONLY to EDITOR role  
    const grantRoleSQL = `GRANT ROLE ${roleReadOnlyName} TO ROLE ${roleEditorName}`;
    try {
            snowflake.execute({ sqlText: grantRoleSQL });
    } catch (err) {
        return `Error creating GRANT ROLE to Parent: ${err}`;
    }

    // assign role to use warehouse        
    const grantWarehouseSQL = `GRANT USAGE ON WAREHOUSE ${warehouseName} TO ROLE ${roleReadOnlyName}`;
    try {
        if (!createRolesOnly) snowflake.execute({ sqlText: grantWarehouseSQL });  // can put back in if we check that the warehouse exists first
    } catch (err) {
        return `Error creating GRANT Warehouse to Role: ${err}`;
    }


    // grant standard usage access 
    const roles = [roleReadOnlyName]; //, roleEditorName

    // DATABASE USAGE
    for (let role of roles) {
        try {
            snowflake.execute({ sqlText: `GRANT USAGE ON DATABASE ${databaseName} TO ROLE ${role}` });
        } catch (err) {
            return `Error GRANT USAGE ROLE: ${err}`;
        }
    }


    // DATABASE FUTURE USAGE
    for (let role of roles) {
        try {
            snowflake.execute({ sqlText: `GRANT USAGE ON FUTURE SCHEMAS IN DATABASE ${databaseName} TO ROLE ${role}` });
        } catch (err) {
            return `Error creating ROLE: ${err}`;
        }
    }


    for (let schema of additionalSchemaNames) {
        for (let role of roles) {

            if (!(excludeReadonlyAccessToSchemas.includes(schema) && role.includes(roleReadOnlySuffix))) {
                try {
                    snowflake.execute({ sqlText: `GRANT SELECT, REFERENCES ON ALL TABLES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role}` });
                } catch (err) {
                    return `Error creating schema: ${err}`;
                }

                try {
                    snowflake.execute({ sqlText: `GRANT SELECT, REFERENCES ON  FUTURE TABLES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role}` });
                } catch (err) {
                    return `Error creating schema: ${err}`;
                }

                try {
                    snowflake.execute({ sqlText: `GRANT SELECT ON ALL DYNAMIC TABLES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role}` });
                } catch (err) {
                    return `Error creating schema: ${err}`;
                }

                try {
                    snowflake.execute({ sqlText: `GRANT SELECT ON  FUTURE DYNAMIC TABLES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role}` });
                } catch (err) {
                    return `Error creating schema: ${err}`;
                }

                try {
                    snowflake.execute({ sqlText: `GRANT USAGE ON ALL FUNCTIONS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role}` });
                } catch (err) {
                    return `Error creating schema: ${err}`;
                }

                try {
                    snowflake.execute({ sqlText: `GRANT USAGE ON  FUTURE FUNCTIONS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role}` });
                } catch (err) {
                    return `Error creating schema: ${err}`;
                }
                try {
                    snowflake.execute({ sqlText: `GRANT SELECT ON ALL MATERIALIZED VIEWS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role}` });
                } catch (err) {
                    return `Error creating schema: ${err}`;
                }

                try {
                    snowflake.execute({ sqlText: `GRANT SELECT ON FUTURE MATERIALIZED VIEWS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role}` });
                } catch (err) {
                    return `Error creating schema: ${err}`;
                }
                try {
                    snowflake.execute({ sqlText: `GRANT SELECT ON ALL VIEWS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role}` });
                } catch (err) {
                    return `Error creating schema: ${err}`;
                }

                try {
                    snowflake.execute({ sqlText: `GRANT SELECT ON FUTURE VIEWS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role}` });
                } catch (err) {
                    return `Error creating schema: ${err}`;
                }
            }

            if (role.includes(roleEditorSuffix)) {
                try {
                    snowflake.execute({ sqlText: `GRANT OWNERSHIP ON ALL TABLES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS` });
                } catch (err) {
                    return `Error creating schema: ${err}`;
                }
                try {
                    snowflake.execute({ sqlText: `GRANT OWNERSHIP ON ALL MATERIALIZED VIEWS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS` });
                } catch (err) {
                    return `Error creating schema: ${err}`;
                }
                try {
                    snowflake.execute({ sqlText: `GRANT OWNERSHIP ON ALL PROCEDURES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS` });
                } catch (err) {
                    return `Error creating schema: ${err}`;
                }
                try {
                    snowflake.execute({ sqlText: `GRANT OWNERSHIP ON ALL VIEWS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS` });
                } catch (err) {
                    return `Error creating schema: ${err}`;
                }
                try {
                    snowflake.execute({ sqlText: `GRANT OWNERSHIP ON ALL SEQUENCES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS` });
                } catch (err) {
                    return `Error creating schema: ${err}`;
                }
                try {
                    snowflake.execute({ sqlText: `GRANT OWNERSHIP ON ALL FUNCTIONS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS` });
                } catch (err) {
                    return `Error creating schema: ${err}`;
                }
                try {
                    snowflake.execute({ sqlText: `GRANT OWNERSHIP ON ALL FUTURE TABLES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS` });
                } catch (err) {
                    return `Error creating schema: ${err}`;
                }
                try {
                    snowflake.execute({ sqlText: `GRANT OWNERSHIP ON ALL FUTURE MATERIALIZED VIEWS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS` });
                } catch (err) {
                    return `Error creating schema: ${err}`;
                }
                try {
                    snowflake.execute({ sqlText: `GRANT OWNERSHIP ON ALL FUTURE PROCEDURES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS` });
                } catch (err) {
                    return `Error creating schema: ${err}`;
                }
                try {
                    snowflake.execute({ sqlText: `GRANT OWNERSHIP ON ALL FUTURE VIEWS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS` });
                } catch (err) {
                    return `Error creating schema: ${err}`;
                }
                try {
                    snowflake.execute({ sqlText: `GRANT OWNERSHIP ON ALL FUTURE SEQUENCES IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS` });
                } catch (err) {
                    return `Error creating schema: ${err}`;
                }
                try {
                    snowflake.execute({ sqlText: `GRANT OWNERSHIP ON ALL FUTURE FUNCTIONS IN SCHEMA ${databaseName}.${schema} TO ROLE ${role} COPY CURRENT GRANTS` });
                } catch (err) {
                    return `Error creating schema: ${err}`;
                }


            }
           

        }
    }


}

';
CREATE OR REPLACE PROCEDURE "GRANT_ACCESS_WITH_PRIVILEGES_IF_ROLES_MATCH"()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
  // Set the desired tag naming convention.
  var stgTagNamingConvention = ''_STG'';



    const rolePermissions = [];
    var rolePermissionsQuery = snowflake.execute( { sqlText: "Select LIKE_MATCHES, ifNULL(SCHEMA_PERMISSIONS, []) as SCHEMA_PERMISSIONS , PUBLIC_ONLY from ADMINISTRATION.PUBLIC.DEFAULT_ROLE_PERMISSIONS_ASSIGNED " } ); 
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